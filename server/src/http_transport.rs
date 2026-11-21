//! A system result owns an absolute delivery deadline through Hyper's buffers.
//!
//! Timing out a handler or an Axum Body only covers work until headers/a frame
//! are yielded. Hyper can retain that frame while a peer stops reading. Keep the
//! timer in the Bytes owner instead, and close the actual socket on expiry.
//! Other routes never arm this timer. HTTP/2 shares its connection, so expiry of
//! one system response also closes the other streams on that connection.
use axum::{extract::connect_info::Connected, serve::IncomingStream};
use std::{
    io,
    net::{Shutdown, SocketAddr},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct Listener(TcpListener);

impl Listener {
    pub fn new(listener: TcpListener) -> Self {
        Self(listener)
    }
}

/// One extra descriptor shares the same socket with the async transport. A
/// shutdown through this handle wakes blocked Hyper reads/writes and releases
/// its buffered Bytes; no permit is released while those Bytes remain retained.
#[derive(Clone, Debug)]
pub(crate) struct Connection(Arc<std::net::TcpStream>);

pub(crate) struct Io {
    stream: TcpStream,
    connection: Connection,
}

impl Connected<IncomingStream<'_, Listener>> for Connection {
    fn connect_info(stream: IncomingStream<'_, Listener>) -> Self {
        stream.io().connection.clone()
    }
}

impl axum::serve::Listener for Listener {
    type Io = Io;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            // Reuse Axum's bounded backoff for accept errors.
            let (stream, address) = axum::serve::Listener::accept(&mut self.0).await;
            let wrapped = (|| -> io::Result<Io> {
                // into_std preserves nonblocking mode; from_std requires it.
                let stream = stream.into_std()?;
                let connection = Connection(Arc::new(stream.try_clone()?));
                Ok(Io {
                    stream: TcpStream::from_std(stream)?,
                    connection,
                })
            })();
            match wrapped {
                Ok(stream) => return (stream, address),
                Err(error) => {
                    tracing::warn!(%error, "could not initialize HTTP connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

impl AsyncRead for Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

pub(crate) struct DeliveryGuard {
    finished: Arc<AtomicBool>,
    timer: JoinHandle<()>,
}

impl Connection {
    pub fn delivery(&self) -> DeliveryGuard {
        let connection = self.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let observed = finished.clone();
        // Capture the deadline now, rather than when the spawned task is polled.
        let deadline = tokio::time::Instant::now() + DELIVERY_TIMEOUT;
        let timer = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            // There is no await between winning expiry and shutting down. If
            // owner Drop wins first, this old timer cannot close a reused socket.
            if !observed.swap(true, Ordering::AcqRel) {
                let _ = connection.0.shutdown(Shutdown::Both);
            }
        });
        DeliveryGuard { finished, timer }
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        self.finished.swap(true, Ordering::AcqRel);
        self.timer.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_result::SystemResult;
    use axum::{
        Router,
        extract::{ConnectInfo, State},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::get,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpSocket,
        sync::Semaphore,
        time::{Instant, timeout},
    };

    struct Server {
        address: SocketAddr,
        slots: Arc<Semaphore>,
        task: JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn large(
        State(slots): State<Arc<Semaphore>>,
        ConnectInfo(connection): ConnectInfo<Connection>,
    ) -> Response {
        let Ok(permit) = slots.try_acquire_owned() else {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        };
        SystemResult::from_arrow_for_test(vec![b'x'; 8 * 1024 * 1024], Some(permit))
            .with_http_delivery(&connection)
            .into_response()
    }

    async fn small(
        State(slots): State<Arc<Semaphore>>,
        ConnectInfo(connection): ConnectInfo<Connection>,
    ) -> Response {
        let Ok(permit) = slots.try_acquire_owned() else {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        };
        SystemResult::from_arrow_for_test(b"result".to_vec(), Some(permit))
            .with_http_delivery(&connection)
            .into_response()
    }

    async fn server() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let slots = Arc::new(Semaphore::new(2));
        let router = Router::new()
            .route("/large", get(large))
            .route("/small", get(small))
            .route(
                "/slow",
                get(|| async {
                    // Longer than a completed system response's old timer.
                    tokio::time::sleep(DELIVERY_TIMEOUT + Duration::from_millis(250)).await;
                    "ordinary response"
                }),
            )
            .with_state(slots.clone());
        let task = tokio::spawn(async move {
            axum::serve(
                Listener::new(listener),
                router.into_make_service_with_connect_info::<Connection>(),
            )
            .await
            .unwrap();
        });
        Server {
            address,
            slots,
            task,
        }
    }

    async fn request(stream: &mut TcpStream, path: &str) {
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
    }

    async fn headers(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            assert!(bytes.len() < 4096, "unexpected HTTP header length");
            bytes.push(stream.read_u8().await.unwrap());
        }
        String::from_utf8(bytes).unwrap()
    }

    async fn response(stream: &mut TcpStream) -> Vec<u8> {
        let headers = headers(stream).await;
        assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .expect("bounded response has a content length");
        assert!(length < 1024, "test should only consume small responses");
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        body
    }

    async fn stalled_client(address: SocketAddr) -> TcpStream {
        let socket = TcpSocket::new_v4().unwrap();
        // Prevent either platform's TCP receive autotuning from absorbing the
        // complete result and making this a body-drop rather than stall test.
        socket.set_recv_buffer_size(1024).unwrap();
        let mut stream = socket.connect(address).await.unwrap();
        request(&mut stream, "/large").await;
        let head = timeout(Duration::from_secs(2), headers(&mut stream))
            .await
            .unwrap();
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        stream
    }

    #[tokio::test]
    async fn stalled_hyper_buffers_keep_permits_until_transport_deadline_closes_connections() {
        let server = server().await;
        let began = Instant::now();
        let first = stalled_client(server.address).await;
        let second = stalled_client(server.address).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(server.slots.available_permits(), 0);

        // These real clients remain alive and do not read or close. The owner
        // lives beyond Body::poll_frame, in Hyper's stalled output buffers.
        timeout(DELIVERY_TIMEOUT + Duration::from_secs(2), async {
            while server.slots.available_permits() != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expired transports must drop both buffered owners and permits");
        assert!(began.elapsed() >= DELIVERY_TIMEOUT);
        let mut recovered = TcpStream::connect(server.address).await.unwrap();
        request(&mut recovered, "/small").await;
        assert_eq!(
            timeout(Duration::from_secs(2), response(&mut recovered))
                .await
                .unwrap(),
            b"result"
        );
        drop((first, second));
    }

    #[tokio::test]
    async fn completed_owner_disarms_timer_before_long_non_system_keepalive_response() {
        let server = server().await;
        let mut stream = TcpStream::connect(server.address).await.unwrap();
        request(&mut stream, "/small").await;
        assert_eq!(
            timeout(Duration::from_secs(2), response(&mut stream))
                .await
                .unwrap(),
            b"result"
        );
        assert_eq!(server.slots.available_permits(), 2);
        // Reuse the exact same TCP socket. The old system response's deadline
        // must not interrupt this ordinary route while it is still executing.
        request(&mut stream, "/slow").await;
        assert_eq!(
            timeout(
                DELIVERY_TIMEOUT + Duration::from_secs(2),
                response(&mut stream)
            )
            .await
            .unwrap(),
            b"ordinary response"
        );
    }
}
