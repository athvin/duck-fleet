//! An uncertain write is reported, never resent: a gateway that accepts the
//! statement request and then drops the connection sees exactly one attempt.

use duckfleet::{Client, ErrorKind, Param, SessionOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&buffer);
        if let Some(end) = text.find("\r\n\r\n") {
            let length = text[..end]
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if buffer.len() >= end + 4 + length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

#[tokio::test]
async fn a_statement_lost_in_transport_is_uncertain_and_sent_once() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let statements = Arc::new(AtomicUsize::new(0));
    let seen = statements.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            tokio::spawn(async move {
                let request = read_request(&mut stream).await;
                if request.starts_with("POST /v1/sessions/s-1/queries") {
                    seen.fetch_add(1, Ordering::SeqCst);
                    drop(stream);
                    return;
                }
                let body = r#"{"session_id":"s-1","placement":{}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    let client = Client::new(&endpoint, "token").unwrap();
    let session = client
        .session(SessionOptions::named("transport-test"))
        .await
        .unwrap();
    let error = session
        .query("INSERT INTO t VALUES (?)", [Param::from(1_i64)])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TransportUncertain, "{error:?}");
    assert_eq!(error.acceptance(), Some("unknown"));
    assert!(!error.retry_safe());
    assert!(error.request_id().is_some());
    assert!(error.status_url().unwrap().starts_with("/v1/requests/"));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        statements.load(Ordering::SeqCst),
        1,
        "the statement was resent"
    );
    server.abort();
}
