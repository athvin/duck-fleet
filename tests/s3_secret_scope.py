"""Prove a bucket-scoped S3 secret is not sent to a prefix-neighbor bucket."""
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import duckdb

requests = []


class S3(BaseHTTPRequestHandler):
    def record(self):
        requests.append((self.path, self.headers.get("Authorization")))
        self.send_error(404)

    do_GET = record
    do_HEAD = record

    def log_message(self, *_):
        pass


server = HTTPServer(("127.0.0.1", 0), S3)
thread = Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    connection = duckdb.connect()
    connection.execute("SET extension_directory='/opt/duckdb/extensions'; LOAD httpfs")
    connection.execute(
        "CREATE SECRET scoped (TYPE S3, KEY_ID 'test', SECRET 'test', "
        f"ENDPOINT '127.0.0.1:{server.server_port}', URL_STYLE 'path', USE_SSL false, "
        "REGION 'us-east-1', SCOPE 's3://test/lake/')"
    )
    try:
        connection.execute("SELECT * FROM read_csv('s3://test/lake/fixture.csv')")
    except duckdb.Error:
        pass
    assert any(path.startswith("/test/lake/") and authorization for path, authorization in requests), requests

    requests.clear()
    try:
        connection.execute("SELECT * FROM read_csv('s3://test-other/lake/fixture.csv')")
    except duckdb.Error:
        pass
    assert all(authorization is None for _, authorization in requests), requests
    assert not any(path.startswith("/test/") for path, _ in requests), requests
    print("PASS: scoped S3 credentials were not sent to a prefix-neighbor bucket")
finally:
    server.shutdown()
    thread.join()
    server.server_close()
