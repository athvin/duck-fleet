"""Exercise Pod Identity credential loading and a real refresh/retry cycle.

No AWS account or real credentials. The first fixture key is rejected by the S3
endpoint with HTTP 403, which must fetch a second key and retry the request.
"""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

import duckdb

credential_requests = []
credential_keys = []
s3_requests = []


class Credentials(BaseHTTPRequestHandler):
    def do_GET(self):
        credential_requests.append(self.headers.get('Authorization'))
        if credential_requests[-1] != 'test-pod-identity-token':
            self.send_error(403)
            return
        key = 'TESTOLDLOCALONLY' if len(credential_keys) == 0 else 'TESTNEWLOCALONLY'
        credential_keys.append(key)
        body = json.dumps({
            'AccessKeyId': key,
            'SecretAccessKey': 'test-local-secret',
            'Token': 'test-local-session',
            'Expiration': (datetime.now(timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        }).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


class S3(BaseHTTPRequestHandler):
    def reply(self):
        authorization = self.headers.get('Authorization') or ''
        s3_requests.append(authorization)
        if 'Credential=TESTOLDLOCALONLY/' in authorization:
            self.send_error(403)
            return
        if 'Credential=TESTNEWLOCALONLY/' not in authorization:
            self.send_error(403)
            return
        body = b'value\n42\n'
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    do_GET = reply
    do_HEAD = reply

    def log_message(self, *_):
        pass


def serve(server):
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


with TemporaryDirectory() as directory:
    token = Path(directory) / 'token'
    token.write_text('test-pod-identity-token')
    credentials = HTTPServer(('127.0.0.1', 0), Credentials)
    storage = HTTPServer(('127.0.0.1', 0), S3)
    threads = [serve(credentials), serve(storage)]
    os.environ['AWS_CONTAINER_CREDENTIALS_FULL_URI'] = f'http://127.0.0.1:{credentials.server_port}/v1/credentials'
    os.environ['AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE'] = str(token)
    os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
    try:
        connection = duckdb.connect()
        connection.execute("SET extension_directory='/opt/duckdb/extensions'; LOAD icu; LOAD httpfs; LOAD postgres; LOAD ducklake; LOAD aws")
        connection.execute(
            "CREATE SECRET aws_s3 (TYPE S3, PROVIDER credential_chain, REFRESH auto, "
            f"ENDPOINT '127.0.0.1:{storage.server_port}', URL_STYLE 'path', USE_SSL false, "
            "REGION 'us-east-2', SCOPE 's3://test/lake/')"
        )
        rows = connection.execute("SELECT * FROM read_csv('s3://test/lake/fixture.csv')").fetchall()
        assert rows == [(42,)], rows
        assert credential_requests and all(value == 'test-pod-identity-token' for value in credential_requests)
        assert credential_keys[:2] == ['TESTOLDLOCALONLY', 'TESTNEWLOCALONLY'], credential_keys
        old = next(i for i, value in enumerate(s3_requests) if 'Credential=TESTOLDLOCALONLY/' in value)
        new = next(i for i, value in enumerate(s3_requests) if 'Credential=TESTNEWLOCALONLY/' in value)
        assert old < new, s3_requests
        assert connection.execute("SELECT provider FROM duckdb_secrets() WHERE name='aws_s3'").fetchone() == ('credential_chain',)
        extensions = connection.execute(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('httpfs', 'postgres_scanner', 'ducklake', 'aws', 'icu') AND loaded"
        ).fetchall()
        assert {row[0] for row in extensions} == {'httpfs', 'postgres_scanner', 'ducklake', 'aws', 'icu'}
        assert all(version for _, version in extensions)
        connection.close()
        print('PASS: container credentials used the token file and a 403 fetched fresh credentials and retried S3')
    finally:
        credentials.shutdown()
        storage.shutdown()
        for thread in threads:
            thread.join()
        credentials.server_close()
        storage.server_close()
