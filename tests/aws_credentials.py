"""Exercise the actual DuckDB AWS extension with a local Pod Identity fixture.

No AWS account or real credentials. This checks the SDK's container endpoint,
authorization-token-file support, and REFRESH syntax for the pinned extension.
The live EKS harness remains necessary to prove IAM and actual S3 access.
"""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

import duckdb

requests = []


class Credentials(BaseHTTPRequestHandler):
    def do_GET(self):
        requests.append(self.headers.get('Authorization'))
        if requests[-1] != 'test-pod-identity-token':
            self.send_error(403)
            return
        body = json.dumps({
            'AccessKeyId': 'TESTLOCALONLY',
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


with TemporaryDirectory() as directory:
    token = Path(directory) / 'token'
    token.write_text('test-pod-identity-token')
    server = HTTPServer(('127.0.0.1', 0), Credentials)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    os.environ['AWS_CONTAINER_CREDENTIALS_FULL_URI'] = f'http://127.0.0.1:{server.server_port}/v1/credentials'
    os.environ['AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE'] = str(token)
    os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
    try:
        connection = duckdb.connect()
        connection.execute("SET extension_directory='/opt/duckdb/extensions'; LOAD httpfs; LOAD aws")
        connection.execute("CREATE SECRET aws_s3 (TYPE S3, PROVIDER credential_chain, REFRESH auto, REGION 'us-east-2', SCOPE 's3://test/lake/')")
        assert requests and all(value == 'test-pod-identity-token' for value in requests)
        assert connection.execute("SELECT provider FROM duckdb_secrets() WHERE name='aws_s3'").fetchone() == ('credential_chain',)
        connection.close()
        print('PASS: pinned DuckDB extension fetched container credentials using the authorization token file')
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
