"""Small DB-API-style transport. No local DuckDB connection or SQL retry path."""
import datetime
import decimal
import json
import re
import urllib.error
import urllib.request
import uuid

import pyarrow.ipc


def encode_parameter(value):
    if isinstance(value, (decimal.Decimal, datetime.date, datetime.time)):
        return str(value)
    raise TypeError(f"Unsupported parameter type: {type(value).__name__}")


class RemoteError(RuntimeError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class Client:
    def __init__(self, endpoint, token):
        self.endpoint = endpoint.rstrip("/")
        self.token = token

    def request(self, method, path, payload=None):
        body = None if payload is None else json.dumps(payload, default=encode_parameter).encode()
        request = urllib.request.Request(
            self.endpoint + path, data=body, method=method,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
        )
        try:
            # Longer than the server's maximum statement deadline. Transport failure
            # is an unknown outcome, never permission to replay a write.
            with urllib.request.urlopen(request, timeout=75) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            message = error.read().decode()
            try:
                message = json.loads(message)["error"]
            except (ValueError, KeyError):
                pass
            raise RemoteError(error.code, message) from error


class RemoteConnection:
    def __init__(self, endpoint, token, name="client"):
        self.client = Client(endpoint, token)
        info = json.loads(self.client.request("POST", "/v1/sessions"))
        self.session_id = info["session_id"]
        self.context = {"connection_name": name}
        self.closed = False

    @property
    def path(self):
        return f"/v1/sessions/{self.session_id}"

    def cursor(self):
        if self.closed:
            raise RuntimeError("connection is closed")
        return RemoteCursor(self)

    def commit(self):
        self.cursor().execute("COMMIT")

    def rollback(self):
        self.cursor().execute("ROLLBACK")

    def close(self):
        if not self.closed:
            self.client.request("DELETE", self.path)
            self.closed = True

    def cancel(self):
        return json.loads(self.client.request("POST", self.path + "/cancel"))


class RemoteCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rows = []
        self.position = 0
        self.rowcount = -1

    def execute(self, sql, parameters=None):
        context = dict(self.connection.context)
        match = re.search(r"/\*\s*(\{.*?\})\s*\*/", sql, flags=re.DOTALL)
        if match:
            try:
                context["dbt"] = json.loads(match.group(1))
            except ValueError:
                pass
        dbt = context.get("dbt", {})
        if dbt.get("node_id") and dbt.get("invocation_id"):
            context["model_run_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, dbt["invocation_id"] + ":" + dbt["node_id"]))
        if dbt.get("platform", {}).get("compute", "persistent") != "persistent":
            raise RuntimeError("local DuckFleet supports only persistent compute")
        self.query_id = str(uuid.uuid4())
        result = self.connection.client.request("POST", self.connection.path + "/queries", {
            "request_id": self.query_id, "sql": sql, "parameters": list(parameters or []), "context": context,
        })
        table = pyarrow.ipc.open_stream(result).read_all()
        self.description = [(field.name, str(field.type), None, None, None, None, field.nullable) for field in table.schema]
        # Column-wise conversion also preserves duplicate column names, decimals,
        # dates, and nulls; dict-based row conversion would lose duplicate names.
        self.rows = list(zip(*(column.to_pylist() for column in table.columns)))
        self.position = 0
        self.rowcount = table.num_rows
        return self

    def fetchone(self):
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size=1):
        rows = self.rows[self.position:self.position + size]
        self.position += len(rows)
        return rows

    def fetchall(self):
        return self.fetchmany(len(self.rows))

    def close(self):
        self.rows = []
