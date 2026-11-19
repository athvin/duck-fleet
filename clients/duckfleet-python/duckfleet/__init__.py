"""Small DB-API-style transport. No local DuckDB connection or SQL retry path.

The whole client surface for DuckFleet, and deliberately its own package rather than
part of the dbt adapter. It depends on `pyarrow` and the standard library, so using
the platform does not mean installing dbt-core. The dbt adapter imports this, so
there is one definition of the wire protocol rather than two that can drift.
"""
import datetime
import decimal
import http.client
import json
import time
import urllib.error
import urllib.request
import uuid

import pyarrow.ipc


def encode_parameter(value):
    if isinstance(value, (decimal.Decimal, datetime.date, datetime.time)):
        return str(value)
    raise TypeError(f"Unsupported parameter type: {type(value).__name__}")


class RemoteError(RuntimeError):
    """A failure the server classified — or one the transport could not deliver.

    `retry_safe` repeats the server's judgement and is true only where the platform
    proved the request left no partial effect. Acting on it is the caller's choice:
    this transport never replays a statement by itself.

    A request that dies in transport (reset, refused, timed out) is raised as
    `failure_kind="transport_failure"` with `status=None` and `retry_safe=False`:
    whether it ran is unknown to the client, but not unknowable — the server records
    every statement in `query_history` before executing it, so its true outcome can
    be read back by `request_id`.
    """

    def __init__(self, status, message, failure_kind=None, retry_safe=False):
        super().__init__(message)
        self.status = status
        self.failure_kind = failure_kind
        self.retry_safe = retry_safe


class Client:
    def __init__(self, endpoint, token):
        self.endpoint = endpoint.rstrip("/")
        self.token = token

    def request(self, method, path, payload=None, timeout=75):
        body = None if payload is None else json.dumps(payload, default=encode_parameter).encode()
        request = urllib.request.Request(
            self.endpoint + path, data=body, method=method,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
        )
        try:
            # Default is longer than the server's maximum statement deadline. Transport
            # failure is an unknown outcome, never permission to replay a write. Opening
            # an isolated session is the one call that can legitimately run longer than
            # a statement — the gateway holds the request while a node is provisioned —
            # so that path passes its own, larger timeout rather than racing the server's
            # own ready budget and retrying into a second pod (#90).
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            message = error.read().decode()
            kind, retry_safe = None, False
            try:
                payload = json.loads(message)
                message = payload["error"]
                kind, retry_safe = payload.get("failure_kind"), bool(payload.get("retry_safe"))
            except (ValueError, KeyError):
                pass
            raise RemoteError(error.code, message, kind, retry_safe) from error
        except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
            # A raw socket error is the one shape a caller cannot act on, so it is
            # classified here. Never retry-safe: the request may have reached the
            # server and run, and only `query_history` can say.
            raise RemoteError(
                None,
                "the request failed in transport before any response arrived, so "
                "whether it ran is unknown; check /v1/queries for its request_id "
                f"before resending a write ({type(error).__name__}: {error})",
                "transport_failure", False) from error


# Refusals a new session may simply wait out: a rollout drains the old gateway
# (`draining`), replaces it (transport failures, or an unclassified 5xx from a
# Service with no ready endpoint), and a worker may still be attaching its catalog
# (`starting`). Opening a session runs no SQL, so waiting costs nothing but time.
# Statements are never retried by this client under any classification.
_OPEN_WAITABLE_KINDS = frozenset({"draining", "starting", "transport_failure"})


class RemoteConnection:
    # Covers the server's own ready budget with margin: an isolated open may hold the
    # request while a node is provisioned, and the client should wait for the gateway's
    # classified verdict rather than time out in transport first — a transport timeout
    # is unknown-outcome and would retry into a second pod.
    OPEN_TIMEOUT = 450

    def __init__(self, endpoint, token, name="client", compute=None, context=None,
                 open_retry_seconds=90):
        self.client = Client(endpoint, token)
        self.context = dict(context or {}, connection_name=name)
        # Bounded by wall clock rather than attempts, sized to a rollout: the old
        # gateway may drain for up to 75 seconds before its replacement serves.
        # `open_retry_seconds=0` disables the wait entirely.
        deadline = time.monotonic() + open_retry_seconds
        while True:
            try:
                info = json.loads(self.client.request("POST", "/v1/sessions", {
                    "compute": compute or {}, "context": self.context,
                }, timeout=self.OPEN_TIMEOUT))
                break
            except RemoteError as error:
                waitable = error.failure_kind in _OPEN_WAITABLE_KINDS or (
                    error.failure_kind is None and error.status in (502, 503, 504))
                if not waitable or time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        self.session_id = info["session_id"]
        self.placement = info["placement"]
        self.closed = False

    @property
    def path(self):
        return f"/v1/sessions/{self.session_id}"

    def _request(self, method, suffix="", payload=None):
        """A session-scoped request, with the one session-scoped failure explained.

        `unknown_session` is what a gateway restart or idle expiry looks like from
        here, and the server's one-liner does not say what to do about it.
        """
        try:
            return self.client.request(method, self.path + suffix, payload)
        except RemoteError as error:
            if error.failure_kind != "unknown_session":
                raise
            raise RemoteError(
                error.status,
                f"{error} — this session no longer exists (the gateway restarted, or "
                "it sat idle past expiry); uncommitted work from it can only be "
                "rolled back, never committed. Open a new connection, and check "
                "/v1/queries by request_id before resending any write",
                error.failure_kind, error.retry_safe) from error

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
            try:
                self.client.request("DELETE", self.path)
            except RemoteError as error:
                # The session being already gone is what closing wanted; anything
                # else still raises, because the session has not been released.
                if error.failure_kind != "unknown_session":
                    raise
            self.closed = True

    def cancel(self):
        return json.loads(self._request("POST", "/cancel"))


class RemoteCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rows = []
        self.position = 0
        self.rowcount = -1

    def execute(self, sql, parameters=None):
        context = dict(self.connection.context)
        self.query_id = str(uuid.uuid4())
        result = self.connection._request("POST", "/queries", {
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
