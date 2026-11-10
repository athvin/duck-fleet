from dataclasses import dataclass, field

from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckfleet.client import RemoteConnection
from dbt.adapters.exceptions import FailedToConnectError


@dataclass
class DuckFleetCredentials(DuckDBCredentials):
    endpoint: str = "http://server:8080"
    token: str = field(default="", repr=False)

    @classmethod
    def __pre_deserialize__(cls, data):
        data = dict(data)
        data.setdefault("path", "lake")
        data.setdefault("database", "lake")
        return super().__pre_deserialize__(data)

    def __post_init__(self):
        super().__post_init__()
        self._ducklake_dbs = {"lake"}
        if self.database != "lake" or self.disable_transactions:
            raise ValueError("local DuckFleet requires database lake and enabled transactions")
        if self.attach or self.extensions or self.plugins or self.secrets or self.settings or self.config_options or self.remote:
            raise ValueError("configure DuckDB on the server, not in the remote dbt profile")
        if not self.token:
            raise ValueError("DuckFleet requires a bearer token")

    @property
    def type(self):
        return "duckfleet"

    @property
    def unique_field(self):
        return self.endpoint

    def _connection_keys(self):
        return ("endpoint", "database", "schema")


class DuckFleetConnectionManager(DuckDBConnectionManager):
    TYPE = "duckfleet"

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            return connection
        credentials = connection.credentials
        try:
            connection.handle = RemoteConnection(credentials.endpoint, credentials.token, connection.name)
            connection.state = ConnectionState.OPEN
        except Exception as error:
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(error)) from error
        return connection

    def add_query(self, *args, **kwargs):
        connection = self.get_thread_connection()
        connection.handle.context["connection_name"] = connection.name
        return super().add_query(*args, **kwargs)

    def cancel(self, connection):
        connection.handle.cancel()
