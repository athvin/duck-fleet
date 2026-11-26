# DuckFleet ADBC driver

`duckfleet-adbc` is an [ADBC](https://arrow.apache.org/adbc/) driver over DuckFleet's HTTP/Arrow API, built on the `duckfleet` Rust SDK. It is the primary interoperability path chosen in [decision 0001](decisions/0001-connectivity.md). Arrow-native clients such as pyarrow, pandas, Polars, DuckDB, and R get typed Arrow results through the same session, request-identity, cancellation, and failure contracts as the SDK and CLI. The driver never replays SQL.

## Install

Download `duckfleet-adbc-<version>-<target>.tar.gz` from the `duckfleet-adbc-v<version>` GitHub release, check it against `SHA256SUMS`, extract it, and run the installer:

```sh
sha256sum -c SHA256SUMS --ignore-missing
tar -xzf duckfleet-adbc-0.1.0-x86_64-unknown-linux-gnu.tar.gz
sh duckfleet-adbc-0.1.0-x86_64-unknown-linux-gnu/install.sh            # per-user driver directory
sh duckfleet-adbc-0.1.0-x86_64-unknown-linux-gnu/install.sh --prefix /opt/adbc/drivers
```

The installer copies the shared library and writes a `duckfleet.toml` driver manifest. The default directory is `~/.config/adbc/drivers` on Linux and `~/Library/Application Support/ADBC/Drivers` on macOS. Driver managers then load the driver by the name `duckfleet`; with `--prefix`, set `ADBC_DRIVER_PATH` to that directory. Release targets: `aarch64-apple-darwin`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`. To build from source, run `cargo build --release -p duckfleet-adbc` in `server/`. That produces `libadbc_duckfleet.{so,dylib}` with entrypoint `AdbcDuckfleetInit`.

## Connect

```python
import adbc_driver_manager.dbapi as dbapi

conn = dbapi.connect(driver="duckfleet", db_kwargs={
    "uri": "https://duckfleet.example.com",    # the gateway's HTTP endpoint
    "password": "<DuckFleet credential>",      # or "adbc.duckfleet.token"
    "adbc.duckfleet.pool": "small",            # optional compute selection
})
cur = conn.cursor()
cur.execute("SELECT ?::INTEGER + 1 AS answer, ?::DECIMAL(10,2) AS price", (41, "9.99"))
print(cur.fetch_arrow_table())    # answer: int32, price: decimal128(10, 2)
df = cur.fetch_df()               # pandas, via the same cursor
```

| Option | Meaning |
|---|---|
| `uri` | Gateway HTTP(S) endpoint. TLS and exposure work as for any HTTP client. |
| `password` / `adbc.duckfleet.token` | A DuckFleet credential (bootstrap token or one from `/v1/credentials`) |
| `adbc.duckfleet.mode` | `persistent` or `isolated` |
| `adbc.duckfleet.pool`, `.cpu`, `.memory`, `.disk` | Explicit compute, fixed for the connection's session |

Each ADBC connection is one DuckFleet session, so temporary tables persist across statements and the compute selection stays fixed. Closing the connection rolls back uncommitted work.

## Behavior

- **Parameters** bind one row of scalars: integers, unsigned integers, floats, strings, booleans, and nulls. Bind decimals, dates, and timestamps as text and cast them in SQL (`?::DECIMAL(10,2)`). Other parameter types, and non-finite floats, are refused before sending.
- **Results** stream the server's Arrow batches unchanged: decimals, microsecond timestamps, lists, structs, and binary. The interactive cap of 100,000 rows / 16 MiB applies. Larger results belong to durable jobs (#190).
- **Transactions**: autocommit is on by default. Turning it off begins a transaction, and `commit()`/`rollback()` end it and begin the next.
- **Cancellation**: `AdbcStatementCancel` / `cursor.adbc_cancel()` cancels through the session cancel endpoint.
- **Failures** keep `failure_kind`, `retry_safe`, and the request ID in the message and map to SQLSTATE: `40001` lost commit race, `57014` canceled or timed out, `42601` SQL error, `28000` authentication, `42501` permission, `08006` uncertain transport or lost session. Nothing is resent automatically.
- **Metadata**: `get_info` (driver, vendor, engine version), `get_table_types`, `get_objects` at every depth with ADBC search-pattern filters, and `get_table_schema`. All of them read the principal's authorized `information_schema`.
- **Not implemented** (they return `NOT_IMPLEMENTED`): bulk ingest, partitioned execution, Substrait, statistics, multi-row parameter binding, and statement options.

`make test-adbc` runs the real-client suite against Compose through adbc-driver-manager 1.13.0, pyarrow 26.0.1, and pandas 3.1.1.
