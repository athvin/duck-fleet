# adbc-duckfleet (prototype)

This is the discovery prototype for [decision 0001](../../docs/decisions/0001-connectivity.md): an ADBC driver over DuckFleet's HTTP/Arrow API, built on the `duckfleet` SDK. **It is not a supported driver.** #189 productionizes it.

```sh
cargo build --release   # target/release/libadbc_duckfleet.{dylib,so}, entrypoint AdbcDuckfleetInit
```

```python
import adbc_driver_manager.dbapi as dbapi
conn = dbapi.connect(driver="target/release/libadbc_duckfleet.dylib", entrypoint="AdbcDuckfleetInit",
                     db_kwargs={"uri": "http://127.0.0.1:18080", "password": "<token>"})
cur = conn.cursor()
cur.execute("SELECT ?::INTEGER + 1 AS answer", (41,))
print(cur.fetch_arrow_table())
```

Implemented: single-row parameter binding, Arrow results, autocommit/commit/rollback, cancellation, `get_table_schema`, and SQLSTATE mapping of classified failures. Everything else returns `NOT_IMPLEMENTED`.
