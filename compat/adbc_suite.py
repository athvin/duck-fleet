"""Real-client suite for the DuckFleet ADBC driver; run in a container by scripts/test-adbc.sh."""

import os
import sys
import threading
import time

import adbc_driver_manager
import adbc_driver_manager.dbapi as dbapi

DRIVER = os.environ["ADBC_DRIVER"]
URL = os.environ.get("DUCKFLEET_URL", "http://127.0.0.1:18080")
TOKEN = os.environ.get("DUCKFLEET_TOKEN", "duckfleet-local-development-token")
failures = []


def check(name):
    def wrap(fn):
        try:
            fn()
            print(f"ok   {name}")
        except Exception as error:
            failures.append(name)
            print(f"FAIL {name}: {type(error).__name__}: {error}")
        return fn

    return wrap


def connect(autocommit=True, **extra):
    return dbapi.connect(
        driver=DRIVER,
        entrypoint="AdbcDuckfleetInit",
        db_kwargs={"uri": URL, "password": TOKEN, **extra},
        autocommit=autocommit,
    )


conn = connect()
setup = conn.cursor()
setup.execute("CREATE SCHEMA IF NOT EXISTS adbc_meta")
setup.execute("CREATE OR REPLACE TABLE adbc_meta.events (id BIGINT NOT NULL, label VARCHAR, happened_at TIMESTAMP)")
setup.execute("CREATE OR REPLACE VIEW adbc_meta.events_view AS SELECT id FROM adbc_meta.events")


@check("typed parameters and typed Arrow results")
def _():
    cur = conn.cursor()
    cur.execute(
        "SELECT ?::INTEGER + 1 AS answer, ?::VARCHAR AS name, ?::DECIMAL(10,2) AS price, ?::TIMESTAMP AS at, NULL::INTEGER AS n",
        (41, "héllo", "9.99", "2026-01-02 03:04:05.123456"),
    )
    table = cur.fetch_arrow_table()
    types = {f.name: str(f.type) for f in table.schema}
    assert table.to_pylist()[0]["answer"] == 42
    assert types == {"answer": "int32", "name": "string", "price": "decimal128(10, 2)", "at": "timestamp[us]", "n": "int32"}, types


@check("nested and binary values stay Arrow-typed")
def _():
    cur = conn.cursor()
    cur.execute("SELECT [1, 2, NULL] AS l, {'a': 1} AS s, '\\xAA'::BLOB AS b")
    row = cur.fetch_arrow_table().to_pylist()[0]
    assert row == {"l": [1, 2, None], "s": {"a": 1}, "b": b"\xaa"}, row


@check("autocommit off: commit and rollback")
def _():
    tx = connect(autocommit=False)
    cur = tx.cursor()
    cur.execute("INSERT INTO adbc_meta.events VALUES (1, 'kept', NULL)")
    tx.commit()
    cur.execute("INSERT INTO adbc_meta.events VALUES (2, 'discarded', NULL)")
    tx.rollback()
    tx.close()
    cur = conn.cursor()
    cur.execute("SELECT list(id ORDER BY id) AS ids FROM adbc_meta.events")
    assert cur.fetch_arrow_table().to_pylist()[0]["ids"] == [1]


@check("cancellation stops a running statement within seconds")
def _():
    cur = conn.cursor()
    threading.Timer(1.5, cur.adbc_cancel).start()
    started = time.time()
    try:
        cur.execute("SELECT sum(sin(range)) FROM range(100000000000)")
        raise AssertionError("not canceled")
    except dbapi.OperationalError as error:
        assert time.time() - started < 15, time.time() - started
        assert "canceled" in str(error), error


@check("a lost commit race is SQLSTATE 40001")
def _():
    a, b = connect(autocommit=False), connect(autocommit=False)
    a.cursor().execute("DELETE FROM adbc_meta.events")
    b.cursor().execute("DELETE FROM adbc_meta.events")
    a.commit()
    try:
        b.commit()
        raise AssertionError("no conflict")
    except dbapi.Error as error:
        assert error.sqlstate == "40001", (error.sqlstate, error)
    a.close()
    b.close()
    conn.cursor().execute("INSERT INTO adbc_meta.events VALUES (1, 'kept', NULL)")


@check("unsupported parameter types are refused before sending")
def _():
    try:
        conn.cursor().execute("SELECT ?", (b"\x00",))
        raise AssertionError("accepted")
    except dbapi.NotSupportedError as error:
        assert "cast in SQL" in str(error), error


@check("get_info names the driver and vendor")
def _():
    info = conn.adbc_get_info()
    assert info["driver_name"] == "ADBC DuckFleet Driver", info
    assert info["vendor_name"] == "DuckFleet", info
    assert str(info["vendor_version"]).startswith("DuckDB"), info


@check("get_table_types")
def _():
    assert set(conn.adbc_get_table_types()) >= {"BASE TABLE", "VIEW"}


@check("get_objects: catalogs, schemas, tables, columns, and filters")
def _():
    tree = conn.adbc_get_objects(depth="all", catalog_filter="lake", db_schema_filter="adbc_meta").read_all().to_pylist()
    assert [c["catalog_name"] for c in tree] == ["lake"], tree
    schemas = tree[0]["catalog_db_schemas"]
    assert [s["db_schema_name"] for s in schemas] == ["adbc_meta"], schemas
    tables = {t["table_name"]: t for t in schemas[0]["db_schema_tables"]}
    assert set(tables) == {"events", "events_view"}, tables
    columns = [(c["column_name"], c["ordinal_position"], c["xdbc_type_name"]) for c in tables["events"]["table_columns"]]
    assert columns == [("id", 1, "BIGINT"), ("label", 2, "VARCHAR"), ("happened_at", 3, "TIMESTAMP")], columns
    assert tables["events"]["table_columns"][0]["xdbc_is_nullable"] == "NO"
    views = conn.adbc_get_objects(depth="tables", db_schema_filter="adbc_meta", table_types_filter=["VIEW"]).read_all().to_pylist()
    found = [t["table_name"] for c in views for s in c["catalog_db_schemas"] or [] for t in s["db_schema_tables"] or []]
    assert found == ["events_view"], found
    shallow = conn.adbc_get_objects(depth="db_schemas", catalog_filter="lake", db_schema_filter="adbc%").read_all().to_pylist()
    assert shallow[0]["catalog_db_schemas"][0]["db_schema_tables"] is None, shallow
    pattern = conn.adbc_get_objects(depth="columns", db_schema_filter="adbc_meta", table_name_filter="events", column_name_filter="l%").read_all().to_pylist()
    cols = [c["column_name"] for cat in pattern for s in cat["catalog_db_schemas"] for t in s["db_schema_tables"] for c in t["table_columns"]]
    assert cols == ["label"], cols


@check("get_table_schema")
def _():
    schema = conn.adbc_get_table_schema("events", db_schema_filter="adbc_meta")
    assert schema.names == ["id", "label", "happened_at"], schema


@check("compute selection through database options")
def _():
    pooled = connect(**{"adbc.duckfleet.pool": "small", "adbc.duckfleet.mode": "persistent"})
    cur = pooled.cursor()
    cur.execute("SELECT 1 AS one")
    assert cur.fetch_arrow_table().to_pylist() == [{"one": 1}]
    pooled.close()
    try:
        connect(**{"adbc.duckfleet.pool": "no-such-pool"})
        raise AssertionError("unknown pool accepted")
    except dbapi.Error as error:
        assert "pool" in str(error).lower(), error
    try:
        connect(**{"adbc.duckfleet.mode": "sideways"})
        raise AssertionError("invalid mode accepted")
    except dbapi.Error:
        pass


@check("unimplemented features say NOT_IMPLEMENTED")
def _():
    cur = conn.cursor()
    try:
        cur.adbc_execute_partitions("SELECT 1")
        raise AssertionError("partitions accepted")
    except dbapi.NotSupportedError:
        pass


@check("pandas via the DB-API cursor")
def _():
    cur = conn.cursor()
    cur.execute("SELECT i AS id, i * 1.5 AS amount FROM range(100) t(i) WHERE i >= ?", (90,))
    frame = cur.fetch_df()
    assert len(frame) == 10 and list(frame.columns) == ["id", "amount"], frame


setup.execute("DROP SCHEMA adbc_meta CASCADE")
conn.close()
print(f"adbc-driver-manager {adbc_driver_manager.__version__}")
sys.exit(1 if failures else 0)
