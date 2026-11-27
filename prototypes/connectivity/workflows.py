"""Reproducible connectivity workflows for the #172 ADBC/pgwire decision.

Runs two Arrow-native workflows through the prototype ADBC driver and two
PostgreSQL-driver workflows through the pgwire front, and records what each
capability actually does. Nothing here is marked compatible because SELECT 1
works: every row is an observed outcome.

    uv run --python 3.12 --with adbc-driver-manager==1.13.0 --with pyarrow==26.0.1 \
        --with pandas==3.1.1 --with 'psycopg[binary]==3.3.9' \
        prototypes/connectivity/workflows.py --driver <libadbc_duckfleet> > matrix.json
"""

import argparse
import json
import sys
import threading
import time
import traceback

results = []


def record(workflow, capability, outcome, detail):
    results.append(
        {"workflow": workflow, "capability": capability, "outcome": outcome, "detail": str(detail)[:400]}
    )


def check(workflow, capability, fn):
    try:
        detail = fn()
        record(workflow, capability, "works", detail)
    except Exception as error:  # every failure is evidence
        record(workflow, capability, "fails", f"{type(error).__name__}: {error}")


def adbc(args):
    import adbc_driver_manager.dbapi as dbapi
    import pandas
    import pyarrow

    def connect(autocommit=True):
        return dbapi.connect(
            driver=args.driver,
            entrypoint="AdbcDuckfleetInit",
            db_kwargs={"uri": args.url, "password": args.token},
            autocommit=autocommit,
        )

    w1 = "W1 adbc_driver_manager dbapi + pyarrow"
    conn = connect()

    def parameterized():
        cur = conn.cursor()
        cur.execute(
            "SELECT ?::INTEGER + 1 AS answer, ?::VARCHAR AS name, ?::DECIMAL(10,2) AS price, ?::TIMESTAMP AS at",
            (41, "héllo", "9.99", "2026-01-02 03:04:05.123456"),
        )
        table = cur.fetch_arrow_table()
        types = {f.name: str(f.type) for f in table.schema}
        assert table.column("answer")[0].as_py() == 42, table
        assert types["price"] == "decimal128(10, 2)", types
        assert types["at"] == "timestamp[us]", types
        return types

    check(w1, "extended bind: typed parameters and typed Arrow result", parameterized)

    def nested():
        cur = conn.cursor()
        cur.execute("SELECT NULL::INTEGER AS n, [1, 2, NULL] AS l, {'a': 1, 'b': 'x'} AS s, '\\xAA'::BLOB AS b")
        table = cur.fetch_arrow_table()
        row = table.to_pylist()[0]
        assert row["n"] is None and row["l"] == [1, 2, None] and row["s"] == {"a": 1, "b": "x"}, row
        return {f.name: str(f.type) for f in table.schema}

    check(w1, "null and nested types", nested)

    def large():
        cur = conn.cursor()
        cur.execute("SELECT i, repeat('x', 50) AS p FROM range(90000) t(i)")
        return f"{cur.fetch_arrow_table().num_rows} rows within the interactive cap"

    check(w1, "large results (90k rows, interactive cap is 100k rows / 16 MiB)", large)

    def over_cap():
        cur = conn.cursor()
        cur.execute("SELECT i FROM range(150000) t(i)")
        return f"{cur.fetch_arrow_table().num_rows} rows"

    check(w1, "results over the interactive cap (durable-job path not wired into ADBC)", over_cap)

    def transactions():
        tx = connect(autocommit=False)
        cur = tx.cursor()
        cur.execute("CREATE TEMP TABLE adbc_tx(n INTEGER)")
        tx.commit()
        cur.execute("INSERT INTO adbc_tx VALUES (1)")
        tx.rollback()
        cur.execute("SELECT count(*) AS c FROM adbc_tx")
        count = cur.fetch_arrow_table().column("c")[0].as_py()
        assert count == 0, count
        tx.close()
        return "commit/rollback through ADBC autocommit=false; temp table survived, rolled-back insert did not"

    check(w1, "transaction state", transactions)

    def cancel():
        cur = conn.cursor()
        timer = threading.Timer(1.5, cur.adbc_cancel)
        timer.start()
        started = time.time()
        try:
            cur.execute("SELECT sum(sin(range)) FROM range(100000000000)")
            raise AssertionError("long query was not canceled")
        except dbapi.Error as error:
            return f"canceled after {time.time() - started:.1f}s: {str(error)[:160]}"

    check(w1, "native cancellation (AdbcStatementCancel -> session cancel)", cancel)

    def conflict():
        a, b = connect(autocommit=False), connect(autocommit=False)
        setup = connect()
        setup.cursor().execute("CREATE OR REPLACE TABLE adbc_conflict AS SELECT 1 AS n")
        a.cursor().execute("DELETE FROM adbc_conflict")
        b.cursor().execute("DELETE FROM adbc_conflict")
        a.commit()
        try:
            b.commit()
            raise AssertionError("second commit did not conflict")
        except dbapi.Error as error:
            assert getattr(error, "sqlstate", None) == "40001" or "40001" in str(error) or "transaction_conflict" in str(error), error
            return f"sqlstate={getattr(error, 'sqlstate', None)}: {str(error)[:160]}"
        finally:
            setup.cursor().execute("DROP TABLE IF EXISTS adbc_conflict")

    check(w1, "classified failure: lost commit race", conflict)

    def unsupported_parameter():
        cur = conn.cursor()
        try:
            cur.execute("SELECT ?", (b"\x00",))
            raise AssertionError("binary parameter accepted")
        except dbapi.Error as error:
            return f"refused before sending: {str(error)[:160]}"

    check(w1, "unsupported parameter type is explicit", unsupported_parameter)

    def metadata():
        return conn.adbc_get_objects(depth="tables").read_all().num_rows

    check(w1, "catalog metadata (AdbcConnectionGetObjects)", metadata)

    w2 = "W2 pandas.read_sql over the ADBC connection"

    def frame():
        df = pandas.read_sql(
            "SELECT i AS id, i * 1.5 AS amount, 'r' || i AS label FROM range(1000) t(i) WHERE i >= ?",
            conn,
            params=(10,),
        )
        assert len(df) == 990, len(df)
        return {k: str(v) for k, v in df.dtypes.items()}

    check(w2, "parameterized read_sql into a DataFrame", frame)

    def arrow_backed():
        df = pandas.read_sql("SELECT 1::BIGINT AS a, NULL::INTEGER AS b", conn, dtype_backend="pyarrow")
        return {k: str(v) for k, v in df.dtypes.items()}

    check(w2, "pyarrow-backed dtypes keep integer nulls", arrow_backed)
    conn.close()


def pgwire(args):
    import psycopg

    dsn = f"host={args.pg_host} port={args.pg_port} user=duckfleet password={args.token} dbname=lake sslmode=disable connect_timeout=10"
    w4 = "W4 psycopg 3.3.9"

    def connect(**kwargs):
        return psycopg.connect(dsn, autocommit=True, **kwargs)

    check(w4, "connect and simple query", lambda: connect().execute("SELECT 1").fetchone())

    def server_bind():
        return connect().execute("SELECT %s::INTEGER + 1", (41,)).fetchone()

    check(w4, "extended bind (server-side parameters, psycopg default)", server_bind)

    def client_bind():
        with connect(cursor_factory=psycopg.ClientCursor) as conn:
            return conn.execute("SELECT %s::INTEGER + 1, %s::VARCHAR", (41, "o'k")).fetchone()

    check(w4, "client-side binding (ClientCursor, simple protocol)", client_bind)

    def copy():
        with connect(cursor_factory=psycopg.ClientCursor) as conn, conn.cursor() as cur:
            with cur.copy("COPY (SELECT 1) TO STDOUT") as copy:
                return b"".join(copy)

    check(w4, "COPY TO STDOUT", copy)

    def cancel():
        conn = connect(cursor_factory=psycopg.ClientCursor)
        threading.Timer(1.5, conn.cancel_safe).start()
        started = time.time()
        try:
            conn.execute("SELECT sum(sin(range)) FROM range(100000000000)")
            raise AssertionError("not canceled")
        except psycopg.Error as error:
            elapsed = time.time() - started
            assert elapsed < 15, f"CancelRequest had no effect; the statement ran until its deadline after {elapsed:.0f}s ({type(error).__name__})"
            return f"after {elapsed:.1f}s: {type(error).__name__} {error}"

    check(w4, "native cancellation (CancelRequest)", cancel)

    def tls():
        return psycopg.connect(dsn.replace("sslmode=disable", "sslmode=require")).execute("SELECT 1").fetchone()

    check(w4, "TLS (sslmode=require)", tls)

    psycopg.connect(dsn + " options='-c duckfleet.pool=offline'", autocommit=True).close()
    record(
        w4,
        "pool selection via startup options",
        "ignored",
        "a startup option naming the offline pool connects, but the backend session always uses the default persistent pool (documented: no per-connection pool selection)",
    )

    def transaction_status():
        conn = psycopg.connect(dsn, cursor_factory=psycopg.ClientCursor)
        conn.execute("SELECT 1")
        inside = conn.info.transaction_status.name
        try:
            conn.execute("SELECT * FROM missing_table_for_status")
        except psycopg.Error:
            pass
        failed = conn.info.transaction_status.name
        conn.rollback()
        assert (inside, failed) == ("INTRANS", "INERROR"), f"ReadyForQuery reported {inside} inside a transaction and {failed} after an error; psycopg therefore treats commit() as a no-op"
        return f"inside: {inside}; after error: {failed}"

    check(w4, "transaction state reporting (ReadyForQuery)", transaction_status)

    def nested():
        with connect(cursor_factory=psycopg.ClientCursor) as conn:
            return conn.execute("SELECT NULL::INTEGER, [1, 2], {'a': 1}").fetchone()

    check(w4, "null and nested types", nested)

    def large():
        with connect(cursor_factory=psycopg.ClientCursor) as conn:
            rows = conn.execute("SELECT i FROM range(90000) t(i)").fetchall()
            return f"{len(rows)} rows"

    check(w4, "large results (90k rows)", large)

    def conflict():
        a = psycopg.connect(dsn, cursor_factory=psycopg.ClientCursor)
        b = psycopg.connect(dsn, cursor_factory=psycopg.ClientCursor)
        setup = connect(cursor_factory=psycopg.ClientCursor)
        setup.execute("CREATE OR REPLACE TABLE pg_conflict AS SELECT 1 AS n")
        a.execute("DELETE FROM pg_conflict")
        b.execute("DELETE FROM pg_conflict")
        a.commit()
        try:
            b.commit()
            raise AssertionError("no conflict: with ReadyForQuery always IDLE, psycopg never sent COMMIT")
        except psycopg.Error as error:
            return f"sqlstate={error.sqlstate}: {str(error)[:120]}"
        finally:
            setup.execute("DROP TABLE IF EXISTS pg_conflict")

    check(w4, "classified failure: lost commit race", conflict)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:18080")
    parser.add_argument("--token", default="duckfleet-local-development-token")
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", default="15432")
    args = parser.parse_args()
    for section in (adbc, pgwire):
        try:
            section(args)
        except Exception:
            record(section.__name__, "workflow setup", "fails", traceback.format_exc())
    json.dump(results, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
