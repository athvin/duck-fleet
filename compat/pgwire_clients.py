"""psycopg 3 behavior against the pgwire front; run in a container by scripts/test-pgwire-clients.sh.

Each check asserts what a real driver observes, not what the server intends.
"""

import os
import sys
import threading
import time

import psycopg

DSN = os.environ["PGWIRE_DSN"]
failures = []


def check(name):
    def wrap(fn):
        try:
            fn()
            print(f"ok   {name}")
        except Exception as error:  # report every failing capability
            failures.append(name)
            print(f"FAIL {name}: {type(error).__name__}: {error}")
        return fn

    return wrap


def connect(autocommit=False):
    return psycopg.connect(DSN, autocommit=autocommit, cursor_factory=psycopg.ClientCursor)


setup = connect(autocommit=True)
setup.execute("CREATE SCHEMA IF NOT EXISTS pgwire_clients")
setup.execute("CREATE OR REPLACE TABLE pgwire_clients.t AS SELECT 1 AS n")


@check("transaction status is INTRANS inside, INERROR after an error, IDLE after rollback")
def _():
    conn = connect()
    conn.execute("SELECT 1")
    assert conn.info.transaction_status.name == "INTRANS", conn.info.transaction_status
    try:
        conn.execute("SELECT * FROM pgwire_clients.missing")
    except psycopg.Error:
        pass
    assert conn.info.transaction_status.name == "INERROR", conn.info.transaction_status
    try:
        conn.execute("SELECT 1")
        raise AssertionError("statement ran in an aborted transaction")
    except psycopg.errors.InFailedSqlTransaction:
        pass
    conn.rollback()
    assert conn.info.transaction_status.name == "IDLE", conn.info.transaction_status
    conn.close()


@check("commit() commits and rollback() discards")
def _():
    conn = connect()
    conn.execute("INSERT INTO pgwire_clients.t VALUES (2)")
    conn.rollback()
    conn.execute("INSERT INTO pgwire_clients.t VALUES (3)")
    conn.commit()
    values = [row[0] for row in setup.execute("SELECT n FROM pgwire_clients.t ORDER BY n").fetchall()]
    assert values == [1, 3], values
    conn.close()


@check("a lost commit race surfaces as 40001 and leaves the connection idle")
def _():
    a, b = connect(), connect()
    a.execute("DELETE FROM pgwire_clients.t WHERE n = 1")
    b.execute("DELETE FROM pgwire_clients.t WHERE n = 1")
    a.commit()
    try:
        b.commit()
        raise AssertionError("second commit did not conflict")
    except psycopg.errors.SerializationFailure:
        pass
    assert b.info.transaction_status.name == "IDLE", b.info.transaction_status
    assert b.execute("SELECT 1").fetchone() == (1,)
    b.rollback()
    a.close()
    b.close()


@check("unsupported result types are a classified error and the connection stays usable")
def _():
    conn = connect(autocommit=True)
    for sql in ("SELECT [1, 2] AS l", "SELECT {'a': 1} AS s", "SELECT '\\xAA'::BLOB AS b"):
        try:
            conn.execute(sql)
            raise AssertionError(f"{sql} was not refused")
        except psycopg.errors.FeatureNotSupported as error:
            assert "cast it to VARCHAR" in str(error), error
    assert conn.execute("SELECT [1, 2]::VARCHAR, 42").fetchone() == ("[1, 2]", 42)
    conn.close()


LONG = "SELECT sum(sin(range)) FROM range(100000000000)"


@check("a forged CancelRequest cancels nothing; the real key cancels within seconds with 57014")
def _():
    import socket
    import struct

    conn = connect(autocommit=True)
    outcome = {}

    def run():
        started = time.time()
        try:
            conn.execute(LONG)
            outcome["result"] = "completed"
        except psycopg.errors.QueryCanceled as error:
            outcome["result"] = "canceled"
            outcome["message"] = str(error)
        outcome["elapsed"] = time.time() - started

    worker = threading.Thread(target=run)
    worker.start()
    time.sleep(1.5)
    info = conn.info
    with socket.create_connection((info.host, info.port), timeout=5) as raw:
        raw.sendall(struct.pack("!iiii", 16, 80877102, info.backend_pid, 12345))
    time.sleep(2)
    assert worker.is_alive(), f"a forged key stopped the statement: {outcome}"
    conn.cancel_safe()
    worker.join(30)
    assert not worker.is_alive(), "the real CancelRequest did not stop the statement"
    assert outcome["result"] == "canceled", outcome
    assert outcome["elapsed"] < 15, outcome
    assert conn.execute("SELECT 1").fetchone() == (1,)
    conn.close()


setup.execute("DROP SCHEMA pgwire_clients CASCADE")
setup.close()
sys.exit(1 if failures else 0)
