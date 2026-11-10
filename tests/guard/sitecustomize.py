"""Loaded only in dbt test subprocesses: a local SQL fallback must fail the gate."""
import duckdb


def forbidden(*args, **kwargs):
    raise RuntimeError("HARNESS: dbt must execute SQL through the DuckFleet server")


duckdb.connect = forbidden
