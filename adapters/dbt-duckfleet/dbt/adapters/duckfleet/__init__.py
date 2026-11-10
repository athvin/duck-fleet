from dbt.adapters.base import AdapterPlugin
from dbt.adapters.duckfleet.connections import DuckFleetCredentials
from dbt.adapters.duckfleet.impl import DuckFleetAdapter
from dbt.include import duckfleet

Plugin = AdapterPlugin(
    adapter=DuckFleetAdapter,
    credentials=DuckFleetCredentials,
    include_path=duckfleet.PACKAGE_PATH,
    dependencies=["duckdb"],
)
