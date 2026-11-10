from dbt.adapters.base.meta import available
from dbt.adapters.duckdb.impl import DuckDBAdapter
from dbt.adapters.duckfleet.connections import DuckFleetConnectionManager
from dbt_common.exceptions import DbtRuntimeError


class DuckFleetAdapter(DuckDBAdapter):
    ConnectionManager = DuckFleetConnectionManager

    @classmethod
    def is_cancelable(cls):
        return True

    @available
    def get_binding_char(self):
        return "?"

    def submit_python_job(self, parsed_model, compiled_code):
        raise DbtRuntimeError("DuckFleet's local harness supports SQL models only")

    @available
    def store_relation(self, *args, **kwargs):
        raise DbtRuntimeError("external materializations and plugins are not supported")
