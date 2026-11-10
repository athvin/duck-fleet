from contextlib import contextmanager
from copy import deepcopy
import uuid
from dbt_common.invocation import get_invocation_id
from dbt.adapters.base.meta import available
from dbt.adapters.duckdb.impl import DuckDBAdapter
from dbt.adapters.duckfleet.connections import DuckFleetConnectionManager
from dbt_common.exceptions import DbtRuntimeError


class DuckFleetAdapter(DuckDBAdapter):
    ConnectionManager = DuckFleetConnectionManager

    @contextmanager
    def connection_named(self, name, query_header_context=None, should_release_connection=True):
        # dbt passes its resolved node before compile/materialization starts. Policy
        # does not depend on optional SQL comments or the first statement's text.
        node = query_header_context
        meta = getattr(getattr(node, "config", None), "meta", {}) or {}
        platform = deepcopy(meta.get("platform", {}))
        if not isinstance(platform, dict) or set(platform) - {"compute", "cpu", "memory", "tags"}:
            raise DbtRuntimeError("meta.platform supports only compute, cpu, memory, and tags")
        compute = {key: value for key, value in platform.items() if key != "tags"}
        invocation_id = get_invocation_id()
        node_id = getattr(node, "unique_id", None)
        context = {"dbt": {"invocation_id": invocation_id, "node_id": node_id, "platform": platform}}
        if node_id:
            context["model_run_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, invocation_id + ":" + node_id))
        previous = getattr(self.connections._SESSION, "request", {})
        self.connections._SESSION.request = {"compute": compute, "context": context}
        try:
            with super().connection_named(name, query_header_context, should_release_connection):
                yield
        finally:
            self.connections._SESSION.request = previous

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
