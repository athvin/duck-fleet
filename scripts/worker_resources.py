"""The one rule for what a worker container's resources may be.

Shared by the chart check, which reads the rendered release, and the deploy
verification, which reads live pods. They previously each carried their own copy of
this rule, and when #22 relaxed `requests == limits` so a class could be scheduled on
capped capacity, only the chart check was updated. The live-pod copy kept failing,
invisibly, because no deploy had ever reached it.
"""


def cores(quantity):
    """Kubernetes CPU quantity in cores; 'm' is the only suffix Kubernetes allows."""
    return float(quantity[:-1]) / 1000 if quantity.endswith("m") else float(quantity)


def assert_worker_resources(resources, cpu, where):
    """Checks one worker container's resources against its class size.

    The CPU limit must be exact: the worker derives its DuckDB thread count and
    memory ceiling from the cgroup, and the cgroup CPU quota comes from the limit.
    The CPU request may sit lower so the pod fits capacity its limit could not, but
    never above the limit, which would place a worker on a node that cannot honour
    the limit its own assertion depends on. Memory is not compressible, so request
    and limit stay equal there.
    """
    limit = resources["limits"]["cpu"]
    if limit != str(cpu):
        raise AssertionError(f"{where}: CPU limit {limit} is not the class size {cpu}")
    for kind in ["requests", "limits"]:
        memory = resources[kind]["memory"]
        if memory != f"{cpu}Gi":
            raise AssertionError(f"{where}: {kind} memory {memory} is not {cpu}Gi")
    request = resources["requests"]["cpu"]
    if not 0 < cores(request) <= cpu:
        raise AssertionError(
            f"{where}: CPU request {request} must be above zero and at most the "
            f"limit of {cpu}"
        )
