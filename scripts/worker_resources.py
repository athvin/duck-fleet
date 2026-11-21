"""Validate declared worker allocations against Kubernetes requests and limits.

The chart and live-pod verifier share this rule. CPU, memory and disk are independent
allocation dimensions; lower CPU requests are allowed, while memory stays guaranteed.
"""
import re


def cores(quantity):
    """Kubernetes CPU quantity in cores."""
    return float(quantity[:-1]) / 1000 if quantity.endswith("m") else float(quantity)


def quantity_bytes(quantity):
    match = re.fullmatch(r"([0-9]+)(Ki|Mi|Gi)?", str(quantity))
    if not match:
        raise AssertionError(f"invalid binary quantity: {quantity!r}")
    return int(match[1]) * {None: 1, "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}[match[2]]


def assert_worker_resources(resources, cpu, where, *, memory=None, disk=None):
    """Check an allocation; omitted memory retains the legacy fixture shorthand."""
    limit = resources["limits"]["cpu"]
    if cores(limit) != cpu:
        raise AssertionError(f"{where}: CPU limit {limit} is not the declared allocation {cpu}")
    expected_memory = quantity_bytes(memory or f"{cpu}Gi")
    if expected_memory < 256 * 1024**2:
        raise AssertionError(f"{where}: memory must be at least 256Mi")
    for kind in ["requests", "limits"]:
        actual = resources[kind]["memory"]
        if quantity_bytes(actual) != expected_memory:
            raise AssertionError(f"{where}: {kind} memory {actual} differs from the declared allocation")
    request = resources["requests"]["cpu"]
    if not 0 < cores(request) <= cpu:
        raise AssertionError(
            f"{where}: CPU request {request} must be above zero and at most the limit of {cpu}")
    if disk is not None:
        expected_disk = quantity_bytes(disk)
        if expected_disk < 1024**3:
            raise AssertionError(f"{where}: disk must be at least 1Gi")
        if quantity_bytes(resources["requests"]["ephemeral-storage"]) != expected_disk:
            raise AssertionError(f"{where}: ephemeral-storage request differs from declared disk")
        if quantity_bytes(resources["limits"]["ephemeral-storage"]) != expected_disk + 1024**3:
            raise AssertionError(f"{where}: ephemeral-storage limit must include 1Gi of pod overhead")
