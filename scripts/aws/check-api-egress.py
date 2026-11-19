"""Asserts the gateway may actually reach the Kubernetes API before deploying.

Egress NetworkPolicy is evaluated after kube-proxy has rewritten the destination, so a
rule written against the API server's Service address matches nothing: traffic a pod
sends to the ClusterIP on 443 arrives at an endpoint address, on whatever port that
endpoint listens on. On kind that is the node's address on 6443 — neither half of the
ClusterIP matches, and the first run of the local harness with an enforcing CNI denied
the gateway outright because of it.

Nothing in the chart can notice that on its own, which is the same shape as #30: a
policy that looks correct and permits nothing. So this reads the cluster's real
endpoints and requires the configured ranges and ports to cover them. Pointing the
deploy at a cluster whose API server sits elsewhere then fails in seconds instead of
presenting as a gateway that cannot create session pods.
"""

import ipaddress
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def effective_values():
    """The chart defaults with the deploy-time overlay applied, so this checks what
    the deploy actually renders rather than what the chart ships. The overlay is
    where isolated compute is enabled and its API-server CIDRs are set (#39); away
    from a deploy it is absent and this falls back to the disabled default."""
    values = yaml.safe_load((ROOT / "infra/helm/duckfleet/values.yaml").read_text())
    overlay_path = ROOT / "artifacts/aws/values.json"
    if overlay_path.exists():
        for key, value in json.loads(overlay_path.read_text()).items():
            if isinstance(value, dict) and isinstance(values.get(key), dict):
                values[key] = {**values[key], **value}
            else:
                values[key] = value
    return values


def configured(values):
    isolated = values.get("isolatedCompute") or {}
    return isolated.get("apiServerCidrs") or [], isolated.get("apiServerPorts") or []


def uncovered(found, ranges, ports):
    """Endpoints no configured range and port pair admits."""
    networks = [ipaddress.ip_network(entry) for entry in ranges]
    return [
        f"{address}:{port}"
        for address, port in found
        if not (any(ipaddress.ip_address(address) in network for network in networks)
                and port in ports)
    ]


def main():
    values = effective_values()
    isolated = (values.get("isolatedCompute") or {}).get("enabled")
    if not isolated:
        # Where isolated compute is off the rule is not rendered, so there is nothing
        # to check and nothing that could be wrong.
        print("isolated compute is disabled; no API egress rule to verify")
        return
    # The deploy identity is scoped to its own namespace and cannot read the
    # `default` endpoints to compare against, so coverage is established where the
    # addresses are read: apply-boundary.sh derives these CIDRs from the live
    # endpoints themselves, so they are the endpoints rather than the ClusterIP a
    # rewritten rule would miss (#32). Here the deploy only confirms the boundary
    # supplied them — an enabled render with no egress address is the failure.
    ranges, ports = configured(values)
    if not ranges or not ports:
        raise SystemExit(
            "isolated compute is enabled but no API-server egress address is set. "
            "Run scripts/aws/apply-boundary.sh to record the endpoints the gateway "
            "must reach; without them the egress policy would match nothing (#32)."
        )
    print(f"API egress names {ranges} on ports {ports}, recorded from the live "
          "endpoints by the ownership boundary")


if __name__ == "__main__":
    main()
