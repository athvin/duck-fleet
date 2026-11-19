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
import subprocess
import sys
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


def endpoints(context, namespace="default"):
    """The addresses and ports the `kubernetes` Service actually resolves to."""
    described = subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, "get", "endpoints",
         "kubernetes", "-o", "json"],
        check=True, capture_output=True, text=True,
    ).stdout
    found = []
    for subset in json.loads(described).get("subsets", []):
        for address in subset.get("addresses", []):
            for port in subset.get("ports", []):
                found.append((address["ip"], port["port"]))
    if not found:
        raise SystemExit("the kubernetes Service resolves to no endpoints")
    return found


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
    context = sys.argv[1]
    values = effective_values()
    isolated = (values.get("isolatedCompute") or {}).get("enabled")
    if not isolated:
        # Where isolated compute is off the rule is not rendered, so there is nothing
        # to check and nothing that could be wrong.
        print("isolated compute is disabled; no API egress rule to verify")
        return
    ranges, ports = configured(values)
    found = endpoints(context)
    missing = uncovered(found, ranges, ports)
    if missing:
        raise SystemExit(
            f"the gateway could not reach the Kubernetes API at {missing}: "
            f"isolatedCompute.apiServerCidrs={ranges} apiServerPorts={ports}. "
            "These must cover the Service's endpoints, not its ClusterIP — egress "
            "policy is evaluated after the destination is rewritten."
        )
    print(f"API egress covers {['%s:%s' % pair for pair in found]}")


if __name__ == "__main__":
    main()
