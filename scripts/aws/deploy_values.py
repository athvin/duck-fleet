"""Writes the deploy-time values only the live cluster can supply.

Enabling isolated compute needs the API server's real address: a rule written
against the `kubernetes` Service ClusterIP matches nothing once kube-proxy has
rewritten the destination (#32), so the egress policy must name the endpoints the
Service actually resolves to — a fact about the cluster, unknown when the chart is
authored. It is computed here and merged over the image digests already in
values.json, so the flip the ownership boundary unblocks (#39) lives in the deploy
rather than in chart defaults every other render would then have to carry: isolated
compute on, the API-server CIDRs and ports its egress needs, the workers pinned to
the DuckFleet node pool with the toleration its taint requires, and the medium
worker requesting its full two CPUs now that it sits on a pool sized for it rather
than squeezing onto a shared one.
"""
import json
import subprocess
import sys
from pathlib import Path


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


def overlay(found):
    """The deploy-scoped values, from the endpoints the API server resolves to."""
    return {
        "isolatedCompute": {
            "enabled": True,
            "apiServerCidrs": sorted({f"{ip}/32" for ip, _ in found}),
            "apiServerPorts": sorted({port for _, port in found}),
        },
        # Merged over the chart's nodeSelector, adding the pool pin; the taint it
        # carries needs the toleration beside it, which check-chart.py asserts.
        "nodeSelector": {
            "kubernetes.io/os": "linux",
            "eks.amazonaws.com/compute-type": "auto",
            "karpenter.sh/nodepool": "duckfleet",
        },
        "tolerations": [{
            "key": "duckfleet.io/dedicated",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }],
        # The shared-pool shim requested one CPU so a 2-CPU pod would fit a 2-vCPU
        # node; on the dedicated pool the medium worker requests the two it is sized
        # for, matching its limit.
        "workerCpuRequests": {"medium": 2},
    }


def main():
    context = sys.argv[1]
    path = Path("artifacts/aws/values.json")
    values = json.loads(path.read_text())
    found = endpoints(context)
    values.update(overlay(found))
    path.write_text(json.dumps(values, indent=2) + "\n")
    reached = sorted({f"{ip}:{port}" for ip, port in found})
    print(f"deploy values: isolated compute on, API egress to {reached}, "
          "workers pinned to the duckfleet pool at a full 2-CPU request")


if __name__ == "__main__":
    main()
