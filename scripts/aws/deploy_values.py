"""Writes the deploy-time values only the live cluster can supply.

Enabling isolated compute needs the API server's real address: a rule written
against the `kubernetes` Service ClusterIP matches nothing once kube-proxy has
rewritten the destination (#32), so the egress policy must name the endpoints the
Service actually resolves to. The deploy identity is scoped to its own namespace
and cannot read those endpoints, which live in `default` — so apply-boundary.sh,
which has the admin access to read them, records them in a ConfigMap here that the
deploy can read. That is where the flip the ownership boundary unblocks (#39)
comes from, merged over the image digests already in values.json: isolated compute
on with those CIDRs and ports, the workers pinned to the DuckFleet node pool with
the toleration its taint needs, and the medium worker at its full two CPUs.

Until the boundary is (re-)applied the ConfigMap is absent, in which case isolated
compute stays off and the deploy renders exactly as it did before — the flip waits
on the operator, it does not fail without them.
"""
import json
import subprocess
import sys
from pathlib import Path


def api_egress(context, namespace):
    """The API-server CIDRs and ports apply-boundary.sh recorded, or None if it has
    not run with this feature yet. Read from the deploy's own namespace, which it is
    permitted to; the endpoints themselves live in `default`, which it is not."""
    result = subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, "get", "configmap",
         "duckfleet-api-egress", "-o", "json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return None
    data = json.loads(result.stdout).get("data", {})
    cidrs = json.loads(data.get("cidrs", "[]"))
    ports = json.loads(data.get("ports", "[]"))
    return (cidrs, ports) if cidrs and ports else None


def overlay(cidrs, ports):
    """The deploy-scoped values, from the API-server address the boundary recorded."""
    return {
        "isolatedCompute": {
            "enabled": True,
            "apiServerCidrs": sorted(cidrs),
            "apiServerPorts": sorted(ports),
            # The first isolated pod needs a node the pool does not have yet — the
            # persistent workers fill the one that exists — so Karpenter provisions a
            # fresh one: EC2 launch, node join, image pull, worker startup. The 120s
            # chart default (kind, where scheduling is instant) is far too short for
            # that cold path on EKS; this is the wait before the gateway gives up, so
            # a generous ceiling only matters when provisioning is genuinely slow.
            "readySeconds": 420,
            # The pod is Unschedulable until Karpenter brings that node up, and the
            # gateway must not read that transient state as a verdict and delete the
            # pod before provisioning completes — which is exactly why no isolated
            # session placed on EKS (#90). Zero on the fixed-node chart default (kind),
            # raised here to span a cold node launch so an on-demand node is waited out
            # rather than refused.
            "provisioningSeconds": 240,
        },
        # The pool pin, and the chart's compute-type selector explicitly removed
        # (null deletes a default key under Helm's merge, which a bare omission would
        # not). On EKS Auto Mode the `eks.amazonaws.com/compute-type: auto` label is
        # stamped onto a node at bootstrap, so Karpenter's scheduling simulation for a
        # *new* node in a custom pool does not carry it — a pod requiring both that
        # label and the pool is one Karpenter decides no new node could satisfy, and
        # it declines to provision ("no new claims"). karpenter.sh/nodepool identifies
        # the pool uniquely on its own; the taint needs the toleration beside it,
        # which check-chart.py asserts.
        "nodeSelector": {
            "kubernetes.io/os": "linux",
            "eks.amazonaws.com/compute-type": None,
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
    namespace = sys.argv[2] if len(sys.argv) > 2 else "duckfleet-dev"
    path = Path("artifacts/aws/values.json")
    values = json.loads(path.read_text())
    egress = api_egress(context, namespace)
    if egress is None:
        print("no duckfleet-api-egress ConfigMap; isolated compute stays off "
              "(run scripts/aws/apply-boundary.sh to record the API-server address)")
        return
    cidrs, ports = egress
    values.update(overlay(cidrs, ports))
    path.write_text(json.dumps(values, indent=2) + "\n")
    print(f"deploy values: isolated compute on, API egress to {sorted(cidrs)} "
          f"ports {sorted(ports)}, workers pinned to the duckfleet pool at a full 2-CPU request")


if __name__ == "__main__":
    main()
