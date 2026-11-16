"""Asserts the release may actually reach the cluster's DNS before deploying it.

Six consecutive deploys failed because the NetworkPolicy permitted DNS only to pods
labelled `k8s-app: kube-dns` in `kube-system`, and EKS Auto Mode has no such pods: it
runs DNS as a managed component. The selector matched nothing, resolution was denied,
and every worker reported an unreachable catalog instead of an unreachable resolver.

Nothing in the chart can notice that on its own, so the check belongs here: resolve
the cluster's own service CIDR, derive the address pods will resolve against, and
require the configured ranges to cover it. Narrowing the ranges, or pointing the
deploy at a differently addressed cluster, then fails in seconds.
"""

import ipaddress
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def service_cidr(cluster, region):
    described = subprocess.run(
        ["aws", "eks", "describe-cluster", "--name", cluster, "--region", region,
         "--query", "cluster.kubernetesNetworkConfig.serviceIpv4Cidr", "--output", "text"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if not described or described == "None":
        raise SystemExit(f"could not read the service CIDR for cluster {cluster}")
    return ipaddress.ip_network(described)


def resolver_address(service_network):
    """Cluster DNS is conventionally the tenth address of the service CIDR."""
    return service_network.network_address + 10


def configured_ranges(values_file):
    values = yaml.safe_load(values_file.read_text())
    ranges = values.get("dnsEgressCidrs")
    if not ranges:
        raise SystemExit(f"{values_file} sets no dnsEgressCidrs; DNS would be denied")
    return [ipaddress.ip_network(entry) for entry in ranges]


def covering_ranges(resolver, ranges):
    """The configured ranges that actually permit reaching `resolver`."""
    return [str(entry) for entry in ranges if resolver in entry]


def main():
    cluster, region = sys.argv[1], sys.argv[2]
    values_file = ROOT / "infra/helm/duckfleet/values.yaml"
    service_network = service_cidr(cluster, region)
    resolver = resolver_address(service_network)
    ranges = configured_ranges(values_file)

    covering = covering_ranges(resolver, ranges)
    report = {
        "cluster": cluster,
        "service_cidr": str(service_network),
        "expected_resolver": str(resolver),
        "configured": [str(entry) for entry in ranges],
        "covering": covering,
    }
    print(json.dumps(report))
    if not covering:
        raise SystemExit(
            f"dnsEgressCidrs does not cover {resolver}, the resolver pods in "
            f"{cluster} will use. Workers would be unable to resolve anything, which "
            "presents as an unreachable catalog rather than as a DNS failure."
        )
    for entry in ranges:
        if not entry.is_private:
            raise SystemExit(f"dnsEgressCidrs must stay private, got {entry}")
    print(f"PASS: cluster DNS at {resolver} is permitted by {', '.join(covering)}")


if __name__ == "__main__":
    main()
