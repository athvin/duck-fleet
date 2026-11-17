"""Rewrites the rendered AWS chart for a local cluster, and only where it must.

The chart is deliberately AWS-shaped: Pod Identity for S3, immutable ECR digests, the
`gp3` storage class, EKS Auto Mode node labels. None of that exists in kind. Rather
than branch the chart or hand-maintain a second copy of the pod spec, this patches
the real rendered output, so the manifests under test still come from the chart that
actually deploys and a chart change that breaks local is caught here.

Every rewrite asserts it matched something. A chart edit that moves one of these out
from under the transform fails loudly instead of quietly leaving the local run
testing a different shape than it claims to.
"""

import argparse
import json
import sys

import yaml

OBJECT_STORE = "object-store:9000"
S3_ACCESS_KEY = "DUCKFLEETLOCAL"
S3_SECRET_KEY = "local-object-store-password"
# Overridable so a provisioned team namespace keeps its own prefix when
# localized; the default matches the primary namespace.
DATA_PATH = "s3://duckfleet/lake/"
# A GitHub runner has 4 vCPU and the release requests more than that in total. The
# cgroup quota a worker asserts comes from its limit, not its request, so lowering
# requests fits the release onto one node while every resource assertion still holds.
LOCAL_CPU_REQUEST = "50m"


class Localizer:
    def __init__(self):
        self.applied = {}

    def mark(self, name):
        self.applied[name] = self.applied.get(name, 0) + 1

    def require(self, names):
        """Fails unless every rewrite the caller expected actually matched.

        The caller states what it believes the render contains, because a partial
        render legitimately has fewer: `--show-only harness.yaml` holds no worker,
        StatefulSet or NetworkPolicy. Checking both directions means a chart change
        that moves one of these out from under the transform fails loudly, and so
        does a driver that asks for a rewrite this render cannot supply.
        """
        missing = [name for name in names if not self.applied.get(name)]
        if missing:
            raise SystemExit(
                f"localize.py matched nothing for {sorted(missing)}; either the chart "
                "changed shape or this render does not contain what was expected. "
                "Update the transform rather than letting it silently skip."
            )
        unexpected = sorted(set(self.applied) - set(names))
        if unexpected:
            raise SystemExit(
                f"localize.py rewrote {unexpected}, which this render was not "
                "expected to contain; confirm the change and update --expect."
            )

    def container(self, spec):
        image = spec.get("image", "")
        if ".dkr.ecr." in image:
            component = "dbt" if "/dbt@" in image or "/dbt:" in image else "worker"
            spec["image"] = f"duckfleet-{component}:local"
            spec["imagePullPolicy"] = "Never"
            self.mark("image")
        requests = spec.get("resources", {}).get("requests", {})
        if "cpu" in requests:
            requests["cpu"] = LOCAL_CPU_REQUEST
            self.mark("cpu-request")
        env = spec.get("env")
        if not env:
            return
        by_name = {entry["name"]: entry for entry in env if "name" in entry}
        if by_name.get("S3_AUTH_MODE", {}).get("value") == "pod_identity":
            by_name["S3_AUTH_MODE"]["value"] = "local"
            env.extend(
                [
                    {"name": "S3_ACCESS_KEY", "value": S3_ACCESS_KEY},
                    {"name": "S3_SECRET_KEY", "value": S3_SECRET_KEY},
                    {"name": "S3_ENDPOINT", "value": OBJECT_STORE},
                ]
            )
            self.mark("s3-auth")
        for name in ["S3_DATA_PATH", "EXPECT_S3_PREFIX"]:
            if name in by_name and by_name[name].get("value", "").startswith("s3://"):
                by_name[name]["value"] = DATA_PATH
                self.mark("s3-path")

    def pod(self, spec):
        selector = spec.get("nodeSelector")
        if selector and "eks.amazonaws.com/compute-type" in selector:
            # kind nodes carry no EKS labels; keep the OS constraint, drop the rest.
            spec["nodeSelector"] = {"kubernetes.io/os": "linux"}
            self.mark("node-selector")
        for spec_key in ["containers", "initContainers"]:
            for container in spec.get(spec_key, []):
                self.container(container)

    def document(self, doc):
        kind = doc.get("kind")
        if kind in ["Deployment", "StatefulSet", "Job"]:
            self.pod(doc["spec"]["template"]["spec"])
        if kind == "StatefulSet":
            for claim in doc["spec"].get("volumeClaimTemplates", []):
                claim["spec"]["storageClassName"] = "standard"
                # kind's local-path provisioner ignores size, but a 10Gi ask is
                # noise in a throwaway cluster.
                claim["spec"]["resources"]["requests"]["storage"] = "2Gi"
                self.mark("storage-class")
        if kind == "ConfigMap" and "pod.json" in doc.get("data", {}):
            # The isolated pod template is a JSON string inside a ConfigMap, so none
            # of the rewrites above reach it. Left alone, the gateway would create a
            # pod that tries to pull from ECR and never becomes ready — and the
            # failure would look like a scheduling problem rather than a packaging one.
            template = json.loads(doc["data"]["pod.json"])
            self.pod(template["spec"])
            doc["data"]["pod.json"] = json.dumps(template)
            self.mark("isolated-pod")
        if kind == "NetworkPolicy":
            # The release reaches S3 over the internet on AWS. Locally the object
            # store is another pod in this namespace, so same-namespace traffic needs
            # its port opened. Peers that also carry a namespaceSelector are left
            # alone: that is the kube-dns rule, which has no business on this port.
            for section in ["ingress", "egress"]:
                for rule in doc["spec"].get(section, []):
                    peers = rule.get("to") or rule.get("from") or []
                    same_namespace = any(
                        "podSelector" in peer and "namespaceSelector" not in peer
                        for peer in peers
                    )
                    if same_namespace:
                        ports = rule.setdefault("ports", [])
                        if {"port": 9000} not in ports:
                            ports.append({"port": 9000})
                            self.mark("object-store-port")
        return doc


def main():
    global DATA_PATH
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expect",
        required=True,
        help="comma-separated rewrites this render must contain, no more and no less",
    )
    parser.add_argument(
        "--data-path",
        default=DATA_PATH,
        help="the local object-store path S3 paths are rewritten to",
    )
    arguments = parser.parse_args()
    expected = [name for name in arguments.expect.split(",") if name]
    DATA_PATH = arguments.data_path

    localizer = Localizer()
    documents = [
        localizer.document(doc)
        for doc in yaml.safe_load_all(sys.stdin.read())
        if doc is not None
    ]
    localizer.require(expected)
    yaml.safe_dump_all(documents, sys.stdout, default_flow_style=False)
    counts = ", ".join(f"{name}={count}" for name, count in sorted(localizer.applied.items()))
    print(f"localized for kind: {counts}", file=sys.stderr)


if __name__ == "__main__":
    main()
