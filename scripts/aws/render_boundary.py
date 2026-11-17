"""Renders the DuckFleet ownership boundary for one namespace.

The boundary is what an operator with cluster admin applies once: the namespace, the
pod grant isolated compute needs, the quota that bounds it, and the node pool DuckFleet's
capacity comes from. The Helm release contains none of it on purpose — the deploy
identity can create neither Roles, ResourceQuotas nor cluster-scoped objects, and
`scripts/aws/check-access.sh` fails the deploy if it ever can.

The namespace is substituted rather than committed because `scripts/aws/target.env` is
the one place the deploy target is named. Every object is then checked to belong to that
namespace, or — where it is cluster-scoped and cannot — to be named for DuckFleet, so
applying these cannot depend on a `-n` flag, cannot bind one deployment's grant to
another deployment's gateway, and cannot create arbitrary cluster objects.
"""
import pathlib
import string
import sys

import yaml

# Applied in filename order, which is why the files are numbered: the namespace has to
# exist before anything can be created inside it, and `kubectl apply` does not reorder.
BOUNDARY = pathlib.Path(__file__).resolve().parents[2] / "infra/kubernetes/boundary"
# Objects that need a cluster this one is running on. Kept apart from the portable ones
# because the local Kubernetes harness has no Karpenter: applying a NodePool there fails
# on an unknown kind rather than being ignored.
EKS = BOUNDARY / "eks"
# Cluster-scoped kinds carry no namespace, so the namespace check cannot apply to them.
CLUSTER_SCOPED = {"Namespace", "NodePool"}
# What a cluster-scoped boundary object must be called. A boundary able to create
# arbitrarily named cluster objects is not bounding anything.
OURS = "duckfleet"


def render(text, namespace, node_class=""):
    """`text` with the target substituted, raising if a placeholder is unresolved."""
    return string.Template(text).substitute(namespace=namespace, node_class=node_class)


def placed(documents, namespace):
    """The documents, having checked each one belongs where this boundary may reach.

    An object with no namespace is not an error to Kubernetes — it lands wherever the
    caller's context points — so this is the check that a boundary applied with the
    wrong context fails instead of granting a gateway somewhere unintended.
    """
    for document in documents:
        kind, metadata = document["kind"], document["metadata"]
        name = metadata.get("name")
        if kind == "Namespace":
            if name != namespace:
                raise ValueError(f"the Namespace is {name}, not {namespace}")
            continue
        if kind in CLUSTER_SCOPED:
            if not str(name).startswith(OURS):
                raise ValueError(f"cluster-scoped {kind}/{name} is not named for DuckFleet")
            continue
        if metadata.get("namespace") != namespace:
            raise ValueError(f"{kind}/{name} does not name namespace {namespace}")
        # A RoleBinding may bind a subject from any namespace, so the grant is only
        # bounded if its subjects are local. This is the one way these files could
        # hand pod-create to something outside the boundary.
        for subject in document.get("subjects") or []:
            if subject.get("kind") == "ServiceAccount" and subject.get("namespace") != namespace:
                raise ValueError(
                    f"{kind}/{name} binds a ServiceAccount outside {namespace}"
                )
    return documents


def documents(namespace, node_class=""):
    """Every boundary object, rendered and checked, in the order it must be applied.

    `node_class` adds the objects that need a real cloud cluster, and names the
    NodeClass the node pool draws its node configuration from. Omitted, the result is
    the portable boundary the local Kubernetes harness applies.
    """
    paths = sorted(BOUNDARY.glob("*.yaml"))
    if node_class:
        paths += sorted(EKS.glob("*.yaml"))
    rendered = []
    for path in paths:
        parsed = yaml.safe_load_all(render(path.read_text(), namespace, node_class))
        rendered.extend(document for document in parsed if document)
    return placed(rendered, namespace)


if __name__ == "__main__":
    try:
        rendered = documents(*sys.argv[1:3])
        print(yaml.safe_dump_all(rendered, sort_keys=False))
    except (KeyError, ValueError) as error:
        raise SystemExit(f"the boundary does not render for this target: {error}") from error
