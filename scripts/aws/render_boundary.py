"""Renders the DuckFleet ownership boundary for one namespace.

The boundary is what an operator with cluster admin applies once: the namespace, the
gateway's identity, the pod grant isolated compute needs, and the quota that bounds it.
The Helm release contains none of it on purpose — the deploy identity can create
neither Roles nor ResourceQuotas, and `scripts/aws/check-access.sh` fails the deploy if
it ever can.

The namespace is substituted rather than committed because `scripts/aws/target.env` is
the one place the deploy target is named. Every object is then checked to carry that
namespace explicitly, so applying these cannot depend on a `-n` flag being passed and
cannot bind one deployment's grant to another deployment's gateway.
"""
import pathlib
import string
import sys

import yaml

# Applied in filename order, which is why the files are numbered: the namespace has to
# exist before anything can be created inside it, and `kubectl apply` does not reorder.
BOUNDARY = pathlib.Path(__file__).resolve().parents[2] / "infra/kubernetes/boundary"
CLUSTER_SCOPED = {"Namespace"}


def render(text, namespace):
    """`text` with the namespace substituted, raising if a placeholder is unresolved."""
    return string.Template(text).substitute(namespace=namespace)


def placed(documents, namespace):
    """The documents, having checked every one names the namespace it belongs to.

    An object with no namespace is not an error to Kubernetes — it lands wherever the
    caller's context points — so this is the check that a boundary applied with the
    wrong context fails instead of granting a gateway somewhere unintended.
    """
    for document in documents:
        kind, metadata = document["kind"], document["metadata"]
        name = metadata.get("name")
        if kind in CLUSTER_SCOPED:
            if name != namespace:
                raise ValueError(f"the {kind} is {name}, not {namespace}")
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


def documents(namespace, directory=BOUNDARY):
    """Every boundary object, rendered and checked, in the order it must be applied."""
    rendered = []
    for path in sorted(directory.glob("*.yaml")):
        parsed = yaml.safe_load_all(render(path.read_text(), namespace))
        rendered.extend(document for document in parsed if document)
    return placed(rendered, namespace)


if __name__ == "__main__":
    try:
        print(yaml.safe_dump_all(documents(sys.argv[1]), sort_keys=False))
    except (KeyError, ValueError) as error:
        raise SystemExit(f"the boundary does not render for this target: {error}") from error
