"""Generate development Secret material with distinct, namespace-scoped DB roles.

Output is a Secret for piping directly to kubectl, never a diagnostic artifact.
Role passwords are not reused for admin, bootstrap, internal auth, or another role.
"""
import argparse
import hashlib
import json
import re
import secrets

PARTS = ("migrator", "gateway", "worker", "maintainer", "catalog", "catalog_reader")
DSN_KEYS = {"migrator": "PLATFORM_MIGRATION_DSN", "gateway": "PLATFORM_GATEWAY_DSN",
            "worker": "PLATFORM_WORKER_DSN", "maintainer": "PLATFORM_MAINTENANCE_DSN",
            "catalog": "CATALOG_DSN", "catalog_reader": "CATALOG_READER_DSN"}


def role_names(namespace):
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", namespace):
        raise ValueError("namespace must be a lowercase Kubernetes namespace name")
    prefix = "df_" + namespace.replace("-", "_")[:36] + "_" + hashlib.sha256(namespace.encode()).hexdigest()[:8]
    return {part: f"{prefix}_{part}" for part in PARTS}


def secret(namespace, name="duckfleet-dev", host="postgres"):
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", host):
        raise ValueError("host must be a DNS name or IPv4 address")
    roles = role_names(namespace)
    values = {"POSTGRES_PASSWORD": secrets.token_hex(32), "DUCKFLEET_TOKEN": secrets.token_hex(32),
              "DUCKFLEET_INTERNAL_TOKEN": secrets.token_hex(32)}
    for part in PARTS:
        password = secrets.token_hex(32)
        values[f"DATABASE_{part.upper()}_PASSWORD"] = password
        database = "lake" if part == "catalog" else "platform"
        values[DSN_KEYS[part]] = f"host={host} port=5432 user={roles[part]} password={password} dbname={database}"
    return {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace},
            "type": "Opaque", "stringData": values}


def retarget(document, host):
    """Change only the endpoint, retaining every component's independent identity."""
    import base64
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", host):
        raise ValueError("host must be a DNS name or IPv4 address")
    data = {key: base64.b64decode(value).decode() for key, value in document.get("data", {}).items()}
    data.update(document.get("stringData", {}))
    rewritten = {}
    for key in DSN_KEYS.values():
        original = data[key]
        updated, count = re.subn(r"(?<!\S)host=\S+", f"host={host}", original)
        if count != 1:
            raise ValueError(f"{key} must have one unquoted host= field for retargeting")
        rewritten[key] = updated
    return {"stringData": rewritten}


def check_secret(document):
    import base64
    data = {key: base64.b64decode(value).decode() for key, value in document.get("data", {}).items()}
    data.update(document.get("stringData", {}))
    required = set(DSN_KEYS.values()) | {"DUCKFLEET_TOKEN", "DUCKFLEET_INTERNAL_TOKEN"}
    missing = required - data.keys()
    if missing:
        raise ValueError("Secret predates separated database roles; stop compute and follow the schema3 provisioning upgrade: missing " + ", ".join(sorted(missing)))
    if data["DUCKFLEET_TOKEN"] == data["DUCKFLEET_INTERNAL_TOKEN"]:
        raise ValueError("public bootstrap and internal tokens must be distinct")
    if len({data[key] for key in DSN_KEYS.values()}) != len(DSN_KEYS):
        raise ValueError("each component requires its own database credential")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("namespace")
    parser.add_argument("--name", default="duckfleet-dev")
    parser.add_argument("--host", default="postgres")
    parser.add_argument("--check", action="store_true", help="validate required separated Secret keys from stdin without printing secrets")
    parser.add_argument("--upgrade", action="store_true", help="read an existing Secret from stdin and add separate role credentials; preserves public/admin tokens")
    args = parser.parse_args()
    if args.check:
        import sys
        try:
            check_secret(json.load(sys.stdin))
        except (ValueError, KeyError) as error:
            raise SystemExit(str(error)) from error
        raise SystemExit(0)
    result = secret(args.namespace, args.name, args.host)
    if args.upgrade:
        import sys, base64
        original = json.load(sys.stdin)
        existing = {key: base64.b64decode(value).decode() for key, value in original.get("data", {}).items()}
        existing.update(original.get("stringData", {}))
        for key in ("POSTGRES_PASSWORD", "DUCKFLEET_TOKEN"):
            if key not in existing:
                raise SystemExit(f"existing Secret lacks {key}; refusing partial upgrade")
        # A repeat must never rotate credentials underneath already provisioned roles.
        for key in list(result["stringData"]):
            if key in existing and (key != "CATALOG_DSN" or "DATABASE_CATALOG_PASSWORD" in existing):
                result["stringData"][key] = existing[key]
        for key, value in existing.items():
            result["stringData"].setdefault(key, value)
    print(json.dumps(result))
