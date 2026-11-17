"""Rewrites the password inside a DuckFleet Secret, leaving everything else alone.

The DSNs carry a host, port, database and user as well as the password. Rebuilding
them from parts would work until one of those differed from what this assumed, at
which point rotation would silently repoint the workers at the wrong database. So only
the password is substituted, and only in the keys known to hold one.

`DUCKFLEET_TOKEN` is deliberately untouched: it is the API bearer token, not a database
credential, and rotating it here would log every caller out as a side effect of a
database maintenance task. #17 replaces that token with per-tenant credentials.
"""
import base64
import json
import os
import re
import sys

PASSWORD_KEYS = ["POSTGRES_PASSWORD"]
DSN_KEYS = ["CATALOG_DSN", "PLATFORM_DSN"]


def decode(secret, key):
    """The value of `key`, whether the Secret carries it encoded or as a string."""
    data = secret.get("stringData") or {}
    encoded = secret.get("data") or {}
    if key in data:
        return data[key]
    if key in encoded:
        return base64.b64decode(encoded[key]).decode()
    raise KeyError(f"the secret has no {key}; refusing to rotate a partial credential")


def rotate(secret, password):
    """The same Secret with the database password replaced and nothing else changed."""
    rotated = {}
    for key in PASSWORD_KEYS:
        decode(secret, key)  # presence check; the value is replaced wholesale
        rotated[key] = password
    for key in DSN_KEYS:
        dsn = decode(secret, key)
        if "password=" not in dsn:
            raise KeyError(f"{key} carries no password= to rotate")
        # Only the password token, so host, port, dbname and user survive verbatim.
        rotated[key] = re.sub(r"password=\S+", f"password={password}", dsn)
    # Everything else is carried forward, so applying this cannot drop a key the chart
    # reads — including the bearer token, which is not ours to change here.
    for key in list(secret.get("stringData") or {}) + list(secret.get("data") or {}):
        if key not in rotated:
            rotated[key] = decode(secret, key)
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": secret["metadata"]["name"],
                     "namespace": secret["metadata"]["namespace"]},
        "type": secret.get("type", "Opaque"),
        "stringData": rotated,
    }


if __name__ == "__main__":
    try:
        print(json.dumps(rotate(json.load(sys.stdin), os.environ["DUCKFLEET_NEW_PASSWORD"])))
    except KeyError as error:
        raise SystemExit(str(error)) from error
