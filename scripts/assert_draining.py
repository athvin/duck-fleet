"""Asserts a draining worker refuses a new session in terms a caller can act on.

A closed connection or an unclassified 503 would be indistinguishable from the worker
having crashed, which is the difference between "open a session elsewhere now" and
"something is wrong with this deployment".
"""
import json
import sys

body = json.loads(open(sys.argv[1]).read())
assert body["failure_kind"] == "draining", body
# Nothing ran, so the caller may open a session elsewhere immediately. This is what
# separates draining from the generic unavailable an internal failure returns.
assert body["retry_safe"] is True, body
assert "no statement was executed" in body["error"], body
print("PASS: a draining worker refused a new session as retry-safe with nothing run")
