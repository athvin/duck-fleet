"""Asserts the drain actually waited for a statement, rather than outliving one.

The in-flight check is only meaningful if the statement was still running when the
termination signal arrived. If it finished first, the drain waited for nothing and the
test passes while proving nothing — which is exactly what the first version of it did,
with a statement that completed in a second, four seconds before the signal.

So the worker's own log is read for the order of three events: the signal, the
statement completing, and the drain ending. A speedup that makes the statement finish
before the signal fails here instead of silently hollowing out the test.
"""
import sys

# Defaults are the worker's three events. The gateway drain passes its own marker for
# the middle one — it has no engine, so what proves the wait is a request that was in
# flight when the signal arrived completing afterwards.
SIGNAL = sys.argv[2] if len(sys.argv) > 2 else "termination signal received"
FINISHED = sys.argv[3] if len(sys.argv) > 3 else "query finished"
COMPLETE = sys.argv[4] if len(sys.argv) > 4 else "drain complete"

lines = open(sys.argv[1], errors="ignore").read().splitlines()


def first(needle, after=-1):
    for index, line in enumerate(lines):
        if index > after and needle in line:
            return index
    return None


signal = first(SIGNAL)
if signal is None:
    raise SystemExit(f"the worker never logged {SIGNAL!r}; the signal was not handled")
finished = first(FINISHED, after=signal)
if finished is None:
    raise SystemExit(
        "no statement completed after the termination signal, so the drain waited for "
        "nothing; the in-flight statement must still be running when the signal lands"
    )
complete = first(COMPLETE, after=finished)
if complete is None:
    raise SystemExit(
        "the drain did not report completing after the statement finished, so the "
        "process stopped before its open work did"
    )
print("PASS: the signal arrived first, a statement finished during the drain, and the "
      "drain ended after it")
