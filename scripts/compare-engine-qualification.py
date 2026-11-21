#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def load(path):
    with path.open() as handle:
        return json.load(handle)


def by_id(report):
    queries = report.get("queries")
    if not isinstance(queries, list):
        raise ValueError("qualification report has no query list")
    result = {}
    for query in queries:
        query_id = query.get("id")
        if not query_id or query_id in result:
            raise ValueError(f"duplicate or missing query id: {query_id!r}")
        result[query_id] = query
    return result


def comparable(query):
    return {
        "sql": query.get("sql"),
        "outcome": query.get("outcome"),
        "profile_status": (query.get("profile") or {}).get("status"),
    }


def performance(baseline, candidate):
    before = baseline.get("elapsed_ms")
    after = candidate.get("elapsed_ms")
    value = {"baseline_ms": before, "candidate_ms": after}
    if isinstance(before, (int, float)) and isinstance(after, (int, float)) and before > 0:
        value["candidate_over_baseline"] = round(after / before, 6)
    return value


def main():
    parser = argparse.ArgumentParser(description="Compare two DuckFleet engine qualification reports")
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--expectations", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    baseline = load(args.baseline)
    candidate = load(args.candidate)
    expectations = load(args.expectations)
    accepted = set(expectations.get("accepted", []))
    fixed = set(expectations.get("fixed", []))
    unknown = (accepted | fixed) - set(by_id(baseline))
    if unknown:
        raise ValueError(f"expectations refer to unknown baseline cases: {sorted(unknown)}")
    if accepted & fixed:
        raise ValueError(f"cases cannot be both accepted and fixed: {sorted(accepted & fixed)}")

    before = by_id(baseline)
    after = by_id(candidate)
    queries = []
    counts = {"unchanged": 0, "accepted": 0, "fixed": 0, "blocking": 0}
    for query_id in sorted(set(before) | set(after)):
        old = before.get(query_id)
        new = after.get(query_id)
        if old is None or new is None:
            status = "blocking"
            difference = {"baseline": old is not None, "candidate": new is not None}
            perf = {}
        elif comparable(old) == comparable(new):
            status = "unchanged"
            difference = None
            perf = performance(old, new)
        else:
            status = "accepted" if query_id in accepted else "fixed" if query_id in fixed else "blocking"
            difference = {"baseline": comparable(old), "candidate": comparable(new)}
            perf = performance(old, new)
        counts[status] += 1
        item = {"id": query_id, "status": status, "performance": perf}
        if difference is not None:
            item["difference"] = difference
        if old is not None and new is not None:
            item["sql"] = new.get("sql")
        queries.append(item)

    version_mismatch = (
        baseline.get("engine_version") == candidate.get("engine_version")
        and not expectations.get("allow_same_version", False)
    )
    report = {
        "schema_version": 1,
        "baseline_engine": baseline.get("engine_version"),
        "candidate_engine": candidate.get("engine_version"),
        "passed": counts["blocking"] == 0 and not version_mismatch,
        "counts": counts,
        "version_mismatch": version_mismatch,
        "accepted_expectations": sorted(accepted),
        "fixed_expectations": sorted(fixed),
        "queries": queries,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "counts": counts, "version_mismatch": version_mismatch}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
