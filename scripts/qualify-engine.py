#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
IMAGES = {
    "worker": ("docker/server.Dockerfile", "duckfleet-worker"),
    "dbt": ("docker/dbt.Dockerfile", "duckfleet-dbt"),
    "api": ("docker/cli.Dockerfile", "duckfleet-api"),
}


def run(args, *, cwd=ROOT, env=None, log=None, check=True):
    if log is None:
        return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, check=check)
    with log.open("a") as handle:
        handle.write("$ " + " ".join(map(str, args)) + "\n")
        handle.flush()
        result = subprocess.run(args, cwd=cwd, env=env, text=True, stdout=handle, stderr=subprocess.STDOUT, check=False)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(map(str, args))}; see {log}")
    return result


def output(args, **kwargs):
    return run(args, **kwargs).stdout.strip()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def resolved_ref(ref):
    return output(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"])


def build_images(tag, ref, directory):
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]*", tag):
        raise ValueError(f"invalid image tag: {tag!r}")
    commit = resolved_ref(ref)
    directory.mkdir(parents=True, exist_ok=True)
    log = directory / "build.log"
    with tempfile.TemporaryDirectory(prefix="source-", dir=directory) as temporary:
        context = Path(temporary)
        archive = directory / "source.tar"
        with archive.open("wb") as handle:
            subprocess.run(["git", "archive", commit], cwd=ROOT, stdout=handle, check=True)
        subprocess.run(["tar", "-xf", str(archive), "-C", str(context)], check=True)
        archive.unlink()
        for dockerfile, repository in IMAGES.values():
            image = f"{repository}:{tag}"
            print(f"Building {image} from {commit[:12]}", flush=True)
            run(["docker", "build", "--progress=plain", "-f", dockerfile, "-t", image, "."], cwd=context, log=log)
    return commit


def image_evidence(tag):
    result = {}
    for _, repository in IMAGES.values():
        image = f"{repository}:{tag}"
        value = json.loads(output(["docker", "image", "inspect", image]))[0]
        result[image] = {"id": value["Id"], "repo_digests": value.get("RepoDigests", [])}
    return result


def compose_args(project):
    return ["docker", "compose", "--project-name", project, "--file", str(ROOT / "compose.yaml")]


def write_override(path, prefix):
    paths = {
        "lake": f"s3://duckfleet/{prefix}/lake/",
        "results": f"s3://duckfleet/{prefix}/results/",
    }
    services = {}
    for service in ["worker-small", "worker-small-b", "worker-pool-interactive-0", "server", "server-pools"]:
        environment = {
            "DUCKFLEET_RESULT_LAKE_DATA_PATH": paths["lake"],
            "DUCKFLEET_RESULT_DATA_PATH": paths["results"],
        }
        if service.startswith("worker"):
            environment["S3_DATA_PATH"] = paths["lake"]
        services[service] = {"environment": environment}
    path.write_text("services:\n" + "\n".join(
        [f"  {name}:\n    environment:\n" + "\n".join(f"      {key}: {json.dumps(value)}" for key, value in service["environment"].items())
         for name, service in services.items()]
    ) + "\n")
    return paths


def container_name(project, service):
    container = output(compose_args(project) + ["ps", "-q", service])
    if not container:
        raise RuntimeError(f"{project}/{service} has no container")
    return output(["docker", "inspect", "--format", "{{.Name}}", container]).lstrip("/")


def resource_evidence(project):
    resources = {}
    for service in ["worker-small", "server"]:
        container = output(compose_args(project) + ["ps", "-q", service])
        inspected = json.loads(output(["docker", "inspect", container]))[0]
        stats = json.loads(output(["docker", "stats", "--no-stream", "--format", "{{json .}}", container]))
        resources[service] = {
            "container": inspected["Name"].lstrip("/"),
            "nano_cpus": inspected["HostConfig"]["NanoCpus"],
            "memory_bytes": inspected["HostConfig"]["Memory"],
            "stats": stats,
        }
    return resources


def isolation_evidence(baseline, candidate, paths):
    networks = {}
    volumes = {}
    names = {}
    for lane, project in [("baseline", baseline), ("candidate", candidate)]:
        networks[lane] = {}
        volumes[lane] = {}
        names[lane] = {}
        for service in ["postgres", "object-store", "server", "worker-small"]:
            container = output(compose_args(project) + ["ps", "-q", service])
            value = json.loads(output(["docker", "inspect", container]))[0]
            name = value["Name"].lstrip("/")
            names[lane][service] = name
            networks[lane][service] = sorted(value["NetworkSettings"]["Networks"])
            if service in ["postgres", "object-store"]:
                volumes[lane][service] = [mount["Name"] for mount in value["Mounts"] if mount.get("Type") == "volume"]
    dns_refusals = {}
    for service in ["postgres", "object-store", "server"]:
        target = names["baseline"][service]
        result = run(compose_args(candidate) + ["exec", "-T", "worker-small", "getent", "hosts", target], check=False)
        dns_refusals[target] = result.returncode
    data_paths = {
        lane: output(compose_args(project) + ["exec", "-T", "worker-small", "printenv", "S3_DATA_PATH"])
        for lane, project in [("baseline", baseline), ("candidate", candidate)]
    }
    baseline_networks = {network for values in networks["baseline"].values() for network in values}
    candidate_networks = {network for values in networks["candidate"].values() for network in values}
    baseline_volumes = {volume for values in volumes["baseline"].values() for volume in values}
    candidate_volumes = {volume for values in volumes["candidate"].values() for volume in values}
    passed = (
        not (baseline_networks & candidate_networks)
        and not (baseline_volumes & candidate_volumes)
        and all(code != 0 for code in dns_refusals.values())
        and data_paths["baseline"] != data_paths["candidate"]
        and data_paths == {"baseline": paths["baseline"]["lake"], "candidate": paths["candidate"]["lake"]}
    )
    return {
        "passed": passed,
        "projects": [baseline, candidate],
        "networks": networks,
        "volumes": volumes,
        "baseline_dns_refusals_from_candidate": dns_refusals,
        "data_paths": data_paths,
    }


def pgwire_checks(base, env, directory):
    token = env.get("DUCKFLEET_TOKEN", "duckfleet-local-development-token")

    def query(sql, password=token):
        command = base + [
            "exec", "-T", "-e", f"PGPASSWORD={password}", "postgres",
            "psql", "-h", "server", "-p", "5432", "-U", "operator", "-d", "lake",
            "-v", "ON_ERROR_STOP=1", "-qtAX", "-c", sql,
        ]
        return run(command, env=env, check=False)

    for sql, expected in [
        ("select 1 + 1", "2"),
        ("select 42::bigint, 'text value', true, 1.5::double", "42|text value|t|1.5"),
        ("select date '2026-01-02', timestamp '2026-01-02 03:04:05'", "2026-01-02|2026-01-02 03:04:05.000000"),
        ("select 12.34::decimal(6,2)", "12.34"),
    ]:
        result = query(sql)
        if result.returncode or result.stdout.strip() != expected:
            raise RuntimeError(f"pgwire result changed for {sql}: {result.stdout} {result.stderr}")
    missing = query("select * from no_such_table_for_pgwire")
    if missing.returncode == 0 or "sql_error" not in missing.stderr:
        raise RuntimeError(f"pgwire missing-relation classification changed: {missing.stderr}")
    if query("select 1", "not-a-credential").returncode == 0:
        raise RuntimeError("pgwire accepted an invalid credential")
    (directory / "pgwire.json").write_text(json.dumps({"passed": True, "typed_results": 4, "error_classification": "sql_error"}) + "\n")


def validate_extensions(report):
    required = {"httpfs", "postgres_scanner", "ducklake", "aws", "icu"}
    extensions = report.get("extensions", [])
    by_name = {item.get("extension_name"): item for item in extensions}
    if set(by_name) != required:
        raise RuntimeError(f"extension manifest mismatch: {extensions}")
    for name, item in by_name.items():
        if item.get("installed") != "true":
            raise RuntimeError(f"extension {name} is not installed: {item}")
        if item.get("loaded") == "true" and not item.get("extension_version"):
            raise RuntimeError(f"loaded extension {name} has no version: {item}")
        path = item.get("install_path")
        if path != "(BUILT-IN)" and "/v" + report["engine_version"].lstrip("v") + "/" not in path:
            raise RuntimeError(f"extension {name} has an unexpected path: {path}")
        if (name != "aws" or report["engine_version"].startswith("v2.")) and item.get("loaded") != "true":
            raise RuntimeError(f"required runtime extension {name} is not loaded: {item}")
    return extensions


def cli_sql(base, env, statement):
    command = base + ["run", "--rm", "--env", "DUCKFLEET_ROLE=cli", "api", "duckfleet-server", "sql", statement, "--format", "json"]
    result = run(command, env=env, check=False)
    if result.returncode:
        raise RuntimeError(f"recovery SQL failed: {statement}: {result.stderr.strip()}")
    return json.loads(result.stdout)


def cli_scalar(base, env, statement, expected):
    rows = cli_sql(base, env, statement)
    if len(rows) != 1 or rows[0].get("value") != expected:
        raise RuntimeError(f"recovery query {statement!r} returned {rows}, expected {expected!r}")
    return rows[0]["value"]


def recovery_drill(base, env, baseline_tag, candidate_tag, directory):
    evidence = {
        "catalog_version": None,
        "restore_verified": False,
        "rollback_verified": False,
        "mixed_version_writers": False,
    }
    recovery_log = directory / "recovery.log"
    try:
        version = cli_scalar(base, env, "SELECT value FROM lake.options() WHERE option_name='version'", "1.0")
        evidence["catalog_version"] = version
        cli_sql(base, env, "CREATE SCHEMA IF NOT EXISTS analytics")
        cli_sql(base, env, "CREATE OR REPLACE TABLE analytics.rollback_fixture(id INTEGER)")
        cli_sql(base, env, "INSERT INTO analytics.rollback_fixture VALUES (1)")
        snapshots = cli_sql(base, env, "SELECT snapshot_id AS value FROM ducklake_snapshots('lake') ORDER BY snapshot_id")
        if not snapshots:
            raise RuntimeError("the 1.0 fixture emitted no snapshot")
        snapshot = snapshots[-1]["value"]
        recovery_point = directory / "recovery-point"
        run(["bash", str(ROOT / "scripts/backup-catalog.sh"), "compose", str(recovery_point)], env=env, log=recovery_log)

        cli_sql(base, env, "INSERT INTO analytics.rollback_fixture VALUES (2)")
        cli_scalar(base, env, "SELECT count(*) AS value FROM analytics.rollback_fixture", 2)
        cli_scalar(base, env, f"SELECT count(*) AS value FROM analytics.rollback_fixture AT (VERSION => {snapshot})", 1)
        cli_sql(base, env, "CALL ducklake_merge_adjacent_files('lake')")

        restore_env = env.copy()
        restore_env["DUCKFLEET_CONFIRM_RESTORE"] = "yes"
        run(["bash", str(ROOT / "scripts/restore-catalog.sh"), "compose", str(recovery_point)], env=restore_env, log=recovery_log)
        running = output(base + ["ps", "--status", "running", "--format", "{{.Service}}"], env=env).splitlines()
        if any(service in running for service in ["server", "worker-small"]):
            raise RuntimeError(f"restore left compute running: {running}")
        run(base + ["up", "-d", "--wait", "server"], env=env, log=recovery_log)
        cli_scalar(base, env, "SELECT count(*) AS value FROM analytics.rollback_fixture", 1)
        cli_scalar(base, env, "SELECT value FROM lake.options() WHERE option_name='version'", "1.0")
        run(base + ["run", "--rm", "--env", "DUCKFLEET_ROLE=cli", "api", "duckfleet-server", "history", "--limit", "5"], env=env, log=recovery_log)
        evidence["restore_verified"] = True
        evidence["post_backup_write_discarded"] = True
        evidence["time_travel_snapshot"] = snapshot
        evidence["maintenance"] = "merge_adjacent_files"

        cli_sql(base, env, "INSERT INTO analytics.rollback_fixture VALUES (3)")
        cli_scalar(base, env, "SELECT count(*) AS value FROM analytics.rollback_fixture", 2)
        run(["bash", str(ROOT / "scripts/stop-compute.sh"), "compose"], env=env, log=recovery_log)

        rollback_env = env.copy()
        rollback_env["DUCKFLEET_IMAGE_TAG"] = baseline_tag
        rollback_env["DUCKFLEET_DBT_IMAGE_TAG"] = baseline_tag
        rollback_env["DUCKFLEET_API_IMAGE_TAG"] = baseline_tag
        run(base + ["up", "-d", "--wait", "server"], env=rollback_env, log=recovery_log)
        cli_scalar(base, rollback_env, "SELECT value FROM lake.options() WHERE option_name='version'", "1.0")
        cli_scalar(base, rollback_env, "SELECT count(*) AS value FROM analytics.rollback_fixture", 2)
        cli_sql(base, rollback_env, "INSERT INTO analytics.rollback_fixture VALUES (4)")
        cli_scalar(base, rollback_env, "SELECT count(*) AS value FROM analytics.rollback_fixture", 3)
        run(base + ["run", "--rm", "--env", "DUCKFLEET_ROLE=cli", "api", "duckfleet-server", "history", "--limit", "5"], env=rollback_env, log=recovery_log)
        evidence["rollback_verified"] = True
        evidence["rollback_engine"] = "v1.5.4"
        evidence["write_after_restore_engine"] = "v2.0.1"
        evidence["passed"] = True
        return evidence
    except Exception as error:
        evidence["passed"] = False
        evidence["error"] = str(error)
        raise
    finally:
        (directory / "recovery.json").write_text(json.dumps(evidence, indent=2) + "\n")


def cli_json(base, env, args):
    command = base + ["run", "--rm", "--env", "DUCKFLEET_ROLE=cli", "api", "duckfleet-server", *args]
    result = run(command, env=env, check=False)
    if result.returncode:
        raise RuntimeError(f"CLI command failed: {args}: {result.stderr.strip()}")
    return json.loads(result.stdout)


def cutover_drill(base, env, baseline_tag, candidate_tag, directory, skip_dbt):
    evidence = {"drained_statement": False, "old_writers": 0, "passed": False}
    log = directory / "cutover.log"
    process = None
    try:
        run(["bash", str(ROOT / "scripts/backup-catalog.sh"), "compose", str(directory / "cutover-point")], env=env, log=log)
        with log.open("a") as handle:
            process = subprocess.Popen(
                base + ["run", "--rm", "--env", "DUCKFLEET_ROLE=cli", "api", "duckfleet-server", "sql", "SELECT sum(sin(i)) FROM range(100000000000) AS source(i)"],
                cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT,
            )
        observed = False
        for _ in range(30):
            workers = cli_json(base, env, ["workers"])["workers"]
            if any(worker.get("active_queries", 0) > 0 for worker in workers):
                observed = True
                break
            if process.poll() is not None:
                break
            time.sleep(0.5)
        if not observed:
            raise RuntimeError("the cutover statement never became active")
        run(["bash", str(ROOT / "scripts/stop-compute.sh"), "compose"], env=env, log=log)
        result = process.wait(timeout=120)
        process = None
        if result != 0:
            raise RuntimeError(f"the active statement did not drain successfully: {result}; see {log}")
        evidence["drained_statement"] = True
        refused = run(base + ["run", "--rm", "--env", "DUCKFLEET_ROLE=cli", "api", "duckfleet-server", "sql", "SELECT 1"], env=env, check=False)
        if refused.returncode == 0:
            raise RuntimeError("stopped compute accepted new SQL")

        env["DUCKFLEET_IMAGE_TAG"] = candidate_tag
        env["DUCKFLEET_DBT_IMAGE_TAG"] = candidate_tag
        env["DUCKFLEET_API_IMAGE_TAG"] = candidate_tag
        run(base + ["up", "-d", "--wait", "server"], env=env, log=log)
        cli_scalar(base, env, "SELECT version() AS value", "v2.0.1")
        cli_scalar(base, env, "SELECT value FROM lake.options() WHERE option_name='version'", "1.0")
        workers = cli_json(base, env, ["workers"])["workers"]
        if not workers or any(worker.get("duckdb_version") != "v2.0.1" for worker in workers):
            raise RuntimeError(f"an old or unknown engine remains after cutover: {workers}")

        expected_image = image_evidence(candidate_tag)["duckfleet-worker:" + candidate_tag]["id"]
        ids = output(base + ["ps", "--status", "running", "--quiet"], env=env).split()
        old = []
        for container in ids:
            inspected = json.loads(output(["docker", "inspect", container]))[0]
            environment = dict(item.split("=", 1) for item in inspected["Config"].get("Env", []) if "=" in item)
            if (environment.get("DUCKFLEET_ROLE") == "gateway" or "DUCKFLEET_WORKER_ID" in environment) and inspected["Image"] != expected_image:
                old.append(inspected["Name"])
        if old:
            raise RuntimeError(f"1.5.4 writers remain after cutover: {old}")
        evidence["old_writers"] = 0

        pgwire_checks(base, env, directory)
        smoke = ["http", "pgwire"]
        if not skip_dbt:
            run(base + ["run", "--rm", "dbt", "dbt", "debug"], env=env, log=log)
            run(base + ["run", "--rm", "dbt", "duckfleet-server", "--jobs"], env=env, log=log)
            smoke.extend(["dbt", "jobs", "artifacts"])
        run(base + ["run", "--rm", "dbt", "duckfleet-server", "--maintenance"], env=env, log=log)
        run(base + ["run", "--rm", "dbt", "duckfleet-server", "--rbac"], env=env, log=log)
        cli_json(base, env, ["history", "--limit", "5"])
        evidence["smoke"] = smoke + ["maintenance", "rbac", "history"]
        evidence["engine"] = "v2.0.1"
        evidence["catalog_version"] = "1.0"
        evidence["passed"] = True
        return evidence
    except Exception as error:
        evidence["error"] = str(error)
        raise
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        (directory / "cutover.json").write_text(json.dumps(evidence, indent=2) + "\n")


def run_lane(name, project, tag, version, directory, override, corpus_tag, skip_dbt):
    directory.mkdir(parents=True, exist_ok=True)
    artifacts = directory / "artifacts"
    artifacts.mkdir(exist_ok=True)
    env = os.environ.copy()
    env.update({
        "COMPOSE_PROJECT_NAME": project,
        "DUCKFLEET_IMAGE_TAG": tag,
        "DUCKFLEET_DBT_IMAGE_TAG": tag,
        "DUCKFLEET_API_IMAGE_TAG": tag,
        "DUCKFLEET_ARTIFACTS_DIRECTORY": str(artifacts),
        "DUCKFLEET_PORT": str(free_port()),
        "DUCKFLEET_POOLS_PORT": str(free_port()),
    })
    base = compose_args(project) + ["--file", str(override)]
    run(base + ["down", "--volumes", "--remove-orphans"], env=env, check=False)
    summary = {"name": name, "project": project, "image_tag": tag, "expected_engine": version, "images": image_evidence(tag), "stages": {}}
    try:
        run(base + ["up", "-d", "--wait", "server"], env=env, log=directory / "startup.log")
        stages = [
            ("api", ["run", "--rm", "api"]),
            ("dbt", ["run", "--rm", "dbt"]),
            ("scale", ["run", "--rm", "dbt", "duckfleet-server", "--scale"]),
            ("concurrency", ["run", "--rm", "dbt", "duckfleet-server", "--concurrency"]),
            ("jobs", ["run", "--rm", "dbt", "duckfleet-server", "--jobs"]),
            ("engine-corpus", ["run", "--rm", "api", "duckfleet-server", "--engine-qualification"]),
            ("observability", ["run", "--rm", "dbt", "duckfleet-server", "--observability"]),
            ("pgwire", None),
        ]
        for stage, command in stages:
            if skip_dbt and stage in ("dbt", "scale"):
                summary["stages"][stage] = "skipped_local_arm"
                continue
            print(f"{name}: {stage}", flush=True)
            if command is None:
                pgwire_checks(base, env, directory)
            else:
                stage_env = env.copy()
                if stage == "engine-corpus":
                    stage_env["DUCKFLEET_API_IMAGE_TAG"] = corpus_tag
                run(base + command, env=stage_env, log=directory / f"{stage}.log")
            summary["stages"][stage] = "passed"
        if name == "candidate":
            stage_env = env.copy()
            stage_env["DUCKFLEET_API_IMAGE_TAG"] = corpus_tag
            run(base + ["run", "--rm", "api", "duckfleet-server", "--extension-security"], env=stage_env, log=directory / "extension-security.log")
            summary["stages"]["extension-security"] = "passed"
        summary["resources"] = resource_evidence(project)
        report = json.loads((artifacts / "engine-qualification.json").read_text())
        summary["actual_engine"] = report.get("engine_version")
        summary["extensions"] = validate_extensions(report)
        summary["passed"] = summary["actual_engine"] == version
        if not summary["passed"]:
            raise RuntimeError(f"{name} expected {version}, got {summary['actual_engine']}")
        return summary, base, env
    except Exception as error:
        summary["passed"] = False
        summary["error"] = str(error)
        run(base + ["logs", "--no-color"], env=env, log=directory / "services.log", check=False)
        (directory / "lane.json").write_text(json.dumps(summary, indent=2) + "\n")
        raise
    finally:
        (directory / "lane.json").write_text(json.dumps(summary, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Build and compare isolated DuckFleet engine lanes")
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--baseline-version", default="v1.5.4")
    parser.add_argument("--candidate-version", default="v2.0.1")
    parser.add_argument("--baseline-tag", default="qualification-baseline")
    parser.add_argument("--candidate-tag", default="qualification-candidate")
    parser.add_argument("--expectations", type=Path, default=ROOT / "examples/engine-qualification-2.0.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-dbt", action="store_true", help="local ARM diagnostic only; the report is not release-complete")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if args.baseline_tag == args.candidate_tag and not args.skip_build:
        parser.error("baseline and candidate tags must differ when building")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    projects = {"baseline": "duckfleet-qualification-baseline", "candidate": "duckfleet-qualification-candidate"}
    running = []
    summary = {"passed": False, "release_complete": not args.skip_dbt, "baseline_ref": resolved_ref(args.baseline_ref), "candidate_ref": resolved_ref(args.candidate_ref), "lanes": {}}
    try:
        if not args.skip_build:
            summary["baseline_ref"] = build_images(args.baseline_tag, args.baseline_ref, output / "build-baseline")
            summary["candidate_ref"] = build_images(args.candidate_tag, args.candidate_ref, output / "build-candidate")
        paths = {}
        runtime = {}
        for lane, tag, version in [("baseline", args.baseline_tag, args.baseline_version), ("candidate", args.candidate_tag, args.candidate_version)]:
            directory = output / lane
            directory.mkdir(parents=True, exist_ok=True)
            paths[lane] = write_override(directory / "override.yaml", f"engine-qualification/{lane}")
            lane_summary, base, env = run_lane(lane, projects[lane], tag, version, directory, directory / "override.yaml", args.candidate_tag, args.skip_dbt)
            running.append((base, env))
            runtime[lane] = (base, env)
            summary["lanes"][lane] = lane_summary
        summary["isolation"] = isolation_evidence(projects["baseline"], projects["candidate"], paths)
        comparison = output / "comparison.json"
        result = run([
            sys.executable, str(ROOT / "scripts/compare-engine-qualification.py"),
            "--baseline", str(output / "baseline/artifacts/engine-qualification.json"),
            "--candidate", str(output / "candidate/artifacts/engine-qualification.json"),
            "--expectations", str(args.expectations),
            "--output", str(comparison),
        ], check=False)
        summary["comparison"] = json.loads(comparison.read_text())
        summary["recovery"] = recovery_drill(
            *runtime["candidate"], args.baseline_tag, args.candidate_tag, output / "candidate"
        )
        summary["cutover"] = cutover_drill(
            *runtime["candidate"], args.baseline_tag, args.candidate_tag, output / "candidate", args.skip_dbt
        )
        summary["passed"] = bool(
            summary["isolation"]["passed"]
            and summary["comparison"]["passed"]
            and summary["recovery"]["passed"]
            and summary["cutover"]["passed"]
            and all(lane.get("passed") for lane in summary["lanes"].values())
        )
        print(json.dumps({"passed": summary["passed"], "output": str(output)}))
        return 0 if summary["passed"] else 1
    except Exception as error:
        summary["error"] = str(error)
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        summary["output"] = str(output)
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        if not args.keep:
            for base, env in reversed(running):
                run(base + ["down", "--volumes", "--remove-orphans"], env=env, check=False)
            for project in projects.values():
                run(compose_args(project) + ["down", "--volumes", "--remove-orphans"], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
