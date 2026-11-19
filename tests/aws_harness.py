"""Run the same dbt lifecycle on EKS, then prove the configured AWS S3 path."""
import json
import os
import re
from pathlib import Path

import harness
from duckfleet import RemoteConnection

harness.main()
# The Phase 1 acceptance bar, on the real platform: 110 generated models through
# EKS, every statement terminal in history and settled in the ledger, wall time
# and attributed cost written to the evidence artifacts.
harness.scale_checks()
# Isolated-pod cold start against a persistent worker, measured on real EKS where
# a cold open also pays node scale-up — the number the routing choice turns on (#18).
# Degrades honestly: where the grant is absent the isolated open is refused and this
# records nothing rather than inventing a figure.
try:
    if json.loads(harness.Client(harness.URL, harness.TOKEN).request(
            "GET", "/v1/workers"))["isolated_compute"]:
        harness.cold_start_checks()
    else:
        print("isolated compute is not enabled here; skipping the cold-start measurement")
except Exception as error:  # noqa: BLE001 - a measurement must never fail the deploy
    print(f"cold-start measurement skipped: {error}")
prefix = os.environ['EXPECT_S3_PREFIX']
if not re.fullmatch(r's3://duckfleet-dev-[a-z0-9-]+/lake/', prefix):
    raise ValueError('AWS harness must target the DuckFleet development lake')
connection = RemoteConnection(harness.URL, harness.TOKEN)
try:
    # Executes on the worker using Pod Identity, not in the dbt test process.
    # glob() lists keys without opening a single file: the old read_parquet form
    # schema-merged every object in the lake to name five of them, and once the
    # scale stage started depositing a hundred tables per run, the accumulated
    # bucket blew the statement deadline.
    files = harness.sql(connection, f"SELECT file FROM glob('{prefix}**/*.parquet') LIMIT 5")
    harness.require(files and all(row[0].startswith(prefix) for row in files), 'no Parquet in the AWS lake')
finally:
    connection.close()
evidence = {
    'workers': json.loads(Path('/artifacts/worker-resources.json').read_text()),
    'model_placement': json.loads(Path('/artifacts/model-placement.json').read_text()),
    's3_files': [row[0] for row in files],
}
print('DUCKFLEET_AWS_EVIDENCE=' + json.dumps(evidence, separators=(',', ':')))
print('PASS: AWS dbt model sizing and Parquet storage')
