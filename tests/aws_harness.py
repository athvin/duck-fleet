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
# Isolated compute is enabled on this lane (the boundary grants it, deploy_values
# turns it on), so the harness asserts it is actually live — the deploy identity
# cannot check from outside the cluster, so this in-cluster run is where a missing
# or broken grant is caught (#39). routing_contract_checks above already exercised
# an isolated open; the cold-start measurement (#18) needs a pod to actually run, so
# it is attempted but tolerated when the shared cluster cannot schedule one. That
# scheduling gap on EKS Auto Mode is duck-fleet#90; the kind harness records the
# measurement where scheduling is not contended.
harness.require(
    json.loads(harness.Client(harness.URL, harness.TOKEN).request(
        "GET", "/v1/workers"))["isolated_compute"],
    "isolated compute is not live: the gateway does not hold the pod grant. "
    "Run scripts/aws/apply-boundary.sh with an administrative profile (#39).")
try:
    harness.cold_start_checks()
except harness.RemoteError as error:
    harness.require(error.failure_kind == "not_placeable",
                    f"the isolated open failed as {error.failure_kind}, not a "
                    f"placement limit: {error}")
    print("cold start not measured on EKS: the shared cluster could not schedule an "
          "isolated pod (not_placeable); see duck-fleet#90")
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
