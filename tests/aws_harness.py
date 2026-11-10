"""Run the same dbt lifecycle on EKS, then prove the configured AWS S3 path."""
import json
import os
import re
from pathlib import Path

import harness
from dbt.adapters.duckfleet.client import RemoteConnection

harness.main()
prefix = os.environ['EXPECT_S3_PREFIX']
if not re.fullmatch(r's3://duckfleet-dev-[a-z0-9-]+/lake/', prefix):
    raise ValueError('AWS harness must target the DuckFleet development lake')
connection = RemoteConnection(harness.URL, harness.TOKEN)
try:
    # Executes on the worker using Pod Identity, not in the dbt test process.
    files = harness.sql(connection, f"SELECT DISTINCT filename FROM read_parquet('{prefix}**/*.parquet', filename=true, union_by_name=true) LIMIT 5")
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
