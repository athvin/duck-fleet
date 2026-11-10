"""Cross-check API/model evidence against independently observed Kubernetes pods."""
import json
from pathlib import Path

root = Path('artifacts/aws')
records = [line.removeprefix('DUCKFLEET_AWS_EVIDENCE=') for line in (root/'harness.log').read_text().splitlines() if line.startswith('DUCKFLEET_AWS_EVIDENCE=')]
if len(records) != 1:
    raise RuntimeError('Expected exactly one completed AWS harness evidence record')
evidence = json.loads(records[0])
pods = json.loads((root/'pods.json').read_text())['items']
after = json.loads((root/'pods-after.json').read_text())['items']
workers = [p for p in pods if p['metadata'].get('labels', {}).get('app') == 'duckfleet-worker' and not p['metadata'].get('deletionTimestamp')]
assert len(workers) == 2, 'Expected exactly two worker pods'
assert {p['metadata']['labels']['size'] for p in workers} == {'small', 'medium'}
assert {w['worker_id'] for w in evidence['workers']} == {'small', 'medium'}
for pod in workers:
    current = next(p for p in after if p['metadata']['uid'] == pod['metadata']['uid'])
    assert not current['metadata'].get('deletionTimestamp'), 'Worker was replaced during the test'
    assert current['status']['containerStatuses'] == pod['status']['containerStatuses'], 'Worker restarted or changed during the test'
    size = pod['metadata']['labels']['size']
    cpu = 1 if size == 'small' else 2
    assert pod['metadata']['namespace'] == 'duckfleet-dev'
    assert pod['status']['phase'] == 'Running'
    assert pod['spec']['serviceAccountName'] == 'duckfleet-worker'
    container = next(c for c in pod['spec']['containers'] if c['name'] == 'worker')
    for kind in ['requests', 'limits']:
        assert container['resources'][kind]['cpu'] == str(cpu)
        assert container['resources'][kind]['memory'] == f'{cpu}Gi'
    observed = next(w for w in evidence['workers'] if w['worker_id'] == size)
    assert observed['cgroup_memory_max'] == cpu * 1024**3
    assert observed['cgroup_cpu_quota'] == cpu * observed['cgroup_cpu_period']
    assert observed['duckdb_threads'] == cpu
    observed['pod_uid'] = pod['metadata']['uid']
    observed['node'] = pod['spec']['nodeName']
assert evidence['s3_files'] and all(p.startswith('s3://duckfleet-dev-797781631727-us-east-2/lake/') for p in evidence['s3_files'])
(root/'evidence.json').write_text(json.dumps(evidence, indent=2)+'\n')
print('PASS: AWS model placement, Kubernetes requests/limits, worker cgroups, and S3 Parquet agree')
