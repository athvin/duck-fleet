"""Require both real main-branch validation workflows on this exact revision."""
import json
import os
import subprocess
import time

revision = os.environ['GITHUB_SHA']
repo = os.environ['GITHUB_REPOSITORY']
expected = {'local-harness.yml', 'infra.yml'}
for attempt in range(120):
    passed = set()
    for workflow in expected:
        runs = json.loads(subprocess.check_output([
            'gh', 'run', 'list', '--repo', repo, '--workflow', workflow,
            '--commit', revision, '--event', 'push', '--branch', 'main', '--limit', '1',
            '--json', 'status,conclusion,databaseId',
        ]))
        if runs and runs[0]['status'] == 'completed':
            if runs[0]['conclusion'] != 'success':
                raise SystemExit(f'{workflow} failed: {runs[0]}')
            passed.add(workflow)
    if passed == expected:
        head = subprocess.check_output([
            'gh', 'api', f'repos/{repo}/git/ref/heads/main', '--jq', '.object.sha',
        ], text=True).strip()
        if head != revision:
            raise SystemExit('Refusing deployment of a superseded main revision')
        print('Both main validation workflows passed on this revision')
        break
    print(f'Waiting for main checks: {sorted(expected - passed)}', flush=True)
    time.sleep(15)
else:
    raise SystemExit('Timed out waiting for main validation')
