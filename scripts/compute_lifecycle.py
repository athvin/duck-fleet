"""Explicit compute shutdown and read-only schema-upgrade preflight.

Never stops PostgreSQL, changes schema, or restarts compute. Kubernetes selection
uses this namespace's DuckFleet app labels; Compose selection uses this project
and process-role environment. A schema marker is rollout intent, not a DB fence.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

PLATFORM_SCHEMA = 7
SCHEMA_MARKER = 'duckfleet.io/platform-schema'
GATEWAY = 'duckfleet-gateway'
WORKER = 'duckfleet-worker'
ROOT = Path(__file__).resolve().parents[1]


def run(command, capture=False, timeout=240):
    return subprocess.run(command, check=True, text=True, cwd=ROOT,
                          stdout=subprocess.PIPE if capture else None, timeout=timeout).stdout


def schema_matches(value):
    if value is not None and str(value).isdigit() and int(value) > PLATFORM_SCHEMA:
        raise ValueError(f'cannot deploy schema {PLATFORM_SCHEMA} over schema {value}; binary rollback is unsupported')
    return value == str(PLATFORM_SCHEMA)


def compute_deployments(items):
    return [item for item in items if item['spec']['template']['metadata']
            .get('labels', {}).get('app') in (GATEWAY, WORKER)]


def compute_pods(items):
    return [item for item in items if item['metadata'].get('labels', {}).get('app')
            in (GATEWAY, WORKER) or item['metadata'].get('labels', {})
            .get('duckfleet-compute') == 'isolated']


def check_kube_state(deployments, pods):
    blocked = []
    for deployment in compute_deployments(deployments):
        marker = deployment['spec']['template']['metadata'].get('annotations', {}).get(SCHEMA_MARKER)
        if not schema_matches(marker) and deployment['spec'].get('replicas', 1) != 0:
            blocked.append('deployment/' + deployment['metadata']['name'])
    for pod in compute_pods(pods):
        if not schema_matches(pod['metadata'].get('annotations', {}).get(SCHEMA_MARKER)):
            blocked.append('pod/' + pod['metadata']['name'])
    if blocked:
        raise ValueError('schema-changing upgrade requires stopped compute: ' + ', '.join(blocked)
                         + '; run scripts/stop-compute.sh kube, then retry the deployment')


def kube_objects(kube, kind):
    return json.loads(run([*kube, 'get', kind, '-o', 'json'], capture=True))['items']


def stop_kube(kube, timeout):
    deployments = compute_deployments(kube_objects(kube, 'deployments'))
    # Stop gateways first so none can create another isolated pod during cleanup.
    for role in (GATEWAY, WORKER):
        for item in deployments:
            if item['spec']['template']['metadata']['labels']['app'] == role:
                name = item['metadata']['name']
                print(f'stopping deployment/{name} (replicas={item["spec"].get("replicas", 1)})', flush=True)
                run([*kube, 'patch', 'deployment/' + name, '--type=strategic',
                     '--patch={"spec":{"replicas":0}}'])
        # Also collect standalone/failed compute pods. Respect their normal grace.
        run([*kube, 'delete', 'pods', '-l', 'app=' + role, '--ignore-not-found',
             '--wait=true', '--timeout=' + str(timeout) + 's'], timeout=timeout+30)
    run([*kube, 'delete', 'pods', '-l', 'duckfleet-compute=isolated', '--ignore-not-found',
         '--wait=true', '--timeout=' + str(timeout) + 's'], timeout=timeout+30)
    remaining = compute_deployments(kube_objects(kube, 'deployments'))
    if any(item['spec'].get('replicas', 1) != 0 for item in remaining):
        raise ValueError('a compute controller was restarted during shutdown; compute remains unmanaged, do not migrate')
    if compute_pods(kube_objects(kube, 'pods')):
        raise ValueError('compute pods remain after shutdown; do not migrate')


def compose_containers():
    ids = run(['docker', 'compose', '--profile', '*', 'ps', '--all', '--quiet'], capture=True).split()
    return json.loads(run(['docker', 'inspect', *ids], capture=True)) if ids else []


def container_role(container):
    env = dict(entry.split('=', 1) for entry in container['Config'].get('Env', []) if '=' in entry)
    if env.get('DUCKFLEET_ROLE') == 'gateway':
        return GATEWAY
    if 'DUCKFLEET_WORKER_ID' in env:
        return WORKER
    return None


def check_compose_state(containers):
    for container in containers:
        if not container_role(container) or not container['State']['Running']:
            continue
        marker = (container['Config'].get('Labels') or {}).get(SCHEMA_MARKER)
        if not schema_matches(marker) and container['State']['Running']:
            raise ValueError('schema-changing upgrade requires stopped compute: '
                             + container.get('Name', container['Id'])
                             + '; run scripts/stop-compute.sh compose, then retry')


def stop_compose():
    containers = compose_containers()
    for role in (GATEWAY, WORKER):
        ids = [item['Id'] for item in containers
               if container_role(item) == role and item['State']['Running']]
        if ids:
            # Project-scoped IDs include retired optional service names too.
            run(['docker', 'stop', '-t', '630', *ids])
    if any(container_role(item) and item['State']['Running'] for item in compose_containers()):
        raise ValueError('compute restarted during shutdown; do not migrate')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'stop'))
    parser.add_argument('mode', choices=('compose', 'kube'))
    parser.add_argument('--context', default=os.environ.get('DUCKFLEET_KUBE_CONTEXT'))
    parser.add_argument('--namespace', default=os.environ.get('DUCKFLEET_NAMESPACE'))
    parser.add_argument('--timeout', type=int, default=180)
    args = parser.parse_args()
    if not 1 <= args.timeout <= 1800:
        parser.error('timeout must be 1..1800 seconds')
    try:
        if args.mode == 'compose':
            if args.action == 'stop':
                stop_compose()
            else:
                check_compose_state(compose_containers())
        else:
            if not args.context or not args.namespace:
                parser.error('set an explicit context and namespace')
            kube = ['kubectl', '--context', args.context, '--namespace', args.namespace]
            if args.action == 'stop':
                stop_kube(kube, args.timeout)
            else:
                check_kube_state(kube_objects(kube, 'deployments'), kube_objects(kube, 'pods'))
        print('Compute stopped; PostgreSQL and migration Jobs were preserved. Restart explicitly after migration.'
              if args.action == 'stop' else 'Compute schema-upgrade preflight passed.')
    except (ValueError, KeyError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
