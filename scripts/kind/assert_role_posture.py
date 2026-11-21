"""Check live compute Pods mount only their component credentials; never print values."""
import argparse
import json
import subprocess


def validate(pods):
    counts = {'worker': 0, 'gateway': 0, 'migrate': 0}
    roles = {'duckfleet-worker': 'worker', 'duckfleet-gateway': 'gateway',
             'duckfleet-platform-migrate': 'migrate'}
    for pod in pods:
        role = roles.get(pod.get('metadata', {}).get('labels', {}).get('app'))
        if role is None:
            continue
        spec = pod['spec']
        assert not any('secret' in volume for volume in spec.get('volumes', [])), 'whole-Secret mount in compute'
        for container in spec['containers']:
            assert not container.get('envFrom'), 'whole-Secret import in compute'
            env = {item['name']: item for item in container.get('env', [])}
            keys = {name: item['valueFrom']['secretKeyRef']['key'] for name,item in env.items()
                    if 'secretKeyRef' in item.get('valueFrom', {})}
            if role == 'worker':
                assert 'DUCKFLEET_TOKEN' not in env and 'DUCKFLEET_MAINTENANCE_DSN' not in env
                assert keys == {'PLATFORM_DSN': 'PLATFORM_WORKER_DSN',
                                'CATALOG_DSN': 'CATALOG_DSN',
                                'DUCKFLEET_INTERNAL_TOKEN': 'DUCKFLEET_INTERNAL_TOKEN'}, 'worker credential set differs'
                assert not spec.get('automountServiceAccountToken', False), 'SQL worker has API token'
            elif role == 'gateway':
                assert keys == {'PLATFORM_DSN': 'PLATFORM_GATEWAY_DSN',
                                'DUCKFLEET_MAINTENANCE_DSN': 'PLATFORM_MAINTENANCE_DSN',
                                'DUCKFLEET_TOKEN': 'DUCKFLEET_TOKEN',
                                'DUCKFLEET_INTERNAL_TOKEN': 'DUCKFLEET_INTERNAL_TOKEN'}, 'gateway credential set differs'
            else:
                assert keys == {'PLATFORM_DSN': 'PLATFORM_MIGRATION_DSN'}, 'migration carries runtime secrets'
        counts[role] += 1
    assert counts['worker'] and counts['gateway'], 'expected live compute Pods'
    return counts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True)
    parser.add_argument('--namespace', required=True)
    args = parser.parse_args()
    pods = json.loads(subprocess.check_output(['kubectl','--context',args.context,'-n',args.namespace,
                                               'get','pods','-o','json'],text=True))['items']
    print(json.dumps({'checked_component_pods': validate(pods)}))
