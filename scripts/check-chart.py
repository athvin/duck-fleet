"""Validate the rendered, deployable resource boundary. Requires PyYAML."""
import sys
import yaml

objects = list(yaml.safe_load_all(open(sys.argv[1])))
by_name = {(o['kind'], o['metadata']['name']): o for o in objects}
for size, cpu in [('small', 1), ('medium', 2)]:
    worker = by_name['Deployment', f'worker-{size}']
    assert worker['spec']['strategy']['type'] == 'Recreate'
    pod = worker['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'duckfleet-worker'
    container = pod['containers'][0]
    for key in ['requests', 'limits']:
        assert container['resources'][key]['cpu'] == str(cpu)
        assert container['resources'][key]['memory'] == f'{cpu}Gi'
    env = {e['name']:e.get('value') for e in container['env']}
    assert env['S3_AUTH_MODE'] == 'pod_identity'
    assert env['AWS_EC2_METADATA_DISABLED'] == 'true'
    assert 'S3_SECRET_KEY' not in env
for o in objects:
    assert o['kind'] not in ['Ingress', 'ClusterRole', 'ClusterRoleBinding', 'NodePool']
    if o['kind'] == 'Service':
        assert o['spec'].get('type', 'ClusterIP') == 'ClusterIP'
    if o['kind'] in ['Deployment', 'StatefulSet', 'Job']:
        pod = o['spec']['template']['spec']
        assert pod['securityContext']['runAsNonRoot']
        for c in pod['containers']+pod.get('initContainers', []):
            assert not c['securityContext']['allowPrivilegeEscalation']
            assert c['securityContext']['capabilities']['drop'] == ['ALL']
            assert c['resources']['requests'] and c['resources']['limits']
assert by_name['NetworkPolicy', 'duckfleet-isolation']['spec']['policyTypes'] == ['Ingress', 'Egress']
print('PASS: chart resource sizes, identity, isolation, and private services')
