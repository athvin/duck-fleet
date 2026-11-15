"""Validate the rendered, deployable resource boundary. Requires PyYAML."""
import sys
import yaml

def cores(quantity):
    """Kubernetes CPU quantity in cores; 'm' is the only suffix Kubernetes allows."""
    return float(quantity[:-1]) / 1000 if quantity.endswith('m') else float(quantity)

objects = list(yaml.safe_load_all(open(sys.argv[1])))
by_name = {(o['kind'], o['metadata']['name']): o for o in objects}
for size, cpu in [('small', 1), ('medium', 2)]:
    worker = by_name['Deployment', f'worker-{size}']
    assert worker['spec']['strategy']['type'] == 'Recreate'
    pod = worker['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'duckfleet-worker'
    container = pod['containers'][0]
    # The worker derives its DuckDB thread count and memory limit from the cgroup,
    # which reflects the *limits*. Those must be exactly the class size or the worker
    # refuses to start.
    assert container['resources']['limits']['cpu'] == str(cpu)
    for key in ['requests', 'limits']:
        assert container['resources'][key]['memory'] == f'{cpu}Gi'
    # A CPU request below the limit is allowed so a class can fit constrained
    # capacity, but never above it and never zero: that would let a worker be
    # scheduled onto a node that cannot give it the CPU its limit promises.
    assert 0 < cores(container['resources']['requests']['cpu']) <= cpu
    env = {e['name']:e.get('value') for e in container['env']}
    assert env['S3_AUTH_MODE'] == 'pod_identity'
    assert env['AWS_EC2_METADATA_DISABLED'] == 'true'
    assert 'S3_SECRET_KEY' not in env
    # A worker reports not-ready while it waits for the catalog. Waiting longer than
    # the startup probe allows would have the pod replaced mid-wait, which is the
    # crash loop the wait exists to prevent.
    probe = container['startupProbe']
    assert int(env['DUCKFLEET_CATALOG_WAIT_SECONDS']) <= probe['periodSeconds'] * probe['failureThreshold']
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
