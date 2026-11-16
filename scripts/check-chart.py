"""Validate the rendered, deployable resource boundary. Requires PyYAML."""
import ipaddress
import sys
import yaml

from worker_resources import assert_worker_resources

objects = list(yaml.safe_load_all(open(sys.argv[1])))
by_name = {(o['kind'], o['metadata']['name']): o for o in objects}
for size, cpu in [('small', 1), ('medium', 2)]:
    worker = by_name['Deployment', f'worker-{size}']
    assert worker['spec']['strategy']['type'] == 'Recreate'
    pod = worker['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'duckfleet-worker'
    container = pod['containers'][0]
    # The same rule the deploy verification applies to live pods, so the rendered
    # release and what actually runs cannot disagree about what is legal.
    assert_worker_resources(container['resources'], cpu, f'rendered worker-{size}')
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
isolation = by_name['NetworkPolicy', 'duckfleet-isolation']['spec']
assert isolation['policyTypes'] == ['Ingress', 'Egress']

# Cluster DNS is permitted by selector and by address, because neither form works on
# every topology. Both must stay port 53 only, and neither may reach a public range:
# port 53 to anywhere would let a SQL engine resolve against an external server.
dns_rules = [rule for rule in isolation['egress']
             if any(port.get('port') == 53 for port in rule.get('ports', []))]
assert dns_rules, 'no DNS egress rule; workers cannot resolve the catalog'
for rule in dns_rules:
    assert {port['port'] for port in rule['ports']} == {53}, rule
    for peer in rule['to']:
        cidr = peer.get('ipBlock', {}).get('cidr')
        if cidr is None:
            continue
        network = ipaddress.ip_network(cidr)
        assert network.is_private, f'DNS egress must stay private, got {cidr}'
        assert network.prefixlen >= 8, f'DNS egress range too broad: {cidr}'
assert any('ipBlock' in peer for rule in dns_rules for peer in rule['to']), (
    'DNS must also be reachable by address; a selector matches nothing on EKS Auto Mode'
)
print('PASS: chart resource sizes, identity, isolation, private services, and scoped DNS')
