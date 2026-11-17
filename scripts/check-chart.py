"""Validate the rendered, deployable resource boundary. Requires PyYAML."""
import ipaddress
import json
import re
import sys
from pathlib import Path

import yaml

from worker_resources import assert_worker_resources

# Read from the server rather than restated here: this is the number the drain has to
# outlast, and a copy of it would be one more thing to keep in step.
MAX_STATEMENT_SECONDS = int(re.search(
    r'!\(1\.\.=(\d+)\)\.contains\(&req\.timeout_seconds\)',
    (Path(__file__).resolve().parents[1] / 'server/src/main.rs').read_text(),
).group(1))

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
    # The worker checks its cgroup against this declaration and refuses to start on a
    # mismatch, so the declaration must match the limits the chart actually renders.
    # Checked here too, because a drift between them would fail at deploy time rather
    # than at render time.
    declared = {e['name']: e.get('value') for e in container['env']}
    assert declared['DUCKFLEET_WORKER_CPU'] == str(cpu), declared['DUCKFLEET_WORKER_CPU']
    assert declared['DUCKFLEET_WORKER_MEMORY'] == f'{cpu}Gi', declared['DUCKFLEET_WORKER_MEMORY']
    assert declared['DUCKFLEET_WORKER_CPU'] == container['resources']['limits']['cpu']
    assert declared['DUCKFLEET_WORKER_MEMORY'] == container['resources']['limits']['memory']
    env = {e['name']:e.get('value') for e in container['env']}
    assert env['S3_AUTH_MODE'] == 'pod_identity'
    assert env['AWS_EC2_METADATA_DISABLED'] == 'true'
    assert 'S3_SECRET_KEY' not in env
    # A persistent worker is shared compute, so it must not claim to be a dedicated
    # pod: that claim is what lets a worker admit an isolated session at all.
    assert 'DUCKFLEET_ISOLATED' not in env
    # A drain has to outlast the longest statement a caller may ask for. The API caps
    # timeout_seconds at 60; unset, Kubernetes defaults this to 30, so a query in its
    # second half could never finish however well the drain was written. The two are
    # coupled, so changing either must fail here rather than quietly truncate drains.
    assert pod['terminationGracePeriodSeconds'] > MAX_STATEMENT_SECONDS, (
        f"worker-{size} drains in {pod['terminationGracePeriodSeconds']}s, which is "
        f"not longer than the {MAX_STATEMENT_SECONDS}s a statement may run")
    # A worker reports not-ready while it waits for the catalog. Waiting longer than
    # the startup probe allows would have the pod replaced mid-wait, which is the
    # crash loop the wait exists to prevent.
    probe = container['startupProbe']
    assert int(env['DUCKFLEET_CATALOG_WAIT_SECONDS']) <= probe['periodSeconds'] * probe['failureThreshold']
placements = {}
for o in objects:
    # The release contains only what the deploy identity may create. Roles,
    # RoleBindings and ResourceQuotas belong to the ownership boundary an operator
    # applies (infra/kubernetes/boundary) — check-access.sh asserts the deploy cannot
    # create any of the three, so a chart that rendered one could not install itself.
    # That is exactly how isolated compute came to be undeployable, so it is asserted
    # here rather than left to be rediscovered.
    assert o['kind'] not in ['Ingress', 'ClusterRole', 'ClusterRoleBinding', 'NodePool',
                             'Role', 'RoleBinding', 'ResourceQuota'], (
        f"{o['kind']}/{o['metadata']['name']} is a boundary object, not a release "
        'object; move it to infra/kubernetes/boundary')
    if o['kind'] == 'Service':
        assert o['spec'].get('type', 'ClusterIP') == 'ClusterIP'
    if o['kind'] in ['Deployment', 'StatefulSet', 'Job']:
        pod = o['spec']['template']['spec']
        assert pod['securityContext']['runAsNonRoot']
        placements[o['metadata']['name']] = (
            json.dumps(pod['nodeSelector'], sort_keys=True),
            json.dumps(pod.get('tolerations'), sort_keys=True))
        for c in pod['containers']+pod.get('initContainers', []):
            assert not c['securityContext']['allowPrivilegeEscalation']
            assert c['securityContext']['capabilities']['drop'] == ['ALL']
            assert c['resources']['requests'] and c['resources']['limits']

# One placement for the whole release, because DuckFleet runs on capacity it owns and
# every pod has to reach it. The two halves only work together — the selector names the
# tainted node pool and the toleration is what gets a pod onto it — so a pod rendered
# with one and not the other either never schedules or lands on a neighbour's node.
# Emitted from `duckfleet.placement`; asserted here so splitting that helper back apart
# fails at render time.
assert len(set(placements.values())) == 1, (
    f'pods disagree about where they run: {placements}')
selector, tolerations = next(iter(placements.values()))
assert 'karpenter.sh/nodepool' in selector, selector
assert tolerations != 'null' and 'duckfleet.io/dedicated' in tolerations, tolerations
# The gateway routes to endpoints it was given, so every one must name a Service the
# release actually renders. A URL pointing at nothing would look like a missing
# worker class at request time rather than a packaging mistake at render time.
gateway = next(c for c in by_name['Deployment', 'server']['spec']['template']['spec']['containers']
               if c['name'] == 'gateway')
worker_urls = {e['name']: e.get('value') for e in gateway['env']}['DUCKFLEET_WORKER_URLS']
services = {name for kind, name in by_name if kind == 'Service'}
assert worker_urls, 'the gateway must be given worker endpoints'
for url in worker_urls.split(','):
    host = url.removeprefix('http://').split(':')[0]
    assert host in services, f'gateway points at {host}, which this release does not render'
    assert ('Deployment', host) in by_name, f'no Deployment backs {host}'
assert len(worker_urls.split(',')) == len([n for k, n in by_name
                                           if k == 'Deployment' and n.startswith('worker-')])

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

# On-demand compute needs the gateway to hold a Kubernetes token, so the parts of it
# the release owns render together or not at all. The Role that makes that token useful
# is not among them: the deploy identity cannot create Roles, so it belongs to the
# ownership boundary an operator applies (#39), where tests/test_aws_ci.py asserts its
# shape. A release with the token but not the egress rule would still fail somewhere
# less obvious than here.
ISOLATED = [('NetworkPolicy', 'duckfleet-gateway-kubernetes'),
            ('ConfigMap', 'duckfleet-isolated-pod')]
present = [key for key in ISOLATED if key in by_name]
assert not present or len(present) == len(ISOLATED), (
    f'isolated compute is half-rendered: {present}')

# Rendered whether or not isolation is wired up, so the gateway pod can always start:
# a ServiceAccount the Deployment named but nothing created leaves the pod unable to
# mount its token. Unbound, it grants nothing.
assert by_name['ServiceAccount', 'duckfleet-gateway']['automountServiceAccountToken'] is True
# Only the gateway holds one. A worker runs user SQL, so a token there would let a
# query reach the API server.
for account in ['duckfleet-worker', 'duckfleet-no-aws']:
    assert by_name['ServiceAccount', account]['automountServiceAccountToken'] is False

gateway_pod = by_name['Deployment', 'server']['spec']['template']['spec']
if not present:
    # The default: the gateway runs as the identity that mounts no token at all, so it
    # cannot reach the API server whatever RBAC exists, and `compute: isolated` is
    # refused rather than served from a shared worker.
    assert gateway_pod['serviceAccountName'] == 'duckfleet-no-aws'
    assert not gateway_pod.get('volumes'), gateway_pod.get('volumes')
else:
    assert gateway_pod['serviceAccountName'] == 'duckfleet-gateway'

    # Reaching the API server is a new egress edge. It stays the gateway's alone, on
    # HTTPS, to declared private addresses: a SQL engine that could reach port 443
    # anywhere is a data path out.
    api = by_name['NetworkPolicy', 'duckfleet-gateway-kubernetes']['spec']
    assert api['podSelector']['matchLabels'] == {'app': 'duckfleet-gateway'}
    assert api['policyTypes'] == ['Egress']
    for rule in api['egress']:
        # A port must be named, and only ports an API server actually listens on. An
        # empty list would render a rule matching every port; 443 is the Service port
        # and 6443 the usual endpoint port, and which one applies depends on the
        # cluster because egress is matched after the destination is rewritten (#32).
        declared = {port['port'] for port in rule['ports']}
        assert declared and declared <= {443, 6443}, rule
        for peer in rule['to']:
            network = ipaddress.ip_network(peer['ipBlock']['cidr'])
            assert network.is_private, f'API egress must stay private, got {network}'
            assert network.prefixlen >= 8, f'API egress range too broad: {network}'

    # The pod the gateway creates. Checked against the persistent worker rather than
    # against a copy of its values, so the two cannot drift: an isolated pod is the
    # same image under the same security rules, differing only in how it is sized and
    # how long it lives.
    template = json.loads(by_name['ConfigMap', 'duckfleet-isolated-pod']['data']['pod.json'])
    persistent = by_name['Deployment', 'worker-small']['spec']['template']['spec']
    isolated, container = template['spec'], template['spec']['containers'][0]
    assert isolated['securityContext'] == persistent['securityContext']
    assert container['securityContext'] == persistent['containers'][0]['securityContext']
    assert container['image'] == persistent['containers'][0]['image']
    # Placement, both halves. A session pod that selected the DuckFleet node pool
    # without tolerating its taint would never schedule, and one that tolerated it
    # without selecting it could land on a neighbour's node — so these are compared
    # against the shared tier rather than against a copy of the values.
    assert isolated['nodeSelector'] == persistent['nodeSelector']
    assert isolated.get('tolerations') == persistent.get('tolerations')
    assert isolated['serviceAccountName'] == 'duckfleet-worker'
    # Both labels are load-bearing. `app` is what the worker NetworkPolicy selects,
    # so without it a session pod gets no S3 egress; the other is what a startup
    # sweep selects on, so without it an abandoned pod is invisible.
    assert template['metadata']['labels']['app'] == 'duckfleet-worker'
    assert template['metadata']['labels']['duckfleet-compute'] == 'isolated'
    # A session pod runs user SQL and must not inherit the gateway's new powers.
    assert isolated['automountServiceAccountToken'] is False
    # Never restarted and never immortal: a restart would reattach the catalog under
    # an identity whose session the gateway has forgotten, and the deadline is the
    # backstop that bounds a leak if every other layer fails.
    assert isolated['restartPolicy'] == 'Never'
    assert isolated['activeDeadlineSeconds'] >= 60
    # Requests equal limits for what the caller asked for: a dedicated pod has no
    # neighbour to borrow headroom from. The placeholder sizes here are replaced per
    # session, so only their equality is meaningful.
    for quantity in ['cpu', 'memory']:
        assert container['resources']['requests'][quantity] == \
               container['resources']['limits'][quantity], quantity
    # The gateway rewrites these three per session and refuses to start a pod without
    # them, so the template has to keep carrying them.
    declared = {e['name']: e.get('value') for e in container['env']}
    for variable in ['DUCKFLEET_WORKER_ID', 'DUCKFLEET_WORKER_CPU', 'DUCKFLEET_WORKER_MEMORY']:
        assert variable in declared, variable
    # Without this the pod would admit shared sessions and report an isolated
    # placement as persistent in history.
    assert declared['DUCKFLEET_ISOLATED'] == 'true', declared.get('DUCKFLEET_ISOLATED')
    # Rendered from the shared helper, so a session pod reaches the same catalog and
    # bucket as a persistent worker rather than a separately maintained guess.
    assert declared['S3_AUTH_MODE'] == 'pod_identity'
    assert declared['AWS_EC2_METADATA_DISABLED'] == 'true'
    assert 'S3_SECRET_KEY' not in declared
    secrets = {e['name'] for e in container['env'] if 'valueFrom' in e}
    assert {'CATALOG_DSN', 'PLATFORM_DSN', 'DUCKFLEET_TOKEN'} <= secrets, secrets

print('PASS: chart resource sizes, identity, isolation, private services, and scoped DNS')
