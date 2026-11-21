"""Validate the rendered, deployable resource boundary. Requires PyYAML."""
import ipaddress
import json
import re
import sys
from pathlib import Path

import yaml

from worker_resources import assert_worker_resources, quantity_bytes

# Read from the server rather than restated here: this is the number the drain has to
# outlast, and a copy of it would be one more thing to keep in step.
MAX_STATEMENT_SECONDS = int(re.search(
    r'!\(1\.\.=(\d+)\)\.contains\(&req\.timeout_seconds\)',
    (Path(__file__).resolve().parents[1] / 'server/src/main.rs').read_text(),
).group(1))

objects = list(yaml.safe_load_all(open(sys.argv[1])))
by_name = {(o['kind'], o['metadata']['name']): o for o in objects}
persistent_workers = {name: value for (kind, name), value in by_name.items()
                      if kind == 'Deployment' and name.startswith('worker-')}
for name, worker in persistent_workers.items():
    assert worker['spec']['strategy']['type'] == 'Recreate'
    pod = worker['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'duckfleet-worker'
    container = pod['containers'][0]
    # The same rule the deploy verification applies to live pods, so the rendered
    # release and what actually runs cannot disagree about what is legal.
    declared = {e['name']: e.get('value') for e in container['env']}
    cpu = int(declared['DUCKFLEET_WORKER_CPU'])
    assert_worker_resources(container['resources'], cpu, f'rendered {name}',
                            memory=declared['DUCKFLEET_WORKER_MEMORY'],
                            disk=declared['DUCKFLEET_WORKER_DISK'])
    assert name == 'worker-' + declared['DUCKFLEET_WORKER_ID']
    assert declared['DUCKFLEET_POOL'] == worker['spec']['template']['metadata']['labels']['duckfleet-pool']
    assert 1 <= int(declared['DUCKFLEET_WORKER_MAX_SESSIONS']) <= 1024
    assert 1 <= int(declared['DUCKFLEET_WORKER_QUEUE_CAPACITY']) <= 4096
    tmp = next(volume['emptyDir'] for volume in pod['volumes'] if volume['name'] == 'tmp')
    assert quantity_bytes(tmp['sizeLimit']) == quantity_bytes(declared['DUCKFLEET_WORKER_DISK'])
    # The worker checks its cgroup against this declaration and refuses to start on a
    # mismatch, so the declaration must match the limits the chart actually renders.
    # Checked here too, because a drift between them would fail at deploy time rather
    # than at render time.
    declared = {e['name']: e.get('value') for e in container['env']}
    assert declared['DUCKFLEET_WORKER_CPU'] == str(cpu), declared['DUCKFLEET_WORKER_CPU']
    assert declared['DUCKFLEET_WORKER_CPU'] == container['resources']['limits']['cpu']
    assert declared['DUCKFLEET_WORKER_MEMORY'] == container['resources']['limits']['memory']
    # Disk is declared-only (no cgroup to check), so it must match the ephemeral-storage
    # request the chart renders, or the worker would trust a size the pod does not have.
    assert declared['DUCKFLEET_WORKER_DISK'] == container['resources']['requests']['ephemeral-storage'], declared.get('DUCKFLEET_WORKER_DISK')
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
        f"{name} drains in {pod['terminationGracePeriodSeconds']}s, which is "
        f"not longer than the {MAX_STATEMENT_SECONDS}s a statement may run")
    # A worker reports not-ready while it waits for the catalog. Waiting longer than
    # the startup probe allows would have the pod replaced mid-wait, which is the
    # crash loop the wait exists to prevent.
    probe = container['startupProbe']
    assert (int(env['DUCKFLEET_CATALOG_WAIT_SECONDS']) + int(env['DUCKFLEET_PLATFORM_WAIT_SECONDS'])
            <= probe['periodSeconds'] * probe['failureThreshold'])
placements = {}
for o in objects:
    # The release contains only what the deploy identity may create. Roles,
    # RoleBindings and ResourceQuotas belong to the ownership boundary an operator
    # applies (infra/kubernetes/boundary) — check-access.sh asserts the deploy cannot
    # create any of the three, so a chart that rendered one could not install itself.
    # That is exactly how isolated compute came to be undeployable, so it is asserted
    # here rather than left to be rediscovered.
    assert o['kind'] not in ['ClusterRole', 'ClusterRoleBinding', 'NodePool',
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
# The invariant is that the two halves agree, not that either is present. Pinning the
# DuckFleet node pool without tolerating its taint renders a release whose every pod is
# permanently Pending; tolerating it without pinning lets a pod land on a neighbour's
# node. Requiring the pin outright would be a different claim — that the ownership
# boundary has already been applied wherever this chart is rendered — which is for
# values to decide, not for this gate.
pinned = 'karpenter.sh/nodepool' in selector
tolerated = tolerations != 'null' and 'duckfleet.io/dedicated' in tolerations
assert pinned == tolerated, (
    f'the DuckFleet node pool is {"pinned" if pinned else "not pinned"} but '
    f'{"tolerated" if tolerated else "not tolerated"}: {selector} / {tolerations}')
# The gateway drains like a worker: a statement it is proxying runs under the same
# 60-second cap, so its grace period is coupled to the same number. Unset it defaults
# to 30, which would SIGKILL the drain partway however well it was written.
#
# Deliberately coupled to the statement cap, NOT to isolatedCompute.readySeconds: a
# cold isolated open can take minutes (node scale-up), and stretching the grace to
# cover it would make every rollout block that long even when idle. An open abandoned
# by a rollout is retry-safe and the client reopens — see the README's isolated
# compute section. Do not "fix" this by raising the grace to readySeconds.
assert by_name['Deployment', 'server']['spec']['template']['spec'][
    'terminationGracePeriodSeconds'] > MAX_STATEMENT_SECONDS, (
    'the gateway grace period is not longer than the longest statement it may proxy')
# The gateway routes to endpoints it was given, so every one must name a Service the
# release actually renders. A URL pointing at nothing would look like a missing
# worker class at request time rather than a packaging mistake at render time.
gateway = next(c for c in by_name['Deployment', 'server']['spec']['template']['spec']['containers']
               if c['name'] == 'gateway')
gateway_env = {entry['name']: entry.get('value') for entry in gateway['env']}
assert 1 <= int(gateway_env['DUCKFLEET_GATEWAY_MAX_SESSIONS']) <= 4096
pool_config = by_name.get(('ConfigMap', 'duckfleet-pools'))
if pool_config:
    assert 'DUCKFLEET_WORKER_URLS' not in gateway_env
    assert gateway_env.get('DUCKFLEET_POOLS_FILE') == '/etc/duckfleet-pools/pools.json'
    config = json.loads(pool_config['data']['pools.json'])
    assert config['default_pool'] in config['pools']
    worker_urls = []
    for pool_name, pool in config['pools'].items():
        for url in pool['endpoints']:
            host = url.removeprefix('http://').split(':')[0]
            assert host in persistent_workers, f'pool {pool_name} points at missing worker {host}'
            container = persistent_workers[host]['spec']['template']['spec']['containers'][0]
            declared = {entry['name']: entry.get('value') for entry in container['env']}
            assert declared['DUCKFLEET_POOL'] == pool_name
            assert int(declared['DUCKFLEET_WORKER_CPU']) == pool['cpu']
            assert quantity_bytes(declared['DUCKFLEET_WORKER_MEMORY']) == quantity_bytes(pool['memory'])
            assert quantity_bytes(declared['DUCKFLEET_WORKER_DISK']) == quantity_bytes(pool['disk'])
            assert int(declared['DUCKFLEET_WORKER_MAX_SESSIONS']) == pool['max_sessions_per_worker']
            worker_urls.append(url)
    mounts = {mount['name']: mount for mount in gateway['volumeMounts']}
    assert mounts['persistent-pools']['readOnly'] and 'subPath' not in mounts['persistent-pools']
else:
    assert 'DUCKFLEET_POOLS_FILE' not in gateway_env
    worker_urls = gateway_env['DUCKFLEET_WORKER_URLS'].split(',')
    assert worker_urls and all(worker_urls), 'the legacy gateway must be given worker endpoints'
services = {name for kind, name in by_name if kind == 'Service'}
for url in worker_urls:
    host = url.removeprefix('http://').split(':')[0]
    assert host in services, f'gateway points at {host}, which this release does not render'
    assert ('Deployment', host) in by_name, f'no Deployment backs {host}'
assert len(set(worker_urls)) == len(worker_urls) == len(persistent_workers)
# One rate card per deployment. Every container that prices or reports work must
# carry the same pair, or admission and the budget position would disagree about
# what a unit-hour costs.
def rate_pair(env_list):
    env = {e['name']: e.get('value') for e in env_list}
    return (env.get('DUCKFLEET_RATE_CARD_VERSION'),
            env.get('DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR'))
cards = {'server': rate_pair(gateway['env'])}
for kind, name in by_name:
    if kind == 'Deployment' and name.startswith('worker-'):
        cards[name] = rate_pair(
            by_name[kind, name]['spec']['template']['spec']['containers'][0]['env'])
if ('ConfigMap', 'duckfleet-isolated-pod') in by_name:
    session_pod = json.loads(by_name['ConfigMap', 'duckfleet-isolated-pod']['data']['pod.json'])
    cards['isolated-pod'] = rate_pair(session_pod['spec']['containers'][0]['env'])
assert len(set(cards.values())) == 1, f'rate cards disagree: {cards}'
rate_version, rate_value = next(iter(cards.values()))
assert rate_version and rate_value and rate_value.isdigit() and int(rate_value) >= 1, (
    f'the rate card must be a named version and a positive integer rate: '
    f'{rate_version!r} {rate_value!r}')
# The wire front is opt-in: a default render neither listens for it nor
# advertises it, and an enabled one must do both.
server_ports = {p['port'] for p in by_name['Service', 'server']['spec']['ports']}
gateway_env = {e['name']: e.get('value') for e in gateway['env']}
if gateway_env.get('DUCKFLEET_PGWIRE') == 'true':
    assert 5432 in server_ports, 'pgwire is enabled but the Service does not carry 5432'
else:
    assert 5432 not in server_ports, 'the Service advertises a wire front nothing listens for'
# Migration is an ordinary, bounded, minimally privileged Job. Its desired name
# is explicit on the gateway so a completed Job from an older image/database run
# can never satisfy the rollout. Suspended releases do not start database work.
server_deployment = by_name['Deployment', 'server']
migration_name = server_deployment['metadata'].get('annotations', {}).get('duckfleet.io/migration-job')
migrations = [o for o in objects if o['kind'] == 'Job'
              and o['metadata'].get('labels', {}).get('app') == 'duckfleet-platform-migrate']
if server_deployment['spec']['replicas'] == 0:
    assert not migration_name and not migrations, 'a suspended release must not migrate'
else:
    assert len(migrations) == 1 and migrations[0]['metadata']['name'] == migration_name
    migration = migrations[0]
    assert 'helm.sh/hook' not in migration['metadata'].get('annotations', {}), 'migration hooks can deadlock fresh installs'
    pod = migration['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'duckfleet-no-aws'
    assert pod['automountServiceAccountToken'] is False
    assert pod['restartPolicy'] == 'Never' and migration['spec']['backoffLimit'] == 0
    migrate = pod['containers'][0]
    assert migrate['image'] == gateway['image']
    env = {item['name']: item.get('value') for item in migrate['env']}
    assert set(env) == {'DUCKFLEET_ROLE', 'PLATFORM_DSN', 'DUCKFLEET_PLATFORM_WAIT_SECONDS',
                        'DUCKFLEET_MIGRATION_TIMEOUT_SECONDS', 'DUCKFLEET_PREPAID_MICRO_USD',
                        'DUCKFLEET_MAX_JOB_MICRO_USD'}, env
    assert env['DUCKFLEET_ROLE'] == 'migrate'
    # Helm may stringify large numeric YAML values in scientific notation.
    # Migrations parse whole micro-USD, and the Kind YAML round trip must retain
    # string-valued Kubernetes environment entries rather than numbers.
    for name in ('DUCKFLEET_PREPAID_MICRO_USD', 'DUCKFLEET_MAX_JOB_MICRO_USD'):
        assert isinstance(env[name], str) and env[name].isascii() and env[name].isdigit(), env
        assert int(env[name]) > 0, f'{name} must be a positive whole number'
    assert migration['spec']['activeDeadlineSeconds'] >= (
        int(env['DUCKFLEET_PLATFORM_WAIT_SECONDS']) + int(env['DUCKFLEET_MIGRATION_TIMEOUT_SECONDS']))
    assert not {'startupProbe', 'readinessProbe', 'livenessProbe'} & set(migrate)
assert 'DUCKFLEET_HISTORY_RETENTION_DAYS' in gateway_env
for kind, name in by_name:
    if kind == 'Deployment' and name.startswith('worker-'):
        env = {item['name']: item.get('value') for item in
               by_name[kind, name]['spec']['template']['spec']['containers'][0]['env']}
        assert 'DUCKFLEET_HISTORY_RETENTION_DAYS' not in env, 'retention belongs to gateway maintenance'
        assert env['DUCKFLEET_PLATFORM_WAIT_SECONDS'] == gateway_env['DUCKFLEET_PLATFORM_WAIT_SECONDS']
        assert int(env['DUCKFLEET_PLATFORM_WAIT_SECONDS']) <= 600

# The catalog is either bundled or the operator's, never a partial mix: a render
# carrying the StatefulSet without its Service or init would start a database
# nothing can reach or seed. Whether named external addresses reach the egress
# policy is proven in check-infra, which knows the values the render was given.
bundled_parts = [('StatefulSet', 'duckfleet-postgres'), ('Service', 'postgres'),
                 ('ConfigMap', 'duckfleet-postgres-init')]
present = [part for part in bundled_parts if part in by_name]
assert present in ([], bundled_parts), f'a partial bundled postgres: {present}'
# Exposure is opt-in. A default render stays ClusterIP-only with no Ingress at all;
# an exposed render must terminate TLS on every host it serves, refuse plain HTTP,
# point only at the gateway, and come with the NetworkPolicy door the controller
# needs — the isolation policy admits only this namespace's pods, so an Ingress
# without it would be a front door onto a wall.
ingresses = [o for o in objects if o['kind'] == 'Ingress']
exposure_policy = by_name.get(('NetworkPolicy', 'duckfleet-exposure'))
if not ingresses:
    assert exposure_policy is None, (
        'an exposure NetworkPolicy without an Ingress admits traffic nothing serves')
else:
    assert len(ingresses) == 1, 'one exposed host, one Ingress'
    ingress = ingresses[0]
    assert exposure_policy is not None, (
        'an Ingress without duckfleet-exposure cannot reach the gateway')
    tls = ingress['spec'].get('tls') or []
    tls_hosts = {host for block in tls for host in block.get('hosts', [])}
    assert all(block.get('secretName') for block in tls), 'TLS without a certificate secret'
    rule_hosts = {rule['host'] for rule in ingress['spec']['rules']}
    assert rule_hosts and rule_hosts <= tls_hosts, (
        f'every exposed host must terminate TLS: {rule_hosts} vs {tls_hosts}')
    assert ingress['metadata']['annotations'].get(
        'nginx.ingress.kubernetes.io/ssl-redirect') == 'true', (
        'the plain-HTTP redirect annotation is the render\'s share of "never over HTTP"')
    for rule in ingress['spec']['rules']:
        for path in rule['http']['paths']:
            service = path['backend']['service']
            assert service['name'] == 'server' and service['port']['number'] == 8080, (
                'exposure may point only at the gateway')
    admitted = exposure_policy['spec']['ingress'][0]['from']
    assert all('namespaceSelector' in source for source in admitted), (
        'the exposure door admits namespaces an operator named, never addresses')

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
    assert {volume['name'] for volume in gateway_pod.get('volumes', [])} <= {'persistent-pools'}
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
    persistent = (next(iter(persistent_workers.values())) if persistent_workers else
                  by_name['Deployment', 'server'])['spec']['template']['spec']
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
    # The gateway rewrites these four per session and refuses to start a pod without
    # them, so the template has to keep carrying them.
    declared = {e['name']: e.get('value') for e in container['env']}
    for variable in ['DUCKFLEET_WORKER_ID', 'DUCKFLEET_WORKER_CPU', 'DUCKFLEET_WORKER_MEMORY', 'DUCKFLEET_WORKER_DISK']:
        assert variable in declared, variable
    # Without this the pod would admit shared sessions and report an isolated
    # placement as persistent in history.
    assert declared['DUCKFLEET_ISOLATED'] == 'true', declared.get('DUCKFLEET_ISOLATED')
    assert 'DUCKFLEET_POOL' not in declared, 'isolated pods cannot inherit a persistent pool'
    assert declared['DUCKFLEET_WORKER_MAX_SESSIONS'] == '1'
    # Rendered from the shared helper, so a session pod reaches the same catalog and
    # bucket as a persistent worker rather than a separately maintained guess.
    assert declared['S3_AUTH_MODE'] == 'pod_identity'
    assert declared['AWS_EC2_METADATA_DISABLED'] == 'true'
    assert 'S3_SECRET_KEY' not in declared
    secrets = {e['name'] for e in container['env'] if 'valueFrom' in e}
    assert {'CATALOG_DSN', 'PLATFORM_DSN', 'DUCKFLEET_TOKEN'} <= secrets, secrets

print('PASS: chart resource sizes, identity, isolation, private services, and scoped DNS')
