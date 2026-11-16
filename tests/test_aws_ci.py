"""Safety regressions for the live deployment lane; no AWS calls."""
import importlib.util
import ipaddress
import os
import tempfile
import fnmatch
import subprocess
import re
import unittest
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
IMAGE = ('797781631727.dkr.ecr.us-east-2.amazonaws.com/duckfleet-dev/{kind}@sha256:'
         + 'a'*64)
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT/path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

guard = load('plan_guard', 'scripts/aws/plan_guard.py')
rollout = load('wait_rollout', 'scripts/aws/wait-rollout.py')
dns = load('check_dns_egress', 'scripts/aws/check-dns-egress.py')
resources = load('worker_resources', 'scripts/worker_resources.py')

def pod(name, uid, unschedulable=None):
    conditions = []
    if unschedulable is not None:
        conditions.append({'type': 'PodScheduled', 'status': 'False',
                           'reason': 'Unschedulable', 'message': unschedulable})
    else:
        conditions.append({'type': 'PodScheduled', 'status': 'True'})
    return {'metadata': {'name': name, 'uid': uid}, 'status': {'conditions': conditions}}

def deployment(name, desired, ready):
    return {'metadata': {'name': name}, 'spec': {'replicas': desired},
            'status': {'readyReplicas': ready}}

class AwsCiTests(unittest.TestCase):
    def plan(self, actions, address='aws_s3_bucket.lake'):
        return {'complete': True, 'errored': False, 'resource_changes': [
            {'address': address, 'mode': 'managed', 'change': {'actions': actions}},
        ]}

    def test_safe_changes(self):
        for action in ['no-op', 'create', 'update']:
            self.assertTrue(guard.validate(self.plan([action])))

    def test_destructive_and_foreign_changes(self):
        for actions in [['delete'], ['delete', 'create'], ['create', 'delete'], ['forget']]:
            with self.assertRaises(ValueError): guard.validate(self.plan(actions))
        for address in ['aws_iam_role.admin', 'aws_s3_bucket.other', 'module.er.aws_s3_bucket.lake']:
            with self.assertRaises(ValueError): guard.validate(self.plan(['create'], address))

    def test_incomplete_and_migrations(self):
        for field in ['complete', 'errored']:
            plan = self.plan(['create']); plan[field] = field == 'errored'
            with self.assertRaises(ValueError): guard.validate(plan)
        for field in ['importing', 'previous_address']:
            plan = self.plan(['create'])
            target = plan['resource_changes'][0]
            if field == 'importing': target = target['change']
            target[field] = {'id': 'foreign'} if field == 'importing' else 'old'
            with self.assertRaises(ValueError): guard.validate(plan)

    def test_oidc_does_not_select_sso_profile(self):
        common = ROOT/'scripts/aws/common.sh'
        command = f'source "{common}"; printf "%s" "${{AWS_PROFILE-unset}}"'
        result = subprocess.check_output(['bash', '-c', command], env={'PATH': '/usr/bin:/bin', 'GITHUB_ACTIONS': 'true'}, text=True)
        self.assertEqual(result, 'unset')

    def test_workflow_boundary_and_cleanup(self):
        workflow = yaml.safe_load((ROOT/'.github/workflows/aws-dev.yml').read_text())
        self.assertFalse(workflow['concurrency']['cancel-in-progress'])
        prepare = workflow['jobs']['prepare']
        self.assertEqual(prepare['if'], "github.ref == 'refs/heads/main'")
        self.assertEqual(workflow['jobs']['publish']['needs'], 'prepare')
        self.assertEqual(set(workflow['jobs']['deploy']['needs']), {'prepare', 'publish'})
        self.assertEqual(workflow['jobs']['suspend']['needs'], 'deploy')
        self.assertEqual({item['arch']: item['runner'] for item in workflow['jobs']['publish']['strategy']['matrix']['include']}, {'amd64': 'ubuntu-24.04', 'arm64': 'ubuntu-24.04-arm'})
        steps = prepare['steps']
        checks = next(i for i,s in enumerate(steps) if s.get('run') == 'python3 scripts/aws/wait-checks.py')
        credentials = next(i for i,s in enumerate(steps) if s.get('uses','').startswith('aws-actions/'))
        self.assertLess(checks, credentials)
        self.assertIn('always()', workflow['jobs']['suspend']['if'])
        self.assertIn('deployment_started', workflow['jobs']['suspend']['if'])
        for job in workflow['jobs'].values():
            for step in job['steps']:
                if 'uses' in step:
                    self.assertRegex(step['uses'], r'@[0-9a-f]{40}$')
        paths = workflow[True]['push']['paths']  # YAML 1.1 parses `on` as True.
        for path in ['server/src/main.rs', 'scripts/aws/publish.sh', 'docker/server.Dockerfile', 'infra/helm/duckfleet/values.yaml', '.github/workflows/aws-dev.yml']:
            self.assertTrue(any(fnmatch.fnmatchcase(path, pattern) for pattern in paths), path)
        for path in ['README.md', 'infra/README.md']:
            self.assertFalse(any(fnmatch.fnmatchcase(path, pattern) for pattern in paths), path)
        # PR workflow must never receive AWS identity.
        pr = yaml.safe_load((ROOT/'.github/workflows/infra.yml').read_text())
        self.assertNotIn('id-token', pr['permissions'])

class RolloutWaitTests(unittest.TestCase):
    GRACE = 90

    def evaluate(self, deployments, pods, first_seen, now):
        return rollout.evaluate(deployments, pods, first_seen, now, self.GRACE)

    def test_ready_only_when_every_deployment_has_its_replicas(self):
        ready = [deployment('worker-small', 1, 1), deployment('server', 1, 1)]
        state, _ = self.evaluate(ready, [pod('worker-small-1', 'a')], {}, 0)
        self.assertEqual(state, 'ready')
        state, message = self.evaluate(
            [deployment('worker-small', 1, 0), *ready], [pod('worker-small-1', 'a')], {}, 0)
        self.assertEqual(state, 'waiting')
        self.assertIn('worker-small 0/1 ready', message)

    def test_a_suspended_deployment_counts_as_ready(self):
        suspended = {'metadata': {'name': 'worker-medium'}, 'spec': {'replicas': 0}, 'status': {}}
        state, _ = self.evaluate([suspended], [], {}, 0)
        self.assertEqual(state, 'ready')

    def test_node_provisioning_is_waited_out_not_failed(self):
        # EKS Auto Mode creates nodes on demand, so a briefly unschedulable pod is
        # normal. Failing on first sight would break every cold-node deploy.
        pods = [pod('worker-medium-1', 'm', '0/3 nodes are available: insufficient cpu')]
        first_seen = {}
        state, _ = self.evaluate([deployment('worker-medium', 1, 0)], pods, first_seen, 0)
        self.assertEqual(state, 'waiting')
        state, _ = self.evaluate(
            [deployment('worker-medium', 1, 0)], pods, first_seen, self.GRACE - 1)
        self.assertEqual(state, 'waiting')

    def test_a_pod_held_unschedulable_fails_with_the_scheduler_message(self):
        reason = '0/3 nodes are available: 3 Insufficient cpu'
        pods = [pod('worker-medium-1', 'm', reason)]
        first_seen = {}
        self.evaluate([deployment('worker-medium', 1, 0)], pods, first_seen, 0)
        state, message = self.evaluate(
            [deployment('worker-medium', 1, 0)], pods, first_seen, self.GRACE)
        self.assertEqual(state, 'failed')
        self.assertIn(reason, message)
        self.assertIn('worker-medium-1', message)

    def test_the_grace_window_restarts_when_a_pod_gets_scheduled(self):
        first_seen = {}
        self.evaluate([deployment('worker-medium', 1, 0)],
                      [pod('worker-medium-1', 'm', 'insufficient cpu')], first_seen, 0)
        self.assertIn('m', first_seen)
        # Scheduled at last: the earlier wait must not count toward the next one.
        self.evaluate([deployment('worker-medium', 1, 1)],
                      [pod('worker-medium-1', 'm')], first_seen, 10)
        self.assertNotIn('m', first_seen)
        state, _ = self.evaluate([deployment('worker-medium', 1, 0)],
                                 [pod('worker-medium-1', 'm', 'insufficient cpu')],
                                 first_seen, 11)
        self.assertEqual(state, 'waiting')

    def test_other_scheduling_conditions_are_not_treated_as_unschedulable(self):
        for condition in [
            {'type': 'PodScheduled', 'status': 'True'},
            {'type': 'Ready', 'status': 'False', 'reason': 'Unschedulable'},
            {'type': 'PodScheduled', 'status': 'False', 'reason': 'SchedulingGated'},
        ]:
            observed = {'metadata': {'name': 'p', 'uid': 'u'},
                        'status': {'conditions': [condition]}}
            self.assertIsNone(rollout.unschedulable_reason(observed), str(condition))

    def test_deploy_waits_separately_from_helm(self):
        deploy = (ROOT/'scripts/aws/deploy.sh').read_text()
        # Comments discuss the flag by name, so only executable lines are checked.
        commands = '\n'.join(line for line in deploy.splitlines()
                             if not line.lstrip().startswith('#'))
        # Handing the wait to helm is what produced the opaque deadline error.
        self.assertNotIn('--wait', commands)
        self.assertIn('wait-rollout.py', commands)

    def test_the_deploy_target_is_named_in_one_place(self):
        """Entity-resolution resources may be named only where the target is configured.

        They are still what this deploys to, but the cluster used to be named in a
        script, a workflow three times, a deploy step and a runbook, so moving the
        target meant finding all of them and missing one would have deployed somewhere
        unintended. The state bucket was a second, quieter instance of the same thing.
        """
        # Every entity-resolution resource this repository points at, not just the
        # cluster: an earlier version of this test matched only "er-dev" while listing
        # main.tf as an exception for naming the state bucket, which "er-dev" never
        # matched. The exception was inert and the comment was wrong.
        borrowed = ["er-dev", "er-platform", "er-prod", "er-terraform-state"]
        searched = [path
                    for pattern in ["scripts/**/*", ".github/workflows/*",
                                    "infra/terraform/**/*.tf", "infra/helm/**/*"]
                    for path in ROOT.glob(pattern) if path.is_file()]
        # A gate over an empty set would pass while proving nothing.
        self.assertGreater(len(searched), 20, "nothing was searched")
        offenders = sorted(
            f"{path.relative_to(ROOT)}: {name}"
            for path in searched for name in borrowed
            if path.relative_to(ROOT).as_posix() != "scripts/aws/target.env"
            and name in path.read_text(errors="ignore")
        )
        self.assertEqual(offenders, [], "name the target in scripts/aws/target.env instead")

    def test_terraform_state_location_comes_from_the_target(self):
        """The backend must not pin a bucket the target configuration also names.

        Two declarations plus a test keeping them equal is the shape that produced
        the duplicated resource rule behind #34; one declaration has nothing to drift.
        """
        backend = (ROOT/'infra/terraform/dev/main.tf').read_text()
        backend = backend[backend.index('backend "s3"'):]
        backend = backend[:backend.index('}')]
        # Matched as assignments rather than substrings: `workspace_key_prefix`
        # legitimately stays here and contains "key".
        pinned = re.findall(r"^\s*(bucket|key|region)\s*=", backend, re.M)
        self.assertEqual(pinned, [],
                         "these belong in target.env, supplied via -backend-config")
        apply = (ROOT/'scripts/aws/apply.sh').read_text()
        for supplied in ["bucket=$DUCKFLEET_STATE_BUCKET", "key=$DUCKFLEET_STATE_KEY",
                         "region=$DUCKFLEET_REGION"]:
            self.assertIn(supplied, apply)

    def test_the_state_bootstrap_refuses_a_bucket_we_do_not_own(self):
        """A mistyped override must not apply a bucket policy to someone else's bucket."""
        bootstrap = (ROOT/'scripts/aws/bootstrap-state.sh').read_text()
        self.assertIn('== duckfleet-*', bootstrap)
        # And it must check the account before touching anything.
        self.assertLess(bootstrap.index('get-caller-identity'), bootstrap.index('create-bucket'))

    def test_the_cluster_is_not_applied_by_ci(self):
        """Adding a cluster must not widen what an automated apply can create.

        The cluster needs iam:CreateRole, eks:CreateCluster and ec2:CreateVpc. CI
        applies on every main push and its guard allows the lake bucket and two image
        repositories, so putting the cluster in that module would have traded a
        permanent widening of an automated apply for a one-time creation.
        """
        guard = load('plan_guard', 'scripts/aws/plan_guard.py')
        # The guard CI runs still allows only storage and images.
        self.assertEqual(
            {address.split('.')[0] for address in guard.ALLOWED},
            {'aws_s3_bucket', 'aws_s3_bucket_public_access_block',
             'aws_s3_bucket_ownership_controls',
             'aws_s3_bucket_server_side_encryption_configuration',
             'aws_s3_bucket_versioning', 'aws_s3_bucket_policy',
             'aws_s3_bucket_lifecycle_configuration', 'aws_ecr_repository'},
            'the CI plan guard must not have grown to cover cluster resources')
        # And the cluster lives in a module CI never applies.
        self.assertTrue((ROOT/'infra/terraform/cluster/main.tf').exists())
        apply = (ROOT/'scripts/aws/apply.sh').read_text()
        self.assertNotIn('terraform/cluster', apply)
        workflow = (ROOT/'.github/workflows/aws-dev.yml').read_text()
        self.assertNotIn('terraform/cluster', workflow)

    def test_the_cluster_module_keeps_its_own_state(self):
        """A mistake in an operator-run apply must not touch the state CI writes."""
        target = (ROOT/'scripts/aws/target.env').read_text()
        self.assertIn('DUCKFLEET_CLUSTER_STATE_KEY', target)
        keys = re.findall(r'DUCKFLEET_(?:CLUSTER_)?STATE_KEY=\$\{[^:]+:-([^}]+)\}', target)
        self.assertEqual(len(keys), 2, keys)
        self.assertNotEqual(keys[0], keys[1], 'the two modules must not share a state key')

    def test_verification_counts_only_persistent_workers(self):
        verify = (ROOT/'scripts/aws/verify.py').read_text()
        # A session pod carries `app: duckfleet-worker` so the worker NetworkPolicy
        # grants it S3 egress, which means selecting on that label alone would count
        # a transient isolated pod as part of the persistent tier and then fail
        # looking for a `size` it does not have.
        self.assertIn("'duckfleet-compute' not in", verify)

    def test_a_release_needing_rbac_it_cannot_create_fails_before_helm(self):
        deploy = (ROOT/'scripts/aws/deploy.sh').read_text()
        commands = '\n'.join(line for line in deploy.splitlines()
                             if not line.lstrip().startswith('#'))
        # Enabling isolated compute against an identity that cannot create Roles has
        # to be refused before anything is applied, and has to say why rather than
        # surfacing as a forbidden object partway through a release.
        self.assertIn('can-i create roles.rbac.authorization.k8s.io', commands)
        self.assertIn('duck-fleet#39', deploy)
        gate = commands.index('can-i create roles')
        self.assertLess(gate, commands.index('helm upgrade'),
                        'the RBAC gate must run before the release is applied')


class ImagePublishingTests(unittest.TestCase):
    def test_indexes_require_both_platforms(self):
        values = load('image_values', 'scripts/aws/image_values.py')
        digest = 'sha256:' + 'a'*64
        amd = {'platform': {'os': 'linux', 'architecture': 'amd64'}}
        arm = {'platform': {'os': 'linux', 'architecture': 'arm64'}}
        attestation = {'platform': {'os': 'unknown', 'architecture': 'unknown'}, 'annotations': {'vnd.docker.reference.type': 'attestation-manifest'}}
        reference = values.image_reference('worker', digest, {'manifests': [arm, amd, attestation]})
        self.assertTrue(reference.endswith('/worker@'+digest))
        for manifests in [[], [amd], [amd, amd], [amd, arm, amd], [amd, {}], [amd, arm, {'platform': {'os': 'windows', 'architecture': 'amd64'}}]]:
            with self.assertRaises(ValueError):
                values.image_reference('worker', digest, {'manifests': manifests})
        with self.assertRaises(ValueError):
            values.image_reference('worker', 'latest', {'manifests': [amd, arm]})

    def test_registry_errors_never_become_cache_misses(self):
        with tempfile.TemporaryDirectory() as directory:
            command = Path(directory)/'aws'
            command.write_text("""#!/usr/bin/env python3
import os,sys
mode=os.environ['FIXTURE_MODE']
if mode == 'hit': print('sha256:'+'a'*64)
elif mode == 'invalid': print('None')
else:
    print('ImageNotFoundException' if mode=='missing' else 'AccessDeniedException', file=sys.stderr)
    sys.exit(254)
""")
            command.chmod(0o755)
            env = dict(os.environ, PATH=directory+os.pathsep+os.environ['PATH'], GITHUB_ACTIONS='true')
            script = f'source "{ROOT}/scripts/aws/image-common.sh"; image_digest worker revision'
            for mode, expected in [('hit', 0), ('missing', 1), ('denied', 2), ('invalid', 2)]:
                result = subprocess.run(['bash', '-c', script], env=dict(env, FIXTURE_MODE=mode), capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stderr)
                if mode != 'hit': self.assertEqual(result.stdout, '')

    def test_wrong_native_runner_fails_before_aws(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path/'uname').write_text('#!/bin/sh\nprintf "x86_64\n"\n')
            (path/'aws').write_text('#!/bin/sh\ntouch "$AWS_CALLED"\nexit 99\n')
            for executable in path.iterdir(): executable.chmod(0o755)
            marker = path/'aws-called'
            result = subprocess.run(['bash', str(ROOT/'scripts/aws/publish.sh'), 'arm64'],
                env=dict(os.environ, PATH=directory+os.pathsep+os.environ['PATH'], AWS_CALLED=str(marker)),
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('must match the runner', result.stderr)
            self.assertFalse(marker.exists())

class WorkerResourceRuleTests(unittest.TestCase):
    """One rule, shared by the chart check and the live-pod verification.

    These were separate copies. #22 relaxed `requests == limits` so a class could be
    scheduled on capped capacity and only the chart copy was updated; the live-pod
    copy stayed broken and unnoticed because no deploy had reached it.
    """

    def spec(self, cpu, request=None, memory=None):
        return {'requests': {'cpu': request or str(cpu), 'memory': memory or f'{cpu}Gi'},
                'limits': {'cpu': str(cpu), 'memory': memory or f'{cpu}Gi'}}

    def test_a_request_below_its_limit_is_allowed(self):
        # Exactly what worker-medium deploys as, and why it can be scheduled at all.
        resources.assert_worker_resources(self.spec(2, request='1'), 2, 'test')
        resources.assert_worker_resources(self.spec(2, request='500m'), 2, 'test')
        resources.assert_worker_resources(self.spec(1), 1, 'test')

    def test_a_request_above_its_limit_is_rejected(self):
        for request in ['3', '2500m']:
            with self.assertRaises(AssertionError):
                resources.assert_worker_resources(self.spec(2, request=request), 2, 'test')

    def test_a_zero_request_is_rejected(self):
        with self.assertRaises(AssertionError):
            resources.assert_worker_resources(self.spec(2, request='0'), 2, 'test')

    def test_the_cpu_limit_must_be_exactly_the_class_size(self):
        # The cgroup quota the worker asserts is derived from the limit.
        wrong = self.spec(2)
        wrong['limits']['cpu'] = '4'
        with self.assertRaises(AssertionError):
            resources.assert_worker_resources(wrong, 2, 'test')

    def test_memory_must_match_on_both_because_it_is_not_compressible(self):
        for kind in ['requests', 'limits']:
            wrong = self.spec(2)
            wrong[kind]['memory'] = '1Gi'
            with self.assertRaises(AssertionError):
                resources.assert_worker_resources(wrong, 2, 'test')

    def test_both_callers_use_the_shared_rule(self):
        # Built in pieces so this does not match its own source.
        call = 'assert_worker' + '_resources('
        for script in ['scripts/check-chart.py', 'scripts/aws/verify.py']:
            source = (ROOT/script).read_text()
            self.assertIn(call, source, f'{script} must use the shared rule')
            self.assertNotIn("for kind in ['requests', 'limits']:\n        assert "
                             "container['resources'][kind]['cpu']", source)

class DnsEgressTests(unittest.TestCase):
    """The policy must permit whatever address pods actually resolve against.

    A selector-only rule matched nothing on EKS Auto Mode and denied DNS outright,
    which presented as an unreachable catalog across six deploys.
    """

    def test_resolver_is_the_tenth_address_of_the_service_cidr(self):
        for cidr, expected in [('172.20.0.0/16', '172.20.0.10'),
                               ('10.100.0.0/16', '10.100.0.10')]:
            network = ipaddress.ip_network(cidr)
            self.assertEqual(str(dns.resolver_address(network)), expected)

    def test_the_shipped_ranges_cover_the_dev_cluster_resolver(self):
        ranges = dns.configured_ranges(ROOT/'infra/helm/duckfleet/values.yaml')
        resolver = dns.resolver_address(ipaddress.ip_network('172.20.0.0/16'))
        self.assertTrue(dns.covering_ranges(resolver, ranges),
                        f'{resolver} not covered by {[str(e) for e in ranges]}')

    def test_ranges_that_miss_the_resolver_are_detected(self):
        # The exact mistake that cost six deploys: a rule that cannot reach DNS.
        resolver = dns.resolver_address(ipaddress.ip_network('172.20.0.0/16'))
        near_miss = [ipaddress.ip_network('10.0.0.0/16'),
                     ipaddress.ip_network('172.21.0.0/16')]
        self.assertEqual(dns.covering_ranges(resolver, near_miss), [])

    def test_every_configured_range_stays_private(self):
        # Port 53 to a public range would let a SQL engine resolve externally.
        for entry in dns.configured_ranges(ROOT/'infra/helm/duckfleet/values.yaml'):
            self.assertTrue(entry.is_private, entry)

    def test_missing_configuration_is_rejected_not_defaulted(self):
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory)/'values.yaml'
            empty.write_text('profiling: true\n')
            with self.assertRaises(SystemExit):
                dns.configured_ranges(empty)

    def test_the_policy_permits_dns_by_address_and_only_on_port_53(self):
        rendered = subprocess.run(
            ['helm', 'template', 'duckfleet', str(ROOT/'infra/helm/duckfleet'),
             '--namespace', 'duckfleet-dev',
             '--set', f'workerImage={IMAGE.format(kind="worker")}',
             '--set', f'dbtImage={IMAGE.format(kind="dbt")}'],
            check=True, capture_output=True, text=True).stdout
        isolation = next(doc['spec'] for doc in yaml.safe_load_all(rendered)
                         if doc and doc['kind'] == 'NetworkPolicy'
                         and doc['metadata']['name'] == 'duckfleet-isolation')
        dns_rules = [rule for rule in isolation['egress']
                     if any(p.get('port') == 53 for p in rule.get('ports', []))]
        self.assertTrue(dns_rules)
        blocks = [peer['ipBlock']['cidr'] for rule in dns_rules
                  for peer in rule['to'] if 'ipBlock' in peer]
        self.assertTrue(blocks, 'a selector alone matches nothing on EKS Auto Mode')
        for rule in dns_rules:
            self.assertEqual({p['port'] for p in rule['ports']}, {53}, rule)
        for cidr in blocks:
            self.assertTrue(ipaddress.ip_network(cidr).is_private, cidr)

if __name__ == '__main__':
    unittest.main()
