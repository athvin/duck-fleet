"""Safety regressions for the live deployment lane; no AWS calls."""
import importlib.util
import ipaddress
import json
import os
import tempfile
import fnmatch
import subprocess
import re
import unittest
from unittest import mock
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
lifecycle = load('compute_lifecycle', 'scripts/compute_lifecycle.py')

def pod(name, uid, unschedulable=None):
    conditions = []
    if unschedulable is not None:
        conditions.append({'type': 'PodScheduled', 'status': 'False',
                           'reason': 'Unschedulable', 'message': unschedulable})
    else:
        conditions.append({'type': 'PodScheduled', 'status': 'True'})
    return {'metadata': {'name': name, 'uid': uid}, 'status': {'conditions': conditions}}

def chart(extra=()):
    """The release with isolated compute on, which is the fullest render."""
    return subprocess.run(
        ['helm', 'template', 'duckfleet', str(ROOT/'infra/helm/duckfleet'),
         '--namespace', 'duckfleet-dev',
         '--set', f'workerImage={IMAGE.format(kind="worker")}',
         '--set', f'dbtImage={IMAGE.format(kind="dbt")}',
         '--set', 'isolatedCompute.enabled=true',
         '--set', 'isolatedCompute.apiServerCidrs[0]=10.96.0.1/32', *extra],
        check=True, capture_output=True, text=True).stdout


def deployment(name, desired, ready):
    return {'metadata': {'name': name}, 'spec': {'replicas': desired},
            'status': {'readyReplicas': ready, 'updatedReplicas': ready, 'replicas': desired}}

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
        for address in ['aws_iam_role.admin', 'aws_s3_bucket.other', 'module.unrelated.aws_s3_bucket.lake']:
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

    def test_old_ready_replicas_do_not_complete_a_new_rollout(self):
        observed = deployment('server', 1, 1)
        observed['metadata']['generation'] = 3
        observed['status']['observedGeneration'] = 2
        self.assertEqual(self.evaluate([observed], [], {}, 0)[0], 'waiting')
        observed['status']['observedGeneration'] = 3
        observed['status']['updatedReplicas'] = 0
        self.assertEqual(self.evaluate([observed], [], {}, 0)[0], 'waiting')
        observed['status']['updatedReplicas'] = 1
        self.assertEqual(self.evaluate([observed], [], {}, 0)[0], 'ready')

    def test_only_the_current_migration_can_complete_the_rollout(self):
        server = deployment('server', 1, 1)
        server['metadata']['annotations'] = {'duckfleet.io/migration-job': 'current'}
        old = {'metadata': {'name': 'old'}, 'status': {'conditions': [
            {'type': 'Complete', 'status': 'True'}]}}
        current = {'metadata': {'name': 'current'}, 'status': {}}
        for jobs in [[], [old], [old, current]]:
            state, message = rollout.evaluate([server], [], {}, 0, self.GRACE, jobs=jobs)
            self.assertEqual(state, 'waiting')
            self.assertIn('current', message)
        current['status']['conditions'] = [{'type': 'Complete', 'status': 'True'}]
        self.assertEqual(rollout.evaluate([server], [], {}, 0, self.GRACE,
                                         jobs=[old, current])[0], 'ready')
        current['status']['conditions'] = [
            {'type': 'Failed', 'status': 'True', 'reason': 'DeadlineExceeded'}]
        state, message = rollout.evaluate([server], [], {}, 0, self.GRACE, jobs=[current])
        self.assertEqual(state, 'failed')
        self.assertIn('DeadlineExceeded', message)
        server['spec']['replicas'] = 0
        server['status'] = {}
        self.assertEqual(rollout.evaluate([server], [], {}, 0, self.GRACE,
                                         jobs=[current])[0], 'ready')

    def test_bundled_postgres_must_be_ready(self):
        database = {'metadata': {'name': 'duckfleet-postgres'},
                    'spec': {'replicas': 1}, 'status': {'readyReplicas': 0}}
        self.assertEqual(rollout.evaluate([deployment('server', 1, 1)], [], {}, 0,
                                         self.GRACE, statefulsets=[database])[0], 'waiting')
        database['status']['readyReplicas'] = 1
        self.assertEqual(rollout.evaluate([deployment('server', 1, 1)], [], {}, 0,
                                         self.GRACE, statefulsets=[database])[0], 'ready')

    def test_migration_pruning_preserves_current_pending_running_and_failed_jobs(self):
        server = deployment('server', 1, 1)
        server['metadata']['annotations'] = {'duckfleet.io/migration-job': 'current'}
        paused = deployment('paused-worker', 0, 0)
        paused['metadata']['annotations'] = {'duckfleet.io/migration-job': 'paused-current'}

        def job(name, condition=None, active=0, label='duckfleet-platform-migrate'):
            return {'metadata': {'name': name, 'labels': {'app': label}}, 'status': {
                'active': active, 'conditions': [] if condition is None else [
                    {'type': condition, 'status': 'True'}]}}

        jobs = [job('current', 'Complete'), job('paused-current', 'Complete'),
                job('old-success', 'Complete'), job('pending'), job('running', active=1),
                job('failed', 'Failed'), job('not-yet-stopped', 'Complete', active=1),
                job('harness', 'Complete', label='duckfleet-harness')]
        self.assertEqual(rollout.completed_migrations_to_prune([server, paused], jobs),
                         ['old-success'])
        # No successful current run means no permission to discard earlier evidence.
        for current_condition in (None, 'Failed'):
            jobs[0] = job('current', current_condition)
            self.assertEqual(rollout.completed_migrations_to_prune([server, paused], jobs), [])
        jobs[0] = job('current', 'Complete')
        self.assertEqual(rollout.completed_migrations_to_prune([paused], jobs), [])
        self.assertEqual(rollout.completed_migrations_to_prune([], jobs), [])

    def test_pruning_deletes_only_selected_jobs_in_the_observed_namespace(self):
        server = deployment('server', 1, 1)
        server['metadata']['annotations'] = {'duckfleet.io/migration-job': 'current'}
        jobs = [{'metadata': {'name': name, 'labels': {'app': 'duckfleet-platform-migrate'}},
                 'status': {'conditions': [{'type': 'Complete', 'status': 'True'}]}}
                for name in ('current', 'old-success')]
        kube = ['kubectl', '--context', 'fixture-context', '-n', 'duckfleet-fixture']
        with mock.patch.object(rollout.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, '', '')
            rollout.prune_migrations(kube, [server], jobs)
            self.assertEqual(run.call_args.args[0],
                [*kube, 'delete', 'job', 'old-success', '--ignore-not-found',
                 '--wait=false', '--cascade=background'])
            self.assertEqual(run.call_args.kwargs['timeout'], 30)
            run.reset_mock()
            rollout.prune_migrations(kube, [server], jobs[:1])
            run.assert_not_called()
            run.return_value = subprocess.CompletedProcess([], 1, '', 'Forbidden')
            with self.assertRaisesRegex(RuntimeError, 'Forbidden'):
                rollout.prune_migrations(kube, [server], jobs)

    def test_pruning_requires_opt_in_and_a_successful_rollout(self):
        server = deployment('server', 1, 1)
        server['metadata']['annotations'] = {'duckfleet.io/migration-job': 'current'}
        job = {'metadata': {'name': 'current'}, 'status': {'conditions': [
            {'type': 'Complete', 'status': 'True'}]}}
        observed = {'deployments': [server], 'jobs': [job], 'pods': [], 'statefulsets': []}
        base = ['wait-rollout.py', '--namespace', 'duckfleet-fixture', '--context', 'fixture']
        with mock.patch.object(rollout, 'observe', side_effect=lambda _, kind: observed[kind]), \
                mock.patch.object(rollout, 'prune_migrations') as prune:
            with mock.patch.object(rollout.sys, 'argv', base):
                self.assertEqual(rollout.main(), 0)
            prune.assert_not_called()
            with mock.patch.object(rollout.sys, 'argv', [*base, '--prune-migrations']):
                self.assertEqual(rollout.main(), 0)
            prune.assert_called_once()
            prune.reset_mock()
            # A failed rollout must not erase successful prior migration evidence.
            server['status']['readyReplicas'] = 0
            with mock.patch.object(rollout.sys, 'argv',
                                   [*base, '--prune-migrations', '--timeout-seconds', '0']):
                self.assertEqual(rollout.main(), 1)
            prune.assert_not_called()

    def test_deploy_waits_separately_from_helm(self):
        deploy = (ROOT/'scripts/aws/deploy.sh').read_text()
        # Comments discuss the flag by name, so only executable lines are checked.
        commands = '\n'.join(line for line in deploy.splitlines()
                             if not line.lstrip().startswith('#'))
        # Handing the wait to helm is what produced the opaque deadline error.
        self.assertNotIn('--wait', commands)
        self.assertIn('wait-rollout.py', commands)

    def test_cluster_operations_require_explicit_target(self):
        common = ROOT/'scripts/aws/common.sh'
        base = {'PATH': '/usr/bin:/bin', 'GITHUB_ACTIONS': 'true'}
        for overrides, missing in [({}, 'DUCKFLEET_CLUSTER'),
                                   ({'DUCKFLEET_CLUSTER': 'test-cluster'}, 'DUCKFLEET_KUBE_CONTEXT')]:
            result = subprocess.run(
                ['bash', '-c', f'source "{common}"; assert_cluster'],
                env={**base, **overrides}, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(f'Set {missing}', result.stderr)
            self.assertNotIn('command not found', result.stderr)

    def test_operator_profile_is_optional_and_explicit(self):
        common = ROOT/'scripts/aws/common.sh'
        for overrides, expected in [({}, 'unset'),
                                    ({'DUCKFLEET_AWS_PROFILE': 'test-profile'}, 'test-profile'),
                                    ({'AWS_PROFILE': 'chosen', 'DUCKFLEET_AWS_PROFILE': 'fallback'}, 'chosen')]:
            result = subprocess.check_output(
                ['bash', '-c', f'source "{common}"; printf "%s" "${{AWS_PROFILE-unset}}"'],
                env={'PATH': '/usr/bin:/bin', **overrides}, text=True)
            self.assertEqual(result, expected)

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

    def test_rotation_replaces_only_the_password(self):
        """Rebuilding the DSNs would repoint the workers the moment one differed."""
        rotate = load('rotate_secret', 'scripts/aws/rotate_secret.py')
        before = {
            'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
            'metadata': {'name': 'duckfleet-dev', 'namespace': 'duckfleet-dev'},
            'stringData': {
                'DATABASE_CATALOG_PASSWORD': 'leaked',
                'POSTGRES_PASSWORD': 'administrator-unchanged',
                # Deliberately not the values this script would have guessed: a
                # non-default port and database are what a reconstruction would break.
                'CATALOG_DSN': 'host=postgres port=6543 dbname=lake_v2 user=duckfleet password=leaked',
                'PLATFORM_DSN': 'host=postgres port=6543 dbname=platform_v2 user=duckfleet_gateway password=platform-unchanged',
                'DUCKFLEET_TOKEN': 'bearer-token-unchanged',
            },
        }
        after = rotate.rotate(before, 'fresh')['stringData']
        self.assertEqual(after['DATABASE_CATALOG_PASSWORD'], 'fresh')
        self.assertEqual(after['POSTGRES_PASSWORD'], 'administrator-unchanged')
        # Everything but the password survives exactly.
        self.assertEqual(after['CATALOG_DSN'],
                         'host=postgres port=6543 dbname=lake_v2 user=duckfleet password=fresh')
        self.assertEqual(after['PLATFORM_DSN'],
                         'host=postgres port=6543 dbname=platform_v2 user=duckfleet_gateway password=platform-unchanged')
        # The bearer token is not a database credential. Rotating it here would log
        # every caller out as a side effect of database maintenance.
        self.assertEqual(after['DUCKFLEET_TOKEN'], 'bearer-token-unchanged')
        # And the old password appears nowhere.
        self.assertNotIn('leaked', json.dumps(after))

    def test_rotation_reads_a_secret_however_it_is_stored(self):
        """A live Secret comes back base64 in `data`, not `stringData`."""
        rotate = load('rotate_secret', 'scripts/aws/rotate_secret.py')
        import base64
        encoded = {
            'apiVersion': 'v1', 'kind': 'Secret',
            'metadata': {'name': 'duckfleet-dev', 'namespace': 'duckfleet-dev'},
            'data': {key: base64.b64encode(value.encode()).decode() for key, value in {
                'DATABASE_CATALOG_PASSWORD': 'leaked',
                'POSTGRES_PASSWORD': 'administrator-unchanged',
                'CATALOG_DSN': 'host=postgres port=5432 dbname=lake user=duckfleet password=leaked',
                'PLATFORM_DSN': 'host=postgres port=5432 dbname=platform user=duckfleet_gateway password=platform-unchanged',
            }.items()},
        }
        after = rotate.rotate(encoded, 'fresh')['stringData']
        self.assertTrue(after['CATALOG_DSN'].endswith('password=fresh'), after)
        self.assertNotIn('leaked', json.dumps(after))

    def test_rotation_refuses_a_partial_secret(self):
        """Rotating half a credential is worse than not rotating: the database would
        accept a password the workers no longer carry."""
        rotate = load('rotate_secret', 'scripts/aws/rotate_secret.py')
        partial = {
            'apiVersion': 'v1', 'kind': 'Secret',
            'metadata': {'name': 'duckfleet-dev', 'namespace': 'duckfleet-dev'},
            'stringData': {'POSTGRES_PASSWORD': 'leaked'},
        }
        with self.assertRaises(KeyError):
            rotate.rotate(partial, 'fresh')
        # A DSN with no password to replace is the same class of mistake.
        passwordless = {
            'apiVersion': 'v1', 'kind': 'Secret',
            'metadata': {'name': 'duckfleet-dev', 'namespace': 'duckfleet-dev'},
            'stringData': {'DATABASE_CATALOG_PASSWORD': 'leaked',
                'POSTGRES_PASSWORD': 'administrator-unchanged',
                           'CATALOG_DSN': 'host=postgres dbname=lake user=duckfleet',
                           'PLATFORM_DSN': 'host=postgres dbname=platform user=duckfleet'},
        }
        with self.assertRaises(KeyError):
            rotate.rotate(passwordless, 'fresh')

    def test_rotation_changes_the_database_and_the_secret_together(self):
        """`deploy.sh` writes that Secret once and the PVC outlives a redeploy, so
        changing either alone leaves workers holding a password the server rejects —
        which shows up as an unreachable catalog rather than a credential problem."""
        script = (ROOT/'scripts/aws/rotate-catalog-credential.sh').read_text()
        commands = '\n'.join(line for line in script.splitlines()
                             if not line.lstrip().startswith('#'))
        self.assertIn('ALTER ROLE :"catalog_role" PASSWORD', commands)
        self.assertIn('rotate_secret.py', commands)
        # The database moves first: the inverse would leave the database reachable
        # only with a credential that no longer exists anywhere.
        self.assertLess(commands.index('ALTER ROLE'), commands.index('apply -f -'))
        # And the fleet is restarted, because a Secret change does not reach a running
        # pod's environment.
        self.assertIn('rollout restart', commands)
        self.assertLess(commands.index('apply -f -'), commands.index('rollout restart'))

    def test_api_egress_must_cover_the_endpoint_not_the_service(self):
        """The rule shipped in #9 matched neither half of the real destination.

        Egress policy is evaluated after kube-proxy rewrites the destination, so
        traffic a pod sends to the API server's ClusterIP on 443 arrives at an
        endpoint address on that endpoint's port. The first local run with an
        enforcing CNI denied the gateway because the rule named the ClusterIP.
        """
        egress = load('check_api_egress', 'scripts/aws/check-api-egress.py')
        # kind: the node's address on 6443, which the ClusterIP rule misses entirely.
        endpoint = [('172.20.0.4', 6443)]
        self.assertEqual(
            egress.uncovered(endpoint, ['10.96.0.1/32'], [443]), ['172.20.0.4:6443'],
            'a rule naming the Service address must be reported as covering nothing')
        self.assertEqual(egress.uncovered(endpoint, ['172.20.0.0/16'], [6443]), [])
        # Right address, wrong port is still denied, and has to be reported as such.
        self.assertEqual(egress.uncovered(endpoint, ['172.20.0.0/16'], [443]),
                         ['172.20.0.4:6443'])
        # EKS: endpoints are ENIs in the VPC, on 443.
        self.assertEqual(egress.uncovered([('10.42.3.17', 443)], ['10.42.0.0/16'], [443]), [])
        self.assertEqual(egress.uncovered([('10.42.3.17', 443)], ['172.20.0.1/32'], [443]),
                         ['10.42.3.17:443'])
        # Every endpoint must be covered, not just one of them.
        pair = [('10.42.3.17', 443), ('10.42.9.4', 443)]
        self.assertEqual(egress.uncovered(pair, ['10.42.3.0/24'], [443]), ['10.42.9.4:443'])

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

    def test_a_release_can_never_carry_rbac_the_deploy_cannot_create(self):
        # This used to be a deploy-time gate: refuse before applying if the render
        # contained a Role the identity could not create. The gate is gone because the
        # condition is now impossible rather than merely caught — the chart renders no
        # RBAC at all, and check-chart.py fails if it ever does again. Asserted over
        # the render that turns isolated compute on, which is the one that used to
        # carry it.
        rendered = chart()
        kinds = {doc['kind'] for doc in yaml.safe_load_all(rendered) if doc}
        self.assertFalse(kinds & {'Role', 'RoleBinding', 'ClusterRole',
                                  'ClusterRoleBinding', 'ResourceQuota'}, kinds)
        # And the deploy no longer asks a question it is not permitted to answer.
        self.assertNotIn('can-i create roles',
                         (ROOT/'scripts/aws/deploy.sh').read_text())


class PlatformPackagingTests(unittest.TestCase):
    def migration(self, *args):
        objects = [item for item in yaml.safe_load_all(chart(args)) if item]
        jobs = [item for item in objects if item['kind'] == 'Job'
                and item['metadata'].get('labels', {}).get('app') == 'duckfleet-platform-migrate']
        return objects, jobs

    def test_fresh_install_job_has_no_hook_or_compute_credentials(self):
        objects, jobs = self.migration()
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertNotIn('helm.sh/hook', job['metadata'].get('annotations', {}))
        pod = job['spec']['template']['spec']
        self.assertEqual(pod['serviceAccountName'], 'duckfleet-no-aws')
        self.assertFalse(pod['automountServiceAccountToken'])
        env = {item['name'] for item in pod['containers'][0]['env']}
        self.assertIn('PLATFORM_DSN', env)
        self.assertFalse({'CATALOG_DSN', 'DUCKFLEET_TOKEN', 'S3_AUTH_MODE'} & env)
        gateway = next(item for item in objects if item['kind'] == 'Deployment'
                       and item['metadata']['name'] == 'server')
        self.assertEqual(gateway['metadata']['annotations']['duckfleet.io/migration-job'],
                         job['metadata']['name'])

    def test_reset_run_id_recreates_job_even_with_the_same_image_and_secret(self):
        _, original = self.migration()
        _, reset = self.migration('--set-string', 'platformMigration.runId=after-restore')
        _, repeat = self.migration('--set-string', 'platformMigration.runId=after-restore')
        self.assertNotEqual(original[0]['metadata']['name'], reset[0]['metadata']['name'])
        self.assertEqual(reset, repeat, 'template/apply must converge for a fixed run identity')
        _, changed_timeout = self.migration('--set', 'platformMigration.timeoutSeconds=241')
        self.assertNotEqual(original[0]['metadata']['name'], changed_timeout[0]['metadata']['name'],
                            'a Job template change must not patch an immutable Job')

    def test_suspended_release_starts_no_migration_and_external_db_still_does(self):
        objects, jobs = self.migration('--set', 'suspended=true')
        self.assertFalse(jobs)
        gateway = next(item for item in objects if item['kind'] == 'Deployment'
                       and item['metadata']['name'] == 'server')
        self.assertNotIn('duckfleet.io/migration-job', gateway['metadata'].get('annotations') or {})
        objects, jobs = self.migration('--set', 'postgres.bundled=false')
        self.assertEqual(len(jobs), 1)
        self.assertFalse(any(item['kind'] == 'StatefulSet' for item in objects))


class NamedPoolPackagingTests(unittest.TestCase):
    POOLS = {
        'small': {'cpu': 1, 'memory': '1Gi', 'disk': '2Gi', 'workers': 2},
        'interactive': {'cpu': 2, 'memory': '3Gi', 'disk': '4Gi', 'workers': 2,
                        'maxSessionsPerWorker': 7},
        'small-1': {'cpu': 1, 'memory': '512Mi', 'disk': '1536Mi', 'workers': 1},
    }

    def render(self, pools=None, *extra):
        effective = self.POOLS if pools is None else pools
        default = next((arg.split('=', 1)[1] for arg in extra if arg.startswith('defaultPool=')), 'small')
        reviewed = dict(pool=default, **{key: effective.get(default, {'cpu':1,'memory':'1Gi','disk':'2Gi'})[key] for key in ('cpu','memory','disk')})
        text = chart(['--set-json', 'persistentPools='+json.dumps(effective),
                      '--set-json', 'authorization.defaultPersistentGrant='+json.dumps(reviewed), *extra])
        objects = [item for item in yaml.safe_load_all(text) if item]
        # Exercise the same resource-boundary validator used on every deployment.
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml') as handle:
            handle.write(text); handle.flush()
            subprocess.run(['python3', str(ROOT/'scripts/check-chart.py'), handle.name],
                           check=True, capture_output=True, text=True)
        return {(item['kind'], item['metadata']['name']): item for item in objects}

    def test_independent_allocations_and_generated_identities_agree_with_registry(self):
        objects = self.render()
        config = json.loads(objects['ConfigMap', 'duckfleet-pools']['data']['pools.json'])
        self.assertEqual(config['pools']['interactive'], {
            'cpu': 2, 'memory': '3Gi', 'disk': '4Gi', 'max_sessions_per_worker': 7,
            'authority': 'writer',
            'endpoints': ['http://worker-pool-interactive-0:8080',
                          'http://worker-pool-interactive-1:8080']})
        self.assertIn(('Deployment', 'worker-small-1'), objects)
        self.assertIn(('Deployment', 'worker-pool-small-1-0'), objects,
                      'a pool called small-1 must not collide with legacy small replica1')
        interactive = objects['Deployment', 'worker-pool-interactive-0']['spec']['template']['spec']
        resource = interactive['containers'][0]['resources']
        self.assertEqual(resource['requests'], {'cpu': '2', 'memory': '3Gi', 'ephemeral-storage': '4Gi'})
        self.assertEqual(resource['limits']['ephemeral-storage'], '5Gi')
        self.assertEqual(interactive['volumes'][0]['emptyDir']['sizeLimit'], '4Gi')
        fractional = objects['Deployment', 'worker-pool-small-1-0']['spec']['template']['spec']['containers'][0]
        self.assertEqual(fractional['resources']['limits']['ephemeral-storage'], '2560Mi')

    def test_reader_pool_separates_identity_and_data_credentials(self):
        pools = dict(self.POOLS)
        pools['readers'] = {'cpu': 1, 'memory': '1Gi', 'disk': '2Gi', 'workers': 1,
                            'authority': 'reader'}
        objects = self.render(pools)
        config = json.loads(objects['ConfigMap', 'duckfleet-pools']['data']['pools.json'])
        self.assertEqual(config['pools']['readers']['authority'], 'reader')
        pod = objects['Deployment', 'worker-pool-readers-0']['spec']['template']['spec']
        self.assertEqual(pod['serviceAccountName'], 'duckfleet-reader')
        env = pod['containers'][0]['env']
        names = {item['name'] for item in env}
        self.assertIn('CATALOG_READER_DSN', names)
        self.assertNotIn('CATALOG_DSN', names)
        self.assertFalse(any(name.startswith('DUCKFLEET_RESULT_') for name in names))
        self.assertEqual(next(item['value'] for item in env if item['name'] == 'DUCKFLEET_AUTHORITY'),
                         'reader')
    def test_compute_policy_renders_and_validates(self):
        policy = {'max_cpu': 2, 'max_principal_cpu': 1, 'idle_timeout_seconds': 30,
                  'max_lifetime_seconds': 300}
        objects = self.render(self.POOLS, '--set-json', 'computePolicy='+json.dumps(policy))
        for name in ('server', 'worker-small'):
            env = objects['Deployment', name]['spec']['template']['spec']['containers'][0]['env']
            rendered = next(item['value'] for item in env if item['name'] == 'DUCKFLEET_COMPUTE_POLICY_JSON')
            self.assertEqual(json.loads(rendered), policy)
        with self.assertRaises(subprocess.CalledProcessError):
            self.render(self.POOLS, '--set', 'computePolicy.max_cpu=0')

    def test_named_small_preserves_legacy_immutable_selectors(self):
        named = self.render()
        legacy = {(item['kind'], item['metadata']['name']): item
                  for item in yaml.safe_load_all(chart(['--set', 'workerReplicas.small=2'])) if item}
        for name in ('worker-small', 'worker-small-1'):
            self.assertEqual(named['Deployment', name]['spec']['selector'],
                             legacy['Deployment', name]['spec']['selector'])
            self.assertEqual(named['Service', name]['spec']['selector'],
                             legacy['Service', name]['spec']['selector'])

    def test_configuration_reload_does_not_change_gateway_pod_template(self):
        initial = self.render()
        updated = self.render(None, '--set', 'defaultPool=interactive')
        self.assertEqual(initial['Deployment', 'server']['spec']['template'],
                         updated['Deployment', 'server']['spec']['template'])
        self.assertNotEqual(initial['ConfigMap', 'duckfleet-pools']['data'],
                            updated['ConfigMap', 'duckfleet-pools']['data'])
        gateway = initial['Deployment', 'server']['spec']['template']['spec']['containers'][0]
        mounts = {mount['name']: mount for mount in gateway['volumeMounts']}
        self.assertNotIn('subPath', mounts['persistent-pools'])
        env = {item['name']: item.get('value') for item in gateway['env']}
        self.assertNotIn('DUCKFLEET_WORKER_URLS', env)

    def test_empty_pool_and_non_small_default_remain_configured(self):
        objects = self.render({'etl': {'cpu': 4, 'memory': '8Gi', 'disk': '20Gi', 'workers': 0}},
                              '--set', 'defaultPool=etl')
        self.assertFalse(any(kind == 'Deployment' and name.startswith('worker-') for kind, name in objects))
        config = json.loads(objects['ConfigMap', 'duckfleet-pools']['data']['pools.json'])
        self.assertEqual(config['default_pool'], 'etl')
        self.assertEqual(config['pools']['etl']['endpoints'], [])

    def test_invalid_pool_configuration_is_rejected_at_render(self):
        for args in [
            ['--set', 'defaultPool=missing'],
            ['--set', 'workerReplicas.small=1'],
            ['--set', 'persistentPools.interactive.memory=128Mi'],
            ['--set', 'persistentPools.interactive.disk=512Mi'],
            ['--set', 'persistentPools.interactive.maxSessionsPerWorker=1025'],
            ['--set', 'gatewayMaxSessions=4097'],
            ['--set', 'workerQueueCapacity=0'],
        ]:
            with self.subTest(args=args), self.assertRaises(subprocess.CalledProcessError):
                self.render(None, *args)
        for name in ['Upper', 'trailing-', '-leading', 'a'*41]:
            with self.subTest(name=name), self.assertRaises(subprocess.CalledProcessError):
                self.render({name: {'cpu': 1, 'memory': '1Gi', 'disk': '2Gi'}},
                            '--set', 'defaultPool='+name)

    def test_clearing_inherited_replica_values_allows_named_pool_adoption(self):
        self.render(None, '--set', 'workerReplicas.small=2', '--set', 'workerReplicas=null')

    def test_named_pools_work_without_isolated_compute(self):
        self.render(None, '--set', 'isolatedCompute.enabled=false')

    def test_compose_pool_profile_is_optional_and_reload_uses_a_directory(self):
        compose = yaml.safe_load((ROOT/'compose.yaml').read_text())['services']
        config = json.loads((ROOT/'examples/pools/compose.json').read_text())
        worker = compose['worker-pool-interactive-0']
        self.assertEqual(worker['profiles'], ['pools'])
        self.assertEqual(worker['environment']['DUCKFLEET_POOL'], 'interactive')
        self.assertEqual(worker['cpus'], config['pools']['interactive']['cpu'])
        self.assertEqual(worker['environment']['DUCKFLEET_WORKER_MEMORY'], '3Gi')
        self.assertEqual(worker['mem_limit'], '3g')
        gateway = compose['server-pools']
        self.assertEqual(gateway['profiles'], ['pools'])
        self.assertNotIn('DUCKFLEET_WORKER_URLS', gateway['environment'])
        self.assertTrue(any('/etc/duckfleet/pools:ro' in mount for mount in gateway['volumes']))
        self.assertNotIn('profiles', compose['server'])
        self.assertNotIn('DUCKFLEET_POOLS_FILE', compose['server']['environment'])


class PlatformOperatorScriptsTests(unittest.TestCase):
    def test_restore_stops_compute_and_migrates_only_after_both_restores_succeed(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            backup = directory/'backup'
            backup.mkdir()
            for database in ('lake', 'platform'):
                (backup/f'{database}.dump').write_text('fixture archive')
            docker = directory/'docker'
            docker.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ['CALL_LOG'], 'a') as handle:
    handle.write(json.dumps(sys.argv[1:]) + '\\n')
if 'ps' in sys.argv:
    print('fixture-worker')
elif 'inspect' in sys.argv:
    print(json.dumps([{'Id':'fixture-worker','Config':{'Env':['DUCKFLEET_WORKER_ID=small']},
                      'State':{'Running':not os.path.exists(os.environ['STOPPED'])}}]))
elif 'stop' in sys.argv:
    open(os.environ['STOPPED'], 'w').close()
if 'psql' in sys.argv:
    print('duckfleet_catalog' if 'lake' in sys.argv else 'duckfleet_migrator')
if 'pg_restore' in sys.argv:
    sys.stdin.read()
    if os.environ.get('FAIL_PLATFORM') == 'yes' and sys.argv[-1] == 'platform':
        sys.exit(42)
""")
            docker.chmod(0o755)
            log = directory/'calls'
            env = dict(os.environ, PATH=str(directory)+os.pathsep+os.environ['PATH'],
                       CALL_LOG=str(log), STOPPED=str(directory/'stopped'), DUCKFLEET_CONFIRM_RESTORE='yes')
            command = ['bash', str(ROOT/'scripts/restore-catalog.sh'), 'compose', str(backup)]
            success = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(success.returncode, 0, success.stderr)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertLess(next(i for i,c in enumerate(calls) if 'stop' in c),
                            next(i for i,c in enumerate(calls) if 'pg_restore' in c))
            restores = [call for call in calls if 'pg_restore' in call]
            self.assertEqual([call[-1] for call in restores], ['lake', 'platform'])
            self.assertTrue(all('--exit-on-error' in call for call in restores))
            self.assertEqual(calls[-1], ['compose', 'run', '--rm', '--no-deps', '-e', 'DUCKFLEET_ROLE=platform-grants', 'platform-migrate'])
            self.assertIn('compute remains stopped', success.stdout)
            log.unlink()
            failed = subprocess.run(command, env=dict(env, FAIL_PLATFORM='yes'),
                                    capture_output=True, text=True)
            self.assertEqual(failed.returncode, 42)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertFalse(any('run' in call or 'up' in call for call in calls),
                             'a partial restore must not migrate or restart compute')

    def test_manual_kube_migration_clones_configuration_with_a_new_job_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = {'metadata': {'annotations': {'duckfleet.io/platform-schema':'7'}},
                      'spec': {'selector': {'matchLabels': {'controller-uid': 'old'}},
                               'manualSelector': False, 'activeDeadlineSeconds': 390,
                               'backoffLimit': 0,
                               'template': {'metadata': {'labels': {
                                   'app': 'duckfleet-platform-migrate',
                                   'controller-uid': 'old', 'job-name': 'old',
                                   'batch.kubernetes.io/controller-uid': 'old',
                                   'batch.kubernetes.io/job-name': 'old'}},
                                   'spec': {'restartPolicy': 'Never', 'containers': [
                                       {'name': 'migrate', 'image': 'same-image', 'env': [
                                           {'name': 'PLATFORM_DSN', 'valueFrom': {
                                               'secretKeyRef': {'name': 'same-secret', 'key': 'PLATFORM_DSN'}}}]}]}}}}
            fixture = directory/'fixture.json'
            fixture.write_text(json.dumps(source))
            created = directory/'created.json'
            kubectl = directory/'kubectl'
            kubectl.write_text("""#!/usr/bin/env python3
import json,os,sys
args=sys.argv[1:]
if 'deployments' in args or 'pods' in args:
    print(json.dumps({'items':[]}))
elif 'deployment' in args:
    print('old-job')
elif 'create' in args:
    open(os.environ['CREATED'], 'w').write(sys.stdin.read())
elif 'old-job' in args:
    print(open(os.environ['FIXTURE']).read())
elif 'get' in args and 'job' in args:
    print(json.dumps({'status': {'conditions': [{'type':'Complete','status':'True'}]}}))
else:
    sys.exit('unexpected kubectl arguments: '+repr(args))
""")
            kubectl.chmod(0o755)
            result = subprocess.run(['bash', str(ROOT/'scripts/migrate-platform.sh'), 'kube'],
                env=dict(os.environ, PATH=str(directory)+os.pathsep+os.environ['PATH'],
                         CREATED=str(created), FIXTURE=str(fixture),
                         DUCKFLEET_KUBE_CONTEXT='fixture', DUCKFLEET_NAMESPACE='duckfleet-fixture'),
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            job = json.loads(created.read_text())
            self.assertNotEqual(job['metadata']['name'], 'old-job')
            self.assertNotIn('selector', job['spec'])
            self.assertNotIn('manualSelector', job['spec'])
            self.assertEqual(job['spec']['template']['metadata']['labels'],
                             {'app': 'duckfleet-platform-migrate'})
            self.assertEqual(job['spec']['template']['spec'], source['spec']['template']['spec'])


class QueueDeploymentTests(unittest.TestCase):
    def test_upgrade_guard_version_matches_binary_and_packaging(self):
        expected = int(re.search(r'SCHEMA_VERSION: i32 = (\d+)',
                                (ROOT/'server/src/platform.rs').read_text()).group(1))
        self.assertEqual(lifecycle.PLATFORM_SCHEMA, expected)
        services = yaml.safe_load((ROOT/'compose.yaml').read_text())['services']
        for name in ('server','server-pools','worker-small','worker-small-b','worker-pool-interactive-0'):
            self.assertEqual(services[name]['labels'][lifecycle.SCHEMA_MARKER], str(expected))
            self.assertEqual(services[name]['stop_grace_period'], '630s')

    def test_policy_overrides_preserve_omission_and_render_exact_worker_bounds(self):
        objects = NamedPoolPackagingTests().render(None,
            '--set', 'persistentPools.interactive.admission.maxRunningQueries=4',
            '--set', 'admissionLimits.default.maxRunningPerPrincipal=1',
            '--set', 'admissionLimits.isolated.maxRunningQueries=3')
        registry = json.loads(objects['ConfigMap', 'duckfleet-pools']['data']['pools.json'])
        self.assertEqual(registry['pools']['interactive']['admission'], {'max_running_queries':4})
        gateway = objects['Deployment', 'server']['spec']['template']['spec']['containers'][0]
        env = {item['name']:item.get('value') for item in gateway['env']}
        self.assertEqual(json.loads(env['DUCKFLEET_ADMISSION_LIMITS_JSON']), {
            'default':{'max_running_per_principal':1}, 'isolated':{'max_running_queries':3}})
        worker = objects['Deployment', 'worker-small']['spec']['template']['spec']['containers'][0]
        env = {item['name']:item.get('value') for item in worker['env']}
        self.assertEqual(env['DUCKFLEET_WORKER_QUEUE_CAPACITY'], '64')
        self.assertEqual(env['DUCKFLEET_WORKER_MAX_QUEUED_QUERIES'], '64')
        self.assertEqual(env['DUCKFLEET_WORKER_MAX_QUEUED_BYTES'], '16777216')
        self.assertNotIn('DUCKFLEET_ADMISSION_LIMITS_JSON', env)

    def test_invalid_admission_and_byte_bounds_fail_before_deployment(self):
        for setting in ('workerMaxQueuedQueries=0','workerMaxQueuedQueries=4097',
                        'workerMaxQueuedBytes=1048575','workerMaxQueuedBytes=268435457',
                        'admissionLimits.default.maxQueuedQueries=65536',
                        'admissionLimits.default.maxRunningQueries=0',
                        'admissionLimits.default.maxQueuedQueries=1',
                        'admissionLimits.default.unknown=2',
                        'persistentPools.interactive.admission.maxRunningQueries=1025',
                        'persistentPools.interactive.admission.maxQueuedQueries=1'):
            with self.subTest(setting=setting), self.assertRaises(subprocess.CalledProcessError):
                NamedPoolPackagingTests().render(None, '--set', setting)
        with self.assertRaises(subprocess.CalledProcessError):
            NamedPoolPackagingTests().render(None,
                '--set', 'admissionLimits.default.maxRunningPerPrincipal=4',
                '--set', 'persistentPools.interactive.admission.maxRunningQueries=2')

    def test_compose_gateway_start_does_not_wait_for_policy_dependent_worker_readiness(self):
        services = yaml.safe_load((ROOT/'compose.yaml').read_text())['services']
        for name in ('server','server-pools'):
            dependencies = services[name]['depends_on']
            self.assertEqual(dependencies['platform-migrate']['condition'], 'service_completed_successfully')
            for worker, condition in dependencies.items():
                if worker.startswith('worker-'):
                    self.assertEqual(condition['condition'], 'service_started')

    @staticmethod
    def deployment(name, role, version=None, replicas=1):
        metadata = {'labels':{'app':role}, 'annotations':{}}
        if version is not None:
            metadata['annotations'][lifecycle.SCHEMA_MARKER] = version
        return {'metadata':{'name':name}, 'spec':{'replicas':replicas,'template':{'metadata':metadata}}}

    @staticmethod
    def pod(name, role, version=None):
        metadata = {'name':name, 'labels':{'app':role},'annotations':{}}
        if version is not None:
            metadata['annotations'][lifecycle.SCHEMA_MARKER] = version
        return {'metadata':metadata}

    def test_upgrade_guard_allows_fresh_and_same_schema_but_requires_actual_quiescence(self):
        lifecycle.check_kube_state([], [])
        current = self.deployment('server', lifecycle.GATEWAY, str(lifecycle.PLATFORM_SCHEMA))
        lifecycle.check_kube_state([current], [self.pod('server-new', lifecycle.GATEWAY, str(lifecycle.PLATFORM_SCHEMA))])
        old = self.deployment('worker-pool-wide-0', lifecycle.WORKER)
        with self.assertRaisesRegex(ValueError, 'stop-compute'):
            lifecycle.check_kube_state([old], [])
        old['spec']['replicas'] = 0
        with self.assertRaisesRegex(ValueError, 'stop-compute'):
            lifecycle.check_kube_state([old], [self.pod('terminating-old', lifecycle.WORKER)])
        lifecycle.check_kube_state([old], [])
        with self.assertRaisesRegex(ValueError, 'rollback is unsupported'):
            lifecycle.check_kube_state([self.deployment('newer', lifecycle.GATEWAY, str(lifecycle.PLATFORM_SCHEMA+1), 0)], [])

    def test_stop_compute_orders_gateway_before_all_workers_and_preserves_database_and_jobs(self):
        deployments = [self.deployment('worker-small', lifecycle.WORKER),
                       self.deployment('server', lifecycle.GATEWAY),
                       self.deployment('worker-pool-wide-0', lifecycle.WORKER),
                       self.deployment('other', 'unrelated')]
        stopped = json.loads(json.dumps(deployments))
        for item in stopped[:3]:
            item['spec']['replicas'] = 0
        kube = ['kubectl','--context','fixture','--namespace','duckfleet-fixture']
        with mock.patch.object(lifecycle, 'kube_objects', side_effect=[deployments,stopped,[]]), \
             mock.patch.object(lifecycle, 'run') as command:
            lifecycle.stop_kube(kube, 180)
        calls = [call.args[0] for call in command.call_args_list]
        self.assertEqual(calls[0], [*kube,'patch','deployment/server','--type=strategic','--patch={"spec":{"replicas":0}}'])
        self.assertIn('app=duckfleet-gateway', calls[1])
        self.assertTrue(all(call[:len(kube)] == kube for call in calls))
        self.assertIn([*kube,'patch','deployment/worker-pool-wide-0','--type=strategic','--patch={"spec":{"replicas":0}}'], calls)
        self.assertFalse(any(any(word in ('statefulset','jobs','secrets','pvc','deployment/other') for word in call) for call in calls))
        with mock.patch.object(lifecycle, 'kube_objects', return_value=deployments), \
             mock.patch.object(lifecycle, 'run', side_effect=[None,subprocess.CalledProcessError(1,['kubectl'])]) as command:
            with self.assertRaises(subprocess.CalledProcessError):
                lifecycle.stop_kube(kube, 180)
            self.assertEqual(command.call_count, 2, 'a gateway-stop failure must not proceed to workers or migration')

    def test_compose_stop_scopes_to_project_compute_and_waits_with_execution_grace(self):
        def container(name, env):
            return {'Id':name,'Config':{'Env':env},'State':{'Running':True}}
        containers = [container('db',['POSTGRES_USER=duckfleet']),
                      container('old-optional-worker',['DUCKFLEET_WORKER_ID=pool-wide-0']),
                      container('gateway',['DUCKFLEET_ROLE=gateway'])]
        with mock.patch.object(lifecycle,'compose_containers',side_effect=[containers,[]]), \
             mock.patch.object(lifecycle,'run') as command:
            lifecycle.stop_compose()
        self.assertEqual([call.args[0] for call in command.call_args_list],
                         [['docker','stop','-t','630','gateway'],['docker','stop','-t','630','old-optional-worker']])
        with self.assertRaisesRegex(ValueError,'stop-compute'):
            lifecycle.check_compose_state(containers)


class JobDeploymentTests(unittest.TestCase):
    def test_named_job_envelopes_survive_render_without_overriding_other_pools(self):
        ceiling = dict(runtime_seconds=900, result_bytes=1073741824,
                       result_rows=10000000, retention_seconds=86400)
        pool_limit = dict(ceiling, runtime_seconds=300)
        pools = {name:dict(value) for name,value in NamedPoolPackagingTests.POOLS.items()}
        pools['interactive']['jobLimits'] = pool_limit
        objects = NamedPoolPackagingTests().render(pools,
            '--set-json', 'jobPolicy='+json.dumps({'ceilings':ceiling}),
            '--set', 'results.enabled=true', '--set', 'isolatedCompute.enabled=false')
        registry = json.loads(objects['ConfigMap', 'duckfleet-pools']['data']['pools.json'])
        self.assertEqual(registry['pools']['interactive']['job_limits'], pool_limit)
        self.assertNotIn('job_limits', registry['pools']['small'])
        gateway = objects['Deployment', 'server']['spec']['template']['spec']
        self.assertEqual(gateway['serviceAccountName'], 'duckfleet-results')
        env = {item['name']:item.get('value') for item in gateway['containers'][0]['env']}
        self.assertEqual(json.loads(env['DUCKFLEET_JOB_POLICY_JSON']), {'ceilings':ceiling})
        self.assertEqual(env['DUCKFLEET_RESULT_AUTH_MODE'], 'pod_identity')
        self.assertNotIn('S3_DATA_PATH', env)
        for invalid in [dict(pool_limit,runtime_seconds=901), {'runtime_seconds':60}]:
            pools['interactive']['jobLimits'] = invalid
            with self.assertRaises(subprocess.CalledProcessError):
                NamedPoolPackagingTests().render(pools,
                    '--set-json', 'jobPolicy='+json.dumps({'ceilings':ceiling}))

    def test_local_results_keep_distinct_prefix_and_explicit_static_identity(self):
        module = load('job_localizer', 'scripts/kind/localize.py')
        localizer = module.Localizer()
        container = {'env':[
            {'name':'DUCKFLEET_RESULT_AUTH_MODE','value':'pod_identity'},
            {'name':'DUCKFLEET_RESULT_ALLOW_HTTP','value':'false'},
            {'name':'DUCKFLEET_RESULT_DATA_PATH','value':'s3://aws-bucket/results/'},
            {'name':'DUCKFLEET_RESULT_LAKE_DATA_PATH','value':'s3://aws-bucket/lake/'},
        ]}
        localizer.container(container)
        env = {item['name']:item['value'] for item in container['env']}
        self.assertEqual(env['DUCKFLEET_RESULT_AUTH_MODE'], 'static')
        self.assertEqual(env['DUCKFLEET_RESULT_DATA_PATH'], 's3://duckfleet/results/')
        self.assertEqual(env['DUCKFLEET_RESULT_LAKE_DATA_PATH'], 's3://duckfleet/lake/')
        self.assertEqual(env['DUCKFLEET_RESULT_ENDPOINT'], 'http://object-store:9000')
        self.assertEqual(env['DUCKFLEET_RESULT_ALLOW_HTTP'], 'true')
        self.assertTrue(env['DUCKFLEET_RESULT_ACCESS_KEY'])
        self.assertTrue(env['DUCKFLEET_RESULT_SECRET_KEY'])


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

    def test_cpu_and_memory_are_independent_declared_dimensions(self):
        resources.assert_worker_resources(self.spec(2, memory='3Gi'), 2, 'test', memory='3Gi')
        resources.assert_worker_resources(self.spec(4, memory='512Mi'), 4, 'test', memory='512Mi')
        with self.assertRaises(AssertionError):
            resources.assert_worker_resources(self.spec(2, memory='3Gi'), 2, 'test', memory='4Gi')

    def test_kind_cgroup_probe_uses_independent_memory_and_all_workers(self):
        limits = load('kind_assert_limits', 'scripts/kind/assert_limits.py')
        pods = []
        for name, cpu, memory in [('small', 1, '1Gi'), ('interactive', 2, '3Gi')]:
            pods.append({'metadata': {'name': name, 'labels': {'app': 'duckfleet-worker'}},
                         'spec': {'containers': [{'name': 'worker', 'env': [
                             {'name': 'DUCKFLEET_WORKER_ID', 'value': name},
                             {'name': 'DUCKFLEET_POOL', 'value': name},
                             {'name': 'DUCKFLEET_WORKER_CPU', 'value': str(cpu)},
                             {'name': 'DUCKFLEET_WORKER_MEMORY', 'value': memory},
                             {'name': 'DUCKFLEET_WORKER_DISK', 'value': '2Gi'}],
                             'resources': {'requests': {'cpu': '50m', 'memory': memory,
                                                        'ephemeral-storage': '2Gi'},
                                           'limits': {'cpu': str(cpu), 'memory': memory,
                                                      'ephemeral-storage': '3Gi'}}}]}})
        observed = {'small': '100000 100000 1073741824',
                    'interactive': '200000 100000 3221225472'}
        def kube(args, context, namespace):
            return json.dumps({'items': pods}) if args[0] == 'get' else observed[args[1]]
        with mock.patch.object(limits, 'kubectl', side_effect=kube) as calls, \
                mock.patch.object(limits.sys, 'argv', ['assert_limits.py', '--context', 'fixture',
                                                     '--namespace', 'duckfleet-fixture']):
            self.assertEqual(limits.main(), 0)
            self.assertEqual(calls.call_count, 3)
            observed['interactive'] = '200000 100000 2147483648'
            self.assertEqual(limits.main(), 1, 'a mismatched memory cgroup must fail')

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

def containers(rendered):
    """Every container the chart would run, including the isolated pod template.

    The session pod is rendered into a ConfigMap rather than as an object, because the
    gateway creates it at runtime — so walking objects alone would miss the one pod
    most likely to be refused by a quota.
    """
    for document in yaml.safe_load_all(rendered):
        if not document:
            continue
        spec = document.get('spec') or {}
        pods = []
        if 'template' in spec:
            pods.append(spec['template']['spec'])
        if document['kind'] == 'ConfigMap' and 'pod.json' in (document.get('data') or {}):
            pods.append(json.loads(document['data']['pod.json'])['spec'])
        for pod in pods:
            for container in pod.get('containers', []) + pod.get('initContainers', []):
                yield document['kind'], document['metadata']['name'], container


class OwnershipBoundaryTests(unittest.TestCase):
    """The objects an operator applies, which the deploy identity cannot create."""

    def render(self, namespace='duckfleet-dev'):
        return load('render_boundary', 'scripts/aws/render_boundary.py').documents(namespace)

    def render_eks(self, namespace='duckfleet-dev', node_class='chosen'):
        """The boundary including the objects that need a real cloud cluster."""
        return load('render_boundary', 'scripts/aws/render_boundary.py').documents(
            namespace, node_class)


    def test_every_object_names_the_target_namespace(self):
        # The namespace is configuration (scripts/aws/target.env), so the boundary has
        # to follow it. Rendering for a different one must move every object, not just
        # the ones that happened to be written with a placeholder.
        for document in self.render('duckfleet-elsewhere'):
            if document['kind'] == 'Namespace':
                self.assertEqual(document['metadata']['name'], 'duckfleet-elsewhere')
            else:
                self.assertEqual(document['metadata']['namespace'], 'duckfleet-elsewhere',
                                 document['metadata']['name'])

    def test_the_grant_is_pods_in_one_namespace_and_nothing_else(self):
        role = next(d for d in self.render() if d['kind'] == 'Role')
        self.assertEqual([rule['resources'] for rule in role['rules']], [['pods'], ['pods/log']])
        self.assertEqual(role['rules'][0]['verbs'], ['create', 'get', 'list', 'watch', 'delete'])
        self.assertEqual(role['rules'][1]['verbs'], ['get'])
        # The gateway must not be able to widen its own grant, read the catalog
        # credential, or raise the quota that caps it. Asserted rather than reviewed:
        # this Role is applied by an identity that could grant all three.
        granted = {resource for rule in role['rules'] for resource in rule['resources']}
        self.assertFalse(granted & {'roles', 'rolebindings', 'secrets', 'resourcequotas'})
        for rule in role['rules']:
            self.assertNotIn('*', rule['verbs'] + rule['resources'] + rule['apiGroups'])

    def test_a_binding_outside_the_namespace_is_refused(self):
        render = load('render_boundary', 'scripts/aws/render_boundary.py')
        escaping = [{'kind': 'RoleBinding', 'metadata': {'name': 'b', 'namespace': 'ours'},
                     'subjects': [{'kind': 'ServiceAccount', 'name': 'gateway',
                                   'namespace': 'theirs'}]}]
        with self.assertRaises(ValueError) as refused:
            render.placed(escaping, 'ours')
        self.assertIn('outside ours', str(refused.exception))

    def test_an_object_with_no_namespace_is_refused(self):
        render = load('render_boundary', 'scripts/aws/render_boundary.py')
        # Not an error to Kubernetes — it lands wherever the caller's context points —
        # so this is what stops a boundary applied with the wrong context from
        # granting pod-create somewhere unintended.
        with self.assertRaises(ValueError):
            render.placed([{'kind': 'Role', 'metadata': {'name': 'r'}}], 'ours')

    def test_an_unresolved_placeholder_fails_the_render(self):
        render = load('render_boundary', 'scripts/aws/render_boundary.py')
        with self.assertRaises(KeyError):
            render.render('namespace: ${cluster}\n', 'duckfleet-dev')

    def test_the_quota_bounds_only_what_every_container_declares(self):
        # A ResourceQuota is conditional: naming a resource makes it mandatory on every
        # container in the namespace. The gateway and PostgreSQL declare no
        # ephemeral-storage, so a quota that bounded it would reject them rather than
        # cap them — and the rejection would arrive as an unschedulable pod on a deploy.
        quota = next(d for d in self.render() if d['kind'] == 'ResourceQuota')
        bounded = {key.split('.', 1)[1] for key in quota['spec']['hard']
                   if '.' in key and '/' not in key}
        self.assertTrue(bounded, quota['spec']['hard'])
        rendered = chart()
        seen = 0
        for kind, name, container in containers(rendered):
            seen += 1
            for side in ['requests', 'limits']:
                declared = set(container['resources'][side])
                self.assertTrue(bounded <= declared,
                                f'{kind}/{name} {container["name"]} declares no '
                                f'{sorted(bounded - declared)} in {side}; the quota '
                                'would reject it')
        self.assertGreater(seen, 4, 'the chart rendered fewer containers than expected')

    def test_the_node_pool_is_only_rendered_for_a_real_cluster(self):
        # The local Kubernetes harness has no Karpenter, so a NodePool there fails on an
        # unknown kind rather than being ignored. It is therefore kept out of the
        # portable boundary that harness applies, and appears only when a NodeClass is
        # named — which is also the one host-cluster fact this needs.
        portable = {d['kind'] for d in self.render()}
        self.assertNotIn('NodePool', portable)
        self.assertIn('ResourceQuota', portable, portable)
        with_cloud = self.render_eks()
        self.assertIn('NodePool', {d['kind'] for d in with_cloud})
        pool = next(d for d in with_cloud if d['kind'] == 'NodePool')
        self.assertEqual(pool['spec']['template']['spec']['nodeClassRef']['name'], 'chosen')

    def test_the_node_pool_is_dedicated_and_capped(self):
        pool = next(d for d in self.render_eks() if d['kind'] == 'NodePool')
        spec = pool['spec']['template']['spec']
        # Tainted, so the pool is DuckFleet's in both directions: a neighbour cannot
        # consume capacity DuckFleet pays for. The chart's toleration must match, which
        # check-chart.py asserts from the other side.
        self.assertEqual(
            [(t['key'], t['value'], t['effect']) for t in spec['taints']],
            [('duckfleet.io/dedicated', 'true', 'NoSchedule')])
        wanted = {r['key'].rsplit('/', 1)[-1]: r for r in spec['requirements']}
        # `xlarge` is the whole point: the shared pool is capped to `large`, which is
        # 2 vCPU with ~1780m allocatable, so a 2-CPU worker claiming 2050m with
        # daemonset overhead could not be provisioned at all.
        self.assertIn('xlarge', wanted['instance-size']['values'])
        # On demand only. A worker holds a DuckLake write transaction, and spot
        # reclamation mid-statement is a rollback nobody asked for.
        self.assertEqual(wanted['capacity-type']['values'], ['on-demand'])
        # A ceiling, so a runaway cannot provision without bound.
        self.assertTrue(pool['spec']['limits']['cpu'])
        # WhenEmpty, not WhenEmptyOrUnderutilized: consolidating a busy node would end
        # a running query, gracefully or otherwise. An empty node still goes away,
        # which is what makes `suspended: true` stop costing money.
        self.assertEqual(pool['spec']['disruption']['consolidationPolicy'], 'WhenEmpty')

    def test_the_boundary_is_required_wherever_duckfleet_deploys(self):
        # The namespace label is applied by apply-boundary.sh alongside the grant, the
        # quota and the node pool, so its absence means a deploy is about to install a
        # release whose gateway cannot create the pods it advertises. Required by
        # default, and required on a cluster DuckFleet owns too: owning it changes who
        # may apply the boundary, not whether one is needed.
        target = (ROOT/'scripts/aws/target.env').read_text()
        self.assertRegex(target, r'DUCKFLEET_REQUIRE_BOOTSTRAP=\$\{[^:]+:-true\}')
        cluster = (ROOT/'infra/terraform/cluster/main.tf').read_text()
        self.assertIn('DUCKFLEET_REQUIRE_BOOTSTRAP=true', cluster)
        self.assertNotIn('DUCKFLEET_REQUIRE_BOOTSTRAP=false', cluster)
        # The one exception, and the reason it is overridable at all: the script that
        # establishes the boundary cannot require it to already exist.
        self.assertIn('DUCKFLEET_REQUIRE_BOOTSTRAP=false',
                      (ROOT/'scripts/aws/apply-boundary.sh').read_text())
        # The gate and the label have to be the same label. Renaming it on either side
        # leaves a deploy that can never satisfy its own precondition, and says only
        # "apply the boundary first" about a boundary that has been applied.
        namespace = next(d for d in self.render() if d['kind'] == 'Namespace')
        label, value = next(iter(namespace['metadata']['labels'].items()))
        common = (ROOT/'scripts/aws/common.sh').read_text()
        self.assertIn(label.replace('.', r'\.'), common, label)
        self.assertIn(f'== {value}', common)

    def test_a_cluster_scoped_object_must_be_named_for_duckfleet(self):
        render = load('render_boundary', 'scripts/aws/render_boundary.py')
        # Cluster-scoped objects sit outside the namespace that bounds everything else
        # here, so the only thing keeping the boundary bounded is the name.
        with self.assertRaises(ValueError) as refused:
            render.placed([{'kind': 'NodePool', 'metadata': {'name': 'general-purpose'}}],
                          'duckfleet-dev')
        self.assertIn('not named for DuckFleet', str(refused.exception))

    def test_pinning_the_pool_and_the_cpu_workaround_are_one_decision(self):
        # Three values, one choice. `workerCpuRequests` exists only to fit the shared
        # pool, whose `large` cap cannot provision a 2-CPU worker at all; the DuckFleet
        # pool can, which is why pinning it is what retires the override. Keeping both
        # would waste the pool's capacity on a Burstable pod; keeping neither would
        # leave the medium worker unprovisionable. Either state is coherent, so this
        # asserts they move together rather than asserting which one is current.
        values = yaml.safe_load((ROOT/'infra/helm/duckfleet/values.yaml').read_text())
        pinned = 'karpenter.sh/nodepool' in values['nodeSelector']
        tolerated = bool(values.get('tolerations'))
        self.assertEqual(pinned, tolerated,
                         'the DuckFleet pool must be pinned and tolerated together')
        # Whichever state, a request may sit below its limit but never above it: the
        # cgroup the worker asserts against comes from the limit, so a request above it
        # schedules onto a node that cannot honour what the worker checks for.
        workers = 0
        for kind, name, container in containers(chart()):
            if container['name'] != 'worker':
                continue
            workers += 1
            resources.assert_worker_resources(
                container['resources'],
                int(str(container['resources']['limits']['cpu']).rstrip('m')), name)
        self.assertGreaterEqual(workers, 1, 'expected at least the persistent worker')

    def test_the_harness_verifies_isolated_compute_not_the_deploy(self):
        deploy = (ROOT/'scripts/aws/deploy.sh').read_text()
        # The deploy identity cannot read RBAC, and cannot reach the gateway from
        # outside the cluster either: the boundary grants it pods create/get/list/
        # watch/delete but not exec or portforward. So it must not try — an earlier
        # version exec'd the gateway to read isolated_compute and failed the deploy
        # the moment the feature was enabled. The in-cluster harness asks instead.
        self.assertNotIn('can-i create roles', deploy)
        self.assertNotIn('exec deployment/server', deploy)
        self.assertNotIn('Bearer $DUCKFLEET_TOKEN', deploy)
        # The AWS harness, which runs in-cluster as the `harness` role of the Rust
        # server binary, asserts the feature is live: its entrypoint drives the
        # cold-start (isolated) path, and the shared harness inspects the gateway's
        # isolated_compute flag rather than the deploy doing it from outside.
        aws_harness = (ROOT/'server/src/harness/aws_harness.rs').read_text()
        self.assertIn('cold_start_checks', aws_harness)
        harness = (ROOT/'server/src/harness.rs').read_text()
        self.assertIn('isolated_compute', harness)

    def test_the_boundary_is_applied_by_an_operator_and_never_by_ci(self):
        script = (ROOT/'scripts/aws/apply-boundary.sh').read_text()
        # Checked before anything is applied, so running this with the deploy profile
        # fails whole rather than leaving a namespace and quota behind with no grant.
        self.assertIn('auth can-i create roles', script)
        self.assertIn('render_boundary.py', script)
        # The deploy lane must not apply it. A pipeline that established its own
        # boundary would be granting itself the capability, which is the whole thing
        # check-access.sh exists to prevent.
        workflow = (ROOT/'.github/workflows/aws-dev.yml').read_text()
        self.assertNotIn('apply-boundary', workflow)
        self.assertIn('check-access.sh', workflow)
        access = (ROOT/'scripts/aws/check-access.sh').read_text()
        self.assertIn('auth can-i patch deployments.apps', access)
        self.assertNotIn('subresource=scale', access)


class DatabaseRolePackagingTests(unittest.TestCase):
    def test_legacy_null_results_render_without_result_storage(self):
        rendered = chart(['--set', 'results=null'])
        self.assertNotIn('DUCKFLEET_RESULT_DATA_PATH', rendered)

    def test_suspension_rebases_legacy_releases_onto_current_defaults(self):
        script = (ROOT/'scripts/aws/suspend.sh').read_text()
        self.assertIn('--reset-then-reuse-values', script)
        self.assertNotIn('--reuse-values --set', script)

    def test_legacy_ownership_transfer_skips_table_owned_sequences(self):
        sql = (ROOT/'infra/helm/duckfleet/files/provision-database.sql').read_text()
        self.assertEqual(sql.count("c.relkind<>'S'"), 2)
        self.assertEqual(sql.count("d.deptype='a'"), 2)

    def test_legacy_database_upgrade_job_is_explicit_and_least_privilege(self):
        ordinary = [o for o in yaml.safe_load_all(chart()) if o]
        self.assertFalse(any(o['metadata']['name'] == 'duckfleet-database-provision' for o in ordinary))
        upgraded = [o for o in yaml.safe_load_all(chart(['--set', 'legacyDatabaseUpgrade=true'])) if o]
        jobs = [o for o in upgraded if o['metadata']['name'] == 'duckfleet-database-provision']
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job['spec']['backoffLimit'], 0)
        pod = job['spec']['template']['spec']
        self.assertEqual(pod['serviceAccountName'], 'duckfleet-no-aws')
        self.assertFalse(pod['automountServiceAccountToken'])
        container = pod['containers'][0]
        self.assertEqual(container['image'], 'postgres:17.6-bookworm')
        command = container['args'][0]
        self.assertIn('/etc/duckfleet/provision-database.sql', command)
        self.assertIn('/dev/termination-log', command)
        self.assertIn("sed -n -E", command)
        self.assertNotIn('envFrom', container)
        env = container['env']
        self.assertEqual(len({item['name'] for item in env}), len(env))
        refs = {item['name']: item['valueFrom']['secretKeyRef']['key']
                for item in env if 'secretKeyRef' in item.get('valueFrom', {})}
        expected = {'PGPASSWORD': 'POSTGRES_PASSWORD'}
        expected.update({'DUCKFLEET_DB_'+part.upper()+'_PASSWORD': 'DATABASE_'+part.upper()+'_PASSWORD'
                         for part in ('migrator', 'gateway', 'worker', 'maintainer', 'catalog', 'catalog_reader')})
        self.assertEqual(refs, expected)
        values = {item['name']: item.get('value') for item in env}
        self.assertEqual(values['DUCKFLEET_ADOPT_DATABASE_OWNER'], 'yes')
        self.assertEqual(values['DUCKFLEET_PLATFORM_DATABASE'], 'platform')
        self.assertEqual(values['DUCKFLEET_LAKE_DATABASE'], 'lake')
        config = next(v['configMap'] for v in pod['volumes'] if v['name'] == 'provisioning')
        self.assertEqual(config['name'], 'duckfleet-postgres-init')
        self.assertEqual(config['items'], [{'key': 'provision-database.sql', 'path': 'provision-database.sql'}])

    def test_legacy_upgrade_is_opt_in_for_workflow_dispatch_only(self):
        workflow = yaml.safe_load((ROOT/'.github/workflows/aws-dev.yml').read_text())
        option = workflow[True]['workflow_dispatch']['inputs']['upgrade_legacy_database']
        self.assertFalse(option['default'])
        self.assertEqual(option['type'], 'boolean')
        deploy = workflow['jobs']['deploy']
        self.assertIn('inputs.upgrade_legacy_database', deploy['env']['DUCKFLEET_UPGRADE_LEGACY_DATABASE'])
        script = (ROOT/'scripts/aws/deploy.sh').read_text()
        self.assertIn('"${DUCKFLEET_UPGRADE_LEGACY_DATABASE:-false}" == true', script)
        self.assertIn('bash scripts/aws/upgrade-database-roles.sh', script)
        upgrade = (ROOT/'scripts/aws/upgrade-database-roles.sh').read_text()
        self.assertIn('@.type=="Complete"', upgrade)
        self.assertIn('@.type=="Failed"', upgrade)
        self.assertIn('.state.terminated.message', upgrade)
        self.assertNotIn('logs', upgrade)

    def test_helm_schema_rejects_invalid_roles_and_authorization_at_the_top_level(self):
        cases = [
            ('databaseRoles', {'worker': 'Invalid-Role'}, 'databaseRoles.worker'),
            ('databaseRoles', {'worker': 12}, 'databaseRoles.worker'),
            ('databaseRoles', {'typo': 'fixture_role'}, 'databaseRoles'),
            ('authorization.defaultPersistentGrant.cpu', 65, 'authorization.defaultPersistentGrant.cpu'),
            ('authorization.defaultPersistentGrant.cpu', 1.5, 'authorization.defaultPersistentGrant.cpu'),
            ('authorization.defaultPersistentGrant.memory', '1GB', 'authorization.defaultPersistentGrant.memory'),
            ('authorization.unknown', True, 'authorization'),
            ('admissionLimits.default.databaseRoles', {'worker': 'fixture_role'}, 'admissionLimits.default'),
        ]
        for key, value, error_path in cases:
            with self.subTest(key=key, value=value):
                with self.assertRaises(subprocess.CalledProcessError) as error:
                    chart(['--set-json', key+'='+json.dumps(value)])
                self.assertIn('schema', error.exception.stderr)
                # Helm validators render the same path as dotted properties or
                # a JSON pointer, depending on the installed Helm version.
                paths = (error_path, '/' + error_path.replace('.', '/'))
                self.assertTrue(any(path in error.exception.stderr for path in paths),
                                error.exception.stderr)
        chart(['--set-json', 'databaseRoles='+json.dumps({
            part: 'fixture_'+part for part in ('migrator','gateway','worker','maintainer','catalog','catalog_reader')})])

    def test_namespace_roles_are_distinct_bounded_and_do_not_alias_sanitized_names(self):
        roles = load('database_roles', 'scripts/database_roles.py')
        first = roles.role_names('a-'+'b'*60)
        second = roles.role_names('a-'+'b'*59+'c')
        self.assertEqual(len(set(first.values())), 6)
        self.assertTrue(all(len(name)<=63 for name in first.values()))
        self.assertFalse(set(first.values()) & set(second.values()))
        with self.assertRaises(ValueError):
            roles.role_names('Bad/Namespace')

    def test_generated_credentials_and_retarget_keep_component_identities_separate(self):
        roles = load('database_roles', 'scripts/database_roles.py')
        document = roles.secret('fixture')
        roles.check_secret(document)
        data = document['stringData']
        self.assertIn('dbname=lake', data['CATALOG_READER_DSN'])
        self.assertNotIn('dbname=platform', data['CATALOG_READER_DSN'])
        passwords = [data['POSTGRES_PASSWORD'],data['DUCKFLEET_TOKEN'],data['DUCKFLEET_INTERNAL_TOKEN']]
        passwords += [data['DATABASE_'+part.upper()+'_PASSWORD'] for part in roles.PARTS]
        self.assertEqual(len(passwords),len(set(passwords)))
        changed = roles.retarget(document,'external-postgres')['stringData']
        for key in roles.DSN_KEYS.values():
            self.assertEqual(changed[key],data[key].replace('host=postgres','host=external-postgres'))
        with self.assertRaises(ValueError):
            roles.check_secret({'stringData':{'PLATFORM_DSN':'old-owner'}})
        data['DUCKFLEET_INTERNAL_TOKEN']=data['DUCKFLEET_TOKEN']
        with self.assertRaises(ValueError):
            roles.check_secret(document)

    def test_secret_upgrade_is_repeatable_and_preserves_admin_and_bootstrap(self):
        original = {'metadata':{'name':'duckfleet-dev'},'stringData':{
            'POSTGRES_PASSWORD':'admin-preserved','DUCKFLEET_TOKEN':'public-preserved',
            'CATALOG_DSN':'host=postgres user=old_owner password=old dbname=lake'}}
        command=['python3',str(ROOT/'scripts/database_roles.py'),'fixture','--upgrade']
        upgraded=json.loads(subprocess.check_output(command,input=json.dumps(original),text=True))
        repeated=json.loads(subprocess.check_output(command,input=json.dumps(upgraded),text=True))
        self.assertEqual(upgraded,repeated)
        self.assertEqual(upgraded['stringData']['POSTGRES_PASSWORD'],'admin-preserved')
        self.assertEqual(upgraded['stringData']['DUCKFLEET_TOKEN'],'public-preserved')
        self.assertNotIn('old_owner',upgraded['stringData']['CATALOG_DSN'])

    def test_real_render_keeps_worker_and_isolated_secrets_restricted(self):
        objects={(o['kind'],o['metadata']['name']):o for o in yaml.safe_load_all(chart()) if o}
        pods=[o['spec']['template']['spec'] for (kind,name),o in objects.items()
              if kind=='Deployment' and name.startswith('worker-')]
        isolated=json.loads(objects['ConfigMap','duckfleet-isolated-pod']['data']['pod.json'])
        pods.append(isolated['spec'])
        for pod in pods:
            env=pod['containers'][0]['env']
            names={item['name'] for item in env}
            refs={item['name']:item['valueFrom']['secretKeyRef']['key'] for item in env if 'secretKeyRef' in item.get('valueFrom',{})}
            self.assertNotIn('DUCKFLEET_TOKEN',names)
            self.assertNotIn('DUCKFLEET_MAINTENANCE_DSN',names)
            self.assertEqual(refs['PLATFORM_DSN'],'PLATFORM_WORKER_DSN')
            self.assertEqual(refs['DUCKFLEET_INTERNAL_TOKEN'],'DUCKFLEET_INTERNAL_TOKEN')
            self.assertFalse(any('secret' in volume for volume in pod.get('volumes',[])))
        migration=next(o for (kind,_),o in objects.items() if kind=='Job' and o['metadata'].get('labels',{}).get('app')=='duckfleet-platform-migrate')
        env=migration['spec']['template']['spec']['containers'][0]['env']
        refs={item['valueFrom']['secretKeyRef']['key'] for item in env if 'secretKeyRef' in item.get('valueFrom',{})}
        self.assertEqual(refs,{'PLATFORM_MIGRATION_DSN'})
        role_env=json.loads(next(item['value'] for item in env if item['name']=='DUCKFLEET_DATABASE_ROLES_JSON'))
        roles=load('database_roles','scripts/database_roles.py').role_names('duckfleet-dev')
        self.assertEqual(role_env,{key:roles[key] for key in ('gateway','worker','maintainer')})

    def test_live_pod_posture_rejects_whole_secret_or_worker_bootstrap(self):
        check=load('role_posture','scripts/kind/assert_role_posture.py')
        objects=[o for o in yaml.safe_load_all(chart()) if o]
        pods=[{'metadata':o['spec']['template']['metadata'],'spec':o['spec']['template']['spec']}
              for o in objects if o['kind']=='Deployment']
        counts=check.validate(pods)
        self.assertEqual(counts['gateway'],1)
        self.assertGreater(counts['worker'],0)
        worker=next(p for p in pods if p['metadata']['labels']['app']=='duckfleet-worker')
        worker['spec']['containers'][0]['env'].append({'name':'DUCKFLEET_TOKEN','value':'forbidden-bootstrap'})
        with self.assertRaises(AssertionError):
            check.validate(pods)
        worker['spec']['containers'][0]['env'].pop()
        worker['spec']['containers'][0]['envFrom']=[{'secretRef':{'name':'shared'}}]
        with self.assertRaises(AssertionError):
            check.validate(pods)

    def test_custom_default_requires_explicit_reviewed_compute_envelope(self):
        pools={'wide':{'cpu':2,'memory':'3Gi','disk':'4Gi','workers':1}}
        args=['--set-json','persistentPools='+json.dumps(pools),'--set','defaultPool=wide']
        with self.assertRaises(subprocess.CalledProcessError):
            chart(args)
        chart(args+['--set-json','authorization.defaultPersistentGrant='+json.dumps(dict(pool='wide',cpu=2,memory='3Gi',disk='4Gi'))])

    def test_compose_provisions_before_migration_and_compute_gets_no_owner_secret(self):
        services=yaml.safe_load((ROOT/'compose.yaml').read_text())['services']
        self.assertEqual(services['platform-migrate']['depends_on']['platform-provision']['condition'],'service_completed_successfully')
        for name in ('worker-small','worker-small-b','worker-pool-interactive-0'):
            env=services[name]['environment']
            self.assertNotIn('DUCKFLEET_TOKEN',env)
            self.assertNotIn('DUCKFLEET_MAINTENANCE_DSN',env)
            self.assertIn('user=duckfleet_worker ',env['PLATFORM_DSN'])
            self.assertIn('user=duckfleet_catalog ',env['CATALOG_DSN'])
        reader = services['worker-small-reader']['environment']
        self.assertEqual(reader['DUCKFLEET_AUTHORITY'], 'reader')
        self.assertIn('user=duckfleet_catalog_reader ', reader['CATALOG_READER_DSN'])
        self.assertNotIn('CATALOG_DSN', reader)
        self.assertNotIn('S3_ACCESS_KEY', reader)
        self.assertNotIn('S3_SECRET_KEY', reader)
        self.assertFalse(any(key.startswith('DUCKFLEET_RESULT_') for key in reader))
        self.assertIn('user=duckfleet_migrator ',services['platform-migrate']['environment']['PLATFORM_DSN'])
        self.assertIn('DUCKFLEET_INTERNAL_TEST_TOKEN',services['dbt']['environment'])

    def test_namespace_probe_uses_current_catalog_role_and_live_service(self):
        roles = load('probe_database_roles', 'scripts/database_roles.py')
        probe = load('team_namespace_probe', 'scripts/kind/team_namespace_probe.py')
        document = roles.secret('duckfleet-dev', host='external-postgres')
        dsn = document['stringData']['CATALOG_DSN']
        query = probe.catalog_probe(dsn, 'duckfleet-dev')
        self.assertIn('host=external-postgres.duckfleet-dev.svc.cluster.local ', query)
        self.assertIn('user=' + roles.role_names('duckfleet-dev')['catalog'] + ' ', query)
        self.assertNotIn('user=duckfleet ', query)
        self.assertIn(" connect_timeout=3'", query)
        self.assertIn('ATTACH ', query)
        self.assertIn('TYPE postgres, READ_ONLY', query)
        self.assertIn('qualification_catalog.public.ducklake_snapshot', query)
        self.assertEqual(query.count('connect_timeout='), 1)
        with self.assertRaises(ValueError):
            probe.catalog_probe(dsn + ' host=obsolete', 'duckfleet-dev')
        with self.assertRaises(ValueError):
            probe.catalog_probe(dsn.replace('host=external-postgres', "host=\"bad'host\""), 'duckfleet-dev')

    def test_namespace_probe_rejects_dns_authentication_and_redaction_false_positives(self):
        probe = load('team_denial_probe', 'scripts/kind/team_namespace_probe.py')
        for message in ('could not translate host name', 'password authentication failed',
                        'postgres_scan does not exist', 'relation ducklake_snapshot does not exist'):
            with self.subTest(message=message), self.assertRaises(AssertionError):
                probe.verify_denial({'failure_kind': 'sql_error', 'error': message}, 'fixture-secret')
        probe.verify_denial({'failure_kind': 'sql_error', 'error': 'connection timed out'}, 'fixture-secret')
        probe.verify_denial({'failure_kind': 'sql_error', 'error': 'connection to server failed: timeout expired'}, 'fixture-secret')
        probe.verify_denial({'failure_kind': 'timeout', 'error': 'query deadline elapsed'}, 'fixture-secret')
        with self.assertRaises(AssertionError):
            probe.verify_denial({'failure_kind': 'timeout', 'error': 'fixture-secret'}, 'fixture-secret')
        calls = []
        def failed_positive(port, token, method, path, payload=None, timeout=70):
            calls.append((method, path))
            if path == '/v1/sessions':
                return b'{"session_id":"fixture"}'
            if method == 'POST':
                raise RuntimeError('positive endpoint unavailable')
            return b'{}'
        with mock.patch.object(probe, 'call', side_effect=failed_positive), self.assertRaises(RuntimeError):
            probe.positive_control(1, 'fixture-token', 'SELECT 1', 'fixture-secret')
        self.assertEqual(calls[-1], ('DELETE', '/v1/sessions/fixture'))


if __name__ == '__main__':
    unittest.main()
