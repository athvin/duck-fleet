"""Safety regressions for the live deployment lane; no AWS calls."""
import importlib.util
import subprocess
import unittest
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT/path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

guard = load('plan_guard', 'scripts/aws/plan_guard.py')
rollout = load('wait_rollout', 'scripts/aws/wait-rollout.py')

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
        deploy = workflow['jobs']['deploy']
        self.assertEqual(deploy['if'], "github.ref == 'refs/heads/main'")
        steps = deploy['steps']
        checks = next(i for i,s in enumerate(steps) if s.get('run') == 'python3 scripts/aws/wait-checks.py')
        credentials = next(i for i,s in enumerate(steps) if s.get('uses','').startswith('aws-actions/'))
        self.assertLess(checks, credentials)
        self.assertIn('always()', workflow['jobs']['suspend']['if'])
        self.assertIn('deployment_started', workflow['jobs']['suspend']['if'])
        for job in workflow['jobs'].values():
            for step in job['steps']:
                if 'uses' in step:
                    self.assertRegex(step['uses'], r'@[0-9a-f]{40}$')
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

if __name__ == '__main__':
    unittest.main()
