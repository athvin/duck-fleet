# Bounded persistent-pool autoscaling

Autoscaling is optional. `autoscaling: {}` (default) preserves fixed-capacity
persistent workers and needs no controller or monitoring service. Configure one
entry per existing `persistentPools` pool; every pool's `workers` value is the
hard maximum endpoint count. This scales whole queries across DuckDB processes,
never one query across processes.

```yaml
persistentPools:
  interactive: {cpu: 2, memory: 3Gi, disk: 4Gi, workers: 4}
autoscaling:
  interactive:
    min_workers: 1
    max_workers: 4
    max_cpu: 8
    max_memory_bytes: 12884901888
    queue_wait_target_ms: 5000
    scale_up_cooldown_seconds: 30
    scale_down_cooldown_seconds: 300
    idle_seconds: 300
    paused: false
```

The gateway reads durable queue age, eligible execution capacity and worker boot
state every five seconds. Oldest queued wait at or above the target can add one
worker after scale-up cooldown. Scale-down requires no active queries or open
sessions, the idle window and scale-down cooldown. Minimum capacity is restored
even while paused. A paused controller takes no other action. CPU/memory bounds
include the additional worker before every scale-up.

Every observation and decision is logged with pool, desired capacity and reason.
Deployment state is re-read immediately before mutation; stale replica counts,
unexpected external changes and API/provisioning failures retain current capacity
instead of retrying blindly. `/v1/workers` reports actual membership without a
gateway restart, and `/v1/compute` retains provisioned usage and lifecycle cost.
Draining and unavailable workers receive no new sessions.

Scale-down first calls the worker's authenticated `/v1/drain`, which removes it
from admission. It is permitted only with no active query or open session, so
statement execution and transaction affinity are never moved. Removed workers
recover their reservation/history through their existing stable identity.

The Kubernetes Role in `infra/kubernetes/boundary` must allow `get`, `list` and
`patch` on `deployments` as well as the existing pod grant. The cluster must
actually provision the requested worker resources; DuckFleet does not create
nodes or promise a cloud price. Capacity-provider refusal remains visible in the
worker/provisioning state. Keep fixed mode on clusters without those permissions
or with unpredictable scheduling.

Rollback: set `paused: true`, let active sessions finish, then remove the
autoscaling entries and scale the affected Deployments to their previous fixed
replica counts. No session or transaction migration is implied.
