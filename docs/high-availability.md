# Gateway ownership and high availability

The default remains one gateway with a `Recreate` deployment strategy. It has no
extra durable-ownership traffic beyond the session records written on open.
Multiple gateways are supported only with `highAvailability.enabled=true`, which
renders two replicas and `RollingUpdate`.

Each session records its gateway pod identity and random epoch, worker and boot
identity, principal, placement, operation, and a 15-second PostgreSQL lease. The
owner renews that lease every five seconds. Another gateway can reconstruct a
read/query route from this record, but it cannot close a live session owned by a
different or expired gateway. That close returns an explicit recoverable routing
error instead of deleting the worker. Worker-confirmed closure is safe to clean
up through either replica.

Startup cleanup deletes only unowned work, durable jobs, and sessions whose lease
has expired. A five-second reconciliation retries expired leases after a replacement
starts; it does not sweep newly starting, unregistered pods. A second gateway does
not delete a live sibling's isolated pod. Admission and credential policy remain in
PostgreSQL rather than becoming independent per-process allowances.

## Availability boundary

Gateway loss and DuckDB worker loss are different. Isolated pods have an owner
reference to their creating gateway, so losing that gateway lets Kubernetes
collect the worker. An in-flight statement can consequently have an unknown
outcome; DuckFleet does not migrate a transaction or replay it. Persistent worker
death also loses its in-memory transaction state and follows the existing
classified recovery contract.

A platform-database outage refuses new ownership registration and closes the
worker/session opened during that outage. A lost registration commit never
justifies silently opening a replacement transaction. Existing requests retain
their original identities and unknown-commit semantics.

For meaningful HA, place PostgreSQL and object storage outside the two gateway
pods' failure domain. The bundled single PostgreSQL is a development fixture, not
an HA control plane.

## Rollback

Stop new admissions and let active sessions finish. Disable any autoscaling,
remove `highAvailability`, and roll the release back to one gateway. Do not force
a single remaining replica while sessions are active or the database is unavailable;
expired leases must be allowed to settle before isolated compute is removed.
No catalog migration or transaction rewrite is required.
