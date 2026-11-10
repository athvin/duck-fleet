# Managed DuckLake: Founding CTO Blueprint

- **Status:** Pre-MVP architecture and business planning
- **Date:** November 9, 2026
- **Working names:** DrakeLake, FatherDuck, DuckFleet (not trademark/domain cleared)
- **Audience:** Founder/CTO, initial engineers, design partners
- **Document type:** Decision-oriented product, engineering, and commercial plan

**Executive thesis:** Build a Rust-first, dbt-native managed DuckDB/DuckLake platform that routes individual dbt models to persistent, pooled, or burst compute, while providing the operational layer customers expect from a warehouse: query history, performance profiling, cost attribution, budgets, secure sharing, backups, and automated insights. Sell predictable baseline capacity with metered, budget-controlled elasticity. The competitive wedge is per-model compute policy plus transparent cost accounting, not simply “hosted DuckDB.”

## 1. Executive decision brief

### 1.1 What we are building

A managed analytics platform on AWS. Each customer gets a logically isolated DuckLake (PostgreSQL metadata catalog and S3 Parquet storage) and a configurable execution environment. A custom dbt adapter accepts model-specific compute policy. A Rust gateway/router schedules SQL execution on a provisioned persistent DuckDB worker or an appropriately sized Kubernetes worker. The same Rust control plane records execution, meters costs, enforces budgets, and exposes a customer console and internal operator console.

The primary user journey: connect an existing dbt project → provision a managed DuckLake → run ordinary models on baseline compute → annotate expensive models with CPU/RAM/isolation preferences → inspect query history and costs → adjust resource policy → publish curated read-only data to consumers when needed.

### 1.2 Why this might win

- Workload-specific control: one model can run on 2 vCPU / 4 GiB while another requests 32 vCPU / 128 GiB, without the user managing AWS or Kubernetes.
- Predictable spend: annual minimum commitment, prepaid elastic usage, per-job limits, and explicit budget enforcement.
- Warehouse ergonomics: query history, profiling, model-level and tag-level chargeback, maintenance, snapshots, and audit trails.
- dbt-native adoption: preserve existing DAGs, refs, tests, lineage, and familiar CLI workflows.
- Data distribution: eventually publish authorized views, SQL endpoints, and managed read-only APIs.

### 1.3 What we are not building initially

Not a general-purpose transactional database; not a fully compatible Snowflake clone; not a general distributed SQL engine; not a complete CDC connector ecosystem; not an unrestricted public SQL proxy; not an AI-first recommendation product; not a multi-region active-active service. DuckDB query execution is primarily single-node: a larger worker increases per-query resources but does not automatically distribute one query across multiple pods.

### 1.4 Initial architecture decisions

| Decision | Proposed choice | Confidence / caveat |
| --- | --- | --- |
| Main analytical engine | DuckDB | High; benchmark target workloads |
| Shared table storage | DuckLake on S3 + PostgreSQL catalog | High conceptually; verify concurrent materialization behavior |
| Platform language | Rust for gateway, scheduler, worker, telemetry | High |
| dbt integration | Thin Python adapter + custom materialization/execution context | High conceptually; requires spike |
| API | Rust Axum, authenticated HTTP/gRPC-style internal contracts | High |
| Worker orchestration | EKS + kube-rs; Karpenter for node scaling | Provisional; compare simpler ECS/EC2 MVP |
| Execution modes | Persistent first, dedicated burst second, warm pool later | High |
| Billing | Annual baseline commitment + prepaid variable usage | High as design hypothesis; prices unvalidated |
| Telemetry | Durable event ledger + Postgres hot store + S3 archive | High |
| External data sharing | Explicit read-only published views / parameterized APIs | Early release, not initial core |
| CDC | PostgreSQL-first, optional, deferred | Medium |

## 2. Market positioning and customer profile

### 2.1 Positioning

“The managed DuckLake built for dbt: your baseline compute, your per-model burst resources, your cost controls.”

The “poor man's Snowflake” phrase is useful internally to explain the ambition, but external marketing should emphasize simple, affordable, controllable analytics infrastructure rather than suggesting a feature-for-feature substitute. The goal is a useful production warehouse for teams that do not need Snowflake's breadth.

### 2.2 Ideal initial customer

Small and midsize data teams (roughly 1–5 analytics/data engineers) using dbt, with batch-oriented workloads, data in or near S3, uneven model resource requirements, and a desire for transparent per-model cost. SaaS companies running per-tenant transformations are especially attractive because tagging, isolation, and chargeback matter to them.

### 2.3 Alternatives and honest differentiation

- MotherDuck: managed DuckDB with dbt and scalable compute options. Do not claim it lacks scaling, isolation, or query history without checking current product documentation. Differentiate on explicit dbt-model CPU/RAM placement, transparent worker policy, and budget enforcement.
- Snowflake: mature warehouse with separate compute, resource governance, account usage/query history, time travel, sharing, and many enterprise controls. We aim for a focused subset with simpler economics.
- Databricks: rich lakehouse management, system tables, workload orchestration, lineage, and optimization. We aim for smaller operational scope and a dbt-specific developer experience.
- Self-managed DuckDB/DuckLake: low software cost but customers own security, orchestration, availability, profiling, metering, backup, and sharing. We sell the managed operational layer.

Validation requirement: interview at least 10–15 target teams before locking pricing; benchmark real dbt DAGs against MotherDuck and Snowflake; establish willingness to pay for per-model compute and observability rather than assuming it.

## 3. Product surface and core workflows

### 3.1 Customer onboarding

1. Create organization, workspace, and environment.
2. Choose a baseline compute profile and an annual minimum commitment.
3. Establish payment method and prepaid elastic balance; set limits.
4. Provision S3 namespace, catalog isolation, credentials, and compute identity.
5. Install dbt adapter and configure profile.
6. Run first dbt model; see model execution and cost in console.
7. Configure compute policies, tags, retention, alerts, and optional sharing.

### 3.2 dbt configuration design

Prefer adapter-owned configuration under dbt's supported meta namespace rather than claiming custom CPU fields are standard dbt options. Exact syntax below is illustrative and must be validated against the chosen dbt version and adapter contracts.

```yaml
# dbt_project.yml — proposed interface
models:
  analytics_project:
    +meta:
      platform:
        compute: persistent
        tags:
          team: analytics
          environment: prod
    marts:
      +meta:
        platform:
          compute: auto
    heavy:
      +meta:
        platform:
          compute: burst
          cpu: 32
          memory: 128Gi
          isolation: dedicated_pod
          max_cost_usd: 8
          tags:
            cost_center: finance
```

```sql
-- models/marts/fct_customer_ltv.sql
{{ config(materialized='incremental', meta={
    'platform': {
      'compute': 'burst',
      'cpu': 16,
      'memory': '64Gi',
      'tags': {'workload': 'customer_ltv'}
    }
}) }}
SELECT customer_id, SUM(amount) AS lifetime_value
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
```

Resolution precedence: organization defaults → environment → project → directory → model → explicitly authorized invocation overrides. Snapshot resolved policy and tags into each execution record; changing a tag tomorrow must not rewrite yesterday's costs. Keep platform billing tags distinct from native dbt selection tags.

### 3.3 Logical model execution versus SQL statements

A dbt model may issue hooks, temp-table DDL, incremental merge/update statements, cleanup, and tests. Do not naively route each statement independently. Create a logical model_run_id encompassing the materialization lifecycle, linked to one or more query_id records. Route the model lifecycle to a consistent execution context and preserve dbt dependency semantics. Tests and ad hoc SQL may be separate execution kinds.

### 3.4 User interface areas

- Workspace overview: health, active queries, recent jobs, spend, budget remaining.
- Query Explorer: SQL, status, model, tags, execution profile, memory/spill, retries, worker, cost.
- Cost Explorer: baseline, burst, S3, catalog, network, serving; group by arbitrary tag combinations.
- Compute: persistent size, profiles, queues, active pods, model overrides, quotas.
- Insights: regressions, overprovisioning, spill, anomalies, actionable recommendations.
- Data: catalogs, tables, snapshots, maintenance, freshness.
- Sharing: published data products, roles, service accounts, keys, quotas.
- Billing: annual commitment, prepaid balance, rate card, invoices, hard caps.

## 4. Technical architecture

### 4.1 Major boundaries

```text
Developer / CI
  dbt Core + thin Python adapter
          |
          v
     Rust API Gateway (Axum)
     auth, tenancy, execution API, cancellation, streaming
          |
          v
     Rust Policy + Compute Router
     dbt context, tags, CPU/RAM, budgets, quotas
          |
          v
     Rust Scheduler / Queue
     admission, concurrency, retries, worker selection
          |
          +----> Persistent DuckDB worker (baseline)
          +----> Warm pooled worker (later)
          +----> Ephemeral DuckDB worker (EKS Job)
                        ^
                        |
              kube-rs controller + Karpenter

     All workers attach to shared customer DuckLake:
          PostgreSQL metadata catalog + S3 Parquet objects

     Worker events --> durable queue --> Rust telemetry consumer
                              |--> Postgres indexed history
                              |--> S3 archival profiles/events
                              |--> billing/metering ledger
                              |--> insights and UI

     Optional later: PostgreSQL CDC --> Rust ingestion -->
          append-only S3 changes + DuckLake raw/current tables
```

### 4.2 Rust workspace (suggested)

```text
workspace/
  crates/
    api/                  # Axum public/private APIs
    identity/             # tenants, principals, credentials, roles
    execution-contracts/  # typed request, status, events
    router/               # compute policy resolution
    scheduler/            # admission, queue, retries
    k8s-controller/       # kube-rs Jobs, worker lifecycle
    worker/               # embedded DuckDB executor
    ducklake-catalog/     # catalog connection lifecycle
    telemetry/            # execution metrics and profiling
    metering/             # immutable billable usage events
    cost-reconciliation/  # AWS CUR / Data Exports allocation
    pricing/              # versioned rate cards, credits
    budgets/              # reservations and hard limits
    sharing/              # authorized read-only data access (later)
    insights/             # batch analytics and recommendations
    cdc/                  # PostgreSQL CDC (later)
    common/
  adapters/dbt-platform/  # thin Python dbt adapter
  infra/                  # Terraform / Kubernetes manifests
  console/                # web frontend
  tests/                  # integration, load, failure, security
```

Start as a modular Rust monolith for gateway/scheduler/ledger where practical; split into services when independent scaling or reliability justifies it. The Python adapter cannot realistically be eliminated because dbt's adapter integration is Python-facing, but heavy orchestration and SQL execution remain Rust-owned.

### 4.3 Execution request contract

```rust
// Illustrative shape, not compile-ready API
struct ExecutionRequest {
    tenant_id: TenantId,
    request_id: RequestId,       // idempotency
    invocation_id: Option<String>,
    model_id: Option<String>,
    execution_kind: ExecutionKind,
    sql_or_plan: ExecutionPayload,
    compute_policy: ComputePolicy,
    resolved_tags: HashMap<String, String>,
    timeout_seconds: u64,
    max_cost_usd: Option<Decimal>,
    principal_id: PrincipalId,
}
```

Server, not client, resolves authorization and final policy. Every execution has an immutable identity and state machine: accepted → admitted/queued → assigned → running → succeeded|failed|canceled|timed_out. Terminal state transitions must be idempotent. Persist accepted execution before dispatch; retain enough detail to reconcile after a worker disappears.

### 4.4 Execution modes

Persistent: long-running per-customer logical worker, provisioned resource profile, best cache reuse and low latency. Storage authority remains S3 + DuckLake catalog, not local .duckdb files.

Dedicated burst: ephemeral Kubernetes Job with requested CPU/RAM and configured scratch/spill disk. Pod-level isolation is not the same as node isolation. Node acquisition and teardown overhead must be measured and billed/accounted for explicitly.

Pooled: pre-warmed workers in fixed resource classes for short tasks. Attractive for latency and utilization, but more complex isolation and attribution; defer until data justifies it.

Auto: later chooses based on observed workload history, predicted memory/CPU, queue time, startup cost, and budget. Initial policy should be deterministic.

### 4.5 AWS infrastructure

- EKS for worker scheduling if the proof of concept validates need; consider ECS/EC2 as an MVP alternative to reduce control-plane and operational complexity.
- Karpenter for node autoscaling, with node pools by resource family and optional Spot for retry-safe workloads.
- S3 for Parquet data, snapshot history, audit archives, CDC changes, and controlled exports.
- PostgreSQL/RDS for DuckLake metadata and separate platform operational databases. Avoid mixing billing authority with customer catalogs.
- SQS or another durable queue for jobs/events; design for duplicate delivery and out-of-order messages.
- IAM roles for service accounts/pod identities, KMS, Secrets Manager, private subnets, S3 gateway endpoints, observability tooling.
- EBS or local scratch capacity for DuckDB spill; specify disk limits and cleanup behavior.

Tenancy: shared control plane; logically isolated customer data and catalog; strict IAM/prefix boundaries. Decide whether small customers share physical nodes. Dedicated nodes or private deployments are premium features, not default economics.

### 4.6 Concurrency, correctness, and recovery

Critical validation: concurrent DuckDB processes against a common DuckLake catalog; catalog commit conflicts; incremental materializations; temp objects; multi-statement hooks; stale worker snapshots; retry after partial completion; schema evolution; snapshot cleanup versus active readers. Do not equate DuckLake transaction support with automatic correctness for arbitrary parallel dbt workflows.

Retries require idempotency keys, commit-awareness, and well-defined compensation. A timed-out worker might still be executing: cancellation must propagate, then terminal state must be reconciled. Distinguish transient infrastructure retries from unsafe SQL retries.

## 5. Observability and query history

### 5.1 Product principle

Query history is not an add-on. It is the shared foundation for debugging, customer chargeback, pricing, auto-sizing, security audit, and the operator profitability dashboard. Capture every managed query, not only dbt models, and link SQL statements to logical model runs.

### 5.2 Execution telemetry schema

| Dimension | Example fields |
| --- | --- |
| Identity | tenant, environment, principal, API key ID, dbt invocation, model unique ID |
| Lifecycle | accepted/queued/started/finished, status, error, retry, cancellation |
| Placement | worker, node, instance type, mode, CPU/RAM requests/limits, isolation |
| Query | SQL text or secured reference, query hash, parameters, tags, input/output relations |
| Performance | wall time, queue time, CPU time, peak memory, spill bytes, S3 bytes, rows returned |
| Economics | rate-card version, metered resource-seconds, allocated infrastructure cost, billable amount |
| Security | authorization decision, data objects accessed, token identity, client IP as permitted |

DuckDB JSON profiling and EXPLAIN supply valuable raw information, but a managed multi-worker history service must persist, index, secure, and correlate it. Operator times in parallel plans may overlap; do not add them blindly as wall-clock time. Profiling overhead should be configurable.

### 5.3 Storage and retention

- Operational Postgres: indexed recent executions, tags, statuses, aggregates, and pointers to profiles.
- S3: immutable/append-only event archives, query plans, JSON profiles, historical Parquet analytics.
- Retention: define tier-based periods and privacy controls; support SQL text redaction and secure deletion where legally required.
- Queryable system views: query_history, model_runs, compute_usage, model_costs, access_history, billing_usage, budgets, insights.

These are platform-provided virtual/system views, not native DuckDB tables. Design query access controls and freshness SLAs.

### 5.4 Custom tags

Support team, tenant, cost_center, environment, product, workload, and arbitrary custom keys. Attach at organization/project/model/invocation levels, with deterministic override rules. Preserve resolved tags on the immutable execution record. Cap key/value sizes and cardinality to prevent cost explosion. Allow grouped spend, success rate, latency, and capacity metrics by tag combinations.

### 5.5 Insights cadence

- Real time / near-real time: active jobs, failures, OOM, queue depth, budget consumption, anomalous spend alerts, cancellation.
- Nightly: model regressions, peak-memory trends, overprovisioned CPU/RAM, small-file maintenance, idle capacity, per-tag spend anomalies, potential right-sizing.
- Later: trial resource changes, compare outcomes, estimate confidence and savings, optional automated policy changes with approval.

Start with rules/statistics, not generative AI. Every recommendation links to supporting queries, profiles, sample size, and estimated impact. Distinguish observed facts from inferred savings.

## 6. Managed warehouse conveniences

### 6.1 MVP essentials

Durable query history; JSON execution profiles; job history; cost per model/tag; cancellation and timeouts; concurrency quotas; budget enforcement; basic managed backups; worker health and retry-safe recovery; schema/catalog administration.

### 6.2 Early commercial release

DuckLake snapshot browsing and guided rollback; retention and cleanup policies; automatic small-file maintenance where supported; model and table lineage; query access history; roles and service accounts; scheduled jobs; spend alerts; more flexible compute classes.

### 6.3 Later features

CDC replication, data sharing, SQL/API serving, automatic compute recommendations, advanced governance (row/column policies), result caching, cross-region recovery, BYOC. Prioritize based on signed design-partner demand.

### 6.4 Snapshots and maintenance caveats

DuckLake supports snapshots/time travel, but a dbt DAG spans multiple commits. “Roll back the whole run” requires careful mapping and may not be atomic. Snapshot expiry and file cleanup must account for retention and active readers. Automated maintenance must be tested against concurrent writes.

## 7. Secure sharing and data endpoints

### 7.1 Three offerings

1. Read-only SQL access: scoped service credentials to approved views/tables; query and response limits.
2. Managed REST data endpoints: published, parameterized, authenticated queries with pagination, quotas, and usage accounting.
3. External data shares/exports: governed dataset access or controlled Parquet exports; potentially recipient-owned compute.

Build in that order. The first production offering should be narrowly scoped published views or templates, not arbitrary unrestricted SQL.

### 7.2 Security requirements

Rust gateway enforces tenant identity, resource grants, client rate limits, cost caps, result-size caps, and allowed query patterns. DuckDB is an embedded engine, not a substitute for a full multi-user authorization service. Prevent bypass via file functions, extensions, external URLs, attachments, arbitrary paths, and direct S3 credentials. Test row-level tenant separation with adversarial inputs. Keys must be scoped, rotatable, expiring, revocable, and never logged in plaintext.

### 7.3 Serving compute

Separate read-only serving pools from transformation workers so a large dbt model cannot starve client APIs. Specify snapshot freshness, cache invalidation, concurrency, p95 latency, and cold-start behavior. DuckDB is excellent for analytics but should not be promised as a low-latency OLTP replacement. Large exports should write to controlled S3 objects with short-lived download authorization; this reduces gateway load but not internet egress.

### 7.4 Data products

Later, treat a published view as a managed data product with owner, schema, version, freshness SLA, audience, authorization policy, quota, cost attribution, and audit history. This turns dbt model → curated dataset → authorized consumer into a coherent workflow.

## 8. Optional PostgreSQL CDC ingestion

Long-term: PostgreSQL initial snapshot + logical replication → Rust ingestion/offset tracking → immutable S3 change log → raw DuckLake history and current-state tables. Support schema evolution, replay, retries, and offset/checkpoint correctness. Avoid building many connectors before demand. The change log's retention and storage charges must be explicit. CDC is not MVP-critical; shipping it prematurely multiplies correctness and support risks.

## 9. Metering, billing, and internal cost accounting

### 9.1 Non-negotiable principle

We must know what each tenant costs before we decide what to charge them. Maintain three distinct numbers:

1. AWS physical cost: reconciled AWS charges, including shared infrastructure and idle capacity.
2. Attributed tenant cost: defensible allocation of physical costs to customer/workload, with unallocated remainder visible.
3. Billable usage/revenue: contractually priced quantities from an immutable usage ledger and versioned rate card.

Never invoice directly from mutable query-history rows or raw AWS bills. Do not confuse query runtime with node uptime or a customer's bill with infrastructure cost.

### 9.2 Cost inventory

| Category | Physical cost source | Tenant allocation / billing considerations |
| --- | --- | --- |
| Baseline compute | EC2/EKS nodes, attached EBS | provisioned uptime, customer reserved share |
| Dedicated burst | node/pod lifecycle | node lifetime, startup/idle allowance, CPU/RAM class |
| Shared pooled compute | shared node invoices | resource-seconds and idle-capacity allocation |
| S3 storage | byte-hours and class | per-tenant object prefix/inventory, snapshots, archives |
| S3 API requests | request classes/counts | attribute via access logs/metrics where practical |
| DuckLake catalog | RDS instance/storage/IO | per-tenant catalog size and activity; shared overhead |
| Network | egress, inter-AZ, NAT, LB | bytes by tenant/client/destination; overhead separately |
| Scratch | EBS byte-hours/IOPS | worker/node lifetime |
| Observability | logs, traces, S3, Postgres | event volume/retention, shared overhead |
| Serving | serving worker + outbound bytes | client/API key, tag, query, endpoint |
| CDC | connector compute + change history | connector hours, stored bytes, transfer |
| Shared platform | EKS control plane, gateway, support tools | allocated fixed overhead; visible unallocated balance |
| Support/payment | processing fees, incidents | include in service-delivery margins |

### 9.3 Metering event contract

Append-only, idempotent, versioned usage events with event_id, tenant_id, resource_id, execution_id, meter_name, quantity, unit, start/end, region, tags, rate_card_version, source, and observed_at. Support corrections through compensating events, never silent edits. Keep source telemetry and invoice ledger separate.

### 9.4 Reconciliation

Collect AWS Cost and Usage data (AWS Data Exports/CUR) daily. Join resource tags, instance IDs, node/pod allocations, S3 inventory and service telemetry. Report:

AWS invoice = attributed tenant cost + explicitly unallocated shared/idle cost + timing/rounding/reconciliation differences.

Alert on variance, unexplained growth, negative tenant margin, unexpected NAT spend, and unattached resources. Do not promise exact per-query physical cost where shared resources make it inherently estimated.

### 9.5 Operator dashboard

MRR/ARR and prepaid liabilities; customer revenue; direct cost; contribution margin; service gross margin; idle capacity; compute packing efficiency; costs by AWS service/region/node; top and bottom tenant margins; S3 requests and egress; unreconciled spend; failed payment exposure; customer budgets and prepaid balances. Slice by tenant, workload, dbt model, tags, endpoint, and time period.

## 10. Proposed customer pricing and credit policy

### 10.1 Commercial structure

Annual minimum baseline commitment + prepaid metered elastic usage + customer-controlled budget. Core platform capabilities are bundled, not nickel-and-dimed. An annual agreement alone does not guarantee cash collection; prefer annual upfront payment for early bootstrapped customers, or monthly autopay with appropriate risk controls.

Illustrative founding offer (not validated): $299/month equivalent paid $3,588 annually for a small baseline service, or $349/month on a 12-month contract, plus prepaid elastic usage. A dedicated always-on 4-vCPU instance may make this price uneconomic; test smaller baseline or shared physical infrastructure. Do not publish a fixed hardware promise before benchmarking.

Alternative: a $3,600 annual minimum with a fixed baseline portion and a flexible prepaid usage allowance. Ensure unused credits, expiration, refunds, cancellation, and accounting treatment are legally and commercially clear.

### 10.2 Billable dimensions

| Dimension | Simple customer unit |
| --- | --- |
| Reserved platform/baseline | annual contract, compute class |
| Burst compute | class × metered seconds, minimum charge if needed |
| Storage | GiB-month beyond included allowance |
| Serving | compute usage / endpoint allowance |
| External egress | GB beyond included allowance |
| CDC (later) | connector-hours + storage |
| Premium retention/support (later) | tiered plan |

For resource-class billing, one proposed normalized unit is max(vCPU, GiB RAM / 4) × hours, but test whether fixed worker classes are simpler and better aligned with actual instance families. Include node startup, fragmentation, and idle costs in the rate card rather than assuming pod lifetime equals EC2 billed lifetime.

### 10.3 Hard budgets

Budget types: monthly variable spend cap, per-job max cost, max CPU/RAM, concurrency quota, egress cap, prepaid balance. Admission reserves estimated funds before scheduling; worker metering decrements reservation; cancellation occurs near threshold; unused reservation is released on terminal reconciliation. Always enforce a safety margin. Existing annual commitments, stored data, delayed egress charges, and external AWS costs cannot all be instantly stopped; customer-facing language must say exactly which charges are hard-capped.

### 10.4 Payment protection

Payment succeeds before production provisioning. No unsecured postpaid burst usage for new customers. On failed renewal, block new variable jobs, notify, allow a defined grace period, then suspend compute. Retain and eventually delete data under disclosed terms. Stripe or equivalent may handle payment collection and invoicing, but Rust's internal ledger is authoritative for workload admission.

### 10.5 Gross margin targets

Planning guardrails, not measured forecasts: initial service gross margin 45–60%, mature 65–75%, with 70–80% infrastructure margin as an aspirational subset where feasible. Contribution margin must include allocated shared infrastructure and customer-specific service costs. Founder salary, engineering, sales, legal, and overhead still determine operating profitability. Never use markup as if it were margin: 2× cost gives 50% gross margin before other costs; 3× gives ~66.7%.

### 10.6 Example unit economics (illustrative only)

Assume a baseline EC2 instance costs ~$147/month, catalog/network/shared allocation $75, and storage $6. If annual equivalent revenue is $299/month, the direct service cost is ~$228 and gross margin is only ~24% before support/payment fees. This is not viable for a 70% target. A cheaper baseline, higher fee, or shared physical compute is necessary. At a 70% target, a $228 service cost requires $760/month revenue (cost / 0.30).

A separate example: 20 customers averaging $700/month and 70% service gross margin generate $14,000 revenue and $9,800 gross profit; after $5,000 fixed monthly operating costs, $4,800 remains before founder compensation (if excluded), taxes, and growth investments. These are scenarios, not forecasts.

## 11. AWS cost and networking assumptions

Pricing must be refreshed from official AWS calculators and actual bills before launch. Illustrative prior research for us-east-1 Linux on-demand m7i instances:

| Instance | vCPU | RAM | Approx. $/hour | Approx. 730h/month |
| --- | --- | --- | --- | --- |
| m7i.large | 2 | 8 GiB | $0.1008 | $73.58 |
| m7i.xlarge | 4 | 16 GiB | $0.2016 | $147.17 |
| m7i.2xlarge | 8 | 32 GiB | $0.4032 | $294.34 |
| m7i.4xlarge | 16 | 64 GiB | $0.8064 | $588.67 |
| m7i.8xlarge | 32 | 128 GiB | $1.6128 | $1,177.34 |
| m7i.16xlarge | 64 | 256 GiB | $3.2256 | $2,354.69 |

Additional cost drivers: EKS cluster fee, RDS instance/storage/backups, S3 byte-hours and GET/PUT requests, EBS scratch, load balancers, NAT, public IPs, CloudWatch/logging, cross-AZ transfer, internet egress, support, payment processing. Spot/Savings Plans may improve cost but add constraints; don't commit AWS spend prematurely.

Networking policy: keep workers, catalog, and S3 in same region; prefer S3 gateway endpoints to avoid NAT processing on S3 traffic; monitor cross-AZ transfers; avoid streaming huge exports through the API gateway. Normal dbt transforms return status and modest results, so internet egress is likely secondary at first, but external sharing and bulk exports can materially change that. Meter outbound bytes per tenant and consumer key from day one. Provide an included allowance and usage charge above it, with a safety cap. An illustrative $0.15/GB overage and 1.5–2× attributable AWS network cost markup are hypotheses, not finalized prices.

## 12. Security, privacy, and operational reliability

### 12.1 Threat model

Multi-tenant SQL execution is inherently sensitive: arbitrary SQL, extensions, file access, attached databases, S3 credentials, metadata catalog access, query text, API keys, and exports. Protect against cross-tenant reads, exfiltration, denial-of-service, budget abuse, privilege escalation, and accidental deletion. Validate isolation with adversarial integration tests before accepting external customer data.

### 12.2 Baseline controls

Tenant-scoped identity, least-privilege IAM, secrets isolation, TLS, encryption at rest, strict S3 prefix/bucket policies, deny-by-default worker networking, approved extensions, query resource limits, rate limiting, service-account key rotation, audit events, backups, tested restoration, retention/deletion policy, incident runbooks, and customer-visible status. Do not claim SOC 2 compliance before achieving it.

### 12.3 Reliability objectives (proposed)

Define realistic MVP availability and recovery targets after deployment testing. Monitor gateway uptime, queue lag, worker scheduling time, catalog errors, failed materializations, telemetry loss, reconciliation drift, and backup restoration. Prioritize correctness over transparent retries for writes. Build an operator kill switch for expensive jobs and tenant-level suspensions.

## 13. MVP build sequence and acceptance gates

### Phase 0 — Technical feasibility spikes (before product build)

Spike A: shared DuckLake concurrency. Run 20 independent DuckDB processes against one catalog and S3; test simultaneous reads, overlapping writes, incremental materializations, transaction conflicts, snapshot visibility, and cleanup. Gate: correctness and documented retry strategy.

Spike B: dbt adapter execution context. Implement a minimal Python adapter that submits model-level work to Rust and preserves materialization hooks and dependency behavior. Gate: representative dbt project passes run/test/incremental cycles.

Spike C: worker economics. Measure persistent vs fresh EKS Job latency, Karpenter startup, spill behavior, node packing, idle cost, S3 request counts, and network cost. Gate: credible cost model.

Spike D: telemetry and budgets. Capture SQL execution profiles and resource usage; enforce per-job admission with prepaid reservation. Gate: no unmetered production execution path.

### Phase 1 — Narrow internal MVP

One region, one tenant, one DuckLake, baseline worker + burst Job, Rust gateway/router/scheduler, minimal dbt adapter, durable execution records, query history, cancellation, metering, simple cost calculator, budget limits, backup/restore test. CLI first; minimal admin UI.

Acceptance: 100+ representative models; deterministic routing; no silent missing terminal events; known cost per execution class; recovery from killed worker; billing ledger replayable; correct downstream model visibility.

### Phase 2 — Paid design-partner beta

Multi-tenancy, authentication, workspace provisioning, payment-before-provisioning, annual commitments, prepaid usage, customer dashboard, operator profitability dashboard, tags, profiles, quotas, alerting, AWS cost reconciliation, documentation, support runbooks. No open public signups until cost caps and tenant isolation are tested.

Acceptance: daily AWS spend reconciles to attributed + unallocated cost within documented tolerance; every customer has positive projected contribution margin; resource quotas withstand abuse; full data restore tested.

### Phase 3 — Early commercial product

Guided snapshots, scheduled maintenance, model-cost insights, warm pool if justified, service accounts and scoped read-only sharing, richer UI and alerts. Iterate pricing from actual usage cohorts.

### Phase 4 — Expansion

CDC, managed REST endpoints, advanced governance, intelligent auto-sizing, BYOC, premium isolation, enterprise security features.

Timelines: do not commit to earlier illustrative 3/6/12-month dates until feasibility spikes and staffing assumptions are validated.

## 14. Test plan and benchmarking

- Correctness: compare model outputs across local DuckDB, managed workers, and alternate warehouses; concurrent writes and retries; schema evolution.
- Performance: model wall time, DAG wall time, queue time, cold start, S3 bytes/requests, peak RAM, spill bytes, CPU utilization.
- Economics: AWS actual billed cost by workload, idle capacity, shared fixed overhead, support cost, effective gross margin.
- Reliability: kill pods/nodes, drop catalog connections, inject S3 errors, simulate duplicate events, partial commit, lost telemetry.
- Security: cross-tenant SQL, extension escape, S3 credential misuse, query endpoint abuse, unauthorized view access.
- Billing: event deduplication, late AWS charges, refunds, credit reservations, cancellation, rounding, prepaid exhaustion, invoice replay.
- Competitive: identical representative dbt workloads on MotherDuck and Snowflake, compared on runtime, price, maintenance burden, and operational features. Publish only reproducible results.

## 15. Product and company naming

Branding should be duck-related if that improves discoverability and communicates ecosystem fit. Candidates: DrakeLake (memorable, DuckLake-adjacent), DuckFleet (compute fleet), FatherDuck (playful but strongly references MotherDuck), DuckDock (hosting), MallardCompute, DuckForge. No domain or trademark availability is established in this document. Choose an inexpensive standard .com when possible, confirm registrar checkout price and trademark risk, and avoid premium aftermarket purchases. Product sub-brand example: company DrakeLake, compute engine DuckFleet.

## 16. Risks and decision register

| Risk / unknown | Impact | Next action |
| --- | --- | --- |
| dbt model-level routing compatibility | Existential | adapter/materialization spike |
| DuckLake concurrent writes/transaction semantics | Existential | concurrency stress tests |
| Shared physical compute isolation | High | security prototype, threat model |
| Dedicated baseline economics | High | AWS calculator + real utilization |
| EKS fixed costs / cold starts | High | compare EKS vs ECS/EC2 |
| Attribution accuracy for pooled workers | High | event schema and cost reconciliation |
| Hard budget enforcement edge cases | High | reservation/settlement state machine |
| Customer willingness to prepay annually | High | design-partner interviews |
| MotherDuck feature overlap | Medium–High | refreshed competitor matrix |
| S3 small-file and request costs | Medium–High | workload benchmark |
| External SQL authorization | High | defer unrestricted access |
| CDC correctness and support scope | High | defer |
| Domain and trademark availability | Low–Medium | registrar and legal checks |

### Decisions to make next, in order

1. Is the entry baseline dedicated physical compute, dedicated process on shared nodes, or scale-to-zero? This determines pricing.
2. Which dbt version and adapter interface will be supported at launch?
3. What are the supported materializations and concurrency semantics?
4. Does EKS earn its fixed cost in the MVP, or should we start simpler?
5. What is the first customer's real DAG, storage size, and resource profile?
6. What exactly does the annual minimum include, and which variable charges are hard-capped?
7. What are our target service gross margin and maximum unallocated cost per tenant?

## 17. First 30 days: founder/CTO execution checklist

Week 1: choose first design-partner workload; lock engine/dbt versions; provision isolated AWS sandbox; create benchmark and correctness harness; draft tenant/meter/event schema.

Week 2: prove Rust worker executes DuckLake SQL from persistent process and Kubernetes Job; capture JSON profile and resource metrics; test concurrent read/write behavior.

Week 3: prove dbt adapter routes one full materialization lifecycle; add durable state machine, cancellation, idempotent events, and basic admission budget.

Week 4: measure AWS cost under representative DAGs; build minimal operator ledger/report; compare EKS and simpler execution alternatives; decide go/no-go for paid beta.

Definition of progress: not number of UI pages or feature checkboxes, but a reliable dbt model executed on the requested resources, against correct shared data, with measured cost, traceable history, and enforced financial limits.

## 18. Reference material and pricing verification checklist

These are primary starting points, not evidence that every competitive claim or price remains current:

- DuckDB documentation: https://duckdb.org/docs/
- DuckDB profiling: https://duckdb.org/docs/stable/dev/profiling/overview
- DuckLake documentation: https://ducklake.select/docs/
- dbt developer/adapter docs: https://docs.getdbt.com/docs/building-a-dbt-project/building-models
- Snowflake Account Usage: https://docs.snowflake.com/en/sql-reference/account-usage
- Databricks system tables: https://docs.databricks.com/aws/en/admin/system-tables/
- AWS EC2 pricing: https://aws.amazon.com/ec2/pricing/on-demand/
- AWS EKS pricing: https://aws.amazon.com/eks/pricing/
- AWS S3 pricing: https://aws.amazon.com/s3/pricing/
- AWS RDS PostgreSQL pricing: https://aws.amazon.com/rds/postgresql/pricing/
- AWS data transfer pricing: https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer
- AWS Cost and Usage / Data Exports: https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html
- AWS Pricing Calculator: https://calculator.aws/
- AWS Savings Plans: https://aws.amazon.com/savingsplans/
- Cloudflare Registrar: https://www.cloudflare.com/products/registrar/
- MotherDuck pricing and product docs: https://motherduck.com/pricing/

Research caveat: Figures and competitive observations consolidated here originated in our planning discussion. They are directional assumptions requiring current official quote/benchmark verification; they are not procurement-ready pricing, legal advice, or validated product-market fit.

## 19. Final CTO recommendation

Build the smallest reliable system that makes one dbt model run on precisely the resources its author requests and produces a defensible execution-level cost record. Treat metering, budget enforcement, and isolation as foundational, not billing polish. Prove DuckLake multi-worker correctness before adding a warm pool, CDC, or external SQL. Sell annual baseline commitments and prepaid burst usage only after measuring the actual cost of the smallest useful customer configuration. Use query history and tagging to build insights once the raw records are trustworthy.

This yields a coherent product: build with dbt, run with the right compute, know exactly what it costs, and safely share the result.
