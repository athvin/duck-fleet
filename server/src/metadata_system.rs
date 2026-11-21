//! Closed metadata manifest and trusted PostgreSQL source generation.
use super::{Relation, RelationPlan, relation_plan};
use crate::{ApiError, authz::MetadataAccess, failure};
use postgres::types::ToSql;
use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SystemType {
    Bool,
    Int64,
    Float64,
    Utf8,
    TimestampUtc,
    JsonText,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Sensitivity {
    Metadata,
    Sql,
    RawProfile,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultType {
    Scalar(SystemType),
    Decimal128 { precision: u8, scale: u8 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnDef {
    pub id: ColumnId,
    pub name: &'static str,
    pub ty: SystemType,
    pub nullable: bool,
    pub sensitivity: Sensitivity,
    pub units: Option<&'static str>,
    pub description: &'static str,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct RelationDef {
    pub relation: Relation,
    pub name: &'static str,
    pub description: &'static str,
    pub time_column: ColumnId,
    pub columns: &'static [ColumnDef],
}
pub(crate) const SYSTEM_CONTRACT_VERSION: u16 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ColumnId {
    RequestId,
    LogicalRequestId,
    RetryOf,
    SessionId,
    HistoryId,
    NamespaceId,
    OwnerPrincipalId,
    OwnerPrincipalName,
    ActorPrincipalId,
    ActorPrincipalName,
    ActorKind,
    ActorCredentialId,
    WorkerId,
    WorkerBootId,
    Protocol,
    PoolScope,
    State,
    Status,
    Outcome,
    FailureKind,
    RetrySafe,
    AcceptedAt,
    QueuedAt,
    SelectedAt,
    ExecutionGrantedAt,
    EngineStartedAt,
    FinishedAt,
    QueueDeadlineAt,
    ExecutionTimeoutSeconds,
    QueueTimeoutSeconds,
    EngineElapsedMs,
    QueueWaitMs,
    PreparationMs,
    LifecycleMs,
    ResultRows,
    SessionSequence,
    OpenSpanId,
    RequestedMode,
    RequestedPool,
    RequestedCpu,
    RequestedMemoryBytes,
    RequestedDiskBytes,
    SelectedMode,
    SelectedPool,
    SelectedCpu,
    SelectedMemoryBytes,
    SelectedDiskBytes,
    EngineVersion,
    RegistryRevision,
    PolicyGeneration,
    AuthorizationPolicyVersion,
    QueryFingerprint,
    QueryFingerprintVersion,
    NormalizationStatus,
    ModelId,
    InvocationId,
    ModelRunId,
    SqlText,
    ErrorText,
    ApplicationContext,
    ProfileStatus,
    ProfileCaptureVersion,
    ProfileJson,
    ProfileLatencySeconds,
    ProfileCpuSeconds,
    ProfileRowsReturned,
    ProfileRowsScanned,
    PeakBufferMemoryBytes,
    PeakTempDirBytes,
    BytesRead,
    BytesWritten,
    LedgerId,
    EventId,
    MeterName,
    Quantity,
    Unit,
    ResourceUnits,
    AmountMicroUsd,
    RateCardVersion,
    RateMicroUsdPerUnitHour,
    Source,
    CorrectsEventId,
    ObservedAt,
    AccountingStartedAt,
    AccountingEndedAt,
}
impl ColumnId {
    const fn definition(self) -> ColumnDef {
        match self {
            Self::RequestId => ColumnDef {
                id: self,
                name: "request_id",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Durable request identifier.",
            },
            Self::LogicalRequestId => ColumnDef {
                id: self,
                name: "logical_request_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Root request identifier in explicit retry lineage.",
            },
            Self::RetryOf => ColumnDef {
                id: self,
                name: "retry_of",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Previous attempt, when this is an explicit retry.",
            },
            Self::SessionId => ColumnDef {
                id: self,
                name: "session_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Engine session identifier.",
            },
            Self::HistoryId => ColumnDef {
                id: self,
                name: "history_id",
                ty: SystemType::Int64,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "History pagination identifier.",
            },
            Self::NamespaceId => ColumnDef {
                id: self,
                name: "namespace_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Authenticated platform namespace identifier.",
            },
            Self::OwnerPrincipalId => ColumnDef {
                id: self,
                name: "owner_principal_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Immutable workload owner identifier; unknown legacy owners remain NULL.",
            },
            Self::OwnerPrincipalName => ColumnDef {
                id: self,
                name: "owner_principal_name",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Recorded workload owner name.",
            },
            Self::ActorPrincipalId => ColumnDef {
                id: self,
                name: "actor_principal_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Authenticated acting principal identifier.",
            },
            Self::ActorPrincipalName => ColumnDef {
                id: self,
                name: "actor_principal_name",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Recorded actor name.",
            },
            Self::ActorKind => ColumnDef {
                id: self,
                name: "actor_kind",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Named, bootstrap, or internal actor kind.",
            },
            Self::ActorCredentialId => ColumnDef {
                id: self,
                name: "actor_credential_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Acting credential identifier, never the bearer secret.",
            },
            Self::WorkerId => ColumnDef {
                id: self,
                name: "worker_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Recorded execution worker identifier.",
            },
            Self::WorkerBootId => ColumnDef {
                id: self,
                name: "worker_boot_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Recorded worker process epoch.",
            },
            Self::Protocol => ColumnDef {
                id: self,
                name: "protocol",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Authenticated ingress protocol.",
            },
            Self::PoolScope => ColumnDef {
                id: self,
                name: "pool_scope",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Stable admission scope.",
            },
            Self::State => ColumnDef {
                id: self,
                name: "state",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Durable request lifecycle state.",
            },
            Self::Status => ColumnDef {
                id: self,
                name: "status",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Recorded execution status.",
            },
            Self::Outcome => ColumnDef {
                id: self,
                name: "outcome",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Known nonexecution, statement result, or unknown outcome; not transaction commit proof.",
            },
            Self::FailureKind => ColumnDef {
                id: self,
                name: "failure_kind",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Stable classified failure kind.",
            },
            Self::RetrySafe => ColumnDef {
                id: self,
                name: "retry_safe",
                ty: SystemType::Bool,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Recorded retry safety guidance; not authorization to take over an attempt.",
            },
            Self::AcceptedAt => ColumnDef {
                id: self,
                name: "accepted_at",
                ty: SystemType::TimestampUtc,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Database-observed durable admission time.",
            },
            Self::QueuedAt => ColumnDef {
                id: self,
                name: "queued_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Database-observed queue admission time.",
            },
            Self::SelectedAt => ColumnDef {
                id: self,
                name: "selected_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Database-observed scheduler selection time.",
            },
            Self::ExecutionGrantedAt => ColumnDef {
                id: self,
                name: "execution_granted_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Database-observed execution grant; not proof of engine entry.",
            },
            Self::EngineStartedAt => ColumnDef {
                id: self,
                name: "engine_started_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Worker-observed engine entry, persisted on completion when available.",
            },
            Self::FinishedAt => ColumnDef {
                id: self,
                name: "finished_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Database-observed terminal persistence time.",
            },
            Self::QueueDeadlineAt => ColumnDef {
                id: self,
                name: "queue_deadline_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Durable queue deadline.",
            },
            Self::ExecutionTimeoutSeconds => ColumnDef {
                id: self,
                name: "execution_timeout_seconds",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("seconds"),
                description: "Requested execution time bound.",
            },
            Self::QueueTimeoutSeconds => ColumnDef {
                id: self,
                name: "queue_timeout_seconds",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("seconds"),
                description: "Requested queue time bound.",
            },
            Self::EngineElapsedMs => ColumnDef {
                id: self,
                name: "engine_elapsed_ms",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("milliseconds"),
                description: "Worker monotonic execution and Arrow serialization interval, excluding later profiling and settlement.",
            },
            Self::QueueWaitMs => ColumnDef {
                id: self,
                name: "queue_wait_ms",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("milliseconds"),
                description: "Selected minus queued database timestamps.",
            },
            Self::PreparationMs => ColumnDef {
                id: self,
                name: "preparation_ms",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("milliseconds"),
                description: "Grant minus selected database timestamps.",
            },
            Self::LifecycleMs => ColumnDef {
                id: self,
                name: "lifecycle_ms",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("milliseconds"),
                description: "Finished minus accepted database timestamps; excludes client delivery.",
            },
            Self::ResultRows => ColumnDef {
                id: self,
                name: "result_rows",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("rows"),
                description: "Recorded result row count.",
            },
            Self::SessionSequence => ColumnDef {
                id: self,
                name: "session_sequence",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Per-session accepted query sequence.",
            },
            Self::OpenSpanId => ColumnDef {
                id: self,
                name: "open_span_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Gateway/worker session-open correlation identifier.",
            },
            Self::RequestedMode => ColumnDef {
                id: self,
                name: "requested_mode",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Explicit original mode; omissions stay NULL.",
            },
            Self::RequestedPool => ColumnDef {
                id: self,
                name: "requested_pool",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Explicit original persistent pool; omissions stay NULL.",
            },
            Self::RequestedCpu => ColumnDef {
                id: self,
                name: "requested_cpu",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("vCPU"),
                description: "Explicit requested CPU requirement.",
            },
            Self::RequestedMemoryBytes => ColumnDef {
                id: self,
                name: "requested_memory_bytes",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "Explicit requested memory requirement.",
            },
            Self::RequestedDiskBytes => ColumnDef {
                id: self,
                name: "requested_disk_bytes",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "Explicit requested disk requirement.",
            },
            Self::SelectedMode => ColumnDef {
                id: self,
                name: "selected_mode",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Resolved execution mode.",
            },
            Self::SelectedPool => ColumnDef {
                id: self,
                name: "selected_pool",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Resolved persistent pool, if applicable.",
            },
            Self::SelectedCpu => ColumnDef {
                id: self,
                name: "selected_cpu",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("vCPU"),
                description: "Resolved container CPU allocation.",
            },
            Self::SelectedMemoryBytes => ColumnDef {
                id: self,
                name: "selected_memory_bytes",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "Resolved container memory allocation, not measured RSS.",
            },
            Self::SelectedDiskBytes => ColumnDef {
                id: self,
                name: "selected_disk_bytes",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "Resolved ephemeral disk allocation, not observed disk consumption.",
            },
            Self::EngineVersion => ColumnDef {
                id: self,
                name: "engine_version",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Worker-observed DuckDB version.",
            },
            Self::RegistryRevision => ColumnDef {
                id: self,
                name: "registry_revision",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Compute registry revision pinned at session open.",
            },
            Self::PolicyGeneration => ColumnDef {
                id: self,
                name: "policy_generation",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Durable admission policy generation.",
            },
            Self::AuthorizationPolicyVersion => ColumnDef {
                id: self,
                name: "authorization_policy_version",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Authorization policy observed at admission.",
            },
            Self::QueryFingerprint => ColumnDef {
                id: self,
                name: "query_fingerprint",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Versioned query-shape grouping hash; not the private idempotency hash.",
            },
            Self::QueryFingerprintVersion => ColumnDef {
                id: self,
                name: "query_fingerprint_version",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Attempted query-shape algorithm version.",
            },
            Self::NormalizationStatus => ColumnDef {
                id: self,
                name: "normalization_status",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Captured, unsupported, too_large, or parse_error; legacy unknown stays NULL.",
            },
            Self::ModelId => ColumnDef {
                id: self,
                name: "model_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Recorded application model label; not authenticated identity.",
            },
            Self::InvocationId => ColumnDef {
                id: self,
                name: "invocation_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Recorded application invocation label.",
            },
            Self::ModelRunId => ColumnDef {
                id: self,
                name: "model_run_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Recorded application run label.",
            },
            Self::SqlText => ColumnDef {
                id: self,
                name: "sql_text",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Original SQL text, subject to SQL-read scope and retention.",
            },
            Self::ErrorText => ColumnDef {
                id: self,
                name: "error_text",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Recorded sanitized engine error; still SQL-sensitive.",
            },
            Self::ApplicationContext => ColumnDef {
                id: self,
                name: "application_context",
                ty: SystemType::JsonText,
                nullable: true,
                sensitivity: Sensitivity::Sql,
                units: None,
                description: "Recorded application JSON context, subject to SQL-read scope.",
            },
            Self::ProfileStatus => ColumnDef {
                id: self,
                name: "profile_status",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Profile capture availability/status.",
            },
            Self::ProfileCaptureVersion => ColumnDef {
                id: self,
                name: "profile_capture_version",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Version of DuckFleet numeric profile extraction, not DuckDB JSON schema.",
            },
            Self::ProfileJson => ColumnDef {
                id: self,
                name: "profile_json",
                ty: SystemType::JsonText,
                nullable: true,
                sensitivity: Sensitivity::RawProfile,
                units: None,
                description: "Bounded raw profile; both SQL-read and profile-read are required.",
            },
            Self::ProfileLatencySeconds => ColumnDef {
                id: self,
                name: "profile_latency_seconds",
                ty: SystemType::Float64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("seconds"),
                description: "DuckDB-reported profile latency.",
            },
            Self::ProfileCpuSeconds => ColumnDef {
                id: self,
                name: "profile_cpu_seconds",
                ty: SystemType::Float64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("seconds"),
                description: "DuckDB-reported operator CPU time, not container accounting.",
            },
            Self::ProfileRowsReturned => ColumnDef {
                id: self,
                name: "profile_rows_returned",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("rows"),
                description: "DuckDB-reported returned rows.",
            },
            Self::ProfileRowsScanned => ColumnDef {
                id: self,
                name: "profile_rows_scanned",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("rows"),
                description: "DuckDB-reported cumulative scanned rows.",
            },
            Self::PeakBufferMemoryBytes => ColumnDef {
                id: self,
                name: "peak_buffer_memory_bytes",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "DuckDB peak buffer memory, not container RSS.",
            },
            Self::PeakTempDirBytes => ColumnDef {
                id: self,
                name: "peak_temp_dir_bytes",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "DuckDB peak temporary-directory size.",
            },
            Self::BytesRead => ColumnDef {
                id: self,
                name: "bytes_read",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "DuckDB-reported bytes read, not provider billable traffic.",
            },
            Self::BytesWritten => ColumnDef {
                id: self,
                name: "bytes_written",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("bytes"),
                description: "DuckDB-reported bytes written.",
            },
            Self::LedgerId => ColumnDef {
                id: self,
                name: "ledger_id",
                ty: SystemType::Int64,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Append-only usage ledger sequence.",
            },
            Self::EventId => ColumnDef {
                id: self,
                name: "event_id",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Idempotent usage event identifier.",
            },
            Self::MeterName => ColumnDef {
                id: self,
                name: "meter_name",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Meter name.",
            },
            Self::Quantity => ColumnDef {
                id: self,
                name: "quantity",
                ty: SystemType::Int64,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Meter quantity in the separately recorded unit.",
            },
            Self::Unit => ColumnDef {
                id: self,
                name: "unit",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Meter quantity unit.",
            },
            Self::ResourceUnits => ColumnDef {
                id: self,
                name: "resource_units",
                ty: SystemType::Int64,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Saved normalized resource multiplier.",
            },
            Self::AmountMicroUsd => ColumnDef {
                id: self,
                name: "amount_micro_usd",
                ty: SystemType::Int64,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: Some("microUSD"),
                description: "Exact recorded amount, including signed corrections.",
            },
            Self::RateCardVersion => ColumnDef {
                id: self,
                name: "rate_card_version",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Rate-card version saved for this event.",
            },
            Self::RateMicroUsdPerUnitHour => ColumnDef {
                id: self,
                name: "rate_micro_usd_per_unit_hour",
                ty: SystemType::Int64,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: Some("microUSD/unit-hour"),
                description: "Saved rate, never inferred from today's configuration.",
            },
            Self::Source => ColumnDef {
                id: self,
                name: "source",
                ty: SystemType::Utf8,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Worker, queue, recovery, or correction source.",
            },
            Self::CorrectsEventId => ColumnDef {
                id: self,
                name: "corrects_event_id",
                ty: SystemType::Utf8,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Corrected event identifier when applicable.",
            },
            Self::ObservedAt => ColumnDef {
                id: self,
                name: "observed_at",
                ty: SystemType::TimestampUtc,
                nullable: false,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Database observation time of the ledger event.",
            },
            Self::AccountingStartedAt => ColumnDef {
                id: self,
                name: "accounting_started_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Settlement-derived accounting interval start, not observed engine start.",
            },
            Self::AccountingEndedAt => ColumnDef {
                id: self,
                name: "accounting_ended_at",
                ty: SystemType::TimestampUtc,
                nullable: true,
                sensitivity: Sensitivity::Metadata,
                units: None,
                description: "Settlement-derived accounting interval end, not observed engine end.",
            },
        }
    }
}
macro_rules! columns { ($($id:ident),* $(,)?) => { &[$(ColumnId::$id.definition()),*] }; }
static RELATIONS: &[RelationDef] = &[
    RelationDef {
        relation: Relation::History,
        name: "query_history",
        description: "Retained query diagnostics; one row per request.",
        time_column: ColumnId::AcceptedAt,
        columns: columns!(
            RequestId,
            LogicalRequestId,
            RetryOf,
            SessionId,
            HistoryId,
            NamespaceId,
            OwnerPrincipalId,
            OwnerPrincipalName,
            ActorPrincipalId,
            ActorPrincipalName,
            ActorKind,
            ActorCredentialId,
            WorkerId,
            WorkerBootId,
            Protocol,
            PoolScope,
            State,
            Status,
            Outcome,
            FailureKind,
            RetrySafe,
            AcceptedAt,
            QueuedAt,
            SelectedAt,
            ExecutionGrantedAt,
            EngineStartedAt,
            FinishedAt,
            QueueDeadlineAt,
            ExecutionTimeoutSeconds,
            QueueTimeoutSeconds,
            EngineElapsedMs,
            QueueWaitMs,
            PreparationMs,
            LifecycleMs,
            ResultRows,
            SessionSequence,
            OpenSpanId,
            RequestedMode,
            RequestedPool,
            RequestedCpu,
            RequestedMemoryBytes,
            RequestedDiskBytes,
            SelectedMode,
            SelectedPool,
            SelectedCpu,
            SelectedMemoryBytes,
            SelectedDiskBytes,
            EngineVersion,
            RegistryRevision,
            PolicyGeneration,
            AuthorizationPolicyVersion,
            QueryFingerprint,
            QueryFingerprintVersion,
            NormalizationStatus,
            ModelId,
            InvocationId,
            ModelRunId,
            SqlText,
            ErrorText,
            ApplicationContext,
            ProfileStatus,
            ProfileCaptureVersion,
            ProfileJson,
            ProfileLatencySeconds,
            ProfileCpuSeconds,
            ProfileRowsReturned,
            ProfileRowsScanned,
            PeakBufferMemoryBytes,
            PeakTempDirBytes,
            BytesRead,
            BytesWritten
        ),
    },
    RelationDef {
        relation: Relation::Attempts,
        name: "query_attempts",
        description: "Durable attempt identity and lifecycle, retained independently of history.",
        time_column: ColumnId::AcceptedAt,
        columns: columns!(
            RequestId,
            LogicalRequestId,
            RetryOf,
            SessionId,
            NamespaceId,
            OwnerPrincipalId,
            OwnerPrincipalName,
            ActorPrincipalId,
            ActorPrincipalName,
            ActorKind,
            ActorCredentialId,
            WorkerId,
            WorkerBootId,
            Protocol,
            PoolScope,
            State,
            Status,
            Outcome,
            FailureKind,
            RetrySafe,
            AcceptedAt,
            QueuedAt,
            SelectedAt,
            ExecutionGrantedAt,
            EngineStartedAt,
            FinishedAt,
            QueueDeadlineAt,
            ExecutionTimeoutSeconds,
            QueueTimeoutSeconds,
            EngineElapsedMs,
            QueueWaitMs,
            PreparationMs,
            LifecycleMs,
            ResultRows,
            SessionSequence,
            OpenSpanId,
            RequestedMode,
            RequestedPool,
            RequestedCpu,
            RequestedMemoryBytes,
            RequestedDiskBytes,
            SelectedMode,
            SelectedPool,
            SelectedCpu,
            SelectedMemoryBytes,
            SelectedDiskBytes,
            EngineVersion,
            RegistryRevision,
            PolicyGeneration,
            AuthorizationPolicyVersion,
            QueryFingerprint,
            QueryFingerprintVersion,
            NormalizationStatus,
            ModelId,
            InvocationId,
            ModelRunId
        ),
    },
    RelationDef {
        relation: Relation::Usage,
        name: "usage",
        description: "Append-only usage events; a request may have multiple events.",
        time_column: ColumnId::ObservedAt,
        columns: columns!(
            RequestId,
            NamespaceId,
            OwnerPrincipalId,
            OwnerPrincipalName,
            ActorPrincipalId,
            ActorPrincipalName,
            ActorKind,
            ActorCredentialId,
            WorkerId,
            SelectedMode,
            SelectedPool,
            ModelId,
            InvocationId,
            ModelRunId,
            ApplicationContext,
            LedgerId,
            EventId,
            MeterName,
            Quantity,
            Unit,
            ResourceUnits,
            AmountMicroUsd,
            RateCardVersion,
            RateMicroUsdPerUnitHour,
            Source,
            CorrectsEventId,
            ObservedAt,
            AccountingStartedAt,
            AccountingEndedAt
        ),
    },
];
pub(crate) fn relations() -> &'static [RelationDef] {
    RELATIONS
}
pub(crate) fn relation_def(relation: Relation) -> &'static RelationDef {
    RELATIONS
        .iter()
        .find(|r| r.relation == relation)
        .expect("closed relation manifest")
}
pub(crate) fn column_def(relation: Relation, id: ColumnId) -> Option<&'static ColumnDef> {
    relation_def(relation).columns.iter().find(|c| c.id == id)
}
pub(crate) fn find_relation(name: &str) -> Option<Relation> {
    RELATIONS
        .iter()
        .find(|r| r.name == name)
        .map(|r| r.relation)
}
pub(crate) fn find_column(relation: Relation, name: &str) -> Option<ColumnId> {
    relation_def(relation)
        .columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.id)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnAccess {
    Readable,
    PartiallyMasked,
    Masked,
}
fn visibility(plan: &RelationPlan, sensitivity: Sensitivity) -> ColumnAccess {
    let sql_full = plan.sql_all || (!plan.all && plan.sql_own);
    let sql_own = plan.sql_all || plan.sql_own;
    let profile_full = plan.profile_all || (!plan.all && plan.profile_own);
    let profile_own = plan.profile_all || plan.profile_own;
    match sensitivity {
        Sensitivity::Metadata => ColumnAccess::Readable,
        Sensitivity::Sql if sql_full => ColumnAccess::Readable,
        Sensitivity::RawProfile if sql_full && profile_full => ColumnAccess::Readable,
        Sensitivity::Sql if sql_own && plan.principal_id.is_some() => ColumnAccess::PartiallyMasked,
        Sensitivity::RawProfile if sql_own && profile_own && plan.principal_id.is_some() => {
            ColumnAccess::PartiallyMasked
        }
        _ => ColumnAccess::Masked,
    }
}
pub(crate) fn column_access(
    access: &MetadataAccess,
    relation: Relation,
    column: ColumnId,
) -> Result<ColumnAccess, ApiError> {
    let plan = relation_plan(access, relation)?;
    let def = column_def(relation, column).ok_or_else(|| ApiError::bad("unknown system column"))?;
    Ok(visibility(&plan, def.sensitivity))
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StorageSchema(String);
impl StorageSchema {
    pub fn validated(name: &str) -> Result<Self, ApiError> {
        if name.is_empty()
            || name.len() > 63
            || !name
                .bytes()
                .enumerate()
                .all(|(i, c)| c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
        {
            return Err(ApiError::bad("invalid configured platform schema"));
        }
        Ok(Self(name.to_owned()))
    }
    pub fn search_path(&self) -> String {
        format!("pg_catalog,\"{}\",pg_temp", self.0)
    }
    fn table(&self, name: &str) -> String {
        format!("\"{}\".\"{name}\"", self.0)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceBounds {
    TimeRange {
        from: SystemTime,
        until: SystemTime,
    },
    RequestIds(Vec<String>),
    Both {
        from: SystemTime,
        until: SystemTime,
        ids: Vec<String>,
    },
}
impl SourceBounds {
    fn validate(&self) -> Result<(), ApiError> {
        let (range, ids) = match self {
            Self::TimeRange { from, until } => (Some((from, until)), None),
            Self::RequestIds(ids) => (None, Some(ids)),
            Self::Both { from, until, ids } => (Some((from, until)), Some(ids)),
        };
        if range.is_some_and(|(from, until)| {
            until
                .duration_since(*from)
                .map_or(true, |d| d.is_zero() || d > Duration::from_secs(31 * 86400))
        }) {
            return Err(ApiError::bad(
                "system source needs a positive window of at most 31 days",
            ));
        }
        if ids.is_some_and(|ids| {
            ids.is_empty()
                || ids.len() > 100
                || ids.iter().any(|id| id.is_empty() || id.len() > 512)
        }) {
            return Err(ApiError::bad(
                "system source needs 1–100 bounded request identifiers",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ColumnUses {
    pub projected: BTreeSet<ColumnId>,
    pub evaluated: BTreeSet<ColumnId>,
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BindValue {
    Null(SystemType),
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Timestamp(SystemTime),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BindId(usize);
impl BindId {
    pub fn index(self) -> usize {
        self.0
    }
}
#[derive(Debug, Default)]
pub(crate) struct Bindings {
    values: Vec<Box<dyn ToSql + Sync + Send>>,
}
impl Bindings {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push(&mut self, value: BindValue) -> BindId {
        let value: Box<dyn ToSql + Sync + Send> = match value {
            BindValue::Bool(v) => Box::new(v),
            BindValue::I64(v) => Box::new(v),
            BindValue::F64(v) => Box::new(v),
            BindValue::Text(v) => Box::new(v),
            BindValue::Timestamp(v) => Box::new(v),
            BindValue::Null(SystemType::Bool) => Box::new(None::<bool>),
            BindValue::Null(SystemType::Int64) => Box::new(None::<i64>),
            BindValue::Null(SystemType::Float64) => Box::new(None::<f64>),
            BindValue::Null(SystemType::TimestampUtc) => Box::new(None::<SystemTime>),
            BindValue::Null(_) => Box::new(None::<String>),
        };
        self.values.push(value);
        BindId(self.values.len())
    }
}
#[derive(Debug)]
pub(crate) struct BoundSql {
    sql: String,
    bindings: Bindings,
}
impl BoundSql {
    pub(crate) fn new(sql: String, bindings: Bindings) -> Self {
        Self { sql, bindings }
    }
    pub fn sql(&self) -> &str {
        &self.sql
    }
    pub fn params(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.bindings
            .values
            .iter()
            .map(|v| &**v as &(dyn ToSql + Sync))
            .collect()
    }
}
#[derive(Clone, Debug)]
pub(crate) struct AuthorizedSource {
    relation: Relation,
    plan: RelationPlan,
    bounds: SourceBounds,
    uses: ColumnUses,
    schema: StorageSchema,
}
pub(crate) fn source_plan(
    access: &MetadataAccess,
    relation: Relation,
    bounds: &SourceBounds,
    uses: &ColumnUses,
    schema: &StorageSchema,
) -> Result<AuthorizedSource, ApiError> {
    bounds.validate()?;
    let plan = relation_plan(access, relation)?;
    for id in uses.projected.union(&uses.evaluated) {
        let def =
            column_def(relation, *id).ok_or_else(|| ApiError::bad("unknown system column"))?;
        if uses.evaluated.contains(id)
            && visibility(&plan, def.sensitivity) != ColumnAccess::Readable
        {
            return Err(ApiError::new(
                failure::FORBIDDEN,
                "evaluating sensitive metadata requires permission for every visible row",
            ));
        }
    }
    Ok(AuthorizedSource {
        relation,
        plan,
        bounds: bounds.clone(),
        uses: uses.clone(),
        schema: schema.clone(),
    })
}

/// Only trusted static aliases/parameter expressions enter this internal helper.
/// HTTP and compiled sources share legacy ownership and namespace semantics.
pub(crate) struct PrivacySql {
    pub from: String,
    pub visible: String,
    pub owner: String,
    pub sql_visible: String,
    pub profile_visible: String,
}
#[derive(Clone, Copy)]
pub(crate) struct PrivacyBindings<'a> {
    pub namespace: usize,
    pub principal: usize,
    pub all: &'a str,
    pub sql_all: &'a str,
    pub sql_own: &'a str,
    pub profile_all: &'a str,
    pub profile_own: &'a str,
}
pub(crate) fn privacy_sql(
    relation: Relation,
    alias: &'static str,
    schema: Option<&StorageSchema>,
    p: PrivacyBindings<'_>,
) -> PrivacySql {
    let table = |name: &str| schema.map_or_else(|| name.to_owned(), |s| s.table(name));
    let name = match relation {
        Relation::History => "query_history",
        Relation::Attempts => "requests",
        Relation::Usage => "usage_events",
    };
    let mut from = format!("{} {alias}", table(name));
    let owner = if relation == Relation::Usage {
        format!("{alias}.owner_principal_id")
    } else {
        from.push_str(&format!(" LEFT JOIN {} l ON {alias}.namespace_id IS NULL AND {alias}.owner_principal_id IS NULL AND l.namespace_id=${} AND l.legacy_name={alias}.principal",table("authz_legacy_owners"),p.namespace));
        format!("COALESCE({alias}.owner_principal_id,l.principal_id)")
    };
    let visible = format!(
        "(({alias}.namespace_id=${0} AND ({1} OR {alias}.owner_principal_id=${2})) OR ({alias}.namespace_id IS NULL AND ({1} OR {owner}=${2})))",
        p.namespace, p.all, p.principal
    );
    let sql_visible = format!(
        "({} OR ({} AND {owner}=${}))",
        p.sql_all, p.sql_own, p.principal
    );
    let profile_visible = format!(
        "({sql_visible} AND ({} OR ({} AND {owner}=${})))",
        p.profile_all, p.profile_own, p.principal
    );
    PrivacySql {
        from,
        visible,
        owner,
        sql_visible,
        profile_visible,
    }
}
struct RenderSource {
    from: String,
    predicate: String,
    owner: String,
    sql_visible: String,
    profile_visible: String,
}
impl AuthorizedSource {
    fn base(&self, bindings: &mut Bindings) -> RenderSource {
        let ns = bindings
            .push(BindValue::Text(self.plan.namespace_id.clone()))
            .index();
        let principal = bindings
            .push(
                self.plan
                    .principal_id
                    .clone()
                    .map(BindValue::Text)
                    .unwrap_or(BindValue::Null(SystemType::Utf8)),
            )
            .index();
        let privacy = privacy_sql(
            self.relation,
            "b",
            Some(&self.schema),
            PrivacyBindings {
                namespace: ns,
                principal,
                all: if self.plan.all { "true" } else { "false" },
                sql_all: if self.plan.sql_all { "true" } else { "false" },
                sql_own: if self.plan.sql_own { "true" } else { "false" },
                profile_all: if self.plan.profile_all {
                    "true"
                } else {
                    "false"
                },
                profile_own: if self.plan.profile_own {
                    "true"
                } else {
                    "false"
                },
            },
        );
        let PrivacySql {
            mut from,
            visible,
            owner,
            sql_visible,
            profile_visible,
        } = privacy;
        if self.relation == Relation::History {
            // Typed/credible ownership must match too: a retained history row cannot be
            // enriched from a same-ID row belonging to another namespace or owner.
            from.push_str(&history_enrichment(
                Some(&self.schema),
                "b",
                "d",
                ns,
                &owner,
            ));
        }
        let (range, ids) = match &self.bounds {
            SourceBounds::TimeRange { from, until } => (Some((*from, *until)), None),
            SourceBounds::RequestIds(ids) => (None, Some(ids)),
            SourceBounds::Both { from, until, ids } => (Some((*from, *until)), Some(ids)),
        };
        let mut predicate = visible;
        if let Some((from, until)) = range {
            let f = bindings.push(BindValue::Timestamp(from)).index();
            let u = bindings.push(BindValue::Timestamp(until)).index();
            let time = if self.relation == Relation::Usage {
                "b.observed_at"
            } else {
                "b.accepted_at"
            };
            predicate.push_str(&format!(" AND {time}>=${f} AND {time}<${u}"));
        }
        if let Some(ids) = ids {
            let ids = ids
                .iter()
                .map(|id| format!("${}", bindings.push(BindValue::Text(id.clone())).index()))
                .collect::<Vec<_>>()
                .join(",");
            let key = match self.relation {
                Relation::History => "b.query_id",
                Relation::Attempts => "b.request_id",
                Relation::Usage => "b.execution_id",
            };
            predicate.push_str(&format!(" AND {key} IN ({ids})"));
        }
        RenderSource {
            from,
            predicate,
            owner,
            sql_visible,
            profile_visible,
        }
    }
    fn raw(&self, id: ColumnId, source: &RenderSource) -> String {
        use ColumnId::*;
        let request = if self.relation == Relation::History {
            "d"
        } else {
            "b"
        };
        let name = id.definition().name;
        match id {
            RequestId => match self.relation {
                Relation::History => "b.query_id",
                Relation::Attempts => "b.request_id",
                Relation::Usage => "b.execution_id",
            }
            .into(),
            HistoryId => "b.history_id".into(),
            NamespaceId => "b.namespace_id".into(),
            OwnerPrincipalId => source.owner.clone(),
            OwnerPrincipalName => if self.relation == Relation::Usage {
                "b.owner_principal_name"
            } else {
                "b.principal"
            }
            .into(),
            ActorPrincipalName => "b.actor_principal".into(),
            EngineElapsedMs => "b.elapsed_ms".into(),
            QueueWaitMs => "(EXTRACT(EPOCH FROM (b.selected_at-b.queued_at))*1000)::BIGINT".into(),
            PreparationMs => {
                "(EXTRACT(EPOCH FROM (b.execution_granted_at-b.selected_at))*1000)::BIGINT".into()
            }
            LifecycleMs => {
                "(EXTRACT(EPOCH FROM (b.finished_at-b.accepted_at))*1000)::BIGINT".into()
            }
            LogicalRequestId
            | RetryOf
            | State
            | Outcome
            | ExecutionTimeoutSeconds
            | QueueTimeoutSeconds
            | RequestedMode
            | RequestedPool
            | RequestedCpu
            | RequestedMemoryBytes
            | RequestedDiskBytes
            | SelectedCpu
            | SelectedMemoryBytes
            | SelectedDiskBytes
            | EngineVersion
            | RegistryRevision
            | PolicyGeneration
            | AuthorizationPolicyVersion
            | QueryFingerprint
            | QueryFingerprintVersion
            | NormalizationStatus => format!("{request}.{name}"),
            SelectedMode | SelectedPool | ModelId | InvocationId | ModelRunId => format!(
                "{}.{name}",
                if self.relation == Relation::Usage {
                    "b"
                } else {
                    request
                }
            ),
            OpenSpanId => "b.open_span_id".into(),
            SqlText => "b.sql".into(),
            ErrorText => "b.error".into(),
            ApplicationContext => "b.context".into(),
            ProfileJson => "b.profile".into(),
            CorrectsEventId => "b.corrects".into(),
            AccountingStartedAt => "b.started_at".into(),
            AccountingEndedAt => "b.ended_at".into(),
            _ => format!("b.{name}"),
        }
    }
    fn projected(&self, id: ColumnId, source: &RenderSource) -> String {
        let raw = self.raw(id, source);
        let raw = match id.definition().ty {
            SystemType::Int64 => format!("({raw})::BIGINT"),
            SystemType::Float64 => format!("({raw})::DOUBLE PRECISION"),
            _ => raw,
        };
        match id.definition().sensitivity {
            Sensitivity::Metadata => raw,
            Sensitivity::Sql => {
                format!("CASE WHEN {} THEN {raw} ELSE NULL END", source.sql_visible)
            }
            Sensitivity::RawProfile => format!(
                "CASE WHEN {} THEN {raw} ELSE NULL END",
                source.profile_visible
            ),
        }
    }
    pub fn cte_sql(&self, source_id: u8, bindings: &mut Bindings) -> String {
        assert!(source_id < 3, "bounded compiler source identifier");
        let source = self.base(bindings);
        let mut ids = self
            .uses
            .projected
            .union(&self.uses.evaluated)
            .copied()
            .collect::<BTreeSet<_>>();
        // COUNT(*) still needs a real row projection, but no payload columns.
        ids.insert(ColumnId::RequestId);
        let columns = ids
            .into_iter()
            .map(|id| {
                format!(
                    "{} AS \"{}\"",
                    self.projected(id, &source),
                    id.definition().name
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "s{source_id} AS MATERIALIZED (SELECT {columns} FROM {} WHERE {})",
            source.from, source.predicate
        )
    }
    pub fn cap_probe(&self, max_rows: u32) -> BoundSql {
        let mut bindings = Bindings::new();
        let source = self.base(&mut bindings);
        let sizes = self
            .uses
            .projected
            .union(&self.uses.evaluated)
            .filter(|id| matches!(id.definition().ty, SystemType::Utf8 | SystemType::JsonText))
            .map(|id| {
                format!(
                    "COALESCE(pg_catalog.octet_length({}),0)::BIGINT",
                    self.projected(*id, &source)
                )
            })
            .collect::<Vec<_>>();
        let size = if sizes.is_empty() {
            "0::BIGINT".into()
        } else {
            sizes.join("+")
        };
        let limit = u64::from(max_rows) + 1;
        BoundSql::new(
            format!(
                "SELECT COUNT(*)::BIGINT AS eligible_rows,COALESCE(SUM(payload_bytes),0)::BIGINT AS selected_payload_bytes FROM (SELECT {size} AS payload_bytes FROM {} WHERE {} LIMIT {limit}) eligible",
                source.from, source.predicate
            ),
            bindings,
        )
    }
}

const DIMENSIONS: &[ColumnId] = &[
    ColumnId::RequestedMode,
    ColumnId::RequestedPool,
    ColumnId::RequestedCpu,
    ColumnId::RequestedMemoryBytes,
    ColumnId::RequestedDiskBytes,
    ColumnId::SelectedMode,
    ColumnId::SelectedPool,
    ColumnId::SelectedCpu,
    ColumnId::SelectedMemoryBytes,
    ColumnId::SelectedDiskBytes,
    ColumnId::EngineVersion,
    ColumnId::RegistryRevision,
    ColumnId::QueryFingerprint,
    ColumnId::QueryFingerprintVersion,
    ColumnId::NormalizationStatus,
    ColumnId::ModelId,
    ColumnId::InvocationId,
    ColumnId::ModelRunId,
    ColumnId::ExecutionTimeoutSeconds,
];
const PROFILE_COLUMNS: &[ColumnId] = &[
    ColumnId::ProfileCaptureVersion,
    ColumnId::ProfileLatencySeconds,
    ColumnId::ProfileCpuSeconds,
    ColumnId::ProfileRowsReturned,
    ColumnId::ProfileRowsScanned,
    ColumnId::PeakBufferMemoryBytes,
    ColumnId::PeakTempDirBytes,
    ColumnId::BytesRead,
    ColumnId::BytesWritten,
];
const USAGE_DIMENSIONS: &[ColumnId] = &[
    ColumnId::OwnerPrincipalName,
    ColumnId::SelectedMode,
    ColumnId::SelectedPool,
    ColumnId::ModelId,
    ColumnId::InvocationId,
    ColumnId::ModelRunId,
    ColumnId::RateMicroUsdPerUnitHour,
];
fn json_fields(ids: &[ColumnId], alias: &'static str, sql_visible: &str) -> Vec<String> {
    ids.iter()
        .map(|id| {
            let d = id.definition();
            let value = format!("{alias}.{}", d.name);
            let value = if d.sensitivity == Sensitivity::Metadata {
                value
            } else {
                format!("CASE WHEN {sql_visible} THEN {value} ELSE NULL END")
            };
            format!("'{}',{value}", d.name)
        })
        .collect()
}
pub(crate) fn history_capture_json(sql_visible: &str) -> String {
    let mut fields = json_fields(DIMENSIONS, "d", sql_visible);
    fields.extend(json_fields(PROFILE_COLUMNS, "h", sql_visible));
    format!("jsonb_build_object({})::text", fields.join(","))
}
pub(crate) fn usage_capture_json(sql_visible: &str) -> String {
    format!(
        "jsonb_build_object({})::text",
        json_fields(USAGE_DIMENSIONS, "u", sql_visible).join(",")
    )
}
pub(crate) fn append_dimensions(
    target: &mut serde_json::Value,
    source: &serde_json::Value,
    sql_visible: bool,
) {
    for id in DIMENSIONS {
        let d = id.definition();
        target[d.name] = if d.sensitivity == Sensitivity::Metadata || sql_visible {
            source
                .get(d.name)
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };
    }
}
pub(crate) fn history_enrichment(
    schema: Option<&StorageSchema>,
    history: &'static str,
    request: &'static str,
    namespace: usize,
    owner: &str,
) -> String {
    let table = |name: &str| schema.map_or_else(|| name.to_owned(), |s| s.table(name));
    format!(
        " LEFT JOIN {} {request} ON {request}.request_id={history}.query_id AND ({request}.namespace_id=${namespace} OR {request}.namespace_id IS NULL) AND (({request}.namespace_id=${namespace} AND {history}.namespace_id=${namespace} AND {request}.owner_principal_id IS NOT DISTINCT FROM {history}.owner_principal_id) OR ({request}.namespace_id IS NULL AND {request}.owner_principal_id IS NULL AND EXISTS(SELECT 1 FROM {} dl WHERE dl.namespace_id=${namespace} AND dl.legacy_name={request}.principal AND dl.principal_id={owner})))",
        table("requests"),
        table("authz_legacy_owners")
    )
}
