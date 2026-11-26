# Changelog

All notable changes to the `duckfleet` crate. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate follows Semantic Versioning.

## [0.1.0] - 2026-11-26

### Added

- `Client`, `Session` (`RemoteConnection`), and `SessionOptions` for explicit sessions with compute selection, context, and bounded open waiting.
- Typed `Param` binding, `Statement` timeouts and caller request IDs, and `QueryResult` with Arrow schema and batches.
- Durable jobs: `submit_job`, `job`, `wait_job`, `cancel_job`, `job_result`, and verified, resumable `download_job_result`.
- `ErrorKind` classification with accessors for `failure_kind`, `retry_safe`, `request_id`, `acceptance`, and `status_url`. SQL is never replayed.
