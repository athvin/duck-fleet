//! Durable job HTTP and dispatch. No request handler waits for engine execution.
use super::*;
use crate::{artifacts, job_artifacts, job_policy::JobRequest, jobs};

fn refusal(error: ApiError, id: Uuid, acceptance: Acceptance) -> Response {
    let status_url = format!("/v1/jobs/{id}");
    (
        error.0.http,
        [
            ("x-query-id", id.to_string()),
            ("x-query-status-url", status_url.clone()),
            ("x-query-acceptance", acceptance.as_str().to_owned()),
        ],
        Json(json!({"error":error.1,"failure_kind":error.0.kind,
        "retry_safe":error.0.retry_safe && acceptance != Acceptance::Unknown,
        "request_id":id,"acceptance":acceptance,"status_url":status_url})),
    )
        .into_response()
}

pub(super) async fn submit(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request: JobRequest =
        serde_json::from_slice(&body).map_err(|_| ApiError::bad("invalid job request"))?;
    let id = request.request_id;
    let request = match request.validate() {
        Ok(request) => request,
        Err(error) => return Ok(refusal(error, id, Acceptance::NotAccepted)),
    };
    if app.draining.load(Ordering::Acquire) {
        return Ok(refusal(draining_refusal(), id, Acceptance::NotAccepted));
    }
    let enabled = app.artifact_store.is_some();
    let registry = app.registry();
    // The bounded blocking transaction continues after a client disconnect. Its
    // stable caller ID can reconcile a lost response without executing SQL twice.
    let accepted = app
        .control_db(move |db| jobs::submit(db, &identity, request, &registry, enabled))
        .await;
    Ok(match accepted {
        Ok(jobs::SubmitResult::Accepted(value)) => {
            (StatusCode::ACCEPTED, Json(value)).into_response()
        }
        Ok(jobs::SubmitResult::Duplicate(value)) => {
            (StatusCode::CONFLICT, Json(value)).into_response()
        }
        Ok(jobs::SubmitResult::Refused(error)) => refusal(error, id, Acceptance::Accepted),
        Err(error) => {
            let acceptance = if error.0 == failure::UNAVAILABLE {
                Acceptance::Unknown
            } else {
                Acceptance::NotAccepted
            };
            refusal(error, id, acceptance)
        }
    })
}

pub(super) async fn detail(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    app.control_db(move |db| jobs::detail(db, &identity, id))
        .await
        .map(Json)
}

/// The response retains its byte admission until Hyper releases the actual data,
/// even when a peer stops consuming. Delivery has a separate absolute deadline.
struct Download {
    _delivery: crate::http_transport::DeliveryGuard,
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Download {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Gateway {
    async fn bind_job(&self, claim: jobs::BindingClaim) -> Result<(), ApiError> {
        let token = claim.token.clone();
        let check = token.clone();
        // Recheck the accepted credential before provisioning any compute.
        if let Err(error) = self
            .control_db(move |db| jobs::binding_for_worker(db, &check))
            .await
        {
            if error.0 != failure::UNAVAILABLE {
                let failure = error.0;
                self.control_db(move |db| {
                    jobs::refuse_unbound(db, &token, failure).map_err(|_| ApiError::internal())
                })
                .await?;
            }
            return Err(error);
        }
        let mut target = match claim.target {
            Some(target) => target,
            None => {
                let target = match claim.placement.mode {
                    Mode::Persistent => jobs::BindingTarget {
                        endpoint: self
                            .endpoints_for(&self.registry(), &claim.placement)
                            .await?
                            .into_iter()
                            .next(),
                        pod_name: None,
                    },
                    Mode::Isolated => jobs::BindingTarget {
                        endpoint: None,
                        // UUID session identity is unique per generation and fits
                        // the Kubernetes63-character name limit with this prefix.
                        pod_name: Some(format!("{}{}", crate::isolated::PREFIX, token.session_id)),
                    },
                };
                let saved = target.clone();
                let binding = token.clone();
                self.control_db(move |db| {
                    jobs::record_binding_target(db, &binding, saved)
                        .map_err(|_| ApiError::internal())
                })
                .await?;
                target
            }
        };
        if let Some(pod_name) = target.pod_name.clone() {
            let Some(isolation) = self.isolation.as_ref() else {
                self.control_db(move |db| {
                    jobs::refuse_unbound(db, &token, failure::COMPUTE_UNAVAILABLE)
                        .map_err(|_| ApiError::internal())
                })
                .await?;
                return Ok(());
            };
            let binding = token.clone();
            let existing = self
                .control_db(move |db| {
                    jobs::inspect_binding(db, &binding).map_err(|_| ApiError::internal())
                })
                .await?;
            let lifecycle_id = pod_name.clone();
            let principal_id = claim.principal_id.clone();
            let reserved = self
                .control_db(move |db| {
                    let mut tx = db.transaction().map_err(|_| ApiError::internal())?;
                    crate::compute_lifecycle::reserve(
                        &mut tx,
                        &lifecycle_id,
                        principal_id.as_deref(),
                        "isolated",
                        claim.placement.resources,
                    )?;
                    tx.commit().map_err(|_| ApiError::internal())
                })
                .await;
            if let Err(error) = reserved {
                if error.0 == failure::CAPACITY {
                    self.control_db(move |db| {
                        jobs::refuse_unbound(db, &token, failure::CAPACITY)
                            .map_err(|_| ApiError::internal())
                    })
                    .await?;
                    return Ok(());
                }
                return Err(error);
            }
            let pod = isolation
                .start_job(
                    claim.placement.resources,
                    &pod_name,
                    existing.pod_uid.as_deref(),
                    claim.policy.limits.runtime_seconds,
                )
                .await;
            let pod = match pod {
                Ok(pod) => pod,
                Err(error) if error.0 == failure::JOB_PREPARE_REFUSED => {
                    self.control_db(move |db| {
                        jobs::refuse_unbound(db, &token, failure::JOB_PREPARE_REFUSED)
                            .map_err(|_| ApiError::internal())
                    })
                    .await?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let binding = token.clone();
            self.control_db(move |db| {
                jobs::record_pod_uid(db, &binding, &pod.uid).map_err(|_| ApiError::internal())
            })
            .await?;
            target.endpoint = Some(pod.pod.endpoint);
            let binding = token.clone();
            let saved = target.clone();
            self.control_db(move |db| {
                jobs::record_binding_target(db, &binding, saved).map_err(|_| ApiError::internal())
            })
            .await?;
        }
        let endpoint = target.endpoint.ok_or_else(ApiError::internal)?;
        let body = Bytes::from(serde_json::to_vec(&token).map_err(|_| ApiError::internal())?);
        // A timeout is an ambiguous Open. Only the same persisted target/token
        // may be retried; neither timeout nor HTTP failure permits SQL replay.
        let response = tokio::time::timeout(
            Duration::from_secs(15),
            self.send(&endpoint, Method::POST, "/v1/internal/jobs/open", body),
        )
        .await
        .map_err(|_| ApiError::internal())??;
        if !response.status().is_success() {
            return Err(ApiError::internal());
        }
        let binding = token.clone();
        let observed = self
            .control_db(move |db| {
                jobs::inspect_binding(db, &binding).map_err(|_| ApiError::internal())
            })
            .await?;
        if !matches!(observed.state.as_str(), "bound" | "closed") {
            return Err(ApiError::internal());
        }
        Ok(())
    }

    async fn bind_job_with_lease(&self, claim: jobs::BindingClaim) {
        let token = claim.token.clone();
        let operation = self.bind_job(claim);
        tokio::pin!(operation);
        let mut renew = tokio::time::interval(Duration::from_secs(3));
        renew.tick().await;
        loop {
            tokio::select! {
                result = &mut operation => {
                    if let Err(error) = result {
                        tracing::debug!(request_id=%token.request_id, kind=error.0.kind, "job binding remains pending reconciliation");
                    }
                    return;
                },
                _ = renew.tick() => {
                    let binding = token.clone();
                    if !matches!(self.control_db(move |db| jobs::renew_binding(db, &binding)
                        .map_err(|_| ApiError::internal())).await, Ok(true)) {
                        return;
                    }
                }
            }
        }
    }

    pub(super) fn spawn_job_dispatcher(&self) -> tokio::task::JoinHandle<()> {
        let app = self.clone();
        tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick.tick() => {},
                    _ = tasks.join_next(), if !tasks.is_empty() => continue,
                }
                if app.draining.load(Ordering::Acquire) {
                    return;
                }
                // Queued cancellations and queue deadlines are terminalized here,
                // not on the minute-scale maintenance tick: an unbound job has no
                // worker control lane, so the dispatcher is its only prompt sweep.
                // The pass is bounded and per-tick small; an in-flight Open for a
                // swept job is refused at the worker because its binding is closed.
                let swept = app
                    .control_db(move |db| {
                        jobs::expire_unbound(db, 16, Duration::from_millis(100))
                            .map_err(|_| ApiError::internal())
                    })
                    .await;
                if let Err(error) = swept {
                    tracing::debug!(kind = error.0.kind, "job queue expiry sweep unavailable");
                }
                if tasks.len() >= 4 {
                    continue;
                }
                let owner = app.dispatcher_owner;
                match app
                    .control_db(move |db| {
                        jobs::claim_binding(db, owner).map_err(|_| ApiError::internal())
                    })
                    .await
                {
                    Ok(Some(claim)) => {
                        let app = app.clone();
                        tasks.spawn(async move {
                            app.bind_job_with_lease(claim).await;
                        });
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::debug!(kind = error.0.kind, "job dispatcher metadata unavailable")
                    }
                }
            }
        })
    }

    pub(super) async fn reconcile_job_pods(&self) {
        let Some(isolation) = self.isolation.as_ref() else {
            return;
        };
        let Ok(candidates) = self
            .control_db(|db| jobs::binding_pod_candidates(db, 4).map_err(|_| ApiError::internal()))
            .await
        else {
            return;
        };
        for candidate in candidates {
            let pod = candidate.cleanup;
            // The target is persisted before the named pod is created. Until a UID
            // proves that creation happened, absence cannot distinguish a dead pod
            // from this ordinary provisioning window.
            if !candidate.terminal && pod.pod_uid.is_none() {
                continue;
            }
            let absent = if candidate.terminal {
                isolation
                    .stop_job_confirmed(&pod.pod_name, pod.pod_uid.as_deref())
                    .await
            } else {
                matches!(
                    isolation
                        .job_pod_present(&pod.pod_name, pod.pod_uid.as_deref())
                        .await,
                    Ok(false)
                )
            };
            if !absent {
                continue;
            }
            let worker = pod.pod_name.clone();
            let rate = self.rate.clone();
            let recovered = self
                .control_db(move |db| {
                    crate::compute_lifecycle::finish(db, &pod.pod_name, Some("job_pod_retired"))?;
                    queue::recover_isolated(db, &worker, &rate)
                        .map_err(|_| ApiError::internal())?;
                    jobs::confirm_pod_cleanup(db, &pod).map_err(|_| ApiError::internal())
                })
                .await;
            if let Err(error) = recovered {
                tracing::debug!(kind = error.0.kind, "job pod retirement remains pending");
            }
        }
    }

    async fn result_object(
        &self,
        identity: Identity,
        id: Uuid,
        chunk: Option<u32>,
        connection: crate::http_transport::Connection,
    ) -> Result<Response, ApiError> {
        let permit =
            self.result_slots.clone().try_acquire_owned().map_err(|_| {
                ApiError::new(failure::CAPACITY, "result download capacity reached")
            })?;
        let descriptor = self
            .control_db(move |db| {
                let mut tx = db.transaction().map_err(|_| ApiError::internal())?;
                jobs::bounded(&mut tx).map_err(|_| ApiError::internal())?;
                let access = authz::resolve_in(&mut tx, &identity)?;
                let object = job_artifacts::authorize_in(&mut tx, &access, id, chunk)?;
                tx.commit().map_err(|_| ApiError::internal())?;
                Ok(object)
            })
            .await?;
        let store = self.artifact_store.as_ref().ok_or_else(|| {
            ApiError::new(
                failure::RESULT_FETCH_UNAVAILABLE,
                "result store is unavailable",
            )
        })?;
        let maximum = if chunk.is_some() {
            artifacts::MAX_CHUNK_BYTES
        } else {
            artifacts::MAX_MANIFEST_BYTES
        };
        let canceled = AtomicBool::new(false);
        let bytes = store.get(&descriptor.relative_key, descriptor.bytes, &descriptor.sha256, maximum,
            tokio::time::Instant::now() + Duration::from_secs(10), &canceled).await
            .map_err(|_| ApiError::new(failure::RESULT_FETCH_UNAVAILABLE,
                "the complete result object could not be verified; retry retrieval using the same job ID"))?;
        if chunk.is_none() {
            let manifest: artifacts::Manifest = serde_json::from_slice(&bytes).map_err(|_| {
                ApiError::new(
                    failure::RESULT_FETCH_UNAVAILABLE,
                    "result manifest is invalid",
                )
            })?;
            if manifest.request_id != descriptor.request_id
                || manifest.generation != descriptor.generation
                || manifest.encode().is_err()
            {
                return Err(ApiError::new(
                    failure::RESULT_FETCH_UNAVAILABLE,
                    "result manifest is invalid",
                ));
            }
        }
        let mime = if chunk.is_some() {
            "application/vnd.apache.arrow.stream"
        } else {
            "application/json"
        };
        let bytes = Bytes::from_owner(Download {
            _delivery: connection.delivery_with_timeout(Duration::from_secs(30)),
            bytes,
            _permit: permit,
        });
        Ok((
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, "no-store"),
            ],
            bytes,
        )
            .into_response())
    }
}

pub(super) async fn manifest(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path(id): Path<Uuid>,
    ConnectInfo(connection): ConnectInfo<crate::http_transport::Connection>,
) -> Result<Response, ApiError> {
    app.result_object(identity, id, None, connection).await
}
pub(super) async fn chunk(
    State(app): State<Gateway>,
    Extension(identity): Extension<Identity>,
    Path((id, chunk)): Path<(Uuid, u32)>,
    ConnectInfo(connection): ConnectInfo<crate::http_transport::Connection>,
) -> Result<Response, ApiError> {
    app.result_object(identity, id, Some(chunk), connection)
        .await
}
