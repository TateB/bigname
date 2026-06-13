use super::*;

const PERSISTED_PRIMARY_NAME_VERIFIED_READBACK_SCAN_LIMIT: i64 = 16;

pub(super) async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
) -> ApiResult<PrimaryNameLookupState> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    match load_primary_name_current_snapshot(pool, address, namespace, &coin_type).await {
        Ok(Some(snapshot)) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TuplePresent(snapshot.row),
            normalized_claim_name: mode
                .includes_declared()
                .then_some(snapshot.normalized_claim_name)
                .flatten(),
            on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
            on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
            persisted_verified: if mode.includes_verified() {
                load_persisted_primary_name_verified_readback(pool, address, namespace, &coin_type)
                    .await?
            } else {
                None
            },
        }),
        Ok(None) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TupleMissing,
            normalized_claim_name: None,
            on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
            on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
            persisted_verified: None,
        }),
        Err(load_error) if primary_name_projection_unavailable(&load_error) => {
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::ProjectionUnavailable,
                normalized_claim_name: None,
                on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
                on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
                persisted_verified: None,
            })
        }
        Err(load_error) => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load primary-name tuple state"
            );
            Err(ApiError::internal_error(format!(
                "failed to load primary-name tuple for address {address}"
            )))
        }
    }
}

pub(super) fn primary_name_projection_unavailable(load_error: &anyhow::Error) -> bool {
    load_error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .is_some_and(|sqlx_error| {
                matches!(
                    sqlx_error,
                    sqlx::Error::Database(error) if error.code().as_deref() == Some("42P01")
                )
            })
    })
}

pub(super) fn canonical_primary_name_coin_type(coin_type: &str) -> ApiResult<String> {
    bigname_storage::canonical_addr_coin_type(coin_type).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "coin_type must fit in an unsigned 64-bit integer".to_owned(),
    })
}

pub(super) async fn load_persisted_primary_name_verified_readback(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<Option<PersistedPrimaryNameVerifiedReadback>> {
    let request_key = primary_name_verified_request_key(namespace, address, coin_type);
    let rows = sqlx::query(
        r#"
        SELECT
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = $2
          AND request_key = $3
        ORDER BY finished_at DESC, execution_trace_id DESC
        LIMIT $4
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(namespace)
    .bind(&request_key)
    .bind(PERSISTED_PRIMARY_NAME_VERIFIED_READBACK_SCAN_LIMIT)
    .fetch_all(pool)
    .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            return Ok(None);
        }
        Err(load_error) => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load persisted verified primary-name outcome"
            );
            return Err(ApiError::internal_error(format!(
                "failed to load persisted verified primary-name outcome for address {address}"
            )));
        }
    };

    for row in rows {
        let outcome = ExecutionOutcome {
            cache_key: ExecutionCacheKey {
                request_key: row.try_get("request_key").map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name request_key"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
                requested_chain_positions: row
                    .try_get("requested_chain_positions")
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            address = %address,
                            namespace = %namespace,
                            coin_type = %coin_type,
                            error = ?load_error,
                            "failed to decode persisted verified primary-name requested_chain_positions"
                        );
                        ApiError::internal_error(format!(
                            "failed to decode persisted verified primary-name outcome for address {address}"
                        ))
                    })?,
                manifest_versions: row.try_get("manifest_versions").map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name manifest_versions"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
                topology_version_boundary: row
                    .try_get("topology_version_boundary")
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            address = %address,
                            namespace = %namespace,
                            coin_type = %coin_type,
                            error = ?load_error,
                            "failed to decode persisted verified primary-name topology_version_boundary"
                        );
                        ApiError::internal_error(format!(
                            "failed to decode persisted verified primary-name outcome for address {address}"
                        ))
                    })?,
                record_version_boundary: row
                    .try_get("record_version_boundary")
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            address = %address,
                            namespace = %namespace,
                            coin_type = %coin_type,
                            error = ?load_error,
                            "failed to decode persisted verified primary-name record_version_boundary"
                        );
                        ApiError::internal_error(format!(
                            "failed to decode persisted verified primary-name outcome for address {address}"
                        ))
                    })?,
            },
            execution_trace_id: row.try_get("execution_trace_id").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name execution_trace_id"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            request_type: row.try_get("request_type").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name request_type"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            namespace: row.try_get("namespace").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name namespace"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            outcome_payload: row.try_get("outcome_payload").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name outcome_payload"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            failure_payload: row.try_get("failure_payload").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name failure_payload"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            finished_at: row.try_get("finished_at").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name finished_at"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
        };

        if outcome.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE
            || outcome.namespace != namespace
            || outcome.cache_key.request_key != request_key
        {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                request_type = %outcome.request_type,
                cached_namespace = %outcome.namespace,
                cached_request_key = %outcome.cache_key.request_key,
                "persisted verified primary-name outcome identity mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name outcome identity mismatch for address {address}"
            )));
        }

        let trace = load_execution_trace(pool, outcome.execution_trace_id)
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    execution_trace_id = %outcome.execution_trace_id,
                    error = ?load_error,
                    "failed to load persisted verified primary-name trace"
                );
                ApiError::internal_error(format!(
                    "failed to load persisted verified primary-name trace for address {address}"
                ))
            })?
            .ok_or_else(|| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    execution_trace_id = %outcome.execution_trace_id,
                    "persisted verified primary-name trace missing"
                );
                ApiError::internal_error(format!(
                    "persisted verified primary-name trace missing for address {address}"
                ))
            })?;

        if !persisted_verified_primary_name_cache_identity_is_current(
            &trace, &outcome, address, namespace, coin_type,
        )? {
            continue;
        }

        let verified_primary_name = persisted_verified_primary_name_section(
            &trace, &outcome, address, namespace, coin_type,
        )?;
        let provenance = primary_name_verified_readback_provenance(
            &trace, &outcome, address, namespace, coin_type,
        )?;

        return Ok(Some(PersistedPrimaryNameVerifiedReadback {
            verified_primary_name,
            provenance,
            finished_at: outcome.finished_at,
        }));
    }

    Ok(None)
}

pub(super) fn primary_name_verified_request_key(
    namespace: &str,
    address: &str,
    coin_type: &str,
) -> String {
    format!("{namespace}:{address}:{coin_type}")
}

pub(super) async fn load_on_demand_primary_name_claim(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<OnDemandPrimaryNameClaimState> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    if namespace != bigname_storage::ENS_NAMESPACE || coin_type != "60" {
        return Ok(OnDemandPrimaryNameClaimState::NotAttempted);
    }

    let Some(on_demand) = (match bigname_execution::lookup_ens_reverse_primary_name(
        bigname_execution::OnDemandEnsPrimaryNameRequest {
            normalized_address: address,
            chain_rpc_urls: &state.chain_rpc_urls,
        },
    )
    .await
    {
        Ok(on_demand) => on_demand,
        Err(error) => {
            warn!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error_kind = ?error.kind(),
                error = %error.message(),
                "on-demand primary-name reverse lookup failed"
            );
            return Ok(OnDemandPrimaryNameClaimState::Unavailable);
        }
    }) else {
        return Ok(OnDemandPrimaryNameClaimState::NotFound);
    };

    let parsed = match normalize_inferred_route_name(&on_demand.name) {
        Ok(parsed) => parsed,
        Err(error) => {
            warn!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                raw_name = %on_demand.name,
                error = %error.message,
                "on-demand primary-name reverse lookup returned an unnormalizable name"
            );
            return Ok(OnDemandPrimaryNameClaimState::InvalidName(
                OnDemandPrimaryNameInvalidClaim {
                    raw_name: on_demand.name,
                    resolver_address: on_demand.resolver_address,
                },
            ));
        }
    };
    if parsed.namespace != namespace {
        return Ok(OnDemandPrimaryNameClaimState::NotFound);
    }

    Ok(OnDemandPrimaryNameClaimState::Found(
        OnDemandPrimaryNameClaim {
            normalized_name: parsed.normalized_name,
            resolver_address: on_demand.resolver_address,
        },
    ))
}

pub(super) async fn load_on_demand_primary_name_verification(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
    claim: &OnDemandPrimaryNameClaim,
) -> ApiResult<OnDemandPrimaryNameVerificationState> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    if namespace != bigname_storage::ENS_NAMESPACE || coin_type != "60" {
        return Ok(OnDemandPrimaryNameVerificationState::NotAttempted);
    }

    let verification = match bigname_execution::verify_ens_primary_name_forward_address(
        bigname_execution::OnDemandEnsPrimaryNameVerificationRequest {
            normalized_address: address,
            normalized_name: &claim.normalized_name,
            chain_rpc_urls: &state.chain_rpc_urls,
        },
    )
    .await
    {
        Ok(verification) => verification,
        Err(error) => {
            warn!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                normalized_name = %claim.normalized_name,
                error_kind = ?error.kind(),
                error = %error.message(),
                "on-demand primary-name forward verification failed"
            );
            return Ok(OnDemandPrimaryNameVerificationState::Verified(json!({
                "status": "execution_failed",
                "failure_reason": "resolver_call_failed",
            })));
        }
    };

    let name = on_demand_primary_name_ref(namespace, &claim.normalized_name)?;
    let section = match verification.resolved_address {
        Some(resolved_address) if resolved_address.eq_ignore_ascii_case(address) => json!({
            "status": "success",
            "name": name,
        }),
        Some(_) => json!({
            "status": "mismatch",
            "name": name,
            "failure_reason": "resolved_target_mismatch",
        }),
        None => json!({
            "status": "not_found",
        }),
    };
    Ok(OnDemandPrimaryNameVerificationState::Verified(section))
}

fn on_demand_primary_name_ref(namespace: &str, normalized_name: &str) -> ApiResult<JsonValue> {
    let namehash = bigname_execution::ens_namehash_hex(normalized_name).map_err(|error| {
        error!(
            service = "api",
            namespace = %namespace,
            normalized_name = %normalized_name,
            error = ?error,
            "failed to build on-demand primary-name namehash"
        );
        ApiError::internal_error(format!(
            "failed to build on-demand primary-name identity for {namespace}/{normalized_name}"
        ))
    })?;

    Ok(json!({
        "logical_name_id": format!("{namespace}:{normalized_name}"),
        "namespace": namespace,
        "normalized_name": normalized_name,
        "canonical_display_name": normalized_name,
        "namehash": namehash,
    }))
}
