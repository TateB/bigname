use anyhow::{Context, Result, bail};
use bigname_storage::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    delete_primary_name_current_in_transaction,
    load_primary_name_current_snapshot_for_update_in_transaction, normalize_evm_address,
    upsert_primary_name_current_snapshots_in_transaction, verified_primary_name_claim_hooks,
};
use futures_util::{TryStreamExt, pin_mut};
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, PgPool, Row};

#[allow(clippy::duplicate_mod)]
#[path = "../staged_rebuild.rs"]
mod staged_rebuild;

use staged_rebuild::{
    PRIMARY_NAMES_CURRENT_COLUMNS, count_rows, create_stage_table, drop_stage_table,
    publish_stage_table, stage_primary_names_current_snapshots,
};

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{Arc, Mutex};

    type TargetedRebuildAfterInvalidationHook =
        Arc<dyn Fn(&str, &str, &str) + Send + Sync + 'static>;

    static TARGETED_REBUILD_AFTER_INVALIDATION_HOOK: Mutex<
        Option<TargetedRebuildAfterInvalidationHook>,
    > = Mutex::new(None);

    pub(crate) struct TargetedRebuildAfterInvalidationHookGuard;

    impl Drop for TargetedRebuildAfterInvalidationHookGuard {
        fn drop(&mut self) {
            *TARGETED_REBUILD_AFTER_INVALIDATION_HOOK
                .lock()
                .expect("targeted rebuild after invalidation hook mutex poisoned") = None;
        }
    }

    pub(crate) fn install_targeted_rebuild_after_invalidation_hook(
        hook: TargetedRebuildAfterInvalidationHook,
    ) -> TargetedRebuildAfterInvalidationHookGuard {
        *TARGETED_REBUILD_AFTER_INVALIDATION_HOOK
            .lock()
            .expect("targeted rebuild after invalidation hook mutex poisoned") = Some(hook);
        TargetedRebuildAfterInvalidationHookGuard
    }

    pub(super) fn run_targeted_rebuild_after_invalidation_hook(
        address: &str,
        namespace: &str,
        coin_type: &str,
    ) {
        let hook = TARGETED_REBUILD_AFTER_INVALIDATION_HOOK
            .lock()
            .expect("targeted rebuild after invalidation hook mutex poisoned")
            .clone();
        if let Some(hook) = hook {
            hook(address, namespace, coin_type);
        }
    }
}

use super::{
    PrimaryNamesCurrentRebuildSummary,
    query::{
        load_latest_name_claim_observation, load_reverse_claim_tuple,
        stream_primary_name_rebuild_inputs,
    },
    types::{NameClaimObservation, PrimaryNameTupleKey, ReverseClaimTuple},
};

const PRIMARY_NAMES_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;

pub async fn rebuild_primary_names_current(
    pool: &PgPool,
    address: Option<&str>,
    namespace: Option<&str>,
    coin_type: Option<&str>,
) -> Result<PrimaryNamesCurrentRebuildSummary> {
    match (address, namespace, coin_type) {
        (Some(address), Some(namespace), Some(coin_type)) => {
            rebuild_one_primary_name(pool, address, namespace, coin_type).await
        }
        (None, None, None) => rebuild_all_primary_names(pool).await,
        _ => bail!(
            "primary_names_current rebuild requires address, namespace, and coin_type together when targeting one tuple"
        ),
    }
}

async fn rebuild_all_primary_names(pool: &PgPool) -> Result<PrimaryNamesCurrentRebuildSummary> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire primary_names_current staging connection")?;
    let stage_table = create_stage_table(&mut conn, "primary_names_current").await?;
    let previous_row_count = count_rows(&mut conn, "primary_names_current", None).await?;
    let mut projections = Vec::with_capacity(PRIMARY_NAMES_CURRENT_REBUILD_BATCH_SIZE);
    let mut status_counts = StatusCounts::default();
    let mut requested_tuple_count = 0usize;
    let mut upserted_row_count = 0usize;

    let inputs = stream_primary_name_rebuild_inputs(pool);
    pin_mut!(inputs);

    while let Some(input) = inputs.try_next().await? {
        requested_tuple_count += 1;
        let projection = primary_name_row(&input.tuple, input.claim_observation.as_ref())?;
        add_status(&mut status_counts, &projection.row);
        projections.push(projection);

        if projections.len() >= PRIMARY_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count +=
                stage_primary_names_current_snapshots(&mut conn, &stage_table, &projections).await?
                    as usize;
            projections.clear();
        }

        if requested_tuple_count % 5_000 == 0 {
            tracing::info!(
                projection = "primary_names_current",
                queued_tuple_count = requested_tuple_count,
                completed_tuple_count = requested_tuple_count,
                upserted_row_count,
                "primary_names_current rebuild tuples processed"
            );
        }
    }

    if !projections.is_empty() {
        upserted_row_count +=
            stage_primary_names_current_snapshots(&mut conn, &stage_table, &projections).await?
                as usize;
    }
    let invalidation_targets =
        load_full_rebuild_claim_change_invalidation_targets(&mut conn, &stage_table).await?;
    invalidate_verified_primary_name_claim_changes(pool, &invalidation_targets).await?;
    let (_deleted_row_count, published_row_count) = publish_stage_table(
        &mut conn,
        "primary_names_current",
        &stage_table,
        PRIMARY_NAMES_CURRENT_COLUMNS,
        None,
    )
    .await?;
    drop_stage_table(&mut conn, &stage_table).await?;
    debug_assert_eq!(published_row_count as usize, upserted_row_count);

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count,
        upserted_row_count,
        deleted_row_count: previous_row_count,
        success_row_count: status_counts.success_row_count,
        not_found_row_count: status_counts.not_found_row_count,
        invalid_name_row_count: status_counts.invalid_name_row_count,
    })
}

async fn rebuild_one_primary_name(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<PrimaryNamesCurrentRebuildSummary> {
    let target = PrimaryNameTupleKey {
        address: normalize_address(address),
        namespace: namespace.to_owned(),
        coin_type: coin_type.to_owned(),
    };
    let projected_row = match load_reverse_claim_tuple(pool, &target).await? {
        Some(tuple) => {
            let claim_observation = load_latest_name_claim_observation(pool, &target).await?;
            Some(primary_name_row(&tuple, claim_observation.as_ref())?)
        }
        None => None,
    };
    let mut transaction = pool
        .begin()
        .await
        .with_context(|| {
            format!(
                "failed to open primary_names_current targeted rebuild transaction for address {} namespace {} coin_type {}",
                target.address, target.namespace, target.coin_type
            )
        })?;
    let previous_row = load_primary_name_current_snapshot_for_update_in_transaction(
        &mut transaction,
        &target.address,
        &target.namespace,
        &target.coin_type,
    )
    .await?;
    let upserted_row_count = match projected_row.as_ref() {
        Some(projection) => {
            let claim_row_changed = previous_row.as_ref() != Some(projection);
            if claim_row_changed {
                let hooks = verified_primary_name_claim_hooks(&projection.row)?;
                super::super::execution::invalidate_verified_primary_name_claim_change_in_transaction(
                    &mut transaction,
                    &hooks.lookup.namespace,
                    &hooks.lookup.request_key(),
                )
                .await?;
                #[cfg(test)]
                test_hooks::run_targeted_rebuild_after_invalidation_hook(
                    &target.address,
                    &target.namespace,
                    &target.coin_type,
                );
            }
            upsert_primary_name_current_snapshots_in_transaction(
                &mut transaction,
                std::slice::from_ref(projection),
            )
            .await?
            .len()
        }
        None => 0,
    };
    let deleted_row_count = match projected_row.as_ref() {
        Some(_) => 0,
        None => {
            if previous_row.is_some() {
                super::super::execution::invalidate_verified_primary_name_claim_change_in_transaction(
                    &mut transaction,
                    &target.namespace,
                    &verified_primary_name_request_key(
                        &target.namespace,
                        &target.address,
                        &target.coin_type,
                    ),
                )
                .await?;
                #[cfg(test)]
                test_hooks::run_targeted_rebuild_after_invalidation_hook(
                    &target.address,
                    &target.namespace,
                    &target.coin_type,
                );
            }
            delete_primary_name_current_in_transaction(
                &mut transaction,
                &target.address,
                &target.namespace,
                &target.coin_type,
            )
            .await?
        }
    };
    transaction
        .commit()
        .await
        .with_context(|| {
            format!(
                "failed to commit primary_names_current targeted rebuild for address {} namespace {} coin_type {}",
                target.address, target.namespace, target.coin_type
            )
        })?;
    let projected_rows = projected_row
        .iter()
        .map(|projection| projection.row.clone())
        .collect::<Vec<_>>();
    let status_counts = count_statuses(&projected_rows);

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count: 1,
        upserted_row_count,
        deleted_row_count,
        success_row_count: status_counts.success_row_count,
        not_found_row_count: status_counts.not_found_row_count,
        invalid_name_row_count: status_counts.invalid_name_row_count,
    })
}

async fn load_full_rebuild_claim_change_invalidation_targets(
    conn: &mut PgConnection,
    stage_table: &str,
) -> Result<Vec<VerifiedPrimaryNameClaimChangeTarget>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT
            COALESCE(stage.namespace, current.namespace) AS namespace,
            COALESCE(stage.address, current.address) AS address,
            COALESCE(stage.coin_type, current.coin_type) AS coin_type
        FROM primary_names_current AS current
        FULL OUTER JOIN {stage_table} AS stage
          ON stage.address = current.address
         AND stage.namespace = current.namespace
         AND stage.coin_type = current.coin_type
        WHERE current.address IS NULL
           OR stage.address IS NULL
           OR current.claim_status IS DISTINCT FROM stage.claim_status
           OR current.raw_claim_name IS DISTINCT FROM stage.raw_claim_name
           OR current.normalized_claim_name IS DISTINCT FROM stage.normalized_claim_name
           OR current.claim_provenance IS DISTINCT FROM stage.claim_provenance
        "#
    ))
    .fetch_all(conn)
    .await
    .with_context(|| {
        format!(
            "failed to load verified primary-name claim invalidation targets from staging table {stage_table}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let namespace: String = row.try_get("namespace")?;
            let address: String = row.try_get("address")?;
            let coin_type: String = row.try_get("coin_type")?;
            Ok(VerifiedPrimaryNameClaimChangeTarget {
                request_key: verified_primary_name_request_key(&namespace, &address, &coin_type),
                namespace,
            })
        })
        .collect()
}

async fn invalidate_verified_primary_name_claim_changes(
    pool: &PgPool,
    targets: &[VerifiedPrimaryNameClaimChangeTarget],
) -> Result<()> {
    for target in targets {
        super::super::execution::invalidate_verified_primary_name_claim_change(
            pool,
            &target.namespace,
            &target.request_key,
        )
        .await?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedPrimaryNameClaimChangeTarget {
    namespace: String,
    request_key: String,
}

fn verified_primary_name_request_key(namespace: &str, address: &str, coin_type: &str) -> String {
    format!("{namespace}:{}:{coin_type}", normalize_evm_address(address))
}

pub(super) fn primary_name_row(
    tuple: &ReverseClaimTuple,
    claim_observation: Option<&NameClaimObservation>,
) -> Result<PrimaryNameCurrentSnapshot> {
    primary_name_row_with_provenance_extensions(tuple, claim_observation, [])
}

pub(super) fn primary_name_row_with_provenance_extensions<const N: usize>(
    tuple: &ReverseClaimTuple,
    claim_observation: Option<&NameClaimObservation>,
    extensions: [(&str, Value); N],
) -> Result<PrimaryNameCurrentSnapshot> {
    let raw_claim = claim_observation.and_then(|observation| observation.raw_name.as_deref());
    let normalized_claim = raw_claim
        .filter(|raw_name| !raw_claim_name_source_is_blank(raw_name))
        .and_then(|raw_name| bigname_domain::normalization::normalize_name(raw_name).ok());
    let (claim_status, raw_claim_name) = match raw_claim {
        Some(raw_name) if raw_claim_name_source_is_blank(raw_name) => {
            (PrimaryNameClaimStatus::NotFound, None)
        }
        Some(raw_name) if normalized_claim.is_some() => (PrimaryNameClaimStatus::Success, None),
        Some(raw_name) => (
            PrimaryNameClaimStatus::InvalidName,
            Some(raw_name.to_owned()),
        ),
        None => (PrimaryNameClaimStatus::NotFound, None),
    };
    let normalized_claim_name = normalized_claim.map(|name| name.normalized_name);

    Ok(PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: tuple.key.address.clone(),
            namespace: tuple.key.namespace.clone(),
            coin_type: tuple.key.coin_type.clone(),
            claim_status,
            raw_claim_name,
            claim_provenance: build_claim_provenance(
                tuple,
                claim_status,
                claim_observation,
                extensions,
            )?,
        },
        normalized_claim_name,
    })
}

fn raw_claim_name_source_is_blank(raw_name: &str) -> bool {
    raw_name.is_empty() || raw_name.chars().all(char::is_whitespace)
}

fn build_claim_provenance<'a>(
    tuple: &ReverseClaimTuple,
    claim_status: PrimaryNameClaimStatus,
    claim_observation: Option<&NameClaimObservation>,
    extensions: impl IntoIterator<Item = (&'a str, Value)>,
) -> Result<Value> {
    let mut claim_provenance = tuple
        .claim_provenance
        .as_object()
        .cloned()
        .context("reverse-claim claim_provenance must be a JSON object")?;
    for (key, value) in extensions {
        claim_provenance.insert(key.to_owned(), value);
    }
    claim_provenance.insert(
        VERIFIED_PRIMARY_NAME_LOOKUP_KEY.to_owned(),
        verified_primary_name_lookup_hook(&tuple.key),
    );
    claim_provenance.insert(
        VERIFIED_PRIMARY_NAME_INVALIDATION_KEY.to_owned(),
        verified_primary_name_invalidation_hook(claim_status, claim_observation),
    );
    Ok(Value::Object(claim_provenance))
}

fn verified_primary_name_lookup_hook(key: &PrimaryNameTupleKey) -> Value {
    json!({
        "address": key.address,
        "namespace": key.namespace,
        "coin_type": key.coin_type,
    })
}

fn verified_primary_name_invalidation_hook(
    claim_status: PrimaryNameClaimStatus,
    claim_observation: Option<&NameClaimObservation>,
) -> Value {
    let mut invalidation =
        Map::from_iter([("claim_status".to_owned(), json!(claim_status.as_str()))]);
    if let Some(claim_observation) = claim_observation {
        invalidation.insert(
            "primary_claim_source".to_owned(),
            claim_observation.primary_claim_source.clone(),
        );
    }
    Value::Object(invalidation)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StatusCounts {
    success_row_count: usize,
    not_found_row_count: usize,
    invalid_name_row_count: usize,
}

fn add_status(counts: &mut StatusCounts, row: &PrimaryNameCurrentRow) {
    match row.claim_status {
        PrimaryNameClaimStatus::Success => counts.success_row_count += 1,
        PrimaryNameClaimStatus::NotFound => counts.not_found_row_count += 1,
        PrimaryNameClaimStatus::InvalidName => counts.invalid_name_row_count += 1,
        PrimaryNameClaimStatus::Unsupported => {}
    }
}

fn count_statuses(rows: &[PrimaryNameCurrentRow]) -> StatusCounts {
    let mut counts = StatusCounts::default();

    for row in rows {
        add_status(&mut counts, row);
    }

    counts
}

fn normalize_address(address: &str) -> String {
    normalize_evm_address(address)
}
