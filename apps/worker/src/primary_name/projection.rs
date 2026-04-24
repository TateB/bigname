use anyhow::{Context, Result, bail};
use bigname_storage::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    clear_primary_names_current, delete_primary_name_current,
    upsert_primary_name_current_snapshots,
};
use serde_json::{Map, Value, json};
use sqlx::PgPool;

use super::{
    PrimaryNamesCurrentRebuildSummary,
    query::{
        load_latest_name_claim_observation, load_latest_name_claim_observations,
        load_reverse_claim_tuple, load_reverse_claim_tuples,
    },
    types::{NameClaimObservation, PrimaryNameTupleKey, ReverseClaimTuple},
};

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
    let tuples = load_reverse_claim_tuples(pool).await?;
    let claim_observations = load_latest_name_claim_observations(pool).await?;
    let projections = tuples
        .iter()
        .map(|tuple| {
            let observation = claim_observations.get(&tuple.key);
            primary_name_row(tuple, observation)
        })
        .collect::<Result<Vec<PrimaryNameCurrentSnapshot>>>()?;
    let rows = projections
        .iter()
        .map(|projection| projection.row.clone())
        .collect::<Vec<_>>();
    let upserted_row_count = upsert_primary_name_current_snapshots(pool, &projections)
        .await?
        .len();
    let deleted_row_count = delete_stale_primary_name_current_rows(pool, &projections).await?;
    let status_counts = count_statuses(&rows);

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count: tuples.len(),
        upserted_row_count,
        deleted_row_count,
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
    let upserted_row_count = match projected_row.as_ref() {
        Some(projection) => {
            upsert_primary_name_current_snapshots(pool, std::slice::from_ref(projection))
                .await?
                .len()
        }
        None => 0,
    };
    let deleted_row_count = match projected_row.as_ref() {
        Some(_) => 0,
        None => {
            delete_primary_name_current(pool, &target.address, &target.namespace, &target.coin_type)
                .await?
        }
    };
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

async fn delete_stale_primary_name_current_rows(
    pool: &PgPool,
    projections: &[PrimaryNameCurrentSnapshot],
) -> Result<u64> {
    if projections.is_empty() {
        return clear_primary_names_current(pool).await;
    }

    let addresses = projections
        .iter()
        .map(|projection| projection.row.address.clone())
        .collect::<Vec<_>>();
    let namespaces = projections
        .iter()
        .map(|projection| projection.row.namespace.clone())
        .collect::<Vec<_>>();
    let coin_types = projections
        .iter()
        .map(|projection| projection.row.coin_type.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM primary_names_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[], $2::TEXT[], $3::TEXT[]) AS replacement(
                address,
                namespace,
                coin_type
            )
            WHERE replacement.address = current.address
              AND replacement.namespace = current.namespace
              AND replacement.coin_type = current.coin_type
        )
        "#,
    )
    .bind(&addresses)
    .bind(&namespaces)
    .bind(&coin_types)
    .execute(pool)
    .await
    .context("failed to delete stale primary_names_current rows after rebuild")
    .map(|result| result.rows_affected())
}

fn primary_name_row(
    tuple: &ReverseClaimTuple,
    claim_observation: Option<&NameClaimObservation>,
) -> Result<PrimaryNameCurrentSnapshot> {
    let (claim_status, raw_claim_name) =
        match claim_observation.and_then(|observation| observation.raw_name.as_deref()) {
            Some(raw_name) if claim_name_looks_normalizable(raw_name) => {
                (PrimaryNameClaimStatus::Success, None)
            }
            Some(raw_name) => (
                PrimaryNameClaimStatus::InvalidName,
                Some(raw_name.to_owned()),
            ),
            None => (PrimaryNameClaimStatus::NotFound, None),
        };

    let normalized_claim_name = claim_observation
        .and_then(|observation| observation.raw_name.as_deref())
        .filter(|_| claim_status == PrimaryNameClaimStatus::Success)
        .map(normalize_claim_name);

    Ok(PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: tuple.key.address.clone(),
            namespace: tuple.key.namespace.clone(),
            coin_type: tuple.key.coin_type.clone(),
            claim_status,
            raw_claim_name,
            claim_provenance: build_claim_provenance(tuple, claim_status, claim_observation)?,
        },
        normalized_claim_name,
    })
}

fn build_claim_provenance(
    tuple: &ReverseClaimTuple,
    claim_status: PrimaryNameClaimStatus,
    claim_observation: Option<&NameClaimObservation>,
) -> Result<Value> {
    let mut claim_provenance = tuple
        .claim_provenance
        .as_object()
        .cloned()
        .context("reverse-claim claim_provenance must be a JSON object")?;
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

fn claim_name_looks_normalizable(raw_name: &str) -> bool {
    if raw_name.is_empty()
        || raw_name.trim() != raw_name
        || raw_name.len() > 255
        || !raw_name.is_ascii()
    {
        return false;
    }

    raw_name.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
    })
}

fn normalize_claim_name(raw_name: &str) -> String {
    raw_name.to_ascii_lowercase()
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StatusCounts {
    success_row_count: usize,
    not_found_row_count: usize,
    invalid_name_row_count: usize,
}

fn count_statuses(rows: &[PrimaryNameCurrentRow]) -> StatusCounts {
    let mut counts = StatusCounts::default();

    for row in rows {
        match row.claim_status {
            PrimaryNameClaimStatus::Success => counts.success_row_count += 1,
            PrimaryNameClaimStatus::NotFound => counts.not_found_row_count += 1,
            PrimaryNameClaimStatus::InvalidName => counts.invalid_name_row_count += 1,
            PrimaryNameClaimStatus::Unsupported => {}
        }
    }

    counts
}

fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}
