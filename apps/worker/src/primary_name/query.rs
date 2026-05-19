use anyhow::{Context, Result, bail};
use bigname_storage::{normalize_evm_address, sql_row};
use futures_util::{Stream, StreamExt};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgRow};

use super::types::{
    NameClaimObservation, PrimaryNameRebuildInput, PrimaryNameTupleKey, ReverseClaimTuple,
};

const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

pub(super) fn stream_primary_name_rebuild_inputs<'a>(
    pool: &'a PgPool,
) -> impl Stream<Item = Result<PrimaryNameRebuildInput>> + 'a {
    sqlx::query(
        r#"
        WITH reverse_claims AS (
            SELECT DISTINCT ON (
                LOWER(ne.after_state->>'address'),
                COALESCE(ne.after_state->>'namespace', ne.namespace),
                ne.after_state->>'coin_type'
            )
                LOWER(ne.after_state->>'address') AS address,
                COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
                ne.after_state->>'coin_type' AS coin_type,
                COALESCE(ne.after_state->'claim_provenance', '{}'::jsonb) AS claim_provenance
            FROM normalized_events ne
            WHERE ne.event_kind = $1
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.after_state->>'address' IS NOT NULL
              AND ne.after_state->>'address' <> ''
              AND COALESCE(ne.after_state->>'namespace', ne.namespace) IS NOT NULL
              AND COALESCE(ne.after_state->>'namespace', ne.namespace) <> ''
              AND ne.after_state->>'coin_type' IS NOT NULL
              AND ne.after_state->>'coin_type' <> ''
            ORDER BY
                LOWER(ne.after_state->>'address') ASC,
                COALESCE(ne.after_state->>'namespace', ne.namespace) ASC,
                ne.after_state->>'coin_type' ASC,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        ),
        name_claims AS (
            SELECT DISTINCT ON (
                LOWER(ne.after_state->'primary_claim_source'->>'address'),
                COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace),
                ne.after_state->'primary_claim_source'->>'coin_type'
            )
                LOWER(ne.after_state->'primary_claim_source'->>'address') AS address,
                COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) AS namespace,
                ne.after_state->'primary_claim_source'->>'coin_type' AS coin_type,
                ne.after_state->>'raw_name' AS raw_name,
                ne.after_state->'primary_claim_source' AS primary_claim_source
            FROM normalized_events ne
            WHERE ne.event_kind = 'RecordChanged'
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.logical_name_id IS NULL
              AND ne.resource_id IS NULL
              AND ne.after_state->>'record_key' = 'name'
              AND ne.after_state ? 'primary_claim_source'
              AND ne.after_state->'primary_claim_source'->>'address' IS NOT NULL
              AND ne.after_state->'primary_claim_source'->>'address' <> ''
              AND COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) IS NOT NULL
              AND COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) <> ''
              AND ne.after_state->'primary_claim_source'->>'coin_type' IS NOT NULL
              AND ne.after_state->'primary_claim_source'->>'coin_type' <> ''
            ORDER BY
                LOWER(ne.after_state->'primary_claim_source'->>'address') ASC,
                COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) ASC,
                ne.after_state->'primary_claim_source'->>'coin_type' ASC,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        )
        SELECT
            reverse_claims.address,
            reverse_claims.namespace,
            reverse_claims.coin_type,
            reverse_claims.claim_provenance,
            name_claims.raw_name,
            name_claims.primary_claim_source
        FROM reverse_claims
        LEFT JOIN name_claims
          ON name_claims.address = reverse_claims.address
         AND name_claims.namespace = reverse_claims.namespace
         AND name_claims.coin_type = reverse_claims.coin_type
        ORDER BY
            reverse_claims.address ASC,
            reverse_claims.namespace ASC,
            reverse_claims.coin_type ASC
        "#,
    )
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .fetch(pool)
    .map(|row| {
        row.context("failed to stream primary_names_current rebuild input")
            .and_then(decode_primary_name_rebuild_input)
    })
}

pub(super) async fn load_reverse_claim_tuple(
    pool: &PgPool,
    target: &PrimaryNameTupleKey,
) -> Result<Option<ReverseClaimTuple>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            COALESCE(ne.after_state->'claim_provenance', '{{}}'::jsonb) AS claim_provenance
        FROM normalized_events ne
        WHERE ne.event_kind = $1
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) = $2
          AND LOWER(ne.after_state->>'address') = $3
          AND ne.after_state->>'coin_type' = $4
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) IS NOT NULL
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
        ORDER BY
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        LIMIT 1
        "#,
    ))
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .bind(&target.namespace)
    .bind(&target.address)
    .bind(&target.coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse-claim tuple for address {} namespace {} coin_type {}",
            target.address, target.namespace, target.coin_type
        )
    })?;

    row.map(decode_reverse_claim_tuple).transpose()
}

pub(super) async fn load_latest_name_claim_observation(
    pool: &PgPool,
    target: &PrimaryNameTupleKey,
) -> Result<Option<NameClaimObservation>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            LOWER(ne.after_state->'primary_claim_source'->>'address') AS address,
            COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) AS namespace,
            ne.after_state->'primary_claim_source'->>'coin_type' AS coin_type,
            ne.after_state->>'raw_name' AS raw_name,
            ne.after_state->'primary_claim_source' AS primary_claim_source
        FROM normalized_events ne
        WHERE ne.event_kind = 'RecordChanged'
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND ne.logical_name_id IS NULL
          AND ne.resource_id IS NULL
          AND ne.after_state->>'record_key' = 'name'
          AND ne.after_state ? 'primary_claim_source'
          AND LOWER(ne.after_state->'primary_claim_source'->>'address') = $2
          AND COALESCE(ne.after_state->'primary_claim_source'->>'namespace', ne.namespace) = $1
          AND ne.after_state->'primary_claim_source'->>'coin_type' = $3
        ORDER BY
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        LIMIT 1
        "#,
    ))
    .bind(&target.namespace)
    .bind(&target.address)
    .bind(&target.coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse-linked name claim observation for address {} namespace {} coin_type {}",
            target.address, target.namespace, target.coin_type
        )
    })?;

    row.map(decode_name_claim_observation).transpose()
}

fn decode_reverse_claim_tuple(row: PgRow) -> Result<ReverseClaimTuple> {
    Ok(ReverseClaimTuple {
        key: decode_tuple_key(&row)?,
        claim_provenance: row
            .try_get("claim_provenance")
            .context("missing reverse-claim claim_provenance")?,
    })
}

fn decode_primary_name_rebuild_input(row: PgRow) -> Result<PrimaryNameRebuildInput> {
    let tuple = ReverseClaimTuple {
        key: decode_tuple_key(&row)?,
        claim_provenance: row
            .try_get("claim_provenance")
            .context("missing reverse-claim claim_provenance")?,
    };
    let primary_claim_source = row
        .try_get::<Option<Value>, _>("primary_claim_source")
        .context("missing primary_claim_source")?;
    let claim_observation = primary_claim_source
        .map(|primary_claim_source| {
            primary_claim_source
                .as_object()
                .context("primary_claim_source must be a JSON object")?;

            Ok::<NameClaimObservation, anyhow::Error>(NameClaimObservation {
                key: tuple.key.clone(),
                raw_name: sql_row::get(&row, "raw_name")?,
                primary_claim_source,
            })
        })
        .transpose()?;

    Ok(PrimaryNameRebuildInput {
        tuple,
        claim_observation,
    })
}

fn decode_name_claim_observation(row: PgRow) -> Result<NameClaimObservation> {
    let primary_claim_source: Value = sql_row::get(&row, "primary_claim_source")?;
    primary_claim_source
        .as_object()
        .context("primary_claim_source must be a JSON object")?;

    Ok(NameClaimObservation {
        key: decode_tuple_key(&row)?,
        raw_name: sql_row::get(&row, "raw_name")?,
        primary_claim_source,
    })
}

fn decode_tuple_key(row: &PgRow) -> Result<PrimaryNameTupleKey> {
    let address = normalize_evm_address(
        &row.try_get::<String, _>("address")
            .context("missing primary-name address")?,
    );
    let namespace = row
        .try_get::<String, _>("namespace")
        .context("missing primary-name namespace")?;
    let coin_type = row
        .try_get::<String, _>("coin_type")
        .context("missing primary-name coin_type")?;

    if address.trim().is_empty() {
        bail!("primary-name tuple is missing address");
    }
    if namespace.trim().is_empty() {
        bail!("primary-name tuple is missing namespace");
    }
    if coin_type.trim().is_empty() {
        bail!("primary-name tuple is missing coin_type");
    }

    Ok(PrimaryNameTupleKey {
        address,
        namespace,
        coin_type,
    })
}
