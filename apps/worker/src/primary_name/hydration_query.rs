use anyhow::{Context, Result};
use bigname_storage::{ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID, normalize_evm_address};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgRow};

use super::super::types::{NameClaimObservation, PrimaryNameTupleKey, ReverseClaimTuple};
use super::{
    COIN_TYPE_ETH, EVENT_KIND_RESOLVER_CHANGED, EVENT_KIND_REVERSE_CHANGED,
    HYDRATION_PROVENANCE_KEY, HydrationCandidate, ReverseNameHydrationChainPosition,
    ReverseNameHydrationTarget, normalize_node,
};

pub(super) async fn load_legacy_reverse_hydration_candidates(
    pool: &PgPool,
    resolver_addresses: &[String],
) -> Result<Vec<HydrationCandidate>> {
    let rows = sqlx::query(
        r#"
        WITH chain_positions AS (
            SELECT
                chain_id,
                canonical_block_number AS hydration_block_number,
                canonical_block_hash AS hydration_block_hash
            FROM chain_checkpoints
            WHERE chain_id = $3
              AND canonical_block_number IS NOT NULL
              AND canonical_block_hash IS NOT NULL
        ),
        reverse_claims AS (
            SELECT DISTINCT ON (
                LOWER(ne.after_state->>'address'),
                COALESCE(ne.after_state->>'namespace', ne.namespace),
                ne.after_state->>'coin_type'
            )
                LOWER(ne.after_state->>'address') AS address,
                COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
                ne.after_state->>'coin_type' AS coin_type,
                LOWER(ne.after_state->>'reverse_node') AS reverse_node,
                COALESCE(ne.after_state->'claim_provenance', '{}'::jsonb) AS claim_provenance
            FROM normalized_events ne
            JOIN chain_positions
              ON chain_positions.chain_id = ne.chain_id
            WHERE ne.event_kind = $2
              AND ne.chain_id = $3
              AND ne.block_number IS NOT NULL
              AND ne.block_number <= chain_positions.hydration_block_number
              AND COALESCE(ne.after_state->>'namespace', ne.namespace) = $4
              AND ne.after_state->>'coin_type' = $5
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.after_state->>'address' IS NOT NULL
              AND ne.after_state->>'address' <> ''
              AND ne.after_state->>'reverse_node' IS NOT NULL
              AND ne.after_state->>'reverse_node' <> ''
            ORDER BY
                LOWER(ne.after_state->>'address') ASC,
                COALESCE(ne.after_state->>'namespace', ne.namespace) ASC,
                ne.after_state->>'coin_type' ASC,
                ne.block_number DESC NULLS LAST,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
        ),
        latest_resolvers AS (
            SELECT DISTINCT ON (LOWER(ne.after_state->'primary_claim_source'->>'reverse_node'))
                LOWER(ne.after_state->'primary_claim_source'->>'reverse_node') AS reverse_node,
                ne.chain_id,
                LOWER(ne.after_state->>'resolver') AS resolver_address,
                ne.after_state->'primary_claim_source' AS primary_claim_source
            FROM normalized_events ne
            JOIN chain_positions
              ON chain_positions.chain_id = ne.chain_id
            WHERE ne.event_kind = $6
              AND ne.chain_id = $3
              AND ne.block_number IS NOT NULL
              AND ne.block_number <= chain_positions.hydration_block_number
              AND ne.logical_name_id IS NULL
              AND ne.resource_id IS NULL
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.after_state ? 'primary_claim_source'
              AND ne.after_state->'primary_claim_source'->>'reverse_node' IS NOT NULL
              AND ne.after_state->'primary_claim_source'->>'reverse_node' <> ''
              AND ne.after_state->>'resolver' IS NOT NULL
              AND ne.after_state->>'resolver' <> ''
            ORDER BY
                LOWER(ne.after_state->'primary_claim_source'->>'reverse_node') ASC,
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
            JOIN chain_positions
              ON chain_positions.chain_id = ne.chain_id
            WHERE ne.event_kind = 'RecordChanged'
              AND ne.chain_id = $3
              AND ne.block_number IS NOT NULL
              AND ne.block_number <= chain_positions.hydration_block_number
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
        ),
        latest_successful_calls AS (
            SELECT DISTINCT ON (
                esc.chain_id,
                LOWER(esc.resolver_address)
            )
                esc.chain_id,
                LOWER(esc.resolver_address) AS resolver_address,
                esc.block_number AS latest_successful_call_block_number,
                esc.block_hash AS latest_successful_call_block_hash,
                esc.transaction_hash AS latest_successful_call_transaction_hash,
                esc.transaction_index AS latest_successful_call_transaction_index
            FROM event_silent_resolver_call_observations esc
            JOIN chain_positions
              ON chain_positions.chain_id = esc.chain_id
             AND esc.block_number <= chain_positions.hydration_block_number
            WHERE esc.chain_id = $3
              AND LOWER(esc.resolver_address) = ANY($1::TEXT[])
              AND esc.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY
                esc.chain_id ASC,
                LOWER(esc.resolver_address) ASC,
                esc.block_number DESC,
                esc.transaction_index DESC,
                esc.transaction_hash DESC
        ),
        candidate_state AS (
            SELECT
            reverse_claims.address,
            reverse_claims.namespace,
            reverse_claims.coin_type,
            reverse_claims.reverse_node,
            reverse_claims.claim_provenance,
            name_claims.raw_name AS base_raw_name,
            name_claims.primary_claim_source AS base_primary_claim_source,
            latest_resolvers.chain_id,
            latest_resolvers.resolver_address,
            latest_resolvers.primary_claim_source,
            chain_positions.hydration_block_number,
            chain_positions.hydration_block_hash,
            latest_successful_calls.latest_successful_call_block_number,
            latest_successful_calls.latest_successful_call_block_hash,
            latest_successful_calls.latest_successful_call_transaction_hash,
            latest_successful_calls.latest_successful_call_transaction_index,
            pnc.claim_provenance -> $7 AS existing_hydration_provenance,
            COALESCE((
                latest_resolvers.resolver_address = ANY($1::TEXT[])
                AND LOWER(latest_resolvers.primary_claim_source->>'address') = reverse_claims.address
                AND COALESCE(latest_resolvers.primary_claim_source->>'namespace', $4) = reverse_claims.namespace
                AND latest_resolvers.primary_claim_source->>'coin_type' = reverse_claims.coin_type
            ), FALSE) AS should_hydrate
            FROM reverse_claims
            LEFT JOIN name_claims
              ON name_claims.address = reverse_claims.address
             AND name_claims.namespace = reverse_claims.namespace
             AND name_claims.coin_type = reverse_claims.coin_type
            LEFT JOIN latest_resolvers
              ON latest_resolvers.reverse_node = reverse_claims.reverse_node
            LEFT JOIN latest_successful_calls
              ON latest_successful_calls.chain_id = latest_resolvers.chain_id
             AND latest_successful_calls.resolver_address = latest_resolvers.resolver_address
            LEFT JOIN chain_positions
              ON chain_positions.chain_id = latest_resolvers.chain_id
            LEFT JOIN primary_names_current pnc
              ON pnc.address = reverse_claims.address
             AND pnc.namespace = reverse_claims.namespace
             AND pnc.coin_type = reverse_claims.coin_type
        )
        SELECT *
        FROM candidate_state
        WHERE (
              existing_hydration_provenance IS NOT NULL
              OR should_hydrate
          )
          AND (
              existing_hydration_provenance IS NULL
              OR NOT should_hydrate
              OR resolver_address IS DISTINCT FROM existing_hydration_provenance ->> 'resolver_address'
              OR reverse_node IS DISTINCT FROM existing_hydration_provenance ->> 'reverse_node'
              OR latest_successful_call_block_number IS DISTINCT FROM CASE
                  WHEN (existing_hydration_provenance ->> 'latest_successful_call_block_number') ~ '^[0-9]+$'
                      THEN (existing_hydration_provenance ->> 'latest_successful_call_block_number')::BIGINT
                  ELSE NULL
              END
              OR latest_successful_call_block_hash IS DISTINCT FROM
                  existing_hydration_provenance ->> 'latest_successful_call_block_hash'
              OR latest_successful_call_transaction_hash IS DISTINCT FROM
                  existing_hydration_provenance ->> 'latest_successful_call_transaction_hash'
              OR latest_successful_call_transaction_index IS DISTINCT FROM CASE
                  WHEN (existing_hydration_provenance ->> 'latest_successful_call_transaction_index') ~ '^[0-9]+$'
                      THEN (existing_hydration_provenance ->> 'latest_successful_call_transaction_index')::BIGINT
                  ELSE NULL
              END
          )
        ORDER BY address ASC, namespace ASC, coin_type ASC
        "#,
    )
    .bind(resolver_addresses)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(ENS_NAMESPACE)
    .bind(COIN_TYPE_ETH)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(HYDRATION_PROVENANCE_KEY)
    .fetch_all(pool)
    .await
    .context("failed to load legacy reverse-resolver primary-name hydration candidates")?;

    rows.into_iter().map(decode_hydration_candidate).collect()
}

fn decode_hydration_candidate(row: PgRow) -> Result<HydrationCandidate> {
    let address = normalize_evm_address(
        &row.try_get::<String, _>("address")
            .context("missing legacy reverse hydration address")?,
    );
    let namespace: String = row
        .try_get("namespace")
        .context("missing legacy reverse hydration namespace")?;
    let coin_type: String = row
        .try_get("coin_type")
        .context("missing legacy reverse hydration coin_type")?;
    let reverse_node: String = row
        .try_get("reverse_node")
        .context("missing legacy reverse hydration reverse_node")?;
    let claim_provenance: Value = row
        .try_get("claim_provenance")
        .context("missing legacy reverse hydration claim_provenance")?;
    let base_primary_claim_source: Option<Value> = row
        .try_get("base_primary_claim_source")
        .context("missing legacy reverse hydration base_primary_claim_source")?;
    let base_claim_observation = base_primary_claim_source
        .map(|primary_claim_source| {
            primary_claim_source.as_object().context(
                "legacy reverse hydration base primary_claim_source must be a JSON object",
            )?;

            Ok::<NameClaimObservation, anyhow::Error>(NameClaimObservation {
                key: PrimaryNameTupleKey {
                    address: address.clone(),
                    namespace: namespace.clone(),
                    coin_type: coin_type.clone(),
                },
                raw_name: row
                    .try_get("base_raw_name")
                    .context("missing legacy reverse hydration base_raw_name")?,
                primary_claim_source,
            })
        })
        .transpose()?;
    let existing_hydration_provenance: Option<Value> = row
        .try_get("existing_hydration_provenance")
        .context("missing existing_hydration_provenance")?;
    let should_hydrate: bool = row
        .try_get("should_hydrate")
        .context("missing legacy reverse hydration should_hydrate")?;
    let hydration_target = if should_hydrate {
        let primary_claim_source: Value = row
            .try_get("primary_claim_source")
            .context("missing legacy reverse hydration primary_claim_source")?;
        primary_claim_source
            .as_object()
            .context("legacy reverse hydration primary_claim_source must be a JSON object")?;
        let resolver_address: String = row
            .try_get("resolver_address")
            .context("missing legacy reverse hydration resolver_address")?;
        let hydration_block_number: Option<i64> = row
            .try_get("hydration_block_number")
            .context("missing legacy reverse hydration hydration_block_number")?;
        let hydration_block_hash: Option<String> = row
            .try_get("hydration_block_hash")
            .context("missing legacy reverse hydration hydration_block_hash")?;
        let Some((block_number, block_hash)) = hydration_block_number.zip(hydration_block_hash)
        else {
            anyhow::bail!(
                "legacy reverse-resolver primary-name hydration requires a canonical chain checkpoint for {}",
                row.try_get::<String, _>("chain_id")
                    .unwrap_or_else(|_| "<unknown>".to_owned())
            );
        };

        Some(ReverseNameHydrationTarget {
            primary_claim_source,
            chain_id: row
                .try_get("chain_id")
                .context("missing legacy reverse hydration chain_id")?,
            resolver_address: normalize_evm_address(&resolver_address),
            reverse_node: normalize_node(&reverse_node),
            position: ReverseNameHydrationChainPosition {
                block_number,
                block_hash,
            },
            latest_successful_call_block_number: row
                .try_get("latest_successful_call_block_number")
                .context("missing latest_successful_call_block_number")?,
            latest_successful_call_block_hash: row
                .try_get("latest_successful_call_block_hash")
                .context("missing latest_successful_call_block_hash")?,
            latest_successful_call_transaction_hash: row
                .try_get("latest_successful_call_transaction_hash")
                .context("missing latest_successful_call_transaction_hash")?,
            latest_successful_call_transaction_index: row
                .try_get("latest_successful_call_transaction_index")
                .context("missing latest_successful_call_transaction_index")?,
        })
    } else {
        None
    };

    Ok(HydrationCandidate {
        tuple: ReverseClaimTuple {
            key: PrimaryNameTupleKey {
                address,
                namespace,
                coin_type,
            },
            claim_provenance,
        },
        base_claim_observation,
        has_existing_hydration: existing_hydration_provenance.is_some(),
        hydration_target,
    })
}
