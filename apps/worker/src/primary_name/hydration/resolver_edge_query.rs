use anyhow::{Context, Result};
use bigname_storage::{ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID, normalize_evm_address};
use sqlx::{PgPool, Row, postgres::PgRow};

use super::super::types::PrimaryNameTupleKey;
use super::{
    COIN_TYPE_ETH, EVENT_KIND_RESOLVER_CHANGED, EVENT_KIND_REVERSE_CHANGED,
    HYDRATION_PROVENANCE_KEY, ResolverEdgeHydrationCandidate, ResolverEdgeHydrationTarget,
    ReverseNameHydrationChainPosition, TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED,
    normalize_node,
};

pub(super) async fn load_legacy_reverse_resolver_edge_hydration_candidates(
    pool: &PgPool,
    resolver_addresses: &[String],
) -> Result<Vec<ResolverEdgeHydrationCandidate>> {
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
        resolver_events AS (
            SELECT
                LOWER(resolver_event_nodes.reverse_node) AS reverse_node,
                ne.chain_id,
                LOWER(ne.after_state->>'resolver') AS resolver_address,
                ne.block_number,
                ne.log_index,
                ne.normalized_event_id
            FROM normalized_events ne
            CROSS JOIN LATERAL (
                SELECT COALESCE(
                    ne.after_state->'primary_claim_source'->>'reverse_node',
                    CASE
                        WHEN ne.logical_name_id IS NULL
                         AND ne.resource_id IS NULL
                            THEN ne.after_state->>'node'
                    END,
                    CASE
                        WHEN ne.logical_name_id IS NULL
                         AND ne.resource_id IS NULL
                            THEN ne.after_state->>'namehash'
                    END
                ) AS reverse_node
            ) resolver_event_nodes
            JOIN chain_positions
              ON chain_positions.chain_id = ne.chain_id
            WHERE ne.event_kind = $6
              AND ne.chain_id = $3
              AND ne.block_number IS NOT NULL
              AND ne.block_number <= chain_positions.hydration_block_number
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND resolver_event_nodes.reverse_node IS NOT NULL
              AND resolver_event_nodes.reverse_node <> ''
              AND ne.after_state->>'resolver' IS NOT NULL
              AND ne.after_state->>'resolver' <> ''
        ),
        latest_resolvers AS (
            SELECT DISTINCT ON (reverse_node)
                reverse_node,
                chain_id,
                resolver_address
            FROM resolver_events
            ORDER BY
                reverse_node ASC,
                block_number DESC NULLS LAST,
                log_index DESC NULLS LAST,
                normalized_event_id DESC
        ),
        reverse_claim_nodes AS (
            SELECT DISTINCT LOWER(ne.after_state->>'reverse_node') AS reverse_node
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
              AND ne.after_state->>'reverse_node' IS NOT NULL
              AND ne.after_state->>'reverse_node' <> ''
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
        existing_hydration AS (
            SELECT
                pnc.address,
                pnc.namespace,
                pnc.coin_type,
                pnc.claim_provenance -> $7 AS existing_hydration_provenance
            FROM primary_names_current pnc
            WHERE pnc.namespace = $4
              AND pnc.coin_type = $5
              AND pnc.claim_provenance -> $7 ->> 'tuple_source' = $8
        ),
        configured_candidates AS (
            SELECT
                existing_hydration.address AS existing_address,
                existing_hydration.namespace AS existing_namespace,
                existing_hydration.coin_type AS existing_coin_type,
                latest_resolvers.chain_id,
                latest_resolvers.resolver_address,
                latest_resolvers.reverse_node,
                chain_positions.hydration_block_number,
                chain_positions.hydration_block_hash,
                latest_successful_calls.latest_successful_call_block_number,
                latest_successful_calls.latest_successful_call_block_hash,
                latest_successful_calls.latest_successful_call_transaction_hash,
                latest_successful_calls.latest_successful_call_transaction_index,
                existing_hydration.existing_hydration_provenance
            FROM latest_resolvers
            LEFT JOIN reverse_claim_nodes
              ON reverse_claim_nodes.reverse_node = latest_resolvers.reverse_node
            LEFT JOIN latest_successful_calls
              ON latest_successful_calls.chain_id = latest_resolvers.chain_id
             AND latest_successful_calls.resolver_address = latest_resolvers.resolver_address
            LEFT JOIN chain_positions
              ON chain_positions.chain_id = latest_resolvers.chain_id
            LEFT JOIN existing_hydration
              ON existing_hydration.existing_hydration_provenance ->> 'reverse_node'
               = latest_resolvers.reverse_node
            WHERE latest_resolvers.resolver_address = ANY($1::TEXT[])
              AND reverse_claim_nodes.reverse_node IS NULL
              AND (
                  existing_hydration.existing_hydration_provenance IS NULL
                  OR latest_resolvers.resolver_address IS DISTINCT FROM
                     existing_hydration.existing_hydration_provenance ->> 'resolver_address'
                  OR latest_resolvers.reverse_node IS DISTINCT FROM
                     existing_hydration.existing_hydration_provenance ->> 'reverse_node'
                  OR chain_positions.hydration_block_number IS DISTINCT FROM CASE
                      WHEN (existing_hydration.existing_hydration_provenance ->> 'block_number') ~ '^[0-9]+$'
                          THEN (existing_hydration.existing_hydration_provenance ->> 'block_number')::BIGINT
                      ELSE NULL
                  END
                  OR chain_positions.hydration_block_hash IS DISTINCT FROM
                      existing_hydration.existing_hydration_provenance ->> 'block_hash'
                  OR latest_successful_call_block_number IS DISTINCT FROM CASE
                      WHEN (existing_hydration.existing_hydration_provenance ->> 'latest_successful_call_block_number') ~ '^[0-9]+$'
                          THEN (existing_hydration.existing_hydration_provenance ->> 'latest_successful_call_block_number')::BIGINT
                      ELSE NULL
                  END
                  OR latest_successful_call_block_hash IS DISTINCT FROM
                      existing_hydration.existing_hydration_provenance ->> 'latest_successful_call_block_hash'
                  OR latest_successful_call_transaction_hash IS DISTINCT FROM
                      existing_hydration.existing_hydration_provenance ->> 'latest_successful_call_transaction_hash'
                  OR latest_successful_call_transaction_index IS DISTINCT FROM CASE
                      WHEN (existing_hydration.existing_hydration_provenance ->> 'latest_successful_call_transaction_index') ~ '^[0-9]+$'
                          THEN (existing_hydration.existing_hydration_provenance ->> 'latest_successful_call_transaction_index')::BIGINT
                      ELSE NULL
                  END
              )
        ),
        stale_existing AS (
            SELECT
                existing_hydration.address AS existing_address,
                existing_hydration.namespace AS existing_namespace,
                existing_hydration.coin_type AS existing_coin_type,
                NULL::TEXT AS chain_id,
                NULL::TEXT AS resolver_address,
                existing_hydration.existing_hydration_provenance ->> 'reverse_node' AS reverse_node,
                NULL::BIGINT AS hydration_block_number,
                NULL::TEXT AS hydration_block_hash,
                NULL::BIGINT AS latest_successful_call_block_number,
                NULL::TEXT AS latest_successful_call_block_hash,
                NULL::TEXT AS latest_successful_call_transaction_hash,
                NULL::BIGINT AS latest_successful_call_transaction_index,
                existing_hydration.existing_hydration_provenance
            FROM existing_hydration
            LEFT JOIN latest_resolvers
              ON latest_resolvers.reverse_node =
                 existing_hydration.existing_hydration_provenance ->> 'reverse_node'
            WHERE EXISTS (SELECT 1 FROM chain_positions)
              AND (
                  latest_resolvers.reverse_node IS NULL
                  OR latest_resolvers.resolver_address <> ALL($1::TEXT[])
              )
        )
        SELECT *
        FROM configured_candidates
        UNION ALL
        SELECT *
        FROM stale_existing
        ORDER BY reverse_node ASC
        "#,
    )
    .bind(resolver_addresses)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(ENS_NAMESPACE)
    .bind(COIN_TYPE_ETH)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(HYDRATION_PROVENANCE_KEY)
    .bind(TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED)
    .fetch_all(pool)
    .await
    .context(
        "failed to load legacy reverse-resolver resolver-edge primary-name hydration candidates",
    )?;

    rows.into_iter()
        .map(decode_resolver_edge_hydration_candidate)
        .collect()
}

fn decode_resolver_edge_hydration_candidate(row: PgRow) -> Result<ResolverEdgeHydrationCandidate> {
    let existing_address: Option<String> = row
        .try_get("existing_address")
        .context("missing resolver-edge hydration existing_address")?;
    let existing_namespace: Option<String> = row
        .try_get("existing_namespace")
        .context("missing resolver-edge hydration existing_namespace")?;
    let existing_coin_type: Option<String> = row
        .try_get("existing_coin_type")
        .context("missing resolver-edge hydration existing_coin_type")?;
    let existing_key = match (existing_address, existing_namespace, existing_coin_type) {
        (Some(address), Some(namespace), Some(coin_type)) => Some(PrimaryNameTupleKey {
            address: normalize_evm_address(&address),
            namespace,
            coin_type,
        }),
        (None, None, None) => None,
        _ => {
            anyhow::bail!("legacy reverse-resolver resolver-edge hydration existing key is partial")
        }
    };

    let chain_id: Option<String> = row
        .try_get("chain_id")
        .context("missing resolver-edge hydration chain_id")?;
    let resolver_address: Option<String> = row
        .try_get("resolver_address")
        .context("missing resolver-edge hydration resolver_address")?;
    let hydration_target = match (chain_id, resolver_address) {
        (Some(chain_id), Some(resolver_address)) => {
            let reverse_node: Option<String> = row
                .try_get("reverse_node")
                .context("missing resolver-edge hydration reverse_node")?;
            let Some(reverse_node) = reverse_node else {
                anyhow::bail!(
                    "legacy reverse-resolver resolver-edge hydration target is missing reverse_node"
                );
            };
            let hydration_block_number: Option<i64> = row
                .try_get("hydration_block_number")
                .context("missing resolver-edge hydration hydration_block_number")?;
            let hydration_block_hash: Option<String> = row
                .try_get("hydration_block_hash")
                .context("missing resolver-edge hydration hydration_block_hash")?;
            let Some((block_number, block_hash)) = hydration_block_number.zip(hydration_block_hash)
            else {
                anyhow::bail!(
                    "legacy reverse-resolver resolver-edge hydration requires a canonical chain checkpoint for {chain_id}"
                );
            };

            Some(ResolverEdgeHydrationTarget {
                chain_id,
                resolver_address: normalize_evm_address(&resolver_address),
                reverse_node: normalize_node(&reverse_node),
                position: ReverseNameHydrationChainPosition {
                    block_number,
                    block_hash,
                },
                latest_successful_call_block_number: row
                    .try_get("latest_successful_call_block_number")
                    .context(
                        "missing resolver-edge hydration latest_successful_call_block_number",
                    )?,
                latest_successful_call_block_hash: row
                    .try_get("latest_successful_call_block_hash")
                    .context("missing resolver-edge hydration latest_successful_call_block_hash")?,
                latest_successful_call_transaction_hash: row
                    .try_get("latest_successful_call_transaction_hash")
                    .context(
                        "missing resolver-edge hydration latest_successful_call_transaction_hash",
                    )?,
                latest_successful_call_transaction_index: row
                    .try_get("latest_successful_call_transaction_index")
                    .context(
                        "missing resolver-edge hydration latest_successful_call_transaction_index",
                    )?,
            })
        }
        (None, None) => None,
        _ => anyhow::bail!("legacy reverse-resolver resolver-edge hydration target is partial"),
    };

    Ok(ResolverEdgeHydrationCandidate {
        existing_key,
        hydration_target,
    })
}
