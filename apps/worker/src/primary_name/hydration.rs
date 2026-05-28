use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_execution::{
    ChainRpcUrls, EnsForwardAddressLookupRequest, EnsReverseNameMulticallBlock,
    EnsReverseNameMulticallRequest, EnsReverseNameMulticallResult, MULTICALL3_ADDRESS,
    execute_ens_reverse_name_multicall, lookup_ens_forward_address_at_block,
};
use bigname_storage::{
    ENS_LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES, ETHEREUM_MAINNET_CHAIN_ID,
    normalize_evm_address,
};
use futures_util::{FutureExt, future::BoxFuture};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use super::{
    PrimaryNameLegacyReverseHydrationSummary,
    projection::{primary_name_row, primary_name_row_with_provenance_extensions},
    types::{NameClaimObservation, PrimaryNameTupleKey, ReverseClaimTuple},
};

#[path = "hydration_query.rs"]
mod hydration_query;
#[path = "hydration/resolver_edge.rs"]
mod resolver_edge;
#[path = "hydration/resolver_edge_query.rs"]
mod resolver_edge_query;
use hydration_query::load_legacy_reverse_hydration_candidates;
use resolver_edge::hydrate_resolver_edge_candidates;
use resolver_edge_query::load_legacy_reverse_resolver_edge_hydration_candidates;

#[cfg(test)]
const LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS: &str =
    ENS_LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES[0];

const COIN_TYPE_ETH: &str = "60";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const HYDRATION_PROVENANCE_KEY: &str = "legacy_reverse_resolver_hydration";
const DERIVATION_KIND_LEGACY_REVERSE_RESOLVER_HYDRATION: &str =
    "ens_v1_legacy_reverse_resolver_hydration";
const SOURCE_FAMILY_ENS_V1_REVERSE_L1: &str = "ens_v1_reverse_l1";
const TUPLE_SOURCE_REVERSE_CLAIM: &str = "reverse_claim";
const TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED: &str = "resolver_edge_forward_confirmed";
const DEFAULT_LEGACY_REVERSE_HYDRATION_BATCH_SIZE: usize = 250;
const LEGACY_REVERSE_HYDRATION_UPSERT_BATCH_SIZE: usize = 1_000;

#[derive(Clone, Debug)]
pub struct PrimaryNameLegacyReverseHydrationConfig {
    pub chain_rpc_urls: ChainRpcUrls,
    pub multicall3_address: String,
    pub batch_size: usize,
    pub resolver_addresses: Vec<String>,
}

impl PrimaryNameLegacyReverseHydrationConfig {
    pub fn new(chain_rpc_urls: ChainRpcUrls) -> Self {
        Self {
            chain_rpc_urls,
            multicall3_address: MULTICALL3_ADDRESS.to_owned(),
            batch_size: DEFAULT_LEGACY_REVERSE_HYDRATION_BATCH_SIZE,
            resolver_addresses: default_legacy_event_silent_reverse_resolver_addresses(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryNameLegacyReverseHydrationTrigger {
    pub resolver_address: String,
    pub block_number: i64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HydrationCandidate {
    tuple: ReverseClaimTuple,
    base_claim_observation: Option<NameClaimObservation>,
    has_existing_hydration: bool,
    hydration_target: Option<ReverseNameHydrationTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseNameHydrationTarget {
    primary_claim_source: Value,
    chain_id: String,
    resolver_address: String,
    reverse_node: String,
    position: ReverseNameHydrationChainPosition,
    latest_successful_call_block_number: Option<i64>,
    latest_successful_call_block_hash: Option<String>,
    latest_successful_call_transaction_hash: Option<String>,
    latest_successful_call_transaction_index: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolverEdgeHydrationCandidate {
    existing_key: Option<PrimaryNameTupleKey>,
    hydration_target: Option<ResolverEdgeHydrationTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolverEdgeHydrationTarget {
    chain_id: String,
    resolver_address: String,
    reverse_node: String,
    position: ReverseNameHydrationChainPosition,
    latest_successful_call_block_number: Option<i64>,
    latest_successful_call_block_hash: Option<String>,
    latest_successful_call_transaction_hash: Option<String>,
    latest_successful_call_transaction_index: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseNameHydrationCall {
    resolver_address: String,
    reverse_node: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseNameHydrationChainPosition {
    block_number: i64,
    block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReverseNameHydrationOutcome {
    Success(String),
    NotFound,
    Failed(String),
}

trait ReverseNameHydrationClient: Sync {
    fn hydrate<'a>(
        &'a self,
        chain_id: &'a str,
        position: &'a ReverseNameHydrationChainPosition,
        calls: &'a [ReverseNameHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<ReverseNameHydrationOutcome>>>;

    fn lookup_forward_address<'a>(
        &'a self,
        _chain_id: &'a str,
        _position: &'a ReverseNameHydrationChainPosition,
        _normalized_name: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        async { Ok(None) }.boxed()
    }
}

struct MulticallReverseNameHydrationClient {
    config: PrimaryNameLegacyReverseHydrationConfig,
}

impl ReverseNameHydrationClient for MulticallReverseNameHydrationClient {
    fn hydrate<'a>(
        &'a self,
        chain_id: &'a str,
        position: &'a ReverseNameHydrationChainPosition,
        calls: &'a [ReverseNameHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<ReverseNameHydrationOutcome>>> {
        async move {
            let batch_size = self.config.batch_size.max(1);
            let mut outcomes = Vec::with_capacity(calls.len());
            let block = EnsReverseNameMulticallBlock {
                block_number: position.block_number,
                block_hash: position.block_hash.clone(),
            };
            for chunk in calls.chunks(batch_size) {
                let requests = chunk
                    .iter()
                    .map(|call| EnsReverseNameMulticallRequest {
                        resolver_address: call.resolver_address.clone(),
                        reverse_node: call.reverse_node.clone(),
                    })
                    .collect::<Vec<_>>();
                let chunk_outcomes = execute_ens_reverse_name_multicall(
                    &self.config.chain_rpc_urls,
                    chain_id,
                    &self.config.multicall3_address,
                    &block,
                    &requests,
                )
                .await?;
                outcomes.extend(chunk_outcomes.into_iter().map(|outcome| match outcome {
                    EnsReverseNameMulticallResult::Success { value } => {
                        ReverseNameHydrationOutcome::Success(value)
                    }
                    EnsReverseNameMulticallResult::NotFound => {
                        ReverseNameHydrationOutcome::NotFound
                    }
                    EnsReverseNameMulticallResult::Failed { message } => {
                        ReverseNameHydrationOutcome::Failed(message)
                    }
                }));
            }
            Ok(outcomes)
        }
        .boxed()
    }

    fn lookup_forward_address<'a>(
        &'a self,
        chain_id: &'a str,
        position: &'a ReverseNameHydrationChainPosition,
        normalized_name: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        async move {
            if chain_id != bigname_storage::ETHEREUM_MAINNET_CHAIN_ID {
                anyhow::bail!(
                    "legacy reverse-resolver forward confirmation only supports {}",
                    bigname_storage::ETHEREUM_MAINNET_CHAIN_ID
                );
            }
            lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
                normalized_name,
                chain_rpc_urls: &self.config.chain_rpc_urls,
                block_number: position.block_number,
                block_hash: &position.block_hash,
                follow_ccip_read: false,
            })
            .await
            .map_err(anyhow::Error::from)
        }
        .boxed()
    }
}

pub(super) async fn hydrate_legacy_reverse_resolver_primary_names(
    pool: &PgPool,
    config: PrimaryNameLegacyReverseHydrationConfig,
) -> Result<PrimaryNameLegacyReverseHydrationSummary> {
    let resolver_addresses = normalize_resolver_addresses(&config.resolver_addresses);
    let client = MulticallReverseNameHydrationClient { config };
    hydrate_legacy_reverse_resolver_primary_names_with_client(pool, &resolver_addresses, &client)
        .await
}

pub(super) async fn load_legacy_reverse_resolver_call_triggers(
    pool: &PgPool,
    config: &PrimaryNameLegacyReverseHydrationConfig,
) -> Result<Vec<PrimaryNameLegacyReverseHydrationTrigger>> {
    let resolver_addresses = normalize_resolver_addresses(&config.resolver_addresses);
    if resolver_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        WITH chain_positions AS (
            SELECT
                chain_id,
                canonical_block_number AS hydration_block_number,
                canonical_block_hash AS hydration_block_hash
            FROM chain_checkpoints
            WHERE chain_id = $2
              AND canonical_block_number IS NOT NULL
              AND canonical_block_hash IS NOT NULL
        )
        SELECT DISTINCT ON (LOWER(esc.resolver_address))
            LOWER(esc.resolver_address) AS resolver_address,
            esc.block_number,
            esc.block_hash,
            esc.transaction_hash,
            esc.transaction_index
        FROM event_silent_resolver_call_observations esc
        JOIN chain_positions
          ON chain_positions.chain_id = esc.chain_id
         AND esc.block_number <= chain_positions.hydration_block_number
        WHERE esc.chain_id = $2
          AND LOWER(esc.resolver_address) = ANY($1::TEXT[])
          AND esc.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY
            LOWER(esc.resolver_address) ASC,
            esc.block_number DESC,
            esc.transaction_index DESC,
            esc.transaction_hash DESC
        "#,
    )
    .bind(&resolver_addresses)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .fetch_all(pool)
    .await
    .context("failed to load latest legacy reverse-resolver direct-call triggers")?;

    rows.into_iter()
        .map(|row| {
            Ok(PrimaryNameLegacyReverseHydrationTrigger {
                resolver_address: row
                    .try_get("resolver_address")
                    .context("missing legacy reverse hydration trigger resolver_address")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing legacy reverse hydration trigger block_number")?,
                block_hash: row
                    .try_get("block_hash")
                    .context("missing legacy reverse hydration trigger block_hash")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("missing legacy reverse hydration trigger transaction_hash")?,
                transaction_index: row
                    .try_get("transaction_index")
                    .context("missing legacy reverse hydration trigger transaction_index")?,
            })
        })
        .collect()
}

async fn hydrate_legacy_reverse_resolver_primary_names_with_client(
    pool: &PgPool,
    resolver_addresses: &[String],
    client: &dyn ReverseNameHydrationClient,
) -> Result<PrimaryNameLegacyReverseHydrationSummary> {
    if resolver_addresses.is_empty() {
        return Ok(PrimaryNameLegacyReverseHydrationSummary::default());
    }

    let candidates = load_legacy_reverse_hydration_candidates(pool, resolver_addresses).await?;
    let resolver_edge_candidates =
        load_legacy_reverse_resolver_edge_hydration_candidates(pool, resolver_addresses).await?;
    let mut summary = PrimaryNameLegacyReverseHydrationSummary {
        candidate_tuple_count: candidates.len() + resolver_edge_candidates.len(),
        ..PrimaryNameLegacyReverseHydrationSummary::default()
    };
    if candidates.is_empty() && resolver_edge_candidates.is_empty() {
        return Ok(summary);
    }

    let mut snapshots = Vec::new();
    for candidate in candidates
        .iter()
        .filter(|candidate| candidate.hydration_target.is_none())
    {
        let snapshot = baseline_snapshot(candidate)?;
        add_snapshot_status(&mut summary, &snapshot);
        snapshots.push(snapshot);
    }

    let calls_by_position = candidates.iter().enumerate().fold(
        BTreeMap::<(String, i64, String), Vec<(usize, ReverseNameHydrationCall)>>::new(),
        |mut by_position, (index, candidate)| {
            let Some(target) = candidate.hydration_target.as_ref() else {
                return by_position;
            };
            by_position
                .entry((
                    target.chain_id.clone(),
                    target.position.block_number,
                    target.position.block_hash.clone(),
                ))
                .or_default()
                .push((
                    index,
                    ReverseNameHydrationCall {
                        resolver_address: target.resolver_address.clone(),
                        reverse_node: target.reverse_node.clone(),
                    },
                ));
            by_position
        },
    );

    for ((chain_id, block_number, block_hash), calls_with_refs) in calls_by_position {
        let position = ReverseNameHydrationChainPosition {
            block_number,
            block_hash,
        };
        let calls = calls_with_refs
            .iter()
            .map(|(_, call)| call.clone())
            .collect::<Vec<_>>();
        summary.queried_tuple_count += calls.len();
        let outcomes = match client.hydrate(&chain_id, &position, &calls).await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                summary.failed_lookup_count += calls.len();
                tracing::warn!(
                    service = "worker",
                    projection = "primary_names_current",
                    chain_id,
                    error = %format!("{error:#}"),
                    failed_lookup_count = calls.len(),
                    "legacy reverse-resolver primary-name hydration batch failed"
                );
                for (candidate_index, _) in &calls_with_refs {
                    let candidate = candidates.get(*candidate_index).context(
                        "legacy reverse-resolver hydration candidate reference is out of bounds",
                    )?;
                    if candidate.has_existing_hydration {
                        let snapshot = baseline_snapshot(candidate)?;
                        add_snapshot_status(&mut summary, &snapshot);
                        snapshots.push(snapshot);
                    }
                }
                continue;
            }
        };
        if outcomes.len() != calls_with_refs.len() {
            anyhow::bail!(
                "legacy reverse-resolver hydration provider returned {} outcomes for {} calls on {chain_id}",
                outcomes.len(),
                calls_with_refs.len()
            );
        }

        for ((candidate_index, _), outcome) in calls_with_refs.iter().zip(outcomes) {
            let candidate = candidates.get(*candidate_index).context(
                "legacy reverse-resolver hydration candidate reference is out of bounds",
            )?;
            let target = candidate.hydration_target.as_ref().context(
                "legacy reverse-resolver hydration candidate is missing hydration target",
            )?;
            let raw_name = match outcome {
                ReverseNameHydrationOutcome::Success(value) => value,
                ReverseNameHydrationOutcome::NotFound => String::new(),
                ReverseNameHydrationOutcome::Failed(_) => {
                    summary.failed_lookup_count += 1;
                    if candidate.has_existing_hydration {
                        let snapshot = baseline_snapshot(candidate)?;
                        add_snapshot_status(&mut summary, &snapshot);
                        snapshots.push(snapshot);
                    }
                    continue;
                }
            };
            let claim_observation = NameClaimObservation {
                key: candidate.tuple.key.clone(),
                raw_name: Some(raw_name),
                primary_claim_source: target.primary_claim_source.clone(),
            };
            let hydration_provenance = hydration_provenance(target, &position);
            let snapshot = primary_name_row_with_provenance_extensions(
                &candidate.tuple,
                Some(&claim_observation),
                [(HYDRATION_PROVENANCE_KEY, hydration_provenance)],
            )?;
            add_snapshot_status(&mut summary, &snapshot);
            snapshots.push(snapshot);
        }
    }

    hydrate_resolver_edge_candidates(
        pool,
        &resolver_edge_candidates,
        client,
        &mut summary,
        &mut snapshots,
    )
    .await?;

    summary.upserted_row_count = upsert_hydration_snapshots_in_batches(
        pool,
        &snapshots,
        LEGACY_REVERSE_HYDRATION_UPSERT_BATCH_SIZE,
    )
    .await?;
    Ok(summary)
}

async fn upsert_hydration_snapshots_in_batches(
    pool: &PgPool,
    snapshots: &[bigname_storage::PrimaryNameCurrentSnapshot],
    batch_size: usize,
) -> Result<usize> {
    if snapshots.is_empty() {
        return Ok(0);
    }
    if batch_size == 0 {
        anyhow::bail!("legacy reverse hydration upsert batch size must be positive");
    }

    let mut upserted_row_count = 0usize;
    for (batch_index, chunk) in snapshots.chunks(batch_size).enumerate() {
        upserted_row_count += bigname_storage::upsert_primary_name_current_snapshots(pool, chunk)
            .await
            .with_context(|| {
                format!("failed to upsert legacy reverse hydration snapshot batch {batch_index}")
            })?
            .len();
    }
    Ok(upserted_row_count)
}

fn baseline_snapshot(
    candidate: &HydrationCandidate,
) -> Result<bigname_storage::PrimaryNameCurrentSnapshot> {
    primary_name_row(&candidate.tuple, candidate.base_claim_observation.as_ref())
}

fn add_snapshot_status(
    summary: &mut PrimaryNameLegacyReverseHydrationSummary,
    snapshot: &bigname_storage::PrimaryNameCurrentSnapshot,
) {
    match snapshot.row.claim_status {
        bigname_storage::PrimaryNameClaimStatus::Success => {
            summary.success_row_count += 1;
        }
        bigname_storage::PrimaryNameClaimStatus::NotFound => {
            summary.not_found_row_count += 1;
        }
        bigname_storage::PrimaryNameClaimStatus::InvalidName => {
            summary.invalid_name_row_count += 1;
        }
        bigname_storage::PrimaryNameClaimStatus::Unsupported => {}
    }
}

fn hydration_provenance(
    target: &ReverseNameHydrationTarget,
    position: &ReverseNameHydrationChainPosition,
) -> Value {
    json!({
        "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
        "derivation_kind": DERIVATION_KIND_LEGACY_REVERSE_RESOLVER_HYDRATION,
        "tuple_source": TUPLE_SOURCE_REVERSE_CLAIM,
        "chain_id": target.chain_id,
        "resolver_address": target.resolver_address,
        "reverse_node": target.reverse_node,
        "block_number": position.block_number,
        "block_hash": position.block_hash,
        "latest_successful_call_block_number": target.latest_successful_call_block_number,
        "latest_successful_call_block_hash": target.latest_successful_call_block_hash,
        "latest_successful_call_transaction_hash": target.latest_successful_call_transaction_hash,
        "latest_successful_call_transaction_index": target.latest_successful_call_transaction_index,
    })
}

fn normalize_resolver_addresses(addresses: &[String]) -> Vec<String> {
    addresses
        .iter()
        .map(|address| normalize_evm_address(address))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn default_legacy_event_silent_reverse_resolver_addresses() -> Vec<String> {
    ENS_LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES
        .iter()
        .map(|address| normalize_evm_address(address))
        .collect()
}

fn normalize_node(node: &str) -> String {
    node.trim().to_ascii_lowercase()
}

#[cfg(test)]
#[path = "hydration/tests.rs"]
mod hydration_tests;
