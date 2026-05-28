use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_execution::{
    ChainRpcUrls, EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest,
    EnsTextRecordMulticallResult, MULTICALL3_ADDRESS, ens_namehash_hex,
    execute_ens_text_record_multicall,
};
use bigname_storage::normalize_evm_address;
use futures_util::{FutureExt, future::BoxFuture};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::{constants::*, types::RecordInventoryTextHydrationSummary};

const DEFAULT_TEXT_HYDRATION_BATCH_SIZE: usize = 250;
const DEFAULT_TEXT_HYDRATION_ROW_BATCH_SIZE: i64 = 500;

#[derive(Clone, Debug)]
pub struct RecordInventoryTextHydrationConfig {
    pub chain_rpc_urls: ChainRpcUrls,
    pub multicall3_address: String,
    pub batch_size: usize,
}

impl RecordInventoryTextHydrationConfig {
    pub fn new(chain_rpc_urls: ChainRpcUrls) -> Self {
        Self {
            chain_rpc_urls,
            multicall3_address: MULTICALL3_ADDRESS.to_owned(),
            batch_size: DEFAULT_TEXT_HYDRATION_BATCH_SIZE,
        }
    }
}

#[derive(Clone, Debug)]
struct HydrationRow {
    resource_id: Uuid,
    record_version_boundary_key: String,
    logical_name_id: String,
    namehash: Option<String>,
    chain_id: String,
    resolver_address: String,
    entries: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TextHydrationCall {
    resolver_address: String,
    name: String,
    namehash: String,
    text_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TextHydrationChainPosition {
    block_number: i64,
    block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TextHydrationOutcome {
    Success(String),
    NotFound,
    Failed(String),
}

trait TextHydrationClient: Sync {
    fn hydrate<'a>(
        &'a self,
        chain_id: &'a str,
        position: &'a TextHydrationChainPosition,
        calls: &'a [TextHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<TextHydrationOutcome>>>;
}

struct MulticallTextHydrationClient {
    config: RecordInventoryTextHydrationConfig,
}

impl TextHydrationClient for MulticallTextHydrationClient {
    fn hydrate<'a>(
        &'a self,
        chain_id: &'a str,
        position: &'a TextHydrationChainPosition,
        calls: &'a [TextHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<TextHydrationOutcome>>> {
        async move {
            let batch_size = self.config.batch_size.max(1);
            let mut outcomes = Vec::with_capacity(calls.len());
            let block = EnsTextRecordMulticallBlock {
                block_number: position.block_number,
                block_hash: position.block_hash.clone(),
            };
            for chunk in calls.chunks(batch_size) {
                let requests = chunk
                    .iter()
                    .map(|call| EnsTextRecordMulticallRequest {
                        resolver_address: call.resolver_address.clone(),
                        namehash: call.namehash.clone(),
                        text_key: call.text_key.clone(),
                    })
                    .collect::<Vec<_>>();
                let chunk_outcomes = execute_ens_text_record_multicall(
                    &self.config.chain_rpc_urls,
                    chain_id,
                    &self.config.multicall3_address,
                    &block,
                    &requests,
                )
                .await?;
                outcomes.extend(chunk_outcomes.into_iter().map(|outcome| match outcome {
                    EnsTextRecordMulticallResult::Success { value } => {
                        TextHydrationOutcome::Success(value)
                    }
                    EnsTextRecordMulticallResult::NotFound => TextHydrationOutcome::NotFound,
                    EnsTextRecordMulticallResult::Failed { message } => {
                        TextHydrationOutcome::Failed(message)
                    }
                }));
            }
            Ok(outcomes)
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
struct CallRef {
    row_index: usize,
    entry_index: usize,
}

#[derive(Clone, Debug)]
struct HydrationPageCursor {
    resource_id: Uuid,
    record_version_boundary_key: String,
}

pub(super) async fn hydrate_record_inventory_text_values(
    pool: &PgPool,
    resource_id: Option<&str>,
    config: RecordInventoryTextHydrationConfig,
) -> Result<RecordInventoryTextHydrationSummary> {
    let client = MulticallTextHydrationClient { config };
    hydrate_record_inventory_text_values_with_client(pool, resource_id, &client).await
}

async fn hydrate_record_inventory_text_values_with_client(
    pool: &PgPool,
    resource_id: Option<&str>,
    client: &dyn TextHydrationClient,
) -> Result<RecordInventoryTextHydrationSummary> {
    let resource_id = resource_id
        .map(Uuid::parse_str)
        .transpose()
        .context("record_inventory_current text hydration resource_id must be a UUID")?;
    let mut summary = RecordInventoryTextHydrationSummary::default();
    let mut after = None;

    loop {
        let mut rows = load_text_hydration_rows(
            pool,
            resource_id,
            after.as_ref(),
            DEFAULT_TEXT_HYDRATION_ROW_BATCH_SIZE,
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|row| HydrationPageCursor {
            resource_id: row.resource_id,
            record_version_boundary_key: row.record_version_boundary_key.clone(),
        });
        summary.candidate_row_count += rows.len();

        let supported_resolvers = load_supported_ensv1_text_resolvers(pool, &rows).await?;
        let mut calls_by_chain = BTreeMap::<String, Vec<(CallRef, TextHydrationCall)>>::new();
        for (row_index, row) in rows.iter().enumerate() {
            let resolver_key = (
                row.chain_id.clone(),
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                normalize_address(&row.resolver_address),
            );
            if !supported_resolvers.contains(&resolver_key) {
                summary.skipped_entry_count += candidate_text_entry_count(&row.entries)?;
                continue;
            }
            let Some(name) = ens_name_from_logical_name_id(&row.logical_name_id) else {
                summary.skipped_entry_count += candidate_text_entry_count(&row.entries)?;
                continue;
            };
            let namehash = match row
                .namehash
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                Some(namehash) => namehash.to_owned(),
                None => match ens_namehash_hex(name) {
                    Ok(namehash) => namehash,
                    Err(_) => {
                        summary.failed_entry_count += candidate_text_entry_count(&row.entries)?;
                        continue;
                    }
                },
            };

            let entries = row
                .entries
                .as_array()
                .context("record_inventory_current.entries must be an array")?;
            for (entry_index, entry) in entries.iter().enumerate() {
                let Some(text_key) = hydration_text_key(entry) else {
                    continue;
                };
                summary.candidate_entry_count += 1;
                calls_by_chain
                    .entry(row.chain_id.clone())
                    .or_default()
                    .push((
                        CallRef {
                            row_index,
                            entry_index,
                        },
                        TextHydrationCall {
                            resolver_address: row.resolver_address.clone(),
                            name: name.to_owned(),
                            namehash: namehash.clone(),
                            text_key: text_key.to_owned(),
                        },
                    ));
            }
        }

        let mut changed_rows = BTreeSet::<usize>::new();
        let chain_ids = calls_by_chain.keys().cloned().collect::<Vec<_>>();
        let chain_positions = load_text_hydration_chain_positions(pool, &chain_ids).await?;
        for (chain_id, calls_with_refs) in calls_by_chain {
            let position = chain_positions
                .get(&chain_id)
                .with_context(|| format!("missing text hydration chain position for {chain_id}"))?;
            let calls = calls_with_refs
                .iter()
                .map(|(_, call)| call.clone())
                .collect::<Vec<_>>();
            let outcomes = client
                .hydrate(&chain_id, position, &calls)
                .await
                .with_context(|| format!("failed to hydrate ENS text records on {chain_id}"))?;
            if outcomes.len() != calls_with_refs.len() {
                anyhow::bail!(
                    "text hydration provider returned {} outcomes for {} calls on {chain_id}",
                    outcomes.len(),
                    calls_with_refs.len()
                );
            }

            for ((call_ref, _), outcome) in calls_with_refs.iter().zip(outcomes) {
                let row = rows
                    .get_mut(call_ref.row_index)
                    .context("text hydration row reference is out of bounds")?;
                let entries = row
                    .entries
                    .as_array_mut()
                    .context("record_inventory_current.entries must be an array")?;
                let entry = entries
                    .get_mut(call_ref.entry_index)
                    .context("text hydration entry reference is out of bounds")?;

                match outcome {
                    TextHydrationOutcome::Success(value) => {
                        set_entry_success(entry, value);
                        changed_rows.insert(call_ref.row_index);
                        summary.hydrated_entry_count += 1;
                    }
                    TextHydrationOutcome::NotFound => {
                        set_entry_not_found(entry);
                        changed_rows.insert(call_ref.row_index);
                        summary.not_found_entry_count += 1;
                    }
                    TextHydrationOutcome::Failed(message)
                        if text_hydration_failure_is_skippable(&message) =>
                    {
                        summary.skipped_entry_count += 1;
                    }
                    TextHydrationOutcome::Failed(_) => {
                        summary.failed_entry_count += 1;
                    }
                }
            }
        }

        for row_index in changed_rows {
            let row = rows
                .get(row_index)
                .context("changed text hydration row reference is out of bounds")?;
            update_record_inventory_entries(pool, row).await?;
            summary.updated_row_count += 1;
        }
    }

    Ok(summary)
}

async fn load_text_hydration_chain_positions(
    pool: &PgPool,
    chain_ids: &[String],
) -> Result<BTreeMap<String, TextHydrationChainPosition>> {
    if chain_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_number,
            canonical_block_hash
        FROM chain_checkpoints
        WHERE chain_id = ANY($1::TEXT[])
        "#,
    )
    .bind(chain_ids)
    .fetch_all(pool)
    .await
    .context("failed to load text hydration chain checkpoints")?;

    let mut positions = BTreeMap::new();
    for row in rows {
        let chain_id: String = row.try_get("chain_id")?;
        let block_number: Option<i64> = row.try_get("canonical_block_number")?;
        let block_hash: Option<String> = row.try_get("canonical_block_hash")?;
        let Some((block_number, block_hash)) = block_number.zip(block_hash) else {
            continue;
        };
        positions.insert(
            chain_id,
            TextHydrationChainPosition {
                block_number,
                block_hash,
            },
        );
    }

    for chain_id in chain_ids {
        if !positions.contains_key(chain_id) {
            anyhow::bail!(
                "record_inventory_current text hydration requires a canonical chain checkpoint for {chain_id}"
            );
        }
    }

    Ok(positions)
}

async fn load_supported_ensv1_text_resolvers(
    pool: &PgPool,
    rows: &[HydrationRow],
) -> Result<BTreeSet<(String, String, String)>> {
    let targets = rows
        .iter()
        .map(|row| {
            (
                row.chain_id.clone(),
                normalize_address(&row.resolver_address),
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let admissions = bigname_manifests::load_ens_v1_public_resolver_profile_admissions_for_targets(
        pool, &targets,
    )
    .await?;
    Ok(admissions
        .into_iter()
        .filter(|admission| {
            admission.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
                && ens_v1_resolver_profile_admitted(&admission.profile)
                && admission.fact_family == "resolver_record:text"
                && admission.status == RESOLVER_PROFILE_STATUS_SUPPORTED
        })
        .map(|admission| {
            (
                admission.chain,
                admission.source_family,
                normalize_address(&admission.address),
            )
        })
        .collect())
}

async fn load_text_hydration_rows(
    pool: &PgPool,
    resource_id: Option<Uuid>,
    after: Option<&HydrationPageCursor>,
    limit: i64,
) -> Result<Vec<HydrationRow>> {
    if limit <= 0 {
        anyhow::bail!("record_inventory_current text hydration row batch limit must be positive");
    }
    let rows = sqlx::query(
        r#"
        WITH candidate_rows AS (
            SELECT
                ric.resource_id,
                ric.record_version_boundary_key,
                ric.record_version_boundary ->> 'logical_name_id' AS logical_name_id,
                ns.namehash,
                ric.entries
            FROM record_inventory_current ric
            LEFT JOIN name_surfaces ns
              ON ns.logical_name_id = ric.record_version_boundary ->> 'logical_name_id'
            WHERE ($1::UUID IS NULL OR ric.resource_id = $1)
              AND (
                  $2::UUID IS NULL
                  OR ric.resource_id > $2
                  OR (
                      ric.resource_id = $2
                      AND ric.record_version_boundary_key > $3
                  )
              )
              AND ric.record_version_boundary ? 'logical_name_id'
              AND ric.record_version_boundary ->> 'logical_name_id' LIKE 'ens:%'
              AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(ric.entries) entry
                  WHERE entry ->> 'record_family' = $4
                    AND entry ->> 'status' = 'unsupported'
                    AND entry ->> 'unsupported_reason' = $5
                    AND entry ->> 'selector_key' IS NOT NULL
              )
        )
        SELECT
            candidate_rows.resource_id,
            candidate_rows.record_version_boundary_key,
            candidate_rows.logical_name_id,
            candidate_rows.namehash,
            resolver_event.chain_id,
            LOWER(resolver_event.after_state ->> 'resolver') AS resolver_address,
            candidate_rows.entries
        FROM candidate_rows
        JOIN LATERAL (
            SELECT ne.chain_id, ne.after_state
            FROM normalized_events ne
            WHERE ne.resource_id = candidate_rows.resource_id
              AND ne.logical_name_id = candidate_rows.logical_name_id
              AND ne.event_kind = $6
              AND ne.source_family = ANY($7::TEXT[])
              AND ne.chain_id IS NOT NULL
              AND ne.block_number IS NOT NULL
              AND ne.block_hash IS NOT NULL
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY
                ne.block_number DESC,
                ne.log_index DESC NULLS LAST,
                ne.normalized_event_id DESC
            LIMIT 1
        ) resolver_event ON TRUE
        WHERE resolver_event.after_state ->> 'resolver' IS NOT NULL
          AND LOWER(resolver_event.after_state ->> 'resolver') <>
              '0x0000000000000000000000000000000000000000'
        ORDER BY candidate_rows.resource_id, candidate_rows.record_version_boundary_key
        LIMIT $8
        "#,
    )
    .bind(resource_id)
    .bind(after.map(|cursor| cursor.resource_id))
    .bind(after.map(|cursor| cursor.record_version_boundary_key.as_str()))
    .bind(SUPPORTED_TEXT_RECORD_FAMILY)
    .bind(CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(vec![
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
    ])
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("failed to load record_inventory_current text hydration rows")?;

    rows.into_iter()
        .map(|row| {
            Ok(HydrationRow {
                resource_id: row.try_get("resource_id")?,
                record_version_boundary_key: row.try_get("record_version_boundary_key")?,
                logical_name_id: row.try_get("logical_name_id")?,
                namehash: row.try_get("namehash")?,
                chain_id: row.try_get("chain_id")?,
                resolver_address: row.try_get("resolver_address")?,
                entries: row.try_get("entries")?,
            })
        })
        .collect()
}

async fn update_record_inventory_entries(pool: &PgPool, row: &HydrationRow) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE record_inventory_current
        SET entries = $3
        WHERE resource_id = $1
          AND record_version_boundary_key = $2
        "#,
    )
    .bind(row.resource_id)
    .bind(&row.record_version_boundary_key)
    .bind(&row.entries)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to update hydrated record_inventory_current entries for resource_id {} boundary {}",
            row.resource_id, row.record_version_boundary_key
        )
    })?;
    Ok(())
}

fn candidate_text_entry_count(entries: &Value) -> Result<usize> {
    Ok(entries
        .as_array()
        .context("record_inventory_current.entries must be an array")?
        .iter()
        .filter(|entry| hydration_text_key(entry).is_some())
        .count())
}

fn hydration_text_key(entry: &Value) -> Option<&str> {
    if entry.get("record_family").and_then(Value::as_str) != Some(SUPPORTED_TEXT_RECORD_FAMILY)
        || entry.get("status").and_then(Value::as_str) != Some("unsupported")
        || entry.get("unsupported_reason").and_then(Value::as_str)
            != Some(CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED)
    {
        return None;
    }

    let text_key = entry.get("selector_key").and_then(Value::as_str)?;
    if text_key.trim().is_empty() {
        return None;
    }
    let expected_record_key = format!("text:{text_key}");
    (entry.get("record_key").and_then(Value::as_str) == Some(expected_record_key.as_str()))
        .then_some(text_key)
}

fn set_entry_success(entry: &mut Value, value: String) {
    entry["status"] = json!("success");
    entry["value"] = json!(value);
    remove_entry_field(entry, "unsupported_reason");
}

fn set_entry_not_found(entry: &mut Value) {
    entry["status"] = json!("not_found");
    remove_entry_field(entry, "value");
    remove_entry_field(entry, "unsupported_reason");
}

fn text_hydration_failure_is_skippable(message: &str) -> bool {
    message.starts_with("resolver text call return data is malformed:")
}

fn remove_entry_field(entry: &mut Value, field: &str) {
    if let Some(object) = entry.as_object_mut() {
        object.remove(field);
    }
}

fn ens_name_from_logical_name_id(logical_name_id: &str) -> Option<&str> {
    logical_name_id
        .strip_prefix("ens:")
        .filter(|name| !name.trim().is_empty())
}

fn normalize_address(address: &str) -> String {
    normalize_evm_address(address.trim())
}

fn ens_v1_resolver_profile_admitted(profile: &str) -> bool {
    matches!(
        profile,
        ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE
            | "public_resolver_wrapper_aware"
            | "public_resolver_legacy_multicoin_dns"
            | "public_resolver_legacy_multicoin"
            | "public_resolver_legacy_eth_addr_text"
            | "public_resolver_legacy_eth_addr"
    )
}

#[cfg(test)]
#[path = "hydration_tests_support.rs"]
pub(super) mod tests_support;
