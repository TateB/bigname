use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, ChildrenCurrentRow, delete_children_current,
    load_canonical_declared_child_sources, load_raw_block, stream_canonical_declared_child_sources,
    upsert_children_current_rows,
};
use futures_util::{TryStreamExt, pin_mut};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinSet;

#[allow(clippy::duplicate_mod)]
#[path = "staged_rebuild.rs"]
mod staged_rebuild;

use crate::projection_json::format_timestamp;
use staged_rebuild::{
    CHILDREN_CURRENT_COLUMNS, count_rows, create_stage_table, drop_stage_table,
    publish_stage_table, stage_children_current_rows,
};

const DECLARED_SURFACE_CLASS: &str = "declared";
const CHILDREN_CURRENT_DERIVATION_KIND: &str = "children_current_rebuild";
const CHILDREN_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;
const CHILDREN_CURRENT_BLOCK_CACHE_LIMIT: usize = 4_096;
const CHILDREN_CURRENT_REBUILD_CONCURRENCY: usize = 8;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChildrenCurrentRebuildSummary {
    pub requested_parent_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_children_current(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<ChildrenCurrentRebuildSummary> {
    match parent_logical_name_id {
        Some(parent_logical_name_id) => rebuild_one_parent(pool, parent_logical_name_id).await,
        None => rebuild_all_parents(pool).await,
    }
}

async fn rebuild_all_parents(pool: &PgPool) -> Result<ChildrenCurrentRebuildSummary> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire children_current staging connection")?;
    let stage_table = create_stage_table(&mut conn, "children_current").await?;
    let previous_row_count = count_rows(
        &mut conn,
        "children_current",
        Some("WHERE surface_class = 'declared'"),
    )
    .await?;
    let mut rows = Vec::with_capacity(CHILDREN_CURRENT_REBUILD_BATCH_SIZE);
    let mut queued_source_count = 0usize;
    let mut completed_source_count = 0usize;
    let mut upserted_row_count = 0usize;

    let sources = stream_canonical_declared_child_sources(pool, None);
    pin_mut!(sources);
    let mut tasks = JoinSet::new();

    while tasks.len() < CHILDREN_CURRENT_REBUILD_CONCURRENCY {
        let Some(source) = sources.try_next().await? else {
            break;
        };
        queued_source_count += 1;
        spawn_children_rebuild_task(&mut tasks, pool, source);
    }

    while let Some(result) = tasks.join_next().await {
        completed_source_count += 1;
        rows.push(result??);
        if rows.len() >= CHILDREN_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count +=
                stage_children_current_rows(&mut conn, &stage_table, &rows).await? as usize;
            rows.clear();
        }

        if completed_source_count % 5_000 == 0 {
            tracing::info!(
                projection = "children_current",
                queued_source_count,
                completed_source_count,
                upserted_row_count,
                "children_current rebuild sources processed"
            );
        }

        while tasks.len() < CHILDREN_CURRENT_REBUILD_CONCURRENCY {
            let Some(source) = sources.try_next().await? else {
                break;
            };
            queued_source_count += 1;
            spawn_children_rebuild_task(&mut tasks, pool, source);
        }
    }

    if !rows.is_empty() {
        upserted_row_count +=
            stage_children_current_rows(&mut conn, &stage_table, &rows).await? as usize;
    }
    let (_deleted_row_count, published_row_count) = publish_stage_table(
        &mut conn,
        "children_current",
        &stage_table,
        CHILDREN_CURRENT_COLUMNS,
        Some("WHERE surface_class = 'declared'"),
    )
    .await?;
    drop_stage_table(&mut conn, &stage_table).await?;
    debug_assert_eq!(published_row_count as usize, upserted_row_count);

    let requested_parent_count = count_children_current_parents(pool).await?;

    Ok(ChildrenCurrentRebuildSummary {
        requested_parent_count,
        upserted_row_count,
        deleted_row_count: previous_row_count,
    })
}

fn spawn_children_rebuild_task(
    tasks: &mut JoinSet<Result<ChildrenCurrentRow>>,
    pool: &PgPool,
    source: bigname_storage::DeclaredChildEventSource,
) {
    let pool = pool.clone();
    tasks.spawn(async move {
        let mut block_cache = BTreeMap::new();
        build_children_row(&pool, &source, &mut block_cache).await
    });
}

async fn rebuild_one_parent(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<ChildrenCurrentRebuildSummary> {
    let sources = load_canonical_declared_child_sources(pool, Some(parent_logical_name_id)).await?;
    let rows = build_children_rows(pool, &sources).await?;
    let upserted_row_count = upsert_children_current_rows(pool, &rows).await?.len();
    let deleted_row_count =
        delete_stale_children_current_rows_for_parent(pool, parent_logical_name_id, &rows).await?;

    Ok(ChildrenCurrentRebuildSummary {
        requested_parent_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_children_rows(
    pool: &PgPool,
    sources: &[bigname_storage::DeclaredChildEventSource],
) -> Result<Vec<ChildrenCurrentRow>> {
    let mut block_cache = BTreeMap::new();
    let mut rows = Vec::with_capacity(sources.len());

    for source in sources {
        rows.push(build_children_row(pool, source, &mut block_cache).await?);
    }

    Ok(rows)
}

async fn delete_stale_children_current_rows_for_parent(
    pool: &PgPool,
    parent_logical_name_id: &str,
    rows: &[ChildrenCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return delete_children_current(pool, parent_logical_name_id).await;
    }

    let child_logical_name_ids = rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM children_current current
        WHERE current.parent_logical_name_id = $1
          AND current.surface_class = $2
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($3::TEXT[]) AS replacement(child_logical_name_id)
            WHERE replacement.child_logical_name_id = current.child_logical_name_id
          )
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(DECLARED_SURFACE_CLASS)
    .bind(&child_logical_name_ids)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete stale children_current rows for parent_logical_name_id {parent_logical_name_id}"
        )
    })
    .map(|result| result.rows_affected())
}

async fn build_children_row(
    pool: &PgPool,
    source: &bigname_storage::DeclaredChildEventSource,
    block_cache: &mut BTreeMap<(String, String), bigname_storage::RawBlock>,
) -> Result<ChildrenCurrentRow> {
    let block = load_source_block(pool, source, block_cache).await?;

    Ok(ChildrenCurrentRow {
        parent_logical_name_id: source.parent_logical_name_id.clone(),
        child_logical_name_id: source.child_logical_name_id.clone(),
        surface_class: DECLARED_SURFACE_CLASS.to_owned(),
        namespace: source.namespace.clone(),
        canonical_display_name: source.canonical_display_name.clone(),
        normalized_name: source.normalized_name.clone(),
        namehash: source.namehash.clone(),
        labelhash: source.labelhash.clone(),
        owner: source.owner.clone(),
        registrant: source.registrant.clone(),
        provenance: build_provenance(source),
        chain_positions: build_chain_positions(source, &block),
        canonicality_summary: build_canonicality_summary(source, block.canonicality_state),
        manifest_version: source.manifest_version,
        last_recomputed_at: block.block_timestamp,
    })
}

async fn load_source_block(
    pool: &PgPool,
    source: &bigname_storage::DeclaredChildEventSource,
    block_cache: &mut BTreeMap<(String, String), bigname_storage::RawBlock>,
) -> Result<bigname_storage::RawBlock> {
    let cache_key = (source.chain_id.clone(), source.block_hash.clone());
    if let Some(block) = block_cache.get(&cache_key) {
        return Ok(block.clone());
    }

    let block = load_raw_block(pool, &source.chain_id, &source.block_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load raw block for child source {} on chain {} block {}",
                source.event_identity, source.chain_id, source.block_hash
            )
        })?
        .with_context(|| {
            format!(
                "missing raw block for child source {} on chain {} block {}",
                source.event_identity, source.chain_id, source.block_hash
            )
        })?;

    block_cache.insert(cache_key, block.clone());
    if block_cache.len() > CHILDREN_CURRENT_BLOCK_CACHE_LIMIT
        && let Some(first_key) = block_cache.keys().next().cloned()
    {
        block_cache.remove(&first_key);
    }
    Ok(block)
}

async fn count_children_current_parents(pool: &PgPool) -> Result<usize> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT parent_logical_name_id)
        FROM children_current
        WHERE surface_class = $1
        "#,
    )
    .bind(DECLARED_SURFACE_CLASS)
    .fetch_one(pool)
    .await
    .context("failed to count children_current rebuilt parents")
    .map(|count| count as usize)
}

fn build_provenance(source: &bigname_storage::DeclaredChildEventSource) -> Value {
    json!({
        "normalized_event_ids": source.normalized_event_ids.clone(),
        "raw_fact_refs": source.raw_fact_refs.clone(),
        "manifest_versions": source.manifest_versions.clone(),
        "label": {
            "labelhash": source.labelhash,
            "status": child_label_status(source),
            "source": source.label_source.clone(),
        },
        "execution_trace_id": Value::Null,
        "derivation_kind": CHILDREN_CURRENT_DERIVATION_KIND,
    })
}

fn child_label_status(source: &bigname_storage::DeclaredChildEventSource) -> &'static str {
    if source.labelhash.is_none() {
        return "not_applicable";
    }

    if source.normalized_name.starts_with('[') {
        "unknown"
    } else {
        "known"
    }
}

fn build_chain_positions(
    source: &bigname_storage::DeclaredChildEventSource,
    block: &bigname_storage::RawBlock,
) -> Value {
    json!({
        chain_slot(&source.chain_id): {
            "chain_id": source.chain_id,
            "block_number": source.block_number,
            "block_hash": source.block_hash,
            "timestamp": format_timestamp(block.block_timestamp),
        }
    })
}

fn build_canonicality_summary(
    source: &bigname_storage::DeclaredChildEventSource,
    state: CanonicalityState,
) -> Value {
    json!({
        "status": state.as_str(),
        "chains": {
            source.chain_id.clone(): state.as_str(),
        }
    })
}

fn chain_slot(chain_id: &str) -> &str {
    match chain_id {
        "ethereum-mainnet" => "ethereum",
        "base-mainnet" => "base",
        _ => chain_id,
    }
}

#[cfg(test)]
mod tests;
