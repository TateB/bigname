mod address_matches;
mod decoders;
mod paging;
mod selectors;
mod summary;

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::{CanonicalityState, address_names::AddressNameRelation};

use address_matches::load_address_history_selector;
#[cfg(test)]
use address_matches::{ENS_V1_AUTHORITY_DERIVATION_KIND, ENS_V2_REGISTRY_DERIVATION_KIND};
use paging::{load_event_history_rows, load_history, load_history_head};
use selectors::{name_history_selector, resource_history_selector};

/// Anchor selection for normalized-event history reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryScope {
    Surface,
    Resource,
    Both,
}

impl HistoryScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Resource => "resource",
            Self::Both => "both",
        }
    }
}

/// Replay-stable normalized event exposed to history readers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryEvent {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub block_timestamp: Option<OffsetDateTime>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: CanonicalityState,
    pub before_state: Value,
    pub after_state: Value,
    pub provenance: Value,
    pub coverage: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryCursor {
    pub normalized_event_id: i64,
    pub event_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryChainPositionSample {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub block_timestamp: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistorySummary {
    pub total_count: u64,
    pub normalized_event_ids: Vec<String>,
    pub raw_fact_refs: Vec<Value>,
    pub manifest_versions: Vec<Value>,
    pub execution_trace_id: Option<String>,
    pub chain_position_samples: Vec<HistoryChainPositionSample>,
    pub last_updated: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistorySummaryMode {
    None,
    Count,
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryPage {
    pub rows: Vec<HistoryEvent>,
    pub next_cursor: Option<HistoryCursor>,
    pub summary: Option<HistorySummary>,
}

#[derive(Debug)]
pub struct InvalidHistoryCursor;

impl std::fmt::Display for InvalidHistoryCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("history page cursor does not match filtered event history")
    }
}

impl std::error::Error for InvalidHistoryCursor {}

/// Address-derived anchor filter for app-facing event history reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventHistoryAddressFilter {
    pub address: String,
    pub relation: Option<AddressNameRelation>,
}

/// Projection-backed filters for canonical normalized-event history reads.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventHistoryFilter {
    pub namespace: Option<String>,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub address: Option<EventHistoryAddressFilter>,
    pub event_kinds: Vec<String>,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub(in crate::history) struct EventHistoryReadFilter {
    pub(in crate::history) selectors: Vec<selectors::HistorySelector>,
    pub(in crate::history) namespace: Option<String>,
    pub(in crate::history) event_kinds: Vec<String>,
    pub(in crate::history) from_block: Option<i64>,
    pub(in crate::history) to_block: Option<i64>,
}

/// Load history rows for one logical name anchor.
pub async fn load_name_history(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history(
        pool,
        name_history_selector(logical_name_id, resource_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load one SQL-keyset page for one logical name anchor.
#[allow(clippy::too_many_arguments)]
pub async fn load_name_history_page(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![name_history_selector(logical_name_id, resource_ids, scope)],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        cursor,
        page_size,
        summary_mode,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history page for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load the first history row for one logical name anchor under the shared default sort.
pub async fn load_name_history_head(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    load_history_head(
        pool,
        name_history_selector(logical_name_id, resource_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history head for logical_name_id {logical_name_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load app-facing event history rows using only canonical normalized-event/history anchors.
pub async fn load_event_history(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    let read_filter = event_history_read_filter(pool, filter, canonical_only).await?;
    load_event_history_rows(pool, read_filter, canonical_only)
        .await
        .context("failed to load app-facing event history")
}

/// Load one SQL-keyset page for app-facing event history filters.
pub async fn load_event_history_page(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    let read_filter = event_history_read_filter(pool, filter, canonical_only).await?;
    paging::load_history_page(
        pool,
        read_filter,
        canonical_only,
        cursor,
        page_size,
        summary_mode,
    )
    .await
    .context("failed to load app-facing event history page")
}

/// Load history rows for one resource anchor.
pub async fn load_resource_history(
    pool: &PgPool,
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history(
        pool,
        resource_history_selector(resource_id, logical_name_ids, scope),
        canonical_only,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history for resource_id {resource_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load one SQL-keyset page for one resource anchor.
#[allow(clippy::too_many_arguments)]
pub async fn load_resource_history_page(
    pool: &PgPool,
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![resource_history_selector(
                resource_id,
                logical_name_ids,
                scope,
            )],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        cursor,
        page_size,
        summary_mode,
    )
    .await
    .with_context(|| {
        format!(
            "failed to load history page for resource_id {resource_id} with scope {}",
            scope.as_str()
        )
    })
}

/// Load history rows for one address-derived anchor set.
pub async fn load_address_history(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    let normalized_address = address.to_ascii_lowercase();
    let selector = load_address_history_selector(
        pool,
        &normalized_address,
        namespace,
        relation,
        scope,
        canonical_only,
    )
    .await?;

    load_history(pool, selector, canonical_only)
        .await
        .with_context(|| {
            let mut parts = vec![format!("address {}", normalized_address)];
            if let Some(namespace) = namespace {
                parts.push(format!("namespace {namespace}"));
            }
            if let Some(relation) = relation {
                parts.push(format!("relation {}", relation.as_str()));
            }
            parts.push(format!("scope {}", scope.as_str()));
            format!("failed to load history for {}", parts.join(" "))
        })
}

/// Load one SQL-keyset page for one address-derived anchor set.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_history_page(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    scope: HistoryScope,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    let normalized_address = address.to_ascii_lowercase();
    let selector = load_address_history_selector(
        pool,
        &normalized_address,
        namespace,
        relation,
        scope,
        canonical_only,
    )
    .await?;

    paging::load_history_page(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        cursor,
        page_size,
        summary_mode,
    )
    .await
    .with_context(|| {
        let mut parts = vec![format!("address {}", normalized_address)];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relation) = relation {
            parts.push(format!("relation {}", relation.as_str()));
        }
        parts.push(format!("scope {}", scope.as_str()));
        format!("failed to load history page for {}", parts.join(" "))
    })
}

async fn event_history_read_filter(
    pool: &PgPool,
    filter: EventHistoryFilter,
    canonical_only: bool,
) -> Result<EventHistoryReadFilter> {
    let mut selectors = Vec::new();

    if let Some(logical_name_id) = filter.logical_name_id.as_deref() {
        let resource_ids =
            load_resource_ids_for_logical_name_id(pool, logical_name_id, canonical_only)
                .await
                .with_context(|| {
                    format!(
                        "failed to load event history resource anchors for logical_name_id {logical_name_id}"
                    )
                })?;
        selectors.push(name_history_selector(
            logical_name_id,
            &resource_ids,
            HistoryScope::Both,
        ));
    }

    if let Some(resource_id) = filter.resource_id {
        let logical_name_ids =
            load_logical_name_ids_for_resource_id(pool, resource_id, canonical_only)
                .await
                .with_context(|| {
                    format!(
                        "failed to load event history surface anchors for resource_id {resource_id}"
                    )
                })?;
        selectors.push(resource_history_selector(
            resource_id,
            &logical_name_ids,
            HistoryScope::Both,
        ));
    }

    if let Some(address_filter) = filter.address.as_ref() {
        let normalized_address = address_filter.address.to_ascii_lowercase();
        selectors.push(
            load_address_history_selector(
                pool,
                &normalized_address,
                filter.namespace.as_deref(),
                address_filter.relation,
                HistoryScope::Both,
                canonical_only,
            )
            .await
            .with_context(|| {
                let mut parts = vec![format!("address {normalized_address}")];
                if let Some(namespace) = filter.namespace.as_ref() {
                    parts.push(format!("namespace {namespace}"));
                }
                if let Some(relation) = address_filter.relation {
                    parts.push(format!("relation {}", relation.as_str()));
                }
                format!(
                    "failed to load event history address anchors for {}",
                    parts.join(" ")
                )
            })?,
        );
    }

    Ok(EventHistoryReadFilter {
        selectors,
        namespace: filter.namespace,
        event_kinds: filter.event_kinds,
        from_block: filter.from_block,
        to_block: filter.to_block,
    })
}

async fn load_resource_ids_for_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
    canonical_only: bool,
) -> Result<Vec<Uuid>> {
    let bindings = if canonical_only {
        crate::load_surface_bindings_by_logical_name_id(pool, logical_name_id).await
    } else {
        crate::load_surface_bindings_by_logical_name_id_including_noncanonical(
            pool,
            logical_name_id,
        )
        .await
    }?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

async fn load_logical_name_ids_for_resource_id(
    pool: &PgPool,
    resource_id: Uuid,
    canonical_only: bool,
) -> Result<Vec<String>> {
    let bindings = if canonical_only {
        crate::load_surface_bindings_by_resource_id(pool, resource_id).await
    } else {
        crate::load_surface_bindings_by_resource_id_including_noncanonical(pool, resource_id).await
    }?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.logical_name_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

#[cfg(test)]
mod tests;
