use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::{
    CanonicalityState,
    address_names::{
        AddressNameRelation, load_address_names_current,
        load_address_names_current_including_noncanonical,
    },
};

const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ADDRESS_HISTORY_MATCH_DERIVATION_KINDS: &[&str] = &[
    ENS_V1_AUTHORITY_DERIVATION_KIND,
    ENS_V2_REGISTRY_DERIVATION_KIND,
];
const ADDRESS_HISTORY_MATCH_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
];

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

#[derive(Clone, Debug)]
enum HistorySelector {
    None,
    LogicalNames(Vec<String>),
    Resources(Vec<Uuid>),
    LogicalNamesOrResources {
        logical_name_ids: Vec<String>,
        resource_ids: Vec<Uuid>,
    },
}

impl HistorySelector {
    fn logical_names(logical_name_ids: Vec<String>) -> Self {
        if logical_name_ids.is_empty() {
            Self::None
        } else {
            Self::LogicalNames(logical_name_ids)
        }
    }

    fn resources(resource_ids: Vec<Uuid>) -> Self {
        if resource_ids.is_empty() {
            Self::None
        } else {
            Self::Resources(resource_ids)
        }
    }

    fn logical_names_or_resources(logical_name_ids: Vec<String>, resource_ids: Vec<Uuid>) -> Self {
        match (logical_name_ids.is_empty(), resource_ids.is_empty()) {
            (true, true) => Self::None,
            (false, true) => Self::LogicalNames(logical_name_ids),
            (true, false) => Self::Resources(resource_ids),
            (false, false) => Self::LogicalNamesOrResources {
                logical_name_ids,
                resource_ids,
            },
        }
    }
}

fn name_history_selector(
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
) -> HistorySelector {
    let logical_name_ids = vec![logical_name_id.to_owned()];
    let resource_ids = resource_ids.to_vec();

    match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    }
}

fn resource_history_selector(
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
) -> HistorySelector {
    let logical_name_ids = logical_name_ids.to_vec();
    let resource_ids = vec![resource_id];

    match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    }
}

async fn load_address_history_selector(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<HistorySelector> {
    let current_rows = if canonical_only {
        load_address_names_current(pool, address, namespace, relation).await
    } else {
        load_address_names_current_including_noncanonical(pool, address, namespace, relation).await
    }
    .with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relation) = relation {
            parts.push(format!("relation {}", relation.as_str()));
        }
        format!(
            "failed to load address_names_current anchors for {}",
            parts.join(" ")
        )
    })?;

    let mut logical_name_ids = current_rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<BTreeSet<_>>();
    let mut resource_ids = current_rows
        .iter()
        .map(|row| row.resource_id)
        .collect::<BTreeSet<_>>();

    let historical_matches =
        load_historical_address_history_matches(pool, address, namespace, relation, canonical_only)
            .await?;
    for anchor in historical_matches {
        if let Some(logical_name_id) = anchor.logical_name_id {
            logical_name_ids.insert(logical_name_id);
        }
        if let Some(resource_id) = anchor.resource_id {
            resource_ids.insert(resource_id);
        }
    }

    let logical_name_ids = logical_name_ids.into_iter().collect::<Vec<_>>();
    let resource_ids = resource_ids.into_iter().collect::<Vec<_>>();

    Ok(match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AddressHistoryAnchor {
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
}

async fn load_historical_address_history_matches(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    canonical_only: bool,
) -> Result<Vec<AddressHistoryAnchor>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT DISTINCT
            ne.logical_name_id,
            ne.resource_id
        FROM normalized_events ne
        LEFT JOIN resources r
          ON r.resource_id = ne.resource_id
        WHERE ne.derivation_kind IN (
        "#,
    );
    let mut separated = builder.separated(", ");
    for derivation_kind in ADDRESS_HISTORY_MATCH_DERIVATION_KINDS {
        separated.push_bind(*derivation_kind);
    }
    separated.push_unseparated(") AND ne.event_kind IN (");
    let mut separated = builder.separated(", ");
    for event_kind in ADDRESS_HISTORY_MATCH_EVENT_KINDS {
        separated.push_bind(*event_kind);
    }
    separated.push_unseparated(")");

    if canonical_only {
        builder.push(
            r#"
            AND ne.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            "#,
        );
    }

    if let Some(namespace) = namespace {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    builder.push(" AND ");
    push_address_match_filter(&mut builder, address, relation);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch historical address-history anchors")?;

    rows.into_iter()
        .map(decode_address_history_anchor)
        .collect()
}

fn push_address_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    relation: Option<AddressNameRelation>,
) {
    match relation {
        Some(AddressNameRelation::Registrant) | Some(AddressNameRelation::TokenHolder) => {
            builder.push("(");
            push_tokenized_address_match_filter(builder, address);
            builder.push(")");
        }
        Some(AddressNameRelation::EffectiveController) => {
            builder.push("(");
            push_tokenized_address_match_filter(builder, address);
            builder.push(" OR ");
            push_registry_owner_match_filter(builder, address);
            builder.push(")");
        }
        None => {
            builder.push("(");
            push_tokenized_address_match_filter(builder, address);
            builder.push(" OR ");
            push_registry_owner_match_filter(builder, address);
            builder.push(")");
        }
    }
}

fn push_tokenized_address_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NOT NULL
                OR ne.namespace = 
        "#,
    );
    builder.push_bind("basenames");
    builder.push(" OR ne.derivation_kind = ");
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND (
                (
                    ne.event_kind = 'RegistrationGranted'
                    AND LOWER(COALESCE(ne.after_state ->> 'registrant', '')) = 
        "#,
    );
    builder.push_bind(address);
    builder.push(
        r#"
                )
                OR (
                    ne.event_kind = 'TokenControlTransferred'
                    AND LOWER(COALESCE(ne.after_state ->> 'to', '')) = 
        "#,
    );
    builder.push_bind(address);
    builder.push(
        r#"
                )
            )
        )
        "#,
    );
}

fn push_registry_owner_match_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
) {
    builder.push(
        r#"
        (
            (
                r.token_lineage_id IS NULL
                OR ne.derivation_kind =
        "#,
    );
    builder.push_bind(ENS_V2_REGISTRY_DERIVATION_KIND);
    builder.push(
        r#"
            )
            AND ne.event_kind = 'AuthorityTransferred'
            AND LOWER(COALESCE(ne.after_state ->> 'owner', '')) = 
        "#,
    );
    builder.push_bind(address);
    builder.push(")");
}

async fn load_history(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(pool, selector, canonical_only, false).await
}

async fn load_history_head(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    let mut rows = load_history_internal(pool, selector, canonical_only, true).await?;
    Ok(rows.drain(..).next())
}

async fn load_history_internal(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
    head_only: bool,
) -> Result<Vec<HistoryEvent>> {
    if matches!(selector, HistorySelector::None) {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE
        "#,
    );

    match &selector {
        HistorySelector::LogicalNames(logical_name_ids) => {
            push_string_filter(&mut builder, "ne.logical_name_id", logical_name_ids);
        }
        HistorySelector::Resources(resource_ids) => {
            push_uuid_filter(&mut builder, "ne.resource_id", resource_ids);
        }
        HistorySelector::LogicalNamesOrResources {
            logical_name_ids,
            resource_ids,
        } => {
            builder.push("(");
            push_string_filter(&mut builder, "ne.logical_name_id", logical_name_ids);
            builder.push(" OR ");
            push_uuid_filter(&mut builder, "ne.resource_id", resource_ids);
            builder.push(")");
        }
        HistorySelector::None => unreachable!("none selector handled before query build"),
    }

    if canonical_only {
        builder.push(
            r#"
            AND ne.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            "#,
        );
    }

    builder.push(
        r#"
        ORDER BY
            CASE WHEN ne.block_number IS NULL THEN 1 ELSE 0 END,
            ne.block_number DESC,
            CASE WHEN ne.chain_id IS NULL THEN 1 ELSE 0 END,
            ne.chain_id ASC,
            CASE WHEN ne.block_hash IS NULL THEN 1 ELSE 0 END,
            ne.block_hash DESC,
            CASE WHEN ne.transaction_hash IS NULL THEN 1 ELSE 0 END,
            ne.transaction_hash DESC,
            COALESCE(ne.log_index, -1) DESC,
            ne.event_identity DESC
        "#,
    );

    if head_only {
        builder.push(" LIMIT 1");
    }

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized-event history rows")?;

    rows.into_iter().map(decode_history_event).collect()
}

fn push_string_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [String],
) {
    builder.push(column);
    push_string_filter_tail(builder, values);
}

fn push_string_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [String]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}

fn push_uuid_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [Uuid],
) {
    builder.push(column);
    push_uuid_filter_tail(builder, values);
}

fn push_uuid_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [Uuid]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}

fn decode_address_history_anchor(row: PgRow) -> Result<AddressHistoryAnchor> {
    Ok(AddressHistoryAnchor {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
    })
}

fn decode_history_event(row: PgRow) -> Result<HistoryEvent> {
    let provenance: Value = row.try_get("provenance").context("missing provenance")?;
    let coverage: Value = row.try_get("coverage").context("missing coverage")?;
    ensure_json_object(&provenance, "provenance")?;
    ensure_json_object(&coverage, "coverage")?;

    Ok(HistoryEvent {
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        event_kind: row.try_get("event_kind").context("missing event_kind")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        log_index: row.try_get("log_index").context("missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        derivation_kind: row
            .try_get("derivation_kind")
            .context("missing derivation_kind")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        before_state: row
            .try_get("before_state")
            .context("missing before_state")?,
        after_state: row.try_get("after_state").context("missing after_state")?,
        provenance,
        coverage,
    })
}

fn ensure_json_object(value: &Value, field_name: &str) -> Result<()> {
    if !value.is_object() {
        bail!("history field {field_name} must be a JSON object");
    }

    Ok(())
}

#[cfg(test)]
mod tests;
