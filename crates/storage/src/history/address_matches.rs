use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::address_names::{
    AddressNameRelation, load_address_names_current_for_relations,
    load_address_names_current_including_noncanonical_for_relations,
};

use super::{HistoryScope, decoders::decode_address_history_anchor, selectors::HistorySelector};

pub(super) const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
pub(super) const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ADDRESS_HISTORY_MATCH_DERIVATION_KINDS: &[&str] = &[
    ENS_V1_AUTHORITY_DERIVATION_KIND,
    ENS_V2_REGISTRY_DERIVATION_KIND,
];
const ADDRESS_HISTORY_MATCH_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AddressHistoryAnchor {
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
}

pub(super) async fn load_address_history_selector(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    canonical_only: bool,
) -> Result<HistorySelector> {
    let current_rows = if canonical_only {
        load_address_names_current_for_relations(pool, address, namespace, relations).await
    } else {
        load_address_names_current_including_noncanonical_for_relations(
            pool, address, namespace, relations,
        )
        .await
    }
    .with_context(|| {
        let mut parts = vec![format!("address {address}")];
        if let Some(namespace) = namespace {
            parts.push(format!("namespace {namespace}"));
        }
        if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
            parts.push(format!(
                "relations {}",
                relations
                    .iter()
                    .map(|relation| relation.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
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

    let historical_matches = load_historical_address_history_matches(
        pool,
        address,
        namespace,
        relations,
        canonical_only,
    )
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

async fn load_historical_address_history_matches(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
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
    push_address_match_filter(&mut builder, address, relations);

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
    relations: Option<&'a [AddressNameRelation]>,
) {
    let include_registrant =
        relations.is_none_or(|relations| relations.contains(&AddressNameRelation::Registrant));
    let include_token_holder =
        relations.is_none_or(|relations| relations.contains(&AddressNameRelation::TokenHolder));
    let include_registry_owner = relations
        .is_none_or(|relations| relations.contains(&AddressNameRelation::EffectiveController));

    builder.push("(");
    let mut needs_or = false;
    if include_registrant {
        push_registrant_match_filter(builder, address);
        needs_or = true;
    }
    if include_token_holder {
        if needs_or {
            builder.push(" OR ");
        }
        push_token_holder_match_filter(builder, address);
        needs_or = true;
    }
    if include_registry_owner {
        if needs_or {
            builder.push(" OR ");
        }
        push_registry_owner_match_filter(builder, address);
        needs_or = true;
    }
    if !needs_or {
        builder.push("FALSE");
    }
    builder.push(")");
}

fn push_registrant_match_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
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
            )
        )
        "#,
    );
}

fn push_token_holder_match_filter<'a>(builder: &mut QueryBuilder<'a, Postgres>, address: &'a str) {
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
