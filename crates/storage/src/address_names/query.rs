use sqlx::{Postgres, QueryBuilder};

use super::types::{AddressNameRelation, AddressNamesCurrentCursor, AddressNamesCurrentDedupe};

pub(super) const DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR token_lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
  )
"#;

pub(super) fn push_address_names_current_grouped_entries_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    address: &'a str,
    namespace: Option<&'a str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
) {
    builder.push(
        r#"
        WITH filtered AS (
            SELECT
                anc.address,
                anc.logical_name_id,
                anc.relation,
                anc.namespace,
                anc.canonical_display_name,
                anc.normalized_name,
                anc.namehash,
                anc.surface_binding_id,
                anc.resource_id,
                anc.token_lineage_id,
                anc.binding_kind,
                anc.provenance,
                anc.coverage,
                anc.chain_positions,
                anc.canonicality_summary,
                anc.manifest_version,
                anc.last_recomputed_at,
                CASE anc.relation
                    WHEN 'registrant' THEN 0
                    WHEN 'token_holder' THEN 1
                    WHEN 'effective_controller' THEN 2
                    ELSE 99
                END AS relation_rank
            FROM address_names_current anc
            JOIN name_surfaces surface
              ON surface.logical_name_id = anc.logical_name_id
            JOIN resources resource
              ON resource.resource_id = anc.resource_id
            JOIN surface_bindings binding
              ON binding.surface_binding_id = anc.surface_binding_id
            LEFT JOIN token_lineages token_lineage
              ON token_lineage.token_lineage_id = anc.token_lineage_id
            WHERE anc.address =
        "#,
    );
    builder.push(" ");
    builder.push_bind(address);

    if let Some(namespace) = namespace {
        builder.push(" AND anc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(relation) = relation {
        builder.push(" AND anc.relation = ");
        builder.push_bind(relation.as_str());
    }
    builder.push(DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER);

    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, logical_name_id)
                address,
                logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                surface_binding_id,
                resource_id,
                token_lineage_id,
                binding_kind,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            FROM filtered
            ORDER BY
                address ASC,
                logical_name_id ASC,
                canonical_display_name ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                logical_name_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, logical_name_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                logical_name_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, logical_name_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.logical_name_id = representatives.logical_name_id
        )
            "#,
        ),
        AddressNamesCurrentDedupe::Resource => builder.push(
            r#"
        ),
        representatives AS (
            SELECT DISTINCT ON (address, resource_id)
                address,
                logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                surface_binding_id,
                resource_id,
                token_lineage_id,
                binding_kind,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            FROM filtered
            ORDER BY
                address ASC,
                resource_id ASC,
                canonical_display_name ASC,
                logical_name_id ASC,
                relation_rank ASC
        ),
        relation_values AS (
            SELECT
                address,
                resource_id,
                relation,
                MIN(relation_rank) AS relation_rank
            FROM filtered
            GROUP BY address, resource_id, relation
        ),
        relation_facets AS (
            SELECT
                address,
                resource_id,
                ARRAY_AGG(relation ORDER BY relation_rank ASC) AS relations
            FROM relation_values
            GROUP BY address, resource_id
        ),
        entries AS (
            SELECT
                representatives.address,
                representatives.logical_name_id,
                representatives.namespace,
                representatives.canonical_display_name,
                representatives.normalized_name,
                representatives.namehash,
                representatives.surface_binding_id,
                representatives.resource_id,
                representatives.token_lineage_id,
                representatives.binding_kind,
                relation_facets.relations,
                representatives.provenance,
                representatives.coverage,
                representatives.chain_positions,
                representatives.canonicality_summary,
                representatives.manifest_version,
                representatives.last_recomputed_at
            FROM representatives
            JOIN relation_facets
              ON relation_facets.address = representatives.address
             AND relation_facets.resource_id = representatives.resource_id
        )
            "#,
        ),
    };
}

pub(super) fn push_address_names_current_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a AddressNamesCurrentCursor,
) {
    let cursor_resource_id = cursor.resource_id.to_string();
    builder.push(
        r#"
        WHERE (
            canonical_display_name >
        "#,
    );
    builder.push(" ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" OR (canonical_display_name = ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" AND logical_name_id > ");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(") OR (canonical_display_name = ");
    builder.push_bind(&cursor.canonical_display_name);
    builder.push(" AND logical_name_id = ");
    builder.push_bind(&cursor.logical_name_id);
    builder.push(" AND resource_id::TEXT > ");
    builder.push_bind(cursor_resource_id);
    builder.push("))");
}
