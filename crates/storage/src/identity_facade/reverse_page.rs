use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER, DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER,
    ReverseIdentityStorageInput, reverse_rows::ReverseIdentityPageRow,
};

pub(super) async fn load_reverse_identity_page_rows(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityPageRow>> {
    let input_indexes = (0..inputs.len() as i32).collect::<Vec<_>>();
    let addresses = inputs
        .iter()
        .map(|input| input.address.clone())
        .collect::<Vec<_>>();
    let coin_types = inputs
        .iter()
        .map(|input| input.coin_type.clone())
        .collect::<Vec<_>>();
    let roles = inputs
        .iter()
        .map(|input| input.roles.storage_value().to_owned())
        .collect::<Vec<_>>();
    let page_sizes = inputs
        .iter()
        .map(|input| input.page_size)
        .collect::<Vec<_>>();
    let cursor_is_primary = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.is_primary))
        .collect::<Vec<_>>();
    let cursor_role_rank = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.role_rank))
        .collect::<Vec<_>>();
    let cursor_normalized_names = inputs
        .iter()
        .map(|input| {
            input
                .cursor
                .as_ref()
                .map(|cursor| cursor.normalized_name.clone())
        })
        .collect::<Vec<_>>();
    let cursor_namespaces = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.namespace.clone()))
        .collect::<Vec<_>>();
    let cursor_namehashes = inputs
        .iter()
        .map(|input| input.cursor.as_ref().map(|cursor| cursor.namehash.clone()))
        .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"
        WITH requested AS (
            SELECT *
            FROM UNNEST(
                $1::INT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::BOOLEAN[],
                $7::SMALLINT[],
                $8::TEXT[],
                $9::TEXT[],
                $10::TEXT[]
            ) AS requested(
                input_index,
                address,
                coin_type,
                roles,
                page_size,
                cursor_is_primary,
                cursor_role_rank,
                cursor_normalized_name,
                cursor_namespace,
                cursor_namehash
            )
        ),
        page_rows AS (
            SELECT
                requested.input_index,
                page.logical_name_id,
                page.is_primary,
                page.role_rank,
                page.normalized_name,
                page.namespace,
                page.namehash
            FROM requested
            CROSS JOIN LATERAL (
                SELECT deduped.*
                FROM (
                    SELECT DISTINCT ON (candidate.logical_name_id)
                        candidate.*
                    FROM (
                        (
                            SELECT
                                anc.logical_name_id,
                                anc.normalized_name,
                                anc.namespace,
                                anc.namehash,
                                CASE
                                    WHEN anc.relation IN ('registrant', 'token_holder') THEN 0::SMALLINT
                                    ELSE 1::SMALLINT
                                END AS role_rank,
                                TRUE AS is_primary
                            FROM primary_names_current pnc
                            JOIN address_names_current anc
                              ON anc.address = requested.address
                             AND anc.namespace = pnc.namespace
                             AND anc.normalized_name = pnc.normalized_claim_name
                            JOIN name_surfaces surface
                              ON surface.logical_name_id = anc.logical_name_id
                            JOIN resources resource
                              ON resource.resource_id = anc.resource_id
                            JOIN surface_bindings binding
                              ON binding.surface_binding_id = anc.surface_binding_id
                            LEFT JOIN token_lineages token_lineage
                              ON token_lineage.token_lineage_id = anc.token_lineage_id
                            JOIN name_current identity_nc
                              ON identity_nc.logical_name_id = anc.logical_name_id
                            JOIN name_surfaces identity_nc_surface
                              ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
                            LEFT JOIN resources identity_nc_resource
                              ON identity_nc_resource.resource_id = identity_nc.resource_id
                            LEFT JOIN surface_bindings identity_nc_binding
                              ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
                            LEFT JOIN token_lineages identity_nc_token_lineage
                              ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
                            WHERE pnc.address = requested.address
                              AND pnc.coin_type = requested.coin_type
                              AND pnc.claim_status = 'success'
                              AND (
                                  requested.roles = 'both'
                                  OR (
                                      requested.roles = 'owned'
                                      AND anc.relation IN ('registrant', 'token_holder')
                                  )
                                  OR (
                                      requested.roles = 'managed'
                                      AND anc.relation = 'effective_controller'
                                  )
                              )
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            {DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER}
                        )
                        UNION ALL
                        (
                            SELECT
                                anc.logical_name_id,
                                anc.normalized_name,
                                anc.namespace,
                                anc.namehash,
                                0::SMALLINT AS role_rank,
                                FALSE AS is_primary
                            FROM address_names_current anc
                            JOIN name_surfaces surface
                              ON surface.logical_name_id = anc.logical_name_id
                            JOIN resources resource
                              ON resource.resource_id = anc.resource_id
                            JOIN surface_bindings binding
                              ON binding.surface_binding_id = anc.surface_binding_id
                            LEFT JOIN token_lineages token_lineage
                              ON token_lineage.token_lineage_id = anc.token_lineage_id
                            JOIN name_current identity_nc
                              ON identity_nc.logical_name_id = anc.logical_name_id
                            JOIN name_surfaces identity_nc_surface
                              ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
                            LEFT JOIN resources identity_nc_resource
                              ON identity_nc_resource.resource_id = identity_nc.resource_id
                            LEFT JOIN surface_bindings identity_nc_binding
                              ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
                            LEFT JOIN token_lineages identity_nc_token_lineage
                              ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
                            LEFT JOIN primary_names_current pnc
                              ON pnc.address = requested.address
                             AND pnc.coin_type = requested.coin_type
                             AND pnc.namespace = anc.namespace
                             AND pnc.claim_status = 'success'
                            WHERE requested.roles IN ('owned', 'both')
                              AND anc.address = requested.address
                              AND anc.relation = 'registrant'
                              AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            {DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER}
                        )
                        UNION ALL
                        (
                            SELECT
                                anc.logical_name_id,
                                anc.normalized_name,
                                anc.namespace,
                                anc.namehash,
                                0::SMALLINT AS role_rank,
                                FALSE AS is_primary
                            FROM address_names_current anc
                            JOIN name_surfaces surface
                              ON surface.logical_name_id = anc.logical_name_id
                            JOIN resources resource
                              ON resource.resource_id = anc.resource_id
                            JOIN surface_bindings binding
                              ON binding.surface_binding_id = anc.surface_binding_id
                            LEFT JOIN token_lineages token_lineage
                              ON token_lineage.token_lineage_id = anc.token_lineage_id
                            JOIN name_current identity_nc
                              ON identity_nc.logical_name_id = anc.logical_name_id
                            JOIN name_surfaces identity_nc_surface
                              ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
                            LEFT JOIN resources identity_nc_resource
                              ON identity_nc_resource.resource_id = identity_nc.resource_id
                            LEFT JOIN surface_bindings identity_nc_binding
                              ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
                            LEFT JOIN token_lineages identity_nc_token_lineage
                              ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
                            LEFT JOIN primary_names_current pnc
                              ON pnc.address = requested.address
                             AND pnc.coin_type = requested.coin_type
                             AND pnc.namespace = anc.namespace
                             AND pnc.claim_status = 'success'
                            WHERE requested.roles IN ('owned', 'both')
                              AND anc.address = requested.address
                              AND anc.relation = 'token_holder'
                              AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            {DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER}
                        )
                        UNION ALL
                        (
                            SELECT
                                anc.logical_name_id,
                                anc.normalized_name,
                                anc.namespace,
                                anc.namehash,
                                1::SMALLINT AS role_rank,
                                FALSE AS is_primary
                            FROM address_names_current anc
                            JOIN name_surfaces surface
                              ON surface.logical_name_id = anc.logical_name_id
                            JOIN resources resource
                              ON resource.resource_id = anc.resource_id
                            JOIN surface_bindings binding
                              ON binding.surface_binding_id = anc.surface_binding_id
                            LEFT JOIN token_lineages token_lineage
                              ON token_lineage.token_lineage_id = anc.token_lineage_id
                            JOIN name_current identity_nc
                              ON identity_nc.logical_name_id = anc.logical_name_id
                            JOIN name_surfaces identity_nc_surface
                              ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
                            LEFT JOIN resources identity_nc_resource
                              ON identity_nc_resource.resource_id = identity_nc.resource_id
                            LEFT JOIN surface_bindings identity_nc_binding
                              ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
                            LEFT JOIN token_lineages identity_nc_token_lineage
                              ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
                            LEFT JOIN primary_names_current pnc
                              ON pnc.address = requested.address
                             AND pnc.coin_type = requested.coin_type
                             AND pnc.namespace = anc.namespace
                             AND pnc.claim_status = 'success'
                            WHERE requested.roles IN ('managed', 'both')
                              AND anc.address = requested.address
                              AND anc.relation = 'effective_controller'
                              AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            {DEFAULT_IDENTITY_NAME_CURRENT_READ_FILTER}
                        )
                    ) candidate
                    ORDER BY
                        candidate.logical_name_id ASC,
                        candidate.is_primary DESC,
                        candidate.role_rank ASC
                ) deduped
                WHERE
                    requested.cursor_is_primary IS NULL
                    OR (
                        (
                            NOT deduped.is_primary,
                            deduped.role_rank,
                            deduped.normalized_name,
                            deduped.namespace,
                            deduped.namehash
                        )
                        >
                        (
                            NOT requested.cursor_is_primary,
                            requested.cursor_role_rank,
                            requested.cursor_normalized_name,
                            requested.cursor_namespace,
                            requested.cursor_namehash
                        )
                    )
                ORDER BY
                    deduped.is_primary DESC,
                    deduped.role_rank ASC,
                    deduped.normalized_name ASC,
                    deduped.namespace ASC,
                    deduped.namehash ASC
                LIMIT requested.page_size + 1
            ) page
        )
        SELECT
            input_index,
            logical_name_id
        FROM page_rows
        ORDER BY
            input_index ASC,
            is_primary DESC,
            role_rank ASC,
            normalized_name ASC,
            namespace ASC,
            namehash ASC
        "#,
    ))
    .bind(&input_indexes)
    .bind(&addresses)
    .bind(&coin_types)
    .bind(&roles)
    .bind(&page_sizes)
    .bind(&cursor_is_primary)
    .bind(&cursor_role_rank)
    .bind(&cursor_normalized_names)
    .bind(&cursor_namespaces)
    .bind(&cursor_namehashes)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse identity page rows for {} inputs",
            inputs.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(ReverseIdentityPageRow {
                input_index: crate::sql_row::get::<i32>(&row, "input_index")? as usize,
                logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
            })
        })
        .collect()
}
