use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use super::{
    ReverseIdentityFeedGroup, ReverseIdentityFeedInput, ReverseIdentityFeedRecordRow,
    ReverseIdentityStorageInput, counts::load_reverse_identity_total_counts,
    reverse_rows::ReverseIdentityFirstPageRow,
};

pub async fn load_reverse_identity_feed_records(
    pool: &PgPool,
    inputs: &[ReverseIdentityFeedInput],
) -> Result<Vec<ReverseIdentityFeedGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let storage_inputs = inputs
        .iter()
        .map(|input| ReverseIdentityStorageInput {
            address: input.address.clone(),
            coin_type: input.coin_type.clone(),
            roles: input.roles,
            page_size: 1,
            cursor: None,
        })
        .collect::<Vec<_>>();
    let counts_future = load_reverse_identity_total_counts(pool, &storage_inputs);
    let rows_future = load_reverse_identity_first_page_rows(pool, &storage_inputs);
    let (page_rows, total_counts) = futures_util::try_join!(rows_future, counts_future)?;
    let rows_by_input = page_rows
        .into_iter()
        .map(|row| (row.input_index, row))
        .collect::<BTreeMap<_, _>>();

    let groups = inputs
        .iter()
        .enumerate()
        .map(|(input_index, input)| {
            let record = rows_by_input
                .get(&input_index)
                .map(|row| ReverseIdentityFeedRecordRow {
                    logical_name_id: row.logical_name_id.clone(),
                    namespace: row.namespace.clone(),
                    canonical_display_name: row.canonical_display_name.clone(),
                    normalized_name: row.normalized_name.clone(),
                    namehash: row.namehash.clone(),
                    chain_positions: row.chain_positions.clone(),
                    coverage: row.coverage.clone(),
                    is_primary: row.is_primary,
                    relation_facets: row.relation_facets.clone(),
                });
            ReverseIdentityFeedGroup {
                input: input.clone(),
                record,
                total_count: Some(
                    *total_counts
                        .get(&(input.address.clone(), input.roles))
                        .unwrap_or(&0),
                ),
            }
        })
        .collect();

    Ok(groups)
}

pub(super) async fn load_reverse_identity_first_page_rows(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityFirstPageRow>> {
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

    let rows = sqlx::query(
        r#"
        WITH requested AS (
            SELECT *
            FROM UNNEST($1::INT[], $2::TEXT[], $3::TEXT[], $4::TEXT[])
              AS requested(input_index, address, coin_type, roles)
        )
        SELECT
            requested.input_index,
            page.logical_name_id,
            page.namespace,
            page.canonical_display_name,
            page.normalized_name,
            page.namehash,
            page.chain_positions,
            page.coverage,
            page.is_primary,
            ARRAY(
                SELECT facet.relation
                FROM address_names_current facet
                WHERE facet.address = requested.address
                  AND facet.logical_name_id = page.logical_name_id
                  AND (
                      requested.roles = 'both'
                      OR (
                          requested.roles = 'owned'
                          AND facet.relation IN ('registrant', 'token_holder')
                      )
                      OR (
                          requested.roles = 'managed'
                          AND facet.relation = 'effective_controller'
                      )
                  )
                ORDER BY
                    CASE
                        WHEN facet.relation = 'registrant' THEN 0
                        WHEN facet.relation = 'token_holder' THEN 1
                        ELSE 2
                    END
            ) AS relation_facets
        FROM requested
        CROSS JOIN LATERAL (
            SELECT
                candidate.logical_name_id,
                candidate.namespace,
                candidate.canonical_display_name,
                candidate.normalized_name,
                candidate.namehash,
                '{}'::JSONB AS chain_positions,
                '{}'::JSONB AS coverage,
                candidate.is_primary
            FROM (
                (
                    SELECT
                        anc.logical_name_id,
                        anc.namespace,
                        anc.canonical_display_name,
                        TRUE AS is_primary,
                        CASE
                            WHEN anc.relation IN ('registrant', 'token_holder') THEN 0::SMALLINT
                            ELSE 1::SMALLINT
                        END AS role_rank,
                        anc.normalized_name,
                        anc.namehash,
                        anc.relation
                    FROM primary_names_current pnc
                    JOIN address_names_current anc
                      ON anc.address = requested.address
                     AND anc.namespace = pnc.namespace
                     AND anc.normalized_name = pnc.normalized_claim_name
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
                    ORDER BY
                        CASE
                            WHEN anc.relation IN ('registrant', 'token_holder') THEN 0
                            ELSE 1
                        END,
                        anc.normalized_name ASC,
                        anc.namespace ASC,
                        anc.namehash ASC,
                        anc.logical_name_id ASC
                    LIMIT 1
                )
                UNION ALL
                (
                    SELECT
                        anc.logical_name_id,
                        anc.namespace,
                        anc.canonical_display_name,
                        FALSE AS is_primary,
                        CASE
                            WHEN anc.relation IN ('registrant', 'token_holder') THEN 0::SMALLINT
                            ELSE 1::SMALLINT
                        END AS role_rank,
                        anc.normalized_name,
                        anc.namehash,
                        anc.relation
                    FROM address_names_current anc
                    LEFT JOIN primary_names_current pnc
                      ON pnc.address = requested.address
                     AND pnc.coin_type = requested.coin_type
                     AND pnc.namespace = anc.namespace
                     AND pnc.claim_status = 'success'
                    WHERE anc.address = requested.address
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
                      AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                    ORDER BY
                        CASE
                            WHEN anc.relation IN ('registrant', 'token_holder') THEN 0
                            ELSE 1
                        END,
                        anc.normalized_name ASC,
                        anc.namespace ASC,
                        anc.namehash ASC,
                        anc.logical_name_id ASC
                    LIMIT 1
                )
            ) candidate
            ORDER BY
                candidate.is_primary DESC,
                candidate.role_rank ASC,
                candidate.normalized_name ASC,
                candidate.namespace ASC,
                candidate.namehash ASC
            LIMIT 1
        ) page
        ORDER BY requested.input_index ASC
        "#,
    )
    .bind(&input_indexes)
    .bind(&addresses)
    .bind(&coin_types)
    .bind(&roles)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse identity first-page feed rows for {} inputs",
            inputs.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let relation_values = crate::sql_row::get::<Vec<String>>(&row, "relation_facets")?;
            let relation_facets = relation_values
                .iter()
                .map(|value| parse_address_name_relation(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(ReverseIdentityFirstPageRow {
                input_index: crate::sql_row::get::<i32>(&row, "input_index")? as usize,
                logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
                namespace: crate::sql_row::get(&row, "namespace")?,
                canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
                normalized_name: crate::sql_row::get(&row, "normalized_name")?,
                namehash: crate::sql_row::get(&row, "namehash")?,
                chain_positions: crate::sql_row::get(&row, "chain_positions")?,
                coverage: crate::sql_row::get(&row, "coverage")?,
                is_primary: crate::sql_row::get(&row, "is_primary")?,
                relation_facets,
            })
        })
        .collect()
}

fn parse_address_name_relation(value: &str) -> Result<crate::address_names::AddressNameRelation> {
    match value {
        "registrant" => Ok(crate::address_names::AddressNameRelation::Registrant),
        "token_holder" => Ok(crate::address_names::AddressNameRelation::TokenHolder),
        "effective_controller" => {
            Ok(crate::address_names::AddressNameRelation::EffectiveController)
        }
        _ => bail!("unknown identity address-name relation {value}"),
    }
}
