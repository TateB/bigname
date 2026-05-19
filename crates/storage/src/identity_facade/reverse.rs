use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use crate::primary_name::PrimaryNameClaimStatus;

use super::{
    DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER, IdentityNameRecordRow, IdentityPrimaryNameSnapshot,
    ReverseIdentityCursor, ReverseIdentityGroup, ReverseIdentityRecordRow,
    ReverseIdentityStorageInput, dedupe_in_order, forward::load_identity_records_by_names,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseIdentityPageRow {
    input_index: usize,
    logical_name_id: String,
}

pub async fn load_reverse_identity_records(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let primary_names_future = load_identity_primary_name_snapshots(pool, inputs);
    let page_records_future = async {
        let page_rows = load_reverse_identity_page_rows(pool, inputs).await?;
        let logical_name_ids =
            dedupe_in_order(page_rows.iter().map(|row| row.logical_name_id.clone()));
        let name_records = load_identity_records_by_names(pool, &logical_name_ids)
            .await?
            .into_iter()
            .map(|record| (record.row.logical_name_id.clone(), record))
            .collect::<BTreeMap<_, _>>();

        Result::<_>::Ok((page_rows, name_records))
    };

    let ((page_rows, name_records), primary_names) =
        futures_util::try_join!(page_records_future, primary_names_future)?;

    let rows_by_input = page_rows.into_iter().fold(
        BTreeMap::<usize, Vec<ReverseIdentityPageRow>>::new(),
        |mut grouped, row| {
            grouped.entry(row.input_index).or_default().push(row);
            grouped
        },
    );

    let groups = inputs
        .iter()
        .enumerate()
        .map(|(input_index, input)| {
            let input_rows = rows_by_input.get(&input_index).cloned().unwrap_or_default();
            let has_more = input_rows.len() as i64 > input.page_size;
            let entries = input_rows
                .into_iter()
                .take(input.page_size.max(0) as usize)
                .filter_map(|row| {
                    reverse_identity_record(&name_records, &primary_names, input, row)
                })
                .collect();

            ReverseIdentityGroup {
                input: input.clone(),
                entries,
                total_count: None,
                has_more,
            }
        })
        .collect();

    Ok(groups)
}

fn reverse_identity_record(
    name_records: &BTreeMap<String, IdentityNameRecordRow>,
    primary_names: &BTreeMap<(String, String, String), IdentityPrimaryNameSnapshot>,
    input: &ReverseIdentityStorageInput,
    row: ReverseIdentityPageRow,
) -> Option<ReverseIdentityRecordRow> {
    let name_record = name_records.get(&row.logical_name_id)?.clone();
    let primary_name = primary_names
        .get(&(
            input.address.clone(),
            name_record.row.namespace.clone(),
            input.coin_type.clone(),
        ))
        .cloned();
    let mut relation_facets = name_record
        .relations
        .iter()
        .filter(|relation| {
            relation.address == input.address && input.roles.includes(relation.relation)
        })
        .map(|relation| relation.relation)
        .collect::<Vec<_>>();
    relation_facets.sort();
    relation_facets.dedup();

    Some(ReverseIdentityRecordRow {
        name_record,
        relation_facets,
        primary_name,
    })
}

async fn load_reverse_identity_page_rows(
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
                              AND (
                                  requested.cursor_is_primary IS NULL
                                  OR (
                                      (
                                          FALSE,
                                          CASE
                                              WHEN anc.relation IN ('registrant', 'token_holder') THEN 0::SMALLINT
                                              ELSE 1::SMALLINT
                                          END,
                                          anc.normalized_name,
                                          anc.namespace,
                                          anc.namehash
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
                              )
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            ORDER BY role_rank ASC, anc.normalized_name ASC, anc.namespace ASC, anc.namehash ASC
                            LIMIT 1
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
                            LEFT JOIN primary_names_current pnc
                              ON pnc.address = requested.address
                             AND pnc.coin_type = requested.coin_type
                             AND pnc.namespace = anc.namespace
                             AND pnc.claim_status = 'success'
                            WHERE requested.roles IN ('owned', 'both')
                              AND anc.address = requested.address
                              AND anc.relation = 'registrant'
                              AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                              AND (
                                  requested.cursor_is_primary IS NULL
                                  OR (
                                      (TRUE, 0::SMALLINT, anc.normalized_name, anc.namespace, anc.namehash)
                                      >
                                      (
                                          NOT requested.cursor_is_primary,
                                          requested.cursor_role_rank,
                                          requested.cursor_normalized_name,
                                          requested.cursor_namespace,
                                          requested.cursor_namehash
                                      )
                                  )
                              )
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            ORDER BY anc.normalized_name ASC, anc.namespace ASC, anc.namehash ASC, anc.logical_name_id ASC
                            LIMIT requested.page_size + 1
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
                            LEFT JOIN primary_names_current pnc
                              ON pnc.address = requested.address
                             AND pnc.coin_type = requested.coin_type
                             AND pnc.namespace = anc.namespace
                             AND pnc.claim_status = 'success'
                            WHERE requested.roles IN ('owned', 'both')
                              AND anc.address = requested.address
                              AND anc.relation = 'token_holder'
                              AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                              AND (
                                  requested.cursor_is_primary IS NULL
                                  OR (
                                      (TRUE, 0::SMALLINT, anc.normalized_name, anc.namespace, anc.namehash)
                                      >
                                      (
                                          NOT requested.cursor_is_primary,
                                          requested.cursor_role_rank,
                                          requested.cursor_normalized_name,
                                          requested.cursor_namespace,
                                          requested.cursor_namehash
                                      )
                                  )
                              )
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            ORDER BY anc.normalized_name ASC, anc.namespace ASC, anc.namehash ASC, anc.logical_name_id ASC
                            LIMIT requested.page_size + 1
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
                            LEFT JOIN primary_names_current pnc
                              ON pnc.address = requested.address
                             AND pnc.coin_type = requested.coin_type
                             AND pnc.namespace = anc.namespace
                             AND pnc.claim_status = 'success'
                            WHERE requested.roles IN ('managed', 'both')
                              AND anc.address = requested.address
                              AND anc.relation = 'effective_controller'
                              AND pnc.normalized_claim_name IS DISTINCT FROM anc.normalized_name
                              AND (
                                  requested.cursor_is_primary IS NULL
                                  OR (
                                      (TRUE, 1::SMALLINT, anc.normalized_name, anc.namespace, anc.namehash)
                                      >
                                      (
                                          NOT requested.cursor_is_primary,
                                          requested.cursor_role_rank,
                                          requested.cursor_normalized_name,
                                          requested.cursor_namespace,
                                          requested.cursor_namehash
                                      )
                                  )
                              )
                            {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                            ORDER BY anc.normalized_name ASC, anc.namespace ASC, anc.namehash ASC, anc.logical_name_id ASC
                            LIMIT requested.page_size + 1
                        )
                    ) candidate
                    ORDER BY
                        candidate.logical_name_id ASC,
                        candidate.is_primary DESC,
                        candidate.role_rank ASC
                ) deduped
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

async fn load_identity_primary_name_snapshots(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<BTreeMap<(String, String, String), IdentityPrimaryNameSnapshot>> {
    let addresses = dedupe_in_order(inputs.iter().map(|input| input.address.clone()));
    let coin_types = dedupe_in_order(inputs.iter().map(|input| input.coin_type.clone()));
    if addresses.is_empty() || coin_types.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            address,
            namespace,
            coin_type,
            claim_status,
            normalized_claim_name
        FROM primary_names_current
        WHERE address = ANY($1::TEXT[])
          AND coin_type = ANY($2::TEXT[])
        ORDER BY address, namespace, coin_type
        "#,
    )
    .bind(&addresses)
    .bind(&coin_types)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to batch load primary_names_current snapshots for {} addresses and {} coin types",
            addresses.len(),
            coin_types.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let address = crate::sql_row::get::<String>(&row, "address")?.to_ascii_lowercase();
            let namespace = crate::sql_row::get::<String>(&row, "namespace")?;
            let coin_type = crate::sql_row::get::<String>(&row, "coin_type")?;
            let claim_status = parse_primary_name_claim_status(&crate::sql_row::get::<String>(
                &row,
                "claim_status",
            )?)?;
            let snapshot = IdentityPrimaryNameSnapshot {
                address: address.clone(),
                namespace: namespace.clone(),
                coin_type: coin_type.clone(),
                claim_status,
                normalized_claim_name: crate::sql_row::get(&row, "normalized_claim_name")?,
            };
            Ok(((address, namespace, coin_type), snapshot))
        })
        .collect()
}

fn parse_primary_name_claim_status(value: &str) -> Result<PrimaryNameClaimStatus> {
    match value {
        "success" => Ok(PrimaryNameClaimStatus::Success),
        "not_found" => Ok(PrimaryNameClaimStatus::NotFound),
        "unsupported" => Ok(PrimaryNameClaimStatus::Unsupported),
        "invalid_name" => Ok(PrimaryNameClaimStatus::InvalidName),
        _ => bail!("unknown identity primary-name status {value}"),
    }
}

#[allow(dead_code)]
fn _cursor_type_guard(_: &ReverseIdentityCursor) {}
