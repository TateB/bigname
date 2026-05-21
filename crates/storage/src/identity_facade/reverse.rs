use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use crate::primary_name::PrimaryNameClaimStatus;

use super::{
    IdentityNameRecordRow, IdentityPrimaryNameSnapshot, ReverseIdentityCursor,
    ReverseIdentityGroup, ReverseIdentityRecordRow, ReverseIdentityStorageInput,
    counts::load_reverse_identity_total_counts, dedupe_in_order,
    forward::load_identity_records_by_names, reverse_page::load_reverse_identity_page_rows,
    reverse_rows::ReverseIdentityPageRow,
};

pub async fn load_reverse_identity_records(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<Vec<ReverseIdentityGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let first_page_feed = inputs
        .iter()
        .all(|input| input.page_size == 1 && input.cursor.is_none());
    let counts_future = load_reverse_identity_total_counts(pool, inputs);
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

    let ((page_rows, name_records), primary_names, total_counts) =
        futures_util::try_join!(page_records_future, primary_names_future, counts_future)?;

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
            let mut entries = input_rows
                .into_iter()
                .filter_map(|row| {
                    reverse_identity_record(&name_records, &primary_names, input, row)
                })
                .collect::<Vec<_>>();
            let total_count = *total_counts
                .get(&(input.address.clone(), input.roles))
                .unwrap_or(&0);
            let has_more = if first_page_feed {
                total_count > input.page_size.max(0) as u64 && !entries.is_empty()
            } else {
                entries.len() as i64 > input.page_size
            };
            entries.truncate(input.page_size.max(0) as usize);

            ReverseIdentityGroup {
                input: input.clone(),
                entries,
                total_count: Some(total_count),
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
        requested_coin_type: input.coin_type.clone(),
    })
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
