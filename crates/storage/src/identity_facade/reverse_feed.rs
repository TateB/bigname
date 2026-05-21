use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use super::{ReverseIdentityFeedGroup, ReverseIdentityFeedInput, ReverseIdentityFeedRecordRow};

pub async fn load_reverse_identity_feed_records(
    pool: &PgPool,
    inputs: &[ReverseIdentityFeedInput],
) -> Result<Vec<ReverseIdentityFeedGroup>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

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
            COALESCE(counts.total_count, 0)::BIGINT AS total_count,
            COALESCE(primary_feed.logical_name_id, fallback_feed.logical_name_id) AS logical_name_id,
            COALESCE(primary_feed.namespace, fallback_feed.namespace) AS namespace,
            COALESCE(
                primary_feed.canonical_display_name,
                fallback_feed.canonical_display_name
            ) AS canonical_display_name,
            COALESCE(primary_feed.normalized_name, fallback_feed.normalized_name) AS normalized_name,
            COALESCE(primary_feed.namehash, fallback_feed.namehash) AS namehash,
            COALESCE(primary_feed.chain_positions, fallback_feed.chain_positions) AS chain_positions,
            COALESCE(primary_feed.coverage, fallback_feed.coverage) AS coverage,
            COALESCE(primary_feed.is_primary, fallback_feed.is_primary) AS is_primary,
            COALESCE(primary_feed.relation_facets, fallback_feed.relation_facets) AS relation_facets
        FROM requested
        LEFT JOIN address_names_current_identity_feed primary_feed
          ON primary_feed.address = requested.address
         AND primary_feed.roles = requested.roles
         AND primary_feed.coin_type = requested.coin_type
        LEFT JOIN address_names_current_identity_feed fallback_feed
          ON fallback_feed.address = requested.address
         AND fallback_feed.roles = requested.roles
         AND fallback_feed.coin_type = ''
        LEFT JOIN address_names_current_identity_counts counts
          ON counts.address = requested.address
         AND counts.roles = requested.roles
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
            "failed to load reverse identity compact feed rows for {} inputs",
            inputs.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let input_index = crate::sql_row::get::<i32>(&row, "input_index")? as usize;
            let input = inputs
                .get(input_index)
                .with_context(|| format!("compact feed row had invalid input index {input_index}"))?
                .clone();
            let total_count = crate::sql_row::get::<i64>(&row, "total_count")?;
            let logical_name_id = crate::sql_row::get::<Option<String>>(&row, "logical_name_id")?;
            let record = match logical_name_id {
                Some(logical_name_id) => {
                    let relation_values =
                        crate::sql_row::get::<Option<Vec<String>>>(&row, "relation_facets")?
                            .unwrap_or_default();
                    let relation_facets = relation_values
                        .iter()
                        .map(|value| parse_address_name_relation(value))
                        .collect::<Result<Vec<_>>>()?;

                    Some(ReverseIdentityFeedRecordRow {
                        logical_name_id,
                        namespace: required_feed_value(&row, "namespace")?,
                        canonical_display_name: required_feed_value(
                            &row,
                            "canonical_display_name",
                        )?,
                        normalized_name: required_feed_value(&row, "normalized_name")?,
                        namehash: required_feed_value(&row, "namehash")?,
                        chain_positions: required_feed_value(&row, "chain_positions")?,
                        coverage: required_feed_value(&row, "coverage")?,
                        is_primary: required_feed_value(&row, "is_primary")?,
                        relation_facets,
                    })
                }
                None => None,
            };

            Ok(ReverseIdentityFeedGroup {
                input,
                record,
                total_count: u64::try_from(total_count).unwrap_or(0),
            })
        })
        .collect()
}

fn required_feed_value<'r, T>(row: &'r sqlx::postgres::PgRow, column: &'static str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
    Option<T>: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    crate::sql_row::get::<Option<T>>(row, column)?
        .with_context(|| format!("compact feed row missing {column}"))
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
