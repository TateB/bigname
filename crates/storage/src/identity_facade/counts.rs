use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{ReverseIdentityRoles, ReverseIdentityStorageInput};

pub(super) async fn load_reverse_identity_total_counts(
    pool: &PgPool,
    inputs: &[ReverseIdentityStorageInput],
) -> Result<BTreeMap<(String, ReverseIdentityRoles), u64>> {
    let requests = inputs
        .iter()
        .map(|input| (input.address.clone(), input.roles))
        .collect::<BTreeSet<_>>();
    if requests.is_empty() {
        return Ok(BTreeMap::new());
    }

    let addresses = requests
        .iter()
        .map(|(address, _)| address.clone())
        .collect::<Vec<_>>();
    let roles = requests
        .iter()
        .map(|(_, roles)| roles.storage_value().to_owned())
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        WITH requested AS (
            SELECT *
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS requested(address, roles)
        )
        SELECT
            requested.address,
            requested.roles,
            COALESCE(counts.total_count, 0)::BIGINT AS total_count
        FROM requested
        LEFT JOIN address_names_current_identity_counts counts
          ON counts.address = requested.address
         AND counts.roles = requested.roles
        "#,
    )
    .bind(&addresses)
    .bind(&roles)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load reverse identity total counts for {} inputs",
            inputs.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let address = crate::sql_row::get::<String>(&row, "address")?;
            let roles = parse_count_roles(&crate::sql_row::get::<String>(&row, "roles")?)?;
            let total_count = crate::sql_row::get::<i64>(&row, "total_count")?;
            Ok(((address, roles), u64::try_from(total_count).unwrap_or(0)))
        })
        .collect()
}

fn parse_count_roles(value: &str) -> Result<ReverseIdentityRoles> {
    match value {
        "owned" => Ok(ReverseIdentityRoles::Owned),
        "managed" => Ok(ReverseIdentityRoles::Managed),
        _ => Ok(ReverseIdentityRoles::Both),
    }
}
