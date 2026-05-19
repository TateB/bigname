use anyhow::{Context, Result};
use bigname_storage::delete_address_names_current;
use sqlx::PgPool;

pub(super) async fn delete_stale_address_names_current_rows_for_address_keys(
    pool: &PgPool,
    address: &str,
    replacement_keys: &[(String, String)],
) -> Result<u64> {
    if replacement_keys.is_empty() {
        return delete_address_names_current(pool, address).await;
    }

    let logical_name_ids = replacement_keys
        .iter()
        .map(|(logical_name_id, _)| logical_name_id.clone())
        .collect::<Vec<_>>();
    let relations = replacement_keys
        .iter()
        .map(|(_, relation)| relation.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM address_names_current current
        WHERE current.address = $1
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($2::TEXT[], $3::TEXT[]) AS replacement(logical_name_id, relation)
            WHERE replacement.logical_name_id = current.logical_name_id
              AND replacement.relation = current.relation
          )
        "#,
    )
    .bind(address)
    .bind(&logical_name_ids)
    .bind(&relations)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete stale address_names_current rows for address {address}")
    })
    .map(|result| result.rows_affected())
}
