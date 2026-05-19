use anyhow::{Context, Result};
use bigname_storage::{clear_address_names_current, upsert_address_names_current_rows};
use futures_util::{TryStreamExt, pin_mut};
use sqlx::PgPool;
use tokio::task::JoinSet;

use super::{
    AddressNamesCurrentRebuildSummary,
    cleanup::delete_stale_address_names_current_rows_for_address_keys,
    load::{load_current_bindings_for_address, stream_current_bindings},
    model::CurrentBindingSeed,
    projection::build_rows_for_binding,
    util::normalize_address,
};

const ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;
const ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY: usize = 8;

pub async fn rebuild_address_names_current(
    pool: &PgPool,
    address: Option<&str>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    match address {
        Some(address) => rebuild_one_address(pool, address).await,
        None => rebuild_all_addresses(pool).await,
    }
}

async fn rebuild_all_addresses(pool: &PgPool) -> Result<AddressNamesCurrentRebuildSummary> {
    let deleted_row_count = clear_address_names_current(pool).await?;
    let mut queued_binding_count = 0usize;
    let mut completed_binding_count = 0usize;
    let mut rows = Vec::with_capacity(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE);
    let mut upserted_row_count = 0usize;

    let bindings = stream_current_bindings(pool);
    pin_mut!(bindings);
    let mut tasks = JoinSet::new();

    while tasks.len() < ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY {
        let Some(binding) = bindings.try_next().await? else {
            break;
        };
        queued_binding_count += 1;
        spawn_address_names_rebuild_task(&mut tasks, pool, binding);
    }

    while let Some(result) = tasks.join_next().await {
        completed_binding_count += 1;
        let binding_rows = result??;
        rows.extend(binding_rows);

        if rows.len() >= ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count += upsert_address_names_current_rows(pool, &rows).await?.len();
            rows.clear();
        }

        if completed_binding_count % 5_000 == 0 {
            tracing::info!(
                projection = "address_names_current",
                queued_binding_count,
                completed_binding_count,
                upserted_row_count,
                "address_names_current rebuild bindings processed"
            );
        }

        while tasks.len() < ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY {
            let Some(binding) = bindings.try_next().await? else {
                break;
            };
            queued_binding_count += 1;
            spawn_address_names_rebuild_task(&mut tasks, pool, binding);
        }
    }

    if !rows.is_empty() {
        upserted_row_count += upsert_address_names_current_rows(pool, &rows).await?.len();
    }

    let requested_address_count = count_address_names_current_addresses(pool).await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count,
        upserted_row_count,
        deleted_row_count,
    })
}

fn spawn_address_names_rebuild_task(
    tasks: &mut JoinSet<Result<Vec<bigname_storage::AddressNameCurrentRow>>>,
    pool: &PgPool,
    binding: CurrentBindingSeed,
) {
    let pool = pool.clone();
    tasks.spawn(async move { build_rows_for_binding(&pool, &binding, None).await });
}

async fn rebuild_one_address(
    pool: &PgPool,
    address: &str,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let normalized_address = normalize_address(address);
    let bindings = load_current_bindings_for_address(pool, &normalized_address).await?;
    let mut replacement_keys = Vec::new();
    let mut rows = Vec::with_capacity(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE);
    let mut upserted_row_count = 0usize;

    for binding in &bindings {
        let binding_rows =
            build_rows_for_binding(pool, binding, Some(normalized_address.as_str())).await?;
        replacement_keys.extend(binding_rows.iter().map(|row| {
            (
                row.logical_name_id.clone(),
                row.relation.as_str().to_owned(),
            )
        }));
        rows.extend(binding_rows);

        if rows.len() >= ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count += upsert_address_names_current_rows(pool, &rows).await?.len();
            rows.clear();
        }
    }

    if !rows.is_empty() {
        upserted_row_count += upsert_address_names_current_rows(pool, &rows).await?.len();
    }

    let deleted_row_count = delete_stale_address_names_current_rows_for_address_keys(
        pool,
        &normalized_address,
        &replacement_keys,
    )
    .await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn count_address_names_current_addresses(pool: &PgPool) -> Result<usize> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT address)
        FROM address_names_current
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to count address_names_current rebuilt addresses")
    .map(|count| count as usize)
}
