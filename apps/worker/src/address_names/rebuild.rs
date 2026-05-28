use anyhow::{Context, Result};
use bigname_storage::{
    AddressNamesCurrentAddressReplacement, AddressNamesCurrentFullRebuild,
    begin_address_names_current_address_replacement, begin_address_names_current_full_rebuild,
    drop_address_names_current_address_replacement, drop_address_names_current_full_rebuild,
    insert_address_names_current_address_replacement_rows,
    insert_address_names_current_full_rebuild_rows,
    publish_address_names_current_address_replacement, publish_address_names_current_full_rebuild,
};
use futures_util::{TryStreamExt, pin_mut};
use sqlx::PgPool;
use tokio::task::JoinSet;

use super::{
    AddressNamesCurrentRebuildSummary,
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
    let rebuild = begin_address_names_current_full_rebuild(pool).await?;
    let deleted_row_count = rebuild.previous_row_count();
    tracing::info!(
        projection = "address_names_current",
        deleted_row_count,
        "address_names_current full rebuild staging started"
    );

    let staged = match stage_all_address_rows(pool, &rebuild).await {
        Ok(staged) => staged,
        Err(error) => {
            if let Err(drop_error) = drop_address_names_current_full_rebuild(pool, &rebuild).await {
                tracing::warn!(
                    projection = "address_names_current",
                    error = %drop_error,
                    "failed to drop address_names_current full rebuild staging table after error"
                );
            }
            return Err(error);
        }
    };

    let (_deleted_row_count, published_row_count) =
        match publish_address_names_current_full_rebuild(pool, &rebuild).await {
            Ok(published) => published,
            Err(error) => {
                if let Err(drop_error) =
                    drop_address_names_current_full_rebuild(pool, &rebuild).await
                {
                    tracing::warn!(
                        projection = "address_names_current",
                        error = %drop_error,
                        "failed to drop address_names_current full rebuild staging table after publish error"
                    );
                }
                return Err(error);
            }
        };
    if let Err(error) = drop_address_names_current_full_rebuild(pool, &rebuild).await {
        tracing::warn!(
            projection = "address_names_current",
            error = %error,
            "failed to drop address_names_current full rebuild staging table after publish"
        );
    }
    tracing::info!(
        projection = "address_names_current",
        upserted_row_count = staged.upserted_row_count,
        published_row_count,
        "address_names_current full rebuild published projection and refreshed identity sidecars"
    );

    let requested_address_count = count_address_names_current_addresses(pool).await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count,
        upserted_row_count: staged.upserted_row_count,
        deleted_row_count,
    })
}

struct AddressNamesCurrentStagingSummary {
    upserted_row_count: usize,
}

async fn stage_all_address_rows(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
) -> Result<AddressNamesCurrentStagingSummary> {
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
            upserted_row_count +=
                insert_address_names_current_full_rebuild_rows(pool, rebuild, &rows)
                    .await?
                    .len();
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
        upserted_row_count += insert_address_names_current_full_rebuild_rows(pool, rebuild, &rows)
            .await?
            .len();
    }

    tracing::info!(
        projection = "address_names_current",
        upserted_row_count,
        "address_names_current full rebuild staged projection rows"
    );

    Ok(AddressNamesCurrentStagingSummary { upserted_row_count })
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
    let replacement =
        begin_address_names_current_address_replacement(pool, &normalized_address).await?;

    let staged = match stage_one_address_rows(
        pool,
        &replacement,
        &normalized_address,
        bindings.as_slice(),
    )
    .await
    {
        Ok(staged) => staged,
        Err(error) => {
            if let Err(drop_error) =
                drop_address_names_current_address_replacement(pool, &replacement).await
            {
                tracing::warn!(
                    projection = "address_names_current",
                    address = %normalized_address,
                    error = %drop_error,
                    "failed to drop address_names_current address replacement staging table after error"
                );
            }
            return Err(error);
        }
    };

    let (deleted_row_count, published_row_count) =
        match publish_address_names_current_address_replacement(pool, &replacement).await {
            Ok(summary) => summary,
            Err(error) => {
                if let Err(drop_error) =
                    drop_address_names_current_address_replacement(pool, &replacement).await
                {
                    tracing::warn!(
                        projection = "address_names_current",
                        address = %normalized_address,
                        error = %drop_error,
                        "failed to drop address_names_current address replacement staging table after publish error"
                    );
                }
                return Err(error);
            }
        };

    if let Err(error) = drop_address_names_current_address_replacement(pool, &replacement).await {
        tracing::warn!(
            projection = "address_names_current",
            address = %normalized_address,
            error = %error,
            "failed to drop address_names_current address replacement staging table after publish"
        );
    }

    tracing::info!(
        projection = "address_names_current",
        address = %normalized_address,
        upserted_row_count = staged.upserted_row_count,
        published_row_count,
        deleted_row_count,
        "address_names_current address replacement published projection and refreshed identity sidecars"
    );

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count: 1,
        upserted_row_count: staged.upserted_row_count,
        deleted_row_count,
    })
}

async fn stage_one_address_rows(
    pool: &PgPool,
    replacement: &AddressNamesCurrentAddressReplacement,
    normalized_address: &str,
    bindings: &[CurrentBindingSeed],
) -> Result<AddressNamesCurrentStagingSummary> {
    let mut rows = Vec::with_capacity(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE);
    let mut upserted_row_count = 0usize;

    for binding in bindings {
        let binding_rows = build_rows_for_binding(pool, binding, Some(normalized_address)).await?;
        rows.extend(binding_rows);

        if rows.len() >= ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count +=
                insert_address_names_current_address_replacement_rows(pool, replacement, &rows)
                    .await?
                    .len();
            rows.clear();
        }
    }

    if !rows.is_empty() {
        upserted_row_count +=
            insert_address_names_current_address_replacement_rows(pool, replacement, &rows)
                .await?
                .len();
    }

    Ok(AddressNamesCurrentStagingSummary { upserted_row_count })
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
