use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    EventHistoryReadFilter, HistoryCursor, HistoryEvent, HistoryPage, HistorySummaryMode,
    InvalidHistoryCursor, decoders::decode_history_event, selectors::HistorySelector,
    summary::load_history_summary,
};
use crate::projection_helpers::{
    checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
};

pub(super) async fn load_history(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        false,
    )
    .await
}

pub(super) async fn load_history_head(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    let mut rows = load_history_internal(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        true,
    )
    .await?;
    Ok(rows.drain(..).next())
}

pub(super) async fn load_event_history_rows(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(pool, filter, canonical_only, false).await
}

pub(super) async fn load_history_page(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
) -> Result<HistoryPage> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin normalized-event history page transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure normalized-event history page transaction")?;

    if let Some(cursor) = cursor {
        ensure_history_cursor_exists(&mut transaction, &filter, canonical_only, cursor).await?;
    }

    let summary =
        load_history_summary(&mut transaction, &filter, canonical_only, summary_mode).await?;

    if filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
    {
        transaction
            .commit()
            .await
            .context("failed to commit normalized-event history page transaction")?;
        return Ok(HistoryPage {
            rows: Vec::new(),
            next_cursor: None,
            summary,
        });
    }

    let page_size = checked_page_size_usize(
        page_size,
        "history page_size must be positive",
        "history page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "history page_size is too large",
        "history page_size exceeds SQL limit",
    )?;

    let mut builder = QueryBuilder::<Postgres>::new("");
    if let Some(cursor) = cursor {
        push_history_cursor_cte(&mut builder, cursor);
    }
    push_history_select(&mut builder, cursor.is_some());
    push_history_filters(&mut builder, &filter, canonical_only);

    if cursor.is_some() {
        builder.push(" AND ");
        push_history_cursor_after(&mut builder);
    }

    push_history_order(&mut builder);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder
        .build()
        .fetch_all(&mut *transaction)
        .await
        .context("failed to fetch normalized-event history page")?;
    let rows = rows
        .into_iter()
        .map(decode_history_event)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, history_cursor_from_row);

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event history page transaction")?;

    Ok(HistoryPage {
        rows,
        next_cursor,
        summary,
    })
}

async fn load_history_internal(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    head_only: bool,
) -> Result<Vec<HistoryEvent>> {
    if filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
    {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_select(&mut builder, false);
    push_history_filters(&mut builder, &filter, canonical_only);
    push_history_order(&mut builder);

    if head_only {
        builder.push(" LIMIT 1");
    }

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized-event history rows")?;

    rows.into_iter().map(decode_history_event).collect()
}

fn push_history_select(builder: &mut QueryBuilder<'_, Postgres>, include_cursor_row: bool) {
    builder.push(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        FROM normalized_events ne
        "#,
    );
    if include_cursor_row {
        builder.push(" CROSS JOIN history_cursor_row cursor_row ");
    }
    builder.push(
        r#"
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE TRUE
        "#,
    );
}

pub(super) fn push_history_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    for selector in &filter.selectors {
        builder.push(" AND ");
        push_selector_filter(builder, selector);
    }

    if let Some(namespace) = filter.namespace.as_ref() {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    if !filter.event_kinds.is_empty() {
        builder.push(" AND ");
        push_string_filter(builder, "ne.event_kind", &filter.event_kinds);
    }

    if let Some(from_block) = filter.from_block {
        builder.push(" AND ne.block_number >= ");
        builder.push_bind(from_block);
    }

    if let Some(to_block) = filter.to_block {
        builder.push(" AND ne.block_number <= ");
        builder.push_bind(to_block);
    }

    if canonical_only {
        builder.push(
            r#"
            AND ne.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            "#,
        );
    }
}

fn push_history_order(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(" ORDER BY ");
    push_history_order_terms(builder);
}

pub(super) fn push_history_order_terms(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
            ne.block_number DESC NULLS LAST,
            ne.chain_id ASC NULLS LAST,
            ne.block_hash DESC NULLS LAST,
            ne.transaction_hash DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.event_identity DESC
        "#,
    );
}

async fn ensure_history_cursor_exists(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    cursor: &HistoryCursor,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM normalized_events ne
            WHERE TRUE
        "#,
    );
    push_history_filters(&mut builder, filter, canonical_only);
    builder.push(" AND ne.normalized_event_id = ");
    builder.push_bind(cursor.normalized_event_id);
    builder.push(" AND ne.event_identity = ");
    builder.push_bind(&cursor.event_identity);
    builder.push(" LIMIT 1)");

    let exists = builder
        .build_query_scalar::<bool>()
        .fetch_one(&mut *connection)
        .await
        .context("failed to validate normalized-event history cursor")?;

    if exists {
        Ok(())
    } else {
        Err(InvalidHistoryCursor.into())
    }
}

fn push_history_cursor_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a HistoryCursor,
) {
    builder.push(
        r#"
        WITH history_cursor_row AS (
            SELECT
                block_number,
                chain_id,
                block_hash,
                transaction_hash,
                log_index,
                event_identity
            FROM normalized_events
            WHERE normalized_event_id =
        "#,
    );
    builder.push_bind(cursor.normalized_event_id);
    builder.push(" AND event_identity = ");
    builder.push_bind(&cursor.event_identity);
    builder.push(") ");
}

fn push_history_cursor_after(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
        (
            CASE WHEN ne.block_number IS NULL THEN 1 ELSE 0 END
                > CASE WHEN cursor_row.block_number IS NULL THEN 1 ELSE 0 END
            OR (
                CASE WHEN ne.block_number IS NULL THEN 1 ELSE 0 END
                    = CASE WHEN cursor_row.block_number IS NULL THEN 1 ELSE 0 END
                AND (
                    ne.block_number < cursor_row.block_number
                    OR (
                        ne.block_number IS NOT DISTINCT FROM cursor_row.block_number
                        AND (
                            CASE WHEN ne.chain_id IS NULL THEN 1 ELSE 0 END
                                > CASE WHEN cursor_row.chain_id IS NULL THEN 1 ELSE 0 END
                            OR (
                                CASE WHEN ne.chain_id IS NULL THEN 1 ELSE 0 END
                                    = CASE WHEN cursor_row.chain_id IS NULL THEN 1 ELSE 0 END
                                AND (
                                    ne.chain_id > cursor_row.chain_id
                                    OR (
                                        ne.chain_id IS NOT DISTINCT FROM cursor_row.chain_id
                                        AND (
                                            CASE WHEN ne.block_hash IS NULL THEN 1 ELSE 0 END
                                                > CASE WHEN cursor_row.block_hash IS NULL THEN 1 ELSE 0 END
                                            OR (
                                                CASE WHEN ne.block_hash IS NULL THEN 1 ELSE 0 END
                                                    = CASE WHEN cursor_row.block_hash IS NULL THEN 1 ELSE 0 END
                                                AND (
                                                    ne.block_hash < cursor_row.block_hash
                                                    OR (
                                                        ne.block_hash IS NOT DISTINCT FROM cursor_row.block_hash
                                                        AND (
                                                            CASE WHEN ne.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                > CASE WHEN cursor_row.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                            OR (
                                                                CASE WHEN ne.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                    = CASE WHEN cursor_row.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                AND (
                                                                    ne.transaction_hash < cursor_row.transaction_hash
                                                                    OR (
                                                                        ne.transaction_hash IS NOT DISTINCT FROM cursor_row.transaction_hash
                                                                        AND (
                                                                            COALESCE(ne.log_index, -1) < COALESCE(cursor_row.log_index, -1)
                                                                            OR (
                                                                                COALESCE(ne.log_index, -1) = COALESCE(cursor_row.log_index, -1)
                                                                                AND ne.event_identity < cursor_row.event_identity
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
        "#,
    );
}

fn history_cursor_from_row(row: &HistoryEvent) -> HistoryCursor {
    HistoryCursor {
        normalized_event_id: row.normalized_event_id,
        event_identity: row.event_identity.clone(),
    }
}

fn push_selector_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    selector: &'a HistorySelector,
) {
    match selector {
        HistorySelector::LogicalNames(logical_name_ids) => {
            push_string_filter(builder, "ne.logical_name_id", logical_name_ids);
        }
        HistorySelector::Resources(resource_ids) => {
            push_uuid_filter(builder, "ne.resource_id", resource_ids);
        }
        HistorySelector::LogicalNamesOrResources {
            logical_name_ids,
            resource_ids,
        } => {
            builder.push("(");
            push_string_filter(builder, "ne.logical_name_id", logical_name_ids);
            builder.push(" OR ");
            push_uuid_filter(builder, "ne.resource_id", resource_ids);
            builder.push(")");
        }
        HistorySelector::None => {
            builder.push("FALSE");
        }
    }
}

fn push_string_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [String],
) {
    builder.push(column);
    push_string_filter_tail(builder, values);
}

fn push_string_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [String]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}

fn push_uuid_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [Uuid],
) {
    builder.push(column);
    push_uuid_filter_tail(builder, values);
}

fn push_uuid_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [Uuid]) {
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
}
