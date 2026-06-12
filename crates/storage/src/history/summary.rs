use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, HistoryChainPositionSample, HistorySummary, HistorySummaryMode,
    paging::{push_history_filters, push_history_order_terms},
};

pub(super) async fn load_history_summary(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    mode: HistorySummaryMode,
) -> Result<Option<HistorySummary>> {
    match mode {
        HistorySummaryMode::None => Ok(None),
        HistorySummaryMode::Count => {
            let total_count = load_history_total_count(connection, filter, canonical_only).await?;
            Ok(Some(HistorySummary {
                total_count,
                normalized_event_ids: Vec::new(),
                raw_fact_refs: Vec::new(),
                manifest_versions: Vec::new(),
                execution_trace_id: None,
                chain_position_samples: Vec::new(),
                last_updated: None,
            }))
        }
        HistorySummaryMode::Full => {
            let mut summary = load_history_full_summary(connection, filter, canonical_only).await?;
            summary.chain_position_samples =
                load_history_chain_position_samples(connection, filter, canonical_only).await?;
            Ok(Some(summary))
        }
    }
}

async fn load_history_total_count(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<u64> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT COUNT(*)::BIGINT AS total_count
        FROM normalized_events ne
        WHERE TRUE
        "#,
    );
    push_history_filters(&mut builder, filter, canonical_only);

    let total_count = builder
        .build_query_scalar::<i64>()
        .fetch_one(&mut *connection)
        .await
        .context("failed to count normalized-event history rows")?;
    u64::try_from(total_count).context("negative normalized-event history total_count")
}

async fn load_history_full_summary(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<HistorySummary> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            COUNT(*)::BIGINT AS total_count,
            COALESCE(
                jsonb_agg(to_jsonb(ne.normalized_event_id::TEXT) ORDER BY
        "#,
    );
    push_history_order_terms(&mut builder);
    builder.push(
        r#"
                ) FILTER (WHERE ne.normalized_event_id IS NOT NULL),
                '[]'::jsonb
            ) AS normalized_event_ids,
            COALESCE(
                jsonb_agg(ne.raw_fact_ref ORDER BY
        "#,
    );
    push_history_order_terms(&mut builder);
    builder.push(
        r#"
                ) FILTER (WHERE ne.raw_fact_ref IS NOT NULL),
                '[]'::jsonb
            ) AS raw_fact_refs,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'manifest_version', ne.manifest_version,
                        'source_family', ne.source_family,
                        'source_manifest_id', ne.source_manifest_id
                    )
                    ORDER BY
        "#,
    );
    push_history_order_terms(&mut builder);
    builder.push(
        r#"
                ) FILTER (WHERE ne.normalized_event_id IS NOT NULL),
                '[]'::jsonb
            ) AS manifest_versions,
            (
                array_agg(
                    COALESCE(
                        CASE
                            WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                                THEN ne.after_state -> 'provenance' ->> 'execution_trace_id'
                        END,
                        CASE
                            WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                                THEN ne.before_state -> 'provenance' ->> 'execution_trace_id'
                        END
                    )
                    ORDER BY
        "#,
    );
    push_history_order_terms(&mut builder);
    builder.push(
        r#"
                ) FILTER (
                    WHERE COALESCE(
                        CASE
                            WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                                THEN ne.after_state -> 'provenance' ->> 'execution_trace_id'
                        END,
                        CASE
                            WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                                THEN ne.before_state -> 'provenance' ->> 'execution_trace_id'
                        END
                    ) IS NOT NULL
                )
            )[1] AS execution_trace_id,
            MAX(rb.block_timestamp) AS last_updated
        FROM normalized_events ne
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE TRUE
        "#,
    );
    push_history_filters(&mut builder, filter, canonical_only);

    let row = builder
        .build()
        .fetch_one(&mut *connection)
        .await
        .context("failed to summarize normalized-event history rows")?;

    Ok(HistorySummary {
        total_count: u64::try_from(crate::sql_row::get::<i64>(&row, "total_count")?)
            .context("negative normalized-event history total_count")?,
        normalized_event_ids: json_string_array(&crate::sql_row::get(
            &row,
            "normalized_event_ids",
        )?)
        .context("failed to decode normalized-event history summary ids")?,
        raw_fact_refs: json_array(&crate::sql_row::get(&row, "raw_fact_refs")?)
            .context("failed to decode normalized-event history summary raw refs")?,
        manifest_versions: json_array(&crate::sql_row::get(&row, "manifest_versions")?)
            .context("failed to decode normalized-event history summary manifest versions")?,
        execution_trace_id: crate::sql_row::get(&row, "execution_trace_id")?,
        chain_position_samples: Vec::new(),
        last_updated: crate::sql_row::get(&row, "last_updated")?,
    })
}

async fn load_history_chain_position_samples(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<Vec<HistoryChainPositionSample>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT DISTINCT ON (ne.chain_id)
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp
        FROM normalized_events ne
        JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.chain_id IS NOT NULL
          AND ne.block_number IS NOT NULL
          AND ne.block_hash IS NOT NULL
          AND rb.block_timestamp IS NOT NULL
        "#,
    );
    push_history_filters(&mut builder, filter, canonical_only);
    builder.push(
        r#"
        ORDER BY
            ne.chain_id ASC,
            ne.block_number DESC,
            ne.block_hash DESC
        "#,
    );

    let rows = builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context("failed to summarize normalized-event history chain positions")?;

    rows.into_iter()
        .map(|row| {
            Ok(HistoryChainPositionSample {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                block_number: crate::sql_row::get(&row, "block_number")?,
                block_hash: crate::sql_row::get(&row, "block_hash")?,
                block_timestamp: crate::sql_row::get(&row, "block_timestamp")?,
            })
        })
        .collect()
}

fn json_array(value: &Value) -> Result<Vec<Value>> {
    value.as_array().cloned().context("expected JSON array")
}

fn json_string_array(value: &Value) -> Result<Vec<String>> {
    value
        .as_array()
        .context("expected JSON array")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .context("expected JSON string")
        })
        .collect()
}
