use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use super::types::ActiveCapabilityRow;

pub(super) async fn load_active_capabilities(
    pool: &PgPool,
) -> Result<HashMap<i64, Vec<ActiveCapabilityRow>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mcf.capability_name AS capability_name,
            mcf.status::text AS status,
            mcf.notes AS notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.namespace, mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mcf.capability_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active capability flags for normalized-event sync")?;

    let mut grouped = HashMap::<i64, Vec<ActiveCapabilityRow>>::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("missing capability manifest_id")?;
        grouped
            .entry(manifest_id)
            .or_default()
            .push(ActiveCapabilityRow {
                capability_name: row
                    .try_get("capability_name")
                    .context("missing capability_name")?,
                status: row.try_get("status").context("missing status")?,
                notes: row.try_get("notes").context("missing notes")?,
            });
    }

    Ok(grouped)
}

pub(super) async fn load_normalized_event_counts_by_kind(
    pool: &PgPool,
) -> Result<BTreeMap<String, usize>> {
    let rows = sqlx::query(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        GROUP BY event_kind
        ORDER BY event_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized-event counts by kind")?;

    let mut counts = BTreeMap::new();
    for row in rows {
        let event_kind = row
            .try_get::<String, _>("event_kind")
            .context("missing event_kind from normalized-event count row")?;
        let event_count = row
            .try_get::<i64, _>("event_count")
            .context("missing event_count from normalized-event count row")?;
        counts.insert(
            event_kind,
            usize::try_from(event_count).context("normalized-event count does not fit in usize")?,
        );
    }

    Ok(counts)
}
