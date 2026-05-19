pub(super) async fn warm_compact_records_route_sql_path(
    state: &AppState,
    max_connections: u32,
) -> Result<()> {
    let Some((namespace, name)) = compact_records_warmup_sample(&state.pool).await? else {
        warn!(
            service = "api",
            "skipped compact records route SQL warm-up because no current name rows exist"
        );
        return Ok(());
    };

    let warm_count = max_connections.clamp(1, 16) as usize;
    let mut tasks = Vec::with_capacity(warm_count);
    for _ in 0..warm_count {
        let state = state.clone();
        let namespace = namespace.clone();
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            compact_name_records_response_for_name(
                &state,
                &namespace,
                &name,
                compact_records_warmup_query(),
                CompactNameRecordsDefaultMode::Declared,
            )
            .await
        }));
    }

    for task in tasks {
        let result = task
            .await
            .context("compact records route SQL warm-up task failed to join")?;
        if let Err(error) = result {
            bail!(
                "compact records route SQL warm-up failed for {namespace}/{name}: {} ({})",
                error.message,
                error.code
            );
        }
    }

    info!(
        service = "api",
        namespace = %namespace,
        name = %name,
        connections_warmed = warm_count,
        "warmed compact records route SQL path"
    );
    Ok(())
}

async fn compact_records_warmup_sample(pool: &PgPool) -> Result<Option<(String, String)>> {
    let inventory_backed_sample = sqlx::query(
        r#"
        WITH inventory_sample AS (
            SELECT record_version_boundary ->> 'logical_name_id' AS logical_name_id
            FROM record_inventory_current
            WHERE record_version_boundary ? 'logical_name_id'
            LIMIT 128
        )
        SELECT nc.namespace, nc.normalized_name
        FROM inventory_sample sample
        JOIN name_current nc
          ON nc.logical_name_id = sample.logical_name_id
        WHERE nc.namespace IN ('ens', 'basenames')
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .context("failed to select inventory-backed compact records warm-up sample")?;

    if let Some(row) = inventory_backed_sample {
        return Ok(Some((
            row.try_get("namespace")
                .context("compact records warm-up sample missing namespace")?,
            row.try_get("normalized_name")
                .context("compact records warm-up sample missing normalized_name")?,
        )));
    }

    sqlx::query(
        r#"
        SELECT namespace, normalized_name
        FROM name_current
        WHERE namespace IN ('ens', 'basenames')
        ORDER BY logical_name_id
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .context("failed to select fallback compact records warm-up sample")?
    .map(|row| {
        Ok((
            row.try_get("namespace")
                .context("compact records fallback warm-up sample missing namespace")?,
            row.try_get("normalized_name")
                .context("compact records fallback warm-up sample missing normalized_name")?,
        ))
    })
    .transpose()
}

fn compact_records_warmup_query() -> NameRecordsQuery {
    NameRecordsQuery {
        mode: Some("declared".to_owned()),
        known_text_keys: Some("true".to_owned()),
        avatar: Some("true".to_owned()),
        content_hash: Some("true".to_owned()),
        include: Some("resolver_address,known_text_keys,avatar,content_hash,coins".to_owned()),
        meta: Some("none".to_owned()),
        ..NameRecordsQuery::default()
    }
}
