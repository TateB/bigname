use super::*;

// These head rows are the same ones bound by request-head selection. The caller's
// final head/hash and Project xmin revalidation rejects any intervening change;
// SQL validity can never admit a response against a later publication.
pub(crate) const OWNER_TARGET_AT_HEAD: &str = r#"EXISTS (
    SELECT 1 FROM bigname_phase.chain_heads owner_head
    WHERE owner_head.chain_id = anc.provenance ->> 'chain_id'
      AND JSONB_TYPEOF(anc.chain_positions -> 'target_block_number') = 'number'
      AND (anc.chain_positions ->> 'target_block_number')::BIGINT >= 0
      AND ((anc.chain_positions ->> 'target_block_number')::BIGINT < owner_head.latest_block_number
        OR ((anc.chain_positions ->> 'target_block_number')::BIGINT = owner_head.latest_block_number
          AND anc.chain_positions ->> 'target_block_hash' = owner_head.latest_block_hash))
)"#;

fn push_prefix_validation(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: GeneratedDomainSort,
    order: NameCurrentListOrder,
    offset: u64,
) -> Result<()> {
    // OFFSET already visits this matching prefix. Keep its validation in SQL so
    // even a million skipped rows transfer only one boolean to the API.
    builder.push(", owner_prefix AS (").push(SELECT_NAMES);
    push_order(builder, sort, order);
    builder.push(" LIMIT ").push_bind(i64::try_from(offset)?);
    builder.push(
        ") SELECT EXISTS (SELECT 1 FROM owner_prefix \
         CROSS JOIN LATERAL ( \
           SELECT JSONB_BUILD_OBJECT('chain_id', position.value -> 'chain_id', \
             'target_block_number', position.value -> 'block_number', \
             'target_block_hash', position.value -> 'block_hash') AS target \
           FROM JSONB_EACH(chain_positions) position \
           UNION ALL SELECT target FROM JSONB_ARRAY_ELEMENTS(membership_targets) target \
         ) witness \
         WHERE NOT EXISTS (SELECT 1 FROM bigname_phase.chain_heads owner_head \
           WHERE owner_head.chain_id = target ->> 'chain_id' \
             AND JSONB_TYPEOF(target -> 'target_block_number') = 'number' \
             AND (target ->> 'target_block_number')::BIGINT >= 0 \
             AND ((target ->> 'target_block_number')::BIGINT < owner_head.latest_block_number \
               OR ((target ->> 'target_block_number')::BIGINT = owner_head.latest_block_number \
                 AND target ->> 'target_block_hash' = owner_head.latest_block_hash))))",
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn load_phase_graphql_name_list_page_offset(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    snapshot_chain_ids: &[String],
    generated_filter: &GeneratedDomainFilter,
    sort: GeneratedDomainSort,
    order: NameCurrentListOrder,
    limit: u64,
    offset: u64,
) -> Result<Vec<PhaseGraphqlNameListRow>> {
    if offset > 0 && owner::active_owner_filter(Some(generated_filter)).is_some() {
        let mut prefix = QueryBuilder::<Postgres>::new("");
        push_filtered_names(
            &mut prefix,
            filter,
            None,
            Some(generated_filter),
            Some(snapshot_chain_ids),
            indexed_page(sort, generated_filter),
        );
        push_prefix_validation(&mut prefix, sort, order, offset)?;
        let invalid: bool = prefix.build_query_scalar().fetch_one(pool).await?;
        anyhow::ensure!(
            !invalid,
            "lookup data is unavailable at the selected snapshot"
        );
    }
    let limit = i64::try_from(limit).context("GraphQL name limit exceeds SQL limit")?;
    let offset = i64::try_from(offset).context("GraphQL name offset exceeds SQL limit")?;
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_names(
        &mut builder,
        filter,
        None,
        Some(generated_filter),
        Some(snapshot_chain_ids),
        indexed_page(sort, generated_filter),
    );
    builder.push(SELECT_NAMES);
    push_order(&mut builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(limit);
    builder.push(" OFFSET ");
    builder.push_bind(offset);
    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load schema-v2 GraphQL names for {filter:?}"))?;
    rows.into_iter().map(decode_row).collect()
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub async fn explain_phase_graphql_name_list_page(
    pool: &PgPool,
    snapshot_chain_ids: &[String],
    filter: &GeneratedDomainFilter,
    sort: GeneratedDomainSort,
    order: NameCurrentListOrder,
    limit: u64,
    offset: u64,
    prefix_validation: bool,
) -> Result<Value> {
    let storage_filter = NameCurrentListFilter {
        namespace: Some("ens".into()),
        ..Default::default()
    };
    let mut builder = QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
    push_filtered_names(
        &mut builder,
        &storage_filter,
        None,
        Some(filter),
        Some(snapshot_chain_ids),
        indexed_page(sort, filter),
    );
    // Explain either actual statement of an OFFSET request independently.
    if prefix_validation {
        push_prefix_validation(&mut builder, sort, order, offset)?;
        return Ok(builder.build().fetch_one(pool).await?.try_get(0)?);
    }
    builder.push(SELECT_NAMES);
    push_order(&mut builder, sort, order);
    builder.push(" LIMIT ").push_bind(i64::try_from(limit)?);
    builder.push(" OFFSET ").push_bind(i64::try_from(offset)?);
    let row = builder.build().fetch_one(pool).await?;
    Ok(row.try_get(0)?)
}
