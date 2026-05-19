use super::*;

pub(super) async fn load_generic_resolver_event_raw_logs(
    pool: &PgPool,
    chain: &str,
    sources: &[GenericResolverEventSource],
    event_topics: &AuthorityEventTopics,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    block_range: Option<(i64, i64)>,
) -> Result<Vec<AuthorityRawLogRow>> {
    if sources.is_empty() {
        return Ok(Vec::new());
    }

    let source_from_blocks = sources
        .iter()
        .map(|source| source.effective_from_block.unwrap_or(0))
        .collect::<Vec<_>>();
    let source_to_blocks = sources
        .iter()
        .map(|source| source.effective_to_block.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    let topic0s = event_topics.ens_resolver_event_topic0s()?;
    let (has_block_range, from_block, to_block) = block_range
        .map(|(from_block, to_block)| (true, from_block, to_block))
        .unwrap_or((false, 0, 0));

    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id AS chain_id,
            rl.block_hash AS block_hash,
            rl.block_number AS block_number,
            rb.block_timestamp AS block_timestamp,
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            rl.emitting_address AS emitting_address,
            rl.topics AS topics,
            rl.data AS data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN chain_lineage rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND ($2::BOOLEAN = FALSE OR rl.block_hash = ANY($3::TEXT[]))
          AND ($4::BOOLEAN = FALSE OR rl.block_number BETWEEN $5::BIGINT AND $6::BIGINT)
          AND lower(rl.topics[1]) = ANY($7::TEXT[])
          AND EXISTS (
              SELECT 1
              FROM unnest($8::BIGINT[], $9::BIGINT[]) AS source_range(
                  effective_from_block,
                  effective_to_block
              )
              WHERE rl.block_number BETWEEN source_range.effective_from_block
                  AND source_range.effective_to_block
          )
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index
        "#,
    )
    .bind(chain)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .bind(has_block_range)
    .bind(from_block)
    .bind(to_block)
    .bind(&topic0s)
    .bind(&source_from_blocks)
    .bind(&source_to_blocks)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load generic ENSv1 resolver-event raw logs for chain {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            let address = sql_row::get::<String>(&row, "emitting_address")?.to_ascii_lowercase();
            let block_number = sql_row::get(&row, "block_number")?;
            let source =
                generic_resolver_event_source_for_block(sources, block_number).with_context(
                    || {
                        format!(
                            "missing generic ENSv1 resolver-event source metadata for chain {chain} block {block_number}"
                        )
                    },
                )?;
            authority_raw_log_from_generic_resolver_source(row, address, block_number, source)
        })
        .collect()
}

fn generic_resolver_event_source_for_block(
    sources: &[GenericResolverEventSource],
    block_number: i64,
) -> Option<&GenericResolverEventSource> {
    sources
        .iter()
        .filter(|source| {
            source
                .effective_from_block
                .is_none_or(|from_block| block_number >= from_block)
                && source
                    .effective_to_block
                    .is_none_or(|to_block| block_number <= to_block)
        })
        .min_by(|left, right| left.source_manifest_id.cmp(&right.source_manifest_id))
}

fn authority_raw_log_from_generic_resolver_source(
    row: sqlx::postgres::PgRow,
    emitting_address: String,
    block_number: i64,
    source: &GenericResolverEventSource,
) -> Result<AuthorityRawLogRow> {
    Ok(AuthorityRawLogRow {
        chain_id: sql_row::get(&row, "chain_id")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_number,
        block_timestamp: sql_row::get(&row, "block_timestamp")?,
        transaction_hash: sql_row::get(&row, "transaction_hash")?,
        transaction_index: sql_row::get(&row, "transaction_index")?,
        log_index: sql_row::get(&row, "log_index")?,
        emitting_address,
        topics: sql_row::get(&row, "topics")?,
        data: sql_row::get(&row, "data")?,
        canonicality_state: sql_row::get(&row, "canonicality_state")?,
        source_manifest_id: source.source_manifest_id,
        namespace: source.namespace.clone(),
        source_family: source.source_family.clone(),
        manifest_version: source.manifest_version,
        normalizer_version: source.normalizer_version.clone(),
        contract_role: None,
    })
}
