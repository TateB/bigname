use anyhow::Result;
use bigname_storage::sql_row;

use super::super::super::{ActiveEmitter, AuthorityRawLogRow, AuthorityRawLogSourceScopeTarget};

pub(super) fn authority_source_scope_block_range(
    source_scope: &[AuthorityRawLogSourceScopeTarget],
) -> Option<(i64, i64)> {
    let from_block = source_scope
        .iter()
        .map(|target| target.effective_from_block)
        .min()?;
    let to_block = source_scope
        .iter()
        .map(|target| target.effective_to_block)
        .max()?;
    Some((from_block, to_block))
}

pub(super) fn authority_raw_log_from_row(
    row: sqlx::postgres::PgRow,
    emitting_address: String,
    block_number: i64,
    emitter: &ActiveEmitter,
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
        source_manifest_id: emitter.source_manifest_id,
        namespace: emitter.namespace.clone(),
        source_family: emitter.source_family.clone(),
        manifest_version: emitter.manifest_version,
        normalizer_version: emitter.normalizer_version.clone(),
        contract_role: emitter.contract_role.clone(),
    })
}
