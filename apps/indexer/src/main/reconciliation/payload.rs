use std::collections::BTreeSet;

use alloy_primitives::{Bytes, hex, keccak256};
use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, RawBlock, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, RawReceipt,
    RawTransaction,
};

use crate::provider::{
    JSON_RPC_PAYLOAD_CONTENT_ENCODING, JSON_RPC_PAYLOAD_CONTENT_TYPE, ProviderBlock,
    ProviderBlockBundle, ProviderCodeObservation, ProviderHeadSnapshot, ProviderLog,
    ProviderRawPayloadCacheMetadata, ProviderReceipt, ProviderTransaction,
};

use super::types::{
    CanonicalReconciliation, CanonicalReconciliationStatus, HeadChangeSet, HeaderAuditMode,
};

pub(crate) fn raw_payload_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = BTreeSet::new();

    for block in &canonical.reconciled_blocks {
        hashes.insert(block.block_hash.clone());
    }

    if (head_change_set.safe_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized)
        && let Some(safe) = &heads.safe
    {
        hashes.insert(safe.block_hash.clone());
    }

    if (head_change_set.finalized_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized)
        && let Some(finalized) = &heads.finalized
    {
        hashes.insert(finalized.block_hash.clone());
    }

    hashes.into_iter().collect()
}

pub(crate) fn raw_code_hash_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if let Some(canonical) = canonical.canonical.as_ref() {
        hashes.insert(canonical.block_hash.clone());
    }
    if let Some(safe) = &heads.safe {
        hashes.insert(safe.block_hash.clone());
    }
    if let Some(finalized) = &heads.finalized {
        hashes.insert(finalized.block_hash.clone());
    }

    hashes.into_iter().collect()
}

pub(crate) fn ensure_provider_bundle_matches_raw_block(
    raw_block: &RawBlock,
    bundle: &ProviderBlockBundle,
) -> Result<()> {
    let candidate = provider_block_to_raw_block_with_header_audit_mode(
        raw_block.chain_id.as_str(),
        &bundle.block,
        raw_block.canonicality_state,
        HeaderAuditMode::RetainAuditFields,
    );

    if candidate.block_hash != raw_block.block_hash
        || candidate.parent_hash != raw_block.parent_hash
        || candidate.block_number != raw_block.block_number
        || candidate.block_timestamp != raw_block.block_timestamp
        || optional_audit_field_conflicts(&candidate.logs_bloom, &raw_block.logs_bloom)
        || optional_audit_field_conflicts(
            &candidate.transactions_root,
            &raw_block.transactions_root,
        )
        || optional_audit_field_conflicts(&candidate.receipts_root, &raw_block.receipts_root)
        || optional_audit_field_conflicts(&candidate.state_root, &raw_block.state_root)
    {
        bail!(
            "provider bundle block {} does not match stored raw block facts for chain {}",
            raw_block.block_hash,
            raw_block.chain_id
        );
    }

    Ok(())
}

fn optional_audit_field_conflicts<T: Eq>(left: &Option<T>, right: &Option<T>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left != right)
}

pub(crate) fn selected_address_set(addresses: &[String]) -> BTreeSet<String> {
    addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect()
}

pub(crate) fn provider_logs_to_selected_raw_logs(
    chain: &str,
    raw_block: &RawBlock,
    logs: &[ProviderLog],
    selected_addresses: &BTreeSet<String>,
) -> Result<Vec<RawLog>> {
    logs.iter()
        .filter(|log| selected_addresses.contains(&log.address.to_ascii_lowercase()))
        .map(|log| provider_log_to_raw_log(chain, raw_block, log))
        .collect()
}

pub(crate) fn provider_logs_to_live_selected_raw_logs(
    chain: &str,
    raw_block: &RawBlock,
    logs: &[ProviderLog],
    selected_addresses: &BTreeSet<String>,
) -> Result<Vec<RawLog>> {
    let selected_transaction_keys = logs
        .iter()
        .filter(|log| selected_addresses.contains(&log.address.to_ascii_lowercase()))
        .map(|log| (log.transaction_hash.clone(), log.transaction_index))
        .collect::<BTreeSet<_>>();

    logs.iter()
        .filter(|log| {
            selected_addresses.contains(&log.address.to_ascii_lowercase())
                || selected_transaction_keys
                    .contains(&(log.transaction_hash.clone(), log.transaction_index))
        })
        .map(|log| provider_log_to_raw_log(chain, raw_block, log))
        .collect()
}

pub(crate) fn retained_transaction_keys_from_raw_logs(logs: &[RawLog]) -> BTreeSet<(String, i64)> {
    logs.iter()
        .map(|log| (log.transaction_hash.clone(), log.transaction_index))
        .collect()
}

pub(crate) fn provider_transactions_to_selected_raw_transactions(
    chain: &str,
    raw_block: &RawBlock,
    transactions: &[ProviderTransaction],
    retained_transaction_keys: &BTreeSet<(String, i64)>,
) -> Result<Vec<RawTransaction>> {
    transactions
        .iter()
        .filter(|transaction| {
            retained_transaction_keys.contains(&(
                transaction.transaction_hash.clone(),
                transaction.transaction_index,
            ))
        })
        .map(|transaction| provider_transaction_to_raw_transaction(chain, raw_block, transaction))
        .collect()
}

pub(crate) fn provider_receipts_to_selected_raw_receipts(
    chain: &str,
    raw_block: &RawBlock,
    receipts: &[ProviderReceipt],
    retained_transaction_keys: &BTreeSet<(String, i64)>,
) -> Result<Vec<RawReceipt>> {
    receipts
        .iter()
        .filter(|receipt| {
            retained_transaction_keys
                .contains(&(receipt.transaction_hash.clone(), receipt.transaction_index))
        })
        .map(|receipt| provider_receipt_to_raw_receipt(chain, raw_block, receipt))
        .collect()
}

pub(crate) fn provider_raw_payload_cache_metadata_to_upserts(
    chain: &str,
    raw_block: &RawBlock,
    payloads: &[ProviderRawPayloadCacheMetadata],
) -> Vec<RawPayloadCacheMetadataUpsert> {
    payloads
        .iter()
        .map(|payload| RawPayloadCacheMetadataUpsert {
            chain_id: chain.to_owned(),
            block_hash: raw_block.block_hash.clone(),
            payload_kind: payload.payload_kind.clone(),
            digest_algorithm: Some(payload.digest_algorithm.clone()),
            retained_digest: Some(payload.retained_digest.clone()),
            block_number: Some(raw_block.block_number),
            payload_size_bytes: payload.payload_size_bytes,
            content_type: Some(JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: payload.cache_metadata.clone(),
            canonicality_state: raw_block.canonicality_state,
        })
        .collect()
}

pub(crate) fn canonical_raw_state(status: CanonicalReconciliationStatus) -> CanonicalityState {
    match status {
        CanonicalReconciliationStatus::AwaitingAncestor => CanonicalityState::Observed,
        CanonicalReconciliationStatus::Initialized
        | CanonicalReconciliationStatus::Unchanged
        | CanonicalReconciliationStatus::Appended
        | CanonicalReconciliationStatus::GapBackfilled
        | CanonicalReconciliationStatus::ReorgReconciled => CanonicalityState::Canonical,
    }
}

pub(crate) fn insert_raw_block_candidate(
    blocks: &mut std::collections::BTreeMap<String, bigname_storage::RawBlock>,
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
    header_audit_mode: HeaderAuditMode,
) {
    let candidate = provider_block_to_raw_block_with_header_audit_mode(
        chain,
        block,
        canonicality_state,
        header_audit_mode,
    );
    blocks
        .entry(candidate.block_hash.clone())
        .and_modify(|existing| {
            existing.canonicality_state =
                preferred_canonicality(existing.canonicality_state, candidate.canonicality_state);
        })
        .or_insert(candidate);
}

pub(crate) fn preferred_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    if incoming.rank() > current.rank() {
        incoming
    } else {
        current
    }
}

pub(crate) fn provider_transaction_to_raw_transaction(
    chain: &str,
    raw_block: &RawBlock,
    transaction: &ProviderTransaction,
) -> Result<RawTransaction> {
    ensure_block_scoped_identity(
        "transaction",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &transaction.block_hash,
        transaction.block_number,
    )?;

    Ok(RawTransaction {
        chain_id: chain.to_owned(),
        block_hash: transaction.block_hash.clone(),
        block_number: transaction.block_number,
        transaction_hash: transaction.transaction_hash.clone(),
        transaction_index: transaction.transaction_index,
        from_address: transaction.from.clone(),
        to_address: transaction.to.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_receipt_to_raw_receipt(
    chain: &str,
    raw_block: &RawBlock,
    receipt: &ProviderReceipt,
) -> Result<RawReceipt> {
    ensure_block_scoped_identity(
        "receipt",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &receipt.block_hash,
        receipt.block_number,
    )?;

    Ok(RawReceipt {
        chain_id: chain.to_owned(),
        block_hash: receipt.block_hash.clone(),
        block_number: receipt.block_number,
        transaction_hash: receipt.transaction_hash.clone(),
        transaction_index: receipt.transaction_index,
        contract_address: receipt.contract_address.clone(),
        status: parse_receipt_status(receipt.status)?,
        gas_used: receipt.gas_used,
        cumulative_gas_used: receipt.cumulative_gas_used,
        logs_bloom: receipt.logs_bloom.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_log_to_raw_log(
    chain: &str,
    raw_block: &RawBlock,
    log: &ProviderLog,
) -> Result<RawLog> {
    ensure_block_scoped_identity(
        "log",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &log.block_hash,
        log.block_number,
    )?;

    Ok(RawLog {
        chain_id: chain.to_owned(),
        block_hash: log.block_hash.clone(),
        block_number: log.block_number,
        transaction_hash: log.transaction_hash.clone(),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        emitting_address: log.address.to_ascii_lowercase(),
        topics: log.topics.clone(),
        data: parse_hex_bytes(&log.data)?,
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn provider_code_observation_to_raw_code_hash(
    chain: &str,
    raw_block: &RawBlock,
    observation: &ProviderCodeObservation,
) -> Result<RawCodeHash> {
    let code_byte_length = i64::try_from(observation.code.len()).with_context(|| {
        format!(
            "provider code observation byte length {} does not fit in i64 for chain {} block {} contract {}",
            observation.code.len(),
            chain,
            raw_block.block_hash,
            observation.address
        )
    })?;

    Ok(RawCodeHash {
        chain_id: chain.to_owned(),
        block_hash: raw_block.block_hash.clone(),
        block_number: raw_block.block_number,
        contract_address: observation.address.clone(),
        code_hash: keccak256_hex(&observation.code),
        code_byte_length,
        canonicality_state: raw_block.canonicality_state,
    })
}

pub(crate) fn ensure_block_scoped_identity(
    fact_kind: &str,
    chain: &str,
    expected_block_hash: &str,
    expected_block_number: i64,
    actual_block_hash: &str,
    actual_block_number: i64,
) -> Result<()> {
    if actual_block_hash != expected_block_hash || actual_block_number != expected_block_number {
        bail!(
            "provider {} block scope mismatch for chain {} expected {}@{} got {}@{}",
            fact_kind,
            chain,
            expected_block_hash,
            expected_block_number,
            actual_block_hash,
            actual_block_number
        );
    }

    Ok(())
}

pub(crate) fn parse_receipt_status(status: Option<i64>) -> Result<Option<bool>> {
    match status {
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(other) => bail!("unsupported receipt status value {other}"),
        None => Ok(None),
    }
}

pub(crate) fn keccak256_hex(bytes: &[u8]) -> String {
    format!("{}", keccak256(bytes))
}

pub(crate) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    parse_rpc_bytes(value).map(|bytes| bytes.to_vec())
}

fn parse_rpc_bytes(value: &str) -> Result<Bytes> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("invalid hex byte string with odd length");
    }

    let bytes = hex::decode(value).with_context(|| format!("failed to parse hex bytes {value}"))?;
    Ok(Bytes::from(bytes))
}

#[allow(dead_code, reason = "kept for crate-local test and fixture helpers")]
pub(crate) fn hex_string(bytes: &[u8]) -> String {
    hex::encode_prefixed(bytes)
}

#[allow(dead_code)]
pub(crate) fn provider_block_to_raw_block(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> bigname_storage::RawBlock {
    provider_block_to_raw_block_with_header_audit_mode(
        chain,
        block,
        canonicality_state,
        HeaderAuditMode::Minimal,
    )
}

pub(crate) fn provider_block_to_raw_block_with_header_audit_mode(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
    header_audit_mode: HeaderAuditMode,
) -> bigname_storage::RawBlock {
    let retain_audit_fields = header_audit_mode.retains_audit_fields();
    bigname_storage::RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: retain_audit_fields
            .then(|| block.logs_bloom.clone())
            .flatten(),
        transactions_root: retain_audit_fields
            .then(|| block.transactions_root.clone())
            .flatten(),
        receipts_root: retain_audit_fields
            .then(|| block.receipts_root.clone())
            .flatten(),
        state_root: retain_audit_fields
            .then(|| block.state_root.clone())
            .flatten(),
        canonicality_state,
    }
}
