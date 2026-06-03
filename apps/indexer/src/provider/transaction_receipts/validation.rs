use std::collections::BTreeMap;

use anyhow::{Result, bail};

use super::super::{
    ProviderReceipt, ProviderResolvedBlock, ProviderTransaction, ProviderTransactionReceiptRequest,
};

pub(super) fn fallback_transactions_by_key(
    resolved_block: &ProviderResolvedBlock,
    transactions: Vec<ProviderTransaction>,
) -> Result<BTreeMap<(String, i64), ProviderTransaction>> {
    let mut transactions_by_key = BTreeMap::new();
    for transaction in transactions {
        if transaction.block_hash != resolved_block.block_hash {
            bail!(
                "provider returned fallback transaction {} for block {} with mismatched block hash {}",
                transaction.transaction_hash,
                resolved_block.block_hash,
                transaction.block_hash
            );
        }
        if transaction.block_number != resolved_block.block_number {
            bail!(
                "provider returned fallback transaction {} for block {} with mismatched block number {}",
                transaction.transaction_hash,
                resolved_block.block_hash,
                transaction.block_number
            );
        }
        let key = (
            transaction.transaction_hash.clone(),
            transaction.transaction_index,
        );
        if transactions_by_key.insert(key, transaction).is_some() {
            bail!(
                "provider returned duplicate fallback transaction in block {}",
                resolved_block.block_hash
            );
        }
    }

    Ok(transactions_by_key)
}

pub(super) fn fallback_receipts_by_key(
    resolved_block: &ProviderResolvedBlock,
    receipts: Vec<ProviderReceipt>,
) -> Result<BTreeMap<(String, i64), ProviderReceipt>> {
    let mut receipts_by_key = BTreeMap::new();
    for receipt in receipts {
        if receipt.block_hash != resolved_block.block_hash {
            bail!(
                "provider returned fallback receipt {} for block {} with mismatched block hash {}",
                receipt.transaction_hash,
                resolved_block.block_hash,
                receipt.block_hash
            );
        }
        if receipt.block_number != resolved_block.block_number {
            bail!(
                "provider returned fallback receipt {} for block {} with mismatched block number {}",
                receipt.transaction_hash,
                resolved_block.block_hash,
                receipt.block_number
            );
        }
        let key = (receipt.transaction_hash.clone(), receipt.transaction_index);
        if receipts_by_key.insert(key, receipt).is_some() {
            bail!(
                "provider returned duplicate fallback receipt in block {}",
                resolved_block.block_hash
            );
        }
    }

    Ok(receipts_by_key)
}

pub(in crate::provider) fn validate_transaction_receipt_pair(
    request: &ProviderTransactionReceiptRequest,
    transaction: &ProviderTransaction,
    receipt: &ProviderReceipt,
) -> Result<()> {
    validate_transaction_request_scope(request, transaction)?;
    validate_receipt_request_scope(request, receipt)?;

    if receipt.transaction_hash != transaction.transaction_hash {
        bail!(
            "provider returned receipt {} for transaction {}",
            receipt.transaction_hash,
            transaction.transaction_hash
        );
    }

    Ok(())
}

fn validate_transaction_request_scope(
    request: &ProviderTransactionReceiptRequest,
    transaction: &ProviderTransaction,
) -> Result<()> {
    if transaction.transaction_hash != request.transaction_hash {
        bail!(
            "provider returned transaction {} for requested transaction {}",
            transaction.transaction_hash,
            request.transaction_hash
        );
    }
    if transaction.block_hash != request.block_hash {
        bail!(
            "provider returned transaction {} for block {} with mismatched block hash {}",
            transaction.transaction_hash,
            request.block_hash,
            transaction.block_hash
        );
    }
    if transaction.block_number != request.block_number {
        bail!(
            "provider returned transaction {} for block {} with mismatched block number {}",
            transaction.transaction_hash,
            request.block_hash,
            transaction.block_number
        );
    }
    if transaction.transaction_index != request.transaction_index {
        bail!(
            "provider returned transaction {} with index {}; expected {}",
            transaction.transaction_hash,
            transaction.transaction_index,
            request.transaction_index
        );
    }

    Ok(())
}

fn validate_receipt_request_scope(
    request: &ProviderTransactionReceiptRequest,
    receipt: &ProviderReceipt,
) -> Result<()> {
    if receipt.transaction_hash != request.transaction_hash {
        bail!(
            "provider returned receipt {} for requested transaction {}",
            receipt.transaction_hash,
            request.transaction_hash
        );
    }
    if receipt.block_hash != request.block_hash {
        bail!(
            "provider returned receipt {} for block {} with mismatched block hash {}",
            receipt.transaction_hash,
            request.block_hash,
            receipt.block_hash
        );
    }
    if receipt.block_number != request.block_number {
        bail!(
            "provider returned receipt {} for block {} with mismatched block number {}",
            receipt.transaction_hash,
            request.block_hash,
            receipt.block_number
        );
    }
    if receipt.transaction_index != request.transaction_index {
        bail!(
            "provider returned receipt {} with transaction index {}; expected {}",
            receipt.transaction_hash,
            receipt.transaction_index,
            request.transaction_index
        );
    }

    Ok(())
}
