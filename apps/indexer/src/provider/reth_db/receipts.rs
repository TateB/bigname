use std::collections::BTreeMap;

use alloy_consensus::{Transaction as _, TxReceipt as _, transaction::SignerRecoverable as _};
use anyhow::{Context, Result, bail};
use reth_ethereum::provider::{
    BlockBodyIndicesProvider, BlockHashReader, ReceiptProvider, TransactionsProvider,
};

use super::{
    RethDbReader,
    convert::{address_hex, hash_hex, i64_to_u64, parse_b256},
};
use crate::provider::{
    ProviderReceipt, ProviderTransaction, ProviderTransactionReceiptBundle,
    ProviderTransactionReceiptRequest, transaction_receipts::validate_transaction_receipt_pair,
};

impl RethDbReader {
    pub(super) fn fetch_transaction_receipt_pairs_by_hashes_sync(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        let factory = self.factory()?;
        let mut requests_by_block =
            BTreeMap::<(String, i64), Vec<&ProviderTransactionReceiptRequest>>::new();
        let mut bundles_by_request =
            BTreeMap::<(String, String, i64), ProviderTransactionReceiptBundle>::new();
        let mut output = Vec::with_capacity(requests.len());

        for request in requests {
            requests_by_block
                .entry((request.block_hash.clone(), request.block_number))
                .or_default()
                .push(request);
        }

        for ((block_hash, block_number), block_requests) in requests_by_block {
            let block_number_u64 = i64_to_u64(block_number, "provider block number")?;
            let stored_block_hash = factory.block_hash(block_number_u64)?.with_context(|| {
                format!("Reth DB did not return block hash for block {block_number}")
            })?;
            let stored_block_hash = hash_hex(stored_block_hash);
            if stored_block_hash != block_hash {
                bail!(
                    "Reth DB block number {} resolved to {}, but transaction/receipt request expected {}",
                    block_number,
                    stored_block_hash,
                    block_hash
                );
            }

            let block_body_indices =
                factory
                    .block_body_indices(block_number_u64)?
                    .with_context(|| {
                        format!(
                            "Reth DB did not return block body indices for block {block_number}"
                        )
                    })?;
            let receipts = factory
                .receipts_by_block(parse_b256(&block_hash, "block hash")?.into())?
                .with_context(|| {
                    format!("Reth DB did not return receipts for block {block_hash}")
                })?;

            for request in block_requests {
                let transaction_index =
                    usize::try_from(request.transaction_index).with_context(|| {
                        format!(
                            "transaction index {} does not fit in usize",
                            request.transaction_index
                        )
                    })?;
                if transaction_index >= receipts.len() {
                    bail!(
                        "Reth DB receipt request for transaction index {} exceeds receipt count {} in block {}",
                        transaction_index,
                        receipts.len(),
                        block_hash
                    );
                }
                let transaction_id = block_body_indices
                    .first_tx_num
                    .checked_add(u64::try_from(transaction_index).with_context(|| {
                        format!("transaction index {transaction_index} does not fit in u64")
                    })?)
                    .context("Reth DB transaction id overflow")?;
                let transaction = factory
                    .transaction_by_id(transaction_id)?
                    .with_context(|| {
                        format!(
                            "Reth DB did not return transaction id {transaction_id} for block {block_hash}"
                        )
                    })?;
                let sender = match factory.transaction_sender(transaction_id)? {
                    Some(sender) => sender,
                    None => transaction.recover_signer_unchecked().with_context(|| {
                        format!(
                            "Reth DB did not return sender for transaction id {transaction_id} in block {block_hash}, and signature recovery failed"
                        )
                    })?,
                };
                let transaction_hash = hash_hex(*transaction.tx_hash());
                let creates_contract = transaction.is_create();
                let nonce = transaction.nonce();
                let transaction = ProviderTransaction {
                    transaction_hash,
                    block_hash: block_hash.clone(),
                    block_number,
                    transaction_index: request.transaction_index,
                    from: address_hex(sender),
                    to: transaction.to().map(address_hex),
                };

                let receipt = &receipts[transaction_index];
                let cumulative_gas = receipt.cumulative_gas_used();
                let previous_cumulative_gas = if transaction_index == 0 {
                    0
                } else {
                    receipts[transaction_index - 1].cumulative_gas_used()
                };
                let gas_used = cumulative_gas
                    .checked_sub(previous_cumulative_gas)
                    .with_context(|| {
                        format!("Reth DB receipt gas regressed in block {block_hash}")
                    })?;
                let status = receipt
                    .status_or_post_state()
                    .as_eip658()
                    .map(|status| if status { 1 } else { 0 });
                let contract_address = if creates_contract && status != Some(0) {
                    Some(address_hex(sender.create(nonce)))
                } else {
                    None
                };
                let receipt = ProviderReceipt {
                    transaction_hash: transaction.transaction_hash.clone(),
                    block_hash: block_hash.clone(),
                    block_number,
                    transaction_index: request.transaction_index,
                    contract_address,
                    status,
                    cumulative_gas_used: Some(i64::try_from(cumulative_gas).with_context(
                        || format!("cumulative gas used {cumulative_gas} does not fit in i64"),
                    )?),
                    gas_used: Some(
                        i64::try_from(gas_used)
                            .with_context(|| format!("gas used {gas_used} does not fit in i64"))?,
                    ),
                    logs_bloom: Some(receipt.bloom().data().to_vec()),
                };

                validate_transaction_receipt_pair(request, &transaction, &receipt)?;
                bundles_by_request.insert(
                    (
                        request.block_hash.clone(),
                        request.transaction_hash.clone(),
                        request.transaction_index,
                    ),
                    ProviderTransactionReceiptBundle {
                        transaction,
                        receipt,
                    },
                );
            }
        }

        for request in requests {
            output.push(
                bundles_by_request
                    .get(&(
                        request.block_hash.clone(),
                        request.transaction_hash.clone(),
                        request.transaction_index,
                    ))
                    .with_context(|| {
                        format!(
                            "Reth DB did not build transaction/receipt bundle for transaction {} in block {}",
                            request.transaction_hash, request.block_hash
                        )
                    })?
                    .clone(),
            );
        }

        Ok(output)
    }
}
