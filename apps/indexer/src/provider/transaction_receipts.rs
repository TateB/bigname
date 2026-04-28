use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::{
    JsonRpcProvider, PROVIDER_BATCH_ITEM_LIMIT, ProviderReceipt, ProviderTransaction,
    ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest, request::JsonRpcBatchCall,
};

const TRANSACTION_RECEIPT_BATCH_PAIR_LIMIT: usize = PROVIDER_BATCH_ITEM_LIMIT / 2;

impl JsonRpcProvider {
    pub async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        let mut bundles = Vec::with_capacity(requests.len());

        for chunk in requests.chunks(TRANSACTION_RECEIPT_BATCH_PAIR_LIMIT.max(1)) {
            let calls = chunk
                .iter()
                .flat_map(|request| {
                    [
                        JsonRpcBatchCall {
                            method: "eth_getTransactionByHash",
                            params: vec![Value::String(request.transaction_hash.clone())],
                        },
                        JsonRpcBatchCall {
                            method: "eth_getTransactionReceipt",
                            params: vec![Value::String(request.transaction_hash.clone())],
                        },
                    ]
                })
                .collect::<Vec<_>>();
            let results = self.fetch_json_rpc_batch_results(calls).await?;
            let mut result_pairs = results.chunks_exact(2);

            for (request, pair) in chunk.iter().zip(&mut result_pairs) {
                let transaction = pair[0]
                    .clone()
                    .with_context(|| {
                        format!(
                            "provider did not return transaction {}",
                            request.transaction_hash
                        )
                    })
                    .and_then(|value| ProviderTransaction::from_value(&value))?;
                let receipt = pair[1]
                    .clone()
                    .with_context(|| {
                        format!(
                            "provider did not return receipt for transaction {}",
                            request.transaction_hash
                        )
                    })
                    .and_then(|value| ProviderReceipt::from_value(&value))?;
                validate_transaction_receipt_pair(request, &transaction, &receipt)?;
                bundles.push(ProviderTransactionReceiptBundle {
                    transaction,
                    receipt,
                });
            }
            if !result_pairs.remainder().is_empty() {
                bail!("provider returned an odd number of transaction/receipt batch results");
            }
        }

        Ok(bundles)
    }
}

pub(super) fn validate_transaction_receipt_pair(
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
