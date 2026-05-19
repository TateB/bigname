use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::{ProviderLogsPayload, ProviderReceiptsPayload};
use crate::provider::{
    JsonRpcProvider, MAX_TRANSACTION_RECEIPT_FALLBACK, ProviderLog, ProviderReceipt,
    ProviderTransaction, RAW_PAYLOAD_KIND_BLOCK_LOGS, RAW_PAYLOAD_KIND_BLOCK_RECEIPTS,
    types::ProviderLogFilter,
};

impl JsonRpcProvider {
    pub(in crate::provider) async fn fetch_logs_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<ProviderLogsPayload> {
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getLogs",
                vec![ProviderLogFilter::block_hash(block_hash)?.json_rpc_parameter()?],
            )
            .await?
            .with_cache_metadata(RAW_PAYLOAD_KIND_BLOCK_LOGS, "eth_getLogs", "block_hash");
        let logs = payload
            .result
            .context("provider returned null logs for exact block hash lookup")?;
        let logs = logs
            .as_array()
            .context("expected logs array in JSON-RPC result")?;

        let logs = logs
            .iter()
            .map(|value| ProviderLog::from_value(value, block_hash, expected_block_number))
            .collect::<Result<Vec<_>>>()?;

        Ok(ProviderLogsPayload {
            logs,
            cache_metadata: payload.cache_metadata,
        })
    }

    pub(in crate::provider) async fn fetch_receipts_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        match self
            .fetch_block_receipts_by_block_hash(block_hash, expected_block_number, transactions)
            .await
        {
            Ok(receipts) => Ok(receipts),
            Err(scoped_error) => self
                .fetch_receipts_by_transaction_hashes(
                    block_hash,
                    expected_block_number,
                    transactions,
                )
                .await
                .with_context(|| {
                    format!("block-scoped receipt fetch for {block_hash} failed: {scoped_error}")
                }),
        }
    }

    async fn fetch_block_receipts_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getBlockReceipts",
                vec![Value::String(block_hash.to_owned())],
            )
            .await?
            .with_cache_metadata(
                RAW_PAYLOAD_KIND_BLOCK_RECEIPTS,
                "eth_getBlockReceipts",
                "block_hash",
            );
        let receipts = payload
            .result
            .context("provider returned null receipts for exact block hash lookup")?;
        let receipts = receipts
            .as_array()
            .context("expected receipts array in JSON-RPC result")?;
        let receipts = receipts
            .iter()
            .map(ProviderReceipt::from_value)
            .collect::<Result<Vec<_>>>()?;

        let receipts = self.order_receipts_by_transaction_hash(
            block_hash,
            expected_block_number,
            receipts,
            transactions,
        )?;

        Ok(ProviderReceiptsPayload {
            receipts,
            cache_metadata: vec![payload.cache_metadata],
        })
    }

    async fn fetch_receipts_by_transaction_hashes(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        if transactions.len() > MAX_TRANSACTION_RECEIPT_FALLBACK {
            bail!(
                "refusing to fan out {} transaction receipts for block {}",
                transactions.len(),
                block_hash
            );
        }

        let mut receipts = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            let receipt = self
                .fetch_json_rpc_result(
                    "eth_getTransactionReceipt",
                    vec![Value::String(transaction.transaction_hash.clone())],
                )
                .await?
                .with_context(|| {
                    format!(
                        "provider did not return receipt for transaction {}",
                        transaction.transaction_hash
                    )
                })?;
            let receipt = ProviderReceipt::from_value(&receipt)?;
            receipts.push(receipt);
        }

        let receipts = self.order_receipts_by_transaction_hash(
            block_hash,
            expected_block_number,
            receipts,
            transactions,
        )?;

        Ok(ProviderReceiptsPayload {
            receipts,
            cache_metadata: Vec::new(),
        })
    }

    pub(in crate::provider) fn order_receipts_by_transaction_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        receipts: Vec<ProviderReceipt>,
        transactions: &[ProviderTransaction],
    ) -> Result<Vec<ProviderReceipt>> {
        let mut receipts_by_hash = BTreeMap::new();
        for receipt in receipts {
            if receipt.block_hash != block_hash {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block hash {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_hash
                );
            }
            if receipt.block_number != expected_block_number {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block number {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_number
                );
            }

            if receipts_by_hash
                .insert(receipt.transaction_hash.clone(), receipt)
                .is_some()
            {
                bail!("provider returned duplicate receipt for block {block_hash}");
            }
        }

        let mut ordered = Vec::new();
        for transaction in transactions {
            let receipt = receipts_by_hash
                .remove(&transaction.transaction_hash)
                .with_context(|| {
                    format!(
                        "provider did not return receipt for transaction {} in block {}",
                        transaction.transaction_hash, block_hash
                    )
                })?;

            if receipt.block_hash != block_hash {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block hash {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_hash
                );
            }
            if receipt.block_number != expected_block_number {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block number {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_number
                );
            }

            ordered.push(receipt);
        }

        if !receipts_by_hash.is_empty() {
            bail!("provider returned extra receipts for block {block_hash}");
        }

        Ok(ordered)
    }
}
