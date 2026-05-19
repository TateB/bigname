use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{RawBlock, RawLog};

use crate::provider::{ProviderBlockCodeObservationRequest, ProviderTransactionReceiptRequest};

pub(super) fn transaction_receipt_requests_from_raw_logs(
    logs: &[RawLog],
) -> Vec<ProviderTransactionReceiptRequest> {
    let mut seen = BTreeMap::<(String, String, i64), ProviderTransactionReceiptRequest>::new();
    for log in logs {
        seen.entry((
            log.block_hash.clone(),
            log.transaction_hash.clone(),
            log.transaction_index,
        ))
        .or_insert_with(|| ProviderTransactionReceiptRequest {
            transaction_hash: log.transaction_hash.clone(),
            block_hash: log.block_hash.clone(),
            block_number: log.block_number,
            transaction_index: log.transaction_index,
        });
    }

    seen.into_values().collect()
}

#[derive(Default)]
pub(super) struct SparseCodeObservationPlan {
    latest_by_address: BTreeMap<String, (i64, String)>,
}

impl SparseCodeObservationPlan {
    pub(super) fn record(&mut self, raw_block: &RawBlock, addresses: &BTreeSet<String>) {
        for address in addresses {
            self.latest_by_address.insert(
                address.clone(),
                (raw_block.block_number, raw_block.block_hash.clone()),
            );
        }
    }

    pub(super) fn requests(&self) -> Vec<ProviderBlockCodeObservationRequest> {
        let mut addresses_by_block = BTreeMap::<(i64, String), BTreeSet<String>>::new();
        for (address, (block_number, block_hash)) in &self.latest_by_address {
            addresses_by_block
                .entry((*block_number, block_hash.clone()))
                .or_default()
                .insert(address.clone());
        }

        addresses_by_block
            .into_iter()
            .map(
                |((_block_number, block_hash), addresses)| ProviderBlockCodeObservationRequest {
                    block_hash,
                    addresses: addresses.into_iter().collect(),
                },
            )
            .collect()
    }
}
