use std::collections::BTreeSet;

use bigname_storage::{CanonicalityState, RawBlock, RawLog};

use crate::provider::{ProviderReceipt, ProviderTransaction};

use super::retained_transaction_keys_from_raw_logs;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EventSilentResolverCallObservation {
    pub(crate) chain_id: String,
    pub(crate) resolver_address: String,
    pub(crate) block_hash: String,
    pub(crate) block_number: i64,
    pub(crate) transaction_hash: String,
    pub(crate) transaction_index: i64,
    pub(crate) canonicality_state: CanonicalityState,
}

pub(crate) fn retained_transaction_keys_from_live_payload(
    logs: &[RawLog],
    transactions: &[ProviderTransaction],
    receipts: &[ProviderReceipt],
    direct_call_addresses: &BTreeSet<String>,
) -> BTreeSet<(String, i64)> {
    let mut retained = retained_transaction_keys_from_raw_logs(logs);
    let successful_receipt_keys = receipts
        .iter()
        .filter(|receipt| receipt.status != Some(0))
        .map(|receipt| (receipt.transaction_hash.clone(), receipt.transaction_index))
        .collect::<BTreeSet<_>>();

    retained.extend(
        transactions
            .iter()
            .filter(|transaction| {
                transaction
                    .to
                    .as_deref()
                    .map(|to| direct_call_addresses.contains(&to.to_ascii_lowercase()))
                    .unwrap_or(false)
            })
            .filter(|transaction| {
                successful_receipt_keys.contains(&(
                    transaction.transaction_hash.clone(),
                    transaction.transaction_index,
                ))
            })
            .map(|transaction| {
                (
                    transaction.transaction_hash.clone(),
                    transaction.transaction_index,
                )
            }),
    );

    retained
}

pub(crate) fn event_silent_resolver_call_observations_from_live_payload(
    chain: &str,
    raw_block: &RawBlock,
    transactions: &[ProviderTransaction],
    receipts: &[ProviderReceipt],
    direct_call_addresses: &BTreeSet<String>,
) -> Vec<EventSilentResolverCallObservation> {
    let successful_receipt_keys = receipts
        .iter()
        .filter(|receipt| receipt.status != Some(0))
        .map(|receipt| (receipt.transaction_hash.clone(), receipt.transaction_index))
        .collect::<BTreeSet<_>>();

    transactions
        .iter()
        .filter_map(|transaction| {
            let resolver_address = transaction.to.as_deref()?.to_ascii_lowercase();
            direct_call_addresses
                .contains(&resolver_address)
                .then_some((transaction, resolver_address))
        })
        .filter(|(transaction, _)| {
            successful_receipt_keys.contains(&(
                transaction.transaction_hash.clone(),
                transaction.transaction_index,
            ))
        })
        .map(
            |(transaction, resolver_address)| EventSilentResolverCallObservation {
                chain_id: chain.to_owned(),
                resolver_address,
                block_hash: raw_block.block_hash.clone(),
                block_number: raw_block.block_number,
                transaction_hash: transaction.transaction_hash.clone(),
                transaction_index: transaction.transaction_index,
                canonicality_state: raw_block.canonicality_state,
            },
        )
        .collect()
}

pub(crate) fn event_silent_direct_call_address_set(
    chain: &str,
    extra_addresses: &[String],
) -> BTreeSet<String> {
    let mut addresses = BTreeSet::new();
    if chain != bigname_storage::ETHEREUM_MAINNET_CHAIN_ID {
        return addresses;
    }
    addresses.extend(
        bigname_storage::ENS_LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES
            .iter()
            .map(|address| address.to_ascii_lowercase()),
    );
    addresses.extend(
        extra_addresses
            .iter()
            .map(|address| address.to_ascii_lowercase()),
    );
    addresses
}
