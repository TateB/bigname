use std::collections::BTreeSet;

use bigname_storage::{CanonicalityState, RawBlock, RawLog};
use sqlx::types::time::OffsetDateTime;

use super::*;

#[test]
fn live_payload_retains_successful_direct_calls_to_selected_addresses() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let transactions = vec![
        ProviderTransaction {
            transaction_hash: "0xaaa".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 0,
            from: "0x0000000000000000000000000000000000000001".to_owned(),
            to: Some("0xA2c122bE93b0074270EBee7f6B7292c7deb45047".to_owned()),
        },
        ProviderTransaction {
            transaction_hash: "0xbbb".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 1,
            from: "0x0000000000000000000000000000000000000001".to_owned(),
            to: Some("0x0000000000000000000000000000000000000002".to_owned()),
        },
    ];
    let receipts = vec![
        ProviderReceipt {
            transaction_hash: "0xaaa".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 0,
            contract_address: None,
            status: Some(1),
            cumulative_gas_used: None,
            gas_used: None,
            logs_bloom: None,
        },
        ProviderReceipt {
            transaction_hash: "0xbbb".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 1,
            contract_address: None,
            status: Some(1),
            cumulative_gas_used: None,
            gas_used: None,
            logs_bloom: None,
        },
    ];

    let retained = retained_transaction_keys_from_live_payload(
        &[],
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert_eq!(retained, BTreeSet::from([("0xaaa".to_owned(), 0)]));
}

#[test]
fn live_payload_records_successful_event_silent_resolver_call_observations() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let raw_block = RawBlock {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        parent_hash: Some("0xparent".to_owned()),
        block_number: 10,
        block_timestamp: OffsetDateTime::UNIX_EPOCH,
        canonicality_state: CanonicalityState::Canonical,
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
    };
    let transactions = vec![ProviderTransaction {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        from: "0x0000000000000000000000000000000000000001".to_owned(),
        to: Some("0xA2c122bE93b0074270EBee7f6B7292c7deb45047".to_owned()),
    }];
    let receipts = vec![ProviderReceipt {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        contract_address: None,
        status: Some(1),
        cumulative_gas_used: None,
        gas_used: None,
        logs_bloom: None,
    }];

    let observations = event_silent_resolver_call_observations_from_live_payload(
        "ethereum-mainnet",
        &raw_block,
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert_eq!(
        observations,
        vec![EventSilentResolverCallObservation {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_hash: "0xaaa".to_owned(),
            transaction_index: 0,
            canonicality_state: CanonicalityState::Canonical,
        }]
    );
}

#[test]
fn live_payload_does_not_retain_failed_direct_calls_without_selected_logs() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let transactions = vec![ProviderTransaction {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        from: "0x0000000000000000000000000000000000000001".to_owned(),
        to: Some("0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()),
    }];
    let receipts = vec![ProviderReceipt {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        contract_address: None,
        status: Some(0),
        cumulative_gas_used: None,
        gas_used: None,
        logs_bloom: None,
    }];

    let retained = retained_transaction_keys_from_live_payload(
        &[],
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert!(retained.is_empty());
}

#[test]
fn live_payload_keeps_transactions_with_selected_logs_even_when_receipt_failed() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let logs = vec![RawLog {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_hash: "0xaaa".to_owned(),
        transaction_index: 0,
        log_index: 0,
        emitting_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
        topics: vec![],
        data: vec![],
        canonicality_state: CanonicalityState::Canonical,
    }];
    let transactions = vec![ProviderTransaction {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        from: "0x0000000000000000000000000000000000000001".to_owned(),
        to: Some("0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()),
    }];
    let receipts = vec![ProviderReceipt {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        contract_address: None,
        status: Some(0),
        cumulative_gas_used: None,
        gas_used: None,
        logs_bloom: None,
    }];

    let retained = retained_transaction_keys_from_live_payload(
        &logs,
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert_eq!(retained, BTreeSet::from([("0xaaa".to_owned(), 0)]));
}
