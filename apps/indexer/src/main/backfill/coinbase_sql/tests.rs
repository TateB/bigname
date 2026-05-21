use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_manifests::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
};
use serde_json::json;
use sqlx::types::Uuid;

use super::{
    planner::build_filter_packs,
    push_deduped_log,
    query::{CoinbaseSqlFilterPack, build_or_split_filter_pack, build_query},
    rows::CoinbaseSqlLogRow,
};
use crate::{
    backfill::{
        BackfillBlockRange, BackfillTopicPlan, CoinbaseSqlValidationMode,
        HistoricalLogPayloadRequest, selection::SelectedTargetIntervalIndex,
    },
    provider::{ProviderLog, ProviderResolvedBlock},
};

fn pack(addresses: Vec<String>, topic0s: Vec<String>) -> CoinbaseSqlFilterPack {
    CoinbaseSqlFilterPack {
        chain: "base-mainnet".to_owned(),
        from_block: 10,
        to_block: 20,
        addresses,
        topic0s,
        scan_all_emitters: false,
        source_families: vec!["basenames_base_registry".to_owned()],
    }
}

#[test]
fn query_builder_batches_addresses_and_topics() -> Result<()> {
    let pack = pack(
        vec![
            "0x1111111111111111111111111111111111111111".to_owned(),
            "0x2222222222222222222222222222222222222222".to_owned(),
        ],
        vec![
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
    );

    let sql = build_query(&pack, None, 50_000)?;

    assert!(sql.contains("WITH active_transactions AS"));
    assert!(sql.contains("FROM base.events l"));
    assert!(sql.contains("FROM base.encoded_logs l"));
    assert!(sql.contains("JOIN active_transactions t"));
    assert!(sql.contains("t.transaction_index AS transaction_index"));
    assert!(!sql.contains(
        "l.transaction_index AS transaction_index,\n    l.log_index AS transaction_log_index"
    ));
    assert!(sql.contains("l.emitting_address IN ('0x1111111111111111111111111111111111111111', '0x2222222222222222222222222222222222222222')"));
    assert!(sql.contains("l.topics[1] IN ('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')"));
    assert!(sql.contains("toString(action) IN ('1', 'added')"));
    assert!(sql.contains("toString(l.action) IN ('-1', 'removed')"));
    assert!(sql.contains("row_number() OVER"));
    assert!(sql.contains("AND t.block_hash = l.block_hash"));
    assert!(sql.contains("HAVING sum(action) > 0"));
    assert!(sql.contains("ORDER BY l.block_number, l.transaction_index, l.log_index"));
    Ok(())
}

#[test]
fn query_splitter_keeps_queries_under_character_budget() -> Result<()> {
    let addresses = (0..32)
        .map(|index| format!("0x{index:040x}"))
        .collect::<Vec<_>>();
    let topic0s = (0..8)
        .map(|index| format!("0x{index:064x}"))
        .collect::<Vec<_>>();
    let char_limit = 5_000;
    let packs = build_or_split_filter_pack(pack(addresses, topic0s), char_limit, 50_000)?;

    assert!(packs.len() > 1);
    for pack in packs {
        assert!(build_query(&pack, None, 50_000)?.len() <= char_limit);
    }
    Ok(())
}

#[test]
fn planner_splits_same_address_when_source_family_topics_change() -> Result<()> {
    let address = "0x1111111111111111111111111111111111111111";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "family_a".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "family_b".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address.to_owned(),
                effective_from_block: 11,
                effective_to_block: 11,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 10,
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 11,
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
        },
    ];
    let selected_addresses = vec![address.to_owned()];
    let topic_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let topic_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([
            ("family_a".to_owned(), vec![topic_a.clone()]),
            ("family_b".to_owned(), vec![topic_b.clone()]),
        ]),
        BTreeSet::new(),
    );

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 11)?,
        validation_mode: CoinbaseSqlValidationMode::Full,
    });

    assert_eq!(packs.len(), 2);
    assert_eq!(packs[0].from_block, 10);
    assert_eq!(packs[0].to_block, 10);
    assert_eq!(packs[0].topic0s, vec![topic_a]);
    assert_eq!(packs[1].from_block, 11);
    assert_eq!(packs[1].to_block, 11);
    assert_eq!(packs[1].topic0s, vec![topic_b]);
    Ok(())
}

#[test]
fn planner_does_not_cartesian_product_addresses_and_topics() -> Result<()> {
    let address_a = "0x1111111111111111111111111111111111111111";
    let address_b = "0x2222222222222222222222222222222222222222";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "family_a".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address_a.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "family_b".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address_b.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address_a.to_owned(), address_b.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 10,
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    }];
    let selected_addresses = vec![address_a.to_owned(), address_b.to_owned()];
    let topic_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let topic_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([
            ("family_a".to_owned(), vec![topic_a.clone()]),
            ("family_b".to_owned(), vec![topic_b.clone()]),
        ]),
        BTreeSet::new(),
    );

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 10)?,
        validation_mode: CoinbaseSqlValidationMode::Full,
    });

    assert_eq!(packs.len(), 2);
    assert_eq!(packs[0].addresses, vec![address_a.to_owned()]);
    assert_eq!(packs[0].topic0s, vec![topic_a]);
    assert_eq!(packs[1].addresses, vec![address_b.to_owned()]);
    assert_eq!(packs[1].topic0s, vec![topic_b]);
    Ok(())
}

#[test]
fn duplicate_sql_pack_log_identities_are_deduped() {
    let log = ProviderLog {
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        block_number: 10,
        transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_owned(),
        transaction_index: 1,
        log_index: 2,
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        topics: vec![
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ],
        data: "0x".to_owned(),
    };
    let mut logs_by_block = BTreeMap::new();
    let mut seen = BTreeSet::new();

    push_deduped_log(&mut logs_by_block, &mut seen, log.clone());
    push_deduped_log(&mut logs_by_block, &mut seen, log);

    assert_eq!(logs_by_block[&10].len(), 1);
}

#[test]
fn row_validation_rejects_block_hash_mismatch() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "topics": ["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"],
        "data": "0x",
        "tx_from": "0x2222222222222222222222222222222222222222",
        "tx_to": "0x3333333333333333333333333333333333333333"
    }))?;
    let pack = pack(
        vec!["0x1111111111111111111111111111111111111111".to_owned()],
        vec!["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned()],
    );
    let resolved = BTreeMap::from([(
        10,
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
    )]);

    let error = row
        .validate_against_filter_pack(&pack, &resolved)
        .expect_err("mismatched validation-provider block hash must fail");
    assert!(format!("{error:?}").contains("validation provider resolved"));
    Ok(())
}
