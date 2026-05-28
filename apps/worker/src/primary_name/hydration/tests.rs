use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_execution::ens_namehash_hex;
use bigname_storage::{
    CanonicalityState, ENS_NAMESPACE, ETHEREUM_MAINNET_CHAIN_ID, NormalizedEvent,
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    default_database_url, load_primary_name_current_snapshot, upsert_normalized_events,
};
use futures_util::{FutureExt, future::BoxFuture};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::super::rebuild_primary_names_current;
use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for legacy reverse hydration tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_wpn_hyd_{}_{}_{}", std::process::id(), unique, sequence);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for legacy reverse hydration tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect legacy reverse hydration test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for legacy reverse hydration tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct MockReverseNameHydrationClient {
    outcomes_by_node: BTreeMap<String, ReverseNameHydrationOutcome>,
}

impl ReverseNameHydrationClient for MockReverseNameHydrationClient {
    fn hydrate<'a>(
        &'a self,
        _chain_id: &'a str,
        _position: &'a ReverseNameHydrationChainPosition,
        calls: &'a [ReverseNameHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<ReverseNameHydrationOutcome>>> {
        async move {
            calls
                .iter()
                .map(|call| {
                    self.outcomes_by_node
                        .get(&call.reverse_node)
                        .cloned()
                        .with_context(|| format!("missing mock outcome for {}", call.reverse_node))
                })
                .collect()
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
struct ResolverCheckingHydrationClient {
    expected_resolver_address: String,
    outcomes_by_node: BTreeMap<String, ReverseNameHydrationOutcome>,
}

impl ReverseNameHydrationClient for ResolverCheckingHydrationClient {
    fn hydrate<'a>(
        &'a self,
        _chain_id: &'a str,
        _position: &'a ReverseNameHydrationChainPosition,
        calls: &'a [ReverseNameHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<ReverseNameHydrationOutcome>>> {
        async move {
            calls
                .iter()
                .map(|call| {
                    if call.resolver_address != self.expected_resolver_address {
                        anyhow::bail!(
                            "expected resolver {}, got {}",
                            self.expected_resolver_address,
                            call.resolver_address
                        );
                    }
                    self.outcomes_by_node
                        .get(&call.reverse_node)
                        .cloned()
                        .with_context(|| format!("missing mock outcome for {}", call.reverse_node))
                })
                .collect()
        }
        .boxed()
    }
}

#[derive(Clone, Debug, Default)]
struct ForwardCheckingHydrationClient {
    outcomes_by_node: BTreeMap<String, ReverseNameHydrationOutcome>,
    forward_addresses_by_name: BTreeMap<String, Option<String>>,
    forward_errors_by_name: BTreeMap<String, ForwardLookupError>,
}

#[derive(Clone, Debug)]
enum ForwardLookupError {
    PrimaryName {
        message: String,
        plain_execution_revert: bool,
        offchain_lookup_required: bool,
    },
}

impl ReverseNameHydrationClient for ForwardCheckingHydrationClient {
    fn hydrate<'a>(
        &'a self,
        _chain_id: &'a str,
        _position: &'a ReverseNameHydrationChainPosition,
        calls: &'a [ReverseNameHydrationCall],
    ) -> BoxFuture<'a, Result<Vec<ReverseNameHydrationOutcome>>> {
        async move {
            calls
                .iter()
                .map(|call| {
                    self.outcomes_by_node
                        .get(&call.reverse_node)
                        .cloned()
                        .with_context(|| format!("missing mock outcome for {}", call.reverse_node))
                })
                .collect()
        }
        .boxed()
    }

    fn lookup_forward_address<'a>(
        &'a self,
        _chain_id: &'a str,
        _position: &'a ReverseNameHydrationChainPosition,
        normalized_name: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        async move {
            if let Some(error) = self.forward_errors_by_name.get(normalized_name) {
                return match error {
                    ForwardLookupError::PrimaryName {
                        message,
                        plain_execution_revert,
                        offchain_lookup_required,
                    } => Err(anyhow::Error::from(
                        bigname_execution::OnDemandEnsPrimaryNameError::synthetic_execution_rpc_error_for_tests(
                            message.clone(),
                            *plain_execution_revert,
                            *offchain_lookup_required,
                        ),
                    )),
                };
            }
            self.forward_addresses_by_name
                .get(normalized_name)
                .cloned()
                .with_context(|| format!("missing mock forward address for {normalized_name}"))
        }
        .boxed()
    }
}

#[tokio::test]
async fn upserts_hydration_snapshots_in_bounded_batches() -> Result<()> {
    let database = TestDatabase::new().await?;
    let snapshots = (0..3)
        .map(|index| PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: format!("0x{index:040x}"),
                namespace: ENS_NAMESPACE.to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
                    "test_index": index,
                }),
            },
            normalized_claim_name: Some(format!("batch-{index}.eth")),
        })
        .collect::<Vec<_>>();

    let upserted_row_count =
        upsert_hydration_snapshots_in_batches(database.pool(), &snapshots, 2).await?;
    assert_eq!(upserted_row_count, 3);

    for snapshot in &snapshots {
        let persisted = load_primary_name_current_snapshot(
            database.pool(),
            &snapshot.row.address,
            &snapshot.row.namespace,
            &snapshot.row.coin_type,
        )
        .await?
        .expect("bounded hydration upsert should persist row");
        assert_eq!(persisted, *snapshot);
    }

    let zero_batch_error = upsert_hydration_snapshots_in_batches(database.pool(), &snapshots, 0)
        .await
        .expect_err("zero batch size should be rejected");
    assert!(
        format!("{zero_batch_error:#}")
            .contains("legacy reverse hydration upsert batch size must be positive")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rerun_repairs_partial_hydration_snapshot_publish() -> Result<()> {
    let database = TestDatabase::new().await?;
    let snapshots = (0..3)
        .map(|index| PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: format!("0x{:040x}", index + 10),
                namespace: ENS_NAMESPACE.to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
                    "test_index": index,
                }),
            },
            normalized_claim_name: Some(format!("repair-{index}.eth")),
        })
        .collect::<Vec<_>>();

    let partial_count =
        upsert_hydration_snapshots_in_batches(database.pool(), &snapshots[..2], 2).await?;
    assert_eq!(partial_count, 2);
    assert!(
        load_primary_name_current_snapshot(
            database.pool(),
            &snapshots[2].row.address,
            &snapshots[2].row.namespace,
            &snapshots[2].row.coin_type,
        )
        .await?
        .is_none()
    );

    let repaired_count =
        upsert_hydration_snapshots_in_batches(database.pool(), &snapshots, 2).await?;
    assert_eq!(repaired_count, 3);
    for snapshot in &snapshots {
        let persisted = load_primary_name_current_snapshot(
            database.pool(),
            &snapshot.row.address,
            &snapshot.row.namespace,
            &snapshot.row.coin_type,
        )
        .await?
        .expect("rerun should repair missing hydration row");
        assert_eq!(persisted, *snapshot);
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hydrates_current_legacy_reverse_resolver_primary_name() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000aaa";
    let reverse_block = 100;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let before = load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
        .await?
        .expect("reverse claim should create primary_names_current row");
    assert_eq!(before.row.claim_status, PrimaryNameClaimStatus::NotFound);
    assert_eq!(before.normalized_claim_name, None);

    let client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("Vitalik.eth".to_owned()),
        )]),
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &client,
    )
    .await?;
    assert_eq!(
        summary,
        PrimaryNameLegacyReverseHydrationSummary {
            candidate_tuple_count: 1,
            queried_tuple_count: 1,
            upserted_row_count: 1,
            deleted_row_count: 0,
            success_row_count: 1,
            not_found_row_count: 0,
            invalid_name_row_count: 0,
            failed_lookup_count: 0,
        }
    );

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("hydration should preserve primary_names_current row");
    assert_eq!(hydrated.row.claim_status, PrimaryNameClaimStatus::Success);
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("vitalik.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY],
        json!({
            "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
            "derivation_kind": DERIVATION_KIND_LEGACY_REVERSE_RESOLVER_HYDRATION,
            "tuple_source": TUPLE_SOURCE_REVERSE_CLAIM,
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "resolver_address": LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            "reverse_node": reverse_node,
            "block_number": 300,
            "block_hash": block_hash_for(300),
            "latest_successful_call_block_number": Value::Null,
            "latest_successful_call_block_hash": Value::Null,
            "latest_successful_call_transaction_hash": Value::Null,
            "latest_successful_call_transaction_index": Value::Null,
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hydrates_resolver_edge_only_legacy_reverse_resolver_primary_name() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            generic_reverse_resolver_changed_event(
                "resolver-edge-only",
                &reverse_node,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
            named_non_reverse_resolver_changed_event(
                "named-non-reverse-resolver-edge",
                "ens:vitalik.eth",
                "0xee6c4522d8d8a00e60990788803eea11e0408ed6cb672574ab9fbf0b389a558f",
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                121,
                1,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let before = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?;
    assert_eq!(before, None);

    let client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("Vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            Some(address.to_owned()),
        )]),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &client,
    )
    .await?;
    assert_eq!(
        summary,
        PrimaryNameLegacyReverseHydrationSummary {
            candidate_tuple_count: 1,
            queried_tuple_count: 1,
            upserted_row_count: 1,
            deleted_row_count: 0,
            success_row_count: 1,
            not_found_row_count: 0,
            invalid_name_row_count: 0,
            failed_lookup_count: 0,
        }
    );

    let hydrated = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?
    .expect("resolver-edge hydration should create primary_names_current row");
    assert_eq!(hydrated.row.claim_status, PrimaryNameClaimStatus::Success);
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("vitalik.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance["tuple_source"],
        json!(TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["tuple_source"],
        json!(TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["reverse_node"],
        json!(reverse_node)
    );
    assert_eq!(
        hydrated.row.claim_provenance["verified_primary_name_invalidation"]["primary_claim_source"]
            ["tuple_source"],
        json!(TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deletes_resolver_edge_row_when_forward_confirmation_stops_matching() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[generic_reverse_resolver_changed_event(
            "resolver-edge-only",
            &reverse_node,
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            120,
            0,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("Vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            Some(address.to_owned()),
        )]),
        ..Default::default()
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    insert_chain_checkpoint(database.pool(), 350).await?;
    let mismatch_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            Some("0x0000000000000000000000000000000000000bad".to_owned()),
        )]),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &mismatch_client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);
    assert_eq!(summary.queried_tuple_count, 1);
    assert_eq!(summary.upserted_row_count, 0);
    assert_eq!(summary.deleted_row_count, 1);
    assert_eq!(summary.failed_lookup_count, 0);

    let deleted = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?;
    assert_eq!(deleted, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn keeps_resolver_edge_row_when_forward_confirmation_errors() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[generic_reverse_resolver_changed_event(
            "resolver-edge-only",
            &reverse_node,
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            120,
            0,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("Vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            Some(address.to_owned()),
        )]),
        ..Default::default()
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    insert_chain_checkpoint(database.pool(), 350).await?;
    let failing_forward_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::new(),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &failing_forward_client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);
    assert_eq!(summary.queried_tuple_count, 1);
    assert_eq!(summary.upserted_row_count, 0);
    assert_eq!(summary.deleted_row_count, 0);
    assert_eq!(summary.failed_lookup_count, 1);

    let retained = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?
    .expect("transient forward-confirmation failure should retain previous row");
    assert_eq!(
        retained.normalized_claim_name.as_deref(),
        Some("vitalik.eth")
    );
    assert_eq!(
        retained.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(300)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn treats_universal_resolver_revert_as_resolver_edge_non_confirmation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[generic_reverse_resolver_changed_event(
            "resolver-edge-only",
            &reverse_node,
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            120,
            0,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("missing-forward.eth".to_owned()),
        )]),
        forward_errors_by_name: BTreeMap::from([(
            "missing-forward.eth".to_owned(),
            ForwardLookupError::PrimaryName {
                message: "ENS primary-name RPC call failed: execution reverted".to_owned(),
                plain_execution_revert: true,
                offchain_lookup_required: false,
            },
        )]),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);
    assert_eq!(summary.queried_tuple_count, 1);
    assert_eq!(summary.upserted_row_count, 0);
    assert_eq!(summary.deleted_row_count, 0);
    assert_eq!(summary.failed_lookup_count, 0);

    let skipped = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?;
    assert_eq!(skipped, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn keeps_resolver_edge_row_when_forward_confirmation_requires_offchain_lookup() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[generic_reverse_resolver_changed_event(
            "resolver-edge-only",
            &reverse_node,
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            120,
            0,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("Vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            Some(address.to_owned()),
        )]),
        ..Default::default()
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    insert_chain_checkpoint(database.pool(), 350).await?;
    let offchain_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("vitalik.eth".to_owned()),
        )]),
        forward_errors_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            ForwardLookupError::PrimaryName {
                message:
                    "ENS primary-name RPC call failed: execution reverted (OffchainLookup required)"
                        .to_owned(),
                plain_execution_revert: false,
                offchain_lookup_required: true,
            },
        )]),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &offchain_client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);
    assert_eq!(summary.queried_tuple_count, 1);
    assert_eq!(summary.upserted_row_count, 0);
    assert_eq!(summary.deleted_row_count, 0);
    assert_eq!(summary.failed_lookup_count, 1);

    let retained = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?
    .expect("offchain-required forward confirmation should retain previous row");
    assert_eq!(
        retained.normalized_claim_name.as_deref(),
        Some("vitalik.eth")
    );
    assert_eq!(
        retained.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(300)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn treats_offchain_required_new_resolver_edge_as_non_confirmation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[generic_reverse_resolver_changed_event(
            "resolver-edge-only",
            &reverse_node,
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            120,
            0,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("ccip-only.eth".to_owned()),
        )]),
        forward_errors_by_name: BTreeMap::from([(
            "ccip-only.eth".to_owned(),
            ForwardLookupError::PrimaryName {
                message:
                    "ENS primary-name RPC call failed: execution reverted (OffchainLookup required)"
                        .to_owned(),
                plain_execution_revert: false,
                offchain_lookup_required: true,
            },
        )]),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);
    assert_eq!(summary.queried_tuple_count, 1);
    assert_eq!(summary.upserted_row_count, 0);
    assert_eq!(summary.deleted_row_count, 0);
    assert_eq!(summary.failed_lookup_count, 0);

    let skipped = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?;
    assert_eq!(skipped, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn keeps_resolver_edge_row_when_checkpoint_is_missing() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let reverse_node = reverse_node_for_address(address)?;

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[generic_reverse_resolver_changed_event(
            "resolver-edge-only",
            &reverse_node,
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
            120,
            0,
        )],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("Vitalik.eth".to_owned()),
        )]),
        forward_addresses_by_name: BTreeMap::from([(
            "vitalik.eth".to_owned(),
            Some(address.to_owned()),
        )]),
        ..Default::default()
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    delete_chain_checkpoint(database.pool()).await?;
    let no_checkpoint_client = ForwardCheckingHydrationClient {
        outcomes_by_node: BTreeMap::new(),
        forward_addresses_by_name: BTreeMap::new(),
        ..Default::default()
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &no_checkpoint_client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 0);
    assert_eq!(summary.deleted_row_count, 0);

    let retained = load_primary_name_current_snapshot(
        database.pool(),
        &normalized_address,
        ENS_NAMESPACE,
        "60",
    )
    .await?
    .expect("missing checkpoint should not stale-delete existing resolver-edge row");
    assert_eq!(
        retained.normalized_claim_name.as_deref(),
        Some("vitalik.eth")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rehydrates_after_new_successful_live_call_to_legacy_resolver() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000bbb";
    let reverse_block = 101;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("first.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    let skipped = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;
    assert_eq!(skipped.candidate_tuple_count, 0);

    insert_chain_checkpoint(database.pool(), 350).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;
    let second_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("second.eth".to_owned()),
        )]),
    };
    let refreshed = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &second_client,
    )
    .await?;
    assert_eq!(refreshed.candidate_tuple_count, 1);
    assert_eq!(refreshed.upserted_row_count, 1);

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("hydration should preserve primary_names_current row");
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("second.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_number"],
        json!(320)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_hash"],
        json!(block_hash_for(320))
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_transaction_hash"],
        json!(transaction_hash_for(320))
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_transaction_index"],
        json!(0)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(350)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ignores_live_call_observation_ahead_of_hydration_checkpoint_until_checkpoint_catches_up()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000333";
    let reverse_block = 108;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;

    let before_checkpoint_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("before-checkpoint.eth".to_owned()),
        )]),
    };
    let before_checkpoint = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &before_checkpoint_client,
    )
    .await?;
    assert_eq!(before_checkpoint.candidate_tuple_count, 1);
    assert_eq!(before_checkpoint.upserted_row_count, 1);

    let hydrated_before =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("hydration should preserve primary_names_current row");
    assert_eq!(
        hydrated_before.normalized_claim_name.as_deref(),
        Some("before-checkpoint.eth")
    );
    assert_eq!(
        hydrated_before.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(300)
    );
    assert_eq!(
        hydrated_before.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_number"],
        Value::Null
    );

    let skipped = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &before_checkpoint_client,
    )
    .await?;
    assert_eq!(skipped.candidate_tuple_count, 0);

    insert_chain_checkpoint(database.pool(), 350).await?;
    let after_checkpoint_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("after-checkpoint.eth".to_owned()),
        )]),
    };
    let after_checkpoint = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &after_checkpoint_client,
    )
    .await?;
    assert_eq!(after_checkpoint.candidate_tuple_count, 1);
    assert_eq!(after_checkpoint.upserted_row_count, 1);

    let hydrated_after =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("checkpoint-caught-up hydration should preserve row");
    assert_eq!(
        hydrated_after.normalized_claim_name.as_deref(),
        Some("after-checkpoint.eth")
    );
    assert_eq!(
        hydrated_after.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(350)
    );
    assert_eq!(
        hydrated_after.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_number"],
        json!(320)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn legacy_reverse_resolver_call_trigger_waits_for_hydration_checkpoint() -> Result<()> {
    let database = TestDatabase::new().await?;
    let config = PrimaryNameLegacyReverseHydrationConfig::new(
        bigname_execution::ChainRpcUrls::from_entries(&[format!(
            "{ETHEREUM_MAINNET_CHAIN_ID}=http://127.0.0.1:1"
        )])?,
    );

    insert_chain_checkpoint(database.pool(), 300).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;

    let before_checkpoint =
        load_legacy_reverse_resolver_call_triggers(database.pool(), &config).await?;
    assert_eq!(before_checkpoint, Vec::new());

    insert_chain_checkpoint(database.pool(), 350).await?;
    let after_checkpoint =
        load_legacy_reverse_resolver_call_triggers(database.pool(), &config).await?;
    assert_eq!(
        after_checkpoint,
        vec![PrimaryNameLegacyReverseHydrationTrigger {
            resolver_address: LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned(),
            block_number: 320,
            block_hash: block_hash_for(320),
            transaction_hash: transaction_hash_for(320),
            transaction_index: 0,
        }]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn legacy_reverse_resolver_call_triggers_track_each_configured_resolver() -> Result<()> {
    let database = TestDatabase::new().await?;
    let other_resolver = "0x0000000000000000000000000000000000000123";
    let mut config = PrimaryNameLegacyReverseHydrationConfig::new(
        bigname_execution::ChainRpcUrls::from_entries(&[format!(
            "{ETHEREUM_MAINNET_CHAIN_ID}=http://127.0.0.1:1"
        )])?,
    );
    config.resolver_addresses = vec![
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned(),
        other_resolver.to_owned(),
    ];

    insert_chain_checkpoint(database.pool(), 400).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        330,
    )
    .await?;
    insert_successful_direct_call(database.pool(), other_resolver, 310).await?;

    let triggers = load_legacy_reverse_resolver_call_triggers(database.pool(), &config).await?;
    assert_eq!(
        triggers,
        vec![
            PrimaryNameLegacyReverseHydrationTrigger {
                resolver_address: bigname_storage::normalize_evm_address(other_resolver),
                block_number: 310,
                block_hash: block_hash_for(310),
                transaction_hash: transaction_hash_for(310),
                transaction_index: 0,
            },
            PrimaryNameLegacyReverseHydrationTrigger {
                resolver_address: LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned(),
                block_number: 330,
                block_hash: block_hash_for(330),
                transaction_hash: transaction_hash_for(330),
                transaction_index: 0,
            },
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ignores_future_reverse_and_resolver_events_until_hydration_checkpoint_catches_up()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000444";
    let initial_reverse_block = 109;
    let future_reverse_block = 320;
    let initial_reverse_node = reverse_node_for_block(initial_reverse_block);
    let future_reverse_node = reverse_node_for_block(future_reverse_block);
    let replacement_resolver = "0x00000000000000000000000000000000000000f4";

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, initial_reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                initial_reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            initial_reverse_node,
            ReverseNameHydrationOutcome::Success("before-future-events.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("future-reverse-claim", address, future_reverse_block, 0),
            reverse_resolver_changed_event(
                "future-reverse-resolver",
                address,
                future_reverse_block,
                replacement_resolver,
                future_reverse_block,
                1,
            ),
        ],
    )
    .await?;
    let skipped = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned(),
            replacement_resolver.to_owned(),
        ],
        &first_client,
    )
    .await?;
    assert_eq!(skipped.candidate_tuple_count, 0);

    insert_chain_checkpoint(database.pool(), 350).await?;
    let replacement_client = ResolverCheckingHydrationClient {
        expected_resolver_address: replacement_resolver.to_owned(),
        outcomes_by_node: BTreeMap::from([(
            future_reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("after-future-events.eth".to_owned()),
        )]),
    };
    let refreshed = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned(),
            replacement_resolver.to_owned(),
        ],
        &replacement_client,
    )
    .await?;
    assert_eq!(refreshed.candidate_tuple_count, 1);
    assert_eq!(refreshed.queried_tuple_count, 1);
    assert_eq!(refreshed.upserted_row_count, 1);

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("checkpoint-caught-up hydration should preserve row");
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("after-future-events.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["resolver_address"],
        json!(replacement_resolver)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["reverse_node"],
        json!(future_reverse_node)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(350)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn restores_event_replayed_row_when_resolver_changes_away_from_configured_set() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000eee";
    let reverse_block = 104;
    let reverse_node = reverse_node_for_block(reverse_block);
    let unconfigured_resolver = "0x00000000000000000000000000000000000000f1";

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("stale.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[reverse_resolver_changed_event(
            "reverse-resolver-away",
            address,
            reverse_block,
            unconfigured_resolver,
            130,
            0,
        )],
    )
    .await?;
    let restore_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::new(),
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &restore_client,
    )
    .await?;
    assert_eq!(
        summary,
        PrimaryNameLegacyReverseHydrationSummary {
            candidate_tuple_count: 1,
            queried_tuple_count: 0,
            upserted_row_count: 1,
            deleted_row_count: 0,
            success_row_count: 0,
            not_found_row_count: 1,
            invalid_name_row_count: 0,
            failed_lookup_count: 0,
        }
    );

    let restored =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("restored row should preserve primary_names_current tuple");
    assert_eq!(restored.row.claim_status, PrimaryNameClaimStatus::NotFound);
    assert_eq!(restored.normalized_claim_name, None);
    assert!(
        restored
            .row
            .claim_provenance
            .get(HYDRATION_PROVENANCE_KEY)
            .is_none()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rehydrates_when_current_resolver_changes_to_another_configured_address() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000fff";
    let reverse_block = 105;
    let reverse_node = reverse_node_for_block(reverse_block);
    let replacement_resolver = "0x00000000000000000000000000000000000000f2";

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("before.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    insert_chain_checkpoint(database.pool(), 360).await?;
    upsert_normalized_events(
        database.pool(),
        &[reverse_resolver_changed_event(
            "reverse-resolver-replacement",
            address,
            reverse_block,
            replacement_resolver,
            140,
            0,
        )],
    )
    .await?;
    let replacement_client = ResolverCheckingHydrationClient {
        expected_resolver_address: replacement_resolver.to_owned(),
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("after.eth".to_owned()),
        )]),
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[
            LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned(),
            replacement_resolver.to_owned(),
        ],
        &replacement_client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);
    assert_eq!(summary.queried_tuple_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("replacement resolver hydration should preserve row");
    assert_eq!(hydrated.normalized_claim_name.as_deref(), Some("after.eth"));
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["resolver_address"],
        json!(replacement_resolver)
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_number"],
        Value::Null
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(360)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn failed_rehydration_after_previous_success_restores_event_replayed_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000111";
    let reverse_block = 106;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 300).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;

    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("before-failure.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    insert_chain_checkpoint(database.pool(), 350).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;
    let failing_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Failed("provider reverted".to_owned()),
        )]),
    };
    let failed = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &failing_client,
    )
    .await?;
    assert_eq!(
        failed,
        PrimaryNameLegacyReverseHydrationSummary {
            candidate_tuple_count: 1,
            queried_tuple_count: 1,
            upserted_row_count: 1,
            deleted_row_count: 0,
            success_row_count: 0,
            not_found_row_count: 1,
            invalid_name_row_count: 0,
            failed_lookup_count: 1,
        }
    );

    let restored =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("failed rehydration should keep event-replayed row");
    assert_eq!(restored.row.claim_status, PrimaryNameClaimStatus::NotFound);
    assert_eq!(restored.normalized_claim_name, None);
    assert!(
        restored
            .row
            .claim_provenance
            .get(HYDRATION_PROVENANCE_KEY)
            .is_none()
    );

    let repeated_failure = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &failing_client,
    )
    .await?;
    assert_eq!(repeated_failure.candidate_tuple_count, 1);
    assert_eq!(repeated_failure.failed_lookup_count, 1);
    assert_eq!(repeated_failure.upserted_row_count, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rehydrates_when_latest_direct_call_observation_disappears_after_reorg() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000222";
    let reverse_block = 107;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 340).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                120,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;
    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("with-trigger.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    orphan_event_silent_call_observation(database.pool(), &block_hash_for(320)).await?;
    insert_chain_checkpoint(database.pool(), 360).await?;

    let second_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("after-orphan.eth".to_owned()),
        )]),
    };
    let refreshed = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &second_client,
    )
    .await?;
    assert_eq!(refreshed.candidate_tuple_count, 1);
    assert_eq!(refreshed.upserted_row_count, 1);

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("hydration should preserve primary_names_current row");
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("after-orphan.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_number"],
        Value::Null
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_hash"],
        Value::Null
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["block_number"],
        json!(360)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hydrates_from_durable_call_observation_after_raw_staging_compaction() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000ccc";
    let reverse_block = 102;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 330).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                121,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;
    delete_raw_direct_call_staging(database.pool()).await?;

    let client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("compacted.eth".to_owned()),
        )]),
    };
    let summary = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &client,
    )
    .await?;
    assert_eq!(summary.candidate_tuple_count, 1);

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("hydration should preserve primary_names_current row");
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("compacted.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_hash"],
        json!(block_hash_for(320))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rehydrates_when_live_call_trigger_reorgs_at_same_height() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000ddd";
    let reverse_block = 103;
    let reverse_node = reverse_node_for_block(reverse_block);

    insert_chain_checkpoint(database.pool(), 340).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            reverse_changed_event("reverse-claim", address, reverse_block, 0),
            reverse_resolver_changed_event(
                "reverse-resolver",
                address,
                reverse_block,
                LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
                122,
                0,
            ),
        ],
    )
    .await?;
    rebuild_primary_names_current(database.pool(), None, None, None).await?;
    insert_successful_direct_call(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
    )
    .await?;
    let first_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node.clone(),
            ReverseNameHydrationOutcome::Success("branch-a.eth".to_owned()),
        )]),
    };
    hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &first_client,
    )
    .await?;

    orphan_event_silent_call_observation(database.pool(), &block_hash_for(320)).await?;
    insert_successful_direct_call_with_identity(
        database.pool(),
        LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS,
        320,
        "0x000000000000000000000000000000000000000000000000000000000000beef",
        "0xtx-reorg",
        1,
    )
    .await?;
    insert_chain_checkpoint(database.pool(), 360).await?;

    let second_client = MockReverseNameHydrationClient {
        outcomes_by_node: BTreeMap::from([(
            reverse_node,
            ReverseNameHydrationOutcome::Success("branch-b.eth".to_owned()),
        )]),
    };
    let refreshed = hydrate_legacy_reverse_resolver_primary_names_with_client(
        database.pool(),
        &[LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESS.to_owned()],
        &second_client,
    )
    .await?;
    assert_eq!(refreshed.candidate_tuple_count, 1);

    let hydrated =
        load_primary_name_current_snapshot(database.pool(), address, ENS_NAMESPACE, "60")
            .await?
            .expect("hydration should preserve primary_names_current row");
    assert_eq!(
        hydrated.normalized_claim_name.as_deref(),
        Some("branch-b.eth")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_block_hash"],
        json!("0x000000000000000000000000000000000000000000000000000000000000beef")
    );
    assert_eq!(
        hydrated.row.claim_provenance[HYDRATION_PROVENANCE_KEY]["latest_successful_call_transaction_hash"],
        json!("0xtx-reorg")
    );

    database.cleanup().await?;
    Ok(())
}

fn reverse_changed_event(
    event_identity: &str,
    address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash_for(block_number)),
        transaction_hash: Some(format!("0xtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_reverse_claim".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "source_event": "ReverseClaimed",
            "address": normalized_address,
            "coin_type": "60",
            "namespace": ENS_NAMESPACE,
            "reverse_namespace": ENS_NAMESPACE,
            "reverse_label": reverse_label,
            "reverse_name": format!("{reverse_label}.addr.reverse"),
            "reverse_node": reverse_node_for_block(block_number),
            "claim_provenance": claim_provenance(block_number),
        }),
    }
}

fn generic_reverse_resolver_changed_event(
    event_identity: &str,
    reverse_node: &str,
    resolver_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash_for(block_number)),
        transaction_hash: Some(format!("0xtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": reverse_node,
        }),
    }
}

fn named_non_reverse_resolver_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    namehash: &str,
    resolver_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: None,
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash_for(block_number)),
        transaction_hash: Some(format!("0xtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": namehash,
        }),
    }
}

fn reverse_resolver_changed_event(
    event_identity: &str,
    address: &str,
    reverse_block_number: i64,
    resolver_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash_for(block_number)),
        transaction_hash: Some(format!("0xtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": reverse_node_for_block(reverse_block_number),
            "primary_claim_source": {
                "address": normalized_address,
                "namespace": ENS_NAMESPACE,
                "coin_type": "60",
                "reverse_name": format!("{reverse_label}.addr.reverse"),
                "reverse_node": reverse_node_for_block(reverse_block_number),
                "claim_provenance": claim_provenance(reverse_block_number),
            },
        }),
    }
}

async fn insert_chain_checkpoint(pool: &PgPool, block_number: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (chain_id, canonical_block_hash, canonical_block_number)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id) DO UPDATE
        SET
            canonical_block_hash = EXCLUDED.canonical_block_hash,
            canonical_block_number = EXCLUDED.canonical_block_number
        "#,
    )
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(block_hash_for(block_number))
    .bind(block_number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn delete_chain_checkpoint(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM chain_checkpoints WHERE chain_id = $1")
        .bind(ETHEREUM_MAINNET_CHAIN_ID)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_successful_direct_call(
    pool: &PgPool,
    resolver_address: &str,
    block_number: i64,
) -> Result<()> {
    insert_successful_direct_call_with_identity(
        pool,
        resolver_address,
        block_number,
        &block_hash_for(block_number),
        &transaction_hash_for(block_number),
        0,
    )
    .await
}

async fn insert_successful_direct_call_with_identity(
    pool: &PgPool,
    resolver_address: &str,
    block_number: i64,
    block_hash: &str,
    transaction_hash: &str,
    transaction_index: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_transactions (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'canonical'::canonicality_state)
        "#,
    )
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(block_hash)
    .bind(block_number)
    .bind(transaction_hash)
    .bind(transaction_index)
    .bind("0x0000000000000000000000000000000000000001")
    .bind(resolver_address)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO event_silent_resolver_call_observations (
            chain_id,
            resolver_address,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'canonical'::canonicality_state)
        "#,
    )
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(resolver_address)
    .bind(block_hash)
    .bind(block_number)
    .bind(transaction_hash)
    .bind(transaction_index)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_receipts (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            status,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, TRUE, 'canonical'::canonicality_state)
        "#,
    )
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(block_hash)
    .bind(block_number)
    .bind(transaction_hash)
    .bind(transaction_index)
    .execute(pool)
    .await?;
    Ok(())
}

async fn delete_raw_direct_call_staging(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM raw_transactions")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM raw_receipts")
        .execute(pool)
        .await?;
    Ok(())
}

async fn orphan_event_silent_call_observation(pool: &PgPool, block_hash: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE event_silent_resolver_call_observations
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(block_hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn claim_provenance(block_number: i64) -> Value {
    json!({
        "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
        "contract_role": "reverse_registrar",
        "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
        "emitting_address": "0x00000000000000000000000000000000000000ad",
    })
}

fn reverse_node_for_block(block_number: i64) -> String {
    format!("0x{block_number:064x}")
}

fn reverse_node_for_address(address: &str) -> Result<String> {
    let normalized_address = bigname_storage::normalize_evm_address(address);
    let label = normalized_address
        .strip_prefix("0x")
        .context("test address must be 0x-prefixed")?;
    ens_namehash_hex(&format!("{label}.addr.reverse"))
}

fn block_hash_for(block_number: i64) -> String {
    format!("0x{block_number:064x}")
}

fn transaction_hash_for(block_number: i64) -> String {
    format!("0xtx{block_number:064x}")
}
