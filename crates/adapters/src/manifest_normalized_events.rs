use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

const DERIVATION_KIND_MANIFEST_SYNC: &str = "manifest_sync";
const EVENT_KIND_SOURCE_MANIFEST_UPDATED: &str = "SourceManifestUpdated";
const EVENT_KIND_CAPABILITY_CHANGED: &str = "CapabilityChanged";
const EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED: &str = "ProxyImplementationChanged";

/// Sync summary for normalized events derived from stored active manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventSyncSummary {
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, ManifestNormalizedEventKindSyncSummary>,
}

/// Per-kind sync summary for logging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

#[derive(Clone, Debug)]
struct ActiveManifestRow {
    manifest_id: i64,
    manifest_version: i64,
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    normalizer_version: String,
}

#[derive(Clone, Debug)]
struct ActiveCapabilityRow {
    capability_name: String,
    status: String,
    notes: Option<String>,
}

#[derive(Clone, Debug)]
struct ActiveContractRow {
    role: String,
    address: String,
    proxy_kind: String,
    implementation: String,
}

/// Sync manifest-derived normalized events from stored active manifest state.
pub async fn sync_manifest_normalized_events(
    pool: &PgPool,
) -> Result<ManifestNormalizedEventSyncSummary> {
    let manifests = load_active_manifests(pool).await?;
    if manifests.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let capabilities = load_active_capabilities(pool).await?;
    let contracts = load_active_proxy_contracts(pool).await?;
    let before_counts = load_normalized_event_counts_by_kind(pool).await?;
    let events = build_normalized_events(&manifests, &capabilities, &contracts)?;

    if events.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let synced_by_kind = count_events_by_kind(&events);
    upsert_normalized_events(pool, &events).await?;
    let after_counts = load_normalized_event_counts_by_kind(pool).await?;

    let mut by_kind = BTreeMap::new();
    let mut total_inserted_count = 0;
    for (kind, synced_count) in synced_by_kind {
        let inserted_count = after_counts
            .get(&kind)
            .copied()
            .unwrap_or(0)
            .saturating_sub(before_counts.get(&kind).copied().unwrap_or(0));
        total_inserted_count += inserted_count;
        by_kind.insert(
            kind,
            ManifestNormalizedEventKindSyncSummary {
                synced_count,
                inserted_count,
            },
        );
    }

    Ok(ManifestNormalizedEventSyncSummary {
        total_synced_count: events.len(),
        total_inserted_count,
        by_kind,
    })
}

fn build_normalized_events(
    manifests: &[ActiveManifestRow],
    capabilities: &HashMap<i64, Vec<ActiveCapabilityRow>>,
    contracts: &HashMap<i64, Vec<ActiveContractRow>>,
) -> Result<Vec<NormalizedEvent>> {
    let mut events = Vec::new();

    for manifest in manifests {
        events.push(build_source_manifest_updated_event(manifest)?);

        if let Some(capability_rows) = capabilities.get(&manifest.manifest_id) {
            for capability in capability_rows {
                events.push(build_capability_changed_event(manifest, capability)?);
            }
        }

        if let Some(contract_rows) = contracts.get(&manifest.manifest_id) {
            for contract in contract_rows {
                events.push(build_proxy_implementation_changed_event(
                    manifest, contract,
                )?);
            }
        }
    }

    Ok(events)
}

fn build_source_manifest_updated_event(manifest: &ActiveManifestRow) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let deployment_epoch = manifest.deployment_epoch.clone();
    let normalizer_version = manifest.normalizer_version.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:source_manifest_updated",
            json!([
                manifest.manifest_id,
                manifest.manifest_version,
                namespace.clone(),
                source_family.clone(),
                chain.clone(),
                deployment_epoch.clone(),
                normalizer_version.clone(),
            ]),
        )?,
        namespace: namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
        source_family: source_family.clone(),
        manifest_version: manifest.manifest_version,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "namespace": namespace.clone(),
            "source_family": source_family.clone(),
            "chain": chain.clone(),
            "deployment_epoch": deployment_epoch.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "manifest_version": manifest.manifest_version,
            "normalizer_version": normalizer_version,
        }),
    })
}

fn build_capability_changed_event(
    manifest: &ActiveManifestRow,
    capability: &ActiveCapabilityRow,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let capability_name = capability.capability_name.clone();
    let status = capability.status.clone();
    let notes = capability.notes.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:capability_changed",
            json!([
                manifest.manifest_id,
                capability_name.clone(),
                status.clone(),
                notes.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest.manifest_version,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "capability_name": capability_name.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "capability_name": capability_name,
            "status": status,
            "notes": notes,
        }),
    })
}

fn build_proxy_implementation_changed_event(
    manifest: &ActiveManifestRow,
    contract: &ActiveContractRow,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let role = contract.role.clone();
    let address = contract.address.clone();
    let proxy_kind = contract.proxy_kind.clone();
    let implementation = contract.implementation.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:proxy_implementation_changed",
            json!([
                manifest.manifest_id,
                role.clone(),
                address.clone(),
                proxy_kind.clone(),
                implementation.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest.manifest_version,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "role": role.clone(),
            "address": address.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "role": role,
            "address": address,
            "proxy_kind": proxy_kind,
            "implementation": implementation,
        }),
    })
}

fn event_identity(prefix: &str, key: Value) -> Result<String> {
    Ok(format!(
        "{prefix}:{}",
        serde_json::to_string(&key).context("failed to serialize normalized-event identity")?
    ))
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

async fn load_active_manifests(pool: &PgPool) -> Result<Vec<ActiveManifestRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            manifest_id,
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY namespace, source_family, chain, deployment_epoch, manifest_version, manifest_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifests for normalized-event sync")?;

    rows.into_iter()
        .map(|row| {
            Ok(ActiveManifestRow {
                manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
                manifest_version: row
                    .try_get("manifest_version")
                    .context("missing manifest_version")?,
                namespace: row.try_get("namespace").context("missing namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("missing source_family")?,
                chain: row.try_get("chain").context("missing chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("missing deployment_epoch")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("missing normalizer_version")?,
            })
        })
        .collect()
}

async fn load_active_capabilities(pool: &PgPool) -> Result<HashMap<i64, Vec<ActiveCapabilityRow>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mcf.capability_name AS capability_name,
            mcf.status::text AS status,
            mcf.notes AS notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.namespace, mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mcf.capability_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active capability flags for normalized-event sync")?;

    let mut grouped = HashMap::<i64, Vec<ActiveCapabilityRow>>::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("missing capability manifest_id")?;
        grouped
            .entry(manifest_id)
            .or_default()
            .push(ActiveCapabilityRow {
                capability_name: row
                    .try_get("capability_name")
                    .context("missing capability_name")?,
                status: row.try_get("status").context("missing status")?,
                notes: row.try_get("notes").context("missing notes")?,
            });
    }

    Ok(grouped)
}

async fn load_active_proxy_contracts(
    pool: &PgPool,
) -> Result<HashMap<i64, Vec<ActiveContractRow>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mc.role AS role,
            mc.address AS address,
            mc.proxy_kind AS proxy_kind,
            mc.implementation AS implementation
        FROM manifest_versions mv
        JOIN manifest_contracts mc ON mc.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mc.implementation IS NOT NULL
        ORDER BY mv.namespace, mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mc.role, mc.address, mc.implementation
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active proxy contracts for normalized-event sync")?;

    let mut grouped = HashMap::<i64, Vec<ActiveContractRow>>::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("missing contract manifest_id")?;
        grouped
            .entry(manifest_id)
            .or_default()
            .push(ActiveContractRow {
                role: row.try_get("role").context("missing role")?,
                address: row.try_get("address").context("missing address")?,
                proxy_kind: row.try_get("proxy_kind").context("missing proxy_kind")?,
                implementation: row
                    .try_get("implementation")
                    .context("missing implementation")?,
            });
    }

    Ok(grouped)
}

async fn load_normalized_event_counts_by_kind(pool: &PgPool) -> Result<BTreeMap<String, usize>> {
    let rows = sqlx::query(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        GROUP BY event_kind
        ORDER BY event_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized-event counts by kind")?;

    let mut counts = BTreeMap::new();
    for row in rows {
        let event_kind = row
            .try_get::<String, _>("event_kind")
            .context("missing event_kind from normalized-event count row")?;
        let event_count = row
            .try_get::<i64, _>("event_count")
            .context("missing event_count from normalized-event count row")?;
        counts.insert(
            event_kind,
            usize::try_from(event_count).context("normalized-event count does not fit in usize")?,
        );
    }

    Ok(counts)
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use bigname_storage::{
        CanonicalityState, default_database_url, load_normalized_event_counts_by_kind,
        load_normalized_events_by_namespace,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

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
                .context("failed to parse database URL for manifest sync tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_manifest_sync_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for manifest sync tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for manifest sync tests")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for manifest sync tests")?;

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

    async fn insert_manifest_version(
        pool: &PgPool,
        manifest_version: i64,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        rollout_status: &str,
        normalizer_version: &str,
        file_path: &str,
    ) -> Result<i64> {
        sqlx::query_scalar(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
            RETURNING manifest_id
            "#,
        )
        .bind(manifest_version)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(normalizer_version)
        .bind(file_path)
        .bind("{}")
        .fetch_one(pool)
        .await
        .context("failed to insert manifest version")
    }

    async fn insert_capability_flag(
        pool: &PgPool,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
        notes: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_capability_flags (
                manifest_id,
                capability_name,
                status,
                notes
            )
            VALUES ($1, $2, $3::capability_support_status, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(status)
        .bind(notes)
        .execute(pool)
        .await
        .context("failed to insert capability flag")?;
        Ok(())
    }

    async fn insert_contract(
        pool: &PgPool,
        manifest_id: i64,
        role: &str,
        address: &str,
        proxy_kind: &str,
        implementation: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_contracts (
                manifest_id,
                role,
                address,
                proxy_kind,
                implementation
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(manifest_id)
        .bind(role)
        .bind(address)
        .bind(proxy_kind)
        .bind(implementation)
        .execute(pool)
        .await
        .context("failed to insert manifest contract")?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_manifest_normalized_events_is_idempotent() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let active_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v2_registry_l1/1.toml",
        )
        .await?;
        let inactive_manifest_id = insert_manifest_version(
            database.pool(),
            2,
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2_shadow",
            "draft",
            "uts46-v1",
            "manifests/ens/ens_v2_registry_l1/2.toml",
        )
        .await?;

        insert_capability_flag(
            database.pool(),
            active_manifest_id,
            "declared_children",
            "supported",
            Some("live"),
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            active_manifest_id,
            "verified_resolution",
            "shadow",
            None,
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            inactive_manifest_id,
            "declared_children",
            "unsupported",
            Some("ignored"),
        )
        .await?;

        insert_contract(
            database.pool(),
            active_manifest_id,
            "registry",
            "0x00000000000000000000000000000000000000aa",
            "erc1967",
            Some("0x00000000000000000000000000000000000000dd"),
        )
        .await?;
        insert_contract(
            database.pool(),
            inactive_manifest_id,
            "registry",
            "0x00000000000000000000000000000000000000bb",
            "erc1967",
            Some("0x00000000000000000000000000000000000000ee"),
        )
        .await?;

        let first_summary = sync_manifest_normalized_events(database.pool()).await?;
        assert_eq!(first_summary.total_synced_count, 4);
        assert_eq!(first_summary.total_inserted_count, 4);
        assert_eq!(
            first_summary.by_kind,
            BTreeMap::from([
                (
                    EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 2,
                        inserted_count: 2,
                    },
                ),
                (
                    EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 1,
                    },
                ),
                (
                    EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 1,
                    },
                ),
            ])
        );

        let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(loaded.len(), 4);
        assert!(loaded.iter().all(|event| {
            event.canonicality_state == CanonicalityState::Finalized
                && event.derivation_kind == DERIVATION_KIND_MANIFEST_SYNC
                && event.source_manifest_id == Some(active_manifest_id)
        }));
        assert_eq!(
            loaded
                .iter()
                .map(|event| event.event_kind.as_str())
                .collect::<Vec<_>>(),
            vec![
                EVENT_KIND_SOURCE_MANIFEST_UPDATED,
                EVENT_KIND_CAPABILITY_CHANGED,
                EVENT_KIND_CAPABILITY_CHANGED,
                EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED,
            ]
        );

        let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
        assert_eq!(
            counts,
            BTreeMap::from([
                (EVENT_KIND_CAPABILITY_CHANGED.to_owned(), 2_usize),
                (EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(), 1_usize),
            ])
        );

        let second_summary = sync_manifest_normalized_events(database.pool()).await?;
        assert_eq!(second_summary.total_synced_count, 4);
        assert_eq!(second_summary.total_inserted_count, 0);
        assert_eq!(
            second_summary.by_kind,
            BTreeMap::from([
                (
                    EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 2,
                        inserted_count: 0,
                    },
                ),
                (
                    EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 0,
                    },
                ),
                (
                    EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 0,
                    },
                ),
            ])
        );

        let loaded_after_rerun =
            load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(loaded_after_rerun, loaded);

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_manifest_normalized_events_skips_inactive_manifests() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let active_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v2_registry_l1/1.toml",
        )
        .await?;
        let inactive_manifest_id = insert_manifest_version(
            database.pool(),
            2,
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2_shadow",
            "deprecated",
            "uts46-v1",
            "manifests/ens/ens_v2_registry_l1/2.toml",
        )
        .await?;

        insert_capability_flag(
            database.pool(),
            active_manifest_id,
            "declared_children",
            "supported",
            None,
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            inactive_manifest_id,
            "declared_children",
            "unsupported",
            None,
        )
        .await?;

        insert_contract(
            database.pool(),
            active_manifest_id,
            "registry",
            "0x00000000000000000000000000000000000000aa",
            "erc1967",
            Some("0x00000000000000000000000000000000000000dd"),
        )
        .await?;
        insert_contract(
            database.pool(),
            inactive_manifest_id,
            "registry",
            "0x00000000000000000000000000000000000000bb",
            "erc1967",
            Some("0x00000000000000000000000000000000000000ee"),
        )
        .await?;

        let summary = sync_manifest_normalized_events(database.pool()).await?;
        assert_eq!(summary.total_synced_count, 3);
        assert_eq!(summary.total_inserted_count, 3);
        assert_eq!(
            load_normalized_events_by_namespace(database.pool(), "ens")
                .await?
                .len(),
            3
        );
        assert_eq!(
            load_normalized_event_counts_by_kind(database.pool(), "ens").await?,
            BTreeMap::from([
                (EVENT_KIND_CAPABILITY_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(), 1_usize),
            ])
        );

        database.cleanup().await
    }
}
