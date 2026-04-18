mod shipped_api {
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/api_main.rs"));

    #[cfg(test)]
    mod conformance {
        use std::{
            collections::BTreeSet,
            path::PathBuf,
            process::Command,
            str::FromStr,
            sync::{
                Mutex,
                atomic::{AtomicU64, Ordering},
            },
            time::{SystemTime, UNIX_EPOCH},
        };

        use anyhow::{Context, Result};
        use axum::{
            body::{Body, to_bytes},
            http::{Request, StatusCode},
            response::Response,
        };
        use bigname_storage::{
            CanonicalityState, ExecutionBoundaryInvalidation, ExecutionCacheKey,
            ExecutionManifestInvalidation, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep,
            NameSurface, NormalizedEvent, PermissionScope, PermissionsCurrentRow, RawBlock,
            RecordInventoryCurrentRow, ResolverCurrentRow, Resource, SurfaceBinding,
            SurfaceBindingKind, TokenLineage, default_database_url,
            invalidate_execution_outcomes_for_manifest_version,
            invalidate_execution_outcomes_for_manifest_version_and_request_key,
            invalidate_execution_outcomes_for_record_boundary,
            invalidate_execution_outcomes_for_record_boundary_and_request_key,
            invalidate_execution_outcomes_for_topology_boundary,
            invalidate_execution_outcomes_for_topology_boundary_and_request_key,
            load_execution_outcome, load_execution_trace, upsert_execution_outcome,
            upsert_execution_trace,
        };
        use serde::de::DeserializeOwned;
        use serde_json::{Value, json};
        use sqlx::{
            ConnectOptions, PgPool, Row,
            postgres::{PgConnectOptions, PgPoolOptions},
            types::{Uuid, time::OffsetDateTime},
        };
        use tower::ServiceExt;

        use super::*;

        static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
        static WORKER_CARGO_LOCK: Mutex<()> = Mutex::new(());

        struct HarnessDatabase {
            admin_pool: PgPool,
            pool: PgPool,
            database_name: String,
            database_url: String,
        }

        impl HarnessDatabase {
            async fn new() -> Result<Self> {
                let database_url = std::env::var("BIGNAME_DATABASE_URL")
                    .or_else(|_| std::env::var("DATABASE_URL"))
                    .unwrap_or_else(|_| default_database_url().to_owned());
                let base_options = PgConnectOptions::from_str(&database_url)
                    .context("failed to parse database URL for conformance harness")?;
                let unique = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("system clock is before unix epoch")?
                    .as_nanos();
                let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
                let database_name = format!(
                    "bigname_conformance_{}_{}_{}",
                    std::process::id(),
                    unique,
                    sequence
                );

                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(base_options.clone())
                    .await
                    .context("failed to connect admin pool for conformance harness")?;

                sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                    .execute(&admin_pool)
                    .await
                    .with_context(|| {
                        format!("failed to create conformance database {database_name}")
                    })?;

                let pool_options = base_options.clone().database(&database_name);
                let database_url = pool_options.to_url_lossy().to_string();
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(pool_options)
                    .await
                    .context("failed to connect conformance pool")?;

                bigname_storage::MIGRATOR
                    .run(&pool)
                    .await
                    .context("failed to apply checked-in migrations for conformance harness")?;

                Ok(Self {
                    admin_pool,
                    pool,
                    database_name,
                    database_url,
                })
            }

            fn app_state(&self) -> AppState {
                AppState {
                    phase: "conformance",
                    pool: self.pool.clone(),
                }
            }

            async fn insert_manifest(
                &self,
                namespace: &str,
                source_family: &str,
                chain: &str,
                deployment_epoch: &str,
                manifest_version: u64,
                rollout_status: &str,
                normalizer_version: &str,
            ) -> Result<i64> {
                let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
                let file_path =
                    format!("tests/{namespace}/{source_family}/{manifest_version}-{sequence}.toml");

                sqlx::query(
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
                .bind(i64::try_from(manifest_version).context("manifest_version exceeds BIGINT")?)
                .bind(namespace)
                .bind(source_family)
                .bind(chain)
                .bind(deployment_epoch)
                .bind(rollout_status)
                .bind(normalizer_version)
                .bind(file_path)
                .bind("{}")
                .fetch_one(&self.pool)
                .await
                .context("failed to insert manifest_version for conformance harness")?
                .try_get("manifest_id")
                .context("failed to read manifest_id for conformance harness")
            }

            async fn insert_capability_flag(
                &self,
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
                .execute(&self.pool)
                .await
                .context("failed to insert manifest capability flag for conformance harness")?;

                Ok(())
            }

            async fn seed_exact_name_rebuild_inputs(
                &self,
                logical_name_id: &str,
                resource_id: Uuid,
                token_lineage_id: Uuid,
                surface_binding_id: Uuid,
            ) -> Result<()> {
                let historical_resource_id = Uuid::from_u128(0x4400);

                bigname_storage::upsert_raw_blocks(
                    &self.pool,
                    &[
                        raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
                        raw_block("ethereum-mainnet", "0xbinding", None, 100, 1_717_171_700),
                        raw_block("ethereum-mainnet", "0xgrant", None, 101, 1_717_171_701),
                        raw_block("ethereum-mainnet", "0xtransfer", None, 102, 1_717_171_702),
                        raw_block("ethereum-mainnet", "0xauthority", None, 103, 1_717_171_703),
                        raw_block("ethereum-mainnet", "0xresolver", None, 104, 1_717_171_704),
                        raw_block(
                            "ethereum-mainnet",
                            "0xhistoryresource",
                            None,
                            105,
                            1_717_171_705,
                        ),
                        raw_block(
                            "ethereum-mainnet",
                            "0xhistorysurface",
                            None,
                            106,
                            1_717_171_706,
                        ),
                    ],
                )
                .await
                .context("failed to upsert raw blocks for exact-name conformance")?;
                bigname_storage::upsert_name_surfaces(&self.pool, &[name_surface(logical_name_id)])
                    .await
                    .context("failed to upsert name surface for exact-name conformance")?;
                bigname_storage::upsert_token_lineages(
                    &self.pool,
                    &[bigname_storage::TokenLineage {
                        token_lineage_id,
                        chain_id: "ethereum-mainnet".to_owned(),
                        block_hash: "0xtoken-lineage".to_owned(),
                        block_number: 101,
                        provenance: json!({"seed": "token_lineage"}),
                        canonicality_state: CanonicalityState::Finalized,
                    }],
                )
                .await
                .context("failed to upsert token lineage for exact-name conformance")?;
                bigname_storage::upsert_resources(
                    &self.pool,
                    &[Resource {
                        resource_id,
                        token_lineage_id: Some(token_lineage_id),
                        chain_id: "ethereum-mainnet".to_owned(),
                        block_hash: "0xresource".to_owned(),
                        block_number: 99,
                        provenance: json!({"seed": "exact_name_resource"}),
                        canonicality_state: CanonicalityState::Canonical,
                    }],
                )
                .await
                .context("failed to upsert resource for exact-name conformance")?;
                bigname_storage::upsert_surface_bindings(
                    &self.pool,
                    &[surface_binding(
                        surface_binding_id,
                        logical_name_id,
                        resource_id,
                        timestamp(1_700_000_001),
                    )],
                )
                .await
                .context("failed to upsert surface binding for exact-name conformance")?;
                bigname_storage::upsert_normalized_events(
                    &self.pool,
                    &[
                        authority_history_event(
                            "exact-name-grant",
                            "ens",
                            logical_name_id,
                            resource_id,
                            "RegistrationGranted",
                            101,
                            "0xgrant",
                            json!({
                                "authority_kind": "registrar",
                                "authority_key": "registrar:ethereum-mainnet:7:alice",
                                "registrant": "0x00000000000000000000000000000000000000aa",
                                "expiry": 1_800_000_000_i64,
                            }),
                        ),
                        authority_history_event(
                            "exact-name-token-control",
                            "ens",
                            logical_name_id,
                            resource_id,
                            "TokenControlTransferred",
                            102,
                            "0xtransfer",
                            json!({
                                "to": "0x00000000000000000000000000000000000000aa",
                            }),
                        ),
                        authority_history_event(
                            "exact-name-authority",
                            "ens",
                            logical_name_id,
                            resource_id,
                            "AuthorityTransferred",
                            103,
                            "0xauthority",
                            json!({
                                "owner": "0x00000000000000000000000000000000000000bb",
                            }),
                        ),
                        authority_history_event(
                            "exact-name-resolver",
                            "ens",
                            logical_name_id,
                            resource_id,
                            "ResolverChanged",
                            104,
                            "0xresolver",
                            json!({
                                "resolver": "0x0000000000000000000000000000000000000abc",
                                "namehash": "namehash:alice.eth",
                            }),
                        ),
                        history_event(
                            "exact-name-resource-head",
                            Some("ens:other.eth"),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(105),
                            Some("0xhistoryresource"),
                            Some("0xtx105"),
                            Some(0),
                            CanonicalityState::Canonical,
                        ),
                        history_event(
                            "exact-name-surface-head",
                            Some(logical_name_id),
                            Some(historical_resource_id),
                            Some("ethereum-mainnet"),
                            Some(106),
                            Some("0xhistorysurface"),
                            Some("0xtx106"),
                            Some(0),
                            CanonicalityState::Canonical,
                        ),
                    ],
                )
                .await
                .context("failed to upsert normalized events for exact-name conformance")?;

                Ok(())
            }

            async fn rebuild_name_current(&self, logical_name_id: &str) -> Result<()> {
                let database_url = self.database_url.clone();
                let logical_name_id = logical_name_id.to_owned();
                let worker_manifest_path =
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

                tokio::task::spawn_blocking(move || -> Result<()> {
                    let _guard = WORKER_CARGO_LOCK
                        .lock()
                        .expect("worker cargo lock must not be poisoned");
                    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
                    let output = Command::new(cargo)
                        .arg("run")
                        .arg("--quiet")
                        .arg("--manifest-path")
                        .arg(worker_manifest_path)
                        .arg("--")
                        .arg("name-current")
                        .arg("rebuild")
                        .arg("--database-url")
                        .arg(&database_url)
                        .arg("--logical-name-id")
                        .arg(&logical_name_id)
                        .output()
                        .with_context(|| {
                            format!(
                                "failed to invoke worker name_current rebuild for {logical_name_id}"
                            )
                        })?;

                    if !output.status.success() {
                        return Err(anyhow::anyhow!(
                            "worker name_current rebuild failed for {logical_name_id}\nstdout:\n{}\nstderr:\n{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr),
                        ));
                    }

                    Ok(())
                })
                .await
                .context("worker name_current rebuild task panicked")??;

                Ok(())
            }

            async fn seed_primary_name_reverse_changed(
                &self,
                address: &str,
                coin_type: &str,
            ) -> Result<()> {
                let normalized_address = address.to_ascii_lowercase();
                let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

                bigname_storage::upsert_normalized_events(
                    &self.pool,
                    &[NormalizedEvent {
                        event_identity: format!(
                            "conformance:ReverseChanged:{normalized_address}:{coin_type}"
                        ),
                        namespace: "ens".to_owned(),
                        logical_name_id: None,
                        resource_id: None,
                        event_kind: "ReverseChanged".to_owned(),
                        source_family: "ens_v1_reverse_l1".to_owned(),
                        manifest_version: 1,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-mainnet".to_owned()),
                        block_number: Some(210),
                        block_hash: Some("0xprimaryname".to_owned()),
                        transaction_hash: Some("0xtxprimaryname".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "chain_id": "ethereum-mainnet",
                            "block_hash": "0xprimaryname",
                            "block_number": 210,
                            "transaction_hash": "0xtxprimaryname",
                            "log_index": 0,
                        }),
                        derivation_kind: "ens_v1_reverse_claim".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "source_event": "ReverseClaimed",
                            "address": normalized_address,
                            "coin_type": coin_type,
                            "reverse_namespace": "ens",
                            "reverse_label": reverse_label,
                            "reverse_name": format!("{reverse_label}.addr.reverse"),
                            "reverse_node": "0x00000000000000000000000000000000000000000000000000000000000000d2",
                        }),
                    }],
                )
                .await
                .context("failed to seed ReverseChanged event for primary-name conformance")?;

                Ok(())
            }

            async fn rebuild_primary_names_current(
                &self,
                address: &str,
                namespace: &str,
                coin_type: &str,
            ) -> Result<()> {
                let database_url = self.database_url.clone();
                let address = address.to_owned();
                let namespace = namespace.to_owned();
                let coin_type = coin_type.to_owned();
                let worker_manifest_path =
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

                tokio::task::spawn_blocking(move || -> Result<()> {
                    let _guard = WORKER_CARGO_LOCK
                        .lock()
                        .expect("worker cargo lock must not be poisoned");
                    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
                    let output = Command::new(cargo)
                        .arg("run")
                        .arg("--quiet")
                        .arg("--manifest-path")
                        .arg(worker_manifest_path)
                        .arg("--")
                        .arg("primary-names-current")
                        .arg("rebuild")
                        .arg("--database-url")
                        .arg(&database_url)
                        .arg("--address")
                        .arg(&address)
                        .arg("--namespace")
                        .arg(&namespace)
                        .arg("--coin-type")
                        .arg(&coin_type)
                        .output()
                        .with_context(|| {
                            format!(
                                "failed to invoke worker primary_names_current rebuild for {address}/{namespace}/{coin_type}"
                            )
                        })?;

                    if !output.status.success() {
                        return Err(anyhow::anyhow!(
                            "worker primary_names_current rebuild failed for {address}/{namespace}/{coin_type}\nstdout:\n{}\nstderr:\n{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr),
                        ));
                    }

                    Ok(())
                })
                .await
                .context("worker primary_names_current rebuild task panicked")??;

                Ok(())
            }

            async fn insert_name_current_row(
                &self,
                row: bigname_storage::NameCurrentRow,
            ) -> Result<()> {
                bigname_storage::upsert_name_current_rows(&self.pool, &[row])
                    .await
                    .context("failed to upsert name_current row for conformance harness")?;
                Ok(())
            }

            async fn insert_record_inventory_current_row(
                &self,
                row: RecordInventoryCurrentRow,
            ) -> Result<()> {
                bigname_storage::upsert_record_inventory_current_rows(&self.pool, &[row])
                    .await
                    .context(
                        "failed to upsert record_inventory_current row for conformance harness",
                    )?;
                Ok(())
            }

            async fn seed_history_binding(
                &self,
                logical_name_id: &str,
                resource_id: Uuid,
                surface_binding_id: Uuid,
            ) -> Result<()> {
                bigname_storage::upsert_name_surfaces(&self.pool, &[name_surface(logical_name_id)])
                    .await
                    .context("failed to upsert name surface for history conformance")?;
                bigname_storage::upsert_resources(&self.pool, &[resource(resource_id)])
                    .await
                    .context("failed to upsert resource for history conformance")?;
                bigname_storage::upsert_surface_bindings(
                    &self.pool,
                    &[surface_binding(
                        surface_binding_id,
                        logical_name_id,
                        resource_id,
                        timestamp(1_700_000_010),
                    )],
                )
                .await
                .context("failed to upsert surface binding for history conformance")?;

                Ok(())
            }

            async fn cleanup(self) -> Result<()> {
                self.pool.close().await;
                sqlx::query(&format!(
                    r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                    self.database_name
                ))
                .execute(&self.admin_pool)
                .await
                .with_context(|| {
                    format!("failed to drop conformance database {}", self.database_name)
                })?;
                self.admin_pool.close().await;
                Ok(())
            }
        }

        #[tokio::test]
        async fn smoke_supported_reads_contract_bootstrap() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let ens_l1 = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l1",
                    "ethereum-mainnet",
                    "ens_v2",
                    1,
                    "active",
                    "uts46-v1",
                )
                .await?;
            database
                .insert_capability_flag(ens_l1, "declared_children", "supported", None)
                .await?;
            database
                .insert_manifest(
                    "ens",
                    "ens_shadow_registry",
                    "ethereum-mainnet",
                    "ens_shadow",
                    2,
                    "shadow",
                    "uts46-v1",
                )
                .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/namespaces/ens")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("namespace metadata smoke request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NamespaceMetadataResponse = read_json(response).await?;
            assert_eq!(payload.data.namespace, "ens");
            assert_eq!(payload.declared_state.active_manifest_count, 1);
            assert_eq!(
                payload.declared_state.active_source_families,
                vec!["ens_v2_registry_l1".to_owned()]
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.consistency, "head");

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn namespace_manifests_contract_lists_active_manifests() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let ens_l1 = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l1",
                    "ethereum-mainnet",
                    "ens_v2",
                    1,
                    "active",
                    "uts46-v1",
                )
                .await?;
            database
                .insert_capability_flag(ens_l1, "declared_children", "supported", None)
                .await?;
            database
                .insert_capability_flag(
                    ens_l1,
                    "verified_resolution",
                    "shadow",
                    Some("tracked but not yet served"),
                )
                .await?;

            let ens_l2 = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l2",
                    "base-mainnet",
                    "ens_v2_base",
                    2,
                    "active",
                    "uts46-v2",
                )
                .await?;
            database
                .insert_capability_flag(ens_l2, "declared_children", "unsupported", Some("pending"))
                .await?;

            database
                .insert_manifest(
                    "ens",
                    "ens_shadow_registry",
                    "ethereum-mainnet",
                    "ens_shadow",
                    3,
                    "shadow",
                    "uts46-v1",
                )
                .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/manifests/ens")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("namespace manifests request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NamespaceManifestsResponse = read_json(response).await?;
            assert_eq!(payload.data.namespace, "ens");
            assert_eq!(payload.declared_state.manifests.len(), 2);
            assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
            assert_eq!(
                payload.declared_state.manifests[0].source_family,
                "ens_v2_registry_l1"
            );
            assert_eq!(
                payload.declared_state.manifests[0]
                    .capability_flags
                    .get("verified_resolution")
                    .and_then(|flag| flag.notes.as_deref()),
                Some("tracked but not yet served")
            );
            assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
            assert_eq!(
                payload.coverage.enumeration_basis,
                "active manifests for the requested namespace"
            );
            assert_eq!(payload.provenance.manifest_versions.len(), 2);
            assert!(payload.verified_state.is_none());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_returns_declared_rows_sorted_with_declared_only_coverage()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let parent_logical_name_id = "ens:parent.eth";

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        parent_logical_name_id,
                        "parent.eth",
                        "node:parent.eth",
                        10,
                    ),
                    collection_name_surface(
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        11,
                    ),
                    collection_name_surface(
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        12,
                    ),
                ],
            )
            .await
            .context("failed to upsert name surfaces for children conformance")?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        201,
                        11,
                    ),
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        202,
                        12,
                    ),
                ],
            )
            .await
            .context("failed to upsert children_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ChildrenResponse = read_json(response).await?;
            assert!(
                payload
                    .declared_state
                    .as_object()
                    .map(|value| value.is_empty())
                    .unwrap_or(false)
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["declared".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "declared_direct_children"
            );
            assert_eq!(payload.coverage.unsupported_reason, None);
            assert_eq!(payload.page.sort, "display_name_asc");
            assert_eq!(payload.page.page_size, 2);
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.last_updated, "2024-05-31T16:13:32Z");
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": ["202", "201"],
                    "raw_fact_refs": [
                        {"kind": "raw_log", "block_number": 12},
                        {"kind": "raw_log", "block_number": 11}
                    ],
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "ens_v1_registry_l1",
                        "source_manifest_id": null
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "children_current_rebuild"
                })
            );
            assert_eq!(
                payload.chain_positions,
                json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 12,
                        "block_hash": "0xblock0c",
                        "timestamp": "2026-04-17T00:00:12Z"
                    }
                })
            );

            let child_ids = payload
                .data
                .iter()
                .map(|row| {
                    row.get("logical_name_id")
                        .and_then(Value::as_str)
                        .expect("child row must include logical_name_id")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                child_ids,
                vec!["ens:alice.parent.eth", "ens:bob.parent.eth"]
            );
            assert_eq!(
                payload.data[0].get("surface_class").and_then(Value::as_str),
                Some("declared")
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?page_size=1")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: ChildrenResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("children first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/parent.eth/children?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: ChildrenResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/parent.eth/children?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: ChildrenResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "display_name_asc",
                2,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_include_counts_returns_declared_subname_count() -> Result<()>
        {
            let database = HarnessDatabase::new().await?;
            let parent_logical_name_id = "ens:parent.eth";

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        parent_logical_name_id,
                        "parent.eth",
                        "node:parent.eth",
                        20,
                    ),
                    collection_name_surface(
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        21,
                    ),
                    collection_name_surface(
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert name surfaces for children counts conformance")?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        301,
                        21,
                    ),
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        302,
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert children_current rows for counts conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?include=counts")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children counts request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ChildrenResponse = read_json(response).await?;
            assert_eq!(payload.declared_state.get("subname_count"), Some(&json!(2)));
            assert_eq!(payload.data.len(), 2);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_rejects_non_declared_surface_classes() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[collection_name_surface(
                    "ens:parent.eth",
                    "parent.eth",
                    "node:parent.eth",
                    30,
                )],
            )
            .await
            .context("failed to upsert parent surface for surface_classes conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?surface_classes=declared,linked")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children unsupported surface_classes request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "unsupported");
            assert_eq!(
                payload.error.message,
                "surface_classes other than declared are not yet supported"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_returns_surface_first_rows_sorted_with_stable_relation_facets()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bbb";
            let alpha_resource_id = Uuid::from_u128(0x8100);
            let alpha_token_lineage_id = Uuid::from_u128(0x8101);
            let alpha_surface_binding_id = Uuid::from_u128(0x8102);
            let beta_resource_id = Uuid::from_u128(0x8200);
            let beta_token_lineage_id = Uuid::from_u128(0x8201);
            let beta_surface_binding_id = Uuid::from_u128(0x8202);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[
                    address_name_token_lineage(alpha_token_lineage_id, "0xalpha", 11),
                    address_name_token_lineage(beta_token_lineage_id, "0xbeta", 12),
                ],
            )
            .await
            .context("failed to upsert token lineages for address-name conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        alpha_resource_id,
                        Some(alpha_token_lineage_id),
                        "0xalpha",
                        11,
                    ),
                    address_name_resource(
                        beta_resource_id,
                        Some(beta_token_lineage_id),
                        "0xbeta",
                        12,
                    ),
                ],
            )
            .await
            .context("failed to upsert resources for address-name conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 12),
                    collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 11),
                ],
            )
            .await
            .context("failed to upsert name surfaces for address-name conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        beta_surface_binding_id,
                        "ens:beta.eth",
                        beta_resource_id,
                        "0xbeta",
                        12,
                        1_717_173_012,
                    ),
                    address_name_surface_binding(
                        alpha_surface_binding_id,
                        "ens:alpha.eth",
                        alpha_resource_id,
                        "0xalpha",
                        11,
                        1_717_173_011,
                    ),
                ],
            )
            .await
            .context("failed to upsert surface bindings for address-name conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:beta.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "beta.eth",
                        "beta.eth",
                        "node:beta.eth",
                        beta_surface_binding_id,
                        beta_resource_id,
                        Some(beta_token_lineage_id),
                        12,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::TokenHolder,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        alpha_resource_id,
                        Some(alpha_token_lineage_id),
                        11,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        alpha_resource_id,
                        Some(alpha_token_lineage_id),
                        11,
                    ),
                ],
            )
            .await
            .context("failed to upsert address_names_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: AddressNamesResponse = read_json(response).await?;
            assert!(
                payload
                    .declared_state
                    .as_object()
                    .map(|value| value.is_empty())
                    .unwrap_or(false)
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["ensv1_registry_path".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "surface_current_relations"
            );
            assert_eq!(payload.page.sort, "display_name_asc");
            assert_eq!(payload.page.page_size, 2);
            assert_eq!(payload.consistency, "finalized");

            let logical_name_ids = payload
                .data
                .iter()
                .map(|row| {
                    row.get("logical_name_id")
                        .and_then(Value::as_str)
                        .expect("address-name row must include logical_name_id")
                })
                .collect::<Vec<_>>();
            assert_eq!(logical_name_ids, vec!["ens:alpha.eth", "ens:beta.eth"]);
            assert_eq!(
                payload.data[0].get("relation_facets"),
                Some(&json!(["registrant", "token_holder"]))
            );
            assert_eq!(
                payload.data[1].get("relation_facets"),
                Some(&json!(["effective_controller"]))
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?page_size=1"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: AddressNamesResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("address names first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: AddressNamesResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: AddressNamesResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "display_name_asc",
                2,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_honors_namespace_and_relation_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let ens_resource_id = Uuid::from_u128(0x8300);
            let ens_token_lineage_id = Uuid::from_u128(0x8301);
            let ens_surface_binding_id = Uuid::from_u128(0x8302);
            let base_resource_id = Uuid::from_u128(0x8400);
            let base_surface_binding_id = Uuid::from_u128(0x8402);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    ens_token_lineage_id,
                    "0xens",
                    21,
                )],
            )
            .await
            .context("failed to upsert filtered token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(ens_resource_id, Some(ens_token_lineage_id), "0xens", 21),
                    address_name_resource(base_resource_id, None, "0xbase", 22),
                ],
            )
            .await
            .context("failed to upsert filtered resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:alice.eth", "alice.eth", "node:alice.eth", 21),
                    collection_name_surface(
                        "basenames:alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered name surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        ens_surface_binding_id,
                        "ens:alice.eth",
                        ens_resource_id,
                        "0xens",
                        21,
                        1_717_173_021,
                    ),
                    address_name_surface_binding(
                        base_surface_binding_id,
                        "basenames:alice.base.eth",
                        base_resource_id,
                        "0xbase",
                        22,
                        1_717_173_022,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered surface bindings for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:alice.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alice.eth",
                        "alice.eth",
                        "node:alice.eth",
                        ens_surface_binding_id,
                        ens_resource_id,
                        Some(ens_token_lineage_id),
                        21,
                    ),
                    address_name_current_row(
                        address,
                        "basenames:alice.base.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        base_surface_binding_id,
                        base_resource_id,
                        None,
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered address_names_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?namespace=ens&relation=registrant"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("filtered address names request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: AddressNamesResponse = read_json(response).await?;
            assert_eq!(payload.data.len(), 1);
            assert_eq!(
                payload.data[0].get("logical_name_id"),
                Some(&Value::String("ens:alice.eth".to_owned()))
            );
            assert_eq!(
                payload.data[0].get("relation_facets"),
                Some(&json!(["registrant"]))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_dedupe_by_resource_changes_grouping_only() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000def";
            let shared_resource_id = Uuid::from_u128(0x8500);
            let shared_token_lineage_id = Uuid::from_u128(0x8501);
            let alpha_surface_binding_id = Uuid::from_u128(0x8502);
            let beta_surface_binding_id = Uuid::from_u128(0x8503);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    shared_token_lineage_id,
                    "0xshared",
                    31,
                )],
            )
            .await
            .context("failed to upsert shared token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[address_name_resource(
                    shared_resource_id,
                    Some(shared_token_lineage_id),
                    "0xshared",
                    31,
                )],
            )
            .await
            .context("failed to upsert shared resource for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 31),
                    collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 31),
                ],
            )
            .await
            .context("failed to upsert shared name surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        beta_surface_binding_id,
                        "ens:beta.eth",
                        shared_resource_id,
                        "0xshared",
                        31,
                        1_717_173_031,
                    ),
                    address_name_surface_binding(
                        alpha_surface_binding_id,
                        "ens:alpha.eth",
                        shared_resource_id,
                        "0xshared",
                        31,
                        1_717_173_031,
                    ),
                ],
            )
            .await
            .context("failed to upsert shared surface bindings for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:beta.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "beta.eth",
                        "beta.eth",
                        "node:beta.eth",
                        beta_surface_binding_id,
                        shared_resource_id,
                        Some(shared_token_lineage_id),
                        31,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        shared_resource_id,
                        Some(shared_token_lineage_id),
                        31,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::TokenHolder,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        shared_resource_id,
                        Some(shared_token_lineage_id),
                        31,
                    ),
                ],
            )
            .await
            .context("failed to upsert shared address_names_current rows for conformance")?;

            let surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?dedupe_by=surface"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("surface-dedupe address names request failed")?;
            let surface_payload: AddressNamesResponse = read_json(surface_response).await?;
            assert_eq!(surface_payload.data.len(), 2);

            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?dedupe_by=resource"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource-dedupe address names request failed")?;

            assert_eq!(resource_response.status(), StatusCode::OK);

            let resource_payload: AddressNamesResponse = read_json(resource_response).await?;
            assert_eq!(resource_payload.data.len(), 1);
            assert_eq!(
                resource_payload.data[0].get("logical_name_id"),
                Some(&Value::String("ens:alpha.eth".to_owned()))
            );
            assert_eq!(
                resource_payload.data[0].get("resource_id"),
                Some(&Value::String(shared_resource_id.to_string()))
            );
            assert_eq!(
                resource_payload.data[0].get("relation_facets"),
                Some(&json!([
                    "registrant",
                    "token_holder",
                    "effective_controller"
                ]))
            );
            assert_eq!(resource_payload.coverage, surface_payload.coverage);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_include_role_summary_is_additive_and_preserves_base_collection_behavior()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000fed";
            let resource_id = Uuid::from_u128(0x8600);
            let token_lineage_id = Uuid::from_u128(0x8601);
            let surface_binding_id = Uuid::from_u128(0x8602);
            let subject = "0x0000000000000000000000000000000000000abc";
            let other_subject = "0x0000000000000000000000000000000000000def";

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(token_lineage_id, "0xalpha", 61)],
            )
            .await
            .context("failed to upsert token lineage for role-summary conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[address_name_resource(
                    resource_id,
                    Some(token_lineage_id),
                    "0xalpha",
                    61,
                )],
            )
            .await
            .context("failed to upsert resource for role-summary conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 61),
                    collection_name_surface(
                        "ens:child-one.alpha.eth",
                        "child-one.alpha.eth",
                        "node:child-one.alpha.eth",
                        62,
                    ),
                    collection_name_surface(
                        "ens:child-two.alpha.eth",
                        "child-two.alpha.eth",
                        "node:child-two.alpha.eth",
                        63,
                    ),
                ],
            )
            .await
            .context("failed to upsert surfaces for role-summary conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    surface_binding_id,
                    "ens:alpha.eth",
                    resource_id,
                    "0xalpha",
                    61,
                    1_717_173_061,
                )],
            )
            .await
            .context("failed to upsert surface binding for role-summary conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    "ens:alpha.eth",
                    bigname_storage::AddressNameRelation::Registrant,
                    "alpha.eth",
                    "alpha.eth",
                    "node:alpha.eth",
                    surface_binding_id,
                    resource_id,
                    Some(token_lineage_id),
                    61,
                )],
            )
            .await
            .context("failed to upsert address_names_current rows for role-summary conformance")?;
            database
                .insert_name_current_row(address_name_name_current_row(
                    "ens:alpha.eth",
                    "alpha.eth",
                    "alpha.eth",
                    "node:alpha.eth",
                    surface_binding_id,
                    resource_id,
                    Some(token_lineage_id),
                    64,
                    json!({
                        "registration": {
                            "status": "active",
                            "authority_kind": "registrar",
                        },
                        "control": {
                            "status": "wrapped",
                            "expiry": "2026-09-01T00:00:00Z",
                            "registrant": address,
                            "registry_owner": subject,
                            "latest_event_kind": "NameWrapped",
                        },
                        "resolver": {
                            "chain_id": "ethereum-mainnet",
                            "address": "0x0000000000000000000000000000000000000aaa",
                            "latest_event_kind": "ResolverChanged",
                        },
                        "record_inventory": {
                            "status": "supported",
                            "count": 2,
                        },
                        "history": {
                            "surface_head": null,
                            "resource_head": null,
                        },
                    }),
                ))
                .await?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        "ens:alpha.eth",
                        "ens:child-one.alpha.eth",
                        "child-one.alpha.eth",
                        "node:child-one.alpha.eth",
                        701,
                        62,
                    ),
                    declared_child_row(
                        "ens:alpha.eth",
                        "ens:child-two.alpha.eth",
                        "child-two.alpha.eth",
                        "node:child-two.alpha.eth",
                        702,
                        63,
                    ),
                ],
            )
            .await
            .context("failed to upsert children_current rows for role-summary conformance")?;
            bigname_storage::upsert_permissions_current_rows(
                &database.pool,
                &[
                    permission_current_row(resource_id, subject, PermissionScope::Resource, 7, 71),
                    permission_current_row(
                        resource_id,
                        subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000aaa"
                                .to_owned(),
                        },
                        8,
                        72,
                    ),
                    permission_current_row(
                        resource_id,
                        other_subject,
                        PermissionScope::Registry,
                        9,
                        73,
                    ),
                ],
            )
            .await
            .context("failed to upsert permissions_current rows for role-summary conformance")?;

            let base_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("base address names request failed")?;
            let include_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?include=role_summary"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("role summary address names request failed")?;

            assert_eq!(base_response.status(), StatusCode::OK);
            assert_eq!(include_response.status(), StatusCode::OK);

            let base_payload: AddressNamesResponse = read_json(base_response).await?;
            let payload: AddressNamesResponse = read_json(include_response).await?;

            assert_eq!(payload.coverage, base_payload.coverage);
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["ensv1_registry_path"]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "surface_current_relations"
            );
            assert_eq!(payload.page, base_payload.page);
            assert_eq!(payload.declared_state, base_payload.declared_state);
            assert_eq!(payload.consistency, base_payload.consistency);
            assert_eq!(payload.data.len(), base_payload.data.len());

            let base_row = base_payload.data[0]
                .as_object()
                .expect("base address-name row must be an object");
            let include_row = payload.data[0]
                .as_object()
                .expect("role-summary address-name row must be an object");
            let base_keys = base_row.keys().cloned().collect::<BTreeSet<_>>();
            let include_keys = include_row.keys().cloned().collect::<BTreeSet<_>>();

            assert_eq!(
                include_keys
                    .difference(&base_keys)
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![
                    "expiry".to_owned(),
                    "record_count".to_owned(),
                    "role_summary".to_owned(),
                    "status".to_owned(),
                    "subname_count".to_owned(),
                ]
            );

            for key in &base_keys {
                assert_eq!(
                    include_row.get(key),
                    base_row.get(key),
                    "include=role_summary must preserve base field {key}"
                );
            }

            assert_eq!(payload.data[0].get("status"), Some(&json!("wrapped")));
            assert_eq!(
                payload.data[0].get("expiry"),
                Some(&json!("2026-09-01T00:00:00Z"))
            );
            assert_eq!(payload.data[0].get("record_count"), Some(&json!(2)));
            assert_eq!(payload.data[0].get("subname_count"), Some(&json!(2)));
            assert_eq!(
                payload.data[0].get("role_summary"),
                Some(&json!({
                    "subjects": [
                        {
                            "subject": subject,
                            "scopes": [
                                {
                                    "scope": {
                                        "kind": "resolver",
                                        "detail": {
                                            "chain_id": "ethereum-mainnet",
                                            "resolver_address": "0x0000000000000000000000000000000000000aaa",
                                        },
                                    },
                                    "effective_powers": ["set_resolver", "create_subnames"],
                                },
                                {
                                    "scope": {
                                        "kind": "resource",
                                        "detail": {},
                                    },
                                    "effective_powers": ["set_resolver", "set_records"],
                                },
                            ],
                        },
                        {
                            "subject": other_subject,
                            "scopes": [
                                {
                                    "scope": {
                                        "kind": "registry",
                                        "detail": {},
                                    },
                                    "effective_powers": ["set_resolver", "set_records"],
                                },
                            ],
                        },
                    ],
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn exact_name_contract_returns_frozen_control_resolver_and_history_summaries()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact name request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NameResponse = read_json(response).await?;
            let declared_state = payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");

            assert_eq!(
                declared_state.get("control"),
                Some(&exact_name_control_summary())
            );
            assert_eq!(
                declared_state.get("resolver"),
                Some(&exact_name_resolver_summary())
            );
            assert_exact_name_history_summary_matches_history_route(
                &database,
                "ens",
                "alice.eth",
                declared_state
                    .get("history")
                    .expect("history summary must be present"),
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn coverage_contract_returns_declared_state_explain_with_shared_top_level_coverage()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let coverage_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/coverage/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("coverage request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name request failed")?;

            assert_eq!(coverage_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let coverage_payload: NameResponse = read_json(coverage_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let name_declared_state = name_payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");

            assert_eq!(coverage_payload.data, name_payload.data);
            assert_eq!(coverage_payload.coverage, name_payload.coverage);
            assert_eq!(coverage_payload.verified_state, None);
            assert_eq!(
                name_declared_state.get("control"),
                Some(&exact_name_control_summary())
            );
            assert_eq!(
                name_declared_state.get("resolver"),
                Some(&exact_name_resolver_summary())
            );
            assert_exact_name_history_summary_matches_history_route(
                &database,
                "ens",
                "alice.eth",
                name_declared_state
                    .get("history")
                    .expect("history summary must be present"),
            )
            .await?;
            assert_eq!(coverage_payload.declared_state, coverage_payload.coverage);
            assert_eq!(
                coverage_payload.declared_state,
                json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "enumeration_basis": "exact_name",
                    "unsupported_reason": null
                })
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn surface_binding_explain_contract_is_declared_only_with_exact_name_coverage_and_frozen_summary()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/names/ens/alice.eth/surface-binding")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("surface-binding explain request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact name request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let explain_payload: NameResponse = read_json(explain_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let name_declared_state = name_payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");
            let history = name_declared_state
                .get("history")
                .cloned()
                .expect("history summary must be present");

            assert_eq!(explain_payload.data, name_payload.data);
            assert_eq!(explain_payload.provenance, name_payload.provenance);
            assert_eq!(explain_payload.coverage, name_payload.coverage);
            assert_eq!(
                explain_payload.chain_positions,
                name_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, name_payload.consistency);
            assert_eq!(explain_payload.last_updated, name_payload.last_updated);
            assert_eq!(explain_payload.verified_state, None);
            assert_eq!(
                explain_payload.declared_state.get("history"),
                Some(&history)
            );
            assert_eq!(
                explain_payload.declared_state,
                json!({
                    "surface_binding": exact_name_surface_binding_summary(surface_binding_id),
                    "history": history,
                })
            );
            assert_exact_name_history_summary_matches_history_route(
                &database,
                "ens",
                "alice.eth",
                explain_payload
                    .declared_state
                    .get("history")
                    .expect("history summary must be present"),
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn authority_control_explain_contract_is_declared_only_with_exact_name_coverage_and_frozen_summaries()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/names/ens/alice.eth/authority-control")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("authority-control explain request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact name request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let explain_payload: NameResponse = read_json(explain_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let name_declared_state = name_payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");
            let authority = exact_name_authority_summary(resource_id, token_lineage_id);
            let control = exact_name_control_summary();

            assert_eq!(explain_payload.data, name_payload.data);
            assert_eq!(explain_payload.provenance, name_payload.provenance);
            assert_eq!(explain_payload.coverage, name_payload.coverage);
            assert_eq!(
                explain_payload.chain_positions,
                name_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, name_payload.consistency);
            assert_eq!(explain_payload.last_updated, name_payload.last_updated);
            assert_eq!(explain_payload.verified_state, None);
            assert_eq!(name_declared_state.get("authority"), Some(&authority));
            assert_eq!(name_declared_state.get("control"), Some(&control));
            assert_eq!(
                explain_payload.declared_state.get("authority"),
                Some(&authority)
            );
            assert_eq!(
                explain_payload.declared_state.get("control"),
                Some(&control)
            );
            assert_eq!(
                explain_payload.declared_state,
                json!({
                    "authority": authority,
                    "control": control,
                })
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_returns_declared_and_verified_sections_by_mode() -> Result<()>
        {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let default_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("default resolution request failed")?;
            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=declared&records=text:com.twitter,addr:60,avatar",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared resolution request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,addr:60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified resolution request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed")?;

            assert_eq!(default_response.status(), StatusCode::OK);
            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let default_payload: ResolutionResponse = read_json(default_response).await?;
            let declared_payload: ResolutionResponse = read_json(declared_response).await?;
            let verified_payload: ResolutionResponse = read_json(verified_response).await?;
            let both_payload: ResolutionResponse = read_json(both_response).await?;
            let expected_default_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["addr:60", "avatar"],
            );
            let expected_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["text:com.twitter", "addr:60", "avatar"],
            );
            let expected_both_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["text:com.twitter"],
            );

            assert_eq!(
                default_payload.declared_state.as_ref(),
                Some(&expected_default_declared_state)
            );
            assert_eq!(default_payload.verified_state, None);
            assert_eq!(
                declared_payload.declared_state.as_ref(),
                Some(&expected_declared_state)
            );
            assert_eq!(declared_payload.verified_state, None);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(resolution_unsupported_verified_state(&["text", "addr:60"]))
            );
            assert_eq!(
                both_payload.declared_state.as_ref(),
                Some(&expected_both_declared_state)
            );
            assert_eq!(
                both_payload.verified_state,
                Some(resolution_unsupported_verified_state(&["text:com.twitter"]))
            );

            let default_declared_state = default_payload
                .declared_state
                .as_ref()
                .expect("default declared_state must be present");
            let inventory_selector_tuples = default_declared_state
                .get("record_inventory")
                .and_then(|value| value.get("selectors"))
                .and_then(Value::as_array)
                .expect("supported record_inventory must expose selectors")
                .iter()
                .map(record_selector_identity_tuple)
                .collect::<Vec<_>>();
            assert_eq!(
                inventory_selector_tuples,
                vec![
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned())
                    ),
                    ("avatar".to_owned(), "avatar".to_owned(), None),
                    (
                        "text:com.twitter".to_owned(),
                        "text".to_owned(),
                        Some("com.twitter".to_owned()),
                    ),
                ]
            );

            let inventory_selector_tuple_set = inventory_selector_tuples
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let topology_record_version_boundary = default_declared_state
                .get("topology")
                .and_then(|value| value.get("version_boundaries"))
                .and_then(|value| value.get("record_version_boundary"))
                .expect("supported topology must expose record_version_boundary");
            let record_inventory_version_boundary = default_declared_state
                .get("record_inventory")
                .and_then(|value| value.get("record_version_boundary"))
                .expect("supported record_inventory must expose record_version_boundary");
            let full_cache = default_declared_state
                .get("record_cache")
                .expect("supported record_cache must be present");
            let full_cache_entries = full_cache
                .get("entries")
                .and_then(Value::as_array)
                .expect("supported record_cache must expose entries");
            let full_cache_selector_tuples = full_cache_entries
                .iter()
                .map(record_selector_identity_tuple)
                .collect::<Vec<_>>();

            assert_eq!(
                record_inventory_version_boundary,
                topology_record_version_boundary
            );
            assert_eq!(
                full_cache.get("record_version_boundary"),
                Some(topology_record_version_boundary)
            );
            assert_eq!(
                full_cache_selector_tuples,
                vec![
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned())
                    ),
                    ("avatar".to_owned(), "avatar".to_owned(), None),
                ]
            );
            assert!(
                full_cache_selector_tuples
                    .iter()
                    .all(|tuple| inventory_selector_tuple_set.contains(tuple))
            );

            let narrowed_cache_selector_tuples = declared_payload
                .declared_state
                .as_ref()
                .and_then(|value| value.get("record_cache"))
                .and_then(|value| value.get("entries"))
                .and_then(Value::as_array)
                .expect("declared mode record_cache must expose entries")
                .iter()
                .map(record_selector_identity_tuple)
                .collect::<Vec<_>>();
            assert_eq!(
                narrowed_cache_selector_tuples,
                vec![
                    (
                        "text:com.twitter".to_owned(),
                        "text".to_owned(),
                        Some("com.twitter".to_owned()),
                    ),
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned())
                    ),
                    ("avatar".to_owned(), "avatar".to_owned(), None),
                ]
            );
            assert!(
                narrowed_cache_selector_tuples
                    .iter()
                    .all(|tuple| inventory_selector_tuple_set.contains(tuple))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_requires_records_for_verified_modes() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified resolution request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed")?;

            assert_eq!(verified_response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(both_response.status(), StatusCode::BAD_REQUEST);

            let verified_payload: ErrorResponse = read_json(verified_response).await?;
            let both_payload: ErrorResponse = read_json(both_response).await?;
            assert_eq!(verified_payload.error.code, "invalid_input");
            assert_eq!(both_payload.error.code, "invalid_input");
            assert_eq!(
                verified_payload.error.message,
                "records is required when mode is verified or both"
            );
            assert_eq!(both_payload.error.message, verified_payload.error.message);
            assert!(verified_payload.error.details.is_empty());
            assert!(both_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_rejects_duplicate_records_for_verified_modes() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,text")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("duplicate resolution request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "invalid_input");
            assert_eq!(
                payload.error.message,
                "records must not contain duplicate selectors"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_rejects_malformed_records() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=:avatar")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("malformed resolution request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "invalid_input");
            assert_eq!(
                payload.error.message,
                "records must contain only valid record selectors"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reuses_exact_name_envelope_fields() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name request failed")?;

            assert_eq!(resolution_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let expected_resolution_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["text:com.twitter", "addr:60"],
            );

            assert_eq!(resolution_payload.data, name_payload.data);
            assert_eq!(resolution_payload.provenance, name_payload.provenance);
            assert_eq!(resolution_payload.coverage, name_payload.coverage);
            assert_eq!(
                resolution_payload.chain_positions,
                name_payload.chain_positions
            );
            assert_eq!(resolution_payload.consistency, name_payload.consistency);
            assert_eq!(resolution_payload.last_updated, name_payload.last_updated);
            assert_eq!(
                resolution_payload.declared_state.as_ref(),
                Some(&expected_resolution_declared_state)
            );
            assert_eq!(
                resolution_payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ]))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reads_persisted_contenthash_answer_on_mixed_route()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000023);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("mixed contenthash resolution requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let persisted_records = parse_resolution_record_keys(
                Some("text:com.twitter,contenthash,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &persisted_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["text:com.twitter", "contenthash", "addr:60"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["text:com.twitter", "contenthash", "addr:60"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    persisted_verified_queries,
                ),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter,contenthash,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed contenthash resolution request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolutionResponse = read_json(response).await?;
            let expected_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["avatar", "text:com.twitter", "contenthash", "addr:60"],
            );
            let expected_verified_state = json!({
                "verified_queries": [
                    {
                        "record_key": "avatar",
                        "status": "unsupported",
                        "unsupported_reason": "verified resolution entrypoint is not yet supported",
                    },
                    {
                        "record_key": "text:com.twitter",
                        "status": "not_found",
                        "failure_reason": "no_text_record",
                        "provenance": {
                            "execution_trace_id": execution_trace_id.to_string(),
                        },
                    },
                    {
                        "record_key": "contenthash",
                        "status": "success",
                        "value": {
                            "value": resolution_contenthash_value(),
                        },
                        "provenance": {
                            "execution_trace_id": execution_trace_id.to_string(),
                        },
                    },
                    {
                        "record_key": "addr:60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x00000000000000000000000000000000000000aa",
                        },
                        "provenance": {
                            "execution_trace_id": execution_trace_id.to_string(),
                        },
                    }
                ]
            });

            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(payload.declared_state.as_ref(), Some(&expected_declared_state));
            assert_eq!(payload.verified_state, Some(expected_verified_state));

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_reads_persisted_answer_and_reuses_resolution_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000021);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution explain requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let explain_records = parse_resolution_record_keys(
                Some("text:com.twitter,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &explain_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["addr:60", "text:com.twitter"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["addr:60", "text:com.twitter"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    persisted_verified_queries,
                ),
            )
            .await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed")?;
            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=verified&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(resolution_response.status(), StatusCode::OK);

            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let expected_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["text:com.twitter", "addr:60"],
            );

            assert_eq!(explain_payload.data, resolution_payload.data);
            assert_eq!(explain_payload.coverage, resolution_payload.coverage);
            assert_eq!(
                explain_payload.chain_positions,
                resolution_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, resolution_payload.consistency);
            assert_eq!(
                explain_payload.last_updated,
                resolution_payload.last_updated
            );
            assert_eq!(
                explain_payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(explain_payload.declared_state, None);
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": resolution_execution_summary(execution_trace_id, resource_id),
                    "verified_queries": expected_verified_queries,
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_reads_persisted_contenthash_answer_and_reuses_resolution_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000024);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution explain requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let explain_records = parse_resolution_record_keys(
                Some("text:com.twitter,contenthash,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &explain_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["text:com.twitter", "contenthash", "addr:60"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["text:com.twitter", "contenthash", "addr:60"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    persisted_verified_queries,
                ),
            )
            .await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,contenthash,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("contenthash execution explain request failed")?;
            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=verified&records=text:com.twitter,contenthash,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("contenthash verified resolution request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(resolution_response.status(), StatusCode::OK);

            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let expected_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["text:com.twitter", "contenthash", "addr:60"],
            );

            assert_eq!(explain_payload.data, resolution_payload.data);
            assert_eq!(explain_payload.coverage, resolution_payload.coverage);
            assert_eq!(
                explain_payload.provenance,
                resolution_payload.provenance
            );
            assert_eq!(
                explain_payload.chain_positions,
                resolution_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, resolution_payload.consistency);
            assert_eq!(
                explain_payload.last_updated,
                resolution_payload.last_updated
            );
            assert_eq!(explain_payload.declared_state, None);
            assert_eq!(
                resolution_payload.verified_state,
                Some(json!({
                    "verified_queries": expected_verified_queries.clone(),
                }))
            );
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": resolution_execution_summary(execution_trace_id, resource_id),
                    "verified_queries": expected_verified_queries,
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_returns_not_found_for_selector_set_cache_miss()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000022);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution explain requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let persisted_records =
                parse_resolution_record_keys(Some("addr:60"), ResolutionMode::Verified)
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &persisted_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["addr:60"]);

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["addr:60"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    persisted_verified_queries,
                ),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed")?;

            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "not_found");
            assert_eq!(
                payload.error.message,
                "persisted resolution execution explain was not found for name alice.eth in namespace ens"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_invalidation_contract_evicts_persisted_answers_from_mixed_and_explain_routes()
        -> Result<()> {
            for invalidation in [
                PersistedResolutionInvalidation::Manifest,
                PersistedResolutionInvalidation::Topology,
                PersistedResolutionInvalidation::Record,
            ] {
                run_resolution_execution_invalidation_case(invalidation)
                    .await
                    .with_context(|| {
                        format!(
                            "persisted verified resolution invalidation failed for {}",
                            invalidation.label()
                        )
                    })?;
            }

            Ok(())
        }

        #[tokio::test]
        async fn resolver_overview_contract_returns_declared_state_with_shared_projection_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let chain_id = "ethereum-mainnet";
            let resolver_address = "0x0000000000000000000000000000000000000aaa";

            bigname_storage::upsert_resolver_current_rows(
                &database.pool,
                &[resolver_current_row(chain_id, resolver_address)],
            )
            .await
            .context("failed to upsert resolver_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000AAA")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolver overview request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolverResponse = read_json(response).await?;
            assert_eq!(
                payload.data,
                json!({
                    "chain_id": chain_id,
                    "resolver_address": resolver_address,
                })
            );
            assert_eq!(
                payload.declared_state,
                json!({
                    "bindings": {
                        "status": "supported",
                        "count": 2,
                        "items": [
                            {
                                "logical_name_id": "ens:alice.eth",
                                "canonical_display_name": "Alice.eth",
                                "normalized_name": "alice.eth",
                                "namehash": "namehash:alice.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b100",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                                "binding_kind": "declared_registry_path",
                            },
                            {
                                "logical_name_id": "ens:beta.eth",
                                "canonical_display_name": "Beta.eth",
                                "normalized_name": "beta.eth",
                                "namehash": "namehash:beta.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b102",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                                "binding_kind": "resolver_alias_path",
                            }
                        ],
                    },
                    "aliases": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "logical_name_id": "ens:beta.eth",
                            "canonical_display_name": "Beta.eth",
                            "normalized_name": "beta.eth",
                            "namehash": "namehash:beta.eth",
                            "resource_id": "00000000-0000-0000-0000-00000000b102",
                            "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                            "binding_kind": "resolver_alias_path",
                        }],
                    },
                    "permissions": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "resource_id": "00000000-0000-0000-0000-00000000b100",
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "effective_powers": ["set_resolver", "set_records"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "resolver-permission-1",
                            },
                            "revocation_source": null,
                        }],
                    },
                    "role_holders": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "resource_count": 1,
                            "permission_row_count": 1,
                            "effective_powers": ["set_records", "set_resolver"],
                            "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                        }],
                    },
                    "event_summary": {
                        "status": "supported",
                        "count": 2,
                        "by_kind": {
                            "PermissionChanged": 1,
                            "ResolverChanged": 1,
                        },
                    },
                })
            );
            assert_eq!(payload.verified_state, None);
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": ["101", "202"],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "chain_id": chain_id,
                        "block_number": 202,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 7,
                        "source_family": "ens_v2_registry_l1",
                        "chain": chain_id,
                        "deployment_epoch": "ens_v2",
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "resolver_current_rebuild",
                })
            );
            assert_eq!(
                payload.coverage,
                json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
                    "enumeration_basis": "resolver_target",
                    "unsupported_reason": null,
                })
            );
            assert_eq!(
                payload.chain_positions,
                json!({
                    "ethereum": {
                        "chain_id": chain_id,
                        "block_number": 202,
                        "block_hash": "0xresolverc8",
                        "timestamp": "2026-04-17T00:00:22Z",
                    }
                })
            );
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.last_updated, "2025-06-01T17:50:02Z");

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_permissions_contract_returns_rows_with_shared_collection_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let resource_id = Uuid::from_u128(0xa300);
            let filtered_subject = "0x0000000000000000000000000000000000000abc";
            let other_subject = "0x0000000000000000000000000000000000000def";

            bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)])
                .await
                .context("failed to upsert resource for permissions conformance")?;
            bigname_storage::upsert_permissions_current_rows(
                &database.pool,
                &[
                    permission_current_row(
                        resource_id,
                        filtered_subject,
                        PermissionScope::Resource,
                        7,
                        41,
                    ),
                    permission_current_row(
                        resource_id,
                        filtered_subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000aaa"
                                .to_owned(),
                        },
                        8,
                        42,
                    ),
                    permission_current_row(
                        resource_id,
                        other_subject,
                        PermissionScope::Registry,
                        9,
                        43,
                    ),
                ],
            )
            .await
            .context("failed to upsert permissions_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/resources/{resource_id}/permissions"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResourcePermissionsResponse = read_json(response).await?;
            assert_eq!(
                permission_subjects(&payload),
                vec![filtered_subject, filtered_subject, other_subject]
            );
            assert!(payload.verified_state.is_none());
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.page_size, 3);
            assert_eq!(payload.page.sort, "subject_scope_asc");
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["permissions_current".to_owned()]
            );
            assert_eq!(payload.coverage.enumeration_basis, "resource_permissions");
            assert_eq!(payload.coverage.unsupported_reason, None);
            assert_eq!(
                payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("permissions_current_rebuild")
            );

            let resource_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resource")
                })
                .expect("resource row");
            assert_eq!(
                resource_row.get("resource_id"),
                Some(&Value::String(resource_id.to_string()))
            );
            assert_eq!(
                resource_row.get("scope"),
                Some(&json!({
                    "kind": "resource",
                    "detail": {},
                }))
            );
            assert_eq!(
                resource_row.get("effective_powers"),
                Some(&json!(["set_resolver", "set_records"]))
            );
            assert_eq!(resource_row.get("revocation_source"), Some(&Value::Null));

            let resolver_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resolver")
                })
                .expect("resolver row");
            assert_eq!(
                resolver_row.get("scope"),
                Some(&json!({
                    "kind": "resolver",
                    "detail": {
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000aaa",
                    },
                }))
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=1"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: ResourcePermissionsResponse =
                read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("resource permissions first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: ResourcePermissionsResponse =
                read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: ResourcePermissionsResponse =
                read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "subject_scope_asc",
                3,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_permissions_contract_honors_subject_and_scope_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let resource_id = Uuid::from_u128(0xa301);
            let shared_subject = "0x0000000000000000000000000000000000000abc";

            bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)])
                .await
                .context("failed to upsert resource for permissions filter conformance")?;
            bigname_storage::upsert_permissions_current_rows(
                &database.pool,
                &[
                    permission_current_row(
                        resource_id,
                        shared_subject,
                        PermissionScope::Resource,
                        7,
                        51,
                    ),
                    permission_current_row(
                        resource_id,
                        shared_subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000bbb"
                                .to_owned(),
                        },
                        8,
                        52,
                    ),
                    permission_current_row(
                        resource_id,
                        "0x0000000000000000000000000000000000000def",
                        PermissionScope::Resource,
                        9,
                        53,
                    ),
                ],
            )
            .await
            .context("failed to upsert permissions_current filter rows for conformance")?;

            let subject_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?subject={shared_subject}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions subject filter request failed")?;
            assert_eq!(subject_response.status(), StatusCode::OK);

            let subject_payload: ResourcePermissionsResponse = read_json(subject_response).await?;
            assert_eq!(
                permission_subjects(&subject_payload),
                vec![shared_subject, shared_subject]
            );

            let scope_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?scope=resolver:ethereum-mainnet:0x0000000000000000000000000000000000000bbb"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions scope filter request failed")?;
            assert_eq!(scope_response.status(), StatusCode::OK);

            let scope_payload: ResourcePermissionsResponse = read_json(scope_response).await?;
            assert_eq!(scope_payload.data.len(), 1);
            assert_eq!(
                scope_payload.data[0].get("scope"),
                Some(&json!({
                    "kind": "resolver",
                    "detail": {
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000bbb",
                    },
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_status_shaped_declared_results_by_mode_for_tuple_present()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            database
                .seed_primary_name_reverse_changed(address, "60")
                .await?;
            database
                .rebuild_primary_names_current(address, "ens", "60")
                .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name bootstrap request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name bootstrap request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("both primary-name bootstrap request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);

            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);

            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );

            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_returns_not_found_results_by_mode_for_tuple_miss()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name tuple-miss request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name tuple-miss request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("both primary-name tuple-miss request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);

            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);

            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "not_found",
                    }
                }))
            );

            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_persisted_verified_primary_name_for_exact_tuple()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000041);
            let finished_at = timestamp(1_717_172_401);
            let verified_primary_name = primary_name_verified_success(
                "ens:alice.eth",
                "alice.eth",
                "Alice.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000123",
                Uuid::from_u128(0x456),
            );

            seed_primary_name_tuple_anchor(&database, address, "60").await?;
            seed_primary_name_tuple_anchor(&database, address, "61").await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "ens",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "ens",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let other_finished_at = timestamp(1_717_172_499);
            let other_verified_primary_name = primary_name_verified_mismatch(
                "ens:other.eth",
                "other.eth",
                "other.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000456",
                Uuid::from_u128(0x999),
                "resolved_address_mismatch",
            );
            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
                    "ens",
                    address,
                    "61",
                    other_verified_primary_name.clone(),
                    other_finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
                    "ens",
                    address,
                    "61",
                    other_verified_primary_name,
                    other_finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name persisted readback request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name persisted readback request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed primary-name persisted readback request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": verified_primary_name.clone(),
                }))
            );
            assert_eq!(
                both_payload.declared_state,
                declared_payload.declared_state
            );
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            assert_primary_name_bootstrap_invariants(&declared_payload);
            assert_primary_name_persisted_readback_invariants(
                &verified_payload,
                execution_trace_id,
                finished_at,
            );
            assert_primary_name_persisted_readback_invariants(
                &both_payload,
                execution_trace_id,
                finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_exact_tuple_invalidation_evicts_only_target_persisted_answer()
        -> Result<()> {
            for invalidation in [
                PersistedPrimaryNameInvalidation::Manifest,
                PersistedPrimaryNameInvalidation::Topology,
                PersistedPrimaryNameInvalidation::Record,
            ] {
                run_primary_name_execution_invalidation_case(invalidation).await?;
            }

            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_keeps_verified_bootstrap_fallback_for_tuple_present()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            database
                .seed_primary_name_reverse_changed(address, "60")
                .await?;
            database
                .rebuild_primary_names_current(address, "ens", "60")
                .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name tuple-present request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name tuple-present request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("both primary-name tuple-present request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);

            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);

            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );

            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_requires_namespace_and_coin_type() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";

            let missing_namespace = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/primary-names/{address}?coin_type=60"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("primary-name missing-namespace request failed")?;
            let missing_coin_type = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/primary-names/{address}?namespace=ens"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("primary-name missing-coin-type request failed")?;

            assert_eq!(missing_namespace.status(), StatusCode::BAD_REQUEST);
            assert_eq!(missing_coin_type.status(), StatusCode::BAD_REQUEST);

            let missing_namespace_payload: ErrorResponse = read_json(missing_namespace).await?;
            let missing_coin_type_payload: ErrorResponse = read_json(missing_coin_type).await?;

            assert_eq!(missing_namespace_payload.error.code, "invalid_input");
            assert_eq!(
                missing_namespace_payload.error.message,
                "namespace is required"
            );
            assert!(missing_namespace_payload.error.details.is_empty());

            assert_eq!(missing_coin_type_payload.error.code, "invalid_input");
            assert_eq!(
                missing_coin_type_payload.error.message,
                "coin_type is required"
            );
            assert!(missing_coin_type_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_rejects_invalid_tuples_and_unsupported_namespaces()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let malformed_address = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/primary-names/not-an-address?namespace=ens&coin_type=60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("malformed primary-name address request failed")?;
            let malformed_coin_type = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60,61")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("malformed primary-name coin_type request failed")?;
            let unsupported_namespace = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=unknown&coin_type=60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("unsupported primary-name namespace request failed")?;

            assert_eq!(malformed_address.status(), StatusCode::BAD_REQUEST);
            assert_eq!(malformed_coin_type.status(), StatusCode::BAD_REQUEST);
            assert_eq!(unsupported_namespace.status(), StatusCode::NOT_FOUND);

            let malformed_address_payload: ErrorResponse = read_json(malformed_address).await?;
            let malformed_coin_type_payload: ErrorResponse = read_json(malformed_coin_type).await?;
            let unsupported_namespace_payload: ErrorResponse =
                read_json(unsupported_namespace).await?;

            assert_eq!(malformed_address_payload.error.code, "invalid_input");
            assert_eq!(
                malformed_address_payload.error.message,
                "address must be a 0x-prefixed 20-byte hex string"
            );
            assert!(malformed_address_payload.error.details.is_empty());

            assert_eq!(malformed_coin_type_payload.error.code, "invalid_input");
            assert_eq!(
                malformed_coin_type_payload.error.message,
                "coin_type must contain only decimal digits"
            );
            assert!(malformed_coin_type_payload.error.details.is_empty());

            assert_eq!(unsupported_namespace_payload.error.code, "not_found");
            assert_eq!(
                unsupported_namespace_payload.error.message,
                "namespace unknown is not supported"
            );
            assert!(unsupported_namespace_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_history_contract_composes_current_and_historical_matches() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let current_resource_id = Uuid::from_u128(0xa240);
            let current_token_lineage_id = Uuid::from_u128(0xa241);
            let current_surface_binding_id = Uuid::from_u128(0xb240);
            let historical_resource_id = Uuid::from_u128(0xa242);
            let historical_token_lineage_id = Uuid::from_u128(0xa243);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x540", None, 540, 1_700_000_540),
                    raw_block(
                        "ethereum-mainnet",
                        "0x541",
                        Some("0x540"),
                        541,
                        1_700_000_541,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x542",
                        Some("0x541"),
                        542,
                        1_700_000_542,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x543",
                        Some("0x542"),
                        543,
                        1_700_000_543,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x544",
                        Some("0x543"),
                        544,
                        1_700_000_544,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x545",
                        Some("0x544"),
                        545,
                        1_700_000_545,
                    ),
                ],
            )
            .await
            .context("failed to upsert raw blocks for address-history conformance")?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[
                    address_name_token_lineage(current_token_lineage_id, "0x540", 540),
                    address_name_token_lineage(historical_token_lineage_id, "0x541", 541),
                ],
            )
            .await
            .context("failed to upsert token lineages for address-history conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        current_resource_id,
                        Some(current_token_lineage_id),
                        "0x540",
                        540,
                    ),
                    address_name_resource(
                        historical_resource_id,
                        Some(historical_token_lineage_id),
                        "0x541",
                        541,
                    ),
                ],
            )
            .await
            .context("failed to upsert resources for address-history conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        "ens:current.eth",
                        "current.eth",
                        "node:current.eth",
                        540,
                    ),
                    collection_name_surface(
                        "ens:historical.eth",
                        "historical.eth",
                        "node:historical.eth",
                        541,
                    ),
                ],
            )
            .await
            .context("failed to upsert name surfaces for address-history conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    current_surface_binding_id,
                    "ens:current.eth",
                    current_resource_id,
                    "0x540",
                    540,
                    1_717_173_540,
                )],
            )
            .await
            .context("failed to upsert surface bindings for address-history conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    "ens:current.eth",
                    bigname_storage::AddressNameRelation::Registrant,
                    "current.eth",
                    "current.eth",
                    "node:current.eth",
                    current_surface_binding_id,
                    current_resource_id,
                    Some(current_token_lineage_id),
                    540,
                )],
            )
            .await
            .context("failed to upsert current address-name anchors for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "current-surface",
                        Some("ens:current.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(544),
                        Some("0x544"),
                        Some("0xtx544"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-resource",
                        None,
                        Some(current_resource_id),
                        Some("ethereum-mainnet"),
                        Some(545),
                        Some("0x545"),
                        Some("0xtx545"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-surface",
                        Some("ens:historical.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(543),
                        Some("0x543"),
                        Some("0xtx543"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-resource",
                        None,
                        Some(historical_resource_id),
                        Some("ethereum-mainnet"),
                        Some(542),
                        Some("0x542"),
                        Some("0xtx542"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    authority_history_event(
                        "historical-match",
                        "ens",
                        "ens:historical.eth",
                        historical_resource_id,
                        "RegistrationGranted",
                        541,
                        "0x541",
                        json!({
                            "registrant": "0x0000000000000000000000000000000000000ABC",
                        }),
                    ),
                ],
            )
            .await
            .context("failed to upsert normalized events for address-history conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/history/addresses/{address}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history base request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "current-resource",
                    "current-surface",
                    "historical-surface",
                    "historical-resource",
                    "historical-match",
                ]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["normalized_events".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("normalized_event_history")
            );
            assert_eq!(payload.consistency, "head");

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/history/addresses/{address}?page_size=1"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("address history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_history_contract_honors_namespace_and_relation_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000def";
            let registrant_resource_id = Uuid::from_u128(0xa250);
            let registrant_token_lineage_id = Uuid::from_u128(0xa251);
            let registrant_surface_binding_id = Uuid::from_u128(0xb250);
            let controller_resource_id = Uuid::from_u128(0xa252);
            let controller_surface_binding_id = Uuid::from_u128(0xb252);
            let basenames_resource_id = Uuid::from_u128(0xa253);
            let basenames_surface_binding_id = Uuid::from_u128(0xb253);
            let historical_resource_id = Uuid::from_u128(0xa254);
            let historical_token_lineage_id = Uuid::from_u128(0xa255);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x560", None, 560, 1_700_000_560),
                    raw_block(
                        "ethereum-mainnet",
                        "0x561",
                        Some("0x560"),
                        561,
                        1_700_000_561,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x562",
                        Some("0x561"),
                        562,
                        1_700_000_562,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x563",
                        Some("0x562"),
                        563,
                        1_700_000_563,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x564",
                        Some("0x563"),
                        564,
                        1_700_000_564,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x565",
                        Some("0x564"),
                        565,
                        1_700_000_565,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x566",
                        Some("0x565"),
                        566,
                        1_700_000_566,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x567",
                        Some("0x566"),
                        567,
                        1_700_000_567,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered raw blocks for conformance")?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[
                    address_name_token_lineage(registrant_token_lineage_id, "0x560", 560),
                    address_name_token_lineage(historical_token_lineage_id, "0x561", 561),
                ],
            )
            .await
            .context("failed to upsert filtered token lineages for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        registrant_resource_id,
                        Some(registrant_token_lineage_id),
                        "0x560",
                        560,
                    ),
                    address_name_resource(controller_resource_id, None, "0x561", 561),
                    address_name_resource(basenames_resource_id, None, "0x566", 566),
                    address_name_resource(
                        historical_resource_id,
                        Some(historical_token_lineage_id),
                        "0x562",
                        562,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        "ens:current-registrant.eth",
                        "current-registrant.eth",
                        "node:current-registrant.eth",
                        560,
                    ),
                    collection_name_surface(
                        "ens:current-controller.eth",
                        "current-controller.eth",
                        "node:current-controller.eth",
                        561,
                    ),
                    collection_name_surface(
                        "basenames:filtered.base.eth",
                        "filtered.base.eth",
                        "node:filtered.base.eth",
                        566,
                    ),
                    collection_name_surface(
                        "ens:historical-registrant.eth",
                        "historical-registrant.eth",
                        "node:historical-registrant.eth",
                        562,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered name surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        registrant_surface_binding_id,
                        "ens:current-registrant.eth",
                        registrant_resource_id,
                        "0x560",
                        560,
                        1_717_173_560,
                    ),
                    address_name_surface_binding(
                        controller_surface_binding_id,
                        "ens:current-controller.eth",
                        controller_resource_id,
                        "0x561",
                        561,
                        1_717_173_561,
                    ),
                    address_name_surface_binding(
                        basenames_surface_binding_id,
                        "basenames:filtered.base.eth",
                        basenames_resource_id,
                        "0x566",
                        566,
                        1_717_173_566,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered surface bindings for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:current-registrant.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "current-registrant.eth",
                        "current-registrant.eth",
                        "node:current-registrant.eth",
                        registrant_surface_binding_id,
                        registrant_resource_id,
                        Some(registrant_token_lineage_id),
                        560,
                    ),
                    address_name_current_row(
                        address,
                        "ens:current-controller.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "current-controller.eth",
                        "current-controller.eth",
                        "node:current-controller.eth",
                        controller_surface_binding_id,
                        controller_resource_id,
                        None,
                        561,
                    ),
                    address_name_current_row(
                        address,
                        "basenames:filtered.base.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "filtered.base.eth",
                        "filtered.base.eth",
                        "node:filtered.base.eth",
                        basenames_surface_binding_id,
                        basenames_resource_id,
                        None,
                        566,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered address-name anchors for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "historical-registrant-match-surface",
                        Some("ens:historical-registrant.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(562),
                        Some("0x562"),
                        Some("0xtx562"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-registrant-match-resource",
                        None,
                        Some(historical_resource_id),
                        Some("ethereum-mainnet"),
                        Some(561),
                        Some("0x561"),
                        Some("0xtx561"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    authority_history_event(
                        "historical-registrant-match",
                        "ens",
                        "ens:historical-registrant.eth",
                        historical_resource_id,
                        "RegistrationGranted",
                        560,
                        "0x560",
                        json!({
                            "registrant": "0x0000000000000000000000000000000000000DEF",
                        }),
                    ),
                    history_event(
                        "current-registrant-surface",
                        Some("ens:current-registrant.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(564),
                        Some("0x564"),
                        Some("0xtx564"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-registrant-resource",
                        None,
                        Some(registrant_resource_id),
                        Some("ethereum-mainnet"),
                        Some(565),
                        Some("0x565"),
                        Some("0xtx565"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-controller-surface",
                        Some("ens:current-controller.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(566),
                        Some("0x566"),
                        Some("0xtx566"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-controller-resource",
                        None,
                        Some(controller_resource_id),
                        Some("ethereum-mainnet"),
                        Some(567),
                        Some("0x567"),
                        Some("0xtx567"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        ..history_event(
                            "filtered-basenames",
                            Some("basenames:filtered.base.eth"),
                            Some(basenames_resource_id),
                            Some("ethereum-mainnet"),
                            Some(563),
                            Some("0x563"),
                            Some("0xtx563"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                ],
            )
            .await
            .context("failed to upsert filtered normalized events for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?namespace=ens&relation=registrant"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("filtered address history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "current-registrant-resource",
                    "current-registrant-surface",
                    "historical-registrant-match-surface",
                    "historical-registrant-match-resource",
                    "historical-registrant-match",
                ]
            );
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_history_contract_honors_scope_and_relation_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000123";
            let current_resource_id = Uuid::from_u128(0xa260);
            let current_token_lineage_id = Uuid::from_u128(0xa261);
            let current_surface_binding_id = Uuid::from_u128(0xb260);
            let historical_resource_id = Uuid::from_u128(0xa262);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x570", None, 570, 1_700_000_570),
                    raw_block(
                        "ethereum-mainnet",
                        "0x571",
                        Some("0x570"),
                        571,
                        1_700_000_571,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x572",
                        Some("0x571"),
                        572,
                        1_700_000_572,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x573",
                        Some("0x572"),
                        573,
                        1_700_000_573,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x574",
                        Some("0x573"),
                        574,
                        1_700_000_574,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x575",
                        Some("0x574"),
                        575,
                        1_700_000_575,
                    ),
                ],
            )
            .await
            .context("failed to upsert scope raw blocks for conformance")?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    current_token_lineage_id,
                    "0x570",
                    570,
                )],
            )
            .await
            .context("failed to upsert scope token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        current_resource_id,
                        Some(current_token_lineage_id),
                        "0x570",
                        570,
                    ),
                    address_name_resource(historical_resource_id, None, "0x571", 571),
                ],
            )
            .await
            .context("failed to upsert scope resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[collection_name_surface(
                    "ens:current-controller.eth",
                    "current-controller.eth",
                    "node:current-controller.eth",
                    570,
                )],
            )
            .await
            .context("failed to upsert scope name surface for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    current_surface_binding_id,
                    "ens:current-controller.eth",
                    current_resource_id,
                    "0x570",
                    570,
                    1_717_173_570,
                )],
            )
            .await
            .context("failed to upsert scope surface binding for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    "ens:current-controller.eth",
                    bigname_storage::AddressNameRelation::EffectiveController,
                    "current-controller.eth",
                    "current-controller.eth",
                    "node:current-controller.eth",
                    current_surface_binding_id,
                    current_resource_id,
                    Some(current_token_lineage_id),
                    570,
                )],
            )
            .await
            .context("failed to upsert scope address-name anchors for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "current-controller-surface",
                        Some("ens:current-controller.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(574),
                        Some("0x574"),
                        Some("0xtx574"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-controller-resource",
                        None,
                        Some(current_resource_id),
                        Some("ethereum-mainnet"),
                        Some(575),
                        Some("0x575"),
                        Some("0xtx575"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-controller-surface",
                        Some("ens:historical-controller.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(573),
                        Some("0x573"),
                        Some("0xtx573"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-controller-resource",
                        None,
                        Some(historical_resource_id),
                        Some("ethereum-mainnet"),
                        Some(572),
                        Some("0x572"),
                        Some("0xtx572"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    authority_history_event(
                        "historical-controller-match",
                        "ens",
                        "ens:historical-controller.eth",
                        historical_resource_id,
                        "AuthorityTransferred",
                        571,
                        "0x571",
                        json!({
                            "owner": "0x0000000000000000000000000000000000000123",
                        }),
                    ),
                ],
            )
            .await
            .context("failed to upsert scope normalized events for conformance")?;

            let surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?relation=effective_controller&scope=surface"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history surface-scope request failed")?;
            assert_eq!(surface_response.status(), StatusCode::OK);

            let surface_payload: HistoryResponse = read_json(surface_response).await?;
            assert_eq!(
                history_event_identities(&surface_payload),
                vec![
                    "current-controller-surface",
                    "historical-controller-surface",
                    "historical-controller-match",
                ]
            );
            assert_eq!(
                surface_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested surface scope"
            );

            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?relation=effective_controller&scope=resource"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history resource-scope request failed")?;
            assert_eq!(resource_response.status(), StatusCode::OK);

            let resource_payload: HistoryResponse = read_json(resource_response).await?;
            assert_eq!(
                history_event_identities(&resource_payload),
                vec![
                    "current-controller-resource",
                    "historical-controller-resource",
                    "historical-controller-match",
                ]
            );
            assert_eq!(
                resource_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested resource scope"
            );

            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?relation=effective_controller&scope=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history combined request failed")?;
            assert_eq!(both_response.status(), StatusCode::OK);

            let both_payload: HistoryResponse = read_json(both_response).await?;
            assert_eq!(
                history_event_identities(&both_payload),
                vec![
                    "current-controller-resource",
                    "current-controller-surface",
                    "historical-controller-surface",
                    "historical-controller-resource",
                    "historical-controller-match",
                ]
            );
            assert_eq!(
                both_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_history_contract_returns_declared_rows_with_empty_declared_state()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0xa001);
            let surface_binding_id = Uuid::from_u128(0xb001);
            let manifest_id_v7 = database
                .insert_manifest(
                    "ens",
                    "ens_v1_registry_l1",
                    "ethereum-mainnet",
                    "bootstrap",
                    7,
                    "active",
                    "history-test-v1",
                )
                .await?;
            let manifest_id_v8 = database
                .insert_manifest(
                    "ens",
                    "ens_v1_registry_l1",
                    "ethereum-mainnet",
                    "bootstrap-next",
                    8,
                    "active",
                    "history-test-v2",
                )
                .await?;

            database
                .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
                .await?;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
                    raw_block(
                        "ethereum-mainnet",
                        "0x101",
                        Some("0x100"),
                        101,
                        1_700_000_101,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x102",
                        Some("0x101"),
                        102,
                        1_700_000_102,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x103",
                        Some("0x102"),
                        103,
                        1_700_000_103,
                    ),
                ],
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        manifest_version: 7,
                        source_manifest_id: Some(manifest_id_v7),
                        ..history_event(
                            "history:canonical",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(100),
                            Some("0x100"),
                            Some("0xtx100"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        manifest_version: 8,
                        source_manifest_id: Some(manifest_id_v8),
                        ..history_event(
                            "history:safe",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(101),
                            Some("0x101"),
                            Some("0xtx101"),
                            Some(0),
                            CanonicalityState::Safe,
                        )
                    },
                    NormalizedEvent {
                        manifest_version: 7,
                        source_manifest_id: Some(manifest_id_v7),
                        ..history_event(
                            "history:finalized",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(102),
                            Some("0x102"),
                            Some("0xtx102"),
                            Some(0),
                            CanonicalityState::Finalized,
                        )
                    },
                    history_event(
                        "history:observed",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("ethereum-mainnet"),
                        Some(103),
                        Some("0x103"),
                        Some("0xtx103"),
                        Some(0),
                        CanonicalityState::Observed,
                    ),
                ],
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec!["history:finalized", "history:safe", "history:canonical"]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                payload
                    .provenance
                    .get("manifest_versions")
                    .and_then(Value::as_array)
                    .map(Vec::len),
                Some(2)
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/alice.eth?page_size=1")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("name history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_history_contract_returns_declared_rows_with_empty_declared_state()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0xa300);
            let surface_binding_id = Uuid::from_u128(0xb300);

            database
                .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
                .await?;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
                    raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
                    raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
                ],
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "no-chain-position",
                        Some(logical_name_id),
                        Some(resource_id),
                        None,
                        None,
                        None,
                        None,
                        None,
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "ethereum-higher-log",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("ethereum-mainnet"),
                        Some(100),
                        Some("0xe100"),
                        Some("0xtx100"),
                        Some(7),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "base-same-height",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("base-mainnet"),
                        Some(100),
                        Some("0xb100"),
                        Some("0xtx090"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "base-higher-height",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("base-mainnet"),
                        Some(101),
                        Some("0xb101"),
                        Some("0xtx101"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                ],
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!("/v1/history/resources/{resource_id}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "base-higher-height",
                    "base-same-height",
                    "ethereum-higher-log",
                    "no-chain-position",
                ]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(
                payload.chain_positions,
                json!({
                    "base": {
                        "chain_id": "base-mainnet",
                        "block_number": 101,
                        "block_hash": "0xb101",
                        "timestamp": "2023-11-14T22:20:01Z"
                    },
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 100,
                        "block_hash": "0xe100",
                        "timestamp": "2023-11-14T22:20:00Z"
                    }
                })
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!("/v1/history/resources/{resource_id}?page_size=1"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("resource history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!(
                            "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!(
                            "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .context("failed to read conformance response body")?;
            serde_json::from_slice(&bytes).context("failed to decode conformance response JSON")
        }

        fn timestamp(seconds: i64) -> OffsetDateTime {
            OffsetDateTime::from_unix_timestamp(seconds)
                .expect("conformance timestamp must be valid")
        }

        fn raw_block(
            chain_id: &str,
            block_hash: &str,
            parent_hash: Option<&str>,
            block_number: i64,
            block_timestamp: i64,
        ) -> RawBlock {
            RawBlock {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                parent_hash: parent_hash.map(str::to_owned),
                block_number,
                block_timestamp: timestamp(block_timestamp),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn resource(resource_id: Uuid) -> Resource {
            Resource {
                resource_id,
                token_lineage_id: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn name_surface(logical_name_id: &str) -> NameSurface {
            let (namespace, normalized_name) = logical_name_id
                .split_once(':')
                .expect("logical_name_id must include namespace");

            NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: namespace.to_owned(),
                input_name: normalized_name.to_owned(),
                canonical_display_name: "Alice.eth".to_owned(),
                normalized_name: normalized_name.to_owned(),
                dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
                namehash: format!("namehash:{normalized_name}"),
                labelhashes: vec!["labelhash:alice".to_owned()],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsurface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn surface_binding(
            surface_binding_id: Uuid,
            logical_name_id: &str,
            resource_id: Uuid,
            active_from: OffsetDateTime,
        ) -> SurfaceBinding {
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from,
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbinding".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn history_event(
            event_identity: &str,
            logical_name_id: Option<&str>,
            resource_id: Option<Uuid>,
            chain_id: Option<&str>,
            block_number: Option<i64>,
            block_hash: Option<&str>,
            transaction_hash: Option<&str>,
            log_index: Option<i64>,
            canonicality_state: CanonicalityState,
        ) -> NormalizedEvent {
            NormalizedEvent {
                event_identity: event_identity.to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: logical_name_id.map(str::to_owned),
                resource_id,
                event_kind: "HistoryEvent".to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                manifest_version: 7,
                source_manifest_id: None,
                chain_id: chain_id.map(str::to_owned),
                block_number,
                block_hash: block_hash.map(str::to_owned),
                transaction_hash: transaction_hash.map(str::to_owned),
                log_index,
                raw_fact_ref: json!({
                    "kind": "raw_log",
                    "event_identity": event_identity,
                }),
                derivation_kind: "history_test".to_owned(),
                canonicality_state,
                before_state: json!({
                    "provenance": {
                        "before": event_identity,
                    }
                }),
                after_state: json!({
                    "provenance": {
                        "after": event_identity,
                    },
                    "coverage": {
                        "status": "full",
                        "exhaustiveness": "authoritative",
                        "source_classes_considered": ["normalized_events"],
                        "enumeration_basis": event_identity,
                        "unsupported_reason": null,
                    }
                }),
            }
        }

        fn authority_history_event(
            event_identity: &str,
            namespace: &str,
            logical_name_id: &str,
            resource_id: Uuid,
            event_kind: &str,
            block_number: i64,
            block_hash: &str,
            after_state: Value,
        ) -> NormalizedEvent {
            NormalizedEvent {
                namespace: namespace.to_owned(),
                event_kind: event_kind.to_owned(),
                source_family: "ens_v1_registrar_l1".to_owned(),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                after_state,
                before_state: json!({}),
                ..history_event(
                    event_identity,
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(block_number),
                    Some(block_hash),
                    Some(&format!("0xtx{block_number}")),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            }
        }

        fn history_event_identities(payload: &HistoryResponse) -> Vec<&str> {
            payload
                .data
                .iter()
                .map(|row| {
                    row.get("event_identity")
                        .and_then(Value::as_str)
                        .expect("history row must include event_identity")
                })
                .collect()
        }

        fn permission_current_row(
            resource_id: Uuid,
            subject: &str,
            scope: PermissionScope,
            manifest_version: i64,
            block_number: i64,
        ) -> PermissionsCurrentRow {
            PermissionsCurrentRow {
                resource_id,
                subject: subject.to_owned(),
                scope,
                effective_powers: json!([
                    "set_resolver",
                    if manifest_version % 2 == 0 {
                        "create_subnames"
                    } else {
                        "set_records"
                    }
                ]),
                grant_source: json!({
                    "kind": "normalized_event",
                    "manifest_version": manifest_version,
                }),
                revocation_source: None,
                inheritance_path: json!([
                    {
                        "kind": "resource_authority",
                        "resource_id": resource_id,
                    }
                ]),
                transfer_behavior: json!({
                    "kind": "resource_rebound",
                }),
                provenance: json!({
                    "normalized_event_ids": [block_number, block_number + 1],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": manifest_version,
                        "source_family": "ens_v2_registry_l1",
                        "chain": "ethereum-mainnet",
                        "deployment_epoch": "ens_v2",
                    }],
                    "derivation_kind": "permissions_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["permissions_current"],
                    "enumeration_basis": "resource_permissions",
                    "unsupported_reason": null,
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xperm{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized",
                    }
                }),
                manifest_version,
                last_recomputed_at: timestamp(1_717_174_000 + block_number),
            }
        }

        fn permission_subjects(payload: &ResourcePermissionsResponse) -> Vec<&str> {
            payload
                .data
                .iter()
                .map(|row| {
                    row.get("subject")
                        .and_then(Value::as_str)
                        .expect("permission row must include subject")
                })
                .collect()
        }

        fn assert_primary_name_route_common_invariants(payload: &PrimaryNameResponse) {
            assert_eq!(
                payload.coverage,
                json!({
                    "status": "unsupported",
                    "exhaustiveness": "not_applicable",
                    "source_classes_considered": [],
                    "enumeration_basis": "primary_name_lookup",
                    "unsupported_reason": "primary-name coverage is not yet supported",
                })
            );
            assert_eq!(payload.chain_positions, json!({}));
            assert_eq!(payload.consistency, "head");
        }

        fn assert_primary_name_bootstrap_invariants(payload: &PrimaryNameResponse) {
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": [],
                    "execution_trace_id": null,
                    "derivation_kind": "primary_name_route_bootstrap",
                })
            );
            assert_primary_name_route_common_invariants(payload);
            assert!(payload.last_updated.ends_with('Z'));
        }

        fn assert_primary_name_persisted_readback_invariants(
            payload: &PrimaryNameResponse,
            execution_trace_id: Uuid,
            finished_at: OffsetDateTime,
        ) {
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": primary_name_execution_manifest_versions(),
                    "execution_trace_id": execution_trace_id.to_string(),
                    "derivation_kind": "primary_name_route_bootstrap",
                })
            );
            assert_primary_name_route_common_invariants(payload);
            assert_eq!(payload.last_updated, format_timestamp(finished_at));
        }

        fn stable_row_strings(rows: &[Value]) -> Vec<String> {
            rows.iter()
                .map(|row| serde_json::to_string(row).expect("response rows must serialize"))
                .collect()
        }

        fn assert_replay_stable_pagination(
            base_rows: &[Value],
            base_page: &HistoryPageResponse,
            first_rows: &[Value],
            first_page: &HistoryPageResponse,
            second_rows: &[Value],
            second_page: &HistoryPageResponse,
            replay_rows: &[Value],
            replay_page: &HistoryPageResponse,
            expected_sort: &str,
            expected_unpaged_page_size: u64,
            expected_paged_page_size: u64,
        ) {
            let base_rows = stable_row_strings(base_rows);
            let first_rows = stable_row_strings(first_rows);
            let second_rows = stable_row_strings(second_rows);
            let replay_rows = stable_row_strings(replay_rows);

            assert_eq!(base_page.cursor, None);
            assert_eq!(base_page.next_cursor, None);
            assert_eq!(base_page.page_size, expected_unpaged_page_size);
            assert_eq!(base_page.sort, expected_sort);

            assert_eq!(first_page.cursor, None);
            assert_eq!(first_page.page_size, expected_paged_page_size);
            assert_eq!(first_page.sort, expected_sort);

            let applied_cursor = first_page
                .next_cursor
                .clone()
                .expect("first page must return a cursor for replay assertions");

            assert_eq!(
                first_rows,
                base_rows
                    .iter()
                    .take(first_rows.len())
                    .cloned()
                    .collect::<Vec<_>>()
            );

            assert_eq!(second_page.cursor.as_deref(), Some(applied_cursor.as_str()));
            assert_eq!(second_page.page_size, expected_paged_page_size);
            assert_eq!(second_page.sort, expected_sort);
            assert_eq!(
                second_rows,
                base_rows
                    .iter()
                    .skip(first_rows.len())
                    .take(second_rows.len())
                    .cloned()
                    .collect::<Vec<_>>()
            );

            assert_eq!(replay_page.cursor.as_deref(), Some(applied_cursor.as_str()));
            assert_eq!(replay_page, second_page);
            assert_eq!(replay_rows, second_rows);
        }

        fn collection_name_surface(
            logical_name_id: &str,
            display_name: &str,
            namehash: &str,
            block_number: i64,
        ) -> NameSurface {
            let namespace = logical_name_id
                .split_once(':')
                .map(|(namespace, _)| namespace)
                .expect("logical_name_id must include namespace")
                .to_owned();

            NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace,
                input_name: display_name.to_owned(),
                canonical_display_name: display_name.to_owned(),
                normalized_name: display_name.to_owned(),
                dns_encoded_name: display_name.as_bytes().to_vec(),
                namehash: namehash.to_owned(),
                labelhashes: vec![format!("labelhash:{display_name}")],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: format!("0xsurface{block_number:02x}"),
                block_number,
                provenance: json!({"seed": "children_surface"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn declared_child_row(
            parent_logical_name_id: &str,
            child_logical_name_id: &str,
            display_name: &str,
            namehash: &str,
            normalized_event_id: i64,
            block_number: i64,
        ) -> bigname_storage::ChildrenCurrentRow {
            bigname_storage::ChildrenCurrentRow {
                parent_logical_name_id: parent_logical_name_id.to_owned(),
                child_logical_name_id: child_logical_name_id.to_owned(),
                surface_class: "declared".to_owned(),
                namespace: "ens".to_owned(),
                canonical_display_name: display_name.to_owned(),
                normalized_name: display_name.to_owned(),
                namehash: namehash.to_owned(),
                provenance: json!({
                    "normalized_event_ids": [normalized_event_id],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "ens_v1_registry_l1",
                        "source_manifest_id": null,
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "children_current_rebuild",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xblock{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized"
                    }
                }),
                manifest_version: 1,
                last_recomputed_at: timestamp(1_717_172_000 + block_number),
            }
        }

        fn address_name_token_lineage(
            token_lineage_id: Uuid,
            block_hash: &str,
            block_number: i64,
        ) -> TokenLineage {
            TokenLineage {
                token_lineage_id,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"seed": "address_name_token_lineage"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn address_name_resource(
            resource_id: Uuid,
            token_lineage_id: Option<Uuid>,
            block_hash: &str,
            block_number: i64,
        ) -> Resource {
            Resource {
                resource_id,
                token_lineage_id,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"seed": "address_name_resource"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn address_name_surface_binding(
            surface_binding_id: Uuid,
            logical_name_id: &str,
            resource_id: Uuid,
            block_hash: &str,
            block_number: i64,
            active_from: i64,
        ) -> SurfaceBinding {
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(active_from),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"seed": "address_name_binding"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn address_name_current_row(
            address: &str,
            logical_name_id: &str,
            relation: bigname_storage::AddressNameRelation,
            display_name: &str,
            normalized_name: &str,
            namehash: &str,
            surface_binding_id: Uuid,
            resource_id: Uuid,
            token_lineage_id: Option<Uuid>,
            block_number: i64,
        ) -> bigname_storage::AddressNameCurrentRow {
            bigname_storage::AddressNameCurrentRow {
                address: address.to_owned(),
                logical_name_id: logical_name_id.to_owned(),
                relation,
                namespace: logical_name_id
                    .split_once(':')
                    .map(|(namespace, _)| namespace)
                    .expect("logical_name_id must include namespace")
                    .to_owned(),
                canonical_display_name: display_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                namehash: namehash.to_owned(),
                surface_binding_id,
                resource_id,
                token_lineage_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                provenance: json!({
                    "normalized_event_ids": [block_number],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 3,
                        "source_family": "ens_v1_registrar_l1",
                        "source_manifest_id": null,
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "address_names_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "unsupported_reason": null,
                    "enumeration_basis": "surface_current_relations",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xaddr{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized"
                    }
                }),
                manifest_version: 3,
                last_recomputed_at: timestamp(1_717_173_000 + block_number),
            }
        }

        fn resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
            ResolverCurrentRow {
                chain_id: chain_id.to_owned(),
                resolver_address: resolver_address.to_owned(),
                declared_summary: json!({
                    "bindings": {
                        "status": "supported",
                        "count": 2,
                        "items": [
                            {
                                "logical_name_id": "ens:alice.eth",
                                "canonical_display_name": "Alice.eth",
                                "normalized_name": "alice.eth",
                                "namehash": "namehash:alice.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b100",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                                "binding_kind": "declared_registry_path",
                            },
                            {
                                "logical_name_id": "ens:beta.eth",
                                "canonical_display_name": "Beta.eth",
                                "normalized_name": "beta.eth",
                                "namehash": "namehash:beta.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b102",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                                "binding_kind": "resolver_alias_path",
                            }
                        ],
                    },
                    "aliases": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "logical_name_id": "ens:beta.eth",
                            "canonical_display_name": "Beta.eth",
                            "normalized_name": "beta.eth",
                            "namehash": "namehash:beta.eth",
                            "resource_id": "00000000-0000-0000-0000-00000000b102",
                            "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                            "binding_kind": "resolver_alias_path",
                        }],
                    },
                    "permissions": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "resource_id": "00000000-0000-0000-0000-00000000b100",
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "effective_powers": ["set_resolver", "set_records"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "resolver-permission-1",
                            },
                            "revocation_source": null,
                        }],
                    },
                    "role_holders": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "resource_count": 1,
                            "permission_row_count": 1,
                            "effective_powers": ["set_records", "set_resolver"],
                            "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                        }],
                    },
                    "event_summary": {
                        "status": "supported",
                        "count": 2,
                        "by_kind": {
                            "PermissionChanged": 1,
                            "ResolverChanged": 1,
                        },
                    },
                }),
                provenance: json!({
                    "normalized_event_ids": [101, 202],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "chain_id": chain_id,
                        "block_number": 202,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 7,
                        "source_family": "ens_v2_registry_l1",
                        "chain": chain_id,
                        "deployment_epoch": "ens_v2",
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "resolver_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
                    "unsupported_reason": null,
                    "enumeration_basis": "resolver_target",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": chain_id,
                        "block_number": 202,
                        "block_hash": "0xresolverc8",
                        "timestamp": "2026-04-17T00:00:22Z",
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        chain_id: "finalized",
                    }
                }),
                manifest_version: 7,
                last_recomputed_at: timestamp(1_748_800_202),
            }
        }

        fn address_name_name_current_row(
            logical_name_id: &str,
            canonical_display_name: &str,
            normalized_name: &str,
            namehash: &str,
            surface_binding_id: Uuid,
            resource_id: Uuid,
            token_lineage_id: Option<Uuid>,
            block_number: i64,
            declared_summary: Value,
        ) -> bigname_storage::NameCurrentRow {
            bigname_storage::NameCurrentRow {
                logical_name_id: logical_name_id.to_owned(),
                namespace: logical_name_id
                    .split_once(':')
                    .map(|(namespace, _)| namespace)
                    .expect("logical_name_id must include namespace")
                    .to_owned(),
                canonical_display_name: canonical_display_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                namehash: namehash.to_owned(),
                surface_binding_id: Some(surface_binding_id),
                resource_id: Some(resource_id),
                token_lineage_id,
                binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
                declared_summary,
                provenance: json!({
                    "normalized_event_ids": [block_number, block_number + 1],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 3,
                        "source_family": "ens_v1_registry",
                        "chain": "ethereum-mainnet",
                        "deployment_epoch": "ens_v1",
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "projection_apply",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "unsupported_reason": null,
                    "enumeration_basis": "exact_name",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xname{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized"
                    }
                }),
                manifest_version: 3,
                last_recomputed_at: timestamp(1_717_175_000 + block_number),
            }
        }

        fn exact_name_control_summary() -> Value {
            json!({
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "AuthorityTransferred",
            })
        }

        fn exact_name_authority_summary(resource_id: Uuid, token_lineage_id: Uuid) -> Value {
            json!({
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path",
            })
        }

        fn exact_name_surface_binding_summary(surface_binding_id: Uuid) -> Value {
            json!({
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path",
            })
        }

        fn exact_name_resolver_summary() -> Value {
            json!({
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged",
            })
        }

        fn resolution_record_inventory_boundary(logical_name_id: &str, resource_id: Uuid) -> Value {
            json!({
                "logical_name_id": logical_name_id,
                "resource_id": resource_id.to_string(),
                "normalized_event_id": null,
                "event_kind": null,
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 106,
                    "block_hash": "0xhistorysurface",
                    "timestamp": "2024-05-31T16:08:26Z",
                },
            })
        }

        fn resolution_record_inventory_enumeration_basis() -> Value {
            json!({
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false,
            })
        }

        fn resolution_record_inventory_selectors() -> Value {
            json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true,
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "cacheable": true,
                },
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "cacheable": false,
                }
            ])
        }

        fn resolution_record_inventory_explicit_gaps() -> Value {
            json!([
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver",
                }
            ])
        }

        fn resolution_record_inventory_unsupported_families() -> Value {
            json!([
                {
                    "record_family": "abi",
                    "unsupported_reason": "resolver_family_pending",
                },
                {
                    "record_family": "pubkey",
                    "unsupported_reason": "resolver_family_pending",
                }
            ])
        }

        fn resolution_record_inventory_last_change() -> Value {
            json!({
                "normalized_event_id": 1200,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 106,
                    "block_hash": "0xhistorysurface",
                    "timestamp": "2024-05-31T16:08:26Z",
                }
            })
        }

        fn resolution_contenthash_value() -> &'static str {
            "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
        }

        fn resolution_record_cache_entries(record_keys: &[&str]) -> Vec<Value> {
            record_keys
                .iter()
                .map(|record_key| match *record_key {
                    "addr:60" => json!({
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x0000000000000000000000000000000000000abc",
                        }
                    }),
                    "avatar" => json!({
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    }),
                    "text:com.twitter" => json!({
                        "record_key": "text:com.twitter",
                        "record_family": "text",
                        "selector_key": "com.twitter",
                        "status": "not_found",
                    }),
                    "contenthash" => json!({
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "status": "not_found",
                    }),
                    unexpected => panic!("unexpected direct ENS record selector {unexpected}"),
                })
                .collect()
        }

        fn resolution_record_inventory_current_row(
            logical_name_id: &str,
            resource_id: Uuid,
        ) -> RecordInventoryCurrentRow {
            RecordInventoryCurrentRow {
                resource_id,
                record_version_boundary: resolution_record_inventory_boundary(
                    logical_name_id,
                    resource_id,
                ),
                enumeration_basis: resolution_record_inventory_enumeration_basis(),
                selectors: resolution_record_inventory_selectors(),
                explicit_gaps: resolution_record_inventory_explicit_gaps(),
                unsupported_families: resolution_record_inventory_unsupported_families(),
                last_change: Some(resolution_record_inventory_last_change()),
                entries: json!(resolution_record_cache_entries(&["addr:60", "avatar"])),
                provenance: json!({
                    "normalized_event_ids": [1200],
                    "derivation_kind": "record_inventory_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "enumeration_basis": "declared_record_inventory",
                }),
                chain_positions: json!({
                    "ethereum-mainnet": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 106,
                        "block_hash": "0xhistorysurface",
                        "timestamp": "2024-05-31T16:08:26Z",
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized",
                    }
                }),
                manifest_version: 7,
                last_recomputed_at: timestamp(1_717_171_718),
            }
        }

        fn resolution_supported_declared_state(
            logical_name_id: &str,
            resource_id: Uuid,
            record_cache_keys: &[&str],
        ) -> Value {
            let record_version_boundary =
                resolution_record_inventory_boundary(logical_name_id, resource_id);
            json!({
                "topology": {
                    "registry_path": [
                        {
                            "logical_name_id": "ens:alice.eth",
                            "namespace": "ens",
                            "normalized_name": "alice.eth",
                            "canonical_display_name": "Alice.eth",
                            "namehash": "namehash:alice.eth",
                            "resource_id": resource_id.to_string(),
                            "binding_kind": "declared_registry_path",
                        }
                    ],
                    "subregistry_path": [],
                    "resolver_path": [
                        {
                            "logical_name_id": "ens:alice.eth",
                            "namespace": "ens",
                            "normalized_name": "alice.eth",
                            "canonical_display_name": "Alice.eth",
                            "resource_id": resource_id.to_string(),
                            "chain_id": "ethereum-mainnet",
                            "address": "0x0000000000000000000000000000000000000abc",
                            "latest_event_kind": "ResolverChanged",
                        }
                    ],
                    "wildcard": {
                        "source": null,
                        "matched_labels": [],
                    },
                    "alias": {
                        "final_target": null,
                        "hops": [],
                    },
                    "version_boundaries": {
                        "topology_version_boundary": record_version_boundary.clone(),
                        "record_version_boundary": record_version_boundary.clone(),
                    },
                    "transport": {
                        "source_chain_id": null,
                        "target_chain_id": null,
                        "contract_address": null,
                        "latest_event_kind": null,
                    },
                },
                "record_inventory": {
                    "record_version_boundary": record_version_boundary.clone(),
                    "enumeration_basis": resolution_record_inventory_enumeration_basis(),
                    "selectors": resolution_record_inventory_selectors(),
                    "explicit_gaps": resolution_record_inventory_explicit_gaps(),
                    "unsupported_families": resolution_record_inventory_unsupported_families(),
                    "last_change": resolution_record_inventory_last_change(),
                },
                "record_cache": {
                    "record_version_boundary": record_version_boundary,
                    "entries": resolution_record_cache_entries(record_cache_keys),
                }
            })
        }

        fn record_selector_identity_tuple(value: &Value) -> (String, String, Option<String>) {
            let selector_key = match value.get("selector_key") {
                Some(Value::Null) => None,
                Some(Value::String(selector_key)) => Some(selector_key.clone()),
                Some(_) => panic!("selector_key must be a string or null"),
                None => panic!("selector_key must be present"),
            };

            (
                value
                    .get("record_key")
                    .and_then(Value::as_str)
                    .expect("record_key must be present")
                    .to_owned(),
                value
                    .get("record_family")
                    .and_then(Value::as_str)
                    .expect("record_family must be present")
                    .to_owned(),
                selector_key,
            )
        }

        fn resolution_unsupported_verified_state(record_keys: &[&str]) -> Value {
            json!({
                "verified_queries": record_keys
                    .iter()
                    .map(|record_key| {
                        json!({
                            "record_key": record_key,
                            "status": "unsupported",
                            "unsupported_reason": "verified resolution entrypoint is not yet supported",
                        })
                    })
                    .collect::<Vec<_>>()
            })
        }

        fn resolution_execution_verified_queries(
            execution_trace_id: Uuid,
            record_keys: &[&str],
        ) -> Value {
            json!(
                record_keys
                    .iter()
                    .map(|record_key| match *record_key {
                        "addr:60" => json!({
                            "record_key": "addr:60",
                            "status": "success",
                            "value": {
                                "coin_type": "60",
                                "value": "0x00000000000000000000000000000000000000aa",
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        "text:com.twitter" => json!({
                            "record_key": "text:com.twitter",
                            "status": "not_found",
                            "failure_reason": "no_text_record",
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        "contenthash" => json!({
                            "record_key": "contenthash",
                            "status": "success",
                            "value": {
                                "value": resolution_contenthash_value(),
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        unexpected => panic!(
                            "unexpected persisted verified resolution selector {unexpected}"
                        ),
                    })
                    .collect::<Vec<_>>()
            )
        }

        fn resolution_execution_trace(
            execution_trace_id: Uuid,
            request_key: &str,
            request_record_keys: &[&str],
            verified_queries: Value,
        ) -> ExecutionTrace {
            ExecutionTrace {
                execution_trace_id,
                request_type: "verified_resolution".to_owned(),
                request_key: request_key.to_owned(),
                namespace: "ens".to_owned(),
                chain_context: json!({
                    "requested_positions": [{
                        "chain_id": "ethereum-mainnet",
                        "block_number": 106,
                        "block_hash": "0xhistorysurface",
                    }],
                }),
                manifest_context: json!({
                    "manifest_versions": [{
                        "source_family": "ens_execution",
                        "manifest_version": 5,
                    }]
                }),
                contracts_called: json!([
                    {
                        "chain_id": "ethereum-mainnet",
                        "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                        "selector": "0x9061b923",
                    }
                ]),
                gateway_digests: json!([]),
                final_payload: Some(json!({
                    "verified_queries": verified_queries.clone(),
                })),
                failure_payload: None,
                request_metadata: json!({
                    "surface": "alice.eth",
                    "record_keys": request_record_keys,
                    "entrypoint": "universal_resolver",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                }),
                finished_at: Some(timestamp(1_717_171_900)),
                steps: vec![
                    ExecutionTraceStep {
                        step_index: 0,
                        step_kind: "load_declared_topology".to_owned(),
                        input_digest: Some("sha256:topology-input".to_owned()),
                        output_digest: Some("sha256:topology-output".to_owned()),
                        latency_ms: Some(4),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "entrypoint": "universal_resolver",
                            "resolver": "0x0000000000000000000000000000000000000abc",
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 1,
                        step_kind: "call_universal_resolver".to_owned(),
                        input_digest: Some("sha256:resolver-input".to_owned()),
                        output_digest: Some("sha256:resolver-output".to_owned()),
                        latency_ms: Some(28),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "name": "alice.eth",
                            "record_count": request_record_keys.len(),
                        }),
                    },
                ],
            }
        }

        fn resolution_execution_outcome(
            execution_trace_id: Uuid,
            cache_key: ExecutionCacheKey,
            verified_queries: Value,
        ) -> ExecutionOutcome {
            ExecutionOutcome {
                cache_key,
                execution_trace_id,
                request_type: "verified_resolution".to_owned(),
                namespace: "ens".to_owned(),
                outcome_payload: Some(json!({
                    "verified_queries": verified_queries,
                })),
                failure_payload: None,
                finished_at: timestamp(1_717_171_900),
            }
        }

        fn resolution_execution_summary(execution_trace_id: Uuid, resource_id: Uuid) -> Value {
            json!({
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900)),
            })
        }

        fn primary_name_execution_requested_chain_positions() -> Value {
            json!([{
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_010,
                "block_hash": "0xprimary",
            }])
        }

        fn primary_name_execution_manifest_versions() -> Value {
            json!([{
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
            }])
        }

        fn primary_name_execution_request_key(
            namespace: &str,
            address: &str,
            coin_type: &str,
        ) -> String {
            format!("{namespace}:{}:{coin_type}", address.to_ascii_lowercase())
        }

        fn primary_name_verified_success(
            logical_name_id: &str,
            normalized_name: &str,
            canonical_display_name: &str,
            namehash: &str,
            resource_id: Uuid,
        ) -> Value {
            json!({
                "status": "success",
                "name": {
                    "logical_name_id": logical_name_id,
                    "namespace": "ens",
                    "normalized_name": normalized_name,
                    "canonical_display_name": canonical_display_name,
                    "namehash": namehash,
                    "resource_id": resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }
            })
        }

        fn primary_name_verified_mismatch(
            logical_name_id: &str,
            normalized_name: &str,
            canonical_display_name: &str,
            namehash: &str,
            resource_id: Uuid,
            failure_reason: &str,
        ) -> Value {
            let mut payload = primary_name_verified_success(
                logical_name_id,
                normalized_name,
                canonical_display_name,
                namehash,
                resource_id,
            );
            let object = payload
                .as_object_mut()
                .expect("verified primary-name payload must be an object");
            object.insert("status".to_owned(), Value::String("mismatch".to_owned()));
            object.insert(
                "failure_reason".to_owned(),
                Value::String(failure_reason.to_owned()),
            );
            payload
        }

        fn primary_name_execution_trace(
            execution_trace_id: Uuid,
            namespace: &str,
            address: &str,
            coin_type: &str,
            verified_primary_name: Value,
            finished_at: OffsetDateTime,
        ) -> ExecutionTrace {
            let normalized_address = address.to_ascii_lowercase();
            ExecutionTrace {
                execution_trace_id,
                request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                request_key: primary_name_execution_request_key(
                    namespace,
                    &normalized_address,
                    coin_type,
                ),
                namespace: namespace.to_owned(),
                chain_context: json!({
                    "requested_positions": primary_name_execution_requested_chain_positions(),
                }),
                manifest_context: json!({
                    "manifest_versions": primary_name_execution_manifest_versions(),
                }),
                contracts_called: json!([{
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                    "selector": "0x9061b923",
                }]),
                gateway_digests: json!([]),
                final_payload: Some(json!({
                    "verified_primary_name": verified_primary_name.clone(),
                })),
                failure_payload: None,
                request_metadata: json!({
                    "normalized_address": normalized_address,
                    "coin_type": coin_type,
                    "namespace": namespace,
                }),
                finished_at: Some(finished_at),
                steps: vec![ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "call_universal_resolver".to_owned(),
                    input_digest: Some("sha256:primary-input".to_owned()),
                    output_digest: Some("sha256:primary-output".to_owned()),
                    latency_ms: Some(14),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized",
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type,
                    }),
                }],
            }
        }

        fn primary_name_execution_outcome(
            execution_trace_id: Uuid,
            namespace: &str,
            address: &str,
            coin_type: &str,
            verified_primary_name: Value,
            finished_at: OffsetDateTime,
            topology_version_boundary: Value,
            record_version_boundary: Value,
        ) -> ExecutionOutcome {
            let normalized_address = address.to_ascii_lowercase();
            ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key: primary_name_execution_request_key(
                        namespace,
                        &normalized_address,
                        coin_type,
                    ),
                    requested_chain_positions: primary_name_execution_requested_chain_positions(),
                    manifest_versions: primary_name_execution_manifest_versions(),
                    topology_version_boundary,
                    record_version_boundary,
                },
                execution_trace_id,
                request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                namespace: namespace.to_owned(),
                outcome_payload: Some(json!({
                    "verified_primary_name": verified_primary_name,
                })),
                failure_payload: None,
                finished_at,
            }
        }

        fn primary_name_shared_topology_boundary() -> Value {
            json!({
                "logical_name_id": "ens:alice.eth",
                "resource_id": Uuid::from_u128(0x0e7ec7ace0000000000000000000aca1).to_string(),
                "normalized_event_id": 1510,
                "event_kind": "ResolverChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_300_010,
                    "block_hash": "0xabd010",
                    "timestamp": "2024-06-04T00:00:27Z",
                },
            })
        }

        fn primary_name_shared_record_boundary() -> Value {
            json!({
                "logical_name_id": "ens:alice.eth",
                "resource_id": Uuid::from_u128(0x0e7ec7ace0000000000000000000aca2).to_string(),
                "normalized_event_id": 1520,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_300_011,
                    "block_hash": "0xabd011",
                    "timestamp": "2024-06-04T00:00:28Z",
                },
            })
        }

        async fn seed_primary_name_tuple_anchor(
            database: &HarnessDatabase,
            address: &str,
            coin_type: &str,
        ) -> Result<()> {
            database
                .seed_primary_name_reverse_changed(address, coin_type)
                .await?;
            database
                .rebuild_primary_names_current(address, "ens", coin_type)
                .await?;
            Ok(())
        }

        #[derive(Clone, Copy, Debug)]
        enum PersistedResolutionInvalidation {
            Manifest,
            Topology,
            Record,
        }

        impl PersistedResolutionInvalidation {
            fn execution_trace_id(self) -> Uuid {
                match self {
                    Self::Manifest => Uuid::from_u128(0x0e7ec7ace00000000000000000000031),
                    Self::Topology => Uuid::from_u128(0x0e7ec7ace00000000000000000000032),
                    Self::Record => Uuid::from_u128(0x0e7ec7ace00000000000000000000033),
                }
            }

            fn label(self) -> &'static str {
                match self {
                    Self::Manifest => "manifest invalidation",
                    Self::Topology => "topology boundary invalidation",
                    Self::Record => "record boundary invalidation",
                }
            }
        }

        struct PersistedResolutionExecutionFixture {
            logical_name_id: &'static str,
            resource_id: Uuid,
            execution_trace_id: Uuid,
            cache_key: ExecutionCacheKey,
        }

        async fn run_resolution_execution_invalidation_case(
            invalidation: PersistedResolutionInvalidation,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture = seed_persisted_resolution_execution_fixture(
                &database,
                invalidation.execution_trace_id(),
            )
            .await?;

            let mixed_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed before invalidation")?;
            let explain_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed before invalidation")?;

            assert_eq!(mixed_before_response.status(), StatusCode::OK);
            assert_eq!(explain_before_response.status(), StatusCode::OK);

            let mixed_before_payload: ResolutionResponse = read_json(mixed_before_response).await?;
            let explain_before_payload: ResolutionResponse =
                read_json(explain_before_response).await?;
            let expected_declared_state = resolution_supported_declared_state(
                fixture.logical_name_id,
                fixture.resource_id,
                &["text:com.twitter", "addr:60"],
            );
            let expected_verified_queries = resolution_execution_verified_queries(
                fixture.execution_trace_id,
                &["text:com.twitter", "addr:60"],
            );

            assert_eq!(
                mixed_before_payload.declared_state.as_ref(),
                Some(&expected_declared_state)
            );
            assert_eq!(
                mixed_before_payload.provenance.get("execution_trace_id"),
                Some(&Value::String(fixture.execution_trace_id.to_string()))
            );
            assert_eq!(
                mixed_before_payload.verified_state,
                Some(json!({
                    "verified_queries": expected_verified_queries.clone(),
                }))
            );
            assert_eq!(
                explain_before_payload.verified_state,
                Some(json!({
                    "execution": resolution_execution_summary(
                        fixture.execution_trace_id,
                        fixture.resource_id,
                    ),
                    "verified_queries": expected_verified_queries,
                }))
            );

            invalidate_persisted_resolution_execution(&database, &fixture.cache_key, invalidation)
                .await?;

            assert_eq!(
                load_execution_outcome(&database.pool, &fixture.cache_key).await?,
                None
            );
            assert!(
                load_execution_trace(&database.pool, fixture.execution_trace_id)
                    .await?
                    .is_some(),
                "execution traces stay durable after cache invalidation",
            );

            let mixed_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed after invalidation")?;
            let explain_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed after invalidation")?;

            assert_eq!(mixed_after_response.status(), StatusCode::OK);
            assert_eq!(explain_after_response.status(), StatusCode::NOT_FOUND);

            let mixed_after_payload: ResolutionResponse = read_json(mixed_after_response).await?;
            let explain_after_payload: ErrorResponse = read_json(explain_after_response).await?;

            assert_eq!(mixed_after_payload.data, mixed_before_payload.data);
            assert_eq!(mixed_after_payload.coverage, mixed_before_payload.coverage);
            assert_eq!(
                mixed_after_payload.chain_positions,
                mixed_before_payload.chain_positions
            );
            assert_eq!(
                mixed_after_payload.consistency,
                mixed_before_payload.consistency
            );
            assert_eq!(
                mixed_after_payload.last_updated,
                mixed_before_payload.last_updated
            );
            assert_eq!(
                mixed_after_payload.declared_state,
                mixed_before_payload.declared_state
            );
            assert_eq!(
                mixed_after_payload.provenance.get("execution_trace_id"),
                Some(&Value::Null)
            );
            assert_eq!(
                mixed_after_payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ]))
            );
            assert_eq!(explain_after_payload.error.code, "not_found");
            assert_eq!(
                explain_after_payload.error.message,
                "persisted resolution execution explain was not found for name alice.eth in namespace ens"
            );
            assert!(explain_after_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_persisted_resolution_execution_fixture(
            database: &HarnessDatabase,
            execution_trace_id: Uuid,
        ) -> Result<PersistedResolutionExecutionFixture> {
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution invalidation requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let records = parse_resolution_record_keys(
                Some("text:com.twitter,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["addr:60", "text:com.twitter"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["addr:60", "text:com.twitter"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key.clone(),
                    persisted_verified_queries,
                ),
            )
            .await?;

            Ok(PersistedResolutionExecutionFixture {
                logical_name_id,
                resource_id,
                execution_trace_id,
                cache_key,
            })
        }

        async fn invalidate_persisted_resolution_execution(
            database: &HarnessDatabase,
            cache_key: &ExecutionCacheKey,
            invalidation: PersistedResolutionInvalidation,
        ) -> Result<()> {
            let summary = match invalidation {
                PersistedResolutionInvalidation::Manifest => {
                    let manifest_entry = cache_key
                        .manifest_versions
                        .as_array()
                        .and_then(|entries| entries.first())
                        .context(
                            "persisted verified resolution cache key must expose manifest_versions",
                        )?;
                    let manifest_version = manifest_entry
                        .get("manifest_version")
                        .and_then(Value::as_i64)
                        .context(
                            "persisted verified resolution manifest invalidation requires manifest_version",
                        )?;
                    let source_manifest_id = manifest_entry
                        .get("source_manifest_id")
                        .and_then(Value::as_i64);
                    let source_family = manifest_entry
                        .get("source_family")
                        .and_then(Value::as_str)
                        .map(str::to_owned);

                    if source_manifest_id.is_none() && source_family.is_none() {
                        return Err(anyhow::anyhow!(
                            "persisted verified resolution manifest invalidation requires a manifest identity"
                        ));
                    }

                    invalidate_execution_outcomes_for_manifest_version(
                        &database.pool,
                        &ExecutionManifestInvalidation {
                            request_type: "verified_resolution".to_owned(),
                            namespace: "ens".to_owned(),
                            source_manifest_id,
                            source_family,
                            manifest_version,
                        },
                    )
                    .await?
                }
                PersistedResolutionInvalidation::Topology => {
                    invalidate_execution_outcomes_for_topology_boundary(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: "verified_resolution".to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.topology_version_boundary.clone(),
                        },
                    )
                    .await?
                }
                PersistedResolutionInvalidation::Record => {
                    invalidate_execution_outcomes_for_record_boundary(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: "verified_resolution".to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.record_version_boundary.clone(),
                        },
                    )
                    .await?
                }
            };

            assert_eq!(summary.deleted_outcome_count, 1);
            Ok(())
        }

        #[derive(Clone, Copy, Debug)]
        enum PersistedPrimaryNameInvalidation {
            Manifest,
            Topology,
            Record,
        }

        impl PersistedPrimaryNameInvalidation {
            fn execution_trace_id(self) -> Uuid {
                match self {
                    Self::Manifest => Uuid::from_u128(0x0e7ec7ace00000000000000000000051),
                    Self::Topology => Uuid::from_u128(0x0e7ec7ace00000000000000000000052),
                    Self::Record => Uuid::from_u128(0x0e7ec7ace00000000000000000000053),
                }
            }

            fn sibling_execution_trace_id(self) -> Uuid {
                match self {
                    Self::Manifest => Uuid::from_u128(0x0e7ec7ace00000000000000000000061),
                    Self::Topology => Uuid::from_u128(0x0e7ec7ace00000000000000000000062),
                    Self::Record => Uuid::from_u128(0x0e7ec7ace00000000000000000000063),
                }
            }

            fn label(self) -> &'static str {
                match self {
                    Self::Manifest => "manifest invalidation",
                    Self::Topology => "topology boundary invalidation",
                    Self::Record => "record boundary invalidation",
                }
            }
        }

        struct PersistedPrimaryNameExecutionFixture {
            address: &'static str,
            target_execution_trace_id: Uuid,
            target_cache_key: ExecutionCacheKey,
            sibling_cache_key: ExecutionCacheKey,
            target_verified_primary_name: Value,
            sibling_verified_primary_name: Value,
            target_finished_at: OffsetDateTime,
            sibling_finished_at: OffsetDateTime,
        }

        async fn run_primary_name_execution_invalidation_case(
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture =
                seed_persisted_primary_name_execution_fixture(&database, invalidation).await?;
            let expected_data = json!({
                "address": fixture.address,
                "namespace": "ens",
                "coin_type": "60",
            });

            let verified_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "verified primary-name request failed before {}",
                        invalidation.label()
                    )
                })?;
            let both_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=both",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "mixed primary-name request failed before {}",
                        invalidation.label()
                    )
                })?;

            assert_eq!(verified_before_response.status(), StatusCode::OK);
            assert_eq!(both_before_response.status(), StatusCode::OK);

            let verified_before_payload: PrimaryNameResponse =
                read_json(verified_before_response).await?;
            let both_before_payload: PrimaryNameResponse = read_json(both_before_response).await?;

            assert_eq!(verified_before_payload.data, expected_data);
            assert_eq!(both_before_payload.data, expected_data);
            assert_eq!(verified_before_payload.declared_state, None);
            assert_eq!(
                verified_before_payload.verified_state,
                Some(json!({
                    "verified_primary_name": fixture.target_verified_primary_name.clone(),
                }))
            );
            assert_eq!(
                both_before_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(
                both_before_payload.verified_state,
                verified_before_payload.verified_state
            );
            assert_primary_name_persisted_readback_invariants(
                &verified_before_payload,
                fixture.target_execution_trace_id,
                fixture.target_finished_at,
            );
            assert_primary_name_persisted_readback_invariants(
                &both_before_payload,
                fixture.target_execution_trace_id,
                fixture.target_finished_at,
            );

            invalidate_persisted_primary_name_execution(
                &database,
                &fixture.target_cache_key,
                invalidation,
            )
            .await?;

            assert_eq!(
                load_execution_outcome(&database.pool, &fixture.target_cache_key).await?,
                None
            );
            assert!(
                load_execution_trace(&database.pool, fixture.target_execution_trace_id)
                    .await?
                    .is_some(),
                "execution traces stay durable after verified-primary cache invalidation",
            );
            assert!(
                load_execution_outcome(&database.pool, &fixture.sibling_cache_key)
                    .await?
                    .is_some(),
                "exact-tuple invalidation must keep sibling tuple outcomes",
            );

            let verified_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "verified primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;
            let both_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=both",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "mixed primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;
            let sibling_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=61&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "sibling primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;

            assert_eq!(verified_after_response.status(), StatusCode::OK);
            assert_eq!(both_after_response.status(), StatusCode::OK);
            assert_eq!(sibling_response.status(), StatusCode::OK);

            let verified_after_payload: PrimaryNameResponse =
                read_json(verified_after_response).await?;
            let both_after_payload: PrimaryNameResponse = read_json(both_after_response).await?;
            let sibling_payload: PrimaryNameResponse = read_json(sibling_response).await?;

            assert_eq!(verified_after_payload.data, expected_data);
            assert_eq!(both_after_payload.data, expected_data);
            assert_eq!(verified_after_payload.declared_state, None);
            assert_eq!(
                verified_after_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );
            assert_eq!(
                both_after_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(
                both_after_payload.verified_state,
                verified_after_payload.verified_state
            );
            assert_primary_name_bootstrap_invariants(&verified_after_payload);
            assert_primary_name_bootstrap_invariants(&both_after_payload);

            assert_eq!(
                sibling_payload.verified_state,
                Some(json!({
                    "verified_primary_name": fixture.sibling_verified_primary_name.clone(),
                }))
            );
            assert_primary_name_persisted_readback_invariants(
                &sibling_payload,
                invalidation.sibling_execution_trace_id(),
                fixture.sibling_finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_persisted_primary_name_execution_fixture(
            database: &HarnessDatabase,
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<PersistedPrimaryNameExecutionFixture> {
            let address = "0x0000000000000000000000000000000000000abc";
            seed_primary_name_tuple_anchor(database, address, "60").await?;
            seed_primary_name_tuple_anchor(database, address, "61").await?;

            let target_finished_at = timestamp(1_717_172_401);
            let sibling_finished_at = timestamp(1_717_172_499);
            let target_verified_primary_name = primary_name_verified_success(
                "ens:alice.eth",
                "alice.eth",
                "Alice.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000123",
                Uuid::from_u128(0x456),
            );
            let sibling_verified_primary_name = primary_name_verified_mismatch(
                "ens:other.eth",
                "other.eth",
                "other.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000456",
                Uuid::from_u128(0x999),
                "resolved_address_mismatch",
            );

            let target_outcome = primary_name_execution_outcome(
                invalidation.execution_trace_id(),
                "ens",
                address,
                "60",
                target_verified_primary_name.clone(),
                target_finished_at,
                primary_name_shared_topology_boundary(),
                primary_name_shared_record_boundary(),
            );
            let sibling_outcome = primary_name_execution_outcome(
                invalidation.sibling_execution_trace_id(),
                "ens",
                address,
                "61",
                sibling_verified_primary_name.clone(),
                sibling_finished_at,
                primary_name_shared_topology_boundary(),
                primary_name_shared_record_boundary(),
            );

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    invalidation.execution_trace_id(),
                    "ens",
                    address,
                    "60",
                    target_verified_primary_name.clone(),
                    target_finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(&database.pool, &target_outcome).await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    invalidation.sibling_execution_trace_id(),
                    "ens",
                    address,
                    "61",
                    sibling_verified_primary_name.clone(),
                    sibling_finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(&database.pool, &sibling_outcome).await?;

            Ok(PersistedPrimaryNameExecutionFixture {
                address,
                target_execution_trace_id: invalidation.execution_trace_id(),
                target_cache_key: target_outcome.cache_key,
                sibling_cache_key: sibling_outcome.cache_key,
                target_verified_primary_name,
                sibling_verified_primary_name,
                target_finished_at,
                sibling_finished_at,
            })
        }

        async fn invalidate_persisted_primary_name_execution(
            database: &HarnessDatabase,
            cache_key: &ExecutionCacheKey,
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<()> {
            let summary = match invalidation {
                PersistedPrimaryNameInvalidation::Manifest => {
                    let manifest_entry = cache_key
                        .manifest_versions
                        .as_array()
                        .and_then(|entries| entries.first())
                        .context(
                            "persisted verified primary-name cache key must expose manifest_versions",
                        )?;
                    let manifest_version = manifest_entry
                        .get("manifest_version")
                        .and_then(Value::as_i64)
                        .context(
                            "persisted verified primary-name manifest invalidation requires manifest_version",
                        )?;
                    let source_manifest_id = manifest_entry
                        .get("source_manifest_id")
                        .and_then(Value::as_i64);
                    let source_family = manifest_entry
                        .get("source_family")
                        .and_then(Value::as_str)
                        .map(str::to_owned);

                    if source_manifest_id.is_none() && source_family.is_none() {
                        return Err(anyhow::anyhow!(
                            "persisted verified primary-name manifest invalidation requires a manifest identity"
                        ));
                    }

                    invalidate_execution_outcomes_for_manifest_version_and_request_key(
                        &database.pool,
                        &ExecutionManifestInvalidation {
                            request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE
                                .to_owned(),
                            namespace: "ens".to_owned(),
                            source_manifest_id,
                            source_family,
                            manifest_version,
                        },
                        &cache_key.request_key,
                    )
                    .await?
                }
                PersistedPrimaryNameInvalidation::Topology => {
                    invalidate_execution_outcomes_for_topology_boundary_and_request_key(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE
                                .to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.topology_version_boundary.clone(),
                        },
                        &cache_key.request_key,
                    )
                    .await?
                }
                PersistedPrimaryNameInvalidation::Record => {
                    invalidate_execution_outcomes_for_record_boundary_and_request_key(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE
                                .to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.record_version_boundary.clone(),
                        },
                        &cache_key.request_key,
                    )
                    .await?
                }
            };

            assert_eq!(summary.deleted_outcome_count, 1);
            Ok(())
        }

        async fn assert_exact_name_history_summary_matches_history_route(
            database: &HarnessDatabase,
            namespace: &str,
            name: &str,
            history: &Value,
        ) -> Result<()> {
            let history = history
                .as_object()
                .expect("exact-name history summary must be an object");
            let surface_head = history
                .get("surface_head")
                .context("surface_head must be present")?;
            let resource_head = history
                .get("resource_head")
                .context("resource_head must be present")?;

            let surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/{namespace}/{name}?scope=surface"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact-name surface history request failed")?;
            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/{namespace}/{name}?scope=resource"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact-name resource history request failed")?;

            assert_eq!(surface_response.status(), StatusCode::OK);
            assert_eq!(resource_response.status(), StatusCode::OK);

            let surface_payload: HistoryResponse = read_json(surface_response).await?;
            let resource_payload: HistoryResponse = read_json(resource_response).await?;

            assert_eq!(
                surface_head,
                &history_pointer_from_history_row(
                    surface_payload
                        .data
                        .first()
                        .context("surface history route must return a head row")?,
                )?
            );
            assert_eq!(
                resource_head,
                &history_pointer_from_history_row(
                    resource_payload
                        .data
                        .first()
                        .context("resource history route must return a head row")?,
                )?
            );

            Ok(())
        }

        fn history_pointer_from_history_row(row: &Value) -> Result<Value> {
            let normalized_event_id = row
                .get("normalized_event_id")
                .and_then(Value::as_str)
                .context("history row must include normalized_event_id")?
                .parse::<i64>()
                .context("history row normalized_event_id must parse as i64")?;

            Ok(json!({
                "normalized_event_id": normalized_event_id,
                "event_kind": row
                    .get("event_kind")
                    .cloned()
                    .context("history row must include event_kind")?,
                "chain_position": row
                    .get("chain_position")
                    .cloned()
                    .context("history row must include chain_position")?,
            }))
        }
    }
}
