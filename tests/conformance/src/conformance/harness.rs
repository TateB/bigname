        use std::{
            collections::{BTreeMap, BTreeSet},
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
            NameSurface, NormalizedEvent, PermissionScope, PermissionsCurrentRow,
            PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, RawBlock,
            RecordInventoryCurrentRow, ResolverCurrentRow, Resource, SurfaceBinding,
            SurfaceBindingKind, TokenLineage, default_database_url,
            invalidate_execution_outcomes_for_manifest_version,
            invalidate_execution_outcomes_for_manifest_version_and_request_key,
            invalidate_execution_outcomes_for_record_boundary,
            invalidate_execution_outcomes_for_record_boundary_and_request_key,
            invalidate_execution_outcomes_for_topology_boundary,
            invalidate_execution_outcomes_for_topology_boundary_and_request_key,
            load_execution_outcome, load_execution_trace, upsert_execution_outcome,
            upsert_execution_trace, upsert_primary_name_current_rows,
            upsert_primary_name_current_snapshots,
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

        #[derive(Clone, Copy, Debug)]
        struct OpenApiConformanceCoverage {
            path: &'static str,
            scope: OpenApiConformanceScope,
        }

        #[derive(Clone, Copy, Debug)]
        enum OpenApiConformanceScope {
            HarnessOwner(&'static str),
            OutOfScope(&'static str),
        }

        const OPENAPI_CONFORMANCE_COVERAGE: &[OpenApiConformanceCoverage] = &[
            OpenApiConformanceCoverage {
                path: "/healthz",
                scope: OpenApiConformanceScope::OutOfScope(
                    "private operator endpoint; not published in docs/api-v1.openapi.json",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/addresses/{address}/names",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "collections.rs::address_names_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/addresses/{address}/names/count",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::names_collection address count; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/coverage/{namespace}/{name}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "exact_name.rs::coverage_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/explain/names/{namespace}/{name}/authority-control",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "exact_name.rs::authority_control_explain_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/explain/names/{namespace}/{name}/surface-binding",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "exact_name.rs::surface_binding_explain_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/explain/resolutions/{namespace}/{name}/execution",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "resolution_and_permissions.rs::resolution_execution_explain_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/history/addresses/{address}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "history.rs::address_history_contract_* and apps/api tests::history compact view; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/history/names/{namespace}/{name}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "history.rs::name_history_contract_* and apps/api tests::history compact view; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/history/resources/{resource_id}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "history.rs::resource_history_contract_* and apps/api tests::history compact view; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/events",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::events; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/manifests/{namespace}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "collections.rs::namespace_manifests_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/names",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::names_collection; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/names/{namespace}/{name}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "exact_name.rs::exact_name_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/names/{namespace}/{name}/children",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "collections.rs::name_children_contract_* and apps/api tests::collections child compact default; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/names/{namespace}/{name}/records",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::records; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/names/{namespace}/{name}/roles",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::roles name roles; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/namespaces/{namespace}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "collections.rs::smoke_supported_reads_contract_bootstrap",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/primary-names/{address}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "primary_names.rs::primary_names_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resolutions/{namespace}/{name}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "resolution_and_permissions.rs::resolution_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resolve/{name}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "resolution_and_permissions.rs::resolution_inferred_route_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resolve/{name}/records",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::records resolve records; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resolvers/{chain_id}/{resolver_address}",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "resolution_and_permissions.rs::resolver_overview_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resolvers/{chain_id}/{resolver_address}/overview",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::resolvers; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resources/lookup",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::roles resource lookup; full first-party cutover still needs app call-site mapping",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/resources/{resource_id}/permissions",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "resolution_and_permissions.rs::resource_permissions_contract_*",
                ),
            },
            OpenApiConformanceCoverage {
                path: "/v1/roles",
                scope: OpenApiConformanceScope::HarnessOwner(
                    "apps/api tests::roles; full first-party cutover still needs app call-site mapping",
                ),
            },
        ];

        #[test]
        fn openapi_public_paths_have_conformance_coverage_owner() -> Result<()> {
            let document: Value = serde_json::from_str(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../docs/api-v1.openapi.json"
            )))
            .context("checked-in OpenAPI artifact must be valid JSON")?;
            let published_paths = document
                .get("paths")
                .and_then(Value::as_object)
                .context("checked-in OpenAPI artifact must expose paths")?;

            let mut coverage_by_path = BTreeMap::new();
            for coverage in OPENAPI_CONFORMANCE_COVERAGE {
                match coverage.scope {
                    OpenApiConformanceScope::HarnessOwner(owner) => {
                        assert!(
                            !owner.trim().is_empty(),
                            "OpenAPI conformance owner must be explicit for {}",
                            coverage.path
                        );
                    }
                    OpenApiConformanceScope::OutOfScope(reason) => {
                        assert!(
                            !reason.trim().is_empty(),
                            "OpenAPI out-of-scope reason must be explicit for {}",
                            coverage.path
                        );
                    }
                }
                assert!(
                    coverage_by_path
                        .insert(coverage.path, coverage.scope)
                        .is_none(),
                    "duplicate OpenAPI conformance coverage entry for {}",
                    coverage.path
                );
            }

            let missing_coverage = published_paths
                .keys()
                .filter(|path| !coverage_by_path.contains_key(path.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            assert!(
                missing_coverage.is_empty(),
                "OpenAPI paths without conformance ownership or explicit out-of-scope reason: {missing_coverage:#?}"
            );
            assert!(
                !published_paths.contains_key("/healthz"),
                "private /healthz must stay outside docs/api-v1.openapi.json"
            );

            Ok(())
        }

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
                    chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
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

            async fn seed_basenames_exact_name_rebuild_inputs(
                &self,
                logical_name_id: &str,
                resource_id: Uuid,
                token_lineage_id: Uuid,
                surface_binding_id: Uuid,
            ) -> Result<()> {
                bigname_storage::upsert_raw_blocks(
                    &self.pool,
                    &[
                        raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
                        raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
                        raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_171_700),
                        raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
                        raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
                        raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
                    ],
                )
                .await
                .context("failed to upsert raw blocks for basenames exact-name conformance")?;
                bigname_storage::upsert_name_surfaces(
                    &self.pool,
                    &[NameSurface {
                        logical_name_id: logical_name_id.to_owned(),
                        namespace: "basenames".to_owned(),
                        input_name: "alice.base.eth".to_owned(),
                        canonical_display_name: "Alice.base.eth".to_owned(),
                        normalized_name: "alice.base.eth".to_owned(),
                        dns_encoded_name: b"alice.base.eth".to_vec(),
                        namehash: "namehash:alice.base.eth".to_owned(),
                        labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
                        normalizer_version: "ensip15@2026-04-16".to_owned(),
                        normalization_warnings: json!([]),
                        normalization_errors: json!([]),
                        chain_id: "base-mainnet".to_owned(),
                        block_hash: "0xbase-surface".to_owned(),
                        block_number: 98,
                        provenance: json!({"seed": "basenames_exact_name_surface"}),
                        canonicality_state: CanonicalityState::Canonical,
                    }],
                )
                .await
                .context("failed to upsert basenames name surface for conformance")?;
                bigname_storage::upsert_token_lineages(
                    &self.pool,
                    &[bigname_storage::TokenLineage {
                        token_lineage_id,
                        chain_id: "base-mainnet".to_owned(),
                        block_hash: "0xbase-resource".to_owned(),
                        block_number: 99,
                        provenance: json!({"seed": "basenames_exact_name_token_lineage"}),
                        canonicality_state: CanonicalityState::Canonical,
                    }],
                )
                .await
                .context("failed to upsert basenames token lineage for conformance")?;
                bigname_storage::upsert_resources(
                    &self.pool,
                    &[Resource {
                        resource_id,
                        token_lineage_id: Some(token_lineage_id),
                        chain_id: "base-mainnet".to_owned(),
                        block_hash: "0xbase-resource".to_owned(),
                        block_number: 99,
                        provenance: json!({"seed": "basenames_exact_name_resource"}),
                        canonicality_state: CanonicalityState::Canonical,
                    }],
                )
                .await
                .context("failed to upsert basenames resource for conformance")?;
                bigname_storage::upsert_surface_bindings(
                    &self.pool,
                    &[SurfaceBinding {
                        surface_binding_id,
                        logical_name_id: logical_name_id.to_owned(),
                        resource_id,
                        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                        active_from: timestamp(1_717_171_700),
                        active_to: None,
                        chain_id: "base-mainnet".to_owned(),
                        block_hash: "0xbase-binding".to_owned(),
                        block_number: 100,
                        provenance: json!({"seed": "basenames_exact_name_binding"}),
                        canonicality_state: CanonicalityState::Canonical,
                    }],
                )
                .await
                .context("failed to upsert basenames surface binding for conformance")?;
                bigname_storage::upsert_normalized_events(
                    &self.pool,
                    &[
                        NormalizedEvent {
                            event_identity: "conformance:basenames:grant".to_owned(),
                            namespace: "basenames".to_owned(),
                            logical_name_id: Some(logical_name_id.to_owned()),
                            resource_id: Some(resource_id),
                            event_kind: "RegistrationGranted".to_owned(),
                            source_family: "basenames_base_registrar".to_owned(),
                            manifest_version: 3,
                            source_manifest_id: None,
                            chain_id: Some("base-mainnet".to_owned()),
                            block_number: Some(101),
                            block_hash: Some("0xbase-grant".to_owned()),
                            transaction_hash: Some("0xtxbasegrant".to_owned()),
                            log_index: Some(0),
                            raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:grant"}),
                            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                            canonicality_state: CanonicalityState::Canonical,
                            before_state: json!({}),
                            after_state: json!({
                                "authority_kind": "registrar",
                                "authority_key": "registrar:base-mainnet:alice",
                                "registrant": "0x00000000000000000000000000000000000000aa",
                                "expiry": 1_900_000_000_i64,
                            }),
                        },
                        NormalizedEvent {
                            event_identity: "conformance:basenames:authority".to_owned(),
                            namespace: "basenames".to_owned(),
                            logical_name_id: Some(logical_name_id.to_owned()),
                            resource_id: Some(resource_id),
                            event_kind: "AuthorityTransferred".to_owned(),
                            source_family: "basenames_base_registry".to_owned(),
                            manifest_version: 3,
                            source_manifest_id: None,
                            chain_id: Some("base-mainnet".to_owned()),
                            block_number: Some(102),
                            block_hash: Some("0xbase-authority".to_owned()),
                            transaction_hash: Some("0xtxbaseauthority".to_owned()),
                            log_index: Some(0),
                            raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:authority"}),
                            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                            canonicality_state: CanonicalityState::Canonical,
                            before_state: json!({}),
                            after_state: json!({
                                "owner": "0x00000000000000000000000000000000000000bb",
                            }),
                        },
                        NormalizedEvent {
                            event_identity: "conformance:basenames:resolver".to_owned(),
                            namespace: "basenames".to_owned(),
                            logical_name_id: Some(logical_name_id.to_owned()),
                            resource_id: Some(resource_id),
                            event_kind: "ResolverChanged".to_owned(),
                            source_family: "basenames_base_resolver".to_owned(),
                            manifest_version: 4,
                            source_manifest_id: None,
                            chain_id: Some("base-mainnet".to_owned()),
                            block_number: Some(103),
                            block_hash: Some("0xbase-resolver".to_owned()),
                            transaction_hash: Some("0xtxbaseresolver".to_owned()),
                            log_index: Some(0),
                            raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:resolver"}),
                            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                            canonicality_state: CanonicalityState::Canonical,
                            before_state: json!({}),
                            after_state: json!({
                                "resolver": "0x0000000000000000000000000000000000000abc",
                                "namehash": "namehash:alice.base.eth",
                            }),
                        },
                    ],
                )
                .await
                .context("failed to upsert basenames normalized events for conformance")?;

                Ok(())
            }

            async fn rebuild_name_current(&self, logical_name_id: &str) -> Result<()> {
                let database_url = self.database_url.clone();
                let logical_name_id_for_worker = logical_name_id.to_owned();
                let logical_name_id_for_seed = logical_name_id.to_owned();
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
                        .arg(&logical_name_id_for_worker)
                        .output()
                        .with_context(|| {
                            format!(
                                "failed to invoke worker name_current rebuild for {logical_name_id_for_worker}"
                            )
                        })?;

                    if !output.status.success() {
                        return Err(anyhow::anyhow!(
                            "worker name_current rebuild failed for {logical_name_id_for_worker}\nstdout:\n{}\nstderr:\n{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr),
                        ));
                    }

                    Ok(())
                })
                .await
                .context("worker name_current rebuild task panicked")??;

                if let Some(row) =
                    bigname_storage::load_name_current(&self.pool, &logical_name_id_for_seed)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to load rebuilt name_current row {logical_name_id_for_seed} for selector seed"
                            )
                        })?
                {
                    self.seed_snapshot_selector_chain_positions(&row.chain_positions)
                        .await?;
                }

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

            async fn seed_basenames_primary_name_claim_observation(
                &self,
                address: &str,
                coin_type: &str,
                raw_name: &str,
            ) -> Result<()> {
                let normalized_address = address.to_ascii_lowercase();
                let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

                bigname_storage::upsert_normalized_events(
                    &self.pool,
                    &[
                        NormalizedEvent {
                            event_identity: format!(
                                "conformance:Basenames:ReverseChanged:{normalized_address}:{coin_type}"
                            ),
                            namespace: "basenames".to_owned(),
                            logical_name_id: None,
                            resource_id: None,
                            event_kind: "ReverseChanged".to_owned(),
                            source_family: "basenames_base_primary".to_owned(),
                            manifest_version: 1,
                            source_manifest_id: None,
                            chain_id: Some("base-mainnet".to_owned()),
                            block_number: Some(260),
                            block_hash: Some("0xbaseprimaryname".to_owned()),
                            transaction_hash: Some("0xtxbaseprimaryname".to_owned()),
                            log_index: Some(0),
                            raw_fact_ref: json!({
                                "kind": "raw_log",
                                "chain_id": "base-mainnet",
                                "block_hash": "0xbaseprimaryname",
                                "block_number": 260,
                                "transaction_hash": "0xtxbaseprimaryname",
                                "log_index": 0,
                            }),
                            derivation_kind: "ens_v1_reverse_claim".to_owned(),
                            canonicality_state: CanonicalityState::Canonical,
                            before_state: json!({}),
                            after_state: json!({
                                "source_event": "ReverseClaimed",
                                "address": normalized_address,
                                "coin_type": coin_type,
                                "namespace": "basenames",
                                "reverse_namespace": "basenames",
                                "reverse_label": reverse_label,
                                "reverse_name": format!("{reverse_label}.addr.reverse"),
                                "reverse_node": "0x0000000000000000000000000000000000000000000000000000000000000104",
                                "claim_provenance": {
                                    "source_family": "basenames_base_primary",
                                    "contract_role": "reverse_registrar",
                                    "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                                },
                            }),
                        },
                        NormalizedEvent {
                            event_identity: format!(
                                "conformance:Basenames:RecordChanged:{normalized_address}:{coin_type}"
                            ),
                            namespace: "basenames".to_owned(),
                            logical_name_id: None,
                            resource_id: None,
                            event_kind: "RecordChanged".to_owned(),
                            source_family: "basenames_base_resolver".to_owned(),
                            manifest_version: 1,
                            source_manifest_id: None,
                            chain_id: Some("base-mainnet".to_owned()),
                            block_number: Some(261),
                            block_hash: Some("0xbaseclaim".to_owned()),
                            transaction_hash: Some("0xtxbaseclaim".to_owned()),
                            log_index: Some(0),
                            raw_fact_ref: json!({
                                "kind": "raw_log",
                                "chain_id": "base-mainnet",
                                "block_hash": "0xbaseclaim",
                                "block_number": 261,
                                "transaction_hash": "0xtxbaseclaim",
                                "log_index": 0,
                            }),
                            derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                            canonicality_state: CanonicalityState::Canonical,
                            before_state: json!({}),
                            after_state: json!({
                                "record_key": "name",
                                "record_family": "name",
                                "selector_key": Value::Null,
                                "raw_name": raw_name,
                                "primary_claim_source": {
                                    "address": normalized_address,
                                    "namespace": "basenames",
                                    "coin_type": coin_type,
                                    "reverse_name": format!("{reverse_label}.addr.reverse"),
                                    "reverse_node": "0x0000000000000000000000000000000000000000000000000000000000000105",
                                    "claim_provenance": {
                                        "source_family": "basenames_base_primary",
                                        "contract_role": "reverse_registrar",
                                        "contract_instance_id": "00000000-0000-0000-0000-000000000105",
                                        "emitting_address": "0x00000000000000000000000000000000000000ad",
                                    },
                                },
                            }),
                        },
                    ],
                )
                .await
                .context("failed to seed Basenames primary-name claim observation for conformance")?;

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

            async fn insert_primary_name_current_row(
                &self,
                row: PrimaryNameCurrentRow,
            ) -> Result<()> {
                upsert_primary_name_current_rows(&self.pool, &[row])
                    .await
                    .context(
                        "failed to upsert primary_names_current row for conformance harness",
                    )?;
                Ok(())
            }

            async fn insert_name_current_row(
                &self,
                row: bigname_storage::NameCurrentRow,
            ) -> Result<()> {
                self.seed_snapshot_selector_chain_positions(&row.chain_positions)
                    .await?;
                bigname_storage::upsert_name_current_rows(&self.pool, &[row])
                    .await
                    .context("failed to upsert name_current row for conformance harness")?;
                Ok(())
            }

            async fn insert_record_inventory_current_row(
                &self,
                row: RecordInventoryCurrentRow,
            ) -> Result<()> {
                let chain_positions = row.chain_positions.clone();
                bigname_storage::upsert_record_inventory_current_rows(&self.pool, &[row])
                    .await
                    .context(
                        "failed to upsert record_inventory_current row for conformance harness",
                    )?;
                self.seed_snapshot_selector_chain_positions(&chain_positions)
                    .await?;
                Ok(())
            }

            async fn seed_snapshot_selector_chain_positions(
                &self,
                chain_positions: &Value,
            ) -> Result<()> {
                let Some(positions) = chain_positions.as_object() else {
                    return Ok(());
                };

                for position in positions.values() {
                    let chain_id = position
                        .get("chain_id")
                        .and_then(Value::as_str)
                        .context("chain_position.chain_id must be present for selector seed")?;
                    let block_hash = position
                        .get("block_hash")
                        .and_then(Value::as_str)
                        .context("chain_position.block_hash must be present for selector seed")?;
                    let block_number = position
                        .get("block_number")
                        .and_then(Value::as_i64)
                        .context("chain_position.block_number must be present for selector seed")?;
                    let timestamp_value = position
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .context("chain_position.timestamp must be present for selector seed")?;
                    let timestamp =
                        bigname_storage::parse_rfc3339_utc_timestamp(timestamp_value)
                            .map_err(|error| anyhow::anyhow!("{error}"))?;

                    sqlx::query(
                        r#"
                        INSERT INTO chain_lineage (
                            chain_id,
                            block_hash,
                            block_number,
                            block_timestamp,
                            canonicality_state
                        )
                        VALUES ($1, $2, $3, $4, 'finalized'::canonicality_state)
                        ON CONFLICT (chain_id, block_hash) DO UPDATE SET
                            block_number = EXCLUDED.block_number,
                            block_timestamp = EXCLUDED.block_timestamp,
                            canonicality_state = EXCLUDED.canonicality_state
                        "#,
                    )
                    .bind(chain_id)
                    .bind(block_hash)
                    .bind(block_number)
                    .bind(timestamp)
                    .execute(&self.pool)
                    .await
                    .with_context(|| {
                        format!("failed to seed chain_lineage for {chain_id} block {block_hash}")
                    })?;

                    sqlx::query(
                        r#"
                        INSERT INTO chain_checkpoints (
                            chain_id,
                            canonical_block_hash,
                            canonical_block_number,
                            safe_block_hash,
                            safe_block_number,
                            finalized_block_hash,
                            finalized_block_number
                        )
                        VALUES ($1, $2, $3, $2, $3, $2, $3)
                        ON CONFLICT (chain_id) DO UPDATE SET
                            canonical_block_hash = EXCLUDED.canonical_block_hash,
                            canonical_block_number = EXCLUDED.canonical_block_number,
                            safe_block_hash = EXCLUDED.safe_block_hash,
                            safe_block_number = EXCLUDED.safe_block_number,
                            finalized_block_hash = EXCLUDED.finalized_block_hash,
                            finalized_block_number = EXCLUDED.finalized_block_number,
                            updated_at = now()
                        "#,
                    )
                    .bind(chain_id)
                    .bind(block_hash)
                    .bind(block_number)
                    .execute(&self.pool)
                    .await
                    .with_context(|| {
                        format!("failed to seed chain checkpoint for {chain_id}")
                    })?;
                }

                Ok(())
            }

            async fn seed_snapshot_selector_for_route(&self, uri: &str) -> Result<()> {
                let Some((namespace, name)) = exact_name_route_target(uri) else {
                    return Ok(());
                };
                let logical_name_id = format!("{namespace}:{name}");
                let Some(row) = bigname_storage::load_name_current(&self.pool, &logical_name_id)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to load name_current row {logical_name_id} for route selector seed"
                        )
                    })?
                else {
                    return Ok(());
                };

                self.seed_snapshot_selector_chain_positions(&row.chain_positions)
                    .await
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

        fn exact_name_route_target(uri: &str) -> Option<(&str, &str)> {
            let path = uri.split('?').next().unwrap_or(uri);
            let parts = path
                .trim_start_matches('/')
                .split('/')
                .collect::<Vec<_>>();
            match parts.as_slice() {
                ["v1", "names", namespace, name] => Some((namespace, name)),
                ["v1", "coverage", namespace, name] => Some((namespace, name)),
                ["v1", "resolutions", namespace, name] => Some((namespace, name)),
                ["v1", "explain", "names", namespace, name, "surface-binding"]
                | ["v1", "explain", "names", namespace, name, "authority-control"]
                | ["v1", "explain", "resolutions", namespace, name, "execution"] => {
                    Some((namespace, name))
                }
                ["v1", "resolve", name] => {
                    Some((infer_conformance_resolution_namespace(name), name))
                }
                _ => None,
            }
        }

        fn infer_conformance_resolution_namespace(name: &str) -> &'static str {
            if name == "base.eth" {
                return "ens";
            }

            if name
                .strip_suffix(".base.eth")
                .is_some_and(|prefix| !prefix.is_empty())
            {
                "basenames"
            } else {
                "ens"
            }
        }
