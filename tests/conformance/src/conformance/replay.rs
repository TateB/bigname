        struct ReplayCorpus {
            logical_name_id: &'static str,
            route_name: &'static str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
            winning_address_names_address: &'static str,
            losing_address_names_address: &'static str,
            winning_control_address: &'static str,
            losing_control_address: &'static str,
            resolver_chain_id: &'static str,
            winning_resolver_address: &'static str,
            losing_resolver_address: &'static str,
            winning_permission_subject: &'static str,
            losing_permission_subject: &'static str,
            primary_name_address: &'static str,
            winning_primary_name: &'static str,
            losing_primary_name: &'static str,
        }

        struct ReplayRoute {
            label: &'static str,
            uri: String,
        }

        const REPLAY_PROFILE_SEED_RESOLVER_ADDRESS: &str =
            "0x0000000000000000000000000000000000000f00";
        const REPLAY_PROFILE_PENDING_ROUTE_NAME: &str = "pending-profile.base.eth";
        const REPLAY_PROFILE_PENDING_LOGICAL_NAME_ID: &str =
            "basenames:pending-profile.base.eth";
        const REPLAY_PROFILE_PENDING_RESOLVER_ADDRESS: &str =
            "0x0000000000000000000000000000000000000f01";
        const REPLAY_PROFILE_UNSUPPORTED_ROUTE_NAME: &str = "unsupported-profile.base.eth";
        const REPLAY_PROFILE_UNSUPPORTED_LOGICAL_NAME_ID: &str =
            "basenames:unsupported-profile.base.eth";
        const REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS: &str =
            "0x0000000000000000000000000000000000000f02";
        const RAW_RETENTION_REPLAY_BLOCK_HASH: &str =
            "0xc0ffee0000000000000000000000000000000000000000000000000000000303";
        const RAW_RETENTION_REPLAY_WATCHED_ADDRESS: &str =
            "0x0000000000000000000000000000000000000c0a";
        const RAW_RETENTION_FULL_BLOCK_PAYLOAD_KIND: &str = "full_block";
        const RAW_RETENTION_BLOCK_RECEIPTS_PAYLOAD_KIND: &str = "block_receipts";
        const RAW_RETENTION_PAYLOAD_CONTENT_TYPE: &str = "application/json";
        const RAW_RETENTION_PAYLOAD_CONTENT_ENCODING: &str = "identity";

        struct RawRetentionProbe {
            chain_id: &'static str,
            block_hash: &'static str,
            block_number: i64,
            watched_address: &'static str,
        }

        pub(crate) async fn run_replay_capability_conformance() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_replay_supported_read_corpus(&database).await?;

            let before_replay = snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
            assert_stale_current_answers_exist(&before_replay, &corpus);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "winning-address-names-before-replay",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.winning_address_names_address
                    ),
                },
            )
            .await?;
            assert_replay_route_status(
                &database,
                ReplayRoute {
                    label: "winning-resolver-before-replay",
                    uri: format!(
                        "/v1/resolvers/{}/{}/overview?meta=full",
                        corpus.resolver_chain_id, corpus.winning_resolver_address
                    ),
                },
                StatusCode::NOT_FOUND,
            )
            .await?;

            let raw_retention_probe =
                seed_cache_first_raw_retention_replay_probe(&database).await?;
            let after_raw_retention_seed =
                snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
            assert_eq!(
                after_raw_retention_seed, before_replay,
                "selected raw facts plus payload-cache metadata must not change shipped public route or consumer-replacement behavior before replay"
            );
            let manifest_policy_before_raw_retention =
                snapshot_manifest_policy_surface(&database).await?;
            replay_raw_fact_normalized_events_for_blocks(
                &database,
                "mainnet",
                raw_retention_probe.chain_id,
                &[raw_retention_probe.block_hash],
            )
            .await?;
            assert_cache_first_raw_retention_replay_probe(&database, &raw_retention_probe).await?;
            let after_raw_retention_replay =
                snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
            assert_eq!(
                after_raw_retention_replay, before_replay,
                "provider-independent raw-fact replay over selected durable facts must not change shipped public route or consumer-replacement behavior"
            );
            assert_eq!(
                snapshot_manifest_policy_surface(&database).await?,
                manifest_policy_before_raw_retention,
                "raw-retention replay must not change manifest rollout or capability policy state"
            );

            replay_all_current_projections(&database).await?;
            let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
            assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "losing-address-names-after-replay",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.losing_address_names_address
                    ),
                },
            )
            .await?;
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "losing-address-history-after-replay",
                    uri: format!(
                        "/v1/history/addresses/{}?namespace=basenames&relation=registrant",
                        corpus.losing_address_names_address
                    ),
                },
            )
            .await?;
            assert_replay_route_status(
                &database,
                ReplayRoute {
                    label: "losing-resolver-after-replay",
                    uri: format!(
                        "/v1/resolvers/{}/{}/overview?meta=full",
                        corpus.resolver_chain_id, corpus.losing_resolver_address
                    ),
                },
                StatusCode::NOT_FOUND,
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn ensv2_sepolia_dev_exact_name_replay() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_ensv2_sepolia_dev_exact_name_replay_corpus(&database).await?;

            let stale_name = request_replay_route(
                &database,
                &ReplayRoute {
                    label: "ensv2-exact-name-before-replay",
                    uri: format!("/v1/names/ens/{}", corpus.route_name),
                },
            )
            .await?;
            assert_eq!(
                stale_name["coverage"]["unsupported_reason"],
                json!("ensv2_exact_name_profile_shadow")
            );
            assert_json_contains(
                &stale_name,
                corpus.stale_registrant,
                "stale current row should be visible before replay",
            );

            replay_all_current_projections(&database).await?;

            let name_payload = request_replay_route(
                &database,
                &ReplayRoute {
                    label: "ensv2-exact-name-after-replay",
                    uri: format!("/v1/names/ens/{}", corpus.route_name),
                },
            )
            .await?;
            let coverage_payload = request_replay_route(
                &database,
                &ReplayRoute {
                    label: "ensv2-coverage-after-replay",
                    uri: format!("/v1/coverage/ens/{}", corpus.route_name),
                },
            )
            .await?;

            assert_ensv2_sepolia_dev_exact_name_replay_payloads(
                &name_payload,
                &coverage_payload,
                &corpus,
            );

            database.cleanup().await?;
            Ok(())
        }

        struct EnsV2ExactNameReplayCorpus {
            logical_name_id: &'static str,
            route_name: &'static str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
            registry_manifest_id: i64,
            registrar_manifest_id: i64,
            registrant: &'static str,
            controller: &'static str,
            stale_registrant: &'static str,
            stale_controller: &'static str,
            stale_resolver: &'static str,
        }

        async fn seed_replay_supported_read_corpus(database: &HarnessDatabase) -> Result<ReplayCorpus> {
            let corpus = ReplayCorpus {
                logical_name_id: "basenames:alice.base.eth",
                route_name: "alice.base.eth",
                resource_id: Uuid::from_u128(0xc910),
                token_lineage_id: Uuid::from_u128(0xc911),
                surface_binding_id: Uuid::from_u128(0xc912),
                winning_address_names_address: "0x00000000000000000000000000000000000000cc",
                losing_address_names_address: "0x00000000000000000000000000000000000000aa",
                winning_control_address: "0x00000000000000000000000000000000000000dd",
                losing_control_address: "0x00000000000000000000000000000000000000bb",
                resolver_chain_id: "base-mainnet",
                winning_resolver_address: "0x0000000000000000000000000000000000000def",
                losing_resolver_address: "0x0000000000000000000000000000000000000abc",
                winning_permission_subject: "0x00000000000000000000000000000000000000ee",
                losing_permission_subject: "0x00000000000000000000000000000000000000bb",
                primary_name_address: "0x0000000000000000000000000000000000000bcd",
                winning_primary_name: "alice.base.eth",
                losing_primary_name: "mallory.base.eth",
            };

            seed_basenames_resolution_rebuild_inputs(
                database,
                corpus.logical_name_id,
                corpus.resource_id,
                corpus.token_lineage_id,
                corpus.surface_binding_id,
            )
            .await?;
            seed_replay_permissions(database, &corpus).await?;

            let child_fixture = EnsV2DeclaredChildFixture::new(
                "ens:parent.eth",
                "ens:alice.parent.eth",
                Uuid::from_u128(0xc920),
                Uuid::from_u128(0xc921),
                90,
            );
            child_fixture.seed(database).await?;

            seed_replay_primary_name_claim_observation(
                database,
                &corpus,
                "losing",
                corpus.losing_primary_name,
                "0xreplay-losing-primary-reverse",
                "0xreplay-losing-primary-claim",
                CanonicalityState::Canonical,
            )
            .await?;

            database.rebuild_name_current(corpus.logical_name_id).await?;
            rebuild_children_current(database, None).await?;
            rebuild_record_inventory_current(database, corpus.resource_id).await?;
            rebuild_permissions_current(database, None).await?;
            rebuild_resolver_current(database, None, None).await?;
            rebuild_address_names_current(database, None).await?;
            database
                .rebuild_primary_names_current(
                    corpus.primary_name_address,
                    "basenames",
                    BASENAMES_PRIMARY_COIN_TYPE,
                )
                .await?;
            seed_replay_primary_name_execution(database, &corpus).await?;
            mark_replay_losing_branch_source_rows_orphaned(database).await?;
            seed_replay_winning_branch_source_rows(database, &corpus).await?;

            Ok(corpus)
        }

        async fn seed_cache_first_raw_retention_replay_probe(
            database: &HarnessDatabase,
        ) -> Result<RawRetentionProbe> {
            let probe = RawRetentionProbe {
                chain_id: "ethereum-mainnet",
                block_hash: RAW_RETENTION_REPLAY_BLOCK_HASH,
                block_number: 303,
                watched_address: RAW_RETENTION_REPLAY_WATCHED_ADDRESS,
            };
            seed_raw_fact_replay_probe(
                database,
                probe.chain_id,
                probe.block_hash,
                probe.watched_address,
            )
            .await?;
            seed_raw_retention_cache_metadata(database, &probe, "replay-capability").await?;

            Ok(probe)
        }

        async fn seed_raw_retention_cache_metadata(
            database: &HarnessDatabase,
            probe: &RawRetentionProbe,
            fixture: &str,
        ) -> Result<()> {
            bigname_storage::upsert_raw_payload_cache_metadata(
                &database.pool,
                &[
                    bigname_storage::RawPayloadCacheMetadataUpsert {
                        chain_id: probe.chain_id.to_owned(),
                        block_hash: probe.block_hash.to_owned(),
                        payload_kind: RAW_RETENTION_FULL_BLOCK_PAYLOAD_KIND.to_owned(),
                        digest_algorithm: None,
                        retained_digest: None,
                        block_number: Some(probe.block_number),
                        payload_size_bytes: 2_048,
                        content_type: Some(RAW_RETENTION_PAYLOAD_CONTENT_TYPE.to_owned()),
                        content_encoding: Some(
                            RAW_RETENTION_PAYLOAD_CONTENT_ENCODING.to_owned(),
                        ),
                        cache_metadata: json!({
                            "fixture": fixture,
                            "source": "json-rpc",
                            "method": "eth_getBlockByHash",
                            "fetch_mode": "block_hash",
                            "retention": "metadata_only",
                            "selected_for_replay": false,
                        }),
                        canonicality_state: CanonicalityState::Canonical,
                    },
                    bigname_storage::RawPayloadCacheMetadataUpsert {
                        chain_id: probe.chain_id.to_owned(),
                        block_hash: probe.block_hash.to_owned(),
                        payload_kind: RAW_RETENTION_BLOCK_RECEIPTS_PAYLOAD_KIND.to_owned(),
                        digest_algorithm: Some("keccak256".to_owned()),
                        retained_digest: Some(
                            "0xfeedface00000000000000000000000000000000000000000000000000000303"
                                .to_owned(),
                        ),
                        block_number: Some(probe.block_number),
                        payload_size_bytes: 4_096,
                        content_type: Some(RAW_RETENTION_PAYLOAD_CONTENT_TYPE.to_owned()),
                        content_encoding: Some(
                            RAW_RETENTION_PAYLOAD_CONTENT_ENCODING.to_owned(),
                        ),
                        cache_metadata: json!({
                            "fixture": fixture,
                            "source": "json-rpc",
                            "method": "eth_getBlockReceipts",
                            "fetch_mode": "block_hash",
                            "retention": "digest_metadata_only",
                            "selected_for_replay": false,
                        }),
                        canonicality_state: CanonicalityState::Canonical,
                    },
                ],
            )
            .await
            .context("failed to seed raw-retention payload-cache metadata")?;

            Ok(())
        }

        async fn assert_cache_first_raw_retention_replay_probe(
            database: &HarnessDatabase,
            probe: &RawRetentionProbe,
        ) -> Result<()> {
            let raw_log_count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM raw_logs
                WHERE chain_id = $1
                  AND block_hash = $2
                  AND block_number = $3
                  AND emitting_address = $4
                  AND canonicality_state = 'canonical'::canonicality_state
                "#,
            )
            .bind(probe.chain_id)
            .bind(probe.block_hash)
            .bind(probe.block_number)
            .bind(probe.watched_address)
            .fetch_one(&database.pool)
            .await
            .context("failed to count raw-retention selected replay logs")?;
            assert_eq!(
                raw_log_count, 1,
                "cache-first replay probe must retain the selected durable raw log"
            );

            let raw_transaction_count = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM raw_transactions WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(probe.chain_id)
            .bind(probe.block_hash)
            .fetch_one(&database.pool)
            .await
            .context("failed to count raw_transactions for raw-retention probe")?;
            assert_eq!(
                raw_transaction_count, 0,
                "raw_transactions must not be required when selected replay facts are already durable"
            );
            let raw_receipt_count = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM raw_receipts WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(probe.chain_id)
            .bind(probe.block_hash)
            .fetch_one(&database.pool)
            .await
            .context("failed to count raw_receipts for raw-retention probe")?;
            assert_eq!(
                raw_receipt_count, 0,
                "raw_receipts must not be required when selected replay facts are already durable"
            );

            let metadata = bigname_storage::list_raw_payload_cache_metadata_by_block_hash(
                &database.pool,
                probe.chain_id,
                probe.block_hash,
            )
            .await
            .context("failed to list raw-retention payload-cache metadata")?;
            assert_eq!(
                metadata.len(),
                2,
                "unselected full block-scoped payloads should be retained only as compact metadata"
            );
            assert!(
                metadata.iter().any(|entry| {
                    entry.payload_kind == RAW_RETENTION_FULL_BLOCK_PAYLOAD_KIND
                        && entry.retained_digest.is_none()
                        && entry.cache_metadata["retention"] == json!("metadata_only")
                }),
                "full block payload bytes must not be required for selected raw-log replay"
            );
            assert!(
                metadata.iter().any(|entry| {
                    entry.payload_kind == RAW_RETENTION_BLOCK_RECEIPTS_PAYLOAD_KIND
                        && entry.digest_algorithm.as_deref() == Some("keccak256")
                        && entry.retained_digest.is_some()
                        && entry.cache_metadata["retention"] == json!("digest_metadata_only")
                }),
                "unselected receipt payloads should remain digest/cache metadata, not durable replay facts"
            );

            let normalized_event_count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM normalized_events
                WHERE chain_id = $1
                  AND block_hash = $2
                  AND source_family = $3
                  AND event_kind = 'ReverseChanged'
                  AND canonicality_state = 'canonical'::canonicality_state
                  AND source_manifest_id IS NOT NULL
                  AND raw_fact_ref->>'block_hash' = $2
                  AND after_state->>'address' = $4
                "#,
            )
            .bind(probe.chain_id)
            .bind(probe.block_hash)
            .bind(RAW_REPLAY_PROBE_SOURCE_FAMILY)
            .bind(RAW_REPLAY_PROBE_CLAIMED_ADDRESS)
            .fetch_one(&database.pool)
            .await
            .context("failed to count raw-retention replay normalized events")?;
            assert_eq!(
                normalized_event_count, 1,
                "raw-fact replay must be provider-independent once the selected durable raw log is retained"
            );

            Ok(())
        }

        async fn snapshot_manifest_policy_surface(
            database: &HarnessDatabase,
        ) -> Result<Vec<(String, String, String, String, String, Value)>> {
            sqlx::query_as::<_, (String, String, String, String, String, Value)>(
                r#"
                SELECT
                    mv.namespace,
                    mv.source_family,
                    mv.chain,
                    mv.deployment_epoch,
                    mv.rollout_status::TEXT AS rollout_status,
                    COALESCE(
                        JSONB_AGG(
                            JSONB_BUILD_OBJECT(
                                'capability_name', flags.capability_name,
                                'status', flags.status::TEXT,
                                'notes', flags.notes
                            )
                            ORDER BY flags.capability_name
                        ) FILTER (WHERE flags.capability_name IS NOT NULL),
                        '[]'::JSONB
                    ) AS capability_flags
                FROM manifest_versions AS mv
                LEFT JOIN manifest_capability_flags AS flags
                  ON flags.manifest_id = mv.manifest_id
                GROUP BY
                    mv.manifest_id,
                    mv.namespace,
                    mv.source_family,
                    mv.chain,
                    mv.deployment_epoch,
                    mv.rollout_status
                ORDER BY
                    mv.namespace,
                    mv.source_family,
                    mv.chain,
                    mv.deployment_epoch,
                    mv.manifest_id
                "#,
            )
            .fetch_all(&database.pool)
            .await
            .context("failed to snapshot manifest rollout and capability policy surface")
        }

        async fn seed_ensv2_sepolia_dev_exact_name_replay_corpus(
            database: &HarnessDatabase,
        ) -> Result<EnsV2ExactNameReplayCorpus> {
            let registry_manifest_id = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l1",
                    "ethereum-sepolia",
                    "ens_v2_sepolia_dev",
                    11,
                    "active",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;
            let registrar_manifest_id = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registrar_l1",
                    "ethereum-sepolia",
                    "ens_v2_sepolia_dev",
                    11,
                    "active",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;
            database
                .insert_capability_flag(
                    registrar_manifest_id,
                    "exact_name_profile",
                    "supported",
                    None,
                )
                .await?;

            let corpus = EnsV2ExactNameReplayCorpus {
                logical_name_id: "ens:sepolia-dev-replay.eth",
                route_name: "sepolia-dev-replay.eth",
                resource_id: Uuid::from_u128(0xc9a0),
                token_lineage_id: Uuid::from_u128(0xc9a1),
                surface_binding_id: Uuid::from_u128(0xc9a2),
                registry_manifest_id,
                registrar_manifest_id,
                registrant: "0x0000000000000000000000000000000000000b0b",
                controller: "0x0000000000000000000000000000000000000c0c",
                stale_registrant: "0x0000000000000000000000000000000000000bad",
                stale_controller: "0x0000000000000000000000000000000000000dad",
                stale_resolver: "0x0000000000000000000000000000000000000fed",
            };

            seed_ens_v2_address_name_rebuild_inputs(
                database,
                corpus.logical_name_id,
                corpus.resource_id,
                corpus.token_lineage_id,
                corpus.surface_binding_id,
                corpus.registrant,
                corpus.controller,
            )
            .await?;
            seed_ensv2_sepolia_dev_exact_name_registrar_truth(database, &corpus).await?;
            assign_ensv2_sepolia_dev_exact_name_manifest_links(database, &corpus).await?;
            database
                .insert_name_current_row(stale_ensv2_sepolia_dev_exact_name_row(&corpus))
                .await?;

            Ok(corpus)
        }

        async fn seed_ensv2_sepolia_dev_exact_name_registrar_truth(
            database: &HarnessDatabase,
            corpus: &EnsV2ExactNameReplayCorpus,
        ) -> Result<()> {
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[raw_block(
                    "ethereum-sepolia",
                    "0xensv2-replay-renew",
                    Some("0xensv2-regen"),
                    207,
                    1_717_182_207,
                )],
            )
            .await
            .context("failed to upsert ENSv2 replay registrar raw block")?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[NormalizedEvent {
                    event_identity: format!(
                        "conformance:{}:ensv2-replay-registrar-renew",
                        corpus.logical_name_id
                    ),
                    namespace: "ens".to_owned(),
                    logical_name_id: Some(corpus.logical_name_id.to_owned()),
                    resource_id: Some(corpus.resource_id),
                    event_kind: "RegistrationRenewed".to_owned(),
                    source_family: "ens_v2_registrar_l1".to_owned(),
                    manifest_version: 11,
                    source_manifest_id: Some(corpus.registrar_manifest_id),
                    chain_id: Some("ethereum-sepolia".to_owned()),
                    block_number: Some(207),
                    block_hash: Some("0xensv2-replay-renew".to_owned()),
                    transaction_hash: Some(format!(
                        "0xtx:{}:ensv2-replay-registrar-renew",
                        corpus.logical_name_id
                    )),
                    log_index: Some(0),
                    raw_fact_ref: json!({
                        "kind": "raw_log",
                        "event_identity": format!(
                            "conformance:{}:ensv2-replay-registrar-renew",
                            corpus.logical_name_id
                        ),
                    }),
                    derivation_kind: "ens_v2_registrar".to_owned(),
                    canonicality_state: CanonicalityState::Finalized,
                    before_state: json!({}),
                    after_state: json!({
                        "duration": 31_536_000_i64,
                        "expiry": 1_931_536_000_i64,
                    }),
                }],
            )
            .await
            .context("failed to upsert ENSv2 replay registrar normalized event")?;

            Ok(())
        }

        async fn assign_ensv2_sepolia_dev_exact_name_manifest_links(
            database: &HarnessDatabase,
            corpus: &EnsV2ExactNameReplayCorpus,
        ) -> Result<()> {
            sqlx::query(
                r#"
                UPDATE normalized_events
                SET source_manifest_id = CASE source_family
                    WHEN 'ens_v2_registry_l1' THEN $2
                    WHEN 'ens_v2_registrar_l1' THEN $3
                    ELSE source_manifest_id
                END
                WHERE logical_name_id = $1
                  AND source_family IN ('ens_v2_registry_l1', 'ens_v2_registrar_l1')
                "#,
            )
            .bind(corpus.logical_name_id)
            .bind(corpus.registry_manifest_id)
            .bind(corpus.registrar_manifest_id)
            .execute(&database.pool)
            .await
            .context("failed to attach ENSv2 replay exact-name source manifests")?;

            Ok(())
        }

        fn stale_ensv2_sepolia_dev_exact_name_row(
            corpus: &EnsV2ExactNameReplayCorpus,
        ) -> bigname_storage::NameCurrentRow {
            bigname_storage::NameCurrentRow {
                logical_name_id: corpus.logical_name_id.to_owned(),
                namespace: "ens".to_owned(),
                canonical_display_name: corpus.route_name.to_owned(),
                normalized_name: corpus.route_name.to_owned(),
                namehash: format!("namehash:{}", corpus.route_name),
                surface_binding_id: Some(corpus.surface_binding_id),
                resource_id: Some(corpus.resource_id),
                token_lineage_id: Some(corpus.token_lineage_id),
                binding_kind: Some(SurfaceBindingKind::LinkedSubregistryPath),
                declared_summary: json!({
                    "registration": {
                        "status": "active",
                        "authority_kind": "ens_v2_registry",
                        "authority_key": format!(
                            "ens-v2-registry:ethereum-sepolia:{}:0xeac",
                            corpus.route_name
                        ),
                        "registrant": corpus.stale_registrant,
                        "expiry": 1_800_000_000_i64,
                        "latest_event_kind": "RegistrationGranted",
                    },
                    "control": {
                        "registrant": corpus.stale_registrant,
                        "registry_owner": corpus.stale_controller,
                        "latest_event_kind": "AuthorityTransferred",
                    },
                    "resolver": {
                        "chain_id": "ethereum-sepolia",
                        "address": corpus.stale_resolver,
                        "latest_event_kind": "ResolverChanged",
                    },
                    "history": {
                        "surface_head": null,
                        "resource_head": null,
                    },
                }),
                provenance: json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": [{
                        "manifest_version": 10,
                        "source_family": "ens_v2_registry_l1",
                        "source_manifest_id": null,
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "stale_name_current_fixture",
                }),
                coverage: json!({
                    "status": "unsupported",
                    "exhaustiveness": "not_applicable",
                    "source_classes_considered": ["ensv2_registry_resource_surface"],
                    "unsupported_reason": "ensv2_exact_name_profile_shadow",
                    "enumeration_basis": "exact_name",
                }),
                chain_positions: json!({
                    "ethereum-sepolia": {
                        "chain_id": "ethereum-sepolia",
                        "block_number": 200,
                        "block_hash": "0xensv2-stale-current",
                        "timestamp": "2024-05-31T16:00:00Z",
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-sepolia": "finalized",
                    }
                }),
                manifest_version: 10,
                last_recomputed_at: timestamp(1_717_182_000),
            }
        }

        async fn seed_replay_permissions(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            let subject = corpus.losing_permission_subject;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xreplay-permission-1", None, 106, 1_717_181_706),
                    raw_block("base-mainnet", "0xreplay-permission-2", None, 107, 1_717_181_707),
                ],
            )
            .await
            .context("failed to upsert replay permission raw blocks")?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:replay:basenames:resource-permission"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 5,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(106),
                        block_hash: Some("0xreplay-permission-1".to_owned()),
                        transaction_hash: Some("0xtxreplaypermission1".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": "conformance:replay:basenames:resource-permission",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resource",
                            },
                            "effective_powers": ["resource_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:replay:basenames:resource-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:basenames:resolver-permission"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xreplay-permission-2".to_owned()),
                        transaction_hash: Some("0xtxreplaypermission2".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": "conformance:replay:basenames:resolver-permission",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                                "scope": {
                                    "kind": "resolver",
                                    "chain_id": corpus.resolver_chain_id,
                                    "resolver_address": corpus.losing_resolver_address,
                                },
                            "effective_powers": ["resolver_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:replay:basenames:resolver-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                ],
            )
            .await
            .context("failed to upsert replay permission normalized events")?;

            Ok(())
        }

        async fn seed_replay_primary_name_execution(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000c91);
            let finished_at = timestamp(1_717_172_410);
            let verified_primary_name = json!({
                "status": "success",
                "name": {
                    "logical_name_id": corpus.logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": corpus.route_name,
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "namehash:alice.base.eth",
                    "resource_id": corpus.resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }
            });

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "basenames",
                    corpus.primary_name_address,
                    BASENAMES_PRIMARY_COIN_TYPE,
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await
            .context("failed to seed replay primary-name execution trace")?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "basenames",
                    corpus.primary_name_address,
                    BASENAMES_PRIMARY_COIN_TYPE,
                    verified_primary_name,
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await
            .context("failed to seed replay primary-name execution outcome")?;

            Ok(())
        }

        async fn mark_replay_losing_branch_source_rows_orphaned(
            database: &HarnessDatabase,
        ) -> Result<()> {
            set_normalized_events_canonicality(
                database,
                &[
                    "conformance:basenames:grant",
                    "conformance:basenames:authority",
                    "conformance:basenames:resolver",
                    "conformance:basenames:record-version",
                    "conformance:basenames:addr",
                    "conformance:basenames:text",
                    "conformance:replay:basenames:resource-permission",
                    "conformance:replay:basenames:resolver-permission",
                    "conformance:replay:basenames:primary:losing:reverse",
                    "conformance:replay:basenames:primary:losing:claim",
                ],
                CanonicalityState::Orphaned,
            )
            .await?;
            set_raw_blocks_canonicality(
                database,
                "base-mainnet",
                &[
                    "0xbase-grant",
                    "0xbase-authority",
                    "0xbase-resolver",
                    "0xreplay-permission-1",
                    "0xreplay-permission-2",
                    "0xreplay-losing-primary-reverse",
                    "0xreplay-losing-primary-claim",
                ],
                CanonicalityState::Orphaned,
            )
            .await?;

            Ok(())
        }

        async fn seed_replay_winning_branch_source_rows(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            seed_replay_basenames_resolver_profile_gate(database, corpus).await?;
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-grant",
                        Some("0xbase-binding"),
                        101,
                        1_717_191_701,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-authority",
                        Some("0xreplay-winning-grant"),
                        102,
                        1_717_191_702,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-resolver",
                        Some("0xreplay-winning-authority"),
                        103,
                        1_717_191_703,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-permission-1",
                        Some("0xreplay-winning-resolver"),
                        106,
                        1_717_191_706,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-permission-2",
                        Some("0xreplay-winning-permission-1"),
                        107,
                        1_717_191_707,
                    ),
                ],
            )
            .await
            .context("failed to upsert replay winning branch raw blocks")?;
            bigname_storage::upsert_raw_logs(
                &database.pool,
                &[
                    replay_raw_log(
                        "base-mainnet",
                        "0xreplay-winning-resolver",
                        103,
                        "0xtxreplaywinningresolver",
                        1,
                        corpus.winning_resolver_address,
                    ),
                    replay_raw_log(
                        "base-mainnet",
                        "0xreplay-winning-resolver",
                        103,
                        "0xtxreplaywinningresolver",
                        2,
                        corpus.winning_resolver_address,
                    ),
                    replay_raw_log(
                        "base-mainnet",
                        "0xreplay-winning-resolver",
                        103,
                        "0xtxreplaywinningresolver",
                        3,
                        corpus.winning_resolver_address,
                    ),
                    replay_raw_log(
                        "base-mainnet",
                        "0xreplay-winning-permission-2",
                        107,
                        "0xtxreplaywinningpermission2",
                        1,
                        corpus.winning_resolver_address,
                    ),
                    replay_raw_log(
                        "base-mainnet",
                        "0xreplay-winning-permission-2",
                        107,
                        "0xtxreplaywinningpermission2",
                        2,
                        corpus.winning_resolver_address,
                    ),
                    replay_raw_log(
                        "base-mainnet",
                        "0xreplay-winning-permission-2",
                        107,
                        "0xtxreplaywinningpermission2",
                        3,
                        corpus.winning_resolver_address,
                    ),
                ],
            )
            .await
            .context("failed to upsert replay winning branch resolver raw logs")?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:grant".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RegistrationGranted".to_owned(),
                        source_family: "basenames_base_registrar".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(101),
                        block_hash: Some("0xreplay-winning-grant".to_owned()),
                        transaction_hash: Some("0xtxreplaywinninggrant".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:grant",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "authority_kind": "registrar",
                            "authority_key": "registrar:base-mainnet:alice",
                            "registrant": corpus.winning_address_names_address,
                            "expiry": 1_900_000_000_i64,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:authority"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "AuthorityTransferred".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(102),
                        block_hash: Some("0xreplay-winning-authority".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningauthority".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:authority",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "owner": corpus.winning_control_address,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:resolver"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:resolver",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": corpus.winning_resolver_address,
                            "namehash": "namehash:alice.base.eth",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:record-version"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordVersionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(1),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:record-version",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({
                            "record_version": 6,
                        }),
                        after_state: json!({
                            "record_version": 7,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:addr".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(2),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:addr",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "addr:60",
                            "record_family": "addr",
                            "selector_key": "60",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:text".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(3),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:text",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "text",
                            "record_family": "text",
                            "selector_key": null,
                        }),
                    },
                    replay_permission_event(
                        "conformance:replay:winning:basenames:resource-permission",
                        corpus,
                        "0xreplay-winning-permission-1",
                        106,
                        "0xtxreplaywinningpermission1",
                        json!({
                            "kind": "resource",
                        }),
                    ),
                    replay_permission_event(
                        "conformance:replay:winning:basenames:resolver-permission",
                        corpus,
                        "0xreplay-winning-permission-2",
                        107,
                        "0xtxreplaywinningpermission2",
                        json!({
                            "kind": "resolver",
                            "chain_id": corpus.resolver_chain_id,
                            "resolver_address": corpus.winning_resolver_address,
                        }),
                    ),
                    NormalizedEvent {
                        event_identity:
                            "conformance:replay:winning:basenames:record-version-current"
                                .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordVersionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xreplay-winning-permission-2".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningpermission2".to_owned()),
                        log_index: Some(1),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:record-version-current",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({
                            "record_version": 7,
                        }),
                        after_state: json!({
                            "record_version": 8,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:addr-current"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xreplay-winning-permission-2".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningpermission2".to_owned()),
                        log_index: Some(2),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:addr-current",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "addr:60",
                            "record_family": "addr",
                            "selector_key": "60",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:text-current"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xreplay-winning-permission-2".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningpermission2".to_owned()),
                        log_index: Some(3),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:text-current",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "text",
                            "record_family": "text",
                            "selector_key": null,
                        }),
                    },
                ],
            )
            .await
            .context("failed to upsert replay winning branch normalized events")?;

            seed_replay_primary_name_claim_observation(
                database,
                corpus,
                "winning",
                corpus.winning_primary_name,
                "0xreplay-winning-primary-reverse",
                "0xreplay-winning-primary-claim",
                CanonicalityState::Canonical,
            )
            .await?;

            seed_replay_profile_gated_resolver_branches(database).await?;

            Ok(())
        }

        async fn seed_replay_basenames_resolver_profile_gate(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            seed_basenames_l2_resolver_profile_gate(
                database,
                Uuid::from_u128(0xc950),
                REPLAY_PROFILE_SEED_RESOLVER_ADDRESS,
                &[
                    (Uuid::from_u128(0xc951), corpus.winning_resolver_address),
                    (
                        Uuid::from_u128(0xc952),
                        REPLAY_PROFILE_PENDING_RESOLVER_ADDRESS,
                    ),
                    (
                        Uuid::from_u128(0xc953),
                        REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS,
                    ),
                ],
                &[corpus.winning_resolver_address],
                &[REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS],
            )
            .await
            .context("failed to seed replay Basenames L2 resolver profile gate")
        }

        async fn seed_replay_profile_gated_resolver_branches(
            database: &HarnessDatabase,
        ) -> Result<()> {
            seed_replay_profile_gated_resolver_branch(
                database,
                "pending",
                REPLAY_PROFILE_PENDING_LOGICAL_NAME_ID,
                REPLAY_PROFILE_PENDING_ROUTE_NAME,
                Uuid::from_u128(0xc940),
                Uuid::from_u128(0xc941),
                Uuid::from_u128(0xc942),
                REPLAY_PROFILE_PENDING_RESOLVER_ADDRESS,
                270,
            )
            .await?;
            seed_replay_profile_gated_resolver_branch(
                database,
                "unsupported",
                REPLAY_PROFILE_UNSUPPORTED_LOGICAL_NAME_ID,
                REPLAY_PROFILE_UNSUPPORTED_ROUTE_NAME,
                Uuid::from_u128(0xc943),
                Uuid::from_u128(0xc944),
                Uuid::from_u128(0xc945),
                REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS,
                280,
            )
            .await?;

            Ok(())
        }

        #[allow(clippy::too_many_arguments)]
        async fn seed_replay_profile_gated_resolver_branch(
            database: &HarnessDatabase,
            branch: &str,
            logical_name_id: &str,
            route_name: &str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
            resolver_address: &str,
            block_base: i64,
        ) -> Result<()> {
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        &format!("0xreplay-profile-{branch}-surface"),
                        None,
                        block_base,
                        1_717_192_000 + block_base,
                    ),
                    raw_block(
                        "base-mainnet",
                        &format!("0xreplay-profile-{branch}-resource"),
                        None,
                        block_base + 1,
                        1_717_192_001 + block_base,
                    ),
                    raw_block(
                        "base-mainnet",
                        &format!("0xreplay-profile-{branch}-binding"),
                        None,
                        block_base + 2,
                        1_717_192_002 + block_base,
                    ),
                    raw_block(
                        "base-mainnet",
                        &format!("0xreplay-profile-{branch}-grant"),
                        None,
                        block_base + 3,
                        1_717_192_003 + block_base,
                    ),
                    raw_block(
                        "base-mainnet",
                        &format!("0xreplay-profile-{branch}-authority"),
                        None,
                        block_base + 4,
                        1_717_192_004 + block_base,
                    ),
                    raw_block(
                        "base-mainnet",
                        &format!("0xreplay-profile-{branch}-resolver"),
                        None,
                        block_base + 5,
                        1_717_192_005 + block_base,
                    ),
                ],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay profile-gated {branch} raw blocks")
            })?;

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[NameSurface {
                    logical_name_id: logical_name_id.to_owned(),
                    namespace: "basenames".to_owned(),
                    input_name: route_name.to_owned(),
                    canonical_display_name: route_name.to_owned(),
                    normalized_name: route_name.to_owned(),
                    dns_encoded_name: route_name.as_bytes().to_vec(),
                    namehash: format!("namehash:{route_name}"),
                    labelhashes: vec![format!("labelhash:{route_name}")],
                    normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
                    normalization_warnings: json!([]),
                    normalization_errors: json!([]),
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: format!("0xreplay-profile-{branch}-surface"),
                    block_number: block_base,
                    provenance: json!({"seed": "replay_profile_gated_surface"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay profile-gated {branch} name surface")
            })?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[TokenLineage {
                    token_lineage_id,
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: format!("0xreplay-profile-{branch}-resource"),
                    block_number: block_base + 1,
                    provenance: json!({"seed": "replay_profile_gated_token_lineage"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay profile-gated {branch} token lineage")
            })?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[Resource {
                    resource_id,
                    token_lineage_id: Some(token_lineage_id),
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: format!("0xreplay-profile-{branch}-resource"),
                    block_number: block_base + 1,
                    provenance: json!({"seed": "replay_profile_gated_resource"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay profile-gated {branch} resource")
            })?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[SurfaceBinding {
                    surface_binding_id,
                    logical_name_id: logical_name_id.to_owned(),
                    resource_id,
                    binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                    active_from: timestamp(1_717_192_002 + block_base),
                    active_to: None,
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: format!("0xreplay-profile-{branch}-binding"),
                    block_number: block_base + 2,
                    provenance: json!({"seed": "replay_profile_gated_binding"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay profile-gated {branch} surface binding")
            })?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: format!("conformance:replay:profile:{branch}:grant"),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RegistrationGranted".to_owned(),
                        source_family: "basenames_base_registrar".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(block_base + 3),
                        block_hash: Some(format!("0xreplay-profile-{branch}-grant")),
                        transaction_hash: Some(format!("0xtxreplayprofile{branch}grant")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": format!("conformance:replay:profile:{branch}:grant"),
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "authority_kind": "registrar",
                            "authority_key": format!("registrar:base-mainnet:{route_name}"),
                            "registrant": "0x0000000000000000000000000000000000000f10",
                            "expiry": 1_900_000_000_i64,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: format!("conformance:replay:profile:{branch}:authority"),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "AuthorityTransferred".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(block_base + 4),
                        block_hash: Some(format!("0xreplay-profile-{branch}-authority")),
                        transaction_hash: Some(format!("0xtxreplayprofile{branch}authority")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": format!("conformance:replay:profile:{branch}:authority"),
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "owner": "0x0000000000000000000000000000000000000f11",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: format!("conformance:replay:profile:{branch}:resolver"),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(block_base + 5),
                        block_hash: Some(format!("0xreplay-profile-{branch}-resolver")),
                        transaction_hash: Some(format!("0xtxreplayprofile{branch}resolver")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": format!("conformance:replay:profile:{branch}:resolver"),
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": resolver_address,
                            "namehash": format!("namehash:{route_name}"),
                        }),
                    },
                ],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay profile-gated {branch} normalized events")
            })?;

            Ok(())
        }

        fn replay_permission_event(
            event_identity: &str,
            corpus: &ReplayCorpus,
            block_hash: &str,
            block_number: i64,
            transaction_hash: &str,
            scope: Value,
        ) -> NormalizedEvent {
            let scope_kind = scope.get("kind").and_then(Value::as_str);
            let effective_powers = match scope_kind {
                Some("resolver") => json!(["resolver_control"]),
                _ => json!(["resource_control"]),
            };

            NormalizedEvent {
                event_identity: event_identity.to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(corpus.logical_name_id.to_owned()),
                resource_id: Some(corpus.resource_id),
                event_kind: "PermissionChanged".to_owned(),
                source_family: match scope_kind {
                    Some("resolver") => "basenames_base_resolver".to_owned(),
                    _ => "basenames_base_registry".to_owned(),
                },
                manifest_version: match scope_kind {
                    Some("resolver") => 6,
                    _ => 5,
                },
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(block_number),
                block_hash: Some(block_hash.to_owned()),
                transaction_hash: Some(transaction_hash.to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({
                    "kind": "raw_log",
                    "branch": "winning",
                    "event_identity": event_identity,
                }),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "subject": corpus.winning_permission_subject,
                    "scope": scope,
                    "effective_powers": effective_powers,
                    "grant_source": {
                        "kind": "normalized_event",
                        "event_identity": event_identity,
                    },
                    "revocation_source": null,
                    "inheritance_path": [],
                    "transfer_behavior": {},
                }),
            }
        }

        fn replay_raw_log(
            chain_id: &str,
            block_hash: &str,
            block_number: i64,
            transaction_hash: &str,
            log_index: i64,
            emitting_address: &str,
        ) -> bigname_storage::RawLog {
            bigname_storage::RawLog {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index,
                emitting_address: emitting_address.to_ascii_lowercase(),
                topics: Vec::new(),
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        async fn seed_replay_primary_name_claim_observation(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
            branch: &str,
            raw_name: &str,
            reverse_block_hash: &str,
            claim_block_hash: &str,
            canonicality_state: CanonicalityState,
        ) -> Result<()> {
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        reverse_block_hash,
                        Some("0xbase-binding"),
                        260,
                        1_717_192_260,
                    ),
                    raw_block(
                        "base-mainnet",
                        claim_block_hash,
                        Some(reverse_block_hash),
                        261,
                        1_717_192_261,
                    ),
                ],
            )
            .await
            .with_context(|| format!("failed to upsert replay {branch} primary-name raw blocks"))?;

            let normalized_address = corpus.primary_name_address.to_ascii_lowercase();
            let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
            let reverse_event_identity =
                format!("conformance:replay:basenames:primary:{branch}:reverse");
            let claim_event_identity =
                format!("conformance:replay:basenames:primary:{branch}:claim");

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: reverse_event_identity.clone(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: None,
                        resource_id: None,
                        event_kind: "ReverseChanged".to_owned(),
                        source_family: "basenames_base_primary".to_owned(),
                        manifest_version: 1,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(260),
                        block_hash: Some(reverse_block_hash.to_owned()),
                        transaction_hash: Some(format!("0xtxreplayprimary{branch}reverse")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": branch,
                            "event_identity": reverse_event_identity,
                        }),
                        derivation_kind: "ens_v1_reverse_claim".to_owned(),
                        canonicality_state,
                        before_state: json!({}),
                        after_state: json!({
                            "source_event": "NameForAddrChanged",
                            "address": normalized_address.clone(),
                            "coin_type": BASENAMES_PRIMARY_COIN_TYPE,
                            "namespace": "basenames",
                            "reverse_namespace": "basenames",
                            "reverse_label": reverse_label.clone(),
                            "reverse_name": format!("{reverse_label}.80002105.reverse"),
                            "reverse_node": format!("0xreplay{branch}reverse"),
                            "claim_provenance": {
                                "source_family": "basenames_base_primary",
                                "contract_role": "reverse_registrar",
                                "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                                "emitting_address": "0x00000000000000000000000000000000000000ad",
                            },
                        }),
                    },
                    NormalizedEvent {
                        event_identity: claim_event_identity.clone(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: None,
                        resource_id: None,
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_primary".to_owned(),
                        manifest_version: 1,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(261),
                        block_hash: Some(claim_block_hash.to_owned()),
                        transaction_hash: Some(format!("0xtxreplayprimary{branch}claim")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": branch,
                            "event_identity": claim_event_identity,
                        }),
                        derivation_kind: "ens_v1_reverse_claim".to_owned(),
                        canonicality_state,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "name",
                            "record_family": "name",
                            "selector_key": null,
                            "source_event": "NameForAddrChanged",
                            "raw_name": raw_name,
                            "primary_claim_source": {
                                "address": normalized_address,
                                "namespace": "basenames",
                                "coin_type": BASENAMES_PRIMARY_COIN_TYPE,
                                "reverse_name": format!("{reverse_label}.80002105.reverse"),
                                "reverse_node": format!("0xreplay{branch}claim"),
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
            .with_context(|| {
                format!("failed to upsert replay {branch} primary-name normalized events")
            })?;

            Ok(())
        }

        async fn snapshot_replay_supported_read_routes(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<Vec<(&'static str, Value)>> {
            let mut snapshots = Vec::new();
            for route in replay_supported_read_routes(corpus) {
                let payload = request_replay_route(database, &route).await?;
                snapshots.push((route.label, payload));
            }

            Ok(snapshots)
        }

        async fn snapshot_replay_stale_current_answer_routes(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<Vec<(&'static str, Value)>> {
            let mut snapshots = Vec::new();
            for route in replay_stale_current_answer_routes(corpus) {
                let payload = request_replay_route(database, &route).await?;
                snapshots.push((route.label, payload));
            }

            Ok(snapshots)
        }

        fn assert_stale_current_answers_exist(
            snapshots: &[(&'static str, Value)],
            corpus: &ReplayCorpus,
        ) {
            let exact_name = replay_route_payload(snapshots, "exact-name");
            assert_declared_exact_name_branch(
                exact_name,
                corpus.losing_address_names_address,
                corpus.losing_control_address,
                corpus.losing_resolver_address,
            );
            assert_json_not_contains(
                exact_name,
                corpus.winning_address_names_address,
                "exact-name route should not expose the winning branch before replay",
            );

            let resolution = replay_route_payload(snapshots, "resolution");
            assert_json_contains(
                resolution,
                corpus.losing_resolver_address,
                "resolution route should expose the stale losing resolver before replay",
            );
            assert_json_not_contains(
                resolution,
                corpus.winning_resolver_address,
                "resolution route should not expose the winning resolver before replay",
            );

            let losing_address_names = replay_route_payload(snapshots, "losing-address-names");
            assert_collection_non_empty(
                losing_address_names,
                "losing address-name collection should be stale before replay",
            );

            let permissions = replay_route_payload(snapshots, "permissions");
            assert_json_contains(
                permissions,
                corpus.losing_permission_subject,
                "permissions route should expose the stale losing subject before replay",
            );
            assert_json_not_contains(
                permissions,
                corpus.winning_permission_subject,
                "permissions route should not expose the winning subject before replay",
            );

            let resolver = replay_route_payload(snapshots, "losing-resolver");
            assert_json_contains(
                resolver,
                corpus.losing_resolver_address,
                "resolver route should expose the stale losing resolver before replay",
            );

            let primary_name = replay_route_payload(snapshots, "primary-name");
            assert_primary_name_claim(primary_name, corpus.losing_primary_name);
        }

        fn assert_replayed_current_answers_are_canonical(
            snapshots: &[(&'static str, Value)],
            corpus: &ReplayCorpus,
        ) {
            let exact_name = replay_route_payload(snapshots, "exact-name");
            assert_declared_exact_name_branch(
                exact_name,
                corpus.winning_address_names_address,
                corpus.winning_control_address,
                corpus.winning_resolver_address,
            );

            let address_names = replay_route_payload(snapshots, "address-names-collection");
            assert_collection_non_empty(
                address_names,
                "winning address-name collection should exist after replay",
            );

            let address_history = replay_route_payload(snapshots, "address-history");
            assert_collection_non_empty(
                address_history,
                "winning address-history route should expose canonical branch events after replay",
            );

            let resolution = replay_route_payload(snapshots, "resolution");
            assert_json_contains(
                resolution,
                corpus.winning_resolver_address,
                "resolution route should expose the canonical winning resolver after replay",
            );

            let permissions = replay_route_payload(snapshots, "permissions");
            assert_json_contains(
                permissions,
                corpus.winning_permission_subject,
                "permissions route should expose the canonical winning subject after replay",
            );

            let resolver = replay_route_payload(snapshots, "resolver");
            assert_json_contains(
                resolver,
                corpus.winning_resolver_address,
                "resolver route should expose the canonical winning resolver after replay",
            );
            assert_supported_profile_gated_resolution(resolution, corpus);
            assert_supported_profile_gated_resolver(resolver, corpus);
            assert_profile_gated_resolution_stays_explicit(
                replay_route_payload(snapshots, "pending-profile-resolution"),
                REPLAY_PROFILE_PENDING_RESOLVER_ADDRESS,
                "pending",
            );
            assert_profile_gated_resolver_stays_explicit(
                replay_route_payload(snapshots, "pending-profile-resolver"),
                REPLAY_PROFILE_PENDING_RESOLVER_ADDRESS,
                "pending",
            );
            assert_profile_gated_resolution_stays_explicit(
                replay_route_payload(snapshots, "unsupported-profile-resolution"),
                REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS,
                "unsupported",
            );
            assert_profile_gated_resolver_stays_explicit(
                replay_route_payload(snapshots, "unsupported-profile-resolver"),
                REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS,
                "unsupported",
            );

            let primary_name = replay_route_payload(snapshots, "primary-name");
            assert_primary_name_claim(primary_name, corpus.winning_primary_name);

            for (label, payload) in snapshots {
                for forbidden in [
                    corpus.losing_address_names_address,
                    corpus.losing_control_address,
                    corpus.losing_resolver_address,
                    corpus.losing_permission_subject,
                    corpus.losing_primary_name,
                    "0xbase-grant",
                    "0xbase-authority",
                    "0xbase-resolver",
                    "0xreplay-permission-1",
                    "0xreplay-permission-2",
                    "0xreplay-losing-primary-reverse",
                    "0xreplay-losing-primary-claim",
                    "branch\":\"losing",
                ] {
                    assert_json_not_contains(
                        payload,
                        forbidden,
                        &format!(
                            "{label} route should not expose orphaned losing-branch marker {forbidden} after replay"
                        ),
                    );
                }
            }
        }

        fn assert_supported_profile_gated_resolution(payload: &Value, corpus: &ReplayCorpus) {
            assert_eq!(
                payload.pointer("/declared_state/topology/resolver_path/0/address"),
                Some(&json!(corpus.winning_resolver_address))
            );
            assert_eq!(
                payload.pointer("/declared_state/record_inventory/selectors"),
                Some(&json!([
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "cacheable": true,
                    },
                    {
                        "record_key": "text",
                        "record_family": "text",
                        "selector_key": null,
                        "cacheable": true,
                    }
                ]))
            );
            assert_eq!(
                payload.pointer("/declared_state/record_inventory/unsupported_families"),
                Some(&json!([]))
            );
            assert_eq!(
                payload.pointer("/declared_state/record_cache/entries"),
                Some(&json!([
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events",
                    },
                    {
                        "record_key": "text",
                        "record_family": "text",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events",
                    }
                ]))
            );
            assert_json_not_contains(
                payload,
                "resolver_family_pending",
                "supported profile-gated resolution must not expose pending resolver-family state",
            );
        }

        fn assert_supported_profile_gated_resolver(payload: &Value, corpus: &ReplayCorpus) {
            assert_eq!(
                payload.pointer("/data/resolver_address"),
                Some(&json!(corpus.winning_resolver_address))
            );
            assert_eq!(payload.pointer("/data/counts"), Some(&json!({})));
            for section in ["nodes", "aliases", "roles", "events"] {
                assert_eq!(
                    payload.pointer(&format!("/data/{section}")),
                    Some(&Value::Null),
                    "supported profile-gated resolver overview section {section}"
                );
            }
            assert_eq!(
                payload.pointer("/meta/support_status"),
                Some(&json!("unsupported"))
            );
            assert_eq!(
                payload.pointer("/meta/unsupported_fields"),
                Some(&json!(["nodes", "aliases", "roles", "events"]))
            );
            assert_eq!(
                payload.pointer("/meta/coverage/unsupported_reason"),
                Some(&json!("resolver_binding_enumeration_not_projected"))
            );
            assert_json_not_contains(
                payload,
                "resolver_family_pending",
                "supported profile-gated resolver overview must not expose pending sections",
            );
            assert_json_not_contains(
                payload,
                "resolver_family_unsupported",
                "supported profile-gated resolver overview must not expose unsupported-profile sections",
            );
        }

        fn assert_profile_gated_resolution_stays_explicit(
            payload: &Value,
            resolver_address: &str,
            case_label: &str,
        ) {
            let resolver_family_reason = match case_label {
                "unsupported" => "resolver_family_unsupported",
                _ => "resolver_family_pending",
            };
            assert_eq!(
                payload.pointer("/declared_state/topology/resolver_path/0/address"),
                Some(&json!(resolver_address)),
                "case {case_label}"
            );
            assert_eq!(
                payload.pointer("/declared_state/record_inventory/selectors"),
                Some(&json!([])),
                "case {case_label}"
            );
            assert_eq!(
                payload.pointer("/declared_state/record_inventory/unsupported_families"),
                Some(&json!([
                    {
                        "record_family": "addr",
                        "unsupported_reason": resolver_family_reason,
                    },
                    {
                        "record_family": "text",
                        "unsupported_reason": resolver_family_reason,
                    }
                ])),
                "case {case_label}"
            );
            assert_eq!(
                payload.pointer("/declared_state/record_cache/entries"),
                Some(&json!([
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "status": "not_found",
                    }
                ])),
                "case {case_label}"
            );
            assert_json_contains(
                payload,
                resolver_family_reason,
                &format!(
                    "{case_label} profile-gated resolution must keep resolver-family state explicit"
                ),
            );
        }

        fn assert_profile_gated_resolver_stays_explicit(
            payload: &Value,
            resolver_address: &str,
            case_label: &str,
        ) {
            assert_eq!(
                payload.pointer("/data/resolver_address"),
                Some(&json!(resolver_address)),
                "case {case_label}"
            );
            assert_eq!(
                payload.pointer("/data/counts"),
                Some(&json!({})),
                "case {case_label}"
            );
            for section in ["nodes", "aliases", "roles", "events"] {
                assert_eq!(
                    payload.pointer(&format!("/data/{section}")),
                    Some(&Value::Null),
                    "case {case_label} section {section}"
                );
            }
            assert_eq!(
                payload.pointer("/meta/support_status"),
                Some(&json!("unsupported")),
                "case {case_label}"
            );
            assert_eq!(
                payload.pointer("/meta/unsupported_fields"),
                Some(&json!(["nodes", "aliases", "roles", "events"])),
                "case {case_label}"
            );
            assert_eq!(
                payload.pointer("/meta/coverage/unsupported_reason"),
                Some(&json!("resolver_family_pending")),
                "case {case_label}"
            );
        }

        fn assert_ensv2_sepolia_dev_exact_name_replay_payloads(
            name_payload: &Value,
            coverage_payload: &Value,
            corpus: &EnsV2ExactNameReplayCorpus,
        ) {
            let expected_coverage = json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["ens_v2_registry_l1", "ens_v2_registrar_l1"],
                "unsupported_reason": null,
                "enumeration_basis": "exact_name_profile",
            });

            assert_eq!(
                name_payload["data"]["logical_name_id"],
                json!(corpus.logical_name_id)
            );
            assert_eq!(name_payload["data"]["namespace"], json!("ens"));
            assert_eq!(
                name_payload["data"]["binding_kind"],
                json!("linked_subregistry_path")
            );
            assert_eq!(
                name_payload["declared_state"]["registration"]["status"],
                json!("active")
            );
            assert_eq!(
                name_payload["declared_state"]["registration"]["authority_kind"],
                json!("ens_v2_registry")
            );
            assert_eq!(
                name_payload["declared_state"]["registration"]["registrant"],
                json!(corpus.registrant)
            );
            assert_eq!(
                name_payload["declared_state"]["registration"]["latest_event_kind"],
                json!("RegistrationRenewed")
            );
            assert_eq!(
                name_payload["declared_state"]["control"]["registry_owner"],
                json!(corpus.controller)
            );
            assert_eq!(
                name_payload["declared_state"]["resolver"]["address"],
                Value::Null
            );
            assert_eq!(
                name_payload["declared_state"]["record_inventory"]["status"],
                json!("unsupported")
            );
            assert_eq!(name_payload["coverage"], expected_coverage);
            assert_eq!(coverage_payload["coverage"], expected_coverage);
            assert_eq!(coverage_payload["declared_state"], expected_coverage);
            assert_eq!(coverage_payload["data"], name_payload["data"]);
            assert_eq!(
                name_payload["chain_positions"]["ethereum-sepolia"]["block_number"],
                json!(207)
            );
            assert_eq!(name_payload["verified_state"], Value::Null);
            assert_eq!(coverage_payload["verified_state"], Value::Null);
            assert_eq!(name_payload["provenance"], Value::Null);
            let coverage_provenance = coverage_payload["provenance"]
                .as_object()
                .expect("coverage route provenance must be an object");
            assert_eq!(
                coverage_provenance.get("derivation_kind"),
                Some(&json!("name_current_rebuild"))
            );
            assert!(
                !coverage_provenance.contains_key("execution_trace_id"),
                "declared-only coverage provenance must omit execution_trace_id"
            );

            let manifest_versions = coverage_provenance
                .get("manifest_versions")
                .and_then(Value::as_array)
                .expect("coverage provenance manifest_versions must be an array");
            assert!(manifest_versions.iter().any(|entry| {
                entry.get("source_family") == Some(&json!("ens_v2_registry_l1"))
                    && entry.get("manifest_version") == Some(&json!(11))
                    && entry.get("source_manifest_id") == Some(&json!(corpus.registry_manifest_id))
            }));
            assert!(manifest_versions.iter().any(|entry| {
                entry.get("source_family") == Some(&json!("ens_v2_registrar_l1"))
                    && entry.get("manifest_version") == Some(&json!(11))
                    && entry.get("source_manifest_id") == Some(&json!(corpus.registrar_manifest_id))
            }));

            for payload in [name_payload, coverage_payload] {
                for forbidden in [
                    corpus.stale_registrant,
                    corpus.stale_controller,
                    corpus.stale_resolver,
                    "ensv2_exact_name_profile_shadow",
                    "mixed_ensv1_ensv2_exact_name_corpus",
                    "ensv2_registry_resource_surface",
                    "ensv1_registry_path",
                    "stale_name_current_fixture",
                    "0xensv2-stale-current",
                ] {
                    assert_json_not_contains(
                        payload,
                        forbidden,
                        &format!(
                            "ENSv2 sepolia-dev replay payload should not expose stale or unsupported marker {forbidden}"
                        ),
                    );
                }
            }
        }

        fn replay_route_payload<'a>(
            snapshots: &'a [(&'static str, Value)],
            label: &str,
        ) -> &'a Value {
            snapshots
                .iter()
                .find_map(|(snapshot_label, payload)| {
                    (*snapshot_label == label).then_some(payload)
                })
                .unwrap_or_else(|| panic!("missing replay route snapshot {label}"))
        }

        fn assert_declared_exact_name_branch(
            payload: &Value,
            registrant: &str,
            registry_owner: &str,
            resolver_address: &str,
        ) {
            assert_eq!(
                string_at(payload, &["declared_state", "registration", "registrant"]),
                registrant
            );
            assert_eq!(
                string_at(payload, &["declared_state", "control", "registry_owner"]),
                registry_owner
            );
            assert_eq!(
                string_at(payload, &["declared_state", "resolver", "address"]),
                resolver_address
            );
        }

        fn assert_primary_name_claim(payload: &Value, expected_name: &str) {
            assert_eq!(
                string_at(
                    payload,
                    &["declared_state", "claimed_primary_name", "name"]
                ),
                expected_name
            );
        }

        fn string_at<'a>(payload: &'a Value, path: &[&str]) -> &'a str {
            let mut current = payload;
            for segment in path {
                current = current
                    .get(*segment)
                    .unwrap_or_else(|| panic!("payload missing JSON path segment {segment}"));
            }
            current
                .as_str()
                .unwrap_or_else(|| panic!("payload JSON path {path:?} must be a string"))
        }

        fn assert_collection_non_empty(payload: &Value, message: &str) {
            let data = payload
                .get("data")
                .and_then(Value::as_array)
                .expect("collection payload must include array data");
            assert!(!data.is_empty(), "{message}");
        }

        fn assert_json_contains(payload: &Value, needle: &str, message: &str) {
            let encoded = serde_json::to_string(payload).expect("payload must serialize");
            assert!(encoded.contains(needle), "{message}");
        }

        fn assert_json_not_contains(payload: &Value, needle: &str, message: &str) {
            let encoded = serde_json::to_string(payload).expect("payload must serialize");
            assert!(!encoded.contains(needle), "{message}");
        }

        async fn assert_replay_collection_empty(
            database: &HarnessDatabase,
            route: ReplayRoute,
        ) -> Result<()> {
            let label = route.label;
            let payload = request_replay_route(database, &route).await?;
            let data = payload
                .get("data")
                .and_then(Value::as_array)
                .with_context(|| format!("{label} payload must include array data"))?;
            assert!(data.is_empty(), "{label} should not return collection rows");

            Ok(())
        }

        async fn assert_replay_route_status(
            database: &HarnessDatabase,
            route: ReplayRoute,
            expected_status: StatusCode,
        ) -> Result<()> {
            let label = route.label;
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(route.uri.as_str())
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("{label} replay route status request failed"))?;

            assert_eq!(
                response.status(),
                expected_status,
                "{label} replay route returned unexpected status",
            );

            Ok(())
        }

        fn replay_stale_current_answer_routes(corpus: &ReplayCorpus) -> Vec<ReplayRoute> {
            vec![
                ReplayRoute {
                    label: "exact-name",
                    uri: format!("/v1/names/basenames/{}", corpus.route_name),
                },
                ReplayRoute {
                    label: "losing-address-names",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.losing_address_names_address
                    ),
                },
                ReplayRoute {
                    label: "resolution",
                    uri: format!(
                        "/v1/profiles/names/{}?mode=declared&meta=full",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "permissions",
                    uri: format!("/v1/resources/{}/permissions", corpus.resource_id),
                },
                ReplayRoute {
                    label: "losing-resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}/overview?meta=full",
                        corpus.resolver_chain_id, corpus.losing_resolver_address
                    ),
                },
                ReplayRoute {
                    label: "primary-name",
                    uri: format!(
                        "/v1/primary-names/{}?namespace=basenames&coin_type={BASENAMES_PRIMARY_COIN_TYPE}&mode=both",
                        corpus.primary_name_address
                    ),
                },
            ]
        }

        fn replay_supported_read_routes(corpus: &ReplayCorpus) -> Vec<ReplayRoute> {
            vec![
                ReplayRoute {
                    label: "exact-name",
                    uri: format!("/v1/names/basenames/{}", corpus.route_name),
                },
                ReplayRoute {
                    label: "children-collection",
                    uri: "/v1/names/ens/parent.eth/children?meta=full".to_owned(),
                },
                ReplayRoute {
                    label: "address-names-collection",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.winning_address_names_address
                    ),
                },
                ReplayRoute {
                    label: "name-history",
                    uri: format!(
                        "/v1/history/names/basenames/{}?scope=both",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "resource-history",
                    uri: format!("/v1/history/resources/{}?scope=both", corpus.resource_id),
                },
                ReplayRoute {
                    label: "address-history",
                    uri: format!(
                        "/v1/history/addresses/{}?namespace=basenames&relation=registrant",
                        corpus.winning_address_names_address
                    ),
                },
                ReplayRoute {
                    label: "resolution",
                    uri: format!(
                        "/v1/profiles/names/{}?mode=declared&meta=full",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "permissions",
                    uri: format!("/v1/resources/{}/permissions", corpus.resource_id),
                },
                ReplayRoute {
                    label: "resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}/overview?meta=full",
                        corpus.resolver_chain_id, corpus.winning_resolver_address
                    ),
                },
                ReplayRoute {
                    label: "pending-profile-resolution",
                    uri: format!(
                        "/v1/profiles/names/{}?mode=declared&meta=full",
                        REPLAY_PROFILE_PENDING_ROUTE_NAME
                    ),
                },
                ReplayRoute {
                    label: "pending-profile-resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}/overview?meta=full",
                        corpus.resolver_chain_id, REPLAY_PROFILE_PENDING_RESOLVER_ADDRESS
                    ),
                },
                ReplayRoute {
                    label: "unsupported-profile-resolution",
                    uri: format!(
                        "/v1/profiles/names/{}?mode=declared&meta=full",
                        REPLAY_PROFILE_UNSUPPORTED_ROUTE_NAME
                    ),
                },
                ReplayRoute {
                    label: "unsupported-profile-resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}/overview?meta=full",
                        corpus.resolver_chain_id, REPLAY_PROFILE_UNSUPPORTED_RESOLVER_ADDRESS
                    ),
                },
                ReplayRoute {
                    label: "primary-name",
                    uri: format!(
                        "/v1/primary-names/{}?namespace=basenames&coin_type={BASENAMES_PRIMARY_COIN_TYPE}&mode=both",
                        corpus.primary_name_address
                    ),
                },
            ]
        }

        async fn request_replay_route(
            database: &HarnessDatabase,
            route: &ReplayRoute,
        ) -> Result<Value> {
            database
                .seed_snapshot_selector_for_route(route.uri.as_str())
                .await?;
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(route.uri.as_str())
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("{} replay route request failed", route.label))?;

            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .with_context(|| {
                    format!("failed to read {} replay route response body", route.label)
                })?;
            assert_eq!(
                status,
                StatusCode::OK,
                "{} replay route returned unexpected status; body {}",
                route.label,
                String::from_utf8_lossy(&bytes)
            );

            serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to decode {} replay route response", route.label))
        }
