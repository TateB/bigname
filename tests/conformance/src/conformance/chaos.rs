        struct ChaosReorgOutcome {
            raw_block_count: u64,
            normalized_event_count: u64,
        }

        pub(crate) async fn run_reorg_chaos_drill_conformance_job() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_chaos_losing_current_corpus(&database).await?;

            let before_reorg = snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
            assert_stale_current_answers_exist(&before_reorg, &corpus);

            stitch_chaos_losing_branch_parent_links(&database).await?;
            let reorg_outcome = apply_chaos_losing_branch_reorg(&database).await?;
            assert_eq!(reorg_outcome.raw_block_count, 0);
            assert_eq!(reorg_outcome.normalized_event_count, 10);

            let raw_replay_block =
                "0xc0a05000000000000000000000000000000000000000000000000000000000303";
            seed_raw_fact_replay_probe(
                &database,
                "ethereum-mainnet",
                raw_replay_block,
                "0x0000000000000000000000000000000000000c0a",
            )
            .await?;
            replay_raw_fact_normalized_events_for_blocks(
                &database,
                "mainnet",
                "ethereum-mainnet",
                &[raw_replay_block],
            )
            .await?;
            assert_chaos_raw_fact_replay_probe_replayed(&database, raw_replay_block).await?;

            seed_replay_winning_branch_source_rows(&database, &corpus).await?;
            replay_all_current_projections(&database).await?;

            let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
            assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "chaos-losing-address-names-after-replay",
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
                    label: "chaos-losing-address-history-after-replay",
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
                    label: "chaos-losing-resolver-after-replay",
                    uri: format!(
                        "/v1/resolvers/{}/{}",
                        corpus.resolver_chain_id, corpus.losing_resolver_address
                    ),
                },
                StatusCode::NOT_FOUND,
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_chaos_losing_current_corpus(database: &HarnessDatabase) -> Result<ReplayCorpus> {
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
                .rebuild_primary_names_current(corpus.primary_name_address, "basenames", "60")
                .await?;
            seed_replay_primary_name_execution(database, &corpus).await?;

            Ok(corpus)
        }

        async fn stitch_chaos_losing_branch_parent_links(database: &HarnessDatabase) -> Result<()> {
            for (block_hash, parent_hash) in [
                ("0xbase-grant", "0xbase-binding"),
                ("0xbase-authority", "0xbase-grant"),
                ("0xbase-resolver", "0xbase-authority"),
                ("0xreplay-permission-1", "0xbase-resolver"),
                ("0xreplay-permission-2", "0xreplay-permission-1"),
            ] {
                let updated = sqlx::query(
                    r#"
                    UPDATE chain_lineage
                    SET parent_hash = $1
                    WHERE chain_id = 'base-mainnet'
                      AND block_hash = $2
                    "#,
                )
                .bind(parent_hash)
                .bind(block_hash)
                .execute(&database.pool)
                .await
                .with_context(|| {
                    format!("failed to stitch chaos parent link for lineage block {block_hash}")
                })?
                .rows_affected();
                anyhow::ensure!(
                    updated == 1,
                    "expected to stitch one chaos parent link for lineage block {block_hash}, updated {updated}"
                );
            }

            Ok(())
        }

        async fn apply_chaos_losing_branch_reorg(
            database: &HarnessDatabase,
        ) -> Result<ChaosReorgOutcome> {
            let main = orphan_chaos_reorg_path(
                database,
                "base-mainnet",
                "0xreplay-permission-2",
                Some("0xbase-binding"),
            )
            .await?;
            let primary = orphan_chaos_reorg_path(
                database,
                "base-mainnet",
                "0xreplay-losing-primary-claim",
                Some("0xbase-binding"),
            )
            .await?;

            Ok(ChaosReorgOutcome {
                raw_block_count: main.raw_block_count + primary.raw_block_count,
                normalized_event_count: main.normalized_event_count + primary.normalized_event_count,
            })
        }

        async fn orphan_chaos_reorg_path(
            database: &HarnessDatabase,
            chain: &str,
            from_hash: &str,
            stop_before_hash: Option<&str>,
        ) -> Result<ChaosReorgOutcome> {
            let raw_counts = bigname_storage::mark_raw_block_facts_range_orphaned(
                &database.pool,
                chain,
                from_hash,
                stop_before_hash,
            )
            .await
            .with_context(|| {
                format!("failed to orphan chaos raw facts for {chain} from {from_hash}")
            })?;
            let normalized_event_count =
                bigname_storage::mark_block_derived_normalized_events_range_orphaned(
                    &database.pool,
                    chain,
                    from_hash,
                    stop_before_hash,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to orphan chaos normalized events for {chain} from {from_hash}"
                    )
                })?;

            Ok(ChaosReorgOutcome {
                raw_block_count: raw_counts.block_count,
                normalized_event_count,
            })
        }

        async fn assert_chaos_raw_fact_replay_probe_replayed(
            database: &HarnessDatabase,
            block_hash: &str,
        ) -> Result<()> {
            let raw_log_count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM raw_logs
                WHERE chain_id = 'ethereum-mainnet'
                  AND block_hash = $1
                  AND canonicality_state = 'canonical'::canonicality_state
                "#,
            )
            .bind(block_hash)
            .fetch_one(&database.pool)
            .await
            .context("failed to count chaos raw-fact replay probe logs")?;
            assert_eq!(
                raw_log_count, 1,
                "chaos raw-fact replay probe should remain a canonical persisted raw log"
            );

            let normalized_event_count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM normalized_events
                WHERE chain_id = 'ethereum-mainnet'
                  AND block_hash = $1
                  AND source_family = $2
                  AND event_kind = 'ReverseChanged'
                  AND canonicality_state = 'canonical'::canonicality_state
                  AND source_manifest_id IS NOT NULL
                  AND raw_fact_ref->>'block_hash' = $1
                  AND after_state->>'address' = $3
                "#,
            )
            .bind(block_hash)
            .bind(RAW_REPLAY_PROBE_SOURCE_FAMILY)
            .bind(RAW_REPLAY_PROBE_CLAIMED_ADDRESS)
            .fetch_one(&database.pool)
            .await
            .context("failed to count chaos raw-fact replay probe normalized events")?;
            assert_eq!(
                normalized_event_count, 1,
                "chaos raw-fact replay probe should insert or refresh one normalized event"
            );

            Ok(())
        }
