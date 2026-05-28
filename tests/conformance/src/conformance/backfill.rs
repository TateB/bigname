        pub(crate) async fn run_backfilled_data_consumer_conformance_job() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_replay_supported_read_corpus(&database).await?;
            let completed_job = seed_completed_backfill_job(&database).await?;

            assert_eq!(
                completed_job.job.status,
                bigname_storage::BackfillLifecycleStatus::Completed
            );
            assert_eq!(completed_job.ranges.len(), 2);
            assert!(completed_job.job.completed_at.is_some());
            assert!(
                completed_job.ranges.iter().all(|range| {
                    range.status == bigname_storage::BackfillLifecycleStatus::Completed
                        && range.checkpoint_block_number == range.range_end_block_number
                        && range.completed_at.is_some()
                }),
                "completed backfill job must persist completed child ranges"
            );

            replay_all_current_projections(&database).await?;

            let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
            assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "backfill-losing-address-names-after-replay",
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
                    label: "backfill-losing-address-history-after-replay",
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
                    label: "backfill-losing-resolver-after-replay",
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
