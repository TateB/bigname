        struct SourceFamilyBackfillFixture {
            namespace: &'static str,
            deployment_profile: &'static str,
            chain_id: &'static str,
            source_family: &'static str,
            contract_instance_id: &'static str,
            address: &'static str,
            range_start_block_number: i64,
            range_end_block_number: i64,
        }

        pub(crate) async fn run_backfill_source_family_existing_response_lock() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_replay_supported_read_corpus(&database).await?;
            let ensv1_logical_name_id = "ens:alice.eth";
            let ensv1_resource_id = Uuid::from_u128(0xc930);
            database
                .seed_exact_name_rebuild_inputs(
                    ensv1_logical_name_id,
                    ensv1_resource_id,
                    Uuid::from_u128(0xc931),
                    Uuid::from_u128(0xc932),
                )
                .await?;

            let completed_jobs = seed_completed_source_family_backfill_jobs(&database).await?;
            assert_completed_source_family_backfill_jobs(&completed_jobs);

            replay_all_current_projections(&database).await?;

            let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
            assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
            assert_existing_ensv1_exact_name_after_jobs_and_replay(
                &database,
                ensv1_logical_name_id,
            )
            .await?;
            assert_ensv2_shadow_exact_name_coverage_is_not_graduated(&after_replay);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "existing-response-source-family-losing-address-names-after-replay",
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
                    label: "existing-response-source-family-losing-address-history-after-replay",
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
                    label: "existing-response-source-family-losing-resolver-after-replay",
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

        async fn seed_completed_source_family_backfill_jobs(
            database: &HarnessDatabase,
        ) -> Result<Vec<(SourceFamilyBackfillFixture, bigname_storage::BackfillJobRecord)>> {
            let mut records = Vec::new();
            for fixture in source_family_backfill_fixtures() {
                let record = seed_completed_source_family_backfill_job(database, &fixture).await?;
                records.push((fixture, record));
            }

            Ok(records)
        }

        async fn seed_completed_source_family_backfill_job(
            database: &HarnessDatabase,
            fixture: &SourceFamilyBackfillFixture,
        ) -> Result<bigname_storage::BackfillJobRecord> {
            let midpoint =
                fixture.range_start_block_number
                    + ((fixture.range_end_block_number - fixture.range_start_block_number) / 2);
            let created = bigname_storage::create_backfill_job(
                &database.pool,
                &bigname_storage::BackfillJobCreate {
                    deployment_profile: fixture.deployment_profile.to_owned(),
                    chain_id: fixture.chain_id.to_owned(),
                    source_identity: source_family_identity(fixture),
                    scan_mode: "hash_pinned_block".to_owned(),
                    range_start_block_number: fixture.range_start_block_number,
                    range_end_block_number: fixture.range_end_block_number,
                    idempotency_key: format!(
                        "conformance-source-family-{}",
                        fixture.source_family
                    ),
                    ranges: vec![
                        bigname_storage::BackfillRangeSpec {
                            range_start_block_number: fixture.range_start_block_number,
                            range_end_block_number: midpoint,
                        },
                        bigname_storage::BackfillRangeSpec {
                            range_start_block_number: midpoint + 1,
                            range_end_block_number: fixture.range_end_block_number,
                        },
                    ],
                },
            )
            .await
            .with_context(|| {
                format!(
                    "failed to create source-family backfill job for {}",
                    fixture.source_family
                )
            })?;

            for (index, range) in created.ranges.iter().enumerate() {
                let lease_token = format!(
                    "conformance-source-family-{}-lease-{index}",
                    fixture.source_family
                );
                let lease_expires_at = OffsetDateTime::from_unix_timestamp(
                    OffsetDateTime::now_utc().unix_timestamp() + 300,
                )
                .context("failed to build source-family conformance backfill lease deadline")?;
                let reserved = bigname_storage::reserve_backfill_range(
                    &database.pool,
                    created.job.backfill_job_id,
                    "conformance-source-family-backfill",
                    &lease_token,
                    lease_expires_at,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to reserve source-family backfill range for {}",
                        fixture.source_family
                    )
                })?
                .with_context(|| {
                    format!(
                        "source-family backfill range should be reservable for {}",
                        fixture.source_family
                    )
                })?;
                anyhow::ensure!(
                    reserved.backfill_range_id == range.backfill_range_id,
                    "reserved unexpected source-family backfill range {} instead of {} for {}",
                    reserved.backfill_range_id,
                    range.backfill_range_id,
                    fixture.source_family
                );

                bigname_storage::advance_backfill_range(
                    &database.pool,
                    range.backfill_range_id,
                    &lease_token,
                    range.range_end_block_number,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to advance source-family backfill range for {}",
                        fixture.source_family
                    )
                })?;
                bigname_storage::complete_backfill_range(
                    &database.pool,
                    range.backfill_range_id,
                    &lease_token,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to complete source-family backfill range for {}",
                        fixture.source_family
                    )
                })?;
            }

            let job =
                bigname_storage::load_backfill_job(&database.pool, created.job.backfill_job_id)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to load completed source-family backfill job for {}",
                            fixture.source_family
                        )
                    })?
                    .with_context(|| {
                        format!(
                            "completed source-family backfill job must exist for {}",
                            fixture.source_family
                        )
                    })?;
            let ranges =
                bigname_storage::load_backfill_ranges(&database.pool, created.job.backfill_job_id)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to load completed source-family backfill ranges for {}",
                            fixture.source_family
                        )
                    })?;

            Ok(bigname_storage::BackfillJobRecord { job, ranges })
        }

        fn assert_completed_source_family_backfill_jobs(
            completed_jobs: &[(SourceFamilyBackfillFixture, bigname_storage::BackfillJobRecord)],
        ) {
            let covered_families = completed_jobs
                .iter()
                .map(|(fixture, _)| fixture.source_family)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                covered_families,
                BTreeSet::from([
                    "ens_v1_registry_l1",
                    "ens_v1_registrar_l1",
                    "ens_v1_reverse_l1",
                    "ens_v2_root_l1",
                    "ens_v2_registry_l1",
                    "ens_v2_registrar_l1",
                    "ens_v2_resolver_l1",
                    "basenames_base_registry",
                    "basenames_base_registrar",
                    "basenames_base_resolver",
                    "basenames_base_primary",
                ])
            );

            for (fixture, completed_job) in completed_jobs {
                let expected_source_identity_hash =
                    stable_source_identity_hash(&source_family_identity_payload_without_hash(
                        fixture,
                    ));
                assert_eq!(
                    completed_job.job.status,
                    bigname_storage::BackfillLifecycleStatus::Completed,
                    "{} source-family job must be completed",
                    fixture.source_family
                );
                assert_eq!(completed_job.job.deployment_profile, fixture.deployment_profile);
                assert_eq!(completed_job.job.chain_id, fixture.chain_id);
                assert_eq!(completed_job.job.scan_mode, "hash_pinned_block");
                assert_eq!(
                    completed_job
                        .job
                        .source_identity
                        .get("selector_kind")
                        .and_then(Value::as_str),
                    Some("source_family")
                );
                assert_eq!(
                    completed_job
                        .job
                        .source_identity
                        .get("source_family")
                        .and_then(Value::as_str),
                    Some(fixture.source_family)
                );
                assert_eq!(
                    completed_job
                        .job
                        .source_identity
                        .get("requested_watched_targets")
                        .and_then(Value::as_array)
                        .map(Vec::len),
                    Some(0),
                    "{} source-family job should persist the canonical empty requested target set",
                    fixture.source_family
                );
                assert_eq!(
                    completed_job
                        .job
                        .source_identity
                        .get("source_identity_hash")
                        .and_then(Value::as_str),
                    Some(expected_source_identity_hash.as_str()),
                    "{} source-family job hash must cover the full selector payload",
                    fixture.source_family
                );
                assert_eq!(
                    completed_job
                        .job
                        .source_identity
                        .get("source_identity_hash")
                        .and_then(Value::as_str)
                        .map(|hash| hash.starts_with("fnv1a64:")),
                    Some(true)
                );
                let selected_targets = completed_job
                    .job
                    .source_identity
                    .get("selected_targets")
                    .and_then(Value::as_array)
                    .expect("source-family job should persist selected targets");
                assert_eq!(selected_targets.len(), 1);
                let selected_target = &selected_targets[0];
                assert_eq!(
                    selected_target
                        .get("source_family")
                        .and_then(Value::as_str),
                    Some(fixture.source_family)
                );
                assert_eq!(
                    selected_target
                        .get("contract_instance_id")
                        .and_then(Value::as_str),
                    Some(fixture.contract_instance_id)
                );
                assert!(
                    Uuid::parse_str(fixture.contract_instance_id).is_ok(),
                    "{} fixture contract_instance_id should be UUID-shaped",
                    fixture.source_family
                );
                assert_ne!(
                    fixture.contract_instance_id, fixture.source_family,
                    "{} fixture contract_instance_id must not collapse to source_family",
                    fixture.source_family
                );
                assert_eq!(
                    selected_target.get("address").and_then(Value::as_str),
                    Some(fixture.address)
                );
                assert_eq!(
                    selected_target
                        .get("effective_from_block")
                        .and_then(Value::as_i64),
                    Some(fixture.range_start_block_number)
                );
                assert_eq!(
                    selected_target
                        .get("effective_to_block")
                        .and_then(Value::as_i64),
                    Some(fixture.range_end_block_number)
                );
                assert!(
                    completed_job.job.completed_at.is_some(),
                    "{} source-family job must record completion time",
                    fixture.source_family
                );
                assert_eq!(completed_job.ranges.len(), 2);
                assert!(
                    completed_job.ranges.iter().all(|range| {
                        range.status == bigname_storage::BackfillLifecycleStatus::Completed
                            && range.checkpoint_block_number == range.range_end_block_number
                            && range.completed_at.is_some()
                    }),
                    "{} source-family job must persist completed child ranges",
                    fixture.source_family
                );
            }
        }

        async fn assert_existing_ensv1_exact_name_after_jobs_and_replay(
            database: &HarnessDatabase,
            logical_name_id: &str,
        ) -> Result<()> {
            let route_name = logical_name_id
                .split_once(':')
                .map(|(_, name)| name)
                .expect("logical_name_id must include namespace");
            let payload = request_replay_route(
                database,
                &ReplayRoute {
                    label: "existing-response-ensv1-exact-name-after-completed-source-family-jobs-and-replay",
                    uri: format!("/v1/names/ens/{route_name}"),
                },
            )
            .await?;

            assert_declared_exact_name_branch(
                &payload,
                "0x00000000000000000000000000000000000000aa",
                "0x00000000000000000000000000000000000000bb",
                "0x0000000000000000000000000000000000000abc",
            );
            assert_eq!(
                payload.get("coverage").and_then(|coverage| {
                    coverage.get("unsupported_reason").and_then(Value::as_str)
                }),
                None
            );

            Ok(())
        }

        fn assert_ensv2_shadow_exact_name_coverage_is_not_graduated(
            snapshots: &[(&'static str, Value)],
        ) {
            let children = replay_route_payload(snapshots, "children-collection");
            assert_json_contains(
                children,
                "ens_v2_registry_l1",
                "ENSv2 child response should remain tied to the admitted registry source family",
            );
            assert_json_not_contains(
                children,
                "ensv2 sepolia-dev exact-name profile is shadow-only",
                "source-family existing-response conformance must not surface exact-name shadow coverage on children responses",
            );
        }

        fn source_family_identity(fixture: &SourceFamilyBackfillFixture) -> Value {
            let mut payload = source_family_identity_payload_without_hash(fixture);
            let source_identity_hash = stable_source_identity_hash(&payload);
            payload
                .as_object_mut()
                .expect("source-family identity payload must be an object")
                .insert(
                    "source_identity_hash".to_owned(),
                    Value::String(source_identity_hash),
                );
            payload
        }

        fn source_family_identity_payload_without_hash(
            fixture: &SourceFamilyBackfillFixture,
        ) -> Value {
            json!({
                "selector_kind": "source_family",
                "source_family": fixture.source_family,
                "requested_watched_targets": [],
                "selected_targets": [{
                    "source_family": fixture.source_family,
                    "contract_instance_id": fixture.contract_instance_id,
                    "address": fixture.address,
                    "effective_from_block": fixture.range_start_block_number,
                    "effective_to_block": fixture.range_end_block_number,
                }],
            })
        }

        fn stable_source_identity_hash(payload_without_hash: &Value) -> String {
            let serialized = serde_json::to_string(payload_without_hash)
                .expect("source-family identity payload must be serializable");
            let hash = serialized
                .bytes()
                .fold(0xcbf29ce484222325, |hash, byte| {
                    (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                });
            format!("fnv1a64:{hash:016x}")
        }

        fn source_family_backfill_fixtures() -> Vec<SourceFamilyBackfillFixture> {
            vec![
                source_family_backfill_fixture(
                    "ens",
                    "mainnet",
                    "ethereum-mainnet",
                    "ens_v1_registry_l1",
                    "00000000-0000-0000-0000-000000009101",
                    "0x0000000000000000000000000000000000000101",
                    90,
                    180,
                ),
                source_family_backfill_fixture(
                    "ens",
                    "mainnet",
                    "ethereum-mainnet",
                    "ens_v1_registrar_l1",
                    "00000000-0000-0000-0000-000000009102",
                    "0x0000000000000000000000000000000000000102",
                    181,
                    260,
                ),
                source_family_backfill_fixture(
                    "ens",
                    "mainnet",
                    "ethereum-mainnet",
                    "ens_v1_reverse_l1",
                    "00000000-0000-0000-0000-000000009103",
                    "0x0000000000000000000000000000000000000103",
                    261,
                    320,
                ),
                source_family_backfill_fixture(
                    "ens",
                    "sepolia-dev",
                    "ethereum-sepolia",
                    "ens_v2_root_l1",
                    "00000000-0000-0000-0000-000000009201",
                    "0x0000000000000000000000000000000000000201",
                    80,
                    120,
                ),
                source_family_backfill_fixture(
                    "ens",
                    "sepolia-dev",
                    "ethereum-sepolia",
                    "ens_v2_registry_l1",
                    "00000000-0000-0000-0000-000000009202",
                    "0x0000000000000000000000000000000000000202",
                    121,
                    180,
                ),
                source_family_backfill_fixture(
                    "ens",
                    "sepolia-dev",
                    "ethereum-sepolia",
                    "ens_v2_registrar_l1",
                    "00000000-0000-0000-0000-000000009203",
                    "0x0000000000000000000000000000000000000203",
                    181,
                    240,
                ),
                source_family_backfill_fixture(
                    "ens",
                    "sepolia-dev",
                    "ethereum-sepolia",
                    "ens_v2_resolver_l1",
                    "00000000-0000-0000-0000-000000009204",
                    "0x0000000000000000000000000000000000000204",
                    241,
                    300,
                ),
                source_family_backfill_fixture(
                    "basenames",
                    "mainnet",
                    "base-mainnet",
                    "basenames_base_registry",
                    "00000000-0000-0000-0000-000000009301",
                    "0x0000000000000000000000000000000000000301",
                    98,
                    150,
                ),
                source_family_backfill_fixture(
                    "basenames",
                    "mainnet",
                    "base-mainnet",
                    "basenames_base_registrar",
                    "00000000-0000-0000-0000-000000009302",
                    "0x0000000000000000000000000000000000000302",
                    151,
                    210,
                ),
                source_family_backfill_fixture(
                    "basenames",
                    "mainnet",
                    "base-mainnet",
                    "basenames_base_resolver",
                    "00000000-0000-0000-0000-000000009303",
                    "0x0000000000000000000000000000000000000303",
                    211,
                    261,
                ),
                source_family_backfill_fixture(
                    "basenames",
                    "mainnet",
                    "base-mainnet",
                    "basenames_base_primary",
                    "00000000-0000-0000-0000-000000009304",
                    "0x0000000000000000000000000000000000000304",
                    262,
                    320,
                ),
            ]
        }

        fn source_family_backfill_fixture(
            namespace: &'static str,
            deployment_profile: &'static str,
            chain_id: &'static str,
            source_family: &'static str,
            contract_instance_id: &'static str,
            address: &'static str,
            range_start_block_number: i64,
            range_end_block_number: i64,
        ) -> SourceFamilyBackfillFixture {
            SourceFamilyBackfillFixture {
                namespace,
                deployment_profile,
                chain_id,
                source_family,
                contract_instance_id,
                address,
                range_start_block_number,
                range_end_block_number,
            }
        }
