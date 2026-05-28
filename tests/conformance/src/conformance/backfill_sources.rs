const ENS_V1_REGISTRY_CURRENT_CONTRACT_INSTANCE_ID: &str = "00000000-0000-0000-0000-000000009101";
const ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID: &str = "00000000-0000-0000-0000-000000009106";
const ENS_V1_REGISTRY_AUTO_BOOTSTRAP_CURRENT_CONTRACT_INSTANCE_ID: &str =
    "00000000-0000-0000-0000-00000000b101";
const ENS_V1_REGISTRY_AUTO_BOOTSTRAP_OLD_CONTRACT_INSTANCE_ID: &str =
    "00000000-0000-0000-0000-00000000b102";
const ENS_V1_REGISTRY_CURRENT_ADDRESS: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const ENS_V1_REGISTRY_OLD_ADDRESS: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
const ENS_V1_REGISTRY_CURRENT_START_BLOCK: i64 = 9_380_380;
const ENS_V1_REGISTRY_OLD_START_BLOCK: i64 = 3_327_417;
const ENS_V1_REGISTRY_BACKFILL_END_BLOCK: i64 = 10_000_000;
const ENS_V1_REGISTRY_OLD_REPLAY_NEW_OWNER_TOPIC0: &str =
    "0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82";
const ENS_V1_REGISTRY_OLD_REPLAY_PARENT_NODE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
const ENS_V1_REGISTRY_OLD_REPLAY_LABELHASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const ENS_V1_REGISTRY_OLD_REPLAY_CURRENT_OWNER: &str = "0x0000000000000000000000000000000000000002";

#[derive(Clone)]
struct SourceFamilyBackfillTarget {
    contract_instance_id: &'static str,
    address: &'static str,
    range_start_block_number: i64,
    range_end_block_number: i64,
}

struct SourceFamilyBackfillFixture {
    namespace: &'static str,
    deployment_profile: &'static str,
    chain_id: &'static str,
    source_family: &'static str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    selected_targets: Vec<SourceFamilyBackfillTarget>,
}

struct DynamicResolverBackfillFixture {
    deployment_profile: &'static str,
    chain_id: &'static str,
    source_family: &'static str,
    contract_instance_id: &'static str,
    address: &'static str,
    range_start_block_number: i64,
    range_end_block_number: i64,
    effective_from_block: i64,
    effective_to_block: i64,
    idempotency_key: &'static str,
}

#[derive(Clone)]
struct AutoBootstrapBackfillFixture {
    namespace: &'static str,
    deployment_profile: &'static str,
    chain_id: &'static str,
    source_family: &'static str,
    contract_instance_id: &'static str,
    address: &'static str,
    manifest_role: &'static str,
    manifest_start_block_number: i64,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
    provider_head_block_number: i64,
    bootstrap_backfill_max_blocks: Option<i64>,
}

struct AutoBootstrapUnknownStartFixture {
    namespace: &'static str,
    chain_id: &'static str,
    source_family: &'static str,
    contract_instance_id: &'static str,
    address: &'static str,
    manifest_role: &'static str,
}

struct HistoricalReplayRetentionProbe {
    chain_id: &'static str,
    watched_address: &'static str,
    claimed_address: &'static str,
    empty_block_hash: &'static str,
    empty_block_number: i64,
    finalized_block_hash: &'static str,
    finalized_block_number: i64,
    safe_block_hash: &'static str,
    safe_block_number: i64,
    observed_block_hash: &'static str,
    observed_block_number: i64,
}

struct EnsRegistryOldMigrationReplayProbe {
    chain_id: &'static str,
    current_registry_address: &'static str,
    old_registry_address: &'static str,
    old_block_hash: &'static str,
    current_block_hash: &'static str,
}

pub(crate) async fn run_backfill_sources_auto_bootstrap() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let corpus = seed_replay_supported_read_corpus(&database).await?;
    let ensv1_logical_name_id = "ens:alice.eth";
    let ensv1_resource_id = Uuid::from_u128(0xf940);
    database
        .seed_exact_name_rebuild_inputs(
            ensv1_logical_name_id,
            ensv1_resource_id,
            Uuid::from_u128(0xf941),
            Uuid::from_u128(0xf942),
        )
        .await?;

    seed_auto_bootstrap_manifest_sources(&database).await?;
    let eligible_targets = load_auto_bootstrap_manifest_started_targets(&database).await?;
    let skipped_targets = load_auto_bootstrap_manifest_skipped_targets(&database).await?;
    assert_auto_bootstrap_manifest_targets_skip_unknown_start(&eligible_targets);
    assert_auto_bootstrap_ens_registry_targets_have_separate_current_and_old_starts(
        &eligible_targets,
    );
    assert_auto_bootstrap_unknown_start_reported_skipped(&skipped_targets);

    let before_jobs = snapshot_auto_bootstrap_existing_routes(&database, &corpus).await?;
    let completed_jobs =
        seed_completed_auto_bootstrap_backfill_jobs(&database, &eligible_targets).await?;
    assert_completed_auto_bootstrap_backfill_jobs(&completed_jobs);
    assert_auto_bootstrap_cap_is_not_full_history(&completed_jobs);
    assert_auto_bootstrap_unknown_start_absent_from_source_identity(&completed_jobs);
    let after_jobs_before_replay =
        snapshot_auto_bootstrap_existing_routes(&database, &corpus).await?;
    assert_eq!(
        after_jobs_before_replay, before_jobs,
        "completed automatic bootstrap backfill jobs must not mutate shipped route responses before replay"
    );

    replay_all_current_projections(&database).await?;

    let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
    assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
    assert_existing_ensv1_exact_name_after_jobs_and_replay(&database, ensv1_logical_name_id)
        .await?;
    assert_ensv2_shadow_exact_name_coverage_is_not_graduated(&after_replay);
    assert_ens_registry_old_admission_does_not_surface_consumer_coverage(&after_replay);
    assert_auto_bootstrap_api_coverage_is_not_graduated(&database, &corpus, &after_replay).await?;
    assert_api_coverage_is_not_graduated_by_old_registry(&database, &corpus, &after_replay).await?;
    assert_replay_collection_empty(
        &database,
        ReplayRoute {
            label: "auto-bootstrap-losing-address-names-after-replay",
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
            label: "auto-bootstrap-losing-address-history-after-replay",
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
            label: "auto-bootstrap-losing-resolver-after-replay",
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

    let before_jobs = snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
    let completed_jobs = seed_completed_source_family_backfill_jobs(&database).await?;
    assert_completed_source_family_backfill_jobs(&completed_jobs);
    let completed_dynamic_resolver_jobs =
        seed_completed_dynamic_resolver_backfill_jobs(&database).await?;
    assert_completed_dynamic_resolver_backfill_jobs(&completed_dynamic_resolver_jobs);
    let source_family_raw_retention_probe =
        seed_source_family_raw_retention_probe(&database, &completed_jobs).await?;
    let registry_old_replay_probe = seed_ens_registry_old_migration_replay_probe(&database).await?;
    let backfill_surface_before_raw_retention =
        snapshot_backfill_lifecycle_surface(&database).await?;
    let manifest_policy_before_raw_retention = snapshot_manifest_policy_surface(&database).await?;
    replay_raw_fact_normalized_events_for_blocks(
        &database,
        "mainnet",
        source_family_raw_retention_probe.chain_id,
        &[source_family_raw_retention_probe.block_hash],
    )
    .await?;
    assert_cache_first_raw_retention_replay_probe(&database, &source_family_raw_retention_probe)
        .await?;
    assert_source_family_raw_retention_probe_scoped(&database, &source_family_raw_retention_probe)
        .await?;
    replay_raw_fact_normalized_events_for_blocks(
        &database,
        "mainnet",
        registry_old_replay_probe.chain_id,
        &[
            registry_old_replay_probe.old_block_hash,
            registry_old_replay_probe.current_block_hash,
        ],
    )
    .await?;
    assert_ens_registry_old_migration_suppression_survived_replay(
        &database,
        &registry_old_replay_probe,
    )
    .await?;
    assert_eq!(
        snapshot_backfill_lifecycle_surface(&database).await?,
        backfill_surface_before_raw_retention,
        "source-family raw-retention and old-registry replay must not mutate completed backfill jobs or range checkpoints"
    );
    assert_eq!(
        snapshot_manifest_policy_surface(&database).await?,
        manifest_policy_before_raw_retention,
        "source-family raw-retention and old-registry replay must not change manifest rollout or capability policy state"
    );
    let after_jobs_before_replay =
        snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
    assert_eq!(
        after_jobs_before_replay, before_jobs,
        "completed source-family jobs, dynamic resolver jobs, cache-first raw-retention replay, and old-registry replay must not mutate shipped route responses before projection replay"
    );

    replay_all_current_projections(&database).await?;

    let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
    assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
    assert_existing_ensv1_exact_name_after_jobs_and_replay(&database, ensv1_logical_name_id)
        .await?;
    assert_ensv2_shadow_exact_name_coverage_is_not_graduated(&after_replay);
    assert_ens_registry_old_admission_does_not_surface_consumer_coverage(&after_replay);
    assert_api_coverage_is_not_graduated_by_old_registry(&database, &corpus, &after_replay).await?;
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

pub(crate) async fn run_backfill_sources_retention_and_replay_semantics() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let completed_jobs = seed_completed_source_family_backfill_jobs(&database).await?;
    assert_completed_source_family_backfill_jobs(&completed_jobs);
    assert_full_profile_started_history_is_covered(&completed_jobs, "mainnet");
    assert_full_profile_started_history_is_covered(&completed_jobs, "sepolia-dev");

    let replay_probe = seed_historical_replay_retention_probe(&database).await?;
    let backfill_surface_before_replay = snapshot_backfill_lifecycle_surface(&database).await?;
    let manifest_policy_before_replay = snapshot_manifest_policy_surface(&database).await?;
    replay_raw_fact_normalized_events_for_blocks(
        &database,
        "mainnet",
        replay_probe.chain_id,
        &[
            replay_probe.empty_block_hash,
            replay_probe.finalized_block_hash,
            replay_probe.safe_block_hash,
            replay_probe.observed_block_hash,
        ],
    )
    .await?;
    assert_empty_historical_block_retention(&database, &replay_probe).await?;
    assert_safe_finalized_historical_facts_replay_as_non_observed(&database, &replay_probe).await?;
    assert_eq!(
        snapshot_backfill_lifecycle_surface(&database).await?,
        backfill_surface_before_replay,
        "historical raw-fact replay must not mutate completed backfill jobs or range checkpoints"
    );
    assert_eq!(
        snapshot_manifest_policy_surface(&database).await?,
        manifest_policy_before_replay,
        "historical raw-fact replay must not change manifest rollout or capability policy state"
    );

    database.cleanup().await?;
    Ok(())
}

async fn seed_completed_auto_bootstrap_backfill_jobs(
    database: &HarnessDatabase,
    fixtures: &[AutoBootstrapBackfillFixture],
) -> Result<
    Vec<(
        AutoBootstrapBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )>,
> {
    let mut records = Vec::new();
    for fixture in fixtures {
        let record = seed_completed_auto_bootstrap_backfill_job(database, fixture).await?;
        records.push((fixture.clone(), record));
    }

    Ok(records)
}

async fn seed_completed_auto_bootstrap_backfill_job(
    database: &HarnessDatabase,
    fixture: &AutoBootstrapBackfillFixture,
) -> Result<bigname_storage::BackfillJobRecord> {
    let range_start_block_number = fixture.effective_from_block_number();
    let range_end_block_number = fixture.effective_to_block_number();
    let created = bigname_storage::create_backfill_job(
        &database.pool,
        &bigname_storage::BackfillJobCreate {
            deployment_profile: fixture.deployment_profile.to_owned(),
            chain_id: fixture.chain_id.to_owned(),
            source_identity: auto_bootstrap_identity(fixture),
            scan_mode: "hash_pinned_block".to_owned(),
            range_start_block_number,
            range_end_block_number,
            idempotency_key: format!(
                "conformance-auto-bootstrap:{}:{}:{}:{}-{}",
                fixture.deployment_profile,
                fixture.chain_id,
                fixture.contract_instance_id,
                range_start_block_number,
                range_end_block_number
            ),
            ranges: vec![bigname_storage::BackfillRangeSpec {
                range_start_block_number,
                range_end_block_number,
            }],
        },
    )
    .await
    .with_context(|| {
        format!(
            "failed to create automatic bootstrap backfill job for {} {}",
            fixture.source_family, fixture.contract_instance_id
        )
    })?;

    let range = created
        .ranges
        .first()
        .expect("automatic bootstrap conformance job should have one range");
    let lease_token = format!(
        "conformance-auto-bootstrap-{}-lease",
        fixture.contract_instance_id
    );
    let lease_expires_at =
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
            .context("failed to build automatic bootstrap conformance lease deadline")?;
    let reserved = bigname_storage::reserve_backfill_range(
        &database.pool,
        created.job.backfill_job_id,
        "conformance-auto-bootstrap-backfill",
        &lease_token,
        lease_expires_at,
    )
    .await
    .with_context(|| {
        format!(
            "failed to reserve automatic bootstrap range for {}",
            fixture.contract_instance_id
        )
    })?
    .with_context(|| {
        format!(
            "automatic bootstrap range should be reservable for {}",
            fixture.contract_instance_id
        )
    })?;
    anyhow::ensure!(
        reserved.backfill_range_id == range.backfill_range_id,
        "reserved unexpected automatic bootstrap range {} instead of {} for {}",
        reserved.backfill_range_id,
        range.backfill_range_id,
        fixture.contract_instance_id
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
            "failed to advance automatic bootstrap range for {}",
            fixture.contract_instance_id
        )
    })?;
    bigname_storage::complete_backfill_range(&database.pool, range.backfill_range_id, &lease_token)
        .await
        .with_context(|| {
            format!(
                "failed to complete automatic bootstrap range for {}",
                fixture.contract_instance_id
            )
        })?;

    let job = bigname_storage::load_backfill_job(&database.pool, created.job.backfill_job_id)
        .await
        .with_context(|| {
            format!(
                "failed to load completed automatic bootstrap job for {}",
                fixture.contract_instance_id
            )
        })?
        .with_context(|| {
            format!(
                "completed automatic bootstrap job must exist for {}",
                fixture.contract_instance_id
            )
        })?;
    let ranges = bigname_storage::load_backfill_ranges(&database.pool, created.job.backfill_job_id)
        .await
        .with_context(|| {
            format!(
                "failed to load completed automatic bootstrap ranges for {}",
                fixture.contract_instance_id
            )
        })?;

    Ok(bigname_storage::BackfillJobRecord { job, ranges })
}

async fn seed_completed_source_family_backfill_jobs(
    database: &HarnessDatabase,
) -> Result<
    Vec<(
        SourceFamilyBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )>,
> {
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
    let midpoint = fixture.range_start_block_number
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
            idempotency_key: format!("conformance-source-family-{}", fixture.source_family),
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
        let lease_expires_at =
            OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
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

    let job = bigname_storage::load_backfill_job(&database.pool, created.job.backfill_job_id)
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
    let ranges = bigname_storage::load_backfill_ranges(&database.pool, created.job.backfill_job_id)
        .await
        .with_context(|| {
            format!(
                "failed to load completed source-family backfill ranges for {}",
                fixture.source_family
            )
        })?;

    Ok(bigname_storage::BackfillJobRecord { job, ranges })
}

async fn seed_source_family_raw_retention_probe(
    database: &HarnessDatabase,
    completed_jobs: &[(
        SourceFamilyBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
) -> Result<RawRetentionProbe> {
    let fixture = completed_jobs
        .iter()
        .map(|(fixture, _)| fixture)
        .find(|fixture| {
            fixture.chain_id == "ethereum-mainnet"
                && fixture.source_family == RAW_REPLAY_PROBE_SOURCE_FAMILY
        })
        .context("source-family backfill fixtures must include ENSv1 reverse replay target")?;
    let target = fixture
        .selected_targets
        .first()
        .context("source-family raw-retention fixture must include a selected target")?;
    let manifest_id = database
        .insert_manifest(
            fixture.namespace,
            fixture.source_family,
            fixture.chain_id,
            "ens_v1",
            81,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let contract_instance_id = Uuid::parse_str(target.contract_instance_id).with_context(|| {
        format!(
            "source-family fixture {} contract_instance_id must parse as UUID",
            fixture.source_family
        )
    })?;
    seed_active_replay_contract(
        database,
        manifest_id,
        contract_instance_id,
        fixture.chain_id,
        RAW_REPLAY_PROBE_CONTRACT_ROLE,
        target.address,
    )
    .await?;

    let probe = RawRetentionProbe {
        chain_id: fixture.chain_id,
        block_hash: "0xbac1f11100000000000000000000000000000000000000000000000000000303",
        block_number: target.range_start_block_number,
        watched_address: target.address,
    };
    bigname_storage::upsert_chain_lineage_blocks(
        &database.pool,
        &[bigname_storage::ChainLineageBlock {
            chain_id: probe.chain_id.to_owned(),
            block_hash: probe.block_hash.to_owned(),
            parent_hash: Some(
                "0xbac1f11000000000000000000000000000000000000000000000000000000302".to_owned(),
            ),
            block_number: probe.block_number,
            block_timestamp: timestamp(1_717_194_303),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await
    .context("failed to seed source-family raw-retention chain lineage")?;
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            probe.chain_id,
            probe.block_hash,
            Some("0xbac1f11000000000000000000000000000000000000000000000000000000302"),
            probe.block_number,
            1_717_194_303,
        )],
    )
    .await
    .context("failed to seed source-family raw-retention raw block")?;
    bigname_storage::upsert_raw_logs(
        &database.pool,
        &[bigname_storage::RawLog {
            chain_id: probe.chain_id.to_owned(),
            block_hash: probe.block_hash.to_owned(),
            block_number: probe.block_number,
            transaction_hash: "0xbac1f1feedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed303"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: probe.watched_address.to_ascii_lowercase(),
            topics: vec![
                RAW_REPLAY_PROBE_REVERSE_CLAIMED_TOPIC0.to_owned(),
                RAW_REPLAY_PROBE_CLAIMED_ADDRESS_TOPIC.to_owned(),
                RAW_REPLAY_PROBE_REVERSE_NODE_TOPIC.to_owned(),
            ],
            data: Vec::new(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await
    .context("failed to seed source-family raw-retention selected raw log")?;
    seed_raw_retention_cache_metadata(database, &probe, "source-family-backfill").await?;

    Ok(probe)
}

async fn assert_source_family_raw_retention_probe_scoped(
    database: &HarnessDatabase,
    probe: &RawRetentionProbe,
) -> Result<()> {
    let unselected_log_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = $2
          AND emitting_address <> $3
        "#,
    )
    .bind(probe.chain_id)
    .bind(probe.block_hash)
    .bind(probe.watched_address)
    .fetch_one(&database.pool)
    .await
    .context("failed to count source-family raw-retention unselected raw logs")?;
    assert_eq!(
        unselected_log_count, 0,
        "source-scoped admission must not retain unselected raw logs for the cache-first probe"
    );

    let selected_job_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM backfill_jobs
        WHERE source_identity->>'selector_kind' = 'source_family'
          AND source_identity->>'source_family' = $1
          AND source_identity->'selected_targets' @> $2::JSONB
        "#,
    )
    .bind(RAW_REPLAY_PROBE_SOURCE_FAMILY)
    .bind(
        json!([{
            "source_family": RAW_REPLAY_PROBE_SOURCE_FAMILY,
            "address": probe.watched_address,
        }])
        .to_string(),
    )
    .fetch_one(&database.pool)
    .await
    .context("failed to count source-family raw-retention selected backfill jobs")?;
    assert_eq!(
        selected_job_count, 1,
        "cache-first source-family probe must stay scoped to the selected source identity"
    );

    Ok(())
}

async fn seed_historical_replay_retention_probe(
    database: &HarnessDatabase,
) -> Result<HistoricalReplayRetentionProbe> {
    let probe = HistoricalReplayRetentionProbe {
        chain_id: "ethereum-mainnet",
        watched_address: RAW_RETENTION_REPLAY_WATCHED_ADDRESS,
        claimed_address: RAW_REPLAY_PROBE_CLAIMED_ADDRESS,
        empty_block_hash: "0xe500000000000000000000000000000000000000000000000000000000000500",
        empty_block_number: 500,
        finalized_block_hash: "0xf501000000000000000000000000000000000000000000000000000000000501",
        finalized_block_number: 501,
        safe_block_hash: "0x5a02000000000000000000000000000000000000000000000000000000000502",
        safe_block_number: 502,
        observed_block_hash: "0x0b03000000000000000000000000000000000000000000000000000000000503",
        observed_block_number: 503,
    };
    let manifest_id = database
        .insert_manifest(
            "ens",
            RAW_REPLAY_PROBE_SOURCE_FAMILY,
            probe.chain_id,
            "ens_v1",
            91,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    seed_active_replay_contract(
        database,
        manifest_id,
        Uuid::from_u128(0xbac500),
        probe.chain_id,
        RAW_REPLAY_PROBE_CONTRACT_ROLE,
        probe.watched_address,
    )
    .await?;
    seed_historical_replay_block(
        database,
        probe.chain_id,
        probe.empty_block_hash,
        probe.empty_block_number,
        CanonicalityState::Finalized,
        false,
        probe.watched_address,
    )
    .await?;
    seed_historical_replay_block(
        database,
        probe.chain_id,
        probe.finalized_block_hash,
        probe.finalized_block_number,
        CanonicalityState::Finalized,
        true,
        probe.watched_address,
    )
    .await?;
    seed_historical_replay_block(
        database,
        probe.chain_id,
        probe.safe_block_hash,
        probe.safe_block_number,
        CanonicalityState::Safe,
        true,
        probe.watched_address,
    )
    .await?;
    seed_historical_replay_block(
        database,
        probe.chain_id,
        probe.observed_block_hash,
        probe.observed_block_number,
        CanonicalityState::Observed,
        true,
        probe.watched_address,
    )
    .await?;

    Ok(probe)
}

async fn seed_historical_replay_block(
    database: &HarnessDatabase,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
    include_selected_log: bool,
    watched_address: &str,
) -> Result<()> {
    let parent_hash = "0xfeed000000000000000000000000000000000000000000000000000000000500";
    bigname_storage::upsert_chain_lineage_blocks(
        &database.pool,
        &[bigname_storage::ChainLineageBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(parent_hash.to_owned()),
            block_number,
            block_timestamp: timestamp(1_717_195_000 + block_number),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state,
        }],
    )
    .await
    .with_context(|| format!("failed to seed historical replay lineage for block {block_hash}"))?;

    let mut block = raw_block(
        chain_id,
        block_hash,
        Some(parent_hash),
        block_number,
        1_717_195_000 + block_number,
    );
    block.canonicality_state = canonicality_state;
    bigname_storage::upsert_raw_blocks(&database.pool, &[block])
        .await
        .with_context(|| format!("failed to seed historical replay raw block {block_hash}"))?;

    if include_selected_log {
        bigname_storage::upsert_raw_logs(
            &database.pool,
            &[bigname_storage::RawLog {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                transaction_hash: format!("0x{block_number:064x}"),
                transaction_index: 0,
                log_index: 0,
                emitting_address: watched_address.to_ascii_lowercase(),
                topics: vec![
                    RAW_REPLAY_PROBE_REVERSE_CLAIMED_TOPIC0.to_owned(),
                    RAW_REPLAY_PROBE_CLAIMED_ADDRESS_TOPIC.to_owned(),
                    RAW_REPLAY_PROBE_REVERSE_NODE_TOPIC.to_owned(),
                ],
                data: Vec::new(),
                canonicality_state,
            }],
        )
        .await
        .with_context(|| {
            format!("failed to seed historical replay selected raw log for block {block_hash}")
        })?;
    }

    Ok(())
}

async fn seed_ens_registry_old_migration_replay_probe(
    database: &HarnessDatabase,
) -> Result<EnsRegistryOldMigrationReplayProbe> {
    let probe = EnsRegistryOldMigrationReplayProbe {
        chain_id: "ethereum-mainnet",
        current_registry_address: ENS_V1_REGISTRY_CURRENT_ADDRESS,
        old_registry_address: ENS_V1_REGISTRY_OLD_ADDRESS,
        old_block_hash: "0xe1501d0000000000000000000000000000000000000000000000000000000041",
        current_block_hash: "0xe1501d0000000000000000000000000000000000000000000000000000000043",
    };
    let manifest_id = database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            probe.chain_id,
            "ens_v1",
            106,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let current_contract_instance_id =
        Uuid::parse_str(ENS_V1_REGISTRY_CURRENT_CONTRACT_INSTANCE_ID)
            .context("current registry replay contract ID must parse")?;
    let old_contract_instance_id = Uuid::parse_str(ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID)
        .context("old registry replay contract ID must parse")?;
    seed_active_replay_contract(
        database,
        manifest_id,
        current_contract_instance_id,
        probe.chain_id,
        "registry",
        probe.current_registry_address,
    )
    .await?;
    seed_active_replay_contract(
        database,
        manifest_id,
        old_contract_instance_id,
        probe.chain_id,
        "registry_old",
        probe.old_registry_address,
    )
    .await?;
    seed_ens_registry_old_replay_root_and_discovery_rule(
        database,
        manifest_id,
        current_contract_instance_id,
        probe.current_registry_address,
    )
    .await?;

    bigname_storage::upsert_chain_lineage_blocks(
        &database.pool,
        &[
            bigname_storage::ChainLineageBlock {
                chain_id: probe.chain_id.to_owned(),
                block_hash: probe.old_block_hash.to_owned(),
                parent_hash: Some(
                    "0xe1501c0000000000000000000000000000000000000000000000000000000040".to_owned(),
                ),
                block_number: 41,
                block_timestamp: timestamp(1_717_196_041),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
            bigname_storage::ChainLineageBlock {
                chain_id: probe.chain_id.to_owned(),
                block_hash: probe.current_block_hash.to_owned(),
                parent_hash: Some(probe.old_block_hash.to_owned()),
                block_number: 43,
                block_timestamp: timestamp(1_717_196_043),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await
    .context("failed to seed ENSRegistryOld replay lineage")?;
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                probe.chain_id,
                probe.old_block_hash,
                Some("0xe1501c0000000000000000000000000000000000000000000000000000000040"),
                41,
                1_717_196_041,
            ),
            raw_block(
                probe.chain_id,
                probe.current_block_hash,
                Some(probe.old_block_hash),
                43,
                1_717_196_043,
            ),
        ],
    )
    .await
    .context("failed to seed ENSRegistryOld replay raw blocks")?;
    bigname_storage::upsert_raw_logs(
        &database.pool,
        &[
            ens_registry_old_replay_new_owner_log(
                probe.chain_id,
                probe.old_block_hash,
                41,
                "0xe1501dfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed41",
                0,
                probe.old_registry_address,
                "0x0000000000000000000000000000000000000001",
            )?,
            ens_registry_old_replay_new_owner_log(
                probe.chain_id,
                probe.current_block_hash,
                43,
                "0xe1501dfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed43",
                0,
                probe.current_registry_address,
                ENS_V1_REGISTRY_OLD_REPLAY_CURRENT_OWNER,
            )?,
            ens_registry_old_replay_new_owner_log(
                probe.chain_id,
                probe.current_block_hash,
                43,
                "0xe1501dfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed43",
                1,
                probe.old_registry_address,
                "0x0000000000000000000000000000000000000003",
            )?,
        ],
    )
    .await
    .context("failed to seed ENSRegistryOld replay raw logs")?;

    Ok(probe)
}

async fn seed_ens_registry_old_replay_root_and_discovery_rule(
    database: &HarnessDatabase,
    manifest_id: i64,
    current_contract_instance_id: Uuid,
    current_registry_address: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address
        )
        VALUES ($1, 'root', 'ENSRegistry', $2, lower($3))
        "#,
    )
    .bind(manifest_id)
    .bind(current_contract_instance_id)
    .bind(current_registry_address)
    .execute(&database.pool)
    .await
    .context("failed to seed ENSRegistryOld replay root contract")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_discovery_rules (manifest_id, edge_kind, from_role, admission)
        VALUES ($1, 'subregistry', 'registry', 'reachable_from_root')
        "#,
    )
    .bind(manifest_id)
    .execute(&database.pool)
    .await
    .context("failed to seed ENSRegistryOld replay subregistry discovery rule")?;

    Ok(())
}

fn ens_registry_old_replay_new_owner_log(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    transaction_hash: &str,
    log_index: i64,
    emitting_address: &str,
    owner: &str,
) -> Result<bigname_storage::RawLog> {
    Ok(bigname_storage::RawLog {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: transaction_hash.to_owned(),
        transaction_index: 0,
        log_index,
        emitting_address: emitting_address.to_ascii_lowercase(),
        topics: vec![
            ENS_V1_REGISTRY_OLD_REPLAY_NEW_OWNER_TOPIC0.to_owned(),
            ENS_V1_REGISTRY_OLD_REPLAY_PARENT_NODE.to_owned(),
            ENS_V1_REGISTRY_OLD_REPLAY_LABELHASH.to_owned(),
        ],
        data: abi_word_address_bytes(owner)?,
        canonicality_state: CanonicalityState::Canonical,
    })
}

fn abi_word_address_bytes(address: &str) -> Result<Vec<u8>> {
    let address = address
        .strip_prefix("0x")
        .context("address must start with 0x")?;
    anyhow::ensure!(address.len() == 40, "address must be 20 bytes");
    let mut word = vec![0_u8; 32];
    for (index, chunk) in address.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("address hex chunk must be utf-8")?;
        word[12 + index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid address byte {hex}"))?;
    }
    Ok(word)
}

async fn assert_ens_registry_old_migration_suppression_survived_replay(
    database: &HarnessDatabase,
    probe: &EnsRegistryOldMigrationReplayProbe,
) -> Result<()> {
    let raw_log_emitters = sqlx::query_scalar::<_, Vec<String>>(
        r#"
        SELECT COALESCE(
            ARRAY_AGG(emitting_address ORDER BY block_number, log_index),
            ARRAY[]::TEXT[]
        )
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(probe.chain_id)
    .bind(vec![
        probe.old_block_hash.to_owned(),
        probe.current_block_hash.to_owned(),
    ])
    .fetch_one(&database.pool)
    .await
    .context("failed to load ENSRegistryOld replay raw emitters")?;
    assert_eq!(
        raw_log_emitters,
        vec![
            probe.old_registry_address.to_owned(),
            probe.current_registry_address.to_owned(),
            probe.old_registry_address.to_owned(),
        ],
        "canonical replay must retain current and old registry raw facts as selected audit inputs"
    );

    let normalized_event_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND event_kind = 'SubregistryChanged'
          AND derivation_kind = 'ens_v1_subregistry_changed'
        "#,
    )
    .bind(probe.chain_id)
    .bind(vec![
        probe.old_block_hash.to_owned(),
        probe.current_block_hash.to_owned(),
    ])
    .fetch_one(&database.pool)
    .await
    .context("failed to load ENSRegistryOld replay normalized events")?;
    assert_eq!(
        normalized_event_count, 0,
        "raw replay must not emit SubregistryChanged without a pre-existing discovery edge"
    );

    let current_contract_instance_id =
        Uuid::parse_str(ENS_V1_REGISTRY_CURRENT_CONTRACT_INSTANCE_ID)
            .context("current registry replay contract ID must parse")?;
    let old_contract_instance_id = Uuid::parse_str(ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID)
        .context("old registry replay contract ID must parse")?;
    let replay_edge_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM discovery_edges
        WHERE chain_id = $1
          AND (
              from_contract_instance_id = ANY($2::UUID[])
              OR to_contract_instance_id = ANY($2::UUID[])
          )
        "#,
    )
    .bind(probe.chain_id)
    .bind(vec![current_contract_instance_id, old_contract_instance_id])
    .fetch_one(&database.pool)
    .await
    .context("failed to count ENSRegistryOld replay discovery edges")?;
    assert_eq!(
        replay_edge_count, 0,
        "raw replay must not synthesize discovery edges for ENSRegistryOld audit facts"
    );

    Ok(())
}

async fn assert_empty_historical_block_retention(
    database: &HarnessDatabase,
    probe: &HistoricalReplayRetentionProbe,
) -> Result<()> {
    assert_eq!(
        block_scoped_table_count(
            database,
            "chain_lineage",
            probe.chain_id,
            probe.empty_block_hash
        )
        .await?,
        1,
        "empty historical block must retain its lineage/header anchor"
    );
    assert_eq!(
        block_scoped_table_count(
            database,
            "chain_header_audit",
            probe.chain_id,
            probe.empty_block_hash
        )
        .await?,
        0,
        "empty historical block without audit fields must not require a header-audit row"
    );
    for table in [
        "raw_logs",
        "raw_transactions",
        "raw_receipts",
        "raw_payload_cache_metadata",
        "normalized_events",
    ] {
        assert_eq!(
            block_scoped_table_count(database, table, probe.chain_id, probe.empty_block_hash)
                .await?,
            0,
            "empty historical block must not retain {table} rows by default"
        );
    }

    let selected_replay_event_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND event_kind = 'ReverseChanged'
          AND after_state->>'address' = $3
        "#,
    )
    .bind(probe.chain_id)
    .bind(vec![
        probe.finalized_block_hash.to_owned(),
        probe.safe_block_hash.to_owned(),
    ])
    .bind(probe.claimed_address)
    .fetch_one(&database.pool)
    .await
    .context("failed to count selected historical replay normalized events")?;
    assert_eq!(
        selected_replay_event_count, 2,
        "selected safe/finalized raw facts must remain replayable without empty-block payload retention"
    );

    Ok(())
}

async fn assert_safe_finalized_historical_facts_replay_as_non_observed(
    database: &HarnessDatabase,
    probe: &HistoricalReplayRetentionProbe,
) -> Result<()> {
    let replayed_states = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT block_number, canonicality_state::TEXT
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND event_kind = 'ReverseChanged'
        ORDER BY block_number
        "#,
    )
    .bind(probe.chain_id)
    .bind(vec![
        probe.finalized_block_hash.to_owned(),
        probe.safe_block_hash.to_owned(),
    ])
    .fetch_all(&database.pool)
    .await
    .context("failed to load safe/finalized historical replay states")?;
    assert_eq!(
        replayed_states,
        vec![
            (probe.finalized_block_number, "finalized".to_owned()),
            (probe.safe_block_number, "safe".to_owned()),
        ],
        "safe/finalized historical raw facts must replay as non-observed normalized events"
    );

    let observed_event_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE chain_id = $1
          AND (
              block_hash = $2
              OR canonicality_state = 'observed'::canonicality_state
          )
          AND source_family = $3
        "#,
    )
    .bind(probe.chain_id)
    .bind(probe.observed_block_hash)
    .bind(RAW_REPLAY_PROBE_SOURCE_FAMILY)
    .fetch_one(&database.pool)
    .await
    .context("failed to count observed historical replay events")?;
    assert_eq!(
        observed_event_count, 0,
        "observed historical raw facts must not enter canonical-only replay or projection inputs"
    );

    database
        .rebuild_primary_names_current(probe.claimed_address, "ens", "60")
        .await?;
    let primary_name = bigname_storage::load_primary_name_current(
        &database.pool,
        probe.claimed_address,
        "ens",
        "60",
    )
    .await?
    .context("safe/finalized reverse-claim replay should rebuild the primary-name tuple")?;
    assert_eq!(primary_name.claim_status, PrimaryNameClaimStatus::NotFound);
    assert_eq!(primary_name.address, probe.claimed_address);
    assert_eq!(primary_name.namespace, "ens");
    assert_eq!(primary_name.coin_type, "60");
    assert_eq!(
        primary_name
            .claim_provenance
            .get("verified_primary_name_lookup")
            .and_then(|lookup| lookup.get("address"))
            .and_then(Value::as_str),
        Some(probe.claimed_address),
        "canonical-only projection rebuild should be driven by replayed safe/finalized facts"
    );

    Ok(())
}

async fn block_scoped_table_count(
    database: &HarnessDatabase,
    table: &str,
    chain_id: &str,
    block_hash: &str,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COUNT(*)::BIGINT FROM {table} WHERE chain_id = $1 AND block_hash = $2"
    ))
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(&database.pool)
    .await
    .with_context(|| format!("failed to count {table} rows for block {block_hash}"))
}

async fn snapshot_backfill_lifecycle_surface(
    database: &HarnessDatabase,
) -> Result<Vec<(i64, String, String, String, String, i64, String, i64)>> {
    sqlx::query_as::<_, (i64, String, String, String, String, i64, String, i64)>(
        r#"
        SELECT
            jobs.backfill_job_id,
            jobs.status::TEXT AS job_status,
            jobs.deployment_profile,
            jobs.chain_id,
            jobs.source_identity::TEXT AS source_identity,
            ranges.backfill_range_id,
            ranges.status::TEXT AS range_status,
            ranges.checkpoint_block_number
        FROM backfill_jobs AS jobs
        JOIN backfill_ranges AS ranges
          ON ranges.backfill_job_id = jobs.backfill_job_id
        ORDER BY jobs.backfill_job_id, ranges.backfill_range_id
        "#,
    )
    .fetch_all(&database.pool)
    .await
    .context("failed to snapshot backfill lifecycle surface")
}

async fn seed_completed_dynamic_resolver_backfill_jobs(
    database: &HarnessDatabase,
) -> Result<
    Vec<(
        DynamicResolverBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )>,
> {
    let mut records = Vec::new();
    for fixture in dynamic_resolver_backfill_fixtures() {
        let record = seed_completed_dynamic_resolver_backfill_job(database, &fixture).await?;
        records.push((fixture, record));
    }

    Ok(records)
}

async fn seed_completed_dynamic_resolver_backfill_job(
    database: &HarnessDatabase,
    fixture: &DynamicResolverBackfillFixture,
) -> Result<bigname_storage::BackfillJobRecord> {
    let midpoint = fixture.range_start_block_number
        + ((fixture.range_end_block_number - fixture.range_start_block_number) / 2);
    let created = bigname_storage::create_backfill_job(
        &database.pool,
        &bigname_storage::BackfillJobCreate {
            deployment_profile: fixture.deployment_profile.to_owned(),
            chain_id: fixture.chain_id.to_owned(),
            source_identity: dynamic_resolver_identity(fixture),
            scan_mode: "hash_pinned_block".to_owned(),
            range_start_block_number: fixture.range_start_block_number,
            range_end_block_number: fixture.range_end_block_number,
            idempotency_key: fixture.idempotency_key.to_owned(),
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
            "failed to create dynamic resolver backfill job for {}",
            fixture.source_family
        )
    })?;

    for (index, range) in created.ranges.iter().enumerate() {
        let lease_token = format!(
            "conformance-dynamic-resolver-{}-lease-{index}",
            fixture.source_family
        );
        let lease_expires_at =
            OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
                .context("failed to build dynamic resolver conformance backfill lease deadline")?;
        let reserved = bigname_storage::reserve_backfill_range(
            &database.pool,
            created.job.backfill_job_id,
            "conformance-dynamic-resolver-backfill",
            &lease_token,
            lease_expires_at,
        )
        .await
        .with_context(|| {
            format!(
                "failed to reserve dynamic resolver backfill range for {}",
                fixture.source_family
            )
        })?
        .with_context(|| {
            format!(
                "dynamic resolver backfill range should be reservable for {}",
                fixture.source_family
            )
        })?;
        anyhow::ensure!(
            reserved.backfill_range_id == range.backfill_range_id,
            "reserved unexpected dynamic resolver backfill range {} instead of {} for {}",
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
                "failed to advance dynamic resolver backfill range for {}",
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
                "failed to complete dynamic resolver backfill range for {}",
                fixture.source_family
            )
        })?;
    }

    let job = bigname_storage::load_backfill_job(&database.pool, created.job.backfill_job_id)
        .await
        .with_context(|| {
            format!(
                "failed to load completed dynamic resolver backfill job for {}",
                fixture.source_family
            )
        })?
        .with_context(|| {
            format!(
                "completed dynamic resolver backfill job must exist for {}",
                fixture.source_family
            )
        })?;
    let ranges = bigname_storage::load_backfill_ranges(&database.pool, created.job.backfill_job_id)
        .await
        .with_context(|| {
            format!(
                "failed to load completed dynamic resolver backfill ranges for {}",
                fixture.source_family
            )
        })?;

    Ok(bigname_storage::BackfillJobRecord { job, ranges })
}

fn assert_completed_auto_bootstrap_backfill_jobs(
    completed_jobs: &[(
        AutoBootstrapBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
) {
    let covered_targets = completed_jobs
        .iter()
        .map(|(fixture, _)| (fixture.source_family, fixture.contract_instance_id))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        covered_targets,
        auto_bootstrap_manifest_started_fixtures()
            .into_iter()
            .map(|fixture| (fixture.source_family, fixture.contract_instance_id))
            .collect::<BTreeSet<_>>()
    );

    for (fixture, completed_job) in completed_jobs {
        let expected_source_identity_hash =
            stable_source_identity_hash(&auto_bootstrap_identity_payload_without_hash(fixture));
        assert_eq!(
            completed_job.job.status,
            bigname_storage::BackfillLifecycleStatus::Completed,
            "{} automatic bootstrap job must be completed",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.deployment_profile,
            fixture.deployment_profile
        );
        assert_eq!(completed_job.job.chain_id, fixture.chain_id);
        assert_eq!(completed_job.job.scan_mode, "hash_pinned_block");
        assert_eq!(
            completed_job.job.range_start_block_number,
            fixture.effective_from_block_number()
        );
        assert_eq!(
            completed_job.job.range_end_block_number,
            fixture.effective_to_block_number()
        );
        assert_eq!(
            completed_job
                .job
                .source_identity
                .get("selector_kind")
                .and_then(Value::as_str),
            Some("watched_target_set")
        );
        assert_eq!(
            completed_job.job.source_identity.get("source_family"),
            Some(&Value::Null),
            "{} automatic bootstrap source identity must not collapse to a source-family selector",
            fixture.source_family
        );
        assert_eq!(
            completed_job
                .job
                .source_identity
                .get("source_identity_hash")
                .and_then(Value::as_str),
            Some(expected_source_identity_hash.as_str()),
            "{} automatic bootstrap job hash must cover the selected target lock",
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

        let requested_targets = completed_job
            .job
            .source_identity
            .get("requested_watched_targets")
            .and_then(Value::as_array)
            .expect("automatic bootstrap job should persist requested targets");
        assert_eq!(requested_targets.len(), 1);
        assert_eq!(
            requested_targets[0]
                .get("contract_instance_id")
                .and_then(Value::as_str),
            Some(fixture.contract_instance_id)
        );

        let selected_targets = completed_job
            .job
            .source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .expect("automatic bootstrap job should persist selected targets");
        assert_eq!(selected_targets.len(), 1);
        let selected_target = &selected_targets[0];
        assert_eq!(
            selected_target.get("source_family").and_then(Value::as_str),
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
            "{} automatic bootstrap contract_instance_id should be UUID-shaped",
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
            Some(fixture.effective_from_block_number())
        );
        assert_eq!(
            selected_target
                .get("effective_to_block")
                .and_then(Value::as_i64),
            Some(fixture.effective_to_block_number())
        );
        assert!(
            completed_job.job.completed_at.is_some(),
            "{} automatic bootstrap job must record completion time",
            fixture.source_family
        );
        assert_eq!(completed_job.ranges.len(), 1);
        assert!(
            completed_job.ranges.iter().all(|range| {
                range.status == bigname_storage::BackfillLifecycleStatus::Completed
                    && range.range_start_block_number == fixture.effective_from_block_number()
                    && range.range_end_block_number == fixture.effective_to_block_number()
                    && range.checkpoint_block_number == range.range_end_block_number
                    && range.completed_at.is_some()
            }),
            "{} automatic bootstrap job must persist one completed finite child range",
            fixture.source_family
        );
    }
}

fn assert_auto_bootstrap_cap_is_not_full_history(
    completed_jobs: &[(
        AutoBootstrapBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
) {
    let capped_jobs = completed_jobs
        .iter()
        .filter(|(fixture, _)| fixture.bootstrap_backfill_max_blocks.is_some())
        .collect::<Vec<_>>();
    assert!(
        !capped_jobs.is_empty(),
        "automatic bootstrap conformance must include a capped startup range"
    );

    for (fixture, completed_job) in capped_jobs {
        let admitted_start = fixture.admitted_history_start_block_number();
        let bootstrap_start = fixture.effective_from_block_number();
        assert!(
            bootstrap_start > admitted_start,
            "{} capped bootstrap job must start after admitted history start to prove partiality",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.range_start_block_number, bootstrap_start,
            "{} capped bootstrap job must persist the capped finite range start",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.range_end_block_number,
            fixture.effective_to_block_number(),
            "{} capped bootstrap job must persist the finite provider-head range end",
            fixture.source_family
        );

        let selected_targets = completed_job
            .job
            .source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .expect("capped automatic bootstrap job should persist selected targets");
        assert_eq!(selected_targets.len(), 1);
        let selected_target = &selected_targets[0];
        assert_eq!(
            selected_target
                .get("effective_from_block")
                .and_then(Value::as_i64),
            Some(bootstrap_start),
            "{} capped bootstrap selected target must use the capped range start",
            fixture.source_family
        );
        assert_ne!(
            selected_target
                .get("effective_from_block")
                .and_then(Value::as_i64),
            Some(admitted_start),
            "{} capped bootstrap source identity must not masquerade as full admitted history",
            fixture.source_family
        );

        let source_identity = serde_json::to_string(&completed_job.job.source_identity)
            .expect("automatic bootstrap source identity should serialize");
        for forbidden in [
            "full_historical_completeness",
            "historical_completeness",
            "consumer_replacement",
            "route_coverage",
            "full_history",
        ] {
            assert!(
                !source_identity.contains(forbidden),
                "capped bootstrap source identity must not carry full-history marker {forbidden}: {source_identity}"
            );
        }
    }
}

fn assert_completed_source_family_backfill_jobs(
    completed_jobs: &[(
        SourceFamilyBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
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
            "ens_v1_wrapper_l1",
            "ens_v1_resolver_l1",
            "ens_v2_root_l1",
            "ens_v2_registry_l1",
            "ens_v2_registrar_l1",
            "ens_v2_resolver_l1",
            "basenames_base_registry",
            "basenames_base_registrar",
            "basenames_base_resolver",
            "basenames_base_primary",
            "basenames_l1_compat",
            "basenames_execution",
        ])
    );

    for (fixture, completed_job) in completed_jobs {
        let expected_source_identity_hash =
            stable_source_identity_hash(&source_family_identity_payload_without_hash(fixture));
        assert_eq!(
            completed_job.job.status,
            bigname_storage::BackfillLifecycleStatus::Completed,
            "{} source-family job must be completed",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.deployment_profile,
            fixture.deployment_profile
        );
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
        assert_eq!(selected_targets.len(), fixture.selected_targets.len());
        let actual_targets = selected_targets
            .iter()
            .map(|selected_target| {
                (
                    selected_target
                        .get("source_family")
                        .and_then(Value::as_str)
                        .expect("selected target should include source_family"),
                    selected_target
                        .get("contract_instance_id")
                        .and_then(Value::as_str)
                        .expect("selected target should include contract_instance_id"),
                    selected_target
                        .get("address")
                        .and_then(Value::as_str)
                        .expect("selected target should include address"),
                    selected_target
                        .get("effective_from_block")
                        .and_then(Value::as_i64)
                        .expect("selected target should include effective_from_block"),
                    selected_target
                        .get("effective_to_block")
                        .and_then(Value::as_i64)
                        .expect("selected target should include effective_to_block"),
                )
            })
            .collect::<BTreeSet<_>>();
        let expected_targets = fixture
            .selected_targets
            .iter()
            .map(|target| {
                assert!(
                    Uuid::parse_str(target.contract_instance_id).is_ok(),
                    "{} fixture contract_instance_id should be UUID-shaped",
                    fixture.source_family
                );
                assert_ne!(
                    target.contract_instance_id, fixture.source_family,
                    "{} fixture contract_instance_id must not collapse to source_family",
                    fixture.source_family
                );
                (
                    fixture.source_family,
                    target.contract_instance_id,
                    target.address,
                    target.range_start_block_number,
                    target.range_end_block_number,
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual_targets, expected_targets,
            "{} source-family selected targets must match the fixture lock",
            fixture.source_family
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
    assert_ens_v1_registry_source_family_selected_targets(completed_jobs);
}

fn assert_full_profile_started_history_is_covered(
    completed_jobs: &[(
        SourceFamilyBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
    deployment_profile: &str,
) {
    let expected_targets = source_family_backfill_fixtures()
        .into_iter()
        .filter(|fixture| fixture.deployment_profile == deployment_profile)
        .flat_map(|fixture| {
            fixture
                .selected_targets
                .iter()
                .map(|target| {
                    (
                        fixture.chain_id,
                        fixture.source_family,
                        target.contract_instance_id,
                        target.address,
                        target.range_start_block_number,
                        target.range_end_block_number,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    let actual_targets = completed_jobs
        .iter()
        .filter(|(fixture, _)| fixture.deployment_profile == deployment_profile)
        .flat_map(|(fixture, completed_job)| {
            let selected_targets = completed_job
                .job
                .source_identity
                .get("selected_targets")
                .and_then(Value::as_array)
                .expect("completed source-family job should persist selected targets");
            assert_eq!(
                selected_targets.len(),
                fixture.selected_targets.len(),
                "{} full-history source-family job should retain its selected target lock",
                fixture.source_family
            );
            selected_targets
                .iter()
                .map(|selected_target| {
                    (
                        completed_job.job.chain_id.as_str(),
                        selected_target
                            .get("source_family")
                            .and_then(Value::as_str)
                            .expect("selected target should include source_family"),
                        selected_target
                            .get("contract_instance_id")
                            .and_then(Value::as_str)
                            .expect("selected target should include contract_instance_id"),
                        selected_target
                            .get("address")
                            .and_then(Value::as_str)
                            .expect("selected target should include address"),
                        selected_target
                            .get("effective_from_block")
                            .and_then(Value::as_i64)
                            .expect("selected target should include effective_from_block"),
                        selected_target
                            .get("effective_to_block")
                            .and_then(Value::as_i64)
                            .expect("selected target should include effective_to_block"),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_targets, expected_targets,
        "{deployment_profile} completed source-family jobs must cover the whole admitted started history for every selected target"
    );

    for (fixture, completed_job) in completed_jobs
        .iter()
        .filter(|(fixture, _)| fixture.deployment_profile == deployment_profile)
    {
        assert_eq!(
            completed_job.job.range_start_block_number, fixture.range_start_block_number,
            "{} full-history job must start at admitted history",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.range_end_block_number, fixture.range_end_block_number,
            "{} full-history job must end at the selected finite history boundary",
            fixture.source_family
        );
        let mut ranges = completed_job
            .ranges
            .iter()
            .map(|range| {
                (
                    range.range_start_block_number,
                    range.range_end_block_number,
                    range.checkpoint_block_number,
                    range.status,
                )
            })
            .collect::<Vec<_>>();
        ranges.sort_by_key(|(start, end, _, _)| (*start, *end));
        assert_eq!(
            ranges.first().map(|(start, _, _, _)| *start),
            Some(fixture.range_start_block_number),
            "{} full-history ranges must start at admitted history",
            fixture.source_family
        );
        assert_eq!(
            ranges.last().map(|(_, end, _, _)| *end),
            Some(fixture.range_end_block_number),
            "{} full-history ranges must end at the selected finite history boundary",
            fixture.source_family
        );
        assert!(
            ranges
                .windows(2)
                .all(|window| window[0].1 + 1 == window[1].0),
            "{} full-history ranges must be contiguous without gaps",
            fixture.source_family
        );
        assert!(
            ranges.iter().all(|(_, end, checkpoint, status)| {
                *status == bigname_storage::BackfillLifecycleStatus::Completed && checkpoint == end
            }),
            "{} full-history ranges must all complete at their declared ends",
            fixture.source_family
        );
    }
}

fn assert_ens_v1_registry_source_family_selected_targets(
    completed_jobs: &[(
        SourceFamilyBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
) {
    let registry_job = completed_jobs
        .iter()
        .find(|(fixture, _)| fixture.source_family == "ens_v1_registry_l1")
        .expect("source-family conformance must include ENSv1 registry");
    let selected_targets = registry_job
        .1
        .job
        .source_identity
        .get("selected_targets")
        .and_then(Value::as_array)
        .expect("ENSv1 registry source identity must include selected targets");
    assert_eq!(
        selected_targets.len(),
        2,
        "ENSv1 registry source-family conformance must select current and old registry targets"
    );
    let current = selected_targets
        .iter()
        .find(|target| {
            target.get("contract_instance_id").and_then(Value::as_str)
                == Some(ENS_V1_REGISTRY_CURRENT_CONTRACT_INSTANCE_ID)
        })
        .expect("ENSv1 registry source identity must include current registry target");
    let old = selected_targets
        .iter()
        .find(|target| {
            target.get("contract_instance_id").and_then(Value::as_str)
                == Some(ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID)
        })
        .expect("ENSv1 registry source identity must include old registry target");
    assert_ne!(
        current.get("contract_instance_id"),
        old.get("contract_instance_id"),
        "current and old registry targets must keep separate contract_instance_id values"
    );
    assert_eq!(
        current.get("source_family").and_then(Value::as_str),
        Some("ens_v1_registry_l1")
    );
    assert_eq!(
        old.get("source_family").and_then(Value::as_str),
        Some("ens_v1_registry_l1")
    );
    assert_eq!(
        current.get("address").and_then(Value::as_str),
        Some(ENS_V1_REGISTRY_CURRENT_ADDRESS)
    );
    assert_eq!(
        old.get("address").and_then(Value::as_str),
        Some(ENS_V1_REGISTRY_OLD_ADDRESS)
    );
    assert_eq!(
        current.get("effective_from_block").and_then(Value::as_i64),
        Some(ENS_V1_REGISTRY_CURRENT_START_BLOCK)
    );
    assert_eq!(
        old.get("effective_from_block").and_then(Value::as_i64),
        Some(ENS_V1_REGISTRY_OLD_START_BLOCK)
    );
}

fn assert_completed_dynamic_resolver_backfill_jobs(
    completed_jobs: &[(
        DynamicResolverBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
) {
    let covered_families = completed_jobs
        .iter()
        .map(|(fixture, _)| fixture.source_family)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        covered_families,
        BTreeSet::from(["ens_v1_resolver_l1", "basenames_base_resolver"])
    );

    for (fixture, completed_job) in completed_jobs {
        let expected_source_identity_hash =
            stable_source_identity_hash(&dynamic_resolver_identity_payload_without_hash(fixture));
        assert_eq!(
            completed_job.job.status,
            bigname_storage::BackfillLifecycleStatus::Completed,
            "{} dynamic resolver job must be completed",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.idempotency_key, fixture.idempotency_key,
            "{} dynamic resolver idempotency key should stay fixture-owned",
            fixture.source_family
        );
        assert_ne!(
            completed_job.job.idempotency_key,
            format!("conformance-source-family-{}", fixture.source_family),
            "{} dynamic resolver job must not reuse the source-family fixture key",
            fixture.source_family
        );
        assert_eq!(
            completed_job.job.deployment_profile,
            fixture.deployment_profile
        );
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
            Some(0)
        );
        assert_eq!(
            completed_job
                .job
                .source_identity
                .get("source_identity_hash")
                .and_then(Value::as_str),
            Some(expected_source_identity_hash.as_str()),
            "{} dynamic resolver job hash must cover the selected target lock",
            fixture.source_family
        );
        let selected_targets = completed_job
            .job
            .source_identity
            .get("selected_targets")
            .and_then(Value::as_array)
            .expect("dynamic resolver job should persist selected targets");
        assert_eq!(selected_targets.len(), 1);
        let selected_target = &selected_targets[0];
        assert_eq!(
            selected_target.get("source_family").and_then(Value::as_str),
            Some(fixture.source_family)
        );
        assert_eq!(
            selected_target
                .get("contract_instance_id")
                .and_then(Value::as_str),
            Some(fixture.contract_instance_id)
        );
        assert_eq!(
            selected_target.get("address").and_then(Value::as_str),
            Some(fixture.address)
        );
        assert_eq!(
            selected_target
                .get("effective_from_block")
                .and_then(Value::as_i64),
            Some(fixture.effective_from_block)
        );
        assert_eq!(
            selected_target
                .get("effective_to_block")
                .and_then(Value::as_i64),
            Some(fixture.effective_to_block)
        );
        assert!(
            fixture.range_start_block_number < fixture.effective_from_block
                && fixture.effective_to_block < fixture.range_end_block_number,
            "{} dynamic resolver selected target should be narrower than the requested job range",
            fixture.source_family
        );
        assert_eq!(completed_job.ranges.len(), 2);
        assert!(
            completed_job.ranges.iter().all(|range| {
                range.status == bigname_storage::BackfillLifecycleStatus::Completed
                    && range.checkpoint_block_number == range.range_end_block_number
                    && range.completed_at.is_some()
            }),
            "{} dynamic resolver job must persist completed child ranges",
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
        payload
            .get("coverage")
            .and_then(|coverage| { coverage.get("unsupported_reason").and_then(Value::as_str) }),
        None
    );

    Ok(())
}

fn assert_ensv2_shadow_exact_name_coverage_is_not_graduated(snapshots: &[(&'static str, Value)]) {
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

fn assert_ens_registry_old_admission_does_not_surface_consumer_coverage(
    snapshots: &[(&'static str, Value)],
) {
    for label in [
        "exact-name",
        "children-collection",
        "name-history",
        "resolver",
        "pending-profile-resolver",
        "unsupported-profile-resolver",
    ] {
        let payload = replay_route_payload(snapshots, label);
        for forbidden in [
            "registry_old",
            "ENSRegistryOld",
            ENS_V1_REGISTRY_OLD_ADDRESS,
            ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID,
            ENS_V1_REGISTRY_AUTO_BOOTSTRAP_OLD_CONTRACT_INSTANCE_ID,
            "ens_registry_old_migration_epoch_input",
            "ens_registry_old_root_resolver_exception",
            "source_identity_hash",
            "consumer_replacement",
            "consumer-replacement",
        ] {
            assert_json_not_contains(
                payload,
                forbidden,
                &format!(
                    "{label} route must not surface old-registry admission as consumer coverage"
                ),
            );
        }
    }
}

async fn assert_api_coverage_is_not_graduated_by_old_registry(
    database: &HarnessDatabase,
    corpus: &ReplayCorpus,
    snapshots: &[(&'static str, Value)],
) -> Result<()> {
    let exact_name = replay_route_payload(snapshots, "exact-name");
    let coverage_payload = request_replay_route(
        database,
        &ReplayRoute {
            label: "old-registry-coverage-non-graduation",
            uri: format!("/v1/coverage/basenames/{}", corpus.route_name),
        },
    )
    .await?;

    assert_eq!(
        coverage_payload.get("coverage"),
        exact_name.get("coverage"),
        "old-registry source-family admission must not graduate a separate API coverage contract"
    );
    assert_eq!(
        coverage_payload.get("data"),
        exact_name.get("data"),
        "coverage route should remain an alias of the existing exact-name response data"
    );
    for forbidden in [
        "registry_old",
        "ENSRegistryOld",
        ENS_V1_REGISTRY_OLD_ADDRESS,
        ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID,
        ENS_V1_REGISTRY_AUTO_BOOTSTRAP_OLD_CONTRACT_INSTANCE_ID,
        "ens_registry_old_migration_epoch_input",
        "backfill_job",
        "source_identity_hash",
        "consumer_replacement",
        "consumer-replacement",
    ] {
        assert_json_not_contains(
            &coverage_payload,
            forbidden,
            &format!(
                "old-registry operational evidence must not surface API coverage marker {forbidden}"
            ),
        );
    }

    Ok(())
}

async fn assert_auto_bootstrap_api_coverage_is_not_graduated(
    database: &HarnessDatabase,
    corpus: &ReplayCorpus,
    snapshots: &[(&'static str, Value)],
) -> Result<()> {
    let exact_name = replay_route_payload(snapshots, "exact-name");
    let coverage_payload = request_replay_route(
        database,
        &ReplayRoute {
            label: "auto-bootstrap-coverage-after-replay",
            uri: format!("/v1/coverage/basenames/{}", corpus.route_name),
        },
    )
    .await?;

    assert_eq!(
        coverage_payload.get("coverage"),
        exact_name.get("coverage"),
        "automatic bootstrap jobs must not graduate a separate API coverage contract"
    );
    assert_eq!(
        coverage_payload.get("data"),
        exact_name.get("data"),
        "coverage route should remain an alias of the existing exact-name response data"
    );
    for forbidden in [
        "auto_bootstrap",
        "bootstrap_backfill",
        "backfill_job",
        "source_identity_hash",
        "watched_target_set",
    ] {
        assert_json_not_contains(
            &coverage_payload,
            forbidden,
            &format!(
                "automatic bootstrap operational evidence must not surface API coverage marker {forbidden}"
            ),
        );
    }

    Ok(())
}

fn assert_auto_bootstrap_manifest_targets_skip_unknown_start(
    eligible_targets: &[AutoBootstrapBackfillFixture],
) {
    let expected_targets = auto_bootstrap_manifest_started_fixtures()
        .into_iter()
        .map(|fixture| (fixture.source_family, fixture.contract_instance_id))
        .collect::<BTreeSet<_>>();
    let actual_targets = eligible_targets
        .iter()
        .map(|fixture| (fixture.source_family, fixture.contract_instance_id))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_targets, expected_targets,
        "automatic bootstrap should only select manifest-started targets"
    );

    let unknown = auto_bootstrap_unknown_start_fixture();
    assert!(
        !actual_targets.contains(&(unknown.source_family, unknown.contract_instance_id)),
        "unknown-start Basenames target must be absent from automatic bootstrap manifest targets"
    );
}

fn assert_auto_bootstrap_ens_registry_targets_have_separate_current_and_old_starts(
    eligible_targets: &[AutoBootstrapBackfillFixture],
) {
    let registry_targets = eligible_targets
        .iter()
        .filter(|fixture| {
            fixture.chain_id == "ethereum-mainnet" && fixture.source_family == "ens_v1_registry_l1"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        registry_targets.len(),
        2,
        "automatic bootstrap must select current and old ENSv1 registry targets"
    );
    let current = registry_targets
        .iter()
        .find(|fixture| fixture.manifest_role == "registry")
        .expect("automatic bootstrap must include current registry role");
    let old = registry_targets
        .iter()
        .find(|fixture| fixture.manifest_role == "registry_old")
        .expect("automatic bootstrap must include old registry role");
    assert_ne!(
        current.contract_instance_id, old.contract_instance_id,
        "automatic bootstrap must keep current and old registry contract instances separate"
    );
    assert_eq!(
        current.contract_instance_id,
        ENS_V1_REGISTRY_AUTO_BOOTSTRAP_CURRENT_CONTRACT_INSTANCE_ID
    );
    assert_eq!(
        old.contract_instance_id,
        ENS_V1_REGISTRY_AUTO_BOOTSTRAP_OLD_CONTRACT_INSTANCE_ID
    );
    assert_eq!(current.address, ENS_V1_REGISTRY_CURRENT_ADDRESS);
    assert_eq!(old.address, ENS_V1_REGISTRY_OLD_ADDRESS);
    assert_eq!(
        current.effective_from_block_number(),
        ENS_V1_REGISTRY_CURRENT_START_BLOCK
    );
    assert_eq!(
        old.effective_from_block_number(),
        ENS_V1_REGISTRY_OLD_START_BLOCK
    );
}

fn assert_auto_bootstrap_unknown_start_reported_skipped(
    skipped_targets: &[bigname_manifests::ManifestBootstrapSkippedTarget],
) {
    let unknown = auto_bootstrap_unknown_start_fixture();
    assert_eq!(skipped_targets.len(), 1);
    let skipped_target = &skipped_targets[0];
    assert_eq!(skipped_target.source_family, unknown.source_family);
    assert_eq!(
        skipped_target.contract_instance_id.to_string(),
        unknown.contract_instance_id
    );
    assert_eq!(skipped_target.address, unknown.address);
    assert_eq!(skipped_target.skip_reason, "unknown_start");
}

fn assert_auto_bootstrap_unknown_start_absent_from_source_identity(
    completed_jobs: &[(
        AutoBootstrapBackfillFixture,
        bigname_storage::BackfillJobRecord,
    )],
) {
    let unknown = auto_bootstrap_unknown_start_fixture();
    for (_, completed_job) in completed_jobs {
        let source_identity = serde_json::to_string(&completed_job.job.source_identity)
            .expect("source identity should serialize for unknown-start absence assertion");
        for forbidden in [
            unknown.source_family,
            unknown.contract_instance_id,
            unknown.address,
        ] {
            assert!(
                !source_identity.contains(forbidden),
                "unknown-start Basenames target marker {forbidden} leaked into automatic bootstrap source_identity: {source_identity}"
            );
        }
    }
}

async fn snapshot_auto_bootstrap_existing_routes(
    database: &HarnessDatabase,
    corpus: &ReplayCorpus,
) -> Result<Vec<(&'static str, Value)>> {
    let mut snapshots = snapshot_replay_stale_current_answer_routes(database, corpus).await?;
    let coverage_payload = request_replay_route(
        database,
        &ReplayRoute {
            label: "auto-bootstrap-coverage-before-replay",
            uri: format!("/v1/coverage/basenames/{}", corpus.route_name),
        },
    )
    .await?;
    snapshots.push(("coverage", coverage_payload));

    Ok(snapshots)
}

fn auto_bootstrap_identity(fixture: &AutoBootstrapBackfillFixture) -> Value {
    let mut payload = auto_bootstrap_identity_payload_without_hash(fixture);
    let source_identity_hash = stable_source_identity_hash(&payload);
    payload
        .as_object_mut()
        .expect("automatic bootstrap identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    payload
}

fn auto_bootstrap_identity_payload_without_hash(fixture: &AutoBootstrapBackfillFixture) -> Value {
    json!({
        "selector_kind": "watched_target_set",
        "source_family": null,
        "requested_watched_targets": [{
            "contract_instance_id": fixture.contract_instance_id,
        }],
        "selected_targets": [{
            "source_family": fixture.source_family,
            "contract_instance_id": fixture.contract_instance_id,
            "address": fixture.address,
            "effective_from_block": fixture.effective_from_block_number(),
            "effective_to_block": fixture.effective_to_block_number(),
        }],
    })
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

fn source_family_identity_payload_without_hash(fixture: &SourceFamilyBackfillFixture) -> Value {
    let selected_targets = fixture
        .selected_targets
        .iter()
        .map(|target| {
            json!({
                "source_family": fixture.source_family,
                "contract_instance_id": target.contract_instance_id,
                "address": target.address,
                "effective_from_block": target.range_start_block_number,
                "effective_to_block": target.range_end_block_number,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "selector_kind": "source_family",
        "source_family": fixture.source_family,
        "requested_watched_targets": [],
        "selected_targets": selected_targets,
    })
}

fn dynamic_resolver_identity(fixture: &DynamicResolverBackfillFixture) -> Value {
    let mut payload = dynamic_resolver_identity_payload_without_hash(fixture);
    let source_identity_hash = stable_source_identity_hash(&payload);
    payload
        .as_object_mut()
        .expect("dynamic resolver identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    payload
}

fn dynamic_resolver_identity_payload_without_hash(
    fixture: &DynamicResolverBackfillFixture,
) -> Value {
    json!({
        "selector_kind": "source_family",
        "source_family": fixture.source_family,
        "requested_watched_targets": [],
        "selected_targets": [{
            "source_family": fixture.source_family,
            "contract_instance_id": fixture.contract_instance_id,
            "address": fixture.address,
            "effective_from_block": fixture.effective_from_block,
            "effective_to_block": fixture.effective_to_block,
        }],
    })
}

fn stable_source_identity_hash(payload_without_hash: &Value) -> String {
    let serialized = serde_json::to_string(payload_without_hash)
        .expect("source-family identity payload must be serializable");
    let hash = serialized.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("fnv1a64:{hash:016x}")
}

fn dynamic_resolver_backfill_fixtures() -> Vec<DynamicResolverBackfillFixture> {
    vec![
        DynamicResolverBackfillFixture {
            deployment_profile: "mainnet",
            chain_id: "ethereum-mainnet",
            source_family: "ens_v1_resolver_l1",
            contract_instance_id: "00000000-0000-0000-0000-00000000a105",
            address: "0x0000000000000000000000000000000000000a51",
            range_start_block_number: 340,
            range_end_block_number: 420,
            effective_from_block: 361,
            effective_to_block: 400,
            idempotency_key: "conformance-dynamic-resolver-ens-v1-lock",
        },
        DynamicResolverBackfillFixture {
            deployment_profile: "mainnet",
            chain_id: "base-mainnet",
            source_family: "basenames_base_resolver",
            contract_instance_id: "00000000-0000-0000-0000-00000000a303",
            address: "0x0000000000000000000000000000000000000a53",
            range_start_block_number: 190,
            range_end_block_number: 280,
            effective_from_block: 211,
            effective_to_block: 261,
            idempotency_key: "conformance-dynamic-resolver-basenames-lock",
        },
    ]
}

fn source_family_backfill_fixtures() -> Vec<SourceFamilyBackfillFixture> {
    vec![
        source_family_backfill_fixture(
            "ens",
            "mainnet",
            "ethereum-mainnet",
            "ens_v1_registry_l1",
            vec![
                source_family_backfill_target(
                    ENS_V1_REGISTRY_CURRENT_CONTRACT_INSTANCE_ID,
                    ENS_V1_REGISTRY_CURRENT_ADDRESS,
                    ENS_V1_REGISTRY_CURRENT_START_BLOCK,
                    ENS_V1_REGISTRY_BACKFILL_END_BLOCK,
                ),
                source_family_backfill_target(
                    ENS_V1_REGISTRY_OLD_CONTRACT_INSTANCE_ID,
                    ENS_V1_REGISTRY_OLD_ADDRESS,
                    ENS_V1_REGISTRY_OLD_START_BLOCK,
                    ENS_V1_REGISTRY_BACKFILL_END_BLOCK,
                ),
            ],
        ),
        source_family_backfill_fixture(
            "ens",
            "mainnet",
            "ethereum-mainnet",
            "ens_v1_registrar_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009102",
                "0x0000000000000000000000000000000000000102",
                181,
                260,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "mainnet",
            "ethereum-mainnet",
            "ens_v1_reverse_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009103",
                "0x0000000000000000000000000000000000000103",
                261,
                320,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "mainnet",
            "ethereum-mainnet",
            "ens_v1_wrapper_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009104",
                "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
                321,
                360,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "mainnet",
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009105",
                "0xf29100983e058b709f3d539b0c765937b804ac15",
                361,
                400,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "sepolia-dev",
            "ethereum-sepolia",
            "ens_v2_root_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009201",
                "0x0000000000000000000000000000000000000201",
                80,
                120,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "sepolia-dev",
            "ethereum-sepolia",
            "ens_v2_registry_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009202",
                "0x0000000000000000000000000000000000000202",
                121,
                180,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "sepolia-dev",
            "ethereum-sepolia",
            "ens_v2_registrar_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009203",
                "0x0000000000000000000000000000000000000203",
                181,
                240,
            )],
        ),
        source_family_backfill_fixture(
            "ens",
            "sepolia-dev",
            "ethereum-sepolia",
            "ens_v2_resolver_l1",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009204",
                "0x0000000000000000000000000000000000000204",
                241,
                300,
            )],
        ),
        source_family_backfill_fixture(
            "basenames",
            "mainnet",
            "base-mainnet",
            "basenames_base_registry",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009301",
                "0x0000000000000000000000000000000000000301",
                98,
                150,
            )],
        ),
        source_family_backfill_fixture(
            "basenames",
            "mainnet",
            "base-mainnet",
            "basenames_base_registrar",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009302",
                "0x0000000000000000000000000000000000000302",
                151,
                210,
            )],
        ),
        source_family_backfill_fixture(
            "basenames",
            "mainnet",
            "base-mainnet",
            "basenames_base_resolver",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009303",
                "0x0000000000000000000000000000000000000303",
                211,
                261,
            )],
        ),
        source_family_backfill_fixture(
            "basenames",
            "mainnet",
            "base-mainnet",
            "basenames_base_primary",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009304",
                "0x0000000000000000000000000000000000000304",
                262,
                320,
            )],
        ),
        source_family_backfill_fixture(
            "basenames",
            "mainnet",
            "ethereum-mainnet",
            "basenames_l1_compat",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009305",
                "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31",
                401,
                440,
            )],
        ),
        source_family_backfill_fixture(
            "basenames",
            "mainnet",
            "ethereum-mainnet",
            "basenames_execution",
            vec![source_family_backfill_target(
                "00000000-0000-0000-0000-000000009306",
                "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31",
                441,
                480,
            )],
        ),
    ]
}

fn source_family_backfill_fixture(
    namespace: &'static str,
    deployment_profile: &'static str,
    chain_id: &'static str,
    source_family: &'static str,
    selected_targets: Vec<SourceFamilyBackfillTarget>,
) -> SourceFamilyBackfillFixture {
    let range_start_block_number = selected_targets
        .iter()
        .map(|target| target.range_start_block_number)
        .min()
        .expect("source-family fixture must include at least one selected target");
    let range_end_block_number = selected_targets
        .iter()
        .map(|target| target.range_end_block_number)
        .max()
        .expect("source-family fixture must include at least one selected target");
    SourceFamilyBackfillFixture {
        namespace,
        deployment_profile,
        chain_id,
        source_family,
        range_start_block_number,
        range_end_block_number,
        selected_targets,
    }
}

fn source_family_backfill_target(
    contract_instance_id: &'static str,
    address: &'static str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> SourceFamilyBackfillTarget {
    SourceFamilyBackfillTarget {
        contract_instance_id,
        address,
        range_start_block_number,
        range_end_block_number,
    }
}

async fn seed_auto_bootstrap_manifest_sources(database: &HarnessDatabase) -> Result<()> {
    let mut fixtures_by_manifest =
        BTreeMap::<(&str, &str, &str), Vec<AutoBootstrapBackfillFixture>>::new();
    for fixture in auto_bootstrap_manifest_started_fixtures() {
        fixtures_by_manifest
            .entry((fixture.namespace, fixture.source_family, fixture.chain_id))
            .or_default()
            .push(fixture);
    }

    for ((namespace, source_family, chain_id), fixtures) in fixtures_by_manifest {
        let contracts = fixtures
            .iter()
            .map(|fixture| {
                json!({
                    "role": fixture.manifest_role,
                    "address": fixture.address,
                    "start_block": fixture.manifest_start_block_number,
                })
            })
            .collect::<Vec<_>>();
        let manifest_id = insert_auto_bootstrap_manifest(
            database,
            namespace,
            source_family,
            chain_id,
            json!({
                "contracts": contracts,
                "roots": [],
            }),
        )
        .await?;
        for fixture in fixtures {
            insert_auto_bootstrap_contract_target(
                database,
                manifest_id,
                fixture.chain_id,
                fixture.contract_instance_id,
                fixture.address,
                fixture.manifest_role,
                fixture.active_from_block_number,
                fixture.active_to_block_number,
            )
            .await?;
        }
    }

    let unknown = auto_bootstrap_unknown_start_fixture();
    let manifest_id = insert_auto_bootstrap_manifest(
        database,
        unknown.namespace,
        unknown.source_family,
        unknown.chain_id,
        json!({
            "contracts": [{
                "role": unknown.manifest_role,
                "address": unknown.address,
            }],
            "roots": [],
        }),
    )
    .await?;
    insert_auto_bootstrap_contract_target(
        database,
        manifest_id,
        unknown.chain_id,
        unknown.contract_instance_id,
        unknown.address,
        unknown.manifest_role,
        None,
        None,
    )
    .await?;

    Ok(())
}

async fn load_auto_bootstrap_manifest_started_targets(
    database: &HarnessDatabase,
) -> Result<Vec<AutoBootstrapBackfillFixture>> {
    let expected_fixtures = auto_bootstrap_manifest_started_fixtures()
        .into_iter()
        .map(|fixture| ((fixture.chain_id, fixture.contract_instance_id), fixture))
        .collect::<BTreeMap<_, _>>();
    let mut resolved_fixtures = Vec::new();
    for chain in ["base-mainnet", "ethereum-mainnet", "ethereum-sepolia"] {
        for target in
            bigname_manifests::load_manifest_declared_bootstrap_targets(&database.pool, chain)
                .await
                .with_context(|| {
                    format!("failed to load automatic bootstrap targets for chain {chain}")
                })?
        {
            let contract_instance_id = target.contract_instance_id.to_string();
            if let Some(fixture) = expected_fixtures.get(&(chain, contract_instance_id.as_str())) {
                assert_eq!(target.source_family, fixture.source_family);
                assert_eq!(target.address, fixture.address);
                assert_eq!(
                    target.effective_from_block,
                    fixture.admitted_history_start_block_number()
                );
                assert_eq!(target.effective_to_block, fixture.active_to_block_number);
                resolved_fixtures.push(fixture.clone());
            } else {
                assert_ne!(
                    contract_instance_id,
                    auto_bootstrap_unknown_start_fixture().contract_instance_id,
                    "unknown-start Basenames target must not be returned by manifest bootstrap helper"
                );
            }
        }
    }

    resolved_fixtures.sort_by_key(|fixture| {
        (
            fixture.source_family,
            fixture.contract_instance_id,
            fixture.address,
            fixture.effective_from_block_number(),
            fixture.effective_to_block_number(),
        )
    });
    Ok(resolved_fixtures)
}

async fn load_auto_bootstrap_manifest_skipped_targets(
    database: &HarnessDatabase,
) -> Result<Vec<bigname_manifests::ManifestBootstrapSkippedTarget>> {
    bigname_manifests::load_manifest_skipped_bootstrap_targets(&database.pool, "base-mainnet")
        .await
        .context("failed to load skipped automatic bootstrap targets for base-mainnet")
}

async fn insert_auto_bootstrap_manifest(
    database: &HarnessDatabase,
    namespace: &str,
    source_family: &str,
    chain: &str,
    manifest_payload: Value,
) -> Result<i64> {
    let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let file_path =
        format!("tests/auto-bootstrap/{namespace}/{source_family}/{chain}-{sequence}.toml");
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
        VALUES (1, $1, $2, $3, 'auto-bootstrap-conformance', 'active', 'ensip15@ens-normalize-0.1.1', $4, $5::jsonb)
        RETURNING manifest_id
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(file_path)
    .bind(manifest_payload)
    .fetch_one(&database.pool)
    .await
    .with_context(|| {
        format!("failed to insert automatic bootstrap manifest for {chain}:{source_family}")
    })?
    .try_get("manifest_id")
    .context("failed to read automatic bootstrap manifest_id")
}

#[allow(clippy::too_many_arguments)]
async fn insert_auto_bootstrap_contract_target(
    database: &HarnessDatabase,
    manifest_id: i64,
    chain: &str,
    contract_instance_id: &str,
    address: &str,
    role: &str,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
) -> Result<()> {
    let contract_instance_id = Uuid::parse_str(contract_instance_id)
        .context("automatic bootstrap contract_instance_id must be UUID-shaped")?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, $2, 'contract')
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .execute(&database.pool)
    .await
    .with_context(|| {
        format!("failed to insert automatic bootstrap contract_instance_id {contract_instance_id}")
    })?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(&database.pool)
    .await
    .with_context(|| {
        format!(
            "failed to insert automatic bootstrap manifest contract {role} for {contract_instance_id}"
        )
    })?;

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_to_block_number,
            source_manifest_id
        )
        VALUES ($1, $2, lower($3), $4, $5, $6)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(address)
    .bind(active_from_block_number)
    .bind(active_to_block_number)
    .bind(manifest_id)
    .execute(&database.pool)
    .await
    .with_context(|| {
        format!(
            "failed to insert automatic bootstrap active address {address} for {contract_instance_id}"
        )
    })?;

    Ok(())
}

impl AutoBootstrapBackfillFixture {
    fn admitted_history_start_block_number(&self) -> i64 {
        self.active_from_block_number
            .map(|active_from| active_from.max(self.manifest_start_block_number))
            .unwrap_or(self.manifest_start_block_number)
    }

    fn effective_from_block_number(&self) -> i64 {
        let admitted_start = self.admitted_history_start_block_number();
        let capped_start = self
            .bootstrap_backfill_max_blocks
            .map(|max_blocks| {
                self.provider_head_block_number
                    .checked_sub(max_blocks - 1)
                    .unwrap_or(0)
                    .max(0)
            })
            .unwrap_or(admitted_start);
        admitted_start.max(capped_start)
    }

    fn effective_to_block_number(&self) -> i64 {
        self.active_to_block_number
            .map(|active_to| active_to.min(self.provider_head_block_number))
            .unwrap_or(self.provider_head_block_number)
    }
}

fn auto_bootstrap_manifest_started_fixtures() -> Vec<AutoBootstrapBackfillFixture> {
    vec![
        AutoBootstrapBackfillFixture {
            namespace: "ens",
            deployment_profile: "mainnet",
            chain_id: "ethereum-mainnet",
            source_family: "ens_v1_registry_l1",
            contract_instance_id: ENS_V1_REGISTRY_AUTO_BOOTSTRAP_CURRENT_CONTRACT_INSTANCE_ID,
            address: ENS_V1_REGISTRY_CURRENT_ADDRESS,
            manifest_role: "registry",
            manifest_start_block_number: ENS_V1_REGISTRY_CURRENT_START_BLOCK,
            active_from_block_number: None,
            active_to_block_number: None,
            provider_head_block_number: 9_400_000,
            bootstrap_backfill_max_blocks: None,
        },
        AutoBootstrapBackfillFixture {
            namespace: "ens",
            deployment_profile: "mainnet",
            chain_id: "ethereum-mainnet",
            source_family: "ens_v1_registry_l1",
            contract_instance_id: ENS_V1_REGISTRY_AUTO_BOOTSTRAP_OLD_CONTRACT_INSTANCE_ID,
            address: ENS_V1_REGISTRY_OLD_ADDRESS,
            manifest_role: "registry_old",
            manifest_start_block_number: ENS_V1_REGISTRY_OLD_START_BLOCK,
            active_from_block_number: None,
            active_to_block_number: None,
            provider_head_block_number: 9_400_000,
            bootstrap_backfill_max_blocks: None,
        },
        AutoBootstrapBackfillFixture {
            namespace: "ens",
            deployment_profile: "sepolia-dev",
            chain_id: "ethereum-sepolia",
            source_family: "ens_v2_registry_l1",
            contract_instance_id: "00000000-0000-0000-0000-00000000b201",
            address: "0x000000000000000000000000000000000000b201",
            manifest_role: "registry",
            manifest_start_block_number: 120,
            active_from_block_number: Some(125),
            active_to_block_number: Some(160),
            provider_head_block_number: 170,
            bootstrap_backfill_max_blocks: None,
        },
        AutoBootstrapBackfillFixture {
            namespace: "ens",
            deployment_profile: "sepolia-dev",
            chain_id: "ethereum-sepolia",
            source_family: "ens_v2_registrar_l1",
            contract_instance_id: "00000000-0000-0000-0000-00000000b301",
            address: "0x000000000000000000000000000000000000b301",
            manifest_role: "registrar",
            manifest_start_block_number: 210,
            active_from_block_number: None,
            active_to_block_number: None,
            provider_head_block_number: 260,
            bootstrap_backfill_max_blocks: None,
        },
        AutoBootstrapBackfillFixture {
            namespace: "ens",
            deployment_profile: "sepolia-dev",
            chain_id: "ethereum-sepolia",
            source_family: "ens_v2_resolver_l1",
            contract_instance_id: "00000000-0000-0000-0000-00000000b401",
            address: "0x000000000000000000000000000000000000b401",
            manifest_role: "resolver",
            manifest_start_block_number: 100,
            active_from_block_number: Some(120),
            active_to_block_number: None,
            provider_head_block_number: 30_200,
            bootstrap_backfill_max_blocks: Some(25_000),
        },
    ]
}

fn auto_bootstrap_unknown_start_fixture() -> AutoBootstrapUnknownStartFixture {
    AutoBootstrapUnknownStartFixture {
        namespace: "basenames",
        chain_id: "base-mainnet",
        source_family: "basenames_base_resolver",
        contract_instance_id: "00000000-0000-0000-0000-00000000b302",
        address: "0x000000000000000000000000000000000000b302",
        manifest_role: "resolver",
    }
}
