fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("conformance timestamp must be valid")
}

const RAW_REPLAY_PROBE_SOURCE_FAMILY: &str = "ens_v1_reverse_l1";
const RAW_REPLAY_PROBE_CONTRACT_ROLE: &str = "reverse_registrar";
const RAW_REPLAY_PROBE_CLAIMED_ADDRESS: &str = "0x1234567890abcdef1234567890abcdef12345678";
const RAW_REPLAY_PROBE_REVERSE_CLAIMED_TOPIC0: &str =
    "0x6ada868dd3058cf77a48a74489fd7963688e5464b2b0fa957ace976243270e92";
const RAW_REPLAY_PROBE_CLAIMED_ADDRESS_TOPIC: &str =
    "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678";
const RAW_REPLAY_PROBE_REVERSE_NODE_TOPIC: &str =
    "0xab5f3e28c9cfb162e62c91f566751059da9be419f5cbd10d0645d765c061d0e3";
const BASENAMES_L2_RESOLVER_PROFILE_CODE_HASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const BASENAMES_UNSUPPORTED_RESOLVER_PROFILE_CODE_HASH: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";

#[derive(Clone, Copy)]
enum BasenamesControlVectorScenario {
    NftOnly,
    ManagementOnly,
    FullTransfer,
}

impl BasenamesControlVectorScenario {
    fn current_token_subject(self) -> &'static str {
        match self {
            Self::NftOnly => "0x00000000000000000000000000000000000000c1",
            Self::ManagementOnly => "0x00000000000000000000000000000000000000a2",
            Self::FullTransfer => "0x00000000000000000000000000000000000000c3",
        }
    }

    fn current_effective_controller(self) -> &'static str {
        match self {
            Self::NftOnly => "0x00000000000000000000000000000000000000b1",
            Self::ManagementOnly => "0x00000000000000000000000000000000000000b2",
            Self::FullTransfer => "0x00000000000000000000000000000000000000c3",
        }
    }

    fn previous_effective_controller(self) -> Option<&'static str> {
        match self {
            Self::FullTransfer => Some("0x00000000000000000000000000000000000000b3"),
            _ => None,
        }
    }
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
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
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

async fn seed_basenames_exact_name_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
            raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
            raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_171_700),
            raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
            raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
            raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "basenames".to_owned(),
            input_name: "alice.base.eth".to_owned(),
            canonical_display_name: "Alice.base.eth".to_owned(),
            normalized_name: "alice.base.eth".to_owned(),
            dns_encoded_name: b"alice.base.eth".to_vec(),
            namehash: "namehash:alice.base.eth".to_owned(),
            labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 98,
            provenance: json!({"seed": "basenames_exact_name_surface"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[TokenLineage {
            token_lineage_id,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "basenames_exact_name_token_lineage"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
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
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
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
    .await?;
    bigname_storage::upsert_normalized_events(
                        &database.pool,
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
                    .await?;

    Ok(())
}

async fn rebuild_address_names_current(
    database: &HarnessDatabase,
    address: Option<&str>,
) -> Result<()> {
    let database_url = std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_database_url().to_owned());
    let base_options = PgConnectOptions::from_str(&database_url)
        .context("failed to parse database URL for conformance address_names rebuild")?;
    let rebuild_database_url = base_options
        .database(&database.database_name)
        .to_url_lossy()
        .to_string();
    let address = address.map(str::to_owned);
    let worker_manifest_path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

    tokio::task::spawn_blocking(move || -> Result<()> {
        let _guard = WORKER_CARGO_LOCK
            .lock()
            .expect("worker cargo lock must not be poisoned");
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let mut command = std::process::Command::new(cargo);
        command
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(worker_manifest_path)
            .arg("--")
            .arg("address-names-current")
            .arg("rebuild")
            .arg("--database-url")
            .arg(&rebuild_database_url);
        if let Some(address) = address.as_deref() {
            command.arg("--address").arg(address);
        }

        let output = command.output().with_context(|| {
            format!(
                "failed to invoke worker address_names_current rebuild for {}",
                address.as_deref().unwrap_or("all")
            )
        })?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "worker address_names_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                address.as_deref().unwrap_or("all"),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ));
        }

        Ok(())
    })
    .await
    .context("worker address_names_current rebuild task panicked")??;

    Ok(())
}

async fn rebuild_children_current(
    database: &HarnessDatabase,
    logical_name_id: Option<&str>,
) -> Result<()> {
    let database_url = database.database_url.clone();
    let logical_name_id = logical_name_id.map(str::to_owned);
    let worker_manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

    tokio::task::spawn_blocking(move || -> Result<()> {
        let _guard = WORKER_CARGO_LOCK
            .lock()
            .expect("worker cargo lock must not be poisoned");
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let mut command = Command::new(cargo);
        command
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(worker_manifest_path)
            .arg("--")
            .arg("children-current")
            .arg("rebuild")
            .arg("--database-url")
            .arg(&database_url);
        if let Some(logical_name_id) = logical_name_id.as_deref() {
            command.arg("--logical-name-id").arg(logical_name_id);
        }

        let output = command.output().with_context(|| {
            format!(
                "failed to invoke worker children_current rebuild for {}",
                logical_name_id.as_deref().unwrap_or("all")
            )
        })?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "worker children_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                logical_name_id.as_deref().unwrap_or("all"),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ));
        }

        Ok(())
    })
    .await
    .context("worker children_current rebuild task panicked")??;

    Ok(())
}

async fn seed_basenames_control_vector_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    scenario: BasenamesControlVectorScenario,
) -> Result<()> {
    let normalized_name = logical_name_id
        .split_once(':')
        .map(|(_, normalized_name)| normalized_name)
        .expect("logical_name_id must include namespace");

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_181_698),
            raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_181_699),
            raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_181_700),
            raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_181_701),
            raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_181_702),
            raw_block("base-mainnet", "0xbase-token", None, 103, 1_717_181_703),
            raw_block(
                "base-mainnet",
                "0xbase-authority-final",
                None,
                104,
                1_717_181_704,
            ),
            raw_block("base-mainnet", "0xbase-resolver", None, 105, 1_717_181_705),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "basenames".to_owned(),
            input_name: normalized_name.to_owned(),
            canonical_display_name: normalized_name.to_owned(),
            normalized_name: normalized_name.to_owned(),
            dns_encoded_name: normalized_name.as_bytes().to_vec(),
            namehash: format!("namehash:{normalized_name}"),
            labelhashes: vec![format!("labelhash:{normalized_name}")],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 98,
            provenance: json!({"seed": "basenames_control_vector_surface"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[TokenLineage {
            token_lineage_id,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "basenames_control_vector_token_lineage"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "basenames_control_vector_resource"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_181_700),
            active_to: None,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-binding".to_owned(),
            block_number: 100,
            provenance: json!({"seed": "basenames_control_vector_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let mut events = vec![NormalizedEvent {
        event_identity: format!("conformance:{logical_name_id}:grant"),
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
        transaction_hash: Some(format!("0xtx:{logical_name_id}:grant")),
        log_index: Some(0),
        raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:grant")}),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "authority_kind": "registrar",
            "authority_key": format!("registrar:base-mainnet:{normalized_name}"),
            "registrant": match scenario {
                BasenamesControlVectorScenario::NftOnly => "0x00000000000000000000000000000000000000a1",
                BasenamesControlVectorScenario::ManagementOnly => "0x00000000000000000000000000000000000000a2",
                BasenamesControlVectorScenario::FullTransfer => "0x00000000000000000000000000000000000000a3",
            },
            "expiry": 1_900_000_000_i64,
        }),
    }];

    match scenario {
        BasenamesControlVectorScenario::NftOnly => {
            events.push(NormalizedEvent {
                                event_identity: format!("conformance:{logical_name_id}:authority"),
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
                                transaction_hash: Some(format!("0xtx:{logical_name_id}:authority")),
                                log_index: Some(0),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:authority")}),
                                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                                canonicality_state: CanonicalityState::Canonical,
                                before_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000a1",
                                }),
                                after_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000b1",
                                }),
                            });
            events.push(NormalizedEvent {
                                event_identity: format!("conformance:{logical_name_id}:token"),
                                namespace: "basenames".to_owned(),
                                logical_name_id: Some(logical_name_id.to_owned()),
                                resource_id: Some(resource_id),
                                event_kind: "TokenControlTransferred".to_owned(),
                                source_family: "basenames_base_registrar".to_owned(),
                                manifest_version: 3,
                                source_manifest_id: None,
                                chain_id: Some("base-mainnet".to_owned()),
                                block_number: Some(103),
                                block_hash: Some("0xbase-token".to_owned()),
                                transaction_hash: Some(format!("0xtx:{logical_name_id}:token")),
                                log_index: Some(0),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:token")}),
                                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                                canonicality_state: CanonicalityState::Canonical,
                                before_state: json!({
                                    "from": "0x00000000000000000000000000000000000000a1",
                                }),
                                after_state: json!({
                                    "to": "0x00000000000000000000000000000000000000c1",
                                }),
                            });
        }
        BasenamesControlVectorScenario::ManagementOnly => {
            events.push(NormalizedEvent {
                                event_identity: format!("conformance:{logical_name_id}:authority"),
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
                                transaction_hash: Some(format!("0xtx:{logical_name_id}:authority")),
                                log_index: Some(0),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:authority")}),
                                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                                canonicality_state: CanonicalityState::Canonical,
                                before_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000a2",
                                }),
                                after_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000b2",
                                }),
                            });
        }
        BasenamesControlVectorScenario::FullTransfer => {
            events.push(NormalizedEvent {
                                event_identity: format!("conformance:{logical_name_id}:authority"),
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
                                transaction_hash: Some(format!("0xtx:{logical_name_id}:authority")),
                                log_index: Some(0),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:authority")}),
                                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                                canonicality_state: CanonicalityState::Canonical,
                                before_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000a3",
                                }),
                                after_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000b3",
                                }),
                            });
            events.push(NormalizedEvent {
                                event_identity: format!("conformance:{logical_name_id}:token"),
                                namespace: "basenames".to_owned(),
                                logical_name_id: Some(logical_name_id.to_owned()),
                                resource_id: Some(resource_id),
                                event_kind: "TokenControlTransferred".to_owned(),
                                source_family: "basenames_base_registrar".to_owned(),
                                manifest_version: 3,
                                source_manifest_id: None,
                                chain_id: Some("base-mainnet".to_owned()),
                                block_number: Some(103),
                                block_hash: Some("0xbase-token".to_owned()),
                                transaction_hash: Some(format!("0xtx:{logical_name_id}:token")),
                                log_index: Some(0),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:token")}),
                                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                                canonicality_state: CanonicalityState::Canonical,
                                before_state: json!({
                                    "from": "0x00000000000000000000000000000000000000a3",
                                }),
                                after_state: json!({
                                    "to": "0x00000000000000000000000000000000000000c3",
                                }),
                            });
            events.push(NormalizedEvent {
                                event_identity: format!("conformance:{logical_name_id}:authority-final"),
                                namespace: "basenames".to_owned(),
                                logical_name_id: Some(logical_name_id.to_owned()),
                                resource_id: Some(resource_id),
                                event_kind: "AuthorityTransferred".to_owned(),
                                source_family: "basenames_base_registry".to_owned(),
                                manifest_version: 3,
                                source_manifest_id: None,
                                chain_id: Some("base-mainnet".to_owned()),
                                block_number: Some(104),
                                block_hash: Some("0xbase-authority-final".to_owned()),
                                transaction_hash: Some(format!("0xtx:{logical_name_id}:authority-final")),
                                log_index: Some(0),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:authority-final")}),
                                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                                canonicality_state: CanonicalityState::Canonical,
                                before_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000b3",
                                }),
                                after_state: json!({
                                    "owner": "0x00000000000000000000000000000000000000c3",
                                }),
                            });
        }
    }

    events.push(NormalizedEvent {
                        event_identity: format!("conformance:{logical_name_id}:resolver"),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(105),
                        block_hash: Some("0xbase-resolver".to_owned()),
                        transaction_hash: Some(format!("0xtx:{logical_name_id}:resolver")),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:resolver")}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": "0x0000000000000000000000000000000000000abc",
                            "namehash": format!("namehash:{normalized_name}"),
                        }),
                    });

    bigname_storage::upsert_normalized_events(&database.pool, &events).await?;

    Ok(())
}

async fn seed_basenames_resolution_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    seed_basenames_exact_name_rebuild_inputs(
        database,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    bigname_storage::upsert_normalized_events(
                        &database.pool,
                        &[
                            NormalizedEvent {
                                event_identity: "conformance:basenames:record-version".to_owned(),
                                namespace: "basenames".to_owned(),
                                logical_name_id: Some(logical_name_id.to_owned()),
                                resource_id: Some(resource_id),
                                event_kind: "RecordVersionChanged".to_owned(),
                                source_family: "basenames_base_resolver".to_owned(),
                                manifest_version: 4,
                                source_manifest_id: None,
                                chain_id: Some("base-mainnet".to_owned()),
                                block_number: Some(103),
                                block_hash: Some("0xbase-resolver".to_owned()),
                                transaction_hash: Some("0xtxbaseresolver".to_owned()),
                                log_index: Some(1),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:record-version"}),
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
                                event_identity: "conformance:basenames:addr".to_owned(),
                                namespace: "basenames".to_owned(),
                                logical_name_id: Some(logical_name_id.to_owned()),
                                resource_id: Some(resource_id),
                                event_kind: "RecordChanged".to_owned(),
                                source_family: "basenames_base_resolver".to_owned(),
                                manifest_version: 4,
                                source_manifest_id: None,
                                chain_id: Some("base-mainnet".to_owned()),
                                block_number: Some(103),
                                block_hash: Some("0xbase-resolver".to_owned()),
                                transaction_hash: Some("0xtxbaseresolver".to_owned()),
                                log_index: Some(2),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:addr"}),
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
                                event_identity: "conformance:basenames:text".to_owned(),
                                namespace: "basenames".to_owned(),
                                logical_name_id: Some(logical_name_id.to_owned()),
                                resource_id: Some(resource_id),
                                event_kind: "RecordChanged".to_owned(),
                                source_family: "basenames_base_resolver".to_owned(),
                                manifest_version: 4,
                                source_manifest_id: None,
                                chain_id: Some("base-mainnet".to_owned()),
                                block_number: Some(103),
                                block_hash: Some("0xbase-resolver".to_owned()),
                                transaction_hash: Some("0xtxbaseresolver".to_owned()),
                                log_index: Some(3),
                                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:text"}),
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
                    .await?;

    Ok(())
}

async fn rebuild_record_inventory_current(
    database: &HarnessDatabase,
    resource_id: Uuid,
) -> Result<()> {
    let database_url = database.database_url.clone();
    let resource_id_value = resource_id;
    let resource_id = resource_id.to_string();
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
                            .arg("record-inventory-current")
                            .arg("rebuild")
                            .arg("--database-url")
                            .arg(&database_url)
                            .arg("--resource-id")
                            .arg(&resource_id)
                            .output()
                            .with_context(|| {
                                format!(
                                    "failed to invoke worker record_inventory_current rebuild for {resource_id}"
                                )
                            })?;

                        if !output.status.success() {
                            return Err(anyhow::anyhow!(
                                "worker record_inventory_current rebuild failed for {resource_id}\nstdout:\n{}\nstderr:\n{}",
                                String::from_utf8_lossy(&output.stdout),
                                String::from_utf8_lossy(&output.stderr),
                            ));
                        }

                        Ok(())
                    })
                    .await
                    .context("worker record_inventory_current rebuild task panicked")??;

    let rows = sqlx::query(
        r#"
        SELECT chain_positions
        FROM record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id_value)
    .fetch_all(&database.pool)
    .await
    .with_context(|| {
        format!(
            "failed to load rebuilt record_inventory_current rows for resource_id {resource_id_value}"
        )
    })?;
    for row in rows {
        let chain_positions = row
            .try_get::<Value, _>("chain_positions")
            .context("record_inventory_current row missing chain_positions")?;
        database
            .seed_snapshot_selector_chain_positions(&chain_positions)
            .await?;
    }

    Ok(())
}

async fn rebuild_permissions_current(
    database: &HarnessDatabase,
    resource_id: Option<Uuid>,
) -> Result<()> {
    let database_url = database.database_url.clone();
    let resource_id = resource_id.map(|value| value.to_string());
    let worker_manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

    tokio::task::spawn_blocking(move || -> Result<()> {
        let _guard = WORKER_CARGO_LOCK
            .lock()
            .expect("worker cargo lock must not be poisoned");
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let mut command = Command::new(cargo);
        command
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(worker_manifest_path)
            .arg("--")
            .arg("permissions-current")
            .arg("rebuild")
            .arg("--database-url")
            .arg(&database_url);
        if let Some(resource_id) = resource_id.as_deref() {
            command.arg("--resource-id").arg(resource_id);
        }

        let output = command.output().with_context(|| {
            format!(
                "failed to invoke worker permissions_current rebuild for {}",
                resource_id.as_deref().unwrap_or("all")
            )
        })?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "worker permissions_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                resource_id.as_deref().unwrap_or("all"),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ));
        }

        Ok(())
    })
    .await
    .context("worker permissions_current rebuild task panicked")??;

    Ok(())
}

async fn rebuild_resolver_current(
    database: &HarnessDatabase,
    chain_id: Option<&str>,
    resolver_address: Option<&str>,
) -> Result<()> {
    match (chain_id, resolver_address) {
        (Some(_), Some(_)) | (None, None) => {}
        _ => {
            return Err(anyhow::anyhow!(
                "resolver_current rebuild requires both chain_id and resolver_address when targeting one resolver"
            ));
        }
    }

    let database_url = database.database_url.clone();
    let chain_id = chain_id.map(str::to_owned);
    let resolver_address = resolver_address.map(str::to_owned);
    let worker_manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

    tokio::task::spawn_blocking(move || -> Result<()> {
        let _guard = WORKER_CARGO_LOCK
            .lock()
            .expect("worker cargo lock must not be poisoned");
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let mut command = Command::new(cargo);
        command
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(worker_manifest_path)
            .arg("--")
            .arg("resolver-current")
            .arg("rebuild")
            .arg("--database-url")
            .arg(&database_url);
        if let (Some(chain_id), Some(resolver_address)) =
            (chain_id.as_deref(), resolver_address.as_deref())
        {
            command.arg("--chain-id").arg(chain_id);
            command.arg("--resolver-address").arg(resolver_address);
        }

        let output = command.output().with_context(|| {
            format!(
                "failed to invoke worker resolver_current rebuild for {}",
                resolver_address.as_deref().unwrap_or("all")
            )
        })?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "worker resolver_current rebuild failed for {}\nstdout:\n{}\nstderr:\n{}",
                resolver_address.as_deref().unwrap_or("all"),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ));
        }

        Ok(())
    })
    .await
    .context("worker resolver_current rebuild task panicked")??;

    Ok(())
}

async fn replay_all_current_projections(database: &HarnessDatabase) -> Result<()> {
    let database_url = database.database_url.clone();
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
            .arg("replay")
            .arg("all-current-projections")
            .arg("--database-url")
            .arg(&database_url)
            .output()
            .context("failed to invoke bigname-worker replay all-current-projections")?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "bigname-worker replay all-current-projections failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ));
        }

        Ok(())
    })
    .await
    .context("worker all-current-projections replay task panicked")??;

    Ok(())
}

async fn replay_raw_fact_normalized_events_for_blocks(
    database: &HarnessDatabase,
    deployment_profile: &str,
    chain: &str,
    block_hashes: &[&str],
) -> Result<()> {
    let database_url = database.database_url.clone();
    let deployment_profile = deployment_profile.to_owned();
    let chain = chain.to_owned();
    let block_hashes = block_hashes
        .iter()
        .map(|block_hash| (*block_hash).to_owned())
        .collect::<Vec<_>>();
    let indexer_manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/indexer/Cargo.toml");

    tokio::task::spawn_blocking(move || -> Result<()> {
                let _guard = WORKER_CARGO_LOCK
                    .lock()
                    .expect("worker cargo lock must not be poisoned");
                let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
                let mut command = Command::new(cargo);
                command
                    .arg("run")
                    .arg("--quiet")
                    .arg("--manifest-path")
                    .arg(indexer_manifest_path)
                    .arg("--")
                    .arg("replay")
                    .arg("normalized-events")
                    .arg("--database-url")
                    .arg(&database_url)
                    .arg("--deployment-profile")
                    .arg(&deployment_profile)
                    .arg("--chain")
                    .arg(&chain);
                for block_hash in &block_hashes {
                    command.arg("--block-hash").arg(block_hash);
                }

                let output = command.output().with_context(|| {
                    format!(
                        "failed to invoke bigname-indexer raw-fact normalized-event replay for {chain}"
                    )
                })?;

                if !output.status.success() {
                    return Err(anyhow::anyhow!(
                        "bigname-indexer raw-fact normalized-event replay failed for {chain}\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr),
                    ));
                }

                Ok(())
            })
            .await
            .context("indexer raw-fact normalized-event replay task panicked")??;

    Ok(())
}

async fn seed_raw_fact_replay_probe(
    database: &HarnessDatabase,
    chain: &str,
    block_hash: &str,
    watched_address: &str,
) -> Result<()> {
    let manifest_id = database
        .insert_manifest(
            "ens",
            RAW_REPLAY_PROBE_SOURCE_FAMILY,
            chain,
            "ens_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let contract_instance_id = Uuid::from_u128(0xc0a05);
    seed_active_replay_contract(
        database,
        manifest_id,
        contract_instance_id,
        chain,
        RAW_REPLAY_PROBE_CONTRACT_ROLE,
        watched_address,
    )
    .await?;

    bigname_storage::upsert_chain_lineage_blocks(
        &database.pool,
        &[bigname_storage::ChainLineageBlock {
            chain_id: chain.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(
                "0xfeed000000000000000000000000000000000000000000000000000000000000".to_owned(),
            ),
            block_number: 303,
            block_timestamp: timestamp(1_717_193_303),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await
    .context("failed to seed chaos raw-fact replay chain lineage")?;
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            chain,
            block_hash,
            Some("0xfeed000000000000000000000000000000000000000000000000000000000000"),
            303,
            1_717_193_303,
        )],
    )
    .await
    .context("failed to seed chaos raw-fact replay raw block")?;
    bigname_storage::upsert_raw_logs(
        &database.pool,
        &[bigname_storage::RawLog {
            chain_id: chain.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 303,
            transaction_hash: "0xfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: watched_address.to_ascii_lowercase(),
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
    .context("failed to seed chaos raw-fact replay raw log")?;

    Ok(())
}

async fn seed_active_replay_contract(
    database: &HarnessDatabase,
    manifest_id: i64,
    contract_instance_id: Uuid,
    chain: &str,
    role: &str,
    address: &str,
) -> Result<()> {
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
    .context("failed to seed chaos replay contract instance")?;
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
                VALUES ($1, 'contract', $2, $3, $4, $2, 'none')
                "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(&database.pool)
    .await
    .context("failed to seed chaos replay manifest contract instance")?;
    sqlx::query(
        r#"
                INSERT INTO contract_instance_addresses (
                    contract_instance_id,
                    chain_id,
                    address,
                    source_manifest_id
                )
                VALUES ($1, $2, $3, $4)
                "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(address)
    .bind(manifest_id)
    .execute(&database.pool)
    .await
    .context("failed to seed chaos replay active contract address")?;

    Ok(())
}

async fn seed_basenames_l2_resolver_profile_gate(
    database: &HarnessDatabase,
    seed_contract_instance_id: Uuid,
    seed_address: &str,
    dynamic_resolvers: &[(Uuid, &str)],
    supported_resolver_addresses: &[&str],
    unsupported_resolver_addresses: &[&str],
) -> Result<()> {
    let resolver_manifest_id = database
        .insert_manifest(
            "basenames",
            "basenames_base_resolver",
            "base-mainnet",
            "basenames_v1",
            71,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let registry_manifest_id = database
        .insert_manifest(
            "basenames",
            "basenames_base_registry",
            "base-mainnet",
            "basenames_v1",
            72,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let registry_contract_instance_id = Uuid::from_u128(0xc95f);

    insert_basenames_resolver_profile_contract(
        database,
        seed_contract_instance_id,
        seed_address,
        resolver_manifest_id,
        "contract",
    )
    .await?;
    insert_basenames_resolver_profile_manifest_contract(
        database,
        resolver_manifest_id,
        "resolver",
        seed_contract_instance_id,
        seed_address,
    )
    .await?;
    insert_basenames_resolver_profile_contract(
        database,
        registry_contract_instance_id,
        "0x000000000000000000000000000000000000c95f",
        registry_manifest_id,
        "root",
    )
    .await?;

    for (contract_instance_id, address) in dynamic_resolvers {
        insert_basenames_resolver_profile_contract(
            database,
            *contract_instance_id,
            address,
            resolver_manifest_id,
            "contract",
        )
        .await?;
        sqlx::query(
            r#"
                    INSERT INTO discovery_edges (
                        chain_id,
                        edge_kind,
                        from_contract_instance_id,
                        to_contract_instance_id,
                        discovery_source,
                        source_manifest_id,
                        admission,
                        provenance
                    )
                    VALUES (
                        'base-mainnet',
                        'resolver',
                        $1,
                        $2,
                        $3,
                        $4,
                        'conformance',
                        '{}'::jsonb
                    )
                    "#,
        )
        .bind(registry_contract_instance_id)
        .bind(contract_instance_id)
        .bind(format!("conformance:basenames-dynamic-resolver:{address}"))
        .bind(registry_manifest_id)
        .execute(&database.pool)
        .await
        .context("failed to seed Basenames dynamic resolver discovery edge")?;
    }

    let mut code_hashes = vec![basenames_resolver_profile_code_hash(
        seed_address,
        BASENAMES_L2_RESOLVER_PROFILE_CODE_HASH,
    )];
    code_hashes.extend(supported_resolver_addresses.iter().map(|address| {
        basenames_resolver_profile_code_hash(address, BASENAMES_L2_RESOLVER_PROFILE_CODE_HASH)
    }));
    code_hashes.extend(unsupported_resolver_addresses.iter().map(|address| {
        basenames_resolver_profile_code_hash(
            address,
            BASENAMES_UNSUPPORTED_RESOLVER_PROFILE_CODE_HASH,
        )
    }));
    bigname_storage::upsert_raw_code_hashes(&database.pool, &code_hashes)
        .await
        .context("failed to seed Basenames resolver profile code hashes")?;

    Ok(())
}

async fn insert_basenames_resolver_profile_contract(
    database: &HarnessDatabase,
    contract_instance_id: Uuid,
    address: &str,
    source_manifest_id: i64,
    contract_kind: &str,
) -> Result<()> {
    sqlx::query(
        r#"
                INSERT INTO contract_instances (
                    contract_instance_id,
                    chain_id,
                    contract_kind,
                    provenance
                )
                VALUES ($1, 'base-mainnet', $2, '{}'::jsonb)
                "#,
    )
    .bind(contract_instance_id)
    .bind(contract_kind)
    .execute(&database.pool)
    .await
    .context("failed to seed Basenames resolver profile contract instance")?;
    sqlx::query(
        r#"
                INSERT INTO contract_instance_addresses (
                    contract_instance_id,
                    chain_id,
                    address,
                    source_manifest_id,
                    provenance
                )
                VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
                "#,
    )
    .bind(contract_instance_id)
    .bind(address)
    .bind(source_manifest_id)
    .execute(&database.pool)
    .await
    .context("failed to seed Basenames resolver profile contract address")?;

    Ok(())
}

async fn insert_basenames_resolver_profile_manifest_contract(
    database: &HarnessDatabase,
    manifest_id: i64,
    role: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
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
    .context("failed to seed Basenames resolver profile manifest contract")?;

    Ok(())
}

fn basenames_resolver_profile_code_hash(
    address: &str,
    code_hash: &str,
) -> bigname_storage::RawCodeHash {
    bigname_storage::RawCodeHash {
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0xreplay-basenames-resolver-profile-code-hash".to_owned(),
        block_number: 41,
        contract_address: address.to_ascii_lowercase(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 5,
        canonicality_state: CanonicalityState::Finalized,
    }
}

async fn seed_completed_backfill_job(
    database: &HarnessDatabase,
) -> Result<bigname_storage::BackfillJobRecord> {
    let created = bigname_storage::create_backfill_job(
        &database.pool,
        &bigname_storage::BackfillJobCreate {
            deployment_profile: "conformance".to_owned(),
            chain_id: "base-mainnet".to_owned(),
            source_identity: json!({
                "source_family": "conformance_backfill",
                "fixture": "phase9-backfilled-data-consumer-conformance-job",
            }),
            scan_mode: "synthetic-local".to_owned(),
            range_start_block_number: 98,
            range_end_block_number: 261,
            idempotency_key: "conformance-backfilled-data-consumer-routes".to_owned(),
            ranges: vec![
                bigname_storage::BackfillRangeSpec {
                    range_start_block_number: 98,
                    range_end_block_number: 180,
                },
                bigname_storage::BackfillRangeSpec {
                    range_start_block_number: 181,
                    range_end_block_number: 261,
                },
            ],
        },
    )
    .await
    .context("failed to create completed backfill job for conformance")?;

    for (index, range) in created.ranges.iter().enumerate() {
        let lease_owner = "conformance-backfill";
        let lease_token = format!("conformance-backfill-lease-{index}");
        let lease_expires_at =
            OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
                .context("failed to build conformance backfill lease deadline")?;
        let reserved = bigname_storage::reserve_backfill_range(
            &database.pool,
            created.job.backfill_job_id,
            lease_owner,
            &lease_token,
            lease_expires_at,
        )
        .await
        .context("failed to reserve conformance backfill range")?
        .context("conformance backfill range should be reservable")?;
        anyhow::ensure!(
            reserved.backfill_range_id == range.backfill_range_id,
            "reserved unexpected backfill range {} instead of {}",
            reserved.backfill_range_id,
            range.backfill_range_id
        );

        bigname_storage::advance_backfill_range(
            &database.pool,
            range.backfill_range_id,
            &lease_token,
            range.range_end_block_number,
        )
        .await
        .context("failed to advance conformance backfill range to completion")?;
        bigname_storage::complete_backfill_range(
            &database.pool,
            range.backfill_range_id,
            &lease_token,
        )
        .await
        .context("failed to complete conformance backfill range")?;
    }

    let job = bigname_storage::load_backfill_job(&database.pool, created.job.backfill_job_id)
        .await
        .context("failed to load completed conformance backfill job")?
        .context("completed conformance backfill job must exist")?;
    let ranges = bigname_storage::load_backfill_ranges(&database.pool, created.job.backfill_job_id)
        .await
        .context("failed to load completed conformance backfill ranges")?;

    anyhow::ensure!(
        job.status == bigname_storage::BackfillLifecycleStatus::Completed,
        "conformance backfill job should be completed, got {}",
        job.status.as_str()
    );
    anyhow::ensure!(
        ranges.iter().all(|range| {
            range.status == bigname_storage::BackfillLifecycleStatus::Completed
                && range.checkpoint_block_number == range.range_end_block_number
                && range.completed_at.is_some()
        }),
        "all conformance backfill ranges must be completed at their declared range end"
    );

    Ok(bigname_storage::BackfillJobRecord { job, ranges })
}

async fn set_normalized_events_canonicality(
    database: &HarnessDatabase,
    event_identities: &[&str],
    state: CanonicalityState,
) -> Result<()> {
    let event_identities = event_identities
        .iter()
        .map(|identity| (*identity).to_owned())
        .collect::<Vec<_>>();
    let updated = sqlx::query(
        r#"
                UPDATE normalized_events
                SET canonicality_state = $1::canonicality_state
                WHERE event_identity = ANY($2::TEXT[])
                "#,
    )
    .bind(state.as_str())
    .bind(&event_identities)
    .execute(&database.pool)
    .await
    .context("failed to update normalized_events canonicality for conformance")?
    .rows_affected();

    anyhow::ensure!(
        updated == event_identities.len() as u64,
        "expected to update {} normalized_events rows to {}, updated {updated}",
        event_identities.len(),
        state.as_str()
    );

    Ok(())
}

async fn set_raw_blocks_canonicality(
    database: &HarnessDatabase,
    chain_id: &str,
    block_hashes: &[&str],
    state: CanonicalityState,
) -> Result<()> {
    let block_hashes = block_hashes
        .iter()
        .map(|block_hash| (*block_hash).to_owned())
        .collect::<Vec<_>>();
    let updated = sqlx::query(
        r#"
                UPDATE chain_lineage
                SET canonicality_state = $1::canonicality_state
                WHERE chain_id = $2
                  AND block_hash = ANY($3::TEXT[])
                "#,
    )
    .bind(state.as_str())
    .bind(chain_id)
    .bind(&block_hashes)
    .execute(&database.pool)
    .await
    .context("failed to update chain_lineage canonicality for conformance")?
    .rows_affected();

    anyhow::ensure!(
        updated == block_hashes.len() as u64,
        "expected to update {} chain_lineage rows to {}, updated {updated}",
        block_hashes.len(),
        state.as_str()
    );

    Ok(())
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
            "derivation_kind": "primary_name_route_bootstrap",
        })
    );
    assert_primary_name_route_common_invariants(payload);
    assert!(payload.last_updated.ends_with('Z'));
}

fn assert_primary_name_persisted_readback_invariants_for_namespace(
    payload: &PrimaryNameResponse,
    namespace: &str,
    execution_trace_id: Uuid,
    finished_at: OffsetDateTime,
) {
    assert_eq!(
        payload.provenance,
        json!({
            "normalized_event_ids": [],
            "raw_fact_refs": [],
            "manifest_versions": primary_name_execution_manifest_versions_for_namespace(namespace),
            "execution_trace_id": execution_trace_id.to_string(),
            "derivation_kind": "primary_name_route_bootstrap",
        })
    );
    assert_primary_name_route_common_invariants(payload);
    assert_eq!(payload.last_updated, format_timestamp(finished_at));
}

fn assert_primary_name_persisted_readback_invariants(
    payload: &PrimaryNameResponse,
    execution_trace_id: Uuid,
    finished_at: OffsetDateTime,
) {
    assert_primary_name_persisted_readback_invariants_for_namespace(
        payload,
        "ens",
        execution_trace_id,
        finished_at,
    );
}

fn seeded_primary_name_claim_provenance() -> Value {
    json!({})
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

fn assert_page_walk_matches_base_prefix(base_rows: &[Value], page_rows: &[&[Value]]) {
    let base_rows = stable_row_strings(base_rows);
    let walked_rows = page_rows
        .iter()
        .flat_map(|rows| stable_row_strings(rows))
        .collect::<Vec<_>>();

    assert_eq!(
        walked_rows,
        base_rows
            .iter()
            .take(walked_rows.len())
            .cloned()
            .collect::<Vec<_>>()
    );
}

async fn read_ok_json<T: DeserializeOwned>(
    database: &HarnessDatabase,
    uri: impl AsRef<str>,
) -> Result<T> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri.as_ref())
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .with_context(|| format!("conformance request failed for {}", uri.as_ref()))?;

    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

fn append_query(uri: &str, query: &str) -> String {
    let separator = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{separator}{query}")
}

async fn assert_large_history_route_pagination(
    database: &HarnessDatabase,
    base_uri: &str,
    base_payload: &HistoryResponse,
    page_size: u64,
) -> Result<()> {
    let first_payload: HistoryResponse = read_ok_json(
        database,
        append_query(base_uri, &format!("page_size={page_size}")),
    )
    .await?;
    let first_cursor = first_payload
        .page
        .next_cursor
        .clone()
        .expect("larger history first page must include next_cursor");

    let second_payload: HistoryResponse = read_ok_json(
        database,
        append_query(
            base_uri,
            &format!("page_size={page_size}&cursor={first_cursor}"),
        ),
    )
    .await?;
    let second_cursor = second_payload
        .page
        .next_cursor
        .clone()
        .expect("larger history second page must include next_cursor");

    let third_payload: HistoryResponse = read_ok_json(
        database,
        append_query(
            base_uri,
            &format!("page_size={page_size}&cursor={second_cursor}"),
        ),
    )
    .await?;
    let replay_payload: HistoryResponse = read_ok_json(
        database,
        append_query(
            base_uri,
            &format!("page_size={page_size}&cursor={first_cursor}"),
        ),
    )
    .await?;

    assert_replay_stable_pagination(
        &base_payload.data,
        &base_payload.page,
        &first_payload.data,
        &first_payload.page,
        &second_payload.data,
        &second_payload.page,
        &replay_payload.data,
        &replay_payload.page,
        "chain_position_desc",
        base_payload.page.page_size,
        page_size,
    );
    assert_page_walk_matches_base_prefix(
        &base_payload.data,
        &[
            &first_payload.data,
            &second_payload.data,
            &third_payload.data,
        ],
    );
    assert_eq!(third_payload.page.next_cursor, None);

    Ok(())
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
    let chain_id = chain_id_for_namespace(&namespace).to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace,
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: vec![direct_child_labelhash(display_name)],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id,
        block_hash: format!("0xsurface{block_number:02x}"),
        block_number,
        provenance: json!({"seed": "children_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn direct_child_labelhash(display_name: &str) -> String {
    let child_label = display_name.split('.').next().unwrap_or(display_name);

    format!(
        "0x{}",
        alloy_primitives::hex::encode(alloy_primitives::keccak256(child_label.as_bytes()))
    )
}

fn declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let namespace = parent_logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("parent_logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::ChildrenCurrentRow {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_logical_name_id: child_logical_name_id.to_owned(),
        surface_class: "declared".to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        labelhash: Some(direct_child_labelhash(display_name)),
        owner: None,
        registrant: None,
        provenance: json!({
            "normalized_event_ids": [normalized_event_id],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "children_current_rebuild",
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xblock{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_172_000 + block_number),
    }
}

fn ens_v2_declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let mut row = declared_child_row(
        parent_logical_name_id,
        child_logical_name_id,
        display_name,
        namehash,
        normalized_event_id,
        block_number,
    );
    row.provenance = json!({
        "normalized_event_ids": [normalized_event_id],
        "raw_fact_refs": [{
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_number": block_number,
        }],
        "manifest_versions": [{
            "manifest_version": 11,
            "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
            "source_manifest_id": null,
        }],
        "execution_trace_id": null,
        "derivation_kind": "children_current_rebuild",
    });
    row.chain_positions = json!({
        "ethereum": {
            "chain_id": "ethereum-sepolia",
            "block_number": block_number,
            "block_hash": format!("0xensv2child{block_number:02x}"),
            "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
        }
    });
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "ethereum-sepolia": "finalized",
        }
    });
    row.manifest_version = 11;
    row
}

fn chain_id_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base-mainnet",
        _ => "ethereum-mainnet",
    }
}

fn chain_slot_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "base",
        _ => "ethereum",
    }
}

fn source_family_for_namespace(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => "basenames_base_registry",
        _ => "ens_v1_registry_l1",
    }
}

const ENSV2_CHAIN_ID: &str = "ethereum-mainnet";
const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
const ENSV2_RESOLVER_SOURCE_FAMILY: &str = "ens_v2_resolver_l1";
const ENSV2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ENSV2_PERMISSIONS_DERIVATION_KIND: &str = "ens_v2_permissions";
const ENSV2_RESOLVER_DERIVATION_KIND: &str = "ens_v2_resolver";
const ENSV2_HISTORY_CHAIN_ID: &str = "ethereum-sepolia";

#[derive(Clone, Debug)]
struct EnsV2HistoryFixture {
    address: &'static str,
    controller: &'static str,
    current_logical_name_id: &'static str,
    historical_logical_name_id: &'static str,
    pending_logical_name_id: &'static str,
    observed_logical_name_id: &'static str,
    current_resource_id: Uuid,
    current_token_lineage_id: Uuid,
    current_surface_binding_id: Uuid,
    historical_resource_id: Uuid,
    historical_token_lineage_id: Uuid,
    observed_resource_id: Uuid,
    observed_token_lineage_id: Uuid,
    base_block_number: i64,
}

impl EnsV2HistoryFixture {
    fn new() -> Self {
        Self {
            address: "0x0000000000000000000000000000000000000b0b",
            controller: "0x0000000000000000000000000000000000000c0c",
            current_logical_name_id: "ens:current-v2.eth",
            historical_logical_name_id: "ens:historical-v2.eth",
            pending_logical_name_id: "ens:pending-v2.eth",
            observed_logical_name_id: "ens:observed-v2.eth",
            current_resource_id: Uuid::from_u128(0xa24a),
            current_token_lineage_id: Uuid::from_u128(0xa24b),
            current_surface_binding_id: Uuid::from_u128(0xb24a),
            historical_resource_id: Uuid::from_u128(0xa24c),
            historical_token_lineage_id: Uuid::from_u128(0xa24d),
            observed_resource_id: Uuid::from_u128(0xa24e),
            observed_token_lineage_id: Uuid::from_u128(0xa24f),
            base_block_number: 430,
        }
    }

    async fn seed(&self, database: &HarnessDatabase) -> Result<()> {
        let raw_blocks = self.raw_blocks();
        bigname_storage::upsert_raw_blocks(&database.pool, &raw_blocks)
            .await
            .context("failed to upsert ENSv2 history fixture raw blocks")?;

        let token_lineages = self.token_lineages();
        bigname_storage::upsert_token_lineages(&database.pool, &token_lineages)
            .await
            .context("failed to upsert ENSv2 history fixture token lineages")?;

        let resources = self.resources();
        bigname_storage::upsert_resources(&database.pool, &resources)
            .await
            .context("failed to upsert ENSv2 history fixture resources")?;

        let name_surfaces = self.name_surfaces();
        bigname_storage::upsert_name_surfaces(&database.pool, &name_surfaces)
            .await
            .context("failed to upsert ENSv2 history fixture name surfaces")?;

        let surface_bindings = self.surface_bindings();
        bigname_storage::upsert_surface_bindings(&database.pool, &surface_bindings)
            .await
            .context("failed to upsert ENSv2 history fixture surface bindings")?;

        let current_rows = self.address_name_current_rows();
        bigname_storage::upsert_address_names_current_rows(&database.pool, &current_rows)
            .await
            .context("failed to upsert ENSv2 history fixture address-name anchors")?;

        let events = self.normalized_events();
        bigname_storage::upsert_normalized_events(&database.pool, &events)
            .await
            .context("failed to upsert ENSv2 history fixture normalized events")?;

        Ok(())
    }

    fn expected_registrant_history_event_identities(&self) -> Vec<&'static str> {
        vec![
            "ensv2-current-resource",
            "ensv2-current-surface",
            "ensv2-historical-surface",
            "ensv2-historical-resource",
            "ensv2-historical-grant",
            "ensv2-historical-authority",
            "ensv2-pending-grant",
            "ensv2-pending-surface",
        ]
    }

    fn expected_effective_controller_history_event_identities(&self) -> Vec<&'static str> {
        vec![
            "ensv2-current-resource",
            "ensv2-current-surface",
            "ensv2-historical-surface",
            "ensv2-historical-resource",
            "ensv2-historical-grant",
            "ensv2-historical-authority",
        ]
    }

    fn raw_blocks(&self) -> Vec<RawBlock> {
        (self.base_block_number..=self.base_block_number + 11)
            .map(|block_number| {
                raw_block(
                    ENSV2_HISTORY_CHAIN_ID,
                    &ens_v2_history_block_hash(block_number),
                    None,
                    block_number,
                    1_700_000_000 + block_number,
                )
            })
            .collect()
    }

    fn token_lineages(&self) -> [TokenLineage; 3] {
        [
            ens_v2_history_token_lineage(self.current_token_lineage_id, self.base_block_number),
            ens_v2_history_token_lineage(
                self.historical_token_lineage_id,
                self.base_block_number + 1,
            ),
            ens_v2_history_token_lineage(
                self.observed_token_lineage_id,
                self.base_block_number + 9,
            ),
        ]
    }

    fn resources(&self) -> [Resource; 3] {
        [
            ens_v2_history_resource(
                self.current_resource_id,
                Some(self.current_token_lineage_id),
                self.base_block_number,
                "ens_v2_history_current_resource",
            ),
            ens_v2_history_resource(
                self.historical_resource_id,
                Some(self.historical_token_lineage_id),
                self.base_block_number + 1,
                "ens_v2_history_historical_resource",
            ),
            ens_v2_history_resource(
                self.observed_resource_id,
                Some(self.observed_token_lineage_id),
                self.base_block_number + 9,
                "ens_v2_history_observed_resource",
            ),
        ]
    }

    fn name_surfaces(&self) -> [NameSurface; 4] {
        [
            ens_v2_history_name_surface(self.current_logical_name_id, self.base_block_number),
            ens_v2_history_name_surface(
                self.historical_logical_name_id,
                self.base_block_number + 1,
            ),
            ens_v2_history_name_surface(self.pending_logical_name_id, self.base_block_number + 2),
            ens_v2_history_name_surface(self.observed_logical_name_id, self.base_block_number + 9),
        ]
    }

    fn surface_bindings(&self) -> [SurfaceBinding; 1] {
        [ens_v2_history_surface_binding(
            self.current_surface_binding_id,
            self.current_logical_name_id,
            self.current_resource_id,
            self.base_block_number,
            1_717_182_430,
        )]
    }

    fn address_name_current_rows(&self) -> [bigname_storage::AddressNameCurrentRow; 2] {
        [
            ens_v2_history_address_name_current_row(
                self.address,
                self.current_logical_name_id,
                bigname_storage::AddressNameRelation::Registrant,
                self.current_surface_binding_id,
                self.current_resource_id,
                Some(self.current_token_lineage_id),
                self.base_block_number,
            ),
            ens_v2_history_address_name_current_row(
                self.controller,
                self.current_logical_name_id,
                bigname_storage::AddressNameRelation::EffectiveController,
                self.current_surface_binding_id,
                self.current_resource_id,
                Some(self.current_token_lineage_id),
                self.base_block_number + 1,
            ),
        ]
    }

    fn normalized_events(&self) -> Vec<NormalizedEvent> {
        vec![
            ens_v2_history_event(
                "ensv2-current-resource",
                None,
                Some(self.current_resource_id),
                self.base_block_number + 7,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_event(
                "ensv2-current-surface",
                Some(self.current_logical_name_id),
                None,
                self.base_block_number + 6,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_event(
                "ensv2-historical-surface",
                Some(self.historical_logical_name_id),
                None,
                self.base_block_number + 5,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_event(
                "ensv2-historical-resource",
                None,
                Some(self.historical_resource_id),
                self.base_block_number + 4,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_registry_match_event(
                "ensv2-historical-grant",
                self.historical_logical_name_id,
                Some(self.historical_resource_id),
                "RegistrationGranted",
                self.base_block_number + 3,
                json!({
                    "registrant": self.address.to_ascii_uppercase(),
                }),
                CanonicalityState::Canonical,
            ),
            ens_v2_history_registry_match_event(
                "ensv2-historical-authority",
                self.historical_logical_name_id,
                Some(self.historical_resource_id),
                "AuthorityTransferred",
                self.base_block_number + 2,
                json!({
                    "owner": self.controller.to_ascii_uppercase(),
                }),
                CanonicalityState::Canonical,
            ),
            ens_v2_history_registry_match_event(
                "ensv2-pending-grant",
                self.pending_logical_name_id,
                None,
                "RegistrationGranted",
                self.base_block_number + 1,
                json!({
                    "registrant": self.address.to_ascii_uppercase(),
                    "resource_pending": true,
                }),
                CanonicalityState::Canonical,
            ),
            ens_v2_history_event(
                "ensv2-pending-surface",
                Some(self.pending_logical_name_id),
                None,
                self.base_block_number,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_event(
                "ensv2-observed-anchor-leak-surface",
                Some(self.observed_logical_name_id),
                None,
                self.base_block_number + 11,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_event(
                "ensv2-observed-anchor-leak-resource",
                None,
                Some(self.observed_resource_id),
                self.base_block_number + 10,
                0,
                CanonicalityState::Canonical,
            ),
            ens_v2_history_registry_match_event(
                "ensv2-observed-grant",
                self.observed_logical_name_id,
                Some(self.observed_resource_id),
                "RegistrationGranted",
                self.base_block_number + 9,
                json!({
                    "registrant": self.address.to_ascii_uppercase(),
                }),
                CanonicalityState::Observed,
            ),
        ]
    }
}

#[derive(Clone, Debug)]
struct EnsV2DeclaredChildFixture {
    parent_logical_name_id: String,
    parent_normalized_name: String,
    parent_namehash: String,
    parent_resource_id: Uuid,
    child_logical_name_id: String,
    child_normalized_name: String,
    child_namehash: String,
    child_resource_id: Uuid,
    parent_contract_instance_id: String,
    child_registry_contract_instance_id: String,
    child_registry_address: String,
    base_block_number: i64,
}

impl EnsV2DeclaredChildFixture {
    fn new(
        parent_logical_name_id: &str,
        child_logical_name_id: &str,
        parent_resource_id: Uuid,
        child_resource_id: Uuid,
        base_block_number: i64,
    ) -> Self {
        let parent_normalized_name = ens_namespace_normalized_name(parent_logical_name_id);
        let child_normalized_name = ens_namespace_normalized_name(child_logical_name_id);
        assert!(
            is_direct_child_name(&parent_normalized_name, &child_normalized_name),
            "ENSv2 child fixtures only model declared direct children"
        );

        Self {
            parent_logical_name_id: parent_logical_name_id.to_owned(),
            parent_namehash: format!("namehash:{parent_normalized_name}"),
            parent_resource_id,
            child_logical_name_id: child_logical_name_id.to_owned(),
            child_namehash: format!("namehash:{child_normalized_name}"),
            child_resource_id,
            parent_contract_instance_id: format!("ensv2:registry:{parent_normalized_name}"),
            child_registry_contract_instance_id: format!(
                "ensv2:subregistry:{parent_normalized_name}"
            ),
            child_registry_address: "0x0000000000000000000000000000000000000c01".to_owned(),
            parent_normalized_name,
            child_normalized_name,
            base_block_number,
        }
    }

    fn name_surfaces(&self) -> [NameSurface; 2] {
        [
            collection_name_surface(
                &self.parent_logical_name_id,
                &self.parent_normalized_name,
                &self.parent_namehash,
                self.base_block_number,
            ),
            collection_name_surface(
                &self.child_logical_name_id,
                &self.child_normalized_name,
                &self.child_namehash,
                self.base_block_number + 1,
            ),
        ]
    }

    fn resources(&self) -> [Resource; 2] {
        [
            ens_v2_resource(
                self.parent_resource_id,
                self.base_block_number,
                "ens_v2_parent_resource",
            ),
            ens_v2_resource(
                self.child_resource_id,
                self.base_block_number + 1,
                "ens_v2_child_resource",
            ),
        ]
    }

    fn normalized_events(&self) -> [NormalizedEvent; 3] {
        [
            self.subregistry_changed_event(),
            self.parent_changed_event(),
            self.child_registration_event(),
        ]
    }

    async fn expected_children_provenance(&self, database: &HarnessDatabase) -> Result<Value> {
        let events = self.normalized_events();
        let event_identities = events
            .iter()
            .map(|event| event.event_identity.clone())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            r#"
                    SELECT event_identity, normalized_event_id
                    FROM normalized_events
                    WHERE event_identity = ANY($1::TEXT[])
                    "#,
        )
        .bind(&event_identities)
        .fetch_all(&database.pool)
        .await
        .context("failed to load ENSv2 child fixture normalized event IDs")?;
        let mut event_ids_by_identity = std::collections::BTreeMap::new();
        for row in rows {
            event_ids_by_identity.insert(
                row.try_get::<String, _>("event_identity")
                    .context("missing ENSv2 fixture event_identity")?,
                row.try_get::<i64, _>("normalized_event_id")
                    .context("missing ENSv2 fixture normalized_event_id")?,
            );
        }
        let normalized_event_ids = event_identities
            .iter()
            .map(|event_identity| {
                event_ids_by_identity
                    .get(event_identity)
                    .with_context(|| {
                        format!("missing normalized_event_id for seeded event {event_identity}")
                    })
                    .map(ToString::to_string)
            })
            .collect::<Result<Vec<_>>>()?;
        let raw_fact_refs = events
            .iter()
            .map(|event| event.raw_fact_ref.clone())
            .collect::<Vec<_>>();

        Ok(json!({
            "normalized_event_ids": normalized_event_ids,
            "raw_fact_refs": raw_fact_refs,
            "manifest_versions": [
                {
                    "manifest_version": 3,
                    "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
                    "source_manifest_id": null
                },
                {
                    "manifest_version": 2,
                    "source_family": ENSV2_ROOT_SOURCE_FAMILY,
                    "source_manifest_id": null
                }
            ],
            "derivation_kind": "children_current_rebuild"
        }))
    }

    async fn seed(&self, database: &HarnessDatabase) -> Result<()> {
        bigname_storage::upsert_name_surfaces(&database.pool, &self.name_surfaces())
            .await
            .context("failed to upsert ENSv2 child fixture name surfaces")?;
        bigname_storage::upsert_resources(&database.pool, &self.resources())
            .await
            .context("failed to upsert ENSv2 child fixture resources")?;
        seed_ens_v2_event_fixture_inputs(&database.pool, &self.normalized_events())
            .await
            .context("failed to seed ENSv2 child fixture events")?;

        Ok(())
    }

    fn subregistry_changed_event(&self) -> NormalizedEvent {
        let mut event = ens_v2_registry_event(
            &format!(
                "conformance:ensv2:{}:subregistry",
                self.parent_normalized_name
            ),
            Some(&self.parent_logical_name_id),
            Some(self.parent_resource_id),
            "SubregistryChanged",
            self.base_block_number,
            0,
            json!({}),
            json!({
                "from_contract_instance_id": self.parent_contract_instance_id,
                "to_contract_instance_id": self.child_registry_contract_instance_id,
            }),
            json!({
                "kind": "raw_log",
                "event_identity": format!(
                    "conformance:ensv2:{}:subregistry",
                    self.parent_normalized_name
                ),
            }),
        );
        event.source_family = ENSV2_ROOT_SOURCE_FAMILY.to_owned();
        event.manifest_version = 2;
        event
    }

    fn parent_changed_event(&self) -> NormalizedEvent {
        ens_v2_registry_event(
            &format!("conformance:ensv2:{}:parent", self.parent_normalized_name),
            Some(&self.parent_logical_name_id),
            Some(self.parent_resource_id),
            "ParentChanged",
            self.base_block_number + 1,
            0,
            json!({}),
            json!({
                "registry_contract_instance_id": self.child_registry_contract_instance_id,
                "parent_contract_instance_id": self.parent_contract_instance_id,
                "registry_name": self.parent_normalized_name,
            }),
            json!({
                "kind": "raw_log",
                "event_identity": format!(
                    "conformance:ensv2:{}:parent",
                    self.parent_normalized_name
                ),
                "emitting_address": self.child_registry_address,
            }),
        )
    }

    fn child_registration_event(&self) -> NormalizedEvent {
        ens_v2_registry_event(
            &format!("conformance:ensv2:{}:child", self.child_normalized_name),
            Some(&self.child_logical_name_id),
            Some(self.child_resource_id),
            "RegistrationGranted",
            self.base_block_number + 2,
            0,
            json!({}),
            json!({
                "registry_contract_instance_id": self.child_registry_contract_instance_id,
                "normalized_name": self.child_normalized_name,
                "active": true,
            }),
            json!({
                "kind": "raw_log",
                "event_identity": format!(
                    "conformance:ensv2:{}:child",
                    self.child_normalized_name
                ),
                "emitting_address": self.child_registry_address,
            }),
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn seed_ens_v2_address_name_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    registrant: &str,
    controller: &str,
) -> Result<()> {
    let normalized_name = ens_namespace_normalized_name(logical_name_id);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "ethereum-sepolia",
                "0xensv2-surface",
                None,
                201,
                1_717_182_201,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xensv2-resource",
                None,
                202,
                1_717_182_202,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xensv2-binding",
                None,
                203,
                1_717_182_203,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xensv2-grant",
                None,
                204,
                1_717_182_204,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xensv2-authority",
                None,
                205,
                1_717_182_205,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xensv2-regen",
                None,
                206,
                1_717_182_206,
            ),
        ],
    )
    .await
    .context("failed to upsert raw blocks for ENSv2 address-name conformance")?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: normalized_name.clone(),
            canonical_display_name: normalized_name.clone(),
            normalized_name: normalized_name.clone(),
            dns_encoded_name: normalized_name.as_bytes().to_vec(),
            namehash: format!("namehash:{normalized_name}"),
            labelhashes: vec![format!("labelhash:{normalized_name}")],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-surface".to_owned(),
            block_number: 201,
            provenance: json!({"seed": "ensv2_address_name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await
    .context("failed to upsert ENSv2 address-name surface for conformance")?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-resource".to_owned(),
            block_number: 202,
            provenance: json!({"seed": "ensv2_address_name_token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await
    .context("failed to upsert ENSv2 address-name token lineage for conformance")?;
    bigname_storage::upsert_resources(
                &database.pool,
                &[Resource {
                    resource_id,
                    token_lineage_id: Some(token_lineage_id),
                    chain_id: "ethereum-sepolia".to_owned(),
                    block_hash: "0xensv2-resource".to_owned(),
                    block_number: 202,
                    provenance: json!({
                        "seed": "ensv2_address_name_resource",
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    }),
                    canonicality_state: CanonicalityState::Finalized,
                }],
            )
            .await
            .context("failed to upsert ENSv2 address-name resource for conformance")?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
            active_from: timestamp(1_717_182_203),
            active_to: None,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2-binding".to_owned(),
            block_number: 203,
            provenance: json!({
                "seed": "ensv2_address_name_binding",
                "binding_kind": "linked_subregistry_path",
            }),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await
    .context("failed to upsert ENSv2 address-name surface binding for conformance")?;

    bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: format!("conformance:{logical_name_id}:ensv2-grant"),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RegistrationGranted".to_owned(),
                        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
                        manifest_version: 11,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-sepolia".to_owned()),
                        block_number: Some(204),
                        block_hash: Some("0xensv2-grant".to_owned()),
                        transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-grant")),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:ensv2-grant")}),
                        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
                        canonicality_state: CanonicalityState::Finalized,
                        before_state: json!({}),
                        after_state: json!({
                            "authority_kind": "ens_v2_registry",
                            "authority_key": format!("ens-v2-registry:ethereum-sepolia:{normalized_name}:0xeac"),
                            "registrant": registrant,
                            "expiry": 1_900_000_000_i64,
                            "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                            "status": "registered",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: format!("conformance:{logical_name_id}:ensv2-authority"),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "AuthorityTransferred".to_owned(),
                        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
                        manifest_version: 11,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-sepolia".to_owned()),
                        block_number: Some(205),
                        block_hash: Some("0xensv2-authority".to_owned()),
                        transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-authority")),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:ensv2-authority")}),
                        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
                        canonicality_state: CanonicalityState::Finalized,
                        before_state: json!({
                            "owner": registrant,
                        }),
                        after_state: json!({
                            "owner": controller,
                            "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: format!("conformance:{logical_name_id}:ensv2-regen"),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "TokenRegenerated".to_owned(),
                        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
                        manifest_version: 11,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-sepolia".to_owned()),
                        block_number: Some(206),
                        block_hash: Some("0xensv2-regen".to_owned()),
                        transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-regen")),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": format!("conformance:{logical_name_id}:ensv2-regen")}),
                        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
                        canonicality_state: CanonicalityState::Finalized,
                        before_state: json!({
                            "token_id": "0x01",
                        }),
                        after_state: json!({
                            "old_token_id": "0x01",
                            "new_token_id": "0x02",
                            "resource_id": resource_id.to_string(),
                        }),
                    },
                ],
            )
            .await
            .context("failed to upsert ENSv2 address-name normalized events for conformance")?;

    Ok(())
}

async fn seed_ens_v2_event_fixture_inputs(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    let mut seen_blocks = BTreeSet::new();
    let mut blocks = Vec::new();

    for event in events {
        let (Some(chain_id), Some(block_hash), Some(block_number)) = (
            event.chain_id.as_deref(),
            event.block_hash.as_deref(),
            event.block_number,
        ) else {
            continue;
        };

        if seen_blocks.insert((chain_id.to_owned(), block_hash.to_owned())) {
            let mut block = raw_block(
                chain_id,
                block_hash,
                None,
                block_number,
                1_717_190_000 + block_number,
            );
            block.canonicality_state = CanonicalityState::Finalized;
            blocks.push(block);
        }
    }

    bigname_storage::upsert_raw_blocks(pool, &blocks)
        .await
        .context("failed to upsert ENSv2 fixture raw blocks")?;
    bigname_storage::upsert_normalized_events(pool, events)
        .await
        .context("failed to upsert ENSv2 fixture normalized events")?;

    Ok(())
}

fn ens_v2_history_name_surface(logical_name_id: &str, block_number: i64) -> NameSurface {
    let normalized_name = ens_namespace_normalized_name(logical_name_id);

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: normalized_name.clone(),
        canonical_display_name: normalized_name.clone(),
        normalized_name: normalized_name.clone(),
        dns_encoded_name: normalized_name.as_bytes().to_vec(),
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec![format!("labelhash:{normalized_name}")],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: ENSV2_HISTORY_CHAIN_ID.to_owned(),
        block_hash: ens_v2_history_block_hash(block_number),
        block_number,
        provenance: json!({"seed": "ens_v2_history_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn ens_v2_history_token_lineage(token_lineage_id: Uuid, block_number: i64) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: ENSV2_HISTORY_CHAIN_ID.to_owned(),
        block_hash: ens_v2_history_block_hash(block_number),
        block_number,
        provenance: json!({"seed": "ens_v2_history_token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn ens_v2_history_resource(
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
    seed: &str,
) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: ENSV2_HISTORY_CHAIN_ID.to_owned(),
        block_hash: ens_v2_history_block_hash(block_number),
        block_number,
        provenance: json!({
            "seed": seed,
            "upstream_resource": format!("0x{resource_id}"),
        }),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn ens_v2_history_surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    block_number: i64,
    active_from: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
        active_from: timestamp(active_from),
        active_to: None,
        chain_id: ENSV2_HISTORY_CHAIN_ID.to_owned(),
        block_hash: ens_v2_history_block_hash(block_number),
        block_number,
        provenance: json!({
            "seed": "ens_v2_history_binding",
            "binding_kind": "linked_subregistry_path",
        }),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn ens_v2_history_address_name_current_row(
    address: &str,
    logical_name_id: &str,
    relation: bigname_storage::AddressNameRelation,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
) -> bigname_storage::AddressNameCurrentRow {
    let normalized_name = ens_namespace_normalized_name(logical_name_id);

    bigname_storage::AddressNameCurrentRow {
        address: address.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: "ens".to_owned(),
        canonical_display_name: normalized_name.clone(),
        normalized_name: normalized_name.clone(),
        namehash: format!("namehash:{normalized_name}"),
        surface_binding_id,
        resource_id,
        token_lineage_id,
        binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "chain_id": ENSV2_HISTORY_CHAIN_ID,
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 11,
                "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "address_names_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [ENSV2_REGISTRY_SOURCE_FAMILY],
            "unsupported_reason": null,
            "enumeration_basis": "surface_current_relations",
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": ENSV2_HISTORY_CHAIN_ID,
                "block_number": block_number,
                "block_hash": ens_v2_history_block_hash(block_number),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                ENSV2_HISTORY_CHAIN_ID: "finalized",
            }
        }),
        manifest_version: 11,
        last_recomputed_at: timestamp(1_717_182_000 + block_number),
    }
}

fn ens_v2_history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let block_hash = ens_v2_history_block_hash(block_number);
    let transaction_hash = ens_v2_history_transaction_hash(block_number);

    NormalizedEvent {
        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
        manifest_version: 11,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ENSV2_HISTORY_CHAIN_ID,
            "event_identity": event_identity,
        }),
        ..history_event(
            event_identity,
            logical_name_id,
            resource_id,
            Some(ENSV2_HISTORY_CHAIN_ID),
            Some(block_number),
            Some(&block_hash),
            Some(&transaction_hash),
            Some(log_index),
            canonicality_state,
        )
    }
}

fn ens_v2_history_registry_match_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Option<Uuid>,
    event_kind: &str,
    block_number: i64,
    after_state: Value,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_kind: event_kind.to_owned(),
        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
        before_state: json!({}),
        after_state,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": ENSV2_HISTORY_CHAIN_ID,
            "event_identity": event_identity,
            "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
        }),
        ..ens_v2_history_event(
            event_identity,
            Some(logical_name_id),
            resource_id,
            block_number,
            0,
            canonicality_state,
        )
    }
}

fn ens_v2_permission_current_row(
    resource_id: Uuid,
    subject: &str,
    scope: PermissionScope,
    effective_powers: &[&str],
    manifest_version: i64,
    block_number: i64,
) -> PermissionsCurrentRow {
    assert!(
        !effective_powers.is_empty(),
        "current permission rows cannot represent fully revoked EAC roles"
    );
    let chain_id = permission_scope_chain_id(&scope).to_owned();
    let chain_position_key = chain_id.clone();
    let chain_position_chain_id = chain_id.clone();
    let canonicality_key = chain_id.clone();

    PermissionsCurrentRow {
        resource_id,
        subject: subject.to_owned(),
        scope,
        effective_powers: json!(effective_powers),
        grant_source: json!({
            "kind": "raw_log",
            "source_event": "EACRolesChanged",
        }),
        revocation_source: None,
        inheritance_path: json!([]),
        transfer_behavior: json!({}),
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": manifest_version,
                "source_family": ENSV2_RESOLVER_SOURCE_FAMILY,
                "chain": chain_id.clone(),
                "deployment_epoch": "ens_v2",
            }],
            "derivation_kind": "permissions_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [ENSV2_RESOLVER_SOURCE_FAMILY],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            chain_position_key: {
                "chain_id": chain_position_chain_id,
                "block_number": block_number,
                "block_hash": ens_v2_block_hash(block_number),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                canonicality_key: "finalized",
            }
        }),
        manifest_version,
        last_recomputed_at: timestamp(1_717_174_000 + block_number),
    }
}

fn ens_v2_permission_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    subject: &str,
    scope: PermissionScope,
    effective_powers: &[&str],
    manifest_version: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let chain_id = permission_scope_chain_id(&scope).to_owned();
    let emitting_address = permission_scope_emitting_address(&scope);
    let source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "resource_id": resource_id.to_string(),
        "changed_powers": effective_powers,
    });
    let grant_source = if effective_powers.is_empty() {
        json!({})
    } else {
        source.clone()
    };
    let revocation_source = if effective_powers.is_empty() {
        source
    } else {
        Value::Null
    };

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: "PermissionChanged".to_owned(),
        source_family: ENSV2_RESOLVER_SOURCE_FAMILY.to_owned(),
        manifest_version,
        source_manifest_id: None,
        chain_id: Some(chain_id.clone()),
        block_number: Some(block_number),
        block_hash: Some(ens_v2_block_hash(block_number)),
        transaction_hash: Some(ens_v2_transaction_hash(block_number)),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "event_identity": event_identity,
            "emitting_address": emitting_address,
        }),
        derivation_kind: ENSV2_PERMISSIONS_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({
            "subject": subject,
            "effective_powers": [],
        }),
        after_state: json!({
            "subject": subject,
            "scope": permission_scope_after_state(&scope),
            "effective_powers": effective_powers,
            "grant_source": grant_source,
            "revocation_source": revocation_source,
            "inheritance_path": [],
            "transfer_behavior": {},
            "source_event": "EACRolesChanged",
        }),
    }
}

fn ens_v2_record_version_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    namehash: &str,
    record_version: &str,
    manifest_version: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    ens_v2_resolver_event(
        event_identity,
        logical_name_id,
        resource_id,
        resolver_address,
        "RecordVersionChanged",
        manifest_version,
        block_number,
        log_index,
        json!({}),
        json!({
            "source_event": "VersionChanged",
            "resolver": resolver_address,
            "resolver_contract_instance_id": format!("ensv2:resolver:{resolver_address}"),
            "node": namehash,
            "record_version": record_version,
        }),
    )
}

fn ens_v2_record_changed_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    namehash: &str,
    record_family: &str,
    selector_key: Option<&str>,
    manifest_version: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let record_key = record_key_for_selector(record_family, selector_key);

    ens_v2_resolver_event(
        event_identity,
        logical_name_id,
        resource_id,
        resolver_address,
        "RecordChanged",
        manifest_version,
        block_number,
        log_index,
        json!({}),
        json!({
            "source_event": resolver_source_event_for_record_family(record_family),
            "resolver": resolver_address,
            "resolver_contract_instance_id": format!("ensv2:resolver:{resolver_address}"),
            "node": namehash,
            "record_key": record_key,
            "record_family": record_family,
            "selector_key": selector_key,
            "value_retained": false,
        }),
    )
}

fn ens_v2_registry_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    block_number: i64,
    log_index: i64,
    before_state: Value,
    after_state: Value,
    raw_fact_ref: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: ENSV2_REGISTRY_SOURCE_FAMILY.to_owned(),
        manifest_version: 3,
        source_manifest_id: None,
        chain_id: Some(ENSV2_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(ens_v2_block_hash(block_number)),
        transaction_hash: Some(ens_v2_transaction_hash(block_number)),
        log_index: Some(log_index),
        raw_fact_ref,
        derivation_kind: ENSV2_REGISTRY_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state,
        after_state,
    }
}

#[allow(clippy::too_many_arguments)]
fn ens_v2_resolver_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    event_kind: &str,
    manifest_version: i64,
    block_number: i64,
    log_index: i64,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: event_kind.to_owned(),
        source_family: ENSV2_RESOLVER_SOURCE_FAMILY.to_owned(),
        manifest_version,
        source_manifest_id: None,
        chain_id: Some(ENSV2_CHAIN_ID.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(ens_v2_block_hash(block_number)),
        transaction_hash: Some(ens_v2_transaction_hash(block_number)),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "event_identity": event_identity,
            "emitting_address": resolver_address,
        }),
        derivation_kind: ENSV2_RESOLVER_DERIVATION_KIND.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state,
        after_state,
    }
}

fn ens_v2_resource(resource_id: Uuid, block_number: i64, seed: &str) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: ENSV2_CHAIN_ID.to_owned(),
        block_hash: ens_v2_block_hash(block_number),
        block_number,
        provenance: json!({"seed": seed}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn ens_namespace_normalized_name(logical_name_id: &str) -> String {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    assert_eq!(namespace, "ens", "ENSv2 fixtures are scoped to ens names");
    normalized_name.to_owned()
}

fn is_direct_child_name(parent: &str, child: &str) -> bool {
    child.ends_with(&format!(".{parent}"))
        && child.split('.').count() == parent.split('.').count() + 1
}

fn permission_scope_after_state(scope: &PermissionScope) -> Value {
    match scope {
        PermissionScope::Resource => json!({
            "kind": "resource",
        }),
        PermissionScope::Resolver {
            chain_id,
            resolver_address,
        } => json!({
            "kind": "resolver",
            "chain_id": chain_id,
            "resolver_address": resolver_address,
        }),
        unexpected => panic!(
            "ENSv2 permission fixtures only model resource and resolver scopes, got {unexpected:?}"
        ),
    }
}

fn permission_scope_chain_id(scope: &PermissionScope) -> &str {
    match scope {
        PermissionScope::Resource => ENSV2_CHAIN_ID,
        PermissionScope::Resolver { chain_id, .. } => chain_id,
        unexpected => panic!(
            "ENSv2 permission fixtures only model resource and resolver scopes, got {unexpected:?}"
        ),
    }
}

fn permission_scope_emitting_address(scope: &PermissionScope) -> &str {
    match scope {
        PermissionScope::Resource => "0x0000000000000000000000000000000000000eac",
        PermissionScope::Resolver {
            resolver_address, ..
        } => resolver_address,
        unexpected => panic!(
            "ENSv2 permission fixtures only model resource and resolver scopes, got {unexpected:?}"
        ),
    }
}

fn record_key_for_selector(record_family: &str, selector_key: Option<&str>) -> String {
    selector_key
        .map(|selector_key| format!("{record_family}:{selector_key}"))
        .unwrap_or_else(|| record_family.to_owned())
}

fn resolver_source_event_for_record_family(record_family: &str) -> &'static str {
    match record_family {
        "addr" => "AddressChanged",
        "contenthash" => "ContenthashChanged",
        "name" => "NameChanged",
        "text" => "TextChanged",
        _ => "RecordChanged",
    }
}

fn ens_v2_block_hash(block_number: i64) -> String {
    format!("0xensv2block{block_number:02x}")
}

fn ens_v2_transaction_hash(block_number: i64) -> String {
    format!("0xensv2tx{block_number:02x}")
}

fn ens_v2_history_block_hash(block_number: i64) -> String {
    format!("0xensv2history{block_number:02x}")
}

fn ens_v2_history_transaction_hash(block_number: i64) -> String {
    format!("0xensv2historytx{block_number:02x}")
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

fn basenames_exact_name_control_summary() -> Value {
    json!({
        "registrant": "0x00000000000000000000000000000000000000aa",
        "registry_owner": "0x00000000000000000000000000000000000000bb",
        "latest_event_kind": "AuthorityTransferred",
    })
}

fn basenames_control_vector_control_summary(scenario: BasenamesControlVectorScenario) -> Value {
    match scenario {
        BasenamesControlVectorScenario::NftOnly => json!({
            "registrant": "0x00000000000000000000000000000000000000c1",
            "registry_owner": "0x00000000000000000000000000000000000000b1",
            "latest_event_kind": "TokenControlTransferred",
        }),
        BasenamesControlVectorScenario::ManagementOnly => json!({
            "registrant": "0x00000000000000000000000000000000000000a2",
            "registry_owner": "0x00000000000000000000000000000000000000b2",
            "latest_event_kind": "AuthorityTransferred",
        }),
        BasenamesControlVectorScenario::FullTransfer => json!({
            "registrant": "0x00000000000000000000000000000000000000c3",
            "registry_owner": "0x00000000000000000000000000000000000000c3",
            "latest_event_kind": "AuthorityTransferred",
        }),
    }
}

fn basenames_exact_name_resolver_summary() -> Value {
    json!({
        "chain_id": "base-mainnet",
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

fn resolution_avatar_value() -> &'static str {
    "https://cdn.example.test/alice.png"
}

fn resolution_alias_avatar_value() -> &'static str {
    "https://cdn.example.test/alice-via-alias.png"
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
        record_version_boundary: resolution_record_inventory_boundary(logical_name_id, resource_id),
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

fn resolution_record_inventory_current_row_without_verified_entrypoint(
    logical_name_id: &str,
    resource_id: Uuid,
) -> RecordInventoryCurrentRow {
    let mut row = resolution_record_inventory_current_row(logical_name_id, resource_id);
    row.coverage["unsupported_reason"] = json!("verified_resolution_entrypoint_unavailable");
    row
}

fn resolution_record_inventory_current_row_with_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    record_version_boundary: Value,
) -> RecordInventoryCurrentRow {
    let mut row = resolution_record_inventory_current_row(logical_name_id, resource_id);
    let chain_position = record_version_boundary
        .get("chain_position")
        .cloned()
        .unwrap_or(Value::Null);
    row.record_version_boundary = record_version_boundary;
    if let Some(last_change) = row.last_change.as_mut() {
        last_change["chain_position"] = chain_position.clone();
    }
    if let Some(chain_id) = chain_position.get("chain_id").and_then(Value::as_str) {
        let mut chain_positions = serde_json::Map::new();
        chain_positions.insert(chain_id.to_owned(), chain_position);
        row.chain_positions = Value::Object(chain_positions);
    }
    row
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

fn resolution_execution_verified_queries(execution_trace_id: Uuid, record_keys: &[&str]) -> Value {
    json!(
        record_keys
            .iter()
            .map(|record_key| match *record_key {
                "avatar" => json!({
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": resolution_avatar_value(),
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string(),
                    }
                }),
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
                unexpected =>
                    panic!("unexpected persisted verified resolution selector {unexpected}"),
            })
            .collect::<Vec<_>>()
    )
}

fn resolution_alias_only_verified_queries(execution_trace_id: Uuid, record_keys: &[&str]) -> Value {
    json!(
        record_keys
            .iter()
            .map(|record_key| match *record_key {
                "avatar" => json!({
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": resolution_alias_avatar_value(),
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string(),
                    }
                }),
                "text:com.twitter" => json!({
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias",
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string(),
                    }
                }),
                unexpected => panic!(
                    "unexpected persisted alias-only verified resolution selector {unexpected}"
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

fn basenames_resolution_execution_request_key(records: &[&str]) -> String {
    let mut records = records
        .iter()
        .map(|record| (*record).to_owned())
        .collect::<Vec<_>>();
    records.sort_unstable();
    format!("basenames:alice.base.eth:{}", records.join(","))
}

fn requested_chain_positions_from_name_current(chain_positions: &Value) -> Value {
    let mut positions = chain_positions
        .as_object()
        .expect("name_current.chain_positions must be an object")
        .values()
        .map(|position| {
            json!({
                "chain_id": position
                    .get("chain_id")
                    .and_then(Value::as_str)
                    .expect("chain_position.chain_id must be present"),
                "block_number": position
                    .get("block_number")
                    .and_then(Value::as_i64)
                    .expect("chain_position.block_number must be present"),
                "block_hash": position
                    .get("block_hash")
                    .and_then(Value::as_str)
                    .expect("chain_position.block_hash must be present"),
            })
        })
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| {
        left.get("chain_id")
            .and_then(Value::as_str)
            .cmp(&right.get("chain_id").and_then(Value::as_str))
    });
    Value::Array(positions)
}

fn projected_resolution_topology(row: &bigname_storage::NameCurrentRow) -> Result<Value> {
    row.declared_summary
        .get("topology")
        .cloned()
        .context("rebuilt name_current row must project supported topology")
}

fn projected_resolution_boundaries(
    row: &bigname_storage::NameCurrentRow,
) -> Result<(Value, Value)> {
    let topology = projected_resolution_topology(row)?;
    let version_boundaries = topology
        .get("version_boundaries")
        .and_then(Value::as_object)
        .context("projected topology must include version_boundaries")?;
    Ok((
        version_boundaries
            .get("topology_version_boundary")
            .cloned()
            .context("projected topology must include topology_version_boundary")?,
        version_boundaries
            .get("record_version_boundary")
            .cloned()
            .context("projected topology must include record_version_boundary")?,
    ))
}

async fn seed_supported_alias_only_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
            raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
            raw_block("ethereum-mainnet", "0xresolver", None, 101, 1_717_171_701),
            raw_block("ethereum-mainnet", "0xalias", None, 102, 1_717_171_702),
            raw_block(
                "ethereum-mainnet",
                "0xbinding-alias",
                None,
                103,
                1_717_171_703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(&database.pool, &[name_surface(logical_name_id)]).await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            token_lineage_id,
            "0xresource",
            99,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            "0xresource",
            99,
        )],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::ResolverAliasPath,
            active_from: timestamp(1_717_171_703),
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xbinding-alias".to_owned(),
            block_number: 103,
            provenance: json!({"seed": "supported_alias_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:alias-resolver".to_owned(),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "ens_v1_unwrapped_authority".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-mainnet".to_owned()),
                        block_number: Some(101),
                        block_hash: Some("0xresolver".to_owned()),
                        transaction_hash: Some("0xtxresolver".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:alias-resolver"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": "0x0000000000000000000000000000000000000abc",
                            "namehash": "namehash:alice.eth",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:alias-changed".to_owned(),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "AliasChanged".to_owned(),
                        source_family: "ens_v2_resolver".to_owned(),
                        manifest_version: 5,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-mainnet".to_owned()),
                        block_number: Some(102),
                        block_hash: Some("0xalias".to_owned()),
                        transaction_hash: Some("0xtxalias".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:alias-changed"}),
                        derivation_kind: "ens_v2_resolver".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "active": true,
                            "alias_state": "active",
                            "to_name": "profile.alice.eth",
                            "to_logical_name_id": "ens:profile.alice.eth",
                            "to_normalized_name": "profile.alice.eth",
                            "to_canonical_display_name": "Profile.alice.eth",
                            "to_namehash": "namehash:profile.alice.eth",
                            "to_resource_id": resource_id.to_string(),
                        }),
                    },
                ],
            )
            .await?;
    database.rebuild_name_current(logical_name_id).await
}

async fn seed_supported_wildcard_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    wildcard_source_resource_id: Uuid,
) -> Result<()> {
    let wildcard_source_token_lineage_id = Uuid::from_u128(0x4401);
    let wildcard_source_binding_id = Uuid::from_u128(0x4402);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "ethereum-mainnet",
                "0xsource-surface",
                None,
                96,
                1_717_171_696,
            ),
            raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
            raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
            raw_block(
                "ethereum-mainnet",
                "0xsource-resource",
                None,
                100,
                1_717_171_700,
            ),
            raw_block("ethereum-mainnet", "0xresolver", None, 101, 1_717_171_701),
            raw_block(
                "ethereum-mainnet",
                "0xsource-record-version",
                None,
                102,
                1_717_171_702,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xbinding-wildcard",
                None,
                103,
                1_717_171_703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            name_surface(logical_name_id),
            NameSurface {
                logical_name_id: "ens:eth".to_owned(),
                namespace: "ens".to_owned(),
                input_name: "eth".to_owned(),
                canonical_display_name: "Eth".to_owned(),
                normalized_name: "eth".to_owned(),
                dns_encoded_name: vec![3, b'e', b't', b'h'],
                namehash: "namehash:eth".to_owned(),
                labelhashes: vec!["labelhash:eth".to_owned()],
                normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsource-surface".to_owned(),
                block_number: 96,
                provenance: json!({"seed": "supported_wildcard_source_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(token_lineage_id, "0xresource", 99),
            address_name_token_lineage(wildcard_source_token_lineage_id, "0xsource-resource", 100),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(resource_id, Some(token_lineage_id), "0xresource", 99),
            address_name_resource(
                wildcard_source_resource_id,
                Some(wildcard_source_token_lineage_id),
                "0xsource-resource",
                100,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            SurfaceBinding {
                surface_binding_id: wildcard_source_binding_id,
                logical_name_id: "ens:eth".to_owned(),
                resource_id: wildcard_source_resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_171_700),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsource-resource".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "supported_wildcard_source_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            },
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::ObservedWildcardPath,
                active_from: timestamp(1_717_171_703),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbinding-wildcard".to_owned(),
                block_number: 103,
                provenance: json!({"seed": "supported_wildcard_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:wildcard-source-resolver".to_owned(),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some("ens:eth".to_owned()),
                        resource_id: Some(wildcard_source_resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "ens_v1_unwrapped_authority".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-mainnet".to_owned()),
                        block_number: Some(101),
                        block_hash: Some("0xresolver".to_owned()),
                        transaction_hash: Some("0xtxwildcardsourceresolver".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:wildcard-source-resolver"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": "0x0000000000000000000000000000000000000def",
                            "namehash": "namehash:eth",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:wildcard-source-record-version".to_owned(),
                        namespace: "ens".to_owned(),
                        logical_name_id: Some("ens:eth".to_owned()),
                        resource_id: Some(wildcard_source_resource_id),
                        event_kind: "RecordVersionChanged".to_owned(),
                        source_family: "ens_v1_unwrapped_authority".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("ethereum-mainnet".to_owned()),
                        block_number: Some(102),
                        block_hash: Some("0xsource-record-version".to_owned()),
                        transaction_hash: Some("0xtxwildcardsourceversion".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:wildcard-source-record-version"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({"record_version": 6}),
                        after_state: json!({"record_version": 7}),
                    },
                ],
            )
            .await?;
    database.rebuild_name_current(logical_name_id).await
}

async fn seed_supported_basenames_rebuild_inputs(
    database: &HarnessDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
            raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
            raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
            raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
            raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
            raw_block(
                "base-mainnet",
                "0xbase-binding-supported",
                None,
                104,
                1_717_171_704,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xbasenamesl1",
                None,
                21_000_100,
                1_717_171_680,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "basenames".to_owned(),
            input_name: "alice.base.eth".to_owned(),
            canonical_display_name: "Alice.base.eth".to_owned(),
            normalized_name: "alice.base.eth".to_owned(),
            dns_encoded_name: b"alice.base.eth".to_vec(),
            namehash: "namehash:alice.base.eth".to_owned(),
            labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 98,
            provenance: json!({"seed": "supported_basenames_surface"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[TokenLineage {
            token_lineage_id,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "supported_basenames_token_lineage"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "supported_basenames_resource"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_171_704),
            active_to: None,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-binding-supported".to_owned(),
            block_number: 104,
            provenance: json!({"seed": "supported_basenames_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:supported-basenames:grant".to_owned(),
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
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:supported-basenames:grant"}),
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
                        event_identity: "conformance:supported-basenames:authority".to_owned(),
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
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:supported-basenames:authority"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "owner": "0x00000000000000000000000000000000000000bb",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:supported-basenames:resolver".to_owned(),
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
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:supported-basenames:resolver"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": "0x0000000000000000000000000000000000000abc",
                            "namehash": "namehash:alice.base.eth",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:supported-basenames:record-version".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RecordVersionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(104),
                        block_hash: Some("0xbase-binding-supported".to_owned()),
                        transaction_hash: Some("0xtxbaserecordversion".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:supported-basenames:record-version"}),
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
                        event_identity: "conformance:supported-basenames:addr".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(104),
                        block_hash: Some("0xbase-binding-supported".to_owned()),
                        transaction_hash: Some("0xtxbaseaddr".to_owned()),
                        log_index: Some(1),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:supported-basenames:addr"}),
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
                        event_identity: "conformance:supported-basenames:text".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(104),
                        block_hash: Some("0xbase-binding-supported".to_owned()),
                        transaction_hash: Some("0xtxbasetext".to_owned()),
                        log_index: Some(2),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:supported-basenames:text"}),
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
            .await?;
    let manifest_id = database
        .insert_manifest(
            "basenames",
            "basenames_execution",
            "ethereum-mainnet",
            "basenames_v1",
            2,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(manifest_id, "verified_resolution", "supported", None)
        .await?;
    insert_basenames_execution_manifest_contract(database, manifest_id).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_100,
                "block_hash": "0xbasenamesl1",
                "timestamp": "2024-05-31T16:08:00Z",
            }
        }))
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    insert_basenames_supported_ethereum_position_for_current_row(database, logical_name_id).await
}

async fn insert_basenames_execution_manifest_contract(
    database: &HarnessDatabase,
    manifest_id: i64,
) -> Result<()> {
    let contract_instance_id = Uuid::from_u128(0x0b45_0000_0000_0000_0000_0000_0000_0002);
    sqlx::query(
        r#"
                INSERT INTO contract_instances (
                    contract_instance_id,
                    chain_id,
                    contract_kind,
                    provenance
                )
                VALUES ($1, 'ethereum-mainnet', 'contract', $2::jsonb)
                ON CONFLICT (contract_instance_id) DO NOTHING
                "#,
    )
    .bind(contract_instance_id)
    .bind(json!({"seed": "conformance_basenames_execution"}))
    .execute(&database.pool)
    .await
    .context("failed to insert Basenames execution contract_instance")?;

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
                VALUES (
                    $1,
                    'contract',
                    'l1_resolver',
                    $2,
                    '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31',
                    'l1_resolver',
                    'none'
                )
                "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .execute(&database.pool)
    .await
    .context("failed to insert Basenames execution manifest_contract_instance")?;

    Ok(())
}

fn basenames_execution_manifest_version() -> Value {
    json!({
        "source_family": "basenames_execution",
        "manifest_version": 2,
        "chain": "ethereum-mainnet",
        "deployment_epoch": "basenames_v1",
    })
}

fn append_basenames_execution_manifest_version(name_row: &mut bigname_storage::NameCurrentRow) {
    let manifest_versions = name_row.provenance["manifest_versions"]
        .as_array_mut()
        .expect("name_current.provenance.manifest_versions must be an array");
    if manifest_versions.iter().any(|item| {
        item.get("source_family").and_then(Value::as_str) == Some("basenames_execution")
            && item.get("manifest_version").and_then(Value::as_i64) == Some(2)
    }) {
        return;
    }
    manifest_versions.push(basenames_execution_manifest_version());
}

fn insert_basenames_supported_ethereum_position(name_row: &mut bigname_storage::NameCurrentRow) {
    let chain_positions = name_row
        .chain_positions
        .as_object_mut()
        .expect("name_current.chain_positions must be an object");
    chain_positions.insert(
        "ethereum".to_owned(),
        json!({
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_100,
            "block_hash": "0xbasenamesl1",
            "timestamp": "2024-05-31T16:08:00Z",
        }),
    );
}

async fn insert_basenames_supported_ethereum_position_for_current_row(
    database: &HarnessDatabase,
    logical_name_id: &str,
) -> Result<()> {
    let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("Basenames fixture requires name_current row before Ethereum selector seed")?;
    append_basenames_execution_manifest_version(&mut name_row);
    insert_basenames_supported_ethereum_position(&mut name_row);
    database.insert_name_current_row(name_row).await
}

fn basenames_resolution_execution_trace(
    execution_trace_id: Uuid,
    request_key: &str,
    request_record_keys: &[&str],
    requested_chain_positions: Value,
    verified_queries: Value,
) -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id,
        request_type: "verified_resolution".to_owned(),
        request_key: request_key.to_owned(),
        namespace: "basenames".to_owned(),
        chain_context: json!({
            "requested_positions": requested_chain_positions,
        }),
        manifest_context: json!({
            "manifest_versions": [{
                "source_family": "basenames_execution",
                "manifest_version": 2,
            }]
        }),
        contracts_called: json!([
            {
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                "selector": "0x9061b923",
            }
        ]),
        gateway_digests: json!(["sha256:ccip-request", "sha256:ccip-response"]),
        final_payload: Some(json!({
            "verified_queries": verified_queries.clone(),
        })),
        failure_payload: None,
        request_metadata: json!({
            "surface": "alice.base.eth",
            "record_keys": request_record_keys,
            "entrypoint": "l1_resolver",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "transport": {
                "source_chain_id": "base-mainnet",
                "target_chain_id": "ethereum-mainnet",
                "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                "latest_event_kind": null,
            }
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
                    "base-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }),
                step_payload: json!({
                    "entrypoint": "l1_resolver",
                    "resolver": "0x0000000000000000000000000000000000000abc",
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "call_l1_resolver".to_owned(),
                input_digest: Some("sha256:l1-input".to_owned()),
                output_digest: Some("sha256:l1-output".to_owned()),
                latency_ms: Some(17),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }),
                step_payload: json!({
                    "name": "alice.base.eth",
                    "record_count": request_record_keys.len(),
                }),
            },
            ExecutionTraceStep {
                step_index: 2,
                step_kind: "ccip_offchain_lookup".to_owned(),
                input_digest: Some("sha256:ccip-input".to_owned()),
                output_digest: Some("sha256:ccip-output".to_owned()),
                latency_ms: Some(29),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }),
                step_payload: json!({
                    "gateway_digest": "sha256:ccip-request",
                }),
            },
            ExecutionTraceStep {
                step_index: 3,
                step_kind: "resolve_with_proof".to_owned(),
                input_digest: Some("sha256:proof-input".to_owned()),
                output_digest: Some("sha256:proof-output".to_owned()),
                latency_ms: Some(11),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }),
                step_payload: json!({
                    "proof_kind": "signature",
                }),
            },
        ],
    }
}

fn basenames_resolution_execution_outcome(
    execution_trace_id: Uuid,
    request_key: &str,
    requested_chain_positions: Value,
    manifest_versions: Value,
    record_version_boundary: Value,
    verified_queries: Value,
) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: request_key.to_owned(),
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary: record_version_boundary.clone(),
            record_version_boundary,
        },
        execution_trace_id,
        request_type: "verified_resolution".to_owned(),
        namespace: "basenames".to_owned(),
        outcome_payload: Some(json!({
            "verified_queries": verified_queries,
        })),
        failure_payload: None,
        finished_at: timestamp(1_717_171_900),
    }
}

fn basenames_resolution_execution_summary(
    execution_trace_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
) -> Value {
    json!({
        "execution_trace_id": execution_trace_id.to_string(),
        "selected_entrypoint": {
            "source_family": "basenames_execution",
            "role": "l1_resolver",
            "chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
        },
        "resolver_discovery_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "basenames",
                "normalized_name": "alice.base.eth",
                "canonical_display_name": "Alice.base.eth",
                "resource_id": resource_id.to_string(),
                "chain_id": "base-mainnet",
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
                    "base-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }
            },
            {
                "step_index": 1,
                "step_kind": "call_l1_resolver",
                "input_digest": "sha256:l1-input",
                "output_digest": "sha256:l1-output",
                "latency": 17,
                "canonicality_dependency": {
                    "ethereum-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }
            },
            {
                "step_index": 2,
                "step_kind": "ccip_offchain_lookup",
                "input_digest": "sha256:ccip-input",
                "output_digest": "sha256:ccip-output",
                "latency": 29,
                "canonicality_dependency": {
                    "ethereum-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }
            },
            {
                "step_index": 3,
                "step_kind": "resolve_with_proof",
                "input_digest": "sha256:proof-input",
                "output_digest": "sha256:proof-output",
                "latency": 11,
                "canonicality_dependency": {
                    "ethereum-mainnet": {
                        "block_hash": "0xbase-binding",
                        "block_number": 100,
                        "state": "finalized",
                    }
                }
            }
        ],
        "finished_at": format_timestamp(timestamp(1_717_171_900)),
    })
}

fn resolution_wildcard_source(
    wildcard_source_logical_name_id: &str,
    wildcard_source_resource_id: Uuid,
) -> Value {
    json!({
        "logical_name_id": wildcard_source_logical_name_id,
        "namespace": "ens",
        "normalized_name": "eth",
        "canonical_display_name": "Eth",
        "namehash": "namehash:eth",
        "resource_id": wildcard_source_resource_id.to_string(),
        "binding_kind": "observed_wildcard_path",
    })
}

fn resolution_wildcard_projected_topology(
    logical_name_id: &str,
    resource_id: Uuid,
    wildcard_source_logical_name_id: &str,
    wildcard_source_resource_id: Uuid,
) -> Value {
    let wildcard_source =
        resolution_wildcard_source(wildcard_source_logical_name_id, wildcard_source_resource_id);
    let wildcard_boundary = resolution_record_inventory_boundary(
        wildcard_source_logical_name_id,
        wildcard_source_resource_id,
    );

    json!({
        "registry_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "observed_wildcard_path",
            }
        ],
        "subregistry_path": [],
        "resolver_path": [
            {
                "logical_name_id": wildcard_source_logical_name_id,
                "namespace": "ens",
                "normalized_name": "eth",
                "canonical_display_name": "Eth",
                "resource_id": wildcard_source_resource_id.to_string(),
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000def",
                "latest_event_kind": "ResolverChanged",
            }
        ],
        "wildcard": {
            "source": wildcard_source,
            "matched_labels": ["alice"],
        },
        "alias": {
            "final_target": null,
            "hops": [],
        },
        "version_boundaries": {
            "topology_version_boundary": wildcard_boundary.clone(),
            "record_version_boundary": wildcard_boundary,
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null,
        },
    })
}

fn resolution_wildcard_execution_summary(
    execution_trace_id: Uuid,
    wildcard_source_logical_name_id: &str,
    wildcard_source_resource_id: Uuid,
) -> Value {
    let wildcard_source =
        resolution_wildcard_source(wildcard_source_logical_name_id, wildcard_source_resource_id);
    let mut execution =
        resolution_execution_summary(execution_trace_id, wildcard_source_resource_id);

    execution["resolver_discovery_path"] = json!([
        {
            "logical_name_id": wildcard_source_logical_name_id,
            "namespace": "ens",
            "normalized_name": "eth",
            "canonical_display_name": "Eth",
            "resource_id": wildcard_source_resource_id.to_string(),
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000def",
            "latest_event_kind": "ResolverChanged",
        }
    ]);
    execution["wildcard"] = json!({
        "source": wildcard_source,
        "matched_labels": ["alice"],
    });
    execution["steps"]
        .as_array_mut()
        .expect("resolution execution summary must expose steps")
        .push(json!({
            "step_index": 2,
            "step_kind": "call_wildcard_resolver",
            "input_digest": "sha256:wildcard-input",
            "output_digest": "sha256:wildcard-output",
            "latency": 19,
            "canonicality_dependency": {
                "ethereum-mainnet": {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical",
                }
            }
        }));

    execution
}

#[derive(Clone, Copy, Debug)]
enum UnsupportedEnsVerifiedResolutionPathCase {
    NonAliasAncestorSelected,
    TransportAssisted,
}

impl UnsupportedEnsVerifiedResolutionPathCase {
    fn execution_trace_id(self) -> Uuid {
        match self {
            Self::NonAliasAncestorSelected => Uuid::from_u128(0x0e7ec7ace00000000000000000000027),
            Self::TransportAssisted => Uuid::from_u128(0x0e7ec7ace00000000000000000000028),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::NonAliasAncestorSelected => "non-alias ancestor-selected",
            Self::TransportAssisted => "transport-assisted",
        }
    }

    fn apply_to_name_row(self, row: &mut bigname_storage::NameCurrentRow) {
        let summary = row
            .declared_summary
            .as_object_mut()
            .expect("resolution negative fixture requires object declared_summary");
        summary.insert(
            "topology".to_owned(),
            self.expected_topology(&row.logical_name_id, row.resource_id),
        );

        if let Some(resolver) = summary.get_mut("resolver").and_then(Value::as_object_mut) {
            resolver.insert(
                "address".to_owned(),
                Value::String(
                    match self {
                        Self::NonAliasAncestorSelected => {
                            "0x0000000000000000000000000000000000000def"
                        }
                        Self::TransportAssisted => "0x0000000000000000000000000000000000000abc",
                    }
                    .to_owned(),
                ),
            );
        }
    }

    fn apply_to_trace(self, trace: &mut ExecutionTrace) {
        let metadata = trace
            .request_metadata
            .as_object_mut()
            .expect("resolution negative fixture requires request_metadata object");
        match self {
            Self::NonAliasAncestorSelected => {
                metadata.insert(
                    "resolver_path".to_owned(),
                    json!([{
                        "logical_name_id": "ens:eth",
                        "namespace": "ens",
                        "normalized_name": "eth",
                        "canonical_display_name": "eth",
                        "resource_id": Uuid::from_u128(0x2210).to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000def",
                        "latest_event_kind": "ResolverChanged",
                    }]),
                );
            }
            Self::TransportAssisted => {
                metadata.insert(
                    "transport".to_owned(),
                    resolution_transport_assisted_transport(),
                );
            }
        }
    }

    fn expected_topology(self, logical_name_id: &str, resource_id: Option<Uuid>) -> Value {
        let resource_id =
            resource_id.expect("resolution negative fixture requires an exact-surface resource_id");
        match self {
            Self::NonAliasAncestorSelected => {
                resolution_non_alias_ancestor_selected_topology(logical_name_id, resource_id)
            }
            Self::TransportAssisted => {
                resolution_transport_assisted_topology(logical_name_id, resource_id)
            }
        }
    }
}

struct UnsupportedEnsVerifiedResolutionFixture {
    logical_name_id: &'static str,
    resource_id: Uuid,
}

async fn run_resolution_negative_verified_path_case(
    path_case: UnsupportedEnsVerifiedResolutionPathCase,
) -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let fixture = seed_unsupported_ens_verified_resolution_fixture(&database, path_case).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .with_context(|| format!("{} mixed resolution request failed", path_case.label()))?;

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{} mixed resolution should keep the declared envelope and explicit unsupported verified results",
        path_case.label()
    );

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .context("mixed negative resolution response must include declared_state")?;
    let topology = declared_state
        .get("topology")
        .context("mixed negative resolution response must include topology")?;

    assert_negative_verified_resolution_topology(path_case, topology, fixture.logical_name_id);
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null),
        "{} mixed resolution must not surface the persisted execution trace id",
        path_case.label()
    );
    assert_eq!(
        payload.verified_state,
        Some(resolution_unsupported_verified_state(&[
            "avatar",
            "text:com.twitter",
        ])),
        "{} mixed resolution must keep selector-local unsupported results",
        path_case.label()
    );

    let expected_topology =
        path_case.expected_topology(fixture.logical_name_id, Some(fixture.resource_id));
    assert_eq!(
        topology,
        &expected_topology,
        "{} mixed resolution topology should stay visible while verified resolution remains unsupported",
        path_case.label()
    );

    database.cleanup().await?;
    Ok(())
}

async fn run_resolution_execution_explain_negative_verified_path_case(
    path_case: UnsupportedEnsVerifiedResolutionPathCase,
) -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let _fixture = seed_unsupported_ens_verified_resolution_fixture(&database, path_case).await?;

    let response = app_router(database.app_state())
                        .oneshot(
                            Request::builder()
                                .uri(
                                    "/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter",
                                )
                                .body(Body::empty())
                                .expect("request must build"),
                        )
                        .await
                        .with_context(|| {
                            format!("{} resolution execution explain request failed", path_case.label())
                        })?;

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "{} resolution execution explain should stay outside the shipped public explain surface",
        path_case.label()
    );

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );
    assert!(
        payload.error.details.is_empty(),
        "{} resolution execution explain should not add extra error details",
        path_case.label()
    );

    database.cleanup().await?;
    Ok(())
}

async fn seed_unsupported_ens_verified_resolution_fixture(
    database: &HarnessDatabase,
    path_case: UnsupportedEnsVerifiedResolutionPathCase,
) -> Result<UnsupportedEnsVerifiedResolutionFixture> {
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
    let record_inventory_row =
        resolution_record_inventory_current_row(logical_name_id, resource_id);
    let supported_name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("resolution negative fixture requires an exact-name current row")?;
    let records = parse_resolution_record_keys(Some("text:com.twitter"), ResolutionMode::Verified)
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let cache_key = build_resolution_execution_cache_key(
        &supported_name_row,
        &records,
        Some(&record_inventory_row),
        supported_name_row.chain_positions.clone(),
    )?;
    let request_key = cache_key.request_key.clone();

    database
        .insert_record_inventory_current_row(record_inventory_row.clone())
        .await?;

    let mut name_row = supported_name_row.clone();
    path_case.apply_to_name_row(&mut name_row);
    database.insert_name_current_row(name_row.clone()).await?;

    let persisted_verified_queries = resolution_execution_verified_queries(
        path_case.execution_trace_id(),
        &["avatar", "text:com.twitter"],
    );

    let mut trace = resolution_execution_trace(
        path_case.execution_trace_id(),
        &request_key,
        &["avatar", "text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    path_case.apply_to_trace(&mut trace);

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(
        &database.pool,
        &resolution_execution_outcome(
            path_case.execution_trace_id(),
            cache_key,
            persisted_verified_queries,
        ),
    )
    .await?;

    Ok(UnsupportedEnsVerifiedResolutionFixture {
        logical_name_id,
        resource_id,
    })
}

fn resolution_non_alias_ancestor_selected_topology(
    logical_name_id: &str,
    resource_id: Uuid,
) -> Value {
    let mut topology = resolution_supported_declared_state(
        logical_name_id,
        resource_id,
        &["avatar", "text:com.twitter"],
    )
    .get("topology")
    .cloned()
    .expect("supported declared resolution state must include topology");

    let topology_object = topology
        .as_object_mut()
        .expect("resolution topology must be an object");
    topology_object.insert(
        "resolver_path".to_owned(),
        json!([{
            "logical_name_id": "ens:eth",
            "namespace": "ens",
            "normalized_name": "eth",
            "canonical_display_name": "eth",
            "resource_id": Uuid::from_u128(0x2210).to_string(),
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000def",
            "latest_event_kind": "ResolverChanged",
        }]),
    );
    topology_object.insert(
        "alias".to_owned(),
        json!({
            "final_target": null,
            "hops": [],
        }),
    );
    topology_object.insert(
        "wildcard".to_owned(),
        json!({
            "source": null,
            "matched_labels": [],
        }),
    );
    topology_object.insert(
        "transport".to_owned(),
        json!({
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null,
        }),
    );
    topology
}

fn resolution_transport_assisted_transport() -> Value {
    json!({
        "source_chain_id": "base-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": "0x0000000000000000000000000000000000000fed",
        "latest_event_kind": "ResolverTransportUpdated",
    })
}

fn resolution_transport_assisted_topology(logical_name_id: &str, resource_id: Uuid) -> Value {
    let mut topology = resolution_supported_declared_state(
        logical_name_id,
        resource_id,
        &["avatar", "text:com.twitter"],
    )
    .get("topology")
    .cloned()
    .expect("supported declared resolution state must include topology");

    let topology_object = topology
        .as_object_mut()
        .expect("resolution topology must be an object");
    topology_object.insert(
        "alias".to_owned(),
        json!({
            "final_target": null,
            "hops": [],
        }),
    );
    topology_object.insert(
        "wildcard".to_owned(),
        json!({
            "source": null,
            "matched_labels": [],
        }),
    );
    topology_object.insert(
        "transport".to_owned(),
        resolution_transport_assisted_transport(),
    );
    topology
}

fn assert_negative_verified_resolution_topology(
    path_case: UnsupportedEnsVerifiedResolutionPathCase,
    topology: &Value,
    logical_name_id: &str,
) {
    assert_eq!(
        topology.get("wildcard"),
        Some(&json!({
            "source": null,
            "matched_labels": [],
        })),
        "{} topology should explicitly stay outside wildcard-derived coverage in this slice",
        path_case.label()
    );
    assert_eq!(
        topology.get("alias"),
        Some(&json!({
            "final_target": null,
            "hops": [],
        })),
        "{} topology should keep alias rewriting out of this negative case",
        path_case.label()
    );

    match path_case {
        UnsupportedEnsVerifiedResolutionPathCase::NonAliasAncestorSelected => {
            assert_eq!(
                topology.get("transport"),
                Some(&json!({
                    "source_chain_id": null,
                    "target_chain_id": null,
                    "contract_address": null,
                    "latest_event_kind": null,
                })),
                "ancestor-selected topology should not rely on transport participation",
            );
            assert_eq!(
                topology
                    .get("resolver_path")
                    .and_then(Value::as_array)
                    .and_then(|resolver_path| resolver_path.first())
                    .and_then(|hop| hop.get("logical_name_id"))
                    .and_then(Value::as_str),
                Some("ens:eth"),
                "ancestor-selected topology should expose the selected ancestor hop",
            );
            assert_ne!(
                topology
                    .get("resolver_path")
                    .and_then(Value::as_array)
                    .and_then(|resolver_path| resolver_path.first())
                    .and_then(|hop| hop.get("logical_name_id"))
                    .and_then(Value::as_str),
                Some(logical_name_id),
                "ancestor-selected topology must not collapse back to the request surface",
            );
        }
        UnsupportedEnsVerifiedResolutionPathCase::TransportAssisted => {
            assert_eq!(
                topology
                    .get("resolver_path")
                    .and_then(Value::as_array)
                    .and_then(|resolver_path| resolver_path.first())
                    .and_then(|hop| hop.get("logical_name_id"))
                    .and_then(Value::as_str),
                Some(logical_name_id),
                "transport-assisted topology should stay exact-surface on the resolver path",
            );
            assert_eq!(
                topology.get("transport"),
                Some(&resolution_transport_assisted_transport()),
                "transport-assisted topology should expose the participating compatibility transport",
            );
        }
    }
}

fn primary_name_execution_requested_chain_positions() -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_010,
        "block_hash": "0xprimary",
    }])
}

fn primary_name_execution_manifest_versions_for_namespace(namespace: &str) -> Value {
    match namespace {
        "ens" => json!([{
            "manifest_version": 3,
            "source_family": "ens_execution",
        }]),
        "basenames" => json!([{
            "manifest_version": 4,
            "source_family": "basenames_execution",
        }]),
        other => panic!("unsupported primary-name test namespace {other}"),
    }
}

fn primary_name_execution_manifest_versions() -> Value {
    primary_name_execution_manifest_versions_for_namespace("ens")
}

fn primary_name_execution_request_key(namespace: &str, address: &str, coin_type: &str) -> String {
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
    let manifest_versions = primary_name_execution_manifest_versions_for_namespace(namespace);
    let status = verified_primary_name
        .get("status")
        .and_then(Value::as_str)
        .expect("verified_primary_name payload must include string status");
    let (contracts_called, gateway_digests, steps) = match (namespace, status) {
        ("ens", "success" | "mismatch" | "execution_failed") => (
            json!([{
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "selector": "0x9061b923",
            }]),
            json!([]),
            vec![ExecutionTraceStep {
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
        ),
        ("basenames", "success" | "mismatch" | "execution_failed") => (
            json!([{
                "chain_id": "ethereum-mainnet",
                "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                "selector": "0x9061b923",
            }]),
            json!(["sha256:basenames-primary-name"]),
            vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "call_l1_resolver".to_owned(),
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
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "complete_offchain_lookup".to_owned(),
                    input_digest: Some("sha256:gateway-input".to_owned()),
                    output_digest: Some("sha256:gateway-output".to_owned()),
                    latency_ms: Some(19),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized",
                        }
                    }),
                    step_payload: json!({
                        "gateway": "https://basenames.example.test",
                    }),
                },
            ],
        ),
        ("ens" | "basenames", "not_found") => (
            json!([]),
            json!([]),
            vec![ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
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
        ),
        ("ens" | "basenames", "invalid_name") => (
            json!([]),
            json!([]),
            vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_primary_name_claim".to_owned(),
                    input_digest: Some("sha256:claim-input".to_owned()),
                    output_digest: Some("sha256:claim-output".to_owned()),
                    latency_ms: Some(2),
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
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "normalize_claimed_name".to_owned(),
                    input_digest: Some("sha256:normalize-input".to_owned()),
                    output_digest: Some("sha256:normalize-output".to_owned()),
                    latency_ms: Some(1),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized",
                        }
                    }),
                    step_payload: json!({
                        "normalizer_version": "ensip15@ens-normalize-0.1.1",
                        "error": "claim_name_not_normalizable",
                    }),
                },
            ],
        ),
        (other, _) if other != "ens" && other != "basenames" => {
            panic!("unsupported primary-name test namespace {other}")
        }
        (_, other) => panic!("unsupported primary-name test status {other}"),
    };
    ExecutionTrace {
        execution_trace_id,
        request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
        request_key: primary_name_execution_request_key(namespace, &normalized_address, coin_type),
        namespace: namespace.to_owned(),
        chain_context: json!({
            "requested_positions": primary_name_execution_requested_chain_positions(),
        }),
        manifest_context: json!({
            "manifest_versions": manifest_versions,
        }),
        contracts_called,
        gateway_digests,
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
        steps,
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
            manifest_versions: primary_name_execution_manifest_versions_for_namespace(namespace),
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
    let fixture =
        seed_persisted_resolution_execution_fixture(&database, invalidation.execution_trace_id())
            .await?;
    let mixed_uri = "/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60";
    let explain_uri =
        "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60";

    database.seed_snapshot_selector_for_route(mixed_uri).await?;
    let mixed_before_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(mixed_uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed before invalidation")?;
    database
        .seed_snapshot_selector_for_route(explain_uri)
        .await?;
    let explain_before_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(explain_uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed before invalidation")?;

    assert_eq!(mixed_before_response.status(), StatusCode::OK);
    assert_eq!(explain_before_response.status(), StatusCode::OK);

    let mixed_before_payload: ResolutionResponse = read_json(mixed_before_response).await?;
    let explain_before_payload: ResolutionResponse = read_json(explain_before_response).await?;
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

    invalidate_persisted_resolution_execution(&database, &fixture.cache_key, invalidation).await?;

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

    database.seed_snapshot_selector_for_route(mixed_uri).await?;
    let mixed_after_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(mixed_uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed after invalidation")?;
    database
        .seed_snapshot_selector_for_route(explain_uri)
        .await?;
    let explain_after_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(explain_uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed after invalidation")?;

    let mixed_after_status = mixed_after_response.status();
    let mixed_after_bytes = to_bytes(mixed_after_response.into_body(), usize::MAX)
        .await
        .context("failed to read mixed resolution response body after invalidation")?;
    assert_eq!(
        mixed_after_status,
        StatusCode::CONFLICT,
        "mixed resolution response after invalidation body {}",
        String::from_utf8_lossy(&mixed_after_bytes)
    );
    let explain_after_status = explain_after_response.status();
    let explain_after_bytes = to_bytes(explain_after_response.into_body(), usize::MAX)
        .await
        .context("failed to read resolution explain response body after invalidation")?;
    assert_eq!(
        explain_after_status,
        StatusCode::NOT_FOUND,
        "resolution explain response after invalidation body {}",
        String::from_utf8_lossy(&explain_after_bytes)
    );

    let mixed_after_payload: ErrorResponse = serde_json::from_slice(&mixed_after_bytes)
        .context("failed to decode mixed resolution error response after invalidation")?;
    let explain_after_payload: ErrorResponse = serde_json::from_slice(&explain_after_bytes)
        .context("failed to decode resolution explain response after invalidation")?;

    assert_eq!(mixed_after_payload.error.code, "stale");
    assert_eq!(
        mixed_after_payload.error.message,
        "persisted verified resolution output is not available for the selected snapshot"
    );
    assert!(mixed_after_payload.error.details.is_empty());
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
    let records =
        parse_resolution_record_keys(Some("text:com.twitter,addr:60"), ResolutionMode::Verified)
            .map_err(|error| anyhow::anyhow!(error.message))?;
    let cache_key = build_resolution_execution_cache_key(
        &name_row,
        &records,
        Some(&record_inventory_row),
        name_row.chain_positions.clone(),
    )?;
    let request_key = cache_key.request_key.clone();
    let persisted_verified_queries =
        resolution_execution_verified_queries(execution_trace_id, &["addr:60", "text:com.twitter"]);

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
                .context("persisted verified resolution cache key must expose manifest_versions")?;
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
    let fixture = seed_persisted_primary_name_execution_fixture(&database, invalidation).await?;
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

    let verified_before_payload: PrimaryNameResponse = read_json(verified_before_response).await?;
    let both_before_payload: PrimaryNameResponse = read_json(both_before_response).await?;
    let mut expected_target_verified_primary_name = fixture.target_verified_primary_name.clone();
    expected_target_verified_primary_name
        .as_object_mut()
        .expect("target verified primary-name fixture must be an object")
        .insert(
            "provenance".to_owned(),
            json!({
                "manifest_versions": primary_name_execution_manifest_versions(),
                "execution_trace_id": fixture.target_execution_trace_id.to_string(),
            }),
        );

    assert_eq!(verified_before_payload.data, expected_data);
    assert_eq!(both_before_payload.data, expected_data);
    assert_eq!(verified_before_payload.declared_state, None);
    assert_eq!(
        verified_before_payload.verified_state,
        Some(json!({
            "verified_primary_name": expected_target_verified_primary_name,
        }))
    );
    assert_eq!(
        both_before_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
                "provenance": seeded_primary_name_claim_provenance(),
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

    invalidate_persisted_primary_name_execution(&database, &fixture.target_cache_key, invalidation)
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

    let verified_after_payload: PrimaryNameResponse = read_json(verified_after_response).await?;
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
                "provenance": seeded_primary_name_claim_provenance(),
            }
        }))
    );
    assert_eq!(
        both_after_payload.verified_state,
        verified_after_payload.verified_state
    );
    assert_primary_name_bootstrap_invariants(&verified_after_payload);
    assert_primary_name_bootstrap_invariants(&both_after_payload);
    let mut expected_sibling_verified_primary_name = fixture.sibling_verified_primary_name.clone();
    expected_sibling_verified_primary_name
        .as_object_mut()
        .expect("sibling verified primary-name fixture must be an object")
        .insert(
            "provenance".to_owned(),
            json!({
                "manifest_versions": primary_name_execution_manifest_versions(),
                "execution_trace_id": invalidation
                    .sibling_execution_trace_id()
                    .to_string(),
            }),
        );

    assert_eq!(
        sibling_payload.verified_state,
        Some(json!({
            "verified_primary_name": expected_sibling_verified_primary_name,
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
                    request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
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
                    request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
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
                    request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
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
