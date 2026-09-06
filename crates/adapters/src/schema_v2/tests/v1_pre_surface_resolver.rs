use super::*;

const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const OLD_REGISTRY: &str = "0x00000000000000000000000000000000000000a0";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000a6";
const OWNER: &str = "0x00000000000000000000000000000000000000a3";
const OWNER_2: &str = "0x00000000000000000000000000000000000000a5";
const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a4";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b4";
const REGISTRY_MANIFEST_ID: i64 = 6131;
const REGISTRAR_MANIFEST_ID: i64 = 6132;
const WRAPPER_MANIFEST_ID: i64 = 6133;

fn fixture() -> (Vec<ManifestInput>, Vec<AddressAdmissionInput>, B256) {
    let registry_manifest = manifest_with_events(
        REGISTRY_MANIFEST_ID,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry", "registry_old"],
                &[
                    "SubregistryChanged",
                    "AuthorityTransferred",
                    "ResolverChanged",
                    "PermissionChanged",
                ],
            ),
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry", "registry_old"],
                &[
                    "AuthorityTransferred",
                    "ResolverChanged",
                    "PermissionChanged",
                ],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry", "registry_old"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    );
    let registrar_manifest = manifest_with_events(
        REGISTRAR_MANIFEST_ID,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            (
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationGranted",
                    "RegistrationRenewed",
                    "ExpiryChanged",
                    "PreimageObserved",
                ],
            ),
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                &[
                    "TokenControlTransferred",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
        ],
    );
    let wrapper_manifest = manifest_with_events(
        WRAPPER_MANIFEST_ID,
        "ens",
        "ens_v1_wrapper_l1",
        &[
            (
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                &["name_wrapper"],
                &[
                    "TokenControlTransferred",
                    "ExpiryChanged",
                    "PermissionScopeChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                    "PermissionChanged",
                    "PreimageObserved",
                ],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &[
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
        ],
    );
    let mut registry = admission(REGISTRY_MANIFEST_ID, "registry");
    registry.address = REGISTRY.to_owned();
    let mut old_registry = admission(REGISTRY_MANIFEST_ID, "registry_old");
    old_registry.address = OLD_REGISTRY.to_owned();
    old_registry.contract_instance_id = Uuid::from_u128(6_130);
    let mut registrar = admission(REGISTRAR_MANIFEST_ID, "registrar");
    registrar.address = REGISTRAR.to_owned();
    let mut wrapper = admission(WRAPPER_MANIFEST_ID, "name_wrapper");
    wrapper.address = WRAPPER.to_owned();
    wrapper.contract_instance_id = Uuid::from_u128(6_133);
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()])
        .parse()
        .expect("fixture node");
    (
        vec![registry_manifest, registrar_manifest, wrapper_manifest],
        vec![registry, old_registry, registrar, wrapper],
        node,
    )
}

fn prefix(owner: &str, node: B256, clear: bool) -> anyhow::Result<Vec<RawLogInput>> {
    let mut logs = vec![raw_at(
        v1_registry::NewResolver {
            node,
            resolver: RESOLVER_A.parse()?,
        }
        .encode_log_data(),
        1,
        0,
        REGISTRY,
    )];
    if clear {
        logs.push(raw_at(
            v1_registry::NewResolver {
                node,
                resolver: ZERO_ADDRESS.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        ));
    }
    logs.push(raw_at(
        v1_registry::Transfer {
            node,
            owner: owner.parse()?,
        }
        .encode_log_data(),
        if clear { 3 } else { 2 },
        0,
        REGISTRY,
    ));
    Ok(logs)
}

fn renewal(block: i64) -> RawLogInput {
    renewal_with_expiry(block, 9_999)
}

fn renewal_with_expiry(block: i64, expiry: u64) -> RawLogInput {
    raw_at(
        NameRenewed {
            name: "pointer".to_owned(),
            label: keccak256(b"pointer"),
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    )
}

fn registration(block: i64, expiry: u64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        NameRegistered {
            name: "pointer".to_owned(),
            label: keccak256(b"pointer"),
            owner: OWNER_2.parse()?,
            expires: U256::from(expiry),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    ))
}

fn wrapped(block: i64) -> anyhow::Result<RawLogInput> {
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()]).parse()?;
    Ok(raw_at(
        NameWrapped {
            node,
            name: b"\x07pointer\x03eth\0".to_vec().into(),
            owner: OWNER_2.parse()?,
            fuses: 1,
            expiry: 9_999,
        }
        .encode_log_data(),
        block,
        0,
        WRAPPER,
    ))
}

fn unwrapped(block: i64) -> anyhow::Result<RawLogInput> {
    let node = super::common::namehash(&["pointer".to_owned(), "eth".to_owned()]).parse()?;
    Ok(raw_at(
        NameUnwrapped {
            node,
            owner: OWNER_2.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        WRAPPER,
    ))
}

fn unsurfaced_wrapper_prior() -> anyhow::Result<PriorEventInput> {
    let output = interpret_test_batch(input(
        fixture().0,
        fixture().1,
        Vec::new(),
        vec![wrapped(2)?],
    ))?;
    let mut prior = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenControlTransferred")
        .map(prior_event)
        .expect("wrapper authority event");
    prior.after_state["surface_known"] = false.into();
    Ok(prior)
}

fn registrar_transfer(from: &str, to: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registrar::Transfer {
            from: from.parse()?,
            to: to.parse()?,
            tokenId: U256::from_be_slice(keccak256(b"pointer").as_slice()),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    ))
}

fn current_new_owner(owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::NewOwner {
            node: super::common::namehash(&["eth".to_owned()]).parse()?,
            label: keccak256(b"pointer"),
            owner: owner.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    ))
}

fn old_new_owner(owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    let mut log = current_new_owner(owner, block)?;
    log.emitting_address = OLD_REGISTRY.to_owned();
    Ok(log)
}

fn current_transfer(node: B256, owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::Transfer {
            node,
            owner: owner.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    ))
}

fn resolver_selection(
    registry: &str,
    node: B256,
    resolver: &str,
    block: i64,
) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::NewResolver {
            node,
            resolver: resolver.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        registry,
    ))
}

fn old_transfer(node: B256, owner: &str, block: i64) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        v1_registry::Transfer {
            node,
            owner: owner.parse()?,
        }
        .encode_log_data(),
        block,
        0,
        OLD_REGISTRY,
    ))
}

fn input(
    manifests: Vec<ManifestInput>,
    admissions: Vec<AddressAdmissionInput>,
    prior_events: Vec<PriorEventInput>,
    raw_logs: Vec<RawLogInput>,
) -> BatchInput {
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events,
        blocks: Vec::new(),
        raw_logs,
    }
}

fn run(owner: &str, clear: bool) -> anyhow::Result<(BatchOutput, BatchOutput)> {
    let (manifests, admissions, node) = fixture();
    let prefix_output = interpret_test_batch(input(
        manifests.clone(),
        admissions.clone(),
        Vec::new(),
        prefix(owner, node, clear)?,
    ))?;
    let mut logs = prefix(owner, node, clear)?;
    logs.push(renewal(if clear { 4 } else { 3 }));
    let output = interpret_test_batch(input(manifests, admissions, Vec::new(), logs))?;
    Ok((prefix_output, output))
}

fn run_live_and_restored(owner: &str, clear: bool) -> anyhow::Result<(BatchOutput, BatchOutput)> {
    let (manifests, admissions, node) = fixture();
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            prefix(owner, node, clear)?,
        ),
        None,
    )?;
    let suffix = renewal(if clear { 4 } else { 3 });
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let (restored, _) =
        interpret_test_batch_incremental(input(manifests, admissions, prior, vec![suffix]), None)?;
    Ok((live, restored))
}

fn prepare_with_provenance(
    mut batch: BatchInput,
    provenance: Vec<ManifestInput>,
    session: Option<AdapterSession>,
) -> anyhow::Result<(BatchOutput, AdapterSession)> {
    if batch.blocks.is_empty() {
        let mut blocks = std::collections::BTreeMap::new();
        for raw in &batch.raw_logs {
            blocks
                .entry((raw.block_number, raw.block_hash.clone()))
                .or_insert_with(|| RawBlockInput {
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: raw.block_number,
                    block_timestamp: raw.block_timestamp,
                    canonicality_state: raw.canonicality_state.clone(),
                });
        }
        batch.blocks = blocks.into_values().collect();
    }
    super::super::prepare_schema_v2_batch_incremental_with_provenance(
        batch,
        provenance,
        session,
        StateCacheCapacity::Unlimited,
    )?
    .finish(Vec::new())
}

fn state_derived_pointer(output: &BatchOutput) -> Option<&NormalizedEvent> {
    output.normalized_events.iter().find(|event| {
        event.event_kind == "ResolverChanged"
            && event.after_state["state_derived"] == true
            && event.after_state["surface_materialization"] == true
    })
}

fn compact_prior(events: &[NormalizedEvent]) -> Vec<PriorEventInput> {
    let prior = events.iter().map(prior_event).collect::<Vec<_>>();
    let mut last_index = std::collections::HashMap::new();
    for (index, event) in prior.iter().enumerate() {
        last_index.insert(event.retained_state_key.clone(), index);
    }
    prior
        .into_iter()
        .enumerate()
        .filter(|(index, event)| last_index[&event.retained_state_key] == *index)
        .map(|(_, event)| event)
        .collect()
}

fn run_batches(
    manifests: &[ManifestInput],
    admissions: &[AddressAdmissionInput],
    batches: Vec<Vec<RawLogInput>>,
) -> anyhow::Result<Vec<NormalizedEvent>> {
    let mut session = None;
    let mut events = Vec::new();
    for raw_logs in batches {
        let (output, next) = interpret_test_batch_incremental(
            input(
                manifests.to_vec(),
                admissions.to_vec(),
                Vec::new(),
                raw_logs,
            ),
            session,
        )?;
        events.extend(output.normalized_events);
        session = Some(next);
    }
    Ok(events)
}

/// Splits replay only at physical block boundaries.
fn assert_four_way_and_restore_parity(
    history: &[RawLogInput],
    prefix_len: usize,
) -> anyhow::Result<(Vec<NormalizedEvent>, BatchOutput)> {
    let (manifests, admissions, _) = fixture();
    let single = run_batches(&manifests, &admissions, vec![history.to_vec()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        history
            .chunk_by(|left, right| {
                left.block_number == right.block_number && left.block_hash == right.block_hash
            })
            .map(<[_]>::to_vec)
            .collect(),
    )?;
    assert!(
        prefix_len == history.len()
            || history[prefix_len - 1].block_hash != history[prefix_len].block_hash,
        "restore prefix must end at a physical block boundary"
    );
    let split_at_prefix = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..prefix_len].to_vec(),
            history[prefix_len..].to_vec(),
        ],
    )?;
    let alternate_at = history
        .iter()
        .position(|event| event.block_hash != history[0].block_hash)
        .unwrap_or(history.len());
    let alternate_split = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..alternate_at].to_vec(),
            history[alternate_at..].to_vec(),
        ],
    )?;
    assert_eq!(single, per_block, "per-block replay drift");
    assert_eq!(single, split_at_prefix, "prefix/suffix replay drift");
    assert_eq!(single, alternate_split, "alternate split replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            history[..prefix_len].to_vec(),
        ),
        None,
    )?;
    let suffix = history[prefix_len..].to_vec();
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            suffix.clone(),
        ),
        Some(session),
    )?;
    for prior in [
        prefix_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        compact_prior(&prefix_output.normalized_events),
    ] {
        let (restored, _) = interpret_test_batch_incremental(
            input(manifests.clone(), admissions.clone(), prior, suffix.clone()),
            None,
        )?;
        assert_eq!(live, restored, "cold restore drift");
    }
    Ok((single, live))
}

fn assert_current_registry_reassignment_replays(
    reassigned_owner: &str,
    transfer_suffix: bool,
) -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let prefix = vec![
        current_new_owner(OWNER, 1)?,
        raw_at(
            v1_registry::NewResolver {
                node,
                resolver: RESOLVER_A.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        ),
        current_new_owner(reassigned_owner, 3)?,
    ];
    let suffix = if transfer_suffix {
        current_transfer(node, OWNER_2, 4)?
    } else {
        renewal(4)
    };
    let mut history = prefix.clone();
    history.push(suffix.clone());

    let single = run_batches(&manifests, &admissions, vec![history.clone()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        history.iter().cloned().map(|event| vec![event]).collect(),
    )?;
    let split_three_one = run_batches(
        &manifests,
        &admissions,
        vec![history[..3].to_vec(), history[3..].to_vec()],
    )?;
    let split_two_two = run_batches(
        &manifests,
        &admissions,
        vec![history[..2].to_vec(), history[2..].to_vec()],
    )?;
    assert_eq!(single, per_block, "per-block replay drift");
    assert_eq!(single, split_three_one, "3|1 replay drift");
    assert_eq!(single, split_two_two, "2|2 replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(manifests.clone(), admissions.clone(), Vec::new(), prefix),
        None,
    )?;
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let full_prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let compacted_prior = compact_prior(&prefix_output.normalized_events);
    let (restored_full, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            full_prior,
            vec![suffix.clone()],
        ),
        None,
    )?;
    let (restored_compacted, _) = interpret_test_batch_incremental(
        input(manifests, admissions, compacted_prior, vec![suffix]),
        None,
    )?;
    assert_eq!(live, restored_full, "full restore drift");
    assert_eq!(live, restored_compacted, "compacted restore drift");
    Ok(())
}
#[test]
fn current_registry_reassignment_preserves_resolver_across_every_replay_shape() -> anyhow::Result<()>
{
    assert_current_registry_reassignment_replays(OWNER_2, false)?;
    assert_current_registry_reassignment_replays(OWNER, false)?;
    assert_current_registry_reassignment_replays(OWNER_2, true)
}
#[test]
fn registered_pre_surface_registry_authority_reactivates_after_expiry_in_every_replay_shape()
-> anyhow::Result<()> {
    const EXPIRY: u64 = 100;
    const RELEASE_BLOCK: i64 = 7_776_101;
    let (manifests, admissions, node) = fixture();
    let prefix = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        registration(3, EXPIRY)?,
    ];
    let release_trigger = current_transfer(B256::ZERO, OWNER_2, RELEASE_BLOCK)?;
    let mut history = prefix.clone();
    history.push(release_trigger.clone());

    let single = run_batches(&manifests, &admissions, vec![history.clone()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        history.iter().cloned().map(|event| vec![event]).collect(),
    )?;
    let split = run_batches(
        &manifests,
        &admissions,
        vec![prefix.clone(), vec![release_trigger.clone()]],
    )?;
    assert_eq!(single, per_block, "per-block replay drift");
    assert_eq!(single, split, "incremental replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(manifests.clone(), admissions.clone(), Vec::new(), prefix),
        None,
    )?;
    let registry_resource = prefix_output
        .normalized_events
        .iter()
        .find(|event| event.block_number == Some(1) && event.event_kind == "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("retained registry authority resource");
    let (live, live_session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![release_trigger.clone()],
        ),
        Some(session),
    )?;
    for prior in [
        prefix_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        compact_prior(&prefix_output.normalized_events),
    ] {
        let (restored, restored_session) = interpret_test_batch_incremental(
            input(
                manifests.clone(),
                admissions.clone(),
                prior,
                vec![release_trigger.clone()],
            ),
            None,
        )?;
        assert_eq!(live, restored, "cold restore output drift");
        assert_eq!(live_session, restored_session, "cold restore state drift");
    }
    let rebound = live
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(RELEASE_BLOCK)
                && event.event_kind == "SurfaceBound"
                && event.resource_id == Some(registry_resource)
        })
        .expect("expiry must reactivate the retained registry surface");
    assert_eq!(rebound.after_state["authority_kind"], "registry_only");
    assert_eq!(
        live_session
            .v1_name("ens", &format!("{node:#x}"))
            .and_then(|authority| authority.owner),
        Some(OWNER.to_owned()),
        "known name must retain its direct-registry owner after registrar expiry"
    );
    Ok(())
}

fn assert_original_unchanged(prefix: &BatchOutput, complete: &BatchOutput) {
    let original = prefix
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged" && event.after_state["resolver"] == RESOLVER_A
        })
        .expect("original pre-surface pointer");
    assert_eq!(original.logical_name_id, None);
    assert_eq!(original.resource_id, None);
    assert_eq!(
        complete
            .normalized_events
            .iter()
            .find(|event| event.event_identity == original.event_identity),
        Some(original),
        "surface materialization must not rewrite the raw-derived pointer"
    );
}
#[test]
fn pre_surface_registry_resolver_materialization_links_current_authority() -> anyhow::Result<()> {
    let (prefix, output) = run(OWNER, false)?;
    assert_original_unchanged(&prefix, &output);
    let authority_resource = prefix
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("registry-only authority resource");
    let pointer = state_derived_pointer(&output).expect("linked materialization pointer");
    assert_eq!(pointer.source_family, "ens_v1_registry_l1");
    assert_eq!(pointer.source_manifest_id, Some(REGISTRY_MANIFEST_ID));
    assert_eq!(pointer.resource_id, Some(authority_resource));
    assert_eq!(pointer.after_state["resolver"], RESOLVER_A);
    assert_eq!(pointer.block_number, Some(3));
    assert_eq!(pointer.transaction_hash.as_deref(), Some("transaction-3"));
    assert_eq!(pointer.log_index, Some(0));
    assert_eq!(pointer.after_state["source_event"], "NameRenewed");
    assert_eq!(output.surface_bindings.len(), 1);
    let surface = output
        .normalized_events
        .iter()
        .find(|event| event.block_number == Some(3) && event.event_kind == "SurfaceBound")
        .expect("surface materialization must make the retained owner projectable");
    assert_eq!(surface.resource_id, Some(authority_resource));
    assert_eq!(surface.after_state["owner"], OWNER);
    assert!(output.normalized_events.iter().all(|event| {
        !(event.block_number == Some(3)
            && matches!(
                event.event_kind.as_str(),
                "AuthorityEpochChanged" | "PermissionChanged"
            ))
    }));
    Ok(())
}
#[test]
fn name_registered_materialization_retains_registration_trigger_provenance() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let prefix = interpret_test_batch(input(
        manifests.clone(),
        admissions.clone(),
        Vec::new(),
        vec![
            old_new_owner(OWNER, 1)?,
            resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        ],
    ))?;
    let mut prior = prefix
        .normalized_events
        .iter()
        .map(prior_event)
        .collect::<Vec<_>>();
    prior.push(unsurfaced_wrapper_prior()?);
    let (output, _) = interpret_test_batch_incremental(
        input(manifests, admissions, prior, vec![registration(3, 9_999)?]),
        None,
    )?;
    let pointer = state_derived_pointer(&output)
        .expect("NameRegistered must materialize the retained registry pointer");
    assert_eq!(pointer.after_state["source_event"], "NameRegistered");
    Ok(())
}
#[test]
fn pre_surface_ownerless_resolver_materialization_links_read_anchor_without_control()
-> anyhow::Result<()> {
    let (prefix, output) = run(REGISTRY, false)?;
    assert_original_unchanged(&prefix, &output);
    let pointer = state_derived_pointer(&output).expect("ownerless retained-anchor pointer");
    assert_eq!(pointer.source_family, "ens_v1_registry_l1");
    assert_eq!(pointer.source_manifest_id, Some(REGISTRY_MANIFEST_ID));
    assert!(pointer.logical_name_id.is_some());
    assert!(pointer.resource_id.is_some());
    assert!(output.surface_bindings.is_empty());
    assert!(output.normalized_events.iter().all(|event| {
        !(event.block_number == Some(3)
            && matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "AuthorityEpochChanged" | "PermissionChanged"
            ))
    }));
    Ok(())
}
#[test]
fn old_registry_resolver_is_not_materialized_after_current_registry_migration() -> anyhow::Result<()>
{
    let (manifests, admissions, node) = fixture();
    let parent = super::common::namehash(&["eth".to_owned()]).parse()?;
    let label = keccak256(b"pointer");
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![
                raw_at(
                    v1_registry::NewResolver {
                        node,
                        resolver: RESOLVER_A.parse()?,
                    }
                    .encode_log_data(),
                    1,
                    0,
                    OLD_REGISTRY,
                ),
                raw_at(
                    v1_registry::NewOwner {
                        node: parent,
                        label,
                        owner: OWNER.parse()?,
                    }
                    .encode_log_data(),
                    2,
                    0,
                    REGISTRY,
                ),
            ],
        ),
        None,
    )?;
    let suffix = renewal(3);
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    assert!(prefix_output.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == RESOLVER_A
            && event.block_number == Some(1)
    }));
    let (restored, _) =
        interpret_test_batch_incremental(input(manifests, admissions, prior, vec![suffix]), None)?;
    assert!(
        state_derived_pointer(&live).is_none(),
        "the old fallback registry resolver must not surface after current-registry handoff"
    );
    assert_eq!(live, restored);
    Ok(())
}
#[test]
fn current_registry_transfer_invalidates_only_old_registry_resolver_links_in_every_replay_shape()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let migrated_history = vec![
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 1)?,
        current_transfer(node, OWNER, 2)?,
        renewal(3),
    ];
    let (single, live) = assert_four_way_and_restore_parity(&migrated_history, 2)?;
    assert!(single.iter().any(|event| {
        event.block_number == Some(1)
            && event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == RESOLVER_A
    }));
    assert!(
        state_derived_pointer(&live).is_none(),
        "a current-registry Transfer must end an old-registry fallback pointer"
    );

    let current_pointer_history = vec![
        current_new_owner(OWNER, 1)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        old_transfer(node, OWNER_2, 3)?,
        renewal(4),
    ];
    let (_, mirror_live) = assert_four_way_and_restore_parity(&current_pointer_history, 3)?;
    let pointer = state_derived_pointer(&mirror_live)
        .expect("an old-registry Transfer must not clear a current-registry resolver pointer");
    assert_eq!(pointer.after_state["resolver"], RESOLVER_A);
    Ok(())
}
#[test]
fn same_owner_transfer_retracts_selected_old_registry_fallback_across_every_replay_shape()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        current_transfer(node, OWNER, 3)?,
        renewal(4),
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 3)?;
    assert!(state_derived_pointer(&live).is_none());
    assert!(single.iter().any(|event| {
        event.block_number == Some(3)
            && event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == ZERO_ADDRESS
            && event.after_state["registry_fallback_handoff"] == true
    }));
    Ok(())
}
#[test]
fn same_owner_transfer_without_a_pointer_still_suppresses_later_old_registry_logs()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        current_transfer(node, OWNER, 2)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 3)?,
        renewal(4),
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 2)?;
    assert!(state_derived_pointer(&live).is_none());
    assert!(single.iter().all(|event| {
        !(event.block_number == Some(3)
            && event.event_kind == "ResolverChanged"
            && event.after_state["resolver"] == RESOLVER_A)
    }));
    assert!(single.iter().any(|event| {
        event.block_number == Some(2)
            && event.event_kind == "AuthorityTransferred"
            && event.after_state["source_event"] == "Transfer"
    }));
    Ok(())
}
#[test]
fn current_registry_record_retracts_a_surfaced_old_registry_resolver() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    for ownership in [
        current_transfer(node, OWNER, 4)?,
        current_new_owner(OWNER, 4)?,
    ] {
        let history = vec![
            old_new_owner(OWNER, 1)?,
            resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
            renewal(3),
            ownership,
            renewal(5),
        ];
        let (single, live) = assert_four_way_and_restore_parity(&history, 4)?;
        assert!(state_derived_pointer(&live).is_none());
        let clear = single
            .iter()
            .find(|event| {
                event.block_number == Some(4)
                    && event.event_kind == "ResolverChanged"
                    && event.after_state["registry_fallback_handoff"] == true
            })
            .expect("current registry ownership must retract the surfaced old pointer");
        assert_eq!(clear.after_state["resolver"], ZERO_ADDRESS);
        assert_eq!(clear.after_state["previous_resolver"], RESOLVER_A);
        assert!(clear.logical_name_id.is_some());
        assert!(clear.resource_id.is_some());
        assert!(single.iter().any(|event| {
            event.block_number == Some(4)
                && event.event_kind == "PermissionChanged"
                && event.after_state["effective_powers"] == json!([])
                && event.after_state["scope"]["resolver_address"] == RESOLVER_A
        }));
    }
    Ok(())
}
#[test]
fn current_registry_handoff_retracts_old_resolver_from_every_linked_resource() -> anyhow::Result<()>
{
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        registration(3, 100)?,
        current_transfer(B256::ZERO, OWNER_2, 7_776_101)?,
        registration(7_776_102, 9_999)?,
        current_new_owner(OWNER, 7_776_103)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 4)?;
    let linked_nonzero = single
        .iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_A
                && event.resource_id.is_some()
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(linked_nonzero.len(), 3);
    for resource_id in linked_nonzero {
        assert!(
            single.iter().any(|event| {
                event.block_number == Some(7_776_103)
                    && event.event_kind == "ResolverChanged"
                    && event.resource_id == Some(resource_id)
                    && event.after_state["resolver"] == ZERO_ADDRESS
                    && event.after_state["registry_fallback_handoff"] == true
            }),
            "current-registry handoff did not retract resolver from {resource_id}"
        );
    }
    Ok(())
}
#[test]
fn same_transaction_registration_keeps_fallback_clear_on_registry_resource() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let mut ownership = current_new_owner(OWNER_2, 4)?;
    ownership.log_index = 0;
    let mut registered = registration(4, 9_999)?;
    registered.log_index = 1;
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        ownership,
        registered,
    ];
    let output = interpret_test_batch(input(fixture().0, fixture().1, Vec::new(), history))?;
    let registry_resource = output
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_A
                && event.after_state["state_derived"] == true
        })
        .and_then(|event| event.resource_id)
        .expect("materialization must link the old-registry resolver to the registry resource");
    assert!(
        output.normalized_events.iter().any(|event| {
            event.block_number == Some(4)
                && event.event_kind == "ResolverChanged"
                && event.resource_id == Some(registry_resource)
                && event.after_state["resolver"] == ZERO_ADDRESS
                && event.after_state["registry_fallback_handoff"] == true
        }),
        "same-transaction reconciliation moved the fallback clear off the registry resource"
    );
    Ok(())
}
#[test]
fn same_transaction_transient_setup_reinserts_each_fallback_clear() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let mut controller_ownership = current_new_owner(REGISTRAR, 5)?;
    controller_ownership.log_index = 0;
    let mut resolver_clear = resolver_selection(REGISTRY, node, ZERO_ADDRESS, 5)?;
    resolver_clear.log_index = 1;
    let mut final_ownership = current_new_owner(OWNER_2, 5)?;
    final_ownership.log_index = 2;
    let mut registered = registration(5, 9_999)?;
    registered.log_index = 3;
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        registrar_transfer(OWNER, OWNER, 4)?,
        controller_ownership,
        resolver_clear,
        final_ownership,
        registered,
    ];
    let output = interpret_test_batch(input(fixture().0, fixture().1, Vec::new(), history))?;
    let linked_resources = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number < Some(5)
                && event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_A
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(linked_resources.len(), 2, "fixture must link two resources");
    for resource_id in linked_resources {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.block_number == Some(5)
                    && event.event_kind == "ResolverChanged"
                    && event.resource_id == Some(resource_id)
                    && event.after_state["resolver"] == ZERO_ADDRESS
                    && event.after_state["registry_fallback_handoff"] == true
            }),
            "same-transaction transient setup dropped the fallback clear for {resource_id}"
        );
    }
    Ok(())
}
#[test]
fn current_registry_handoff_retracts_old_resolver_from_wrapper_resource_across_replay_shapes()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        wrapped(4)?,
        current_new_owner(OWNER_2, 5)?,
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 4)?;
    let wrapper_resource = single
        .iter()
        .find(|event| {
            event.block_number == Some(4)
                && event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .and_then(|event| event.resource_id)
        .expect("wrapper authority resource");
    assert!(single.iter().any(|event| {
        event.block_number == Some(4)
            && event.event_kind == "ResolverChanged"
            && event.resource_id == Some(wrapper_resource)
            && event.after_state["resolver"] == RESOLVER_A
    }));
    assert!(live.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.resource_id == Some(wrapper_resource)
            && event.after_state["resolver"] == ZERO_ADDRESS
            && event.after_state["registry_fallback_handoff"] == true
    }));
    Ok(())
}
#[test]
fn current_registry_handoff_retracts_old_resolver_from_historical_wrapper_resource()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 2)?,
        renewal(3),
        wrapped(4)?,
        unwrapped(5)?,
        current_new_owner(OWNER_2, 6)?,
    ];
    let (single, live) = assert_four_way_and_restore_parity(&history, 5)?;
    let wrapper_resource = single
        .iter()
        .find(|event| {
            event.block_number == Some(4)
                && event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .and_then(|event| event.resource_id)
        .expect("wrapper authority resource");
    assert!(single.iter().any(|event| {
        event.block_number == Some(4)
            && event.event_kind == "ResolverChanged"
            && event.resource_id == Some(wrapper_resource)
            && event.after_state["resolver"] == RESOLVER_A
    }));
    assert!(
        live.normalized_events.iter().any(|event| {
            event.event_kind == "ResolverChanged"
                && event.resource_id == Some(wrapper_resource)
                && event.after_state["resolver"] == ZERO_ADDRESS
                && event.after_state["registry_fallback_handoff"] == true
        }),
        "current-registry handoff left a stale pointer on the historical wrapper resource"
    );
    Ok(())
}
#[test]
fn changed_old_registry_selection_restores_inactive_resources_until_handoff_or_reactivation()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    for (replacement, reactivate) in [(RESOLVER_B, false), (ZERO_ADDRESS, true)] {
        let mut history = vec![
            old_new_owner(OWNER, 1)?,
            registration(2, 9_999)?,
            resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 3)?,
            wrapped(4)?,
        ];
        if reactivate {
            history.push(resolver_selection(OLD_REGISTRY, node, replacement, 5)?);
            history.push(unwrapped(6)?);
        } else {
            history.push(unwrapped(5)?);
            history.push(resolver_selection(OLD_REGISTRY, node, replacement, 6)?);
            history.push(current_new_owner(OWNER_2, 7)?);
        }
        let (final_block, resource_block, resource_kind) = if reactivate {
            (6, 2, "RegistrationGranted")
        } else {
            (7, 4, "TokenControlTransferred")
        };
        let (single, live) = assert_four_way_and_restore_parity(&history, history.len() - 1)?;
        let inactive_resource = single
            .iter()
            .find_map(|event| {
                (event.block_number == Some(resource_block) && event.event_kind == resource_kind)
                    .then_some(event.resource_id)
                    .flatten()
            })
            .expect("inactive old-registry-linked resource");
        let clear = live
            .normalized_events
            .iter()
            .find(|event| {
                event.block_number == Some(final_block)
                    && event.event_kind == "ResolverChanged"
                    && event.resource_id == Some(inactive_resource)
            })
            .expect("inactive resource must receive a resolver clear");
        assert_eq!(clear.after_state["resolver"], ZERO_ADDRESS);
    }
    Ok(())
}
#[test]
fn current_registry_resolver_replacement_survives_the_ownership_handoff() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        registration(2, 9_999)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 3)?,
        resolver_selection(REGISTRY, node, RESOLVER_B, 4)?,
        current_new_owner(OWNER_2, 5)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 4)?;
    assert_eq!(
        single
            .iter()
            .filter(|event| {
                event.block_number == Some(4)
                    && event.event_kind == "ResolverChanged"
                    && event.after_state["resolver"] == RESOLVER_B
            })
            .count(),
        2,
        "current selection must reach both resources"
    );
    assert_eq!(
        single
            .iter()
            .filter(|event| {
                event.block_number == Some(5)
                    && event.event_kind == "ResolverChanged"
                    && event.after_state["resolver"] == ZERO_ADDRESS
            })
            .count(),
        0,
        "ownership handoff cleared a current-registry resolver"
    );
    Ok(())
}
#[test]
fn old_registry_raw_resolver_survives_authority_reconvergence_compaction() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER_2, 1)?,
        registration(2, 9_999)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 3)?,
        old_new_owner(OWNER, 4)?,
        old_new_owner(OWNER_2, 5)?,
        wrapped(6)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 5)?;
    assert_eq!(
        single
            .iter()
            .filter(|event| {
                event.block_number == Some(6)
                    && event.event_kind == "ResolverChanged"
                    && event.after_state["resolver"] == RESOLVER_A
                    && event.after_state["resolver_source_role"] == "registry_old"
            })
            .count(),
        1,
        "wrapper activation omitted the restored old-registry resolver",
    );

    let (manifests, admissions, _) = fixture();
    let prefix = interpret_test_batch(input(
        manifests,
        admissions,
        Vec::new(),
        history[..5].to_vec(),
    ))?;
    let compacted = compact_prior(&prefix.normalized_events);
    assert_eq!(
        compacted
            .iter()
            .filter(|event| {
                event.event_kind == "ResolverChanged"
                    && event.after_state["emitter_role"] == "registry_old"
                    && event.after_state["resolver_source_role"].is_null()
                    && event.retained_state_key.ends_with(":NewResolver")
            })
            .count(),
        2,
        "compaction discarded raw old-registry NewResolver rows",
    );
    Ok(())
}
#[test]
fn registry_reactivation_after_old_registry_zero_clears_the_registrar_resource()
-> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        old_new_owner(OWNER, 1)?,
        registration(2, 9_999)?,
        resolver_selection(OLD_REGISTRY, node, RESOLVER_A, 3)?,
        old_new_owner(OWNER_2, 4)?,
        resolver_selection(OLD_REGISTRY, node, ZERO_ADDRESS, 5)?,
        old_new_owner(OWNER, 6)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 5)?;
    let registrar_resource = single
        .iter()
        .find_map(|event| {
            (event.block_number == Some(2) && event.event_kind == "RegistrationGranted")
                .then_some(event.resource_id)
                .flatten()
        })
        .expect("registrar resource");
    assert_eq!(
        single
            .iter()
            .filter(|event| {
                event.block_number == Some(6)
                    && event.event_kind == "ResolverChanged"
                    && event.resource_id == Some(registrar_resource)
                    && event.after_state["resolver"] == ZERO_ADDRESS
            })
            .count(),
        1,
        "registry-path reactivation did not clear the registrar resource",
    );
    Ok(())
}
#[test]
fn unwrap_after_current_registry_zero_keeps_the_registrar_resource_clear() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        current_new_owner(OWNER_2, 1)?,
        registration(2, 9_999)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 3)?,
        wrapped(4)?,
        resolver_selection(REGISTRY, node, ZERO_ADDRESS, 5)?,
        unwrapped(6)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 5)?;
    let registrar_resource = single
        .iter()
        .find_map(|event| {
            (event.block_number == Some(2) && event.event_kind == "RegistrationGranted")
                .then_some(event.resource_id)
                .flatten()
        })
        .expect("registrar resource");
    assert!(
        single.iter().any(|event| event.block_number == Some(6)
            && event.event_kind == "ResolverChanged"
            && event.resource_id == Some(registrar_resource)
            && event.after_state["resolver"] == ZERO_ADDRESS),
        "unwrap reactivated the registrar resource's stale current-registry pointer",
    );
    Ok(())
}
#[test]
fn current_registry_zero_to_nonzero_grants_without_revoking_the_zero_address() -> anyhow::Result<()>
{
    let (_, _, node) = fixture();
    let history = vec![
        current_new_owner(OWNER_2, 1)?,
        registration(2, 9_999)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 3)?,
        resolver_selection(REGISTRY, node, ZERO_ADDRESS, 4)?,
        resolver_selection(REGISTRY, node, RESOLVER_B, 5)?,
    ];
    let (_, live) = assert_four_way_and_restore_parity(&history, 4)?;
    assert!(live.normalized_events.iter().any(|event| {
        event.block_number == Some(5)
            && event.event_kind == "ResolverChanged"
            && event.before_state["resolver"] == ZERO_ADDRESS
            && event.after_state["resolver"] == RESOLVER_B
    }));
    let resolver_permissions = live
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(5) && event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(resolver_permissions.len(), 1);
    let grant = resolver_permissions[0];
    assert_eq!(grant.after_state["scope"]["resolver_address"], RESOLVER_B);
    assert_eq!(
        grant.after_state["effective_powers"],
        json!(["resolver_control"])
    );
    assert!(grant.after_state["grant_source"].is_object());
    assert!(grant.after_state["revocation_source"].is_null());
    Ok(())
}
#[test]
fn wrapper_fallback_registrar_activation_grants_resolver_control() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let mut wrap = wrapped(1)?;
    wrap.log_index = 1;
    let mut unwrap = unwrapped(3)?;
    unwrap.log_index = 1;
    let mut transfer = registrar_transfer(WRAPPER, OWNER_2, 3)?;
    transfer.log_index = 2;
    let history = vec![
        current_transfer(node, WRAPPER, 1)?,
        wrap,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        current_transfer(node, OWNER_2, 3)?,
        unwrap,
        transfer,
    ];
    let output = interpret_test_batch(input(manifests, admissions, Vec::new(), history))?;
    let active_resource = output
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(3) && event.event_kind == "TokenControlTransferred"
        })
        .and_then(|event| event.resource_id)
        .expect("fallback registrar resource");
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.block_number == Some(3)
                && event.event_kind == "PermissionChanged"
                && event.resource_id == Some(active_resource)
                && event.after_state["subject"] == OWNER_2
                && event.after_state["scope"]["resolver_address"] == RESOLVER_A
                && event.after_state["effective_powers"] == json!(["resolver_control"]))
            .count(),
        1,
    );

    let mut inactive_wrap = wrapped(4)?;
    inactive_wrap.log_index = 1;
    let mut inactive_unwrap = unwrapped(6)?;
    inactive_unwrap.log_index = 1;
    let mut inactive_transfer = registrar_transfer(WRAPPER, OWNER_2, 6)?;
    inactive_transfer.log_index = 2;
    let registry_activation_history = vec![
        current_transfer(node, WRAPPER, 4)?,
        inactive_wrap,
        resolver_selection(REGISTRY, node, RESOLVER_A, 5)?,
        current_transfer(node, OWNER, 6)?,
        inactive_unwrap,
        inactive_transfer,
        current_transfer(node, OWNER_2, 7)?,
    ];
    let registry_activation = interpret_test_batch(input(
        fixture().0,
        fixture().1,
        Vec::new(),
        registry_activation_history,
    ))?;
    let registry_resource = registry_activation
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(6)
                && event.event_kind == "AuthorityEpochChanged"
                && event.after_state["authority_kind"] == "registry_only"
        })
        .and_then(|event| event.resource_id)
        .expect("registry authority resource");
    for (scope_kind, power) in [
        ("resolver", "resolver_control"),
        ("resource", "resource_control"),
    ] {
        assert_eq!(
            registry_activation
                .normalized_events
                .iter()
                .filter(|event| {
                    event.block_number == Some(6)
                        && event.event_kind == "PermissionChanged"
                        && event.resource_id == Some(registry_resource)
                        && event.after_state["subject"] == OWNER
                        && event.after_state["scope"]["kind"] == scope_kind
                        && event.after_state["effective_powers"] == json!([power])
                })
                .count(),
            1,
            "registrar-driven registry activation omitted the {power} grant",
        );
        assert_eq!(
            registry_activation
                .normalized_events
                .iter()
                .filter(|event| {
                    event.block_number == Some(7)
                        && event.event_kind == "PermissionChanged"
                        && event.resource_id == Some(registry_resource)
                        && event.after_state["subject"] == OWNER
                        && event.after_state["scope"]["kind"] == scope_kind
                        && event.after_state["effective_powers"] == json!([])
                })
                .count(),
            1,
            "registry transfer did not balance the {power} grant",
        );
    }

    let mut ownerless_wrap = wrapped(8)?;
    ownerless_wrap.log_index = 1;
    let mut ownerless_unwrap = unwrapped(10)?;
    ownerless_unwrap.log_index = 1;
    let mut ownerless_transfer = registrar_transfer(WRAPPER, OWNER_2, 10)?;
    ownerless_transfer.log_index = 2;
    let ownerless_history = vec![
        current_transfer(node, WRAPPER, 8)?,
        ownerless_wrap,
        resolver_selection(REGISTRY, node, RESOLVER_A, 9)?,
        current_transfer(node, ZERO_ADDRESS, 10)?,
        ownerless_unwrap,
        ownerless_transfer,
    ];
    let ownerless = interpret_test_batch(input(
        fixture().0,
        fixture().1,
        Vec::new(),
        ownerless_history,
    ))?;
    assert!(ownerless.normalized_events.iter().all(|event| {
        event.block_number != Some(10)
            || event.event_kind != "AuthorityEpochChanged"
            || event.after_state["authority_kind"] != "registry_only"
    }));
    assert_eq!(
        ownerless
            .normalized_events
            .iter()
            .filter(|event| {
                event.block_number == Some(10)
                    && event.event_kind == "PermissionChanged"
                    && event.after_state["scope"]["resolver_address"] == RESOLVER_A
                    && event.after_state["effective_powers"] == json!(["resolver_control"])
            })
            .count(),
        0,
        "wrapper fallback granted resolver control when no authority activated",
    );

    Ok(())
}
#[test]
fn registrar_transfer_retires_registry_resource_control() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        current_new_owner(OWNER_2, 7)?,
        registration(8, 9_999)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 9)?,
        current_new_owner(OWNER, 10)?,
        registrar_transfer(OWNER_2, OWNER, 11)?,
    ];
    let (events, _) = assert_four_way_and_restore_parity(&history, 4)?;
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    let registrar_resource = events
        .iter()
        .find(|event| event.block_number == Some(8) && event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar resource");
    for (resource, kind, powers) in [
        (registry_resource, "resource", json!([])),
        (registry_resource, "resolver", json!([])),
        (registrar_resource, "resource", json!(["resource_control"])),
        (registrar_resource, "resolver", json!(["resolver_control"])),
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.block_number == Some(11)
                    && event.event_kind == "PermissionChanged"
                    && event.resource_id == Some(resource)
                    && event.after_state["subject"] == OWNER
                    && event.after_state["scope"]["kind"] == kind
                    && event.after_state["effective_powers"] == powers)
                .count(),
            1,
            "registrar-driven registry-to-registrar permission transition was not emitted exactly once"
        );
    }
    Ok(())
}
#[test]
fn registrar_transfer_grants_registry_resource_control() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        current_new_owner(OWNER_2, 7)?,
        registration(8, 9_999)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 9)?,
        registrar_transfer(OWNER_2, OWNER, 10)?,
        current_transfer(node, OWNER, 11)?,
    ];
    assert_four_way_and_restore_parity(&history[..4], 3)?;
    let (events, _) = assert_four_way_and_restore_parity(&history, 4)?;
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    for (block, powers, message) in [
        (
            10,
            json!(["resource_control"]),
            "registrar-driven registrar-to-registry transition did not grant resource_control on the registry resource",
        ),
        (
            11,
            json!([]),
            "registry-driven registry-to-registrar transition did not revoke resource_control on the registry resource",
        ),
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.block_number == Some(block)
                    && event.event_kind == "PermissionChanged"
                    && event.resource_id == Some(registry_resource)
                    && event.after_state["subject"] == OWNER_2
                    && event.after_state["scope"]["kind"] == "resource"
                    && event.after_state["effective_powers"] == powers)
                .count(),
            1,
            "{message}",
        );
    }
    Ok(())
}
#[test]
fn registrar_transfer_preserves_explicit_ownerless_registry_state() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let history = vec![
        old_new_owner(ZERO_ADDRESS, 1)?,
        renewal(2),
        registrar_transfer(OWNER, OWNER_2, 3)?,
    ];
    let (_, live) = assert_four_way_and_restore_parity(&history, 2)?;
    assert!(live.surface_bindings.is_empty());
    assert!(
        live.normalized_events.iter().all(|event| {
            !matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "AuthorityEpochChanged"
            )
        }),
        "registrar transfer reopened ownerless registry control"
    );
    let transfer_permissions = live
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(3) && event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(
        transfer_permissions.len(),
        2,
        "registrar token transfer lost its resource-scoped audit rows"
    );
    assert!(transfer_permissions.iter().all(|event| {
        event.after_state["scope"]["kind"] == "resource"
            && event.after_state["scope"].get("resolver_address").is_none()
    }));
    assert!(transfer_permissions.iter().any(|event| {
        event.after_state["subject"] == OWNER && event.after_state["effective_powers"] == json!([])
    }));
    assert!(transfer_permissions.iter().any(|event| {
        event.after_state["subject"] == OWNER_2
            && event.after_state["effective_powers"] == json!(["resource_control"])
    }));
    let (_, session) =
        interpret_test_batch_incremental(input(manifests, admissions, Vec::new(), history), None)?;
    assert!(
        session.v1_name("ens", &format!("{node:#x}")).is_none(),
        "registrar transfer made the retained registrar current"
    );
    Ok(())
}
#[test]
fn parent_resolver_does_not_capture_child_authority_link() -> anyhow::Result<()> {
    let (_, _, child) = fixture();
    let parent = super::common::namehash(&["eth".to_owned()]).parse()?;
    let mut child_divergence = old_new_owner(OWNER, 5)?;
    child_divergence.log_index = 0;
    let mut parent_handoff = current_transfer(parent, OWNER, 5)?;
    parent_handoff.log_index = 1;
    let history = vec![
        resolver_selection(OLD_REGISTRY, parent, RESOLVER_A, 1)?,
        old_new_owner(OWNER_2, 2)?,
        registration(3, 9_999)?,
        resolver_selection(OLD_REGISTRY, child, RESOLVER_A, 4)?,
        child_divergence,
        parent_handoff,
    ];
    let (events, _) = assert_four_way_and_restore_parity(&history, 4)?;
    let child_resource = events
        .iter()
        .find(|event| {
            event.block_number == Some(5)
                && event.log_index == Some(0)
                && event.event_kind == "AuthorityEpochChanged"
        })
        .and_then(|event| event.resource_id)
        .expect("child authority resource");
    assert!(
        events.iter().all(|event| event.block_number != Some(5)
            || event.event_kind != "ResolverChanged"
            || event.resource_id != Some(child_resource)
            || event.after_state["registry_fallback_handoff"] != true),
        "parent handoff cleared a child authority resolver"
    );
    Ok(())
}
#[test]
fn wrapper_first_surface_links_the_retained_registry_read_resource() -> anyhow::Result<()> {
    let (_, _, node) = fixture();
    let history = vec![
        current_transfer(node, OWNER, 1)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        wrapped(3)?,
        current_transfer(node, ZERO_ADDRESS, 4)?,
        unwrapped(5)?,
    ];
    let (single, _) = assert_four_way_and_restore_parity(&history, 3)?;
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    assert!(
        single.iter().any(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.source_family == "ens_v1_registry_l1"
                && event.logical_name_id.is_some()
                && event.resource_id == Some(registry_resource)
                && event.after_state["resolver"] == RESOLVER_A
                && event.after_state["surface_materialization"] == true
        }),
        "wrapper-first materialization did not link the retained registry read resource"
    );
    assert!(
        single.iter().all(|event| {
            !(event.block_number == Some(3)
                && event.event_kind == "SurfaceBound"
                && event.resource_id == Some(registry_resource))
        }),
        "wrapper-first materialization must not bind the dormant registry resource"
    );
    Ok(())
}
#[test]
fn same_transaction_registration_keeps_wrapper_surface_pointer_on_registry_resource()
-> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let mut wrapper_surface = wrapped(3)?;
    wrapper_surface.log_index = 0;
    let mut registered = registration(3, 9_999)?;
    registered.log_index = 1;
    let history = vec![
        current_transfer(node, OWNER, 1)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 2)?,
        wrapper_surface,
        registered,
        current_transfer(node, ZERO_ADDRESS, 4)?,
        unwrapped(5)?,
    ];
    let single = run_batches(&manifests, &admissions, vec![history.clone()])?;
    let per_block = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..1].to_vec(),
            history[1..2].to_vec(),
            history[2..4].to_vec(),
            history[4..5].to_vec(),
            history[5..].to_vec(),
        ],
    )?;
    let split = run_batches(
        &manifests,
        &admissions,
        vec![
            history[..2].to_vec(),
            history[2..4].to_vec(),
            history[4..].to_vec(),
        ],
    )?;
    assert_eq!(single, per_block, "physical-block replay drift");
    assert_eq!(single, split, "split replay drift");

    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            history[..4].to_vec(),
        ),
        None,
    )?;
    let suffix = history[4..].to_vec();
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            suffix.clone(),
        ),
        Some(session),
    )?;
    for prior in [
        prefix_output
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        compact_prior(&prefix_output.normalized_events),
    ] {
        let (restored, _) = interpret_test_batch_incremental(
            input(manifests.clone(), admissions.clone(), prior, suffix.clone()),
            None,
        )?;
        assert_eq!(live, restored, "cold restore drift");
    }

    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    assert!(
        single.iter().any(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.source_family == "ens_v1_registry_l1"
                && event.resource_id == Some(registry_resource)
                && event.after_state["resolver"] == RESOLVER_A
                && event.after_state["surface_materialization"] == true
                && event.after_state["source_event"] == "NameWrapped"
        }),
        "same-transaction registration moved the wrapper surface pointer off the registry resource"
    );
    Ok(())
}

#[test]
fn current_registry_ownership_preserves_the_old_registry_root_resolver() -> anyhow::Result<()> {
    const ROOT_NODE: B256 = B256::ZERO;
    let history = vec![
        resolver_selection(OLD_REGISTRY, ROOT_NODE, RESOLVER_A, 1)?,
        current_transfer(ROOT_NODE, OWNER, 2)?,
        resolver_selection(OLD_REGISTRY, ROOT_NODE, RESOLVER_B, 3)?,
    ];
    let (_, live) = assert_four_way_and_restore_parity(&history, 2)?;
    assert!(
        live.normalized_events.iter().all(|event| {
            !(event.event_kind == "ResolverChanged"
                && event.after_state["registry_fallback_handoff"] == true)
        }),
        "current-registry ownership incorrectly cleared the old-registry root resolver exception"
    );
    let replacement = live
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(3)
                && event.event_kind == "ResolverChanged"
                && event.after_state["resolver"] == RESOLVER_B
        })
        .expect("later old-registry root resolver update");
    assert_eq!(replacement.before_state["resolver"], RESOLVER_A);
    Ok(())
}

#[test]
fn latest_pre_surface_zero_resolver_clear_suppresses_materialization_pointer() -> anyhow::Result<()>
{
    for owner in [OWNER, REGISTRY] {
        let (output, restored) = run_live_and_restored(owner, true)?;
        assert_eq!(output, restored, "owner case {owner} restore drift");
        assert_eq!(
            output
                .normalized_events
                .iter()
                .filter(|event| event.after_state["state_derived"] == true
                    && event.event_kind == "ResolverChanged")
                .count(),
            0,
            "owner case {owner} resurrected resolver A"
        );
        assert!(output.normalized_events.iter().all(|event| {
            !(event.block_number == Some(4) && event.after_state["resolver"] == RESOLVER_A)
        }));
        if owner == OWNER {
            assert!(output.normalized_events.iter().any(|event| {
                event.block_number == Some(4)
                    && event.event_kind == "SurfaceBound"
                    && event.after_state["state_derived"] == true
            }));
        } else {
            assert!(output.surface_bindings.is_empty());
        }
    }
    Ok(())
}

fn assert_restore(owner: &str) -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            prefix(owner, node, false)?,
        ),
        None,
    )?;
    let suffix = renewal(3);
    let (live, _) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let (restored, _) =
        interpret_test_batch_incremental(input(manifests, admissions, prior, vec![suffix]), None)?;
    assert!(state_derived_pointer(&live).is_some());
    assert_eq!(live, restored);
    Ok(())
}
#[test]
fn pre_surface_registry_resolver_surface_promotion_restores_exactly() -> anyhow::Result<()> {
    assert_restore(OWNER)
}
#[test]
fn pre_surface_ownerless_resolver_surface_promotion_restores_exactly() -> anyhow::Result<()> {
    assert_restore(REGISTRY)
}
#[test]
fn deprecated_registry_manifest_remains_available_for_materialization_provenance()
-> anyhow::Result<()> {
    let (provenance, admissions, node) = fixture();
    let active = vec![provenance[1].clone()];
    let registrar_admissions = vec![admissions[2].clone()];
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            provenance.clone(),
            admissions,
            Vec::new(),
            prefix(OWNER, node, false)?,
        ),
        None,
    )?;
    let suffix = renewal(3);
    let (live, _) = prepare_with_provenance(
        input(
            active.clone(),
            registrar_admissions.clone(),
            Vec::new(),
            vec![suffix.clone()],
        ),
        provenance.clone(),
        Some(session),
    )?;
    let prior = prefix_output
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    let (restored, _) = prepare_with_provenance(
        input(active, registrar_admissions, prior, vec![suffix]),
        provenance,
        None,
    )?;
    let pointer = state_derived_pointer(&live).expect("deprecated-manifest pointer");
    assert_eq!(pointer.source_manifest_id, Some(REGISTRY_MANIFEST_ID));
    assert_eq!(pointer.source_family, "ens_v1_registry_l1");
    assert_eq!(live, restored);
    Ok(())
}
#[test]
fn unknown_registry_manifest_fails_live_and_cold_restore_with_context() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let active = vec![manifests[1].clone()];
    let registrar_admissions = vec![admissions[2].clone()];
    let (prefix_output, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            prefix(OWNER, node, false)?,
        ),
        None,
    )?;
    let live_error = prepare_with_provenance(
        input(
            active.clone(),
            registrar_admissions.clone(),
            Vec::new(),
            vec![renewal(3)],
        ),
        active.clone(),
        Some(session),
    )
    .expect_err("live materialization must reject unknown provenance");

    let mut history = prefix(OWNER, node, false)?;
    history.push(renewal(3));
    let complete = interpret_test_batch(input(manifests, admissions, Vec::new(), history))?;
    let mut restore = super::super::begin_schema_v2_adapter_restore_with_provenance(
        CHAIN.to_owned(),
        active.clone(),
        active,
        Vec::new(),
        registrar_admissions,
        StateCacheCapacity::Unlimited,
    )?;
    let restore_error = restore
        .apply_prior_events(complete.normalized_events.iter().map(prior_event).collect())
        .expect_err("cold restore must reject unknown provenance");
    let expected = format!(
        "state-derived source manifest is missing for namespace ens, namehash {node:#x}, manifest {REGISTRY_MANIFEST_ID}"
    );
    assert!(
        format!("{live_error:#}").contains(&expected),
        "{live_error:#}"
    );
    assert!(
        restore_error.to_string().contains(&expected),
        "{restore_error:#}"
    );
    assert!(prefix_output.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.source_manifest_id == Some(REGISTRY_MANIFEST_ID)
    }));
    Ok(())
}

#[test]
fn surfaced_transfer_fallback_without_manifest_never_requires_materialization_provenance()
-> anyhow::Result<()> {
    let mut state = super::super::state::State::new(Vec::new(), Vec::new());
    let namehash = format!("{:#x}", B256::ZERO);
    let authority = super::super::state::V1NameState {
        logical_name_id: format!("ens:{namehash}"),
        surface_known: true,
        resource_id: Uuid::from_u128(613),
        token_lineage_id: None,
        authority_source_family: "ens_v1_registry_l1".to_owned(),
        source_manifest_id: None,
        labelhash: Some(format!("{:#x}", B256::ZERO)),
        expiry: None,
        owner: Some(OWNER.to_owned()),
        registry_contract: None,
        authority_key: Some("transfer-fallback".to_owned()),
        wrapper_fallback: false,
    };
    state.remember_v1_registry_authority("ens", &namehash, authority.clone());
    state.activate_v1_authority("ens", &namehash, Some(authority));
    assert_eq!(
        state.materialize_v1_active_surface(
            "ens",
            &namehash,
            &format!("ens:{namehash}"),
            &format!("{:#x}", B256::ZERO),
        )?,
        super::super::state::V1SurfaceMaterialization::AlreadyMaterialized
    );
    Ok(())
}

#[test]
fn ownerless_renewal_refreshes_already_current_registrar_state() -> anyhow::Result<()> {
    let (manifests, admissions, node) = fixture();
    let (initial, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![renewal_with_expiry(1, 100)],
        ),
        None,
    )?;
    let (ownerless, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            Vec::new(),
            vec![current_transfer(node, ZERO_ADDRESS, 2)?],
        ),
        Some(session),
    )?;
    let (output, session) = interpret_test_batch_incremental(
        input(
            manifests,
            admissions,
            Vec::new(),
            vec![renewal_with_expiry(3, 9_999)],
        ),
        Some(session),
    )?;
    let current = session
        .v1_name("ens", &format!("{node:#x}"))
        .expect("current registrar");
    assert_eq!(current.expiry, Some(9_999));
    assert_eq!(current.authority_source_family, "ens_v1_registrar_l1");
    assert!(output.normalized_events.iter().all(|event| {
        !(event.block_number == Some(3)
            && matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "AuthorityEpochChanged" | "PermissionChanged"
            ))
    }));
    let complete = initial
        .normalized_events
        .iter()
        .chain(&ownerless.normalized_events)
        .chain(&output.normalized_events)
        .cloned()
        .collect::<Vec<_>>();
    for prior in [
        complete.iter().map(prior_event).collect(),
        compact_prior(&complete),
    ] {
        let (_, restored) = interpret_test_batch_incremental(
            input(fixture().0, fixture().1, prior, Vec::new()),
            None,
        )?;
        assert_eq!(
            restored
                .v1_name("ens", &format!("{node:#x}"))
                .and_then(|current| current.expiry),
            Some(9_999)
        );
    }
    Ok(())
}

#[test]
fn admitted_registrar_self_transfer_reconciles_authority_permissions() -> anyhow::Result<()> {
    // Exercise a first retained label-bearing renewal using deployed controller/registrar
    // admissions, never a controller event attributed to the ERC721 emitter.
    // (upstream: .refs/ens_v1/deployments/archive/ETHRegistrarController_mainnet_9380471.sol/ETHRegistrarController_mainnet_9380471.json:L72-L98 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L168 @ ens_v1@91c966f)
    let (mut manifests, mut admissions, node) = fixture();
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet/ethereum"),
    )?;
    let deployed = &repository
        .manifests()
        .iter()
        .find(|loaded| loaded.relative_path.to_string_lossy() == "ens/ens_v1_registrar_l1/v1.toml")
        .expect("Mainnet registrar manifest")
        .manifest;
    manifests[1].payload_json = serde_json::to_string(deployed)?;
    admissions.retain(|entry| entry.source_manifest_id != Some(REGISTRAR_MANIFEST_ID));
    for (index, contract) in deployed.contracts.iter().enumerate() {
        let mut entry = admission(REGISTRAR_MANIFEST_ID, &contract.role);
        entry.address = contract.address.clone();
        entry.contract_instance_id = Uuid::from_u128(70_000 + index as u128);
        entry.active_from_block = contract.start_block.map(|block| block as i64);
        admissions.push(entry);
    }
    let emitter = |role: &str| {
        deployed
            .contracts
            .iter()
            .find(|contract| contract.role == role)
            .unwrap()
            .address
            .clone()
    };
    let mut renewal = raw_at(
        wrapped_controller::NameRenewed {
            name: "pointer".to_owned(),
            label: keccak256(b"pointer"),
            cost: U256::from(1),
            expires: U256::from(2_000_000_000u64),
        }
        .encode_log_data(),
        10_000_002,
        1,
        &emitter("legacy_registrar_controller"),
    );
    renewal.block_timestamp =
        (std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_600_000_000)).into();
    mod numeric_renewal {
        use super::*;
        sol! { event NameRenewed(uint256 indexed id, uint256 expires); }
    }
    let mut numeric = raw_at(
        numeric_renewal::NameRenewed {
            id: U256::from_be_slice(keccak256(b"pointer").as_slice()),
            expires: U256::from(2_000_000_000u64),
        }
        .encode_log_data(),
        10_000_002,
        0,
        &emitter("registrar"),
    );
    numeric.block_timestamp = renewal.block_timestamp;
    let prefix = vec![
        current_new_owner(OWNER, 10_000_000)?,
        resolver_selection(REGISTRY, node, RESOLVER_A, 10_000_001)?,
        numeric,
        renewal,
    ];
    let mut suffix = registrar_transfer(OWNER, OWNER, 10_000_003)?;
    suffix.emitting_address = emitter("registrar");
    suffix.block_timestamp =
        (std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_600_000_001)).into();
    let (before, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            vec![],
            prefix.clone(),
        ),
        None,
    )?;
    let (live, session) = interpret_test_batch_incremental(
        input(
            manifests.clone(),
            admissions.clone(),
            vec![],
            vec![suffix.clone()],
        ),
        Some(session),
    )?;
    for prior in [
        before.normalized_events.iter().map(prior_event).collect(),
        compact_prior(&before.normalized_events),
    ] {
        let (cold, _) = interpret_test_batch_incremental(
            input(
                manifests.clone(),
                admissions.clone(),
                prior,
                vec![suffix.clone()],
            ),
            None,
        )?;
        assert_eq!(live, cold, "self-transfer authority restore drift");
    }
    let old_resource = before
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .unwrap()
        .resource_id;
    let selected_resource = live
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityEpochChanged")
        .expect("registrar activation")
        .resource_id;
    assert_ne!(old_resource, selected_resource);
    let permissions: Vec<_> = live
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .collect();
    for (resource, grant) in [(old_resource, false), (selected_resource, true)] {
        for kind in ["resource", "resolver"] {
            assert!(
                permissions.iter().any(|event| event.resource_id == resource
                    && event.after_state["scope"]["kind"] == kind
                    && event.after_state["subject"] == OWNER
                    && (kind != "resolver"
                        || event.after_state["scope"]["resolver_address"] == RESOLVER_A)
                    && event.after_state["effective_powers"]
                        == if grant {
                            json!([format!("{kind}_control")])
                        } else {
                            json!([])
                        }),
                "missing {kind} permission grant={grant} on {resource:?}"
            );
        }
    }
    let mut repeated = registrar_transfer(OWNER, OWNER, 10_000_004)?;
    repeated.emitting_address = emitter("registrar");
    repeated.block_timestamp =
        (std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_600_000_002)).into();
    let (unchanged, _) = interpret_test_batch_incremental(
        input(manifests, admissions, vec![], vec![repeated]),
        Some(session),
    )?;
    assert!(
        !unchanged
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "PermissionChanged")
    );
    Ok(())
}
