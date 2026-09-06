use imbl::ordset::OrdSet;
use serde_json::json;
use uuid::Uuid;

use super::{State, V1NameState, V1RegistryReadAnchor, V1Release, v1_key, v1_registration_is_live};
use crate::schema_v2::model::PriorEventInput;

const NAMESPACE: &str = "test";

#[test]
fn resolver_linked_resources_only_tracks_old_registry_selection() {
    const RESOLVER: &str = "0x0000000000000000000000000000000000000001";
    let mut state = State::new(Vec::new(), Vec::new());

    state.set_v1_resolver_link(
        NAMESPACE,
        "current-node",
        Some(RESOLVER.to_owned()),
        Some(Uuid::from_u128(1)),
        Some("test:current-node".to_owned()),
        Some("registry".to_owned()),
    );
    assert!(
        !state
            .v1_resolver_linked_resources
            .contains_key("test:current-node"),
        "current-registry selection must not allocate old-registry fan-out state"
    );

    state.set_v1_resolver_link(
        NAMESPACE,
        "old-node",
        Some(RESOLVER.to_owned()),
        Some(Uuid::from_u128(2)),
        Some("test:old-node".to_owned()),
        Some("registry_old".to_owned()),
    );
    assert_eq!(
        state
            .v1_resolver_linked_resources
            .get("test:old-node")
            .map(|links| links.len()),
        Some(1),
        "old-registry selection must retain its linked resource for fallback handoff"
    );
}

#[test]
fn restore_keys_authority_derived_resolver_links_by_child() {
    let mut state = State::new(Vec::new(), Vec::new());
    let event = PriorEventInput {
        retained_state_key: "derived-resolver".to_owned(),
        chain_id: "test-chain".to_owned(),
        namespace: NAMESPACE.to_owned(),
        logical_name_id: Some("test:child".to_owned()),
        resource_id: Some(Uuid::from_u128(1)),
        event_kind: "ResolverChanged".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(1),
        emitting_address: None,
        state_scope: None,
        block_timestamp: None,
        after_state: json!({
            "source_event": "AuthorityEpochChanged",
            "node": "parent",
            "child_node": "child",
            "resolver": "0x0000000000000000000000000000000000000001",
            "resolver_source_role": "registry_old",
        }),
    };
    crate::schema_v2::state_restore::v1(&mut state, &event);
    assert_eq!(
        state
            .v1_resolver_linked_resources
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["test:child"],
        "authority-derived resolver row was not keyed only by the affected child name",
    );
}

#[test]
fn wrapper_preimage_restore_derives_registry_labelhash_from_raw_label() {
    const NODE: &str = "node";
    const OWNER: &str = "0x0000000000000000000000000000000000000001";
    let mut state = State::new(Vec::new(), Vec::new());
    state.remember_v1_registry_authority(
        NAMESPACE,
        NODE,
        V1NameState {
            logical_name_id: "test:unknown".to_owned(),
            surface_known: false,
            resource_id: Uuid::from_u128(1),
            token_lineage_id: None,
            authority_source_family: "ens_v1_registry_l1".to_owned(),
            source_manifest_id: Some(1),
            labelhash: None,
            expiry: None,
            owner: Some(OWNER.to_owned()),
            registry_contract: None,
            authority_key: Some("registry-only:node".to_owned()),
            wrapper_fallback: false,
        },
    );
    state.remember_v1_registry_read_anchor(
        NAMESPACE,
        NODE,
        V1RegistryReadAnchor {
            logical_name_id: "test:unknown".to_owned(),
            resource_id: Uuid::from_u128(1),
            surface_known: false,
            source_family: "ens_v1_registry_l1".to_owned(),
            source_manifest_id: Some(1),
            registry_contract: None,
        },
    );
    state.set_v1_registry_owner_views(NAMESPACE, NODE, OWNER.to_owned(), OWNER.to_owned(), None);
    state.activate_v1_authority(
        NAMESPACE,
        NODE,
        Some(V1NameState {
            logical_name_id: "test:node".to_owned(),
            surface_known: true,
            resource_id: Uuid::from_u128(2),
            token_lineage_id: Some(Uuid::from_u128(3)),
            authority_source_family: "ens_v1_wrapper_l1".to_owned(),
            source_manifest_id: Some(2),
            labelhash: Some(crate::schema_v2::common::hash_hex(b"pointer")),
            expiry: Some(9_999),
            owner: Some(OWNER.to_owned()),
            registry_contract: None,
            authority_key: Some("wrapper:node".to_owned()),
            wrapper_fallback: false,
        }),
    );
    let event = PriorEventInput {
        retained_state_key: "wrapper-preimage".to_owned(),
        chain_id: "test-chain".to_owned(),
        namespace: NAMESPACE.to_owned(),
        logical_name_id: Some("test:node".to_owned()),
        resource_id: None,
        event_kind: "PreimageObserved".to_owned(),
        source_family: "ens_v1_wrapper_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(2),
        emitting_address: None,
        state_scope: None,
        block_timestamp: None,
        after_state: json!({
            "namehash": NODE,
            "raw_name": "pointer.eth",
            "raw_labels": ["pointer", "eth"],
        }),
    };

    crate::schema_v2::state_restore::v1_surface::restore_preimage(&mut state, &event);

    let expected_labelhash = crate::schema_v2::common::hash_hex(b"pointer");
    assert_eq!(
        state
            .v1_registry_authorities
            .get(&v1_key(NAMESPACE, NODE))
            .and_then(|authority| authority.labelhash.as_deref()),
        Some(expected_labelhash.as_str())
    );
}

#[test]
fn observed_v1_active_surface_upgrades_an_existing_registry_read_anchor() {
    let mut state = State::new(Vec::new(), Vec::new());
    state.remember_v1_registry_read_anchor(
        NAMESPACE,
        "node",
        V1RegistryReadAnchor {
            logical_name_id: "test:node".to_owned(),
            resource_id: Uuid::from_u128(1),
            surface_known: false,
            source_family: "ens_v1_registry_l1".to_owned(),
            source_manifest_id: Some(1),
            registry_contract: None,
        },
    );

    state.observe_v1_active_surface(NAMESPACE, "node");

    assert!(
        state
            .v1_registry_read_anchor(NAMESPACE, "node")
            .is_some_and(|anchor| anchor.surface_known)
    );
}

#[test]
fn registrar_state_does_not_snapshot_registry_contract() {
    let mut state = State::new(Vec::new(), Vec::new());
    state.observe_v1_registry(
        NAMESPACE,
        "node",
        "test:node".to_owned(),
        true,
        Uuid::from_u128(3),
        "ens_v1_registry_l1".to_owned(),
        Some("0x0000000000000000000000000000000000000001".to_owned()),
        Some("0x0000000000000000000000000000000000000066".to_owned()),
        None,
    );
    observe_registrar(&mut state, "node", Some(100));
    assert_eq!(
        state
            .v1_registrar(NAMESPACE, "node")
            .unwrap()
            .registry_contract,
        None
    );
}

#[test]
fn zero_getter_blocks_stale_registry_authority_fallback_during_registrar_transfer() {
    const NODE: &str = "node";
    const OWNER: &str = "0x0000000000000000000000000000000000000001";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";
    let mut state = State::new(Vec::new(), Vec::new());
    state.observe_v1_registrar(
        NAMESPACE,
        NODE,
        format!("{NAMESPACE}:{NODE}"),
        true,
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        "ens_v1_registrar_l1".to_owned(),
        Some(1),
        None,
        Some(1_000),
        Some(OWNER.to_owned()),
        Some(format!("registrar:{NODE}")),
        false,
        true,
    );
    state.set_v1_registry_owner_views(
        NAMESPACE,
        NODE,
        ZERO.to_owned(),
        ZERO.to_owned(),
        Some("literal_zero".to_owned()),
    );
    state.observe_v1_registry(
        NAMESPACE,
        NODE,
        format!("{NAMESPACE}:{NODE}"),
        true,
        Uuid::from_u128(3),
        "ens_v1_registry_l1".to_owned(),
        Some(OWNER.to_owned()),
        None,
        Some(format!("registry-only:{NODE}")),
    );
    assert!(
        state.has_v1_registry_authority(NAMESPACE, NODE),
        "the fixture deliberately retains the stale entry to mutation-pin the fallback guard"
    );

    let next = state.converge_v1_registrar_transfer(
        NAMESPACE,
        NODE,
        1_000 + super::ENS_GRACE_PERIOD_SECS + 1,
    );
    assert!(
        next.is_none(),
        "a zero getter must not resurrect the stale pre-zero registry owner"
    );
}

#[test]
fn v1_release_order_matches_naive_scan_after_expiry_updates_and_removals() {
    let mut state = State::new(Vec::new(), Vec::new());
    observe_registrar(&mut state, "zeta", Some(100));
    observe_registrar(&mut state, "alpha", Some(100));
    observe_registrar(&mut state, "middle", Some(50));
    observe_registrar(&mut state, "live", Some(500));
    observe_registrar(&mut state, "updated", Some(50));
    observe_registrar(&mut state, "updated", Some(500));
    observe_registrar(&mut state, "removed", Some(50));
    state.restore_v1_registration_release(NAMESPACE, "removed");

    assert_expiry_index_is_derived(&state);
    let timestamp = 100 + super::ENS_GRACE_PERIOD_SECS + 1;
    let expected = naive_due_keys(&state, timestamp);
    let actual = release_keys(state.settle_v1_releases(timestamp));

    assert_eq!(expected, vec!["test:alpha", "test:middle", "test:zeta"]);
    assert_eq!(actual, expected);
    assert_expiry_index_is_derived(&state);
}

#[test]
fn v1_expiry_index_matches_naive_scan_over_generated_mutations() {
    let mut state = State::new(Vec::new(), Vec::new());
    let mut sequence = Sequence(0xd1ff_3e12_395a_11ce);

    for step in 0..1_024 {
        let namehash = format!("name-{:02}", sequence.next() % 48);
        match sequence.next() % 5 {
            0..=2 => {
                let expiry = match sequence.next() % 8 {
                    0 => None,
                    1 => Some(i64::MAX),
                    _ => Some((sequence.next() % 4_000) as i64 - 2_000),
                };
                observe_registrar(&mut state, &namehash, expiry);
            }
            3 => state.restore_v1_registration_release(NAMESPACE, &namehash),
            _ => {
                let timestamp =
                    super::ENS_GRACE_PERIOD_SECS + (sequence.next() % 6_000) as i64 - 3_000;
                assert_release_sequence(&mut state, timestamp, step);
            }
        }
        assert_expiry_index_is_derived(&state);
    }

    assert_release_sequence(&mut state, i64::MAX, 1_024);
    assert_expiry_index_is_derived(&state);
}

#[test]
fn v1_release_occurs_one_second_after_the_grace_boundary() {
    let expiry = 100;
    let boundary = expiry + super::ENS_GRACE_PERIOD_SECS;
    let mut state = State::new(Vec::new(), Vec::new());
    observe_registrar(&mut state, "boundary", Some(expiry));

    assert!(state.settle_v1_releases(boundary - 1).is_empty());
    assert!(state.settle_v1_releases(boundary).is_empty());
    assert_eq!(
        release_keys(state.settle_v1_releases(boundary + 1)),
        vec!["test:boundary"]
    );
}

fn assert_release_sequence(state: &mut State, timestamp: i64, step: usize) {
    let expected = naive_due_keys(state, timestamp);
    let actual = release_keys(state.settle_v1_releases(timestamp));
    assert_eq!(actual, expected, "release sequence differed at step {step}");
}

// Test-only copy of the pre-index full scan, including its registrar-key iteration order.
fn naive_due_keys(state: &State, timestamp: i64) -> Vec<String> {
    state
        .v1_registrars
        .iter()
        .filter_map(|(key, registrar)| {
            let expiry = registrar.expiry?;
            (!v1_registration_is_live(Some(expiry), timestamp)).then(|| key.clone())
        })
        .collect()
}

fn release_keys(releases: Vec<V1Release>) -> Vec<String> {
    releases
        .into_iter()
        .map(|release| format!("{NAMESPACE}:{}", release.namehash))
        .collect()
}

fn assert_expiry_index_is_derived(state: &State) {
    let expected = state
        .v1_registrars
        .iter()
        .filter_map(|(key, registrar)| registrar.expiry.map(|expiry| (expiry, key.clone())))
        .collect::<OrdSet<_>>();
    assert_eq!(state.v1_expiries, expected);
}

fn observe_registrar(state: &mut State, namehash: &str, expiry: Option<i64>) {
    state.observe_v1_registrar(
        NAMESPACE,
        namehash,
        format!("{NAMESPACE}:{namehash}"),
        true,
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        "ens_v1_registrar_l1".to_owned(),
        Some(1),
        None,
        expiry,
        Some("0x0000000000000000000000000000000000000001".to_owned()),
        Some(format!("registrar:{namehash}")),
        false,
        true,
    );
}

struct Sequence(u64);

impl Sequence {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}
