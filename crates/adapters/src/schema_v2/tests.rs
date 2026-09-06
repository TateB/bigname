use alloy_primitives::{Address, B256, U256, hex, keccak256};
use alloy_sol_types::{SolEvent, sol};
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;

mod migration;

#[path = "tests/v1_pre_surface_resolver.rs"]
mod v1_pre_surface_resolver;

const CHAIN: &str = "adapter-test";
const CONTRACT: &str = "0x0000000000000000000000000000000000000042";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

sol! {
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires);
    event NameRenewed(string name, bytes32 indexed label, uint256 expires);
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event ExpiryExtended(bytes32 indexed node, uint64 expiry);
    event FusesSet(bytes32 indexed node, uint32 fuses);
    event RegistryCreated();
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
}

mod v1_registrar {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }
}

mod raw_v1_registrar {
    use alloy_sol_types::sol;

    sol! {
        event RawNameRegistered(bytes name, bytes32 indexed label, address indexed owner, uint256 expires);
        event RawNameRenewed(bytes name, bytes32 indexed label, uint256 expires);
    }
}

mod wrapped_controller {
    use alloy_sol_types::sol;

    sol! {
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    }
}

mod v2_registry {
    use alloy_sol_types::sol;

    sol! {
        event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
        event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
        event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
        event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
        event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
        event ParentUpdated(address indexed parent, string label, address indexed sender);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
        event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
        event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    }
}

mod raw_v2_registry {
    use alloy_sol_types::sol;

    sol! {
        event RawLabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, bytes label, address owner, uint64 expiry, address indexed sender);
        event RawParentUpdated(address indexed parent, bytes label, address indexed sender);
    }
}

mod v2_resolver {
    use alloy_sol_types::sol;

    sol! {
        event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
        event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName);
        event NamedResource(uint256 indexed resource, bytes name);
        event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);
    }
}

mod resolver {
    use alloy_sol_types::sol;

    sol! {
        event AddrChanged(bytes32 indexed node, address a);
        event VersionChanged(bytes32 indexed node, uint64 newVersion);
    }
}

mod approvals {
    use alloy_sol_types::sol;

    sol! {
        event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
        event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
        event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved);
    }
}

mod resolver_name {
    use alloy_sol_types::sol;

    sol! {
        event NameChanged(bytes32 indexed node, string name);
    }
}

mod resolver_strings {
    use alloy_sol_types::sol;

    sol! {
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
        event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData);
        event NameForAddrChanged(address indexed addr, string name);
        event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);
    }
}

mod raw_resolver_strings {
    use alloy_sol_types::sol;

    sol! {
        event RawNameChanged(bytes32 indexed node, bytes name);
        event RawTextChanged(bytes32 indexed node, bytes32 indexed indexedKey, bytes key, bytes value);
        event RawDataChanged(bytes32 indexed node, bytes32 indexed indexedKey, bytes key, bytes32 indexed indexedData);
        event RawNameForAddrChanged(address indexed addr, bytes name);
        event RawNamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, bytes key);
    }
}

mod legacy_text_without_value {
    use alloy_sol_types::sol;

    sol! {
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key);
    }
}

mod legacy_unindexed_text_without_value {
    use alloy_sol_types::sol;

    sol! {
        event TextChanged(bytes32 indexed node, string indexedKey, string key);
    }
}

mod v1_registry {
    use alloy_sol_types::sol;

    sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event NewTTL(bytes32 indexed node, uint64 ttl);
        event Transfer(bytes32 indexed node, address owner);
    }
}

mod v2_registrar {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
    }
}

mod raw_v2_registrar {
    use alloy_sol_types::sol;

    sol! {
        event RawNameRegistered(uint256 indexed tokenId, bytes label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium);
        event RawNameRenewed(uint256 indexed tokenId, bytes label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    }
}

#[test]
fn wrapper_adapter_expands_the_manifest_wrapper_transition() -> anyhow::Result<()> {
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let encoded = NameWrapped {
        node: super::common::namehash(&labels).parse::<B256>()?,
        name: b"\x07wrapped\x03eth\0".to_vec().into(),
        owner: CONTRACT.parse::<Address>()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            4,
            "ens_v1_wrapper_l1",
            "NameWrapped",
            "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            &["name_wrapper"],
            &[
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
                "PreimageObserved",
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(4, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(kinds.contains("TokenControlTransferred"));
    assert!(kinds.contains("ExpiryChanged"));
    assert!(kinds.contains("PermissionScopeChanged"));
    assert!(kinds.contains("SurfaceBound"));
    assert!(kinds.contains("AuthorityEpochChanged"));
    assert!(kinds.contains("PreimageObserved"));
    assert_eq!(output.name_surfaces[0].raw_name, "wrapped.eth");
    Ok(())
}

#[test]
fn non_utf8_wrapper_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    assert_hostile_wrapper_label(vec![0xff], None)
}

#[test]
fn non_utf8_v1_registrar_string_payload_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe, 0xfd];
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    let encoded = with_topic0(
        raw_v1_registrar::RawNameRegistered {
            name: raw_label.clone().into(),
            label: labelhash,
            owner: CONTRACT.parse()?,
            expires: U256::from(42),
        }
        .encode_log_data(),
        NameRegistered::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            63,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(63, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn non_utf8_v2_registry_string_payload_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe];
    let token_id = U256::from_be_bytes(*keccak256(&raw_label));
    let encoded = with_topic0(
        raw_v2_registry::RawLabelRegistered {
            tokenId: token_id,
            labelHash: keccak256(&raw_label),
            label: raw_label.clone().into(),
            owner: CONTRACT.parse()?,
            expiry: 42,
            sender: CONTRACT.parse()?,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            64,
            "ens_v2_registry_l1",
            "LabelRegistered",
            "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
            &["registry"],
            &["RegistrationGranted", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(64, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash =
        super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_label, None);
    Ok(())
}

#[test]
fn hostile_parent_update_retracts_a_bound_descendant_and_records_the_shadow_claim()
-> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000066";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let hostile_label = b"a\0b".to_vec();
    let manifest = manifest_with_events(
        66,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut child_admission = admission(66, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let hostile_parent_update = with_topic0(
        raw_v2_registry::RawParentUpdated {
            parent: CONTRACT.parse()?,
            label: hostile_label.clone().into(),
            sender,
        }
        .encode_log_data(),
        v2_registry::ParentUpdated::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 66,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(66, "registry"), child_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(7),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
            raw_at(hostile_parent_update, 6, 0, CHILD),
        ],
    })?;

    let active = output
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name == "leaf.sub.eth")
        .expect("the valid mutual claim first binds the descendant");
    assert!(output.binding_closures.iter().any(|closure| {
        closure.logical_name_id == active.logical_name_id && closure.block_number == 6
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "ParentChanged"
            && event.block_number == Some(6)
            && event.after_state["raw_label_hex"] == hex::encode(&hostile_label)
            && event.after_state["decoded_label"].is_null()
    }));
    let shadow_namehash =
        super::common::namehash_raw([hostile_label.as_slice(), b"eth"].into_iter());
    assert_shadow_output(&output, &shadow_namehash, &hostile_label, None);
    Ok(())
}

#[test]
fn announced_registry_hostile_registration_waits_for_suffix_authority() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000067";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let raw_label = b"a\0b".to_vec();
    let child_token = versioned_token_bytes(&raw_label, 1);
    let parent_token = versioned_token("sub", 1);
    let manifest = manifest_with_events(
        67,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut child_admission = admission(67, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let admissions = || vec![admission(67, "registry"), child_admission.clone()];
    let rules = || {
        vec![DiscoveryRuleInput {
            manifest_id: 67,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }]
    };
    let hostile_registration = with_topic0(
        raw_v2_registry::RawLabelRegistered {
            tokenId: child_token,
            labelHash: keccak256(&raw_label),
            label: raw_label.clone().into(),
            owner,
            expiry: 1_000,
            sender,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered::SIGNATURE_HASH,
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(hostile_registration, 1, 0, CHILD)],
    })?;

    assert!(
        first.name_surfaces.is_empty(),
        "an unattached registry cannot turn a token ID into a logical name"
    );
    assert!(first.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationGranted"
            && event.logical_name_id.is_none()
            && event.after_state["raw_label_hex"] == hex::encode(&raw_label)
            && event.after_state["decoded_label"].is_null()
    }));

    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CHILD,
            ),
        ],
    })?;
    let shadow_namehash = super::common::namehash_raw(
        [raw_label.as_slice(), b"sub".as_slice(), b"eth".as_slice()].into_iter(),
    );
    assert_shadow_output(&second, &shadow_namehash, &raw_label, None);
    assert!(second.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn non_utf8_v2_registrar_string_payload_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe];
    let token_id = U256::from_be_bytes(*keccak256(&raw_label));
    let encoded = with_topic0(
        raw_v2_registrar::RawNameRegistered {
            tokenId: token_id,
            label: raw_label.clone().into(),
            owner: CONTRACT.parse()?,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
            duration: 42,
            paymentToken: Address::ZERO,
            referrer: B256::ZERO,
            base: U256::ZERO,
            premium: U256::ZERO,
        }
        .encode_log_data(),
        v2_registrar::NameRegistered::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            65,
            "ens_v2_registrar_l1",
            "NameRegistered",
            "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium)",
            &["registrar"],
            &["RegistrarNameRegistered", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(65, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash =
        super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_label, None);
    Ok(())
}

#[test]
fn hostile_v1_registrar_label_retains_registration_renewal_and_transfer_lifecycle()
-> anyhow::Result<()> {
    let raw_label = b"a\0b".to_vec();
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let next_owner: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let registered = with_topic0(
        raw_v1_registrar::RawNameRegistered {
            name: raw_label.clone().into(),
            label: labelhash,
            owner,
            expires: U256::from(42),
        }
        .encode_log_data(),
        NameRegistered::SIGNATURE_HASH,
    );
    let renewed = with_topic0(
        raw_v1_registrar::RawNameRenewed {
            name: raw_label.clone().into(),
            label: labelhash,
            expires: U256::from(84),
        }
        .encode_log_data(),
        NameRenewed::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            68,
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
                    "Transfer",
                    "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                    &["registrar"],
                    &["TokenControlTransferred"],
                ),
                (
                    "NameRenewed",
                    "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
                    &["registrar"],
                    &["RegistrationRenewed"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(68, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(
                v1_registrar::Transfer {
                    from: owner,
                    to: next_owner,
                    tokenId: U256::from_be_bytes(*labelhash),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(renewed, 3, 0, CONTRACT),
        ],
    })?;

    for kind in [
        "RegistrationGranted",
        "TokenControlTransferred",
        "RegistrationRenewed",
    ] {
        assert!(
            output
                .normalized_events
                .iter()
                .any(|event| event.event_kind == kind),
            "missing hostile-label lifecycle event {kind}"
        );
    }
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationGranted" && event.after_state["decoded_label"].is_null()
    }));
    assert!(!output.resources.is_empty());
    assert!(!output.token_lineages.is_empty());
    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn normalization_changed_v1_label_remains_shadow_after_registry_authority_transition()
-> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000072";
    let raw_label = b"Alice".to_vec();
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let next_owner: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let registrar_manifest = manifest(
        72,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registrar_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(72, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            NameRegistered {
                name: "Alice".to_owned(),
                label: labelhash,
                owner,
                expires: U256::from(1_000),
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let first_surface = first
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == node)
        .expect("normalization-changed shadow identity");
    assert_eq!(first_surface.visibility_state, "shadow");
    assert!(first.surface_bindings.is_empty());
    assert!(first.label_preimages.iter().any(|preimage| {
        preimage.raw_label == raw_label
            && preimage.decoded_label.as_deref() == Some("Alice")
            && !preimage.normalized_under_version
    }));

    let registry_manifest = manifest(
        73,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &["SubregistryChanged", "AuthorityTransferred"],
    );
    let mut registry_admission = admission(73, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewOwner {
                node: super::common::namehash(&["eth".to_owned()]).parse()?,
                label: labelhash,
                owner: next_owner,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    })?;
    let logical_name_id = format!("ens:{node}");
    assert!(
        second
            .surface_bindings
            .iter()
            .all(|binding| binding.logical_name_id != logical_name_id)
    );
    assert!(second.normalized_events.iter().all(|event| {
        event.event_kind != "SurfaceBound"
            || event.logical_name_id.as_deref() != Some(logical_name_id.as_str())
    }));
    Ok(())
}

#[test]
fn resolver_event_on_materialized_shadow_surface_keeps_identity() -> anyhow::Result<()> {
    const RESOLVER: &str = "0x0000000000000000000000000000000000000073";

    let raw_label = b"Alice".to_vec();
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            74,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(74, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            NameRegistered {
                name: "Alice".to_owned(),
                label: labelhash,
                owner: CONTRACT.parse()?,
                expires: U256::from(42),
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let shadow = first
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == node)
        .expect("normalization-changed shadow surface");
    assert_eq!(shadow.visibility_state, "shadow");
    let registration_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            75,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::AddrChanged {
                node: node.parse()?,
                a: CONTRACT.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            RESOLVER,
        )],
    })?;
    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("resolver record");
    assert_eq!(
        record.logical_name_id.as_deref(),
        Some(shadow.logical_name_id.as_str())
    );
    assert_eq!(record.resource_id, Some(registration_resource));
    assert_batch_referential_integrity(
        &output,
        &first
            .resources
            .iter()
            .map(|resource| (resource.chain_id.clone(), resource.resource_id))
            .collect(),
        &first
            .name_surfaces
            .iter()
            .map(|surface| (surface.chain_id.clone(), surface.logical_name_id.clone()))
            .collect(),
    )?;
    Ok(())
}

#[test]
fn hostile_wrapper_label_retains_transfer_and_unwrap_authority_lifecycle() -> anyhow::Result<()> {
    let raw_label = vec![0xff];
    let dns_name = vec![1, 0xff, 3, b'e', b't', b'h', 0];
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let next_owner: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            69,
            "ens",
            "ens_v1_wrapper_l1",
            &[
                (
                    "NameWrapped",
                    "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
                (
                    "NameUnwrapped",
                    "event NameUnwrapped(bytes32 indexed node, address owner)",
                    &["name_wrapper"],
                    &["SurfaceUnbound"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(69, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameWrapped {
                    node: node.parse()?,
                    name: dns_name.into(),
                    owner,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferSingle {
                    operator: owner,
                    from: owner,
                    to: next_owner,
                    id: node.parse()?,
                    value: U256::from(1),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                NameUnwrapped {
                    node: node.parse()?,
                    owner: next_owner,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
        ],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "TokenControlTransferred")
            .count()
            >= 2
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "AuthorityEpochChanged"
            && event.after_state["source_event"] == "NameUnwrapped"
    }));
    assert!(!output.resources.is_empty());
    assert!(!output.token_lineages.is_empty());
    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn hostile_v2_registrar_label_retains_registration_and_renewal_events() -> anyhow::Result<()> {
    let raw_label = b"a\0b".to_vec();
    let token_id = versioned_token_bytes(&raw_label, 1);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let registered = with_topic0(
        raw_v2_registrar::RawNameRegistered {
            tokenId: token_id,
            label: raw_label.clone().into(),
            owner,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
            duration: 42,
            paymentToken: Address::ZERO,
            referrer: B256::ZERO,
            base: U256::ZERO,
            premium: U256::ZERO,
        }
        .encode_log_data(),
        v2_registrar::NameRegistered::SIGNATURE_HASH,
    );
    let renewed = with_topic0(
        raw_v2_registrar::RawNameRenewed {
            tokenId: token_id,
            label: raw_label.clone().into(),
            duration: 42,
            newExpiry: 84,
            paymentToken: Address::ZERO,
            referrer: B256::ZERO,
            amount: U256::ZERO,
        }
        .encode_log_data(),
        alloy_primitives::keccak256(
            b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
        ),
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            70,
            "ens",
            "ens_v2_registrar_l1",
            &[
                (
                    "NameRegistered",
                    "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium)",
                    &["registrar"],
                    &["RegistrarNameRegistered"],
                ),
                (
                    "NameRenewed",
                    "event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount)",
                    &["registrar"],
                    &["RegistrationRenewed"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(70, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(renewed, 2, 0, CONTRACT),
        ],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "RegistrarNameRegistered")
    );
    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "RegistrationRenewed")
    );
    assert!(output.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrarNameRegistered" | "RegistrationRenewed"
        ) || event.after_state["decoded_label"].is_null()
    }));
    assert_shadow_output(&output, &node, &raw_label, None);
    Ok(())
}

#[test]
fn embedded_dot_wrapper_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    assert_hostile_wrapper_label(b"a.b".to_vec(), Some("a.b"))
}

#[test]
fn embedded_nul_wrapper_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    assert_hostile_wrapper_label(b"a\0b".to_vec(), None)
}

#[test]
fn two_hundred_fifty_six_byte_registrar_label_becomes_a_shadow_observation() -> anyhow::Result<()> {
    let raw_label = vec![b'a'; 256];
    let label = String::from_utf8(raw_label.clone())?;
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    let encoded = NameRegistered {
        name: label.clone(),
        label: keccak256(&raw_label),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            61,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(61, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_shadow_output(&output, &node, &raw_label, Some(&label));
    Ok(())
}

fn assert_hostile_wrapper_label(
    raw_label: Vec<u8>,
    decoded_label: Option<&str>,
) -> anyhow::Result<()> {
    let mut dns_name = Vec::with_capacity(raw_label.len() + 6);
    dns_name.push(u8::try_from(raw_label.len())?);
    dns_name.extend_from_slice(&raw_label);
    dns_name.extend_from_slice(b"\x03eth\0");
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth".as_slice()].into_iter());
    let encoded = NameWrapped {
        node: node.parse()?,
        name: dns_name.into(),
        owner: CONTRACT.parse()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            62,
            "ens_v1_wrapper_l1",
            "NameWrapped",
            "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            &["name_wrapper"],
            &["TokenControlTransferred", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(62, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_shadow_output(&output, &node, &raw_label, decoded_label);
    Ok(())
}

fn assert_shadow_output(
    output: &BatchOutput,
    namehash: &str,
    raw_label: &[u8],
    decoded_label: Option<&str>,
) {
    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == namehash)
        .expect("hostile label shadow identity");
    assert_eq!(surface.logical_name_id, format!("ens:{namehash}"));
    assert_eq!(surface.visibility_state, "shadow");
    assert_eq!(
        surface.deactivation_reason.as_deref(),
        Some("normalization_gate")
    );
    assert!(
        output
            .surface_bindings
            .iter()
            .all(|binding| binding.logical_name_id != surface.logical_name_id)
    );
    assert!(
        output
            .binding_closures
            .iter()
            .all(|closure| closure.logical_name_id != surface.logical_name_id)
    );
    let preimage = output
        .label_preimages
        .iter()
        .find(|preimage| preimage.raw_label == raw_label)
        .expect("hostile raw label preimage");
    assert_eq!(preimage.decoded_label.as_deref(), decoded_label);
    assert!(!preimage.normalized_under_version);
    assert!(preimage.normalization_error.is_some());
    let shadow = output
        .normalized_events
        .iter()
        .find(|event| event.after_state["visibility_state"] == "shadow")
        .expect("shadow identity observation");
    assert_eq!(
        shadow.logical_name_id.as_deref(),
        Some(surface.logical_name_id.as_str())
    );
    assert_eq!(shadow.after_state["namehash"], namehash);
    assert_eq!(
        shadow.after_state["logical_name_id"],
        format!("ens:{namehash}")
    );
}

#[test]
fn wrapper_incremental_updates_keep_identity_and_prior_state_across_batches() -> anyhow::Result<()>
{
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let manifest = manifest_with_events(
        56,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "ExpiryExtended",
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
                &["name_wrapper"],
                &["ExpiryChanged"],
            ),
            (
                "FusesSet",
                "event FusesSet(bytes32 indexed node, uint32 fuses)",
                &["name_wrapper"],
                &["PermissionScopeChanged"],
            ),
        ],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            NameWrapped {
                node,
                name: b"\x07wrapped\x03eth\0".to_vec().into(),
                owner: CONTRACT.parse()?,
                fuses: 1,
                expiry: 42,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let wrapped_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "SurfaceBound")
        .and_then(|event| event.resource_id)
        .expect("wrapped resource");
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "name_wrapper")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                ExpiryExtended { node, expiry: 84 }.encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                FusesSet { node, fuses: 3 }.encode_log_data(),
                2,
                1,
                CONTRACT,
            ),
        ],
    })?;

    let expiry = second
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "ExpiryExtended")
        .expect("expiry extension");
    let fuses = second
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "FusesSet")
        .expect("fuse update");
    assert_eq!(expiry.resource_id, Some(wrapped_resource));
    assert_eq!(expiry.before_state["expiry"], 42);
    assert_eq!(fuses.resource_id, Some(wrapped_resource));
    assert_eq!(fuses.before_state["fuses"], 1);
    Ok(())
}

#[test]
fn unexpired_rewrap_restores_retained_parent_fuses_and_expiry() -> anyhow::Result<()> {
    let labels = vec!["child".to_owned(), "parent".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let manifest = manifest_with_events(
        65,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "AuthorityEpochChanged"],
            ),
        ],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(65, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameWrapped {
                    node,
                    name: b"\x05child\x06parent\0".to_vec().into(),
                    owner: CONTRACT.parse()?,
                    fuses: 65_536,
                    expiry: 100,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                NameUnwrapped {
                    node,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
        ],
    })?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(65, "name_wrapper")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            NameWrapped {
                node,
                name: b"\x05child\x06parent\0".to_vec().into(),
                owner: CONTRACT.parse()?,
                fuses: 0,
                expiry: 0,
            }
            .encode_log_data(),
            3,
            0,
            CONTRACT,
        )],
    })?;

    let scope = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionScopeChanged")
        .expect("rewrap permission scope");
    assert_eq!(scope.after_state["fuses"], 65_536);
    assert_eq!(scope.after_state["expiry"], 100);
    assert_eq!(scope.after_state["wrapper_state"], "emancipated");
    Ok(())
}

#[test]
fn wrapped_controller_renewal_updates_the_wrapper_resource_expiry() -> anyhow::Result<()> {
    const WRAPPER: &str = "0x0000000000000000000000000000000000000043";
    const CONTROLLER: &str = "0x0000000000000000000000000000000000000044";

    let labels = vec!["renewed".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let label = keccak256(b"renewed");
    let wrapper_manifest = manifest_with_events(
        66,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "ExpiryExtended",
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
                &["name_wrapper"],
                &["ExpiryChanged"],
            ),
            (
                "FusesSet",
                "event FusesSet(bytes32 indexed node, uint32 fuses)",
                &["name_wrapper"],
                &["PermissionScopeChanged"],
            ),
        ],
    );
    let registrar_manifest = manifest(
        67,
        "ens_v1_registrar_l1",
        "NameRenewed",
        "event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires)",
        &["wrapped_registrar_controller"],
        &[
            "RegistrationGranted",
            "RegistrationRenewed",
            "ExpiryChanged",
        ],
    );
    let mut wrapper_admission = admission(66, "name_wrapper");
    wrapper_admission.address = WRAPPER.to_owned();
    let mut controller_admission = admission(67, "wrapped_registrar_controller");
    controller_admission.address = CONTROLLER.to_owned();
    let block = |number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };
    let (first, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![wrapper_manifest.clone(), registrar_manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![wrapper_admission.clone(), controller_admission.clone()],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                NameWrapped {
                    node,
                    name: b"\x07renewed\x03eth\0".to_vec().into(),
                    owner: CONTRACT.parse()?,
                    fuses: 196_608,
                    expiry: 7_776_100,
                }
                .encode_log_data(),
                1,
                0,
                WRAPPER,
            )],
        },
        None,
    )?;
    let wrapper_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionScopeChanged")
        .and_then(|event| event.resource_id)
        .expect("wrapper resource");
    let (output, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![wrapper_manifest.clone(), registrar_manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![wrapper_admission.clone(), controller_admission.clone()],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                wrapped_controller::NameRenewed {
                    name: "renewed".to_owned(),
                    label,
                    cost: U256::from(1),
                    expires: U256::from(200),
                }
                .encode_log_data(),
                2,
                0,
                CONTROLLER,
            )],
        },
        Some(session),
    )?;

    let expiry = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ExpiryChanged" && event.resource_id == Some(wrapper_resource)
        })
        .expect("wrapper-linked renewal expiry");
    assert_eq!(expiry.source_family, "ens_v1_registrar_l1");
    assert_eq!(expiry.after_state["source_event"], "NameRenewed");
    assert_eq!(expiry.after_state["authority_kind"], "wrapper");
    assert_eq!(expiry.before_state["expiry"], 7_776_100);
    assert_eq!(expiry.after_state["expiry"], 7_776_200);

    let prior_events = seam::fold_prior_events(Vec::new(), &first.normalized_events, &[block(1)])?;
    let prior_events =
        seam::fold_prior_events(prior_events, &output.normalized_events, &[block(2)])?;
    let later_input = BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![wrapper_manifest, registrar_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![wrapper_admission, controller_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                ExpiryExtended {
                    node,
                    expiry: 7_776_300,
                }
                .encode_log_data(),
                3,
                0,
                WRAPPER,
            ),
            raw_at(
                FusesSet {
                    node,
                    fuses: 196_609,
                }
                .encode_log_data(),
                3,
                1,
                WRAPPER,
            ),
        ],
    };
    let mut compacted_input = later_input.clone();
    compacted_input.prior_events = prior_events;
    let compacted = interpret_test_batch(compacted_input)?;
    let (later, _) = interpret_test_batch_incremental(later_input, Some(session))?;
    assert_eq!(later, compacted);
    let later_expiry = later
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "ExpiryExtended")
        .expect("later wrapper expiry");
    let later_fuses = later
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "FusesSet")
        .expect("later wrapper fuses");
    assert_eq!(later_expiry.before_state["expiry"], 7_776_200);
    assert_eq!(later_fuses.before_state["fuses"], 196_608);
    assert_eq!(later_fuses.before_state["expiry"], 7_776_300);
    Ok(())
}

#[test]
fn incremental_session_state_matches_a_fresh_compacted_restore() -> anyhow::Result<()> {
    let labels = vec!["session".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let manifest = manifest_with_events(
        56,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "ExpiryExtended",
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
                &["name_wrapper"],
                &["ExpiryChanged"],
            ),
            (
                "FusesSet",
                "event FusesSet(bytes32 indexed node, uint32 fuses)",
                &["name_wrapper"],
                &["PermissionScopeChanged"],
            ),
        ],
    );
    let blocks = |number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };
    let (first, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![admission(56, "name_wrapper")],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                NameWrapped {
                    node,
                    name: b"\x07session\x03eth\0".to_vec().into(),
                    owner: CONTRACT.parse()?,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            )],
        },
        None,
    )?;
    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &[blocks(1)])?;
    let second_input = BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            ExpiryExtended { node, expiry: 84 }.encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    };
    let mut fresh_second_input = second_input.clone();
    fresh_second_input.prior_events = prior.clone();
    let fresh_second = interpret_test_batch(fresh_second_input)?;
    let (second, session) = interpret_test_batch_incremental(second_input, Some(session))?;
    assert_eq!(second, fresh_second);
    let prior = seam::fold_prior_events(prior, &second.normalized_events, &[blocks(2)])?;
    let (third, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![admission(56, "name_wrapper")],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                FusesSet { node, fuses: 3 }.encode_log_data(),
                3,
                0,
                CONTRACT,
            )],
        },
        Some(session),
    )?;
    let prior = seam::fold_prior_events(prior, &third.normalized_events, &[blocks(3)])?;
    let (_, restored) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest],
            discovery_rules: Vec::new(),
            admissions: vec![admission(56, "name_wrapper")],
            prior_events: prior,
            blocks: Vec::new(),
            raw_logs: Vec::new(),
        },
        None,
    )?;

    assert_eq!(session, restored);
    Ok(())
}

#[test]
fn incremental_session_refreshes_names_when_v2_suffix_anchors_change() -> anyhow::Result<()> {
    const REPLACEMENT_ANCHOR: &str = "0x0000000000000000000000000000000000000043";
    let manifest = manifest(
        57,
        "ens_v2_registry_l1",
        "LabelRegistered",
        "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
        &["registry"],
        &["RegistrationGranted"],
    );
    let label = "anchor-change";
    let (first, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![admission(57, "registry")],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                v2_registry::LabelRegistered {
                    tokenId: versioned_token(label, 1),
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner: CONTRACT.parse()?,
                    expiry: 1_000,
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            )],
        },
        None,
    )?;
    let prior = seam::fold_prior_events(
        Vec::new(),
        &first.normalized_events,
        &[RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
        }],
    )?;
    let mut replacement_admission = admission(57, "registry");
    replacement_admission.address = REPLACEMENT_ANCHOR.to_owned();
    let (_, resumed) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![replacement_admission.clone()],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: Vec::new(),
        },
        Some(session),
    )?;
    let (_, restored) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest],
            discovery_rules: Vec::new(),
            admissions: vec![replacement_admission],
            prior_events: prior,
            blocks: Vec::new(),
            raw_logs: Vec::new(),
        },
        None,
    )?;

    assert_eq!(resumed, restored);
    Ok(())
}

#[test]
fn incremental_v2_displacement_drops_the_replaced_active_resource() -> anyhow::Result<()> {
    let manifest = manifest_with_events(
        85,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationReserved"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let block = |number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };
    let label = "displaced";
    let first_token = versioned_token(label, 1);
    let second_token = versioned_token(label, 2);
    let sender: Address = CONTRACT.parse()?;
    let (first, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![admission(85, "registry")],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![
                raw_at(
                    v2_registry::LabelRegistered {
                        tokenId: first_token,
                        labelHash: keccak256(label.as_bytes()),
                        label: label.to_owned(),
                        owner: sender,
                        expiry: 1_000,
                        sender,
                    }
                    .encode_log_data(),
                    1,
                    0,
                    CONTRACT,
                ),
                raw_at(
                    v2_registry::TokenResource {
                        tokenId: first_token,
                        resource: U256::from(99),
                    }
                    .encode_log_data(),
                    1,
                    1,
                    CONTRACT,
                ),
            ],
        },
        None,
    )?;
    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &[block(1)])?;
    let (second, resumed) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![admission(85, "registry")],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                v2_registry::LabelReserved {
                    tokenId: second_token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            )],
        },
        Some(session),
    )?;
    let prior = seam::fold_prior_events(prior, &second.normalized_events, &[block(2)])?;
    let (_, restored) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest],
            discovery_rules: Vec::new(),
            admissions: vec![admission(85, "registry")],
            prior_events: prior,
            blocks: Vec::new(),
            raw_logs: Vec::new(),
        },
        None,
    )?;

    assert_eq!(resumed, restored);
    Ok(())
}

#[test]
#[rustfmt::skip]
fn already_expired_detached_reservation_emits_resource_scoped_release() -> anyhow::Result<()> {
    const DETACHED: &str = "0x0000000000000000000000000000000000000069";
    let manifest = manifest_with_events(
        86,
        "ens",
        "ens_v2_registry_l1",
        &[
            ("LabelReserved", "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)", &["registry"], &["RegistrationReserved"]),
            ("ExpiryUpdated", "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)", &["registry"], &["ExpiryChanged", "RegistrationRenewed"]),
        ],
    );
    let mut detached = admission(86, "registry");
    detached.address = DETACHED.to_owned(); detached.contract_instance_id = super::common::contract_id(CHAIN, DETACHED); detached.role = None;
    detached.discovery_edge_kind = Some("registry_announcement".to_owned()); detached.discovery_from_contract_instance_id = Some(super::common::contract_id(CHAIN, DETACHED));
    detached.discovery_observation_key = Some("registry-announcement:detached".to_owned());
    let discovery_rules = vec![DiscoveryRuleInput { manifest_id: 86, edge_kind: "subregistry".to_owned(), from_role: Some("registry".to_owned()), admission: "linked_subregistry_event".to_owned() }];
    let token = versioned_token("stale", 0);
    let sender: Address = CONTRACT.parse()?;
    let (output, live) = interpret_test_batch_incremental(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: discovery_rules.clone(),
            admissions: vec![detached.clone()],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"stale"), label: "stale".to_owned(), expiry: 1, sender }.encode_log_data(), 1, 0, DETACHED), raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"stale"), label: "stale".to_owned(), expiry: 1, sender }.encode_log_data(), 1, 1, DETACHED)],
        }, None)?;
    let release = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationReleased"
                && event.after_state["source_event"] == "RegistryPathExpired"
        })
        .unwrap_or_else(|| panic!("missing detached expiry release: {output:#?}"));
    let release_ids = output.normalized_events.iter().filter(|event| event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired").map(|event| &event.event_identity).collect::<std::collections::BTreeSet<_>>(); assert_eq!(release_ids.len(), 2);
    assert!(release.logical_name_id.is_none());
    assert!(release.resource_id.is_some());
    assert_eq!(release.before_state["status"], "reserved");
    assert_eq!(release.after_state["derived_from"], "interpreter_state");
    assert_eq!(release.after_state["terminal_reason"], "registry_name_binding_expired");
    let resource_id = release.resource_id;
    assert_eq!((release.block_number, release.transaction_index, release.log_index), (Some(1), None, None));
    assert!(release.transaction_hash.is_none());
    let lifecycle = output.normalized_events.iter()
        .filter(|event| matches!(event.event_kind.as_str(), "RegistrationReserved" | "RegistrationReleased"))
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert_eq!(lifecycle, ["RegistrationReserved", "RegistrationReleased", "RegistrationReserved", "RegistrationReleased"]);
    let prior = seam::fold_prior_events(
        Vec::new(),
        &output.normalized_events,
        &[RawBlockInput { chain_id: CHAIN.to_owned(), block_hash: "block-1".to_owned(), block_number: 1, block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1), canonicality_state: "canonical".to_owned() }],
    )?;
    let (_, restored) = interpret_test_batch_incremental(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: discovery_rules.clone(),
            admissions: vec![detached.clone()],
            prior_events: prior,
            blocks: Vec::new(),
            raw_logs: Vec::new(),
    }, None)?;
    assert_eq!(live, restored);
    let (revival, _) = interpret_test_batch_incremental(BatchInput {
        chain_id: CHAIN.to_owned(), manifests: vec![manifest], discovery_rules, admissions: vec![detached], prior_events: Vec::new(), blocks: Vec::new(),
        raw_logs: vec![raw_at(v2_registry::ExpiryUpdated { tokenId: token, newExpiry: 100, sender }.encode_log_data(), 2, 0, DETACHED)],
    }, Some(live))?;
    assert!(revival.normalized_events.iter().any(|event| event.event_kind == "RegistrationRenewed" && event.logical_name_id.is_none() && event.resource_id == resource_id && event.after_state["revived_from_expiry"] == true));
    Ok(())
}

#[test]
fn incremental_v2_delta_refreshes_only_the_affected_topology_component() {
    const OTHER_REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    let retained_registration = |registry: &str, ordinal: usize, timestamp: i64| {
        let token_id = format!("0x{ordinal:064x}");
        PriorEventInput {
            retained_state_key: format!("retained:{registry}:{ordinal}"),
            chain_id: CHAIN.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "RegistrationGranted".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(58),
            emitting_address: None,
            state_scope: Some(format!("{registry}:-:{token_id}:-:LabelRegistered")),
            block_timestamp: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(timestamp)),
            after_state: json!({
                "source_event":"LabelRegistered",
                "token_id":token_id,
                "raw_label_hex":hex::encode(format!("name-{ordinal}")),
                "expiry":10_000,
            }),
        }
    };
    let retained = (0..128)
        .map(|ordinal| retained_registration(CONTRACT, ordinal, 1))
        .collect::<Vec<_>>();
    let anchors = vec![
        (
            CONTRACT.to_owned(),
            "ens".to_owned(),
            vec!["eth".to_owned()],
        ),
        (
            OTHER_REGISTRY.to_owned(),
            "ens".to_owned(),
            vec!["eth".to_owned()],
        ),
    ];
    let mut state = super::state::State::new(retained.clone(), anchors.clone());
    let delta = vec![retained_registration(OTHER_REGISTRY, 999, 2)];

    super::state::reset_v2_refresh_visits();
    state.apply_prior_event_delta(delta.clone());

    assert_eq!(
        super::state::v2_refresh_visits(),
        1,
        "a one-token delta must not revisit the unrelated retained registry"
    );
    let restored = super::state::State::new(retained.into_iter().chain(delta).collect(), anchors);
    assert_eq!(state, restored);
}

#[test]
fn registrar_adapter_emits_raw_namehash_identity_and_preimages() -> anyhow::Result<()> {
    let encoded = NameRegistered {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            1,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(1, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registrar event");
    assert!(
        event
            .logical_name_id
            .as_deref()
            .is_some_and(|id| id.starts_with("ens:0x"))
    );
    assert_eq!(output.name_surfaces[0].raw_labels, ["alice", "eth"]);
    assert_eq!(output.surface_bindings.len(), 1);
    assert_eq!(
        output.surface_bindings[0].binding_kind,
        "declared_registry_path"
    );
    let kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "RegistrationGranted",
        "ExpiryChanged",
        "PermissionChanged",
        "SurfaceBound",
        "AuthorityEpochChanged",
    ] {
        assert!(kinds.contains(expected), "missing {expected}");
    }
    assert_eq!(
        output
            .label_preimages
            .iter()
            .filter_map(|preimage| preimage.decoded_label.as_deref())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["alice", "eth"]),
    );
    Ok(())
}

#[test]
fn first_seen_renewal_synthesizes_the_retained_registration_anchor() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            60,
            "ens_v1_registrar_l1",
            "NameRenewed",
            "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
            &["registrar"],
            &[
                "RegistrationGranted",
                "RegistrationRenewed",
                "ExpiryChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(60, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(NameRenewed {
            name: "renewed".to_owned(),
            label: keccak256(b"renewed"),
            expires: U256::from(42),
        }
        .encode_log_data())],
    })?;

    let kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "RegistrationGranted",
        "RegistrationRenewed",
        "ExpiryChanged",
        "SurfaceBound",
        "AuthorityEpochChanged",
        "PreimageObserved",
    ] {
        assert!(kinds.contains(expected), "missing {expected}");
    }
    let grant = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("synthetic registration grant");
    assert_eq!(
        grant.after_state["registrant"],
        "0x0000000000000000000000000000000000000000"
    );
    assert_eq!(output.surface_bindings.len(), 1);
    Ok(())
}

#[test]
fn synthetic_renewal_grant_restore_matches_live_registry_authority() -> anyhow::Result<()> {
    assert_registration_grant_restore_matches_live(false)
}

#[test]
fn registration_grant_restore_matches_live_registrar_authority() -> anyhow::Result<()> {
    assert_registration_grant_restore_matches_live(true)
}

#[test]
fn same_transaction_synthetic_renewal_restore_matches_live_registry_authority() -> anyhow::Result<()>
{
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";

    let label = "same-transaction-renewal";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&[label.to_owned(), "eth".to_owned()]);
    let registry_owner = "0x0000000000000000000000000000000000000007";
    let mut registry_admission = admission(62, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let manifests = vec![
        manifest_with_events(
            62,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry"],
                    &["SubregistryChanged", "AuthorityTransferred"],
                ),
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry"],
                    &["ResolverChanged"],
                ),
            ],
        ),
        manifest(
            63,
            "ens_v1_registrar_l1",
            "NameRenewed",
            "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        ),
    ];
    let admissions = vec![registry_admission, admission(63, "registrar")];
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at_transaction(
                v1_registry::NewOwner {
                    node: parent_node,
                    label: labelhash,
                    owner: registry_owner.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                0,
                REGISTRY,
            ),
            raw_at_transaction(
                NameRenewed {
                    name: label.to_owned(),
                    label: labelhash,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                0,
                1,
                CONTRACT,
            ),
            raw_at_transaction(
                v1_registry::NewResolver {
                    node: node.parse()?,
                    resolver: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                1,
                2,
                REGISTRY,
            ),
        ],
    })?;
    let live_resource = first
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "NewResolver"
        })
        .and_then(|event| event.resource_id)
        .expect("live resolver resource");
    let prior_events = seam::fold_prior_events(
        Vec::new(),
        &first.normalized_events,
        &[RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
        }],
    )?;
    let restored = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewResolver {
                node: node.parse()?,
                resolver: registry_owner.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    })?;
    let restored_resource = restored
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "NewResolver"
        })
        .and_then(|event| event.resource_id)
        .expect("restored resolver resource");

    assert_eq!(
        restored_resource, live_resource,
        "compacted restore must preserve the registry-only authority selected live",
    );
    Ok(())
}

#[test]
fn compacted_set_owner_convergence_restore_matches_live_registrar_authority() -> anyhow::Result<()>
{
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const USER: &str = "0x0000000000000000000000000000000000000047";
    const DIVERGED_OWNER: &str = "0x0000000000000000000000000000000000000048";

    let label = "compacted-convergence";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&[label.to_owned(), "eth".to_owned()]);
    let mut registry_admission = admission(64, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let manifests = vec![
        manifest_with_events(
            64,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry"],
                    &["SubregistryChanged", "AuthorityTransferred"],
                ),
                (
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry"],
                    &["AuthorityTransferred"],
                ),
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry"],
                    &["ResolverChanged"],
                ),
            ],
        ),
        manifest(
            65,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        ),
    ];
    let admissions = vec![registry_admission, admission(65, "registrar")];
    let block = |block_number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block_number),
        canonicality_state: "canonical".to_owned(),
    };
    let registered = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at_transaction(
                v1_registry::NewOwner {
                    node: parent_node,
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                0,
                REGISTRY,
            ),
            raw_at_transaction(
                v1_registry::Transfer {
                    node: node.parse()?,
                    owner: USER.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                1,
                REGISTRY,
            ),
            raw_at_transaction(
                NameRegistered {
                    name: label.to_owned(),
                    label: labelhash,
                    owner: USER.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                0,
                2,
                CONTRACT,
            ),
        ],
    })?;
    let registrar_resource = registered
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar resource");
    let registered_prior =
        seam::fold_prior_events(Vec::new(), &registered.normalized_events, &[block(1)])?;
    let diverged = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: registered_prior.clone(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::Transfer {
                node: node.parse()?,
                owner: DIVERGED_OWNER.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    })?;
    let diverged_prior =
        seam::fold_prior_events(registered_prior, &diverged.normalized_events, &[block(2)])?;
    let converged = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: diverged_prior.clone(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::Transfer {
                node: node.parse()?,
                owner: USER.parse()?,
            }
            .encode_log_data(),
            3,
            0,
            REGISTRY,
        )],
    })?;
    assert!(converged.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound" && event.resource_id == Some(registrar_resource)
    }));
    let compacted =
        seam::fold_prior_events(diverged_prior, &converged.normalized_events, &[block(3)])?;
    let restored = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: compacted,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewResolver {
                node: node.parse()?,
                resolver: USER.parse()?,
            }
            .encode_log_data(),
            4,
            0,
            REGISTRY,
        )],
    })?;
    let restored_resources = restored
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "NewResolver"
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node}"));

    assert_eq!(
        restored_resources,
        std::collections::BTreeSet::from([registrar_resource, registry_resource]),
        "compacted convergence must restore the live registrar pointer and the independent registry read anchor",
    );
    Ok(())
}

fn assert_registration_grant_restore_matches_live(registration: bool) -> anyhow::Result<()> {
    let label = if registration {
        "registered-restore"
    } else {
        "renewed-restore"
    };
    let labelhash = keccak256(label.as_bytes());
    let namehash = super::common::namehash(&[label.to_owned(), "eth".to_owned()]);
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{namehash}"));
    let registry_owner = "0x0000000000000000000000000000000000000007";
    let registrant = "0x0000000000000000000000000000000000000008";
    let registry_prior = PriorEventInput {
        retained_state_key: format!("registry-prior:{namehash}"),
        chain_id: CHAIN.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(format!("ens:{namehash}")),
        resource_id: Some(registry_resource),
        event_kind: "AuthorityTransferred".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        emitting_address: None,
        state_scope: Some(format!("registry:{namehash}")),
        block_timestamp: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
        after_state: json!({
            "source_event":"NewOwner",
            "child_node":namehash,
            "labelhash":format!("{labelhash:#x}"),
            "owner":registry_owner,
            "authority_kind":"registry_only",
            "authority_key":format!("registry-only:{CHAIN}:{namehash}"),
        }),
    };
    let (manifest, raw) = if registration {
        (
            manifest(
                61,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            raw_at(
                NameRegistered {
                    name: label.to_owned(),
                    label: labelhash,
                    owner: registrant.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
        )
    } else {
        (
            manifest(
                61,
                "ens_v1_registrar_l1",
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            raw_at(
                NameRenewed {
                    name: label.to_owned(),
                    label: labelhash,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
        )
    };
    let catalog =
        super::catalog::Catalog::new(vec![manifest], Vec::new(), vec![admission(61, "registrar")])?;
    let selected = catalog.select(&raw)?.expect("selected registrar event");
    let mut live_state = super::state::State::new(vec![registry_prior.clone()], Vec::new());
    let interpreted = super::protocol::interpret(&selected, &raw, &mut live_state, false)?;
    let grant = interpreted
        .events
        .into_iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registration grant");
    let restored_grant = PriorEventInput {
        retained_state_key: format!("registration-grant:{namehash}"),
        chain_id: CHAIN.to_owned(),
        namespace: selected.source.namespace.clone(),
        logical_name_id: grant.logical_name_id,
        resource_id: grant.resource_id,
        event_kind: grant.event_kind,
        source_family: selected.source.source_family.clone(),
        manifest_version: selected.source.manifest_version,
        source_manifest_id: Some(selected.source.manifest_id),
        emitting_address: None,
        state_scope: Some(grant.state_scope),
        block_timestamp: Some(raw.block_timestamp),
        after_state: grant.after_state,
    };
    let restored_state = super::state::State::new(vec![registry_prior, restored_grant], Vec::new());

    assert_eq!(
        v1_state_snapshot(live_state.v1_name("ens", &namehash)),
        v1_state_snapshot(restored_state.v1_name("ens", &namehash)),
        "restored current authority must match live processing",
    );
    assert_eq!(
        v1_state_snapshot(live_state.v1_registrar("ens", &namehash)),
        v1_state_snapshot(restored_state.v1_registrar("ens", &namehash)),
        "restored registrar anchor must match live processing",
    );
    let expected_family = if registration {
        "ens_v1_registrar_l1"
    } else {
        "ens_v1_registry_l1"
    };
    assert_eq!(
        live_state
            .v1_name("ens", &namehash)
            .expect("live current authority")
            .authority_source_family,
        expected_family,
    );
    Ok(())
}

fn v1_state_snapshot(state: Option<super::state::V1NameState>) -> serde_json::Value {
    state.map_or(serde_json::Value::Null, |state| {
        json!({
            "logical_name_id":state.logical_name_id,
            "surface_known":state.surface_known,
            "resource_id":state.resource_id,
            "token_lineage_id":state.token_lineage_id,
            "authority_source_family":state.authority_source_family,
            "source_manifest_id":state.source_manifest_id,
            "labelhash":state.labelhash,
            "expiry":state.expiry,
            "owner":state.owner,
            "authority_key":state.authority_key,
        })
    })
}

#[test]
fn registry_created_emits_the_ruled_self_edge() -> anyhow::Result<()> {
    let encoded = RegistryCreated {}.encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            2,
            "ens_v2_registry_l1",
            "RegistryCreated",
            "event RegistryCreated()",
            &[],
            &["RegistryCreated"],
        )],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 2,
            edge_kind: "registry_announcement".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert_eq!(output.discovery_edges.len(), 1);
    let edge = &output.discovery_edges[0];
    assert_eq!(edge.edge_kind, "registry_announcement");
    assert_eq!(edge.from_contract_instance_id, edge.to_contract_instance_id);
    assert_eq!(edge.active_from_block_number, 1);
    Ok(())
}

#[test]
fn registry_created_selects_the_rule_role_independent_of_admission_order() -> anyhow::Result<()> {
    let root = admission(2, "ETHRegistry");
    let registry = admission(2, "registry");
    for admissions in [vec![root.clone(), registry.clone()], vec![registry, root]] {
        let output = interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest(
                2,
                "ens_v2_registry_l1",
                "RegistryCreated",
                "event RegistryCreated()",
                &[],
                &["RegistryCreated"],
            )],
            discovery_rules: vec![DiscoveryRuleInput {
                manifest_id: 2,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            }],
            admissions,
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw(RegistryCreated {}.encode_log_data())],
        })?;

        assert_eq!(output.discovery_edges.len(), 1);
    }
    Ok(())
}

#[test]
fn role_free_role_sensitive_event_is_rejected() -> anyhow::Result<()> {
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            71,
            "ens_v1_registry_l1",
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            &[],
            &["SubregistryChanged", "AuthorityTransferred"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(71, "registry_old")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v1_registry::NewOwner {
            node: B256::ZERO,
            label: keccak256(b"role-sensitive"),
            owner: CONTRACT.parse()?,
        }
        .encode_log_data())],
    })
    .expect_err("role-free NewOwner must fail manifest validation");

    assert_eq!(
        error.to_string(),
        "manifest 71 source family ens_v1_registry_l1 event NewOwner has empty emitter_roles; declare emitter_roles, or add the (source_family, event) pair to bigname_manifests::ROLE_INSENSITIVE_EVENTS with a justification that the adapter does not consume Selected.emitter_role"
    );
    Ok(())
}

#[test]
fn role_sensitive_single_admission_preserves_emitter_role() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            72,
            "ens_v1_registry_l1",
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            &["registry_old"],
            &["SubregistryChanged", "AuthorityTransferred"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(72, "registry_old")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v1_registry::NewOwner {
            node: B256::ZERO,
            label: keccak256(b"role-preserved"),
            owner: CONTRACT.parse()?,
        }
        .encode_log_data())],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.after_state["source_event"] == "NewOwner")
        .expect("NewOwner normalized event");
    assert_eq!(event.after_state["emitter_role"], "registry_old");
    Ok(())
}

#[test]
fn role_sensitive_distinct_role_tie_errors_in_either_admission_order() -> anyhow::Result<()> {
    let registry = admission(73, "registry");
    let registry_old = admission(73, "registry_old");
    for admissions in [
        vec![registry.clone(), registry_old.clone()],
        vec![registry_old, registry],
    ] {
        let catalog = super::catalog::Catalog::new(
            vec![manifest(
                73,
                "ens_v1_registry_l1",
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry", "registry_old"],
                &["SubregistryChanged", "AuthorityTransferred"],
            )],
            Vec::new(),
            admissions,
        )?;
        let raw = raw(v1_registry::NewOwner {
            node: B256::ZERO,
            label: keccak256(b"ambiguous-role"),
            owner: CONTRACT.parse()?,
        }
        .encode_log_data());
        let error = catalog
            .select(&raw)
            .expect_err("a distinct-role tie without a discovery rule must fail selection");
        assert_eq!(
            error.to_string(),
            "raw log block-1:0 has ambiguous admitted adapters: ens_v1_registry_l1:NewOwner(bytes32,bytes32,address) (role=registry, instance=00000000-0000-0000-0000-000000000049), ens_v1_registry_l1:NewOwner(bytes32,bytes32,address) (role=registry_old, instance=00000000-0000-0000-0000-000000000049)"
        );
    }
    Ok(())
}

#[test]
fn role_insensitivity_metadata_matches_adapter_implementations() -> anyhow::Result<()> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .context("adapters crate sits two directories below the workspace root")?;
    let schema_v2_root = workspace_root.join("crates/adapters/src/schema_v2");
    let known_families = bigname_manifests::ROLE_INSENSITIVE_EVENTS
        .iter()
        .map(|entry| entry.source_family.to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    let routes = checked_in_protocol_adapter_paths(&schema_v2_root, &known_families)?;
    let sources = read_rust_source_map(&protocol_rule_lookup_source_paths()?)?;
    validate_role_insensitivity_metadata(
        workspace_root,
        bigname_manifests::ROLE_INSENSITIVE_EVENTS,
        &routes,
        &sources,
    )?;

    // `protocol_rule_lookup_producers` excludes [discovery edges](../../../../docs/glossary.md#discovery-graph--discovery-edge)
    // that bypass `Catalog::rule`; today that permits the `Upgraded` -> `proxy_implementation` entry.
    let shared_discovery_overlap = role_insensitivity_discovery_overlap(
        bigname_manifests::ROLE_INSENSITIVE_EVENTS,
        &protocol_rule_lookup_producers()?,
    );
    anyhow::ensure!(
        shared_discovery_overlap.is_empty(),
        "ROLE_INSENSITIVE_EVENTS must not contain rule-backed discovery producers that consume Selected.emitter_role: {shared_discovery_overlap:?}"
    );
    Ok(())
}

#[test]
fn role_insensitivity_metadata_rejects_a_stale_declared_adapter_route() -> anyhow::Result<()> {
    let workspace_root = std::path::Path::new("/workspace");
    let schema_v2_root = workspace_root.join("crates/adapters/src/schema_v2");
    let known_families = std::collections::BTreeSet::from(["ens_v2_resolver_l1".to_owned()]);
    let routes = protocol_adapter_paths_from_dispatch_sources(
        &schema_v2_root,
        &known_families,
        r#"
            fn interpret(selected: &Selected, raw: &Raw, state: &mut State) {
                match selected.source.source_family.as_str() {
                    "ens_v2_resolver_l1" => role_consuming::interpret(selected, raw, state),
                    _ => unreachable!(),
                }
            }
        "#,
        "",
        "",
    )?;
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "ens_v2_resolver_l1",
        event: "AddressChanged",
        justification: "test fixture",
        adapter_file: "crates/adapters/src/schema_v2/protocol/v2_resolver.rs",
    }];
    let routed_path = schema_v2_root.join("protocol/role_consuming.rs");
    let sources = std::collections::BTreeMap::from([(
        routed_path.clone(),
        r#"
            fn interpret(selected: &Selected) {
                match selected.event.name.as_str() {
                    "AddressChanged" => consume(selected.emitter_role.as_deref()),
                    _ => unreachable!(),
                }
            }
        "#
        .to_owned(),
    )]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("a stale adapter_file must not survive a production reroute");
    assert!(
        error
            .to_string()
            .contains("ens_v2_resolver_l1 AddressChanged")
    );
    assert!(error.to_string().contains("protocol/v2_resolver.rs"));
    assert!(
        error
            .to_string()
            .contains(&routed_path.display().to_string())
    );
    Ok(())
}

#[test]
fn role_insensitivity_metadata_scans_routed_descendant_helpers() {
    let workspace_root = std::path::Path::new("/workspace");
    let routed_path = workspace_root.join("adapter/role_independent.rs");
    let helper_path = workspace_root.join("adapter/role_independent/helper.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "adapter/role_independent.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([
        (
            routed_path,
            r#"
                mod helper;
                fn interpret(selected: &Selected) {
                    match selected.event.name.as_str() {
                        "SharedEvent" => helper::consume_role(selected),
                        _ => unreachable!(),
                    }
                }
            "#
            .to_owned(),
        ),
        (
            helper_path.clone(),
            "fn consume_role(selected: &Selected) { consume(selected.emitter_role.as_deref()); }"
                .to_owned(),
        ),
    ]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("a delegated Selected.emitter_role read must fail the guard");
    assert!(
        error
            .to_string()
            .contains("role_independent_family SharedEvent")
    );
    assert!(
        error
            .to_string()
            .contains(&helper_path.display().to_string())
    );
}

#[test]
fn role_insensitivity_metadata_scans_selected_bearing_sibling_helpers() {
    let workspace_root = std::path::Path::new("/workspace");
    let routed_path = workspace_root.join("schema_v2/protocol/resolver.rs");
    let helper_path = workspace_root.join("schema_v2/protocol/helper.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "schema_v2/protocol/resolver.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([
        (
            routed_path,
            r#"
                use super::helper::consume_role;
                fn interpret(selected: &Selected) {
                    match selected.event.name.as_str() {
                        "SharedEvent" => consume_role(selected),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
        (
            helper_path.clone(),
            "fn consume_role(selected: &Selected) { consume(selected.emitter_role.as_deref()); }"
                .to_owned(),
        ),
    ]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("a Selected-bearing sibling helper must be scanned");
    assert!(
        error
            .to_string()
            .contains(&helper_path.display().to_string())
    );
}

#[test]
fn role_insensitivity_metadata_scans_glob_imported_selected_helpers() {
    let workspace_root = std::path::Path::new("/workspace");
    let routed_path = workspace_root.join("schema_v2/protocol/resolver.rs");
    let helper_path = workspace_root.join("schema_v2/protocol/helper.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "schema_v2/protocol/resolver.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([
        (
            routed_path,
            r#"
                use super::helper::*;
                fn interpret(selected: &Selected) {
                    match selected.event.name.as_str() {
                        "SharedEvent" => consume_role(selected),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
        (
            helper_path.clone(),
            "fn consume_role(selected: &Selected) { consume(selected.emitter_role.as_deref()); }"
                .to_owned(),
        ),
    ]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("a glob-imported Selected helper must be scanned");
    assert!(
        error
            .to_string()
            .contains(&helper_path.display().to_string())
    );
}

#[test]
fn role_insensitivity_metadata_tracks_forwarded_selected_aliases() {
    let workspace_root = std::path::Path::new("/workspace");
    let routed_path = workspace_root.join("schema_v2/protocol/resolver.rs");
    let helper_path = workspace_root.join("schema_v2/protocol/helper.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "schema_v2/protocol/resolver.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([
        (
            routed_path,
            r#"
                use super::helper::consume_role;
                fn interpret(selected: &Selected) {
                    let forwarded = selected;
                    match selected.event.name.as_str() {
                        "SharedEvent" => consume_role(forwarded),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
        (
            helper_path.clone(),
            "fn consume_role(selected: &Selected) { consume(selected.emitter_role.as_deref()); }"
                .to_owned(),
        ),
    ]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("an alias-forwarded Selected helper must be scanned");
    assert!(
        error
            .to_string()
            .contains(&helper_path.display().to_string())
    );
}

#[test]
fn role_insensitivity_metadata_follows_reexported_selected_helpers() {
    let workspace_root = std::path::Path::new("/workspace");
    let routed_path = workspace_root.join("schema_v2/protocol/resolver.rs");
    let exports_path = workspace_root.join("schema_v2/protocol/exports.rs");
    let helper_path = workspace_root.join("schema_v2/protocol/helper.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "schema_v2/protocol/resolver.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([
        (
            routed_path,
            r#"
                use super::exports::consume_role;
                fn interpret(selected: &Selected) {
                    match selected.event.name.as_str() {
                        "SharedEvent" => consume_role(selected),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
        (
            exports_path,
            "pub use super::helper::consume_role;".to_owned(),
        ),
        (
            helper_path.clone(),
            "fn consume_role(selected: &Selected) { consume(selected.emitter_role.as_deref()); }"
                .to_owned(),
        ),
    ]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("a re-exported Selected helper must be scanned");
    assert!(
        error
            .to_string()
            .contains(&helper_path.display().to_string())
    );
}

#[test]
fn role_insensitivity_metadata_scans_shared_dispatchers() {
    let workspace_root = std::path::Path::new("/workspace");
    let schema_v2_root = workspace_root.join("schema_v2");
    let routed_path = schema_v2_root.join("protocol/v1/resolver.rs");
    let dispatcher_path = schema_v2_root.join("protocol/v1.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "schema_v2/protocol/v1/resolver.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([
        (
            routed_path,
            r#"
                fn interpret(selected: &Selected) {
                    match selected.event.name.as_str() {
                        "SharedEvent" => consume(),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
        (
            dispatcher_path.clone(),
            "fn interpret(selected: &Selected) { consume(selected.emitter_role.as_deref()); }"
                .to_owned(),
        ),
    ]);

    let error = validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("a shared dispatcher Selected.emitter_role read must fail the guard");
    assert!(
        error
            .to_string()
            .contains(&dispatcher_path.display().to_string())
    );
}

#[test]
fn role_insensitivity_metadata_detects_pattern_destructuring() -> anyhow::Result<()> {
    assert!(rust_source_reads_emitter_role(
        "fn consume(selected: &Selected) { let Selected { emitter_role, .. } = selected; use_role(emitter_role); }"
    )?);
    Ok(())
}

#[test]
fn role_insensitivity_metadata_detects_role_reads_in_macro_tokens() -> anyhow::Result<()> {
    assert!(rust_source_reads_emitter_role(
        r#"fn consume(selected: &Selected) { let _ = json!({"role": selected.emitter_role}); }"#
    )?);
    Ok(())
}

#[test]
fn role_insensitivity_metadata_detects_role_patterns_in_macro_tokens() -> anyhow::Result<()> {
    assert!(rust_source_reads_emitter_role(
        "fn consume(selected: &Selected) { let _ = matches!(selected, Selected { emitter_role: Some(_), .. }); }"
    )?);
    Ok(())
}

#[test]
fn role_insensitivity_metadata_rejects_a_stale_event_literal() {
    let workspace_root = std::path::Path::new("/workspace");
    let routed_path = workspace_root.join("adapter/role_independent.rs");
    let entries = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "StaleEvent",
        justification: "test fixture",
        adapter_file: "adapter/role_independent.rs",
    }];
    let routes = std::collections::BTreeMap::from([(
        "role_independent_family".to_owned(),
        routed_path.clone(),
    )]);
    let sources = std::collections::BTreeMap::from([(
        routed_path,
        r#"
            fn interpret(selected: &Selected) {
                const STALE_DIAGNOSTIC: &str = "StaleEvent";
                match selected.event.name.as_str() {
                    "LiveEvent" => consume(STALE_DIAGNOSTIC),
                    _ => {}
                }
            }
        "#
        .to_owned(),
    )]);

    validate_role_insensitivity_metadata(workspace_root, &entries, &routes, &sources)
        .expect_err("an event name outside a selected.event handler arm must not validate");
}

#[test]
fn role_insensitivity_discovery_overlap_is_source_family_scoped() {
    let role_events = [bigname_manifests::RoleInsensitiveEvent {
        source_family: "role_independent_family",
        event: "SharedEvent",
        justification: "test fixture",
        adapter_file: "adapter/role_independent.rs",
    }];
    let mut producers = std::collections::BTreeSet::from([(
        "rule_backed_family".to_owned(),
        "SharedEvent".to_owned(),
        "resolver".to_owned(),
    )]);

    assert!(
        role_insensitivity_discovery_overlap(&role_events, &producers).is_empty(),
        "the same event name in another source family must not overlap"
    );
    let exact_overlap = (
        "role_independent_family".to_owned(),
        "SharedEvent".to_owned(),
        "resolver".to_owned(),
    );
    producers.insert((
        exact_overlap.0.clone(),
        exact_overlap.1.clone(),
        exact_overlap.2.clone(),
    ));
    assert_eq!(
        role_insensitivity_discovery_overlap(&role_events, &producers),
        std::collections::BTreeSet::from([exact_overlap]),
        "a rule-backed producer in the same source family must overlap"
    );
}

#[test]
fn protocol_rule_lookup_producer_scoping_tracks_dispatch_and_shared_helpers() -> anyhow::Result<()>
{
    let schema_v2_root = std::path::Path::new("/workspace/schema_v2");
    let known_families = std::collections::BTreeSet::from([
        "ens_v2_registry_l1".to_owned(),
        "ens_v2_registrar_l1".to_owned(),
        "future_registrar_family".to_owned(),
        "ens_v2_resolver_l1".to_owned(),
    ]);
    let protocol_source = r#"
        fn interpret(selected: &Selected, raw: &Raw, state: &mut State) {
            const ROUTE_DIAGNOSTIC: &str = "future_registrar_family";
            match unrelated.as_str() {
                "future_registrar_family" => v2_resolver::interpret(selected, raw, state),
                _ => v2_registry::interpret(selected, raw, state),
            }
            match selected.source.source_family.as_str() {
                "ens_v2_resolver_l1" => v2_resolver::interpret(selected, raw, state),
                "ens_v2_registry_l1" => v2_registry::interpret(selected, raw, state),
                "ens_v2_registrar_l1" | "future_registrar_family" => {
                    v2_registry::interpret(selected, raw, state)
                }
                _ => unreachable!(),
            }
        }
    "#;
    let v2_registry_source = r#"
        fn interpret(selected: &Selected, raw: &Raw, state: &mut State) {
            /*
            if selected.source.source_family == "future_registrar_family" {
                return v2_resolver::interpret(selected, raw, state);
            }
            */
            if selected.source.source_family == "ens_v2_registrar_l1"
                || selected.source.source_family == "future_registrar_family"
            {
                return registrar::interpret(selected, raw, state);
            }
        }
    "#;
    let adapter_paths = protocol_adapter_paths_from_dispatch_sources(
        schema_v2_root,
        &known_families,
        protocol_source,
        "",
        v2_registry_source,
    )?;
    let locations = std::collections::BTreeSet::from([(
        schema_v2_root.join("protocol/v2_registry/registrar.rs"),
        "SharedEvent".to_owned(),
        "resolver".to_owned(),
    )]);
    let manifest_events = std::collections::BTreeSet::from([
        ("ens_v2_registry_l1".to_owned(), "SharedEvent".to_owned()),
        ("ens_v2_registrar_l1".to_owned(), "SharedEvent".to_owned()),
        (
            "future_registrar_family".to_owned(),
            "SharedEvent".to_owned(),
        ),
        ("ens_v2_resolver_l1".to_owned(), "SharedEvent".to_owned()),
        ("ens_v2_resolver_l1".to_owned(), "DelegatedEvent".to_owned()),
    ]);

    assert_eq!(
        scope_protocol_rule_lookup_producers(
            schema_v2_root,
            &locations,
            &manifest_events,
            &adapter_paths,
        )?,
        std::collections::BTreeSet::from([
            (
                "ens_v2_registrar_l1".to_owned(),
                "SharedEvent".to_owned(),
                "resolver".to_owned(),
            ),
            (
                "future_registrar_family".to_owned(),
                "SharedEvent".to_owned(),
                "resolver".to_owned(),
            ),
        ])
    );

    let helper_location = std::collections::BTreeSet::from([(
        schema_v2_root.join("protocol/shared_helper.rs"),
        "DelegatedEvent".to_owned(),
        "resolver".to_owned(),
    )]);
    let error = scope_protocol_rule_lookup_producers(
        schema_v2_root,
        &helper_location,
        &manifest_events,
        &adapter_paths,
    )
    .expect_err("an unowned shared helper must fail closed");
    assert!(error.to_string().contains("has no protocol dispatch owner"));
    Ok(())
}

#[test]
fn checked_in_protocol_adapter_paths_follow_production_dispatch() -> anyhow::Result<()> {
    let schema_v2_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/schema_v2");
    let known_families = checked_in_manifest_source_family_events()?
        .into_iter()
        .map(|(source_family, _)| source_family)
        .collect::<std::collections::BTreeSet<_>>();
    let routes = checked_in_protocol_adapter_paths(&schema_v2_root, &known_families)?;

    assert_eq!(
        routes.get("ens_v1_resolver_l1"),
        routes.get("basenames_base_resolver")
    );
    assert_eq!(
        routes.get("ens_v2_registry_l1"),
        Some(&schema_v2_root.join("protocol/v2_registry.rs"))
    );
    assert_eq!(
        routes.get("ens_v2_registrar_l1"),
        Some(&schema_v2_root.join("protocol/v2_registry/registrar.rs"))
    );
    Ok(())
}

#[test]
fn protocol_adapter_paths_honor_a_top_level_v1_family_reroute() -> anyhow::Result<()> {
    let schema_v2_root = std::path::Path::new("/workspace/schema_v2");
    let known_families = std::collections::BTreeSet::from(["ens_v1_resolver_l1".to_owned()]);
    let routes = protocol_adapter_paths_from_dispatch_sources(
        schema_v2_root,
        &known_families,
        r#"
            fn interpret(selected: &Selected) {
                match selected.source.source_family.as_str() {
                    "ens_v1_resolver_l1" => v2_resolver::interpret(selected),
                    _ => unreachable!(),
                }
            }
        "#,
        r#"
            fn interpret(selected: &Selected) {
                match selected.source.source_family.as_str() {
                    "ens_v1_resolver_l1" => resolver::interpret(selected),
                    _ => unreachable!(),
                }
            }
        "#,
        "",
    )?;

    assert_eq!(
        routes.get("ens_v1_resolver_l1"),
        Some(&schema_v2_root.join("protocol/v2_resolver.rs"))
    );
    Ok(())
}

#[test]
fn guarded_dispatch_respects_conjunctions() -> anyhow::Result<()> {
    let expression = syn::parse_str::<syn::Expr>(
        r#"family.starts_with("ens_v1_") && family != "ens_v1_resolver_l1""#,
    )?;
    assert!(!guard_accepts_source_family(
        &expression,
        "family",
        "ens_v1_resolver_l1"
    ));
    Ok(())
}

#[test]
fn role_insensitive_events_collapse_distinct_admission_roles() -> anyhow::Result<()> {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests");
    let mut variants = std::collections::BTreeMap::<
        (String, String),
        std::collections::BTreeMap<String, Vec<String>>,
    >::new();
    for environment in ["mainnet", "sepolia"] {
        let repository = bigname_manifests::load_repository(manifest_root.join(environment))?;
        for loaded in repository.manifests() {
            for event in &loaded.manifest.abi.events {
                variants
                    .entry((loaded.manifest.source_family.clone(), event.name.clone()))
                    .or_default()
                    .insert(event.fragment.clone(), event.normalized_events.clone());
            }
        }
    }

    let mut manifest_id = 720;
    for entry in bigname_manifests::ROLE_INSENSITIVE_EVENTS {
        let event_variants = variants
            .get(&(entry.source_family.to_owned(), entry.event.to_owned()))
            .with_context(|| {
                format!(
                    "ROLE_INSENSITIVE_EVENTS entry for {} {} has no checked-in manifest event",
                    entry.source_family, entry.event,
                )
            })?;
        for (fragment, normalized_events) in event_variants {
            let normalized_events = normalized_events
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let source = manifest(
                manifest_id,
                entry.source_family,
                entry.event,
                fragment,
                &[],
                &normalized_events,
            );
            let mut first = admission(manifest_id, "first_role");
            let second = admission(manifest_id, "second_role");
            first.contract_instance_id = second.contract_instance_id;
            let catalog =
                super::catalog::Catalog::new(vec![source], Vec::new(), vec![first, second])?;
            let topic0 = alloy_json_abi::Event::parse(fragment)?
                .selector()
                .to_string();
            let selected = catalog.select(&raw_with_topic0(topic0))?.with_context(|| {
                format!(
                    "{} {} must select from distinct-role admissions",
                    entry.source_family, entry.event,
                )
            })?;
            assert_eq!(
                selected.emitter_role, None,
                "{} {} must clear the selected role for distinct admissions",
                entry.source_family, entry.event,
            );
            manifest_id += 1;
        }
    }
    Ok(())
}

#[test]
fn role_specific_discovery_event_does_not_widen_a_different_role_rule() -> anyhow::Result<()> {
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            2,
            "ens_v2_registry_l1",
            "SubregistryUpdated",
            "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
            &["ETHRegistry"],
            &["SubregistryChanged"],
        )],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 2,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: vec![admission(2, "ETHRegistry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(
            v2_registry::SubregistryUpdated {
                tokenId: U256::from(1),
                subregistry: "0x0000000000000000000000000000000000000043".parse()?,
                sender: CONTRACT.parse()?,
            }
            .encode_log_data(),
        )],
    })
    .expect_err("a non-registry role must not satisfy the subregistry discovery rule");

    assert_eq!(
        error.to_string(),
        "SubregistryUpdated is not admitted by a subregistry manifest rule"
    );
    Ok(())
}

#[test]
fn required_discovery_rules_cover_protocol_rule_lookup_producers() -> anyhow::Result<()> {
    let producers = protocol_rule_lookup_producers()?;
    anyhow::ensure!(
        !producers.is_empty(),
        "protocol source must expose at least one manifest-rule lookup producer"
    );
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests");
    let mut manifest_id = 700;
    let mut covered_producers = std::collections::BTreeSet::new();
    let mut covered_cases = std::collections::BTreeSet::new();
    for environment in ["mainnet", "sepolia"] {
        let repository = bigname_manifests::load_repository(manifest_root.join(environment))?;
        for loaded in repository
            .manifests()
            .iter()
            .filter(|loaded| loaded.manifest.rollout_status.is_active())
        {
            let source = &loaded.manifest;
            for event in &source.abi.events {
                for (_, _, edge_kind) in
                    producers
                        .iter()
                        .filter(|(producer_family, producer_event, _)| {
                            producer_family == &source.source_family
                                && producer_event == &event.name
                        })
                {
                    covered_producers.insert((
                        source.source_family.clone(),
                        event.name.clone(),
                        edge_kind.clone(),
                    ));
                    if !covered_cases.insert((
                        environment.to_owned(),
                        source.namespace.clone(),
                        source.chain.clone(),
                        source.deployment_epoch.clone(),
                        source.manifest_version,
                        source.rollout_status.as_db_value(),
                        source.source_family.clone(),
                        event.name.clone(),
                        edge_kind.clone(),
                    )) {
                        continue;
                    }

                    let checked_in_rule = source
                        .discovery_rules
                        .iter()
                        .find(|rule| rule.edge_kind == *edge_kind)
                        .with_context(|| {
                            format!(
                                "{} {} has no checked-in {edge_kind} discovery rule",
                                source.source_family, event.name
                            )
                        })?;
                    let normalized_events = event
                        .normalized_events
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    let emitter_roles = event
                        .emitter_roles
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    let source_input = manifest(
                        manifest_id,
                        &source.source_family,
                        &event.name,
                        &event.fragment,
                        &emitter_roles,
                        &normalized_events,
                    );
                    let mut unrelated = admission(manifest_id, "unrelated_role");
                    unrelated.contract_instance_id = Uuid::from_u128(700);
                    let mut other = admission(manifest_id, &checked_in_rule.from_role);
                    other.contract_instance_id = unrelated.contract_instance_id;
                    let catalog = super::catalog::Catalog::new(
                        vec![source_input],
                        vec![DiscoveryRuleInput {
                            manifest_id,
                            edge_kind: checked_in_rule.edge_kind.clone(),
                            from_role: Some(checked_in_rule.from_role.clone()),
                            admission: checked_in_rule.admission.clone(),
                        }],
                        vec![unrelated, other],
                    )?;
                    let topic0 = event
                        .topic0()?
                        .with_context(|| format!("{} must have topic0", event.name))?;
                    let raw = raw_with_topic0(topic0);
                    let selected = catalog.select(&raw)?.with_context(|| {
                        format!(
                            "{} {} must select an admitted role",
                            source.source_family, event.name
                        )
                    })?;
                    assert_eq!(
                        selected.emitter_role.as_deref(),
                        Some(checked_in_rule.from_role.as_str()),
                        "required_discovery_rule must cover {} {} -> {edge_kind}",
                        source.source_family,
                        event.name,
                    );
                    manifest_id += 1;
                }
            }
        }
    }

    assert_eq!(
        covered_producers, producers,
        "every rule-lookup producer in protocol source must have a checked-in manifest event"
    );
    Ok(())
}

#[test]
fn producer_guard_scans_schema_v2_sibling_modules_but_not_tests() -> anyhow::Result<()> {
    let sources = protocol_rule_lookup_source_paths()?;
    let schema_v2_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/schema_v2");

    assert!(sources.contains(&schema_v2_root.join("protocol.rs")));
    assert!(!sources.contains(&schema_v2_root.join("tests.rs")));
    Ok(())
}

#[test]
fn producer_guard_resolves_discovery_draft_variant_aliases() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/aliased.rs");
    let sources = std::collections::BTreeMap::from([(
        adapter_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft::{
                Edge as RoutedEdge,
                RegistryAnnouncement as Announcement,
            };

            fn interpret(selected: &Selected, output: &mut Interpreted) {
                match selected.event.name.as_str() {
                    "ResolverUpdated" => output.discovery.push(RoutedEdge {
                        edge_kind: "resolver".to_owned(),
                        target: Address::ZERO,
                        observation_key: String::new(),
                    }),
                    "RegistryCreated" => output.discovery.push(Announcement),
                    _ => {}
                }
            }
        "#
        .to_owned(),
    )]);
    let locations = protocol_rule_lookup_producer_locations_from_sources(
        &sources,
        "registry_announcement",
        &std::collections::BTreeSet::new(),
    )?;

    assert_eq!(
        locations,
        std::collections::BTreeSet::from([
            (
                adapter_path.clone(),
                "RegistryCreated".to_owned(),
                "registry_announcement".to_owned(),
            ),
            (
                adapter_path,
                "ResolverUpdated".to_owned(),
                "resolver".to_owned(),
            ),
        ])
    );
    Ok(())
}

#[test]
fn producer_guard_resolves_self_renamed_discovery_draft_import() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/aliased.rs");
    let sources = std::collections::BTreeMap::from([(
        adapter_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft::{self as Draft};

            fn interpret(selected: &Selected, output: &mut Interpreted) {
                match selected.event.name.as_str() {
                    "ResolverUpdated" => output.discovery.push(Draft::Edge {
                        edge_kind: "resolver".to_owned(),
                        target: Address::ZERO,
                        observation_key: String::new(),
                    }),
                    _ => {}
                }
            }
        "#
        .to_owned(),
    )]);

    assert_eq!(
        protocol_rule_lookup_producer_locations_from_sources(
            &sources,
            "registry_announcement",
            &std::collections::BTreeSet::new(),
        )?,
        std::collections::BTreeSet::from([(
            adapter_path,
            "ResolverUpdated".to_owned(),
            "resolver".to_owned(),
        )])
    );
    Ok(())
}

#[test]
fn producer_guard_resolves_discovery_draft_type_alias() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/aliased.rs");
    let sources = std::collections::BTreeMap::from([(
        adapter_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft;
            type Draft = DiscoveryDraft;

            fn interpret(selected: &Selected, output: &mut Interpreted) {
                match selected.event.name.as_str() {
                    "ResolverUpdated" => output.discovery.push(Draft::Edge {
                        edge_kind: "resolver".to_owned(),
                        target: Address::ZERO,
                        observation_key: String::new(),
                    }),
                    _ => {}
                }
            }
        "#
        .to_owned(),
    )]);

    assert_eq!(
        protocol_rule_lookup_producer_locations_from_sources(
            &sources,
            "registry_announcement",
            &std::collections::BTreeSet::new(),
        )?,
        std::collections::BTreeSet::from([(
            adapter_path,
            "ResolverUpdated".to_owned(),
            "resolver".to_owned(),
        )])
    );
    Ok(())
}

#[test]
fn producer_guard_resolves_reexported_discovery_draft_variant() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/adapter.rs");
    let drafts_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/drafts.rs");
    let sources = std::collections::BTreeMap::from([
        (
            drafts_path,
            "pub use crate::schema_v2::protocol::DiscoveryDraft::Edge;".to_owned(),
        ),
        (
            adapter_path.clone(),
            r#"
                use super::drafts::Edge;

                fn interpret(selected: &Selected, output: &mut Interpreted) {
                    match selected.event.name.as_str() {
                        "ResolverUpdated" => output.discovery.push(Edge {
                            edge_kind: "resolver".to_owned(),
                            target: Address::ZERO,
                            observation_key: String::new(),
                        }),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
    ]);

    assert_eq!(
        protocol_rule_lookup_producer_locations_from_sources(
            &sources,
            "registry_announcement",
            &std::collections::BTreeSet::new(),
        )?,
        std::collections::BTreeSet::from([(
            adapter_path,
            "ResolverUpdated".to_owned(),
            "resolver".to_owned(),
        )])
    );
    Ok(())
}

#[test]
fn producer_guard_resolves_module_qualified_reexported_variant() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/adapter.rs");
    let drafts_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/drafts.rs");
    let sources = std::collections::BTreeMap::from([
        (
            drafts_path,
            "pub use crate::schema_v2::protocol::DiscoveryDraft::Edge;".to_owned(),
        ),
        (
            adapter_path.clone(),
            r#"
                use super::drafts;

                fn interpret(selected: &Selected, output: &mut Interpreted) {
                    match selected.event.name.as_str() {
                        "ResolverUpdated" => output.discovery.push(drafts::Edge {
                            edge_kind: "resolver".to_owned(),
                            target: Address::ZERO,
                            observation_key: String::new(),
                        }),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
    ]);

    assert_eq!(
        protocol_rule_lookup_producer_locations_from_sources(
            &sources,
            "registry_announcement",
            &std::collections::BTreeSet::new(),
        )?,
        std::collections::BTreeSet::from([(
            adapter_path,
            "ResolverUpdated".to_owned(),
            "resolver".to_owned(),
        )])
    );
    Ok(())
}

#[test]
fn producer_guard_resolves_directly_qualified_variants_and_type_aliases() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/adapter.rs");
    let drafts_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/drafts.rs");
    let sources = std::collections::BTreeMap::from([
        (
            drafts_path,
            r#"
                pub use crate::schema_v2::protocol::DiscoveryDraft::Edge;
                pub type Draft = crate::schema_v2::protocol::DiscoveryDraft;
            "#
            .to_owned(),
        ),
        (
            adapter_path.clone(),
            r#"
                fn interpret(selected: &Selected, output: &mut Interpreted) {
                    match selected.event.name.as_str() {
                        "ResolverUpdated" => output.discovery.push(super::drafts::Edge {
                            edge_kind: "resolver".to_owned(),
                            target: Address::ZERO,
                            observation_key: String::new(),
                        }),
                        "SubregistryUpdated" => output.discovery.push(super::drafts::Draft::Edge {
                            edge_kind: "subregistry".to_owned(),
                            target: Address::ZERO,
                            observation_key: String::new(),
                        }),
                        _ => {}
                    }
                }
            "#
            .to_owned(),
        ),
    ]);

    assert_eq!(
        protocol_rule_lookup_producer_locations_from_sources(
            &sources,
            "registry_announcement",
            &std::collections::BTreeSet::new(),
        )?,
        std::collections::BTreeSet::from([
            (
                adapter_path.clone(),
                "ResolverUpdated".to_owned(),
                "resolver".to_owned(),
            ),
            (
                adapter_path,
                "SubregistryUpdated".to_owned(),
                "subregistry".to_owned(),
            ),
        ])
    );
    Ok(())
}

#[test]
fn producer_guard_detects_discovery_draft_in_macro_tokens() -> anyhow::Result<()> {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/macro.rs");
    let sources = std::collections::BTreeMap::from([(
        adapter_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft;

            fn interpret(selected: &Selected, output: &mut Interpreted) {
                match selected.event.name.as_str() {
                    "ResolverUpdated" => output.discovery.extend(vec![DiscoveryDraft::Edge {
                        edge_kind: "resolver".to_owned(),
                        target: Address::ZERO,
                        observation_key: String::new(),
                    }]),
                    _ => {}
                }
            }
        "#
        .to_owned(),
    )]);

    assert_eq!(
        protocol_rule_lookup_producer_locations_from_sources(
            &sources,
            "registry_announcement",
            &std::collections::BTreeSet::new(),
        )?,
        std::collections::BTreeSet::from([(
            adapter_path,
            "ResolverUpdated".to_owned(),
            "resolver".to_owned(),
        )])
    );
    Ok(())
}

#[test]
fn producer_guard_rejects_nonliteral_macro_edge_kind() {
    let adapter_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/macro.rs");
    let sources = std::collections::BTreeMap::from([(
        adapter_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft;

            fn interpret(selected: &Selected, output: &mut Interpreted) {
                match selected.event.name.as_str() {
                    "ResolverUpdated" => output.discovery.extend(vec![DiscoveryDraft::Edge {
                        edge_kind: choose_kind(),
                        target: Address::ZERO,
                        observation_key: "resolver".to_owned(),
                    }]),
                    _ => {}
                }
            }
        "#
        .to_owned(),
    )]);

    let error = protocol_rule_lookup_producer_locations_from_sources(
        &sources,
        "registry_announcement",
        &std::collections::BTreeSet::new(),
    )
    .expect_err("a non-literal macro edge kind must fail closed");
    assert!(
        error
            .to_string()
            .contains(&adapter_path.display().to_string())
    );
    assert!(error.to_string().contains("without a literal edge_kind"));
}

#[test]
fn producer_guard_rejects_a_helper_constructed_discovery_draft() {
    let helper_path = std::path::PathBuf::from("/workspace/schema_v2/protocol/helper.rs");
    let sources = std::collections::BTreeMap::from([(
        helper_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft;

            fn make_discovery() -> DiscoveryDraft {
                DiscoveryDraft::Edge {
                    edge_kind: "resolver".into(),
                    target: Address::ZERO,
                    observation_key: String::new(),
                }
            }
        "#
        .to_owned(),
    )]);

    let error = protocol_rule_lookup_producer_locations_from_sources(
        &sources,
        "registry_announcement",
        &std::collections::BTreeSet::new(),
    )
    .expect_err("a helper constructor outside a named event arm must fail closed");
    assert!(
        error
            .to_string()
            .contains(&helper_path.display().to_string())
    );
    assert!(
        error
            .to_string()
            .contains("outside a named selected.event arm")
    );
}

#[test]
fn producer_guard_rejects_a_discovery_module_constructor() {
    let discovery_path = std::path::PathBuf::from("/workspace/schema_v2/discovery.rs");
    let sources = std::collections::BTreeMap::from([(
        discovery_path.clone(),
        r#"
            use crate::schema_v2::protocol::DiscoveryDraft::Edge;

            fn construct_in_discovery() -> DiscoveryDraft {
                Edge {
                    edge_kind: "resolver".to_owned(),
                    target: Address::ZERO,
                    observation_key: String::new(),
                }
            }
        "#
        .to_owned(),
    )]);

    let error = protocol_rule_lookup_producer_locations_from_sources(
        &sources,
        "registry_announcement",
        &std::collections::BTreeSet::new(),
    )
    .expect_err("a discovery.rs constructor without an owned producer must fail closed");
    assert!(
        error
            .to_string()
            .contains(&discovery_path.display().to_string())
    );
    assert!(
        error
            .to_string()
            .contains("outside a named selected.event arm")
    );
}

#[test]
fn root_resolver_updated_is_admitted_in_active_sepolia_manifest() -> anyhow::Result<()> {
    const MANIFEST_ID: i64 = 374;
    const RESOLVER_MANIFEST_ID: i64 = 375;
    const RESOLVER: &str = "0x0000000000000000000000000000000000000043";

    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    let repository = bigname_manifests::load_repository(manifest_root)?;
    let source = &repository
        .manifests()
        .iter()
        .find(|loaded| {
            loaded.manifest.source_family == "ens_v2_root_l1"
                && loaded.manifest.rollout_status.is_active()
        })
        .context("active Sepolia ens_v2_root_l1 manifest is missing")?
        .manifest;
    let contract = source
        .contracts
        .iter()
        .find(|contract| contract.role == "root_registry")
        .context("active RootRegistry contract role is missing")?;
    let root = source
        .roots
        .iter()
        .find(|root| root.address.eq_ignore_ascii_case(&contract.address))
        .context("RootRegistry root and contract declarations must share an address")?;
    let resolver_rule = source
        .discovery_rules
        .iter()
        .find(|rule| rule.edge_kind == "resolver")
        .context("active RootRegistry resolver rule is missing")?;
    assert_eq!(resolver_rule.from_role, "root_registry");
    assert_eq!(resolver_rule.admission, "reachable_from_root");
    let mut root_admission = admission(MANIFEST_ID, &root.name);
    root_admission.address = root.address.clone();
    let mut contract_admission = admission(MANIFEST_ID, &contract.role);
    contract_admission.address = contract.address.clone();
    let root_instance_id = contract_admission.contract_instance_id;
    let mut resolver_updated = raw_at(
        v2_registry::ResolverUpdated {
            tokenId: U256::from(1),
            resolver: RESOLVER.parse()?,
            sender: CONTRACT.parse()?,
        }
        .encode_log_data(),
        1,
        0,
        &contract.address,
    );
    resolver_updated.chain_id = source.chain.clone();
    let root_manifest = ManifestInput {
        manifest_id: MANIFEST_ID,
        manifest_version: i64::try_from(source.manifest_version)?,
        namespace: source.namespace.clone(),
        source_family: source.source_family.clone(),
        chain_id: source.chain.clone(),
        deployment_label: source.deployment_epoch.clone(),
        normalizer_version: source.normalizer_version.clone(),
        payload_json: serde_json::to_string(source)?,
    };
    let mut resolver_manifest = manifest(
        RESOLVER_MANIFEST_ID,
        "ens_v2_resolver_l1",
        "TextChanged",
        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
        &[],
        &["RecordChanged"],
    );
    resolver_manifest.namespace = source.namespace.clone();
    resolver_manifest.chain_id = source.chain.clone();
    resolver_manifest.deployment_label = source.deployment_epoch.clone();
    let output = interpret_test_batch(BatchInput {
        chain_id: source.chain.clone(),
        manifests: vec![root_manifest.clone(), resolver_manifest.clone()],
        discovery_rules: source
            .discovery_rules
            .iter()
            .map(|rule| DiscoveryRuleInput {
                manifest_id: MANIFEST_ID,
                edge_kind: rule.edge_kind.clone(),
                from_role: Some(rule.from_role.clone()),
                admission: rule.admission.clone(),
            })
            .collect(),
        admissions: vec![root_admission, contract_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![resolver_updated],
    })?;

    let changed = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .context("RootRegistry ResolverUpdated did not emit ResolverChanged")?;
    assert_eq!(changed.source_family, "ens_v2_root_l1");
    assert_eq!(changed.after_state["resolver"], RESOLVER);
    let edge = output
        .discovery_edges
        .iter()
        .find(|edge| edge.edge_kind == "resolver")
        .context("RootRegistry ResolverUpdated did not emit a resolver discovery edge")?;
    assert_eq!(edge.from_contract_instance_id, root_instance_id);
    assert_eq!(edge.source_manifest_id, MANIFEST_ID);
    assert_eq!(edge.admission_basis, "reachable_from_root");
    let address = output
        .contract_addresses
        .iter()
        .find(|address| address.address == RESOLVER)
        .context("resolver discovery did not emit its address interval")?;
    let selected = super::catalog::Catalog::new(
        vec![root_manifest, resolver_manifest],
        source
            .discovery_rules
            .iter()
            .map(|rule| DiscoveryRuleInput {
                manifest_id: MANIFEST_ID,
                edge_kind: rule.edge_kind.clone(),
                from_role: Some(rule.from_role.clone()),
                admission: rule.admission.clone(),
            })
            .collect(),
        vec![AddressAdmissionInput {
            address: address.address.clone(),
            contract_instance_id: address.contract_instance_id,
            source_manifest_id: Some(MANIFEST_ID),
            role: None,
            discovery_edge_kind: Some("resolver".to_owned()),
            discovery_from_contract_instance_id: Some(root_instance_id),
            discovery_observation_key: Some(edge.observation_key.clone()),
            active_from_block: Some(address.active_from_block_number),
            active_to_block: None,
        }],
    )?
    .select(&raw_at(
        resolver_strings::TextChanged {
            node: B256::ZERO,
            indexedKey: keccak256(b"url"),
            key: "url".to_owned(),
            value: "https://example.test".to_owned(),
        }
        .encode_log_data(),
        2,
        0,
        RESOLVER,
    ))?
    .context("root-discovered resolver did not select a target adapter")?;
    assert_eq!(selected.source.source_family, "ens_v2_resolver_l1");
    Ok(())
}

#[test]
fn mainnet_double_declarations_select_the_event_role_in_either_order() -> anyhow::Result<()> {
    const MAINNET_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
    const MAINNET_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = bigname_manifests::load_repository(root)?;
    let cases = [
        (
            91,
            "ens_v1_registry_l1",
            v1_registry::NewOwner {
                node: B256::ZERO,
                label: keccak256(b"deterministic"),
                owner: CONTRACT.parse()?,
            }
            .encode_log_data(),
            MAINNET_REGISTRY,
            "ENSRegistry",
            "registry",
        ),
        (
            92,
            "ens_v1_registrar_l1",
            v1_registrar::Transfer {
                from: CONTRACT.parse()?,
                to: "0x0000000000000000000000000000000000000043".parse()?,
                tokenId: U256::from(1),
            }
            .encode_log_data(),
            MAINNET_REGISTRAR,
            "ETHRegistrar",
            "registrar",
        ),
    ];

    for (manifest_id, source_family, encoded, address, root_role, event_role) in cases {
        let loaded = repository
            .manifests()
            .iter()
            .find(|loaded| {
                loaded.manifest.source_family == source_family
                    && loaded.manifest.rollout_status.is_active()
            })
            .with_context(|| format!("mainnet manifest is missing {source_family}"))?;
        let source = &loaded.manifest;
        assert!(
            source.roots.iter().any(|root| {
                root.name == root_role && root.address.eq_ignore_ascii_case(address)
            })
        );
        assert!(source.contracts.iter().any(|contract| {
            contract.role == event_role && contract.address.eq_ignore_ascii_case(address)
        }));
        let manifest = ManifestInput {
            manifest_id,
            manifest_version: i64::try_from(source.manifest_version)?,
            namespace: source.namespace.clone(),
            source_family: source.source_family.clone(),
            chain_id: source.chain.clone(),
            deployment_label: source.deployment_epoch.clone(),
            normalizer_version: source.normalizer_version.clone(),
            payload_json: serde_json::to_string(source)?,
        };
        let mut root_admission = admission(manifest_id, root_role);
        root_admission.address = address.to_owned();
        let mut contract = admission(manifest_id, event_role);
        contract.address = address.to_owned();
        for admissions in [
            vec![root_admission.clone(), contract.clone()],
            vec![contract.clone(), root_admission.clone()],
        ] {
            let catalog =
                super::catalog::Catalog::new(vec![manifest.clone()], Vec::new(), admissions)?;
            let selected = catalog
                .select(&raw_at(encoded.clone(), 1, 0, address))?
                .with_context(|| {
                    format!("double-declared mainnet {source_family} emitter must be admitted")
                })?;
            assert_eq!(selected.emitter_role.as_deref(), Some(event_role));
        }
    }
    Ok(())
}

#[test]
fn announced_registry_prefers_a_same_namespace_declaring_manifest() -> anyhow::Result<()> {
    let event = (
        "RegistryCreated",
        "event RegistryCreated()",
        &["registry"][..],
        &["RegistryCreated"][..],
    );
    let mut announced = admission(65, "registry");
    announced.role = None;
    announced.discovery_edge_kind = Some("registry_announcement".to_owned());
    announced.discovery_from_contract_instance_id = Some(announced.contract_instance_id);
    announced.discovery_observation_key = Some("registry-announcement:fixture".to_owned());
    let mut declared = admission(66, "registry");
    declared.contract_instance_id = announced.contract_instance_id;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(65, "ens", "ens_v2_registry_l1", &[event]),
            manifest_with_events(66, "ens", "ens_v2_registry_l1", &[event]),
        ],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 65,
                edge_kind: "registry_announcement".to_owned(),
                from_role: None,
                admission: "event_announcement".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 66,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "declared_deployment".to_owned(),
            },
        ],
        admissions: vec![announced, declared],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(RegistryCreated {}.encode_log_data())],
    })?;

    let normalized = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistryCreated")
        .expect("registry announcement event");
    assert_eq!(normalized.source_manifest_id, Some(66));
    assert_eq!(output.discovery_edges[0].source_manifest_id, 66);
    Ok(())
}

#[test]
fn foreign_announcement_does_not_make_declared_admission_tie_match_all() -> anyhow::Result<()> {
    let resolver_event = (
        "AddrChanged",
        "event AddrChanged(bytes32 indexed node, address a)",
        &[][..],
        &["RecordChanged"][..],
    );
    let mut foreign_announcement = admission(68, "registry");
    foreign_announcement.role = None;
    foreign_announcement.discovery_edge_kind = Some("registry_announcement".to_owned());
    foreign_announcement.discovery_from_contract_instance_id =
        Some(foreign_announcement.contract_instance_id);
    foreign_announcement.discovery_observation_key =
        Some("registry-announcement:foreign".to_owned());
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(67, "ens", "ens_v1_resolver_l1", &[resolver_event]),
            manifest_with_events(
                68,
                "foreign",
                "ens_v2_registry_l1",
                &[(
                    "RegistryCreated",
                    "event RegistryCreated()",
                    &["registry"][..],
                    &["RegistryCreated"][..],
                )],
            ),
            manifest_with_events(
                69,
                "basenames",
                "basenames_base_resolver",
                &[resolver_event],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(67, "resolver"), foreign_announcement],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(resolver::AddrChanged {
            node: B256::repeat_byte(0x67),
            a: CONTRACT.parse()?,
        }
        .encode_log_data())],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("declared resolver admission");
    assert_eq!(event.source_manifest_id, Some(67));
    assert_eq!(event.namespace, "ens");
    Ok(())
}

#[test]
fn declared_resolver_out_ranks_same_namespace_resolver_discovery() -> anyhow::Result<()> {
    const DECLARED_ID: i64 = 70;
    const DISCOVERY_ID: i64 = 71;
    const RESOLVER_ID: i64 = 72;
    let text_event = (
        "TextChanged",
        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
        &[][..],
        &["RecordChanged"][..],
    );
    let target = super::common::contract_id(CHAIN, CONTRACT);
    let mut declared = admission(DECLARED_ID, "public_resolver");
    declared.contract_instance_id = target;
    let discovered = |source_manifest_id, from, key: &str| AddressAdmissionInput {
        address: CONTRACT.to_owned(),
        contract_instance_id: target,
        source_manifest_id: Some(source_manifest_id),
        role: None,
        discovery_edge_kind: Some("resolver".to_owned()),
        discovery_from_contract_instance_id: Some(from),
        discovery_observation_key: Some(key.to_owned()),
        active_from_block: Some(0),
        active_to_block: None,
    };
    let raw = raw(resolver_strings::TextChanged {
        node: B256::repeat_byte(0x70),
        indexedKey: keccak256(b"url"),
        key: "url".to_owned(),
        value: "https://example.test".to_owned(),
    }
    .encode_log_data());

    for discovery_family in ["ens_v2_registry_l1", "ens_v2_root_l1"] {
        let manifests = vec![
            manifest_with_events(DECLARED_ID, "ens", "ens_v1_resolver_l1", &[text_event]),
            manifest_with_events(DISCOVERY_ID, "ens", discovery_family, &[]),
            manifest_with_events(RESOLVER_ID, "ens", "ens_v2_resolver_l1", &[text_event]),
            manifest_with_events(77, "foreign", "ens_v2_registry_l1", &[]),
        ];
        let edge = discovered(DISCOVERY_ID, Uuid::from_u128(700), "resolver:first");
        let mut foreign = discovered(77, Uuid::from_u128(705), "announcement:foreign");
        foreign.discovery_edge_kind = Some("registry_announcement".to_owned());
        for admissions in [
            vec![declared.clone(), edge.clone()],
            vec![edge.clone(), declared.clone()],
            vec![declared.clone(), edge.clone(), foreign],
        ] {
            let selected = super::catalog::Catalog::new(manifests.clone(), Vec::new(), admissions)?
                .select(&raw)?
                .context("resolver log must be selected")?;
            assert_eq!(selected.source.source_family, "ens_v1_resolver_l1");
        }
    }

    let mut foreign = manifest_with_events(73, "foreign", "ens_v1_resolver_l1", &[text_event]);
    foreign.deployment_label = "fixture".to_owned();
    let selected = super::catalog::Catalog::new(
        vec![
            foreign,
            manifest_with_events(DISCOVERY_ID, "ens", "ens_v2_registry_l1", &[]),
            manifest_with_events(RESOLVER_ID, "ens", "ens_v2_resolver_l1", &[text_event]),
        ],
        Vec::new(),
        vec![
            AddressAdmissionInput {
                source_manifest_id: Some(73),
                ..declared.clone()
            },
            discovered(DISCOVERY_ID, Uuid::from_u128(701), "resolver:foreign"),
        ],
    )?
    .select(&raw)?
    .context("foreign declaration must not suppress resolver discovery")?;
    assert_eq!(selected.source.source_family, "ens_v2_resolver_l1");

    let selected = super::catalog::Catalog::new(
        vec![
            manifest_with_events(DISCOVERY_ID, "ens", "ens_v2_registry_l1", &[]),
            manifest_with_events(RESOLVER_ID, "ens", "ens_v2_resolver_l1", &[text_event]),
        ],
        Vec::new(),
        vec![
            discovered(DISCOVERY_ID, Uuid::from_u128(702), "resolver:first"),
            discovered(DISCOVERY_ID, Uuid::from_u128(703), "resolver:second"),
        ],
    )?
    .select(&raw)?
    .context("equivalent resolver discoveries must collapse to one adapter")?;
    assert_eq!(selected.source.source_family, "ens_v2_resolver_l1");
    let run = |admissions| {
        interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![
                manifest_with_events(DECLARED_ID, "ens", "ens_v1_resolver_l1", &[text_event]),
                manifest_with_events(DISCOVERY_ID, "ens", "ens_v2_registry_l1", &[]),
                manifest_with_events(RESOLVER_ID, "ens", "ens_v2_resolver_l1", &[text_event]),
            ],
            discovery_rules: Vec::new(),
            admissions,
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw.clone()],
        })
    };
    let direct = run(vec![declared.clone()])?;
    let overlapping = run(vec![
        declared,
        discovered(DISCOVERY_ID, Uuid::from_u128(704), "resolver:stable"),
    ])?;
    let stable = |output: &BatchOutput| {
        let event = output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == "RecordChanged")
            .expect("resolver record event");
        json!({
            "event_identity": event.event_identity,
            "namespace": event.namespace,
            "logical_name_id": event.logical_name_id,
            "resource_id": event.resource_id,
            "event_kind": event.event_kind,
            "source_family": event.source_family,
            "manifest_version": event.manifest_version,
            "source_manifest_id": event.source_manifest_id,
            "chain_id": event.chain_id,
            "block_number": event.block_number,
            "block_hash": event.block_hash,
            "transaction_hash": event.transaction_hash,
            "transaction_index": event.transaction_index,
            "log_index": event.log_index,
            "raw_fact_ref": event.raw_fact_ref,
            "derivation_kind": event.derivation_kind,
            "canonicality_state": event.canonicality_state,
            "before_state": event.before_state,
            "after_state": event.after_state,
            "consumer_visibility": event.consumer_visibility,
        })
    };
    assert_eq!(
        serde_json::to_vec(&stable(&direct))?,
        serde_json::to_vec(&stable(&overlapping))?
    );
    Ok(())
}

#[test]
fn registry_permission_adapter_selects_one_root_event() -> anyhow::Result<()> {
    let encoded = EACRolesChanged {
        resource: U256::ZERO,
        account: CONTRACT.parse::<Address>()?,
        oldRoleBitmap: U256::from(1),
        newRoleBitmap: U256::from(2),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            3,
            "ens_v2_registry_l1",
            "EACRolesChanged",
            "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
            &["registry"],
            &["PermissionChanged", "RootPermissionChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(3, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let permission_events = output
        .normalized_events
        .iter()
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "PermissionChanged" | "RootPermissionChanged"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(permission_events.len(), 1);
    assert_eq!(permission_events[0].event_kind, "RootPermissionChanged");
    assert_eq!(
        permission_events[0].before_state["role_bitmap"],
        json!(format!("{:#066x}", U256::from(1)))
    );
    Ok(())
}

#[test]
fn registrar_transfer_reuses_and_materializes_the_registration_resource() -> anyhow::Result<()> {
    let labelhash = keccak256(b"alice");
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: labelhash,
        owner: CONTRACT.parse::<Address>()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let transfer = v1_registrar::Transfer {
        from: "0x0000000000000000000000000000000000000001".parse()?,
        to: "0x0000000000000000000000000000000000000002".parse()?,
        tokenId: U256::from_be_slice(labelhash.as_slice()),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            10,
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
                    "Transfer",
                    "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                    &["registrar"],
                    &["TokenControlTransferred"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(10, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration, 1, 0, CONTRACT),
            raw_at(transfer, 2, 0, CONTRACT),
        ],
    })?;
    let registered_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let transfer_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenControlTransferred")
        .and_then(|event| event.resource_id)
        .expect("transfer resource");

    assert_eq!(transfer_resource, registered_resource);
    assert!(
        output
            .resources
            .iter()
            .any(|resource| resource.resource_id == transfer_resource),
        "every normalized-event resource must be materialized for the schema foreign key"
    );
    Ok(())
}

#[test]
fn ens_v1_reregistration_mints_a_fresh_registrar_anchor() -> anyhow::Result<()> {
    let registration = || {
        NameRegistered {
            name: "alice".to_owned(),
            label: keccak256(b"alice"),
            owner: CONTRACT.parse::<Address>().unwrap(),
            expires: U256::from(42),
        }
        .encode_log_data()
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            28,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(28, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration(), 1, 0, CONTRACT),
            raw_at(registration(), 3, 0, CONTRACT),
        ],
    })?;

    let resources = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationGranted")
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    let lineages = output
        .resources
        .iter()
        .filter_map(|resource| resource.token_lineage_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(resources.len(), 2);
    assert_eq!(lineages.len(), 2);
    assert_eq!(output.surface_bindings.len(), 2);
    assert_ne!(
        output.surface_bindings[0].surface_binding_id,
        output.surface_bindings[1].surface_binding_id
    );
    Ok(())
}

#[test]
fn same_block_v1_releases_have_distinct_permission_event_identities() -> anyhow::Result<()> {
    let manifest = manifest(
        58,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let registration = |label: &str, log_index| {
        raw_at(
            NameRegistered {
                name: label.to_owned(),
                label: keccak256(label.as_bytes()),
                owner: CONTRACT.parse::<Address>().unwrap(),
                expires: U256::from(2),
            }
            .encode_log_data(),
            1,
            log_index,
            CONTRACT,
        )
    };
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(58, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![registration("alice", 0), registration("bob", 1)],
    })?;
    let boundary = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(58, "registrar")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "release-block".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10_000_000),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;

    let releases = boundary
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 2);
    assert_ne!(releases[0].logical_name_id, releases[1].logical_name_id);
    assert_ne!(releases[0].event_identity, releases[1].event_identity);
    Ok(())
}

#[test]
fn same_transaction_controller_setup_is_attributed_to_the_registration() -> anyhow::Result<()> {
    assert_same_transaction_controller_setup(
        "ens",
        "ens_v1_registry_l1",
        "ens_v1_registrar_l1",
        &["eth"],
    )
}

#[test]
fn basenames_same_transaction_controller_setup_uses_the_registration_resource() -> anyhow::Result<()>
{
    // Basenames likewise writes the registry subnode before emitting the registration event.
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L423 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L425 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/Registry.sol:L120 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/Registry.sol:L122 @ basenames@1809bbc)
    assert_same_transaction_controller_setup(
        "basenames",
        "basenames_base_registry",
        "basenames_base_registrar",
        &["base", "eth"],
    )
}

#[derive(Clone, Copy)]
enum RegistrationSetupFlow {
    LegacyTwoOwnerChanges,
    LegacyTwoOwnerChangesWithControllerAsPriorOwner,
    LegacyTwoOwnerChangesWithWrappedOwner,
    ModernSingleOwnerChange,
    ModernSingleOwnerChangeWithWrappedOwner,
}

#[test]
fn legacy_registration_preserves_the_prior_registry_owner_revocation() -> anyhow::Result<()> {
    assert_registration_preserves_the_prior_registry_owner_revocation(
        RegistrationSetupFlow::LegacyTwoOwnerChanges,
    )
}

#[test]
fn legacy_registration_preserves_a_controller_that_was_the_real_prior_owner() -> anyhow::Result<()>
{
    assert_registration_preserves_the_prior_registry_owner_revocation(
        RegistrationSetupFlow::LegacyTwoOwnerChangesWithControllerAsPriorOwner,
    )
}

#[test]
fn modern_registration_preserves_the_prior_registry_owner_revocation() -> anyhow::Result<()> {
    assert_registration_preserves_the_prior_registry_owner_revocation(
        RegistrationSetupFlow::ModernSingleOwnerChange,
    )
}

// NameWrapper remains the registrar/registry owner while `wrappedOwner` is the user, so the
// incoming registry grant's subject intentionally differs from NameRegistered.owner.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L291 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L300 @ ens_v1@91c966f)
#[test]
fn legacy_wrapped_registration_routes_the_incoming_owner_grant_to_the_registrar_resource()
-> anyhow::Result<()> {
    assert_registration_preserves_the_prior_registry_owner_revocation(
        RegistrationSetupFlow::LegacyTwoOwnerChangesWithWrappedOwner,
    )
}

#[test]
fn modern_wrapped_registration_routes_the_incoming_owner_grant_to_the_registrar_resource()
-> anyhow::Result<()> {
    assert_registration_preserves_the_prior_registry_owner_revocation(
        RegistrationSetupFlow::ModernSingleOwnerChangeWithWrappedOwner,
    )
}

#[test]
fn born_wrapped_unwrap_revokes_the_name_wrapper_registration_grant() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const REGISTRAR: &str = "0x0000000000000000000000000000000000000044";
    const CONTROLLER: &str = "0x0000000000000000000000000000000000000045";
    const NAME_WRAPPER: &str = "0x0000000000000000000000000000000000000046";
    const USER: &str = "0x0000000000000000000000000000000000000047";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000048";

    let labelhash = keccak256(b"alice");
    let node = super::common::namehash(&["alice".to_owned(), "eth".to_owned()]);
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let mut registry_admission = admission(40, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let mut controller_admission = admission(41, "wrapped_registrar_controller");
    controller_admission.address = CONTROLLER.to_owned();
    controller_admission.contract_instance_id = Uuid::from_u128(4101);
    let mut registrar_admission = admission(41, "registrar");
    registrar_admission.address = REGISTRAR.to_owned();
    registrar_admission.contract_instance_id = Uuid::from_u128(4102);
    let mut wrapper_admission = admission(42, "name_wrapper");
    wrapper_admission.address = NAME_WRAPPER.to_owned();
    wrapper_admission.contract_instance_id = Uuid::from_u128(4201);

    let manifests = vec![
        manifest_with_events(
            40,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry"],
                    &["SubregistryChanged", "AuthorityTransferred"],
                ),
                (
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry"],
                    &["AuthorityTransferred"],
                ),
            ],
        ),
        manifest_with_events(
            41,
            "ens",
            "ens_v1_registrar_l1",
            &[
                (
                    "NameRegistered",
                    "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                    &["wrapped_registrar_controller"],
                    &["RegistrationGranted"],
                ),
                (
                    "Transfer",
                    "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                    &["registrar"],
                    &["TokenControlTransferred"],
                ),
            ],
        ),
        manifest_with_events(
            42,
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
                        "SurfaceBound",
                        "AuthorityEpochChanged",
                    ],
                ),
                (
                    "NameUnwrapped",
                    "event NameUnwrapped(bytes32 indexed node, address owner)",
                    &["name_wrapper"],
                    &["SurfaceUnbound", "AuthorityEpochChanged"],
                ),
            ],
        ),
        manifest(
            43,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        ),
    ];
    let admissions = vec![
        registry_admission,
        controller_admission,
        registrar_admission,
        wrapper_admission,
    ];
    let registered = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent_node,
                    label: labelhash,
                    owner: NAME_WRAPPER.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                NameWrapped {
                    node: node.parse()?,
                    name: b"\x05alice\x03eth\0".to_vec().into(),
                    owner: USER.parse()?,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                1,
                1,
                NAME_WRAPPER,
            ),
            raw_at(
                resolver::AddrChanged {
                    node: node.parse()?,
                    a: USER.parse()?,
                }
                .encode_log_data(),
                1,
                2,
                RESOLVER,
            ),
            raw_at(
                NameRegistered {
                    name: "alice".to_owned(),
                    label: labelhash,
                    owner: USER.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                3,
                CONTROLLER,
            ),
        ],
    })?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: registered
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::Transfer {
                    node: node.parse()?,
                    owner: USER.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                REGISTRY,
            ),
            raw_at(
                NameUnwrapped {
                    node: node.parse()?,
                    owner: USER.parse()?,
                }
                .encode_log_data(),
                2,
                1,
                NAME_WRAPPER,
            ),
            raw_at(
                v1_registrar::Transfer {
                    from: NAME_WRAPPER.parse()?,
                    to: USER.parse()?,
                    tokenId: U256::from_be_bytes(*labelhash),
                }
                .encode_log_data(),
                2,
                2,
                REGISTRAR,
            ),
        ],
    })?;

    let registrar_resource = registered
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let wrapper_resource = registered
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .and_then(|event| event.resource_id)
        .expect("wrapper resource");
    let wrapped_record = registered
        .normalized_events
        .iter()
        .find(|event| event.source_family == "ens_v1_resolver_l1")
        .expect("born-wrapped resolver record");
    assert_eq!(wrapped_record.resource_id, Some(wrapper_resource));
    assert_ne!(wrapped_record.resource_id, Some(registrar_resource));
    assert!(
        registered.normalized_events.iter().all(|event| {
            event.event_kind != "SurfaceBound" || event.resource_id != Some(registrar_resource)
        }),
        "the controller event emitted after NameWrapped must retain the wrapper binding"
    );
    let wrapper_permissions = registered
        .normalized_events
        .iter()
        .chain(&output.normalized_events)
        .filter(|event| event.event_kind == "PermissionChanged")
        .filter(|event| event.resource_id == Some(registrar_resource))
        .filter(|event| event.after_state["subject"] == NAME_WRAPPER)
        .filter(|event| event.after_state["scope"]["kind"] == "resource")
        .collect::<Vec<_>>();
    let unwrap_epoch = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "AuthorityEpochChanged"
                && event.source_family == "ens_v1_wrapper_l1"
                && event.after_state["source_event"] == "NameUnwrapped"
        })
        .expect("unwrap authority epoch");
    assert_eq!(
        unwrap_epoch.before_state["authority_kind"], "wrapper",
        "restoring the later controller event must not displace wrapper authority"
    );
    assert!(
        wrapper_permissions
            .iter()
            .any(|event| { event.after_state["effective_powers"] == json!(["resource_control"]) })
    );
    assert!(
        wrapper_permissions
            .iter()
            .any(|event| event.after_state["effective_powers"] == json!([])),
        "unwrapping must revoke NameWrapper's registrar-resource control"
    );
    Ok(())
}

#[test]
fn wrapper_fallback_registrar_identity_matches_live_full_replay_and_cold_restore()
-> anyhow::Result<()> {
    const REGISTRAR: &str = "0x0000000000000000000000000000000000000044";
    const NAME_WRAPPER: &str = "0x0000000000000000000000000000000000000046";
    const USER: &str = "0x0000000000000000000000000000000000000047";
    const NEXT_OWNER: &str = "0x0000000000000000000000000000000000000048";
    const FINAL_OWNER: &str = "0x0000000000000000000000000000000000000049";
    const REGISTRAR_EXPIRY: u64 = 1_000_000;
    const WRAPPER_EXPIRY: u64 = REGISTRAR_EXPIRY + 90 * 24 * 60 * 60;

    let labelhash = keccak256(b"alice");
    let node = super::common::namehash(&["alice".to_owned(), "eth".to_owned()]);
    let manifests = || {
        vec![
            manifest(
                41,
                "ens_v1_registrar_l1",
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                &["TokenControlTransferred"],
            ),
            manifest_with_events(
                42,
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
                            "SurfaceBound",
                            "AuthorityEpochChanged",
                        ],
                    ),
                    (
                        "NameUnwrapped",
                        "event NameUnwrapped(bytes32 indexed node, address owner)",
                        &["name_wrapper"],
                        &["SurfaceUnbound", "AuthorityEpochChanged"],
                    ),
                ],
            ),
        ]
    };
    let admissions = || {
        let mut registrar = admission(41, "registrar");
        registrar.address = REGISTRAR.to_owned();
        let mut wrapper = admission(42, "name_wrapper");
        wrapper.address = NAME_WRAPPER.to_owned();
        vec![registrar, wrapper]
    };
    let wrap = raw_at(
        NameWrapped {
            node: node.parse()?,
            name: b"\x05alice\x03eth\0".to_vec().into(),
            owner: USER.parse()?,
            fuses: 1,
            expiry: WRAPPER_EXPIRY,
        }
        .encode_log_data(),
        1,
        0,
        NAME_WRAPPER,
    );
    let unwrap = raw_at(
        NameUnwrapped {
            node: node.parse()?,
            owner: USER.parse()?,
        }
        .encode_log_data(),
        2,
        0,
        NAME_WRAPPER,
    );
    let fallback_transfer = raw_at(
        v1_registrar::Transfer {
            from: NAME_WRAPPER.parse()?,
            to: USER.parse()?,
            tokenId: U256::from_be_bytes(*labelhash),
        }
        .encode_log_data(),
        2,
        1,
        REGISTRAR,
    );
    let later_transfer = raw_at(
        v1_registrar::Transfer {
            from: USER.parse()?,
            to: NEXT_OWNER.parse()?,
            tokenId: U256::from_be_bytes(*labelhash),
        }
        .encode_log_data(),
        3,
        0,
        REGISTRAR,
    );
    let final_transfer = raw_at(
        v1_registrar::Transfer {
            from: NEXT_OWNER.parse()?,
            to: FINAL_OWNER.parse()?,
            tokenId: U256::from_be_bytes(*labelhash),
        }
        .encode_log_data(),
        4,
        0,
        REGISTRAR,
    );
    let block = |number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };

    let wrapped = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![wrap.clone()],
    })?;
    let fallback = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: wrapped.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![unwrap.clone(), fallback_transfer.clone()],
    })?;
    let fallback_event = fallback
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .expect("ordered unwrap transfer creates the missing registrar identity");
    assert_eq!(fallback_event.after_state["fallback_from_wrapper"], true);
    assert_eq!(fallback_event.after_state["expiry"], REGISTRAR_EXPIRY);
    let fallback_resource = fallback_event.resource_id.expect("fallback resource");
    let fallback_lineage = fallback_event.after_state["token_lineage_id"].clone();

    let fallback_history = wrapped
        .normalized_events
        .iter()
        .chain(&fallback.normalized_events)
        .cloned()
        .collect::<Vec<_>>();
    let cold_prior = seam::fold_prior_events(Vec::new(), &fallback_history, &[block(1), block(2)])?;
    let cold = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: cold_prior,
        blocks: Vec::new(),
        raw_logs: vec![later_transfer.clone()],
    })?;
    let cold_later = cold
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .expect("cold restore retains the fallback registrar identity");
    assert_eq!(cold_later.resource_id, Some(fallback_resource));
    assert_eq!(cold_later.after_state["token_lineage_id"], fallback_lineage);

    let full = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            wrap.clone(),
            unwrap.clone(),
            fallback_transfer.clone(),
            later_transfer.clone(),
            final_transfer.clone(),
        ],
    })?;
    let full_later = full
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
                && event.block_number == Some(3)
        })
        .expect("full replay retains the fallback registrar identity");
    assert_eq!(full_later.resource_id, cold_later.resource_id);
    assert_eq!(full_later.before_state, cold_later.before_state);
    assert_eq!(full_later.after_state, cold_later.after_state);

    let transfer_history = wrapped
        .normalized_events
        .iter()
        .chain(&fallback.normalized_events)
        .chain(&cold.normalized_events)
        .cloned()
        .collect::<Vec<_>>();
    let transfer_prior = seam::fold_prior_events(
        Vec::new(),
        &transfer_history,
        &[block(1), block(2), block(3)],
    )?;
    let retained_transfers = transfer_prior
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .collect::<Vec<_>>();
    assert_eq!(retained_transfers.len(), 1);
    assert_eq!(retained_transfers[0].after_state["to"], NEXT_OWNER);
    assert_eq!(
        retained_transfers[0].after_state["fallback_from_wrapper"],
        true
    );
    assert_eq!(retained_transfers[0].after_state["fallback_from"], USER);
    let cold_final = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: transfer_prior,
        blocks: Vec::new(),
        raw_logs: vec![final_transfer],
    })?;
    let full_final = full
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(4))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(cold_final.normalized_events, full_final);

    let transfer_before_unwrap = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: wrapped.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registrar::Transfer {
                    from: NAME_WRAPPER.parse()?,
                    to: USER.parse()?,
                    tokenId: U256::from_be_bytes(*labelhash),
                }
                .encode_log_data(),
                2,
                0,
                REGISTRAR,
            ),
            raw_at(
                NameUnwrapped {
                    node: node.parse()?,
                    owner: USER.parse()?,
                }
                .encode_log_data(),
                2,
                1,
                NAME_WRAPPER,
            ),
        ],
    })?;
    assert!(
        transfer_before_unwrap
            .normalized_events
            .iter()
            .all(|event| {
                event.source_family != "ens_v1_registrar_l1"
                    || event.event_kind != "TokenControlTransferred"
            })
    );

    let different_transaction = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: wrapped.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at_transaction(
                NameUnwrapped {
                    node: node.parse()?,
                    owner: USER.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                0,
                NAME_WRAPPER,
            ),
            raw_at_transaction(
                v1_registrar::Transfer {
                    from: NAME_WRAPPER.parse()?,
                    to: USER.parse()?,
                    tokenId: U256::from_be_bytes(*labelhash),
                }
                .encode_log_data(),
                2,
                1,
                1,
                REGISTRAR,
            ),
        ],
    })?;
    assert!(different_transaction.normalized_events.iter().all(|event| {
        event.source_family != "ens_v1_registrar_l1"
            || event.event_kind != "TokenControlTransferred"
    }));
    Ok(())
}

#[test]
fn registration_to_the_controller_discards_the_transient_registry_self_grant() -> anyhow::Result<()>
{
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";

    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let owner_change = v1_registry::NewOwner {
        node: parent_node,
        label: labelhash,
        owner: CONTRACT.parse()?,
    }
    .encode_log_data();
    let final_transfer = v1_registry::Transfer {
        node: super::common::namehash(&[label.to_owned(), "eth".to_owned()]).parse()?,
        owner: CONTRACT.parse()?,
    }
    .encode_log_data();
    let registration = NameRegistered {
        name: label.to_owned(),
        label: labelhash,
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let mut registry_admission = admission(38, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(
                38,
                "ens",
                "ens_v1_registry_l1",
                &[
                    (
                        "NewOwner",
                        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                        &["registry"],
                        &["SubregistryChanged", "AuthorityTransferred"],
                    ),
                    (
                        "Transfer",
                        "event Transfer(bytes32 indexed node, address owner)",
                        &["registry"],
                        &["AuthorityTransferred"],
                    ),
                ],
            ),
            manifest(
                39,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(39, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(owner_change, 1, 0, REGISTRY),
            raw_at(final_transfer, 1, 1, REGISTRY),
            raw_at(registration, 1, 2, CONTRACT),
        ],
    })?;

    let registrar_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let active_controller_grants = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .filter(|event| event.after_state["subject"] == CONTRACT)
        .filter(|event| event.after_state["effective_powers"] == json!(["resource_control"]))
        .collect::<Vec<_>>();
    assert!(!active_controller_grants.is_empty());
    assert!(
        active_controller_grants
            .iter()
            .all(|event| event.resource_id == Some(registrar_resource)),
        "the controller/registrant must have no active duplicate grant on the transient registry-only resource"
    );
    let registry_only_resources = output
        .resources
        .iter()
        .filter(|resource| resource.token_lineage_id.is_none())
        .map(|resource| resource.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        registry_only_resources.len(),
        1,
        "the superseded registry-only resource is retained at its first derivation block: {:?}",
        output.resources
    );
    assert_eq!(
        output
            .resources
            .iter()
            .map(|resource| resource.resource_id)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from_iter(
            registry_only_resources
                .into_iter()
                .chain([registrar_resource])
        )
    );
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;
    Ok(())
}

fn incomplete_registration_event(
    after_state: serde_json::Value,
    raw_fact_ref: serde_json::Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: "registration-event".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some("ens:node".to_owned()),
        resource_id: Some(Uuid::nil()),
        event_kind: "RegistrationGranted".to_owned(),
        source_family: "ens_v1_registrar_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(1),
        chain_id: "ethereum-mainnet".to_owned(),
        block_number: Some(1),
        block_hash: Some("block".to_owned()),
        transaction_hash: Some("transaction".to_owned()),
        transaction_index: Some(0),
        log_index: Some(1),
        raw_fact_ref,
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state,
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "activated".to_owned(),
        before_state_explicit: false,
    }
}

#[test]
#[should_panic(expected = "after_state.registrant")]
fn registration_without_registrant_cannot_silently_skip_reconciliation() {
    let mut output = BatchOutput {
        normalized_events: vec![incomplete_registration_event(
            json!({"source_event":"NameRegistered"}),
            json!({"emitting_address":"0x0000000000000000000000000000000000000001"}),
        )],
        ..BatchOutput::default()
    };
    super::protocol::reconcile_same_transaction_setups_for_test(&mut output);
}

#[test]
#[should_panic(expected = "raw_fact_ref.emitting_address")]
fn registration_without_emitter_cannot_silently_skip_reconciliation() {
    let mut output = BatchOutput {
        normalized_events: vec![incomplete_registration_event(
            json!({
                "source_event":"NameRegistered",
                "registrant":"0x0000000000000000000000000000000000000001",
            }),
            json!({}),
        )],
        ..BatchOutput::default()
    };
    super::protocol::reconcile_same_transaction_setups_for_test(&mut output);
}

#[test]
fn same_batch_prior_reference_retains_the_registry_only_resource() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const REGISTRANT: &str = "0x0000000000000000000000000000000000000047";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000049";

    let labelhash = keccak256(b"alice");
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&["alice".to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let owner_change = v1_registry::NewOwner {
        node: parent_node,
        label: labelhash,
        owner: REGISTRANT.parse()?,
    }
    .encode_log_data();
    let mut registry_admission = admission(36, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let seed = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            36,
            "ens_v1_registry_l1",
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            &["registry"],
            &["SubregistryChanged", "AuthorityTransferred"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at_transaction(owner_change.clone(), 1, 0, 0, REGISTRY)],
    })?;
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: labelhash,
        owner: REGISTRANT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let resolver_change = v1_registry::NewResolver {
        node,
        resolver: RESOLVER.parse()?,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(
                36,
                "ens",
                "ens_v1_registry_l1",
                &[
                    (
                        "NewOwner",
                        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                        &["registry"],
                        &["SubregistryChanged", "AuthorityTransferred"],
                    ),
                    (
                        "NewResolver",
                        "event NewResolver(bytes32 indexed node, address resolver)",
                        &["registry"],
                        &["ResolverChanged"],
                    ),
                ],
            ),
            manifest(
                37,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(37, "registrar")],
        prior_events: seed.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at_transaction(resolver_change, 2, 0, 0, REGISTRY),
            raw_at_transaction(owner_change, 2, 1, 0, REGISTRY),
            raw_at_transaction(registration, 2, 1, 1, CONTRACT),
        ],
    })?;

    let registrar_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let registry_resource = output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registry_l1"
                && event.transaction_index == Some(0)
                && event.event_kind == "ResolverChanged"
        })
        .and_then(|event| event.resource_id)
        .expect("earlier registry-only resolver event");
    assert_ne!(registry_resource, registrar_resource);
    assert!(
        output
            .resources
            .iter()
            .any(|resource| { resource.resource_id == registry_resource })
    );
    assert!(output.normalized_events.iter().all(|event| {
        event.source_family != "ens_v1_registry_l1"
            || event.transaction_index != Some(1)
            || event.resource_id.is_none()
            || event.resource_id == Some(registrar_resource)
    }));
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;
    Ok(())
}

#[test]
fn resolver_record_before_registration_setup_retains_predecessor_resource() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000044";
    const PRIOR_OWNER: &str = "0x0000000000000000000000000000000000000046";
    const REGISTRANT: &str = "0x0000000000000000000000000000000000000047";

    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&[label.to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let owner_change = |owner: &str| -> anyhow::Result<_> {
        Ok(v1_registry::NewOwner {
            node: parent_node,
            label: labelhash,
            owner: owner.parse()?,
        }
        .encode_log_data())
    };
    let mut registry_admission = admission(36, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let manifests = vec![
        manifest(
            36,
            "ens_v1_registry_l1",
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            &["registry"],
            &["SubregistryChanged", "AuthorityTransferred"],
        ),
        manifest(
            37,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        ),
        manifest(
            38,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        ),
    ];
    let admissions = vec![registry_admission, admission(38, "registrar")];
    let seed = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at_transaction(owner_change(PRIOR_OWNER)?, 1, 0, 0, REGISTRY),
            raw_at_transaction(
                NameRegistered {
                    name: label.to_owned(),
                    label: labelhash,
                    owner: PRIOR_OWNER.parse()?,
                    expires: U256::from(21),
                }
                .encode_log_data(),
                1,
                0,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let predecessor_resource = seed
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("predecessor registry resource");

    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: seed.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at_transaction(
                resolver::AddrChanged {
                    node,
                    a: PRIOR_OWNER.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                0,
                RESOLVER,
            ),
            raw_at_transaction(owner_change(REGISTRANT)?, 2, 0, 1, REGISTRY),
            raw_at_transaction(
                NameRegistered {
                    name: label.to_owned(),
                    label: labelhash,
                    owner: REGISTRANT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                2,
                0,
                2,
                CONTRACT,
            ),
        ],
    })?;
    let record_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .and_then(|event| event.resource_id)
        .expect("resolver record resource");
    let registration_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");

    assert_eq!(record_resource, predecessor_resource);
    assert_ne!(record_resource, registration_resource);
    assert_batch_referential_integrity(
        &output,
        &seed
            .resources
            .iter()
            .map(|resource| (resource.chain_id.clone(), resource.resource_id))
            .collect(),
        &seed
            .name_surfaces
            .iter()
            .map(|surface| (surface.chain_id.clone(), surface.logical_name_id.clone()))
            .collect(),
    )?;
    Ok(())
}

#[test]
fn legacy_register_with_config_midflow_record_uses_registration_resource() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000044";
    const REGISTRATION_CONTROLLER: &str = "0x0000000000000000000000000000000000000045";
    const REGISTRANT: &str = "0x0000000000000000000000000000000000000046";

    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&[label.to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let owner_change = |owner: &str| -> anyhow::Result<_> {
        Ok(v1_registry::NewOwner {
            node: parent_node,
            label: labelhash,
            owner: owner.parse()?,
        }
        .encode_log_data())
    };
    let mut registry_admission = admission(96, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                96,
                "ens_v1_registry_l1",
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            manifest(
                97,
                "ens_v1_resolver_l1",
                "AddrChanged",
                "event AddrChanged(bytes32 indexed node, address a)",
                &[],
                &["RecordChanged"],
            ),
            manifest(
                98,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(98, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(owner_change(REGISTRATION_CONTROLLER)?, 1, 0, REGISTRY),
            raw_at(
                resolver::AddrChanged {
                    node,
                    a: REGISTRANT.parse()?,
                }
                .encode_log_data(),
                1,
                1,
                RESOLVER,
            ),
            raw_at(owner_change(REGISTRANT)?, 1, 2, REGISTRY),
            raw_at(
                NameRegistered {
                    name: label.to_owned(),
                    label: labelhash,
                    owner: REGISTRANT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                3,
                CONTRACT,
            ),
        ],
    })?;
    let registration = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registration event");
    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("mid-flow resolver record");

    assert_eq!(record.logical_name_id, registration.logical_name_id);
    assert_eq!(
        record.logical_name_id.as_deref(),
        Some(format!("ens:{node:#x}").as_str())
    );
    assert_eq!(record.resource_id, registration.resource_id);
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;
    Ok(())
}

#[test]
fn reconciled_resolver_burst_preserves_intra_selector_before_state() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000044";
    const FIRST_ADDRESS: &str = "0x0000000000000000000000000000000000000045";
    const SECOND_ADDRESS: &str = "0x0000000000000000000000000000000000000046";
    const THIRD_ADDRESS: &str = "0x0000000000000000000000000000000000000047";

    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let node = super::common::namehash(&[label.to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let mut registry_admission = admission(93, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                93,
                "ens_v1_registry_l1",
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            manifest(
                94,
                "ens_v1_resolver_l1",
                "AddrChanged",
                "event AddrChanged(bytes32 indexed node, address a)",
                &[],
                &["RecordChanged"],
            ),
            manifest(
                95,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(95, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent_node,
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                resolver::AddrChanged {
                    node,
                    a: FIRST_ADDRESS.parse()?,
                }
                .encode_log_data(),
                1,
                1,
                RESOLVER,
            ),
            raw_at(
                resolver::AddrChanged {
                    node,
                    a: SECOND_ADDRESS.parse()?,
                }
                .encode_log_data(),
                1,
                2,
                RESOLVER,
            ),
            raw_at(
                NameRegistered {
                    name: label.to_owned(),
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                3,
                CONTRACT,
            ),
            raw_at(
                resolver::AddrChanged {
                    node,
                    a: THIRD_ADDRESS.parse()?,
                }
                .encode_log_data(),
                1,
                4,
                RESOLVER,
            ),
        ],
    })?;
    let registration_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let records = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RecordChanged")
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 3);
    assert!(records.iter().all(|event| {
        event.logical_name_id.as_deref() == Some(format!("ens:{node:#x}").as_str())
            && event.resource_id == Some(registration_resource)
    }));
    assert_eq!(records[0].before_state, json!({}));
    assert_eq!(records[1].before_state, records[0].after_state);
    assert_eq!(records[2].before_state, records[1].after_state);
    Ok(())
}

fn assert_registration_preserves_the_prior_registry_owner_revocation(
    flow: RegistrationSetupFlow,
) -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const PRIOR_OWNER: &str = "0x0000000000000000000000000000000000000046";
    const REGISTRANT: &str = "0x0000000000000000000000000000000000000047";
    const REGISTRATION_CONTROLLER: &str = "0x0000000000000000000000000000000000000048";
    const WRAPPED_OWNER: &str = "0x0000000000000000000000000000000000000049";

    let prior_owner = if matches!(
        flow,
        RegistrationSetupFlow::LegacyTwoOwnerChangesWithControllerAsPriorOwner
    ) {
        REGISTRATION_CONTROLLER
    } else {
        PRIOR_OWNER
    };
    let final_registry_owner = if matches!(
        flow,
        RegistrationSetupFlow::LegacyTwoOwnerChangesWithWrappedOwner
            | RegistrationSetupFlow::ModernSingleOwnerChangeWithWrappedOwner
    ) {
        WRAPPED_OWNER
    } else {
        REGISTRANT
    };

    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let owner_change = |owner: &str| {
        Ok::<_, anyhow::Error>(
            v1_registry::NewOwner {
                node: parent_node,
                label: labelhash,
                owner: owner.parse()?,
            }
            .encode_log_data(),
        )
    };
    let registration = NameRegistered {
        name: label.to_owned(),
        label: labelhash,
        owner: REGISTRANT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let mut registry_admission = admission(32, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let mut controller_admission = admission(33, "legacy_registrar_controller");
    controller_admission.address = REGISTRATION_CONTROLLER.to_owned();
    controller_admission.contract_instance_id = Uuid::from_u128(333);
    let mut raw_logs = vec![raw_at_transaction(
        owner_change(prior_owner)?,
        1,
        0,
        0,
        REGISTRY,
    )];
    if matches!(
        flow,
        RegistrationSetupFlow::LegacyTwoOwnerChanges
            | RegistrationSetupFlow::LegacyTwoOwnerChangesWithControllerAsPriorOwner
            | RegistrationSetupFlow::LegacyTwoOwnerChangesWithWrappedOwner
    ) {
        raw_logs.push(raw_at_transaction(
            owner_change(REGISTRATION_CONTROLLER)?,
            1,
            1,
            0,
            REGISTRY,
        ));
    }
    let final_owner_log = i64::try_from(raw_logs.len() - 1)?;
    raw_logs.push(raw_at_transaction(
        owner_change(final_registry_owner)?,
        1,
        1,
        final_owner_log,
        REGISTRY,
    ));
    raw_logs.push(raw_at_transaction(
        registration,
        1,
        1,
        final_owner_log + 1,
        REGISTRATION_CONTROLLER,
    ));
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                32,
                "ens_v1_registry_l1",
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            manifest(
                33,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["legacy_registrar_controller"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![
            registry_admission,
            admission(33, "registrar"),
            controller_admission,
        ],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    })?;

    let registration_event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registration event");
    let registrar_resource = registration_event
        .resource_id
        .expect("registration resource");
    let prior_owner_events = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .filter(|event| event.after_state["subject"] == prior_owner)
        .collect::<Vec<_>>();
    assert_eq!(
        prior_owner_events.len(),
        2,
        "the prior owner's grant and registration-time revocation must both survive"
    );
    let prior_grant = prior_owner_events
        .iter()
        .find(|event| event.after_state["effective_powers"] == json!(["resource_control"]))
        .expect("prior owner grant");
    let prior_revocation = prior_owner_events
        .iter()
        .find(|event| event.after_state["effective_powers"] == json!([]))
        .expect("prior owner revocation");
    let registry_resource = prior_grant.resource_id.expect("registry-only resource");
    assert_ne!(registry_resource, registrar_resource);
    assert_eq!(prior_revocation.resource_id, Some(registry_resource));
    assert_eq!(
        prior_revocation.logical_name_id,
        registration_event.logical_name_id
    );
    assert_eq!(
        prior_revocation.after_state["revocation_source"]["authority_kind"],
        "registry_only"
    );
    assert!(
        prior_revocation
            .raw_fact_ref
            .get(seam::INTERPRETER_STATE_KEY)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|state_key| state_key.contains(&registry_resource.to_string())),
        "the prior owner's revocation state must remain keyed to its registry-only resource"
    );
    assert!(
        output
            .resources
            .iter()
            .any(|resource| resource.resource_id == registry_resource),
        "a registry-only resource referenced by earlier-epoch history must be retained"
    );
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "PermissionChanged"
                && event.after_state["subject"] == REGISTRANT
                && event.resource_id == Some(registrar_resource)
                && event.after_state["effective_powers"] == json!(["resource_control"])
        }),
        "the registrant's grant belongs to the new registrar authority epoch"
    );
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "PermissionChanged"
                && event.after_state["subject"] == final_registry_owner
                && event.resource_id == Some(registrar_resource)
                && event.after_state["effective_powers"] == json!(["resource_control"])
                && event.after_state["grant_source"]["authority_kind"] == "registrar"
        }),
        "the final registry owner's incoming grant belongs to the registrar authority epoch"
    );
    if final_registry_owner == WRAPPED_OWNER {
        assert!(
            output.normalized_events.iter().all(|event| {
                event.event_kind != "PermissionChanged"
                    || event.after_state["subject"] != WRAPPED_OWNER
                    || event.resource_id != Some(registry_resource)
                    || event.after_state["effective_powers"] != json!(["resource_control"])
            }),
            "the wrapped incoming owner's grant must not remain active on the closed registry-only resource"
        );
    }
    if prior_owner != REGISTRATION_CONTROLLER {
        assert!(
            output.normalized_events.iter().all(|event| {
                event.event_kind != "PermissionChanged"
                    || event.transaction_index != Some(1)
                    || event.after_state["subject"] != REGISTRATION_CONTROLLER
            }),
            "the transient registration controller's self grant and revoke must not survive"
        );
    }
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;
    Ok(())
}

#[test]
fn same_transaction_reconciliation_leaves_an_unrelated_registry_change_untouched()
-> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
    const REGISTRANT: &str = "0x0000000000000000000000000000000000000047";
    const UNRELATED_OWNER: &str = "0x0000000000000000000000000000000000000048";

    let target_labelhash = keccak256(b"alice");
    let unrelated_labelhash = keccak256(b"bob");
    let parent_node = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let owner_change = |label, owner: &str| {
        Ok::<_, anyhow::Error>(
            v1_registry::NewOwner {
                node: parent_node,
                label,
                owner: owner.parse()?,
            }
            .encode_log_data(),
        )
    };
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: target_labelhash,
        owner: REGISTRANT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let mut registry_admission = admission(34, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                34,
                "ens_v1_registry_l1",
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            manifest(
                35,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(35, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(owner_change(target_labelhash, CONTRACT)?, 1, 0, REGISTRY),
            raw_at(
                owner_change(unrelated_labelhash, UNRELATED_OWNER)?,
                1,
                1,
                REGISTRY,
            ),
            raw_at(owner_change(target_labelhash, REGISTRANT)?, 1, 2, REGISTRY),
            raw_at(registration, 1, 3, CONTRACT),
        ],
    })?;

    let registrar_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    let unrelated_events = output
        .normalized_events
        .iter()
        .filter(|event| event.source_family == "ens_v1_registry_l1")
        .filter(|event| event.log_index == Some(1))
        .collect::<Vec<_>>();
    assert!(!unrelated_events.is_empty());
    let unrelated_resource = unrelated_events[0]
        .resource_id
        .expect("unrelated registry resource");
    assert_ne!(unrelated_resource, registrar_resource);
    assert!(unrelated_events.iter().all(|event| {
        event.logical_name_id.is_none() && event.resource_id == Some(unrelated_resource)
    }));
    assert!(unrelated_events.iter().all(|event| {
        event
            .after_state
            .get("authority_kind")
            .is_none_or(|kind| kind == "registry_only")
    }));
    assert!(unrelated_events.iter().all(|event| {
        event
            .after_state
            .get("grant_source")
            .and_then(|source| source.get("authority_kind"))
            .is_none_or(|kind| kind == "registry_only")
    }));
    assert!(
        output
            .resources
            .iter()
            .any(|resource| resource.resource_id == unrelated_resource)
    );
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;
    Ok(())
}

fn assert_same_transaction_controller_setup(
    namespace: &str,
    registry_family: &str,
    registrar_family: &str,
    suffix: &[&str],
) -> anyhow::Result<()> {
    let label = "alice";
    let labelhash = keccak256(label.as_bytes());
    let suffix = suffix
        .iter()
        .map(|label| (*label).to_owned())
        .collect::<Vec<_>>();
    let labels = std::iter::once(label.to_owned())
        .chain(suffix.iter().cloned())
        .collect::<Vec<_>>();
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let parent_node = super::common::namehash(&suffix).parse::<B256>()?;
    let registry_address = "0x0000000000000000000000000000000000000043";
    let resolver_address = "0x0000000000000000000000000000000000000044";
    let registrant_address = "0x0000000000000000000000000000000000000045";
    let registration = NameRegistered {
        name: label.to_owned(),
        label: labelhash,
        owner: registrant_address.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let transient_owner = v1_registry::NewOwner {
        node: parent_node,
        label: labelhash,
        owner: CONTRACT.parse()?,
    }
    .encode_log_data();
    let owner = v1_registry::NewOwner {
        node: parent_node,
        label: labelhash,
        owner: registrant_address.parse()?,
    }
    .encode_log_data();
    let resolver = v1_registry::NewResolver {
        node,
        resolver: resolver_address.parse()?,
    }
    .encode_log_data();
    let registry = manifest_with_events(
        30,
        namespace,
        registry_family,
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            ),
        ],
    );
    let mut registry_admission = admission(30, "registry");
    registry_admission.address = registry_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            registry,
            manifest_with_events(
                31,
                namespace,
                registrar_family,
                &[(
                    "NameRegistered",
                    "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                    &["registrar"],
                    &["RegistrationGranted"],
                )],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(31, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(transient_owner, 1, 0, registry_address),
            raw_at(resolver, 1, 1, registry_address),
            raw_at(owner, 1, 2, registry_address),
            raw_at(registration, 1, 3, CONTRACT),
        ],
    })?;
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;

    let registration_event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registration event");
    let registrar_resource = registration_event
        .resource_id
        .expect("registration resource");
    let registrar_authority_key = registration_event
        .after_state
        .get("authority_key")
        .and_then(serde_json::Value::as_str)
        .expect("registration authority key");
    let registry_permission_events = output
        .normalized_events
        .iter()
        .filter(|event| event.source_family == registry_family)
        .filter(|event| event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert!(!registry_permission_events.is_empty());
    assert!(
        registry_permission_events
            .iter()
            .all(|event| event.resource_id == Some(registrar_resource)),
        "same-transaction registry permission events must use the registrar resource: {registry_permission_events:#?}"
    );
    let resource_bearing_registry_events = output
        .normalized_events
        .iter()
        .filter(|event| event.source_family == registry_family)
        .filter(|event| event.resource_id.is_some())
        .collect::<Vec<_>>();
    assert!(!resource_bearing_registry_events.is_empty());
    assert!(
        resource_bearing_registry_events
            .iter()
            .all(|event| event.resource_id == Some(registrar_resource)),
        "every reconciled registry event must use the registrar resource"
    );
    let registry_only_resource_ids = output
        .resources
        .iter()
        .filter(|resource| resource.token_lineage_id.is_none())
        .map(|resource| resource.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        registry_only_resource_ids.len(),
        1,
        "the superseded registry-only resource is retained at its first derivation block: {:?}",
        output.resources
    );
    let registry_only_resource = registry_only_resource_ids
        .into_iter()
        .next()
        .expect("one registry-only resource");
    assert!(
        output
            .resources
            .iter()
            .filter(|resource| resource.resource_id == registry_only_resource)
            .all(|resource| resource.block_number == 1),
        "every emission of the retained registry-only resource anchors at its first derivation block"
    );
    let batch_resource_ids = output
        .resources
        .iter()
        .map(|resource| resource.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        batch_resource_ids,
        std::collections::BTreeSet::from([registrar_resource, registry_only_resource]),
        "the batch carries the registrar resource plus the retained superseded registry-only resource"
    );
    assert!(
        registry_permission_events
            .iter()
            .all(|event| event.log_index != Some(0)),
        "permissions derived from transient setup ownership must be discarded with that observation"
    );
    for event in &registry_permission_events {
        assert!(
            event
                .raw_fact_ref
                .get(seam::INTERPRETER_STATE_KEY)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|state_key| state_key.contains(&registrar_resource.to_string())),
            "retargeted event state must be keyed to the registrar resource"
        );
        let mut saw_authority_source = false;
        for field in ["grant_source", "revocation_source"] {
            let Some(source) = event
                .after_state
                .get(field)
                .and_then(serde_json::Value::as_object)
                .filter(|source| {
                    source.get("kind").and_then(serde_json::Value::as_str)
                        == Some("ens_v1_authority")
                })
            else {
                continue;
            };
            saw_authority_source = true;
            assert_eq!(
                source
                    .get("authority_kind")
                    .and_then(serde_json::Value::as_str),
                Some("registrar"),
                "retargeted permission provenance must identify the registrar authority"
            );
            assert_eq!(
                source
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str),
                Some(registrar_authority_key),
                "retargeted permission provenance must use the registration authority key"
            );
        }
        assert!(
            saw_authority_source,
            "registry permission event must retain its ENSv1 authority source"
        );
    }
    for event in resource_bearing_registry_events {
        assert!(
            event
                .raw_fact_ref
                .get(seam::INTERPRETER_STATE_KEY)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|state_key| state_key.contains(&registrar_resource.to_string())),
            "every reconciled event state must be keyed to the registrar resource"
        );
    }
    for kind in [
        "SubregistryChanged",
        "AuthorityTransferred",
        "ResolverChanged",
        "PermissionChanged",
    ] {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.event_kind == kind && event.resource_id == Some(registrar_resource)
            }),
            "missing linked {kind}"
        );
    }
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "AuthorityTransferred")
            .count(),
        1,
        "only the final same-transaction registry owner is authoritative"
    );
    Ok(())
}

#[test]
fn registry_owner_observation_mints_a_node_scoped_registry_authority() -> anyhow::Result<()> {
    let parent = B256::repeat_byte(0x61);
    let labelhash = keccak256(b"child");
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            57,
            "ens_v1_registry_l1",
            "NewOwner",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            &["registry"],
            &["SubregistryChanged", "AuthorityTransferred"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(57, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v1_registry::NewOwner {
            node: parent,
            label: labelhash,
            owner: "0x0000000000000000000000000000000000000061".parse()?,
        }
        .encode_log_data())],
    })?;

    let authority = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .and_then(|event| event.resource_id)
        .expect("registry-only authority resource");
    assert!(output.resources.iter().any(|resource| {
        resource.resource_id == authority && resource.token_lineage_id.is_none()
    }));
    Ok(())
}

#[test]
fn registry_self_owner_word_uses_zero_getter_view_without_grant() -> anyhow::Result<()> {
    let node = B256::repeat_byte(0x60);
    for (namespace, family, emitter) in [
        ("ens", "ens_v1_registry_l1", CONTRACT),
        ("basenames", "basenames_base_registry", BASENAMES_REGISTRY),
    ] {
        let mut registry_admission = admission(57, "registry");
        registry_admission.address = emitter.to_owned();
        let output = interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest_with_events(
                57,
                namespace,
                family,
                &[(
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry"],
                    &["AuthorityTransferred"],
                )],
            )],
            discovery_rules: Vec::new(),
            admissions: vec![registry_admission],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                v1_registry::Transfer {
                    node,
                    owner: emitter.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                emitter,
            )],
        })?;

        let transfer = output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == "AuthorityTransferred")
            .expect("registry-self write must retain ownership history");
        assert_eq!(transfer.namespace, namespace);
        assert_eq!(transfer.after_state["owner"], json!(emitter));
        assert_eq!(transfer.after_state["owner_getter"], json!(ZERO_ADDRESS));
        assert_eq!(
            transfer.after_state["owner_getter_reason"],
            json!("registry_self")
        );
        assert_eq!(transfer.after_state["authority_kind"], json!(null));
        assert!(
            output
                .normalized_events
                .iter()
                .all(|event| event.event_kind != "PermissionChanged"),
            "registry self must receive no owner-derived grant"
        );
    }
    Ok(())
}

#[test]
fn unchanged_owner_word_emits_when_the_getter_view_changes_with_the_registry() -> anyhow::Result<()>
{
    const FIRST_REGISTRY: &str = "0x0000000000000000000000000000000000000062";
    const SECOND_REGISTRY: &str = "0x0000000000000000000000000000000000000063";
    let node = B256::repeat_byte(0x62);
    let manifest = manifest(
        59,
        "ens_v1_registry_l1",
        "Transfer",
        "event Transfer(bytes32 indexed node, address owner)",
        &["registry"],
        &["AuthorityTransferred", "PermissionChanged"],
    );
    let mut first = admission(59, "registry");
    first.address = FIRST_REGISTRY.to_owned();
    let mut second = admission(59, "registry");
    second.address = SECOND_REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![first, second],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: [FIRST_REGISTRY, SECOND_REGISTRY]
            .into_iter()
            .enumerate()
            .map(|(index, emitter)| {
                raw_at(
                    v1_registry::Transfer {
                        node,
                        owner: SECOND_REGISTRY.parse().expect("fixture owner"),
                    }
                    .encode_log_data(),
                    i64::try_from(index + 1).expect("fixture block"),
                    0,
                    emitter,
                )
            })
            .collect(),
    })?;
    let transfers = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "AuthorityTransferred")
        .collect::<Vec<_>>();
    assert_eq!(transfers.len(), 2);
    assert_eq!(transfers[0].after_state["owner_getter"], SECOND_REGISTRY);
    assert_eq!(transfers[1].before_state["owner_getter"], SECOND_REGISTRY);
    assert_eq!(transfers[1].after_state["owner_getter"], ZERO_ADDRESS);
    assert_eq!(
        transfers[1].after_state["owner_getter_reason"],
        "registry_self"
    );
    Ok(())
}

#[test]
fn literal_zero_owner_word_records_zero_getter_reason() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            57,
            "ens_v1_registry_l1",
            "Transfer",
            "event Transfer(bytes32 indexed node, address owner)",
            &["registry"],
            &["AuthorityTransferred"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(57, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v1_registry::Transfer {
            node: B256::repeat_byte(0x61),
            owner: ZERO_ADDRESS.parse()?,
        }
        .encode_log_data())],
    })?;
    let transfer = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .expect("literal-zero write must retain ownership history");
    assert_eq!(transfer.after_state["owner"], json!(ZERO_ADDRESS));
    assert_eq!(transfer.after_state["owner_getter"], json!(ZERO_ADDRESS));
    assert_eq!(
        transfer.after_state["owner_getter_reason"],
        json!("literal_zero")
    );
    let anchor_resource_id = transfer
        .resource_id
        .expect("ownerless registry event retains its read resource");
    assert_eq!(
        output
            .resources
            .iter()
            .filter(|resource| resource.resource_id == anchor_resource_id)
            .count(),
        1,
        "the retained registry resource must be emitted once"
    );
    Ok(())
}

#[test]
fn zero_equivalent_registry_owner_restores_exactly_across_batches() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000064";
    const OWNER: &str = "0x0000000000000000000000000000000000000065";
    let node = B256::repeat_byte(0x64);
    let manifest = manifest_with_events(
        64,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry"],
                &["AuthorityTransferred", "PermissionChanged"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            ),
        ],
    );
    let mut registry_admission = admission(64, "registry");
    registry_admission.address = REGISTRY.to_owned();

    for zero_owner in [ZERO_ADDRESS, REGISTRY] {
        let (first, session) = interpret_test_batch_incremental(
            BatchInput {
                chain_id: CHAIN.to_owned(),
                manifests: vec![manifest.clone()],
                discovery_rules: Vec::new(),
                admissions: vec![registry_admission.clone()],
                prior_events: Vec::new(),
                blocks: Vec::new(),
                raw_logs: vec![
                    raw_at(
                        v1_registry::Transfer {
                            node,
                            owner: OWNER.parse()?,
                        }
                        .encode_log_data(),
                        1,
                        0,
                        REGISTRY,
                    ),
                    raw_at(
                        v1_registry::NewResolver {
                            node,
                            resolver: CONTRACT.parse()?,
                        }
                        .encode_log_data(),
                        1,
                        1,
                        REGISTRY,
                    ),
                ],
            },
            None,
        )?;
        let prior = first
            .normalized_events
            .iter()
            .map(prior_event)
            .collect::<Vec<_>>();
        let zero_log = raw_at(
            v1_registry::Transfer {
                node,
                owner: zero_owner.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        );
        let next_input = BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![registry_admission.clone()],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![zero_log.clone()],
        };
        let (live_output, live_session) =
            interpret_test_batch_incremental(next_input, Some(session))?;
        let restored_input = BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: Vec::new(),
            admissions: vec![registry_admission.clone()],
            prior_events: prior,
            blocks: Vec::new(),
            raw_logs: vec![zero_log],
        };
        let (restored_output, _) = interpret_test_batch_incremental(restored_input.clone(), None)?;
        let compacted = seam::fold_prior_events(
            restored_input.prior_events,
            &live_output.normalized_events,
            &[RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-2".to_owned(),
                block_number: 2,
                block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2),
                canonicality_state: "canonical".to_owned(),
            }],
        )?;
        let (_, restored_session) = interpret_test_batch_incremental(
            BatchInput {
                chain_id: CHAIN.to_owned(),
                manifests: vec![manifest.clone()],
                discovery_rules: Vec::new(),
                admissions: vec![registry_admission.clone()],
                prior_events: compacted,
                blocks: Vec::new(),
                raw_logs: Vec::new(),
            },
            None,
        )?;

        assert_eq!(live_output, restored_output, "zero owner {zero_owner}");
        assert!(!live_session.has_v1_registry_authority("ens", &format!("{node:#x}")));
        assert!(!restored_session.has_v1_registry_authority("ens", &format!("{node:#x}")));
        assert_eq!(
            live_session, restored_session,
            "zero owner {zero_owner} must forget the prior registry-direct authority in both live and restored state"
        );
    }
    Ok(())
}

#[test]
fn wrapper_fallback_transfer_before_expiry_replays_exactly_with_zero_getter() -> anyhow::Result<()>
{
    // BaseRegistrar rejects post-expiry transfers, and NameWrapper blocks .eth unwrapping during
    // grace, so no admitted raw-log sequence can drive the defensive expired-state convergence
    // branch. Its zero-getter guard is mutation-pinned directly in
    // `state_tests::zero_getter_blocks_stale_registry_authority_fallback_during_registrar_transfer`;
    // this adapter test covers the reachable wrapper fallback and exact cold replay for both zero
    // encodings. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L75 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L202-L221 @ ens_v1@91c966f)
    const REGISTRY: &str = "0x0000000000000000000000000000000000000066";
    const REGISTRAR: &str = "0x0000000000000000000000000000000000000069";
    const WRAPPER: &str = "0x000000000000000000000000000000000000006a";
    const OWNER: &str = "0x0000000000000000000000000000000000000067";
    const NEXT_OWNER: &str = "0x0000000000000000000000000000000000000068";
    // .eth wrapping always burns the parent-control and .eth marker fuses while leaving
    // CANNOT_UNWRAP clear when the caller requests no user fuses.
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10-L19 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1014 @ ens_v1@91c966f)
    const DOT_ETH_WRAPPER_FUSES: u32 = (1 << 16) | (1 << 17);
    const REGISTRAR_EXPIRY: i64 = 1_000_000;
    const WRAPPER_EXPIRY: u64 = REGISTRAR_EXPIRY as u64 + 90 * 24 * 60 * 60;
    const TRANSFER_OBSERVATION: i64 = REGISTRAR_EXPIRY - 1;
    let labelhash = keccak256(b"stale");
    let node = super::common::namehash(&["stale".to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let registry_manifest = manifest(
        66,
        "ens_v1_registry_l1",
        "Transfer",
        "event Transfer(bytes32 indexed node, address owner)",
        &["registry"],
        &["AuthorityTransferred", "PermissionChanged"],
    );
    let wrapper_manifest = manifest_with_events(
        67,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "AuthorityEpochChanged"],
            ),
        ],
    );
    let registrar_manifest = manifest(
        68,
        "ens_v1_registrar_l1",
        "Transfer",
        "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
        &["registrar"],
        &[
            "TokenControlTransferred",
            "AuthorityTransferred",
            "PermissionChanged",
            "SurfaceBound",
            "SurfaceUnbound",
            "AuthorityEpochChanged",
        ],
    );
    let mut registry_admission = admission(66, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let mut wrapper_admission = admission(67, "name_wrapper");
    wrapper_admission.address = WRAPPER.to_owned();
    let mut registrar_admission = admission(68, "registrar");
    registrar_admission.address = REGISTRAR.to_owned();
    let manifests = vec![registry_manifest, wrapper_manifest, registrar_manifest];
    let admissions = vec![registry_admission, wrapper_admission, registrar_admission];
    let block = |number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };

    for zero_owner in [ZERO_ADDRESS, REGISTRY] {
        let (first, session) = interpret_test_batch_incremental(
            BatchInput {
                chain_id: CHAIN.to_owned(),
                manifests: manifests.clone(),
                discovery_rules: Vec::new(),
                admissions: admissions.clone(),
                prior_events: Vec::new(),
                blocks: Vec::new(),
                raw_logs: vec![
                    raw_at(
                        v1_registry::Transfer {
                            node,
                            owner: OWNER.parse()?,
                        }
                        .encode_log_data(),
                        1,
                        0,
                        REGISTRY,
                    ),
                    raw_at(
                        NameWrapped {
                            node,
                            name: b"\x05stale\x03eth\0".to_vec().into(),
                            owner: OWNER.parse()?,
                            fuses: DOT_ETH_WRAPPER_FUSES,
                            expiry: WRAPPER_EXPIRY,
                        }
                        .encode_log_data(),
                        2,
                        0,
                        WRAPPER,
                    ),
                ],
            },
            None,
        )?;
        let (zeroed, session) = interpret_test_batch_incremental(
            BatchInput {
                chain_id: CHAIN.to_owned(),
                manifests: manifests.clone(),
                discovery_rules: Vec::new(),
                admissions: admissions.clone(),
                prior_events: Vec::new(),
                blocks: Vec::new(),
                raw_logs: vec![raw_at(
                    v1_registry::Transfer {
                        node,
                        owner: zero_owner.parse()?,
                    }
                    .encode_log_data(),
                    3,
                    0,
                    REGISTRY,
                )],
            },
            Some(session),
        )?;
        let unwrap = raw_at(
            NameUnwrapped {
                node,
                owner: OWNER.parse()?,
            }
            .encode_log_data(),
            TRANSFER_OBSERVATION,
            0,
            WRAPPER,
        );
        let transfer = raw_at(
            v1_registrar::Transfer {
                from: WRAPPER.parse()?,
                to: NEXT_OWNER.parse()?,
                tokenId: U256::from_be_slice(labelhash.as_slice()),
            }
            .encode_log_data(),
            TRANSFER_OBSERVATION,
            1,
            REGISTRAR,
        );
        let input = BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests.clone(),
            discovery_rules: Vec::new(),
            admissions: admissions.clone(),
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![unwrap.clone(), transfer.clone()],
        };
        let (live, _) = interpret_test_batch_incremental(input, Some(session))?;
        let prior =
            seam::fold_prior_events(Vec::new(), &first.normalized_events, &[block(1), block(2)])?;
        let prior = seam::fold_prior_events(prior, &zeroed.normalized_events, &[block(3)])?;
        let (redo, _) = interpret_test_batch_incremental(
            BatchInput {
                chain_id: CHAIN.to_owned(),
                manifests: manifests.clone(),
                discovery_rules: Vec::new(),
                admissions: admissions.clone(),
                prior_events: prior,
                blocks: Vec::new(),
                raw_logs: vec![unwrap, transfer],
            },
            None,
        )?;

        assert!(live.normalized_events.iter().any(|event| {
            event.event_kind == "TokenControlTransferred"
                && event.source_family == "ens_v1_registrar_l1"
        }));
        assert_eq!(
            live.normalized_events, redo.normalized_events,
            "zero owner {zero_owner}: live and redo event streams must match"
        );
        assert!(live.normalized_events.iter().all(|event| {
            !matches!(
                event.event_kind.as_str(),
                "AuthorityTransferred" | "SurfaceBound"
            ) || (event
                .after_state
                .get("owner")
                .and_then(serde_json::Value::as_str)
                != Some(OWNER)
                && event
                    .after_state
                    .get("authority_kind")
                    .and_then(serde_json::Value::as_str)
                    != Some("registry_only"))
        }));
    }
    Ok(())
}

#[test]
fn registry_old_self_getter_matches_admitted_contract_implementation() -> anyhow::Result<()> {
    for (registry, getter_is_zero) in [
        ("0x314159265dd8dbb310642f98f50c066173c1259b", false),
        ("0x94f523b8261b815b87effcf4d18e6abef18d6e4b", true),
    ] {
        let mut old = admission(58, "registry_old");
        old.address = registry.to_owned();
        let output = interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest(
                58,
                "ens_v1_registry_l1",
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry_old"],
                &["AuthorityTransferred", "PermissionChanged"],
            )],
            discovery_rules: Vec::new(),
            admissions: vec![old],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                v1_registry::Transfer {
                    node: B256::repeat_byte(0x6f),
                    owner: registry.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                registry,
            )],
        })?;
        let transfer = output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == "AuthorityTransferred")
            .expect("old-registry transfer");
        let has_grant = output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "PermissionChanged");
        if getter_is_zero {
            assert_eq!(transfer.after_state["owner_getter"], ZERO_ADDRESS);
            assert_eq!(transfer.after_state["owner_getter_reason"], "registry_self");
            assert!(!has_grant);
        } else {
            assert_eq!(transfer.after_state["owner_getter"], registry);
            assert!(transfer.after_state.get("owner_getter_reason").is_none());
            assert!(has_grant);
        }
    }
    Ok(())
}

#[test]
fn ownerless_registry_resolver_uses_retained_anchor_without_reopening_control() -> anyhow::Result<()>
{
    const REGISTRY: &str = "0x0000000000000000000000000000000000000070";
    const WRAPPER_ADDRESS: &str = "0x0000000000000000000000000000000000000071";
    const OWNER: &str = "0x0000000000000000000000000000000000000072";
    const FIRST_RESOLVER: &str = "0x0000000000000000000000000000000000000073";
    const SECOND_RESOLVER: &str = "0x0000000000000000000000000000000000000074";
    const ACTIVE_OWNER: &str = "0x0000000000000000000000000000000000000075";

    let labels = vec!["read".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let wrapper_manifest = manifest_with_events(
        91,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "PreimageObserved",
                ],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "AuthorityEpochChanged"],
            ),
        ],
    );
    let registry_manifest = manifest_with_events(
        92,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry"],
                &["AuthorityTransferred", "PermissionChanged"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    );
    let mut wrapper_admission = admission(91, "name_wrapper");
    wrapper_admission.address = WRAPPER_ADDRESS.to_owned();
    let mut registry_admission = admission(92, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let manifests = vec![wrapper_manifest, registry_manifest];
    let admissions = vec![wrapper_admission, registry_admission];

    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::Transfer {
                    node,
                    owner: ACTIVE_OWNER.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                NameWrapped {
                    node,
                    name: b"\x04read\x03eth\0".to_vec().into(),
                    owner: OWNER.parse()?,
                    fuses: 0,
                    expiry: 100,
                }
                .encode_log_data(),
                2,
                0,
                WRAPPER_ADDRESS,
            ),
            raw_at(
                v1_registry::NewResolver {
                    node,
                    resolver: FIRST_RESOLVER.parse()?,
                }
                .encode_log_data(),
                3,
                0,
                REGISTRY,
            ),
            raw_at(
                v1_registry::Transfer {
                    node,
                    owner: REGISTRY.parse()?,
                }
                .encode_log_data(),
                4,
                0,
                REGISTRY,
            ),
            raw_at(
                NameUnwrapped {
                    node,
                    owner: OWNER.parse()?,
                }
                .encode_log_data(),
                5,
                0,
                WRAPPER_ADDRESS,
            ),
        ],
    })?;
    let linked_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{node:#x}"));
    let wrapper_resource = first
        .surface_bindings
        .iter()
        .find(|binding| binding.block_number == 2)
        .map(|binding| binding.resource_id)
        .expect("wrapper control resource");
    let first_resolver_events = first
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged" && event.after_state["resolver"] == FIRST_RESOLVER
        })
        .collect::<Vec<_>>();
    let resolver_resources = first_resolver_events
        .iter()
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        resolver_resources,
        std::collections::BTreeSet::from([linked_resource, wrapper_resource]),
        "registry selection while wrapper control is live must retain both the control pointer and the independent registry read anchor"
    );
    assert_eq!(
        first_resolver_events
            .iter()
            .filter(|event| event
                .event_identity
                .contains(":ResolverChanged:registry-read:"))
            .count(),
        1,
        "the additional registry-resource row must carry the product-history suppression suffix"
    );
    let self_transfer = first
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.after_state["owner_getter_reason"] == "registry_self"
        })
        .expect("registry-self transfer");
    assert_eq!(self_transfer.resource_id, Some(linked_resource));
    assert_eq!(self_transfer.after_state["owner"], REGISTRY);
    assert_eq!(self_transfer.after_state["owner_getter"], ZERO_ADDRESS);
    assert_eq!(
        self_transfer.after_state["authority_kind"],
        json!("wrapper"),
        "registry-self transition: {:?}",
        self_transfer.after_state
    );
    let block_five = first
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(5))
        .collect::<Vec<_>>();
    assert!(
        block_five
            .iter()
            .any(|event| event.event_kind == "SurfaceUnbound"),
        "block-five events: {block_five:?}"
    );
    assert!(
        first.normalized_events.iter().all(|event| {
            !(event.event_kind == "SurfaceBound" && event.block_number == Some(5))
        })
    );
    assert!(first.normalized_events.iter().all(|event| {
        !(event.event_kind == "PermissionChanged"
            && event.after_state["subject"] == REGISTRY
            && event
                .after_state
                .get("effective_powers")
                .is_some_and(|powers| powers.as_array().is_some_and(|powers| !powers.is_empty())))
    }));

    let restored = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewResolver {
                node,
                resolver: SECOND_RESOLVER.parse()?,
            }
            .encode_log_data(),
            6,
            0,
            REGISTRY,
        )],
    })?;
    let replacement = restored
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("ownerless resolver replacement");
    assert_eq!(replacement.resource_id, Some(linked_resource));
    assert_eq!(replacement.after_state["resolver"], SECOND_RESOLVER);
    assert!(
        restored.surface_bindings.is_empty(),
        "read serving must not reopen a control binding"
    );
    assert!(restored.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "SurfaceBound" | "PermissionChanged"
        )
    }));
    Ok(())
}

#[test]
fn pre_surface_ownerless_resolver_is_unlinked_before_surface() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000075";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000076";
    let node = B256::repeat_byte(0x76);
    let mut registry_admission = admission(93, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            93,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry"],
                    &["AuthorityTransferred"],
                ),
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry"],
                    &["ResolverChanged"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::Transfer {
                    node,
                    owner: REGISTRY.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                v1_registry::NewResolver {
                    node,
                    resolver: RESOLVER.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                REGISTRY,
            ),
        ],
    })?;
    let pointer = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("resolver history remains normalized");
    assert_eq!(pointer.resource_id, None);
    assert_eq!(pointer.logical_name_id, None);
    assert!(output.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn surface_before_first_owner_links_a_later_ownerless_resolver() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000079";
    const RESOLVER: &str = "0x000000000000000000000000000000000000007a";
    let node = B256::repeat_byte(0x79);
    let logical_name_id = format!("ens:{node:#x}");
    let prior_surface = PriorEventInput {
        retained_state_key: format!("surface:{node:#x}"),
        chain_id: CHAIN.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.clone()),
        resource_id: None,
        event_kind: "PreimageObserved".to_owned(),
        source_family: "ens_v1_registrar_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(94),
        emitting_address: None,
        state_scope: Some(format!("surface:{node:#x}")),
        block_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
        after_state: json!({"source_event":"NameRegistered", "namehash":format!("{node:#x}")}),
    };
    let mut registry = admission(95, "registry");
    registry.address = REGISTRY.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            95,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry"],
                    &["AuthorityTransferred"],
                ),
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry"],
                    &["ResolverChanged"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![registry],
        prior_events: vec![prior_surface],
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::Transfer {
                    node,
                    owner: REGISTRY.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                REGISTRY,
            ),
            raw_at(
                v1_registry::NewResolver {
                    node,
                    resolver: RESOLVER.parse()?,
                }
                .encode_log_data(),
                3,
                0,
                REGISTRY,
            ),
        ],
    })?;
    let pointer = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("post-surface ownerless resolver");
    assert_eq!(pointer.logical_name_id.as_deref(), Some(&*logical_name_id));
    assert_eq!(
        pointer.resource_id,
        Some(super::common::stable_uuid(&format!(
            "resource:registry-only:{CHAIN}:{node:#x}"
        )))
    );
    assert!(output.surface_bindings.is_empty());
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "PermissionChanged")
    );
    Ok(())
}

#[test]
fn restored_surface_after_registry_owner_keeps_resolver_name_scope() -> anyhow::Result<()> {
    const REGISTRY: &str = "0x000000000000000000000000000000000000007b";
    const OWNER: &str = "0x000000000000000000000000000000000000007c";
    const RESOLVER: &str = "0x000000000000000000000000000000000000007d";
    const REGISTRAR_CONTROLLER: &str = "0x000000000000000000000000000000000000007e";
    let node = super::common::namehash(&["anchor".to_owned(), "eth".to_owned()]).parse::<B256>()?;
    let logical_name_id = format!("ens:{node:#x}");
    let registry_manifest = manifest_with_events(
        96,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry"],
                &["AuthorityTransferred", "PermissionChanged"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    );
    let surface_manifest = manifest(
        97,
        "ens_v1_registrar_l1",
        "NameRenewed",
        "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted", "PreimageObserved"],
    );
    let mut registry = admission(96, "registry");
    registry.address = REGISTRY.to_owned();
    let mut registrar_controller = admission(97, "registrar");
    registrar_controller.address = REGISTRAR_CONTROLLER.to_owned();
    let manifests = vec![registry_manifest, surface_manifest];
    let admissions = vec![registry, registrar_controller];
    let (owner, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests.clone(),
            discovery_rules: Vec::new(),
            admissions: admissions.clone(),
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                v1_registry::Transfer {
                    node,
                    owner: OWNER.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            )],
        },
        None,
    )?;
    let (surface, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests.clone(),
            discovery_rules: Vec::new(),
            admissions: admissions.clone(),
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw_at(
                NameRenewed {
                    name: "anchor".to_owned(),
                    label: keccak256(b"anchor"),
                    expires: U256::from(42),
                }
                .encode_log_data(),
                2,
                0,
                REGISTRAR_CONTROLLER,
            )],
        },
        Some(session),
    )?;
    assert!(
        surface.normalized_events.iter().any(|event| {
            event.event_kind == "PreimageObserved"
                && event.logical_name_id.as_deref() == Some(&*logical_name_id)
        }),
        "surface events: {:#?}",
        surface.normalized_events
    );
    let resolver_log = raw_at(
        v1_registry::NewResolver {
            node,
            resolver: RESOLVER.parse()?,
        }
        .encode_log_data(),
        3,
        0,
        REGISTRY,
    );
    let (live, _) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests.clone(),
            discovery_rules: Vec::new(),
            admissions: admissions.clone(),
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![resolver_log.clone()],
        },
        Some(session),
    )?;
    let prior = seam::fold_prior_events(
        Vec::new(),
        &owner.normalized_events,
        &[RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
        }],
    )?;
    let prior = seam::fold_prior_events(
        prior,
        &surface.normalized_events,
        &[RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-2".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2),
            canonicality_state: "canonical".to_owned(),
        }],
    )?;
    let (restored, _) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests,
            discovery_rules: Vec::new(),
            admissions,
            prior_events: prior,
            blocks: Vec::new(),
            raw_logs: vec![resolver_log],
        },
        None,
    )?;
    assert_eq!(live.normalized_events, restored.normalized_events);
    let pointer = live
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("post-surface resolver pointer");
    assert_eq!(pointer.logical_name_id.as_deref(), Some(&*logical_name_id));
    Ok(())
}

#[test]
fn migrated_v1_node_ignores_later_old_registry_updates_across_batches() -> anyhow::Result<()> {
    const CURRENT: &str = "0x0000000000000000000000000000000000000062";
    const OLD: &str = "0x0000000000000000000000000000000000000063";
    let parent = B256::repeat_byte(0x62);
    let labelhash = keccak256(b"migrated");
    let manifest = manifest_with_events(
        58,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry", "registry_old"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            (
                "NewTTL",
                "event NewTTL(bytes32 indexed node, uint64 ttl)",
                &["registry", "registry_old"],
                &["AuthorityEpochChanged"],
            ),
        ],
    );
    let mut current = admission(58, "registry");
    current.address = CURRENT.to_owned();
    let mut current_root = current.clone();
    current_root.role = Some("ENSRegistry".to_owned());
    let mut old = admission(58, "registry_old");
    old.address = OLD.to_owned();
    old.contract_instance_id = Uuid::from_u128(580);
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![current_root.clone(), current.clone(), old.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewOwner {
                node: parent,
                label: labelhash,
                owner: "0x0000000000000000000000000000000000000064".parse()?,
            }
            .encode_log_data(),
            1,
            0,
            CURRENT,
        )],
    })?;
    assert!(first.normalized_events.iter().any(|event| {
        event.after_state["source_event"] == "NewOwner"
            && event.after_state["emitter_role"] == "registry"
    }));
    let child = {
        let mut input = [0u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(labelhash.as_slice());
        B256::from(keccak256(input))
    };
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![current_root, current, old],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent,
                    label: labelhash,
                    owner: "0x0000000000000000000000000000000000000065".parse()?,
                }
                .encode_log_data(),
                2,
                0,
                OLD,
            ),
            raw_at(
                v1_registry::NewTTL {
                    node: child,
                    ttl: 60,
                }
                .encode_log_data(),
                2,
                1,
                OLD,
            ),
        ],
    })?;

    assert!(second.normalized_events.iter().all(|event| {
        !matches!(
            event.after_state["source_event"].as_str(),
            Some("NewOwner" | "NewTTL")
        )
    }));
    Ok(())
}

const LLL_OLD_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
// Mainnet block 3,800,374, tx 0x96f71a1980e1b33ba7a67a56007bafdc513f5c584270e9aec14efbb7527e5fc2
// log 23: the caller passed the node itself as the resolver argument and the 2017 LLL registry
// stored and logged the argument word without masking it to the declared address type (#361).
const LLL_UNMASKED_WORD: &str =
    "0x93fc662b9e04a687b1e92785d98285f7567f8792844f9b90060cf135648dfc80";
const LLL_UNMASKED_WORD_ADDRESS: &str = "0xd98285f7567f8792844f9b90060cf135648dfc80";
const LLL_NEW_RESOLVER_TOPIC0: &str =
    "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0";
// Mainnet block 5,648,711, tx 0x6ec35801f33b0567db124139383e98866f1a650f9f732a28a81bb6e680fc0681
// log 174: the caller passed the ASCII hex string "c0684cb53c168148eaa013c38d1c0f39" as the
// owner argument and the LLL registry logged it unmasked (#361 census: 2 dirty NewOwner logs).
const LLL_UNMASKED_OWNER_WORD_ASCII: &str =
    "0x6330363834636235336331363831343865616130313363333864316330663339";
const LLL_UNMASKED_OWNER_WORD_ASCII_LOW20: &str = "0x3831343865616130313363333864316330663339";
// Mainnet block 7,460,548, tx 0x4e7397f41323fc05b48319e9aed64507140c71229af84fe97a39f5a49117484e
// log 59: the second and last dirty NewOwner word chain-wide.
const LLL_UNMASKED_OWNER_WORD: &str =
    "0x32d5c6d3dd27313d9a3280b8b535d797a3e2309d85ab3d522a52d1bae889c843";
const LLL_UNMASKED_OWNER_WORD_LOW20: &str = "0xb535d797a3e2309d85ab3d522a52d1bae889c843";
const LLL_NEW_OWNER_TOPIC0: &str =
    "0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82";
// Mainnet block 4,003,999, tx 0xafb6d7ac92f6beb3f3df6a9bbfaeb2f99b9db020ee69199af95f2e8ea5253467
// log 18: the old registry's only NewTTL with an unmasked word — a 20-byte value sits in the
// declared uint64 slot (#361 census: 1 of 7 NewTTL logs chain-wide).
const LLL_UNMASKED_TTL_WORD: &str =
    "0x0000000000000000000000005ffc014343cd971b7eb70732021e26c35b744cc4";
const LLL_NEW_TTL_TOPIC0: &str =
    "0x1d4f9bbfc9cab89d66e1a1562f2233ccbf1308cb4f63de2ead5787adddb8fa68";
const LLL_NEW_TTL_NODE: &str = "0xfac963bc058381048d445ea4f8cadd6b70b057034ee1096cec5d49562735b446";
const BASENAMES_REGISTRY: &str = "0xb94704422c2a1e396835a571837aa5ae53285a95";

fn lll_old_registry_input(raw_logs: Vec<RawLogInput>) -> BatchInput {
    let mut old = admission(71, "registry_old");
    old.address = LLL_OLD_REGISTRY.to_owned();
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            71,
            "ens",
            "ens_v1_registry_l1",
            &[
                (
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry", "registry_old"],
                    &["SubregistryChanged", "AuthorityTransferred"],
                ),
                (
                    "NewResolver",
                    "event NewResolver(bytes32 indexed node, address resolver)",
                    &["registry", "registry_old"],
                    &["ResolverChanged"],
                ),
                (
                    "Transfer",
                    "event Transfer(bytes32 indexed node, address owner)",
                    &["registry", "registry_old"],
                    &["AuthorityTransferred"],
                ),
                (
                    "NewTTL",
                    "event NewTTL(bytes32 indexed node, uint64 ttl)",
                    &["registry", "registry_old"],
                    &[],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![old],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    }
}

fn lll_old_registry_raw(topics: Vec<String>, data: Vec<u8>) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: "0xb3b2587814e723ec627a79ac8a7a334321e8b665aa9d46e29068ff3216e60ee8".to_owned(),
        block_number: 3_800_374,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(3_800_374),
        canonicality_state: "canonical".to_owned(),
        transaction_hash: "0x96f71a1980e1b33ba7a67a56007bafdc513f5c584270e9aec14efbb7527e5fc2"
            .to_owned(),
        transaction_index: 34,
        log_index: 23,
        emitting_address: LLL_OLD_REGISTRY.to_owned(),
        topics,
        data,
    }
}

#[test]
fn lll_era_unmasked_resolver_word_decodes_as_its_low_20_bytes() -> anyhow::Result<()> {
    let output = interpret_test_batch(lll_old_registry_input(vec![lll_old_registry_raw(
        vec![
            LLL_NEW_RESOLVER_TOPIC0.to_owned(),
            LLL_UNMASKED_WORD.to_owned(),
        ],
        hex::decode(LLL_UNMASKED_WORD)?,
    )]))?;
    let resolver_changed = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "NewResolver"
        })
        .expect("unmasked resolver word must still yield the resolver change");
    assert_eq!(
        resolver_changed.after_state["node"],
        json!(LLL_UNMASKED_WORD)
    );
    assert_eq!(
        resolver_changed.after_state["resolver"],
        json!(LLL_UNMASKED_WORD_ADDRESS)
    );
    assert_eq!(
        resolver_changed.after_state["resolver_word_unmasked"],
        json!(true)
    );
    assert_eq!(
        resolver_changed.after_state["resolver_word_raw"],
        json!(LLL_UNMASKED_WORD)
    );
    Ok(())
}

#[test]
fn lll_era_unmasked_owner_words_are_recorded_without_authority() -> anyhow::Result<()> {
    // Both dirty NewOwner logs that exist chain-wide (#361 census), with their real bytes. The
    // masked low-20 value stays in the body as the read-equivalent owner, but it is an address no
    // caller can authenticate as: it must receive no authority and no permission grant.
    for (raw, expected_owner, raw_word) in [
        (
            RawLogInput {
                block_hash: "0x018c38ad118e456dc6b6fcc310490a4f60134171ded4ae783c45a362e821f3e9"
                    .to_owned(),
                block_number: 5_648_711,
                transaction_hash:
                    "0x6ec35801f33b0567db124139383e98866f1a650f9f732a28a81bb6e680fc0681".to_owned(),
                transaction_index: 102,
                log_index: 174,
                ..lll_old_registry_raw(
                    vec![
                        LLL_NEW_OWNER_TOPIC0.to_owned(),
                        "0x8352da3d0ebe15fd4bd7def280458b52cdb17c9c50ef26bed05f77a09a37033d"
                            .to_owned(),
                        "0x6630353636393265383962393531363132383865306565373939353535636238"
                            .to_owned(),
                    ],
                    hex::decode(LLL_UNMASKED_OWNER_WORD_ASCII)?,
                )
            },
            LLL_UNMASKED_OWNER_WORD_ASCII_LOW20,
            LLL_UNMASKED_OWNER_WORD_ASCII,
        ),
        (
            RawLogInput {
                block_hash: "0xf6010f4a2a9be5eb308d28dc2354c19e0c0fa73479bfca765fc762b682f7faf2"
                    .to_owned(),
                block_number: 7_460_548,
                transaction_hash:
                    "0x4e7397f41323fc05b48319e9aed64507140c71229af84fe97a39f5a49117484e".to_owned(),
                transaction_index: 57,
                log_index: 59,
                ..lll_old_registry_raw(
                    vec![
                        LLL_NEW_OWNER_TOPIC0.to_owned(),
                        "0x9b30bbebf69e7d4322e460170bffd882b7c81cddd3b5d6a6989b78e226657321"
                            .to_owned(),
                        "0x1000000000000000000000000000000000000000000000000000000000000000"
                            .to_owned(),
                    ],
                    hex::decode(LLL_UNMASKED_OWNER_WORD)?,
                )
            },
            LLL_UNMASKED_OWNER_WORD_LOW20,
            LLL_UNMASKED_OWNER_WORD,
        ),
    ] {
        let output = interpret_test_batch(lll_old_registry_input(vec![raw]))?;
        let subregistry_changed = output
            .normalized_events
            .iter()
            .find(|event| {
                event.event_kind == "SubregistryChanged"
                    && event.after_state["source_event"] == "NewOwner"
            })
            .expect("unmasked owner word must still yield the subregistry change");
        assert_eq!(
            subregistry_changed.after_state["owner"],
            json!(expected_owner)
        );
        assert_eq!(
            subregistry_changed.after_state["owner_word_unmasked"],
            json!(true)
        );
        assert_eq!(
            subregistry_changed.after_state["owner_word_raw"],
            json!(raw_word)
        );
        let authority_transferred = output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == "AuthorityTransferred")
            .expect("the dirty write closes authority like a zero-owner write");
        assert_eq!(authority_transferred.before_state, json!({"owner": null}));
        assert_eq!(
            authority_transferred.after_state["authority_kind"],
            json!(null)
        );
        assert_eq!(
            authority_transferred.after_state["authority_key"],
            json!(null)
        );
        assert!(
            output
                .normalized_events
                .iter()
                .all(|event| event.event_kind != "PermissionChanged"),
            "an unmasked owner word must not mint permission grants"
        );
        assert!(
            output
                .normalized_events
                .iter()
                .all(|event| event.resource_id.is_none()),
            "an unmasked owner word must not activate an authority resource"
        );
        assert!(output.resources.is_empty());
    }
    Ok(())
}

#[test]
fn lll_era_unmasked_transfer_word_is_recorded_without_authority() -> anyhow::Result<()> {
    // Transfer has zero dirty words chain-wide (#361 census), so unlike the NewOwner/
    // NewResolver/NewTTL fixtures this one legitimately stays synthetic.
    let node = B256::repeat_byte(0x22);
    let output = interpret_test_batch(lll_old_registry_input(vec![lll_old_registry_raw(
        vec![
            format!("{:#x}", v1_registry::Transfer::SIGNATURE_HASH),
            format!("{node:#x}"),
        ],
        hex::decode(LLL_UNMASKED_WORD)?,
    )]))?;
    let authority_transferred = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.after_state["source_event"] == "Transfer"
        })
        .expect("unmasked owner word must still yield the authority transfer");
    assert_eq!(
        authority_transferred.after_state["owner"],
        json!(LLL_UNMASKED_WORD_ADDRESS)
    );
    assert_eq!(
        authority_transferred.after_state["owner_word_unmasked"],
        json!(true)
    );
    assert_eq!(
        authority_transferred.after_state["owner_word_raw"],
        json!(LLL_UNMASKED_WORD)
    );
    assert_eq!(authority_transferred.before_state, json!({"owner": null}));
    assert_eq!(
        authority_transferred.after_state["authority_kind"],
        json!(null)
    );
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "PermissionChanged"),
        "an unmasked owner word must not mint permission grants"
    );
    assert!(output.resources.is_empty());
    Ok(())
}

#[test]
fn unmasked_owner_word_closes_a_prior_owner_grant_and_stays_forgotten() -> anyhow::Result<()> {
    // A dirty write over a node with a real prior owner: the prior grant closes like the
    // zero-owner arm, the masked tail gains nothing, and the registry-owner state forgets the
    // node — across a state restore — so a later clean write reports an empty explicit_before.
    let parent = B256::repeat_byte(0x51);
    let labelhash = B256::repeat_byte(0x52);
    let prior_owner = "0x0000000000000000000000000000000000000a11";
    let clean = v1_registry::NewOwner {
        node: parent,
        label: labelhash,
        owner: prior_owner.parse()?,
    }
    .encode_log_data();
    let first = interpret_test_batch(lll_old_registry_input(vec![RawLogInput {
        block_number: 10,
        ..lll_old_registry_raw(
            clean
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            clean.data.to_vec(),
        )
    }]))?;
    assert!(
        first.normalized_events.iter().any(|event| {
            event.event_kind == "PermissionChanged"
                && event.after_state["subject"] == json!(prior_owner)
                && event.after_state["effective_powers"] == json!(["resource_control"])
        }),
        "the clean write must establish the prior owner's grant"
    );

    let second = interpret_test_batch(BatchInput {
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        ..lll_old_registry_input(vec![RawLogInput {
            block_number: 11,
            ..lll_old_registry_raw(
                vec![
                    LLL_NEW_OWNER_TOPIC0.to_owned(),
                    format!("{parent:#x}"),
                    format!("{labelhash:#x}"),
                ],
                hex::decode(LLL_UNMASKED_OWNER_WORD)?,
            )
        }])
    })?;
    let authority_transferred = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .expect("the dirty write must close the prior authority");
    assert_eq!(
        authority_transferred.before_state,
        json!({"owner": prior_owner})
    );
    assert_eq!(
        authority_transferred.after_state["owner"],
        json!(LLL_UNMASKED_OWNER_WORD_LOW20)
    );
    assert_eq!(
        authority_transferred.after_state["authority_kind"],
        json!(null)
    );
    assert!(
        ["owner_getter", "owner_getter_reason"]
            .iter()
            .all(|field| authority_transferred.after_state.get(field).is_none())
    );
    let permission_changes = second
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert!(
        permission_changes
            .iter()
            .any(|event| event.after_state["subject"] == json!(prior_owner)
                && event.after_state["effective_powers"] == json!([])),
        "the prior owner's grant must close on the dirty write"
    );
    assert!(
        permission_changes
            .iter()
            .all(|event| { event.after_state["subject"] != json!(LLL_UNMASKED_OWNER_WORD_LOW20) }),
        "the masked tail must receive no grant"
    );

    let successor_owner = "0x0000000000000000000000000000000000000b22";
    let successor = v1_registry::NewOwner {
        node: parent,
        label: labelhash,
        owner: successor_owner.parse()?,
    }
    .encode_log_data();
    let third = interpret_test_batch(BatchInput {
        prior_events: first
            .normalized_events
            .iter()
            .chain(second.normalized_events.iter())
            .map(prior_event)
            .collect(),
        ..lll_old_registry_input(vec![RawLogInput {
            block_number: 12,
            ..lll_old_registry_raw(
                successor
                    .topics()
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect(),
                successor.data.to_vec(),
            )
        }])
    })?;
    let succession = third
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AuthorityTransferred")
        .expect("the clean successor write must transfer authority");
    assert_eq!(
        succession.before_state,
        json!({"owner": null, "owner_getter": null}),
        "the dirty write is forgotten rather than remembered as an owner, so the honest \
         predecessor of the clean successor is no authority holder"
    );
    Ok(())
}

#[test]
fn v1_registry_unmasked_address_word_at_non_word_lengths_stays_terminal() -> anyhow::Result<()> {
    // Only the unmasked-word-at-wrong-length case is terminal here: the strict decoder never
    // checks buffer exhaustion, so a clean word with trailing bytes decodes on the strict first
    // pass instead (#367 tracks that adapter-wide question).
    let word = hex::decode(LLL_UNMASKED_WORD)?;
    for data in [
        word[..31].to_vec(),
        word.iter().copied().chain([0u8]).collect(),
    ] {
        let error = interpret_test_batch(lll_old_registry_input(vec![lll_old_registry_raw(
            vec![
                LLL_NEW_RESOLVER_TOPIC0.to_owned(),
                LLL_UNMASKED_WORD.to_owned(),
            ],
            data,
        )]))
        .expect_err("an unmasked word whose data is not exactly one 32-byte word is never retried");
        assert!(
            format!("{error:#}").contains("NewResolver log is malformed"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn basenames_registry_unmasked_word_stays_terminal() -> anyhow::Result<()> {
    let mut registry_admission = admission(72, "registry");
    registry_admission.address = BASENAMES_REGISTRY.to_owned();
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            72,
            "basenames",
            "basenames_base_registry",
            &[(
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            )],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            emitting_address: BASENAMES_REGISTRY.to_owned(),
            ..lll_old_registry_raw(
                vec![
                    LLL_NEW_RESOLVER_TOPIC0.to_owned(),
                    LLL_UNMASKED_WORD.to_owned(),
                ],
                hex::decode(LLL_UNMASKED_WORD)?,
            )
        }],
    })
    .expect_err("basenames_base_registry shares the adapter but keeps the strict decode");
    assert!(
        format!("{error:#}").contains("NewResolver log is malformed"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn lll_era_unmasked_ttl_word_validates_as_its_low_8_bytes() -> anyhow::Result<()> {
    // Non-vacuity pairing: if the log silently failed to route,
    // v1_registry_unmasked_ttl_word_at_non_word_lengths_stays_terminal would fail.
    let output = interpret_test_batch(lll_old_registry_input(vec![RawLogInput {
        block_hash: "0x012fa0c0011ed099f81e9ea6abb7fe9b92d1a8b63e262603fb8b5f58b75d9efb".to_owned(),
        block_number: 4_003_999,
        transaction_hash: "0xafb6d7ac92f6beb3f3df6a9bbfaeb2f99b9db020ee69199af95f2e8ea5253467"
            .to_owned(),
        transaction_index: 27,
        log_index: 18,
        ..lll_old_registry_raw(
            vec![LLL_NEW_TTL_TOPIC0.to_owned(), LLL_NEW_TTL_NODE.to_owned()],
            hex::decode(LLL_UNMASKED_TTL_WORD)?,
        )
    }]))?;
    assert!(
        output.normalized_events.is_empty(),
        "NewTTL is decode-validation only and yields no normalized events: {:?}",
        output.normalized_events
    );
    Ok(())
}

#[test]
fn v1_registry_unmasked_ttl_word_at_non_word_lengths_stays_terminal() -> anyhow::Result<()> {
    let word = hex::decode(LLL_UNMASKED_TTL_WORD)?;
    for data in [
        word[..31].to_vec(),
        word.iter().copied().chain([0u8]).collect(),
    ] {
        let error = interpret_test_batch(lll_old_registry_input(vec![lll_old_registry_raw(
            vec![LLL_NEW_TTL_TOPIC0.to_owned(), LLL_NEW_TTL_NODE.to_owned()],
            data,
        )]))
        .expect_err("an unmasked word whose data is not exactly one 32-byte word is never retried");
        assert!(
            format!("{error:#}").contains("NewTTL log is malformed"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn basenames_registry_unmasked_ttl_word_stays_terminal() -> anyhow::Result<()> {
    let mut registry_admission = admission(73, "registry");
    registry_admission.address = BASENAMES_REGISTRY.to_owned();
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            73,
            "basenames",
            "basenames_base_registry",
            &[(
                "NewTTL",
                "event NewTTL(bytes32 indexed node, uint64 ttl)",
                &["registry"],
                &[],
            )],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            emitting_address: BASENAMES_REGISTRY.to_owned(),
            ..lll_old_registry_raw(
                vec![LLL_NEW_TTL_TOPIC0.to_owned(), LLL_NEW_TTL_NODE.to_owned()],
                hex::decode(LLL_UNMASKED_TTL_WORD)?,
            )
        }],
    })
    .expect_err("basenames_base_registry shares the adapter but keeps the strict decode");
    assert!(
        format!("{error:#}").contains("NewTTL log is malformed"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn same_block_binding_transitions_follow_log_order() -> anyhow::Result<()> {
    let labels = vec!["alice".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let wrapper_address = "0x0000000000000000000000000000000000000043";
    let registration = NameRegistered {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let wrapped = NameWrapped {
        node,
        name: b"\x05alice\x03eth\0".to_vec().into(),
        owner: CONTRACT.parse()?,
        fuses: 1,
        expiry: 42,
    }
    .encode_log_data();
    let mut wrapper_admission = admission(33, "name_wrapper");
    wrapper_admission.address = wrapper_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                32,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            manifest(
                33,
                "ens_v1_wrapper_l1",
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                &["name_wrapper"],
                &[
                    "TokenControlTransferred",
                    "ExpiryChanged",
                    "PermissionScopeChanged",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(32, "registrar"), wrapper_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration, 1, 0, CONTRACT),
            raw_at(wrapped, 1, 1, wrapper_address),
        ],
    })?;

    assert_eq!(output.surface_bindings.len(), 2);
    assert!(output.surface_bindings[0].active_from < output.surface_bindings[1].active_from);
    assert!(output.binding_closures.iter().any(|closure| {
        closure.log_index == 1 && closure.active_to == output.surface_bindings[1].active_from
    }));
    Ok(())
}

#[test]
fn ens_v1_registry_and_match_all_resolver_reuse_the_registered_authority() -> anyhow::Result<()> {
    let label = "alice";
    let labels = vec![label.to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let registry_address = "0x0000000000000000000000000000000000000043";
    let resolver_address = "0x0000000000000000000000000000000000000044";
    let registration = NameRegistered {
        name: label.to_owned(),
        label: keccak256(label.as_bytes()),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let resolver_change = v1_registry::NewResolver {
        node,
        resolver: resolver_address.parse()?,
    }
    .encode_log_data();
    let record = resolver::AddrChanged {
        node,
        a: "0x0000000000000000000000000000000000000045".parse()?,
    }
    .encode_log_data();
    let mut registry_admission = admission(22, "registry");
    registry_admission.address = registry_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                21,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            manifest(
                22,
                "ens_v1_registry_l1",
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            ),
            manifest(
                23,
                "ens_v1_resolver_l1",
                "AddrChanged",
                "event AddrChanged(bytes32 indexed node, address a)",
                &[],
                &["RecordChanged"],
            ),
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(21, "registrar"), registry_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registration, 1, 0, CONTRACT),
            raw_at(resolver_change, 1, 1, registry_address),
            raw_at(record, 1, 2, resolver_address),
        ],
    })?;
    let resource_id = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registration resource");
    for kind in ["ResolverChanged", "PermissionChanged", "RecordChanged"] {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.event_kind == kind && event.resource_id == Some(resource_id)
            })
        );
    }
    Ok(())
}

#[test]
fn ens_v2_token_resource_unifies_registration_binding_and_permissions() -> anyhow::Result<()> {
    let label = "alice";
    let token_id = versioned_token(label, 1);
    let upstream_resource = U256::from(99);
    let registry = manifest_with_events(
        11,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &["registry"],
                &["PermissionChanged", "RootPermissionChanged"],
            ),
        ],
    );
    let registered = v2_registry::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        expiry: 42,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: upstream_resource,
    }
    .encode_log_data();
    let permission = EACRolesChanged {
        resource: upstream_resource,
        account: "0x0000000000000000000000000000000000000003".parse()?,
        oldRoleBitmap: U256::ZERO,
        newRoleBitmap: U256::from(1),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry],
        discovery_rules: Vec::new(),
        admissions: vec![admission(11, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(linked, 1, 1, CONTRACT),
            raw_at(permission, 1, 2, CONTRACT),
        ],
    })?;

    let linked_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("linked EAC resource");
    let permission_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged")
        .and_then(|event| event.resource_id)
        .expect("permission EAC resource");
    assert_eq!(linked_resource, permission_resource);
    assert_eq!(output.surface_bindings.len(), 1);
    assert_eq!(output.surface_bindings[0].resource_id, linked_resource);
    for kind in [
        "RegistrationGranted",
        "AuthorityTransferred",
        "ExpiryChanged",
    ] {
        assert!(
            output.normalized_events.iter().any(|event| {
                event.event_kind == kind && event.resource_id == Some(linked_resource)
            }),
            "TokenResource must complete the resource-bound {kind} transition"
        );
    }
    assert!(
        output
            .resources
            .iter()
            .filter(|resource| resource.resource_id == linked_resource)
            .all(|resource| resource.token_lineage_id.is_some())
    );
    Ok(())
}

#[test]
fn ens_v2_registrar_intent_reuses_the_registry_eac_resource_without_rebinding() -> anyhow::Result<()>
{
    let label = "alice";
    let token_id = versioned_token(label, 1);
    let registrar_address = "0x0000000000000000000000000000000000000055";
    let registry = manifest_with_events(
        17,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let registrar = manifest_with_events(
        18,
        "ens",
        "ens_v2_registrar_l1",
        &[(
            "NameRegistered",
            "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 indexed referrer, uint256 base, uint256 premium)",
            &["registrar"],
            &["RegistrarNameRegistered"],
        )],
    );
    let registered = v2_registry::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        expiry: 42,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: U256::from(99),
    }
    .encode_log_data();
    let intent = v2_registrar::NameRegistered {
        tokenId: token_id,
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        subregistry: Address::ZERO,
        resolver: Address::ZERO,
        duration: 100,
        paymentToken: Address::ZERO,
        referrer: B256::ZERO,
        base: U256::from(1),
        premium: U256::ZERO,
    }
    .encode_log_data();
    let mut registrar_admission = admission(18, "registrar");
    registrar_admission.address = registrar_address.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry, registrar],
        discovery_rules: Vec::new(),
        admissions: vec![admission(17, "registry"), registrar_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(linked, 1, 1, CONTRACT),
            raw_at(intent, 1, 2, registrar_address),
        ],
    })?;
    let resource = |kind: &str| {
        output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == kind)
            .and_then(|event| event.resource_id)
            .expect("linked normalized resource")
    };
    assert_eq!(
        resource("TokenResourceLinked"),
        resource("RegistrarNameRegistered")
    );
    assert_eq!(output.surface_bindings.len(), 1);
    Ok(())
}

#[test]
fn ens_v2_resource_and_lineage_survive_prior_state_and_token_regeneration() -> anyhow::Result<()> {
    let label = "alice";
    let old_token = versioned_token(label, 1);
    let new_token = versioned_token(label, 2);
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            19,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "LabelRegistered",
                    "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                    &["registry"],
                    &["RegistrationGranted"],
                ),
                (
                    "TokenResource",
                    "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                    &["registry"],
                    &["TokenResourceLinked"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(19, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: old_token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 42,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: old_token,
                    resource: U256::from(99),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let linked_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("linked resource");
    let prior_events = first
        .normalized_events
        .iter()
        .filter(|event| event.block_number.is_some())
        .map(|event| PriorEventInput {
            retained_state_key: seam::retained_prior_state_key(
                event
                    .raw_fact_ref
                    .get(seam::INTERPRETER_STATE_KEY)
                    .and_then(serde_json::Value::as_str),
                &event.event_identity,
            ),
            chain_id: event.chain_id.clone(),
            namespace: event.namespace.clone(),
            logical_name_id: event.logical_name_id.clone(),
            resource_id: event.resource_id,
            event_kind: event.event_kind.clone(),
            source_family: event.source_family.clone(),
            manifest_version: event.manifest_version,
            source_manifest_id: event.source_manifest_id,
            emitting_address: event
                .raw_fact_ref
                .get("emitting_address")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            state_scope: event
                .raw_fact_ref
                .get("state_scope")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            block_timestamp: event
                .block_number
                .map(|number| OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number)),
            after_state: event.after_state.clone(),
        })
        .collect();
    let regenerated = v2_registry::TokenRegenerated {
        oldTokenId: old_token,
        newTokenId: new_token,
    }
    .encode_log_data();
    let transferred = v2_registry::TransferSingle {
        operator: "0x0000000000000000000000000000000000000001".parse()?,
        from: "0x0000000000000000000000000000000000000001".parse()?,
        to: "0x0000000000000000000000000000000000000002".parse()?,
        id: new_token,
        value: U256::from(1),
    }
    .encode_log_data();
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            19,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "TokenRegenerated",
                    "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                    &["registry"],
                    &["TokenRegenerated"],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["registry"],
                    &["TokenControlTransferred"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(19, "registry")],
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(regenerated, 2, 0, CONTRACT),
            raw_at(transferred, 2, 1, CONTRACT),
        ],
    })?;
    for kind in ["TokenRegenerated", "TokenControlTransferred"] {
        assert_eq!(
            second
                .normalized_events
                .iter()
                .find(|event| event.event_kind == kind)
                .and_then(|event| event.resource_id),
            Some(linked_resource),
        );
    }
    assert!(
        second
            .resources
            .iter()
            .filter(|resource| resource.resource_id == linked_resource)
            .all(|resource| resource.token_lineage_id.is_some())
    );
    assert_eq!(
        second
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "SurfaceBound")
            .count(),
        0,
        "restoring a live v2 name must not synthesize another binding at a batch boundary"
    );
    Ok(())
}

#[test]
fn ens_v2_prior_state_re_registration_discards_the_displaced_token() -> anyhow::Result<()> {
    let label = "alice";
    let old_token = versioned_token(label, 1);
    let new_token = versioned_token(label, 2);
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let old_resolver: Address = "0x0000000000000000000000000000000000000051".parse()?;
    let old_subregistry: Address = "0x0000000000000000000000000000000000000052".parse()?;
    let new_resolver: Address = "0x0000000000000000000000000000000000000053".parse()?;
    let new_subregistry: Address = "0x0000000000000000000000000000000000000054".parse()?;
    let manifest = manifest_with_events(
        57,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
        ],
    );
    let rules = || {
        ["resolver", "subregistry"]
            .into_iter()
            .map(|edge_kind| DiscoveryRuleInput {
                manifest_id: 57,
                edge_kind: edge_kind.to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "protocol_event".to_owned(),
            })
            .collect()
    };
    let registration = |token_id: U256, expiry: u64, block_number: i64| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token_id,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry,
                sender,
            }
            .encode_log_data(),
            block_number,
            0,
            CONTRACT,
        )
    };
    let link = |token_id: U256, resource: u64, block_number: i64| {
        raw_at(
            v2_registry::TokenResource {
                tokenId: token_id,
                resource: U256::from(resource),
            }
            .encode_log_data(),
            block_number,
            1,
            CONTRACT,
        )
    };
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: vec![admission(57, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            registration(old_token, 7, 1),
            link(old_token, 99, 1),
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: old_token,
                    resolver: old_resolver,
                    sender,
                }
                .encode_log_data(),
                1,
                2,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: old_token,
                    subregistry: old_subregistry,
                    sender,
                }
                .encode_log_data(),
                1,
                3,
                CONTRACT,
            ),
        ],
    })?;
    let old_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("old resource");
    let expiry = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: vec![admission(57, "registry")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "version-bump-expiry".to_owned(),
            block_number: 7,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(7),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: vec![admission(57, "registry")],
        prior_events: first
            .normalized_events
            .iter()
            .chain(&expiry.normalized_events)
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![registration(new_token, 100, 8), link(new_token, 100, 8)],
    })?;
    let new_resource = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked")
        .and_then(|event| event.resource_id)
        .expect("new resource");
    assert_ne!(old_resource, new_resource);
    assert!(second.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "ResolverChanged" | "SubregistryChanged"
        ) || event.resource_id != Some(new_resource)
    }));

    let third = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: rules(),
        admissions: vec![admission(57, "registry")],
        prior_events: first
            .normalized_events
            .iter()
            .chain(&expiry.normalized_events)
            .chain(&second.normalized_events)
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: new_token,
                    resolver: new_resolver,
                    sender,
                }
                .encode_log_data(),
                9,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: new_token,
                    subregistry: new_subregistry,
                    sender,
                }
                .encode_log_data(),
                9,
                1,
                CONTRACT,
            ),
        ],
    })?;
    assert!(
        third
            .normalized_events
            .iter()
            .all(|event| event.resource_id != Some(old_resource))
    );
    assert!(
        third
            .resources
            .iter()
            .all(|resource| resource.resource_id != old_resource)
    );
    assert!(third.normalized_events.iter().any(|event| {
        event.event_kind == "ResolverChanged"
            && event.resource_id == Some(new_resource)
            && event.after_state["resolver"] == format!("{new_resolver:#x}")
    }));
    assert!(third.normalized_events.iter().any(|event| {
        event.event_kind == "SubregistryChanged"
            && event.resource_id == Some(new_resource)
            && event.after_state["subregistry"] == format!("{new_subregistry:#x}")
    }));
    Ok(())
}

#[test]
fn ens_v2_expiry_extension_orders_the_old_unbind_before_the_new_binding() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000054";
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let manifest = manifest_with_events(
        54,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &["registry"],
                &["ExpiryChanged", "RegistrationRenewed"],
            ),
        ],
    );
    let mut child_admission = admission(54, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = Uuid::from_u128(540);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(540));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 54,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(54, "registry"), child_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 7,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: parent_token,
                    resource: U256::from(99),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 100,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(100),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::ExpiryUpdated {
                    tokenId: parent_token,
                    newExpiry: 100,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                7,
                0,
                CONTRACT,
            ),
        ],
    })?;

    let boundary_kinds = output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(7))
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    let unbound = boundary_kinds
        .iter()
        .position(|kind| *kind == "SurfaceUnbound")
        .expect("expired surface is unbound before renewal");
    let rebound = boundary_kinds
        .iter()
        .rposition(|kind| *kind == "SurfaceBound")
        .expect("extended registration restores the surface");
    assert!(
        unbound < rebound,
        "the final topology state must remain bound"
    );
    Ok(())
}

#[test]
fn ens_v2_parent_expiry_retracts_descendant_at_the_block_boundary() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000059";
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let manifest = manifest_with_events(
        59,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &["registry"],
                &["ExpiryChanged", "RegistrationRenewed"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["registry"],
                &["RegistrationReleased"],
            ),
        ],
    );
    let mut child_admission = admission(59, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = Uuid::from_u128(590);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(590));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 7,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner,
                    expiry: 100,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(100),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
        ],
    })?;
    let leaf = first
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name == "leaf.sub.eth")
        .expect("descendant surface before parent expiry");
    assert!(
        first
            .surface_bindings
            .iter()
            .any(|binding| binding.logical_name_id == leaf.logical_name_id)
    );

    let boundary = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission.clone()],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "expiry-boundary".to_owned(),
            block_number: 7,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(7),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    assert!(
        boundary
            .binding_closures
            .iter()
            .any(|closure| closure.logical_name_id == leaf.logical_name_id)
    );
    for kind in ["SurfaceUnbound", "RegistrationReleased"] {
        assert!(boundary.normalized_events.iter().any(|event| {
            event.event_kind == kind
                && event.logical_name_id.as_deref() == Some(&leaf.logical_name_id)
                && event.block_number == Some(7)
                && event.transaction_index.is_none()
        }));
    }
    let prior_events = first
        .normalized_events
        .iter()
        .chain(&boundary.normalized_events)
        .map(prior_event)
        .collect::<Vec<_>>();
    let late_unregister = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission.clone()],
        prior_events: prior_events.clone(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::LabelUnregistered {
                tokenId: child_token,
                sender,
            }
            .encode_log_data(),
            8,
            0,
            CHILD,
        )],
    })?;
    assert!(late_unregister.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationReleased"
            && event.logical_name_id.as_deref() == Some(&leaf.logical_name_id)
            && event.transaction_hash.is_some()
            && event.log_index == Some(0)
    }));
    let restored = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 59,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(59, "registry"), child_admission],
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::ExpiryUpdated {
                tokenId: parent_token,
                newExpiry: 100,
                sender,
            }
            .encode_log_data(),
            8,
            0,
            CONTRACT,
        )],
    })?;
    assert!(restored.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id.as_deref() == Some(&leaf.logical_name_id)
    }));
    Ok(())
}

#[test]
#[rustfmt::skip]
fn shadow_only_v2_descendant_expiry_is_a_non_binding_boundary() -> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000068";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let parent_token = versioned_token("sub", 1);
    let raw_label = b"a\0b".to_vec();
    let child_token = versioned_token_bytes(&raw_label, 1);
    let manifest = manifest_with_events(
        68,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            ("LabelReserved", "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)", &["registry"], &["RegistrationReserved"]),
            ("TokenResource", "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)", &["registry"], &["TokenResourceLinked"]),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
            ("ExpiryUpdated", "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)", &["registry"], &["ExpiryChanged", "RegistrationRenewed"]),
        ],
    );
    let mut child_admission = admission(68, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let rules = || {
        vec![DiscoveryRuleInput {
            manifest_id: 68,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }]
    };
    let admissions = || vec![admission(68, "registry"), child_admission.clone()];
    let hostile_registration = with_topic0(
        raw_v2_registry::RawLabelRegistered {
            tokenId: child_token,
            labelHash: keccak256(&raw_label),
            label: raw_label.clone().into(),
            owner,
            expiry: 100,
            sender,
        }
        .encode_log_data(),
        v2_registry::LabelRegistered::SIGNATURE_HASH,
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 7,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(hostile_registration, 2, 0, CHILD),
            raw_at(v2_registry::TokenResource { tokenId: child_token, resource: U256::from(68) }.encode_log_data(), 2, 1, CHILD),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: CONTRACT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CHILD,
            ),
            raw_at(with_topic0(raw_v2_registry::RawLabelRegistered { tokenId: versioned_token_bytes(b"c\0d", 1), labelHash: keccak256(b"c\0d"), label: b"c\0d".to_vec().into(), owner, expiry: 100, sender }.encode_log_data(), v2_registry::LabelRegistered::SIGNATURE_HASH), 5, 0, CHILD),
            raw_at(v2_registry::LabelReserved { tokenId: versioned_token_bytes(b"e\0f", 1), labelHash: keccak256(b"e\0f"), label: "e\0f".to_owned(), expiry: 100, sender }.encode_log_data(), 6, 0, CHILD),
        ],
    })?;
    let shadow_namehash = super::common::namehash_raw(
        [raw_label.as_slice(), b"sub".as_slice(), b"eth".as_slice()].into_iter(),
    );
    assert_shadow_output(&first, &shadow_namehash, &raw_label, None);

    let (boundary, session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest.clone()],
            discovery_rules: rules(),
            admissions: admissions(),
            prior_events: first.normalized_events.iter().map(prior_event).collect(),
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "shadow-expiry-boundary".to_owned(),
                block_number: 7,
                block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(7),
                canonicality_state: "canonical".to_owned(),
            }],
            raw_logs: Vec::new(),
        },
        None,
    )?;
    let shadow_id = format!("ens:{shadow_namehash}");
    let direct_shadow_id = format!("ens:{}", super::common::namehash_raw([b"c\0d".as_slice(), b"sub".as_slice(), b"eth".as_slice()].into_iter())); let reserved_shadow_id = format!("ens:{}", super::common::namehash_raw([b"e\0f".as_slice(), b"sub".as_slice(), b"eth".as_slice()].into_iter()));
    assert_eq!(first.normalized_events.iter().filter(|event| event.logical_name_id.as_deref() == Some(direct_shadow_id.as_str())).map(|event| event.event_kind.as_str()).collect::<Vec<_>>(), ["RegistrationGranted", "PreimageObserved"]);
    assert_eq!(first.normalized_events.iter().filter(|event| event.logical_name_id.as_deref() == Some(reserved_shadow_id.as_str())).map(|event| event.event_kind.as_str()).collect::<Vec<_>>(), ["RegistrationReserved", "PreimageObserved"]);
    assert_eq!(first.normalized_events.iter().find(|event| event.event_kind == "RegistrationReserved" && event.logical_name_id.as_deref() == Some(reserved_shadow_id.as_str())).and_then(|event| event.after_state.get("reservation_resource")).and_then(serde_json::Value::as_bool), Some(false));
    assert!(
        boundary
            .binding_closures
            .iter()
            .all(|closure| closure.logical_name_id != shadow_id)
    );
    let shadow_release = boundary
        .normalized_events
        .iter()
        .filter(|event| event.logical_name_id.as_deref() == Some(shadow_id.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(shadow_release.len(), 1);
    assert_eq!(shadow_release[0].event_kind, "RegistrationReleased");
    assert_eq!(shadow_release[0].before_state["status"], "registered");
    assert_eq!(
        shadow_release[0].after_state["terminal_reason"],
        "registry_name_binding_expired"
    );
    let (revived, _) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest],
            discovery_rules: rules(),
            admissions: admissions(),
            prior_events: Vec::new(),
            blocks: vec![test_block(8)],
            raw_logs: vec![raw_at(v2_registry::ExpiryUpdated { tokenId: parent_token, newExpiry: 100, sender }.encode_log_data(), 8, 0, CONTRACT)],
        },
        Some(session),
    )?;
    assert!(revived.normalized_events.iter().any(|event| event.event_kind == "RegistrationGranted" && event.logical_name_id.as_deref() == Some(shadow_id.as_str()) && event.resource_id.is_some()));
    assert!(revived.normalized_events.iter().any(|event| event.event_kind == "RegistrationReserved" && event.logical_name_id.as_deref() == Some(reserved_shadow_id.as_str()) && event.resource_id.is_none()));
    Ok(())
}

#[test]
fn ens_v2_unregister_emits_a_surface_binding_closure() -> anyhow::Result<()> {
    let label = "terminal";
    let token_id = versioned_token(label, 1);
    let manifest = manifest_with_events(
        12,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["registry"],
                &["RegistrationReleased"],
            ),
        ],
    );
    let registered = v2_registry::LabelRegistered {
        tokenId: token_id,
        labelHash: keccak256(label.as_bytes()),
        label: label.to_owned(),
        owner: "0x0000000000000000000000000000000000000001".parse()?,
        expiry: 42,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: U256::from(123),
    }
    .encode_log_data();
    let released = v2_registry::LabelUnregistered {
        tokenId: token_id,
        sender: "0x0000000000000000000000000000000000000002".parse()?,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(12, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registered, 1, 0, CONTRACT),
            raw_at(linked, 1, 1, CONTRACT),
            raw_at(released, 2, 0, CONTRACT),
        ],
    })?;

    let binding = output.surface_bindings.first().expect("opened binding");
    assert!(
        output
            .binding_closures
            .iter()
            .any(|closure| closure.logical_name_id == binding.logical_name_id),
        "LabelUnregistered must close the active surface binding"
    );
    Ok(())
}

#[test]
fn ens_v2_unregister_closes_attached_topology_with_null_boundaries() -> anyhow::Result<()> {
    let label = "terminal";
    let token_id = versioned_token(label, 1);
    let target = "0x0000000000000000000000000000000000000066";
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            20,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "LabelRegistered",
                    "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                    &["registry"],
                    &["RegistrationGranted"],
                ),
                (
                    "TokenResource",
                    "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                    &["registry"],
                    &["TokenResourceLinked"],
                ),
                (
                    "SubregistryUpdated",
                    "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                    &["registry"],
                    &["SubregistryChanged"],
                ),
                (
                    "LabelUnregistered",
                    "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                    &["registry"],
                    &["RegistrationReleased"],
                ),
            ],
        )],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 20,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: vec![admission(20, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: token_id,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner: "0x0000000000000000000000000000000000000001".parse()?,
                    expiry: 42,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token_id,
                    resource: U256::from(123),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: token_id,
                    subregistry: target.parse()?,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                1,
                2,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelUnregistered {
                    tokenId: token_id,
                    sender: "0x0000000000000000000000000000000000000002".parse()?,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
        ],
    })?;

    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceUnbound"
            && event.after_state["source_event"] == "LabelUnregistered"
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SubregistryChanged"
            && event.after_state["source_event"] == "LabelUnregistered"
            && event.after_state["subregistry"].is_null()
    }));
    Ok(())
}

#[test]
fn reserved_child_on_a_claim_path_keeps_reservation_scope_through_topology_legs()
-> anyhow::Result<()> {
    const CHILD: &str = "0x0000000000000000000000000000000000000068";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let parent_token = versioned_token("sub", 1);
    let kid_token = versioned_token("kid", 0);
    let ctrl_token = versioned_token("ctrl", 1);
    let manifest = manifest_with_events(
        68,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationReserved"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["registry"],
                &["RegistrationReleased"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut child_admission = admission(68, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let block = |number: i64| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };
    let claim = |parent: Address, block_number| {
        raw_at(
            v2_registry::ParentUpdated {
                parent,
                label: "sub".to_owned(),
                sender,
            }
            .encode_log_data(),
            block_number,
            0,
            CHILD,
        )
    };
    let reserve = |block_number| {
        raw_at(
            v2_registry::LabelReserved {
                tokenId: kid_token,
                labelHash: keccak256(b"kid"),
                label: "kid".to_owned(),
                expiry: 5_000,
                sender,
            }
            .encode_log_data(),
            block_number,
            0,
            CHILD,
        )
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 68,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(68, "registry"), child_admission],
        prior_events: Vec::new(),
        // The trailing empty block crosses the parent registration expiry so the claim path dies
        // at a block boundary rather than under a raw log.
        blocks: (1..=11).map(block).chain([block(1_001)]).collect(),
        raw_logs: vec![
            reserve(1),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: ctrl_token,
                    labelHash: keccak256(b"ctrl"),
                    label: "ctrl".to_owned(),
                    owner,
                    expiry: 5_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: ctrl_token,
                    resource: ctrl_token,
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CONTRACT,
            ),
            claim(CONTRACT.parse()?, 6),
            claim(Address::ZERO, 7),
            claim(CONTRACT.parse()?, 8),
            raw_at(
                v2_registry::LabelUnregistered {
                    tokenId: kid_token,
                    sender,
                }
                .encode_log_data(),
                9,
                0,
                CHILD,
            ),
            reserve(10),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: kid_token,
                    resource: kid_token,
                }
                .encode_log_data(),
                11,
                0,
                CHILD,
            ),
        ],
    })?;

    let name_id = |labels: &[&str]| {
        let labels = labels
            .iter()
            .map(|label| (*label).to_owned())
            .collect::<Vec<_>>();
        format!("ens:{}", super::common::namehash(&labels))
    };
    let kid = name_id(&["kid", "sub", "eth"]);
    let ctrl = name_id(&["ctrl", "sub", "eth"]);
    let sub = name_id(&["sub", "eth"]);

    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.raw_name == "kid.sub.eth" && surface.block_number == 6),
        "the mutual claim must materialize the reserved child surface"
    );
    assert!(
        output
            .surface_bindings
            .iter()
            .all(|binding| binding.logical_name_id != kid),
        "a reservation must never open a surface binding: {:#?}",
        output.surface_bindings
    );
    assert_eq!(
        output
            .surface_bindings
            .iter()
            .filter(|binding| binding.logical_name_id == ctrl)
            .map(|binding| binding.block_number)
            .collect::<Vec<_>>(),
        [6, 8],
        "the registered control binds on each claim materialization"
    );

    let lifecycle = |logical_name_id: &str| {
        output
            .normalized_events
            .iter()
            .filter(|event| {
                event.logical_name_id.as_deref() == Some(logical_name_id)
                    && matches!(
                        event.event_kind.as_str(),
                        "SurfaceBound" | "SurfaceUnbound" | "RegistrationReleased"
                    )
            })
            .map(|event| (event.block_number.unwrap(), event.event_kind.as_str()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        lifecycle(&kid),
        [
            (7, "RegistrationReleased"),
            (9, "RegistrationReleased"),
            (1_001, "RegistrationReleased"),
        ],
        "reservation lifecycle drift: {:#?}",
        output.normalized_events
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.block_number == Some(7)
            && event.event_kind == "RegistrationReleased"
            && event.logical_name_id.as_deref() == Some(kid.as_str())
            && event.before_state["status"] == "reserved"
            && event.after_state["terminal_reason"] == "registry_name_binding_changed"
    }));
    assert_eq!(
        lifecycle(&ctrl),
        [
            (6, "SurfaceBound"),
            (7, "SurfaceUnbound"),
            (7, "RegistrationReleased"),
            (8, "SurfaceBound"),
            (1_001, "SurfaceUnbound"),
            (1_001, "RegistrationReleased"),
        ],
        "registered control lifecycle drift: {:#?}",
        output.normalized_events
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.block_number == Some(7)
            && event.event_kind == "RegistrationReleased"
            && event.after_state["terminal_reason"] == "registry_name_binding_changed"
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.block_number == Some(1_001)
            && event.event_kind == "RegistrationReleased"
            && event.after_state["terminal_reason"] == "registry_name_binding_expired"
    }));
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "TokenResourceLinked"
                && event.block_number == Some(11)
                && event.logical_name_id.as_deref() == Some(kid.as_str())
                && event.resource_id.is_some()
        }),
        "the retained-resource confirmation must stay a plain link"
    );

    let mut closures = output
        .binding_closures
        .iter()
        .map(|closure| (closure.block_number, closure.logical_name_id.as_str()))
        .collect::<Vec<_>>();
    closures.sort();
    assert_eq!(
        closures,
        [
            (4, sub.as_str()),
            (6, ctrl.as_str()),
            (7, ctrl.as_str()),
            (8, ctrl.as_str()),
            (1_001, ctrl.as_str()),
        ],
        "binding-closure drift: {:#?}",
        output.binding_closures
    );
    Ok(())
}

#[test]
fn a_reservation_never_closes_another_holders_binding_on_the_same_name() -> anyhow::Result<()> {
    // Two admitted registries anchored to the same suffix both compute "kid.eth": one holds it
    // through a registered, resource-linked token; the other only reserves the label. Binding
    // closures are arm-wide per logical name, so a closure emitted for the reservation lands on
    // the *other* registry's live binding — and the election is unmoved (a reservation cannot win
    // a surface), so nothing reopens it. A reservation must leave no surface-binding effect.
    const RIVAL: &str = "0x0000000000000000000000000000000000000069";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let held = versioned_token("kid", 1);
    let reserved = versioned_token("kid", 0);
    let rival_grant = versioned_token("kid", 2);
    let manifest = manifest_with_events(
        69,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationReserved"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let mut rival_admission = admission(69, "registry");
    rival_admission.address = RIVAL.to_owned();
    rival_admission.contract_instance_id = super::common::contract_id(CHAIN, RIVAL);
    let batch = |rival_log: RawLogInput| BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(69, "registry"), rival_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: held,
                    labelHash: keccak256(b"kid"),
                    label: "kid".to_owned(),
                    owner,
                    expiry: 5_000,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: held,
                    resource: U256::from(0xaa),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            rival_log,
        ],
    };
    let output = interpret_test_batch(batch(raw_at(
        v2_registry::LabelReserved {
            tokenId: reserved,
            labelHash: keccak256(b"kid"),
            label: "kid".to_owned(),
            expiry: 5_000,
            sender,
        }
        .encode_log_data(),
        2,
        0,
        RIVAL,
    )))?;

    let kid = format!(
        "ens:{}",
        super::common::namehash(&["kid".to_owned(), "eth".to_owned()])
    );
    let held_binding = output
        .surface_bindings
        .iter()
        .find(|binding| binding.logical_name_id == kid)
        .expect("the registered, resource-linked holder binds the name")
        .clone();
    assert_eq!(
        output.surface_bindings.len(),
        1,
        "a reservation must never open a surface binding: {:#?}",
        output.surface_bindings
    );
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "RegistrationReserved" && event.block_number == Some(2)
        }),
        "the reservation must still normalize: {:#?}",
        output.normalized_events
    );
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|event| event.block_number == Some(2)
                && matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")),
        "a reservation must move no surface lifecycle: {:#?}",
        output.normalized_events
    );
    let persisted = simulate_binding_writer(&output);
    let held_row = persisted
        .iter()
        .find(|row| row.surface_binding_id == held_binding.surface_binding_id)
        .expect("the holder's binding is written");
    assert_eq!(
        held_row.active_to, None,
        "the reservation closed a different holder's live binding: {persisted:#?}"
    );
    assert!(
        output
            .binding_closures
            .iter()
            .all(|closure| closure.block_number != 2),
        "a reservation must emit no binding closure: {:#?}",
        output.binding_closures
    );

    // Control: a real registration on the same name keeps its stale-binding clear.
    let control = interpret_test_batch(batch(raw_at(
        v2_registry::LabelRegistered {
            tokenId: rival_grant,
            labelHash: keccak256(b"kid"),
            label: "kid".to_owned(),
            owner,
            expiry: 5_000,
            sender,
        }
        .encode_log_data(),
        2,
        0,
        RIVAL,
    )))?;
    assert!(
        control
            .binding_closures
            .iter()
            .any(|closure| closure.logical_name_id == kid && closure.block_number == 2),
        "a registration assert must still clear stale bindings on its name: {:#?}",
        control.binding_closures
    );
    Ok(())
}

#[test]
fn release_the_winner_reasserts_the_surviving_holders_persisted_binding() -> anyhow::Result<()> {
    assert_contested_surface_departure_reasserts_survivor(true)
}

#[test]
fn release_the_loser_reasserts_the_surviving_holders_persisted_binding() -> anyhow::Result<()> {
    assert_contested_surface_departure_reasserts_survivor(false)
}

#[test]
fn regeneration_collision_reasserts_a_displaced_names_surviving_coholder() -> anyhow::Result<()> {
    const RIVAL: &str = "0x0000000000000000000000000000000000000069";
    const MANIFEST_ID: i64 = 96;
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let alpha = versioned_token("alpha", 1);
    let beta = versioned_token("beta", 1);
    let manifest = manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                &[
                    "SurfaceUnbound",
                    "RegistrationReleased",
                    "TokenRegenerated",
                    "PreimageObserved",
                    "SurfaceBound",
                    "RegistrationGranted",
                    "AuthorityTransferred",
                    "ExpiryChanged",
                    "ResolverChanged",
                    "SubregistryChanged",
                ],
            ),
        ],
    );
    let mut rival_admission = admission(MANIFEST_ID, "registry");
    rival_admission.address = RIVAL.to_owned();
    rival_admission.contract_instance_id = super::common::contract_id(CHAIN, RIVAL);
    let register = |emitter: &str, token_id, label: &str, resource, block| {
        vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: token_id,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner,
                    expiry: 5_000,
                    sender,
                }
                .encode_log_data(),
                block,
                0,
                emitter,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token_id,
                    resource,
                }
                .encode_log_data(),
                block,
                1,
                emitter,
            ),
        ]
    };
    let mut logs = register(CONTRACT, alpha, "alpha", U256::from(0xaa), 1);
    logs.extend(register(RIVAL, alpha, "alpha", U256::from(0xbb), 2));
    logs.extend(register(CONTRACT, beta, "beta", U256::from(0xcc), 3));
    logs.push(raw_at(
        v2_registry::TokenRegenerated {
            oldTokenId: beta,
            newTokenId: alpha,
        }
        .encode_log_data(),
        4,
        0,
        CONTRACT,
    ));
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(MANIFEST_ID, "registry"), rival_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: logs,
    })?;
    let coholder_resource = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked" && event.block_number == Some(2))
        .and_then(|event| event.resource_id)
        .expect("the rival coholder links a resource");
    let reassertion = output
        .normalized_events
        .iter()
        .find(|event| {
            event.block_number == Some(4)
                && event.event_kind == "PreimageObserved"
                && event.after_state["source_event"] == "TokenRegenerated"
        })
        .expect("the collision reasserts the surviving coholder");
    assert_eq!(reassertion.after_state["arm_wide_binding_close"], true);
    assert_eq!(reassertion.after_state["closed_authority_arm"], "ens_v2");
    assert!(
        output.surface_bindings.iter().any(|binding| {
            binding.block_number == 4 && binding.resource_id == coholder_resource
        })
    );
    assert!(output.normalized_events.iter().all(|event| {
        event.block_number != Some(4)
            || event.resource_id != Some(coholder_resource)
            || !matches!(
                event.event_kind.as_str(),
                "SurfaceUnbound"
                    | "RegistrationReleased"
                    | "SurfaceBound"
                    | "RegistrationGranted"
                    | "AuthorityTransferred"
                    | "ExpiryChanged"
                    | "ResolverChanged"
                    | "SubregistryChanged"
            )
    }));
    let persisted = simulate_binding_writer(&output);
    assert_eq!(
        persisted
            .iter()
            .filter(|binding| {
                binding.live()
                    && binding.active_to.is_none()
                    && binding.logical_name_id
                        == reassertion
                            .logical_name_id
                            .clone()
                            .expect("reasserted name")
                    && binding.authority_arm == "ens_v2"
            })
            .count(),
        1
    );
    Ok(())
}

#[test]
fn regeneration_collision_closes_displaced_resolver_when_survivor_is_resolverless()
-> anyhow::Result<()> {
    const MANIFEST_ID: i64 = 97;
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let resolver: Address = "0x0000000000000000000000000000000000000011".parse()?;
    let old_token = versioned_token("alpha", 1);
    let new_token = versioned_token("alpha", 2);
    let manifest = manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                &[
                    "SurfaceUnbound",
                    "RegistrationReleased",
                    "TokenRegenerated",
                    "PreimageObserved",
                    "SurfaceBound",
                    "RegistrationGranted",
                    "AuthorityTransferred",
                    "ExpiryChanged",
                    "ResolverChanged",
                    "SubregistryChanged",
                ],
            ),
        ],
    );
    let register = |token_id, label: &str, block| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token_id,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: 5_000,
                sender,
            }
            .encode_log_data(),
            block,
            0,
            CONTRACT,
        )
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: MANIFEST_ID,
            edge_kind: "resolver".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "protocol_event".to_owned(),
        }],
        admissions: vec![admission(MANIFEST_ID, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            register(new_token, "alpha", 1),
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: new_token,
                    resolver,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            register(old_token, "beta", 3),
            raw_at(
                v2_registry::TokenRegenerated {
                    oldTokenId: old_token,
                    newTokenId: new_token,
                }
                .encode_log_data(),
                4,
                0,
                CONTRACT,
            ),
        ],
    })?;
    let mut masked = new_token.to_be_bytes::<32>();
    masked[28..].fill(0);
    let observation_key = format!("resolver:{}:{:#x}", CONTRACT, U256::from_be_bytes(masked));
    assert!(
        output.discovery_edge_closures.iter().any(|closure| {
            closure.active_to_block_number == 4
                && closure.edge_kind == "resolver"
                && closure.observation_key == observation_key
        }),
        "the resolverless survivor left the displaced resolver edge open: {:#?}",
        output.discovery_edge_closures
    );
    Ok(())
}

#[test]
fn regeneration_collision_does_not_release_pointer_only_displaced_state() -> anyhow::Result<()> {
    const MANIFEST_ID: i64 = 97;
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let resolver: Address = "0x0000000000000000000000000000000000000011".parse()?;
    let old_token = versioned_token("alpha", 1);
    let new_token = versioned_token("alpha", 2);
    let manifest = manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                &[
                    "SurfaceUnbound",
                    "RegistrationReleased",
                    "TokenRegenerated",
                    "PreimageObserved",
                    "SurfaceBound",
                    "RegistrationGranted",
                    "AuthorityTransferred",
                    "ExpiryChanged",
                    "ResolverChanged",
                    "SubregistryChanged",
                ],
            ),
        ],
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: MANIFEST_ID,
            edge_kind: "resolver".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "protocol_event".to_owned(),
        }],
        admissions: vec![admission(MANIFEST_ID, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: new_token,
                    resolver,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: old_token,
                    labelHash: keccak256(b"beta"),
                    label: "beta".to_owned(),
                    owner,
                    expiry: 5_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TokenRegenerated {
                    oldTokenId: old_token,
                    newTokenId: new_token,
                }
                .encode_log_data(),
                3,
                0,
                CONTRACT,
            ),
        ],
    })?;
    let releases = output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(3) && event.event_kind == "RegistrationReleased")
        .count();
    assert_eq!(
        releases, 0,
        "pointer-only displaced state published a fabricated registration release"
    );
    assert!(
        output.discovery_edge_closures.iter().any(|closure| {
            closure.active_to_block_number == 3 && closure.edge_kind == "resolver"
        }),
        "pointer-only displaced state must still close its resolver edge: {:#?}",
        output.discovery_edge_closures
    );
    Ok(())
}

#[test]
fn regenerated_resolver_alias_stays_live_while_another_successor_retains_it() -> anyhow::Result<()>
{
    const MANIFEST_ID: i64 = 97;
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let resolver_one: Address = "0x0000000000000000000000000000000000000011".parse()?;
    let resolver_two: Address = "0x0000000000000000000000000000000000000012".parse()?;
    let resolver_replacement: Address = "0x0000000000000000000000000000000000000013".parse()?;
    let alpha = versioned_token("alpha", 1);
    let beta = versioned_token("alpha", 2);
    let alpha_successor = versioned_token("gamma", 1);
    let beta_successor = versioned_token("delta", 1);
    let manifest = manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                &["TokenRegenerated"],
            ),
        ],
    );
    let registration = |token_id, label: &str, block| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token_id,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: 5_000,
                sender,
            }
            .encode_log_data(),
            block,
            0,
            CONTRACT,
        )
    };
    let resolver = |token_id, resolver, block| {
        raw_at(
            v2_registry::ResolverUpdated {
                tokenId: token_id,
                resolver,
                sender,
            }
            .encode_log_data(),
            block,
            0,
            CONTRACT,
        )
    };
    let regeneration = |old_token_id, new_token_id, block| {
        raw_at(
            v2_registry::TokenRegenerated {
                oldTokenId: old_token_id,
                newTokenId: new_token_id,
            }
            .encode_log_data(),
            block,
            0,
            CONTRACT,
        )
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: MANIFEST_ID,
            edge_kind: "resolver".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "protocol_event".to_owned(),
        }],
        admissions: vec![admission(MANIFEST_ID, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            registration(alpha, "alpha", 1),
            registration(beta, "beta", 2),
            resolver(alpha, resolver_one, 3),
            resolver(beta, resolver_two, 4),
            regeneration(alpha, alpha_successor, 5),
            regeneration(beta, beta_successor, 6),
            resolver(alpha_successor, resolver_replacement, 7),
        ],
    })?;
    let mut masked = alpha.to_be_bytes::<32>();
    masked[28..].fill(0);
    let observation_key = format!("resolver:{}:{:#x}", CONTRACT, U256::from_be_bytes(masked));
    assert!(
        output.discovery_edge_closures.iter().all(|closure| {
            closure.active_to_block_number != 7
                || closure.edge_kind != "resolver"
                || closure.observation_key != observation_key
        }),
        "the first successor retired resolver coverage retained by the second: {:#?}",
        output.discovery_edge_closures
    );
    Ok(())
}

#[test]
fn contested_loser_reregistration_emits_one_marked_name_observation() -> anyhow::Result<()> {
    const RIVAL: &str = "0x0000000000000000000000000000000000000069";
    const MANIFEST_ID: i64 = 96;
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let old_token = versioned_token("alpha", 1);
    let new_token = versioned_token("alpha", 2);
    let manifest = manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let mut rival_admission = admission(MANIFEST_ID, "registry");
    rival_admission.address = RIVAL.to_owned();
    rival_admission.contract_instance_id = super::common::contract_id(CHAIN, RIVAL);
    let register = |emitter: &str, token_id, block| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token_id,
                labelHash: keccak256(b"alpha"),
                label: "alpha".to_owned(),
                owner,
                expiry: 5_000,
                sender,
            }
            .encode_log_data(),
            block,
            0,
            emitter,
        )
    };
    let link = |emitter: &str, resource, block| {
        raw_at(
            v2_registry::TokenResource {
                tokenId: old_token,
                resource,
            }
            .encode_log_data(),
            block,
            1,
            emitter,
        )
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(MANIFEST_ID, "registry"), rival_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            register(CONTRACT, old_token, 1),
            link(CONTRACT, U256::from(0xaa), 1),
            register(RIVAL, old_token, 2),
            link(RIVAL, U256::from(0xbb), 2),
            register(CONTRACT, new_token, 3),
        ],
    })?;

    let observations = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(3)
                && event.event_kind == seam::PREIMAGE_OBSERVATION_EVENT_KIND
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observations.len(),
        1,
        "one raw log and name must not produce divergent normalized-event identities: {observations:#?}"
    );
    assert_eq!(
        observations[0]
            .after_state
            .get(seam::ARM_WIDE_BINDING_CLOSE_KEY)
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "the coalesced observation must retain the redo close marker"
    );
    Ok(())
}

#[test]
fn topology_departure_reasserts_the_survivor_across_replay_shapes() -> anyhow::Result<()> {
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059";
    let departure = raw_at(
        v2_registry::ParentUpdated {
            parent: Address::ZERO,
            label: "eth".to_owned(),
            sender: CONTRACT.parse()?,
        }
        .encode_log_data(),
        6,
        0,
        CLAIM_REGISTRY,
    );
    let setup_logs = contested_claim_path_logs(100)?;
    let mut full_logs = setup_logs.clone();
    full_logs.push(departure.clone());
    let full = interpret_test_batch(contested_claim_path_input(
        full_logs,
        Vec::new(),
        Vec::new(),
    )?)?;
    let survivor_resource = contested_claim_path_survivor(&full, 6);

    let (setup, session) = interpret_test_batch_incremental(
        contested_claim_path_input(setup_logs, Vec::new(), Vec::new())?,
        None,
    )?;
    let (incremental, _) = interpret_test_batch_incremental(
        contested_claim_path_input(vec![departure.clone()], Vec::new(), Vec::new())?,
        Some(session),
    )?;
    let prior = seam::fold_prior_events(
        Vec::new(),
        &setup.normalized_events,
        &(1..=5).map(test_block).collect::<Vec<_>>(),
    )?;
    let compacted = interpret_test_batch(contested_claim_path_input(
        vec![departure],
        prior,
        Vec::new(),
    )?)?;

    assert_eq!(incremental, compacted);
    assert_eq!(
        departure_identity_effects(&full, 6),
        departure_identity_effects(&incremental, 6)
    );
    assert_eq!(
        contested_claim_path_survivor(&incremental, 6),
        survivor_resource
    );
    Ok(())
}

#[test]
fn contested_expiry_reasserts_the_survivor_across_replay_shapes() -> anyhow::Result<()> {
    let setup_logs = contested_claim_path_logs(7)?;
    let full = interpret_test_batch(contested_claim_path_input(
        setup_logs.clone(),
        Vec::new(),
        (1..=7).map(test_block).collect(),
    )?)?;
    let survivor_resource = contested_claim_path_survivor(&full, 7);

    let (setup, session) = interpret_test_batch_incremental(
        contested_claim_path_input(setup_logs, Vec::new(), Vec::new())?,
        None,
    )?;
    let boundary_input = || contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(7)]);
    let (incremental, _) = interpret_test_batch_incremental(boundary_input()?, Some(session))?;
    let prior = seam::fold_prior_events(
        Vec::new(),
        &setup.normalized_events,
        &(1..=5).map(test_block).collect::<Vec<_>>(),
    )?;
    let compacted = interpret_test_batch(contested_claim_path_input(
        Vec::new(),
        prior,
        vec![test_block(7)],
    )?)?;

    assert_eq!(incremental, compacted);
    assert_eq!(
        departure_identity_effects(&full, 7),
        departure_identity_effects(&incremental, 7)
    );
    assert_eq!(
        contested_claim_path_survivor(&incremental, 7),
        survivor_resource
    );
    Ok(())
}

#[test]
#[rustfmt::skip]
fn immediate_named_reservation_expiry_replays_across_physical_batches() -> anyhow::Result<()> {
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059"; let sender: Address = "0x0000000000000000000000000000000000000002".parse()?; let token = versioned_token("alpha", 0);
    let mut prefix = contested_claim_path_logs(100)?; prefix.retain(|log| log.block_number >= 2 && log.block_number != 3);
    prefix.push(raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"alpha"), label: "alpha".to_owned(), expiry: 6, sender }.encode_log_data(), 6, 0, CLAIM_REGISTRY));
    let suffix = vec![
        raw_at(v2_registry::ParentUpdated { parent: Address::ZERO, label: "eth".to_owned(), sender }.encode_log_data(), 7, 0, CLAIM_REGISTRY),
        raw_at(v2_registry::ExpiryUpdated { tokenId: token, newExpiry: 10, sender }.encode_log_data(), 8, 0, CLAIM_REGISTRY),
    ];
    let mut all = prefix.clone(); all.extend(suffix.clone());
    let full = interpret_test_batch(contested_claim_path_input(all, Vec::new(), (2..=10).map(test_block).collect())?)?;
    let (first, session) = interpret_test_batch_incremental(contested_claim_path_input(prefix, Vec::new(), (2..=6).map(test_block).collect())?, None)?;
    let (split, _) = interpret_test_batch_incremental(contested_claim_path_input(suffix.clone(), Vec::new(), (7..=10).map(test_block).collect())?, Some(session))?;
    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &(2..=6).map(test_block).collect::<Vec<_>>())?;
    let restored = interpret_test_batch(contested_claim_path_input(suffix, prior, (7..=10).map(test_block).collect())?)?;
    let releases = |output: &BatchOutput| output.normalized_events.iter().filter(|event| event.block_number == Some(10) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired").count(); let renewals = |output: &BatchOutput| output.normalized_events.iter().filter(|event| event.block_number == Some(8) && event.event_kind == "RegistrationRenewed" && event.after_state["revived_from_expiry"] == true).count();
    assert_eq!((renewals(&full), renewals(&split), renewals(&restored)), (1, 1, 1)); assert_eq!((releases(&full), releases(&split), releases(&restored)), (1, 1, 1));
    assert_eq!(split, restored);
    Ok(())
}

#[test]
#[rustfmt::skip]
fn detached_formerly_named_reservation_renewal_restates_its_resource_lifecycle() -> anyhow::Result<()> {
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059"; let sender: Address = "0x0000000000000000000000000000000000000002".parse()?; let token = versioned_token("beta", 0); let mut prefix = contested_claim_path_logs(100)?; prefix.extend([raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), expiry: 8, sender }.encode_log_data(), 6, 0, CLAIM_REGISTRY), raw_at(v2_registry::ParentUpdated { parent: Address::ZERO, label: "eth".to_owned(), sender }.encode_log_data(), 7, 0, CLAIM_REGISTRY)]); let suffix = vec![raw_at(v2_registry::ExpiryUpdated { tokenId: token, newExpiry: 8, sender }.encode_log_data(), 9, 0, CLAIM_REGISTRY), raw_at(v2_registry::ExpiryUpdated { tokenId: token, newExpiry: 20, sender }.encode_log_data(), 10, 0, CLAIM_REGISTRY)];
    let (first, session) = interpret_test_batch_incremental(contested_claim_path_input(prefix, Vec::new(), (1..=8).map(test_block).collect())?, None)?; let (live, _) = interpret_test_batch_incremental(contested_claim_path_input(suffix.clone(), Vec::new(), vec![test_block(9), test_block(10)])?, Some(session))?;
    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &(1..=8).map(test_block).collect::<Vec<_>>())?; let restored = interpret_test_batch(contested_claim_path_input(suffix, prior, vec![test_block(9), test_block(10)])?)?; assert_eq!(live, restored); let release = first.normalized_events.iter().find(|event| event.block_number == Some(8) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.logical_name_id.is_none()).expect("detached own-expiry release");
    assert_eq!(live.normalized_events.iter().filter(|event| event.event_kind == "RegistrationRenewed" && event.resource_id == release.resource_id && event.logical_name_id.is_none() && event.after_state["revived_from_expiry"] == true && event.after_state["status"] == "reserved" && event.after_state["reservation_resource"] == true).map(|event| event.block_number).collect::<Vec<_>>(), [Some(10)]);
    Ok(())
}

#[test]
fn detached_expired_reservation_promotion_rearms_its_resource_retirement() -> anyhow::Result<()> {
    assert_detached_expired_reservation_reinstall_rearms_resource_retirement(true)
}

#[test]
fn detached_expired_reservation_rereserve_rearms_its_resource_retirement() -> anyhow::Result<()> {
    assert_detached_expired_reservation_reinstall_rearms_resource_retirement(false)
}

#[test]
fn ancestor_expired_reservation_promotion_rearms_its_resource_retirement() -> anyhow::Result<()> {
    assert_ancestor_expired_reservation_promotion_rearms_resource_retirement()
}

#[test]
#[rustfmt::skip]
fn ancestor_expired_reservation_renewal_preserves_resource_retirement_suppression() -> anyhow::Result<()> {
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059"; let sender: Address = "0x0000000000000000000000000000000000000002".parse()?; let token = versioned_token("beta", 0); let mut prefix = contested_claim_path_logs(8)?; prefix.push(raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), expiry: 20, sender }.encode_log_data(), 6, 0, CLAIM_REGISTRY)); let renewal = raw_at(v2_registry::ExpiryUpdated { tokenId: token, newExpiry: 30, sender }.encode_log_data(), 10, 0, CLAIM_REGISTRY);
    let (first, session) = interpret_test_batch_incremental(contested_claim_path_input(prefix, Vec::new(), (1..=8).map(test_block).collect())?, None)?; let reservation_resource = first.normalized_events.iter().find(|event| event.block_number == Some(6) && event.event_kind == "RegistrationReserved").and_then(|event| event.resource_id).expect("named reservation resource"); let first_release = first.normalized_events.iter().find(|event| event.block_number == Some(8) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.resource_id == Some(reservation_resource)).expect("ancestor-expiry release"); assert!(first_release.logical_name_id.is_some());
    let first_blocks = (1..=8).map(test_block).collect::<Vec<_>>(); let live_prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &first_blocks)?; let (_, live_session) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), live_prior, Vec::new())?, None)?; let (live, _) = interpret_test_batch_incremental(contested_claim_path_input(vec![renewal.clone()], Vec::new(), vec![test_block(10), test_block(30)])?, Some(live_session))?; let (renewed, renewed_session) = interpret_test_batch_incremental(contested_claim_path_input(vec![renewal], Vec::new(), vec![test_block(10)])?, Some(session))?; let (split_crossing, _) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(30)])?, Some(renewed_session))?;
    assert_eq!(renewed.normalized_events.iter().filter(|event| event.block_number == Some(10) && event.event_kind == "ExpiryChanged" && event.resource_id == Some(reservation_resource) && event.after_state["source_event"] == "ExpiryUpdated" && event.after_state["expiry"] == 30).count(), 1);
    let mut all_events = first.normalized_events.clone(); all_events.extend(renewed.normalized_events); let mut all_blocks = first_blocks; all_blocks.push(test_block(10)); let prior = seam::fold_prior_events(Vec::new(), &all_events, &all_blocks)?; let (_, restored_session) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), prior, Vec::new())?, None)?; let (restored_crossing, _) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(30)])?, Some(restored_session))?; assert_eq!(split_crossing, restored_crossing); assert_eq!(live.normalized_events.iter().filter(|event| event.block_number == Some(30)).collect::<Vec<_>>(), split_crossing.normalized_events.iter().collect::<Vec<_>>());
    let releases = |output: &BatchOutput| output.normalized_events.iter().filter(|event| event.block_number == Some(30) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.resource_id == Some(reservation_resource)).count(); assert_eq!((releases(&live), releases(&split_crossing), releases(&restored_crossing)), (0, 0, 0));
    Ok(())
}

#[rustfmt::skip]
fn assert_detached_expired_reservation_reinstall_rearms_resource_retirement(registered: bool) -> anyhow::Result<()> {
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059"; let owner: Address = "0x0000000000000000000000000000000000000001".parse()?; let sender: Address = "0x0000000000000000000000000000000000000002".parse()?; let token = versioned_token("beta", 0); let mut prefix = contested_claim_path_logs(100)?; prefix.extend([raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), expiry: 8, sender }.encode_log_data(), 6, 0, CLAIM_REGISTRY), raw_at(v2_registry::ParentUpdated { parent: Address::ZERO, label: "eth".to_owned(), sender }.encode_log_data(), 7, 0, CLAIM_REGISTRY)]);
    let reinstall = if registered { raw_at(v2_registry::LabelRegistered { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), owner, expiry: 20, sender }.encode_log_data(), 10, 0, CLAIM_REGISTRY) } else { raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), expiry: 20, sender }.encode_log_data(), 10, 0, CLAIM_REGISTRY) };
    let (first, session) = interpret_test_batch_incremental(contested_claim_path_input(prefix, Vec::new(), (1..=8).map(test_block).collect())?, None)?; let first_release = first.normalized_events.iter().find(|event| event.block_number == Some(8) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.logical_name_id.is_none()).expect("detached own-expiry release");
    let (installed, live_session) = interpret_test_batch_incremental(contested_claim_path_input(vec![reinstall], Vec::new(), vec![test_block(10)])?, Some(session))?; let mut all_events = first.normalized_events.clone(); all_events.extend(installed.normalized_events.clone()); let mut all_blocks = (1..=8).map(test_block).collect::<Vec<_>>(); all_blocks.push(test_block(10)); let prior = seam::fold_prior_events(Vec::new(), &all_events, &all_blocks)?; let (_, restored_session) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), prior, Vec::new())?, None)?; assert_eq!(live_session, restored_session);
    let (live_crossing, _) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(20)])?, Some(live_session))?; let (restored_crossing, _) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(20)])?, Some(restored_session))?; assert_eq!(live_crossing, restored_crossing); let releases = |output: &BatchOutput| output.normalized_events.iter().filter(|event| event.block_number == Some(20) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.logical_name_id.is_none() && event.resource_id == first_release.resource_id).count(); assert_eq!((releases(&live_crossing), releases(&restored_crossing)), (1, 1));
    Ok(())
}

#[rustfmt::skip]
fn assert_ancestor_expired_reservation_promotion_rearms_resource_retirement() -> anyhow::Result<()> {
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059"; let owner: Address = "0x0000000000000000000000000000000000000001".parse()?; let sender: Address = "0x0000000000000000000000000000000000000002".parse()?; let token = versioned_token("beta", 0); let mut prefix = contested_claim_path_logs(8)?; prefix.push(raw_at(v2_registry::LabelReserved { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), expiry: 20, sender }.encode_log_data(), 6, 0, CLAIM_REGISTRY));
    let reinstall = raw_at(v2_registry::LabelRegistered { tokenId: token, labelHash: keccak256(b"beta"), label: "beta".to_owned(), owner, expiry: 20, sender }.encode_log_data(), 10, 0, CLAIM_REGISTRY);
    let (first, session) = interpret_test_batch_incremental(contested_claim_path_input(prefix, Vec::new(), (1..=8).map(test_block).collect())?, None)?; let reservation_resource = first.normalized_events.iter().find(|event| event.block_number == Some(6) && event.event_kind == "RegistrationReserved").and_then(|event| event.resource_id).expect("named reservation resource"); let first_release = first.normalized_events.iter().find(|event| event.block_number == Some(8) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.resource_id == Some(reservation_resource)).expect("ancestor-expiry release"); assert!(first_release.logical_name_id.is_some());
    let (installed, live_session) = interpret_test_batch_incremental(contested_claim_path_input(vec![reinstall], Vec::new(), vec![test_block(10)])?, Some(session))?; let mut all_events = first.normalized_events.clone(); all_events.extend(installed.normalized_events.clone()); let mut all_blocks = (1..=8).map(test_block).collect::<Vec<_>>(); all_blocks.push(test_block(10)); let prior = seam::fold_prior_events(Vec::new(), &all_events, &all_blocks)?; let (_, restored_session) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), prior, Vec::new())?, None)?; assert_eq!(live_session, restored_session);
    let (live_crossing, _) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(20)])?, Some(live_session))?; let (restored_crossing, _) = interpret_test_batch_incremental(contested_claim_path_input(Vec::new(), Vec::new(), vec![test_block(20)])?, Some(restored_session))?; assert_eq!(live_crossing, restored_crossing); let releases = |output: &BatchOutput| output.normalized_events.iter().filter(|event| event.block_number == Some(20) && event.event_kind == "RegistrationReleased" && event.after_state["source_event"] == "RegistryPathExpired" && event.logical_name_id.is_none() && event.resource_id == Some(reservation_resource)).count(); assert_eq!((releases(&live_crossing), releases(&restored_crossing)), (1, 1));
    Ok(())
}

#[test]
#[rustfmt::skip]
fn renewed_then_detached_v2_name_retires_equally_after_cold_restore() -> anyhow::Result<()> {
    const CHILD77: &str = "0x0000000000000000000000000000000000000077";
    const OTHER77: &str = "0x0000000000000000000000000000000000000078";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let parent_token = versioned_token("sub", 1);
    let child_token = versioned_token("leaf", 1);
    let manifest = manifest_with_events(
        77,
        "ens",
        "ens_v2_registry_l1",
        &[
            ("LabelRegistered", "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)", &["registry"], &["RegistrationGranted"]),
            ("TokenResource", "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)", &["registry"], &["TokenResourceLinked"]),
            ("SubregistryUpdated", "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)", &["registry"], &["SubregistryChanged"]),
            ("ParentUpdated", "event ParentUpdated(address indexed parent, string label, address indexed sender)", &["registry"], &["ParentChanged"]),
            ("ExpiryUpdated", "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)", &["registry"], &["ExpiryChanged", "RegistrationRenewed"]),
        ],
    );
    let mut child_admission = admission(77, "registry");
    child_admission.address = CHILD77.to_owned();
    child_admission.contract_instance_id = super::common::contract_id(CHAIN, CHILD77);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id =
        Some(super::common::contract_id(CHAIN, CHILD77));
    child_admission.discovery_observation_key = Some("registry-announcement:child77".to_owned());
    let input = |logs, prior, blocks| BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 77,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![admission(77, "registry"), child_admission.clone()],
        prior_events: prior,
        blocks,
        raw_logs: logs,
    };
    let setup = vec![
        raw_at(v2_registry::LabelRegistered { tokenId: parent_token, labelHash: keccak256(b"sub"), label: "sub".to_owned(), owner, expiry: 1_000, sender }.encode_log_data(), 1, 0, CONTRACT),
        raw_at(v2_registry::SubregistryUpdated { tokenId: parent_token, subregistry: CHILD77.parse()?, sender }.encode_log_data(), 1, 1, CONTRACT),
        raw_at(v2_registry::LabelRegistered { tokenId: child_token, labelHash: keccak256(b"leaf"), label: "leaf".to_owned(), owner, expiry: 10, sender }.encode_log_data(), 2, 0, CHILD77),
        raw_at(v2_registry::TokenResource { tokenId: child_token, resource: U256::from(71) }.encode_log_data(), 2, 1, CHILD77),
        raw_at(v2_registry::ParentUpdated { parent: CONTRACT.parse()?, label: "sub".to_owned(), sender }.encode_log_data(), 2, 2, CHILD77),
    ];
    let renew = vec![
        raw_at(v2_registry::ExpiryUpdated { tokenId: child_token, newExpiry: 500, sender }.encode_log_data(), 11, 0, CHILD77),
    ];
    let repoint = vec![
        raw_at(v2_registry::SubregistryUpdated { tokenId: parent_token, subregistry: OTHER77.parse()?, sender }.encode_log_data(), 12, 0, CONTRACT),
    ];
    let (out_setup, sess1) = interpret_test_batch_incremental(input(setup, Vec::new(), Vec::new()), None)?;
    let leaf = out_setup.name_surfaces.iter().find(|s| s.raw_name == "leaf.sub.eth").expect("leaf.sub.eth binds").logical_name_id.clone();
    let (out_10, sess2) = interpret_test_batch_incremental(input(Vec::new(), Vec::new(), vec![test_block(10)]), Some(sess1))?;
    assert_eq!(out_10.normalized_events.iter().filter(|event| event.event_kind == "RegistrationReleased" && event.logical_name_id.as_deref() == Some(leaf.as_str())).count(), 1);
    let (out_11, sess3) = interpret_test_batch_incremental(input(renew, Vec::new(), Vec::new()), Some(sess2))?;
    let (out_12, sess_live) = interpret_test_batch_incremental(input(repoint, Vec::new(), Vec::new()), Some(sess3))?;
    let all_events = [out_setup.normalized_events.clone(), out_10.normalized_events.clone(), out_11.normalized_events.clone(), out_12.normalized_events.clone()].concat();
    let blocks = [test_block(1), test_block(2), test_block(10), test_block(11), test_block(12)];
    let prior = seam::fold_prior_events(Vec::new(), &all_events, &blocks)?;
    let (_, sess_restored) = interpret_test_batch_incremental(input(Vec::new(), prior, Vec::new()), None)?;
    assert_eq!(sess_live, sess_restored);
    let (live_500, _) = interpret_test_batch_incremental(input(Vec::new(), Vec::new(), vec![test_block(500)]), Some(sess_live))?;
    let (restored_500, _) = interpret_test_batch_incremental(input(Vec::new(), Vec::new(), vec![test_block(500)]), Some(sess_restored))?;
    assert_eq!(live_500, restored_500);
    let releases = live_500.normalized_events.iter().filter(|event| event.event_kind == "RegistrationReleased").collect::<Vec<_>>();
    assert_eq!(releases.len(), 1);
    assert!(releases[0].logical_name_id.is_none());
    assert!(releases[0].resource_id.is_some());
    assert_eq!(releases[0].after_state["source_event"], "RegistryPathExpired");
    Ok(())
}

fn contested_claim_path_survivor(output: &BatchOutput, boundary_block: i64) -> Uuid {
    let replacement = output
        .surface_bindings
        .iter()
        .find(|binding| binding.block_number == boundary_block)
        .expect("the closure boundary reasserts a binding");
    let persisted = simulate_binding_writer(output);
    let open = persisted
        .iter()
        .filter(|binding| {
            binding.logical_name_id == replacement.logical_name_id
                && binding.authority_arm == "ens_v2"
                && binding.active_to.is_none()
        })
        .collect::<Vec<_>>();
    assert_eq!(open.len(), 1, "unexpected binding history: {persisted:#?}");
    assert_eq!(open[0].position.0, boundary_block);
    assert_eq!(open[0].resource_id, replacement.resource_id);
    let fabricated_lifecycle = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(boundary_block)
                && event.resource_id == Some(replacement.resource_id)
                && matches!(
                    event.event_kind.as_str(),
                    "SurfaceUnbound"
                        | "RegistrationReleased"
                        | "SurfaceBound"
                        | "RegistrationGranted"
                        | "AuthorityTransferred"
                        | "ExpiryChanged"
                        | "ResolverChanged"
                        | "SubregistryChanged"
                )
        })
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert!(
        fabricated_lifecycle.is_empty(),
        "reassertion fabricated survivor lifecycle: {fabricated_lifecycle:?}"
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.block_number == Some(boundary_block)
            && event.logical_name_id.as_deref() == Some(&replacement.logical_name_id)
            && event.event_kind == seam::PREIMAGE_OBSERVATION_EVENT_KIND
    }));
    replacement.resource_id
}

#[rustfmt::skip]
fn contested_claim_path_input(
    raw_logs: Vec<RawLogInput>,
    prior_events: Vec<PriorEventInput>,
    blocks: Vec<RawBlockInput>,
) -> anyhow::Result<BatchInput> {
    const ROOT_MANIFEST: i64 = 94;
    const REGISTRY_MANIFEST: i64 = 95;
    const CLAIM_ROOT: &str = "0x0000000000000000000000000000000000000058";
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059";
    let registry_events = [
        (
            "LabelRegistered",
            "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
            &["registry"][..],
            &["RegistrationGranted"][..],
        ),
        (
            "TokenResource",
            "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
            &["registry"][..],
            &["TokenResourceLinked"][..],
        ),
        (
            "ParentUpdated",
            "event ParentUpdated(address indexed parent, string label, address indexed sender)",
            &["registry"][..],
            &["ParentChanged"][..],
        ),
        ("LabelReserved", "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)", &["registry"][..], &["RegistrationReserved"][..]),
        ("ExpiryUpdated", "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)", &["registry"][..], &["ExpiryChanged", "RegistrationRenewed"][..]),
    ];
    let root_events = [
        registry_events[0],
        (
            "SubregistryUpdated",
            "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
            &["registry"][..],
            &["SubregistryChanged"][..],
        ),
    ];
    let mut claim_root = admission(ROOT_MANIFEST, "registry");
    claim_root.address = CLAIM_ROOT.to_owned();
    claim_root.contract_instance_id = super::common::contract_id(CHAIN, CLAIM_ROOT);
    let mut claim_registry = admission(REGISTRY_MANIFEST, "registry");
    claim_registry.address = CLAIM_REGISTRY.to_owned();
    claim_registry.contract_instance_id = super::common::contract_id(CHAIN, CLAIM_REGISTRY);
    claim_registry.role = None;
    claim_registry.discovery_edge_kind = Some("registry_announcement".to_owned());
    claim_registry.discovery_from_contract_instance_id = Some(claim_root.contract_instance_id);
    claim_registry.discovery_observation_key = Some("contested-claim-registry".to_owned());
    Ok(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(ROOT_MANIFEST, "ens", "ens_v2_root_l1", &root_events),
            manifest_with_events(
                REGISTRY_MANIFEST,
                "ens",
                "ens_v2_registry_l1",
                &registry_events,
            ),
        ],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: ROOT_MANIFEST,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "linked_subregistry_event".to_owned(),
        }],
        admissions: vec![
            admission(REGISTRY_MANIFEST, "registry"),
            claim_root,
            claim_registry,
        ],
        prior_events,
        blocks,
        raw_logs,
    })
}

fn contested_claim_path_logs(parent_expiry: u64) -> anyhow::Result<Vec<RawLogInput>> {
    const CLAIM_ROOT: &str = "0x0000000000000000000000000000000000000058";
    const CLAIM_REGISTRY: &str = "0x0000000000000000000000000000000000000059";
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let alpha = versioned_token("alpha", 1);
    let eth = versioned_token("eth", 1);
    Ok(vec![
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: alpha,
                labelHash: keccak256(b"alpha"),
                label: "alpha".to_owned(),
                owner,
                expiry: 100,
                sender,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        ),
        raw_at(
            v2_registry::TokenResource {
                tokenId: alpha,
                resource: U256::from(0xaa),
            }
            .encode_log_data(),
            1,
            1,
            CONTRACT,
        ),
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: eth,
                labelHash: keccak256(b"eth"),
                label: "eth".to_owned(),
                owner,
                expiry: parent_expiry,
                sender,
            }
            .encode_log_data(),
            2,
            0,
            CLAIM_ROOT,
        ),
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: alpha,
                labelHash: keccak256(b"alpha"),
                label: "alpha".to_owned(),
                owner,
                expiry: 100,
                sender,
            }
            .encode_log_data(),
            3,
            0,
            CLAIM_REGISTRY,
        ),
        raw_at(
            v2_registry::TokenResource {
                tokenId: alpha,
                resource: U256::from(0xbb),
            }
            .encode_log_data(),
            3,
            1,
            CLAIM_REGISTRY,
        ),
        raw_at(
            v2_registry::SubregistryUpdated {
                tokenId: eth,
                subregistry: CLAIM_REGISTRY.parse()?,
                sender,
            }
            .encode_log_data(),
            4,
            0,
            CLAIM_ROOT,
        ),
        raw_at(
            v2_registry::ParentUpdated {
                parent: CLAIM_ROOT.parse()?,
                label: "eth".to_owned(),
                sender,
            }
            .encode_log_data(),
            5,
            0,
            CLAIM_REGISTRY,
        ),
    ])
}

fn assert_contested_surface_departure_reasserts_survivor(
    release_winner: bool,
) -> anyhow::Result<()> {
    const RIVAL: &str = "0x0000000000000000000000000000000000000069";
    const MANIFEST_ID: i64 = 93;
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let token = versioned_token("alpha", 1);
    let manifest = manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["registry"],
                &["RegistrationReleased"],
            ),
        ],
    );
    let mut rival_admission = admission(MANIFEST_ID, "registry");
    rival_admission.address = RIVAL.to_owned();
    rival_admission.contract_instance_id = super::common::contract_id(CHAIN, RIVAL);
    let admissions = || vec![admission(MANIFEST_ID, "registry"), rival_admission.clone()];
    let register = |emitter: &str, resource, block| {
        vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: token,
                    labelHash: keccak256(b"alpha"),
                    label: "alpha".to_owned(),
                    owner,
                    expiry: 5_000,
                    sender,
                }
                .encode_log_data(),
                block,
                0,
                emitter,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token,
                    resource,
                }
                .encode_log_data(),
                block,
                1,
                emitter,
            ),
        ]
    };
    let mut setup_logs = register(CONTRACT, U256::from(0xaa), 1);
    setup_logs.extend(register(RIVAL, U256::from(0xbb), 2));
    let departed = if release_winner { RIVAL } else { CONTRACT };
    let survivor_link_block = if release_winner { 1 } else { 2 };
    let release = raw_at(
        v2_registry::LabelUnregistered {
            tokenId: token,
            sender,
        }
        .encode_log_data(),
        3,
        0,
        departed,
    );
    let input = |raw_logs, prior_events| BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events,
        blocks: Vec::new(),
        raw_logs,
    };

    let mut all_logs = setup_logs.clone();
    all_logs.push(release.clone());
    let full = interpret_test_batch(input(all_logs, Vec::new()))?;
    let survivor_resource = full
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "TokenResourceLinked"
                && event.block_number == Some(survivor_link_block)
        })
        .and_then(|event| event.resource_id)
        .expect("the surviving token links a resource");
    let logical_name_id = full
        .surface_bindings
        .iter()
        .find(|binding| binding.resource_id == survivor_resource)
        .map(|binding| binding.logical_name_id.clone())
        .expect("the surviving token initially binds the contested surface");
    let survivor_lifecycle = full
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(3)
                && event.resource_id == Some(survivor_resource)
                && matches!(
                    event.event_kind.as_str(),
                    "SurfaceUnbound"
                        | "RegistrationReleased"
                        | "SurfaceBound"
                        | "RegistrationGranted"
                        | "AuthorityTransferred"
                        | "ExpiryChanged"
                        | "ResolverChanged"
                        | "SubregistryChanged"
                )
        })
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert!(
        survivor_lifecycle.is_empty(),
        "identity reassertion must not fabricate survivor lifecycle events: {survivor_lifecycle:?}"
    );
    let persisted = simulate_binding_writer(&full);
    let open = persisted
        .iter()
        .filter(|row| {
            row.live()
                && row.logical_name_id == logical_name_id
                && row.authority_arm == "ens_v2"
                && row.active_to.is_none()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        open.len(),
        1,
        "the contested surface must retain exactly one open persisted ENSv2 binding: {persisted:#?}"
    );
    assert_eq!(open[0].resource_id, survivor_resource);
    assert_eq!(
        open[0].position.0, 3,
        "the departure boundary must reassert the elected survivor"
    );

    let (setup, session) = interpret_test_batch_incremental(input(setup_logs, Vec::new()), None)?;
    // The departure is the last and only raw log in this batch; its survivor reassertion cannot
    // wait for another batch-scoped refresh.
    let (incremental, _) =
        interpret_test_batch_incremental(input(vec![release.clone()], Vec::new()), Some(session))?;
    let prior = seam::fold_prior_events(
        Vec::new(),
        &setup.normalized_events,
        &[test_block(1), test_block(2)],
    )?;
    let compacted = interpret_test_batch(input(vec![release], prior))?;
    assert_eq!(
        incremental, compacted,
        "retained-session incremental interpretation must match compacted cold restore"
    );
    assert_eq!(
        departure_identity_effects(&full, 3),
        departure_identity_effects(&incremental, 3),
        "full replay must emit the same departure identity transition as incremental interpretation"
    );
    Ok(())
}

fn departure_identity_effects(
    output: &BatchOutput,
    block_number: i64,
) -> (
    Vec<NormalizedEvent>,
    Vec<SurfaceBinding>,
    Vec<BindingClosure>,
) {
    (
        output
            .normalized_events
            .iter()
            .filter(|event| event.block_number == Some(block_number))
            .cloned()
            .collect(),
        output
            .surface_bindings
            .iter()
            .filter(|binding| binding.block_number == block_number)
            .cloned()
            .collect(),
        output
            .binding_closures
            .iter()
            .filter(|closure| closure.block_number == block_number)
            .cloned()
            .collect(),
    )
}

fn test_block(block_number: i64) -> RawBlockInput {
    RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block_number),
        canonicality_state: "canonical".to_owned(),
    }
}

#[test]
fn discovery_reconciliation_keys_are_scoped_per_registry_token() -> anyhow::Result<()> {
    let known_target = "0x0000000000000000000000000000000000000011";
    let known_target_id = Uuid::from_u128(1_313);
    let manifest = manifest_with_events(
        13,
        "ens",
        "ens_v2_registry_l1",
        &[(
            "SubregistryUpdated",
            "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
            &["registry"],
            &["SubregistryChanged"],
        )],
    );
    let update = |token_id, target: &str| {
        v2_registry::SubregistryUpdated {
            tokenId: token_id,
            subregistry: target.parse().unwrap(),
            sender: "0x0000000000000000000000000000000000000002"
                .parse()
                .unwrap(),
        }
        .encode_log_data()
    };
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 13,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: vec![
            admission(13, "registry"),
            AddressAdmissionInput {
                address: known_target.to_owned(),
                contract_instance_id: known_target_id,
                source_manifest_id: Some(13),
                role: Some("registry".to_owned()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            },
        ],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                update(versioned_token("alice", 1), known_target),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                update(
                    versioned_token("bob", 1),
                    "0x0000000000000000000000000000000000000022",
                ),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                update(
                    versioned_token("alice", 2),
                    "0x0000000000000000000000000000000000000033",
                ),
                2,
                0,
                CONTRACT,
            ),
        ],
    })?;
    let keys = output
        .discovery_edges
        .iter()
        .map(|edge| edge.provenance["observation_key"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 3);
    assert_ne!(keys[0], keys[1]);
    assert_eq!(keys[0], keys[2]);
    assert_eq!(
        output.discovery_edges[0].to_contract_instance_id,
        known_target_id
    );
    Ok(())
}

#[test]
fn ens_v2_topology_restatement_restores_against_the_transitioned_registry() -> anyhow::Result<()> {
    const PARENT: &str = "0x0000000000000000000000000000000000000062";
    const CHILD: &str = "0x0000000000000000000000000000000000000063";
    const CHILD_RESOLVER: &str = "0x0000000000000000000000000000000000000064";
    const PARENT_RESOLVER: &str = "0x0000000000000000000000000000000000000065";
    let token = versioned_token("same", 1);
    let sender: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let manifest = manifest_with_events(
        62,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let rules = || {
        vec![
            DiscoveryRuleInput {
                manifest_id: 62,
                edge_kind: "resolver".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "protocol_event".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 62,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ]
    };
    let mut parent_admission = admission(62, "registry");
    parent_admission.address = PARENT.to_owned();
    parent_admission.contract_instance_id = Uuid::from_u128(620);
    let mut child_admission = admission(62, "registry");
    child_admission.address = CHILD.to_owned();
    child_admission.contract_instance_id = Uuid::from_u128(621);
    child_admission.role = None;
    child_admission.discovery_edge_kind = Some("registry_announcement".to_owned());
    child_admission.discovery_from_contract_instance_id = Some(Uuid::from_u128(621));
    child_admission.discovery_observation_key = Some("registry-announcement:child".to_owned());
    let admissions = || vec![parent_admission.clone(), child_admission.clone()];
    let registration = |emitter: &str, block| {
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: token,
                labelHash: keccak256(b"same"),
                label: "same".to_owned(),
                owner: sender,
                expiry: 100,
                sender,
            }
            .encode_log_data(),
            block,
            0,
            emitter,
        )
    };
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            registration(PARENT, 1),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token,
                    resource: U256::from(1),
                }
                .encode_log_data(),
                2,
                0,
                PARENT,
            ),
            registration(CHILD, 3),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token,
                    resource: U256::from(2),
                }
                .encode_log_data(),
                4,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: token,
                    resolver: CHILD_RESOLVER.parse()?,
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: PARENT.parse()?,
                    label: "same".to_owned(),
                    sender,
                }
                .encode_log_data(),
                6,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                7,
                0,
                PARENT,
            ),
        ],
    })?;
    let child_restatement = first
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "SubregistryUpdated"
        })
        .expect("parent topology change restates the child resolver");
    assert!(
        child_restatement.raw_fact_ref["state_scope"]
            .as_str()
            .is_some_and(|scope| scope.starts_with(&CHILD.to_ascii_lowercase()))
    );
    let parent_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenResourceLinked" && event.block_number == Some(2))
        .and_then(|event| event.resource_id)
        .expect("parent resource");

    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: rules(),
        admissions: admissions(),
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::ResolverUpdated {
                tokenId: token,
                resolver: PARENT_RESOLVER.parse()?,
                sender,
            }
            .encode_log_data(),
            8,
            0,
            PARENT,
        )],
    })?;
    let parent_update = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("parent resolver update");
    assert_eq!(parent_update.resource_id, Some(parent_resource));
    assert_ne!(
        parent_update
            .before_state
            .get("resolver")
            .and_then(serde_json::Value::as_str),
        Some(CHILD_RESOLVER)
    );
    Ok(())
}

#[test]
fn erc1155_transfers_require_positive_value_nonzero_endpoints_and_keep_lineage()
-> anyhow::Result<()> {
    let token_id = U256::from(7);
    let upstream_resource = U256::from(77);
    let manifest = manifest_with_events(
        14,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "TransferSingle",
                "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                &["registry"],
                &["TokenControlTransferred"],
            ),
            (
                "TransferBatch",
                "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                &["registry"],
                &["TokenControlTransferred"],
            ),
        ],
    );
    let operator: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let from: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let to: Address = "0x0000000000000000000000000000000000000003".parse()?;
    let linked = v2_registry::TokenResource {
        tokenId: token_id,
        resource: upstream_resource,
    }
    .encode_log_data();
    let batch = v2_registry::TransferBatch {
        operator,
        from,
        to,
        ids: vec![token_id, token_id],
        values: vec![U256::ZERO, U256::from(1)],
    }
    .encode_log_data();
    let mint = v2_registry::TransferSingle {
        operator,
        from: Address::ZERO,
        to,
        id: token_id,
        value: U256::from(1),
    }
    .encode_log_data();
    let burn = v2_registry::TransferSingle {
        operator,
        from,
        to: Address::ZERO,
        id: token_id,
        value: U256::from(1),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(14, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(linked, 1, 0, CONTRACT),
            raw_at(batch, 1, 1, CONTRACT),
            raw_at(mint, 1, 2, CONTRACT),
            raw_at(burn, 1, 3, CONTRACT),
        ],
    })?;
    let transfers = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "TokenControlTransferred")
        .collect::<Vec<_>>();
    assert_eq!(transfers.len(), 1);
    let resource_id = transfers[0].resource_id.expect("transfer resource");
    assert!(
        output.resources.iter().any(|resource| {
            resource.resource_id == resource_id && resource.token_lineage_id.is_some()
        }),
        "a transfer must retain the linked resource's token lineage"
    );
    Ok(())
}

#[test]
fn resolver_before_state_is_scoped_by_emitter_node_and_selector() -> anyhow::Result<()> {
    let node = B256::repeat_byte(0x11);
    let emitter_one = "0x0000000000000000000000000000000000000011";
    let emitter_two = "0x0000000000000000000000000000000000000022";
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            15,
            "ens_v1_resolver_l1",
            "VersionChanged",
            "event VersionChanged(bytes32 indexed node, uint64 newVersion)",
            &[],
            &["RecordVersionChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                resolver::VersionChanged {
                    node,
                    newVersion: 1,
                }
                .encode_log_data(),
                1,
                0,
                emitter_one,
            ),
            raw_at(
                resolver::VersionChanged {
                    node,
                    newVersion: 2,
                }
                .encode_log_data(),
                1,
                1,
                emitter_two,
            ),
            raw_at(
                resolver::VersionChanged {
                    node,
                    newVersion: 3,
                }
                .encode_log_data(),
                2,
                0,
                emitter_one,
            ),
        ],
    })?;
    let events = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RecordVersionChanged")
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].before_state, json!({}));
    assert_eq!(events[1].before_state, json!({}));
    assert_eq!(events[2].before_state, events[0].after_state);
    Ok(())
}

#[test]
fn legacy_text_changed_without_value_uses_its_three_argument_decoder() -> anyhow::Result<()> {
    let encoded = legacy_text_without_value::TextChanged {
        node: B256::repeat_byte(0x33),
        indexedKey: keccak256(b"avatar"),
        key: "avatar".to_owned(),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            24,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("legacy text transition");
    assert_eq!(event.after_state["record_key"], "text:avatar");
    assert_eq!(event.after_state["selector_key"], "avatar");
    assert!(event.after_state.get("value").is_none());
    Ok(())
}

#[test]
fn admitted_old_public_resolver_unindexed_text_changed_matches_indexed_output() -> anyhow::Result<()>
{
    const MAINNET: &str = "ethereum-mainnet";
    const RESOLVER: &str = "0x226159d592e2b063810a10ebf6dcbada94ed68b8";
    const NODE: &str = "0x2d76384bbe48eafe3426abf5f285d74b5c2f6db8f1104afb03ca326cfc11300a";
    const BLOCK_HASH: &str = "0xdaf328a6e3ac42a14efabd1455664bd9176dc9848558ddaa8902cb3c40ceabaf";
    const TRANSACTION_HASH: &str =
        "0x6e56cea53c44bf7756a3f6c2313b537a20d890c82dec6eb2273590c6842df78b";
    const TOPIC0: &str = "0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550";
    const MANIFEST_ID: i64 = 84;

    let data = hex::decode(concat!(
        "0000000000000000000000000000000000000000000000000000000000000040",
        "0000000000000000000000000000000000000000000000000000000000000080",
        "0000000000000000000000000000000000000000000000000000000000000005",
        "656d61696c000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000005",
        "656d61696c000000000000000000000000000000000000000000000000000000",
    ))?;
    let fixture = RawLogInput {
        chain_id: MAINNET.to_owned(),
        block_hash: BLOCK_HASH.to_owned(),
        block_number: 8_711_672,
        block_timestamp: OffsetDateTime::UNIX_EPOCH,
        canonicality_state: "canonical".to_owned(),
        transaction_hash: TRANSACTION_HASH.to_owned(),
        transaction_index: 184,
        log_index: 129,
        emitting_address: RESOLVER.to_owned(),
        topics: vec![TOPIC0.to_owned(), NODE.to_owned()],
        data,
    };
    let mut text_manifest = manifest(
        MANIFEST_ID,
        "ens_v1_resolver_l1",
        "TextChanged",
        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)",
        &[],
        &["RecordChanged"],
    );
    text_manifest.chain_id = MAINNET.to_owned();
    let mut resolver_admission = admission(MANIFEST_ID, "public_resolver_226159d5");
    resolver_admission.address = RESOLVER.to_owned();

    let output = interpret_test_batch(BatchInput {
        chain_id: MAINNET.to_owned(),
        manifests: vec![text_manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![resolver_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![fixture.clone()],
    })?;
    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("real legacy text transition");
    assert_eq!(event.block_number, Some(8_711_672));
    assert_eq!(event.block_hash.as_deref(), Some(BLOCK_HASH));
    assert_eq!(event.transaction_hash.as_deref(), Some(TRANSACTION_HASH));
    assert_eq!(event.transaction_index, Some(184));
    assert_eq!(event.log_index, Some(129));
    assert_eq!(event.before_state, json!({}));
    assert_eq!(
        event.after_state,
        json!({
            "source_event": "TextChanged",
            "resolver": RESOLVER,
            "resolver_contract_instance_id": Uuid::from_u128(MANIFEST_ID as u128).to_string(),
            "node": NODE,
            "record_key": "text:email",
            "record_family": "text",
            "selector_key": "email",
            "value_retained": false,
        })
    );

    let indexed = legacy_text_without_value::TextChanged {
        node: NODE.parse()?,
        indexedKey: keccak256(b"email"),
        key: "email".to_owned(),
    }
    .encode_log_data();
    let mut indexed_fixture = raw_at(indexed, 8_711_672, 129, RESOLVER);
    indexed_fixture.chain_id = MAINNET.to_owned();
    indexed_fixture.block_hash = BLOCK_HASH.to_owned();
    indexed_fixture.block_timestamp = fixture.block_timestamp;
    indexed_fixture.transaction_hash = TRANSACTION_HASH.to_owned();
    indexed_fixture.transaction_index = 184;
    let indexed_output = interpret_test_batch(BatchInput {
        chain_id: MAINNET.to_owned(),
        manifests: vec![text_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![resolver_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![indexed_fixture],
    })?;
    assert_eq!(
        indexed_output.normalized_events[0].after_state,
        event.after_state
    );
    Ok(())
}

#[test]
fn legacy_unindexed_text_changed_with_unequal_strings_is_skipped() -> anyhow::Result<()> {
    let encoded = legacy_unindexed_text_without_value::TextChanged {
        node: B256::repeat_byte(0x33),
        indexedKey: "email".to_owned(),
        key: "url".to_owned(),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            85,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    assert!(output.normalized_events.is_empty());
    Ok(())
}

#[test]
fn ens_reverse_node_resolver_events_are_state_keyed_without_surface_identity() -> anyhow::Result<()>
{
    assert_reverse_node_resolver_events_are_state_keyed(
        "ens",
        "ens_v1_registry_l1",
        "ens_v1_resolver_l1",
        "ens_v1_reverse_l1",
        &["addr", "reverse"],
    )
}

#[test]
fn basenames_reverse_node_resolver_events_are_state_keyed_without_surface_identity()
-> anyhow::Result<()> {
    assert_reverse_node_resolver_events_are_state_keyed(
        "basenames",
        "basenames_base_registry",
        "basenames_base_resolver",
        "basenames_base_primary",
        &["80002105", "reverse"],
    )
}

fn assert_reverse_node_resolver_events_are_state_keyed(
    namespace: &str,
    registry_family: &str,
    resolver_family: &str,
    primary_family: &str,
    reverse_suffix: &[&str],
) -> anyhow::Result<()> {
    const REGISTRY_ADDRESS: &str = "0x0000000000000000000000000000000000000043";
    const RESOLVER_ADDRESS: &str = "0x0000000000000000000000000000000000000044";

    let reverse_label = CONTRACT.trim_start_matches("0x");
    let reverse_suffix = reverse_suffix
        .iter()
        .map(|label| (*label).to_owned())
        .collect::<Vec<_>>();
    let reverse_labels = std::iter::once(reverse_label.to_owned())
        .chain(reverse_suffix.iter().cloned())
        .collect::<Vec<_>>();
    let reverse_node = super::common::namehash(&reverse_labels).parse::<B256>()?;
    let reverse_parent = super::common::namehash(&reverse_suffix).parse::<B256>()?;
    let registry_log = v1_registry::NewOwner {
        node: reverse_parent,
        label: keccak256(reverse_label.as_bytes()),
        owner: CONTRACT.parse()?,
    }
    .encode_log_data();
    let name_log = resolver_name::NameChanged {
        node: reverse_node,
        name: "alice.eth".to_owned(),
    }
    .encode_log_data();
    let text_log = resolver_strings::TextChanged {
        node: reverse_node,
        indexedKey: keccak256(b"avatar"),
        key: "avatar".to_owned(),
        value: "ipfs://avatar".to_owned(),
    }
    .encode_log_data();

    let (primary_manifest, primary_log) = if primary_family == "ens_v1_reverse_l1" {
        (
            manifest_with_events(
                92,
                namespace,
                primary_family,
                &[(
                    "ReverseClaimed",
                    "event ReverseClaimed(address indexed addr, bytes32 indexed node)",
                    &["reverse_registrar"],
                    &["ReverseChanged"],
                )],
            ),
            ReverseClaimed {
                addr: CONTRACT.parse()?,
                node: reverse_node,
            }
            .encode_log_data(),
        )
    } else {
        (
            manifest_with_events(
                92,
                namespace,
                primary_family,
                &[(
                    "NameForAddrChanged",
                    "event NameForAddrChanged(address indexed addr, string name)",
                    &["reverse_registrar"],
                    &["ReverseChanged", "RecordChanged"],
                )],
            ),
            resolver_strings::NameForAddrChanged {
                addr: CONTRACT.parse()?,
                name: "alice.base.eth".to_owned(),
            }
            .encode_log_data(),
        )
    };
    let mut registry_admission = admission(90, "registry");
    registry_admission.address = REGISTRY_ADDRESS.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest_with_events(
                90,
                namespace,
                registry_family,
                &[(
                    "NewOwner",
                    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                    &["registry"],
                    &["SubregistryChanged", "AuthorityTransferred"],
                )],
            ),
            manifest_with_events(
                91,
                namespace,
                resolver_family,
                &[
                    (
                        "NameChanged",
                        "event NameChanged(bytes32 indexed node, string name)",
                        &[],
                        &["RecordChanged"],
                    ),
                    (
                        "TextChanged",
                        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                        &[],
                        &["RecordChanged"],
                    ),
                ],
            ),
            primary_manifest,
        ],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(92, "reverse_registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(registry_log, 1, 0, REGISTRY_ADDRESS),
            raw_at(primary_log, 1, 1, CONTRACT),
            raw_at(name_log, 1, 2, RESOLVER_ADDRESS),
            raw_at(text_log, 1, 3, RESOLVER_ADDRESS),
        ],
    })?;

    assert!(
        output.name_surfaces.is_empty(),
        "reverse-node activity and primary claims must not materialize exact-name surfaces"
    );
    let resolver_events = output
        .normalized_events
        .iter()
        .filter(|event| event.source_family == resolver_family)
        .filter(|event| event.event_kind == "RecordChanged")
        .collect::<Vec<_>>();
    assert_eq!(resolver_events.len(), 2);
    assert!(resolver_events.iter().any(|event| {
        event.after_state["source_event"] == "NameChanged"
            && event.after_state["raw_name"] == "alice.eth"
    }));
    assert!(resolver_events.iter().any(|event| {
        event.after_state["source_event"] == "TextChanged"
            && event.after_state["record_key"] == "text:avatar"
    }));
    assert!(
        resolver_events
            .iter()
            .all(|event| { event.logical_name_id.is_none() && event.resource_id.is_none() })
    );
    assert_batch_referential_integrity(
        &output,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )?;
    Ok(())
}

#[test]
fn ens_v1_unwrap_prior_state_reactivates_the_live_registrar_anchor() -> anyhow::Result<()> {
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let wrapper_address = "0x0000000000000000000000000000000000000043";
    let wrapper = manifest_with_events(
        25,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "AuthorityEpochChanged"],
            ),
        ],
    );
    let mut wrapper_admission = admission(25, "name_wrapper");
    wrapper_admission.address = wrapper_address.to_owned();
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                24,
                "ens_v1_registrar_l1",
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationGranted"],
            ),
            wrapper,
        ],
        discovery_rules: Vec::new(),
        admissions: vec![admission(24, "registrar"), wrapper_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameRegistered {
                    name: "wrapped".to_owned(),
                    label: keccak256(b"wrapped"),
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                NameWrapped {
                    node,
                    name: b"\x07wrapped\x03eth\0".to_vec().into(),
                    owner: CONTRACT.parse()?,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                2,
                0,
                wrapper_address,
            ),
            raw_at(
                NameUnwrapped {
                    node,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                3,
                0,
                wrapper_address,
            ),
        ],
    })?;
    let registrar_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar resource");
    assert!(first.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.block_number == Some(3)
            && event.resource_id == Some(registrar_resource)
    }));
    let prior_events = first
        .normalized_events
        .iter()
        .filter(|event| event.block_number.is_some())
        .map(prior_event)
        .collect();
    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            26,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::AddrChanged {
                node,
                a: CONTRACT.parse()?,
            }
            .encode_log_data(),
            4,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;

    let record = second
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("resolver record");
    assert_eq!(record.resource_id, Some(registrar_resource));
    assert_eq!(
        record.logical_name_id.as_deref(),
        Some(format!("ens:{}", super::common::namehash(&labels)).as_str())
    );
    Ok(())
}

#[test]
fn released_registration_restores_registry_authority_across_batches() -> anyhow::Result<()> {
    const OLD_REGISTRY: &str = "0x0000000000000000000000000000000000000065";
    const REGISTRY: &str = "0x0000000000000000000000000000000000000066";
    const RESOLVER: &str = "0x0000000000000000000000000000000000000067";
    let parent_labels = vec!["eth".to_owned()];
    let parent = super::common::namehash(&parent_labels).parse::<B256>()?;
    let labelhash = keccak256(b"released");
    let child_labels = vec!["released".to_owned(), "eth".to_owned()];
    let child = super::common::namehash(&child_labels);
    let registry_manifest = manifest(
        61,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &["SubregistryChanged", "AuthorityTransferred"],
    );
    let registrar_manifest = manifest(
        62,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let mut registry_admission = admission(61, "registry");
    registry_admission.address = OLD_REGISTRY.to_owned();
    let mut current_registry_admission = registry_admission.clone();
    current_registry_admission.address = REGISTRY.to_owned();
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest.clone(), registrar_manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission.clone(), admission(62, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent,
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                OLD_REGISTRY,
            ),
            raw_at(
                NameRegistered {
                    name: "released".to_owned(),
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{child}"));
    let registrar_resource = first
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar authority");
    assert_ne!(registry_resource, registrar_resource);
    let transfer_manifest = manifest(
        63,
        "ens_v1_registrar_l1",
        "Transfer",
        "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
        &["registrar"],
        &["TokenControlTransferred"],
    );
    let current_owner = raw_at(
        v1_registry::NewOwner {
            node: parent,
            label: labelhash,
            owner: CONTRACT.parse()?,
        }
        .encode_log_data(),
        2,
        0,
        REGISTRY,
    );
    let transfer = raw_at(
        v1_registrar::Transfer {
            from: CONTRACT.parse()?,
            to: "0x0000000000000000000000000000000000000068".parse()?,
            tokenId: U256::from_be_bytes(*labelhash),
        }
        .encode_log_data(),
        2,
        1,
        CONTRACT,
    );
    let transferred = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest.clone(), transfer_manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![
            current_registry_admission.clone(),
            admission(63, "registrar"),
        ],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![current_owner.clone(), transfer.clone()],
    })?;
    let transition = |kind, resource| {
        transferred
            .normalized_events
            .iter()
            .find(|event| event.event_kind == kind && event.resource_id == Some(resource))
    };
    let unbound = transition("SurfaceUnbound", registrar_resource)
        .expect("registrar authority detached after the token transfer");
    let rebound = transition("SurfaceBound", registry_resource)
        .expect("registry authority restored after the token transfer");
    assert_eq!(unbound.source_family, "ens_v1_registrar_l1");
    assert_eq!(rebound.source_family, "ens_v1_registrar_l1");
    assert_eq!(rebound.after_state["owner_getter"], CONTRACT);
    assert_eq!(rebound.after_state["registry_contract"], REGISTRY);
    let refreshed = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![current_registry_admission],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![current_owner],
    })?;
    let mut split_prior = first
        .normalized_events
        .iter()
        .chain(&refreshed.normalized_events)
        .map(prior_event)
        .collect::<Vec<_>>();
    split_prior.sort_by_key(|event| event.block_timestamp);
    let split = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![transfer_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(63, "registrar")],
        prior_events: split_prior,
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            block_number: 3,
            block_hash: "block-3".to_owned(),
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(3),
            transaction_hash: "transaction-3".to_owned(),
            ..transfer
        }],
    })?;
    let split_rebound = split
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound" && event.resource_id == Some(registry_resource)
        })
        .expect("split registry authority");
    assert_eq!(split_rebound.after_state["registry_contract"], REGISTRY);
    let released = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest, registrar_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![registry_admission, admission(62, "registrar")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "release-block".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH
                + time::Duration::seconds(42 + 90 * 24 * 60 * 60 + 1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    let restored = released
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound" && event.resource_id == Some(registry_resource)
        })
        .expect("restored registry authority");
    assert_eq!(restored.after_state["owner_getter"], CONTRACT);
    assert_eq!(restored.after_state["registry_contract"], OLD_REGISTRY);
    let persisted_resources = first
        .resources
        .iter()
        .map(|resource| (resource.chain_id.clone(), resource.resource_id))
        .collect();
    let persisted_surfaces = first
        .name_surfaces
        .iter()
        .map(|surface| (surface.chain_id.clone(), surface.logical_name_id.clone()))
        .collect();
    assert_batch_referential_integrity(&released, &persisted_resources, &persisted_surfaces)?;

    let mut prior_events = first
        .normalized_events
        .iter()
        .chain(&released.normalized_events)
        .map(prior_event)
        .collect::<Vec<_>>();
    prior_events.sort_by_key(|event| event.block_timestamp);
    let linked = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            63,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events,
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::AddrChanged {
                node: child.parse()?,
                a: CONTRACT.parse()?,
            }
            .encode_log_data(),
            3,
            0,
            RESOLVER,
        )],
    })?;
    let record = linked
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("resolver record after release");
    assert_eq!(record.resource_id, Some(registry_resource));
    assert_eq!(
        record.logical_name_id.as_deref(),
        Some(format!("ens:{child}").as_str())
    );
    Ok(())
}

#[test]
fn shadow_v1_expiry_releases_resource_without_surface_boundary() -> anyhow::Result<()> {
    let raw_label = vec![0xff];
    let labelhash = keccak256(&raw_label);
    let node = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let owner: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let registered = with_topic0(
        raw_v1_registrar::RawNameRegistered {
            name: raw_label.clone().into(),
            label: labelhash,
            owner,
            expires: U256::from(42),
        }
        .encode_log_data(),
        NameRegistered::SIGNATURE_HASH,
    );
    let manifest = manifest(
        71,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(71, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(registered, 1, 0, CONTRACT)],
    })?;
    assert_shadow_output(&first, &node, &raw_label, None);

    let released = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(71, "registrar")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "shadow-v1-release".to_owned(),
            block_number: 2,
            block_timestamp: OffsetDateTime::UNIX_EPOCH
                + time::Duration::seconds(42 + 90 * 24 * 60 * 60 + 1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;
    assert!(released.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationReleased"
            && event.logical_name_id.as_deref() == Some(format!("ens:{node}").as_str())
    }));
    assert!(released.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "SurfaceUnbound" | "SurfaceBound" | "AuthorityEpochChanged"
        )
    }));
    assert!(released.binding_closures.is_empty());
    assert!(released.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn lapsed_registration_after_registry_divergence_preserves_the_registry_anchor()
-> anyhow::Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000000066";
    const DIVERGED_OWNER: &str = "0x0000000000000000000000000000000000000068";
    let parent = super::common::namehash(&["eth".to_owned()]).parse::<B256>()?;
    let labelhash = keccak256(b"diverged");
    let child = super::common::namehash(&["diverged".to_owned(), "eth".to_owned()]);
    let registry_manifest = manifest(
        64,
        "ens_v1_registry_l1",
        "NewOwner",
        "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
        &["registry"],
        &["SubregistryChanged", "AuthorityTransferred"],
    );
    let registrar_manifest = manifest(
        65,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    let mut registry_admission = admission(64, "registry");
    registry_admission.address = REGISTRY.to_owned();
    let admissions = vec![registry_admission.clone(), admission(65, "registrar")];
    let manifests = vec![registry_manifest.clone(), registrar_manifest.clone()];
    let registered = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v1_registry::NewOwner {
                    node: parent,
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            raw_at(
                NameRegistered {
                    name: "diverged".to_owned(),
                    label: labelhash,
                    owner: CONTRACT.parse()?,
                    expires: U256::from(42),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;
    let registrar_resource = registered
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("registrar authority");
    let registry_resource =
        super::common::stable_uuid(&format!("resource:registry-only:{CHAIN}:{child}"));

    let diverged = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests.clone(),
        discovery_rules: Vec::new(),
        admissions: admissions.clone(),
        prior_events: registered
            .normalized_events
            .iter()
            .map(prior_event)
            .collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v1_registry::NewOwner {
                node: parent,
                label: labelhash,
                owner: DIVERGED_OWNER.parse()?,
            }
            .encode_log_data(),
            2,
            0,
            REGISTRY,
        )],
    })?;
    assert!(diverged.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound" && event.resource_id == Some(registry_resource)
    }));
    assert!(
        diverged
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "PermissionChanged"),
        "the divergence must perform the registrar-to-registry permission transition"
    );

    let mut prior_events = registered
        .normalized_events
        .iter()
        .chain(&diverged.normalized_events)
        .map(prior_event)
        .collect::<Vec<_>>();
    prior_events.sort_by_key(|event| event.block_timestamp);
    let lapsed = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules: Vec::new(),
        admissions,
        prior_events,
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "release-after-divergence".to_owned(),
            block_number: 3,
            block_timestamp: OffsetDateTime::UNIX_EPOCH
                + time::Duration::seconds(42 + 90 * 24 * 60 * 60 + 1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: Vec::new(),
    })?;

    let releases = lapsed
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationReleased")
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].resource_id, Some(registrar_resource));
    assert_eq!(
        releases[0].logical_name_id.as_deref(),
        Some(format!("ens:{child}").as_str())
    );
    assert!(lapsed.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "PermissionChanged" | "SurfaceUnbound" | "SurfaceBound" | "AuthorityEpochChanged"
        )
    }));
    assert!(lapsed.binding_closures.is_empty());
    assert!(lapsed.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn wrapper_transfers_fan_out_only_supported_moves_with_identity() -> anyhow::Result<()> {
    let labels = vec!["wrapped".to_owned(), "eth".to_owned()];
    let node = super::common::namehash(&labels).parse::<B256>()?;
    let token_id = U256::from_be_slice(node.as_slice());
    let operator: Address = "0x0000000000000000000000000000000000000001".parse()?;
    let from: Address = "0x0000000000000000000000000000000000000002".parse()?;
    let to: Address = "0x0000000000000000000000000000000000000003".parse()?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            27,
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
                        "SurfaceBound",
                        "AuthorityEpochChanged",
                    ],
                ),
                (
                    "TransferBatch",
                    "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
                (
                    "TransferSingle",
                    "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                    &["name_wrapper"],
                    &["TokenControlTransferred"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(27, "name_wrapper")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                NameWrapped {
                    node,
                    name: b"\x07wrapped\x03eth\0".to_vec().into(),
                    owner: from,
                    fuses: 1,
                    expiry: 42,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferBatch {
                    operator,
                    from,
                    to,
                    ids: vec![token_id, U256::from(999)],
                    values: vec![U256::from(1), U256::ZERO],
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferSingle {
                    operator,
                    from: Address::ZERO,
                    to,
                    id: token_id,
                    value: U256::from(1),
                }
                .encode_log_data(),
                1,
                2,
                CONTRACT,
            ),
            raw_at(
                v2_registry::TransferSingle {
                    operator,
                    from,
                    to,
                    id: token_id,
                    value: U256::from(2),
                }
                .encode_log_data(),
                1,
                3,
                CONTRACT,
            ),
        ],
    })?;

    let transfers = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "TokenControlTransferred")
        .collect::<Vec<_>>();
    assert_eq!(transfers.len(), 2, "one wrap transition plus one transfer");
    let transfer = transfers
        .iter()
        .find(|event| event.after_state["source_event"] == "TransferBatch")
        .expect("supported batch transfer");
    assert!(transfer.logical_name_id.is_some());
    assert!(transfer.resource_id.is_some());
    Ok(())
}

#[test]
fn basenames_resolver_signatures_match_all_emitters() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            16,
            "basenames",
            "basenames_base_resolver",
            &[(
                "VersionChanged",
                "event VersionChanged(bytes32 indexed node, uint64 newVersion)",
                &[],
                &["RecordVersionChanged"],
            )],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver::VersionChanged {
                node: B256::repeat_byte(0x22),
                newVersion: 1,
            }
            .encode_log_data(),
            1,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;

    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "RecordVersionChanged")
            .count(),
        1
    );
    Ok(())
}

#[test]
fn declared_approval_grants_revocations_and_clears_are_decode_only() -> anyhow::Result<()> {
    let owner = CONTRACT.parse::<Address>()?;
    let operator = "0x0000000000000000000000000000000000000043".parse::<Address>()?;
    let zero = Address::ZERO;
    let approval = [operator, zero].map(|approved| {
        approvals::Approval {
            owner,
            approved,
            tokenId: U256::from(7),
        }
        .encode_log_data()
    });
    let approved = [true, false].map(|approved| {
        approvals::Approved {
            owner,
            node: B256::repeat_byte(0x11),
            delegate: operator,
            approved,
        }
        .encode_log_data()
    });
    let cases = [
        (
            "ens_v1_registrar_l1",
            "registrar",
            "Approval",
            "event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId)",
            approval.as_slice(),
        ),
        (
            "basenames_base_registrar",
            "registrar",
            "Approval",
            "event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId)",
            approval.as_slice(),
        ),
        (
            "ens_v1_wrapper_l1",
            "name_wrapper",
            "Approval",
            "event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId)",
            approval.as_slice(),
        ),
        (
            "ens_v1_resolver_l1",
            "public_resolver",
            "Approved",
            "event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved)",
            approved.as_slice(),
        ),
        (
            "basenames_base_resolver",
            "resolver",
            "Approved",
            "event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved)",
            approved.as_slice(),
        ),
    ];
    for (manifest_id, (source_family, role, name, fragment, encoded)) in (100_i64..).zip(cases) {
        let namespace = if source_family.starts_with("basenames_") {
            "basenames"
        } else {
            "ens"
        };
        let output = interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest_with_events(
                manifest_id,
                namespace,
                source_family,
                &[(name, fragment, &[role], &[])],
            )],
            discovery_rules: Vec::new(),
            admissions: vec![admission(manifest_id, role)],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: encoded.iter().cloned().map(raw).collect(),
        })?;
        assert_eq!(
            output,
            BatchOutput::default(),
            "{source_family} {name} must not mutate interpretation state"
        );
    }
    Ok(())
}

#[test]
fn registry_approval_for_all_emits_account_permission_state() -> anyhow::Result<()> {
    let owner = CONTRACT.parse::<Address>()?;
    let operator = "0x0000000000000000000000000000000000000043".parse::<Address>()?;
    for (manifest_id, source_family, namespace) in [
        (111, "ens_v1_registry_l1", "ens"),
        (112, "basenames_base_registry", "basenames"),
    ] {
        let logs = [true, false].map(|approved| {
            raw(approvals::ApprovalForAll {
                owner,
                operator,
                approved,
            }
            .encode_log_data())
        });
        let output = interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest_with_events(
                manifest_id,
                namespace,
                source_family,
                &[(
                    "ApprovalForAll",
                    "event ApprovalForAll(address indexed owner, address indexed operator, bool approved)",
                    &["registry"],
                    &[],
                )],
            )],
            discovery_rules: Vec::new(),
            admissions: vec![admission(manifest_id, "registry")],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: logs.into_iter().collect(),
        })?;
        assert_eq!(output.normalized_events.len(), 2);
        let grant = &output.normalized_events[0];
        let revoke = &output.normalized_events[1];
        assert_eq!(grant.event_kind, "AccountPermissionChanged");
        assert_eq!(grant.derivation_kind, "standard_approval");
        assert!(grant.logical_name_id.is_none() && grant.resource_id.is_none());
        assert_eq!(
            grant.after_state["effective_powers"],
            json!(["registry_control"])
        );
        assert_eq!(revoke.before_state["approved"], json!(true));
        assert_eq!(revoke.after_state["approved"], json!(false));
        assert_eq!(revoke.after_state["effective_powers"], json!([]));
    }
    Ok(())
}

#[test]
fn resolver_approval_cannot_use_match_all_fallback() -> anyhow::Result<()> {
    let encoded = approvals::Approved {
        owner: CONTRACT.parse()?,
        node: B256::repeat_byte(0x22),
        delegate: "0x0000000000000000000000000000000000000043".parse()?,
        approved: true,
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            108,
            "ens_v1_resolver_l1",
            "Approved",
            "event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved)",
            &["public_resolver"],
            &[],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            encoded,
            1,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;
    assert_eq!(output, BatchOutput::default());
    Ok(())
}

#[test]
fn approval_admission_rejects_unknown_output_and_malformed_logs() -> anyhow::Result<()> {
    let unsupported = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            109,
            "ens_v1_registry_l1",
            "UnknownApproval",
            "event UnknownApproval(address indexed owner)",
            &["registry"],
            &[],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(109, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: Vec::new(),
    })
    .expect_err("an arbitrary empty-output signature must remain unsupported");
    assert!(
        unsupported
            .to_string()
            .contains("has no typed schema-v2 adapter")
    );

    let topic0 = format!(
        "{}",
        keccak256("ApprovalForAll(address,address,bool)".as_bytes())
    );
    let malformed = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            110,
            "ens_v1_registry_l1",
            "ApprovalForAll",
            "event ApprovalForAll(address indexed owner, address indexed operator, bool approved)",
            &["registry"],
            &[],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(110, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_with_topic0(topic0)],
    })
    .expect_err("a malformed declared approval must remain fatal");
    assert!(
        format!("{malformed:#}").contains("ApprovalForAll log is malformed"),
        "unexpected error: {malformed:#}"
    );
    Ok(())
}

#[test]
fn ens_v2_shared_resolver_signature_requires_address_admission() -> anyhow::Result<()> {
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            55,
            "ens_v2_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            resolver_name::NameChanged {
                node: B256::repeat_byte(0x55),
                name: "unadmitted.eth".to_owned(),
            }
            .encode_log_data(),
            1,
            0,
            "0x0000000000000000000000000000000000000099",
        )],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "RecordChanged")
    );
    Ok(())
}

#[test]
fn equal_alias_endpoints_emit_one_name_preimage_observation() -> anyhow::Result<()> {
    let encoded_name = b"\x05alice\x03eth\0".to_vec();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            56,
            "ens_v2_resolver_l1",
            "AliasChanged",
            "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
            &[],
            &["AliasChanged", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(56, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v2_resolver::AliasChanged {
            indexedFromName: keccak256(&encoded_name),
            indexedToName: keccak256(&encoded_name),
            fromName: encoded_name.clone().into(),
            toName: encoded_name.into(),
        }
        .encode_log_data())],
    })?;

    assert_eq!(output.name_surfaces.len(), 1);
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "AliasChanged")
            .count(),
        1
    );
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "PreimageObserved")
            .count(),
        1
    );
    let identities = output
        .normalized_events
        .iter()
        .map(|event| &event.event_identity)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(identities.len(), output.normalized_events.len());
    Ok(())
}

#[test]
fn hostile_alias_endpoints_emit_shadow_preimages() -> anyhow::Result<()> {
    let from_label = vec![0xff];
    let to_label = b"a\0b".to_vec();
    let from_name = vec![1, 0xff, 3, b'e', b't', b'h', 0];
    let to_name = vec![3, b'a', 0, b'b', 3, b'e', b't', b'h', 0];
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            74,
            "ens_v2_resolver_l1",
            "AliasChanged",
            "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
            &[],
            &["AliasChanged", "PreimageObserved"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(74, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(v2_resolver::AliasChanged {
            indexedFromName: keccak256(&from_name),
            indexedToName: keccak256(&to_name),
            fromName: from_name.into(),
            toName: to_name.into(),
        }
        .encode_log_data())],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "AliasChanged")
    );
    for (raw_label, namehash) in [
        (
            from_label.as_slice(),
            super::common::namehash_raw([from_label.as_slice(), b"eth"].into_iter()),
        ),
        (
            to_label.as_slice(),
            super::common::namehash_raw([to_label.as_slice(), b"eth"].into_iter()),
        ),
    ] {
        assert!(output.name_surfaces.iter().any(|surface| {
            surface.namehash == namehash && surface.visibility_state == "shadow"
        }));
        assert!(
            output
                .label_preimages
                .iter()
                .any(|preimage| preimage.raw_label == raw_label)
        );
    }
    assert!(output.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn malformed_dns_wire_names_emit_no_normalized_identity_observation() -> anyhow::Result<()> {
    let malformed = vec![3, b'a', 0];
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest_with_events(
            77,
            "ens",
            "ens_v2_resolver_l1",
            &[
                (
                    "AliasChanged",
                    "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
                    &[],
                    &["AliasChanged", "PreimageObserved"],
                ),
                (
                    "NamedResource",
                    "event NamedResource(uint256 indexed resource, bytes name)",
                    &[],
                    &["PreimageObserved"],
                ),
            ],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(77, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_resolver::AliasChanged {
                    indexedFromName: keccak256(&malformed),
                    indexedToName: keccak256([]),
                    fromName: malformed.clone().into(),
                    toName: Vec::new().into(),
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_resolver::NamedResource {
                    resource: U256::from(1),
                    name: malformed.into(),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;

    assert!(output.normalized_events.is_empty());
    assert!(output.name_surfaces.is_empty());
    assert!(output.label_preimages.is_empty());
    Ok(())
}

#[test]
fn non_utf8_named_resource_emits_shadow_without_resolver_hint() -> anyhow::Result<()> {
    assert_named_resource_shadow_without_hint(vec![0xff], None)
}

#[test]
fn normalization_changed_named_resource_does_not_seed_resolver_hint() -> anyhow::Result<()> {
    assert_named_resource_shadow_without_hint(b"Alice".to_vec(), Some("Alice"))
}

#[test]
fn nul_named_text_key_remains_lossless_in_later_permission_hint() -> anyhow::Result<()> {
    let encoded_name = b"\x05alice\x03eth\0".to_vec();
    let raw_key = b"x\0y";
    let resource = U256::from(8);
    let manifest = manifest_with_events(
        76,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "NamedTextResource",
                "event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key)",
                &[],
                &["PreimageObserved"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
        ],
    );
    let named = with_topic0(
        raw_resolver_strings::RawNamedTextResource {
            resource,
            name: encoded_name.into(),
            keyHash: keccak256(raw_key),
            key: raw_key.to_vec().into(),
        }
        .encode_log_data(),
        v2_resolver::NamedTextResource::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(76, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(named, 1, 0, CONTRACT),
            raw_at(
                v2_resolver::EACRolesChanged {
                    resource,
                    account: CONTRACT.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1),
                }
                .encode_log_data(),
                1,
                1,
                CONTRACT,
            ),
        ],
    })?;

    let permission = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged")
        .expect("permission using named-text hint");
    assert_eq!(
        permission.after_state["selector"]["key"],
        json!({"encoding":"hex","bytes":"0x780079"})
    );
    Ok(())
}

fn assert_named_resource_shadow_without_hint(
    raw_label: Vec<u8>,
    decoded_label: Option<&str>,
) -> anyhow::Result<()> {
    let mut encoded_name = vec![u8::try_from(raw_label.len())?];
    encoded_name.extend_from_slice(&raw_label);
    encoded_name.extend_from_slice(&[3, b'e', b't', b'h', 0]);
    let resource = U256::from(7);
    let manifest = manifest_with_events(
        75,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "NamedResource",
                "event NamedResource(uint256 indexed resource, bytes name)",
                &[],
                &["PreimageObserved"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
        ],
    );
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(75, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_resolver::NamedResource {
                resource,
                name: encoded_name.into(),
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let namehash = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    let surface = first
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == namehash)
        .expect("named-resource shadow identity");
    assert_eq!(surface.visibility_state, "shadow");
    let preimage = first
        .label_preimages
        .iter()
        .find(|preimage| preimage.raw_label == raw_label)
        .expect("named-resource raw preimage");
    assert_eq!(preimage.decoded_label.as_deref(), decoded_label);
    assert!(!preimage.normalized_under_version);
    assert!(first.surface_bindings.is_empty());

    let second = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(75, "resolver")],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_resolver::EACRolesChanged {
                resource,
                account: CONTRACT.parse()?,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    })?;
    assert!(second.normalized_events.iter().any(|event| {
        event.event_kind == "PermissionChanged" && event.logical_name_id.is_none()
    }));
    assert!(second.surface_bindings.is_empty());
    Ok(())
}

#[test]
fn malformed_manifest_declared_log_stays_fatal_with_family_and_position_context() {
    let encoded = v2_registry::TokenResource {
        tokenId: U256::from(1),
        resource: U256::from(2),
    }
    .encode_log_data();
    let mut raw = raw(encoded);
    raw.topics.push(format!("{:#x}", B256::repeat_byte(0xff)));
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            76,
            "ens_v2_registry_l1",
            "TokenResource",
            "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
            &["registry"],
            &["TokenResourceLinked"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(76, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw],
    })
    .expect_err("a malformed log from a manifest-declared emitter stays fatal");

    assert!(format!("{error:#}").contains(
        "ens_v2_registry_l1 adapter failed for raw log block-1:0: TokenResource log is malformed"
    ));
}

#[test]
fn malformed_discovery_admitted_log_is_skipped_and_recorded() -> anyhow::Result<()> {
    const ANNOUNCED: &str = "0x0000000000000000000000000000000000000098";
    let encoded = v2_registry::TokenResource {
        tokenId: U256::from(1),
        resource: U256::from(2),
    }
    .encode_log_data();
    let mut raw = raw_at(encoded, 1, 0, ANNOUNCED);
    raw.topics.push(format!("{:#x}", B256::repeat_byte(0xff)));
    let mut announced = admission(77, "registry");
    announced.address = ANNOUNCED.to_owned();
    announced.contract_instance_id = super::common::contract_id(CHAIN, ANNOUNCED);
    announced.role = None;
    announced.discovery_edge_kind = Some("registry_announcement".to_owned());
    announced.discovery_from_contract_instance_id = Some(announced.contract_instance_id);
    announced.discovery_observation_key = Some("registry-announcement:fixture".to_owned());
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            77,
            "ens_v2_registry_l1",
            "TokenResource",
            "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
            &["registry"],
            &["TokenResourceLinked"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![announced],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw],
    })?;

    assert_eq!(output.decode_skips.len(), 1);
    let skip = &output.decode_skips[0];
    assert_eq!(skip.chain_id, CHAIN);
    assert_eq!(skip.block_hash, "block-1");
    assert_eq!(skip.block_number, 1);
    assert_eq!(skip.transaction_hash, "transaction-1");
    assert_eq!(skip.log_index, 0);
    assert_eq!(skip.emitting_address, ANNOUNCED);
    assert_eq!(skip.source_family, "ens_v2_registry_l1");
    assert_eq!(
        skip.selection_topic0,
        format!("{:#x}", v2_registry::TokenResource::SIGNATURE_HASH)
    );
    assert!(!skip.match_all);
    assert_eq!(skip.decode_context, "TokenResource log is malformed");
    let mut state_output = output.clone();
    state_output.decode_skips.clear();
    assert_eq!(state_output, BatchOutput::default());
    Ok(())
}

#[test]
fn malformed_skip_matches_omitting_log_with_retained_v2_state() -> anyhow::Result<()> {
    const ANNOUNCED: &str = "0x0000000000000000000000000000000000000098";
    let manifest = || {
        manifest_with_events(
            78,
            "ens",
            "ens_v2_registry_l1",
            &[
                (
                    "LabelRegistered",
                    "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                    &["registry"],
                    &["RegistrationGranted"],
                ),
                (
                    "TokenResource",
                    "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                    &["registry"],
                    &["TokenResourceLinked"],
                ),
            ],
        )
    };
    let announced_admission = || {
        let mut announced = admission(78, "registry");
        announced.address = ANNOUNCED.to_owned();
        announced.contract_instance_id = super::common::contract_id(CHAIN, ANNOUNCED);
        announced.role = None;
        announced.discovery_edge_kind = Some("registry_announcement".to_owned());
        announced.discovery_from_contract_instance_id = Some(announced.contract_instance_id);
        announced.discovery_observation_key =
            Some("registry-announcement:state-fixture".to_owned());
        announced
    };
    let token_id = versioned_token("expiring", 1);
    let sender: Address = CONTRACT.parse()?;
    let setup_input = || BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest()],
        discovery_rules: Vec::new(),
        admissions: vec![announced_admission()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: token_id,
                    labelHash: keccak256(b"expiring"),
                    label: "expiring".to_owned(),
                    owner: sender,
                    expiry: 1,
                    sender,
                }
                .encode_log_data(),
                1,
                0,
                ANNOUNCED,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: token_id,
                    resource: U256::from(2),
                }
                .encode_log_data(),
                1,
                1,
                ANNOUNCED,
            ),
        ],
    };
    let (_, skipped_session) = interpret_test_batch_incremental(setup_input(), None)?;
    let (_, omitted_session) = interpret_test_batch_incremental(setup_input(), None)?;

    let encoded = v2_registry::TokenResource {
        tokenId: token_id,
        resource: U256::from(3),
    }
    .encode_log_data();
    let mut malformed = raw_at(encoded, 2, 0, ANNOUNCED);
    malformed
        .topics
        .push(format!("{:#x}", B256::repeat_byte(0xff)));
    let (mut skipped, skipped_session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest()],
            discovery_rules: Vec::new(),
            admissions: vec![announced_admission()],
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![malformed],
        },
        Some(skipped_session),
    )?;
    let (omitted, omitted_session) = interpret_test_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest()],
            discovery_rules: Vec::new(),
            admissions: vec![announced_admission()],
            prior_events: Vec::new(),
            blocks: vec![test_block(2)],
            raw_logs: Vec::new(),
        },
        Some(omitted_session),
    )?;

    assert_eq!(skipped.decode_skips.len(), 1);
    skipped.decode_skips.clear();
    assert_eq!(skipped, omitted);
    assert_eq!(skipped_session, omitted_session);
    Ok(())
}

#[test]
fn malformed_match_all_log_from_declared_emitter_is_fatal() -> anyhow::Result<()> {
    const DECLARED_RESOLVER: &str = "0x0000000000000000000000000000000000000099";
    let mut declared_resolver = admission(40, "resolver");
    declared_resolver.address = DECLARED_RESOLVER.to_owned();
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            40,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![declared_resolver],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: "transaction-1".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: DECLARED_RESOLVER.to_owned(),
            topics: vec![format!("{:#x}", resolver::AddrChanged::SIGNATURE_HASH)],
            data: vec![0x01],
        }],
    })
    .expect_err("a malformed log from a declared emitter must halt interpretation");

    assert!(format!("{error:#}").contains(
        "ens_v1_resolver_l1 adapter failed for raw log block-1:0: AddrChanged log is malformed"
    ));
    Ok(())
}

#[test]
fn malformed_match_all_lookalike_from_undeclared_emitter_is_skipped_and_recorded()
-> anyhow::Result<()> {
    const UNDECLARED_RESOLVER: &str = "0x000000000000000000000000000000000000009a";
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            40,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: "transaction-1".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: UNDECLARED_RESOLVER.to_owned(),
            topics: vec![format!("{:#x}", resolver::AddrChanged::SIGNATURE_HASH)],
            data: vec![0x01],
        }],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "RecordChanged")
    );
    assert_eq!(output.decode_skips.len(), 1);
    let skip = &output.decode_skips[0];
    assert_eq!(skip.chain_id, CHAIN);
    assert_eq!(skip.block_hash, "block-1");
    assert_eq!(skip.block_number, 1);
    assert_eq!(skip.transaction_hash, "transaction-1");
    assert_eq!(skip.log_index, 0);
    assert_eq!(skip.emitting_address, UNDECLARED_RESOLVER);
    assert_eq!(skip.source_family, "ens_v1_resolver_l1");
    assert_eq!(
        skip.selection_topic0,
        format!("{:#x}", resolver::AddrChanged::SIGNATURE_HASH)
    );
    assert!(skip.match_all);
    assert_eq!(skip.decode_context, "AddrChanged log is malformed");
    let mut state_output = output.clone();
    state_output.decode_skips.clear();
    assert_eq!(state_output, BatchOutput::default());
    Ok(())
}

#[test]
fn malformed_log_from_declared_and_discovery_admitted_emitter_is_fatal() -> anyhow::Result<()> {
    const DUAL_ADMITTED_RESOLVER: &str = "0x000000000000000000000000000000000000009b";
    let mut declared = admission(41, "resolver");
    declared.address = DUAL_ADMITTED_RESOLVER.to_owned();
    declared.contract_instance_id = super::common::contract_id(CHAIN, DUAL_ADMITTED_RESOLVER);
    let mut discovered = declared.clone();
    discovered.role = None;
    discovered.discovery_edge_kind = Some("resolver".to_owned());
    discovered.discovery_from_contract_instance_id = Some(discovered.contract_instance_id);
    discovered.discovery_observation_key = Some("resolver:dual-admission".to_owned());
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            41,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![declared, discovered],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![RawLogInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: "transaction-1".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: DUAL_ADMITTED_RESOLVER.to_owned(),
            topics: vec![format!("{:#x}", resolver::AddrChanged::SIGNATURE_HASH)],
            data: vec![0x01],
        }],
    })
    .expect_err("direct manifest declaration must outrank discovery skip posture");

    assert!(format!("{error:#}").contains(
        "ens_v1_resolver_l1 adapter failed for raw log block-1:0: AddrChanged log is malformed"
    ));
    Ok(())
}

#[test]
fn non_malformed_error_from_undeclared_match_all_selection_is_fatal() -> anyhow::Result<()> {
    const UNDECLARED_RESOLVER: &str = "0x000000000000000000000000000000000000009c";
    let encoded = resolver::AddrChanged {
        node: B256::repeat_byte(0x42),
        a: UNDECLARED_RESOLVER.parse()?,
    }
    .encode_log_data();
    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            42,
            "ens_v1_resolver_l1",
            "AddrChanged",
            "event AddrChanged(bytes32 indexed node, address a)",
            &[],
            &[],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(encoded, 1, 0, UNDECLARED_RESOLVER)],
    })
    .expect_err("only malformed ABI decode errors may take the skip path");

    assert!(format!("{error:#}").contains(
        "ens_v1_resolver_l1 adapter failed for raw log block-1:0: manifest event AddrChanged(bytes32,address) for ens_v1_resolver_l1 does not declare required normalized event RecordChanged"
    ));
    Ok(())
}

#[test]
fn detached_resolver_loses_address_scoped_admission_in_the_same_batch() -> anyhow::Result<()> {
    const OLD_RESOLVER: &str = "0x0000000000000000000000000000000000000050";
    const NEW_RESOLVER: &str = "0x0000000000000000000000000000000000000051";
    let registry_id = Uuid::from_u128(50);
    let old_resolver_id = Uuid::from_u128(51);
    let mut registry_admission = admission(50, "registry");
    registry_admission.contract_instance_id = registry_id;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            manifest(
                50,
                "ens_v2_registry_l1",
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            manifest(
                51,
                "ens_v2_resolver_l1",
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
        ],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 50,
            edge_kind: "resolver".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "protocol_event".to_owned(),
        }],
        admissions: vec![
            registry_admission,
            AddressAdmissionInput {
                address: OLD_RESOLVER.to_owned(),
                contract_instance_id: old_resolver_id,
                source_manifest_id: Some(50),
                role: None,
                discovery_edge_kind: Some("resolver".to_owned()),
                discovery_from_contract_instance_id: Some(registry_id),
                discovery_observation_key: Some(format!("resolver:{CONTRACT}:0x0")),
                active_from_block: Some(0),
                active_to_block: None,
            },
        ],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: U256::from(7),
                    resolver: NEW_RESOLVER.parse()?,
                    sender: CONTRACT.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                CONTRACT,
            ),
            raw_at(
                v2_resolver::EACRolesChanged {
                    resource: U256::from(8),
                    account: CONTRACT.parse()?,
                    oldRoleBitmap: U256::ZERO,
                    newRoleBitmap: U256::from(1),
                }
                .encode_log_data(),
                1,
                1,
                OLD_RESOLVER,
            ),
        ],
    })?;

    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "PermissionChanged"),
        "a resolver detached at an earlier log position must lose address-scoped admission"
    );
    Ok(())
}

#[test]
fn restored_reservation_does_not_become_a_registration_on_resource_link() -> anyhow::Result<()> {
    let manifest = manifest_with_events(
        52,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationReserved"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
        ],
    );
    let token = U256::from(7);
    let reserved = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(52, "registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::LabelReserved {
                tokenId: token,
                labelHash: keccak256(b"reserved"),
                label: "reserved".to_owned(),
                expiry: 1_000,
                sender: CONTRACT.parse()?,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    })?;
    let linked = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: vec![admission(52, "registry")],
        prior_events: reserved.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: vec![raw_at(
            v2_registry::TokenResource {
                tokenId: token,
                resource: U256::from(8),
            }
            .encode_log_data(),
            2,
            0,
            CONTRACT,
        )],
    })?;

    assert!(linked.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "AuthorityTransferred"
        )
    }));
    Ok(())
}

#[test]
fn nul_text_value_is_retained_as_tagged_raw_bytes() -> anyhow::Result<()> {
    let key = b"url";
    let raw_value = b"a\0b";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x31),
            indexedKey: keccak256(key),
            key: key.to_vec().into(),
            value: raw_value.to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            53,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("NUL text value observation");
    assert_eq!(event.after_state["selector_key"], "url");
    assert_eq!(
        event.after_state["value"],
        json!({"encoding":"hex","bytes":"0x610062"})
    );
    Ok(())
}

#[test]
fn nul_text_key_hashes_raw_bytes_and_uses_a_lossless_selector() -> anyhow::Result<()> {
    let raw_key = b"x\0y";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x32),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"ok".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            54,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("NUL text key observation");
    assert_eq!(event.after_state["record_key"], "text_opaque:0x780079");
    assert_eq!(event.after_state["record_family"], "text_opaque");
    assert_eq!(event.after_state["selector_key"], "0x780079");
    assert_eq!(
        event.after_state["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x780079"})
    );
    assert_eq!(event.after_state["value"], "ok");
    Ok(())
}

#[test]
fn whitespace_text_key_is_retained_as_an_opaque_record_across_families() -> anyhow::Result<()> {
    let raw_key = b" ";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x33),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"hello".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    for (manifest_id, namespace, source_family) in [
        (81, "ens", "ens_v1_resolver_l1"),
        (82, "basenames", "basenames_base_resolver"),
        (83, "ens", "ens_v2_resolver_l1"),
    ] {
        let output = interpret_test_batch(BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest_with_events(
                manifest_id,
                namespace,
                source_family,
                &[(
                    "TextChanged",
                    "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                    &[],
                    &["RecordChanged"],
                )],
            )],
            discovery_rules: Vec::new(),
            admissions: (source_family == "ens_v2_resolver_l1")
                .then(|| admission(manifest_id, "resolver"))
                .into_iter()
                .collect(),
            prior_events: Vec::new(),
            blocks: Vec::new(),
            raw_logs: vec![raw(encoded.clone())],
        })?;

        let event = output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == "RecordChanged")
            .expect("whitespace text key observation");
        assert_eq!(event.after_state["record_key"], "text_opaque:0x20");
        assert_eq!(event.after_state["selector_key"], "0x20");
        assert_eq!(event.after_state["value"], "hello");
    }
    Ok(())
}

#[test]
fn leading_tilde_text_key_remains_a_plain_projection_selector() -> anyhow::Result<()> {
    let raw_key = b"~url";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x34),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"ok".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            82,
            "ens_v1_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("leading-tilde text key observation");
    assert_eq!(event.after_state["record_key"], "text:~url");
    assert_eq!(event.after_state["record_family"], "text");
    assert_eq!(event.after_state["selector_key"], "~url");
    assert!(event.after_state.get("raw_selector_key").is_none());
    Ok(())
}

#[test]
fn nul_v2_text_key_uses_a_projection_safe_opaque_selector() -> anyhow::Result<()> {
    let raw_key = b"v2\0key";
    let encoded = with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: B256::repeat_byte(0x35),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            value: b"ok".to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            83,
            "ens_v2_resolver_l1",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(83, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("ENSv2 NUL text key observation");
    assert_eq!(event.after_state["record_family"], "text_opaque");
    assert_eq!(event.after_state["selector_key"], "0x7632006b6579");
    assert_eq!(
        event.after_state["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x7632006b6579"})
    );
    Ok(())
}

#[test]
fn nul_data_key_uses_a_projection_safe_opaque_selector() -> anyhow::Result<()> {
    let raw_key = b"data\0key";
    let encoded = with_topic0(
        raw_resolver_strings::RawDataChanged {
            node: B256::repeat_byte(0x36),
            indexedKey: keccak256(raw_key),
            key: raw_key.to_vec().into(),
            indexedData: B256::repeat_byte(0x37),
        }
        .encode_log_data(),
        resolver_strings::DataChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            84,
            "ens_v1_resolver_l1",
            "DataChanged",
            "event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("NUL data key observation");
    assert_eq!(event.after_state["record_family"], "data_opaque");
    assert_eq!(event.after_state["selector_key"], "0x64617461006b6579");
    assert_eq!(
        event.after_state["raw_selector_key"],
        json!({"encoding":"hex","bytes":"0x64617461006b6579"})
    );
    Ok(())
}

#[test]
fn nul_resolver_name_emits_a_shadow_with_exact_raw_preimage() -> anyhow::Result<()> {
    assert_hostile_resolver_name(b"bad\0name".to_vec(), 55)
}

#[test]
fn invalid_utf8_resolver_name_emits_no_lossy_fabricated_hash() -> anyhow::Result<()> {
    let raw_label = vec![0xff, 0xfe];
    let output = hostile_resolver_name_output(raw_label.clone(), 56)?;
    let raw_namehash = super::common::namehash_raw([raw_label.as_slice()].into_iter());
    assert_shadow_output(&output, &raw_namehash, &raw_label, None);
    assert!(
        output
            .label_preimages
            .iter()
            .all(|preimage| preimage.raw_label != "��".as_bytes()),
        "lossy UTF-8 replacement bytes must never become a chain preimage"
    );
    let event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("invalid UTF-8 name record");
    assert_eq!(
        event.after_state["raw_name"],
        json!({"encoding":"hex","bytes":"0xfffe"})
    );
    Ok(())
}

#[test]
fn invalid_utf8_reverse_name_emits_exact_shadow_preimage() -> anyhow::Result<()> {
    let raw_name = vec![0xff];
    let encoded = with_topic0(
        raw_resolver_strings::RawNameForAddrChanged {
            addr: CONTRACT.parse()?,
            name: raw_name.clone().into(),
        }
        .encode_log_data(),
        resolver_strings::NameForAddrChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            78,
            "basenames_base_primary",
            "NameForAddrChanged",
            "event NameForAddrChanged(address indexed addr, string name)",
            &["reverse_registrar"],
            &["ReverseChanged", "RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(78, "reverse_registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash = super::common::namehash_raw([raw_name.as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_name, None);
    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("reverse name record");
    assert_eq!(record.after_state["raw_name"], json!(null));
    assert_eq!(
        record.after_state["raw_name_bytes"],
        json!({"encoding":"hex","bytes":"0xff"})
    );
    Ok(())
}

#[test]
fn nul_v2_resolver_name_emits_exact_shadow_preimage() -> anyhow::Result<()> {
    let raw_name = b"bad\0name".to_vec();
    let encoded = with_topic0(
        raw_resolver_strings::RawNameChanged {
            node: B256::repeat_byte(0x79),
            name: raw_name.clone().into(),
        }
        .encode_log_data(),
        resolver_name::NameChanged::SIGNATURE_HASH,
    );
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            79,
            "ens_v2_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(79, "resolver")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash = super::common::namehash_raw([raw_name.as_slice()].into_iter());
    assert_shadow_output(&output, &namehash, &raw_name, None);
    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("ENSv2 hostile name record");
    assert_eq!(
        record.after_state["raw_name"],
        json!({"encoding":"hex","bytes":"0x626164006e616d65"})
    );
    Ok(())
}

#[test]
fn trailing_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b"alice.eth.", 85)
}

#[test]
fn leading_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b".alice.eth", 86)
}

#[test]
fn consecutive_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b"a..b", 87)
}

#[test]
fn bare_dot_resolver_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    assert_empty_segment_resolver_name(b".", 88)
}

#[test]
fn empty_registrar_name_emits_shadow_without_empty_preimage() -> anyhow::Result<()> {
    let raw_label = b"";
    let encoded = NameRegistered {
        name: String::new(),
        label: keccak256(raw_label),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            89,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(89, "registrar")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })?;

    let namehash = super::common::namehash_raw([raw_label.as_slice(), b"eth"].into_iter());
    assert_empty_segment_shadow(&output, &namehash, ".eth");
    Ok(())
}

fn assert_empty_segment_resolver_name(raw_name: &[u8], manifest_id: i64) -> anyhow::Result<()> {
    let output = hostile_resolver_name_output(raw_name.to_vec(), manifest_id)?;
    let raw_labels = raw_name.split(|byte| *byte == b'.').collect::<Vec<_>>();
    let namehash = super::common::namehash_raw(raw_labels.iter().copied());
    assert_empty_segment_shadow(&output, &namehash, std::str::from_utf8(raw_name)?);
    Ok(())
}

fn assert_empty_segment_shadow(output: &BatchOutput, namehash: &str, raw_name: &str) {
    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == namehash)
        .expect("empty-segment shadow identity");
    assert_eq!(surface.visibility_state, "shadow");
    assert_eq!(surface.raw_name, raw_name);
    assert!(
        output
            .label_preimages
            .iter()
            .all(|preimage| !preimage.raw_label.is_empty()),
        "empty label content must not reach label_preimages"
    );
    assert!(
        surface.dns_encoded_name.is_empty(),
        "an empty label segment has no valid DNS wire encoding"
    );
}

fn assert_hostile_resolver_name(raw_name: Vec<u8>, manifest_id: i64) -> anyhow::Result<()> {
    let output = hostile_resolver_name_output(raw_name.clone(), manifest_id)?;
    let raw_labels = raw_name
        .split(|byte| *byte == b'.')
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let raw_namehash = super::common::namehash_raw(raw_labels.iter().map(Vec::as_slice));
    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.namehash == raw_namehash)
        .expect("hostile resolver-name shadow");
    assert_eq!(surface.visibility_state, "shadow");
    assert!(output.surface_bindings.is_empty());
    for raw_label in raw_labels {
        assert!(
            output
                .label_preimages
                .iter()
                .any(|preimage| preimage.raw_label == raw_label)
        );
    }
    Ok(())
}

fn hostile_resolver_name_output(
    raw_name: Vec<u8>,
    manifest_id: i64,
) -> anyhow::Result<BatchOutput> {
    let encoded = with_topic0(
        raw_resolver_strings::RawNameChanged {
            node: B256::repeat_byte(0x33),
            name: raw_name.into(),
        }
        .encode_log_data(),
        resolver_name::NameChanged::SIGNATURE_HASH,
    );
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            manifest_id,
            "ens_v1_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![raw(encoded)],
    })
}

#[test]
fn announced_registry_gains_a_suffix_only_after_mutual_parent_agreement() -> anyhow::Result<()> {
    const PARENT: &str = "0x0000000000000000000000000000000000000060";
    const CHILD: &str = "0x0000000000000000000000000000000000000061";
    let parent_label = "sub";
    let child_label = "leaf";
    let parent_token = U256::from(1);
    let child_token = U256::from(2);
    let sender = CONTRACT.parse::<Address>()?;
    let manifest = manifest_with_events(
        41,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "RegistryCreated",
                "event RegistryCreated()",
                &["registry"],
                &["RegistryCreated"],
            ),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut parent_admission = admission(41, "registry");
    parent_admission.address = PARENT.to_owned();
    let logs = vec![
        raw_at(RegistryCreated {}.encode_log_data(), 1, 0, CHILD),
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: parent_token,
                labelHash: keccak256(parent_label.as_bytes()),
                label: parent_label.to_owned(),
                owner: sender,
                expiry: 1_000,
                sender,
            }
            .encode_log_data(),
            2,
            0,
            PARENT,
        ),
        raw_at(
            v2_registry::SubregistryUpdated {
                tokenId: parent_token,
                subregistry: CHILD.parse()?,
                sender,
            }
            .encode_log_data(),
            3,
            0,
            PARENT,
        ),
        raw_at(
            v2_registry::ParentUpdated {
                parent: PARENT.parse()?,
                label: parent_label.to_owned(),
                sender,
            }
            .encode_log_data(),
            4,
            0,
            CHILD,
        ),
        raw_at(
            v2_registry::LabelRegistered {
                tokenId: child_token,
                labelHash: keccak256(child_label.as_bytes()),
                label: child_label.to_owned(),
                owner: sender,
                expiry: 1_000,
                sender,
            }
            .encode_log_data(),
            5,
            0,
            CHILD,
        ),
        raw_at(
            v2_registry::TokenResource {
                tokenId: child_token,
                resource: U256::from(7),
            }
            .encode_log_data(),
            6,
            0,
            CHILD,
        ),
    ];
    let mut later_logs = logs;
    let first_logs = later_logs.drain(..4).collect::<Vec<_>>();
    let first = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest.clone()],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ],
        admissions: vec![parent_admission.clone()],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: first_logs,
    })?;
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 41,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ],
        admissions: vec![
            parent_admission,
            AddressAdmissionInput {
                address: CHILD.to_owned(),
                contract_instance_id: super::common::contract_id(CHAIN, CHILD),
                source_manifest_id: Some(41),
                role: None,
                discovery_edge_kind: Some("registry_announcement".to_owned()),
                discovery_from_contract_instance_id: Some(super::common::contract_id(CHAIN, CHILD)),
                discovery_observation_key: Some(format!("registry-announcement:{CHILD}")),
                active_from_block: Some(1),
                active_to_block: None,
            },
        ],
        prior_events: first.normalized_events.iter().map(prior_event).collect(),
        blocks: Vec::new(),
        raw_logs: later_logs,
    })?;

    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.raw_name == "leaf.sub.eth")
    );
    assert!(output.surface_bindings.iter().any(|binding| {
        output.name_surfaces.iter().any(|surface| {
            surface.raw_name == "leaf.sub.eth" && surface.logical_name_id == binding.logical_name_id
        })
    }));
    Ok(())
}

#[test]
fn mutual_parent_changes_rebind_and_retract_existing_child_resources() -> anyhow::Result<()> {
    const PARENT: &str = "0x0000000000000000000000000000000000000070";
    const CHILD: &str = "0x0000000000000000000000000000000000000071";
    let sender = CONTRACT.parse::<Address>()?;
    let parent_token = U256::from(1);
    let child_token = U256::from(2);
    let manifest = manifest_with_events(
        42,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "RegistryCreated",
                "event RegistryCreated()",
                &["registry"],
                &["RegistryCreated"],
            ),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    );
    let mut parent_admission = admission(42, "registry");
    parent_admission.address = PARENT.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: 42,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: 42,
                edge_kind: "subregistry".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "linked_subregistry_event".to_owned(),
            },
        ],
        admissions: vec![parent_admission],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![
            raw_at(RegistryCreated {}.encode_log_data(), 1, 0, CHILD),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: child_token,
                    labelHash: keccak256(b"leaf"),
                    label: "leaf".to_owned(),
                    owner: sender,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                2,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::TokenResource {
                    tokenId: child_token,
                    resource: U256::from(7),
                }
                .encode_log_data(),
                3,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::LabelRegistered {
                    tokenId: parent_token,
                    labelHash: keccak256(b"sub"),
                    label: "sub".to_owned(),
                    owner: sender,
                    expiry: 1_000,
                    sender,
                }
                .encode_log_data(),
                4,
                0,
                PARENT,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: CHILD.parse()?,
                    sender,
                }
                .encode_log_data(),
                5,
                0,
                PARENT,
            ),
            raw_at(
                v2_registry::ParentUpdated {
                    parent: PARENT.parse()?,
                    label: "sub".to_owned(),
                    sender,
                }
                .encode_log_data(),
                6,
                0,
                CHILD,
            ),
            raw_at(
                v2_registry::SubregistryUpdated {
                    tokenId: parent_token,
                    subregistry: Address::ZERO,
                    sender,
                }
                .encode_log_data(),
                7,
                0,
                PARENT,
            ),
        ],
    })?;

    let surface = output
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name == "leaf.sub.eth")
        .expect("mutual agreement binds the retained child resource");
    assert!(
        output
            .surface_bindings
            .iter()
            .any(|binding| binding.logical_name_id == surface.logical_name_id)
    );
    assert!(output.binding_closures.iter().any(|closure| {
        closure.logical_name_id == surface.logical_name_id && closure.block_number == 7
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id == Some(surface.logical_name_id.clone())
    }));
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceUnbound"
            && event.logical_name_id == Some(surface.logical_name_id.clone())
    }));
    Ok(())
}

#[test]
fn checked_in_manifest_event_corpus_has_typed_schema_v2_adapters() -> anyhow::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests");
    let mut manifest_id = 0i64;
    for environment in ["mainnet", "sepolia"] {
        let repository = bigname_manifests::load_repository(root.join(environment))?;
        for loaded in repository.manifests() {
            manifest_id += 1;
            let manifest = &loaded.manifest;
            let source = super::manifest::decode(ManifestInput {
                manifest_id,
                manifest_version: i64::try_from(manifest.manifest_version)?,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                chain_id: manifest.chain.clone(),
                deployment_label: manifest.deployment_epoch.clone(),
                normalizer_version: manifest.normalizer_version.clone(),
                payload_json: serde_json::to_string(manifest)?,
            })?;
            let rules = manifest
                .discovery_rules
                .iter()
                .map(|rule| DiscoveryRuleInput {
                    manifest_id,
                    edge_kind: rule.edge_kind.clone(),
                    from_role: Some(rule.from_role.clone()),
                    admission: rule.admission.clone(),
                })
                .collect::<Vec<_>>();
            super::protocol::validate_manifest(&source, &rules)?;
        }
    }
    Ok(())
}

#[test]
fn schema_v2_flags_require_the_compiled_manifest_normalizer_version() {
    let mut source = manifest(
        99,
        "ens_v1_registrar_l1",
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &["RegistrationGranted"],
    );
    source.normalizer_version = "ensip15@different".to_owned();

    let error = interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![source],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: Vec::new(),
    })
    .expect_err("the flag engine must reject a differently versioned manifest");

    assert!(error.to_string().contains("schema-v2 label flags use"));
}

#[test]
fn raw_log_without_a_loaded_block_errors_loudly() -> anyhow::Result<()> {
    let encoded = NameRegistered {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        owner: CONTRACT.parse()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let input = BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            63,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: vec![admission(63, "registrar")],
        prior_events: Vec::new(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: vec![raw_at(encoded, 2, 0, CONTRACT)],
    };

    let error = super::interpret_schema_v2_batch(input)
        .expect_err("an unanchored raw log must not be interpreted out of position");
    assert!(
        error
            .to_string()
            .contains("no matching loaded live-lineage block")
    );
    Ok(())
}

#[test]
fn multiple_live_hashes_at_one_height_error_loudly() {
    let error = super::interpret_schema_v2_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest(
            64,
            "ens_v1_registrar_l1",
            "NameRegistered",
            "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            &["registrar"],
            &["RegistrationGranted"],
        )],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: vec![
            RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-a".to_owned(),
                block_number: 1,
                block_timestamp: OffsetDateTime::UNIX_EPOCH,
                canonicality_state: "canonical".to_owned(),
            },
            RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-b".to_owned(),
                block_number: 1,
                block_timestamp: OffsetDateTime::UNIX_EPOCH,
                canonicality_state: "safe".to_owned(),
            },
        ],
        raw_logs: Vec::new(),
    })
    .expect_err("one height cannot have multiple live hashes");
    assert!(error.to_string().contains("multiple live-lineage hashes"));
}

fn manifest(
    manifest_id: i64,
    source_family: &str,
    name: &str,
    fragment: &str,
    emitter_roles: &[&str],
    normalized_events: &[&str],
) -> ManifestInput {
    manifest_with_events(
        manifest_id,
        "ens",
        source_family,
        &[(name, fragment, emitter_roles, normalized_events)],
    )
}

fn interpret_test_batch(mut input: BatchInput) -> anyhow::Result<BatchOutput> {
    if input.blocks.is_empty() {
        let mut blocks = std::collections::BTreeMap::new();
        for raw in &input.raw_logs {
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
        input.blocks = blocks.into_values().collect();
    }
    super::interpret_schema_v2_batch(input)
}

fn interpret_test_batch_incremental(
    mut input: BatchInput,
    session: Option<AdapterSession>,
) -> anyhow::Result<(BatchOutput, AdapterSession)> {
    if input.blocks.is_empty() {
        let mut blocks = std::collections::BTreeMap::new();
        for raw in &input.raw_logs {
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
        input.blocks = blocks.into_values().collect();
    }
    super::prepare_schema_v2_batch_incremental(input, session, StateCacheCapacity::Unlimited)?
        .finish(Vec::new())
}

#[test]
fn bounded_state_cannot_use_a_public_one_call_incremental_helper() {
    let schema_source = include_str!("../schema_v2.rs");
    let session_source = include_str!("session.rs");
    assert!(!schema_source.contains("interpret_schema_v2_batch_incremental,"));
    assert!(!session_source.contains("pub fn interpret_schema_v2_batch_incremental("));
}

fn assert_batch_referential_integrity(
    output: &BatchOutput,
    persisted_resources: &std::collections::BTreeSet<(String, Uuid)>,
    persisted_surfaces: &std::collections::BTreeSet<(String, String)>,
) -> anyhow::Result<()> {
    let available_resources = output
        .resources
        .iter()
        .map(|resource| (resource.chain_id.clone(), resource.resource_id))
        .chain(persisted_resources.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    for event in &output.normalized_events {
        if let Some(resource_id) = event.resource_id {
            anyhow::ensure!(
                available_resources.contains(&(event.chain_id.clone(), resource_id)),
                "normalized event {} references resource {} on chain {} without a batch or persisted resource row",
                event.event_identity,
                resource_id,
                event.chain_id,
            );
        }
    }
    let available_surfaces = output
        .name_surfaces
        .iter()
        .map(|surface| (surface.chain_id.clone(), surface.logical_name_id.clone()))
        .chain(persisted_surfaces.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    for event in &output.normalized_events {
        if let Some(logical_name_id) = event.logical_name_id.as_ref() {
            anyhow::ensure!(
                available_surfaces.contains(&(event.chain_id.clone(), logical_name_id.clone())),
                "normalized event {} references logical name {} on chain {} without a batch or persisted name surface row",
                event.event_identity,
                logical_name_id,
                event.chain_id,
            );
        }
    }
    for binding in &output.surface_bindings {
        anyhow::ensure!(
            available_resources.contains(&(binding.chain_id.clone(), binding.resource_id)),
            "surface binding {} references resource {} on chain {} without a batch or persisted resource row",
            binding.surface_binding_id,
            binding.resource_id,
            binding.chain_id,
        );
        anyhow::ensure!(
            available_surfaces
                .contains(&(binding.chain_id.clone(), binding.logical_name_id.clone(),)),
            "surface binding {} references logical name {} on chain {} without a batch or persisted name surface row",
            binding.surface_binding_id,
            binding.logical_name_id,
            binding.chain_id,
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct SimulatedBinding {
    surface_binding_id: Uuid,
    logical_name_id: String,
    resource_id: Uuid,
    chain_id: String,
    authority_arm: String,
    position: (i64, i64, i64),
    active_from: OffsetDateTime,
    active_to: Option<OffsetDateTime>,
    canonicality_state: String,
}

impl SimulatedBinding {
    fn live(&self) -> bool {
        matches!(
            self.canonicality_state.as_str(),
            "canonical" | "safe" | "finalized"
        )
    }
}

/// Replays the identity binding writer (`crates/interpret/src/write/identity.rs`) over one batch's
/// emitted drafts against an empty table, so an adapter fixture can assert which persisted rows a
/// batch leaves open. Closures are arm-wide over `logical_name_id + chain_id + authority_arm`:
/// nothing narrows them to the token that emitted them.
fn simulate_binding_writer(output: &BatchOutput) -> Vec<SimulatedBinding> {
    enum Operation<'a> {
        Close(&'a BindingClosure),
        Open(&'a SurfaceBinding),
    }
    let index = |provenance: &serde_json::Value, key: &str| {
        provenance
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(-1)
    };
    let mut operations = output
        .binding_closures
        .iter()
        .map(|closure| {
            (
                (
                    closure.block_number,
                    closure.transaction_index,
                    closure.log_index,
                    0,
                ),
                Operation::Close(closure),
            )
        })
        .chain(output.surface_bindings.iter().map(|binding| {
            (
                (
                    binding.block_number,
                    index(&binding.provenance, seam::TRANSACTION_INDEX_KEY),
                    index(&binding.provenance, seam::LOG_INDEX_KEY),
                    1,
                ),
                Operation::Open(binding),
            )
        }))
        .collect::<Vec<_>>();
    operations.sort_by_key(|(order, _)| *order);
    let mut rows: Vec<SimulatedBinding> = Vec::new();
    for ((block_number, transaction_index, log_index, _), operation) in operations {
        let position = (block_number, transaction_index, log_index);
        match operation {
            Operation::Close(closure) => {
                for row in rows.iter_mut().filter(|row| {
                    row.live()
                        && row.logical_name_id == closure.logical_name_id
                        && row.chain_id == closure.chain_id
                        && row.authority_arm == closure.authority_arm
                        && row.position < position
                        && closure.except_surface_binding_id != Some(row.surface_binding_id)
                }) {
                    let clamped = closure
                        .active_to
                        .max(row.active_from + time::Duration::microseconds(1));
                    if row.active_to.is_none_or(|active_to| active_to > clamped) {
                        row.active_to = Some(clamped);
                    }
                }
            }
            Operation::Open(binding) => {
                let peer = |row: &SimulatedBinding| {
                    row.live()
                        && row.logical_name_id == binding.logical_name_id
                        && row.chain_id == binding.chain_id
                        && row.authority_arm == binding.authority_arm
                        && row.surface_binding_id != binding.surface_binding_id
                };
                let predecessor = rows
                    .iter()
                    .filter(|row| peer(row) && row.position < position)
                    .max_by_key(|row| (row.position, row.surface_binding_id))
                    .map(|row| row.active_from);
                let active_from = seam::binding_open_time(binding.active_from, predecessor);
                let successor = rows
                    .iter()
                    .filter(|row| peer(row) && row.position > position)
                    .min_by_key(|row| (row.position, row.surface_binding_id))
                    .map(|row| row.active_from);
                for row in rows.iter_mut().filter(|row| {
                    peer(row)
                        && row.position < position
                        && row.active_from < active_from
                        && row
                            .active_to
                            .is_none_or(|active_to| active_to > active_from)
                }) {
                    row.active_to = Some(active_from);
                }
                rows.push(SimulatedBinding {
                    surface_binding_id: binding.surface_binding_id,
                    logical_name_id: binding.logical_name_id.clone(),
                    resource_id: binding.resource_id,
                    chain_id: binding.chain_id.clone(),
                    authority_arm: binding.authority_arm.clone(),
                    position,
                    active_from,
                    active_to: successor,
                    canonicality_state: binding.canonicality_state.clone(),
                });
            }
        }
    }
    rows
}

type EventSpec<'a> = (&'a str, &'a str, &'a [&'a str], &'a [&'a str]);

fn manifest_with_events(
    manifest_id: i64,
    namespace: &str,
    source_family: &str,
    events: &[EventSpec<'_>],
) -> ManifestInput {
    let events = events
        .iter()
        .map(|(name, fragment, emitter_roles, normalized_events)| {
            json!({
                "name": name,
                "fragment": fragment,
                "emitter_roles": emitter_roles,
                "normalized_events": normalized_events,
            })
        })
        .collect::<Vec<_>>();
    ManifestInput {
        manifest_id,
        manifest_version: 1,
        namespace: namespace.to_owned(),
        source_family: source_family.to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: "fixture".to_owned(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        payload_json: json!({"abi": {"events": events}}).to_string(),
    }
}

fn admission(manifest_id: i64, role: &str) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: CONTRACT.to_owned(),
        contract_instance_id: Uuid::from_u128(u128::try_from(manifest_id).unwrap()),
        source_manifest_id: Some(manifest_id),
        role: Some(role.to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn raw(encoded: alloy_primitives::LogData) -> RawLogInput {
    raw_at(encoded, 1, 0, CONTRACT)
}

fn raw_with_topic0(topic0: String) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: "block-1".to_owned(),
        block_number: 1,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
        canonicality_state: "canonical".to_owned(),
        transaction_hash: "transaction-1".to_owned(),
        transaction_index: 0,
        log_index: 0,
        emitting_address: CONTRACT.to_owned(),
        topics: vec![topic0],
        data: Vec::new(),
    }
}

fn protocol_rule_lookup_producers()
-> anyhow::Result<std::collections::BTreeSet<(String, String, String)>> {
    let schema_v2_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/schema_v2");
    let manifest_events = checked_in_manifest_source_family_events()?;
    let known_families = manifest_events
        .iter()
        .map(|(source_family, _)| source_family.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let adapter_paths = checked_in_protocol_adapter_paths(&schema_v2_root, &known_families)?;
    scope_protocol_rule_lookup_producers(
        &schema_v2_root,
        &protocol_rule_lookup_producer_locations()?,
        &manifest_events,
        &adapter_paths,
    )
}

fn protocol_rule_lookup_producer_locations()
-> anyhow::Result<std::collections::BTreeSet<(std::path::PathBuf, String, String)>> {
    let schema_v2_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/schema_v2");
    let discovery_path = schema_v2_root.join("discovery.rs");
    let discovery_source = std::fs::read_to_string(&discovery_path)?;
    let announcement_edge_kind = registry_announcement_rule_edge_kind(&discovery_source)?;
    let bypass_edge_kinds = rule_lookup_bypass_edge_kinds(&discovery_source)?;
    let sources = read_rust_source_map(&protocol_rule_lookup_source_paths()?)?;
    protocol_rule_lookup_producer_locations_from_sources(
        &sources,
        &announcement_edge_kind,
        &bypass_edge_kinds,
    )
}

fn protocol_rule_lookup_producer_locations_from_sources(
    sources: &std::collections::BTreeMap<std::path::PathBuf, String>,
    announcement_edge_kind: &str,
    bypass_edge_kinds: &std::collections::BTreeSet<String>,
) -> anyhow::Result<std::collections::BTreeSet<(std::path::PathBuf, String, String)>> {
    // This syn pass sees source-level struct/path expressions, including direct, imported, and
    // `type`-aliased variants and direct constructions present in macro tokens. Identifiers
    // created only during macro expansion and aliases supplied through associated types remain
    // invisible because expansion and semantic name resolution are outside this test-side pass.
    let mut locations = std::collections::BTreeSet::new();
    let files = sources
        .iter()
        .map(|(path, source)| {
            Ok((
                path.clone(),
                syn::parse_file(source).with_context(|| {
                    format!("parse protocol producer source {}", path.display())
                })?,
            ))
        })
        .collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?;
    let aliases_by_source = discovery_draft_aliases_by_source(&files);
    for (path, file) in &files {
        let aliases = aliases_by_source
            .get(path)
            .expect("every parsed source has discovery aliases");
        let mut visitor = DiscoveryDraftConstructionVisitor::new(aliases);
        syn::visit::Visit::visit_file(&mut visitor, file);
        for site in visitor.sites {
            let edge_kind = match site.construction {
                DiscoveryDraftConstruction::RegistryAnnouncement => {
                    announcement_edge_kind.to_owned()
                }
                DiscoveryDraftConstruction::Edge(Some(edge_kind)) => edge_kind,
                DiscoveryDraftConstruction::Edge(None) => anyhow::bail!(
                    "protocol producer {} constructs DiscoveryDraft::Edge without a literal edge_kind",
                    path.display(),
                ),
            };
            if bypass_edge_kinds.contains(&edge_kind) {
                continue;
            }
            anyhow::ensure!(
                !site.events.is_empty(),
                "protocol producer {} constructs DiscoveryDraft for {edge_kind} outside a named selected.event arm",
                path.display(),
            );
            locations.extend(
                site.events
                    .into_iter()
                    .map(|event| (path.clone(), event, edge_kind.clone())),
            );
        }
    }
    Ok(locations)
}

#[derive(Clone, Default)]
struct DiscoveryDraftAliases {
    draft_types: std::collections::BTreeSet<String>,
    edge_variants: std::collections::BTreeSet<String>,
    announcement_variants: std::collections::BTreeSet<String>,
    qualified_draft_types: std::collections::BTreeSet<(String, String)>,
    qualified_edge_variants: std::collections::BTreeSet<(String, String)>,
    qualified_announcement_variants: std::collections::BTreeSet<(String, String)>,
}

fn discovery_draft_aliases_by_source(
    files: &std::collections::BTreeMap<std::path::PathBuf, syn::File>,
) -> std::collections::BTreeMap<std::path::PathBuf, DiscoveryDraftAliases> {
    let mut aliases = files
        .iter()
        .map(|(path, file)| (path.clone(), discovery_draft_aliases(file)))
        .collect::<std::collections::BTreeMap<_, _>>();
    loop {
        let mut additions = Vec::new();
        let mut qualified_additions = Vec::new();
        for (path, file) in files {
            let mut imports = Vec::new();
            let mut collector = UseImportCollector {
                imports: &mut imports,
            };
            syn::visit::Visit::visit_file(&mut collector, file);
            for import in imports {
                if !import.glob {
                    if let (Some(local), Some(exporter_path)) = (
                        import.local.clone(),
                        resolve_local_module_source(path, &import.path, files.keys()),
                    ) {
                        if let Some(exported) = aliases.get(&exporter_path) {
                            qualified_additions.push((
                                path.clone(),
                                exported
                                    .draft_types
                                    .iter()
                                    .map(|draft| (local.clone(), draft.clone()))
                                    .collect::<std::collections::BTreeSet<_>>(),
                                exported
                                    .edge_variants
                                    .iter()
                                    .map(|variant| (local.clone(), variant.clone()))
                                    .collect::<std::collections::BTreeSet<_>>(),
                                exported
                                    .announcement_variants
                                    .iter()
                                    .map(|variant| (local.clone(), variant.clone()))
                                    .collect::<std::collections::BTreeSet<_>>(),
                            ));
                        }
                    }
                }
                let (module, imported_name) = if import.glob {
                    (import.path.as_slice(), None)
                } else {
                    let Some((imported_name, module)) = import.path.split_last() else {
                        continue;
                    };
                    (module, Some(imported_name.as_str()))
                };
                let Some(exporter_path) = resolve_local_module_source(path, module, files.keys())
                else {
                    continue;
                };
                let Some(exported) = aliases.get(&exporter_path) else {
                    continue;
                };
                if import.glob {
                    additions.push((
                        path.clone(),
                        exported.draft_types.clone(),
                        exported.edge_variants.clone(),
                        exported.announcement_variants.clone(),
                    ));
                    continue;
                }
                let imported_name = imported_name.expect("non-glob import has a final name");
                let Some(local) = import.local else {
                    continue;
                };
                let draft_types = if exported.draft_types.contains(imported_name) {
                    std::collections::BTreeSet::from([local.clone()])
                } else {
                    std::collections::BTreeSet::new()
                };
                let edge_variants = if exported.edge_variants.contains(imported_name) {
                    std::collections::BTreeSet::from([local.clone()])
                } else {
                    std::collections::BTreeSet::new()
                };
                let announcement_variants =
                    if exported.announcement_variants.contains(imported_name) {
                        std::collections::BTreeSet::from([local])
                    } else {
                        std::collections::BTreeSet::new()
                    };
                additions.push((
                    path.clone(),
                    draft_types,
                    edge_variants,
                    announcement_variants,
                ));
            }

            let mut type_aliases = Vec::new();
            let mut type_alias_collector = TypeAliasCollector {
                aliases: &mut type_aliases,
            };
            syn::visit::Visit::visit_file(&mut type_alias_collector, file);
            for (local, target) in type_aliases {
                let Some((target_name, module)) = target.split_last() else {
                    continue;
                };
                let Some(exporter_path) = resolve_local_module_source(path, module, files.keys())
                else {
                    continue;
                };
                if aliases
                    .get(&exporter_path)
                    .is_some_and(|exported| exported.draft_types.contains(target_name))
                {
                    additions.push((
                        path.clone(),
                        std::collections::BTreeSet::from([local]),
                        std::collections::BTreeSet::new(),
                        std::collections::BTreeSet::new(),
                    ));
                }
            }

            let mut source_paths = Vec::new();
            let mut source_path_collector = RustPathCollector {
                paths: &mut source_paths,
            };
            syn::visit::Visit::visit_file(&mut source_path_collector, file);
            for source_path in source_paths {
                let Some((variant, owner_path)) = source_path.split_last() else {
                    continue;
                };
                if owner_path.is_empty() {
                    continue;
                }
                if matches!(variant.as_str(), "Edge" | "RegistryAnnouncement") {
                    if let Some(exporter_path) =
                        resolve_local_module_source(path, owner_path, files.keys())
                    {
                        if let Some(exported) = aliases.get(&exporter_path) {
                            let module = owner_path
                                .last()
                                .expect("resolved module path has a final segment")
                                .clone();
                            let edge_variants =
                                if variant == "Edge" && exported.edge_variants.contains(variant) {
                                    std::collections::BTreeSet::from([(
                                        module.clone(),
                                        variant.clone(),
                                    )])
                                } else {
                                    std::collections::BTreeSet::new()
                                };
                            let announcement_variants = if variant == "RegistryAnnouncement"
                                && exported.announcement_variants.contains(variant)
                            {
                                std::collections::BTreeSet::from([(module, variant.clone())])
                            } else {
                                std::collections::BTreeSet::new()
                            };
                            qualified_additions.push((
                                path.clone(),
                                std::collections::BTreeSet::new(),
                                edge_variants,
                                announcement_variants,
                            ));
                        }
                    }
                }
                if owner_path.len() < 2 {
                    continue;
                }
                let draft = owner_path
                    .last()
                    .expect("qualified variant has a draft owner");
                let module_path = &owner_path[..owner_path.len() - 1];
                let Some(exporter_path) =
                    resolve_local_module_source(path, module_path, files.keys())
                else {
                    continue;
                };
                if aliases
                    .get(&exporter_path)
                    .is_some_and(|exported| exported.draft_types.contains(draft))
                {
                    qualified_additions.push((
                        path.clone(),
                        std::collections::BTreeSet::from([(
                            module_path
                                .last()
                                .expect("resolved module path has a final segment")
                                .clone(),
                            draft.clone(),
                        )]),
                        std::collections::BTreeSet::new(),
                        std::collections::BTreeSet::new(),
                    ));
                }
            }
        }
        let mut changed = false;
        for (path, draft_types, edge_variants, announcement_variants) in additions {
            let entry = aliases
                .get_mut(&path)
                .expect("alias addition targets a parsed source");
            let before = (
                entry.draft_types.len(),
                entry.edge_variants.len(),
                entry.announcement_variants.len(),
            );
            entry.draft_types.extend(draft_types);
            entry.edge_variants.extend(edge_variants);
            entry.announcement_variants.extend(announcement_variants);
            changed |= before
                != (
                    entry.draft_types.len(),
                    entry.edge_variants.len(),
                    entry.announcement_variants.len(),
                );
        }
        for (path, draft_types, edge_variants, announcement_variants) in qualified_additions {
            let entry = aliases
                .get_mut(&path)
                .expect("qualified alias addition targets a parsed source");
            let before = (
                entry.qualified_draft_types.len(),
                entry.qualified_edge_variants.len(),
                entry.qualified_announcement_variants.len(),
            );
            entry.qualified_draft_types.extend(draft_types);
            entry.qualified_edge_variants.extend(edge_variants);
            entry
                .qualified_announcement_variants
                .extend(announcement_variants);
            changed |= before
                != (
                    entry.qualified_draft_types.len(),
                    entry.qualified_edge_variants.len(),
                    entry.qualified_announcement_variants.len(),
                );
        }
        if !changed {
            return aliases;
        }
    }
}

struct RustPathCollector<'a> {
    paths: &'a mut Vec<Vec<String>>,
}

impl syn::visit::Visit<'_> for RustPathCollector<'_> {
    fn visit_path(&mut self, path: &syn::Path) {
        self.paths.push(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        );
        syn::visit::visit_path(self, path);
    }
}

fn discovery_draft_aliases(file: &syn::File) -> DiscoveryDraftAliases {
    let mut imports = Vec::new();
    let mut collector = UseImportCollector {
        imports: &mut imports,
    };
    syn::visit::Visit::visit_file(&mut collector, file);

    let mut aliases = DiscoveryDraftAliases::default();
    aliases.draft_types.insert("DiscoveryDraft".to_owned());
    for import in &imports {
        if import
            .path
            .last()
            .is_some_and(|name| name == "DiscoveryDraft")
        {
            if let Some(local) = &import.local {
                aliases.draft_types.insert(local.clone());
            }
        }
    }
    let mut type_aliases = Vec::new();
    let mut type_alias_collector = TypeAliasCollector {
        aliases: &mut type_aliases,
    };
    syn::visit::Visit::visit_file(&mut type_alias_collector, file);
    loop {
        let mut changed = false;
        for (alias, target) in &type_aliases {
            if target
                .last()
                .is_some_and(|target| aliases.draft_types.contains(target))
            {
                changed |= aliases.draft_types.insert(alias.clone());
            }
        }
        if !changed {
            break;
        }
    }
    for import in imports {
        if import.glob
            && import
                .path
                .last()
                .is_some_and(|name| aliases.draft_types.contains(name))
        {
            aliases.edge_variants.insert("Edge".to_owned());
            aliases
                .announcement_variants
                .insert("RegistryAnnouncement".to_owned());
            continue;
        }
        let Some(variant) = import.path.last() else {
            continue;
        };
        let Some(owner) = import.path.iter().rev().nth(1) else {
            continue;
        };
        if !aliases.draft_types.contains(owner) {
            continue;
        }
        let local = import.local.unwrap_or_else(|| variant.clone());
        if variant == "Edge" {
            aliases.edge_variants.insert(local);
        } else if variant == "RegistryAnnouncement" {
            aliases.announcement_variants.insert(local);
        }
    }
    aliases
}

struct TypeAliasCollector<'a> {
    aliases: &'a mut Vec<(String, Vec<String>)>,
}

impl syn::visit::Visit<'_> for TypeAliasCollector<'_> {
    fn visit_item_type(&mut self, item: &syn::ItemType) {
        if let Some(target) = type_path_segments(&item.ty) {
            self.aliases.push((item.ident.to_string(), target));
        }
        syn::visit::visit_item_type(self, item);
    }
}

fn type_path_segments(value: &syn::Type) -> Option<Vec<String>> {
    match value {
        syn::Type::Path(path) if path.qself.is_none() => Some(
            path.path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect(),
        ),
        syn::Type::Group(group) => type_path_segments(&group.elem),
        syn::Type::Paren(paren) => type_path_segments(&paren.elem),
        _ => None,
    }
}

struct UseImport {
    path: Vec<String>,
    local: Option<String>,
    glob: bool,
}

struct UseImportCollector<'a> {
    imports: &'a mut Vec<UseImport>,
}

impl syn::visit::Visit<'_> for UseImportCollector<'_> {
    fn visit_item_use(&mut self, item: &syn::ItemUse) {
        flatten_use_tree(&item.tree, &mut Vec::new(), self.imports);
        syn::visit::visit_item_use(self, item);
    }
}

fn flatten_use_tree(tree: &syn::UseTree, prefix: &mut Vec<String>, output: &mut Vec<UseImport>) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, output);
            prefix.pop();
        }
        syn::UseTree::Name(name) => {
            let imported = name.ident.to_string();
            let (path, local) = if imported == "self" {
                (prefix.clone(), prefix.last().cloned())
            } else {
                let mut path = prefix.clone();
                path.push(imported.clone());
                (path, Some(imported))
            };
            output.push(UseImport {
                path,
                local,
                glob: false,
            });
        }
        syn::UseTree::Rename(rename) => {
            let path = if rename.ident == "self" {
                prefix.clone()
            } else {
                let mut path = prefix.clone();
                path.push(rename.ident.to_string());
                path
            };
            output.push(UseImport {
                path,
                local: Some(rename.rename.to_string()),
                glob: false,
            });
        }
        syn::UseTree::Glob(_) => output.push(UseImport {
            path: prefix.clone(),
            local: None,
            glob: true,
        }),
        syn::UseTree::Group(group) => {
            for item in &group.items {
                flatten_use_tree(item, prefix, output);
            }
        }
    }
}

enum DiscoveryDraftConstruction {
    RegistryAnnouncement,
    Edge(Option<String>),
}

struct DiscoveryDraftConstructionSite {
    events: std::collections::BTreeSet<String>,
    construction: DiscoveryDraftConstruction,
}

struct DiscoveryDraftConstructionVisitor<'a> {
    aliases: &'a DiscoveryDraftAliases,
    current_events: std::collections::BTreeSet<String>,
    sites: Vec<DiscoveryDraftConstructionSite>,
}

impl<'a> DiscoveryDraftConstructionVisitor<'a> {
    fn new(aliases: &'a DiscoveryDraftAliases) -> Self {
        Self {
            aliases,
            current_events: std::collections::BTreeSet::new(),
            sites: Vec::new(),
        }
    }
}

impl<'ast> syn::visit::Visit<'ast> for DiscoveryDraftConstructionVisitor<'_> {
    fn visit_pat(&mut self, _pattern: &'ast syn::Pat) {
        // Enum patterns describe the materializer input; only expression construction sites are
        // producers.
    }

    fn visit_expr_match(&mut self, expression: &'ast syn::ExprMatch) {
        if selected_event_name_match_expression(&expression.expr) {
            let outer_events = self.current_events.clone();
            for arm in &expression.arms {
                self.current_events = pattern_string_values(&arm.pat);
                syn::visit::Visit::visit_expr(self, &arm.body);
                if let Some(guard) = &arm.guard {
                    syn::visit::Visit::visit_expr(self, &guard.1);
                }
            }
            self.current_events = outer_events;
            return;
        }
        syn::visit::visit_expr_match(self, expression);
    }

    fn visit_expr_struct(&mut self, expression: &'ast syn::ExprStruct) {
        if discovery_draft_variant_path(&expression.path, "Edge", self.aliases) {
            let edge_kind = expression
                .fields
                .iter()
                .find(|field| {
                    matches!(&field.member, syn::Member::Named(member) if member == "edge_kind")
                })
                .and_then(|field| rust_string_expression(&field.expr));
            self.sites.push(DiscoveryDraftConstructionSite {
                events: self.current_events.clone(),
                construction: DiscoveryDraftConstruction::Edge(edge_kind),
            });
        }
        syn::visit::visit_expr_struct(self, expression);
    }

    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        if discovery_draft_variant_path(&expression.path, "RegistryAnnouncement", self.aliases) {
            self.sites.push(DiscoveryDraftConstructionSite {
                events: self.current_events.clone(),
                construction: DiscoveryDraftConstruction::RegistryAnnouncement,
            });
        }
        syn::visit::visit_expr_path(self, expression);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        let compact = compact_rust_tokens(&value.tokens.to_string());
        for construction in macro_discovery_draft_constructions(&compact, self.aliases) {
            self.sites.push(DiscoveryDraftConstructionSite {
                events: self.current_events.clone(),
                construction,
            });
        }
        syn::visit::visit_macro(self, value);
    }
}

fn compact_rust_tokens(tokens: &str) -> String {
    tokens
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn macro_discovery_draft_constructions(
    tokens: &str,
    aliases: &DiscoveryDraftAliases,
) -> Vec<DiscoveryDraftConstruction> {
    let mut edge_markers = aliases
        .draft_types
        .iter()
        .map(|alias| format!("{alias}::Edge{{"))
        .chain(
            aliases
                .edge_variants
                .iter()
                .map(|alias| format!("{alias}{{")),
        )
        .chain(
            aliases
                .qualified_edge_variants
                .iter()
                .map(|(module, variant)| format!("{module}::{variant}{{")),
        )
        .collect::<Vec<_>>();
    edge_markers.sort();
    edge_markers.dedup();
    let mut constructions = Vec::new();
    for marker in edge_markers {
        for offset in rust_occurrences(tokens, &marker) {
            let open = offset + marker.len() - 1;
            let edge_kind = matching_rust_brace(tokens, open)
                .and_then(|close| discovery_edge_kind_from_expression(&tokens[offset..=close]));
            constructions.push(DiscoveryDraftConstruction::Edge(edge_kind));
        }
    }

    let mut announcement_markers = aliases
        .draft_types
        .iter()
        .map(|alias| format!("{alias}::RegistryAnnouncement"))
        .chain(aliases.announcement_variants.iter().cloned())
        .chain(
            aliases
                .qualified_announcement_variants
                .iter()
                .map(|(module, variant)| format!("{module}::{variant}")),
        )
        .collect::<Vec<_>>();
    announcement_markers.sort();
    announcement_markers.dedup();
    for marker in announcement_markers {
        constructions.extend(
            rust_occurrences(tokens, &marker)
                .into_iter()
                .map(|_| DiscoveryDraftConstruction::RegistryAnnouncement),
        );
    }
    constructions
}

fn discovery_edge_kind_from_expression(source: &str) -> Option<String> {
    let syn::Expr::Struct(expression) = syn::parse_str::<syn::Expr>(source).ok()? else {
        return None;
    };
    expression
        .fields
        .iter()
        .find(|field| matches!(&field.member, syn::Member::Named(member) if member == "edge_kind"))
        .and_then(|field| rust_string_expression(&field.expr))
}

fn discovery_draft_variant_path(
    path: &syn::Path,
    variant: &str,
    aliases: &DiscoveryDraftAliases,
) -> bool {
    let segments = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let Some(last) = segments.last() else {
        return false;
    };
    if segments.len() == 1 {
        return if variant == "Edge" {
            aliases.edge_variants.contains(last)
        } else {
            aliases.announcement_variants.contains(last)
        };
    }
    last == variant
        && segments.len() >= 2
        && (aliases.draft_types.contains(&segments[segments.len() - 2])
            || (segments.len() >= 3
                && aliases.qualified_draft_types.contains(&(
                    segments[segments.len() - 3].clone(),
                    segments[segments.len() - 2].clone(),
                )))
            || if variant == "Edge" {
                aliases
                    .qualified_edge_variants
                    .contains(&(segments[segments.len() - 2].clone(), last.clone()))
            } else {
                aliases
                    .qualified_announcement_variants
                    .contains(&(segments[segments.len() - 2].clone(), last.clone()))
            })
}

fn selected_event_name_match_expression(expression: &syn::Expr) -> bool {
    match expression {
        syn::Expr::MethodCall(call) if call.method == "as_str" => {
            let syn::Expr::Field(name) = call.receiver.as_ref() else {
                return false;
            };
            let syn::Member::Named(name_member) = &name.member else {
                return false;
            };
            let syn::Expr::Field(event) = name.base.as_ref() else {
                return false;
            };
            let syn::Member::Named(event_member) = &event.member else {
                return false;
            };
            let syn::Expr::Path(selected) = event.base.as_ref() else {
                return false;
            };
            name_member == "name" && event_member == "event" && selected.path.is_ident("selected")
        }
        syn::Expr::Group(group) => selected_event_name_match_expression(&group.expr),
        syn::Expr::Paren(paren) => selected_event_name_match_expression(&paren.expr),
        _ => false,
    }
}

fn pattern_string_values(pattern: &syn::Pat) -> std::collections::BTreeSet<String> {
    match pattern {
        syn::Pat::Lit(literal) => match &literal.lit {
            syn::Lit::Str(value) => std::collections::BTreeSet::from([value.value()]),
            _ => std::collections::BTreeSet::new(),
        },
        syn::Pat::Or(alternatives) => alternatives
            .cases
            .iter()
            .flat_map(pattern_string_values)
            .collect(),
        syn::Pat::Paren(paren) => pattern_string_values(&paren.pat),
        _ => std::collections::BTreeSet::new(),
    }
}

fn rust_string_expression(expression: &syn::Expr) -> Option<String> {
    match expression {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) => Some(value.value()),
        syn::Expr::Group(group) => rust_string_expression(&group.expr),
        syn::Expr::Paren(paren) => rust_string_expression(&paren.expr),
        syn::Expr::Reference(reference) => rust_string_expression(&reference.expr),
        syn::Expr::MethodCall(call)
            if matches!(
                call.method.to_string().as_str(),
                "into" | "to_owned" | "to_string"
            ) && call.args.is_empty() =>
        {
            rust_string_expression(&call.receiver)
        }
        syn::Expr::Call(call)
            if call.args.len() == 1
                && matches!(
                    call.func.as_ref(),
                    syn::Expr::Path(function)
                        if function.path.segments.last().is_some_and(|segment| segment.ident == "from")
                ) =>
        {
            call.args.first().and_then(rust_string_expression)
        }
        _ => None,
    }
}

fn checked_in_manifest_source_family_events()
-> anyhow::Result<std::collections::BTreeSet<(String, String)>> {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests");
    let mut events = std::collections::BTreeSet::new();
    for environment in ["mainnet", "sepolia"] {
        let repository = bigname_manifests::load_repository(manifest_root.join(environment))?;
        for loaded in repository.manifests() {
            events.extend(
                loaded
                    .manifest
                    .abi
                    .events
                    .iter()
                    .map(|event| (loaded.manifest.source_family.clone(), event.name.clone())),
            );
        }
    }
    Ok(events)
}

fn scope_protocol_rule_lookup_producers(
    schema_v2_root: &std::path::Path,
    locations: &std::collections::BTreeSet<(std::path::PathBuf, String, String)>,
    manifest_events: &std::collections::BTreeSet<(String, String)>,
    adapter_paths: &std::collections::BTreeMap<String, std::path::PathBuf>,
) -> anyhow::Result<std::collections::BTreeSet<(String, String, String)>> {
    let mut producers = std::collections::BTreeSet::new();
    for (path, event, edge_kind) in locations {
        let owning_routes = adapter_paths
            .iter()
            .filter(|(_, adapter_path)| routed_adapter_owns_source(adapter_path, path))
            .collect::<Vec<_>>();
        let most_specific_depth = owning_routes
            .iter()
            .map(|(_, adapter_path)| adapter_path.components().count())
            .max();
        let mut routed_families = owning_routes
            .into_iter()
            .filter(|(_, adapter_path)| {
                Some(adapter_path.components().count()) == most_specific_depth
            })
            .map(|(source_family, _)| source_family.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if path == &schema_v2_root.join("protocol.rs") {
            routed_families.extend(adapter_paths.keys().cloned());
        }
        anyhow::ensure!(
            !routed_families.is_empty(),
            "protocol producer {} {} -> {} has no protocol dispatch owner",
            path.display(),
            event,
            edge_kind,
        );
        let scoped = routed_families
            .into_iter()
            .filter(|source_family| {
                manifest_events.contains(&(source_family.clone(), event.clone()))
            })
            .map(|source_family| (source_family, event.clone(), edge_kind.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(
            !scoped.is_empty(),
            "protocol producer {} {} -> {} has no checked-in manifest event in a routed source family",
            path.display(),
            event,
            edge_kind,
        );
        producers.extend(scoped);
    }
    Ok(producers)
}

fn routed_adapter_owns_source(
    adapter_path: &std::path::Path,
    source_path: &std::path::Path,
) -> bool {
    if adapter_path == source_path {
        return true;
    }
    let descendant_root = if adapter_path
        .file_name()
        .is_some_and(|name| name == "mod.rs")
    {
        adapter_path.parent().unwrap_or(adapter_path).to_path_buf()
    } else {
        adapter_path.with_extension("")
    };
    source_path.starts_with(descendant_root)
}

fn checked_in_protocol_adapter_paths(
    schema_v2_root: &std::path::Path,
    known_families: &std::collections::BTreeSet<String>,
) -> anyhow::Result<std::collections::BTreeMap<String, std::path::PathBuf>> {
    let protocol_source = std::fs::read_to_string(schema_v2_root.join("protocol.rs"))?;
    let v1_source = std::fs::read_to_string(schema_v2_root.join("protocol/v1.rs"))?;
    let v2_registry_source =
        std::fs::read_to_string(schema_v2_root.join("protocol/v2_registry.rs"))?;
    protocol_adapter_paths_from_dispatch_sources(
        schema_v2_root,
        known_families,
        &protocol_source,
        &v1_source,
        &v2_registry_source,
    )
}

fn protocol_adapter_paths_from_dispatch_sources(
    schema_v2_root: &std::path::Path,
    known_families: &std::collections::BTreeSet<String>,
    protocol_source: &str,
    v1_source: &str,
    v2_registry_source: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, std::path::PathBuf>> {
    let mut paths = std::collections::BTreeMap::new();
    for source_family in known_families {
        if source_family.ends_with("_execution") || source_family == "basenames_l1_compat" {
            continue;
        }
        let top_level_module = dispatch_module_for_family(protocol_source, source_family)?;
        let path = if top_level_module == "v1" {
            let module = dispatch_module_for_family(v1_source, source_family)?;
            schema_v2_root.join(format!("protocol/v1/{module}.rs"))
        } else if top_level_module == "v2_registry" {
            if let Some(nested_module) =
                nested_dispatch_module_for_family(v2_registry_source, source_family)?
            {
                schema_v2_root.join(format!("protocol/v2_registry/{nested_module}.rs"))
            } else {
                schema_v2_root.join("protocol/v2_registry.rs")
            }
        } else {
            schema_v2_root.join(format!("protocol/{top_level_module}.rs"))
        };
        paths.insert(source_family.clone(), path);
    }
    Ok(paths)
}

fn dispatch_module_for_family(source: &str, source_family: &str) -> anyhow::Result<String> {
    let file = syn::parse_file(source)?;
    let function = production_interpret_function(&file)?;
    let mut visitor = SourceFamilyMatchVisitor::new(source_family);
    syn::visit::Visit::visit_block(&mut visitor, &function.block);
    anyhow::ensure!(
        visitor.routes.len() == 1,
        "production dispatch must have exactly one adapter arm for {source_family}, found {:?}",
        visitor.routes,
    );
    one_interpret_target(
        visitor.routes.pop().expect("one adapter arm"),
        source_family,
    )
}

fn nested_dispatch_module_for_family(
    source: &str,
    source_family: &str,
) -> anyhow::Result<Option<String>> {
    let file = syn::parse_file(source)?;
    let function = production_interpret_function(&file)?;
    let mut match_visitor = SourceFamilyMatchVisitor::new(source_family);
    syn::visit::Visit::visit_block(&mut match_visitor, &function.block);
    let mut if_visitor = SourceFamilyIfVisitor::new(source_family);
    syn::visit::Visit::visit_block(&mut if_visitor, &function.block);
    let mut routes = match_visitor.routes;
    routes.extend(if_visitor.routes);
    anyhow::ensure!(
        routes.len() <= 1,
        "nested production dispatch has more than one adapter arm for {source_family}: {routes:?}",
    );
    let Some(route) = routes.pop() else {
        return Ok(None);
    };
    one_interpret_target(route, source_family).map(Some)
}

fn production_interpret_function(file: &syn::File) -> anyhow::Result<&syn::ItemFn> {
    let functions = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Fn(function) if function.sig.ident == "interpret" => Some(function),
            _ => None,
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        functions.len() == 1,
        "production source must have exactly one top-level interpret function, found {}",
        functions.len(),
    );
    Ok(functions[0])
}

fn one_interpret_target(
    modules: std::collections::BTreeSet<String>,
    source_family: &str,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        modules.len() == 1,
        "production dispatch arm for {source_family} must target exactly one adapter module, found {modules:?}",
    );
    Ok(modules.into_iter().next().expect("one adapter module"))
}

struct SourceFamilyMatchVisitor<'a> {
    source_family: &'a str,
    routes: Vec<std::collections::BTreeSet<String>>,
}

impl<'a> SourceFamilyMatchVisitor<'a> {
    fn new(source_family: &'a str) -> Self {
        Self {
            source_family,
            routes: Vec::new(),
        }
    }
}

impl<'ast> syn::visit::Visit<'ast> for SourceFamilyMatchVisitor<'_> {
    fn visit_expr_match(&mut self, expression: &'ast syn::ExprMatch) {
        if source_family_match_expression(&expression.expr) {
            for arm in &expression.arms {
                if pattern_names_family(&arm.pat, self.source_family)
                    || guarded_pattern_names_family(arm, self.source_family)
                {
                    let modules = interpret_targets_in_expression(&arm.body);
                    if !modules.is_empty() {
                        self.routes.push(modules);
                    }
                }
            }
            return;
        }
        syn::visit::visit_expr_match(self, expression);
    }
}

fn guarded_pattern_names_family(arm: &syn::Arm, source_family: &str) -> bool {
    let syn::Pat::Ident(binding) = &arm.pat else {
        return false;
    };
    arm.guard.as_ref().is_some_and(|(_, guard)| {
        guard_accepts_source_family(guard, &binding.ident.to_string(), source_family)
    })
}

fn guard_accepts_source_family(expression: &syn::Expr, binding: &str, source_family: &str) -> bool {
    match expression {
        syn::Expr::MethodCall(call)
            if call.method == "starts_with"
                && call.args.len() == 1
                && matches!(
                    call.receiver.as_ref(),
                    syn::Expr::Path(receiver) if receiver.path.is_ident(binding)
                ) =>
        {
            call.args
                .first()
                .and_then(|argument| match argument {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(prefix),
                        ..
                    }) => Some(prefix.value()),
                    _ => None,
                })
                .is_some_and(|prefix| source_family.starts_with(&prefix))
        }
        syn::Expr::Binary(binary) => match &binary.op {
            syn::BinOp::And(_) => {
                guard_accepts_source_family(&binary.left, binding, source_family)
                    && guard_accepts_source_family(&binary.right, binding, source_family)
            }
            syn::BinOp::Or(_) => {
                guard_accepts_source_family(&binary.left, binding, source_family)
                    || guard_accepts_source_family(&binary.right, binding, source_family)
            }
            syn::BinOp::Eq(_) | syn::BinOp::Ne(_) => {
                let compared = binding_string_comparison(&binary.left, &binary.right, binding)
                    .or_else(|| binding_string_comparison(&binary.right, &binary.left, binding));
                compared.is_some_and(|value| {
                    if matches!(&binary.op, syn::BinOp::Eq(_)) {
                        source_family == value
                    } else {
                        source_family != value
                    }
                })
            }
            _ => false,
        },
        syn::Expr::Unary(unary) if matches!(unary.op, syn::UnOp::Not(_)) => {
            !guard_accepts_source_family(&unary.expr, binding, source_family)
        }
        syn::Expr::Group(group) => guard_accepts_source_family(&group.expr, binding, source_family),
        syn::Expr::Paren(paren) => guard_accepts_source_family(&paren.expr, binding, source_family),
        _ => false,
    }
}

fn binding_string_comparison(
    binding_expression: &syn::Expr,
    value_expression: &syn::Expr,
    binding: &str,
) -> Option<String> {
    let syn::Expr::Path(path) = binding_expression else {
        return None;
    };
    if !path.path.is_ident(binding) {
        return None;
    }
    let syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(value),
        ..
    }) = value_expression
    else {
        return None;
    };
    Some(value.value())
}

struct SourceFamilyIfVisitor<'a> {
    source_family: &'a str,
    routes: Vec<std::collections::BTreeSet<String>>,
}

impl<'a> SourceFamilyIfVisitor<'a> {
    fn new(source_family: &'a str) -> Self {
        Self {
            source_family,
            routes: Vec::new(),
        }
    }
}

impl<'ast> syn::visit::Visit<'ast> for SourceFamilyIfVisitor<'_> {
    fn visit_expr_if(&mut self, expression: &'ast syn::ExprIf) {
        if condition_names_family(&expression.cond, self.source_family) {
            let modules = interpret_targets_in_block(&expression.then_branch);
            if !modules.is_empty() {
                self.routes.push(modules);
            }
            return;
        }
        syn::visit::visit_expr_if(self, expression);
    }
}

#[derive(Default)]
struct InterpretTargetVisitor {
    modules: std::collections::BTreeSet<String>,
}

impl<'ast> syn::visit::Visit<'ast> for InterpretTargetVisitor {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = call.func.as_ref() {
            let segments = function.path.segments.iter().collect::<Vec<_>>();
            if segments
                .last()
                .is_some_and(|segment| segment.ident == "interpret")
                && segments.len() >= 2
            {
                self.modules
                    .insert(segments[segments.len() - 2].ident.to_string());
            }
        }
        syn::visit::visit_expr_call(self, call);
    }
}

fn interpret_targets_in_expression(expression: &syn::Expr) -> std::collections::BTreeSet<String> {
    let mut visitor = InterpretTargetVisitor::default();
    syn::visit::Visit::visit_expr(&mut visitor, expression);
    visitor.modules
}

fn interpret_targets_in_block(block: &syn::Block) -> std::collections::BTreeSet<String> {
    let mut visitor = InterpretTargetVisitor::default();
    syn::visit::Visit::visit_block(&mut visitor, block);
    visitor.modules
}

fn source_family_match_expression(expression: &syn::Expr) -> bool {
    match expression {
        syn::Expr::MethodCall(call) if call.method == "as_str" => {
            selected_source_family_expression(&call.receiver)
        }
        syn::Expr::Group(group) => source_family_match_expression(&group.expr),
        syn::Expr::Paren(paren) => source_family_match_expression(&paren.expr),
        _ => false,
    }
}

fn selected_source_family_expression(expression: &syn::Expr) -> bool {
    let syn::Expr::Field(source_family) = expression else {
        return false;
    };
    let syn::Member::Named(source_family_member) = &source_family.member else {
        return false;
    };
    let syn::Expr::Field(source) = source_family.base.as_ref() else {
        return false;
    };
    let syn::Member::Named(source_member) = &source.member else {
        return false;
    };
    let syn::Expr::Path(selected) = source.base.as_ref() else {
        return false;
    };
    source_family_member == "source_family"
        && source_member == "source"
        && selected.path.is_ident("selected")
}

fn pattern_names_family(pattern: &syn::Pat, source_family: &str) -> bool {
    match pattern {
        syn::Pat::Lit(literal) => {
            matches!(&literal.lit, syn::Lit::Str(value) if value.value() == source_family)
        }
        syn::Pat::Or(alternatives) => alternatives
            .cases
            .iter()
            .any(|pattern| pattern_names_family(pattern, source_family)),
        syn::Pat::Paren(paren) => pattern_names_family(&paren.pat, source_family),
        _ => false,
    }
}

fn condition_names_family(condition: &syn::Expr, source_family: &str) -> bool {
    match condition {
        syn::Expr::Binary(binary) => {
            let direct_match = matches!(binary.op, syn::BinOp::Eq(_))
                && ((selected_source_family_expression(&binary.left)
                    && string_expression_is(&binary.right, source_family))
                    || (selected_source_family_expression(&binary.right)
                        && string_expression_is(&binary.left, source_family)));
            direct_match
                || condition_names_family(&binary.left, source_family)
                || condition_names_family(&binary.right, source_family)
        }
        syn::Expr::Group(group) => condition_names_family(&group.expr, source_family),
        syn::Expr::Paren(paren) => condition_names_family(&paren.expr, source_family),
        _ => false,
    }
}

fn string_expression_is(expression: &syn::Expr, expected: &str) -> bool {
    matches!(
        expression,
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) if value.value() == expected
    )
}

fn protocol_rule_lookup_source_paths() -> anyhow::Result<Vec<std::path::PathBuf>> {
    let schema_v2_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/schema_v2");
    let mut protocol_sources = Vec::new();
    collect_rust_sources(&schema_v2_root, &mut protocol_sources)?;
    Ok(protocol_sources)
}

fn read_rust_source_map(
    paths: &[std::path::PathBuf],
) -> anyhow::Result<std::collections::BTreeMap<std::path::PathBuf, String>> {
    paths
        .iter()
        .map(|path| {
            Ok((
                path.clone(),
                std::fs::read_to_string(path)
                    .with_context(|| format!("read Rust source {}", path.display()))?,
            ))
        })
        .collect()
}

fn validate_role_insensitivity_metadata(
    workspace_root: &std::path::Path,
    entries: &[bigname_manifests::RoleInsensitiveEvent],
    routes: &std::collections::BTreeMap<String, std::path::PathBuf>,
    sources: &std::collections::BTreeMap<std::path::PathBuf, String>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !entries.is_empty(),
        "ROLE_INSENSITIVE_EVENTS must enumerate at least one adapter event"
    );
    let mut pairs = std::collections::BTreeSet::new();
    for entry in entries {
        anyhow::ensure!(
            pairs.insert((entry.source_family.to_owned(), entry.event.to_owned())),
            "duplicate ROLE_INSENSITIVE_EVENTS entry for {} {}",
            entry.source_family,
            entry.event,
        );
        anyhow::ensure!(
            !entry.justification.trim().is_empty(),
            "ROLE_INSENSITIVE_EVENTS entry for {} {} requires a justification",
            entry.source_family,
            entry.event,
        );
        let routed_path = routes.get(entry.source_family).with_context(|| {
            format!(
                "ROLE_INSENSITIVE_EVENTS entry for {} {} has no production dispatch route",
                entry.source_family, entry.event,
            )
        })?;
        let declared_path = workspace_root.join(entry.adapter_file);
        anyhow::ensure!(
            normalize_path(&declared_path) == normalize_path(routed_path),
            "ROLE_INSENSITIVE_EVENTS entry for {} {} declares adapter_file {}, but production dispatch routes to {}",
            entry.source_family,
            entry.event,
            declared_path.display(),
            routed_path.display(),
        );
        let routed_source = sources
            .get(routed_path)
            .with_context(|| format!("read routed adapter source {}", routed_path.display()))?;
        anyhow::ensure!(
            rust_source_handles_selected_event(routed_source, entry.event)?,
            "ROLE_INSENSITIVE_EVENTS entry for {} {} does not name an event handled by routed adapter {}",
            entry.source_family,
            entry.event,
            routed_path.display(),
        );

        // The static boundary is the production dispatcher chain, routed module tree, and local
        // sibling modules reached by a function call that passes a `Selected` parameter. Macro
        // expansion, trait/method dispatch, function pointers, and `Selected` hidden inside
        // another value remain outside this test-side analysis.
        for path in role_read_source_paths(routed_path, sources)? {
            let source = sources
                .get(&path)
                .expect("routed module source path came from the source map");
            anyhow::ensure!(
                !rust_source_reads_emitter_role(source)?,
                "ROLE_INSENSITIVE_EVENTS entry for {} {} routes through {} which reads Selected.emitter_role",
                entry.source_family,
                entry.event,
                path.display(),
            );
        }
    }
    Ok(())
}

fn role_read_source_paths(
    routed_path: &std::path::Path,
    sources: &std::collections::BTreeMap<std::path::PathBuf, String>,
) -> anyhow::Result<std::collections::BTreeSet<std::path::PathBuf>> {
    let mut pending = routed_module_source_paths(routed_path, sources.keys());
    pending.extend(role_dispatch_source_paths(routed_path, sources.keys()));
    let mut reachable = std::collections::BTreeSet::new();
    while let Some(path) = pending.pop() {
        if !reachable.insert(path.clone()) {
            continue;
        }
        let source = sources
            .get(&path)
            .expect("role-read source path came from the source map");
        let file = syn::parse_file(source)?;
        let mut imported_modules = selected_bearing_imported_modules(&file);
        imported_modules.extend(local_reexported_modules(&file));
        for module in imported_modules {
            if let Some(helper_path) = resolve_local_module_source(&path, &module, sources.keys()) {
                pending.push(helper_path);
            }
        }
    }
    Ok(reachable)
}

fn local_reexported_modules(file: &syn::File) -> Vec<Vec<String>> {
    let mut modules = Vec::new();
    for item in &file.items {
        let syn::Item::Use(import) = item else {
            continue;
        };
        if matches!(import.vis, syn::Visibility::Inherited) {
            continue;
        }
        let mut reexports = Vec::new();
        flatten_use_tree(&import.tree, &mut Vec::new(), &mut reexports);
        for reexport in reexports {
            modules.push(reexport.path.clone());
            if !reexport.glob {
                let mut owner = reexport.path;
                owner.pop();
                if !owner.is_empty() {
                    modules.push(owner);
                }
            }
        }
    }
    modules
}

fn role_dispatch_source_paths<'a>(
    routed_path: &std::path::Path,
    source_paths: impl Iterator<Item = &'a std::path::PathBuf>,
) -> Vec<std::path::PathBuf> {
    let Some(schema_v2_root) = routed_path
        .ancestors()
        .find(|ancestor| ancestor.file_name().is_some_and(|name| name == "schema_v2"))
    else {
        return Vec::new();
    };
    let mut candidates = vec![schema_v2_root.join("protocol.rs")];
    if routed_path.starts_with(schema_v2_root.join("protocol/v1")) {
        candidates.push(schema_v2_root.join("protocol/v1.rs"));
    } else if routed_path.starts_with(schema_v2_root.join("protocol/v2_registry")) {
        candidates.push(schema_v2_root.join("protocol/v2_registry.rs"));
    }
    let source_paths = source_paths.collect::<std::collections::BTreeSet<_>>();
    candidates
        .into_iter()
        .filter(|candidate| source_paths.contains(candidate))
        .collect()
}

fn selected_bearing_imported_modules(file: &syn::File) -> Vec<Vec<String>> {
    let mut imports = Vec::new();
    let mut import_collector = UseImportCollector {
        imports: &mut imports,
    };
    syn::visit::Visit::visit_file(&mut import_collector, file);
    let glob_modules = imports
        .iter()
        .filter(|import| import.glob)
        .map(|import| import.path.clone())
        .collect::<Vec<_>>();
    let imported_by_local = imports
        .into_iter()
        .filter_map(|import| import.local.map(|local| (local, import.path)))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut calls = Vec::new();
    let mut call_collector = SelectedPassingCallCollector { calls: &mut calls };
    syn::visit::Visit::visit_file(&mut call_collector, file);
    let mut modules = Vec::new();
    for call in calls {
        let Some(first) = call.first() else {
            continue;
        };
        if matches!(first.as_str(), "crate" | "self" | "super") {
            if call.len() >= 2 {
                modules.push(call[..call.len() - 1].to_vec());
            }
            continue;
        }
        let Some(mut imported) = imported_by_local.get(first).cloned() else {
            if call.len() == 1 {
                modules.extend(glob_modules.iter().cloned());
            }
            continue;
        };
        if call.len() == 1 {
            imported.pop();
        } else {
            imported.extend_from_slice(&call[1..call.len() - 1]);
        }
        modules.push(imported);
    }
    modules
}

struct SelectedPassingCallCollector<'a> {
    calls: &'a mut Vec<Vec<String>>,
}

impl syn::visit::Visit<'_> for SelectedPassingCallCollector<'_> {
    fn visit_item_fn(&mut self, function: &syn::ItemFn) {
        let selected_parameters = function
            .sig
            .inputs
            .iter()
            .filter_map(|argument| match argument {
                syn::FnArg::Typed(argument) if type_is_selected(&argument.ty) => {
                    match argument.pat.as_ref() {
                        syn::Pat::Ident(identifier) => Some(identifier.ident.to_string()),
                        _ => None,
                    }
                }
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        let mut visitor = SelectedArgumentCallVisitor {
            selected_names: selected_parameters,
            calls: self.calls,
        };
        syn::visit::Visit::visit_block(&mut visitor, &function.block);
        syn::visit::visit_item_fn(self, function);
    }
}

fn type_is_selected(value: &syn::Type) -> bool {
    match value {
        syn::Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "Selected"),
        syn::Type::Reference(reference) => type_is_selected(&reference.elem),
        syn::Type::Group(group) => type_is_selected(&group.elem),
        syn::Type::Paren(paren) => type_is_selected(&paren.elem),
        _ => false,
    }
}

struct SelectedArgumentCallVisitor<'a> {
    selected_names: std::collections::BTreeSet<String>,
    calls: &'a mut Vec<Vec<String>>,
}

impl<'ast> syn::visit::Visit<'ast> for SelectedArgumentCallVisitor<'_> {
    fn visit_local(&mut self, local: &'ast syn::Local) {
        if local.init.as_ref().is_some_and(|initializer| {
            expression_references_any_name(&initializer.expr, &self.selected_names)
        }) {
            if let Some(local_name) = local_pattern_name(&local.pat) {
                self.selected_names.insert(local_name);
            }
        }
        syn::visit::visit_local(self, local);
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        let carries_selected = call
            .args
            .iter()
            .any(|argument| expression_references_any_name(argument, &self.selected_names));
        if carries_selected {
            if let syn::Expr::Path(function) = call.func.as_ref() {
                self.calls.push(
                    function
                        .path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect(),
                );
            }
        }
        syn::visit::visit_expr_call(self, call);
    }
}

fn local_pattern_name(pattern: &syn::Pat) -> Option<String> {
    match pattern {
        syn::Pat::Ident(identifier) => Some(identifier.ident.to_string()),
        syn::Pat::Type(typed) => local_pattern_name(&typed.pat),
        syn::Pat::Paren(paren) => local_pattern_name(&paren.pat),
        _ => None,
    }
}

fn expression_references_any_name(
    expression: &syn::Expr,
    names: &std::collections::BTreeSet<String>,
) -> bool {
    let mut visitor = NamedExpressionVisitor {
        names,
        found: false,
    };
    syn::visit::Visit::visit_expr(&mut visitor, expression);
    visitor.found
}

struct NamedExpressionVisitor<'a> {
    names: &'a std::collections::BTreeSet<String>,
    found: bool,
}

impl<'ast> syn::visit::Visit<'ast> for NamedExpressionVisitor<'_> {
    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        if expression
            .path
            .get_ident()
            .is_some_and(|name| self.names.contains(&name.to_string()))
        {
            self.found = true;
        }
        syn::visit::visit_expr_path(self, expression);
    }
}

fn resolve_local_module_source<'a>(
    current_path: &std::path::Path,
    module: &[String],
    source_paths: impl Iterator<Item = &'a std::path::PathBuf>,
) -> Option<std::path::PathBuf> {
    let source_paths = source_paths
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let (mut logical_path, remaining) = if module.first().is_some_and(|part| part == "crate") {
        let crate_src = current_path
            .ancestors()
            .find(|ancestor| ancestor.file_name().is_some_and(|name| name == "src"))?;
        (crate_src.to_path_buf(), &module[1..])
    } else {
        let logical_path = if current_path
            .file_name()
            .is_some_and(|name| name == "mod.rs")
        {
            current_path.parent()?.to_path_buf()
        } else {
            current_path.with_extension("")
        };
        (logical_path, module)
    };
    for part in remaining {
        match part.as_str() {
            "self" => {}
            "super" => {
                logical_path.pop();
            }
            part => logical_path.push(part),
        }
    }
    let file_candidate = logical_path.with_extension("rs");
    if source_paths.contains(&file_candidate) {
        return Some(file_candidate);
    }
    let module_candidate = logical_path.join("mod.rs");
    source_paths
        .contains(&module_candidate)
        .then_some(module_candidate)
}

fn rust_source_handles_selected_event(source: &str, event: &str) -> anyhow::Result<bool> {
    let file = syn::parse_file(source)?;
    let function = production_interpret_function(&file)?;
    let mut visitor = SelectedEventArmVisitor::default();
    syn::visit::Visit::visit_block(&mut visitor, &function.block);
    Ok(visitor.events.contains(event))
}

#[derive(Default)]
struct SelectedEventArmVisitor {
    events: std::collections::BTreeSet<String>,
}

impl<'ast> syn::visit::Visit<'ast> for SelectedEventArmVisitor {
    fn visit_expr_match(&mut self, expression: &'ast syn::ExprMatch) {
        if selected_event_name_match_expression(&expression.expr) {
            self.events.extend(
                expression
                    .arms
                    .iter()
                    .flat_map(|arm| pattern_string_values(&arm.pat)),
            );
        }
        syn::visit::visit_expr_match(self, expression);
    }
}

fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn routed_module_source_paths<'a>(
    routed_path: &std::path::Path,
    source_paths: impl Iterator<Item = &'a std::path::PathBuf>,
) -> Vec<std::path::PathBuf> {
    source_paths
        .filter(|path| routed_adapter_owns_source(routed_path, path))
        .cloned()
        .collect()
}

fn rust_source_reads_emitter_role(source: &str) -> anyhow::Result<bool> {
    let file = syn::parse_file(source)?;
    let mut visitor = EmitterRoleReadVisitor::default();
    syn::visit::Visit::visit_file(&mut visitor, &file);
    Ok(visitor.found)
}

#[derive(Default)]
struct EmitterRoleReadVisitor {
    found: bool,
}

impl<'ast> syn::visit::Visit<'ast> for EmitterRoleReadVisitor {
    fn visit_expr_field(&mut self, expression: &'ast syn::ExprField) {
        if matches!(&expression.member, syn::Member::Named(member) if member == "emitter_role") {
            self.found = true;
        }
        syn::visit::visit_expr_field(self, expression);
    }

    fn visit_pat_struct(&mut self, pattern: &'ast syn::PatStruct) {
        if pattern.fields.iter().any(
            |field| matches!(&field.member, syn::Member::Named(member) if member == "emitter_role"),
        ) {
            self.found = true;
        }
        syn::visit::visit_pat_struct(self, pattern);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        let tokens = rust_tokens_without_quoted_literals(&value.tokens.to_string());
        if compact_rust_tokens(&tokens).contains("emitter_role") {
            self.found = true;
        }
        syn::visit::visit_macro(self, value);
    }
}

fn rust_tokens_without_quoted_literals(tokens: &str) -> String {
    let mut output = String::with_capacity(tokens.len());
    let mut quoted = false;
    let mut escaped = false;
    for character in tokens.chars() {
        if quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
            continue;
        }
        if character == '"' {
            quoted = true;
        } else {
            output.push(character);
        }
    }
    output
}

fn role_insensitivity_discovery_overlap(
    role_events: &[bigname_manifests::RoleInsensitiveEvent],
    producers: &std::collections::BTreeSet<(String, String, String)>,
) -> std::collections::BTreeSet<(String, String, String)> {
    role_events
        .iter()
        .flat_map(|role_event| {
            producers
                .iter()
                .filter(move |(producer_family, producer_event, _)| {
                    producer_family == role_event.source_family
                        && producer_event == role_event.event
                })
                .map(move |(_, _, edge_kind)| {
                    (
                        role_event.source_family.to_owned(),
                        role_event.event.to_owned(),
                        edge_kind.clone(),
                    )
                })
        })
        .collect()
}

fn collect_rust_sources(
    directory: &std::path::Path,
    output: &mut Vec<std::path::PathBuf>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            if path.file_name().is_none_or(|name| name != "tests") {
                collect_rust_sources(&path, output)?;
            }
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && path.file_name().is_none_or(|name| name != "tests.rs")
        {
            output.push(path);
        }
    }
    output.sort();
    Ok(())
}

fn registry_announcement_rule_edge_kind(source: &str) -> anyhow::Result<String> {
    let marker = "DiscoveryDraft::RegistryAnnouncement => {";
    let start = source
        .find(marker)
        .context("discovery materializer has no RegistryAnnouncement arm")?;
    let open = start
        + source[start..]
            .find('{')
            .expect("RegistryAnnouncement marker includes an opening brace");
    let close = matching_rust_brace(source, open)
        .context("discovery materializer has an unterminated RegistryAnnouncement arm")?;
    let rule_call = source[open..close]
        .find(".rule(")
        .map(|offset| open + offset)
        .context("RegistryAnnouncement arm does not call Catalog::rule")?;
    first_rust_string(&source[rule_call..close])
        .map(str::to_owned)
        .context("RegistryAnnouncement rule call has no literal edge kind")
}

fn rule_lookup_bypass_edge_kinds(
    source: &str,
) -> anyhow::Result<std::collections::BTreeSet<String>> {
    let assignment = source
        .split_once("let rule_basis =")
        .map(|(_, tail)| tail)
        .context("discovery materializer does not assign rule_basis")?;
    let rule_lookup = assignment
        .split_once("catalog\n")
        .map(|(bypass, _)| bypass)
        .context("rule_basis does not fall through to Catalog::rule")?;
    let kinds = rust_occurrences(rule_lookup, "if edge_kind == \"")
        .into_iter()
        .filter_map(|offset| first_rust_string(&rule_lookup[offset..]).map(str::to_owned))
        .collect::<std::collections::BTreeSet<_>>();
    anyhow::ensure!(
        !kinds.is_empty(),
        "discovery materializer must declare its Catalog::rule bypass edge kinds"
    );
    Ok(kinds)
}

fn first_rust_string(source: &str) -> Option<&str> {
    let start = source.find('"')? + 1;
    let end = source[start..].find('"')? + start;
    Some(&source[start..end])
}

fn rust_occurrences(source: &str, marker: &str) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = source[cursor..].find(marker) {
        let offset = cursor + relative;
        offsets.push(offset);
        cursor = offset + marker.len();
    }
    offsets
}

fn matching_rust_brace(source: &str, open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    let mut index = open;
    let mut string = false;
    let mut character = false;
    let mut escaped = false;
    let mut line_comment = false;
    let mut block_comment_depth = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        if line_comment {
            line_comment = byte != b'\n';
        } else if block_comment_depth > 0 {
            if byte == b'/' && next == Some(b'*') {
                block_comment_depth += 1;
                index += 1;
            } else if byte == b'*' && next == Some(b'/') {
                block_comment_depth -= 1;
                index += 1;
            }
        } else if string || character {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if (string && byte == b'"') || (character && byte == b'\'') {
                string = false;
                character = false;
            }
        } else if byte == b'/' && next == Some(b'/') {
            line_comment = true;
            index += 1;
        } else if byte == b'/' && next == Some(b'*') {
            block_comment_depth = 1;
            index += 1;
        } else if byte == b'"' {
            string = true;
        } else if byte == b'\'' {
            character = true;
        } else if byte == b'{' {
            depth += 1;
        } else if byte == b'}' {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(index);
            }
        }
        index += 1;
    }
    None
}

fn with_topic0(mut encoded: alloy_primitives::LogData, topic0: B256) -> alloy_primitives::LogData {
    encoded.topics_mut()[0] = topic0;
    encoded
}

fn raw_at(
    encoded: alloy_primitives::LogData,
    block_number: i64,
    log_index: i64,
    emitting_address: &str,
) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(block_number),
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("transaction-{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: emitting_address.to_owned(),
        topics: encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: encoded.data.to_vec(),
    }
}

fn raw_at_transaction(
    encoded: alloy_primitives::LogData,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
    emitting_address: &str,
) -> RawLogInput {
    let mut raw = raw_at(encoded, block_number, log_index, emitting_address);
    raw.transaction_hash = format!("transaction-{block_number}-{transaction_index}");
    raw.transaction_index = transaction_index;
    raw
}

fn versioned_token(label: &str, version: u32) -> U256 {
    versioned_token_bytes(label.as_bytes(), version)
}

fn versioned_token_bytes(label: &[u8], version: u32) -> U256 {
    let mut bytes = *keccak256(label);
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}

fn prior_event(event: &NormalizedEvent) -> PriorEventInput {
    PriorEventInput {
        retained_state_key: seam::retained_prior_state_key(
            event
                .raw_fact_ref
                .get(seam::INTERPRETER_STATE_KEY)
                .and_then(serde_json::Value::as_str),
            &event.event_identity,
        ),
        chain_id: event.chain_id.clone(),
        namespace: event.namespace.clone(),
        logical_name_id: event.logical_name_id.clone(),
        resource_id: event.resource_id,
        event_kind: event.event_kind.clone(),
        source_family: event.source_family.clone(),
        manifest_version: event.manifest_version,
        source_manifest_id: event.source_manifest_id,
        emitting_address: event
            .raw_fact_ref
            .get("emitting_address")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        state_scope: event
            .raw_fact_ref
            .get("state_scope")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        block_timestamp: event
            .block_number
            .map(|number| OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number)),
        after_state: event.after_state.clone(),
    }
}

#[test]
#[ignore = "diagnostic reconciliation timing probe"]
fn same_transaction_reconciliation_complexity_probe() {
    let cases = std::env::var("BIGNAME_RECON_PROBE_CASES")
        .unwrap_or_else(|_| "1x64,2x64,4x64,8x64,16x64".to_owned());
    for case in cases.split(',') {
        let (transactions, logs_per_transaction) = case
            .split_once('x')
            .expect("probe cases must use TRANSACTIONSxLOGS_PER_TRANSACTION");
        let transactions = transactions.parse::<usize>().expect("transaction count");
        let logs_per_transaction = logs_per_transaction
            .parse::<usize>()
            .expect("logs per transaction");
        let mut output = dense_reconciliation_output(transactions, logs_per_transaction);
        let normalized_events_before = output.normalized_events.len();
        let pairwise_json_started = std::time::Instant::now();
        let pairwise_json_matches = reconciliation_probe_pairwise_json(&output.normalized_events);
        let pairwise_json_elapsed = pairwise_json_started.elapsed();
        let typed_started = std::time::Instant::now();
        let typed = output
            .normalized_events
            .iter()
            .map(ReconciliationProbeFields::extract)
            .collect::<Vec<_>>();
        let pairwise_typed_matches = reconciliation_probe_pairwise_typed(&typed);
        let pairwise_typed_elapsed = typed_started.elapsed();
        let started = std::time::Instant::now();
        super::protocol::reconcile_same_transaction_setups_for_test(&mut output);
        let elapsed = started.elapsed();
        assert_eq!(pairwise_json_matches, pairwise_typed_matches);
        std::hint::black_box(&output);
        eprintln!(
            "reconcile_probe transactions={transactions} logs_per_transaction={logs_per_transaction} raw_logs={} normalized_events={normalized_events_before} pairwise_json_ms={:.3} pairwise_typed_ms={:.3} elapsed_ms={:.3}",
            transactions * logs_per_transaction,
            pairwise_json_elapsed.as_secs_f64() * 1_000.0,
            pairwise_typed_elapsed.as_secs_f64() * 1_000.0,
            elapsed.as_secs_f64() * 1_000.0,
        );
    }
}

fn reconciliation_probe_pairwise_json(events: &[NormalizedEvent]) -> usize {
    events
        .iter()
        .filter(|event| event.event_kind == "RegistrationGranted")
        .map(|registration| {
            let transaction_hash = registration.transaction_hash.as_deref().unwrap();
            let registration_log_index = registration.log_index.unwrap();
            let namehash = registration
                .after_state
                .get("namehash")
                .unwrap()
                .as_str()
                .unwrap();
            events
                .iter()
                .filter(|event| {
                    event.transaction_hash.as_deref() == Some(transaction_hash)
                        && event
                            .log_index
                            .is_some_and(|index| index < registration_log_index)
                        && event
                            .after_state
                            .get("child_node")
                            .or_else(|| event.after_state.get("node"))
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|target| target.eq_ignore_ascii_case(namehash))
                })
                .count()
        })
        .sum()
}

struct ReconciliationProbeFields<'a> {
    registration_namehash: Option<&'a str>,
    target_namehash: Option<&'a str>,
    transaction_hash: Option<&'a str>,
    log_index: Option<i64>,
}

impl<'a> ReconciliationProbeFields<'a> {
    fn extract(event: &'a NormalizedEvent) -> Self {
        Self {
            registration_namehash: (event.event_kind == "RegistrationGranted")
                .then(|| event.after_state.get("namehash")?.as_str())
                .flatten(),
            target_namehash: event
                .after_state
                .get("child_node")
                .or_else(|| event.after_state.get("node"))
                .and_then(serde_json::Value::as_str),
            transaction_hash: event.transaction_hash.as_deref(),
            log_index: event.log_index,
        }
    }
}

fn reconciliation_probe_pairwise_typed(events: &[ReconciliationProbeFields<'_>]) -> usize {
    events
        .iter()
        .filter_map(|registration| Some((registration.registration_namehash?, registration)))
        .map(|(namehash, registration)| {
            events
                .iter()
                .filter(|event| {
                    event.transaction_hash == registration.transaction_hash
                        && event
                            .log_index
                            .zip(registration.log_index)
                            .is_some_and(|(index, registration_index)| index < registration_index)
                        && event
                            .target_namehash
                            .is_some_and(|target| target.eq_ignore_ascii_case(namehash))
                })
                .count()
        })
        .sum()
}

fn dense_reconciliation_output(transactions: usize, logs_per_transaction: usize) -> BatchOutput {
    const EMITTER: &str = "0x0000000000000000000000000000000000000001";
    const REGISTRANT: &str = "0x0000000000000000000000000000000000000002";
    let mut normalized_events = Vec::new();
    let shapes_per_transaction = logs_per_transaction / 4;
    for transaction in 0..transactions {
        let transaction_hash = format!("0x{transaction:064x}");
        for shape in 0..shapes_per_transaction {
            let ordinal = transaction * shapes_per_transaction + shape;
            let namehash = format!("0x{:064x}", ordinal + 1);
            let logical_name_id = format!("ens:{namehash}");
            let stale_resource = Uuid::from_u128((ordinal as u128) * 2 + 1);
            let registrar_resource = Uuid::from_u128((ordinal as u128) * 2 + 2);
            let first_log = (shape * 4) as i64;
            normalized_events.push(reconciliation_probe_event(
                ordinal,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                &transaction_hash,
                transaction as i64,
                first_log,
                Some(stale_resource),
                json!({
                    "authority_kind":"registry_only",
                    "child_node":namehash,
                    "owner":EMITTER,
                    "source_event":"NewOwner",
                }),
            ));
            normalized_events.push(reconciliation_probe_event(
                ordinal,
                "PermissionChanged",
                "ens_v1_registry_l1",
                &transaction_hash,
                transaction as i64,
                first_log,
                Some(stale_resource),
                json!({
                    "authority_kind":"registry_only",
                    "grant_source":{"kind":"ens_v1_authority","authority_kind":"registry_only"},
                    "node":namehash,
                    "scope":{"kind":"resource"},
                    "subject":EMITTER,
                }),
            ));
            normalized_events.push(reconciliation_probe_event(
                ordinal,
                "RecordChanged",
                "ens_v1_resolver_l1",
                &transaction_hash,
                transaction as i64,
                first_log + 1,
                Some(stale_resource),
                json!({"node":namehash,"selector":"text:avatar","value":"dense"}),
            ));
            normalized_events.push(reconciliation_probe_event(
                ordinal,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                &transaction_hash,
                transaction as i64,
                first_log + 2,
                Some(stale_resource),
                json!({
                    "authority_kind":"registry_only",
                    "child_node":namehash,
                    "owner":REGISTRANT,
                    "source_event":"NewOwner",
                }),
            ));
            normalized_events.push(reconciliation_probe_event(
                ordinal,
                "PermissionChanged",
                "ens_v1_registry_l1",
                &transaction_hash,
                transaction as i64,
                first_log + 2,
                Some(stale_resource),
                json!({
                    "authority_kind":"registry_only",
                    "grant_source":{"kind":"ens_v1_authority","authority_kind":"registry_only"},
                    "node":namehash,
                    "scope":{"kind":"resource"},
                    "subject":REGISTRANT,
                }),
            ));
            let mut registration = reconciliation_probe_event(
                ordinal,
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                &transaction_hash,
                transaction as i64,
                first_log + 3,
                Some(registrar_resource),
                json!({
                    "authority_key":format!("registrar:{namehash}"),
                    "namehash":namehash,
                    "registrant":REGISTRANT,
                    "source_event":"NameRegistered",
                }),
            );
            registration.logical_name_id = Some(logical_name_id);
            registration.raw_fact_ref["emitting_address"] = json!(EMITTER);
            normalized_events.push(registration);
        }
        for unrelated in (shapes_per_transaction * 4)..logs_per_transaction {
            normalized_events.push(reconciliation_probe_event(
                transactions * shapes_per_transaction
                    + transaction * logs_per_transaction
                    + unrelated,
                "RecordChanged",
                "ens_v1_resolver_l1",
                &transaction_hash,
                transaction as i64,
                unrelated as i64,
                None,
                json!({
                    "node":format!("0x{:064x}", usize::MAX - unrelated),
                    "selector":"text:unrelated",
                    "value":"mixed",
                }),
            ));
        }
    }
    BatchOutput {
        normalized_events,
        ..BatchOutput::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn reconciliation_probe_event(
    ordinal: usize,
    event_kind: &str,
    source_family: &str,
    transaction_hash: &str,
    transaction_index: i64,
    log_index: i64,
    resource_id: Option<Uuid>,
    after_state: serde_json::Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("probe:{ordinal}:{event_kind}:{log_index}"),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(1),
        chain_id: CHAIN.to_owned(),
        block_number: Some(1),
        block_hash: Some("0xdense".to_owned()),
        transaction_hash: Some(transaction_hash.to_owned()),
        transaction_index: Some(transaction_index),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            seam::INTERPRETER_STATE_KEY:"probe-state",
            seam::STATE_SCOPE_KEY:"probe-scope",
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state,
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "activated".to_owned(),
        before_state_explicit: false,
    }
}
