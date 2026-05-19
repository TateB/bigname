use std::{collections::BTreeSet, fs, path::Path};

use super::*;

#[test]
fn normalized_event_producer_inventory_is_classified() {
    let actual = scan_normalized_event_producers();
    let expected = NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .map(|contract| contract.adapter)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn stateless_raw_fact_contracts_have_restricted_replay_proofs() {
    for contract in NORMALIZED_EVENT_REPLAY_CONTRACTS {
        if contract.raw_fact_replay_participant
            && contract.model == ReplayDependencyModel::StatelessRawFact
        {
            assert!(
                contract.closure_replay_supported,
                "{} must advertise restricted replay support",
                contract.adapter.as_str()
            );
            assert!(
                !contract.restricted_replay_proof_tests.is_empty(),
                "{} must name tests proving restricted replay",
                contract.adapter.as_str()
            );
        }
    }
}

#[test]
fn closure_or_dependency_contracts_do_not_claim_restricted_replay_proofs() {
    for contract in NORMALIZED_EVENT_REPLAY_CONTRACTS {
        if contract.raw_fact_replay_participant
            && contract.model != ReplayDependencyModel::StatelessRawFact
        {
            assert!(
                contract.restricted_replay_proof_tests.is_empty(),
                "{} must not cite restricted replay tests",
                contract.adapter.as_str()
            );
        }
    }
}

#[test]
fn implemented_full_closure_contracts_are_enumerated() {
    let actual = NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| contract.model != ReplayDependencyModel::StatelessRawFact)
        .filter(|contract| contract.closure_replay_supported)
        .map(|contract| contract.adapter)
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
        NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority,
        NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
        NormalizedEventReplayAdapter::EnsV2Registrar,
        NormalizedEventReplayAdapter::EnsV2Resolver,
        NormalizedEventReplayAdapter::EnsV2Permissions,
    ]);
    assert_eq!(actual, expected);
    assert!(
        unsupported_closure_replay_adapters(&actual.into_iter().collect::<Vec<_>>()).is_empty()
    );
}

fn scan_normalized_event_producers() -> BTreeSet<NormalizedEventReplayAdapter> {
    let mut adapters = BTreeSet::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("crates/adapters/src");
    scan_dir(root, &mut adapters);
    adapters
}

fn scan_dir(path: impl AsRef<Path>, adapters: &mut BTreeSet<NormalizedEventReplayAdapter>) {
    for entry in fs::read_dir(path.as_ref()).expect("adapter source directory must be readable") {
        let entry = entry.expect("adapter source entry must be readable");
        let path = entry.path();
        if path.is_dir() {
            scan_dir(&path, adapters);
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let path_text = path.to_string_lossy();
        if path_text.contains("/tests.rs") || path_text.ends_with("/normalized_event_support.rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("adapter source must be readable");
        if !source.contains("upsert_normalized_events") && !source.contains("NormalizedEvent {") {
            continue;
        }
        adapters.insert(adapter_for_producer_path(&path_text));
    }
}

fn adapter_for_producer_path(path: &str) -> NormalizedEventReplayAdapter {
    if path.contains("block_derived_normalized_events") {
        NormalizedEventReplayAdapter::BlockDerivedNormalizedEvents
    } else if path.contains("ens_v1_reverse_claim") {
        NormalizedEventReplayAdapter::EnsV1ReverseClaim
    } else if path.contains("ens_v1_subregistry_discovery") {
        NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery
    } else if path.contains("ens_v1_unwrapped_authority") {
        NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority
    } else if path.contains("ens_v2_registry") {
        NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface
    } else if path.contains("ens_v2_registrar") {
        NormalizedEventReplayAdapter::EnsV2Registrar
    } else if path.contains("ens_v2_resolver") {
        NormalizedEventReplayAdapter::EnsV2Resolver
    } else if path.contains("ens_v2_permissions") {
        NormalizedEventReplayAdapter::EnsV2Permissions
    } else if path.contains("manifest_normalized_events") {
        NormalizedEventReplayAdapter::ManifestNormalizedEvents
    } else {
        panic!("unclassified normalized-event producer path: {path}");
    }
}
