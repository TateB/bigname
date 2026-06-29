use serde_json::json;

use super::*;

#[test]
fn registration_status_classifier_covers_authority_kind_domain() {
    let active = json!({
        "status": "active",
        "authority_kind": "registrar",
        "released_at": null,
        "expiry": "2000-01-01T00:00:00Z"
    });
    assert_eq!(
        classify_registration_status("ens", Some(&active), Some("0xabc"), true),
        RegistrationStatus::Active
    );
    assert_eq!(
        classify_registration_status("basenames", Some(&active), Some("0xabc"), true),
        RegistrationStatus::Active
    );

    let registered = json!({
        "status": "active",
        "authority_kind": "registry_only",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&registered), Some("0xabc"), true),
        RegistrationStatus::Registered
    );

    let ens_v2_registered = json!({
        "status": "active",
        "authority_kind": "ens_v2_registry",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&ens_v2_registered), Some("0xabc"), true),
        RegistrationStatus::Registered
    );

    let wrapped = json!({
        "status": "active",
        "authority_kind": "wrapper",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&wrapped), Some("0xabc"), true),
        RegistrationStatus::Wrapped
    );
    assert_eq!(
        classify_registration_status("basenames", Some(&wrapped), Some("0xabc"), true),
        RegistrationStatus::Unregistered
    );

    let released = json!({
        "status": "released",
        "authority_kind": "registrar",
        "released_at": "2026-06-14T00:00:00Z"
    });
    assert_eq!(
        classify_registration_status("ens", Some(&released), Some("0xabc"), true),
        RegistrationStatus::Released
    );

    let unregistered = json!({
        "status": "active",
        "authority_kind": "unknown_authority",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&active), Some("0xabc"), false),
        RegistrationStatus::Unregistered
    );
    assert_eq!(
        classify_registration_status("ens", Some(&unregistered), Some("0xabc"), true),
        RegistrationStatus::Unregistered
    );
}

#[test]
fn resolver_omits_unknown_chain_id_instead_of_guessing_mainnet() {
    let missing_chain = json!({
        "resolver": {
            "address": "0x0000000000000000000000000000000000000abc"
        }
    });
    assert_eq!(resolver(&missing_chain), None);

    let unknown_chain = json!({
        "resolver": {
            "chain_id": "unknown-mainnet",
            "address": "0x0000000000000000000000000000000000000abc"
        }
    });
    assert_eq!(resolver(&unknown_chain), None);
}
