use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ManifestDriftAlertInspection, ManifestDriftAlertKind,
    ManifestDriftAlertObservation,
};
use clap::Parser;
use serde_json::json;
use sqlx::types::time::OffsetDateTime;

use crate::cli::{Cli, Command, ManifestDriftCommand, RawFactsCommand, ReplayCommand};
use crate::inspect;
use crate::manifest_drift::{
    enforce_manifest_drift_audit_exit_policy, manifest_proxy_implementation_candidate_observation,
};

fn fixed_manifest_drift_observed_at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000)
        .expect("fixed manifest drift timestamp must be valid")
}

fn manifest_drift_alert(status: &str) -> ManifestDriftAlertObservation {
    ManifestDriftAlertObservation {
        normalized_event_id: 1,
        event_identity: format!("manifest_alert:{status}"),
        alert_kind: ManifestDriftAlertKind::CodeHashDrift,
        namespace: "ens".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("eth-mainnet".to_owned()),
        block_number: Some(1),
        block_hash: Some("0x01".to_owned()),
        raw_fact_ref: json!({}),
        canonicality_state: CanonicalityState::Canonical,
        alert_state: json!({
            "alert_status": status,
        }),
        observed_at: fixed_manifest_drift_observed_at(),
    }
}

#[test]
fn replay_all_current_projections_cli_is_available() {
    let cli = Cli::parse_from(["bigname-worker", "replay", "all-current-projections"]);
    assert!(!cli.writes_machine_json());

    match cli.command {
        Command::Replay(args) => match args.command {
            ReplayCommand::AllCurrentProjections(args) => {
                assert!(!args.json);
                assert!(args.chain_rpc_urls.is_empty());
                assert_eq!(args.text_hydration_batch_size, 250);
            }
        },
        other => panic!("expected replay command, got {other:?}"),
    }
}

#[test]
fn replay_all_current_projections_json_flag_is_available() {
    let cli = Cli::parse_from([
        "bigname-worker",
        "replay",
        "all-current-projections",
        "--json",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Replay(args) => match args.command {
            ReplayCommand::AllCurrentProjections(args) => {
                assert!(args.json);
            }
        },
        other => panic!("expected replay command, got {other:?}"),
    }
}

#[test]
fn raw_facts_compact_log_staging_cli_is_available() {
    let cli = Cli::parse_from([
        "bigname-worker",
        "raw-facts",
        "compact-log-staging",
        "--dry-run",
        "--json",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::RawFacts(args) => match args.command {
            RawFactsCommand::CompactLogStaging(args) => {
                assert!(args.dry_run);
                assert!(args.json);
            }
        },
        other => panic!("expected raw-facts command, got {other:?}"),
    }
}

#[test]
fn inspect_canonicality_cli_is_available() {
    let cli = Cli::parse_from([
        "bigname-worker",
        "inspect",
        "canonicality",
        "--chain-id",
        "eth-mainnet",
        "--block-hash",
        "0xabc",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Inspect(args) => match args.command {
            inspect::InspectCommand::Canonicality(args) => {
                assert_eq!(args.chain_id, "eth-mainnet");
                assert_eq!(args.block_hash, "0xabc");
            }
            other => panic!("expected canonicality inspect command, got {other:?}"),
        },
        other => panic!("expected inspect command, got {other:?}"),
    }
}

#[test]
fn inspect_backfill_job_cli_is_available() {
    let cli = Cli::parse_from([
        "bigname-worker",
        "inspect",
        "backfill-job",
        "--backfill-job-id",
        "42",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Inspect(args) => match args.command {
            inspect::InspectCommand::BackfillJob(args) => {
                assert_eq!(args.backfill_job_id, 42);
            }
            other => panic!("expected backfill job inspect command, got {other:?}"),
        },
        other => panic!("expected inspect command, got {other:?}"),
    }
}

#[test]
fn inspect_execution_trace_cli_is_available() {
    let trace_id = "0e7ec7ac-e000-0000-0000-000000000abc";
    let cli = Cli::parse_from([
        "bigname-worker",
        "inspect",
        "execution-trace",
        "--execution-trace-id",
        trace_id,
        "--json",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Inspect(args) => match args.command {
            inspect::InspectCommand::ExecutionTrace(args) => {
                assert_eq!(args.execution_trace_id.to_string(), trace_id);
                assert!(args.json);
            }
            other => panic!("expected execution trace inspect command, got {other:?}"),
        },
        other => panic!("expected inspect command, got {other:?}"),
    }
}

#[test]
fn inspect_manifest_drift_cli_is_available() {
    let cli = Cli::parse_from(["bigname-worker", "inspect", "manifest-drift", "--json"]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Inspect(args) => match args.command {
            inspect::InspectCommand::ManifestDrift(args) => {
                assert!(args.json);
            }
            other => panic!("expected manifest drift inspect command, got {other:?}"),
        },
        other => panic!("expected inspect command, got {other:?}"),
    }
}

#[test]
fn manifest_drift_audit_cli_is_available() {
    let cli = Cli::parse_from(["bigname-worker", "manifest-drift", "audit", "--json"]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::ManifestDrift(args) => match args.command {
            ManifestDriftCommand::Audit(args) => {
                assert!(args.json);
                assert!(!args.fail_on_alert);
            }
        },
        other => panic!("expected manifest drift command, got {other:?}"),
    }
}

#[test]
fn manifest_drift_audit_fail_on_alert_cli_is_available() {
    let cli = Cli::parse_from([
        "bigname-worker",
        "manifest-drift",
        "audit",
        "--json",
        "--fail-on-alert",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::ManifestDrift(args) => match args.command {
            ManifestDriftCommand::Audit(args) => {
                assert!(args.json);
                assert!(args.fail_on_alert);
            }
        },
        other => panic!("expected manifest drift command, got {other:?}"),
    }
}

#[test]
fn manifest_drift_audit_exit_policy_fails_only_when_requested() -> Result<()> {
    let clean_inspection = ManifestDriftAlertInspection::default();
    let drift_inspection = ManifestDriftAlertInspection {
        code_hash_drift_alerts: vec![manifest_drift_alert("active")],
        proxy_implementation_alerts: vec![manifest_drift_alert("acknowledged")],
    };

    enforce_manifest_drift_audit_exit_policy(&clean_inspection, true)?;
    enforce_manifest_drift_audit_exit_policy(&drift_inspection, false)?;
    let error = enforce_manifest_drift_audit_exit_policy(&drift_inspection, true)
        .expect_err("fail-on-alert must fail when audit contains actionable alerts");
    assert!(
        error
            .to_string()
            .contains("manifest drift audit found 2 actionable persisted alert(s)"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn manifest_drift_audit_exit_policy_ignores_inactive_persisted_alerts() -> Result<()> {
    let inspection = ManifestDriftAlertInspection {
        code_hash_drift_alerts: vec![manifest_drift_alert("remediated")],
        proxy_implementation_alerts: vec![manifest_drift_alert("dismissed")],
    };

    enforce_manifest_drift_audit_exit_policy(&inspection, true)
}

#[test]
fn manifest_proxy_implementation_observation_uses_supplied_timestamp() -> Result<()> {
    let observed_at = fixed_manifest_drift_observed_at();
    let candidate = json!({
        "candidate_identity": "manifest_alert:proxy:test",
        "candidate_reason": "implementation_mismatch",
        "namespace": "ens",
        "source_family": "ens_v1_registry_l1",
        "manifest_version": 7,
        "source_manifest_id": 11,
        "chain": "eth-mainnet",
        "declaration": {
            "name": "RegistryProxy",
            "role": "registry",
            "proxy_kind": "eip1967"
        },
        "proxy": {
            "contract_instance_id": "contract:proxy",
            "address": "0x0000000000000000000000000000000000000001"
        },
        "expected_implementation": {
            "contract_instance_id": "contract:expected",
            "address": "0x0000000000000000000000000000000000000002"
        },
        "observed_implementation": {
            "contract_instance_id": "contract:observed",
            "address": "0x0000000000000000000000000000000000000003"
        },
        "implementation_edge": {
            "discovery_edge_id": 19,
            "admission": "active",
            "active_from_block_number": 100,
            "active_to_block_number": null,
            "provenance": {
                "source": "test"
            }
        }
    });

    let observation = manifest_proxy_implementation_candidate_observation(&candidate, observed_at)?;

    assert_eq!(observation.observed_at, observed_at);

    Ok(())
}

#[test]
fn inspect_watch_plan_cli_is_available() {
    let cli = Cli::parse_from(["bigname-worker", "inspect", "watch-plan", "--json"]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Inspect(args) => match args.command {
            inspect::InspectCommand::WatchPlan(args) => {
                assert!(args.json);
            }
            other => panic!("expected watch plan inspect command, got {other:?}"),
        },
        other => panic!("expected inspect command, got {other:?}"),
    }
}

#[test]
fn inspect_stored_lineage_range_cli_is_available() {
    let cli = Cli::parse_from([
        "bigname-worker",
        "inspect",
        "stored-lineage-range",
        "--chain-id",
        "eth-mainnet",
        "--range-start-block-number",
        "10",
        "--range-end-block-number",
        "12",
    ]);
    assert!(cli.writes_machine_json());

    match cli.command {
        Command::Inspect(args) => match args.command {
            inspect::InspectCommand::StoredLineageRange(args) => {
                assert_eq!(args.chain_id, "eth-mainnet");
                assert_eq!(args.range_start_block_number, 10);
                assert_eq!(args.range_end_block_number, 12);
            }
            other => panic!("expected stored lineage range inspect command, got {other:?}"),
        },
        other => panic!("expected inspect command, got {other:?}"),
    }
}
