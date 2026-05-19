use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sqlx::Row;

use crate::ens_v1_resolver::{
    GENERIC_SOURCE_SCOPE_ADDRESS, SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s,
};

use super::ReplayRawLogSelection;
use crate::reconciliation::types::{
    RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
};

pub(crate) const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(crate) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(crate) const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
pub(crate) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(crate) const SOURCE_FAMILY_ENS_V1_REVERSE_L1: &str = "ens_v1_reverse_l1";
pub(crate) const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
pub(crate) const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
pub(crate) const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
pub(crate) const SOURCE_FAMILY_BASENAMES_BASE_PRIMARY: &str = "basenames_base_primary";
pub(crate) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
pub(crate) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
pub(crate) const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";

const BLOCK_DERIVED_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
];
const ENS_V1_REVERSE_CLAIM_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V1_REVERSE_L1,
    SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
];
const ENS_V1_SUBREGISTRY_DISCOVERY_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
];
const ENS_V1_UNWRAPPED_AUTHORITY_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
    SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
];
const ENS_V2_REGISTRY_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
];
const ENS_V2_REGISTRAR_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_REGISTRAR_L1];
const ENS_V2_RESOLVER_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_RESOLVER_L1];
const ENS_V2_PERMISSIONS_SOURCE_FAMILIES: &[&str] = &[SOURCE_FAMILY_ENS_V2_RESOLVER_L1];

const ENS_V2_REGISTRAR_DEPENDENCY_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
];
const ENS_V2_RESOLVER_DEPENDENCY_SOURCE_FAMILIES: &[&str] = &[
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplayDependencyModel {
    StatelessRawFact,
    StatefulClosureRequired,
    ContextualDependencyRequired,
}

impl ReplayDependencyModel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StatelessRawFact => "stateless_raw_fact",
            Self::StatefulClosureRequired => "stateful_closure_required",
            Self::ContextualDependencyRequired => "contextual_dependency_required",
        }
    }

    const fn restricted_replay_supported(self) -> bool {
        matches!(self, Self::StatelessRawFact)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum NormalizedEventReplayAdapter {
    BlockDerivedNormalizedEvents,
    EnsV1ReverseClaim,
    EnsV1SubregistryDiscovery,
    EnsV1UnwrappedAuthority,
    EnsV2RegistryResourceSurface,
    EnsV2Registrar,
    EnsV2Resolver,
    EnsV2Permissions,
    ManifestNormalizedEvents,
}

impl NormalizedEventReplayAdapter {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BlockDerivedNormalizedEvents => "block_derived_normalized_events",
            Self::EnsV1ReverseClaim => "ens_v1_reverse_claim",
            Self::EnsV1SubregistryDiscovery => "ens_v1_subregistry_discovery",
            Self::EnsV1UnwrappedAuthority => "ens_v1_unwrapped_authority",
            Self::EnsV2RegistryResourceSurface => "ens_v2_registry_resource_surface",
            Self::EnsV2Registrar => "ens_v2_registrar",
            Self::EnsV2Resolver => "ens_v2_resolver",
            Self::EnsV2Permissions => "ens_v2_permissions",
            Self::ManifestNormalizedEvents => "manifest_normalized_events",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdapterReplayContract {
    pub(crate) adapter: NormalizedEventReplayAdapter,
    pub(crate) model: ReplayDependencyModel,
    pub(crate) raw_fact_replay_participant: bool,
    pub(crate) source_families: &'static [&'static str],
    pub(crate) closure_source_families: &'static [&'static str],
    pub(crate) dependency_adapters: &'static [NormalizedEventReplayAdapter],
    pub(crate) producer_paths: &'static [&'static str],
    pub(crate) restricted_replay_proof_tests: &'static [&'static str],
    pub(crate) closure_replay_supported: bool,
    pub(crate) replay_note: &'static str,
}

const NO_DEPENDENCIES: &[NormalizedEventReplayAdapter] = &[];
const ENS_V2_REGISTRY_DEPENDENCY: &[NormalizedEventReplayAdapter] =
    &[NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface];

pub(crate) const NORMALIZED_EVENT_REPLAY_CONTRACTS: &[AdapterReplayContract] = &[
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::BlockDerivedNormalizedEvents,
        model: ReplayDependencyModel::StatelessRawFact,
        raw_fact_replay_participant: true,
        source_families: BLOCK_DERIVED_SOURCE_FAMILIES,
        closure_source_families: BLOCK_DERIVED_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/block_derived_normalized_events"],
        restricted_replay_proof_tests: &[
            "replay_normalized_events_is_idempotent_without_checkpoint_mutation",
            "sync_block_derived_normalized_events_is_idempotent",
            "sync_block_derived_normalized_events_replays_scoped_selected_logs_without_payload_rows",
        ],
        closure_replay_supported: true,
        replay_note: "preimage rows are decoded from each selected raw log and manifest event-topic constants",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV1ReverseClaim,
        model: ReplayDependencyModel::StatelessRawFact,
        raw_fact_replay_participant: true,
        source_families: ENS_V1_REVERSE_CLAIM_SOURCE_FAMILIES,
        closure_source_families: ENS_V1_REVERSE_CLAIM_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v1_reverse_claim"],
        restricted_replay_proof_tests: &[
            "replay_normalized_events_runs_full_persisted_raw_adapter_boundary",
            "replay_normalized_events_scoped_block_range_selects_only_requested_targets",
            "replay_normalized_events_skips_noncanonical_raw_logs_in_selected_block_hashes",
        ],
        closure_replay_supported: true,
        replay_note: "reverse rows are decoded from the selected ReverseClaimed raw log",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V1_SUBREGISTRY_DISCOVERY_SOURCE_FAMILIES,
        closure_source_families: ENS_V1_SUBREGISTRY_DISCOVERY_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v1_subregistry_discovery"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "normalized rows include discovery-edge contract-instance context, so raw-log selection alone is insufficient",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV1UnwrappedAuthority,
        model: ReplayDependencyModel::StatefulClosureRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V1_UNWRAPPED_AUTHORITY_SOURCE_FAMILIES,
        closure_source_families: ENS_V1_UNWRAPPED_AUTHORITY_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v1_unwrapped_authority"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "authority state, before_state, resource identity, and permission provenance depend on ordered in-memory history",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2RegistryResourceSurface,
        model: ReplayDependencyModel::StatefulClosureRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_REGISTRY_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_REGISTRY_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v2_registry"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "registry/resource rows depend on ordered token, suffix, binding, and discovery state",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2Registrar,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_REGISTRAR_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_REGISTRAR_DEPENDENCY_SOURCE_FAMILIES,
        dependency_adapters: ENS_V2_REGISTRY_DEPENDENCY,
        producer_paths: &["crates/adapters/src/ens_v2_registrar"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "registrar rows resolve logical_name_id/resource_id through stable ENSv2 registry normalized output",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2Resolver,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_RESOLVER_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_RESOLVER_DEPENDENCY_SOURCE_FAMILIES,
        dependency_adapters: ENS_V2_REGISTRY_DEPENDENCY,
        producer_paths: &["crates/adapters/src/ens_v2_resolver"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "resolver rows resolve name/resource links from stable name_surfaces and surface_bindings",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::EnsV2Permissions,
        model: ReplayDependencyModel::StatefulClosureRequired,
        raw_fact_replay_participant: true,
        source_families: ENS_V2_PERMISSIONS_SOURCE_FAMILIES,
        closure_source_families: ENS_V2_PERMISSIONS_SOURCE_FAMILIES,
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/ens_v2_permissions"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: true,
        replay_note: "permission resources and role events depend on prior resolver resource hint observations in canonical order",
    },
    AdapterReplayContract {
        adapter: NormalizedEventReplayAdapter::ManifestNormalizedEvents,
        model: ReplayDependencyModel::ContextualDependencyRequired,
        raw_fact_replay_participant: false,
        source_families: &[],
        closure_source_families: &[],
        dependency_adapters: NO_DEPENDENCIES,
        producer_paths: &["crates/adapters/src/manifest_normalized_events"],
        restricted_replay_proof_tests: &[],
        closure_replay_supported: false,
        replay_note: "manifest rows are derived from manifest, capability, code-hash, and discovery-edge corpus state, not raw-log replay",
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawFactReplayContractMode {
    StatelessRestricted,
    FullClosure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawFactReplayContractPlan {
    mode: RawFactReplayContractMode,
}

impl RawFactReplayContractPlan {
    pub(crate) const fn stateless_restricted() -> Self {
        Self {
            mode: RawFactReplayContractMode::StatelessRestricted,
        }
    }

    pub(crate) const fn full_closure() -> Self {
        Self {
            mode: RawFactReplayContractMode::FullClosure,
        }
    }

    pub(crate) fn permits_nonstateless_adapters(self) -> bool {
        match self.mode {
            RawFactReplayContractMode::StatelessRestricted => false,
            RawFactReplayContractMode::FullClosure => true,
        }
    }

    pub(crate) fn ensure_adapter_allowed(
        self,
        adapter: NormalizedEventReplayAdapter,
    ) -> Result<()> {
        let contract = replay_contract(adapter);
        if !contract.raw_fact_replay_participant {
            bail!(
                "normalized-event replay adapter {} is not part of raw-fact replay",
                adapter.as_str()
            );
        }
        if contract.model.restricted_replay_supported() {
            return Ok(());
        }
        if self.permits_nonstateless_adapters() && contract.closure_replay_supported {
            return Ok(());
        }
        bail!(
            "normalized-event replay adapter {} is classified as {}; raw-fact restricted replay is disabled until a documented closure/dependency replay session is implemented",
            adapter.as_str(),
            contract.model.as_str()
        )
    }
}

pub(crate) async fn classify_raw_fact_replay_contract(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    raw_log_selection: &ReplayRawLogSelection,
    source_scope: &[(String, String, i64, i64)],
) -> Result<RawFactReplayContractPlan> {
    let selected_contracts = selected_raw_fact_contracts(source_scope);
    let nonstateless_contracts = selected_contracts
        .iter()
        .copied()
        .map(replay_contract)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .collect::<Vec<_>>();
    if nonstateless_contracts.is_empty() {
        return Ok(RawFactReplayContractPlan::stateless_restricted());
    }

    let adapter_list = adapter_list(&nonstateless_contracts);
    let RawFactNormalizedEventReplaySelection::BlockRange { from_block, .. } = &request.selection
    else {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {adapter_list}; block-hash and source-scoped replay are disabled for these adapters"
        );
    };
    let from_block = *from_block;

    let Some((selected_from_block, _)) = raw_log_selection.range else {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {adapter_list} without a replay range"
        );
    };
    if selected_from_block != from_block {
        bail!(
            "normalized-event replay selected range {selected_from_block} but request starts at {from_block} for closure/context-dependent adapter(s) {adapter_list}"
        );
    }

    let closure_source_families = closure_source_families_for_contracts(&nonstateless_contracts);
    if let Some(closure_start_block) = earliest_required_raw_fact_block(
        pool,
        &request.chain,
        source_scope,
        &closure_source_families,
    )
    .await?
        && from_block > closure_start_block
    {
        bail!(
            "normalized-event replay for closure/context-dependent adapter(s) {adapter_list} must start at closure boundary block {closure_start_block}; requested block {from_block}"
        );
    }

    let unsupported_adapters = nonstateless_contracts
        .iter()
        .filter(|contract| !contract.closure_replay_supported)
        .map(|contract| contract.adapter.as_str())
        .collect::<Vec<_>>();
    if !unsupported_adapters.is_empty() {
        bail!(
            "normalized-event replay selected closure/context-dependent adapter(s) {}; full closure replay is not implemented for these adapters",
            unsupported_adapters.join(", ")
        );
    }

    Ok(RawFactReplayContractPlan::full_closure())
}

pub(crate) async fn chain_has_closure_or_dependency_replay_adapter(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM manifest_versions
            WHERE chain = $1
              AND rollout_status = 'active'::manifest_rollout_status
              AND source_family = ANY($2::TEXT[])
        )
        "#,
    )
    .bind(chain)
    .bind(closure_or_dependency_source_families())
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to classify normalized-event replay adapters for chain {chain}")
    })
}

pub(crate) async fn active_closure_or_dependency_replay_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<Vec<NormalizedEventReplayAdapter>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT source_family
        FROM manifest_versions
        WHERE chain = $1
          AND rollout_status = 'active'::manifest_rollout_status
          AND source_family = ANY($2::TEXT[])
        ORDER BY source_family
        "#,
    )
    .bind(chain)
    .bind(closure_or_dependency_source_families())
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to list active closure/dependency replay adapters for chain {chain}")
    })?;
    let active_source_families = rows
        .into_iter()
        .map(|row| row.get::<String, _>("source_family"))
        .collect::<BTreeSet<_>>();

    Ok(NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .filter(|contract| {
            active_source_families
                .iter()
                .any(|source_family| source_family_in(source_family, contract.source_families))
        })
        .map(|contract| contract.adapter)
        .collect())
}

pub(crate) fn unsupported_closure_replay_adapters(
    adapters: &[NormalizedEventReplayAdapter],
) -> Vec<&'static str> {
    adapters
        .iter()
        .copied()
        .map(replay_contract)
        .filter(|contract| !contract.closure_replay_supported)
        .map(|contract| contract.adapter.as_str())
        .collect()
}

pub(crate) fn replay_contract(
    adapter: NormalizedEventReplayAdapter,
) -> &'static AdapterReplayContract {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .find(|contract| contract.adapter == adapter)
        .expect("normalized-event replay adapter is missing a central replay contract")
}

pub(crate) fn source_scope_includes_adapter(
    source_scope: &[(String, String, i64, i64)],
    adapter: NormalizedEventReplayAdapter,
) -> bool {
    let contract = replay_contract(adapter);
    source_scope
        .iter()
        .any(|(source_family, _, _, _)| source_family_in(source_family, contract.source_families))
}

fn selected_raw_fact_contracts(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<NormalizedEventReplayAdapter> {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| {
            source_scope.iter().any(|(source_family, _, _, _)| {
                source_family_in(source_family, contract.source_families)
            })
        })
        .map(|contract| contract.adapter)
        .collect()
}

fn closure_source_families_for_contracts(
    contracts: &[&'static AdapterReplayContract],
) -> Vec<&'static str> {
    contracts
        .iter()
        .flat_map(|contract| contract.closure_source_families.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn closure_or_dependency_source_families() -> Vec<String> {
    NORMALIZED_EVENT_REPLAY_CONTRACTS
        .iter()
        .filter(|contract| contract.raw_fact_replay_participant)
        .filter(|contract| !contract.model.restricted_replay_supported())
        .flat_map(|contract| contract.source_families.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn adapter_list(contracts: &[&'static AdapterReplayContract]) -> String {
    contracts
        .iter()
        .map(|contract| contract.adapter.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

async fn earliest_required_raw_fact_block(
    pool: &sqlx::PgPool,
    chain: &str,
    source_scope: &[(String, String, i64, i64)],
    closure_source_families: &[&str],
) -> Result<Option<i64>> {
    let required_scope = source_scope
        .iter()
        .filter(|(source_family, _, _, _)| source_family_in(source_family, closure_source_families))
        .map(|(source_family, address, _, _)| (source_family.clone(), address.to_ascii_lowercase()))
        .collect::<Vec<_>>();
    if required_scope.is_empty() {
        return Ok(None);
    }

    let mut source_families = Vec::with_capacity(required_scope.len());
    let mut addresses = Vec::with_capacity(required_scope.len());
    for (source_family, address) in required_scope {
        source_families.push(source_family);
        addresses.push(address);
    }
    let generic_resolver_topic0s = generic_resolver_record_topic0s()
        .into_iter()
        .map(|topic0| topic0.to_ascii_lowercase())
        .collect::<Vec<_>>();

    let row = sqlx::query(
        r#"
        WITH required_scope AS (
            SELECT DISTINCT source_family, address
            FROM unnest($2::TEXT[], $3::TEXT[]) AS scope(source_family, address)
        )
        SELECT MIN(logs.block_number) AS closure_start_block
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM required_scope
              WHERE (
                    required_scope.address <> $4
                    AND LOWER(logs.emitting_address) = required_scope.address
                )
                OR (
                    required_scope.source_family = $5
                    AND required_scope.address = $4
                    AND LOWER(logs.topics[1]) = ANY($6::TEXT[])
                )
          )
        "#,
    )
    .bind(chain)
    .bind(&source_families)
    .bind(&addresses)
    .bind(GENERIC_SOURCE_SCOPE_ADDRESS)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .bind(&generic_resolver_topic0s)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to load normalized replay closure boundary for chain {chain}")
    })?;

    Ok(row.get::<Option<i64>, _>("closure_start_block"))
}

fn source_family_in(source_family: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| source_family == *candidate)
}

#[cfg(test)]
#[path = "classification/tests.rs"]
mod tests;
