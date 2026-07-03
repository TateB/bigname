use serde::{Deserialize, Serialize};

use super::{V2Error, V2Result};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    NotFound,
    InvalidName,
    Mismatch,
    Unsupported,
    Stale,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OpsStatus {
    Ready,
    Degraded,
    Stale,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Completeness {
    Full,
    Partial,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Indexed,
    Verified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Finality {
    Latest,
    Safe,
    Finalized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryScope {
    Name,
    Registration,
    Both,
}

impl HistoryScope {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Registration => "registration",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryEventType {
    Registration,
    Renewal,
    Release,
    Expiry,
    Transfer,
    Authority,
    Resolver,
    Record,
    PrimaryName,
    Permission,
}

impl HistoryEventType {
    pub(crate) const ALL: [Self; 10] = [
        Self::Registration,
        Self::Renewal,
        Self::Release,
        Self::Expiry,
        Self::Transfer,
        Self::Authority,
        Self::Resolver,
        Self::Record,
        Self::PrimaryName,
        Self::Permission,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Registration => "registration",
            Self::Renewal => "renewal",
            Self::Release => "release",
            Self::Expiry => "expiry",
            Self::Transfer => "transfer",
            Self::Authority => "authority",
            Self::Resolver => "resolver",
            Self::Record => "record",
            Self::PrimaryName => "primary_name",
            Self::Permission => "permission",
        }
    }

    pub(crate) const fn storage_event_kinds(self) -> &'static [&'static str] {
        match self {
            Self::Registration => &["RegistrationGranted", "LabelRegistered"],
            Self::Renewal => &["RegistrationRenewed"],
            Self::Release => &["RegistrationReleased"],
            Self::Expiry => &["ExpiryChanged"],
            Self::Transfer => &["TokenControlTransferred"],
            Self::Authority => &["AuthorityTransferred", "AuthorityEpochChanged"],
            Self::Resolver => &["ResolverChanged"],
            Self::Record => &["RecordChanged", "RecordVersionChanged"],
            Self::PrimaryName => &["ReverseChanged"],
            Self::Permission => &[
                "PermissionChanged",
                "PermissionScopeChanged",
                "RolesChanged",
                "EACRolesChanged",
            ],
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegistrationStatus {
    Active,
    Wrapped,
    Registered,
    Released,
    Unregistered,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Relation {
    Owner,
    Manager,
    Registrant,
}

impl Relation {
    pub(crate) const ALL: [Self; 3] = [Self::Owner, Self::Manager, Self::Registrant];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Manager => "manager",
            Self::Registrant => "registrant",
        }
    }

    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "manager" => Some(Self::Manager),
            "registrant" => Some(Self::Registrant),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelationSet {
    relations: Vec<Relation>,
}

impl RelationSet {
    pub(crate) fn all() -> Self {
        Self {
            relations: Relation::ALL.to_vec(),
        }
    }

    pub(crate) fn from_relations(relations: impl IntoIterator<Item = Relation>) -> Option<Self> {
        let requested = relations.into_iter().collect::<Vec<_>>();
        let mut normalized = Vec::new();
        for candidate in Relation::ALL {
            if requested.contains(&candidate) && !normalized.contains(&candidate) {
                normalized.push(candidate);
            }
        }
        (!normalized.is_empty()).then_some(Self {
            relations: normalized,
        })
    }

    pub(crate) fn as_slice(&self) -> &[Relation] {
        &self.relations
    }

    pub(crate) fn canonical_value(&self) -> String {
        self.relations
            .iter()
            .map(|relation| relation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn is_all(&self) -> bool {
        self.relations == Relation::ALL
    }

    pub(crate) fn is_exact_manager(&self) -> bool {
        self.relations == [Relation::Manager]
    }

    pub(crate) fn is_exact_owner_and_registrant(&self) -> bool {
        self.relations == [Relation::Owner, Relation::Registrant]
    }
}

impl From<Relation> for RelationSet {
    fn from(value: Relation) -> Self {
        Self {
            relations: vec![value],
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AddressNamesDedupe {
    Name,
    Registration,
}

impl AddressNamesDedupe {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Registration => "registration",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AddressNamesSort {
    Name,
    ExpiresAt,
    RegisteredAt,
}

impl AddressNamesSort {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::ExpiresAt => "expires_at",
            Self::RegisteredAt => "registered_at",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Resolver {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
}

pub(crate) const PRODUCT_PIPELINE_TERMS: &[&str] = &[
    "projection",
    "sidecar",
    "manifest_version",
    "manifest",
    "normalized_event",
    "normalized event",
    "permission_row",
    "raw_log",
    "raw_fact",
    "raw fact",
    "coverage",
    "resource_authority",
    "resource_rebound",
    "derivation_kind",
    "exhaustiveness",
    "enumeration_basis",
    "source_classes_considered",
    "address_names_current",
    "address_names_current_identity_counts",
    "address_names_current_identity_feed",
    "backfill_jobs",
    "backfill_ranges",
    "chain_checkpoints",
    "chain_header_audit",
    "chain_lineage",
    "children_current",
    "contract_instance_addresses",
    "contract_instances",
    "current_projection_replay_status",
    "discovery_edges",
    "event_silent_resolver_call_observations",
    "execution_cache_outcomes",
    "execution_steps",
    "execution_traces",
    "label_preimage_backfill_runs",
    "label_preimages",
    "manifest_alert_observations",
    "manifest_capability_flags",
    "manifest_contract_instances",
    "manifest_discovery_rules",
    "manifest_versions",
    "name_current",
    "name_surface_normalization_repair_findings",
    "name_surfaces",
    "normalized_events",
    "normalized_replay_adapter_checkpoint_items",
    "normalized_replay_adapter_checkpoints",
    "normalized_replay_cursors",
    "permission_current",
    "permissions_current",
    "primary_names_current",
    "projection_apply_cursors",
    "projection_invalidations",
    "projection_invalidation_dead_letters",
    "projection_normalized_event_changes",
    "raw_call_snapshots",
    "raw_code_hashes",
    "raw_logs",
    "raw_payload_cache_metadata",
    "raw_receipts",
    "raw_transactions",
    "record_inventory_current",
    "resolver_current",
    "resources",
    "surface_bindings",
    "token_lineages",
];

pub(crate) fn contains_boundary_vocabulary(candidate: &str, terms: &[&str]) -> bool {
    !matched_boundary_vocabulary_terms(candidate, terms).is_empty()
}

const SHARED_PRODUCT_REASON_MAP: &[(&str, &str)] = &[
    ("projection_read_failed", "read_failed"),
    (
        "ensv2_exact_name_profile_shadow",
        "exact_name_profile_not_supported",
    ),
    (
        "mixed_ensv1_ensv2_exact_name_corpus",
        "mixed_exact_name_corpus",
    ),
];

pub(crate) fn shared_product_reason(
    reason: &str,
    pipeline_rejection_log: &'static str,
    pipeline_rejection_error: &'static str,
) -> V2Result<String> {
    if let Some((_, product_reason)) = SHARED_PRODUCT_REASON_MAP
        .iter()
        .find(|(storage_reason, _)| *storage_reason == reason)
    {
        return Ok((*product_reason).to_owned());
    }

    if contains_boundary_vocabulary(reason, PRODUCT_PIPELINE_TERMS) {
        tracing::error!(%reason, "{}", pipeline_rejection_log);
        return Err(V2Error::internal_error(pipeline_rejection_error));
    }

    Ok(reason.to_owned())
}

pub(crate) fn matched_boundary_vocabulary_terms<'a>(
    candidate: &str,
    terms: &'a [&'a str],
) -> Vec<&'a str> {
    let normalized_candidate = normalize_pipeline_candidate(candidate);
    terms
        .iter()
        .copied()
        .filter(|term| pipeline_term_matches(&normalized_candidate, term))
        .collect()
}

fn pipeline_term_matches(normalized_candidate: &str, term: &str) -> bool {
    let normalized_term = normalize_pipeline_candidate(term);
    pipeline_term_variants(&normalized_term)
        .iter()
        .any(|variant| candidate_has_underscore_boundary_term(normalized_candidate, variant))
}

fn normalize_pipeline_candidate(candidate: &str) -> String {
    candidate
        .chars()
        .map(|ch| match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            '-' | ' ' => '_',
            _ => ch,
        })
        .collect()
}

fn pipeline_term_variants(term: &str) -> Vec<String> {
    let mut variants = vec![term.to_owned(), format!("{term}s"), format!("{term}es")];
    if let Some(singular) = term.strip_suffix('s') {
        variants.push(singular.to_owned());
    }
    variants.sort_unstable();
    variants.dedup();
    variants
}

fn candidate_has_underscore_boundary_term(candidate: &str, term: &str) -> bool {
    candidate
        .match_indices(term)
        .any(|(start, _)| term_match_has_underscore_boundaries(candidate, term, start))
}

fn term_match_has_underscore_boundaries(candidate: &str, term: &str, start: usize) -> bool {
    let before_is_boundary = start == 0 || candidate.as_bytes()[start - 1] == b'_';
    if !before_is_boundary {
        return false;
    }

    let end = start + term.len();
    if end == candidate.len() || candidate.as_bytes()[end] == b'_' {
        return true;
    }

    candidate.as_bytes()[end] == b's'
        && (end + 1 == candidate.len() || candidate.as_bytes()[end + 1] == b'_')
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    fn assert_wire<T: Serialize>(value: T, expected: &str) {
        let serialized = serde_json::to_value(value).expect("value must serialize");
        assert_eq!(serialized, serde_json::Value::String(expected.to_owned()));
    }

    #[test]
    fn status_variants_use_exact_wire_spelling() {
        assert_wire(Status::Ok, "ok");
        assert_wire(Status::NotFound, "not_found");
        assert_wire(Status::InvalidName, "invalid_name");
        assert_wire(Status::Mismatch, "mismatch");
        assert_wire(Status::Unsupported, "unsupported");
        assert_wire(Status::Stale, "stale");
        assert_wire(Status::Failed, "failed");
    }

    #[test]
    fn ops_status_variants_use_exact_wire_spelling() {
        assert_wire(OpsStatus::Ready, "ready");
        assert_wire(OpsStatus::Degraded, "degraded");
        assert_wire(OpsStatus::Stale, "stale");
    }

    #[test]
    fn completeness_variants_use_exact_wire_spelling() {
        assert_wire(Completeness::Full, "full");
        assert_wire(Completeness::Partial, "partial");
        assert_wire(Completeness::Unsupported, "unsupported");
    }

    #[test]
    fn source_variants_use_exact_wire_spelling() {
        assert_wire(Source::Indexed, "indexed");
        assert_wire(Source::Verified, "verified");
    }

    #[test]
    fn finality_variants_use_exact_wire_spelling() {
        assert_wire(Finality::Latest, "latest");
        assert_wire(Finality::Safe, "safe");
        assert_wire(Finality::Finalized, "finalized");
    }

    #[test]
    fn history_scope_variants_use_exact_wire_spelling() {
        assert_wire(HistoryScope::Name, "name");
        assert_wire(HistoryScope::Registration, "registration");
        assert_wire(HistoryScope::Both, "both");
    }

    #[test]
    fn history_event_type_variants_use_exact_wire_spelling() {
        assert_wire(HistoryEventType::Registration, "registration");
        assert_wire(HistoryEventType::Renewal, "renewal");
        assert_wire(HistoryEventType::Release, "release");
        assert_wire(HistoryEventType::Expiry, "expiry");
        assert_wire(HistoryEventType::Transfer, "transfer");
        assert_wire(HistoryEventType::Authority, "authority");
        assert_wire(HistoryEventType::Resolver, "resolver");
        assert_wire(HistoryEventType::Record, "record");
        assert_wire(HistoryEventType::PrimaryName, "primary_name");
        assert_wire(HistoryEventType::Permission, "permission");
    }

    #[test]
    fn history_event_type_storage_kinds_round_trip_to_product_types() {
        for event_type in [
            HistoryEventType::Registration,
            HistoryEventType::Renewal,
            HistoryEventType::Release,
            HistoryEventType::Expiry,
            HistoryEventType::Transfer,
            HistoryEventType::Authority,
            HistoryEventType::Resolver,
            HistoryEventType::Record,
            HistoryEventType::PrimaryName,
            HistoryEventType::Permission,
        ] {
            for storage_kind in event_type.storage_event_kinds() {
                assert_eq!(
                    crate::v2::history_event_type(storage_kind),
                    Some(event_type)
                );
            }
        }
    }

    #[test]
    fn registration_status_variants_use_exact_wire_spelling() {
        assert_wire(RegistrationStatus::Active, "active");
        assert_wire(RegistrationStatus::Wrapped, "wrapped");
        assert_wire(RegistrationStatus::Registered, "registered");
        assert_wire(RegistrationStatus::Released, "released");
        assert_wire(RegistrationStatus::Unregistered, "unregistered");
    }

    #[test]
    fn relation_variants_use_exact_wire_spelling() {
        assert_wire(Relation::Owner, "owner");
        assert_wire(Relation::Manager, "manager");
        assert_wire(Relation::Registrant, "registrant");
    }

    #[test]
    fn address_names_dedupe_variants_use_exact_wire_spelling() {
        assert_wire(AddressNamesDedupe::Name, "name");
        assert_wire(AddressNamesDedupe::Registration, "registration");
    }

    #[test]
    fn address_names_sort_variants_use_exact_wire_spelling() {
        assert_wire(AddressNamesSort::Name, "name");
        assert_wire(AddressNamesSort::ExpiresAt, "expires_at");
        assert_wire(AddressNamesSort::RegisteredAt, "registered_at");
    }

    #[test]
    fn boundary_vocabulary_matching_uses_underscore_boundaries_and_plural_suffixes() {
        const TERMS: &[&str] = &["coverage", "raw_fact", "normalized_events"];

        assert_eq!(
            matched_boundary_vocabulary_terms("insufficient_coverage", TERMS),
            vec!["coverage"]
        );
        assert!(contains_boundary_vocabulary("coverage_gap", TERMS));
        assert!(contains_boundary_vocabulary("coverages", TERMS));
        assert!(contains_boundary_vocabulary("raw facts", TERMS));
        assert!(contains_boundary_vocabulary("normalized_event", TERMS));
        assert!(contains_boundary_vocabulary(
            "identity_sidecar_missing",
            PRODUCT_PIPELINE_TERMS
        ));
        assert!(!contains_boundary_vocabulary("discoverage", TERMS));
        assert!(!contains_boundary_vocabulary("rawfactory", TERMS));
    }
}
