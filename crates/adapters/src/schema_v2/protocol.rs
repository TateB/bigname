mod migration;
pub(super) mod permissions;
mod standard_approvals;
pub(super) mod v1;

#[cfg(test)]
pub(super) fn reconcile_same_transaction_setups_for_test(output: &mut super::model::BatchOutput) {
    v1::reconcile_same_transaction_setups(output);
}
pub(super) mod v2_registry;
mod v2_resolver;

use anyhow::bail;
use serde_json::Value;
use uuid::Uuid;

use super::{
    catalog::Selected,
    manifest::{ManifestEvent, ManifestSource},
    model::{DiscoveryRuleInput, RawLogInput},
    state::State,
};

pub(super) fn role_insensitivity_justification(
    source_family: &str,
    event: &str,
) -> Option<&'static str> {
    bigname_manifests::role_insensitivity_justification(source_family, event)
}

pub(super) fn event_allows_empty_emitter_roles(
    source_family: &str,
    event: &str,
    has_registry_announcement_rule: bool,
) -> bool {
    bigname_manifests::event_allows_empty_emitter_roles(
        source_family,
        event,
        has_registry_announcement_rule,
    )
}

#[derive(Clone, Debug)]
pub(super) struct Interpreted {
    pub events: Vec<EventDraft>,
    pub sourced_events: Vec<SourcedEventBatch>,
    pub boundary_events: Vec<EventDraft>,
    pub labels: Vec<LabelDraft>,
    pub names: Vec<NameDraft>,
    pub shadow_names: Vec<ShadowNameDraft>,
    pub resources: Vec<ResourceDraft>,
    pub binding_closures: Vec<BindingClosureDraft>,
    pub bindings: Vec<BindingDraft>,
    pub discovery: Vec<DiscoveryDraft>,
    /// Rows whose raw log belongs to a separately admitted ENSv1 registrar manifest while
    /// persisted normalized provenance remains `ens_v2_migration_l1`.
    pub migration_events: Vec<EventDraft>,
    pub migration_observations: Vec<MigrationObservation>,
}

impl Interpreted {
    pub(super) fn new() -> Self {
        Self {
            events: Vec::new(),
            sourced_events: Vec::new(),
            boundary_events: Vec::new(),
            labels: Vec::new(),
            names: Vec::new(),
            shadow_names: Vec::new(),
            resources: Vec::new(),
            binding_closures: Vec::new(),
            bindings: Vec::new(),
            discovery: Vec::new(),
            migration_events: Vec::new(),
            migration_observations: Vec::new(),
        }
    }

    pub(super) fn append(&mut self, other: &mut Self) {
        self.events.append(&mut other.events);
        self.sourced_events.append(&mut other.sourced_events);
        self.boundary_events.append(&mut other.boundary_events);
        self.labels.append(&mut other.labels);
        self.names.append(&mut other.names);
        self.shadow_names.append(&mut other.shadow_names);
        self.resources.append(&mut other.resources);
        self.binding_closures.append(&mut other.binding_closures);
        self.bindings.append(&mut other.bindings);
        self.discovery.append(&mut other.discovery);
        self.migration_events.append(&mut other.migration_events);
        self.migration_observations
            .append(&mut other.migration_observations);
    }
}

#[derive(Clone, Debug)]
pub(super) struct SourcedEventBatch {
    pub source_manifest_id: i64,
    pub events: Vec<EventDraft>,
}

#[derive(Clone, Debug)]
pub(super) struct MigrationObservation {
    pub source_family: String,
    pub source_manifest_id: i64,
    pub event_name: String,
    pub emitter_role: Option<String>,
    pub contract_instance_id: Uuid,
    pub raw: RawLogInput,
    pub decoded: Value,
    pub correlated_wrapper_expiry: Option<u64>,
}

pub(super) fn v2_boundary_expiration(
    transition: super::state::V2NameTransition,
    released_at: i64,
) -> anyhow::Result<Interpreted> {
    v2_registry::boundary_expiration(transition, released_at)
}

#[derive(Clone, Debug)]
pub(super) struct EventDraft {
    pub event_kind: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub identity_suffix: String,
    pub explicit_before: Option<Value>,
    pub after_state: Value,
    pub state_scope: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LabelDraft {
    pub raw_label: Vec<u8>,
    pub source_kind: String,
    /// The caller already emitted the byte-bearing
    /// [preimage observation](../../../../docs/glossary.md#preimage-observation--label-preimage)
    /// for this labelhash.
    pub explicit_preimage_observed: bool,
}

impl LabelDraft {
    pub(super) fn skips_automatic_preimage(&self, already_represented: bool) -> bool {
        self.raw_label.is_empty() || self.explicit_preimage_observed || already_represented
    }
}

#[derive(Clone, Debug)]
pub(super) struct ShadowNameDraft {
    pub raw_labels: Vec<Vec<u8>>,
    pub namehash: String,
    pub source_kind: String,
}

pub(super) fn raw_name_observation(
    raw_name: &[u8],
    source_kind: &str,
) -> (Vec<LabelDraft>, Vec<ShadowNameDraft>) {
    if raw_name.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let raw_labels = raw_name
        .split(|byte| *byte == b'.')
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let raw_namehash = super::common::namehash_raw(raw_labels.iter().map(Vec::as_slice));
    let has_empty_segment = raw_labels.iter().any(Vec::is_empty);
    if !has_empty_segment && super::common::surface_labels(&raw_labels).is_some() {
        (
            raw_labels
                .into_iter()
                .map(|raw_label| LabelDraft {
                    raw_label,
                    source_kind: source_kind.to_owned(),
                    explicit_preimage_observed: false,
                })
                .collect(),
            Vec::new(),
        )
    } else {
        (
            Vec::new(),
            vec![ShadowNameDraft {
                raw_labels,
                namehash: raw_namehash,
                source_kind: source_kind.to_owned(),
            }],
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct NameDraft {
    pub labels: Vec<String>,
    pub namehash: String,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub surface_binding_id: Option<Uuid>,
    pub bind: bool,
    pub binding_kind: String,
    pub authority_arm: String,
    pub source_kind: String,
    pub preimage_metadata: Option<Value>,
}

#[derive(Clone, Debug)]
pub(super) struct ResourceDraft {
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub(super) struct BindingClosureDraft {
    pub logical_name_id: String,
    pub authority_arm: String,
}

#[derive(Clone, Debug)]
pub(super) struct BindingDraft {
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub binding_kind: String,
    pub authority_arm: String,
    pub surface_binding_id: Option<Uuid>,
    pub active_from: Option<time::OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub(super) enum DiscoveryDraft {
    RegistryAnnouncement,
    Close {
        edge_kind: String,
        observation_key: String,
    },
    Edge {
        edge_kind: String,
        to_address: String,
        admission_basis: String,
        observation_key: String,
    },
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    registrar_migration_enabled: bool,
) -> anyhow::Result<Interpreted> {
    if let Some(output) = standard_approvals::interpret(selected, raw)? {
        return Ok(output);
    }
    let mut output = match selected.source.source_family.as_str() {
        family if family.starts_with("ens_v1_") || family.starts_with("basenames_") => {
            v1::interpret(selected, raw, state, registrar_migration_enabled)
        }
        "ens_v2_registry_l1" | "ens_v2_root_l1" | "ens_v2_registrar_l1" => {
            v2_registry::interpret(selected, raw, state)
        }
        "ens_v2_resolver_l1" => v2_resolver::interpret(selected, raw, state),
        "ens_v2_migration_l1" => migration::interpret(selected, raw),
        family => bail!("source family {family} has no schema-v2 adapter"),
    }?;
    for event in &mut output.events {
        if event.state_scope.is_empty() {
            event.state_scope = state_scope(selected, raw, event);
        }
    }
    Ok(output)
}

pub(super) fn reconcile_batch(output: &mut super::model::BatchOutput) {
    v1::reconcile_same_transaction_setups(output);
}

fn state_scope(selected: &Selected, raw: &RawLogInput, event: &EventDraft) -> String {
    let after = &event.after_state;
    let subject = after.get("subject").and_then(Value::as_str).unwrap_or("-");
    let node = after
        .get("child_node")
        .or_else(|| after.get("reverse_node"))
        .or_else(|| after.get("node"))
        .or_else(|| {
            after
                .get("primary_claim_source")
                .and_then(|source| source.get("reverse_node"))
        })
        .and_then(Value::as_str)
        .unwrap_or("-");
    let token = after
        .get("token_id")
        .or_else(|| after.get("current_token_id"))
        .or_else(|| after.get("old_token_id"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    let source_event = after
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or(event.event_kind.as_str());
    let selector = if selected.source.source_family == "ens_v1_wrapper_l1"
        && matches!(source_event, "NameWrapped" | "ExpiryExtended" | "FusesSet")
    {
        "wrapper".to_owned()
    } else if event.event_kind == "RecordChanged"
        && let Some(record_key) = after.get("record_key").and_then(Value::as_str)
    {
        record_key.to_owned()
    } else {
        match source_event {
            "AddrChanged" => "address:60".to_owned(),
            "AddressChanged" | "NamedAddrResource" => format!(
                "address:{}",
                after
                    .get("coin_type")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
            ),
            "TextChanged" | "NamedTextResource" => format!(
                "text:{}",
                after.get("key").and_then(Value::as_str).unwrap_or("-")
            ),
            "VersionChanged" => "version".to_owned(),
            "NameChanged" => "name".to_owned(),
            "ContentChanged" => "content".to_owned(),
            "ContenthashChanged" => "contenthash".to_owned(),
            "ABIChanged" => format!(
                "abi:{}",
                after
                    .get("content_type")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
            ),
            "InterfaceChanged" => format!(
                "interface:{}",
                after
                    .get("interface_id")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
            ),
            "DNSRecordChanged" | "DNSRecordDeleted" => format!(
                "dns:{}:{}",
                after.get("name").and_then(Value::as_str).unwrap_or("-"),
                after
                    .get("resource")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "-".to_owned())
            ),
            "DNSZonehashChanged" => "dns-zonehash".to_owned(),
            source => source.to_owned(),
        }
    };
    format!(
        "{}:{node}:{token}:{subject}:{selector}",
        raw.emitting_address.to_ascii_lowercase()
    )
}

pub(super) fn validate_manifest(
    source: &ManifestSource,
    rules: &[DiscoveryRuleInput],
) -> anyhow::Result<()> {
    for event in &source.events {
        if !supports_signature(&source.source_family, &event.signature) {
            bail!(
                "source family {} has no typed schema-v2 adapter for {}",
                source.source_family,
                event.signature
            );
        }
        if bigname_manifests::is_address_scoped_approval(&source.source_family, &event.signature)
            && !event.normalized_events.is_empty()
        {
            bail!(
                "source family {} adapter-owned standard approval mapping {} must declare no normalized events",
                source.source_family,
                event.signature
            );
        }
        let has_registry_announcement_rule = rules.iter().any(|rule| {
            rule.manifest_id == source.manifest_id && rule.edge_kind == "registry_announcement"
        });
        if event.emitter_roles.is_empty()
            && !event_allows_empty_emitter_roles(
                &source.source_family,
                &event.name,
                has_registry_announcement_rule,
            )
        {
            bail!(
                "manifest {} source family {} event {} has empty emitter_roles; declare emitter_roles, or add the (source_family, event) pair to bigname_manifests::ROLE_INSENSITIVE_EVENTS with a justification that the adapter does not consume Selected.emitter_role",
                source.manifest_id,
                source.source_family,
                event.name,
            );
        }
    }
    Ok(())
}

pub(super) fn is_match_all(
    source: &ManifestSource,
    event: &ManifestEvent,
    rules: &[DiscoveryRuleInput],
) -> bool {
    match source.source_family.as_str() {
        "ens_v1_resolver_l1" | "basenames_base_resolver" => {
            !bigname_manifests::is_address_scoped_approval(&source.source_family, &event.signature)
        }
        "ens_v2_registry_l1" if event.name == "RegistryCreated" => rules.iter().any(|rule| {
            rule.manifest_id == source.manifest_id && rule.edge_kind == "registry_announcement"
        }),
        "ens_v2_resolver_l1" => matches!(
            event.name.as_str(),
            "AliasChanged" | "NamedResource" | "NamedTextResource" | "NamedAddrResource"
        ),
        _ => false,
    }
}

fn supports_signature(source_family: &str, signature: &str) -> bool {
    if bigname_manifests::is_address_scoped_approval(source_family, signature) {
        return true;
    }
    match source_family {
        "ens_v1_registrar_l1" => matches!(
            signature,
            "ControllerAdded(address)"
                | "ControllerRemoved(address)"
                | "NameRegistered(uint256,address,uint256)"
                | "NameRenewed(uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)"
                | "NameRenewed(string,bytes32,uint256)"
                | "NameRenewed(string,bytes32,uint256,uint256)"
                | "NameRenewed(string,bytes32,uint256,uint256,bytes32)"
                | "Transfer(address,address,uint256)"
                | "Upgraded(address)"
        ),
        "basenames_base_registrar" => matches!(
            signature,
            "NameRegistered(string,bytes32,address,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256,uint256)"
                | "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)"
                | "NameRenewed(string,bytes32,uint256)"
                | "NameRenewed(string,bytes32,uint256,uint256)"
                | "NameRenewed(string,bytes32,uint256,uint256,bytes32)"
                | "Transfer(address,address,uint256)"
                | "Upgraded(address)"
        ),
        "ens_v2_migration_l1" => matches!(
            signature,
            "ProxyDeployed(address,address,uint256,address)"
                | "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)"
        ),
        "ens_v1_registry_l1" | "basenames_base_registry" => matches!(
            signature,
            "NewOwner(bytes32,bytes32,address)"
                | "Transfer(bytes32,address)"
                | "NewResolver(bytes32,address)"
                | "NewTTL(bytes32,uint64)"
        ),
        "ens_v1_resolver_l1" | "basenames_base_resolver" => matches!(
            signature,
            "ABIChanged(bytes32,uint256)"
                | "AddrChanged(bytes32,address)"
                | "AddressChanged(bytes32,uint256,bytes)"
                | "ContentChanged(bytes32,bytes32)"
                | "ContenthashChanged(bytes32,bytes)"
                | "DNSRecordChanged(bytes32,bytes,uint16,bytes)"
                | "DNSRecordDeleted(bytes32,bytes,uint16)"
                | "DNSZonehashChanged(bytes32,bytes,bytes)"
                | "DataChanged(bytes32,string,string,bytes)"
                | "InterfaceChanged(bytes32,bytes4,address)"
                | "NameChanged(bytes32,string)"
                | "TextChanged(bytes32,string,string)"
                | "TextChanged(bytes32,string,string,string)"
                | "VersionChanged(bytes32,uint64)"
        ),
        "ens_v1_wrapper_l1" => matches!(
            signature,
            "ExpiryExtended(bytes32,uint64)"
                | "FusesSet(bytes32,uint32)"
                | "NameUnwrapped(bytes32,address)"
                | "NameWrapped(bytes32,bytes,address,uint32,uint64)"
                | "TransferBatch(address,address,address,uint256[],uint256[])"
                | "TransferSingle(address,address,address,uint256,uint256)"
        ),
        "ens_v1_reverse_l1" => signature == "ReverseClaimed(address,bytes32)",
        "basenames_base_primary" => signature == "NameForAddrChanged(address,string)",
        "ens_v2_registrar_l1" => matches!(
            signature,
            "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)"
                | "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)"
        ),
        "ens_v2_registry_l1" | "ens_v2_root_l1" => matches!(
            signature,
            "RegistryCreated()"
                | "LabelRegistered(uint256,bytes32,string,address,uint64,address)"
                | "LabelReserved(uint256,bytes32,string,uint64,address)"
                | "LabelUnregistered(uint256,address)"
                | "ExpiryUpdated(uint256,uint64,address)"
                | "SubregistryUpdated(uint256,address,address)"
                | "ResolverUpdated(uint256,address,address)"
                | "TokenResource(uint256,uint256)"
                | "TransferSingle(address,address,address,uint256,uint256)"
                | "TransferBatch(address,address,address,uint256[],uint256[])"
                | "EACRolesChanged(uint256,address,uint256,uint256)"
                | "TokenRegenerated(uint256,uint256)"
                | "ParentUpdated(address,string,address)"
                | "Upgraded(address)"
        ),
        "ens_v2_resolver_l1" => matches!(
            signature,
            "AddressChanged(bytes32,uint256,bytes)"
                | "TextChanged(bytes32,string,string,string)"
                | "ContenthashChanged(bytes32,bytes)"
                | "NameChanged(bytes32,string)"
                | "VersionChanged(bytes32,uint64)"
                | "AliasChanged(bytes,bytes,bytes,bytes)"
                | "NamedResource(uint256,bytes)"
                | "NamedTextResource(uint256,bytes,bytes32,string)"
                | "NamedAddrResource(uint256,bytes,uint256)"
                | "EACRolesChanged(uint256,address,uint256,uint256)"
                | "Upgraded(address)"
        ),
        _ => false,
    }
}

pub(super) fn ensure_declared(selected: &Selected, expected: &[&str]) -> anyhow::Result<()> {
    for event_kind in expected {
        if !selected
            .event
            .normalized_events
            .iter()
            .any(|declared| declared == event_kind)
        {
            bail!(
                "manifest event {} for {} does not declare required normalized event {}",
                selected.event.signature,
                selected.source.source_family,
                event_kind
            );
        }
    }
    Ok(())
}
