use bigname_manifests::WatchedContractSource;
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use sqlx::types::{Uuid, time::OffsetDateTime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveEmitter {
    pub(super) address: String,
    pub(super) contract_instance_id: Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) normalizer_version: String,
    pub(super) role: Option<String>,
    pub(super) source: WatchedContractSource,
    pub(super) source_rank: i32,
    pub(super) active_from_block_number: Option<i64>,
    pub(super) active_to_block_number: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveManifestMetadata {
    pub(super) manifest_id: i64,
    pub(super) chain: String,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) normalizer_version: String,
    pub(super) role: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegistryRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) block_timestamp: OffsetDateTime,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) emitting_contract_instance_id: Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) normalizer_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegistryRawLogSourceScopeTarget {
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegistryNameState {
    pub(super) token_id: String,
    pub(super) labelhash: String,
    pub(super) label: String,
    pub(super) full_name: String,
    pub(super) name: NameMetadata,
    pub(super) owner: Option<String>,
    pub(super) expiry: Option<u64>,
    pub(super) status: &'static str,
    pub(super) first_ref: ObservationRef,
    pub(super) current_ref: ObservationRef,
    pub(super) registry_address: String,
    pub(super) registry_contract_instance_id: Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) resource: Option<RegistryResourceLink>,
    pub(super) resolver: Option<String>,
    pub(super) subregistry: Option<String>,
    pub(super) binding_kind: SurfaceBindingKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegistryResourceLink {
    pub(super) upstream_resource: String,
    pub(super) observed_token_id: String,
    pub(super) resource_id: Uuid,
    pub(super) token_lineage_id: Uuid,
    pub(super) surface_binding_id: Uuid,
    pub(super) linked_ref: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NameMetadata {
    pub(super) namespace: String,
    pub(super) logical_name_id: String,
    pub(super) input_name: String,
    pub(super) canonical_display_name: String,
    pub(super) normalized_name: String,
    pub(super) dns_encoded_name: Vec<u8>,
    pub(super) namehash: String,
    pub(super) labelhashes: Vec<String>,
    pub(super) normalizer_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ObservationRef {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) block_timestamp: OffsetDateTime,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) emitting_contract_instance_id: Uuid,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) namespace: String,
    pub(super) source_manifest_id: i64,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RegistryObservation {
    LabelRegistered {
        token_id: String,
        labelhash: String,
        label: String,
        owner: String,
        expiry: u64,
        sender: String,
        reference: ObservationRef,
    },
    LabelReserved {
        token_id: String,
        labelhash: String,
        label: String,
        expiry: u64,
        sender: String,
        reference: ObservationRef,
    },
    LabelUnregistered {
        token_id: String,
        sender: String,
        reference: ObservationRef,
    },
    ExpiryUpdated {
        token_id: String,
        new_expiry: u64,
        sender: String,
        reference: ObservationRef,
    },
    SubregistryUpdated {
        token_id: String,
        subregistry: String,
        sender: String,
        reference: ObservationRef,
    },
    ResolverUpdated {
        token_id: String,
        resolver: String,
        sender: String,
        reference: ObservationRef,
    },
    TokenResource {
        token_id: String,
        upstream_resource: String,
        reference: ObservationRef,
    },
    TokenRegenerated {
        old_token_id: String,
        new_token_id: String,
        reference: ObservationRef,
    },
    ParentUpdated {
        parent: String,
        label: String,
        sender: String,
        reference: ObservationRef,
    },
}

impl RegistryRawLogRow {
    pub(super) fn reference(&self) -> ObservationRef {
        ObservationRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            emitting_address: self.emitting_address.clone(),
            emitting_contract_instance_id: self.emitting_contract_instance_id,
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}
