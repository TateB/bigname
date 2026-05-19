use std::collections::BTreeSet;

use super::*;
use crate::adapter_manifest::{
    ActiveManifestEventTopic0sBySignature, load_required_active_manifest_event_topic0s_by_signature,
};

const ENS_REGISTRAR_EVENT_SIGNATURES: &[&str] = &[
    NAME_REGISTERED_SIGNATURE,
    WRAPPED_NAME_REGISTERED_SIGNATURE,
    UNWRAPPED_NAME_REGISTERED_SIGNATURE,
    NAME_RENEWED_SIGNATURE,
    UNWRAPPED_NAME_RENEWED_SIGNATURE,
    TRANSFER_SIGNATURE,
];
const BASENAMES_REGISTRAR_EVENT_SIGNATURES: &[&str] = &[
    BASENAMES_NAME_REGISTERED_SIGNATURE,
    BASENAMES_NAME_RENEWED_SIGNATURE,
    TRANSFER_SIGNATURE,
];
const REGISTRY_EVENT_SIGNATURES: &[&str] = &[
    NEW_OWNER_SIGNATURE,
    NEW_RESOLVER_SIGNATURE,
    REGISTRY_TRANSFER_SIGNATURE,
    NEW_TTL_SIGNATURE,
];
const ENS_RESOLVER_EVENT_SIGNATURES: &[&str] = &[
    ABI_CHANGED_SIGNATURE,
    ADDR_CHANGED_SIGNATURE,
    ADDRESS_CHANGED_SIGNATURE,
    CONTENT_CHANGED_SIGNATURE,
    CONTENTHASH_CHANGED_SIGNATURE,
    DNS_RECORD_CHANGED_SIGNATURE,
    DNS_RECORD_DELETED_SIGNATURE,
    DNS_ZONEHASH_CHANGED_SIGNATURE,
    DATA_CHANGED_SIGNATURE,
    INTERFACE_CHANGED_SIGNATURE,
    NAME_CHANGED_SIGNATURE,
    TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE,
    TEXT_CHANGED_WITH_VALUE_SIGNATURE,
    VERSION_CHANGED_SIGNATURE,
];
const BASENAMES_RESOLVER_EVENT_SIGNATURES: &[&str] = &[
    ADDR_CHANGED_SIGNATURE,
    ADDRESS_CHANGED_SIGNATURE,
    NAME_CHANGED_SIGNATURE,
    TEXT_CHANGED_WITH_VALUE_SIGNATURE,
    VERSION_CHANGED_SIGNATURE,
];
const WRAPPER_EVENT_SIGNATURES: &[&str] = &[
    NAME_WRAPPED_SIGNATURE,
    NAME_UNWRAPPED_SIGNATURE,
    FUSES_SET_SIGNATURE,
    EXPIRY_EXTENDED_SIGNATURE,
    TRANSFER_SINGLE_SIGNATURE,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AuthorityEventTopics {
    topic0s: ActiveManifestEventTopic0sBySignature,
}

impl AuthorityEventTopics {
    pub(super) async fn load_for_authority_sources(
        pool: &PgPool,
        chain: &str,
        active_emitters: &[ActiveEmitter],
        generic_resolver_event_sources: &[GenericResolverEventSource],
    ) -> Result<Self> {
        let manifest_ids = authority_event_topic_manifest_ids(
            pool,
            chain,
            active_emitters,
            generic_resolver_event_sources,
        )
        .await?;
        Self::load(pool, &manifest_ids).await
    }

    pub(super) async fn load(pool: &PgPool, manifest_ids: &[i64]) -> Result<Self> {
        let manifest_ids = manifest_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let active_manifests = crate::adapter_manifest::load_active_manifest_metadata(
            pool,
            &manifest_ids,
            "ENSv1 unwrapped authority event topics",
        )
        .await?;

        let mut required_signatures = BTreeSet::<&str>::new();
        for manifest_id in &manifest_ids {
            let manifest = active_manifests.get(manifest_id).with_context(|| {
                format!("missing active manifest metadata for authority manifest_id {manifest_id}")
            })?;
            required_signatures.extend(required_signatures_for_source_family(
                &manifest.source_family,
            ));
        }
        let required_signatures = required_signatures.into_iter().collect::<Vec<_>>();

        Ok(Self {
            topic0s: load_required_active_manifest_event_topic0s_by_signature(
                pool,
                &manifest_ids,
                &required_signatures,
                "ENSv1 unwrapped authority",
            )
            .await?,
        })
    }

    pub(super) fn topic0(&self, canonical_signature: &str) -> Result<&str> {
        self.topic0s.topic0(canonical_signature)
    }

    pub(super) fn optional_topic0(&self, canonical_signature: &str) -> Option<&str> {
        self.topic0s.optional_topic0(canonical_signature)
    }

    pub(super) fn matches(&self, canonical_signature: &str, topic0: &str) -> Result<bool> {
        self.topic0s.matches(canonical_signature, topic0)
    }

    pub(super) fn is_text_changed_topic0(&self, source_family: &str, topic0: &str) -> Result<bool> {
        Ok(text_changed_signatures(source_family).iter().try_fold(
            false,
            |matched, signature| {
                Ok::<_, anyhow::Error>(matched || self.matches(signature, topic0)?)
            },
        )?)
    }

    pub(super) fn ens_resolver_event_topic0s(&self) -> Result<Vec<String>> {
        self.topic0s.topic0s(ENS_RESOLVER_EVENT_SIGNATURES)
    }

    pub(super) fn authority_replay_event_topic0s(&self) -> Vec<String> {
        [
            ENS_REGISTRAR_EVENT_SIGNATURES,
            BASENAMES_REGISTRAR_EVENT_SIGNATURES,
            REGISTRY_EVENT_SIGNATURES,
            ENS_RESOLVER_EVENT_SIGNATURES,
            BASENAMES_RESOLVER_EVENT_SIGNATURES,
            WRAPPER_EVENT_SIGNATURES,
        ]
        .into_iter()
        .flatten()
        .filter_map(|signature| self.optional_topic0(signature))
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
    }

    pub(super) fn for_ens_v1_text_decoding() -> Self {
        Self::from_signatures([
            TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE,
            TEXT_CHANGED_WITH_VALUE_SIGNATURE,
        ])
    }

    fn from_signatures(signatures: impl IntoIterator<Item = &'static str>) -> Self {
        let mut topic0s = HashMap::new();
        for signature in signatures {
            topic0s.insert(
                signature.to_owned(),
                crate::evm_abi::keccak256_hex(signature.as_bytes()),
            );
        }
        Self {
            topic0s: ActiveManifestEventTopic0sBySignature::new(topic0s),
        }
    }

    #[cfg(test)]
    pub(super) fn for_tests() -> Self {
        Self::from_signatures(all_test_signatures())
    }
}

async fn authority_event_topic_manifest_ids(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    generic_resolver_event_sources: &[GenericResolverEventSource],
) -> Result<Vec<i64>> {
    let mut manifest_ids = active_emitters
        .iter()
        .map(|emitter| emitter.source_manifest_id)
        .chain(
            generic_resolver_event_sources
                .iter()
                .map(|source| source.source_manifest_id),
        )
        .collect::<BTreeSet<_>>();

    let source_families = active_emitters
        .iter()
        .map(|emitter| emitter.source_family.as_str())
        .chain(
            generic_resolver_event_sources
                .iter()
                .map(|source| source.source_family.as_str()),
        )
        .collect::<BTreeSet<_>>();

    for source_family in source_families {
        if let Some(manifest) =
            crate::adapter_manifest::load_latest_active_manifest_metadata_for_source_family(
                pool,
                chain,
                source_family,
                "ENSv1 unwrapped authority event topic manifest",
            )
            .await?
        {
            manifest_ids.insert(manifest.manifest_id);
        }
    }

    Ok(manifest_ids.into_iter().collect())
}

pub(super) fn required_signatures_for_source_family(
    source_family: &str,
) -> &'static [&'static str] {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => ENS_REGISTRAR_EVENT_SIGNATURES,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR => BASENAMES_REGISTRAR_EVENT_SIGNATURES,
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 | SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => {
            REGISTRY_EVENT_SIGNATURES
        }
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => ENS_RESOLVER_EVENT_SIGNATURES,
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => BASENAMES_RESOLVER_EVENT_SIGNATURES,
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => WRAPPER_EVENT_SIGNATURES,
        _ => &[],
    }
}

fn text_changed_signatures(source_family: &str) -> &'static [&'static str] {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => &[
            TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE,
            TEXT_CHANGED_WITH_VALUE_SIGNATURE,
        ],
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => &[TEXT_CHANGED_WITH_VALUE_SIGNATURE],
        _ => &[],
    }
}

#[cfg(test)]
fn all_test_signatures() -> Vec<&'static str> {
    [
        ENS_REGISTRAR_EVENT_SIGNATURES,
        BASENAMES_REGISTRAR_EVENT_SIGNATURES,
        REGISTRY_EVENT_SIGNATURES,
        ENS_RESOLVER_EVENT_SIGNATURES,
        BASENAMES_RESOLVER_EVENT_SIGNATURES,
        WRAPPER_EVENT_SIGNATURES,
        &[PUBKEY_CHANGED_SIGNATURE],
    ]
    .into_iter()
    .flatten()
    .copied()
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect()
}
