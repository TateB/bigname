use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::normalize_evm_address;
use serde_json::Value;
use sqlx::PgPool;

use super::{constants::*, types::RelevantEvent};

#[derive(Clone, Debug)]
pub(super) struct ResolverProfileGate {
    admissions: BTreeMap<(String, String, String, String), String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolverRecordFamilyStatuses {
    pub(super) addr: String,
    pub(super) text: String,
    pub(super) data: String,
}

impl ResolverRecordFamilyStatuses {
    pub(super) fn all_supported(&self) -> bool {
        self.addr == RESOLVER_PROFILE_STATUS_SUPPORTED
            && self.text == RESOLVER_PROFILE_STATUS_SUPPORTED
    }

    pub(super) fn any_supported(&self) -> bool {
        self.addr == RESOLVER_PROFILE_STATUS_SUPPORTED
            || self.text == RESOLVER_PROFILE_STATUS_SUPPORTED
    }

    pub(super) fn non_supported_families(&self) -> Vec<(&'static str, &str)> {
        let mut families = Vec::new();
        if self.addr != RESOLVER_PROFILE_STATUS_SUPPORTED {
            families.push((SUPPORTED_ADDR_RECORD_FAMILY, self.addr.as_str()));
        }
        if self.text != RESOLVER_PROFILE_STATUS_SUPPORTED {
            families.push((SUPPORTED_TEXT_RECORD_FAMILY, self.text.as_str()));
        }
        families
    }

    pub(super) fn status_for_record_family(&self, record_family: &str) -> Option<&str> {
        match record_family {
            SUPPORTED_ADDR_RECORD_FAMILY => Some(self.addr.as_str()),
            SUPPORTED_TEXT_RECORD_FAMILY => Some(self.text.as_str()),
            DATA_RESOLVER_RECORD_FAMILY => Some(self.data.as_str()),
            _ => None,
        }
    }
}

impl ResolverProfileGate {
    pub(super) async fn load_for_events(pool: &PgPool, events: &[RelevantEvent]) -> Result<Self> {
        let mut ens_v1_targets = BTreeSet::<(String, String)>::new();
        let mut basenames_targets = BTreeSet::<(String, String)>::new();

        for event in events {
            match resolver_profile_target_for_event(event) {
                Some((SOURCE_FAMILY_ENS_V1_RESOLVER_L1, chain_id, address)) => {
                    ens_v1_targets.insert((chain_id, address));
                }
                Some((SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, chain_id, address)) => {
                    basenames_targets.insert((chain_id, address));
                }
                _ => {}
            }
        }

        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions_for_targets(
                pool,
                &ens_v1_targets.into_iter().collect::<Vec<_>>(),
            )
            .await
            .context("failed to load scoped ENSv1 PublicResolver profile admissions")?;
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions_for_targets(
                pool,
                &basenames_targets.into_iter().collect::<Vec<_>>(),
            )
            .await
            .context("failed to load scoped Basenames L2Resolver profile admissions")?,
        );

        Ok(Self::from_admissions(admissions))
    }

    fn from_admissions(admissions: Vec<bigname_manifests::ResolverProfileAdmission>) -> Self {
        let admissions = admissions
            .into_iter()
            .filter(|admission| resolver_profile_admitted(admission))
            .map(|admission| {
                (
                    (
                        admission.chain,
                        admission.source_family,
                        normalize_address(&admission.address),
                        admission.fact_family,
                    ),
                    admission.status,
                )
            })
            .collect();

        Self { admissions }
    }

    fn status_for(
        &self,
        chain_id: &str,
        source_family: &str,
        resolver_address: &str,
        fact_family: &str,
    ) -> Option<&str> {
        self.admissions
            .get(&(
                chain_id.to_owned(),
                source_family.to_owned(),
                normalize_address(resolver_address),
                fact_family.to_owned(),
            ))
            .map(String::as_str)
    }

    pub(super) fn allows_event(&self, event: &RelevantEvent) -> bool {
        let Some(source_family) = resolver_local_source_family(&event.source_family) else {
            return true;
        };
        if ignored_resolver_record_event(source_family, event) {
            return false;
        }

        let fact_families = resolver_fact_families_for_event(source_family, event);
        if fact_families.is_empty() {
            return event.event_kind != EVENT_KIND_RECORD_CHANGED;
        }
        let Some(emitting_address) = event.emitting_address.as_deref() else {
            return false;
        };

        fact_families.iter().any(|fact_family| {
            self.status_for(
                &event.chain_id,
                source_family,
                emitting_address,
                fact_family,
            ) == Some(RESOLVER_PROFILE_STATUS_SUPPORTED)
        })
    }

    pub(super) fn allows_event_for_current_resolver(
        &self,
        event: &RelevantEvent,
        current_resolver_event: Option<&RelevantEvent>,
    ) -> bool {
        if self.allows_event(event) {
            return true;
        }
        if event.event_kind != EVENT_KIND_RECORD_CHANGED {
            return false;
        }

        let Some(source_family) = resolver_local_source_family(&event.source_family) else {
            return false;
        };
        if ignored_resolver_record_event(source_family, event) {
            return false;
        }
        let Some(current_resolver_event) = current_resolver_event else {
            return false;
        };
        if resolver_source_family_for_resolver_event(&current_resolver_event.source_family)
            != Some(source_family)
            || current_resolver_event.chain_id != event.chain_id
        {
            return false;
        }
        let Some(resolver_address) = resolver_address_from_event(current_resolver_event) else {
            return false;
        };
        if event
            .emitting_address
            .as_deref()
            .is_some_and(|emitting_address| normalize_address(emitting_address) != resolver_address)
        {
            return false;
        }

        self.allows_observed_current_resolver_record_event(event, source_family, &resolver_address)
    }

    pub(super) fn current_record_family_statuses(
        &self,
        event: &RelevantEvent,
    ) -> Option<ResolverRecordFamilyStatuses> {
        if event.event_kind != EVENT_KIND_RESOLVER_CHANGED {
            return None;
        }

        let source_family = resolver_source_family_for_resolver_event(&event.source_family)?;
        let resolver_address = resolver_address_from_event(event)?;
        if resolver_address == "0x0000000000000000000000000000000000000000" {
            return None;
        }

        Some(ResolverRecordFamilyStatuses {
            addr: self.record_family_status(
                &event.chain_id,
                source_family,
                &resolver_address,
                SUPPORTED_ADDR_RECORD_FAMILY,
                Some(SUPPORTED_NATIVE_ADDR_SELECTOR_KEY),
            ),
            text: self.record_family_status(
                &event.chain_id,
                source_family,
                &resolver_address,
                SUPPORTED_TEXT_RECORD_FAMILY,
                None,
            ),
            data: self.record_family_status(
                &event.chain_id,
                source_family,
                &resolver_address,
                DATA_RESOLVER_RECORD_FAMILY,
                None,
            ),
        })
    }

    fn record_family_status(
        &self,
        chain_id: &str,
        source_family: &str,
        resolver_address: &str,
        record_family: &str,
        selector_key: Option<&str>,
    ) -> String {
        resolver_record_fact_families(source_family, record_family, selector_key)
            .iter()
            .find_map(|fact_family| {
                self.status_for(chain_id, source_family, resolver_address, fact_family)
            })
            .unwrap_or(RESOLVER_PROFILE_STATUS_PENDING)
            .to_owned()
    }

    fn allows_observed_current_resolver_record_event(
        &self,
        event: &RelevantEvent,
        source_family: &str,
        resolver_address: &str,
    ) -> bool {
        let fact_families = resolver_fact_families_for_event(source_family, event);
        if fact_families.is_empty() {
            return false;
        }

        let mut pending_or_unknown = false;
        let keep_unadmitted_observation =
            keep_unadmitted_observable_record_event(source_family, event);
        for fact_family in fact_families {
            match self.status_for(
                &event.chain_id,
                source_family,
                resolver_address,
                fact_family,
            ) {
                None => pending_or_unknown = true,
                Some(RESOLVER_PROFILE_STATUS_PENDING) => pending_or_unknown = true,
                Some(RESOLVER_PROFILE_STATUS_SUPPORTED) => return true,
                Some(RESOLVER_PROFILE_STATUS_UNSUPPORTED) => {
                    if keep_unadmitted_observation {
                        pending_or_unknown = true;
                    } else {
                        return false;
                    }
                }
                Some(_) => pending_or_unknown = true,
            }
        }

        pending_or_unknown
    }
}

fn ignored_resolver_record_event(source_family: &str, event: &RelevantEvent) -> bool {
    source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        && resolver_record_family(event) == Some(PUBKEY_RECORD_FAMILY)
}

fn keep_unadmitted_observable_record_event(source_family: &str, event: &RelevantEvent) -> bool {
    source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        && resolver_record_family(event) == Some(DATA_RESOLVER_RECORD_FAMILY)
}

fn resolver_record_family(event: &RelevantEvent) -> Option<&str> {
    if event.event_kind != EVENT_KIND_RECORD_CHANGED {
        return None;
    }
    event
        .after_state
        .get("record_family")
        .and_then(Value::as_str)
}

fn resolver_profile_target_for_event(
    event: &RelevantEvent,
) -> Option<(&'static str, String, String)> {
    if let Some(source_family) = resolver_local_source_family(&event.source_family) {
        let emitting_address = event.emitting_address.as_deref()?;
        return Some((
            source_family,
            event.chain_id.clone(),
            normalize_address(emitting_address),
        ));
    }

    let source_family = resolver_source_family_for_resolver_event(&event.source_family)?;
    let resolver_address = resolver_address_from_event(event)?;
    if resolver_address == "0x0000000000000000000000000000000000000000" {
        return None;
    }

    Some((source_family, event.chain_id.clone(), resolver_address))
}

fn resolver_profile_admitted(admission: &bigname_manifests::ResolverProfileAdmission) -> bool {
    match admission.source_family.as_str() {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => ens_v1_resolver_profile_admitted(&admission.profile),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => {
            admission.profile == BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE
        }
        _ => false,
    }
}

fn ens_v1_resolver_profile_admitted(profile: &str) -> bool {
    matches!(
        profile,
        ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE
            | "public_resolver_wrapper_aware"
            | "public_resolver_legacy_multicoin_dns"
            | "public_resolver_legacy_multicoin"
            | "public_resolver_legacy_eth_addr_text"
            | "public_resolver_legacy_eth_addr"
    )
}

fn resolver_source_family_for_resolver_event(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 | SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => {
            Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
        }
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

pub(super) fn resolver_local_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

fn resolver_fact_families_for_event(
    source_family: &str,
    event: &RelevantEvent,
) -> Vec<&'static str> {
    match (source_family, event.event_kind.as_str()) {
        (_, EVENT_KIND_RECORD_CHANGED) => event
            .after_state
            .get("record_family")
            .and_then(Value::as_str)
            .map(|record_family| {
                resolver_record_fact_families(
                    source_family,
                    record_family,
                    event
                        .after_state
                        .get("selector_key")
                        .and_then(Value::as_str),
                )
            })
            .unwrap_or_default(),
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            vec![RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION]
        }
        (SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            vec![RESOLVER_PROFILE_FACT_FAMILY_RECORD]
        }
        _ => Vec::new(),
    }
}

fn resolver_record_fact_families(
    source_family: &str,
    record_family: &str,
    selector_key: Option<&str>,
) -> Vec<&'static str> {
    match (source_family, record_family) {
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, SUPPORTED_ADDR_RECORD_FAMILY)
            if selector_key == Some(SUPPORTED_NATIVE_ADDR_SELECTOR_KEY) =>
        {
            vec!["resolver_record:addr", RESOLVER_PROFILE_FACT_FAMILY_RECORD]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, SUPPORTED_ADDR_RECORD_FAMILY) => {
            vec![
                "resolver_record:multicoin_addr",
                RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            ]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, SUPPORTED_TEXT_RECORD_FAMILY) => {
            vec!["resolver_record:text", RESOLVER_PROFILE_FACT_FAMILY_RECORD]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, "name") => {
            vec!["resolver_record:name", RESOLVER_PROFILE_FACT_FAMILY_RECORD]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, "abi") => {
            vec!["resolver_record:abi", RESOLVER_PROFILE_FACT_FAMILY_RECORD]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, "content" | UNSUPPORTED_CONTENTHASH_RECORD_FAMILY) => {
            vec![
                "resolver_record:contenthash",
                RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            ]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, "dns" | "dns_record") => {
            vec!["resolver_record:dns", RESOLVER_PROFILE_FACT_FAMILY_RECORD]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, "interface") => {
            vec![
                "resolver_record:interface",
                RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            ]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, DATA_RESOLVER_RECORD_FAMILY) => {
            vec!["resolver_record:data"]
        }
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, _) => Vec::new(),
        (SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, _) => vec![RESOLVER_PROFILE_FACT_FAMILY_RECORD],
        _ => Vec::new(),
    }
}

fn resolver_address_from_event(event: &RelevantEvent) -> Option<String> {
    event
        .after_state
        .get("resolver")
        .and_then(Value::as_str)
        .map(normalize_address)
}

fn normalize_address(value: &str) -> String {
    normalize_evm_address(value)
}
