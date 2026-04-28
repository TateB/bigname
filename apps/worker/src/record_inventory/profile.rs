use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

use super::{constants::*, types::RelevantEvent};

#[derive(Clone, Debug)]
pub(super) struct ResolverProfileGate {
    admissions: BTreeMap<(String, String, String, String), String>,
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
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
            })
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

        let Some(fact_family) = resolver_fact_family_for_event(source_family, &event.event_kind)
        else {
            return true;
        };
        if event_evidenced_resolver_fact(event) {
            return true;
        }
        let Some(emitting_address) = event.emitting_address.as_deref() else {
            return false;
        };

        self.status_for(
            &event.chain_id,
            source_family,
            emitting_address,
            fact_family,
        ) == Some(RESOLVER_PROFILE_STATUS_SUPPORTED)
    }

    pub(super) fn current_record_status(&self, event: &RelevantEvent) -> Option<&str> {
        if event.event_kind != EVENT_KIND_RESOLVER_CHANGED {
            return None;
        }

        let source_family = resolver_source_family_for_resolver_event(&event.source_family)?;
        let resolver_address = resolver_address_from_event(event)?;
        if resolver_address == "0x0000000000000000000000000000000000000000" {
            return None;
        }
        Some(
            self.status_for(
                &event.chain_id,
                source_family,
                &resolver_address,
                RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            )
            .unwrap_or(RESOLVER_PROFILE_STATUS_PENDING),
        )
    }
}

fn event_evidenced_resolver_fact(event: &RelevantEvent) -> bool {
    match event.event_kind.as_str() {
        EVENT_KIND_RECORD_CHANGED => event.after_state.get("value").is_some(),
        EVENT_KIND_RECORD_VERSION_CHANGED => true,
        _ => false,
    }
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

fn resolver_profile_for_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE),
        _ => None,
    }
}

fn resolver_source_family_for_resolver_event(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
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

fn resolver_fact_family_for_event(source_family: &str, event_kind: &str) -> Option<&'static str> {
    match (source_family, event_kind) {
        (_, EVENT_KIND_RECORD_CHANGED) => Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD),
        (SOURCE_FAMILY_ENS_V1_RESOLVER_L1, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION)
        }
        (SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, EVENT_KIND_RECORD_VERSION_CHANGED) => {
            Some(RESOLVER_PROFILE_FACT_FAMILY_RECORD)
        }
        _ => None,
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
    value.to_ascii_lowercase()
}
