use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE, ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE,
    RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION, RESOLVER_PROFILE_FACT_FAMILY_RECORD,
    RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION, RESOLVER_PROFILE_STATUS_PENDING,
    RESOLVER_PROFILE_STATUS_SUPPORTED, SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
    SOURCE_FAMILY_BASENAMES_BASE_RESOLVER, SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    target_loading::{CurrentBindingSeed, ResolverTarget, normalize_resolver_address},
};

#[derive(Clone, Debug)]
pub(super) struct ResolverProfileGate {
    admissions: BTreeMap<(String, String, String, String), String>,
    binding_enumeration_skipped_targets: BTreeSet<(String, String)>,
}

impl ResolverProfileGate {
    pub(super) async fn load(pool: &PgPool) -> Result<Self> {
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions(pool)
                .await
                .context("failed to load ENSv1 PublicResolver profile admissions")?
                .into_iter()
                .collect::<Vec<_>>();
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions(pool)
                .await
                .context("failed to load Basenames L2Resolver profile admissions")?,
        );

        Ok(Self::from_admissions(admissions))
    }

    pub(super) async fn load_for_target(pool: &PgPool, target: &ResolverTarget) -> Result<Self> {
        let targets = [(target.chain_id.clone(), target.resolver_address.clone())];
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions_for_targets(
                pool, &targets,
            )
            .await
            .context("failed to load scoped ENSv1 PublicResolver profile admissions")?;
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions_for_targets(
                pool, &targets,
            )
            .await
            .context("failed to load scoped Basenames L2Resolver profile admissions")?,
        );

        Ok(Self::from_admissions(admissions))
    }

    fn from_admissions(admissions: Vec<bigname_manifests::ResolverProfileAdmission>) -> Self {
        let mut binding_enumeration_skipped_targets = BTreeSet::new();
        let admissions = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_admitted(&admission.source_family, &admission.profile)
            })
            .map(|admission| {
                let normalized_address = normalize_resolver_address(&admission.address);
                if admission.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
                    binding_enumeration_skipped_targets
                        .insert((admission.chain.clone(), normalized_address.clone()));
                }
                (
                    (
                        admission.chain,
                        admission.source_family,
                        normalized_address,
                        admission.fact_family,
                    ),
                    admission.status,
                )
            })
            .collect();

        Self {
            admissions,
            binding_enumeration_skipped_targets,
        }
    }

    pub(super) fn target_status_for_bindings(
        &self,
        target: &ResolverTarget,
        bindings: &[CurrentBindingSeed],
    ) -> &str {
        resolver_profile_source_family_for_bindings(bindings)
            .map(|source_family| self.target_status(target, source_family))
            .unwrap_or(RESOLVER_PROFILE_STATUS_SUPPORTED)
    }

    pub(super) fn target_status_for_source_family(
        &self,
        target: &ResolverTarget,
        source_family: &str,
    ) -> &str {
        self.target_status(target, source_family)
    }

    pub(super) fn skips_binding_enumeration(&self, target: &ResolverTarget) -> bool {
        self.binding_enumeration_skipped_targets
            .contains(&(target.chain_id.clone(), target.resolver_address.clone()))
    }

    fn target_status(&self, target: &ResolverTarget, source_family: &str) -> &str {
        for &fact_family in resolver_overview_fact_families(source_family) {
            let Some(status) = self.admissions.get(&(
                target.chain_id.clone(),
                source_family.to_owned(),
                target.resolver_address.clone(),
                fact_family.to_owned(),
            )) else {
                return RESOLVER_PROFILE_STATUS_PENDING;
            };
            if status != RESOLVER_PROFILE_STATUS_SUPPORTED {
                return status.as_str();
            }
        }

        RESOLVER_PROFILE_STATUS_SUPPORTED
    }
}

fn resolver_profile_admitted(source_family: &str, profile: &str) -> bool {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => matches!(
            profile,
            ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE
                | "public_resolver_wrapper_aware"
                | "public_resolver_legacy_multicoin_dns"
                | "public_resolver_legacy_multicoin"
                | "public_resolver_legacy_eth_addr_text"
                | "public_resolver_legacy_eth_addr"
        ),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => {
            profile == BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE
        }
        _ => false,
    }
}

fn resolver_source_family_for_binding(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 => Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => Some(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER),
        _ => None,
    }
}

fn resolver_profile_source_family_for_bindings(
    bindings: &[CurrentBindingSeed],
) -> Option<&'static str> {
    let mut source_families = bindings
        .iter()
        .filter_map(|binding| resolver_source_family_for_binding(&binding.source_family))
        .collect::<BTreeSet<_>>();
    if source_families.len() == 1 {
        source_families.pop_first()
    } else {
        None
    }
}

fn resolver_overview_fact_families(source_family: &str) -> &'static [&'static str] {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => &[
            RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION,
            RESOLVER_PROFILE_FACT_FAMILY_RECORD,
            RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION,
        ],
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => &[
            RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION,
            RESOLVER_PROFILE_FACT_FAMILY_RECORD,
        ],
        _ => &[],
    }
}
