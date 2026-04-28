use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct ResolverProfileGate {
    supported_fact_families: HashSet<(String, String, String, &'static str)>,
}

impl ResolverProfileGate {
    pub(super) async fn load(pool: &PgPool) -> Result<Self> {
        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions(pool)
                .await
                .context("failed to load ENSv1 PublicResolver profile admissions")?;
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions(pool)
                .await
                .context("failed to load Basenames L2Resolver profile admissions")?,
        );
        let supported_fact_families = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
                    && admission.status == "supported"
            })
            .filter_map(|admission| {
                resolver_fact_family_key(&admission.fact_family).map(|fact_family| {
                    (
                        admission.chain,
                        admission.source_family,
                        admission.address.to_ascii_lowercase(),
                        fact_family,
                    )
                })
            })
            .collect();

        Ok(Self {
            supported_fact_families,
        })
    }

    pub(super) async fn load_for_raw_logs(
        pool: &PgPool,
        raw_logs: &[AuthorityRawLogRow],
    ) -> Result<Self> {
        let mut ens_v1_targets = Vec::<(String, String)>::new();
        let mut basenames_targets = Vec::<(String, String)>::new();
        let mut seen_targets = HashSet::<(String, String, String)>::new();

        for raw_log in raw_logs {
            if resolver_profile_for_source_family(&raw_log.source_family).is_none() {
                continue;
            }
            let Some(topic0) = raw_log.topics.first() else {
                continue;
            };
            if resolver_fact_family_for_topic0(&raw_log.source_family, topic0).is_none() {
                continue;
            }

            let address = raw_log.emitting_address.to_ascii_lowercase();
            if !seen_targets.insert((
                raw_log.chain_id.clone(),
                raw_log.source_family.clone(),
                address.clone(),
            )) {
                continue;
            }

            match raw_log.source_family.as_str() {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => {
                    ens_v1_targets.push((raw_log.chain_id.clone(), address));
                }
                SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => {
                    basenames_targets.push((raw_log.chain_id.clone(), address));
                }
                _ => {}
            }
        }

        let mut admissions =
            bigname_manifests::load_ens_v1_public_resolver_profile_admissions_for_targets(
                pool,
                &ens_v1_targets,
            )
            .await
            .context("failed to load scoped ENSv1 PublicResolver profile admissions")?;
        admissions.extend(
            bigname_manifests::load_basenames_l2_resolver_profile_admissions_for_targets(
                pool,
                &basenames_targets,
            )
            .await
            .context("failed to load scoped Basenames L2Resolver profile admissions")?,
        );

        Ok(Self::from_admissions(admissions))
    }

    fn from_admissions(admissions: Vec<bigname_manifests::ResolverProfileAdmission>) -> Self {
        let supported_fact_families = admissions
            .into_iter()
            .filter(|admission| {
                resolver_profile_for_source_family(&admission.source_family)
                    .is_some_and(|profile| admission.profile == profile)
                    && admission.status == "supported"
            })
            .filter_map(|admission| {
                resolver_fact_family_key(&admission.fact_family).map(|fact_family| {
                    (
                        admission.chain,
                        admission.source_family,
                        admission.address.to_ascii_lowercase(),
                        fact_family,
                    )
                })
            })
            .collect();

        Self {
            supported_fact_families,
        }
    }

    pub(super) fn rejects_resolver_local_fact(&self, raw_log: &AuthorityRawLogRow) -> bool {
        if resolver_profile_for_source_family(&raw_log.source_family).is_none() {
            return false;
        }

        let Some(topic0) = raw_log.topics.first() else {
            return false;
        };
        let Some(fact_family) = resolver_fact_family_for_topic0(&raw_log.source_family, topic0)
        else {
            return false;
        };
        let Some(fact_family) = resolver_fact_family_key(fact_family) else {
            return true;
        };

        !self.supported_fact_families.contains(&(
            raw_log.chain_id.clone(),
            raw_log.source_family.clone(),
            raw_log.emitting_address.to_ascii_lowercase(),
            fact_family,
        ))
    }
}

fn resolver_fact_family_key(fact_family: &str) -> Option<&'static str> {
    match fact_family {
        "resolver_record" => Some("resolver_record"),
        "resolver_record_version" => Some("resolver_record_version"),
        _ => None,
    }
}

fn resolver_profile_for_source_family(source_family: &str) -> Option<&'static str> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some("public_resolver_compatible"),
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some("l2_resolver_compatible"),
        _ => None,
    }
}

pub(super) fn resolver_fact_family_for_topic0(
    source_family: &str,
    topic0: &str,
) -> Option<&'static str> {
    if topic0.eq_ignore_ascii_case(&text_changed_topic0())
        || topic0.eq_ignore_ascii_case(&name_changed_topic0())
        || topic0.eq_ignore_ascii_case(&addr_changed_topic0())
        || topic0.eq_ignore_ascii_case(&address_changed_topic0())
    {
        return Some("resolver_record");
    }

    if topic0.eq_ignore_ascii_case(&version_changed_topic0()) {
        return match source_family {
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some("resolver_record_version"),
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some("resolver_record"),
            _ => None,
        };
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_profile_gate_rejects_record_facts_without_supported_profile() {
        let supported_resolver = "0x00000000000000000000000000000000000000c1";
        let unsupported_resolver = "0x00000000000000000000000000000000000000c2";
        let gate = ResolverProfileGate {
            supported_fact_families: HashSet::from([(
                "ethereum-mainnet".to_owned(),
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                supported_resolver.to_owned(),
                "resolver_record",
            )]),
        };

        assert!(
            !gate.rejects_resolver_local_fact(&resolver_log(
                supported_resolver,
                name_changed_topic0(),
            ))
        );
        assert!(gate.rejects_resolver_local_fact(&resolver_log(
            unsupported_resolver,
            name_changed_topic0(),
        )));
        assert!(gate.rejects_resolver_local_fact(&resolver_log(
            supported_resolver,
            version_changed_topic0(),
        )));
    }

    fn resolver_log(emitting_address: &str, topic0: String) -> AuthorityRawLogRow {
        AuthorityRawLogRow {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: emitting_address.to_owned(),
            topics: vec![
                topic0,
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ],
            data: Vec::new(),
            canonicality_state: CanonicalityState::Canonical,
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
            manifest_version: 1,
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            contract_role: Some("public_resolver".to_owned()),
        }
    }
}
