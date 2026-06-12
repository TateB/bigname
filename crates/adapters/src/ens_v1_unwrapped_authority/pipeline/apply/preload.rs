use super::*;

pub(in crate::ens_v1_unwrapped_authority::pipeline) fn name_intro_positions_for_raw_logs(
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<HashMap<String, Vec<RawLogPosition>>> {
    let mut positions = HashMap::<String, Vec<RawLogPosition>>::new();
    for raw_log in raw_logs {
        for observation in build_authority_observations(raw_log, event_topics)? {
            let Some(namehash) = observation_intro_namehash(&observation)? else {
                continue;
            };
            let Some(position) = observation_raw_log_position(&observation) else {
                continue;
            };
            positions
                .entry(namehash.to_ascii_lowercase())
                .or_default()
                .push(position);
        }
    }
    Ok(positions)
}

fn observation_intro_namehash(observation: &AuthorityObservation) -> Result<Option<String>> {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => Ok(Some(
            registrar_observation_namehash(&value.label, &value.reference)?,
        )),
        AuthorityObservation::RegistrationRenewed(value) => Ok(Some(
            registrar_observation_namehash(&value.label, &value.reference)?,
        )),
        AuthorityObservation::WrapperNameWrapped(value) => Ok(Some(value.name.namehash.clone())),
        _ => Ok(None),
    }
}

fn registrar_observation_namehash(label: &str, reference: &ObservationRef) -> Result<String> {
    Ok(observe_registrar_name_with_reference(label, reference, ENS_NORMALIZER_VERSION)?.namehash)
}

pub(in crate::ens_v1_unwrapped_authority::pipeline) async fn preload_name_metadata_for_raw_logs(
    pool: &PgPool,
    raw_logs: &[AuthorityRawLogRow],
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    event_topics: &AuthorityEventTopics,
) -> Result<()> {
    let mut namehashes = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        for observation in build_authority_observations(raw_log, event_topics)? {
            if let Some(namehash) = observation_namehash(&observation) {
                namehashes.insert(namehash.to_ascii_lowercase());
            }
        }
    }
    if namehashes.is_empty() {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            namespace,
            logical_name_id,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version
        FROM name_surfaces
        WHERE lower(namehash) = ANY($1)
          AND labelhashes[1] IS NOT NULL
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(namehashes.into_iter().collect::<Vec<_>>())
    .fetch_all(pool)
    .await
    .context("failed to preload name metadata for ENSv1 namehash observations")?;

    for row in rows {
        let name = NameMetadata {
            namespace: row.try_get("namespace")?,
            logical_name_id: row.try_get("logical_name_id")?,
            input_name: row.try_get("input_name")?,
            canonical_display_name: row.try_get("canonical_display_name")?,
            normalized_name: row.try_get("normalized_name")?,
            dns_encoded_name: row.try_get("dns_encoded_name")?,
            namehash: row.try_get::<String, _>("namehash")?.to_ascii_lowercase(),
            labelhashes: row.try_get("labelhashes")?,
            normalizer_version: row.try_get("normalizer_version")?,
        };
        known_names_by_namehash.insert(name.namehash.clone(), name);
    }
    Ok(())
}
