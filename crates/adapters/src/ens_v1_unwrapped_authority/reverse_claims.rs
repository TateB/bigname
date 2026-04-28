use super::*;

pub(super) async fn load_reverse_claim_sources(
    pool: &PgPool,
    chain: &str,
) -> Result<HashMap<String, ReverseClaimSource>> {
    load_reverse_claim_sources_internal(pool, chain, None).await
}

pub(super) async fn load_reverse_claim_sources_for_nodes(
    pool: &PgPool,
    chain: &str,
    reverse_nodes: &[String],
) -> Result<HashMap<String, ReverseClaimSource>> {
    if reverse_nodes.is_empty() {
        return Ok(HashMap::new());
    }

    load_reverse_claim_sources_internal(pool, chain, Some(reverse_nodes)).await
}

async fn load_reverse_claim_sources_internal(
    pool: &PgPool,
    chain: &str,
    reverse_nodes: Option<&[String]>,
) -> Result<HashMap<String, ReverseClaimSource>> {
    let rows = if let Some(reverse_nodes) = reverse_nodes {
        sqlx::query(
            r#"
        SELECT DISTINCT ON (LOWER(ne.after_state->>'reverse_node'))
            LOWER(ne.after_state->>'reverse_node') AS reverse_node,
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            ne.after_state->>'reverse_name' AS reverse_name,
            COALESCE(
                ne.after_state->'claim_provenance'->>'source_family',
                ne.source_family
            ) AS claim_source_family,
            COALESCE(
                ne.after_state->'claim_provenance'->>'contract_role',
                $4
            ) AS claim_contract_role,
            ne.after_state->'claim_provenance'->>'contract_instance_id' AS claim_contract_instance_id,
            COALESCE(
                ne.after_state->'claim_provenance'->>'emitting_address',
                ne.raw_fact_ref->>'emitting_address'
            ) AS claim_emitting_address
        FROM normalized_events ne
        WHERE ne.chain_id = $1
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) IN ($2, $3)
          AND ne.event_kind = $5
          AND ne.derivation_kind = $6
          AND LOWER(ne.after_state->>'reverse_node') = ANY($7::TEXT[])
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND ne.after_state->>'reverse_node' IS NOT NULL
          AND ne.after_state->>'reverse_node' <> ''
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
          AND ne.after_state->>'reverse_name' IS NOT NULL
          AND ne.after_state->>'reverse_name' <> ''
        ORDER BY
            LOWER(ne.after_state->>'reverse_node'),
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
        )
        .bind(chain)
        .bind("ens")
        .bind("basenames")
        .bind(CONTRACT_ROLE_REVERSE_REGISTRAR)
        .bind(EVENT_KIND_REVERSE_CHANGED)
        .bind(DERIVATION_KIND_ENS_V1_REVERSE_CLAIM)
        .bind(reverse_nodes)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load scoped reverse claim sources for chain {chain}")
        })?
    } else {
        sqlx::query(
        r#"
        SELECT DISTINCT ON (LOWER(ne.after_state->>'reverse_node'))
            LOWER(ne.after_state->>'reverse_node') AS reverse_node,
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            ne.after_state->>'reverse_name' AS reverse_name,
            COALESCE(
                ne.after_state->'claim_provenance'->>'source_family',
                ne.source_family
            ) AS claim_source_family,
            COALESCE(
                ne.after_state->'claim_provenance'->>'contract_role',
                $4
            ) AS claim_contract_role,
            ne.after_state->'claim_provenance'->>'contract_instance_id' AS claim_contract_instance_id,
            COALESCE(
                ne.after_state->'claim_provenance'->>'emitting_address',
                ne.raw_fact_ref->>'emitting_address'
            ) AS claim_emitting_address
        FROM normalized_events ne
        WHERE ne.chain_id = $1
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) IN ($2, $3)
          AND ne.event_kind = $5
          AND ne.derivation_kind = $6
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND ne.after_state->>'reverse_node' IS NOT NULL
          AND ne.after_state->>'reverse_node' <> ''
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
          AND ne.after_state->>'reverse_name' IS NOT NULL
          AND ne.after_state->>'reverse_name' <> ''
        ORDER BY
            LOWER(ne.after_state->>'reverse_node'),
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
        )
        .bind(chain)
        .bind("ens")
        .bind("basenames")
        .bind(CONTRACT_ROLE_REVERSE_REGISTRAR)
        .bind(EVENT_KIND_REVERSE_CHANGED)
        .bind(DERIVATION_KIND_ENS_V1_REVERSE_CLAIM)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load reverse claim sources for chain {chain}"))?
    };

    rows.into_iter()
        .map(|row| {
            let reverse_node = row
                .try_get::<String, _>("reverse_node")
                .context("missing reverse_node")?;
            let address = row
                .try_get::<String, _>("address")
                .context("missing reverse claim address")?;
            let namespace = row
                .try_get::<String, _>("namespace")
                .context("missing reverse claim namespace")?;
            let coin_type = row
                .try_get::<String, _>("coin_type")
                .context("missing reverse claim coin_type")?;
            let reverse_name = row
                .try_get::<String, _>("reverse_name")
                .context("missing reverse claim reverse_name")?;

            Ok((
                reverse_node.clone(),
                ReverseClaimSource {
                    address,
                    namespace,
                    coin_type,
                    reverse_name,
                    reverse_node,
                    claim_provenance: ReverseClaimProvenance {
                        source_family: row
                            .try_get::<String, _>("claim_source_family")
                            .context("missing reverse claim source_family")?,
                        contract_role: row
                            .try_get::<String, _>("claim_contract_role")
                            .context("missing reverse claim contract_role")?,
                        contract_instance_id: row
                            .try_get("claim_contract_instance_id")
                            .context("missing reverse claim contract_instance_id column")?,
                        emitting_address: row
                            .try_get("claim_emitting_address")
                            .context("missing reverse claim emitting_address column")?,
                    },
                },
            ))
        })
        .collect()
}

pub(super) fn apply_reverse_claim_source_observation(
    history: &mut ReverseClaimSourceHistory,
    observation: AuthorityObservation,
) -> Result<()> {
    match observation {
        AuthorityObservation::ResolverChanged(event) => {
            let before_resolver = history.current_resolver.clone();
            let before_normalized_resolver = nonzero_address(before_resolver.as_deref());
            let after_normalized_resolver = nonzero_address(Some(event.resolver.as_str()));
            if before_normalized_resolver != after_normalized_resolver {
                history.current_record_version = None;
            }
            history.current_resolver = Some(event.resolver.clone());
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RESOLVER_CHANGED,
                json!({
                    "resolver": before_resolver,
                }),
                resolver_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "resolver:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordChanged(event) => {
            if !current_reverse_source_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            if event.selector.record_key != "name" {
                return Ok(());
            }
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                record_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "record-change:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordVersionChanged(event) => {
            if !current_reverse_source_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let before_version = history.current_record_version;
            history.current_record_version = Some(event.record_version);
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RECORD_VERSION_CHANGED,
                json!({
                    "record_version": before_version,
                }),
                record_version_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "record-version:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RegistrationGranted(_)
        | AuthorityObservation::RegistrationRenewed(_)
        | AuthorityObservation::TokenTransferred(_)
        | AuthorityObservation::RegistryOwnerChanged(_)
        | AuthorityObservation::WrapperNameWrapped(_)
        | AuthorityObservation::WrapperNameUnwrapped(_)
        | AuthorityObservation::WrapperFusesSet(_)
        | AuthorityObservation::WrapperExpiryExtended(_)
        | AuthorityObservation::WrapperTokenTransferred(_) => {}
    }

    Ok(())
}

fn current_reverse_source_resolver_matches(
    history: &ReverseClaimSourceHistory,
    observed_resolver: &str,
) -> bool {
    match (
        nonzero_address(history.current_resolver.as_deref()),
        nonzero_address(Some(observed_resolver)),
    ) {
        (Some(current), Some(observed)) => current == observed,
        _ => false,
    }
}
