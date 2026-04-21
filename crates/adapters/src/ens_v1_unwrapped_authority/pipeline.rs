pub async fn sync_ens_v1_unwrapped_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_scope(pool, chain, false, &[]).await
}

impl EnsV1UnwrappedAuthoritySyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(pool, chain, true, block_hashes).await
    }
}

async fn sync_ens_v1_unwrapped_authority_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let canonical_blocks = load_canonical_blocks(pool, chain).await?;
    if canonical_blocks.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let block_index = CanonicalBlockIndex {
        blocks: canonical_blocks,
    };
    let reverse_claim_sources = load_reverse_claim_sources(pool, chain).await?;
    let raw_logs = load_authority_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let mut histories = BTreeMap::<String, NameHistory>::new();
    let mut reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let mut namehash_to_labelhash = HashMap::<String, String>::new();
    let mut matched_log_count = 0usize;
    for raw_log in &raw_logs {
        let Some(observation) = build_authority_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;

        let labelhash = if let Some(namehash) = observation_namehash(&observation) {
            if let Some(labelhash) = namehash_to_labelhash.get(namehash).cloned() {
                labelhash
            } else if let Some(claim_source) = reverse_claim_sources.get(namehash).cloned() {
                let history = reverse_histories
                    .entry(namehash.to_owned())
                    .or_insert_with(|| ReverseClaimSourceHistory {
                        claim_source,
                        current_resolver: None,
                        current_record_version: None,
                        events: Vec::new(),
                    });
                apply_reverse_claim_source_observation(history, observation)?;
                continue;
            } else {
                continue;
            }
        } else {
            observation_labelhash(&observation)
        };
        let history = histories
            .entry(labelhash.clone())
            .or_insert_with(|| NameHistory {
                name: None,
                labelhash: labelhash.clone(),
                first_name_ref: None,
                current_registration: None,
                current_registry_owner: None,
                current_resolver: None,
                current_record_version: None,
                open_binding: None,
                bindings: Vec::new(),
                events: Vec::new(),
                registry_resource_anchor: None,
                latest_registry_owner_ref: None,
                latest_registry_owner_before_registration: None,
            });

        apply_observation(history, observation, &block_index).await?;
        if let Some(name) = history.name.as_ref() {
            namehash_to_labelhash.insert(name.namehash.clone(), labelhash);
        }
    }

    let head_block = block_index
        .blocks
        .last()
        .cloned()
        .context("canonical block index must contain a head block")?;
    let head_ref = BoundaryRef {
        chain_id: head_block.chain_id.clone(),
        block_hash: head_block.block_hash.clone(),
        block_number: head_block.block_number,
        block_timestamp: head_block.block_timestamp,
        canonicality_state: head_block.canonicality_state,
        namespace: active_emitters
            .first()
            .map(|emitter| emitter.namespace.clone())
            .unwrap_or_else(|| "ens".to_owned()),
    };

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = Vec::<NormalizedEvent>::new();

    for history in histories.into_values() {
        let Some(name) = history.name.clone() else {
            continue;
        };

        let finalized = finalize_history(history, &head_ref)?;
        if let Some(surface) =
            build_name_surface(pool, &name, finalized.first_name_ref.as_ref()).await?
        {
            surfaces.push(surface);
        }

        if let Some(registry_anchor) = finalized.registry_resource_anchor.as_ref() {
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!(
                        "resource:registry-only:{}:{}",
                        chain, finalized.labelhash
                    )),
                    None,
                    &registry_anchor.chain_id,
                    registry_anchor,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registry_only",
                        "authority_key": format!("registry-only:{}:{}", chain, finalized.labelhash),
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                        "current_registry_owner": finalized.current_registry_owner,
                    }),
                )
                .await?,
            );
        }

        for lease in &finalized.registrar_leases {
            let token_lineage_id =
                deterministic_uuid(&format!("token-lineage:{}", lease.authority_key));
            token_lineages.push(
                build_token_lineage(
                    pool,
                    token_lineage_id,
                    &lease.start_ref.chain_id,
                    &lease.start_ref,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                    }),
                )
                .await?,
            );
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!("resource:{}", lease.authority_key)),
                    Some(token_lineage_id),
                    &lease.start_ref.chain_id,
                    &lease.start_ref.as_boundary_ref(),
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                        "expiry": lease.expiry.unix_timestamp(),
                        "registrant": lease.registrant,
                        "released_at": lease.release_ref.as_ref().map(|value| value.block_timestamp.unix_timestamp()),
                    }),
                )
                .await?,
            );
        }

        for segment in finalized.bindings {
            bindings.push(
                build_surface_binding(pool, &name.logical_name_id, &segment, &head_ref.chain_id)
                    .await?,
            );
        }
        events.extend(finalized.events);
    }
    for history in reverse_histories.into_values() {
        events.extend(history.events);
    }

    let by_kind = count_events_by_kind(&events);
    upsert_token_lineages(pool, &token_lineages).await?;
    upsert_resources(pool, &resources).await?;
    upsert_name_surfaces(pool, &surfaces).await?;
    upsert_surface_bindings(pool, &bindings).await?;
    upsert_normalized_events(pool, &events).await?;

    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        by_kind,
    })
}

#[derive(Clone, Debug)]
struct FinalizedHistory {
    labelhash: String,
    first_name_ref: Option<ObservationRef>,
    bindings: Vec<BindingSegment>,
    events: Vec<NormalizedEvent>,
    registrar_leases: Vec<RegistrationLease>,
    registry_resource_anchor: Option<BoundaryRef>,
    current_registry_owner: Option<String>,
}

fn finalize_history(mut history: NameHistory, head_ref: &BoundaryRef) -> Result<FinalizedHistory> {
    if let Some(lease) = history.current_registration.take() {
        if let Some(release_ref) = lease.release_ref.clone() {
            if release_ref.block_timestamp <= head_ref.block_timestamp {
                emit_registration_released_event(&mut history, &lease, &release_ref)?;
                let registry_after = registry_anchor_for_history(
                    &history,
                    &lease.reference_chain(),
                    &lease.labelhash,
                );
                transition_authority(
                    &mut history,
                    Some(build_registrar_anchor(&lease)),
                    registry_after.clone(),
                    &release_ref,
                    release_ref.block_timestamp,
                )?;
                if let (Some(name), Some(anchor), Some(subject)) = (
                    history.name.as_ref(),
                    registry_after.as_ref(),
                    nonzero_address(history.current_registry_owner.as_deref()),
                ) {
                    emit_boundary_permission_grants(
                        &mut history.events,
                        &release_ref,
                        &name.logical_name_id,
                        anchor,
                        &subject,
                        history.current_resolver.as_deref(),
                        &release_ref.chain_id,
                        EVENT_KIND_REGISTRATION_RELEASED,
                    );
                }
            } else if history.open_binding.is_none() {
                let registrar_anchor = build_registrar_anchor(&lease);
                history.open_binding = Some(OpenBinding {
                    surface_binding_id: deterministic_uuid(&format!(
                        "binding:{}:{}",
                        registrar_anchor.authority_key,
                        lease.start_ref.block_timestamp.unix_timestamp()
                    )),
                    authority: registrar_anchor,
                    active_from: lease.start_ref.block_timestamp,
                    anchor_ref: lease.start_ref.as_boundary_ref(),
                });
            }
        } else if history.open_binding.is_none() {
            let registrar_anchor = build_registrar_anchor(&lease);
            history.open_binding = Some(OpenBinding {
                surface_binding_id: deterministic_uuid(&format!(
                    "binding:{}:{}",
                    registrar_anchor.authority_key,
                    lease.start_ref.block_timestamp.unix_timestamp()
                )),
                authority: registrar_anchor,
                active_from: lease.start_ref.block_timestamp,
                anchor_ref: lease.start_ref.as_boundary_ref(),
            });
        }

        history.current_registration = Some(lease);
    }

    if history.open_binding.is_none()
        && history.current_registration.is_none()
        && history
            .current_registry_owner
            .as_deref()
            .is_some_and(|owner| owner != ZERO_ADDRESS)
        && let Some(anchor) =
            registry_anchor_for_history(&history, &head_ref.chain_id, &history.labelhash)
    {
        history.open_binding = Some(OpenBinding {
            surface_binding_id: deterministic_uuid(&format!(
                "binding:{}:{}",
                anchor.authority_key,
                anchor
                    .binding_manifest_id
                    .checked_mul(0)
                    .unwrap_or_default()
                    + head_ref.block_timestamp.unix_timestamp()
            )),
            authority: anchor,
            active_from: head_ref.block_timestamp,
            anchor_ref: head_ref.clone(),
        });
    }

    if let Some(open_binding) = history.open_binding.take() {
        history.bindings.push(BindingSegment {
            surface_binding_id: open_binding.surface_binding_id,
            authority: open_binding.authority,
            active_from: open_binding.active_from,
            active_to: None,
            anchor_ref: open_binding.anchor_ref,
        });
    }

    let registrar_leases = history.current_registration.into_iter().collect::<Vec<_>>();

    Ok(FinalizedHistory {
        labelhash: history.labelhash,
        first_name_ref: history.first_name_ref,
        bindings: history.bindings,
        events: history.events,
        registrar_leases,
        registry_resource_anchor: history.registry_resource_anchor,
        current_registry_owner: history.current_registry_owner,
    })
}

fn build_registrar_anchor(lease: &RegistrationLease) -> AuthorityAnchor {
    AuthorityAnchor {
        kind: AuthorityKind::Registrar,
        authority_key: lease.authority_key.clone(),
        resource_id: deterministic_uuid(&format!("resource:{}", lease.authority_key)),
        token_lineage_id: Some(deterministic_uuid(&format!(
            "token-lineage:{}",
            lease.authority_key
        ))),
        binding_source_family: lease.start_ref.source_family.clone(),
        binding_manifest_version: lease.start_ref.manifest_version,
        binding_manifest_id: lease.start_ref.source_manifest_id,
    }
}

fn registry_anchor_for_history(
    history: &NameHistory,
    chain: &str,
    labelhash: &str,
) -> Option<AuthorityAnchor> {
    if history
        .current_registry_owner
        .as_deref()
        .is_none_or(|owner| owner == ZERO_ADDRESS)
    {
        return None;
    }

    let reference = history
        .latest_registry_owner_ref
        .as_ref()
        .or(history.latest_registry_owner_before_registration.as_ref())?;
    Some(AuthorityAnchor {
        kind: AuthorityKind::RegistryOnly,
        authority_key: format!("registry-only:{chain}:{labelhash}"),
        resource_id: deterministic_uuid(&format!("resource:registry-only:{chain}:{labelhash}")),
        token_lineage_id: None,
        binding_source_family: reference.source_family.clone(),
        binding_manifest_version: reference.manifest_version,
        binding_manifest_id: reference.source_manifest_id,
    })
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_default() += 1;
    }
    counts
}

async fn load_reverse_claim_sources(
    pool: &PgPool,
    chain: &str,
) -> Result<HashMap<String, ReverseClaimSource>> {
    let rows = sqlx::query(
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
                $3
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
    .with_context(|| format!("failed to load reverse claim sources for chain {chain}"))?;

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

async fn build_name_surface(
    pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&ObservationRef>,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    if let Some(existing) =
        load_name_surface_including_noncanonical(pool, &name.logical_name_id).await?
    {
        return Ok(Some(NameSurface {
            logical_name_id: existing.logical_name_id,
            namespace: existing.namespace,
            input_name: existing.input_name,
            canonical_display_name: existing.canonical_display_name,
            normalized_name: existing.normalized_name,
            dns_encoded_name: existing.dns_encoded_name,
            namehash: existing.namehash,
            labelhashes: existing.labelhashes,
            normalizer_version: existing.normalizer_version,
            normalization_warnings: existing.normalization_warnings,
            normalization_errors: existing.normalization_errors,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: json!({
                "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                "logical_name_id": name.logical_name_id,
            }),
            canonicality_state: reference.canonicality_state,
        }));
    }

    Ok(Some(NameSurface {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        input_name: name.input_name.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        dns_encoded_name: name.dns_encoded_name.clone(),
        namehash: name.namehash.clone(),
        labelhashes: name.labelhashes.clone(),
        normalizer_version: name.normalizer_version.clone(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: reference.chain_id.clone(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "logical_name_id": name.logical_name_id,
            "source_event": "registrar_name_observation",
        }),
        canonicality_state: reference.canonicality_state,
    }))
}

async fn build_token_lineage(
    pool: &PgPool,
    token_lineage_id: Uuid,
    chain: &str,
    reference: &ObservationRef,
    provenance: serde_json::Value,
) -> Result<TokenLineage> {
    if let Some(existing) =
        load_token_lineage_including_noncanonical(pool, token_lineage_id).await?
    {
        return Ok(TokenLineage {
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance,
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(TokenLineage {
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance,
        canonicality_state: reference.canonicality_state,
    })
}

async fn build_resource(
    pool: &PgPool,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    chain: &str,
    reference: &BoundaryRef,
    provenance: serde_json::Value,
) -> Result<Resource> {
    if let Some(existing) = load_resource_including_noncanonical(pool, resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id.or(token_lineage_id),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance,
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id,
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance,
        canonicality_state: reference.canonicality_state,
    })
}

async fn build_surface_binding(
    pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
    if let Some(existing) =
        load_surface_binding_including_noncanonical(pool, segment.surface_binding_id).await?
    {
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: segment.active_to.or(existing.active_to),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: existing.provenance,
            canonicality_state: segment.anchor_ref.canonicality_state,
        });
    }

    Ok(SurfaceBinding {
        surface_binding_id: segment.surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: segment.authority.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: segment.active_from,
        active_to: segment.active_to,
        chain_id: chain.to_owned(),
        block_hash: segment.anchor_ref.block_hash.clone(),
        block_number: segment.anchor_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": segment.authority.kind.as_str(),
            "authority_key": segment.authority.authority_key,
        }),
        canonicality_state: segment.anchor_ref.canonicality_state,
    })
}

async fn apply_observation(
    history: &mut NameHistory,
    observation: AuthorityObservation,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    match observation {
        AuthorityObservation::RegistrationGranted(event) => {
            let name = observe_registrar_name_with_reference(
                &event.label,
                &event.reference,
                ENS_NORMALIZER_VERSION,
            )?;
            history
                .first_name_ref
                .get_or_insert(event.reference.clone());
            history.name = Some(name.clone());
            history.latest_registry_owner_before_registration =
                history.latest_registry_owner_ref.clone();

            let before_anchor = active_anchor_for_history(history, &event.reference.chain_id);
            let authority_key = format!(
                "registrar:{}:{}:{}:{}:{}",
                event.reference.chain_id,
                event.reference.source_manifest_id,
                event.labelhash,
                event.reference.block_hash,
                event.reference.log_index.unwrap_or_default()
            );
            let lease = RegistrationLease {
                authority_key,
                labelhash: event.labelhash.clone(),
                registrant: event.registrant.clone(),
                expiry: event.expiry,
                release_ref: block_index.first_block_at_or_after(
                    release_after_grace(event.expiry)?,
                    &event.reference.namespace,
                ),
                start_ref: event.reference.clone(),
            };
            let after_anchor = Some(build_registrar_anchor(&lease));
            let before_expiry = history
                .current_registration
                .as_ref()
                .map(|value| value.expiry);
            history.current_registration = Some(lease.clone());

            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                after_anchor.as_ref().map(|value| value.resource_id),
                EVENT_KIND_REGISTRATION_GRANTED,
                json!({
                    "authority_kind": before_anchor.as_ref().map(|value| value.kind.as_str()),
                    "registrant": before_anchor.as_ref().and_then(|value| value.token_lineage_id).map(|_| serde_json::Value::Null),
                }),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": lease.authority_key,
                    "registrant": event.registrant,
                    "expiry": event.expiry.unix_timestamp(),
                    "labelhash": event.labelhash,
                }),
                format!(
                    "grant:{}:{}:{}",
                    event.reference.block_hash,
                    event.reference.transaction_hash.as_deref().unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                after_anchor.as_ref().map(|value| value.resource_id),
                EVENT_KIND_EXPIRY_CHANGED,
                json!({
                    "expiry": before_expiry.map(|value| value.unix_timestamp()),
                }),
                json!({
                    "expiry": event.expiry.unix_timestamp(),
                }),
                format!(
                    "expiry:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            if let (Some(anchor), Some(subject)) = (
                after_anchor.as_ref(),
                nonzero_address(Some(event.registrant.as_str())),
            ) {
                emit_observation_permission_grants(
                    &mut history.events,
                    &event.reference,
                    &name.logical_name_id,
                    anchor,
                    &subject,
                    history.current_resolver.as_deref(),
                    EVENT_KIND_REGISTRATION_GRANTED,
                );
            }

            transition_authority(
                history,
                before_anchor,
                after_anchor,
                &event.reference.as_boundary_ref(),
                event.reference.block_timestamp,
            )?;
        }
        AuthorityObservation::RegistrationRenewed(event) => {
            if history.name.is_none() {
                history.name = Some(observe_registrar_name_with_reference(
                    &event.label,
                    &event.reference,
                    ENS_NORMALIZER_VERSION,
                )?);
                history
                    .first_name_ref
                    .get_or_insert(event.reference.clone());
                let name = history
                    .name
                    .clone()
                    .context("failed to build registrar name metadata")?;
                let lease = RegistrationLease {
                    authority_key: format!(
                        "registrar:{}:{}:{}:{}:{}",
                        event.reference.chain_id,
                        event.reference.source_manifest_id,
                        event.labelhash,
                        event.reference.block_hash,
                        event.reference.log_index.unwrap_or_default()
                    ),
                    labelhash: event.labelhash.clone(),
                    registrant: history
                        .current_registration
                        .as_ref()
                        .map(|value| value.registrant.clone())
                        .unwrap_or_else(|| ZERO_ADDRESS.to_owned()),
                    expiry: event.expiry,
                    release_ref: block_index.first_block_at_or_after(
                        release_after_grace(event.expiry)?,
                        &event.reference.namespace,
                    ),
                    start_ref: event.reference.clone(),
                };
                history.current_registration = Some(lease.clone());
                let anchor = Some(build_registrar_anchor(&lease));
                transition_authority(
                    history,
                    None,
                    anchor.clone(),
                    &event.reference.as_boundary_ref(),
                    event.reference.block_timestamp,
                )?;
                history.events.push(build_normalized_event(
                    &event.reference,
                    Some(name.logical_name_id.clone()),
                    anchor.as_ref().map(|value| value.resource_id),
                    EVENT_KIND_REGISTRATION_GRANTED,
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "registrant": lease.registrant,
                        "expiry": event.expiry.unix_timestamp(),
                        "labelhash": event.labelhash,
                    }),
                    format!(
                        "grant:{}:{}:{}",
                        event.reference.block_hash,
                        event
                            .reference
                            .transaction_hash
                            .as_deref()
                            .unwrap_or_default(),
                        event.reference.log_index.unwrap_or_default()
                    ),
                ));
                if let (Some(anchor), Some(subject)) = (
                    anchor.as_ref(),
                    nonzero_address(Some(lease.registrant.as_str())),
                ) {
                    emit_observation_permission_grants(
                        &mut history.events,
                        &event.reference,
                        &name.logical_name_id,
                        anchor,
                        &subject,
                        history.current_resolver.as_deref(),
                        EVENT_KIND_REGISTRATION_GRANTED,
                    );
                }
            }
            let name = history
                .name
                .clone()
                .context("failed to build registrar name metadata")?;

            if let Some(current_registration) = history.current_registration.as_mut() {
                let before_expiry = current_registration.expiry;
                current_registration.expiry = event.expiry;
                current_registration.release_ref = block_index.first_block_at_or_after(
                    release_after_grace(event.expiry)?,
                    &event.reference.namespace,
                );

                history.events.push(build_normalized_event(
                    &event.reference,
                    Some(name.logical_name_id.clone()),
                    Some(deterministic_uuid(&format!(
                        "resource:{}",
                        current_registration.authority_key
                    ))),
                    EVENT_KIND_REGISTRATION_RENEWED,
                    json!({
                        "expiry": before_expiry.unix_timestamp(),
                    }),
                    json!({
                        "expiry": event.expiry.unix_timestamp(),
                        "labelhash": event.labelhash,
                    }),
                    format!(
                        "renewal:{}:{}:{}",
                        event.reference.block_hash,
                        event
                            .reference
                            .transaction_hash
                            .as_deref()
                            .unwrap_or_default(),
                        event.reference.log_index.unwrap_or_default()
                    ),
                ));
                history.events.push(build_normalized_event(
                    &event.reference,
                    Some(name.logical_name_id.clone()),
                    Some(deterministic_uuid(&format!(
                        "resource:{}",
                        current_registration.authority_key
                    ))),
                    EVENT_KIND_EXPIRY_CHANGED,
                    json!({
                        "expiry": before_expiry.unix_timestamp(),
                    }),
                    json!({
                        "expiry": event.expiry.unix_timestamp(),
                    }),
                    format!(
                        "expiry:{}:{}:{}",
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
        }
        AuthorityObservation::TokenTransferred(event) => {
            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            let current_resolver = history.current_resolver.clone();
            let Some(current_registration) = history.current_registration.as_mut() else {
                return Ok(());
            };
            if event.from_address == ZERO_ADDRESS || event.to_address == ZERO_ADDRESS {
                return Ok(());
            }
            let previous_registrant = current_registration.registrant.clone();
            current_registration.registrant = event.to_address.clone();
            let anchor = build_registrar_anchor(current_registration);
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                Some(anchor.resource_id),
                EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
                json!({
                    "from": previous_registrant,
                }),
                json!({
                    "to": event.to_address,
                    "labelhash": event.labelhash,
                }),
                format!(
                    "token-transfer:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            emit_observation_permission_subject_change(
                &mut history.events,
                &event.reference,
                &name.logical_name_id,
                &anchor,
                Some(previous_registrant.as_str()),
                Some(event.to_address.as_str()),
                current_resolver.as_deref(),
                EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
            );
        }
        AuthorityObservation::ResolverChanged(event) => {
            let before_resolver = history.current_resolver.clone();
            let before_normalized_resolver = nonzero_address(before_resolver.as_deref());
            let after_normalized_resolver = nonzero_address(Some(event.resolver.as_str()));
            if before_normalized_resolver != after_normalized_resolver {
                history.current_record_version = None;
            }
            history.current_resolver = Some(event.resolver.clone());

            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            let authority = active_anchor_for_history(history, &event.reference.chain_id);
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                authority.as_ref().map(|value| value.resource_id),
                EVENT_KIND_RESOLVER_CHANGED,
                json!({
                    "resolver": before_resolver,
                }),
                resolver_changed_after_state(&event, None),
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
            let authority_subject = match authority.as_ref().map(|value| value.kind) {
                Some(AuthorityKind::Registrar) => history
                    .current_registration
                    .as_ref()
                    .map(|registration| registration.registrant.as_str()),
                Some(AuthorityKind::RegistryOnly) => history.current_registry_owner.as_deref(),
                None => None,
            };
            if let (Some(anchor), Some(subject)) =
                (authority.as_ref(), nonzero_address(authority_subject))
            {
                let before_resolver = before_normalized_resolver;
                let after_resolver = after_normalized_resolver;
                if before_resolver != after_resolver {
                    if let Some(previous_resolver) = before_resolver.as_deref() {
                        history
                            .events
                            .push(build_observation_permission_change_event(
                                &event.reference,
                                &name.logical_name_id,
                                anchor,
                                &subject,
                                resolver_permission_scope(
                                    &event.reference.chain_id,
                                    previous_resolver,
                                ),
                                format!("resolver:{previous_resolver}"),
                                PERMISSION_POWER_RESOLVER_CONTROL,
                                PermissionAction::Revoke,
                                EVENT_KIND_RESOLVER_CHANGED,
                            ));
                    }
                    if let Some(current_resolver) = after_resolver.as_deref() {
                        history
                            .events
                            .push(build_observation_permission_change_event(
                                &event.reference,
                                &name.logical_name_id,
                                anchor,
                                &subject,
                                resolver_permission_scope(
                                    &event.reference.chain_id,
                                    current_resolver,
                                ),
                                format!("resolver:{current_resolver}"),
                                PERMISSION_POWER_RESOLVER_CONTROL,
                                PermissionAction::Grant,
                                EVENT_KIND_RESOLVER_CHANGED,
                            ));
                    }
                }
            }
        }
        AuthorityObservation::RecordChanged(event) => {
            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            if !current_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let Some(authority) = active_anchor_for_history(history, &event.reference.chain_id)
            else {
                return Ok(());
            };
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                Some(authority.resource_id),
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                record_changed_after_state(&event, None),
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
            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            if !current_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let Some(authority) = active_anchor_for_history(history, &event.reference.chain_id)
            else {
                return Ok(());
            };
            let before_version = history.current_record_version;
            history.current_record_version = Some(event.record_version);
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                Some(authority.resource_id),
                EVENT_KIND_RECORD_VERSION_CHANGED,
                json!({
                    "record_version": before_version,
                }),
                record_version_changed_after_state(&event, None),
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
        AuthorityObservation::RegistryOwnerChanged(event) => {
            let before_anchor = active_anchor_for_history(history, &event.reference.chain_id);
            let before_owner = history.current_registry_owner.clone();
            history.current_registry_owner = Some(event.owner.clone());
            history.latest_registry_owner_ref = Some(event.reference.clone());
            history
                .registry_resource_anchor
                .get_or_insert_with(|| event.reference.as_boundary_ref());

            let after_anchor = active_anchor_for_history(history, &event.reference.chain_id);
            if matches!(
                (&before_anchor, &after_anchor),
                (Some(left), Some(right))
                    if left.kind == AuthorityKind::RegistryOnly
                        && right.kind == AuthorityKind::RegistryOnly
                        && before_owner != history.current_registry_owner
            ) {
                if let Some(name) = history.name.as_ref() {
                    history.events.push(build_normalized_event(
                        &event.reference,
                        Some(name.logical_name_id.clone()),
                        after_anchor.as_ref().map(|value| value.resource_id),
                        EVENT_KIND_AUTHORITY_TRANSFERRED,
                        json!({
                            "owner": before_owner,
                        }),
                        json!({
                            "owner": history.current_registry_owner,
                            "labelhash": event.labelhash,
                        }),
                        format!(
                            "registry-transfer:{}:{}:{}",
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
            }
            if let Some(name) = history.name.clone() {
                match (before_anchor.as_ref(), after_anchor.as_ref()) {
                    (Some(before), Some(after))
                        if before.kind == AuthorityKind::RegistryOnly
                            && after.kind == AuthorityKind::RegistryOnly =>
                    {
                        emit_observation_permission_subject_change(
                            &mut history.events,
                            &event.reference,
                            &name.logical_name_id,
                            after,
                            before_owner.as_deref(),
                            history.current_registry_owner.as_deref(),
                            history.current_resolver.as_deref(),
                            EVENT_KIND_AUTHORITY_TRANSFERRED,
                        );
                    }
                    (_, Some(after)) if after.kind == AuthorityKind::RegistryOnly => {
                        if let Some(subject) =
                            nonzero_address(history.current_registry_owner.as_deref())
                        {
                            emit_observation_permission_grants(
                                &mut history.events,
                                &event.reference,
                                &name.logical_name_id,
                                after,
                                &subject,
                                history.current_resolver.as_deref(),
                                EVENT_KIND_AUTHORITY_TRANSFERRED,
                            );
                        }
                    }
                    _ => {}
                }
            }
            transition_authority(
                history,
                before_anchor,
                after_anchor,
                &event.reference.as_boundary_ref(),
                event.reference.block_timestamp,
            )?;
        }
    }

    Ok(())
}

fn apply_reverse_claim_source_observation(
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
        | AuthorityObservation::RegistryOwnerChanged(_) => {}
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

fn resolver_changed_after_state(
    event: &ResolverObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([
        ("resolver".to_owned(), Value::String(event.resolver.clone())),
        ("namehash".to_owned(), Value::String(event.namehash.clone())),
    ]);
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

fn record_changed_after_state(
    event: &RecordChangeObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([
        (
            "record_key".to_owned(),
            Value::String(event.selector.record_key.clone()),
        ),
        (
            "record_family".to_owned(),
            Value::String(event.selector.record_family.clone()),
        ),
        (
            "selector_key".to_owned(),
            event
                .selector
                .selector_key
                .as_ref()
                .map(|value| Value::String(value.clone()))
                .unwrap_or(Value::Null),
        ),
    ]);
    if let Some(raw_name) = event.raw_name.as_ref() {
        state.insert("raw_name".to_owned(), Value::String(raw_name.clone()));
    }
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

fn record_version_changed_after_state(
    event: &RecordVersionObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([(
        "record_version".to_owned(),
        Value::Number(event.record_version.into()),
    )]);
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

fn transition_authority(
    history: &mut NameHistory,
    before: Option<AuthorityAnchor>,
    after: Option<AuthorityAnchor>,
    reference: &BoundaryRef,
    effective_time: OffsetDateTime,
) -> Result<()> {
    if authority_eq(before.as_ref(), after.as_ref()) {
        return Ok(());
    }

    history.current_record_version = None;

    if let Some(open_binding) = history.open_binding.take()
        && open_binding.active_from < effective_time
    {
        history.bindings.push(BindingSegment {
            surface_binding_id: open_binding.surface_binding_id,
            authority: open_binding.authority.clone(),
            active_from: open_binding.active_from,
            active_to: Some(effective_time),
            anchor_ref: open_binding.anchor_ref.clone(),
        });
        if let Some(name) = history.name.as_ref() {
            history.events.push(build_boundary_event(
                reference,
                Some(name.logical_name_id.clone()),
                Some(open_binding.authority.resource_id),
                EVENT_KIND_SURFACE_UNBOUND,
                json!({
                    "authority_kind": open_binding.authority.kind.as_str(),
                    "authority_key": open_binding.authority.authority_key,
                }),
                json!({
                    "authority_kind": open_binding.authority.kind.as_str(),
                    "authority_key": open_binding.authority.authority_key,
                    "active_to": effective_time.unix_timestamp(),
                }),
                format!(
                    "surface-unbound:{}:{}:{}",
                    reference.block_hash, name.logical_name_id, open_binding.surface_binding_id
                ),
                open_binding.authority.binding_source_family.clone(),
                open_binding.authority.binding_manifest_version,
                Some(open_binding.authority.binding_manifest_id),
                reference.canonicality_state,
            ));
        }
    }

    if let Some(after_anchor) = after.clone() {
        let surface_binding_id = deterministic_uuid(&format!(
            "binding:{}:{}",
            after_anchor.authority_key,
            effective_time.unix_timestamp()
        ));
        history.open_binding = Some(OpenBinding {
            surface_binding_id,
            authority: after_anchor.clone(),
            active_from: effective_time,
            anchor_ref: reference.clone(),
        });
        if let Some(name) = history.name.as_ref() {
            history.events.push(build_boundary_event(
                reference,
                Some(name.logical_name_id.clone()),
                Some(after_anchor.resource_id),
                EVENT_KIND_SURFACE_BOUND,
                json!({}),
                json!({
                    "authority_kind": after_anchor.kind.as_str(),
                    "authority_key": after_anchor.authority_key,
                    "active_from": effective_time.unix_timestamp(),
                    "binding_kind": SurfaceBindingKind::DeclaredRegistryPath.as_str(),
                }),
                format!(
                    "surface-bound:{}:{}:{}",
                    reference.block_hash, name.logical_name_id, surface_binding_id
                ),
                after_anchor.binding_source_family.clone(),
                after_anchor.binding_manifest_version,
                Some(after_anchor.binding_manifest_id),
                reference.canonicality_state,
            ));
        }
    }

    if let Some(name) = history.name.as_ref() {
        let source_family = after
            .as_ref()
            .map(|value| value.binding_source_family.clone())
            .or_else(|| {
                before
                    .as_ref()
                    .map(|value| value.binding_source_family.clone())
            })
            .unwrap_or_else(|| default_registrar_source_family(&name.namespace).to_owned());
        let manifest_version = after
            .as_ref()
            .map(|value| value.binding_manifest_version)
            .or_else(|| before.as_ref().map(|value| value.binding_manifest_version))
            .unwrap_or(1);
        let manifest_id = after
            .as_ref()
            .map(|value| value.binding_manifest_id)
            .or_else(|| before.as_ref().map(|value| value.binding_manifest_id))
            .unwrap_or(0);
        history.events.push(build_boundary_event(
            reference,
            Some(name.logical_name_id.clone()),
            after
                .as_ref()
                .map(|value| value.resource_id)
                .or(before.as_ref().map(|value| value.resource_id)),
            EVENT_KIND_AUTHORITY_EPOCH_CHANGED,
            json!({
                "authority_kind": before.as_ref().map(|value| value.kind.as_str()),
                "authority_key": before.as_ref().map(|value| value.authority_key.clone()),
            }),
            json!({
                "authority_kind": after.as_ref().map(|value| value.kind.as_str()),
                "authority_key": after.as_ref().map(|value| value.authority_key.clone()),
            }),
            format!(
                "authority-epoch:{}:{}:{}:{}:{}",
                reference.block_hash,
                name.logical_name_id,
                effective_time.unix_timestamp(),
                before
                    .as_ref()
                    .map(|value| value.authority_key.as_str())
                    .unwrap_or("none"),
                after
                    .as_ref()
                    .map(|value| value.authority_key.as_str())
                    .unwrap_or("none")
            ),
            source_family,
            manifest_version,
            Some(manifest_id).filter(|value| *value > 0),
            reference.canonicality_state,
        ));
    }

    Ok(())
}

fn authority_eq(left: Option<&AuthorityAnchor>, right: Option<&AuthorityAnchor>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.authority_key == right.authority_key,
        _ => false,
    }
}

fn active_anchor_for_history(history: &NameHistory, chain: &str) -> Option<AuthorityAnchor> {
    if let Some(registration) = history.current_registration.as_ref() {
        return Some(build_registrar_anchor(registration));
    }
    registry_anchor_for_history(history, chain, &history.labelhash)
}

fn current_resolver_matches(history: &NameHistory, resolver: &str) -> bool {
    nonzero_address(history.current_resolver.as_deref())
        .is_some_and(|current| current.eq_ignore_ascii_case(resolver))
}

fn nonzero_address(value: Option<&str>) -> Option<String> {
    value
        .filter(|address| !address.eq_ignore_ascii_case(ZERO_ADDRESS))
        .map(ToOwned::to_owned)
}

fn resource_permission_scope() -> serde_json::Value {
    json!({
        "kind": "resource",
    })
}

fn resolver_permission_scope(chain_id: &str, resolver: &str) -> serde_json::Value {
    json!({
        "kind": "resolver",
        "chain_id": chain_id,
        "resolver_address": resolver,
    })
}

fn permission_source(anchor: &AuthorityAnchor, source_event_kind: &str) -> serde_json::Value {
    json!({
        "kind": "ens_v1_authority",
        "authority_kind": anchor.kind.as_str(),
        "authority_key": anchor.authority_key,
        "source_event_kind": source_event_kind,
    })
}

fn permission_state(
    subject: &str,
    scope: serde_json::Value,
    effective_powers: &[&str],
    grant_source: Option<serde_json::Value>,
    revocation_source: Option<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "subject": subject,
        "scope": scope,
        "effective_powers": effective_powers,
        "grant_source": grant_source,
        "revocation_source": revocation_source,
        "inheritance_path": [],
        "transfer_behavior": PERMISSION_TRANSFER_BEHAVIOR,
    })
}

fn build_observation_permission_change_event(
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    scope: serde_json::Value,
    scope_identity: String,
    power: &str,
    action: PermissionAction,
    source_event_kind: &str,
) -> NormalizedEvent {
    let source = permission_source(anchor, source_event_kind);
    let before_state = match action {
        PermissionAction::Grant => permission_state(subject, scope.clone(), &[], None, None),
        PermissionAction::Revoke => {
            permission_state(subject, scope.clone(), &[power], Some(source.clone()), None)
        }
    };
    let after_state = match action {
        PermissionAction::Grant => permission_state(subject, scope, &[power], Some(source), None),
        PermissionAction::Revoke => permission_state(subject, scope, &[], None, Some(source)),
    };

    build_normalized_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        EVENT_KIND_PERMISSION_CHANGED,
        before_state,
        after_state,
        format!(
            "permission:{}:{}:{}:{}:{}:{}",
            action.as_str(),
            scope_identity,
            subject,
            reference.block_hash,
            reference.transaction_hash.as_deref().unwrap_or_default(),
            reference.log_index.unwrap_or_default()
        ),
    )
}

fn build_boundary_permission_change_event(
    reference: &BoundaryRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    scope: serde_json::Value,
    scope_identity: String,
    power: &str,
    action: PermissionAction,
    source_event_kind: &str,
) -> NormalizedEvent {
    let source = permission_source(anchor, source_event_kind);
    let before_state = match action {
        PermissionAction::Grant => permission_state(subject, scope.clone(), &[], None, None),
        PermissionAction::Revoke => {
            permission_state(subject, scope.clone(), &[power], Some(source.clone()), None)
        }
    };
    let after_state = match action {
        PermissionAction::Grant => permission_state(subject, scope, &[power], Some(source), None),
        PermissionAction::Revoke => permission_state(subject, scope, &[], None, Some(source)),
    };

    build_boundary_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        EVENT_KIND_PERMISSION_CHANGED,
        before_state,
        after_state,
        format!(
            "permission:{}:{}:{}:{}:{}",
            action.as_str(),
            scope_identity,
            subject,
            reference.block_hash,
            anchor.authority_key
        ),
        anchor.binding_source_family.clone(),
        anchor.binding_manifest_version,
        Some(anchor.binding_manifest_id),
        reference.canonicality_state,
    )
}

fn emit_observation_permission_grants(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    source_event_kind: &str,
) {
    events.push(build_observation_permission_change_event(
        reference,
        logical_name_id,
        anchor,
        subject,
        resource_permission_scope(),
        "resource".to_owned(),
        PERMISSION_POWER_RESOURCE_CONTROL,
        PermissionAction::Grant,
        source_event_kind,
    ));

    if let Some(resolver) = nonzero_address(resolver) {
        events.push(build_observation_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            subject,
            resolver_permission_scope(&reference.chain_id, &resolver),
            format!("resolver:{resolver}"),
            PERMISSION_POWER_RESOLVER_CONTROL,
            PermissionAction::Grant,
            source_event_kind,
        ));
    }
}

fn emit_boundary_permission_grants(
    events: &mut Vec<NormalizedEvent>,
    reference: &BoundaryRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    chain_id: &str,
    source_event_kind: &str,
) {
    events.push(build_boundary_permission_change_event(
        reference,
        logical_name_id,
        anchor,
        subject,
        resource_permission_scope(),
        "resource".to_owned(),
        PERMISSION_POWER_RESOURCE_CONTROL,
        PermissionAction::Grant,
        source_event_kind,
    ));

    if let Some(resolver) = nonzero_address(resolver) {
        events.push(build_boundary_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            subject,
            resolver_permission_scope(chain_id, &resolver),
            format!("resolver:{resolver}"),
            PERMISSION_POWER_RESOLVER_CONTROL,
            PermissionAction::Grant,
            source_event_kind,
        ));
    }
}

fn emit_observation_permission_subject_change(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    before_subject: Option<&str>,
    after_subject: Option<&str>,
    resolver: Option<&str>,
    source_event_kind: &str,
) {
    let before_subject = nonzero_address(before_subject);
    let after_subject = nonzero_address(after_subject);
    if before_subject == after_subject {
        return;
    }

    if let Some(subject) = before_subject.as_deref() {
        events.push(build_observation_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            subject,
            resource_permission_scope(),
            "resource".to_owned(),
            PERMISSION_POWER_RESOURCE_CONTROL,
            PermissionAction::Revoke,
            source_event_kind,
        ));
        if let Some(resolver) = nonzero_address(resolver) {
            events.push(build_observation_permission_change_event(
                reference,
                logical_name_id,
                anchor,
                subject,
                resolver_permission_scope(&reference.chain_id, &resolver),
                format!("resolver:{resolver}"),
                PERMISSION_POWER_RESOLVER_CONTROL,
                PermissionAction::Revoke,
                source_event_kind,
            ));
        }
    }

    if let Some(subject) = after_subject.as_deref() {
        emit_observation_permission_grants(
            events,
            reference,
            logical_name_id,
            anchor,
            subject,
            resolver,
            source_event_kind,
        );
    }
}

fn emit_registration_released_event(
    history: &mut NameHistory,
    lease: &RegistrationLease,
    release_ref: &BoundaryRef,
) -> Result<()> {
    let Some(name) = history.name.as_ref() else {
        return Ok(());
    };
    history.events.push(build_boundary_event(
        release_ref,
        Some(name.logical_name_id.clone()),
        Some(deterministic_uuid(&format!(
            "resource:{}",
            lease.authority_key
        ))),
        EVENT_KIND_REGISTRATION_RELEASED,
        json!({
            "registrant": lease.registrant,
            "expiry": lease.expiry.unix_timestamp(),
        }),
        json!({
            "released_at": release_ref.block_timestamp.unix_timestamp(),
            "labelhash": lease.labelhash,
        }),
        format!(
            "release:{}:{}:{}",
            release_ref.block_hash, name.logical_name_id, lease.authority_key
        ),
        lease.start_ref.source_family.clone(),
        lease.start_ref.manifest_version,
        Some(lease.start_ref.source_manifest_id),
        release_ref.canonicality_state,
    ));
    Ok(())
}

impl RegistrationLease {
    fn reference_chain(&self) -> String {
        self.start_ref.chain_id.clone()
    }
}

impl ObservationRef {
    fn as_boundary_ref(&self) -> BoundaryRef {
        BoundaryRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
        }
    }
}

fn release_after_grace(expiry: OffsetDateTime) -> Result<OffsetDateTime> {
    let release_unix = expiry
        .unix_timestamp()
        .checked_add(ENS_GRACE_PERIOD_SECS)
        .context("ENSv1 release timestamp overflowed i64")?;
    OffsetDateTime::from_unix_timestamp(release_unix)
        .context("ENSv1 release timestamp is not a valid unix timestamp")
}

fn build_normalized_event(
    reference: &ObservationRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: serde_json::Value,
    after_state: serde_json::Value,
    identity_suffix: String,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, event_kind, identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: reference.source_family.clone(),
        manifest_version: reference.manifest_version,
        source_manifest_id: Some(reference.source_manifest_id),
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: reference.transaction_hash.clone(),
        log_index: reference.log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "transaction_hash": reference.transaction_hash,
            "transaction_index": reference.transaction_index,
            "log_index": reference.log_index,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state: reference.canonicality_state,
        before_state,
        after_state,
    }
}

fn build_boundary_event(
    reference: &BoundaryRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: serde_json::Value,
    after_state: serde_json::Value,
    identity_suffix: String,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, event_kind, identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family,
        manifest_version,
        source_manifest_id,
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "kind": "raw_block",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "block_timestamp": reference.block_timestamp.unix_timestamp(),
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state,
        before_state,
        after_state,
    }
}

fn observation_labelhash(observation: &AuthorityObservation) -> String {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => value.labelhash.clone(),
        AuthorityObservation::RegistrationRenewed(value) => value.labelhash.clone(),
        AuthorityObservation::TokenTransferred(value) => value.labelhash.clone(),
        AuthorityObservation::RegistryOwnerChanged(value) => value.labelhash.clone(),
        AuthorityObservation::ResolverChanged(_)
        | AuthorityObservation::RecordChanged(_)
        | AuthorityObservation::RecordVersionChanged(_) => {
            unreachable!("resolver observations must be resolved by namehash before use")
        }
    }
}

fn observation_namehash(observation: &AuthorityObservation) -> Option<&str> {
    match observation {
        AuthorityObservation::ResolverChanged(value) => Some(&value.namehash),
        AuthorityObservation::RecordChanged(value) => Some(&value.namehash),
        AuthorityObservation::RecordVersionChanged(value) => Some(&value.namehash),
        _ => None,
    }
}
