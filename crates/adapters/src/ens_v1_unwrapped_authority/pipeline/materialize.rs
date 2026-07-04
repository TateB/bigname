use super::*;

pub(super) struct AuthorityMaterialization {
    pub(super) token_lineage_count: usize,
    pub(super) resource_count: usize,
    pub(super) surface_count: usize,
    pub(super) bindings: Vec<SurfaceBinding>,
    pub(super) events: Vec<NormalizedEvent>,
    pub(super) token_lineages_upsert_ms: u128,
    pub(super) resources_upsert_ms: u128,
    pub(super) surfaces_upsert_ms: u128,
}

const IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE: usize = 10_000;

pub(super) struct AuthorityIdentityBuffers {
    token_lineages: Vec<TokenLineage>,
    resources: Vec<Resource>,
    surfaces: Vec<NameSurface>,
    token_lineage_ids: HashSet<Uuid>,
    resource_ids: HashSet<Uuid>,
    surface_ids: HashSet<String>,
    token_lineage_count: usize,
    resource_count: usize,
    surface_count: usize,
    token_lineages_upsert_ms: u128,
    resources_upsert_ms: u128,
    surfaces_upsert_ms: u128,
}

impl AuthorityIdentityBuffers {
    fn new() -> Self {
        Self {
            token_lineages: Vec::with_capacity(IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE),
            resources: Vec::with_capacity(IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE),
            surfaces: Vec::with_capacity(IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE),
            token_lineage_ids: HashSet::new(),
            resource_ids: HashSet::new(),
            surface_ids: HashSet::new(),
            token_lineage_count: 0,
            resource_count: 0,
            surface_count: 0,
            token_lineages_upsert_ms: 0,
            resources_upsert_ms: 0,
            surfaces_upsert_ms: 0,
        }
    }

    pub(super) fn push_token_lineage(&mut self, token_lineage: TokenLineage) {
        if self
            .token_lineage_ids
            .insert(token_lineage.token_lineage_id)
        {
            self.token_lineages.push(token_lineage);
        }
    }

    pub(super) fn push_resource(&mut self, resource: Resource) {
        if self.resource_ids.insert(resource.resource_id) {
            self.resources.push(resource);
        }
    }

    fn push_surface(&mut self, surface: NameSurface) {
        if self.surface_ids.insert(surface.logical_name_id.clone()) {
            self.surfaces.push(surface);
        }
    }

    async fn flush_if_needed(&mut self, pool: &PgPool) -> Result<()> {
        if self.token_lineages.len() >= IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE
            || self.resources.len() >= IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE
            || self.surfaces.len() >= IDENTITY_MATERIALIZATION_FLUSH_BATCH_SIZE
        {
            self.flush(pool).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, pool: &PgPool) -> Result<()> {
        if !self.token_lineages.is_empty() {
            let started = Instant::now();
            upsert_token_lineages_without_snapshots(pool, &self.token_lineages).await?;
            self.token_lineages_upsert_ms += started.elapsed().as_millis();
            self.token_lineage_count += self.token_lineages.len();
            self.token_lineages.clear();
        }
        if !self.resources.is_empty() {
            let started = Instant::now();
            upsert_resources_without_snapshots(pool, &self.resources).await?;
            self.resources_upsert_ms += started.elapsed().as_millis();
            self.resource_count += self.resources.len();
            self.resources.clear();
        }
        if !self.surfaces.is_empty() {
            let started = Instant::now();
            upsert_name_surfaces_without_snapshots(pool, &self.surfaces).await?;
            self.surfaces_upsert_ms += started.elapsed().as_millis();
            self.surface_count += self.surfaces.len();
            self.surfaces.clear();
        }
        Ok(())
    }
}

fn registry_resource_provenance(
    name: &NameMetadata,
    finalized: &FinalizedHistory,
    chain: &str,
    registry_resource_id: Uuid,
    head_ref: &BoundaryRef,
) -> Value {
    let registry_authority_key = format!("registry-only:{}:{}", chain, name.namehash);
    let registry_binding =
        registry_resource_provenance_segment(&finalized.bindings, registry_resource_id, head_ref);
    let mut provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_kind": "registry_only",
        "authority_key": registry_authority_key,
        "logical_name_id": name.logical_name_id.clone(),
        "namehash": name.namehash.clone(),
        "labelhash": finalized.labelhash.clone(),
        "current_registry_owner": finalized.current_registry_owner.clone(),
    });
    if let (Some(object), Some(segment)) = (provenance.as_object_mut(), registry_binding) {
        object.insert(
            "binding_source_family".to_owned(),
            Value::String(segment.authority.binding_source_family.clone()),
        );
        object.insert(
            "binding_manifest_version".to_owned(),
            Value::Number(segment.authority.binding_manifest_version.into()),
        );
        object.insert(
            "binding_manifest_id".to_owned(),
            Value::Number(segment.authority.binding_manifest_id.into()),
        );
    }
    provenance
}

fn registry_resource_provenance_segment<'a>(
    bindings: &'a [BindingSegment],
    registry_resource_id: Uuid,
    head_ref: &BoundaryRef,
) -> Option<&'a BindingSegment> {
    bindings
        .iter()
        .filter(|segment| {
            segment.authority.kind == AuthorityKind::RegistryOnly
                && segment.authority.resource_id == registry_resource_id
                && segment.active_from <= head_ref.block_timestamp
        })
        .max_by_key(|segment| {
            (
                segment_active_at_head(segment, head_ref),
                segment.active_from.unix_timestamp(),
                segment.anchor_ref.block_number,
            )
        })
}

fn segment_active_at_head(segment: &BindingSegment, head_ref: &BoundaryRef) -> bool {
    segment.active_from <= head_ref.block_timestamp
        && segment
            .active_to
            .is_none_or(|active_to| active_to > head_ref.block_timestamp)
}

pub(super) async fn materialize_authority_histories(
    pool: &PgPool,
    chain: &str,
    head_ref: &BoundaryRef,
    histories: BTreeMap<String, NameHistory>,
    reverse_histories: BTreeMap<String, ReverseClaimSourceHistory>,
) -> Result<AuthorityMaterialization> {
    let mut identity = AuthorityIdentityBuffers::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = Vec::<NormalizedEvent>::new();

    for history in histories.into_values() {
        let Some(name) = history.name.clone() else {
            continue;
        };

        let finalized = finalize_history(history, head_ref)?;
        let surface = if let Some(reference) = finalized.first_name_ref.as_ref() {
            build_name_surface(pool, &name, Some(reference)).await?
        } else {
            build_name_surface_from_boundary(
                pool,
                &name,
                finalized
                    .bindings
                    .first()
                    .map(|segment| &segment.anchor_ref),
                "authority_binding_known_name",
            )
            .await?
        };
        if let Some(surface) = surface {
            identity.push_surface(surface);
        }

        if let Some(registry_anchor) = finalized.registry_resource_anchor.as_ref() {
            let registry_authority_key = format!("registry-only:{}:{}", chain, name.namehash);
            let registry_resource_id =
                deterministic_uuid(&format!("resource:{registry_authority_key}"));
            let provenance = registry_resource_provenance(
                &name,
                &finalized,
                chain,
                registry_resource_id,
                head_ref,
            );
            identity.push_resource(
                build_resource(
                    pool,
                    registry_resource_id,
                    None,
                    &registry_anchor.chain_id,
                    registry_anchor,
                    provenance,
                )
                .await?,
            );
        }

        for lease in &finalized.registrar_leases {
            let token_lineage_id =
                deterministic_uuid(&format!("token-lineage:{}", lease.authority_key));
            identity.push_token_lineage(
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
            identity.push_resource(
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

        for authority in &finalized.wrapper_authorities {
            let token_lineage_id =
                deterministic_uuid(&format!("token-lineage:{}", authority.authority_key));
            identity.push_token_lineage(
                build_token_lineage(
                    pool,
                    token_lineage_id,
                    &authority.start_ref.chain_id,
                    &authority.start_ref,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "wrapper",
                        "authority_key": authority.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "namehash": authority.node,
                    }),
                )
                .await?,
            );
            identity.push_resource(
                build_resource(
                    pool,
                    deterministic_uuid(&format!("resource:{}", authority.authority_key)),
                    Some(token_lineage_id),
                    &authority.start_ref.chain_id,
                    &authority.start_ref.as_boundary_ref(),
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "wrapper",
                        "authority_key": authority.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "namehash": authority.node,
                        "owner": authority.owner,
                        "fuses": authority.fuses,
                        "expiry": authority.expiry.unix_timestamp(),
                        "unwrapped_at": authority.end_ref.as_ref().map(|value| value.block_timestamp.unix_timestamp()),
                    }),
                )
                .await?,
            );
        }

        for segment in finalized.bindings {
            ensure_binding_authority_identity_rows(
                pool,
                &mut identity,
                &name.logical_name_id,
                &segment,
            )
            .await?;
            bindings.push(
                build_surface_binding(pool, &name.logical_name_id, &segment, &head_ref.chain_id)
                    .await?,
            );
        }
        events.extend(finalized.events);
        identity.flush_if_needed(pool).await?;
    }
    for history in reverse_histories.into_values() {
        events.extend(history.events);
    }
    identity.flush(pool).await?;

    Ok(AuthorityMaterialization {
        token_lineage_count: identity.token_lineage_count,
        resource_count: identity.resource_count,
        surface_count: identity.surface_count,
        bindings,
        events,
        token_lineages_upsert_ms: identity.token_lineages_upsert_ms,
        resources_upsert_ms: identity.resources_upsert_ms,
        surfaces_upsert_ms: identity.surfaces_upsert_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_resource_provenance_uses_current_reused_registry_segment() -> Result<()> {
        let name = observe_registrar_name_with_version(
            "based1",
            AuthorityProfile::Basenames,
            ENS_NORMALIZER_VERSION,
        )?;
        let labelhash = name.labelhashes[0].clone();
        let authority_key = format!("registry-only:base-mainnet:{}", name.namehash);
        let resource_id = deterministic_uuid(&format!("resource:{authority_key}"));
        let first_ref = basenames_boundary_ref(10, 1_700_000_010, "11")?;
        let leave_ref = basenames_boundary_ref(20, 1_700_000_020, "22")?;
        let current_ref = basenames_boundary_ref(30, 1_700_000_030, "33")?;
        let head_ref = basenames_boundary_ref(40, 1_700_000_040, "44")?;
        let next_ref = basenames_boundary_ref(50, 1_700_000_050, "55")?;
        let finalized = FinalizedHistory {
            labelhash: labelhash.clone(),
            first_name_ref: None,
            bindings: vec![
                registry_segment(
                    Uuid::from_u128(0x3210),
                    &authority_key,
                    resource_id,
                    &first_ref,
                    Some(leave_ref.block_timestamp),
                    1,
                    101,
                ),
                registry_segment(
                    Uuid::from_u128(0x3211),
                    &authority_key,
                    resource_id,
                    &current_ref,
                    None,
                    2,
                    202,
                ),
            ],
            events: Vec::new(),
            registrar_leases: Vec::new(),
            wrapper_authorities: Vec::new(),
            registry_resource_anchor: Some(first_ref),
            current_registry_owner: Some("0x0000000000000000000000000000000000000202".to_owned()),
        };

        let provenance =
            registry_resource_provenance(&name, &finalized, "base-mainnet", resource_id, &head_ref);

        assert_eq!(
            provenance
                .get("binding_source_family")
                .and_then(Value::as_str),
            Some(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
        );
        assert_eq!(
            provenance
                .get("binding_manifest_version")
                .and_then(Value::as_i64),
            Some(2)
        );
        assert_eq!(
            provenance
                .get("binding_manifest_id")
                .and_then(Value::as_i64),
            Some(202)
        );

        let mut history = empty_preloaded_history(labelhash, Some(name));
        preload_registry_history(
            &mut history,
            &provenance,
            &current_ref,
            Uuid::from_u128(0x3212),
            resource_id,
            None,
        );
        let before_anchor = history
            .open_binding
            .as_ref()
            .map(|binding| binding.authority.clone());
        transition_authority(
            &mut history,
            before_anchor,
            None,
            &next_ref,
            next_ref.block_timestamp,
        )?;

        let surface_unbound = history
            .events
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
            .context("preloaded registry binding should emit SurfaceUnbound")?;
        assert_eq!(
            surface_unbound.source_family,
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        );
        assert_eq!(surface_unbound.manifest_version, 2);
        assert_eq!(surface_unbound.source_manifest_id, Some(202));

        Ok(())
    }

    fn basenames_boundary_ref(
        block_number: i64,
        block_timestamp: i64,
        block_hash_seed: &str,
    ) -> Result<BoundaryRef> {
        Ok(BoundaryRef {
            chain_id: "base-mainnet".to_owned(),
            block_hash: format!("0x{}", block_hash_seed.repeat(32)),
            block_number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(block_timestamp)?,
            canonicality_state: CanonicalityState::Finalized,
            namespace: AuthorityProfile::Basenames.namespace().to_owned(),
        })
    }

    fn registry_segment(
        surface_binding_id: Uuid,
        authority_key: &str,
        resource_id: Uuid,
        anchor_ref: &BoundaryRef,
        active_to: Option<OffsetDateTime>,
        binding_manifest_version: i64,
        binding_manifest_id: i64,
    ) -> BindingSegment {
        BindingSegment {
            surface_binding_id,
            authority: AuthorityAnchor {
                kind: AuthorityKind::RegistryOnly,
                authority_key: authority_key.to_owned(),
                resource_id,
                token_lineage_id: None,
                binding_source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
                binding_manifest_version,
                binding_manifest_id,
            },
            active_from: anchor_ref.block_timestamp,
            active_to,
            anchor_ref: anchor_ref.clone(),
        }
    }
}
