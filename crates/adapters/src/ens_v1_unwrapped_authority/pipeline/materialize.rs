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
            identity.push_resource(
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
