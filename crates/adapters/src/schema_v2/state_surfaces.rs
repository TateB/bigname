use uuid::Uuid;

use super::{
    State, V1NameState, V1RegistryReadAnchor, V1ResolverLink, v1_key, v1_registration_is_live,
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) enum V1SurfaceMaterialization {
    RegistryAuthority {
        previous: Box<V1NameState>,
        promoted: Box<V1NameState>,
        resolver: Option<V1ResolverLink>,
        source_manifest_id: i64,
    },
    RegistryRead {
        anchor: V1RegistryReadAnchor,
        resolver: Option<V1ResolverLink>,
        source_manifest_id: i64,
    },
    AlreadyMaterialized,
}

impl State {
    pub(in crate::schema_v2) fn set_v1_resolver_link(
        &mut self,
        namespace: &str,
        namehash: &str,
        resolver: Option<String>,
        resource_id: Option<Uuid>,
        logical_name_id: Option<String>,
        source_role: Option<String>,
    ) -> Option<V1ResolverLink> {
        let key = v1_key(namespace, namehash);
        let previous = self.v1_resolver_links.remove(&key);
        self.v1_resolvers.remove(&key);
        let retain_linked_resource = source_role.as_deref() == Some("registry_old");
        if !retain_linked_resource {
            self.v1_resolver_linked_resources.remove(&key);
        }
        if let Some(resolver_address) = resolver {
            self.v1_resolvers
                .insert(key.clone(), resolver_address.clone());
            let link = V1ResolverLink {
                resolver_address,
                resource_id,
                logical_name_id,
                source_role,
            };
            self.v1_resolver_links.insert(key.clone(), link.clone());
            if retain_linked_resource && let Some(resource_id) = resource_id {
                self.v1_resolver_linked_resources
                    .entry(key)
                    .or_default()
                    .insert(resource_id, link);
            }
        } else {
            if source_role.as_deref() == Some("registry")
                && let Some(registrar) = self.v1_registrars.get(&key)
            {
                self.v1_resolver_links.insert(
                    key.clone(),
                    V1ResolverLink {
                        resolver_address: ZERO_ADDRESS.to_owned(),
                        resource_id: Some(registrar.resource_id),
                        logical_name_id: registrar
                            .surface_known
                            .then(|| registrar.logical_name_id.clone()),
                        source_role,
                    },
                );
            }
            if let Some(resource_id) = resource_id {
                self.remove_v1_resolver_linked_resource(&key, resource_id);
            }
        }
        previous
    }

    pub(in crate::schema_v2) fn remember_v1_resolver_linked_resource(
        &mut self,
        namespace: &str,
        namehash: &str,
        resolver: &str,
        resource_id: Uuid,
        logical_name_id: Option<String>,
    ) {
        let key = v1_key(namespace, namehash);
        if resolver.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000") {
            self.remove_v1_resolver_linked_resource(&key, resource_id);
            return;
        }
        let Some(selected) = self.v1_resolver_links.get(&key) else {
            return;
        };
        if selected.source_role.as_deref() != Some("registry_old")
            || !selected.resolver_address.eq_ignore_ascii_case(resolver)
        {
            return;
        }
        self.v1_resolver_linked_resources
            .entry(key)
            .or_default()
            .insert(
                resource_id,
                V1ResolverLink {
                    resolver_address: selected.resolver_address.clone(),
                    resource_id: Some(resource_id),
                    logical_name_id,
                    source_role: selected.source_role.clone(),
                },
            );
    }

    pub(in crate::schema_v2) fn restore_v1_resolver_linked_resource(
        &mut self,
        namespace: &str,
        namehash: &str,
        resolver: &str,
        resource_id: Uuid,
        logical_name_id: Option<String>,
        source_role: &str,
    ) {
        let key = v1_key(namespace, namehash);
        if resolver.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000") {
            self.remove_v1_resolver_linked_resource(&key, resource_id);
            return;
        }
        if source_role != "registry_old" {
            return;
        }
        self.v1_resolver_linked_resources
            .entry(key)
            .or_default()
            .insert(
                resource_id,
                V1ResolverLink {
                    resolver_address: resolver.to_owned(),
                    resource_id: Some(resource_id),
                    logical_name_id,
                    source_role: Some(source_role.to_owned()),
                },
            );
    }

    fn remove_v1_resolver_linked_resource(&mut self, key: &str, resource_id: Uuid) {
        let remove_name = self
            .v1_resolver_linked_resources
            .get_mut(key)
            .is_some_and(|resources| {
                resources.remove(&resource_id);
                resources.is_empty()
            });
        if remove_name {
            self.v1_resolver_linked_resources.remove(key);
        }
    }

    pub(in crate::schema_v2) fn v1_resolver_link(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<V1ResolverLink> {
        self.v1_resolver_links
            .get(&v1_key(namespace, namehash))
            .filter(|link| !link.resolver_address.eq_ignore_ascii_case(ZERO_ADDRESS))
            .cloned()
    }

    pub(in crate::schema_v2) fn v1_resolver_link_for_resource_activation(
        &self,
        namespace: &str,
        namehash: &str,
        resource_id: Uuid,
    ) -> Option<V1ResolverLink> {
        let key = v1_key(namespace, namehash);
        let retained = self
            .v1_resolver_linked_resources
            .get(&key)
            .and_then(|resources| resources.get(&resource_id));
        self.v1_resolver_link(namespace, namehash)
            .or_else(|| {
                retained.map(|link| V1ResolverLink {
                    resolver_address: ZERO_ADDRESS.to_owned(),
                    resource_id: Some(resource_id),
                    logical_name_id: link.logical_name_id.clone(),
                    source_role: link.source_role.clone(),
                })
            })
            .or_else(|| {
                self.v1_resolver_links
                    .get(&key)
                    .filter(|link| {
                        link.source_role.as_deref() == Some("registry")
                            && link.resource_id == Some(resource_id)
                            && link.resolver_address.eq_ignore_ascii_case(ZERO_ADDRESS)
                    })
                    .cloned()
            })
    }

    pub(in crate::schema_v2) fn v1_resolver_for_activation(
        &self,
        namespace: &str,
        namehash: &str,
        authority: Option<&V1NameState>,
    ) -> Option<V1ResolverLink> {
        authority.and_then(|authority| {
            self.v1_resolver_link_for_resource_activation(
                namespace,
                namehash,
                authority.resource_id,
            )
        })
    }

    pub(in crate::schema_v2) fn converge_v1_registrar_transfer(
        &mut self,
        namespace: &str,
        namehash: &str,
        at_unix_timestamp: i64,
    ) -> Option<V1NameState> {
        let current = self.v1_name(namespace, namehash);
        if current
            .as_ref()
            .is_some_and(|authority| authority.authority_source_family == "ens_v1_wrapper_l1")
        {
            return current;
        }
        let registrar = self.v1_registrar(namespace, namehash)?;
        let registrar_was_current = current
            .as_ref()
            .is_some_and(|authority| authority.resource_id == registrar.resource_id);
        let registry_owner = self.v1_registry_owner(namespace, namehash);
        let registrar_matches_registry = registry_owner.as_deref().is_none_or(|owner| {
            if owner.eq_ignore_ascii_case(ZERO_ADDRESS) {
                registrar_was_current
            } else {
                registrar
                    .owner
                    .as_deref()
                    .is_some_and(|registrant| registrant.eq_ignore_ascii_case(owner))
            }
        });
        let next = if registrar_matches_registry
            && v1_registration_is_live(registrar.expiry, at_unix_timestamp)
        {
            Some(registrar)
        } else {
            self.v1_registry_authority_if_authentic(&v1_key(namespace, namehash))
        };
        self.activate_v1_authority(namespace, namehash, next.clone());
        next
    }

    pub(in crate::schema_v2) fn v1_resolver(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<String> {
        self.v1_resolvers.get(&v1_key(namespace, namehash)).cloned()
    }

    pub(in crate::schema_v2) fn replace_known_source_manifest_ids(
        &mut self,
        manifest_ids: imbl::OrdSet<i64>,
    ) {
        self.known_source_manifest_ids = Some(manifest_ids);
    }

    pub(in crate::schema_v2) fn ensure_restore_succeeded(&self) -> anyhow::Result<()> {
        if let Some(error) = self.restore_error.as_deref() {
            anyhow::bail!("{error}");
        }
        Ok(())
    }

    pub(in crate::schema_v2) fn record_restore_error(&mut self, error: anyhow::Error) {
        if self.restore_error.is_none() {
            self.restore_error = Some(format!("{error:#}"));
        }
    }

    fn require_source_manifest(
        &self,
        namespace: &str,
        namehash: &str,
        manifest_id: i64,
    ) -> anyhow::Result<()> {
        if self
            .known_source_manifest_ids
            .as_ref()
            .is_some_and(|ids| !ids.contains(&manifest_id))
        {
            anyhow::bail!(
                "state-derived source manifest is missing for namespace {namespace}, namehash {namehash}, manifest {manifest_id}"
            );
        }
        Ok(())
    }

    pub(in crate::schema_v2) fn mark_v1_migrated(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) -> (bool, Vec<V1ResolverLink>) {
        let key = v1_key(namespace, namehash);
        let newly_migrated = self.v1_migrated_nodes.insert(key.clone()).is_none();
        if namehash.eq_ignore_ascii_case(
            "0x0000000000000000000000000000000000000000000000000000000000000000",
        ) {
            return (newly_migrated, Vec::new());
        }
        let retired_resolver = if self
            .v1_resolver_links
            .get(&key)
            .is_some_and(|link| link.source_role.as_deref() == Some("registry_old"))
        {
            let link = self.v1_resolver_links.remove(&key);
            self.v1_resolvers.remove(&key);
            link
        } else {
            None
        };
        let mut retired_links: Vec<V1ResolverLink> = self
            .v1_resolver_linked_resources
            .remove(&key)
            .map(|resources| resources.into_iter().map(|(_, link)| link).collect())
            .unwrap_or_default();
        if let Some(retired) = retired_resolver {
            let mut remember = |link: V1ResolverLink| {
                if let Some(existing) = retired_links
                    .iter_mut()
                    .find(|known| known.resource_id == link.resource_id)
                {
                    *existing = link;
                } else {
                    retired_links.push(link);
                }
            };
            remember(retired.clone());
            for authority in [
                self.v1_names.get(&key),
                self.v1_registrars.get(&key),
                self.v1_registry_authorities.get(&key),
            ]
            .into_iter()
            .flatten()
            {
                remember(V1ResolverLink {
                    resolver_address: retired.resolver_address.clone(),
                    resource_id: Some(authority.resource_id),
                    logical_name_id: authority
                        .surface_known
                        .then(|| authority.logical_name_id.clone()),
                    source_role: retired.source_role.clone(),
                });
            }
            if let Some(anchor) = self.v1_registry_read_anchors.get(&key) {
                remember(V1ResolverLink {
                    resolver_address: retired.resolver_address.clone(),
                    resource_id: Some(anchor.resource_id),
                    logical_name_id: anchor.surface_known.then(|| anchor.logical_name_id.clone()),
                    source_role: retired.source_role.clone(),
                });
            }
        }
        (newly_migrated, retired_links)
    }

    pub(in crate::schema_v2) fn sync_registry_surface_from_registrar(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        surface_known: bool,
        labelhash: Option<&str>,
    ) {
        let key = v1_key(namespace, namehash);
        if let Some(registry) = self.v1_registry_authorities.get_mut(&key) {
            registry.logical_name_id = logical_name_id.to_owned();
            registry.surface_known = surface_known;
            registry.labelhash = labelhash.map(str::to_owned);
        }
        if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
            anchor.logical_name_id = logical_name_id.to_owned();
            anchor.surface_known |= surface_known;
        }
    }

    pub(in crate::schema_v2) fn v1_registry_binding(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<(String, String)> {
        let key = v1_key(namespace, namehash);
        Some((
            self.v1_registry_owners.get(&key)?.clone(),
            self.v1_registry_read_anchors
                .get(&key)?
                .registry_contract
                .clone()?,
        ))
    }

    pub(in crate::schema_v2) fn observe_v1_surface(&mut self, namespace: &str, namehash: &str) {
        self.v1_materialized_surfaces
            .insert(v1_key(namespace, namehash));
    }

    pub(in crate::schema_v2) fn observe_v1_active_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) {
        let key = v1_key(namespace, namehash);
        let logical_name_id = format!("{namespace}:{namehash}");
        self.v1_materialized_surfaces.insert(key.clone());
        self.remember_known_surface(logical_name_id.clone());
        if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
            anchor.logical_name_id = logical_name_id;
            anchor.surface_known = true;
        }
    }

    pub(in crate::schema_v2) fn materialize_v1_active_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        labelhash: &str,
    ) -> anyhow::Result<V1SurfaceMaterialization> {
        let key = v1_key(namespace, namehash);
        self.v1_materialized_surfaces.insert(key.clone());
        self.remember_known_surface(logical_name_id.to_owned());

        if let Some(previous) = self.v1_names.get(&key).cloned()
            && previous.token_lineage_id.is_none()
            && !previous.surface_known
        {
            let source_manifest_id = previous.source_manifest_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "registry authority for {namespace}:{namehash} has no source manifest"
                )
            })?;
            self.require_source_manifest(namespace, namehash, source_manifest_id)?;
            let mut promoted = previous.clone();
            promoted.logical_name_id = logical_name_id.to_owned();
            promoted.labelhash = Some(labelhash.to_owned());
            promoted.surface_known = true;
            self.v1_names.insert(key.clone(), promoted.clone());
            self.v1_registry_authorities
                .insert(key.clone(), promoted.clone());
            self.active_resources
                .insert(logical_name_id.to_owned(), promoted.resource_id);
            if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
                anchor.logical_name_id = logical_name_id.to_owned();
                anchor.surface_known = true;
            }
            let resolver = self.v1_resolver_links.get(&key).cloned();
            if let Some(link) = self.v1_resolver_links.get_mut(&key) {
                link.resource_id = Some(promoted.resource_id);
                link.logical_name_id = Some(logical_name_id.to_owned());
            }
            return Ok(V1SurfaceMaterialization::RegistryAuthority {
                previous: Box::new(previous),
                promoted: Box::new(promoted),
                resolver,
                source_manifest_id,
            });
        }

        let explicitly_ownerless = self.v1_registry_owners.get(&key).is_some_and(|owner| {
            owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
        });
        let wrapper_is_current = self
            .v1_names
            .get(&key)
            .is_some_and(|authority| authority.authority_source_family == "ens_v1_wrapper_l1");
        if (explicitly_ownerless || wrapper_is_current)
            && let Some(mut anchor) = self.v1_registry_read_anchors.get(&key).cloned()
            && !anchor.surface_known
        {
            let source_manifest_id = anchor.source_manifest_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "registry read anchor for {namespace}:{namehash} has no source manifest"
                )
            })?;
            self.require_source_manifest(namespace, namehash, source_manifest_id)?;
            anchor.logical_name_id = logical_name_id.to_owned();
            anchor.surface_known = true;
            self.v1_registry_read_anchors
                .insert(key.clone(), anchor.clone());
            self.sync_registry_surface_from_registrar(
                namespace,
                namehash,
                logical_name_id,
                true,
                Some(labelhash),
            );
            let resolver = self.v1_resolver_links.get(&key).cloned();
            if let Some(link) = self.v1_resolver_links.get_mut(&key) {
                link.resource_id = Some(anchor.resource_id);
                link.logical_name_id = Some(logical_name_id.to_owned());
            }
            return Ok(V1SurfaceMaterialization::RegistryRead {
                anchor,
                resolver,
                source_manifest_id,
            });
        }

        Ok(V1SurfaceMaterialization::AlreadyMaterialized)
    }

    pub(in crate::schema_v2) fn materialize_or_sync_v1_active_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        labelhash: &str,
    ) -> anyhow::Result<V1SurfaceMaterialization> {
        let materialization =
            self.materialize_v1_active_surface(namespace, namehash, logical_name_id, labelhash)?;
        if materialization == V1SurfaceMaterialization::AlreadyMaterialized {
            self.sync_registry_surface_from_registrar(
                namespace,
                namehash,
                logical_name_id,
                true,
                Some(labelhash),
            );
        }
        Ok(materialization)
    }

    pub(in crate::schema_v2) fn v1_explicit_ownerless_registry_evidence(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        let key = v1_key(namespace, namehash);
        self.v1_registry_read_anchors.contains_key(&key)
            && self.v1_registry_owners.get(&key).is_some_and(|owner| {
                owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
            })
    }

    pub(in crate::schema_v2) fn activate_retained_v1_registry_authority(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) {
        let authority = self
            .v1_registry_authorities
            .get(&v1_key(namespace, namehash))
            .cloned();
        self.activate_v1_authority(namespace, namehash, authority);
    }

    pub(in crate::schema_v2) fn v1_active_surface_materialized(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        self.known_surfaces
            .contains(&format!("{namespace}:{namehash}"))
    }

    pub(in crate::schema_v2) fn v1_surface_materialized(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        self.v1_materialized_surfaces
            .contains(&v1_key(namespace, namehash))
    }
}
