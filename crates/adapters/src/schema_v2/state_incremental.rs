use imbl::{ordmap::OrdMap, ordset::OrdSet};
use serde_json::Value;
use uuid::Uuid;

use super::{State, StateCacheCapacity, StateResidency};
use crate::schema_v2::{model::PriorEventInput, state_key::interpreter_state_key};

impl State {
    #[cfg(test)]
    pub(in crate::schema_v2) fn new(
        prior: Vec<PriorEventInput>,
        v2_suffix_anchors: Vec<(String, String, Vec<String>)>,
    ) -> Self {
        Self::with_cache_capacity(prior, v2_suffix_anchors, StateCacheCapacity::Unlimited)
    }

    #[cfg(test)]
    fn with_cache_capacity(
        prior: Vec<PriorEventInput>,
        v2_suffix_anchors: Vec<(String, String, Vec<String>)>,
        cache_capacity: StateCacheCapacity,
    ) -> Self {
        Self::with_cache_capacity_and_manifest_ids(prior, v2_suffix_anchors, cache_capacity, None)
    }

    pub(in crate::schema_v2) fn with_cache_capacity_and_manifest_ids(
        prior: Vec<PriorEventInput>,
        v2_suffix_anchors: Vec<(String, String, Vec<String>)>,
        cache_capacity: StateCacheCapacity,
        known_source_manifest_ids: Option<OrdSet<i64>>,
    ) -> Self {
        let mut state = Self {
            values: StateResidency::new(cache_capacity),
            provisional_values: OrdMap::new(),
            v1_names: OrdMap::new(),
            v1_wrapper_data: OrdMap::new(),
            v1_pending_unwraps: OrdMap::new(),
            v1_registrar_controller_transaction: None,
            v1_registrar_controllers: OrdSet::new(),
            v1_pending_wrapper_sync_expiries: OrdMap::new(),
            v1_correlated_wrapper_expiries: OrdMap::new(),
            v1_registrars: OrdMap::new(),
            v1_expiries: OrdSet::new(),
            v1_registry_authorities: OrdMap::new(),
            v1_registry_owners: OrdMap::new(),
            v1_registry_owner_words: OrdMap::new(),
            v1_registry_owner_reasons: OrdMap::new(),
            v1_registry_read_anchors: OrdMap::new(),
            v1_resolvers: OrdMap::new(),
            v1_resolver_links: OrdMap::new(),
            v1_resolver_linked_resources: OrdMap::new(),
            known_source_manifest_ids,
            restore_error: None,
            v1_migrated_nodes: OrdSet::new(),
            v1_materialized_surfaces: OrdSet::new(),
            known_surfaces: OrdSet::new(),
            restored_surface_sources: OrdMap::new(),
            restored_surface_counts: OrdMap::new(),
            v2_current_surface_counts: OrdMap::new(),
            surface_removal_candidates: OrdSet::new(),
            restoring_state_key: None,
            active_resources: OrdMap::new(),
            v2_tokens: OrdMap::new(),
            v2_subregistry_tokens_by_observation: OrdMap::new(),
            v2_resolver_tokens_by_observation: OrdMap::new(),
            v2_resolver_aliases_by_observation: OrdMap::new(),
            v2_expiries: OrdSet::new(),
            v2_dirty_tokens: OrdSet::new(),
            v2_dirty_registries: OrdSet::new(),
            v2_terminal_closure_hits: OrdSet::new(),
            v2_token_by_upstream_resource_index: OrdMap::new(),
            v2_token_by_name_index: OrdMap::new(),
            v2_tokens_by_current_name_index: OrdMap::new(),
            v2_entry_by_parent_label: OrdMap::new(),
            v2_parent_claims: OrdMap::new(),
            v2_resolver_hints: OrdMap::new(),
            materialized_token_lineages: OrdSet::new(),
            v2_suffix_anchors: v2_suffix_anchors
                .into_iter()
                .map(|(address, namespace, suffix)| {
                    (address.to_ascii_lowercase(), (namespace, suffix))
                })
                .collect(),
            latest_v2_timestamp: None,
        };
        state.restore_prior_events(prior);
        state
    }

    fn restore_prior_events(&mut self, prior: Vec<PriorEventInput>) {
        self.apply_prior_events(prior, true, true);
    }

    pub(in crate::schema_v2) fn apply_prior_event_delta(&mut self, prior: Vec<PriorEventInput>) {
        self.apply_prior_events(prior, false, false);
    }

    pub(in crate::schema_v2) fn begin_batch(&mut self) {
        self.clear_provisional_values();
        self.v2_terminal_closure_hits.clear();
    }

    pub(in crate::schema_v2) fn pending_v2_terminal_closure_hit(
        &self,
    ) -> Option<&(String, String)> {
        self.v2_terminal_closure_hits.get_min()
    }

    pub(in crate::schema_v2) fn replace_ens_v1_protocol_state_from_replay(
        &mut self,
        replayed: Self,
    ) {
        self.v1_names = replayed.v1_names;
        self.v1_wrapper_data = replayed.v1_wrapper_data;
        self.v1_registrars = replayed.v1_registrars;
        self.v1_expiries = replayed.v1_expiries;
        self.v1_registry_authorities = replayed.v1_registry_authorities;
        self.v1_registry_owners = replayed.v1_registry_owners;
        self.v1_registry_owner_words = replayed.v1_registry_owner_words;
        self.v1_registry_owner_reasons = replayed.v1_registry_owner_reasons;
        self.v1_registry_read_anchors = replayed.v1_registry_read_anchors;
        self.v1_resolvers = replayed.v1_resolvers;
        self.v1_resolver_links = replayed.v1_resolver_links;
        self.v1_resolver_linked_resources = replayed.v1_resolver_linked_resources;
        self.v1_migrated_nodes = replayed.v1_migrated_nodes;
        self.v1_materialized_surfaces = replayed.v1_materialized_surfaces;
    }

    pub(in crate::schema_v2) fn commit_v2_batch_boundary(&mut self, at_unix_timestamp: i64) {
        if self.latest_v2_timestamp.is_none() && self.v2_tokens.is_empty() {
            return;
        }
        self.refresh_dirty_v2_names(at_unix_timestamp);
        self.prune_unbacked_surfaces();
    }

    pub(in crate::schema_v2) fn restore_prior_event_chunk(&mut self, prior: Vec<PriorEventInput>) {
        self.apply_prior_events(prior, true, false);
    }

    pub(in crate::schema_v2) fn finish_prior_event_restore(
        &mut self,
        resume_predecessor_timestamp: Option<i64>,
    ) {
        if let Some(timestamp) = self.latest_v2_timestamp {
            // Replaying prior events reconstructs topology before the next raw-log batch. The
            // resulting transitions already exist in normalized_events, so only retain the
            // reconstructed name state here.
            self.refresh_all_v2_names(timestamp);
        }
        if let Some(timestamp) = resume_predecessor_timestamp
            && (self.latest_v2_timestamp.is_some() || !self.v2_tokens.is_empty())
        {
            self.refresh_dirty_v2_names(timestamp);
        }
        crate::schema_v2::state_restore::rebuild_v2_indexes(self);
        self.prune_unbacked_surfaces();
    }

    fn apply_prior_events(
        &mut self,
        prior: Vec<PriorEventInput>,
        full_restore: bool,
        finish_restore: bool,
    ) {
        for token_lineage_id in prior.iter().filter_map(|event| {
            event
                .after_state
                .get(crate::schema_v2::seam::TOKEN_LINEAGE_ID_KEY)
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
        }) {
            self.materialized_token_lineages.insert(token_lineage_id);
        }
        let previous_v2_timestamp = self.latest_v2_timestamp;
        let mut latest_delta_timestamp = None;
        let mut refresh_targets = V2RefreshTargets::default();
        for event in prior {
            self.replace_restored_surface_source(&event.retained_state_key);
            self.restoring_state_key = Some(event.retained_state_key.clone());
            let is_v2_topology = matches!(
                event.source_family.as_str(),
                "ens_v2_registry_l1" | "ens_v2_root_l1"
            );
            if is_v2_topology
                && event.event_kind == "PreimageObserved"
                && event
                    .after_state
                    .get("visibility_state")
                    .and_then(Value::as_str)
                    != Some("shadow")
                && let Some(logical_name_id) = event.logical_name_id.as_ref()
            {
                // A canonical [name surface](../../../../docs/glossary.md#surface-name-surface)
                // remains known after its registry binding or resource ends.
                // Normalization-rejected name observations never enter this state.
                self.observe_name_surface(logical_name_id.clone());
            }
            if is_v2_topology {
                latest_delta_timestamp = latest_delta_timestamp.max(
                    event
                        .block_timestamp
                        .map(time::OffsetDateTime::unix_timestamp),
                );
                if !full_restore {
                    self.capture_v2_refresh_targets(&event, &mut refresh_targets);
                }
            }
            let scope = event.state_scope.as_deref().unwrap_or("legacy");
            let key = interpreter_state_key(
                &event.namespace,
                event.logical_name_id.as_deref(),
                event.resource_id,
                &event.event_kind,
                &event.source_family,
                scope,
            );
            self.values.insert(key, event.after_state.clone());
            crate::schema_v2::state_restore::v1(self, &event);
            crate::schema_v2::state_restore::v2(self, &event);
            if is_v2_topology && !full_restore {
                self.capture_v2_refresh_targets(&event, &mut refresh_targets);
            }
            self.restoring_state_key = None;
        }
        self.latest_v2_timestamp = if full_restore {
            previous_v2_timestamp.max(latest_delta_timestamp)
        } else {
            previous_v2_timestamp
        };
        if full_restore && finish_restore {
            self.finish_prior_event_restore(None);
        } else if let Some(delta_timestamp) = latest_delta_timestamp {
            self.v2_dirty_tokens.extend(refresh_targets.tokens);
            self.v2_dirty_registries.extend(refresh_targets.registries);
            self.refresh_dirty_v2_names(delta_timestamp);
        }
        if !full_restore {
            self.prune_unbacked_surfaces();
        }
    }

    fn capture_v2_refresh_targets(&self, event: &PriorEventInput, targets: &mut V2RefreshTargets) {
        let Some(emitter) = event
            .state_scope
            .as_deref()
            .and_then(|scope| scope.split(':').next())
            .map(str::to_ascii_lowercase)
        else {
            return;
        };
        if event.event_kind == "ParentChanged" {
            targets.registries.insert(emitter.clone());
        }
        if matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "RegistrationReserved"
        ) && let Some(raw_label) = restored_raw_label(&event.after_state)
            && let Some(displaced) = self
                .v2_entry_by_parent_label
                .get(&(emitter.clone(), raw_label))
        {
            self.capture_v2_token_target(displaced, targets);
        }
        for token_id in [
            "token_id",
            "current_token_id",
            "old_token_id",
            "new_token_id",
        ]
        .into_iter()
        .filter_map(|field| event.after_state.get(field).and_then(Value::as_str))
        {
            self.capture_v2_token_target(&v2_token_key(&emitter, token_id), targets);
        }
        if let Some(subregistry) = event.after_state.get("subregistry").and_then(Value::as_str) {
            targets.registries.insert(subregistry.to_ascii_lowercase());
        }
    }

    fn capture_v2_token_target(&self, token_key: &str, targets: &mut V2RefreshTargets) {
        targets.tokens.insert(token_key.to_owned());
        if let Some(subregistry) = self
            .v2_tokens
            .get(token_key)
            .and_then(|token| token.subregistry.as_ref())
        {
            targets.registries.insert(subregistry.to_ascii_lowercase());
        }
    }

    fn replace_restored_surface_source(&mut self, state_key: &str) {
        let Some(surfaces) = self.restored_surface_sources.remove(state_key) else {
            return;
        };
        for surface in surfaces {
            decrement_count(&mut self.restored_surface_counts, &surface);
            self.surface_removal_candidates.insert(surface);
        }
    }

    pub(super) fn remember_known_surface(&mut self, logical_name_id: String) {
        self.known_surfaces.insert(logical_name_id.clone());
        let Some(state_key) = self.restoring_state_key.clone() else {
            return;
        };
        if self
            .restored_surface_sources
            .entry(state_key)
            .or_default()
            .insert(logical_name_id.clone())
            .is_none()
        {
            increment_count(&mut self.restored_surface_counts, logical_name_id);
        }
    }

    pub(super) fn replace_v2_current_surface(
        &mut self,
        previous: Option<&str>,
        current: Option<&str>,
    ) {
        if previous == current {
            return;
        }
        if let Some(previous) = previous {
            decrement_count(&mut self.v2_current_surface_counts, previous);
            self.surface_removal_candidates.insert(previous.to_owned());
        }
        if let Some(current) = current {
            increment_count(&mut self.v2_current_surface_counts, current.to_owned());
            self.known_surfaces.insert(current.to_owned());
        }
    }

    fn prune_unbacked_surfaces(&mut self) {
        for surface in std::mem::take(&mut self.surface_removal_candidates) {
            if !self.restored_surface_counts.contains_key(&surface)
                && !self.v2_current_surface_counts.contains_key(&surface)
            {
                self.known_surfaces.remove(&surface);
            }
        }
    }

    pub(in crate::schema_v2) fn replace_v2_suffix_anchors(
        &mut self,
        anchors: Vec<(String, String, Vec<String>)>,
    ) {
        let anchors = anchors
            .into_iter()
            .map(|(address, namespace, suffix)| (address.to_ascii_lowercase(), (namespace, suffix)))
            .collect();
        if self.v2_suffix_anchors == anchors {
            return;
        }
        self.v2_suffix_anchors = anchors;
        if let Some(timestamp) = self.latest_v2_timestamp {
            self.refresh_all_v2_names(timestamp);
            self.prune_unbacked_surfaces();
        }
    }
}

#[derive(Default)]
struct V2RefreshTargets {
    tokens: OrdSet<String>,
    registries: OrdSet<String>,
}

fn v2_token_key(emitter: &str, token_id: &str) -> String {
    format!(
        "{}:{}",
        emitter.to_ascii_lowercase(),
        token_id.to_ascii_lowercase()
    )
}

fn restored_raw_label(after_state: &Value) -> Option<Vec<u8>> {
    after_state
        .get("raw_label_hex")
        .and_then(Value::as_str)
        .and_then(|value| alloy_primitives::hex::decode(value).ok())
        .or_else(|| {
            after_state
                .get("label")
                .and_then(Value::as_str)
                .map(|label| label.as_bytes().to_vec())
        })
        .or_else(|| {
            after_state
                .get("raw_labels")
                .and_then(Value::as_array)
                .and_then(|labels| labels.first())
                .and_then(Value::as_str)
                .map(|label| label.as_bytes().to_vec())
        })
}

fn increment_count(counts: &mut OrdMap<String, usize>, key: String) {
    let next = counts.get(&key).copied().unwrap_or_default() + 1;
    counts.insert(key, next);
}

fn decrement_count(counts: &mut OrdMap<String, usize>, key: &str) {
    let Some(previous) = counts.get(key).copied() else {
        return;
    };
    if previous == 1 {
        counts.remove(key);
    } else {
        counts.insert(key.to_owned(), previous - 1);
    }
}
