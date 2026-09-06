//! Hash-covered schema-v2 output seam for the production interpret phase.
//!
//! The phase runner supplies immutable raw facts and stored manifest/admission rows. Adapters
//! decode them into schema-v2 rows; persistence remains outside this hash root.

mod catalog;
mod common;
mod discovery;
mod identity;
mod manifest;
mod migration;
#[cfg(feature = "test-activation")]
pub use migration::inject_activated_transition_for_test;
mod model;
mod normalized;
mod protocol;
pub mod seam;
mod session;
mod sourced_events;
mod state;
mod state_key;
mod state_residency;
mod state_restore;

pub use model::*;
pub use session::{
    AdapterSession, AdapterSessionRestore, InterpreterStateRequest, InterpreterStateValue,
    PreparedAdapterBatch, begin_schema_v2_adapter_restore,
    begin_schema_v2_adapter_restore_with_provenance, interpret_schema_v2_batch,
    prepare_schema_v2_batch_incremental, prepare_schema_v2_batch_incremental_with_provenance,
};
pub use state_residency::StateCacheCapacity;

use anyhow::{Context, bail};

use self::{catalog::Catalog, state::State};

fn settle_block_boundary(
    catalog: &Catalog,
    block: &RawBlockInput,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for release in state.settle_v1_releases(block.block_timestamp.unix_timestamp()) {
        let registrar_source = release
            .registrar
            .source_manifest_id
            .and_then(|manifest_id| catalog.source(manifest_id))
            .or_else(|| catalog.source_for_family(&release.registrar.authority_source_family))
            .with_context(|| {
                format!(
                    "registration release for {} has no active {} manifest",
                    release.namehash, release.registrar.authority_source_family
                )
            })?
            .clone();
        let transition_authority = release
            .next_authority
            .as_ref()
            .filter(|authority| authority.surface_known)
            .or_else(|| {
                release
                    .previous_authority
                    .as_ref()
                    .filter(|authority| authority.surface_known)
            });
        let active_source = transition_authority
            .and_then(|authority| {
                authority
                    .source_manifest_id
                    .and_then(|manifest_id| catalog.source(manifest_id))
                    .or_else(|| catalog.source_for_family(&authority.authority_source_family))
            })
            .unwrap_or(&registrar_source)
            .clone();
        let logical_name_id = release.registrar.logical_name_id.clone();
        let registrar_key = release
            .registrar
            .authority_key
            .clone()
            .unwrap_or_else(|| format!("registrar:{}", release.namehash));
        let mut registration_events = vec![protocol::EventDraft {
            event_kind: "RegistrationReleased".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(release.registrar.resource_id),
            identity_suffix: format!("RegistrationReleased:{}:{registrar_key}", release.namehash),
            explicit_before: Some(serde_json::json!({
                "registrant":release.registrar.owner,
                "expiry":release.registrar.expiry,
            })),
            after_state: serde_json::json!({
                "source_event":"RegistrationReleased",
                "released_at":block.block_timestamp.unix_timestamp(),
                "labelhash":release.registrar.labelhash,
                "namehash":release.namehash,
            }),
            state_scope: format!("boundary:{}:registration", release.namehash),
        }];
        if release.release_was_active
            && let (Some(subject), Some(authority_key)) = (
                release.registrar.owner.as_deref(),
                release.registrar.authority_key.as_deref(),
            )
        {
            append_boundary_permission(
                &mut registration_events,
                &release.registrar,
                subject,
                serde_json::json!({"kind":"resource"}),
                "resource_control",
                false,
                "registrar-release",
            );
            if let Some(resolver) = release.resolver.as_deref() {
                append_boundary_permission(
                    &mut registration_events,
                    &release.registrar,
                    subject,
                    serde_json::json!({
                        "kind":"resolver",
                        "chain_id":block.chain_id,
                        "resolver_address":resolver,
                    }),
                    "resolver_control",
                    false,
                    "registrar-resolver-release",
                );
            }
            let _ = authority_key;
        }
        normalized::materialize_boundary(
            &registrar_source,
            block,
            registration_events,
            state,
            output,
        );
        if release
            .previous_authority
            .as_ref()
            .map(|authority| authority.authority_key.as_ref())
            == release
                .next_authority
                .as_ref()
                .map(|authority| authority.authority_key.as_ref())
        {
            continue;
        }
        let previous_surface = release
            .previous_authority
            .as_ref()
            .filter(|authority| authority.surface_known);
        let next_surface = release
            .next_authority
            .as_ref()
            .filter(|authority| authority.surface_known);
        if previous_surface.is_none() && next_surface.is_none() {
            continue;
        }
        let mut transition_events = Vec::new();
        if let Some(previous) = previous_surface {
            let previous_kind = v1_authority_kind(&previous.authority_source_family);
            let previous_key = previous
                .authority_key
                .clone()
                .unwrap_or_else(|| format!("{previous_kind}:{}", release.namehash));
            transition_events.push(protocol::EventDraft {
                event_kind: "SurfaceUnbound".to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(previous.resource_id),
                identity_suffix: format!("SurfaceUnbound:{}:{previous_key}", release.namehash),
                explicit_before: Some(serde_json::json!({
                    "authority_kind":previous_kind,
                    "authority_key":previous_key,
                })),
                after_state: serde_json::json!({
                    "source_event":"RegistrationReleased",
                    "authority_kind":previous_kind,
                    "authority_key":previous_key,
                    "active_to":block.block_timestamp.unix_timestamp(),
                }),
                state_scope: format!("boundary:{}:surface", release.namehash),
            });
        }
        if let Some(next) = next_surface {
            let next_kind = v1_authority_kind(&next.authority_source_family);
            let next_key = next
                .authority_key
                .clone()
                .unwrap_or_else(|| format!("{next_kind}:{}", release.namehash));
            transition_events.push(protocol::EventDraft {
                event_kind: "SurfaceBound".to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(next.resource_id),
                identity_suffix: format!("SurfaceBound:{}:{next_key}", release.namehash),
                explicit_before: Some(serde_json::json!({})),
                after_state: serde_json::json!({
                    "source_event":"RegistrationReleased",
                    "authority_kind":next_kind,
                    "authority_key":next_key,
                    // The expiry fallback has no registry observation of its own.
                    "owner_getter":next.owner,
                    "registry_contract":next.registry_contract,
                    "active_from":block.block_timestamp.unix_timestamp(),
                    "binding_kind":"declared_registry_path",
                }),
                state_scope: format!("boundary:{}:surface", release.namehash),
            });
        }
        transition_events.push(protocol::EventDraft {
            event_kind: "AuthorityEpochChanged".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: next_surface
                .or(previous_surface)
                .map(|authority| authority.resource_id),
            identity_suffix: format!("AuthorityEpochChanged:{}", release.namehash),
            explicit_before: Some(authority_boundary_state(
                release.previous_authority.as_ref(),
            )),
            after_state: authority_boundary_state(release.next_authority.as_ref()),
            state_scope: format!("boundary:{}:authority", release.namehash),
        });
        if let (Some(next), Some(resolver)) = (next_surface, release.resolver.as_deref()) {
            transition_events.push(protocol::EventDraft {
                event_kind: "ResolverChanged".to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(next.resource_id),
                identity_suffix: format!("ResolverChanged:{}:{resolver}", release.namehash),
                explicit_before: Some(serde_json::json!({"resolver":serde_json::Value::Null})),
                after_state: serde_json::json!({
                    "source_event":"AuthorityEpochChanged",
                    "resolver":resolver,
                    "namehash":release.namehash,
                }),
                state_scope: format!("boundary:{}:resolver", release.namehash),
            });
            if let Some(subject) = next.owner.as_deref() {
                append_boundary_permission(
                    &mut transition_events,
                    next,
                    subject,
                    serde_json::json!({"kind":"resource"}),
                    "resource_control",
                    true,
                    "registry-grant",
                );
                append_boundary_permission(
                    &mut transition_events,
                    next,
                    subject,
                    serde_json::json!({
                        "kind":"resolver",
                        "chain_id":block.chain_id,
                        "resolver_address":resolver,
                    }),
                    "resolver_control",
                    true,
                    "registry-resolver-grant",
                );
            }
        } else if let Some(next) = next_surface
            && let Some(subject) = next.owner.as_deref()
        {
            append_boundary_permission(
                &mut transition_events,
                next,
                subject,
                serde_json::json!({"kind":"resource"}),
                "resource_control",
                true,
                "registry-grant",
            );
        }
        normalized::materialize_boundary(&active_source, block, transition_events, state, output);
        let next_binding_id = next_surface.map(|next| {
            common::stable_uuid(&format!(
                "binding:{}:{}",
                next.authority_key.as_deref().unwrap_or("registry-only"),
                block.block_hash,
            ))
        });
        if previous_surface.is_some() {
            output.binding_closures.push(BindingClosure {
                logical_name_id: logical_name_id.clone(),
                authority_arm: if active_source.namespace == "basenames" {
                    "basenames"
                } else {
                    "ens_v1"
                }
                .to_owned(),
                chain_id: block.chain_id.clone(),
                except_surface_binding_id: next_binding_id,
                active_to: block.block_timestamp,
                block_number: block.block_number,
                transaction_index: -1,
                log_index: -1,
            });
        }
        if let (Some(next), Some(surface_binding_id)) = (next_surface, next_binding_id) {
            let boundary_provenance = serde_json::json!({
                "kind":"raw_block",
                "chain_id":block.chain_id,
                "block_hash":block.block_hash,
                "block_number":block.block_number,
                "source_manifest_id":active_source.manifest_id,
            });
            output.resources.push(Resource {
                resource_id: next.resource_id,
                token_lineage_id: next.token_lineage_id,
                chain_id: block.chain_id.clone(),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                provenance: boundary_provenance.clone(),
                canonicality_state: block.canonicality_state.clone(),
            });
            output.surface_bindings.push(SurfaceBinding {
                surface_binding_id,
                logical_name_id,
                resource_id: next.resource_id,
                binding_kind: "declared_registry_path".to_owned(),
                authority_arm: if active_source.namespace == "basenames" {
                    "basenames"
                } else {
                    "ens_v1"
                }
                .to_owned(),
                active_from: block.block_timestamp,
                chain_id: block.chain_id.clone(),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                provenance: boundary_provenance,
                canonicality_state: block.canonicality_state.clone(),
            });
        }
    }
    for transition in state.refresh_dirty_v2_names(block.block_timestamp.unix_timestamp()) {
        let registry_instance = transition
            .registry_contract_instance_id
            .context("ENSv2 boundary transition has no registry contract identity")?;
        let source = catalog
            .source_for_contract_instance(registry_instance)
            .with_context(|| {
                format!(
                    "ENSv2 boundary transition for registry {registry_instance} has no active manifest"
                )
            })?
            .clone();
        if let Some(interpreted) = protocol::v2_registry::boundary_reassertion(&transition, block) {
            identity::materialize_v2_boundary(&source, block, interpreted, state, output)?;
            continue;
        }
        let mut interpreted =
            protocol::v2_boundary_expiration(transition, block.block_timestamp.unix_timestamp())?;
        if !interpreted.labels.is_empty()
            || !interpreted.names.is_empty()
            || !interpreted.resources.is_empty()
            || !interpreted.bindings.is_empty()
            || !interpreted.discovery.is_empty()
        {
            bail!("ENSv2 expiration boundary produced an unsupported materialization");
        }
        for closure in interpreted.binding_closures.drain(..) {
            output.binding_closures.push(BindingClosure {
                logical_name_id: closure.logical_name_id,
                authority_arm: closure.authority_arm,
                chain_id: block.chain_id.clone(),
                except_surface_binding_id: None,
                active_to: block.block_timestamp,
                block_number: block.block_number,
                transaction_index: -1,
                log_index: -1,
            });
        }
        normalized::materialize_boundary(&source, block, interpreted.events, state, output);
    }
    Ok(())
}

fn authority_boundary_state(authority: Option<&state::V1NameState>) -> serde_json::Value {
    serde_json::json!({
        "source_event":"RegistrationReleased",
        "authority_kind":authority.map(|value| v1_authority_kind(&value.authority_source_family)),
        "authority_key":authority.and_then(|value| value.authority_key.clone()),
        "owner":authority.and_then(|value| value.owner.clone()),
    })
}

#[allow(clippy::too_many_arguments)]
fn append_boundary_permission(
    events: &mut Vec<protocol::EventDraft>,
    authority: &state::V1NameState,
    subject: &str,
    scope: serde_json::Value,
    power: &str,
    grant: bool,
    suffix: &str,
) {
    let Some(authority_key) = authority.authority_key.as_deref() else {
        return;
    };
    let authority_kind = v1_authority_kind(&authority.authority_source_family);
    let (before, after) = if grant {
        protocol::permissions::v1_grant_states(
            subject,
            scope,
            power,
            authority_kind,
            authority_key,
            "RegistrationReleased",
        )
    } else {
        protocol::permissions::v1_revoke_states(
            subject,
            scope,
            power,
            authority_kind,
            authority_key,
            "RegistrationReleased",
        )
    };
    events.push(protocol::EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: authority
            .surface_known
            .then(|| authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix: format!("PermissionChanged:{suffix}:{subject}:{authority_key}"),
        explicit_before: Some(before),
        after_state: after,
        state_scope: format!("boundary:{}:{suffix}:{subject}", authority.logical_name_id),
    });
}

fn v1_authority_kind(source_family: &str) -> &'static str {
    match source_family {
        "ens_v1_wrapper_l1" => "wrapper",
        "ens_v1_registrar_l1" | "basenames_base_registrar" => "registrar",
        _ => "registry_only",
    }
}

fn validate_order(input: &BatchInput) -> anyhow::Result<()> {
    if input.chain_id.trim().is_empty() {
        bail!("schema-v2 adapter batch chain ID must not be empty");
    }
    let mut previous = None;
    let mut live_hash_by_height = std::collections::BTreeMap::new();
    for block in &input.blocks {
        if block.chain_id != input.chain_id {
            bail!(
                "raw block chain {} does not match adapter batch chain {}",
                block.chain_id,
                input.chain_id
            );
        }
        if let Some(existing) = live_hash_by_height.insert(block.block_number, &block.block_hash)
            && existing != &block.block_hash
        {
            bail!(
                "schema-v2 adapter received multiple live-lineage hashes at block {}: {} and {}",
                block.block_number,
                existing,
                block.block_hash
            );
        }
        let position = (block.block_number, block.block_hash.as_str());
        if previous.is_some_and(|previous| previous > position) {
            bail!("schema-v2 adapter raw blocks are not in block order");
        }
        previous = Some(position);
    }
    let mut previous = None;
    for raw in &input.raw_logs {
        if raw.chain_id != input.chain_id {
            bail!(
                "raw log chain {} does not match adapter batch chain {}",
                raw.chain_id,
                input.chain_id
            );
        }
        if raw.transaction_index < 0 || raw.log_index < 0 {
            bail!(
                "raw log {}:{} requires non-negative transaction and log indexes",
                raw.block_hash,
                raw.log_index
            );
        }
        let position = (
            raw.block_number,
            raw.transaction_index,
            raw.log_index,
            raw.block_hash.as_str(),
        );
        if previous.is_some_and(|previous| previous > position) {
            bail!("schema-v2 adapter raw logs are not in block order");
        }
        previous = Some(position);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
