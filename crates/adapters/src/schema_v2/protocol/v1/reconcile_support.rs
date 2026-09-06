mod event_index;
mod side_index;

use std::collections::{BTreeMap, BTreeSet};

use uuid::Uuid;

use self::{
    event_index::{
        EventFields, EventIndex, PermissionRevocation, Position, Registration, SourceEvent,
        SourceFamily,
    },
    side_index::{BindingIndex, ClosureIndex},
};
use super::{refresh_interpreter_state_key, retarget_permission_authority};
use crate::schema_v2::model::BatchOutput;

pub(super) fn reconcile(output: &mut BatchOutput) {
    // Extract JSON-backed comparison fields and build the transaction/name, position, and
    // resource indexes once. Reconciliation below visits only matching candidates.
    let mut events = EventIndex::new(&output.normalized_events);
    let registrations = events.registrations(&output.normalized_events);
    let mut bindings = BindingIndex::new(output);
    let mut closures = ClosureIndex::new(output);

    for registration in registrations {
        reconcile_registration(
            output,
            &mut events,
            &mut bindings,
            &mut closures,
            &registration,
        );
    }

    retain_by_flags(&mut output.normalized_events, &events.active);
    retain_by_flags(&mut output.surface_bindings, &bindings.active);
    retain_by_flags(&mut output.binding_closures, &closures.active);
    // Superseded registry-only resource emissions are always retained at their first derivation
    // block, whether or not a surviving row in this batch references them: dropping an
    // unreferenced emission and re-emitting at a later referencing event would anchor the same
    // resource at different blocks depending on where batch boundaries fall.
}

fn reconcile_registration(
    output: &mut BatchOutput,
    events: &mut EventIndex,
    bindings: &mut BindingIndex,
    closures: &mut ClosureIndex,
    registration: &Registration,
) {
    let target_candidates = events
        .by_target
        .get(&registration.key)
        .cloned()
        .unwrap_or_default();
    let pending = target_candidates
        .iter()
        .copied()
        .filter(|index| {
            events.active[*index]
                && events.fields[*index].family == SourceFamily::Registry
                && events.fields[*index]
                    .position
                    .is_some_and(|position| position.2 < registration.log_index)
        })
        .collect::<Vec<_>>();
    let pending_positions = pending
        .iter()
        .filter_map(|index| events.fields[*index].position)
        .collect::<BTreeSet<_>>();
    let stale_resources = pending
        .iter()
        .filter(|index| events.fields[**index].registry_only)
        .filter_map(|index| events.fields[*index].resource_id)
        .collect::<BTreeSet<_>>();
    let first_ownership_log_index = pending
        .iter()
        .filter(|index| {
            matches!(
                events.fields[**index].source_event,
                SourceEvent::NewOwner | SourceEvent::Transfer
            )
        })
        .filter_map(|index| events.fields[*index].position.map(|position| position.2))
        .min();
    let owner_positions = pending
        .iter()
        .filter(|index| events.fields[**index].source_event == SourceEvent::NewOwner)
        .filter_map(|index| {
            Some((
                events.fields[*index].position?,
                events.fields[*index].owner.clone()?,
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let last_owner_position = owner_positions.keys().next_back().copied();
    let transient_owner_positions = owner_positions
        .iter()
        .filter(|(position, owner)| {
            Some(**position) != last_owner_position
                && *owner == &registration.emitter
                && events.candidates_at(**position).into_iter().any(|index| {
                    let fields = &events.fields[index];
                    events.active[index]
                        && fields.permission
                        && fields.resource_scope
                        && fields.grant
                        && fields.subject.as_ref() == Some(*owner)
                        && fields
                            .resource_id
                            .is_some_and(|resource| stale_resources.contains(&resource))
                })
        })
        .map(|(position, _)| *position)
        .collect::<BTreeSet<_>>();
    let predecessor_owner_positions = owner_positions
        .keys()
        .filter(|position| {
            Some(**position) != last_owner_position && !transient_owner_positions.contains(position)
        })
        .copied()
        .collect::<BTreeSet<_>>();
    let transient_owners = transient_owner_positions
        .iter()
        .filter_map(|position| owner_positions.get(position).cloned())
        .collect::<BTreeSet<_>>();

    remove_transient_events(
        events,
        &pending_positions,
        &stale_resources,
        &transient_owner_positions,
        &transient_owners,
    );
    let setup_revocations = setup_revocations(events, &pending_positions, &stale_resources);
    // Revocations that close preceding registry-only permission grants, plus those grants, remain
    // attached to the preceding resource. Other incoming grants may move to the registration.
    // Attachment never reaches across a block boundary: the block is the atomic unit every batch
    // grid loads, while a registration from a later block cannot identify an observation in every
    // run shape.
    let predecessor_events = predecessor_candidates(
        events,
        registration,
        &stale_resources,
        &predecessor_owner_positions,
        &setup_revocations,
    );
    for index in predecessor_events {
        let event = &mut output.normalized_events[index];
        event.logical_name_id = Some(registration.logical_name_id.clone());
        refresh_interpreter_state_key(event);
    }

    let retarget_candidates = retarget_candidates(events, &target_candidates, &pending_positions);
    for index in retarget_candidates {
        // `wrapETH2LD` emits `NameWrapped` before its resolver write; a later controller
        // registration sets its registry record before the controller emits `NameRegistered`.
        // When those calls share a transaction, the wrapper-materialized pointer remains on the
        // dormant registry read resource instead of moving to registrar control.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1019 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L287-L341 @ ens_v1@91c966f)
        let preserves_registry_read_pointer = output.normalized_events[index].event_kind
            == "ResolverChanged"
            && output.normalized_events[index].after_state["state_derived"] == true
            && output.normalized_events[index].after_state["surface_materialization"] == true
            && output.normalized_events[index].after_state["source_event"] == "NameWrapped";
        if preserves_registry_read_pointer {
            continue;
        }
        if concerns_predecessor_epoch(
            &events.fields[index],
            &stale_resources,
            &predecessor_owner_positions,
            &setup_revocations,
        ) {
            continue;
        }
        let fields = &events.fields[index];
        let targets_registry = fields.family == SourceFamily::Registry
            && fields
                .position
                .is_some_and(|position| position.2 < registration.log_index)
            && target_candidates.binary_search(&index).is_ok();
        let targets_resolver = fields.family == SourceFamily::Resolver
            && fields
                .resource_id
                .is_none_or(|resource| stale_resources.contains(&resource))
            && fields.position.is_some_and(|position| {
                position.2 < registration.log_index
                    // Resolver retargeting starts strictly after the first qualifying ownership
                    // setup, preserving records written before the incoming authority exists.
                    && first_ownership_log_index
                        .is_some_and(|first_log_index| position.2 > first_log_index)
            })
            && target_candidates.binary_search(&index).is_ok();
        let references_pending_resource = fields
            .position
            .is_some_and(|position| pending_positions.contains(&position))
            && fields
                .resource_id
                .is_some_and(|resource| stale_resources.contains(&resource));
        if !(targets_registry || targets_resolver || references_pending_resource) {
            continue;
        }
        let event = &mut output.normalized_events[index];
        event.logical_name_id = Some(registration.logical_name_id.clone());
        event.resource_id = Some(registration.resource_id);
        if let Some(authority_key) = registration.authority_key.as_deref() {
            retarget_permission_authority(&mut event.after_state, authority_key);
        }
        if let Some(state) = event.after_state.as_object_mut() {
            state.remove("authority_kind");
            state.remove("authority_key");
        }
        refresh_interpreter_state_key(event);
        // Stream-chained befores are re-derived from the surviving rows after reconciliation;
        // this blank only survives on interpreter-declared explicit befores, which the re-thread
        // leaves alone.
        event.before_state = serde_json::json!({});
        events.fields[index].registry_only = false;
        events.update_resource(index, registration.resource_id);
    }
    bindings.remove(&stale_resources, &pending_positions);
    closures.remove(&registration.logical_name_id, &pending_positions);
}

fn remove_transient_events(
    events: &mut EventIndex,
    pending_positions: &BTreeSet<Position>,
    stale_resources: &BTreeSet<Uuid>,
    transient_positions: &BTreeSet<Position>,
    transient_owners: &BTreeSet<String>,
) {
    for position in pending_positions {
        for index in events.candidates_at(*position) {
            let fields = &events.fields[index];
            if events.active[index]
                && fields.permission
                && fields
                    .resource_id
                    .is_some_and(|resource| stale_resources.contains(&resource))
                && fields
                    .subject
                    .as_ref()
                    .is_some_and(|subject| transient_owners.contains(subject))
            {
                events.active[index] = false;
            }
        }
    }
    for position in transient_positions {
        for index in events.candidates_at(*position) {
            let fields = &events.fields[index];
            if events.active[index]
                && !fields.permission
                && fields
                    .resource_id
                    .is_some_and(|resource| stale_resources.contains(&resource))
            {
                events.active[index] = false;
            }
        }
    }
}

fn setup_revocations(
    events: &EventIndex,
    pending_positions: &BTreeSet<Position>,
    stale_resources: &BTreeSet<Uuid>,
) -> Vec<PermissionRevocation> {
    let mut revocations = Vec::new();
    for position in pending_positions {
        for index in events.candidates_at(*position) {
            let fields = &events.fields[index];
            let Some((resource_id, subject, scope)) = fields
                .resource_id
                .zip(fields.subject.clone())
                .zip(fields.scope.clone())
                .map(|((resource_id, subject), scope)| (resource_id, subject, scope))
            else {
                continue;
            };
            if events.active[index]
                && fields.permission
                && fields.revocation
                && stale_resources.contains(&resource_id)
            {
                revocations.push(PermissionRevocation {
                    resource_id,
                    subject,
                    scope,
                    position: *position,
                });
            }
        }
    }
    revocations
}

fn predecessor_candidates(
    events: &EventIndex,
    registration: &Registration,
    stale_resources: &BTreeSet<Uuid>,
    predecessor_positions: &BTreeSet<Position>,
    setup_revocations: &[PermissionRevocation],
) -> Vec<usize> {
    let mut candidates = predecessor_positions
        .iter()
        .flat_map(|position| events.candidates_at(*position))
        .collect::<Vec<_>>();
    for resource_id in stale_resources {
        candidates.extend(
            events
                .by_resource
                .get(resource_id)
                .into_iter()
                .flatten()
                .filter(|index| {
                    events.fields[**index]
                        .position
                        .is_some_and(|position| position.0 == registration.position.0)
                }),
        );
    }
    sort_unique(&mut candidates);
    candidates.retain(|index| {
        events.active[*index]
            && concerns_predecessor_epoch(
                &events.fields[*index],
                stale_resources,
                predecessor_positions,
                setup_revocations,
            )
    });
    candidates
}

fn concerns_predecessor_epoch(
    fields: &EventFields,
    stale_resources: &BTreeSet<Uuid>,
    predecessor_positions: &BTreeSet<Position>,
    setup_revocations: &[PermissionRevocation],
) -> bool {
    if !fields.permission {
        return fields
            .position
            .is_some_and(|position| predecessor_positions.contains(&position));
    }
    let Some(resource_id) = fields
        .resource_id
        .filter(|resource| stale_resources.contains(resource))
    else {
        return false;
    };
    if fields.revocation {
        return true;
    }
    let (Some(position), Some(subject), Some(scope)) = (
        fields.position,
        fields.subject.as_ref(),
        fields.scope.as_ref(),
    ) else {
        return false;
    };
    fields.grant
        && setup_revocations.iter().any(|revocation| {
            revocation.resource_id == resource_id
                && &revocation.subject == subject
                && &revocation.scope == scope
                && revocation.position > position
        })
}

fn retarget_candidates(
    events: &EventIndex,
    target_candidates: &[usize],
    pending_positions: &BTreeSet<Position>,
) -> Vec<usize> {
    let mut candidates = target_candidates.to_vec();
    for position in pending_positions {
        candidates.extend(events.candidates_at(*position));
    }
    sort_unique(&mut candidates);
    candidates.retain(|index| events.active[*index]);
    candidates
}

fn sort_unique(values: &mut Vec<usize>) {
    values.sort_unstable();
    values.dedup();
}

fn retain_by_flags<T>(values: &mut Vec<T>, active: &[bool]) {
    let mut index = 0;
    values.retain(|_| {
        let keep = active[index];
        index += 1;
        keep
    });
}
