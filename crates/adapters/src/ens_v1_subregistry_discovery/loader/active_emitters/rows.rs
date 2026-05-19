use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::super::super::{
    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY, ENS_V1_REGISTRY_SOURCE_FAMILY,
    hex_topic::normalize_address, scope::RegistryRawLogSourceScopeTarget,
};
use super::super::ActiveEmitter;

pub(super) fn active_emitters_from_rows(rows: Vec<PgRow>) -> Result<Vec<ActiveEmitter>> {
    let mut emitters_by_scope =
        HashMap::<(String, String, Option<i64>, Option<i64>), ActiveEmitter>::new();
    for row in rows {
        let address = normalize_address(&row.try_get::<String, _>("address")?);
        let candidate = ActiveEmitter {
            address,
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("missing registry emitter contract_instance_id")?,
            source_manifest_id: row
                .try_get("source_manifest_id")
                .context("missing registry emitter source_manifest_id")?,
            namespace: row
                .try_get("namespace")
                .context("missing registry emitter namespace")?,
            source_family: row
                .try_get("source_family")
                .context("missing registry emitter source_family")?,
            manifest_version: row
                .try_get("manifest_version")
                .context("missing registry emitter manifest_version")?,
            contract_role: row
                .try_get("contract_role")
                .context("missing registry emitter contract_role")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("missing registry emitter active_from_block_number")?,
            active_to_block_number: row
                .try_get("active_to_block_number")
                .context("missing registry emitter active_to_block_number")?,
            source_rank: row
                .try_get("source_rank")
                .context("missing registry emitter source_rank")?,
        };

        let scope_key = (
            candidate.source_family.clone(),
            candidate.address.clone(),
            candidate.active_from_block_number,
            candidate.active_to_block_number,
        );
        match emitters_by_scope.get(&scope_key) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_scope.insert(scope_key, candidate);
            }
        }
    }

    let mut emitters = emitters_by_scope.into_values().collect::<Vec<_>>();
    sort_active_emitters(&mut emitters);
    let mut emitters = remove_shadowed_emitters(emitters);
    sort_active_emitters(&mut emitters);
    Ok(emitters)
}

pub(super) fn sort_active_emitters(emitters: &mut [ActiveEmitter]) {
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_family.cmp(&right.source_family))
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
}

pub(super) fn source_scope_covered_by_emitters(
    source_scope: &[RegistryRawLogSourceScopeTarget],
    emitters: &[ActiveEmitter],
) -> bool {
    source_scope
        .iter()
        .filter(|target| {
            target.source_family == ENS_V1_REGISTRY_SOURCE_FAMILY
                || target.source_family == BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
        })
        .all(|target| {
            emitters.iter().any(|emitter| {
                target.source_family == emitter.source_family
                    && target.address == emitter.address
                    && source_scope_target_intersects_emitter(target, emitter)
            })
        })
}

fn source_scope_target_intersects_emitter(
    target: &RegistryRawLogSourceScopeTarget,
    emitter: &ActiveEmitter,
) -> bool {
    let emitter_from = emitter.active_from_block_number.unwrap_or(0);
    let emitter_to = emitter.active_to_block_number.unwrap_or(i64::MAX);
    target.effective_from_block <= emitter_to && emitter_from <= target.effective_to_block
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (
        candidate.source_rank,
        candidate.source_manifest_id,
        candidate.contract_instance_id,
    ) < (
        current.source_rank,
        current.source_manifest_id,
        current.contract_instance_id,
    )
}

fn remove_shadowed_emitters(emitters: Vec<ActiveEmitter>) -> Vec<ActiveEmitter> {
    let mut emitters_by_address = HashMap::<(String, String), Vec<ActiveEmitter>>::new();
    for emitter in emitters {
        emitters_by_address
            .entry((emitter.address.clone(), emitter.source_family.clone()))
            .or_default()
            .push(emitter);
    }

    let mut compacted = Vec::new();
    for (_, mut address_emitters) in emitters_by_address {
        sort_active_emitters(&mut address_emitters);
        let mut retained = Vec::<ActiveEmitter>::new();
        'candidate: for candidate in address_emitters {
            for retained_emitter in &retained {
                if emitter_shadows(retained_emitter, &candidate) {
                    continue 'candidate;
                }
            }
            retained.push(candidate);
        }
        compacted.extend(retained);
    }
    compacted
}

fn emitter_shadows(retained: &ActiveEmitter, candidate: &ActiveEmitter) -> bool {
    retained.address == candidate.address
        && retained.source_family == candidate.source_family
        && range_covers(retained, candidate)
        && emitter_precedes_or_ties(retained, candidate)
        && (emitter_precedes(retained, candidate)
            || retained.contract_role == candidate.contract_role)
}

fn emitter_precedes(left: &ActiveEmitter, right: &ActiveEmitter) -> bool {
    emitter_precedence(left) < emitter_precedence(right)
}

fn emitter_precedes_or_ties(left: &ActiveEmitter, right: &ActiveEmitter) -> bool {
    emitter_precedence(left) <= emitter_precedence(right)
}

fn emitter_precedence(emitter: &ActiveEmitter) -> (i32, i64, sqlx::types::Uuid) {
    (
        emitter.source_rank,
        emitter.source_manifest_id,
        emitter.contract_instance_id,
    )
}

fn range_covers(retained: &ActiveEmitter, candidate: &ActiveEmitter) -> bool {
    range_start(retained) <= range_start(candidate) && range_end(retained) >= range_end(candidate)
}

fn range_start(emitter: &ActiveEmitter) -> i64 {
    emitter.active_from_block_number.unwrap_or(i64::MIN)
}

fn range_end(emitter: &ActiveEmitter) -> i64 {
    emitter.active_to_block_number.unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use sqlx::types::Uuid;

    use super::*;

    fn emitter(
        address: &str,
        role: Option<&str>,
        active_from_block_number: Option<i64>,
        active_to_block_number: Option<i64>,
        source_rank: i32,
    ) -> ActiveEmitter {
        ActiveEmitter {
            address: address.to_owned(),
            contract_instance_id: Uuid::nil(),
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: ENS_V1_REGISTRY_SOURCE_FAMILY.to_owned(),
            manifest_version: 1,
            contract_role: role.map(str::to_owned),
            active_from_block_number,
            active_to_block_number,
            source_rank,
        }
    }

    #[test]
    fn shadowed_emitters_do_not_keep_covered_lower_precedence_ranges() {
        let root = emitter("0xabc", Some("registry"), Some(1), None, 0);
        let discovered = emitter("0xabc", Some("registry"), Some(10), None, 2);

        let compacted = remove_shadowed_emitters(vec![discovered, root.clone()]);

        assert_eq!(compacted, vec![root]);
    }

    #[test]
    fn shadowed_emitters_keep_ranges_with_uncovered_blocks() {
        let early = emitter("0xabc", Some("subregistry"), Some(1), None, 2);
        let later_better = emitter("0xabc", Some("registry"), Some(10), None, 0);

        let compacted = remove_shadowed_emitters(vec![early.clone(), later_better.clone()]);

        assert_eq!(compacted.len(), 2);
        assert!(compacted.contains(&early));
        assert!(compacted.contains(&later_better));
    }
}
