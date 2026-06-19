use bigname_storage::sql_row;
use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_manifests::load_watched_contracts;
use sqlx::{PgPool, Row, types::Uuid};

use crate::adapter_manifest::{
    ActiveManifestMetadata, active_manifest_for_watched_contract,
    ensure_watched_contract_manifest_chain, load_active_manifest_metadata,
    load_latest_active_manifest_metadata_for_source_family, watched_contract_manifest_ids,
};
pub(crate) use crate::evm_abi::{keccak256_bytes, keccak256_hex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveEmitter {
    pub(crate) address: String,
    pub(crate) contract_instance_id: Uuid,
    pub(crate) source_manifest_id: i64,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) manifest_version: i64,
    pub(crate) active_from_block_number: Option<i64>,
    pub(crate) active_to_block_number: Option<i64>,
}

/// Identifies one distinct discovery/watched interval for an emitting address:
/// `(address, active_from_block_number, active_to_block_number)`. Two rows that share this key are
/// the same interval reported by different sources and are deduplicated; rows that differ are kept
/// as separate [`ActiveEmitter`]s so disjoint windows survive (see [`insert_distinct_emitter`]).
type EmitterScopeKey = (String, Option<i64>, Option<i64>);

pub(crate) fn source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
    source_family_filter: &str,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != source_family_filter {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}

/// Record each emitting address's discovery/watched intervals as DISTINCT emitters — without
/// collapsing them. A discovered resolver carries one discovery edge per (name, registry) that
/// referenced it, and those edges can be disjoint in block space (a name points at the resolver,
/// drops it, and a different name points at it again later). Keeping each edge as its own
/// [`ActiveEmitter`] lets [`active_emitter_for_block`] match the interval that actually covers a
/// log — and skip blocks that fall in the GAPS between edges — while still honouring every edge
/// (the bug this guards against was keeping a single arbitrary edge, which dropped logs covered
/// only by the others). Two sources reporting the identical interval are deduplicated, preferring
/// the lower `(source_manifest_id, contract_instance_id)` for determinism. Collapsing to a hull
/// (`min(from)..max(to)`) instead would admit gap blocks and stamp them with the wrong edge's
/// identity.
///
/// This mirrors the *keying* of `ens_v2_registry::preferred_emitters_by_scope`
/// (`(source_family, address, active_from, active_to)`); it deliberately does NOT match its
/// selection bound or tie-break. The registry selects with an inclusive upper bound and breaks
/// ties by `source_rank`; here selection is exclusive (`active_emitter_for_block`, correct for
/// discovery-edge `active_to`) and the dedup tie-break is plain id ordering — `ActiveEmitter` here
/// carries no `source_rank`, and a collision only happens on an exact identical interval.
fn insert_distinct_emitter(
    emitters_by_scope: &mut HashMap<EmitterScopeKey, ActiveEmitter>,
    emitter: ActiveEmitter,
) {
    use std::collections::hash_map::Entry;
    let scope_key = (
        emitter.address.clone(),
        emitter.active_from_block_number,
        emitter.active_to_block_number,
    );
    match emitters_by_scope.entry(scope_key) {
        Entry::Vacant(entry) => {
            entry.insert(emitter);
        }
        Entry::Occupied(mut entry) => {
            let existing = entry.get_mut();
            if (emitter.source_manifest_id, emitter.contract_instance_id)
                < (existing.source_manifest_id, existing.contract_instance_id)
            {
                *existing = emitter;
            }
        }
    }
}

pub(crate) fn emitters_by_address(
    emitters: &[ActiveEmitter],
) -> HashMap<String, Vec<ActiveEmitter>> {
    let mut output = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        output
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    output
}

/// Find the interval covering `block_number`, matching the half-open range `[active_from,
/// active_to)`. The upper bound is EXCLUSIVE on purpose and must not be "reconciled" with the
/// inclusive bound used by `ens_v2_registry::emitter_active_at_block`: a superseded discovery
/// edge's `active_to` is written as the successor edge's `active_from` (the repoint block), so two
/// adjacent edges `[from, X)` and `[X, ..)` partition cleanly — block `X` belongs to the successor
/// only. An inclusive bound would match both edges at `X` and double-attribute the repoint log.
pub(crate) fn active_emitter_for_block(
    emitters: &[ActiveEmitter],
    block_number: i64,
) -> Option<&ActiveEmitter> {
    emitters.iter().find(|emitter| {
        emitter
            .active_from_block_number
            .is_none_or(|active_from| block_number >= active_from)
            && emitter
                .active_to_block_number
                .is_none_or(|active_to| block_number < active_to)
    })
}

pub(crate) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    resolver_edge_kind: &str,
    adapter_label: &str,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .with_context(|| format!("failed to load watched contracts for {adapter_label} adapter"))?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let context_label = format!("{adapter_label} emitters");
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, &context_label).await?;

    let mut emitters_by_scope = HashMap::<EmitterScopeKey, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        if manifest.source_family != source_family {
            continue;
        }
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;

        insert_distinct_emitter(
            &mut emitters_by_scope,
            ActiveEmitter {
                address: watched_contract.address,
                contract_instance_id: watched_contract.contract_instance_id,
                source_manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: watched_contract.active_from_block_number,
                active_to_block_number: watched_contract.active_to_block_number,
            },
        );
    }
    if let Some(manifest) =
        load_active_source_family_manifest_metadata(pool, chain, source_family).await?
    {
        for emitter in
            load_discovered_resolver_emitters(pool, chain, resolver_edge_kind, &manifest).await?
        {
            insert_distinct_emitter(&mut emitters_by_scope, emitter);
        }
    }

    let mut emitters = emitters_by_scope.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

async fn load_discovered_resolver_emitters(
    pool: &PgPool,
    chain: &str,
    resolver_edge_kind: &str,
    manifest: &ActiveManifestMetadata,
) -> Result<Vec<ActiveEmitter>> {
    let rows = sqlx::query(
        r#"
        SELECT
            cia.address,
            de.to_contract_instance_id,
            de.active_from_block_number,
            de.active_to_block_number
        FROM discovery_edges de
        JOIN manifest_versions source_mv
          ON source_mv.manifest_id = de.source_manifest_id
         AND source_mv.rollout_status = 'active'
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
        WHERE de.chain_id = $1
          AND de.edge_kind = $2
        ORDER BY lower(cia.address), de.active_from_block_number NULLS FIRST, de.discovery_edge_id
        "#,
    )
    .bind(chain)
    .bind(resolver_edge_kind)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 discovered resolver emitters for {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let address = normalize_address(
                &row.try_get::<String, _>("address")
                    .context("missing discovered resolver address")?,
            );
            Ok(ActiveEmitter {
                address,
                contract_instance_id: row
                    .try_get("to_contract_instance_id")
                    .context("missing discovered resolver contract_instance_id")?,
                source_manifest_id: manifest.manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: sql_row::get(&row, "active_from_block_number")?,
                active_to_block_number: sql_row::get(&row, "active_to_block_number")?,
            })
        })
        .collect()
}

async fn load_active_source_family_manifest_metadata(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<Option<ActiveManifestMetadata>> {
    load_latest_active_manifest_metadata_for_source_family(
        pool,
        chain,
        source_family,
        "active ENSv2 resolver manifest",
    )
    .await
}

pub(crate) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(crate) fn dns_decode_optional(bytes: &[u8]) -> Result<Option<String>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        dns_decode(bytes).map(Some)
    }
}

pub(crate) fn dns_decode(bytes: &[u8]) -> Result<String> {
    bigname_domain::normalization::normalize_dns_encoded_name(bytes)
        .map(|name| name.normalized_name)
        .map_err(anyhow::Error::from)
}

pub(crate) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    crate::evm_abi::hex_string_without_prefix(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emitter_with(
        address: &str,
        instance: u128,
        source_manifest_id: i64,
        active_from: Option<i64>,
        active_to: Option<i64>,
    ) -> ActiveEmitter {
        ActiveEmitter {
            address: address.to_owned(),
            contract_instance_id: Uuid::from_u128(instance),
            source_manifest_id,
            namespace: "ens".to_owned(),
            source_family: "ens_v2_resolver_l1".to_owned(),
            manifest_version: 1,
            active_from_block_number: active_from,
            active_to_block_number: active_to,
        }
    }

    fn emitter(active_from: Option<i64>, active_to: Option<i64>) -> ActiveEmitter {
        emitter_with("0xresolver", 1, 1, active_from, active_to)
    }

    #[test]
    fn insert_distinct_emitter_preserves_disjoint_intervals_per_address() {
        let mut by_scope = HashMap::new();

        // Two genuinely disjoint discovery edges for the same resolver address, discovered in
        // arbitrary order. They must NOT collapse into one envelope [100, 600): the gap [200, 500)
        // is covered by neither edge, and each edge keeps its own manifest/instance identity.
        insert_distinct_emitter(
            &mut by_scope,
            emitter_with("0xresolver", 2, 2, Some(500), Some(600)),
        );
        insert_distinct_emitter(
            &mut by_scope,
            emitter_with("0xresolver", 1, 1, Some(100), Some(200)),
        );
        // The same interval reported by a second source deduplicates, keeping the lower
        // (source_manifest_id, contract_instance_id).
        insert_distinct_emitter(
            &mut by_scope,
            emitter_with("0xresolver", 9, 9, Some(100), Some(200)),
        );
        assert_eq!(by_scope.len(), 2);

        let emitters = by_scope.into_values().collect::<Vec<_>>();
        let by_address = emitters_by_address(&emitters);
        let resolver = by_address.get("0xresolver").expect("address present");

        // Inside either edge: covered, and attributed to THAT edge's identity (not the first).
        let first = active_emitter_for_block(resolver, 150).expect("first edge covers 150");
        assert_eq!(first.active_from_block_number, Some(100));
        assert_eq!(first.source_manifest_id, 1);
        assert_eq!(first.contract_instance_id, Uuid::from_u128(1));
        let second = active_emitter_for_block(resolver, 550).expect("second edge covers 550");
        assert_eq!(second.active_from_block_number, Some(500));
        assert_eq!(second.source_manifest_id, 2);
        assert_eq!(second.contract_instance_id, Uuid::from_u128(2));

        // In the gap between the disjoint edges, and outside both: NOT covered. The old envelope
        // (hull) bug admitted block 300 here and mis-attributed it to the first edge's identity.
        assert!(active_emitter_for_block(resolver, 300).is_none());
        assert!(active_emitter_for_block(resolver, 50).is_none());
        assert!(active_emitter_for_block(resolver, 700).is_none());
    }

    #[test]
    fn active_emitter_for_block_matches_the_covering_interval() {
        // A single open-ended edge covers from its activation block onward; exclusive upper bound.
        let open = [emitter(Some(10_696_215), None)];
        assert!(active_emitter_for_block(&open, 10_696_215).is_some());
        assert!(active_emitter_for_block(&open, 10_704_585).is_some());
        assert!(active_emitter_for_block(&open, 10_696_214).is_none());
    }

    #[test]
    fn adjacent_edges_attribute_the_boundary_block_to_the_successor() {
        // A superseded edge's active_to is the successor's active_from (the repoint block), so two
        // touching edges [100, 200) and [200, 300) partition cleanly: block 200 belongs to the
        // successor only, with no block falling through a crack. This is the case the exclusive
        // upper bound exists for.
        let emitters = vec![
            emitter_with("0xresolver", 1, 1, Some(100), Some(200)),
            emitter_with("0xresolver", 2, 2, Some(200), Some(300)),
        ];
        let by_address = emitters_by_address(&emitters);
        let resolver = by_address.get("0xresolver").expect("address present");

        assert_eq!(
            active_emitter_for_block(resolver, 199)
                .expect("predecessor covers 199")
                .source_manifest_id,
            1
        );
        let boundary = active_emitter_for_block(resolver, 200).expect("successor covers 200");
        assert_eq!(boundary.source_manifest_id, 2);
        assert_eq!(boundary.active_from_block_number, Some(200));
    }

    #[test]
    fn overlapping_edges_select_the_earliest_activating_interval() {
        // Two genuinely overlapping intervals for one address with distinct identity. Given the
        // loader's active_from-asc ordering, active_emitter_for_block's first match is the
        // earliest-activating edge — a stable, order-independent choice for a block in the overlap.
        let mut emitters = vec![
            emitter_with("0xresolver", 2, 2, Some(200), Some(600)),
            emitter_with("0xresolver", 1, 1, Some(100), Some(400)),
        ];
        emitters.sort_by(|left, right| {
            left.active_from_block_number
                .cmp(&right.active_from_block_number)
                .then(
                    left.active_to_block_number
                        .cmp(&right.active_to_block_number),
                )
                .then(left.source_manifest_id.cmp(&right.source_manifest_id))
        });
        let by_address = emitters_by_address(&emitters);
        let resolver = by_address.get("0xresolver").expect("address present");

        // Block 300 is inside both [100, 400) and [200, 600); the earliest-activating edge wins.
        let covering = active_emitter_for_block(resolver, 300).expect("overlap covered");
        assert_eq!(covering.active_from_block_number, Some(100));
        assert_eq!(covering.source_manifest_id, 1);
    }

    #[test]
    fn insert_distinct_emitter_dedup_prefers_lower_identity_across_sources() {
        // The same interval reported by two different sources (a watched-contract row and a
        // discovered-resolver row carry different source_manifest_id) dedups to one emitter,
        // keeping the lower (source_manifest_id, contract_instance_id).
        let mut by_scope = HashMap::new();
        insert_distinct_emitter(
            &mut by_scope,
            emitter_with("0xresolver", 7, 5, Some(100), Some(200)),
        );
        insert_distinct_emitter(
            &mut by_scope,
            emitter_with("0xresolver", 3, 2, Some(100), Some(200)),
        );
        assert_eq!(by_scope.len(), 1);
        let kept = by_scope.values().next().expect("one emitter");
        assert_eq!(kept.source_manifest_id, 2);
        assert_eq!(kept.contract_instance_id, Uuid::from_u128(3));
    }
}
