use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use super::types::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation, AddressNamesCurrentDedupe,
};

/// Collapse relation rows into stable storage-local collection representatives.
pub fn collapse_address_name_current_rows(
    rows: &[AddressNameCurrentRow],
    dedupe_by: AddressNamesCurrentDedupe,
) -> Vec<AddressNameCurrentEntry> {
    let mut groups = BTreeMap::<AddressNameGroupKey, GroupAccumulator>::new();

    for row in rows {
        let group_key = match dedupe_by {
            AddressNamesCurrentDedupe::Surface => AddressNameGroupKey::Surface {
                address: row.address.clone(),
                logical_name_id: row.logical_name_id.clone(),
            },
            AddressNamesCurrentDedupe::Resource => AddressNameGroupKey::Resource {
                address: row.address.clone(),
                resource_id: row.resource_id.to_string(),
            },
        };

        match groups.get_mut(&group_key) {
            Some(group) => {
                group.relations.insert(row.relation);
                if compare_row_sort_key(row, &group.representative) == Ordering::Less {
                    group.representative = row.clone();
                }
            }
            None => {
                groups.insert(
                    group_key,
                    GroupAccumulator {
                        representative: row.clone(),
                        relations: BTreeSet::from([row.relation]),
                    },
                );
            }
        }
    }

    let mut entries = groups
        .into_values()
        .map(|group| {
            let representative = group.representative;
            AddressNameCurrentEntry {
                address: representative.address,
                logical_name_id: representative.logical_name_id,
                namespace: representative.namespace,
                canonical_display_name: representative.canonical_display_name,
                normalized_name: representative.normalized_name,
                namehash: representative.namehash,
                surface_binding_id: representative.surface_binding_id,
                resource_id: representative.resource_id,
                token_lineage_id: representative.token_lineage_id,
                binding_kind: representative.binding_kind,
                relations: group.relations.into_iter().collect(),
                provenance: representative.provenance,
                coverage: representative.coverage,
                chain_positions: representative.chain_positions,
                canonicality_summary: representative.canonicality_summary,
                manifest_version: representative.manifest_version,
                last_recomputed_at: representative.last_recomputed_at,
            }
        })
        .collect::<Vec<_>>();

    entries.sort_by(compare_entry_sort_key);
    entries
}

fn compare_row_sort_key(left: &AddressNameCurrentRow, right: &AddressNameCurrentRow) -> Ordering {
    left.address
        .cmp(&right.address)
        .then_with(|| {
            left.canonical_display_name
                .cmp(&right.canonical_display_name)
        })
        .then_with(|| left.logical_name_id.cmp(&right.logical_name_id))
        .then_with(|| left.relation.sort_rank().cmp(&right.relation.sort_rank()))
}

fn compare_entry_sort_key(
    left: &AddressNameCurrentEntry,
    right: &AddressNameCurrentEntry,
) -> Ordering {
    left.address
        .cmp(&right.address)
        .then_with(|| {
            left.canonical_display_name
                .cmp(&right.canonical_display_name)
        })
        .then_with(|| left.logical_name_id.cmp(&right.logical_name_id))
        .then_with(|| {
            left.resource_id
                .to_string()
                .cmp(&right.resource_id.to_string())
        })
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum AddressNameGroupKey {
    Surface {
        address: String,
        logical_name_id: String,
    },
    Resource {
        address: String,
        resource_id: String,
    },
}

#[derive(Clone, Debug)]
struct GroupAccumulator {
    representative: AddressNameCurrentRow,
    relations: BTreeSet<AddressNameRelation>,
}
