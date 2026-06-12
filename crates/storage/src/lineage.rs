mod canonicality;
mod decode;
mod orphaning;
mod reads;
mod types;
mod upserts;
mod validation;

pub use orphaning::mark_chain_lineage_range_orphaned;
pub use reads::{chain_lineage_contains_ancestor, load_chain_lineage_block};
pub use types::{CanonicalityState, ChainLineageBlock};
pub use upserts::{
    upsert_chain_lineage_blocks, upsert_chain_lineage_blocks_recanonicalizing_orphaned,
    upsert_chain_lineage_blocks_without_snapshots,
    upsert_chain_lineage_blocks_without_snapshots_recanonicalizing_orphaned,
};

pub(crate) use canonicality::promote_chain_lineage_path;
pub(crate) use reads::{chain_lineage_contains_ancestor_internal, ensure_chain_lineage_block};

#[cfg(test)]
mod tests;
