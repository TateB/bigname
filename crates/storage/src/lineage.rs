mod canonicality;
mod decode;
mod orphaning;
mod reads;
mod types;
mod upserts;
mod validation;

pub use orphaning::mark_chain_lineage_range_orphaned;
pub use reads::load_chain_lineage_block;
pub use types::{CanonicalityState, ChainLineageBlock};
pub use upserts::upsert_chain_lineage_blocks;

pub(crate) use canonicality::promote_chain_lineage_path;
pub(crate) use reads::ensure_chain_lineage_block;

#[cfg(test)]
mod tests;
