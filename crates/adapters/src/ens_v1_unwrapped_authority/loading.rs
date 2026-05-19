use super::*;

mod active_emitters;
mod blocks;
mod raw_logs;

pub(super) use active_emitters::{load_active_emitters, load_generic_resolver_event_sources};
pub(super) use blocks::{
    load_canonical_blocks, load_canonical_blocks_for_restricted_authority_sync,
};
pub(super) use raw_logs::{
    AuthorityRawLogStreamSourceRouter, load_authority_raw_logs,
    select_authority_raw_log_stream_to_block, stream_authority_raw_logs,
};

impl AuthorityRawLogRow {
    pub(super) fn reference(&self) -> ObservationRef {
        ObservationRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            transaction_hash: Some(self.transaction_hash.clone()),
            transaction_index: Some(self.transaction_index),
            log_index: Some(self.log_index),
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}

impl CanonicalBlockIndex {
    pub(super) fn first_block_at_or_after(
        &self,
        timestamp: OffsetDateTime,
        namespace: &str,
    ) -> Option<BoundaryRef> {
        let index = self
            .blocks
            .partition_point(|block| block.block_timestamp < timestamp);
        self.blocks.get(index).map(|block| BoundaryRef {
            chain_id: block.chain_id.clone(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            block_timestamp: block.block_timestamp,
            canonicality_state: block.canonicality_state,
            namespace: namespace.to_owned(),
        })
    }
}
