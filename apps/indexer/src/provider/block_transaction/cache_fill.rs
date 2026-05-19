use anyhow::{Context, Result, bail};
use bigname_storage::{RawPayloadCacheDigestVerification, verify_raw_payload_cache_digest};
use serde_json::Value;

use super::super::{
    JsonRpcProvider, ProviderBlock, RAW_PAYLOAD_KIND_FULL_BLOCK, decode::normalize_hash,
};

impl JsonRpcProvider {
    #[allow(dead_code, reason = "staged cache-fill helper covered by tests")]
    pub async fn cache_fill_full_block_by_hash(
        &self,
        pool: &sqlx::PgPool,
        chain: &str,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<ProviderBlock> {
        if expected_block_number < 0 {
            bail!("provider cache-fill expected block number cannot be negative");
        }

        let block_hash = normalize_hash(block_hash);
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getBlockByHash",
                vec![Value::String(block_hash.clone()), Value::Bool(true)],
            )
            .await?
            .with_cache_metadata(
                RAW_PAYLOAD_KIND_FULL_BLOCK,
                "eth_getBlockByHash",
                "block_hash",
            );

        verify_raw_payload_cache_digest(
            pool,
            &RawPayloadCacheDigestVerification {
                chain_id: chain.to_owned(),
                block_hash: block_hash.clone(),
                payload_kind: RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
                digest_algorithm: payload.cache_metadata.digest_algorithm.clone(),
                candidate_digest: payload.cache_metadata.retained_digest.clone(),
                payload_size_bytes: payload.cache_metadata.payload_size_bytes,
            },
        )
        .await?;

        let block = ProviderBlock::from_value(
            payload
                .result
                .context("provider cache-fill returned null full block payload")?,
        )?;
        if block.block_hash != block_hash {
            bail!(
                "provider cache-fill returned block {} for requested hash {}",
                block.block_hash,
                block_hash
            );
        }
        if block.block_number != expected_block_number {
            bail!(
                "provider cache-fill returned block {} for requested hash {} with block number {}; expected {}",
                block.block_hash,
                block_hash,
                block.block_number,
                expected_block_number
            );
        }

        Ok(block)
    }
}
