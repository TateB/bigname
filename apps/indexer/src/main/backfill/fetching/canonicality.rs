use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, CheckpointBlockRef, chain_lineage_contains_ancestor,
    load_chain_checkpoint,
};

use crate::provider::{ChainProviderOps, ProviderBlock, ProviderHeadSnapshot};

const MAX_STORED_CHECKPOINT_ANCESTRY_DISTANCE: i64 = 1_024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BackfillCanonicalityEvidence {
    provider_canonical: Option<CheckpointBlockRef>,
    provider_safe: Option<CheckpointBlockRef>,
    provider_finalized: Option<CheckpointBlockRef>,
    stored_canonical: Option<CheckpointBlockRef>,
    stored_safe: Option<CheckpointBlockRef>,
    stored_finalized: Option<CheckpointBlockRef>,
}

impl BackfillCanonicalityEvidence {
    fn from_heads(heads: &ProviderHeadSnapshot) -> Self {
        Self {
            provider_canonical: Some(checkpoint_ref_from_provider_block(&heads.canonical)),
            provider_safe: heads.safe.as_ref().map(checkpoint_ref_from_provider_block),
            provider_finalized: heads
                .finalized
                .as_ref()
                .map(checkpoint_ref_from_provider_block),
            stored_canonical: None,
            stored_safe: None,
            stored_finalized: None,
        }
    }

    fn include_checkpoint(&mut self, checkpoint: Option<&ChainCheckpoint>) {
        let Some(checkpoint) = checkpoint else {
            return;
        };

        self.stored_canonical = checkpoint_ref_from_checkpoint(
            checkpoint.canonical_block_hash.as_ref(),
            checkpoint.canonical_block_number,
        );
        self.stored_safe = checkpoint_ref_from_checkpoint(
            checkpoint.safe_block_hash.as_ref(),
            checkpoint.safe_block_number,
        );
        self.stored_finalized = checkpoint_ref_from_checkpoint(
            checkpoint.finalized_block_hash.as_ref(),
            checkpoint.finalized_block_number,
        );
    }

    pub(crate) async fn states_for_blocks(
        &self,
        pool: &sqlx::PgPool,
        chain: &str,
        provider: &(impl ChainProviderOps + ?Sized),
        blocks: &[ProviderBlock],
    ) -> Result<BTreeMap<String, CanonicalityState>> {
        let mut states = blocks
            .iter()
            .map(|block| (block.block_hash.clone(), CanonicalityState::Observed))
            .collect::<BTreeMap<_, _>>();

        self.apply_revalidated_provider_evidence(provider, blocks, &mut states)
            .await?;
        self.apply_stored_checkpoint_evidence(pool, chain, blocks, &mut states)
            .await?;

        Ok(states)
    }

    async fn apply_stored_checkpoint_evidence(
        &self,
        pool: &sqlx::PgPool,
        chain: &str,
        blocks: &[ProviderBlock],
        states: &mut BTreeMap<String, CanonicalityState>,
    ) -> Result<()> {
        for block in blocks {
            self.apply_stored_anchor(
                pool,
                chain,
                block,
                &self.stored_canonical,
                CanonicalityState::Canonical,
                states,
            )
            .await?;
            self.apply_stored_anchor(
                pool,
                chain,
                block,
                &self.stored_safe,
                CanonicalityState::Safe,
                states,
            )
            .await?;
            self.apply_stored_anchor(
                pool,
                chain,
                block,
                &self.stored_finalized,
                CanonicalityState::Finalized,
                states,
            )
            .await?;
        }

        Ok(())
    }

    async fn apply_stored_anchor(
        &self,
        pool: &sqlx::PgPool,
        chain: &str,
        block: &ProviderBlock,
        anchor: &Option<CheckpointBlockRef>,
        target_state: CanonicalityState,
        states: &mut BTreeMap<String, CanonicalityState>,
    ) -> Result<()> {
        let Some(anchor) = anchor else {
            return Ok(());
        };
        if block.block_number > anchor.block_number {
            return Ok(());
        }
        if anchor.block_number - block.block_number > MAX_STORED_CHECKPOINT_ANCESTRY_DISTANCE {
            return Ok(());
        }
        if states
            .get(&block.block_hash)
            .is_some_and(|state| state.rank() >= target_state.rank())
        {
            return Ok(());
        }

        let proven =
            chain_lineage_contains_ancestor(pool, chain, &anchor.block_hash, &block.block_hash)
                .await?;
        if proven {
            promote_backfill_state(states, &block.block_hash, target_state);
        }

        Ok(())
    }

    async fn apply_revalidated_provider_evidence(
        &self,
        provider: &(impl ChainProviderOps + ?Sized),
        blocks: &[ProviderBlock],
        states: &mut BTreeMap<String, CanonicalityState>,
    ) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let fresh_heads = provider
            .fetch_chain_heads()
            .await
            .context("failed to refresh provider heads before backfill canonicality assignment")?;
        let fresh_evidence = BackfillCanonicalityEvidence::from_heads(&fresh_heads);
        let block_numbers = blocks
            .iter()
            .map(|block| block.block_number)
            .collect::<Vec<_>>();
        let fresh_resolved = provider
            .fetch_block_hashes_by_numbers(&block_numbers)
            .await
            .context("failed to revalidate backfill block hashes before canonicality assignment")?;
        if fresh_resolved.len() != blocks.len() {
            bail!(
                "provider revalidated {} backfill blocks but expected {}",
                fresh_resolved.len(),
                blocks.len()
            );
        }

        for (block, fresh) in blocks.iter().zip(fresh_resolved.iter()) {
            if fresh.block_number != block.block_number || fresh.block_hash != block.block_hash {
                continue;
            }

            let state = fresh_evidence.provider_state_for_revalidated_block(block);
            promote_backfill_state(states, &block.block_hash, state);
        }

        Ok(())
    }

    fn provider_state_for_revalidated_block(&self, block: &ProviderBlock) -> CanonicalityState {
        if self
            .provider_finalized
            .as_ref()
            .is_some_and(|finalized| block.block_number <= finalized.block_number)
        {
            CanonicalityState::Finalized
        } else if self
            .provider_safe
            .as_ref()
            .is_some_and(|safe| block.block_number <= safe.block_number)
        {
            CanonicalityState::Safe
        } else if self
            .provider_canonical
            .as_ref()
            .is_some_and(|canonical| block.block_number <= canonical.block_number)
        {
            CanonicalityState::Canonical
        } else {
            CanonicalityState::Observed
        }
    }
}

pub(crate) async fn load_backfill_canonicality_evidence(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &(impl ChainProviderOps + ?Sized),
) -> Result<BackfillCanonicalityEvidence> {
    let heads = provider.fetch_chain_heads().await.with_context(|| {
        format!("failed to load provider checkpoint evidence for chain {chain}")
    })?;
    let checkpoint = load_chain_checkpoint(pool, chain)
        .await
        .with_context(|| format!("failed to load stored checkpoint evidence for chain {chain}"))?;
    let mut evidence = BackfillCanonicalityEvidence::from_heads(&heads);
    evidence.include_checkpoint(checkpoint.as_ref());

    Ok(evidence)
}

fn checkpoint_ref_from_provider_block(block: &ProviderBlock) -> CheckpointBlockRef {
    CheckpointBlockRef {
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
    }
}

fn checkpoint_ref_from_checkpoint(
    block_hash: Option<&String>,
    block_number: Option<i64>,
) -> Option<CheckpointBlockRef> {
    block_hash
        .zip(block_number)
        .map(|(block_hash, block_number)| CheckpointBlockRef {
            block_hash: block_hash.clone(),
            block_number,
        })
}

fn promote_backfill_state(
    states: &mut BTreeMap<String, CanonicalityState>,
    block_hash: &str,
    target_state: CanonicalityState,
) {
    states
        .entry(block_hash.to_owned())
        .and_modify(|state| {
            if target_state.rank() > state.rank() {
                *state = target_state;
            }
        })
        .or_insert(target_state);
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use sqlx::PgPool;

    use super::*;
    use crate::provider::{
        ProviderBlockBundle, ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations,
        ProviderBlockSelection, ProviderCodeObservation, ProviderLog, ProviderResolvedBlock,
        ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest,
    };

    #[derive(Clone)]
    struct RevalidatingProvider {
        heads: ProviderHeadSnapshot,
        hashes_by_number: BTreeMap<i64, String>,
    }

    impl ChainProviderOps for RevalidatingProvider {
        async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
            Ok(self.heads.clone())
        }

        async fn fetch_block_hashes_by_numbers(
            &self,
            block_numbers: &[i64],
        ) -> Result<Vec<ProviderResolvedBlock>> {
            block_numbers
                .iter()
                .map(|block_number| {
                    self.hashes_by_number
                        .get(block_number)
                        .cloned()
                        .map(|block_hash| ProviderResolvedBlock {
                            block_number: *block_number,
                            block_hash,
                        })
                        .ok_or_else(|| {
                            anyhow::anyhow!("missing block number {block_number} in test provider")
                        })
                })
                .collect()
        }

        async fn fetch_block_by_hash(&self, _block_hash: &str) -> Result<ProviderBlock> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_block_headers_by_hashes(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
        ) -> Result<Vec<ProviderBlock>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_block_bundles_by_hashes(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
        ) -> Result<Vec<ProviderBlockBundle>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_block_bundles_without_logs_by_hashes(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
        ) -> Result<Vec<ProviderBlockBundle>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_block_bundle_by_hash(
            &self,
            _block_hash: &str,
        ) -> Result<ProviderBlockBundle> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_logs_by_block_range(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
            _addresses: &[String],
        ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
            _topic0s: &[String],
            _addresses: &[String],
        ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_transaction_receipt_pairs_by_hashes(
            &self,
            _requests: &[ProviderTransactionReceiptRequest],
        ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_code_observations_at_block(
            &self,
            _addresses: &[String],
            _block: ProviderBlockSelection,
        ) -> Result<Vec<ProviderCodeObservation>> {
            bail!("unused in backfill canonicality revalidation tests")
        }

        async fn fetch_code_observations_at_block_hashes(
            &self,
            _requests: &[ProviderBlockCodeObservationRequest],
        ) -> Result<Vec<ProviderBlockCodeObservations>> {
            bail!("unused in backfill canonicality revalidation tests")
        }
    }

    #[tokio::test]
    async fn provider_checkpoint_evidence_requires_fresh_number_hash_match() -> Result<()> {
        let canonical_40 = test_provider_block(
            "0x4000000000000000000000000000000000000000000000000000000000000040",
            40,
        );
        let canonical_41 = test_provider_block(
            "0x4100000000000000000000000000000000000000000000000000000000000041",
            41,
        );
        let canonical_42 = test_provider_block(
            "0x4200000000000000000000000000000000000000000000000000000000000042",
            42,
        );
        let losing_40 = test_provider_block(
            "0xdead000000000000000000000000000000000000000000000000000000000040",
            40,
        );
        let evidence = BackfillCanonicalityEvidence::from_heads(&ProviderHeadSnapshot {
            canonical: canonical_42.clone(),
            safe: Some(canonical_41.clone()),
            finalized: Some(canonical_40.clone()),
        });
        let provider = RevalidatingProvider {
            heads: ProviderHeadSnapshot {
                canonical: canonical_42,
                safe: Some(canonical_41),
                finalized: Some(canonical_40.clone()),
            },
            hashes_by_number: BTreeMap::from([(40, canonical_40.block_hash)]),
        };
        let pool = PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("canonicality evidence test does not touch the lazy pool");

        let states = evidence
            .states_for_blocks(&pool, "ethereum-mainnet", &provider, &[losing_40.clone()])
            .await?;

        assert_eq!(
            states.get(&losing_40.block_hash),
            Some(&CanonicalityState::Observed)
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_revalidation_preempts_distant_stored_checkpoint_walk() -> Result<()> {
        let canonical_40 = test_provider_block(
            "0x4000000000000000000000000000000000000000000000000000000000000040",
            40,
        );
        let checkpoint = test_provider_block(
            "0x5000000000000000000000000000000000000000000000000000000000005000",
            5_000,
        );
        let mut evidence = BackfillCanonicalityEvidence::from_heads(&ProviderHeadSnapshot {
            canonical: checkpoint.clone(),
            safe: Some(checkpoint.clone()),
            finalized: Some(checkpoint.clone()),
        });
        evidence.include_checkpoint(Some(&ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(checkpoint.block_hash.clone()),
            canonical_block_number: Some(checkpoint.block_number),
            safe_block_hash: Some(checkpoint.block_hash.clone()),
            safe_block_number: Some(checkpoint.block_number),
            finalized_block_hash: Some(checkpoint.block_hash.clone()),
            finalized_block_number: Some(checkpoint.block_number),
        }));
        let provider = RevalidatingProvider {
            heads: ProviderHeadSnapshot {
                canonical: checkpoint.clone(),
                safe: Some(checkpoint.clone()),
                finalized: Some(checkpoint),
            },
            hashes_by_number: BTreeMap::from([(40, canonical_40.block_hash.clone())]),
        };
        let pool = PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:1/bigname")
            .expect("test pool must stay lazy unless stored lineage is queried");

        let states = evidence
            .states_for_blocks(
                &pool,
                "ethereum-mainnet",
                &provider,
                &[canonical_40.clone()],
            )
            .await?;

        assert_eq!(
            states.get(&canonical_40.block_hash),
            Some(&CanonicalityState::Finalized)
        );
        Ok(())
    }

    #[tokio::test]
    async fn distant_stored_checkpoint_is_not_used_without_provider_hash_match() -> Result<()> {
        let canonical_40 = test_provider_block(
            "0x4000000000000000000000000000000000000000000000000000000000000040",
            40,
        );
        let losing_40 = test_provider_block(
            "0xdead000000000000000000000000000000000000000000000000000000000040",
            40,
        );
        let checkpoint = test_provider_block(
            "0x5000000000000000000000000000000000000000000000000000000000005000",
            5_000,
        );
        let mut evidence = BackfillCanonicalityEvidence::from_heads(&ProviderHeadSnapshot {
            canonical: checkpoint.clone(),
            safe: Some(checkpoint.clone()),
            finalized: Some(checkpoint.clone()),
        });
        evidence.include_checkpoint(Some(&ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(checkpoint.block_hash.clone()),
            canonical_block_number: Some(checkpoint.block_number),
            safe_block_hash: Some(checkpoint.block_hash.clone()),
            safe_block_number: Some(checkpoint.block_number),
            finalized_block_hash: Some(checkpoint.block_hash.clone()),
            finalized_block_number: Some(checkpoint.block_number),
        }));
        let provider = RevalidatingProvider {
            heads: ProviderHeadSnapshot {
                canonical: checkpoint.clone(),
                safe: Some(checkpoint.clone()),
                finalized: Some(checkpoint),
            },
            hashes_by_number: BTreeMap::from([(40, canonical_40.block_hash)]),
        };
        let pool = PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:1/bigname")
            .expect("test pool must stay lazy unless stored lineage is queried");

        let states = evidence
            .states_for_blocks(&pool, "ethereum-mainnet", &provider, &[losing_40.clone()])
            .await?;

        assert_eq!(
            states.get(&losing_40.block_hash),
            Some(&CanonicalityState::Observed)
        );
        Ok(())
    }

    fn test_provider_block(block_hash: &str, block_number: i64) -> ProviderBlock {
        ProviderBlock {
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp_unix_secs: 1_717_171_717 + block_number,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
        }
    }
}
