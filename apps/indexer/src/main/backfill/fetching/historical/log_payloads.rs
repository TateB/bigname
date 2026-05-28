use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

use crate::backfill::{CoinbaseSqlValidationMode, HistoricalLogValidationFilter};
use crate::provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock};

pub(super) async fn fill_log_payloads_from_validation_provider(
    validation_provider: &(impl ChainProviderOps + ?Sized),
    resolved_blocks: &[ProviderResolvedBlock],
    logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
    validation_filters: &[HistoricalLogValidationFilter],
    _validation_mode: CoinbaseSqlValidationMode,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    let validation_filters = effective_validation_filters(validation_filters, &logs_by_block);
    if validation_filters.is_empty() {
        return Ok(logs_by_block);
    }

    let mut provider_logs_by_identity = BTreeMap::<LogIdentity, ProviderLog>::new();
    for validation_filter in &validation_filters {
        let filter_blocks = resolved_blocks_for_filter(resolved_blocks, validation_filter);
        if filter_blocks.is_empty() || validation_filter.addresses.is_empty() {
            continue;
        }
        let provider_logs_by_block = if validation_filter.topic0s.is_empty() {
            validation_provider
                .fetch_logs_by_block_range(&filter_blocks, &validation_filter.addresses)
                .await
        } else {
            validation_provider
                .fetch_logs_by_block_range_for_topic0s_and_addresses(
                    &filter_blocks,
                    &validation_filter.topic0s,
                    &validation_filter.addresses,
                )
                .await
        }
        .context("failed to fetch validation-provider log payloads for Coinbase SQL identities")?;
        for logs in provider_logs_by_block.into_values() {
            for log in logs {
                provider_logs_by_identity.insert(LogIdentity::from_log(&log), log);
            }
        }
    }

    let filled = logs_by_block
        .into_iter()
        .map(|(block_number, logs)| {
            let filled_logs = logs
                .into_iter()
                .map(|identity_log| fill_one_log(&mut provider_logs_by_identity, identity_log))
                .collect::<Result<Vec<_>>>()?;
            Ok((block_number, filled_logs))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    if !provider_logs_by_identity.is_empty() {
        let missing = provider_logs_by_identity
            .values()
            .next()
            .expect("provider log map is not empty");
        bail!(
            "Coinbase SQL omitted validation-provider log identity at block {} transaction {} log {}",
            missing.block_number,
            missing.transaction_hash,
            missing.log_index
        );
    }

    Ok(filled)
}

fn effective_validation_filters(
    validation_filters: &[HistoricalLogValidationFilter],
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
) -> Vec<HistoricalLogValidationFilter> {
    if !validation_filters.is_empty() {
        return validation_filters
            .iter()
            .map(|filter| HistoricalLogValidationFilter {
                from_block: filter.from_block,
                to_block: filter.to_block,
                addresses: filter
                    .addresses
                    .iter()
                    .map(|address| address.to_ascii_lowercase())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                topic0s: filter
                    .topic0s
                    .iter()
                    .map(|topic0| topic0.to_ascii_lowercase())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            })
            .collect();
    }

    let addresses = logs_by_block
        .values()
        .flat_map(|logs| logs.iter().map(|log| log.address.to_ascii_lowercase()))
        .collect::<BTreeSet<_>>();
    if addresses.is_empty() {
        return Vec::new();
    }
    let from_block = logs_by_block.keys().next().copied().unwrap_or_default();
    let to_block = logs_by_block
        .keys()
        .next_back()
        .copied()
        .unwrap_or(from_block);

    vec![HistoricalLogValidationFilter {
        from_block,
        to_block,
        addresses: addresses.into_iter().collect(),
        topic0s: Vec::new(),
    }]
}

fn resolved_blocks_for_filter(
    resolved_blocks: &[ProviderResolvedBlock],
    validation_filter: &HistoricalLogValidationFilter,
) -> Vec<ProviderResolvedBlock> {
    resolved_blocks
        .iter()
        .filter(|block| {
            block.block_number >= validation_filter.from_block
                && block.block_number <= validation_filter.to_block
        })
        .cloned()
        .collect()
}

fn fill_one_log(
    provider_logs_by_identity: &mut BTreeMap<LogIdentity, ProviderLog>,
    identity_log: ProviderLog,
) -> Result<ProviderLog> {
    let identity = LogIdentity::from_log(&identity_log);
    let provider_log = provider_logs_by_identity
        .remove(&identity)
        .with_context(|| {
            format!(
                "validation provider did not return Coinbase SQL log identity at block {} transaction {} log {}",
                identity_log.block_number, identity_log.transaction_hash, identity_log.log_index
            )
        })?;
    if provider_log.topics != identity_log.topics {
        bail!(
            "validation provider topics differ for Coinbase SQL log identity at block {} transaction {} log {}",
            identity_log.block_number,
            identity_log.transaction_hash,
            identity_log.log_index
        );
    }

    Ok(provider_log)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LogIdentity {
    block_hash: String,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    address: String,
}

impl LogIdentity {
    fn from_log(log: &ProviderLog) -> Self {
        Self {
            block_hash: log.block_hash.to_ascii_lowercase(),
            transaction_hash: log.transaction_hash.to_ascii_lowercase(),
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            address: log.address.to_ascii_lowercase(),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;

    use super::*;
    use crate::provider::{
        ProviderBlock, ProviderBlockBundle, ProviderBlockCodeObservationRequest,
        ProviderBlockCodeObservations, ProviderBlockSelection, ProviderCodeObservation,
        ProviderHeadSnapshot, ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest,
    };

    const BLOCK_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TX_HASH: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const ADDRESS: &str = "0x1111111111111111111111111111111111111111";
    const TOPIC0: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[tokio::test]
    async fn full_validation_rejects_provider_log_missing_from_coinbase() {
        let provider = LogProvider::new(vec![provider_log("0x1234")]);
        let error = fill_log_payloads_from_validation_provider(
            &provider,
            &resolved_blocks(),
            BTreeMap::new(),
            &[validation_filter()],
            CoinbaseSqlValidationMode::Full,
        )
        .await
        .expect_err("full validation must reject provider logs omitted by Coinbase SQL");

        assert!(format!("{error:#}").contains("Coinbase SQL omitted validation-provider log"));
    }

    #[tokio::test]
    async fn sample_validation_rejects_provider_log_missing_from_coinbase_in_first_slice() {
        let provider = LogProvider::new(vec![provider_log("0x1234")]);
        let error = fill_log_payloads_from_validation_provider(
            &provider,
            &resolved_blocks(),
            BTreeMap::new(),
            &[validation_filter()],
            CoinbaseSqlValidationMode::Sample,
        )
        .await
        .expect_err("sample validation is conservative/full in the first Coinbase SQL slice");

        assert!(format!("{error:#}").contains("Coinbase SQL omitted validation-provider log"));
    }

    #[tokio::test]
    async fn validation_provider_payload_replaces_coinbase_identity_placeholder() {
        let provider = LogProvider::new(vec![provider_log("0x1234")]);
        let mut coinbase_logs = BTreeMap::new();
        coinbase_logs.insert(10, vec![provider_log("0x")]);

        let filled = fill_log_payloads_from_validation_provider(
            &provider,
            &resolved_blocks(),
            coinbase_logs,
            &[validation_filter()],
            CoinbaseSqlValidationMode::Full,
        )
        .await
        .expect("matching Coinbase identity should be filled by validation provider payload");

        assert_eq!(filled[&10][0].data, "0x1234");
    }

    #[tokio::test]
    async fn validation_filters_are_block_scoped_for_dynamic_targets() {
        let later_log = ProviderLog {
            block_hash: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                .to_owned(),
            block_number: 11,
            transaction_hash: "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                .to_owned(),
            transaction_index: 1,
            log_index: 2,
            address: ADDRESS.to_owned(),
            topics: vec![TOPIC0.to_owned()],
            data: "0x5678".to_owned(),
        };
        let provider = LogProvider {
            logs_by_block: BTreeMap::from([
                (10, vec![provider_log("0x1234")]),
                (11, vec![later_log]),
            ]),
        };
        let mut coinbase_logs = BTreeMap::new();
        coinbase_logs.insert(10, vec![provider_log("0x")]);

        let filled = fill_log_payloads_from_validation_provider(
            &provider,
            &vec![
                ProviderResolvedBlock {
                    block_number: 10,
                    block_hash: BLOCK_HASH.to_owned(),
                },
                ProviderResolvedBlock {
                    block_number: 11,
                    block_hash:
                        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                            .to_owned(),
                },
            ],
            coinbase_logs,
            &[validation_filter()],
            CoinbaseSqlValidationMode::Full,
        )
        .await
        .expect("logs outside the active validation subrange should not be considered omitted");

        assert_eq!(filled[&10][0].data, "0x1234");
    }

    fn resolved_blocks() -> Vec<ProviderResolvedBlock> {
        vec![ProviderResolvedBlock {
            block_number: 10,
            block_hash: BLOCK_HASH.to_owned(),
        }]
    }

    fn validation_filter() -> HistoricalLogValidationFilter {
        HistoricalLogValidationFilter {
            from_block: 10,
            to_block: 10,
            addresses: vec![ADDRESS.to_owned()],
            topic0s: vec![TOPIC0.to_owned()],
        }
    }

    fn provider_log(data: &str) -> ProviderLog {
        ProviderLog {
            block_hash: BLOCK_HASH.to_owned(),
            block_number: 10,
            transaction_hash: TX_HASH.to_owned(),
            transaction_index: 1,
            log_index: 2,
            address: ADDRESS.to_owned(),
            topics: vec![TOPIC0.to_owned()],
            data: data.to_owned(),
        }
    }

    #[derive(Clone)]
    struct LogProvider {
        logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
    }

    impl LogProvider {
        fn new(logs: Vec<ProviderLog>) -> Self {
            Self {
                logs_by_block: BTreeMap::from([(10, logs)]),
            }
        }

        fn filtered_logs(
            &self,
            resolved_blocks: &[ProviderResolvedBlock],
            addresses: &[String],
            topic0s: &[String],
        ) -> BTreeMap<i64, Vec<ProviderLog>> {
            let allowed_blocks = resolved_blocks
                .iter()
                .map(|block| block.block_number)
                .collect::<BTreeSet<_>>();
            let addresses = addresses
                .iter()
                .map(|address| address.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();
            let topic0s = topic0s
                .iter()
                .map(|topic0| topic0.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();

            self.logs_by_block
                .iter()
                .filter(|(block_number, _)| allowed_blocks.contains(block_number))
                .filter_map(|(block_number, logs)| {
                    let logs = logs
                        .iter()
                        .filter(|log| addresses.contains(&log.address.to_ascii_lowercase()))
                        .filter(|log| {
                            topic0s.is_empty()
                                || log.topics.first().is_some_and(|topic0| {
                                    topic0s.contains(&topic0.to_ascii_lowercase())
                                })
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    (!logs.is_empty()).then_some((*block_number, logs))
                })
                .collect()
        }
    }

    impl ChainProviderOps for LogProvider {
        async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_block_hashes_by_numbers(
            &self,
            _block_numbers: &[i64],
        ) -> Result<Vec<ProviderResolvedBlock>> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_block_by_hash(&self, _block_hash: &str) -> Result<ProviderBlock> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_block_headers_by_hashes(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
        ) -> Result<Vec<ProviderBlock>> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_block_bundles_by_hashes(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
        ) -> Result<Vec<ProviderBlockBundle>> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_block_bundles_without_logs_by_hashes(
            &self,
            _resolved_blocks: &[ProviderResolvedBlock],
        ) -> Result<Vec<ProviderBlockBundle>> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_block_bundle_by_hash(
            &self,
            _block_hash: &str,
        ) -> Result<ProviderBlockBundle> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_logs_by_block_range(
            &self,
            resolved_blocks: &[ProviderResolvedBlock],
            addresses: &[String],
        ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
            Ok(self.filtered_logs(resolved_blocks, addresses, &[]))
        }

        async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
            &self,
            resolved_blocks: &[ProviderResolvedBlock],
            topic0s: &[String],
            addresses: &[String],
        ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
            Ok(self.filtered_logs(resolved_blocks, addresses, topic0s))
        }

        async fn fetch_transaction_receipt_pairs_by_hashes(
            &self,
            _requests: &[ProviderTransactionReceiptRequest],
        ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_code_observations_at_block(
            &self,
            _addresses: &[String],
            _block: ProviderBlockSelection,
        ) -> Result<Vec<ProviderCodeObservation>> {
            bail!("unused in log payload validation tests")
        }

        async fn fetch_code_observations_at_block_hashes(
            &self,
            _requests: &[ProviderBlockCodeObservationRequest],
        ) -> Result<Vec<ProviderBlockCodeObservations>> {
            bail!("unused in log payload validation tests")
        }
    }
}
