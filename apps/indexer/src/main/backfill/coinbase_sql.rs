use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};

#[path = "coinbase_sql/auth.rs"]
mod auth;
#[path = "coinbase_sql/client.rs"]
mod client;
#[path = "coinbase_sql/pagination.rs"]
mod pagination;
#[path = "coinbase_sql/planner.rs"]
mod planner;
#[path = "coinbase_sql/query.rs"]
mod query;
#[path = "coinbase_sql/rate_limit.rs"]
mod rate_limit;
#[path = "coinbase_sql/rows.rs"]
mod rows;

use crate::provider::{ProviderLog, ProviderReceipt, ProviderTransaction};

use super::{
    CoinbaseSqlBackfillConfig, HistoricalBackfillSourceOps, HistoricalLogPayload,
    HistoricalLogPayloadRequest, HistoricalLogValidationFilter,
};

pub(crate) use planner::load_backfill_topic_plan;
#[cfg(test)]
#[path = "coinbase_sql/tests.rs"]
mod tests;

pub(crate) const DEFAULT_COINBASE_SQL_API_KEY_ID_ENV: &str = "COINBASE_CDP_SQL_API_KEY_ID";
pub(crate) const DEFAULT_COINBASE_SQL_API_KEY_SECRET_ENV: &str = "COINBASE_CDP_SQL_API_KEY_SECRET";
const COINBASE_SQL_DEFAULT_RUN_URL: &str =
    "https://api.cdp.coinbase.com/platform/v2/data/query/run";

#[derive(Clone, Debug)]
pub(crate) struct CoinbaseSqlSourceRegistry {
    urls_by_chain: BTreeMap<String, String>,
    api_key_id_env: String,
    api_key_secret_env: String,
    config: CoinbaseSqlBackfillConfig,
}

impl CoinbaseSqlSourceRegistry {
    pub(crate) fn from_entries(
        entries: &[String],
        api_key_id_env: String,
        api_key_secret_env: String,
        config: CoinbaseSqlBackfillConfig,
    ) -> Result<Self> {
        let mut urls_by_chain = BTreeMap::new();
        for entry in entries {
            let (chain, url) = parse_chain_source_entry(
                entry,
                "Coinbase SQL URL",
                "<chain>=<https://api.cdp.coinbase.com/platform/v2/data/query/run>",
            )?;
            if urls_by_chain.insert(chain.clone(), url).is_some() {
                bail!("duplicate Coinbase SQL source configuration for {chain}");
            }
        }

        Ok(Self {
            urls_by_chain,
            api_key_id_env,
            api_key_secret_env,
            config,
        })
    }

    pub(crate) fn has_source_for(&self, chain: &str) -> bool {
        self.urls_by_chain.contains_key(chain)
    }

    pub(crate) fn source_for(&self, chain: &str) -> Result<Option<CoinbaseSqlBackfillSource>> {
        let Some(url) = self.urls_by_chain.get(chain) else {
            return Ok(None);
        };

        CoinbaseSqlBackfillSource::new(
            chain.to_owned(),
            url.clone(),
            self.api_key_id_env.clone(),
            self.api_key_secret_env.clone(),
            self.config.clone(),
        )
        .map(Some)
    }
}

fn parse_chain_source_entry(
    entry: &str,
    source_label: &str,
    expected_shape: &str,
) -> Result<(String, String)> {
    let (chain, value) = entry.split_once('=').with_context(|| {
        format!("invalid {source_label} source entry {entry}; expected {expected_shape}")
    })?;
    let chain = chain.trim();
    let value = value.trim();
    if chain.is_empty() || value.is_empty() {
        bail!("invalid {source_label} source entry {entry}; expected non-empty {expected_shape}");
    }

    Ok((chain.to_owned(), value.to_owned()))
}

#[derive(Clone)]
pub(crate) struct CoinbaseSqlBackfillSource {
    chain: String,
    config: CoinbaseSqlBackfillConfig,
    client: client::CoinbaseSqlClient,
}

impl CoinbaseSqlBackfillSource {
    fn new(
        chain: String,
        url: String,
        api_key_id_env: String,
        api_key_secret_env: String,
        config: CoinbaseSqlBackfillConfig,
    ) -> Result<Self> {
        config.validate()?;
        let url = if url == "default" {
            COINBASE_SQL_DEFAULT_RUN_URL.to_owned()
        } else {
            url
        };
        let client =
            client::CoinbaseSqlClient::new(&url, &api_key_id_env, &api_key_secret_env, &config)?;
        Ok(Self {
            chain,
            config,
            client,
        })
    }
}

impl HistoricalBackfillSourceOps for CoinbaseSqlBackfillSource {
    async fn fetch_selected_log_payloads(
        &self,
        request: HistoricalLogPayloadRequest<'_>,
    ) -> Result<HistoricalLogPayload> {
        if request.chain != self.chain {
            bail!(
                "Coinbase SQL source configured for chain {} was asked to fetch {}",
                self.chain,
                request.chain
            );
        }

        let mut logs_by_block = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let transactions_by_block = BTreeMap::<i64, Vec<ProviderTransaction>>::new();
        let receipts_by_block = BTreeMap::<i64, Vec<ProviderReceipt>>::new();
        let mut stats = super::CoinbaseSqlFetchStats::default();
        let mut validation_filters = Vec::new();
        let mut seen_log_identities = BTreeSet::new();
        let resolved_by_number = request
            .resolved_blocks
            .iter()
            .map(|block| (block.block_number, block.block_hash.clone()))
            .collect::<BTreeMap<_, _>>();
        let packs = planner::build_filter_packs(&request);

        for pack in packs {
            for split_pack in query::build_or_split_filter_pack(
                pack,
                self.config.sql_char_limit,
                self.config.page_limit,
            )? {
                validation_filters.push(HistoricalLogValidationFilter {
                    from_block: split_pack.from_block,
                    to_block: split_pack.to_block,
                    addresses: split_pack.addresses.clone(),
                    topic0s: split_pack.topic0s.clone(),
                });
                let pages = pagination::fetch_all_pages(
                    &self.client,
                    &split_pack,
                    self.config.page_limit,
                    self.config.sql_char_limit,
                )
                .await?;
                stats.merge(pages.stats);
                for row in pages.rows {
                    row.validate_against_filter_pack(&split_pack, &resolved_by_number)?;
                    let log = row.to_provider_log()?;
                    push_deduped_log(&mut logs_by_block, &mut seen_log_identities, log);
                }
            }
        }

        for logs in logs_by_block.values_mut() {
            logs.sort_by(|left, right| {
                left.transaction_index
                    .cmp(&right.transaction_index)
                    .then_with(|| left.log_index.cmp(&right.log_index))
            });
        }

        Ok(HistoricalLogPayload {
            logs_by_block,
            transactions_by_block,
            receipts_by_block,
            logs_need_validation_provider_payload: true,
            validation_filters,
            validation_mode: request.validation_mode,
            source_stats: stats,
        })
    }
}

fn push_deduped_log(
    logs_by_block: &mut BTreeMap<i64, Vec<ProviderLog>>,
    seen_log_identities: &mut BTreeSet<CoinbaseSqlLogIdentity>,
    log: ProviderLog,
) {
    if seen_log_identities.insert(CoinbaseSqlLogIdentity::from_log(&log)) {
        logs_by_block.entry(log.block_number).or_default().push(log);
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CoinbaseSqlLogIdentity {
    block_hash: String,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    address: String,
}

impl CoinbaseSqlLogIdentity {
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
