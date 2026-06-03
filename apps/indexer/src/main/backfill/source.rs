use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::provider::{ProviderLog, ProviderReceipt, ProviderResolvedBlock, ProviderTransaction};

use super::{
    BackfillBlockRange, CoinbaseSqlValidationMode, selection::SelectedTargetIntervalIndex,
};
use bigname_manifests::WatchedSourceSelectorPlan;

pub(crate) trait HistoricalBackfillSourceOps {
    fn fetch_selected_log_payloads(
        &self,
        request: HistoricalLogPayloadRequest<'_>,
    ) -> impl Future<Output = Result<HistoricalLogPayload>> + Send;
}

#[allow(
    dead_code,
    reason = "source request mirrors the historical-source contract"
)]
pub(crate) struct HistoricalLogPayloadRequest<'a> {
    pub(crate) chain: &'a str,
    pub(crate) source_plan: &'a WatchedSourceSelectorPlan,
    pub(crate) selected_target_index: &'a SelectedTargetIntervalIndex,
    pub(crate) resolved_blocks: &'a [ProviderResolvedBlock],
    pub(crate) selected_target_addresses_for_chunk: &'a [String],
    pub(crate) topic_plan: &'a BackfillTopicPlan,
    pub(crate) range: BackfillBlockRange,
    pub(crate) validation_mode: CoinbaseSqlValidationMode,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HistoricalLogPayload {
    pub(crate) logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
    pub(crate) transactions_by_block: BTreeMap<i64, Vec<ProviderTransaction>>,
    pub(crate) receipts_by_block: BTreeMap<i64, Vec<ProviderReceipt>>,
    pub(crate) logs_need_validation_provider_payload: bool,
    pub(crate) logs_filtered_by_selected_target_index: bool,
    pub(crate) code_observation_scope: HistoricalCodeObservationScope,
    pub(crate) validation_filters: Vec<HistoricalLogValidationFilter>,
    pub(crate) validation_mode: CoinbaseSqlValidationMode,
    pub(crate) source_stats: CoinbaseSqlFetchStats,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum HistoricalCodeObservationScope {
    #[default]
    SelectedAddresses,
    LogEmittersOnly,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HistoricalLogValidationFilter {
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) addresses: Vec<String>,
    pub(crate) topic0s: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CoinbaseSqlFetchStats {
    pub(crate) query_count: usize,
    pub(crate) page_count: usize,
    pub(crate) row_count: usize,
    pub(crate) retry_count: usize,
}

impl CoinbaseSqlFetchStats {
    pub(crate) fn record_page(&mut self, row_count: usize) {
        self.page_count += 1;
        self.row_count += row_count;
    }

    pub(crate) fn merge(&mut self, other: CoinbaseSqlFetchStats) {
        self.query_count += other.query_count;
        self.page_count += other.page_count;
        self.row_count += other.row_count;
        self.retry_count += other.retry_count;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BackfillTopicPlan {
    topic0s_by_source_family: BTreeMap<String, Vec<String>>,
    event_signatures_by_source_family: BTreeMap<String, Vec<String>>,
    source_families_without_topics: BTreeSet<String>,
}

impl BackfillTopicPlan {
    pub(crate) fn new(
        topic0s_by_source_family: BTreeMap<String, Vec<String>>,
        event_signatures_by_source_family: BTreeMap<String, Vec<String>>,
        source_families_without_topics: BTreeSet<String>,
    ) -> Self {
        Self {
            topic0s_by_source_family,
            event_signatures_by_source_family,
            source_families_without_topics,
        }
    }

    pub(crate) fn topic0s_for_source_family(&self, source_family: &str) -> &[String] {
        self.topic0s_by_source_family
            .get(source_family)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn event_signatures_for_source_family(&self, source_family: &str) -> &[String] {
        self.event_signatures_by_source_family
            .get(source_family)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn source_family_has_topics(&self, source_family: &str) -> bool {
        self.topic0s_by_source_family
            .get(source_family)
            .is_some_and(|topics| !topics.is_empty())
    }

    #[allow(
        dead_code,
        reason = "kept for diagnostics and future validation policies"
    )]
    pub(crate) fn source_families_without_topics(&self) -> &BTreeSet<String> {
        &self.source_families_without_topics
    }

    pub(crate) fn source_identity_payload(&self) -> Result<Value> {
        #[derive(Serialize)]
        struct TopicPlanIdentity<'a> {
            topic0s_by_source_family: &'a BTreeMap<String, Vec<String>>,
            event_signatures_by_source_family: &'a BTreeMap<String, Vec<String>>,
            source_families_without_topics: &'a BTreeSet<String>,
        }

        serde_json::to_value(TopicPlanIdentity {
            topic0s_by_source_family: &self.topic0s_by_source_family,
            event_signatures_by_source_family: &self.event_signatures_by_source_family,
            source_families_without_topics: &self.source_families_without_topics,
        })
        .context("failed to serialize Coinbase SQL topic plan identity")
    }
}
