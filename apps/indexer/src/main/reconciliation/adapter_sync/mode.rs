use anyhow::Result;

use super::super::replay::{
    NormalizedEventReplayAdapter, RawFactReplayContractPlan, source_scope_includes_adapter,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PersistedRawPayloadAdapterSyncMode {
    LivePoll,
    LiveOrBackfill,
    RawFactReplay {
        canonical_raw_log_count: usize,
        replay_contract_plan: RawFactReplayContractPlan,
    },
}

pub(super) fn ensure_raw_fact_adapter_allowed(
    mode: PersistedRawPayloadAdapterSyncMode,
    adapter: NormalizedEventReplayAdapter,
) -> Result<()> {
    if let PersistedRawPayloadAdapterSyncMode::RawFactReplay {
        replay_contract_plan,
        ..
    } = mode
    {
        replay_contract_plan.ensure_adapter_allowed(adapter)?;
    }
    Ok(())
}

pub(super) fn adapter_selected_by_scope(
    source_scope: Option<&[(String, String, i64, i64)]>,
    adapter: NormalizedEventReplayAdapter,
) -> bool {
    source_scope.map_or(true, |source_scope| {
        source_scope_includes_adapter(source_scope, adapter)
    })
}
