mod shipped_api {
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/api_main.rs"));

    #[cfg(test)]
    pub(crate) mod conformance {
        include!("conformance/harness.rs");

        include!("conformance/helpers.rs");

        include!("conformance/collections.rs");

        include!("conformance/exact_name.rs");

        include!("conformance/resolution_and_permissions.rs");

        include!("conformance/primary_names.rs");

        include!("conformance/history.rs");

        include!("conformance/replay.rs");

        include!("conformance/backfill.rs");

        include!("conformance/backfill_sources.rs");

        include!("conformance/chaos.rs");
    }
}

#[cfg(test)]
#[tokio::test]
async fn replay_capability_conformance() -> anyhow::Result<()> {
    shipped_api::conformance::run_replay_capability_conformance().await
}

#[cfg(test)]
#[tokio::test]
async fn backfilled_data_consumer_conformance_job() -> anyhow::Result<()> {
    shipped_api::conformance::run_backfilled_data_consumer_conformance_job().await
}

#[cfg(test)]
#[tokio::test]
async fn backfill_sources_source_family_existing_response_lock() -> anyhow::Result<()> {
    shipped_api::conformance::run_backfill_source_family_existing_response_lock().await
}

#[cfg(test)]
#[tokio::test]
async fn reorg_chaos_drill_conformance_job() -> anyhow::Result<()> {
    shipped_api::conformance::run_reorg_chaos_drill_conformance_job().await
}
