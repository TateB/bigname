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
    }
}

#[cfg(test)]
#[tokio::test]
async fn replay_capability_conformance() -> anyhow::Result<()> {
    shipped_api::conformance::run_replay_capability_conformance().await
}
