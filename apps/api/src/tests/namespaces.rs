#[tokio::test]
async fn get_namespace_manifests_returns_active_entries() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_l1 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l1, "declared_children", "supported", None)
        .await?;
    database
        .insert_capability_flag(
            ens_l1,
            "verified_resolution",
            "shadow",
            Some("tracked but not yet served"),
        )
        .await?;

    let ens_l2 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l2",
            "base-mainnet",
            "ens_v2_base",
            2,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l2, "declared_children", "unsupported", Some("pending"))
        .await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            3,
            "shadow",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let basenames = database
        .insert_manifest(
            "basenames",
            "base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(basenames, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceManifestsResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert!(payload.provenance.normalized_event_ids.is_empty());
    assert!(payload.provenance.raw_fact_refs.is_empty());
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.provenance.execution_trace_id, None);
    assert_eq!(payload.provenance.manifest_versions.len(), 2);
    assert_eq!(payload.declared_state.manifests.len(), 2);

    assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
    assert_eq!(
        payload.declared_state.manifests[0].source_family,
        "ens_v2_registry_l1"
    );
    assert_eq!(
        payload.declared_state.manifests[0].chain,
        "ethereum-mainnet"
    );
    assert_eq!(
        payload.declared_state.manifests[0].deployment_epoch,
        "ens_v2"
    );
    assert_eq!(
        payload.declared_state.manifests[0].normalizer_version,
        "ensip15@ens-normalize-0.1.1"
    );
    assert_eq!(
        payload.declared_state.manifests[0]
            .capability_flags
            .get("declared_children")
            .expect("declared_children capability")
            .status,
        bigname_manifests::CapabilitySupportStatus::Supported
    );
    assert_eq!(
        payload.declared_state.manifests[0]
            .capability_flags
            .get("verified_resolution")
            .expect("verified_resolution capability")
            .notes
            .as_deref(),
        Some("tracked but not yet served")
    );
    assert_eq!(
        payload.provenance.manifest_versions[0],
        ManifestVersionRef {
            manifest_version: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            deployment_epoch: "ens_v2".to_owned(),
        }
    );

    assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
    assert_eq!(
        payload.declared_state.manifests[1].source_family,
        "ens_v2_registry_l2"
    );
    assert_eq!(payload.declared_state.manifests[1].chain, "base-mainnet");
    assert_eq!(
        payload.declared_state.manifests[1].deployment_epoch,
        "ens_v2_base"
    );
    assert_eq!(
        payload.declared_state.manifests[1].normalizer_version,
        "ensip15@ens-normalize-0.1.1"
    );
    assert_eq!(
        payload.declared_state.manifests[1]
            .capability_flags
            .get("declared_children")
            .expect("declared_children capability")
            .status,
        bigname_manifests::CapabilitySupportStatus::Unsupported
    );
    assert_eq!(
        payload.provenance.manifest_versions[1],
        ManifestVersionRef {
            manifest_version: 2,
            source_family: "ens_v2_registry_l2".to_owned(),
            chain: "base-mainnet".to_owned(),
            deployment_epoch: "ens_v2_base".to_owned(),
        }
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_active_summary() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_l1 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l1, "declared_children", "supported", None)
        .await?;

    let ens_l2 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l2",
            "base-mainnet",
            "ens_v2_base",
            2,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l2, "declared_children", "shadow", Some("shadowed"))
        .await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            3,
            "shadow",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "verified_resolution", "shadow", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceMetadataResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.declared_state.active_manifest_count, 2);
    assert_eq!(
        payload.declared_state.active_source_families,
        vec![
            "ens_v2_registry_l1".to_owned(),
            "ens_v2_registry_l2".to_owned()
        ]
    );
    assert_eq!(
        payload.declared_state.chains,
        vec!["base-mainnet".to_owned(), "ethereum-mainnet".to_owned()]
    );
    assert_eq!(
        payload.declared_state.normalizer_versions,
        vec!["ensip15@ens-normalize-0.1.1".to_owned()]
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.provenance.execution_trace_id, None);
    assert!(payload.provenance.normalized_event_ids.is_empty());
    assert!(payload.provenance.raw_fact_refs.is_empty());
    assert_eq!(payload.provenance.manifest_versions.len(), 2);
    assert_eq!(
        payload.provenance.manifest_versions[0],
        ManifestVersionRef {
            manifest_version: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            deployment_epoch: "ens_v2".to_owned(),
        }
    );
    assert_eq!(
        payload.provenance.manifest_versions[1],
        ManifestVersionRef {
            manifest_version: 2,
            source_family: "ens_v2_registry_l2".to_owned(),
            chain: "base-mainnet".to_owned(),
            deployment_epoch: "ens_v2_base".to_owned(),
        }
    );
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_empty_summary_when_namespace_has_no_active_manifests()
-> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            1,
            "shadow",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceMetadataResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.declared_state.active_manifest_count, 0);
    assert!(payload.declared_state.active_source_families.is_empty());
    assert!(payload.declared_state.chains.is_empty());
    assert!(payload.declared_state.normalizer_versions.is_empty());
    assert!(payload.provenance.manifest_versions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_internal_error_envelope_on_load_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load namespace metadata for namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_not_found_for_unknown_namespace() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_empty_list_when_namespace_has_no_active_entries()
-> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            1,
            "shadow",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceManifestsResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert!(payload.declared_state.manifests.is_empty());
    assert!(payload.provenance.manifest_versions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_internal_error_envelope_on_load_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load manifest snapshot for namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_not_found_for_unknown_namespace() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}
