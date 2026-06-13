use super::*;
use crate::CanonicalityState;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

#[test]
fn migration_invalidates_existing_children_current_parent_keys() {
    let migration = include_str!(
        "../../../../migrations/20260608150000_label_preimages_and_unknown_children.sql"
    );
    let lookup_migration = include_str!(
        "../../../../migrations/20260608160000_label_preimage_invalidation_lookup.sql"
    );

    assert!(
        migration.contains("FROM public.children_current"),
        "migration must invalidate existing children_current parent keys on upgraded databases"
    );
    assert!(
        migration.contains("FROM public.normalized_events ne"),
        "migration must invalidate parent keys from existing subregistry normalized events"
    );
    assert!(
        !migration.contains("WITH changed_labelhashes AS"),
        "migration invalidation must not be rooted only in the newly-created label_preimages table"
    );
    assert!(
        lookup_migration.contains("normalized_events_children_v1_labelhash_lookup_idx"),
        "follow-up migration must index labelhash lookups before Rust retained-fact backfill"
    );
}

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_storage_label_preimages_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for label_preimages tests")
            .admin_connect_context("failed to connect admin pool for label_preimages tests")
            .pool_connect_context("failed to connect label_preimages test pool"),
        &crate::MIGRATOR,
        "failed to apply migrations for label_preimages tests",
    )
    .await
}

#[test]
fn normalizes_and_hashes_single_label_preimage() -> Result<()> {
    let preimage = label_preimage_from_label("Test", "unit", 1, json!({}))?;

    assert_eq!(preimage.normalized_label, "test");
    assert_eq!(preimage.canonical_display_label, "test");
    assert_eq!(
        preimage.labelhash,
        "0x9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658"
    );
    Ok(())
}

#[test]
fn rejects_multi_label_preimage() {
    assert!(label_preimage_from_label("bad.label", "unit", 1, json!({})).is_err());
}

#[tokio::test]
async fn rejects_direct_upsert_with_hash_mismatched_label_preimage() -> Result<()> {
    let database = test_database().await?;
    let mut preimage = label_preimage_from_label("alice", "unit", 1, json!({}))?;
    preimage.label = "bob".to_owned();
    preimage.normalized_label = "bob".to_owned();
    preimage.canonical_display_label = "bob".to_owned();

    let error = upsert_label_preimages(database.pool(), &[preimage])
        .await
        .expect_err("hash-mismatched preimage must be rejected");

    assert!(
        format!("{error:?}").contains("labelhash mismatch"),
        "unexpected error: {error:?}"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn backfills_existing_canonical_name_surface_and_preimage_event_facts() -> Result<()> {
    let database = test_database().await?;
    let surface_labelhashes = ["foo", "eth"]
        .into_iter()
        .map(|label| label_preimage_from_label(label, "unit", 1, json!({})))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|preimage| preimage.labelhash)
        .collect::<Vec<_>>();
    let event_labelhashes = ["bar", "eth"]
        .into_iter()
        .map(|label| label_preimage_from_label(label, "unit", 1, json!({})))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|preimage| preimage.labelhash)
        .collect::<Vec<_>>();

    crate::upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:foo.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "foo.eth".to_owned(),
            canonical_display_name: "foo.eth".to_owned(),
            normalized_name: "foo.eth".to_owned(),
            dns_encoded_name: b"foo.eth".to_vec(),
            namehash: "0xnamehashfoo".to_owned(),
            labelhashes: surface_labelhashes,
            normalizer_version: "unit".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xblocksurface".to_owned(),
            block_number: 1,
            provenance: json!({}),
            canonicality_state: CanonicalityState::Observed,
        }],
    )
    .await?;
    crate::upsert_normalized_events(
        database.pool(),
        &[NormalizedEvent {
            event_identity: "preimage:bar".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "PreimageObserved".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(2),
            block_hash: Some("0xblockevent".to_owned()),
            transaction_hash: Some("0xtxevent".to_owned()),
            log_index: Some(0),
            raw_fact_ref: json!({}),
            derivation_kind: "unit".to_owned(),
            canonicality_state: CanonicalityState::Orphaned,
            before_state: json!({}),
            after_state: json!({
                "decoded_name": "bar.eth",
                "labelhashes": event_labelhashes,
            }),
        }],
    )
    .await?;

    sqlx::query("DELETE FROM label_preimages")
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM label_preimage_backfill_runs")
        .execute(database.pool())
        .await?;

    let summary = backfill_label_preimages_from_existing_facts(database.pool(), Some(1)).await?;
    assert_eq!(summary.scanned_row_count, 2);
    assert_eq!(summary.retained_row_count, 3);

    let labels = sqlx::query_scalar::<_, String>(
        r#"
        SELECT normalized_label
        FROM label_preimages
        ORDER BY normalized_label
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(labels, vec!["bar", "eth", "foo"]);

    let rerun = backfill_label_preimages_from_existing_facts(database.pool(), Some(1)).await?;
    assert_eq!(rerun, LabelPreimageImportSummary::default());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn backfill_skips_name_surface_labels_rejected_by_active_normalizer() -> Result<()> {
    let database = test_database().await?;
    let bad_labelhash = label_preimage_from_label("bad", "unit", 1, json!({}))?.labelhash;
    let eth_labelhash = label_preimage_from_label("eth", "unit", 1, json!({}))?.labelhash;
    let rejected_labelhash =
        "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    let labelhashes = vec![
        bad_labelhash.clone(),
        rejected_labelhash,
        eth_labelhash.clone(),
    ];

    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES (
            'ens:bad..eth',
            'ens',
            'bad..eth',
            'bad..eth',
            'bad..eth',
            $1,
            '0xnamehashbad',
            $2,
            'old-normalizer',
            '[]'::jsonb,
            $3,
            'ethereum-mainnet',
            '0xblockbad',
            1,
            '{}'::jsonb,
            'observed'::canonicality_state
        )
        "#,
    )
    .bind(b"bad..eth".to_vec())
    .bind(&labelhashes)
    .bind(json!(["current normalizer rejects one retained label"]))
    .execute(database.pool())
    .await?;

    let summary = backfill_label_preimages_from_existing_facts(database.pool(), Some(1)).await?;
    assert_eq!(summary.scanned_row_count, 1);
    assert_eq!(summary.retained_row_count, 2);

    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT labelhash, normalized_label
        FROM label_preimages
        ORDER BY normalized_label
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (bad_labelhash, "bad".to_owned()),
            (eth_labelhash, "eth".to_owned()),
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[test]
fn derives_all_label_preimages_from_normalized_event() -> Result<()> {
    let labelhashes = ["foo", "parent", "eth"]
        .into_iter()
        .map(|label| label_preimage_from_label(label, "unit", 1, json!({})))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|preimage| preimage.labelhash)
        .collect::<Vec<_>>();
    let event = NormalizedEvent {
        event_identity: "preimage:unit".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "PreimageObserved".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(1),
        block_hash: Some("0xblock".to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "unit".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "decoded_name": "foo.parent.eth",
            "labelhashes": labelhashes,
        }),
    };

    let preimages =
        label_preimages_from_normalized_event(&event).expect("event should expose preimages")?;

    assert_eq!(
        preimages
            .iter()
            .map(|preimage| preimage.normalized_label.as_str())
            .collect::<Vec<_>>(),
        vec!["foo", "parent", "eth"]
    );
    assert_eq!(preimages[0].provenance["label_index"], json!(0));
    Ok(())
}

#[test]
fn skips_unverified_normalized_event_label_preimage_candidates() -> Result<()> {
    let foo_labelhash = label_preimage_from_label("foo", "unit", 1, json!({}))?.labelhash;
    let eth_labelhash = label_preimage_from_label("eth", "unit", 1, json!({}))?.labelhash;
    let event = NormalizedEvent {
        event_identity: "preimage:unit".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "PreimageObserved".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(1),
        block_hash: Some("0xblock".to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "unit".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "decoded_name": "foo..eth",
            "labelhashes": [
                foo_labelhash,
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                eth_labelhash,
            ],
        }),
    };

    let preimages =
        label_preimages_from_normalized_event(&event).expect("event should expose preimages")?;

    assert_eq!(
        preimages
            .iter()
            .map(|preimage| preimage.normalized_label.as_str())
            .collect::<Vec<_>>(),
        vec!["foo", "eth"]
    );
    Ok(())
}

#[test]
fn derives_verified_label_preimages_from_noncanonical_normalized_event() -> Result<()> {
    let labelhash = label_preimage_from_label("foo", "unit", 1, json!({}))?.labelhash;
    let event = NormalizedEvent {
        event_identity: "preimage:unit".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "PreimageObserved".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(1),
        block_hash: Some("0xblock".to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "unit".to_owned(),
        canonicality_state: CanonicalityState::Orphaned,
        before_state: json!({}),
        after_state: json!({
            "decoded_name": "foo",
            "labelhashes": [labelhash],
        }),
    };

    let preimages =
        label_preimages_from_normalized_event(&event).expect("event should expose preimages")?;

    assert_eq!(preimages.len(), 1);
    assert_eq!(preimages[0].normalized_label, "foo");
    Ok(())
}

#[test]
fn derives_verified_label_preimages_from_name_surface() -> Result<()> {
    let labelhashes = ["foo", "parent", "eth"]
        .into_iter()
        .map(|label| label_preimage_from_label(label, "unit", 1, json!({})))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|preimage| preimage.labelhash)
        .collect::<Vec<_>>();
    let surface = NameSurface {
        logical_name_id: "ens:foo.parent.eth".to_owned(),
        namespace: "ens".to_owned(),
        input_name: "foo.parent.eth".to_owned(),
        canonical_display_name: "foo.parent.eth".to_owned(),
        normalized_name: "foo.parent.eth".to_owned(),
        dns_encoded_name: Vec::new(),
        namehash: "0xnamehash".to_owned(),
        labelhashes,
        normalizer_version: "unit".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 1,
        provenance: json!({}),
        canonicality_state: CanonicalityState::Orphaned,
    };

    let preimages = label_preimages_from_name_surface(&surface)?;

    assert_eq!(preimages.len(), 3);
    assert_eq!(preimages[0].normalized_label, "foo");
    assert_eq!(preimages[0].source_kind, NAME_SURFACE_SOURCE_KIND);
    Ok(())
}

#[test]
fn skips_unverified_name_surface_labelhashes() -> Result<()> {
    let surface = NameSurface {
        logical_name_id: "ens:foo.eth".to_owned(),
        namespace: "ens".to_owned(),
        input_name: "foo.eth".to_owned(),
        canonical_display_name: "foo.eth".to_owned(),
        normalized_name: "foo.eth".to_owned(),
        dns_encoded_name: Vec::new(),
        namehash: "0xnamehash".to_owned(),
        labelhashes: vec!["labelhash:foo".to_owned()],
        normalizer_version: "unit".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 1,
        provenance: json!({}),
        canonicality_state: CanonicalityState::Canonical,
    };

    assert!(label_preimages_from_name_surface(&surface)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn imports_verified_ens_rainbow_rows_from_source_table() -> Result<()> {
    let database = test_database().await?;
    let valid = label_preimage_from_label("rainbow", "unit", 1, json!({}))?;

    sqlx::query(
        r#"
        CREATE TABLE ens_names (
            hash text NOT NULL,
            name text NOT NULL
        )
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO ens_names (hash, name)
        VALUES
            ($1, 'rainbow'),
            ('0x0000000000000000000000000000000000000000000000000000000000000000', 'wrong')
        "#,
    )
    .bind(&valid.labelhash)
    .execute(database.pool())
    .await?;

    let summary =
        import_label_preimages_from_ens_names_table(database.pool(), Some(1), None).await?;

    assert_eq!(summary.scanned_row_count, 2);
    assert_eq!(summary.retained_row_count, 1);
    assert_eq!(summary.invalidated_parent_count, 0);

    let rows = sqlx::query_as::<_, (String, String, String)>(
        r#"
        SELECT labelhash, normalized_label, source_kind
        FROM label_preimages
        ORDER BY labelhash
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![(
            valid.labelhash,
            "rainbow".to_owned(),
            ENS_RAINBOW_SOURCE_KIND.to_owned()
        )]
    );

    database.cleanup().await
}
