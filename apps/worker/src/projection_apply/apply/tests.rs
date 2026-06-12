use super::*;
use crate::projection_apply::apply_locks::invalidation_apply_lock_key;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_worker_projection_apply_claim_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for projection apply claim tests")
            .admin_connect_context("failed to connect admin pool for projection apply claim tests")
            .pool_connect_context("failed to connect projection apply claim test pool"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for projection apply claim tests",
    )
    .await
}

#[tokio::test]
async fn stale_projection_invalidation_claims_are_reclaimed() -> Result<()> {
    let database = test_database().await?;
    let stale_token = Uuid::new_v4();
    let new_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:stale.eth",
        stale_token,
        "10 minutes",
    )
    .await?;

    let claimed = claim_pending_invalidations(database.pool(), 10, new_token).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].projection, "name_current");
    assert_eq!(claimed[0].projection_key, "ens:stale.eth");
    assert_eq!(claimed[0].claim_token, new_token);

    let (claim_token, attempt_count): (Uuid, i64) = sqlx::query_as(
        r#"
        SELECT claim_token, attempt_count
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:stale.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load reclaimed projection invalidation")?;
    assert_eq!(claim_token, new_token);
    assert_eq!(
        attempt_count, 0,
        "claim recovery is not a failed projection apply attempt"
    );

    database.cleanup().await
}

#[tokio::test]
async fn reinvalidation_after_dead_letter_preserves_dead_letter_history() -> Result<()> {
    let database = test_database().await?;

    insert_poison_invalidation(&database, "poisoned-key", 4).await?;
    apply_pending_invalidations(database.pool(), 10, None).await?;

    insert_poison_invalidation(&database, "poisoned-key", 4).await?;
    apply_pending_invalidations(database.pool(), 10, None).await?;

    let dead_letters = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT generation, attempt_count
        FROM projection_invalidation_dead_letters
        WHERE projection = 'unsupported_projection'
          AND projection_key = 'poisoned-key'
        ORDER BY generation
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to load projection invalidation dead-letter history")?;

    assert_eq!(
        dead_letters,
        vec![(0, 5), (1, 5)],
        "each dead-lettered generation must remain operator-visible"
    );

    database.cleanup().await
}

#[tokio::test]
async fn fresh_projection_invalidation_claims_are_not_reclaimed() -> Result<()> {
    let database = test_database().await?;
    let fresh_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:fresh.eth",
        fresh_token,
        "1 minute",
    )
    .await?;

    let claimed = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4()).await?;
    assert!(claimed.is_empty());

    let claim_token: Uuid = sqlx::query_scalar(
        r#"
        SELECT claim_token
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = 'ens:fresh.eth'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load fresh projection invalidation")?;
    assert_eq!(claim_token, fresh_token);

    database.cleanup().await
}

#[tokio::test]
async fn repeated_failures_move_projection_invalidation_to_dead_letter() -> Result<()> {
    let database = test_database().await?;

    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            attempt_count
        )
        VALUES (
            'unsupported_projection',
            'poisoned-key',
            '{}'::jsonb,
            4
        )
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to seed poison projection invalidation")?;

    let summary = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(
        summary,
        ProjectionInvalidationApplySummary {
            claimed_invalidation_count: 1,
            applied_invalidation_count: 0,
            failed_invalidation_count: 1,
        }
    );

    let queue_row_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE projection = 'unsupported_projection'
          AND projection_key = 'poisoned-key'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to count live projection invalidation rows after dead-letter")?;
    assert_eq!(
        queue_row_count, 0,
        "dead-lettered invalidations must leave the live queue"
    );

    let (state, attempt_count, dead_lettered_at, failure_reason): (
        String,
        i64,
        Option<sqlx::types::time::OffsetDateTime>,
        Option<String>,
    ) = sqlx::query_as(
        r#"
        SELECT
            state::TEXT AS state,
            attempt_count,
            dead_lettered_at,
            last_failure_reason
        FROM projection_invalidation_dead_letters
        WHERE projection = 'unsupported_projection'
          AND projection_key = 'poisoned-key'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load dead-lettered projection invalidation")?;
    assert_eq!(state, "dead_letter");
    assert_eq!(attempt_count, 5);
    assert!(dead_lettered_at.is_some());
    assert!(
        failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("unsupported projection invalidation family"))
    );

    let indexing_status = bigname_storage::load_indexing_status(database.pool()).await?;
    assert!(
        !indexing_status.has_unscoped_pending_invalidations,
        "dead-lettered invalidations must not poison indexing status"
    );

    let claimed = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4()).await?;
    assert!(
        claimed.is_empty(),
        "dead-lettered invalidations must not be claimed again"
    );

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_claim_heartbeat_refreshes_claimed_at() -> Result<()> {
    let database = test_database().await?;
    let claim_token = Uuid::new_v4();

    insert_claimed_invalidation(
        &database,
        "address_names_current",
        "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
        claim_token,
        "4 minutes",
    )
    .await?;

    let before: sqlx::types::time::OffsetDateTime = sqlx::query_scalar(
        r#"
        SELECT claimed_at
        FROM projection_invalidations
        WHERE projection = 'address_names_current'
          AND projection_key = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load initial claim timestamp")?;

    let invalidation = ClaimedInvalidation {
        projection: "address_names_current".to_owned(),
        projection_key: "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401".to_owned(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token,
        attempt_count: 0,
    };
    refresh_claimed_invalidation_claim(database.pool(), &invalidation).await?;

    let (after, refreshed_token): (sqlx::types::time::OffsetDateTime, Uuid) = sqlx::query_as(
        r#"
            SELECT claimed_at, claim_token
            FROM projection_invalidations
            WHERE projection = 'address_names_current'
              AND projection_key = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
            "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load refreshed claim timestamp")?;

    assert!(after > before);
    assert_eq!(refreshed_token, claim_token);

    database.cleanup().await
}

#[tokio::test]
async fn projection_invalidation_apply_locks_serialize_same_key() -> Result<()> {
    let database = test_database().await?;
    let invalidation = ClaimedInvalidation {
        projection: "permissions_current".to_owned(),
        projection_key: Uuid::new_v4().to_string(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token: Uuid::new_v4(),
        attempt_count: 0,
    };

    let mut first_lock =
        acquire_invalidation_apply_locks(database.pool(), std::slice::from_ref(&invalidation))
            .await?;
    let lock_key = invalidation_apply_lock_key(&invalidation);
    let mut second_conn = database
        .pool()
        .acquire()
        .await
        .context("failed to acquire second lock probe connection")?;

    let acquired_while_locked: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(&lock_key)
            .fetch_one(&mut *second_conn)
            .await
            .context("failed to probe held projection invalidation apply lock")?;
    assert!(!acquired_while_locked);

    release_invalidation_apply_locks(&mut first_lock).await?;
    drop(first_lock);
    let acquired_after_release: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(&lock_key)
            .fetch_one(&mut *second_conn)
            .await
            .context("failed to acquire released projection invalidation apply lock")?;
    assert!(acquired_after_release);
    let released_probe_lock: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(&lock_key)
            .fetch_one(&mut *second_conn)
            .await
            .context("failed to release second projection invalidation apply lock")?;
    assert!(released_probe_lock);
    drop(second_conn);

    database.cleanup().await
}

#[tokio::test]
async fn basenames_name_current_invalidations_are_claimed_before_older_ens_names() -> Result<()> {
    let database = test_database().await?;
    insert_unclaimed_invalidation(&database, "name_current", "ens:older.eth").await?;
    insert_unclaimed_invalidation(&database, "name_current", "basenames:newer.base.eth").await?;
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET last_changed_at = now() - '10 minutes'::INTERVAL
        WHERE projection = 'name_current'
          AND projection_key = 'ens:older.eth'
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to age ENS projection invalidation")?;

    let claimed = claim_pending_invalidations(database.pool(), 1, Uuid::new_v4()).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].projection, "name_current");
    assert_eq!(claimed[0].projection_key, "basenames:newer.base.eth");

    database.cleanup().await
}

#[test]
fn claimed_invalidation_apply_order_prioritizes_basenames_name_current() {
    let mut invalidations = vec![
        claimed_invalidation("name_current", "ens:later.eth"),
        claimed_invalidation("permissions_current", "permissions:example"),
        claimed_invalidation("name_current", "basenames:base-name.base.eth"),
        claimed_invalidation("name_current", "ens:earlier.eth"),
    ];

    sort_claimed_invalidations_for_apply(&mut invalidations);

    let ordered_keys = invalidations
        .iter()
        .map(|invalidation| invalidation.projection_key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_keys,
        vec![
            "basenames:base-name.base.eth",
            "ens:earlier.eth",
            "ens:later.eth",
            "permissions:example"
        ]
    );
}

#[test]
fn address_name_invalidations_are_grouped_by_address() {
    let mut invalidations = vec![
        address_names_claimed_invalidation(
            "0x1111111111111111111111111111111111111111",
            Some("ens:first.eth"),
        ),
        address_names_claimed_invalidation(
            "0x1111111111111111111111111111111111111111",
            Some("ens:second.eth"),
        ),
        address_names_claimed_invalidation(
            "0x2222222222222222222222222222222222222222",
            Some("ens:third.eth"),
        ),
        claimed_invalidation("primary_names_current", "primary:example"),
    ];

    let group = drain_address_names_group(&mut invalidations);

    assert_eq!(group.len(), 2);
    assert_eq!(
        group
            .iter()
            .map(|invalidation| {
                payload_str(&invalidation.key_payload, "logical_name_id").unwrap()
            })
            .collect::<Vec<_>>(),
        vec!["ens:first.eth", "ens:second.eth"]
    );
    assert_eq!(invalidations.len(), 2);
    assert_eq!(
        payload_str(&invalidations[0].key_payload, "address").unwrap(),
        "0x2222222222222222222222222222222222222222"
    );
}

#[tokio::test]
async fn address_name_default_payload_invalidation_rebuilds_by_projection_key() -> Result<()> {
    let database = test_database().await?;
    let address = "0x3333333333333333333333333333333333333333";

    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            last_changed_at,
            invalidated_at
        )
        VALUES (
            'address_names_current',
            $1,
            '{}'::jsonb,
            now(),
            now()
        )
        "#,
    )
    .bind(address)
    .execute(database.pool())
    .await
    .context("failed to seed broad address_names_current invalidation")?;

    let summary = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(
        summary,
        ProjectionInvalidationApplySummary {
            claimed_invalidation_count: 1,
            applied_invalidation_count: 1,
            failed_invalidation_count: 0,
        }
    );

    let remaining_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE projection = 'address_names_current'
          AND projection_key = $1
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to count remaining broad address_names_current invalidation")?;
    assert_eq!(remaining_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn primary_name_repair_invalidations_delete_old_tuple_and_rebuild_new_tuple() -> Result<()> {
    let database = test_database().await?;
    let address = "0x7e50c29692e8d701a375bf53de93b62f9aa47af8";

    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            claim_provenance,
            normalized_claim_name
        )
        VALUES ($1, '60', 'basenames', 'success', NULL, '{}'::jsonb, 'old.base.eth')
        "#,
    )
    .bind(address)
    .execute(database.pool())
    .await
    .context("failed to seed stale old primary-name tuple")?;

    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state,
            observed_at
        )
        VALUES (
            'projection-apply:base-primary-repair-reverse',
            'basenames',
            NULL,
            NULL,
            'ReverseChanged',
            'basenames_base_primary',
            1,
            'base-mainnet',
            100,
            '0xbase100',
            '{}'::jsonb,
            'ens_v1_reverse_claim',
            'canonical'::canonicality_state,
            '{}'::jsonb,
            $1,
            now()
        ),
        (
            'projection-apply:base-primary-repair-name',
            'basenames',
            NULL,
            NULL,
            'RecordChanged',
            'basenames_base_primary',
            1,
            'base-mainnet',
            100,
            '0xbase100',
            '{}'::jsonb,
            'ens_v1_reverse_claim',
            'canonical'::canonicality_state,
            '{}'::jsonb,
            $2,
            now()
        )
        "#,
    )
    .bind(serde_json::json!({
        "address": address,
        "namespace": "basenames",
        "coin_type": "2147492101",
        "claim_provenance": {
            "contract_role": "reverse_registrar",
            "source_family": "basenames_base_primary",
            "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664"
        }
    }))
    .bind(serde_json::json!({
        "record_key": "name",
        "raw_name": "fixed.base.eth",
        "primary_claim_source": {
            "address": address,
            "namespace": "basenames",
            "coin_type": "2147492101",
            "reverse_name": "7e50c29692e8d701a375bf53de93b62f9aa47af8.80002105.reverse",
            "reverse_node": "0x76097049b6146b77e9cd73ee786c29ae4eefb49e4772d0a3cefd99f7087760c5",
            "claim_provenance": {
                "contract_role": "reverse_registrar",
                "source_family": "basenames_base_primary",
                "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664"
            }
        }
    }))
    .execute(database.pool())
    .await
    .context("failed to seed corrected primary-name normalized events")?;

    insert_primary_invalidation(&database, address, "basenames", "60").await?;
    insert_primary_invalidation(&database, address, "basenames", "2147492101").await?;

    let summary = apply_pending_invalidations(database.pool(), 10, None).await?;
    assert_eq!(summary.claimed_invalidation_count, 2);
    assert_eq!(summary.applied_invalidation_count, 2);
    assert_eq!(summary.failed_invalidation_count, 0);

    let old_tuple_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM primary_names_current
        WHERE address = $1
          AND namespace = 'basenames'
          AND coin_type = '60'
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to count stale old primary-name tuple")?;
    assert_eq!(old_tuple_count, 0);

    let new_claim = sqlx::query_scalar::<_, String>(
        r#"
        SELECT normalized_claim_name
        FROM primary_names_current
        WHERE address = $1
          AND namespace = 'basenames'
          AND coin_type = '2147492101'
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to load rebuilt corrected primary-name tuple")?;
    assert_eq!(new_claim, "fixed.base.eth");

    database.cleanup().await
}

fn claimed_invalidation(projection: &str, projection_key: &str) -> ClaimedInvalidation {
    ClaimedInvalidation {
        projection: projection.to_string(),
        projection_key: projection_key.to_string(),
        key_payload: Value::Object(Default::default()),
        generation: 0,
        claim_token: Uuid::nil(),
        attempt_count: 0,
    }
}

fn address_names_claimed_invalidation(
    address: &str,
    logical_name_id: Option<&str>,
) -> ClaimedInvalidation {
    let projection_key = match logical_name_id {
        Some(logical_name_id) => format!("{address}:{logical_name_id}"),
        None => address.to_owned(),
    };
    let key_payload = match logical_name_id {
        Some(logical_name_id) => {
            serde_json::json!({ "address": address, "logical_name_id": logical_name_id })
        }
        None => serde_json::json!({ "address": address }),
    };
    ClaimedInvalidation {
        projection: "address_names_current".to_owned(),
        projection_key,
        key_payload,
        generation: 0,
        claim_token: Uuid::nil(),
        attempt_count: 0,
    }
}

async fn insert_unclaimed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ($1, $2, '{}'::jsonb)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .execute(database.pool())
    .await
    .context("failed to insert projection invalidation")?;

    Ok(())
}

async fn insert_primary_invalidation(
    database: &TestDatabase,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES (
            'primary_names_current',
            $1 || ':' || $2 || ':' || $3,
            jsonb_build_object(
                'address', $1,
                'namespace', $2,
                'coin_type', $3
            )
        )
        "#,
    )
    .bind(address)
    .bind(namespace)
    .bind(coin_type)
    .execute(database.pool())
    .await
    .context("failed to insert primary-name projection invalidation")?;

    Ok(())
}

async fn insert_poison_invalidation(
    database: &TestDatabase,
    projection_key: &str,
    attempt_count: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            attempt_count
        )
        VALUES (
            'unsupported_projection',
            $1,
            '{}'::jsonb,
            $2
        )
        "#,
    )
    .bind(projection_key)
    .bind(attempt_count)
    .execute(database.pool())
    .await
    .context("failed to insert poison projection invalidation")?;

    Ok(())
}

async fn insert_claimed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
    claim_token: Uuid,
    claim_age: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            claim_token,
            claimed_at
        )
        VALUES ($1, $2, '{}'::jsonb, $3, now() - $4::INTERVAL)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .bind(claim_token)
    .bind(claim_age)
    .execute(database.pool())
    .await
    .context("failed to insert claimed projection invalidation")?;

    Ok(())
}
