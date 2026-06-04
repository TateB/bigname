use anyhow::{Context, Result};
use sqlx::postgres::PgConnection;

pub(super) async fn queue_surface_binding_projection_invalidations(
    executor: &mut PgConnection,
    logical_name_ids: &[String],
) -> Result<()> {
    if logical_name_ids.is_empty() {
        return Ok(());
    }

    queue_name_current_invalidations(executor, logical_name_ids).await?;
    queue_address_names_current_invalidations(executor, logical_name_ids).await
}

async fn queue_name_current_invalidations(
    executor: &mut PgConnection,
    logical_name_ids: &[String],
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            last_changed_at,
            invalidated_at
        )
        SELECT
            'name_current'::TEXT AS projection,
            logical_name_id AS projection_key,
            jsonb_build_object('logical_name_id', logical_name_id) AS key_payload,
            now() AS last_changed_at,
            now() AS invalidated_at
        FROM unnest($1::TEXT[]) AS input(logical_name_id)
        WHERE btrim(logical_name_id) <> ''
        GROUP BY logical_name_id
        ON CONFLICT (projection, projection_key)
        DO UPDATE SET
            key_payload = EXCLUDED.key_payload,
            generation = projection_invalidations.generation + 1,
            last_changed_at = GREATEST(
                projection_invalidations.last_changed_at,
                EXCLUDED.last_changed_at
            ),
            invalidated_at = EXCLUDED.invalidated_at,
            claim_token = NULL,
            claimed_at = NULL,
            last_failure_reason = NULL,
            last_failure_at = NULL
        "#,
    )
    .bind(logical_name_ids)
    .execute(&mut *executor)
    .await
    .context("failed to queue name_current invalidations for surface-binding repair")?;

    Ok(())
}

async fn queue_address_names_current_invalidations(
    executor: &mut PgConnection,
    logical_name_ids: &[String],
) -> Result<()> {
    sqlx::query(
        r#"
        WITH affected_names AS (
            SELECT DISTINCT logical_name_id
            FROM unnest($1::TEXT[]) AS input(logical_name_id)
            WHERE btrim(logical_name_id) <> ''
        ),
        projected_addresses AS (
            SELECT DISTINCT
                lower(address) AS address,
                logical_name_id
            FROM address_names_current
            WHERE logical_name_id IN (
                SELECT logical_name_id FROM affected_names
            )
        ),
        event_addresses AS (
            SELECT DISTINCT
                lower(address.address) AS address,
                ne.logical_name_id
            FROM normalized_events ne
            JOIN affected_names affected
              ON affected.logical_name_id = ne.logical_name_id
            CROSS JOIN LATERAL (
                VALUES
                    (ne.after_state ->> 'registrant'),
                    (ne.before_state ->> 'registrant'),
                    (ne.after_state ->> 'to'),
                    (ne.before_state ->> 'to'),
                    (ne.after_state ->> 'owner'),
                    (ne.before_state ->> 'owner')
            ) AS address(address)
            WHERE ne.event_kind IN (
                'RegistrationGranted',
                'TokenControlTransferred',
                'AuthorityTransferred',
                'AuthorityEpochChanged',
                'TokenRegenerated'
            )
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND address.address IS NOT NULL
              AND address.address <> ''

            UNION

            SELECT DISTINCT
                lower(address.address) AS address,
                ne.logical_name_id
            FROM normalized_events ne
            JOIN affected_names affected
              ON affected.logical_name_id = ne.logical_name_id
            CROSS JOIN LATERAL (
                VALUES
                    (ne.after_state ->> 'subject', ne.after_state -> 'scope'),
                    (ne.before_state ->> 'subject', ne.before_state -> 'scope')
            ) AS address(address, scope)
            WHERE ne.event_kind = 'PermissionChanged'
              AND address.scope ->> 'kind' = 'resource'
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND address.address IS NOT NULL
              AND address.address <> ''
        ),
        candidate_keys AS (
            SELECT address, logical_name_id
            FROM projected_addresses

            UNION

            SELECT address, logical_name_id
            FROM event_addresses
        )
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            last_changed_at,
            invalidated_at
        )
        SELECT
            'address_names_current'::TEXT AS projection,
            address || ':' || logical_name_id AS projection_key,
            jsonb_build_object(
                'address', address,
                'logical_name_id', logical_name_id
            ) AS key_payload,
            now() AS last_changed_at,
            now() AS invalidated_at
        FROM candidate_keys
        WHERE btrim(address) <> ''
          AND btrim(logical_name_id) <> ''
        GROUP BY address, logical_name_id
        ON CONFLICT (projection, projection_key)
        DO UPDATE SET
            key_payload = EXCLUDED.key_payload,
            generation = projection_invalidations.generation + 1,
            last_changed_at = GREATEST(
                projection_invalidations.last_changed_at,
                EXCLUDED.last_changed_at
            ),
            invalidated_at = EXCLUDED.invalidated_at,
            claim_token = NULL,
            claimed_at = NULL,
            last_failure_reason = NULL,
            last_failure_at = NULL
        "#,
    )
    .bind(logical_name_ids)
    .execute(&mut *executor)
    .await
    .context("failed to queue address_names_current invalidations for surface-binding repair")?;

    Ok(())
}
