use super::*;

pub(crate) async fn close_weaker_overlapping_existing_surface_bindings(
    pool: &PgPool,
    incoming: &[SurfaceBinding],
) -> Result<usize> {
    let repairable = incoming
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
        .collect::<Vec<_>>();
    if repairable.is_empty() {
        return Ok(0);
    }

    let mut surface_binding_ids = Vec::with_capacity(repairable.len());
    let mut logical_name_ids = Vec::with_capacity(repairable.len());
    let mut active_froms = Vec::with_capacity(repairable.len());
    let mut authority_ranks = Vec::with_capacity(repairable.len());
    for binding in repairable {
        surface_binding_ids.push(binding.surface_binding_id);
        logical_name_ids.push(binding.logical_name_id.clone());
        active_froms.push(binding.active_from);
        authority_ranks.push(i16::from(surface_binding_authority_rank(binding)));
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open overlapping surface-binding closure transaction")?;
    let rows = sqlx::query_as::<_, (Uuid, String)>(
        r#"
        WITH incoming(
            surface_binding_id,
            logical_name_id,
            active_from,
            authority_rank
        ) AS (
            SELECT *
            FROM unnest(
                $1::UUID[],
                $2::TEXT[],
                $3::TIMESTAMPTZ[],
                $4::SMALLINT[]
            )
        ),
        candidate AS (
            SELECT
                surface_bindings.surface_binding_id,
                MIN(incoming.active_from) AS close_at
            FROM surface_bindings
            JOIN incoming
              ON incoming.logical_name_id = surface_bindings.logical_name_id
             AND incoming.surface_binding_id <> surface_bindings.surface_binding_id
             AND surface_bindings.active_from < incoming.active_from
             AND (
                 surface_bindings.active_to IS NULL
                 OR surface_bindings.active_to > incoming.active_from
             )
            WHERE surface_bindings.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND surface_bindings.provenance->>'adapter' = $5
              AND (
                  CASE surface_bindings.provenance->>'authority_kind'
                      WHEN 'wrapper' THEN 3
                      WHEN 'registrar' THEN 2
                      WHEN 'registry_only' THEN 1
                      ELSE 0
                  END
              ) <= incoming.authority_rank
            GROUP BY surface_bindings.surface_binding_id
        )
        UPDATE surface_bindings
        SET
            active_to = candidate.close_at,
            observed_at = now()
        FROM candidate
        WHERE surface_bindings.surface_binding_id = candidate.surface_binding_id
          AND surface_bindings.active_from < candidate.close_at
          AND (
              surface_bindings.active_to IS NULL
              OR surface_bindings.active_to > candidate.close_at
          )
        RETURNING
            surface_bindings.surface_binding_id,
            surface_bindings.logical_name_id
        "#,
    )
    .bind(&surface_binding_ids)
    .bind(&logical_name_ids)
    .bind(&active_froms)
    .bind(&authority_ranks)
    .bind(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
    .fetch_all(transaction.as_mut())
    .await
    .context("failed to close weaker overlapping surface bindings before authority replay")?;

    if !rows.is_empty() {
        let invalidated_logical_name_ids = rows
            .iter()
            .map(|(_, logical_name_id)| logical_name_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        queue_surface_binding_projection_invalidations(
            transaction.as_mut(),
            &invalidated_logical_name_ids,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit overlapping surface-binding closure transaction")?;
        tracing::warn!(
            adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            repaired_surface_binding_overlap_count = rows.len(),
            "closed weaker overlapping surface bindings before authority replay"
        );
    }

    Ok(rows.len())
}
