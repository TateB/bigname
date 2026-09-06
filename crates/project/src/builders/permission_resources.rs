use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn build_registry_binding(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH latest_registry_observation AS (
            SELECT DISTINCT ON (COALESCE(event.logical_name_id, event.resource_id::text))
                   COALESCE(current_name.resource_id, event.resource_id) AS resource_id,
                   event.normalized_event_id,
                   CASE WHEN event.event_kind = 'SurfaceUnbound' THEN NULL
                        ELSE lower(event.after_state ->> 'owner_getter') END AS registry_owner,
                   lower(CASE WHEN event.source_family IN (
                                      'ens_v1_registrar_l1', 'basenames_base_registrar'
                                  ) OR (event.event_kind = 'SurfaceBound'
                                      AND event.after_state @> '{"state_derived":true,"authority_kind":"registry_only"}')
                                   THEN event.after_state ->> 'registry_contract'
                              ELSE COALESCE(event.raw_fact_ref ->> 'emitting_address',
                                            event.after_state ->> 'registry_contract') END)
                       AS registry_contract,
                   jsonb_build_object(
                       'normalized_event_ids', jsonb_build_array(event.normalized_event_id),
                       'raw_fact_ref', event.raw_fact_ref,
                       'chain_id', event.chain_id,
                       'derivation_kind', 'registry_owner_binding_rebuild'
                   ) AS provenance,
                   jsonb_build_object(
                       'block_number', event.block_number,
                       'block_hash', event.block_hash,
                       'transaction_index', event.transaction_index,
                       'log_index', event.log_index
                   ) AS chain_positions,
                   event.manifest_version
            FROM project_events event
            LEFT JOIN project_stage_name_current current_name
              ON event.event_kind IN ('AuthorityTransferred', 'SubregistryChanged')
             AND current_name.logical_name_id = event.logical_name_id
             AND current_name.provenance #>> '{authority_selection,authority_arm}'
                 IN ('ens_v1', 'basenames')
            WHERE event.event_kind IN (
                      'AuthorityTransferred', 'SubregistryChanged', 'SurfaceBound', 'SurfaceUnbound'
                  )
              AND (event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
                   OR (event.event_kind IN ('SurfaceBound', 'SurfaceUnbound')
                       AND event.source_family IN (
                           'ens_v1_registrar_l1', 'basenames_base_registrar'
                       )))
              AND event.resource_id IS NOT NULL
            ORDER BY COALESCE(event.logical_name_id, event.resource_id::text),
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ), latest_registry_resource AS (
            SELECT DISTINCT ON (resource_id) * FROM latest_registry_observation
            ORDER BY resource_id, (chain_positions ->> 'block_number')::bigint DESC NULLS LAST,
                     (chain_positions ->> 'transaction_index')::bigint DESC NULLS LAST,
                     (chain_positions ->> 'log_index')::bigint DESC NULLS LAST,
                     normalized_event_id DESC
        ), classified AS (
            SELECT observation.*,
                   observation.registry_owner ~ '^0x[0-9a-f]{40}$'
                   AND observation.registry_owner <>
                       '0x0000000000000000000000000000000000000000'
                   AND observation.registry_contract ~ '^0x[0-9a-f]{40}$'
                       AS applicable
            FROM latest_registry_resource observation
        )
        UPDATE project_stage_permissions_current_resource_summary summary
        SET registry_owner = CASE WHEN binding.applicable THEN binding.registry_owner END,
            registry_contract = CASE WHEN binding.applicable THEN binding.registry_contract END,
            registry_binding_provenance = CASE WHEN binding.applicable THEN binding.provenance END,
            registry_binding_chain_positions = CASE WHEN binding.applicable THEN binding.chain_positions END,
            provenance = (summary.provenance - 'registry_binding_clear_event_id') ||
                CASE WHEN binding.applicable THEN '{}'::jsonb ELSE jsonb_build_object(
                    'registry_binding_clear_event_id', binding.normalized_event_id) END,
            manifest_version = greatest(summary.manifest_version, binding.manifest_version)
        FROM classified binding
        WHERE summary.resource_id = binding.resource_id
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build registry-owner bindings", error))?;
    Ok(())
}
