use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    // Inventory ranks each resource's ResolverChanged events only after joining staged readable
    // surfaces, so an earlier event may win when a later event's name has no such surface. Once
    // selected, only that resolver contributes the boundary, selectors, and entries; a selected
    // clear suppresses the inventory row.
    sqlx::query(
        r#"
        WITH latest_pointers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   event.logical_name_id,
                   event.namespace AS pointer_namespace,
                   event.source_family AS pointer_source_family,
                   lower(surface.namehash) AS namehash,
                   lower(event.after_state ->> 'resolver') AS resolver_address,
                   event.manifest_version AS pointer_manifest_version,
                   event.normalized_event_id AS pointer_event_id,
                   event.block_number AS pointer_block_number,
                   event.block_hash AS pointer_block_hash
            FROM project_events event
            JOIN project_surfaces surface USING (logical_name_id)
            WHERE event.event_kind = 'ResolverChanged'
              AND event.resource_id IS NOT NULL
              AND event.logical_name_id IS NOT NULL
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        pointers AS (
            SELECT * FROM latest_pointers
            WHERE resolver_address IS NOT NULL
              AND resolver_address NOT IN (
                  '0x0000000000000000000000000000000000000000', ''
              )
        ),
        pointer_eligibility AS (
            SELECT pointer.resource_id,
                   COALESCE(
                       resolver.support_status = 'supported'
                       AND NOT (
                           resolver.declared_summary #>> '{classification,basis}' =
                               'manifest_declared_address'
                           AND declaration_manifest.manifest_id IS NULL
                       ),
                       false
                   ) AS supported,
                   CASE
                       WHEN resolver.resolver_address IS NULL
                           THEN 'resolver_classification_missing'
                       WHEN resolver.support_status IS DISTINCT FROM 'supported'
                           THEN COALESCE(
                               resolver.unsupported_reason,
                               'resolver_classification_missing'
                           )
                       WHEN resolver.declared_summary #>> '{classification,basis}' =
                            'manifest_declared_address'
                        AND declaration_manifest.manifest_id IS NULL
                           THEN 'resolver_classification_missing'
                       ELSE NULL
                   END AS unsupported_reason
            FROM pointers pointer
            LEFT JOIN project_stage_resolver_current resolver
              ON resolver.chain_id = $1
             AND resolver.resolver_address = pointer.resolver_address
            LEFT JOIN project_manifests declaration_manifest
              ON declaration_manifest.manifest_id =
                 (resolver.provenance ->> 'manifest_id')::bigint
             AND declaration_manifest.namespace = pointer.pointer_namespace
        ),
        attributed_events AS (
            SELECT pointer.resource_id AS attributed_resource_id,
                   pointer.pointer_source_family AS attributed_pointer_source_family,
                   event.*
            FROM pointers pointer
            JOIN project_events event
              ON event.logical_name_id = pointer.logical_name_id
             AND lower(COALESCE(
                    NULLIF(event.after_state ->> 'resolver', ''),
                    NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
                 )) = pointer.resolver_address
            WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
            UNION ALL
            -- ENSv1 reads the registry's current resolver and then reads node-keyed resolver
            -- storage, independent of write time.
            -- (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f)
            SELECT pointer.resource_id AS attributed_resource_id,
                   pointer.pointer_source_family AS attributed_pointer_source_family,
                   event.*
            FROM pointers pointer
             JOIN project_events event
               ON event.chain_id = $1
              AND event.logical_name_id IS NULL
              AND lower(event.after_state ->> 'node') = pointer.namehash
              AND lower(COALESCE(
                    NULLIF(event.after_state ->> 'resolver', ''),
                    NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
                 )) = pointer.resolver_address
            WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
              AND (
                  (
                      event.source_family = 'ens_v1_resolver_l1'
                      AND pointer.pointer_source_family IN (
                          'ens_v1_registry_l1',
                          'ens_v1_registrar_l1',
                          'ens_v1_wrapper_l1'
                      )
                  )
                  OR (
                      event.source_family = 'basenames_base_resolver'
                      AND pointer.pointer_source_family =
                          'basenames_base_registry'
                  )
              )
            UNION ALL
            -- The guarded ENSv2-origin exception uses the exact declaration already selected by
            -- resolver classification and applies only to pointers in that declaration's namespace.
            SELECT pointer.resource_id AS attributed_resource_id,
                   pointer.pointer_source_family AS attributed_pointer_source_family,
                   event.*
            FROM pointers pointer
            JOIN project_stage_resolver_current resolver
              ON resolver.chain_id = $1
             AND resolver.resolver_address = pointer.resolver_address
             AND resolver.support_status = 'supported'
             AND resolver.declared_summary #>> '{classification,source_family}' =
                 'ens_v1_resolver_l1'
             AND resolver.declared_summary #>> '{classification,basis}' =
                 'manifest_declared_address'
            JOIN project_manifests declaration_manifest
              ON declaration_manifest.manifest_id =
                 (resolver.provenance ->> 'manifest_id')::bigint
             AND declaration_manifest.namespace = pointer.pointer_namespace
            JOIN project_events event
              ON event.chain_id = $1
             AND event.logical_name_id IS NULL
             AND event.source_family = 'ens_v1_resolver_l1'
             AND lower(event.after_state ->> 'node') = pointer.namehash
             AND lower(COALESCE(
                    NULLIF(event.after_state ->> 'resolver', ''),
                    NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
                 )) = pointer.resolver_address
            WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
              AND pointer.pointer_source_family IN (
                  'ens_v2_registry_l1', 'ens_v2_root_l1'
              )
        ),
        ranked_versions AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.attributed_resource_id
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS version_rank
            FROM attributed_events event
            WHERE event.event_kind = 'RecordVersionChanged'
        ),
        versions AS (
            SELECT * FROM ranked_versions WHERE version_rank = 1
        ),
        eligible_records AS (
            SELECT event.*,
                   event.after_state ->> 'record_family' = 'addr'
                   AND event.after_state ->> 'selector_key' = '60'
                   AND event.after_state ->> 'source_event' = 'AddressChanged'
                   AND event.log_index IS NOT NULL
                   AND EXISTS (
                       SELECT 1
                       FROM attributed_events sibling
                       WHERE sibling.attributed_resource_id = event.attributed_resource_id
                         AND sibling.chain_id = event.chain_id
                         AND sibling.block_number = event.block_number
                         AND sibling.transaction_hash IS NOT DISTINCT FROM event.transaction_hash
                         AND sibling.transaction_index IS NOT DISTINCT FROM event.transaction_index
                         AND sibling.log_index = event.log_index + 1
                         AND sibling.event_kind = 'RecordChanged'
                         AND sibling.after_state ->> 'record_key' = 'addr:60'
                         AND sibling.after_state ->> 'source_event' = 'AddrChanged'
                   ) AS coin60_compatibility_source,
                   (
                       (
                           event.source_family = 'ens_v1_resolver_l1'
                           AND event.attributed_pointer_source_family IN (
                               'ens_v1_registry_l1',
                               'ens_v1_registrar_l1',
                               'ens_v1_wrapper_l1'
                           )
                       )
                       OR (
                           event.source_family = 'basenames_base_resolver'
                           AND event.attributed_pointer_source_family =
                               'basenames_base_registry'
                       )
                   )
                   AND event.after_state ->> 'record_key' = 'addr:60'
                   AND event.after_state ->> 'record_family' = 'addr'
                   AND event.after_state ->> 'selector_key' = '60'
                   AND lower(COALESCE(
                       event.after_state #>> '{value,bytes}',
                       event.after_state ->> 'value'
                   )) = '0x0000000000000000000000000000000000000000'
                       AS coin60_zero_address_is_absent
            FROM attributed_events event
            LEFT JOIN versions version USING (attributed_resource_id)
            WHERE event.event_kind = 'RecordChanged'
              AND (
                  version.normalized_event_id IS NULL
                  OR ROW(
                      event.block_number,
                      COALESCE(event.transaction_index, -1),
                      COALESCE(event.log_index, -1),
                      event.normalized_event_id
                  ) > ROW(
                      version.block_number,
                      COALESCE(version.transaction_index, -1),
                      COALESCE(version.log_index, -1),
                      version.normalized_event_id
                  )
              )
        ),
        ranked_records AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.attributed_resource_id,
                                    event.after_state ->> 'record_key'
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                -- setAddr(node, 60, bytes) emits AddressChanged before its
                                -- compatibility AddrChanged sibling. Prefer the storage-faithful
                                -- bytes payload only for that adjacent log pair, including empty
                                -- clears, without outranking a later write in the transaction.
                                -- (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f)
                                CASE WHEN event.coin60_compatibility_source
                                    THEN event.log_index + 1
                                    ELSE event.log_index END DESC NULLS LAST,
                                event.coin60_compatibility_source DESC,
                                event.normalized_event_id DESC
                   ) AS record_rank
            FROM eligible_records event
        ),
        current_records AS (
            SELECT * FROM ranked_records WHERE record_rank = 1
        ),
        record_rollups AS (
            SELECT event.attributed_resource_id AS resource_id,
                   jsonb_agg(DISTINCT event.after_state ->> 'record_key'
                       ORDER BY event.after_state ->> 'record_key')
                       FILTER (WHERE event.coin60_zero_address_is_absent) AS exact_nonempty_keys,
                   jsonb_agg(jsonb_build_object(
                       'record_key', event.after_state ->> 'record_key',
                       'record_family', event.after_state ->> 'record_family',
                       'selector_key', event.after_state -> 'selector_key',
                       'cacheable', true
                   ) ORDER BY event.after_state ->> 'record_key')
                   FILTER (WHERE
                       (
                           event.after_state ->> 'record_family' = 'text'
                           AND (
                               event.after_state ->> 'record_key' = 'text'
                               OR event.after_state ->> 'record_key' = concat(
                                   'text:', event.after_state ->> 'selector_key'
                               )
                           )
                       ) OR (
                           event.after_state ->> 'record_family' = 'addr'
                           AND event.after_state ->> 'selector_key' IS NOT NULL
                           AND event.after_state ->> 'record_key' = concat(
                               'addr:', event.after_state ->> 'selector_key'
                           )
                       ) OR (
                           event.after_state ->> 'record_family' = 'contenthash'
                           AND event.after_state ->> 'record_key' = 'contenthash'
                       )
                   ) AS selectors,
                   jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                       'record_key', event.after_state ->> 'record_key',
                       'record_family', event.after_state ->> 'record_family',
                       'selector_key', event.after_state -> 'selector_key',
                       'status', CASE
                           WHEN event.coin60_zero_address_is_absent THEN 'not_found'
                           WHEN event.after_state ? 'value' THEN CASE
                               WHEN event.after_state ->> 'record_family' = 'contenthash'
                                AND event.after_state ->> 'value' IN ('', '0x')
                                   THEN 'not_found'
                               WHEN event.after_state ->> 'record_family' = 'addr'
                                AND (
                                    event.after_state ->> 'value' IN ('', '0x')
                                    OR COALESCE(
                                        event.after_state #>> '{value,bytes}' IN ('', '0x'),
                                        false
                                    )
                                ) THEN 'not_found'
                               ELSE 'success'
                           END
                           WHEN event.after_state ? 'contenthash_hex' THEN CASE
                               WHEN event.after_state ->> 'contenthash_hex' IN ('', '0x')
                                   THEN 'not_found'
                               ELSE 'success'
                           END
                           WHEN event.after_state ? 'address_bytes_hex' THEN CASE
                               WHEN event.after_state ->> 'address_bytes_hex' IN ('', '0x')
                                   THEN 'not_found'
                               ELSE 'success'
                           END
                           ELSE 'unsupported'
                       END,
                       'value', CASE
                           WHEN event.coin60_zero_address_is_absent THEN NULL::jsonb
                           WHEN event.after_state ? 'value'
                            AND NOT (
                                (
                                    event.after_state ->> 'record_family' = 'contenthash'
                                    AND event.after_state ->> 'value' IN ('', '0x')
                                ) OR (
                                    event.after_state ->> 'record_family' = 'addr'
                                    AND (
                                        event.after_state ->> 'value' IN ('', '0x')
                                        OR COALESCE(
                                            event.after_state #>> '{value,bytes}' IN ('', '0x'),
                                            false
                                        )
                                    )
                                )
                            ) THEN event.after_state -> 'value'
                           WHEN event.after_state ? 'contenthash_hex'
                            AND event.after_state ->> 'contenthash_hex' NOT IN ('', '0x')
                               THEN jsonb_build_object(
                                   'encoding', 'hex',
                                   'bytes', event.after_state ->> 'contenthash_hex'
                               )
                           WHEN event.after_state ? 'address_bytes_hex'
                            AND event.after_state ->> 'address_bytes_hex' NOT IN ('', '0x')
                               THEN event.after_state -> 'address_bytes_hex'
                       END,
                       'unsupported_reason', CASE
                           WHEN NOT (event.after_state ? 'value')
                            AND NOT (event.after_state ? 'contenthash_hex')
                            AND NOT (event.after_state ? 'address_bytes_hex')
                               THEN 'value_not_retained_in_normalized_events'
                       END
                   )) ORDER BY event.after_state ->> 'record_key')
                   FILTER (WHERE event.after_state ->> 'record_family' IN (
                       'text', 'addr', 'contenthash'
                   )) AS entries,
                   COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                       'record_family', event.after_state ->> 'record_family',
                       'unsupported_reason',
                           'record_family_not_supported_in_phase6_projection'
                   ) ORDER BY jsonb_build_object(
                       'record_family', event.after_state ->> 'record_family',
                       'unsupported_reason',
                           'record_family_not_supported_in_phase6_projection'
                   )) FILTER (WHERE event.after_state ->> 'record_family' NOT IN (
                       'text', 'addr', 'contenthash'
                   )), '[]'::jsonb) AS unsupported_families,
                   jsonb_agg(to_jsonb(event.normalized_event_id)
                             ORDER BY event.normalized_event_id) AS event_ids,
                   max(event.block_number) AS block_number,
                   (array_agg(event.block_hash
                              ORDER BY event.block_number DESC,
                                       event.transaction_index DESC NULLS LAST,
                                       event.log_index DESC NULLS LAST,
                                       event.normalized_event_id DESC))[1] AS block_hash,
                   max(event.manifest_version) AS manifest_version,
                   (array_agg(jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_kind', event.event_kind,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       ))
                   ) ORDER BY event.block_number DESC NULLS LAST,
                              event.transaction_index DESC NULLS LAST,
                              event.log_index DESC NULLS LAST,
                              event.normalized_event_id DESC))[1] AS last_change
            FROM current_records event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            GROUP BY event.attributed_resource_id
        ),
        position_candidates AS (
            SELECT resource_id, pointer_block_number AS block_number,
                   pointer_block_hash AS block_hash
            FROM pointers
            UNION ALL
            SELECT attributed_resource_id, block_number, block_hash FROM versions
            UNION ALL
            SELECT resource_id, block_number, block_hash FROM record_rollups
        ),
        ranked_positions AS (
            SELECT candidate.*,
                   row_number() OVER (
                       PARTITION BY resource_id
                       ORDER BY block_number DESC, block_hash DESC
                   ) AS position_rank
            FROM position_candidates candidate
            WHERE block_number IS NOT NULL AND block_hash IS NOT NULL
        ),
        latest_positions AS (
            SELECT * FROM ranked_positions WHERE position_rank = 1
        )
        INSERT INTO project_stage_record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, last_change, entries,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT pointer.resource_id,
               concat(
                   octet_length(pointer.logical_name_id), ':',
                   pointer.logical_name_id, ';',
                   octet_length(pointer.resource_id::text), ':',
                   pointer.resource_id::text, ';',
                   octet_length(COALESCE(version.normalized_event_id::text, '')), ':',
                   COALESCE(version.normalized_event_id::text, ''), ';',
                   octet_length(CASE
                       WHEN version.normalized_event_id IS NOT NULL
                           THEN 'RecordVersionChanged'
                       ELSE ''
                   END), ':',
                   CASE WHEN version.normalized_event_id IS NOT NULL
                       THEN 'RecordVersionChanged' ELSE '' END, ';',
                   octet_length($1::text), ':', $1::text, ';',
                   octet_length(boundary.block_number::text), ':',
                   boundary.block_number::text, ';',
                   octet_length(boundary.block_hash), ':', boundary.block_hash, ';',
                   octet_length(to_jsonb(boundary.block_timestamp) #>> '{}'), ':',
                   to_jsonb(boundary.block_timestamp) #>> '{}', ';'
               ),
               jsonb_build_object(
                   'logical_name_id', pointer.logical_name_id,
                   'resource_id', pointer.resource_id,
                   'normalized_event_id', version.normalized_event_id,
                   'event_kind', CASE
                       WHEN version.normalized_event_id IS NOT NULL
                           THEN 'RecordVersionChanged'
                       ELSE NULL
                   END,
                   'chain_position', jsonb_strip_nulls(jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   ))
               ),
               COALESCE(records.selectors, '[]'::jsonb),
               COALESCE(records.unsupported_families, '[]'::jsonb) || CASE
                   WHEN eligibility.supported THEN '[]'::jsonb
                   ELSE jsonb_build_array(jsonb_build_object(
                       'record_family', 'resolver_classification',
                       'unsupported_reason', eligibility.unsupported_reason
                   ))
               END,
               COALESCE(records.last_change, jsonb_build_object(
                   'normalized_event_id', COALESCE(
                       version.normalized_event_id,
                       pointer.pointer_event_id
                   ),
                   'event_kind', CASE
                       WHEN version.normalized_event_id IS NOT NULL
                           THEN 'RecordVersionChanged'
                       ELSE 'ResolverChanged'
                   END,
                   'chain_position', jsonb_strip_nulls(jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   ))
               )),
               COALESCE(records.entries, '[]'::jsonb),
               CASE WHEN eligibility.supported
                   THEN 'supported' ELSE 'unsupported' END,
               eligibility.unsupported_reason,
               jsonb_build_object(
                   'chain_id', $1,
                   'logical_name_id', pointer.logical_name_id,
                   'resolver_address', pointer.resolver_address,
                   'resolver_pointer_event_id', pointer.pointer_event_id,
                   'record_event_ids', COALESCE(records.event_ids, '[]'::jsonb),
                   'read_rules', CASE WHEN COALESCE(
                       resolver.declared_summary -> 'classification' -> 'read_features',
                       '[]'::jsonb
                   ) ? 'ensip19_default_address' THEN jsonb_build_array(
                       jsonb_build_object(
                           'kind', 'ensip19_default_address',
                           'source_record_key', 'addr:2147483648'
                       )
                   ) ELSE '[]'::jsonb END,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ) || CASE WHEN records.exact_nonempty_keys IS NULL THEN '{}'::jsonb
                   ELSE jsonb_build_object('exact_nonempty_not_found_record_keys',
                       records.exact_nonempty_keys) END,
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', latest_position.block_number,
                   'block_hash', latest_position.block_hash,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   pointer.pointer_manifest_version,
                   COALESCE(version.manifest_version, 1),
                   COALESCE(records.manifest_version, 1),
                   COALESCE(resolver.manifest_version, 1)
               )
        FROM pointers pointer
        JOIN pointer_eligibility eligibility USING (resource_id)
        LEFT JOIN project_stage_resolver_current resolver
          ON resolver.chain_id = $1
         AND lower(resolver.resolver_address) = pointer.resolver_address
        LEFT JOIN versions version
          ON version.attributed_resource_id = pointer.resource_id
        LEFT JOIN chain_lineage boundary
          ON boundary.chain_id = $1
         AND boundary.block_number = COALESCE(
             version.block_number, pointer.pointer_block_number
         )
         AND boundary.block_hash = COALESCE(
             version.block_hash, pointer.pointer_block_hash
         )
        LEFT JOIN record_rollups records
          ON records.resource_id = pointer.resource_id
        LEFT JOIN latest_positions latest_position
          ON latest_position.resource_id = pointer.resource_id
        ORDER BY pointer.resource_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build record_inventory_current", error))?;
    Ok(())
}
