use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    // Address relations fold registration, token-holder, and controller events for the staged
    // current name/resource. They read no resolver classification or pointer history; a resolver
    // event can therefore affect only its directly scoped name/resource here.
    //
    // Every per-name fold reads project_authority_events, the selected authority epoch's event
    // set, rather than project_events. Ranking the name's whole cross-era history here would
    // re-decide authority per relation, so a superseded-resource event could outrank the
    // selected registration; the resource-keyed folds below are already scoped by their join to
    // the staged current resource.
    sqlx::query(
        r#"
        WITH RECURSIVE target_time AS (
            SELECT extract(epoch FROM lineage.block_timestamp) AS epoch_seconds
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = $2
              AND lineage.block_hash = $3
        ),
        wrapper_constants AS (
            SELECT 131072::bigint AS is_dot_eth,
                   7776000::numeric AS grace_period_seconds
        ),
        wrapper_expiries AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric >= 0
                        AND (event.after_state ->> 'expiry')::numeric <=
                            18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric
                   END AS expiry_seconds
            FROM project_events event
            WHERE event.event_kind = 'ExpiryChanged'
              AND event.resource_id IS NOT NULL
              AND (
                    event.source_family = 'ens_v1_wrapper_l1'
                 OR (
                        event.source_family = 'ens_v1_registrar_l1'
                    AND event.after_state ->> 'source_event' = 'NameRenewed'
                    AND event.after_state ->> 'authority_kind' = 'wrapper'
                 )
              )
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        scope_modifiers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric >= 0
                        AND (event.after_state ->> 'fuses')::numeric <=
                            9223372036854775807
                        AND expiry.expiry_seconds IS NOT NULL
                        AND target_time.epoch_seconds IS NOT NULL
                           THEN (
                               (event.after_state ->> 'fuses')::bigint
                                   & wrapper_constants.is_dot_eth
                           ) <> 0
                           AND expiry.expiry_seconds >= target_time.epoch_seconds
                           AND expiry.expiry_seconds
                               - wrapper_constants.grace_period_seconds
                               < target_time.epoch_seconds
                   END AS in_grace
            FROM project_events event
            LEFT JOIN wrapper_expiries expiry USING (resource_id)
            LEFT JOIN target_time ON TRUE
            CROSS JOIN wrapper_constants
            WHERE event.event_kind = 'PermissionScopeChanged'
              AND event.resource_id IS NOT NULL
              AND event.source_family = 'ens_v1_wrapper_l1'
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        -- ENSv1 splits a name's registry ownership from its registrar leasehold across two
        -- resources of one authority arm. The registrar's ERC721 transfer writes no registry
        -- state; after registration only `reclaim` does
        -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L174
        -- @ ens_v1@91c966f). A transfer with no reclaim therefore leaves the previous owner in
        -- the registry, and the registry-only binding that records this supersedes the registrar
        -- resource, putting that owner's own events outside the selected event set. Readmit them
        -- for the control fold only: events on the immediate predecessor binding's resource, from
        -- that binding's block up to the selected binding's position. The exact-name authority
        -- handles the same divergence log-precisely (`name_authority::stage`); this window is
        -- deliberately block-granular at its lower bound, for the reason on the bound below.
        --
        -- The window has to stop at the predecessor. Registry-only authority is the normal state
        -- of every plain subname and of every expired .eth name, so an unbounded reach would
        -- republish owners from long-superseded eras -- and for a name that expired while
        -- wrapped, where registry-owner events are attributed to the wrapper resource, it would
        -- publish the wrapper contract itself as the controller of every such name.
        registry_only_names AS (
            SELECT authority.logical_name_id,
                   authority.selected_authority_arm
            FROM project_name_authority authority
            WHERE authority.unsupported_reason IS NULL
              AND authority.selected_authority_arm IN ('ens_v1', 'basenames')
              AND EXISTS (
                  SELECT 1
                  FROM project_events epoch
                  WHERE epoch.logical_name_id = authority.logical_name_id
                    AND epoch.resource_id = authority.selected_resource_id
                    AND epoch.event_kind = 'AuthorityEpochChanged'
                    AND epoch.after_state ->> 'authority_kind' = 'registry_only'
              )
        ),
        registry_only_authority_transfers AS (
            SELECT event.*
            FROM registry_only_names authority
            JOIN project_events event
              ON event.logical_name_id = authority.logical_name_id
             AND event.event_kind = 'AuthorityTransferred'
            WHERE EXISTS (
                  SELECT 1
                  FROM project_bindings selected_binding
                  JOIN LATERAL (
                      SELECT predecessor.*
                      FROM project_binding_candidates predecessor
                      WHERE predecessor.logical_name_id = selected_binding.logical_name_id
                        AND predecessor.authority_arm = authority.selected_authority_arm
                        AND (
                            predecessor.block_number,
                            COALESCE(
                                (predecessor.provenance ->> 'transaction_index')::bigint, -1
                            ),
                            COALESCE((predecessor.provenance ->> 'log_index')::bigint, -1)
                        ) < (
                            selected_binding.block_number,
                            COALESCE(
                                (selected_binding.provenance ->> 'transaction_index')::bigint, -1
                            ),
                            COALESCE((selected_binding.provenance ->> 'log_index')::bigint, -1)
                        )
                      ORDER BY predecessor.block_number DESC,
                               COALESCE(
                                   (predecessor.provenance ->> 'transaction_index')::bigint, -1
                               ) DESC,
                               COALESCE(
                                   (predecessor.provenance ->> 'log_index')::bigint, -1
                               ) DESC,
                               predecessor.surface_binding_id DESC
                      LIMIT 1
                  ) predecessor ON TRUE
                  WHERE selected_binding.logical_name_id = authority.logical_name_id
                    AND predecessor.resource_id = event.resource_id
                    -- Block granularity, not the full position tuple: the transaction that
                    -- registers a name both writes the registry owner and creates the binding,
                    -- and the registry write comes first within it (`_register` calls
                    -- `setSubnodeOwner` before emitting `NameRegistered`, whose log the binding
                    -- records as its provenance -- upstream:
                    -- .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L152
                    -- @ ens_v1@91c966f). Basenames' conditional direct-registrar path likewise
                    -- writes the registry owner before emitting `NameRegistered` (upstream:
                    -- .refs/basenames/src/L2/BaseRegistrar.sol:L423-L425 @ basenames@1809bbc).
                    -- The Basenames controller paths used here call `registerWithRecord`, which
                    -- writes the registry record before returning, and only then emit
                    -- `NameRegistered`
                    -- (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L265-L275 @
                    -- basenames@1809bbc)
                    -- (upstream: .refs/basenames/src/L2/RegistrarController.sol:L535-L548 @
                    -- basenames@1809bbc)
                    -- (upstream:
                    -- .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L615-L633 @
                    -- basenames@1809bbc). Comparing log positions would drop exactly the
                    -- registration-time owner this readmission exists to recover.
                    AND event.block_number >= predecessor.block_number
                    AND (
                        event.block_number,
                        COALESCE(event.transaction_index, -1),
                        COALESCE(event.log_index, -1)
                    ) <= (
                        selected_binding.block_number,
                        COALESCE(
                            (selected_binding.provenance ->> 'transaction_index')::bigint, -1
                        ),
                        COALESCE((selected_binding.provenance ->> 'log_index')::bigint, -1)
                    )
              )
              -- Anti-join instead of a DISTINCT ON over the union: `project_authority_events` is
              -- indexed on (logical_name_id, normalized_event_id) but not on the event id alone,
              -- so deduplicating afterwards would sort the whole authority-event set on a rebuild.
              AND NOT EXISTS (
                  SELECT 1
                  FROM project_authority_events base
                  WHERE base.logical_name_id = event.logical_name_id
                    AND base.normalized_event_id = event.normalized_event_id
              )
        ),
        controller_source AS (
            SELECT * FROM project_authority_events
            WHERE event_kind IN ('AuthorityTransferred', 'PermissionChanged')
               OR (event_kind = 'SurfaceBound'
                   AND after_state ->> 'state_derived' = 'true'
                   AND after_state ->> 'authority_kind' = 'registry_only')
            UNION ALL
            SELECT * FROM registry_only_authority_transfers
        ),
        controller_events AS (
            SELECT name.logical_name_id,
                   name.resource_id,
                   event.normalized_event_id,
                   event.block_number,
                   event.block_hash,
                   event.manifest_version,
                   row_number() OVER (
                       PARTITION BY name.logical_name_id
                       ORDER BY event.block_number NULLS FIRST,
                                event.transaction_index NULLS FIRST,
                                event.log_index NULLS FIRST,
                                event.normalized_event_id
                   ) AS event_order,
                   CASE
                       WHEN event.event_kind IN ('AuthorityTransferred', 'SurfaceBound')
                           THEN 'set'
                       WHEN event.event_kind = 'PermissionChanged'
                        AND event.resource_id = name.resource_id
                        AND event.after_state -> 'scope' ->> 'kind' = 'resource'
                        AND jsonb_typeof(event.after_state -> 'effective_powers') = 'array'
                        AND EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements_text(
                                     event.after_state -> 'effective_powers'
                                 ) power
                            WHERE power.value = 'resource_control'
                        )
                        AND (
                            modifier.resource_id IS NULL
                            OR (
                                name.declared_summary ->> 'wrapper_state'
                                    IN ('wrapped', 'emancipated')
                                AND modifier.in_grace IS FALSE
                            )
                        ) THEN 'set'
                       WHEN event.event_kind = 'PermissionChanged'
                        AND event.resource_id = name.resource_id
                        AND event.after_state -> 'scope' ->> 'kind' = 'resource'
                        AND jsonb_typeof(event.after_state -> 'effective_powers') = 'array'
                           THEN 'revoke'
                       ELSE 'ignore'
                   END AS action,
                   lower(CASE event.event_kind
                       WHEN 'AuthorityTransferred' THEN CASE
                           -- A masked owner word authenticates no caller: the event clears
                           -- the controller exactly like a zero-owner transition.
                           WHEN event.after_state ->> 'owner_word_unmasked' = 'true'
                               THEN '0x0000000000000000000000000000000000000000'
                           ELSE COALESCE(
                               event.after_state ->> 'registry_owner',
                               event.after_state ->> 'owner'
                           )
                       END
                       WHEN 'SurfaceBound' THEN event.after_state ->> 'owner'
                       WHEN 'PermissionChanged'
                           THEN event.after_state ->> 'subject'
                   END) AS subject
            FROM project_stage_name_current name
            JOIN controller_source event
              ON event.logical_name_id = name.logical_name_id
            LEFT JOIN scope_modifiers modifier
              ON modifier.resource_id = name.resource_id
        ),
        controller_fold AS (
            SELECT event.logical_name_id,
                   event.resource_id,
                   event.event_order,
                   CASE WHEN event.action = 'set' THEN event.subject END AS controller,
                   CASE WHEN event.action IN ('set', 'revoke')
                       THEN event.normalized_event_id END AS normalized_event_id,
                   CASE WHEN event.action IN ('set', 'revoke')
                       THEN event.block_number END AS block_number,
                   CASE WHEN event.action IN ('set', 'revoke')
                       THEN event.block_hash END AS block_hash,
                   CASE WHEN event.action IN ('set', 'revoke')
                       THEN event.manifest_version END AS manifest_version
            FROM controller_events event
            WHERE event.event_order = 1

            UNION ALL

            SELECT event.logical_name_id,
                   event.resource_id,
                   event.event_order,
                   CASE
                       WHEN event.action = 'set' THEN event.subject
                       WHEN event.action = 'revoke'
                        AND prior.controller = event.subject THEN NULL
                       ELSE prior.controller
                   END,
                   CASE
                       WHEN event.action = 'set'
                        OR (event.action = 'revoke' AND prior.controller = event.subject)
                           THEN event.normalized_event_id
                       ELSE prior.normalized_event_id
                   END,
                   CASE
                       WHEN event.action = 'set'
                        OR (event.action = 'revoke' AND prior.controller = event.subject)
                           THEN event.block_number
                       ELSE prior.block_number
                   END,
                   CASE
                       WHEN event.action = 'set'
                        OR (event.action = 'revoke' AND prior.controller = event.subject)
                           THEN event.block_hash
                       ELSE prior.block_hash
                   END,
                   CASE
                       WHEN event.action = 'set'
                        OR (event.action = 'revoke' AND prior.controller = event.subject)
                           THEN event.manifest_version
                       ELSE prior.manifest_version
                   END
            FROM controller_fold prior
            JOIN controller_events event
              ON event.logical_name_id = prior.logical_name_id
             AND event.event_order = prior.event_order + 1
        ),
        controllers AS (
            SELECT DISTINCT ON (logical_name_id)
                   logical_name_id,
                   controller,
                   normalized_event_id,
                   block_number,
                   block_hash,
                   manifest_version
            FROM controller_fold
            ORDER BY logical_name_id, event_order DESC
        ),
        binding_state AS (
            SELECT name.*,
                   registration.registrant,
                   registration.normalized_event_id AS registration_event_id,
                   registration.block_number AS registration_block_number,
                   registration.block_hash AS registration_block_hash,
                   registration.manifest_version AS registration_manifest_version,
                   token_holder.token_holder,
                   token_holder.normalized_event_id AS token_event_id,
                   token_holder.block_number AS token_block_number,
                   token_holder.block_hash AS token_block_hash,
                   token_holder.manifest_version AS token_manifest_version,
                   controller.controller,
                   controller.normalized_event_id AS controller_event_id,
                   controller.block_number AS controller_block_number,
                   controller.block_hash AS controller_block_hash,
                   controller.manifest_version AS controller_manifest_version,
                   modifier.resource_id AS wrapper_modifier_resource_id,
                   modifier.in_grace AS wrapper_in_grace
            FROM project_stage_name_current name
            LEFT JOIN LATERAL (
                SELECT lower(CASE event.event_kind
                           WHEN 'TokenControlTransferred' THEN event.after_state ->> 'to'
                           ELSE event.after_state ->> 'registrant'
                       END) AS registrant,
                       event.*
                FROM project_authority_events event
                WHERE event.logical_name_id = name.logical_name_id
                  AND event.event_kind IN (
                      'RegistrationGranted', 'TokenControlTransferred'
                  )
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) registration ON TRUE
            LEFT JOIN LATERAL (
                SELECT lower(event.after_state ->> 'to') AS token_holder,
                       event.*
                FROM project_authority_events event
                WHERE event.logical_name_id = name.logical_name_id
                  AND event.event_kind = 'TokenControlTransferred'
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) token_holder ON TRUE
            LEFT JOIN controllers controller
              ON controller.logical_name_id = name.logical_name_id
            LEFT JOIN scope_modifiers modifier
              ON modifier.resource_id = name.resource_id
            WHERE name.surface_binding_id IS NOT NULL
              AND name.resource_id IS NOT NULL
              AND name.binding_kind IS NOT NULL
        ),
        relations AS (
            SELECT lower(state.registrant) AS address,
                   state.logical_name_id,
                   'registrant'::text AS relation,
                   state.registration_event_id AS normalized_event_id,
                   state.registration_block_number AS block_number,
                   state.registration_block_hash AS block_hash,
                   state.registration_manifest_version AS manifest_version
            FROM binding_state state
            WHERE state.token_lineage_id IS NOT NULL
            UNION ALL
            SELECT lower(COALESCE(state.token_holder, state.registrant)),
                   state.logical_name_id,
                   'token_holder',
                   COALESCE(state.token_event_id, state.registration_event_id),
                   COALESCE(state.token_block_number, state.registration_block_number),
                   COALESCE(state.token_block_hash, state.registration_block_hash),
                   GREATEST(state.token_manifest_version,
                            state.registration_manifest_version)
            FROM binding_state state
            WHERE state.token_lineage_id IS NOT NULL
              AND (
                  state.wrapper_modifier_resource_id IS NULL
                  OR state.declared_summary ->> 'wrapper_state'
                      IN ('wrapped', 'emancipated', 'locked')
              )
            UNION ALL
            SELECT lower(CASE
                       WHEN state.token_lineage_id IS NOT NULL THEN COALESCE(
                           state.controller, state.token_holder, state.registrant
                       )
                       ELSE state.controller
                   END),
                   state.logical_name_id,
                   'effective_controller',
                   COALESCE(
                       state.controller_event_id,
                       state.token_event_id,
                       state.registration_event_id
                   ),
                   COALESCE(
                       state.controller_block_number,
                       state.token_block_number,
                       state.registration_block_number
                   ),
                   COALESCE(
                       state.controller_block_hash,
                       state.token_block_hash,
                       state.registration_block_hash
                   ),
                   GREATEST(
                       state.controller_manifest_version,
                       state.token_manifest_version,
                       state.registration_manifest_version
                   )
            FROM binding_state state
            WHERE state.token_lineage_id IS NULL
               OR state.wrapper_modifier_resource_id IS NULL
               OR (
                   state.declared_summary ->> 'wrapper_state'
                       IN ('wrapped', 'emancipated')
                   AND state.wrapper_in_grace IS FALSE
               )
        ),
        selected AS (
            SELECT * FROM relations
            WHERE address IS NOT NULL
              AND btrim(address) <> ''
              AND address <> '0x0000000000000000000000000000000000000000'
        )
        INSERT INTO project_stage_address_names_current (
            address, logical_name_id, relation, namespace, raw_name, namehash,
            surface_binding_id, resource_id, token_lineage_id, binding_kind,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT selected.address,
               selected.logical_name_id,
               selected.relation,
               name.namespace,
               name.raw_name,
               name.namehash,
               name.surface_binding_id,
               name.resource_id,
               name.token_lineage_id,
               name.binding_kind,
               CASE
                   WHEN selected.relation = 'effective_controller'
                       AND summary.support_status = 'unsupported'
                       AND summary.unsupported_reason =
                           'operator_approval_surfaces_not_ingested'
                       THEN 'supported'
                   WHEN selected.relation = 'effective_controller'
                       THEN summary.support_status
                   ELSE 'supported'
               END,
               CASE
                   WHEN selected.relation = 'effective_controller'
                       AND summary.support_status = 'unsupported'
                       AND summary.unsupported_reason =
                           'operator_approval_surfaces_not_ingested'
                       THEN NULL
                   WHEN selected.relation = 'effective_controller'
                       THEN summary.unsupported_reason
                   ELSE NULL
               END,
               jsonb_build_object(
                   'chain_id', $1,
                   'normalized_event_id', selected.normalized_event_id,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', selected.block_number,
                   'block_hash', selected.block_hash,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(selected.manifest_version, name.manifest_version)
        FROM selected
        JOIN project_stage_name_current name USING (logical_name_id)
        LEFT JOIN project_stage_permissions_current_resource_summary summary
          ON summary.resource_id = name.resource_id
        ORDER BY selected.address, selected.logical_name_id, selected.relation
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build address_names_current", error))?;
    Ok(())
}
