#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ "${1:-}" != "--database-ready" ]; then
    if [ -n "${BIGNAME_TEST_DATABASE_URL:-}" ]; then
        export SCHEMA_V2_EXTERNAL_DATABASE=1
    fi
    exec "$ROOT/scripts/test-db" -- "$ROOT/schema-v2/apply-check.sh" --database-ready
fi

container="${BIGNAME_TEST_POSTGRES_CONTAINER:-bigname-test-postgres}"
image="${BIGNAME_TEST_POSTGRES_IMAGE:-postgres:16-alpine}"
database="${BIGNAME_TEST_POSTGRES_DB:-bigname}"
user="${BIGNAME_TEST_POSTGRES_USER:-bigname}"

psql_mode=""
if [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = "1" ] \
    && command -v psql >/dev/null 2>&1; then
    psql_mode="host"
elif [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = "1" ] \
    && command -v docker >/dev/null 2>&1 \
    && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    psql_mode="client-container"
elif command -v docker >/dev/null 2>&1 \
    && docker inspect "$container" >/dev/null 2>&1 \
    && [ "$(docker inspect --format '{{.State.Running}}' "$container")" = "true" ]; then
    psql_mode="database-container"
elif command -v psql >/dev/null 2>&1; then
    psql_mode="host"
elif command -v docker >/dev/null 2>&1 && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    psql_mode="client-container"
else
    printf '%s\n' "psql or Docker is required for the schema check" >&2
    exit 1
fi

run_psql() {
    case "$psql_mode" in
        database-container)
            docker exec -i "$container" \
                psql -X -q -v ON_ERROR_STOP=1 -U "$user" -d "$database"
            ;;
        host)
            psql -X -q -v ON_ERROR_STOP=1 "${BIGNAME_DATABASE_URL:?}"
            ;;
        client-container)
            docker run --rm --network host -i "$image" \
                psql -X -q -v ON_ERROR_STOP=1 "$BIGNAME_DATABASE_URL"
            ;;
    esac
}

render_phase_migration() {
    local migration_file="$1"
    sed "s/bigname_phase/$scratch_schema/g" "$migration_file"
}
# Inventory membership requires the whole quoted or bare literal bigname_phase token
# after stripping -- line comments. Search-path-relative phase SQL would be silently
# excluded. Today only the two public-schema service-loop files are outside the inventory;
# block comments are not stripped, so a token-only mention is included and fails loud.
phase_migration_uses_production_schema() {
    awk '
        { sub(/--.*$/, "") }
        /(^|[^[:alnum:]_])"?bigname_phase"?([^[:alnum:]_]|$)/ { found = 1 }
        END { exit !found }
    ' "$1"
}
report_timing() {
    local elapsed=$((SECONDS - timing_started - ${2:-0}))
    if [ "${SCHEMA_V2_APPLY_CHECK_TIMING:-0}" = 1 ]; then
        printf 'schema-v2 timing: %s=%ss\n' "$1" "$elapsed"
    fi
    timing_started=$SECONDS
}
emit_phase_migration() {
    local migration_file="$1"
    local context="$2"
    if [ ! -f "$migration_file" ]; then
        printf '%s\n' "schema-migration path does not exist: $migration_file" >&2
        exit 1
    fi
    printf '%s|%s\n' "$context" "$(basename "$migration_file")" \
        >> "$migration_application_log"
    render_phase_migration "$migration_file"
}
assert_migration_context_count() {
    local migration_file="$1"
    local context="$2"
    local expected_count="$3"
    local migration_basename
    local observed_count
    migration_basename="$(basename "$migration_file")"
    observed_count="$(
        awk -F '|' -v context="$context" -v migration="$migration_basename" \
            '$1 == context && $2 == migration { count += 1 } END { print count + 0 }' \
            "$migration_application_log"
    )"
    if [ "$observed_count" -ne "$expected_count" ]; then
        printf '%s\n' \
            "$migration_basename in $context: expected $expected_count, observed $observed_count successful applications" >&2
        exit 1
    fi
}
assert_reviewed_phase_migrations_applied() {
    local migration_file
    local migration_basename
    local skip_entry
    local skip_basename
    local skip_reason
    local skip_count=0
    local -a expected_migrations=()
    local -a without_predecessor_shape_proof=()
    local -A expected_lookup=()
    local -A skipped_lookup=()
    while IFS= read -r migration_file; do
        migration_basename="$(basename "$migration_file")"
        expected_migrations+=("$migration_basename")
        expected_lookup["$migration_basename"]=1
    done < <(
        for migration_file in "$ROOT"/migrations/*.sql; do
            if phase_migration_uses_production_schema "$migration_file"; then
                printf '%s\n' "$migration_file"
            fi
        done | sort
    )
    for skip_entry in "${intentional_phase_migration_skips[@]}"; do
        skip_basename="${skip_entry%%|*}"
        skip_reason="${skip_entry#*|}"
        if [ "$skip_basename" = "$skip_entry" ] || [ -z "$skip_reason" ]; then
            printf '%s\n' \
                "intentional schema-migration skip lacks a one-line reason: $skip_entry" >&2
            exit 1
        fi
        if [ -z "${expected_lookup[$skip_basename]:-}" ]; then
            printf '%s\n' \
                "intentional schema-migration skip names an unreviewed or missing file: $skip_basename" >&2
            exit 1
        fi
        if [ -n "${skipped_lookup[$skip_basename]:-}" ]; then
            printf '%s\n' \
                "duplicate intentional schema-migration skip: $skip_basename" >&2
            exit 1
        fi
        skipped_lookup["$skip_basename"]="$skip_reason"
        skip_count=$((skip_count + 1))
    done
    for migration_basename in "${expected_migrations[@]}"; do
        if [ -n "${skipped_lookup[$migration_basename]:-}" ]; then
            continue
        fi
        if ! awk -F '|' -v migration="$migration_basename" \
            '$1 == "empty-schema" && $2 == migration { found = 1 } END { exit !found }' \
            "$migration_application_log"
        then
            printf '%s\n' \
                "reviewed phase schema-migration was not applied on empty-schema path: $migration_basename" >&2
            exit 1
        fi
        if ! awk -F '|' -v migration="$migration_basename" \
            '($1 == "baseline-first" || $1 == "preceding-shape" || $1 == "specialized") && $2 == migration { found = 1 } END { exit !found }' \
            "$migration_application_log"
        then
            printf '%s\n' \
                "reviewed phase schema-migration was not applied on an initialized-schema path: $migration_basename" >&2
            exit 1
        fi
        if awk -F '|' -v migration="$migration_basename" \
            '($1 == "preceding-shape" || $1 == "specialized") && $2 == migration { found = 1 } END { exit !found }' \
            "$migration_application_log"; then
            predecessor_shape_proof_count=$((predecessor_shape_proof_count + 1))
        else
            without_predecessor_shape_proof+=("$migration_basename")
        fi
    done
    if [ "$predecessor_shape_proof_count" -ne "$expected_predecessor_shape_proof_count" ]; then
        printf '%s\n' "exact-predecessor-shape proof count: expected $expected_predecessor_shape_proof_count, observed $predecessor_shape_proof_count" >&2
        exit 1
    fi
    if [ "${SCHEMA_V2_APPLY_CHECK_VERBOSE:-0}" = 1 ]; then
        printf 'phase schema-migrations without exact-predecessor-shape proof (%s):\n' "${#without_predecessor_shape_proof[@]}" >&2
        printf '  %s\n' "${without_predecessor_shape_proof[@]}" >&2
    fi
    expected_reviewed_phase_migration_count="${#expected_migrations[@]}"
    unique_successful_migration_count="$(
        cut -d '|' -f 2 "$migration_application_log" | sort -u | wc -l \
            | tr -d ' '
    )"
    total_successful_migration_applications="$(wc -l < "$migration_application_log")"
    intentional_phase_migration_skip_count="$skip_count"
}
assert_migration_refusal() {
    local label="$1"
    local migration_file="$2"
    local exact_message="$3"
    local setup_sql
    local refusal_stderr
    local observed_error
    local refusal_started=$SECONDS
    setup_sql="$(cat)"
    if refusal_stderr="$({
        printf 'BEGIN;\n'
        printf 'SET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' "$setup_sql"
        render_phase_migration "$migration_file"
        printf 'ROLLBACK;\n'
    } | run_psql 2>&1 >/dev/null)"; then
        printf '%s\n' "$label: migration unexpectedly succeeded" >&2
        exit 1
    fi
    observed_error="$(
        printf '%s\n' "$refusal_stderr" \
            | sed -n 's/^ERROR:[[:space:]]*//p' \
            | sed -n '1p'
    )"
    if [ "$observed_error" != "$exact_message" ]; then
        printf '%s\n' \
            "$label: expected PostgreSQL error: $exact_message" \
            "$label: observed PostgreSQL error: $observed_error" \
            "$label: complete stderr:" \
            "$refusal_stderr" >&2
        exit 1
    fi
    refusal_assertions_passed=$((refusal_assertions_passed + 1))
    refusal_probe_seconds=$((refusal_probe_seconds + SECONDS - refusal_started))
}
assert_unconfigured_settlement_constraint() {
    local provenance="$1"
    local false_error

    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' \
            "INSERT INTO chain_phase_state (" \
            "    chain_id, phase_name, settled_while_unconfigured" \
            ") VALUES (" \
            "    'phase-settlement-${provenance}-true', 'ingest', TRUE" \
            ");"
    } | run_psql

    if false_error="$({
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' \
            "INSERT INTO chain_phase_state (" \
            "    chain_id, phase_name, settled_while_unconfigured" \
            ") VALUES (" \
            "    'phase-settlement-${provenance}-false', 'project', FALSE" \
            ");"
    } | run_psql 2>&1)"; then
        printf '%s\n' \
            "$provenance settlement constraint accepted a FALSE marker" >&2
        exit 1
    fi
    if [[ "$false_error" != *chain_phase_state_unconfigured_settlement_check* ]]; then
        printf '%s\n' \
            "$provenance FALSE marker failed without the named settlement constraint" >&2
        printf '%s\n' "$false_error" >&2
        exit 1
    fi
    printf '%s\n' \
        "$provenance settlement constraint accepts non-Verify TRUE and rejects FALSE"
}

wait_for_schema_v2_race_session() {
    local application_name="$1"
    local status
    local attempt

    for ((attempt = 1; attempt <= 100; attempt += 1)); do
        status="$(
            {
                printf 'SET search_path TO "%s";\n' "$scratch_schema"
                printf '%s\n' \
                    "SELECT CASE WHEN EXISTS (" \
                    "    SELECT 1" \
                    "    FROM pg_stat_activity" \
                    "    WHERE application_name = '$application_name'" \
                    "      AND wait_event = 'PgSleep'" \
                    ") THEN 'schema_v2_race_ready'" \
                    "ELSE 'schema_v2_race_waiting' END;"
            } | run_psql
        )"
        if [[ "$status" == *schema_v2_race_ready* ]]; then
            return 0
        fi
        sleep 0.05
    done

    return 1
}

scratch_schema="schema_v2_apply_check_${PPID}_$$"
if [[ ! "$scratch_schema" =~ ^[a-z0-9_]+$ ]]; then
    printf '%s\n' "invalid scratch schema name" >&2
    exit 1
fi
migration_application_log="$(
    mktemp "${TMPDIR:-/tmp}/schema-v2-migration-applications.XXXXXX"
)"
# Future entries must use basename|one-line reason.
intentional_phase_migration_skips=()
refusal_assertions_passed=0
expected_refusal_assertions=7
predecessor_shape_proof_count=0
expected_predecessor_shape_proof_count=24
refusal_probe_seconds=0
timing_started=$SECONDS

cleanup() {
    if [ -n "${schema_v2_race_pid:-}" ] \
        && kill -0 "$schema_v2_race_pid" >/dev/null 2>&1; then
        kill "$schema_v2_race_pid" >/dev/null 2>&1 || true
        wait "$schema_v2_race_pid" >/dev/null 2>&1 || true
    fi
    if [ -n "${schema_v2_race_log:-}" ]; then
        rm -f -- "$schema_v2_race_log"
    fi
    if [ -n "${migration_application_log:-}" ]; then
        rm -f -- "$migration_application_log"
    fi
    printf 'DROP SCHEMA IF EXISTS "%s" CASCADE;\n' "$scratch_schema" \
        | run_psql >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf 'CREATE SCHEMA "%s";\n' "$scratch_schema" | run_psql

apply_baseline() {
    local sql_file
    for sql_file in "$ROOT"/schema-v2/baseline/*.sql; do
        {
            printf 'SET client_min_messages TO warning;\n'
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            cat "$sql_file"
        } | run_psql
    done
}

# A schema-migration database can exist before phase-runner installs the phase
# baseline. Every reviewed phase schema-migration must be a no-op on that empty path.
for migration_file in \
    "$ROOT/migrations/20260809120000_manifest_authority_attestation_audit.sql" \
    "$ROOT/migrations/20260810120000_remove_l2_migration_remnants.sql" \
    "$ROOT/migrations/20260811120000_ens_v2_migration_slice_1.sql" \
    "$ROOT/migrations/20260811120100_ens_v2_migration_slice_1_validate.sql" \
    "$ROOT/migrations/20260811120200_ens_v2_migration_slice_1_constraints.sql" \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    "$ROOT/migrations/20260814120000_project_redo_resolver_evidence.sql" \
    "$ROOT/migrations/20260814122000_verify_unconfigured_settlement.sql" \
    "$ROOT/migrations/20260814122100_verify_unconfigured_settlement_validate.sql" \
    "$ROOT/migrations/20260814123000_ingest_redo_source_boundary_markers.sql" \
    "$ROOT/migrations/20260814124000_redo_attempt_generation.sql" \
    "$ROOT/migrations/20260814125000_ingest_redo_manifest_authority.sql" \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    "$ROOT/migrations/20260814131000_project_generation_failure_audit.sql" \
    "$ROOT/migrations/20260814132000_project_generation_failure_child_authority.sql" \
    "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
    "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
    "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
    "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
    "$ROOT/migrations/20260826120000_interpret_decode_skip_audit.sql" \
    "$ROOT/migrations/20260826120100_manifest_applied_change_count.sql" \
    "$ROOT/migrations/20260827120000_normalized_events_ens_v1_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260827130000_normalized_events_v1_after_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130200_normalized_events_v1_before_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130300_normalized_events_v1_before_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130400_normalized_events_v2_subregistry_pointer_scope_idx.sql" \
    "$ROOT/migrations/20260828120000_name_current_serving_resource.sql" \
    "$ROOT/migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql" \
    "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
    "$ROOT/migrations/20260831150000_normalized_events_v2_expiry_scope_idx.sql" \
    "$ROOT/migrations/20260902120000_normalized_events_basenames_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
    "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
    "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
    "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
    "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql" \
    "$ROOT/migrations/20260904120000_project_redo_child_registration_history.sql" \
    "$ROOT/migrations/20260906120000_exact_zero_addr60_default_derivation.sql"
do
    emit_phase_migration "$migration_file" empty-schema | run_psql
done

# SQLx migrations run before phase-runner installs a fresh schema-v2 baseline.
# They must leave that namespace empty so init-schema can still accept it.
# Comment-only upgrades must tolerate that same pre-initialization path.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration "$ROOT/migrations/20260814121000_phase_heartbeat_liveness_comment.sql" empty-schema
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = current_schema()
        UNION ALL
        SELECT 1
        FROM pg_proc function
        JOIN pg_namespace namespace ON namespace.oid = function.pronamespace
        WHERE namespace.nspname = current_schema()
        UNION ALL SELECT 1 FROM pg_type type_row JOIN pg_namespace namespace ON namespace.oid = type_row.typnamespace WHERE namespace.nspname = current_schema()
    ) THEN
        RAISE EXCEPTION
            'schema-migrations created objects before fresh schema initialization';
    END IF;
END
$$;
SQL
} | run_psql
report_timing empty-schema

apply_baseline
apply_baseline
report_timing baseline-install


{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    invalid_indexes text;
BEGIN
    SELECT string_agg(required.index_name, ', ' ORDER BY required.index_name)
    INTO invalid_indexes
    FROM (
        VALUES
            (
                'normalized_events_pointer_after_resolver_history_idx',
                '%chain_id%lower%after_state%resolver%block_number%block_hash%INCLUDE%normalized_event_id%',
                'ResolverChanged',
                NULL,
                false
            ),
            (
                'normalized_events_pointer_before_resolver_history_idx',
                '%chain_id%lower%before_state%resolver%block_number%block_hash%INCLUDE%normalized_event_id%',
                'ResolverChanged',
                NULL,
                false
            ),
            (
                'normalized_events_permission_after_resolver_history_idx',
                '%chain_id%lower%after_state%scope%resolver_address%block_number%block_hash%INCLUDE%resource_id%',
                'PermissionChanged',
                '%after_state%scope%kind%resolver%',
                true
            ),
            (
                'normalized_events_permission_before_resolver_history_idx',
                '%chain_id%lower%before_state%scope%resolver_address%block_number%block_hash%INCLUDE%resource_id%',
                'PermissionChanged',
                '%before_state%scope%kind%resolver%',
                true
            )
    ) AS required(
        index_name, definition_pattern, event_kind, scope_pattern, resource_required
    )
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_class index_relation
        JOIN pg_namespace namespace
          ON namespace.oid = index_relation.relnamespace
        JOIN pg_index index_state
          ON index_state.indexrelid = index_relation.oid
        WHERE namespace.nspname = current_schema()
          AND index_relation.relname = required.index_name
          AND index_state.indisvalid
          AND index_state.indisready
          AND index_state.indislive
          AND pg_get_indexdef(index_relation.oid) LIKE required.definition_pattern
          AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
              LIKE format('%%event_kind%%%s%%', required.event_kind)
          AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
              LIKE '%consumer_visibility%activated%'
          AND pg_get_expr(index_state.indpred, index_state.indrelid, true)
              LIKE '%canonicality_state%canonical%safe%finalized%'
          AND (
              required.scope_pattern IS NULL
              OR pg_get_expr(index_state.indpred, index_state.indrelid, true)
                  LIKE required.scope_pattern
          )
          AND (
              NOT required.resource_required
              OR pg_get_expr(index_state.indpred, index_state.indrelid, true)
                  LIKE '%resource_id%IS NOT NULL%'
          )
    );

    IF invalid_indexes IS NOT NULL THEN
        RAISE EXCEPTION
            'resolver history indexes do not match the baseline: %',
            invalid_indexes;
    END IF;
END
$$;
SQL
} | run_psql

# TestDatabase installs the current baseline before SQLx applies reviewed
# migrations. The same reviewed files must be idempotent on that baseline-first
# path, including the validation and metadata-swap steps.
for migration_file in \
    "$ROOT/migrations/20260809120000_manifest_authority_attestation_audit.sql" \
    "$ROOT/migrations/20260811120000_ens_v2_migration_slice_1.sql" \
    "$ROOT/migrations/20260811120100_ens_v2_migration_slice_1_validate.sql" \
    "$ROOT/migrations/20260811120200_ens_v2_migration_slice_1_constraints.sql" \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    "$ROOT/migrations/20260814120000_project_redo_resolver_evidence.sql" \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    "$ROOT/migrations/20260814131000_project_generation_failure_audit.sql" \
    "$ROOT/migrations/20260814131000_project_generation_failure_audit.sql" \
    "$ROOT/migrations/20260814132000_project_generation_failure_child_authority.sql" \
    "$ROOT/migrations/20260814132000_project_generation_failure_child_authority.sql" \
    "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
    "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
    "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
    "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
    "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
    "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
    "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
    "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
    "$ROOT/migrations/20260826120000_interpret_decode_skip_audit.sql" \
    "$ROOT/migrations/20260826120000_interpret_decode_skip_audit.sql" \
    "$ROOT/migrations/20260826120100_manifest_applied_change_count.sql" \
    "$ROOT/migrations/20260827120000_normalized_events_ens_v1_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260827130000_normalized_events_v1_after_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130200_normalized_events_v1_before_node_scope_idx.sql" \
    "$ROOT/migrations/20260827130300_normalized_events_v1_before_child_scope_idx.sql" \
    "$ROOT/migrations/20260827130400_normalized_events_v2_subregistry_pointer_scope_idx.sql" \
    "$ROOT/migrations/20260828120000_name_current_serving_resource.sql" \
    "$ROOT/migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql" \
    "$ROOT/migrations/20260831120000_retire_direct_divergences_for_null_resolver.sql" \
    "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
    "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
    "$ROOT/migrations/20260831150000_normalized_events_v2_expiry_scope_idx.sql" \
    "$ROOT/migrations/20260902120000_normalized_events_basenames_record_node_resolver_idx.sql" \
    "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
    "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
    "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
    "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
    "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
    "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
    "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
    "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
    "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql" \
    "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql" \
    "$ROOT/migrations/20260904120000_project_redo_child_registration_history.sql" \
    "$ROOT/migrations/20260904120000_project_redo_child_registration_history.sql"
do
    emit_phase_migration "$migration_file" baseline-first | run_psql
done
report_timing baseline-first
# Exercise reverse_hydration_attempt_state_upgrade from the exact predecessor
# shape, then validate the additive tuple invariant independently. Both files
# must remain idempotent after the upgrade completes.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE primary_names_current
    DROP CONSTRAINT primary_names_current_reverse_hydration_attempt_check,
    DROP COLUMN reverse_hydration_attempted_block_number,
    DROP COLUMN reverse_hydration_attempted_block_hash,
    DROP COLUMN reverse_hydration_attempt_ordinal;
DROP SEQUENCE reverse_hydration_attempt_ordinal_seq;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF to_regclass(
        current_schema() || '.reverse_hydration_attempt_ordinal_seq'
    ) IS NULL THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade did not restore the sequence';
    END IF;
    IF (
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'primary_names_current'
          AND (
              (column_name = 'reverse_hydration_attempted_block_number'
               AND data_type = 'bigint' AND is_nullable = 'YES')
              OR (column_name = 'reverse_hydration_attempted_block_hash'
                  AND data_type = 'text' AND is_nullable = 'YES')
              OR (column_name = 'reverse_hydration_attempt_ordinal'
                  AND data_type = 'bigint' AND is_nullable = 'YES')
          )
    ) <> 3 THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade did not restore all three columns';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'primary_names_current'::regclass
          AND conname = 'primary_names_current_reverse_hydration_attempt_check'
          AND NOT convalidated
          AND pg_get_expr(conbin, conrelid) =
              '(((reverse_hydration_attempted_block_number IS NULL) AND (reverse_hydration_attempted_block_hash IS NULL) AND (reverse_hydration_attempt_ordinal IS NULL)) OR ((reverse_hydration_attempted_block_number IS NOT NULL) AND (reverse_hydration_attempted_block_number >= 0) AND (reverse_hydration_attempted_block_hash IS NOT NULL) AND (btrim(reverse_hydration_attempted_block_hash) <> ''''::text) AND (reverse_hydration_attempt_ordinal IS NOT NULL) AND (reverse_hydration_attempt_ordinal > 0)))'
    ) THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade constraint is missing, already validated, or has the wrong tuple predicate';
    END IF;
    IF col_description(
        'primary_names_current'::regclass,
        (SELECT attnum FROM pg_attribute
         WHERE attrelid = 'primary_names_current'::regclass
           AND attname = 'reverse_hydration_attempted_block_number'
           AND NOT attisdropped)
    ) IS DISTINCT FROM 'This internal reverse-name polling selection value identifies the head height of the latest attempt. Readers never use it as serving data.'
       OR col_description(
        'primary_names_current'::regclass,
        (SELECT attnum FROM pg_attribute
         WHERE attrelid = 'primary_names_current'::regclass
           AND attname = 'reverse_hydration_attempted_block_hash'
           AND NOT attisdropped)
    ) IS DISTINCT FROM 'This internal reverse-name polling selection value identifies the head hash of the latest attempt. Readers never use it as serving data.'
       OR col_description(
        'primary_names_current'::regclass,
        (SELECT attnum FROM pg_attribute
         WHERE attrelid = 'primary_names_current'::regclass
           AND attname = 'reverse_hydration_attempt_ordinal'
           AND NOT attisdropped)
    ) IS DISTINCT FROM 'This internal value orders reverse-name polling attempts for fair rolling selection. It never records or validates a provider result.'
       OR obj_description(
        'reverse_hydration_attempt_ordinal_seq'::regclass, 'pg_class'
    ) IS DISTINCT FROM 'This sequence assigns durable order to reverse-name polling batches; its values are not serving data.'
    THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade is missing an expected primary_names_current column or reverse_hydration_attempt_ordinal_seq comment';
    END IF;
END
$$;
SQL
} | run_psql
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_phase_migration \
        "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'primary_names_current'::regclass
          AND conname = 'primary_names_current_reverse_hydration_attempt_check'
          AND convalidated
    ) THEN
        RAISE EXCEPTION
            'reverse_hydration_attempt_state_upgrade constraint was not validated';
    END IF;
END
$$;
SQL
} | run_psql
emit_phase_migration \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    preceding-shape | run_psql
emit_phase_migration \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql" \
    preceding-shape | run_psql
for migration_file in \
    "$ROOT/migrations/20260813120000_reverse_hydration_attempt_state.sql" \
    "$ROOT/migrations/20260813120100_reverse_hydration_attempt_state_validate.sql"
do
    assert_migration_context_count "$migration_file" empty-schema 1
    assert_migration_context_count "$migration_file" baseline-first 2
    assert_migration_context_count "$migration_file" preceding-shape 2
done
report_timing reverse-hydration
# Exercise the service-loop upgrade's exact predecessor constraint discovery.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    render_phase_migration \
        "$ROOT/migrations/20260721130000_service_loop_heartbeats.sql"
} | run_psql
assert_migration_refusal \
    service-loop-scope-constraint \
    "$ROOT/migrations/20260721140000_service_loop_phase_heartbeats.sql" \
    'service_loop_heartbeats scope constraint was not found' <<'SQL'
DO $$
DECLARE
    scope_constraint text;
BEGIN
    SELECT constraint_row.conname
    INTO scope_constraint
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conrelid = 'service_loop_heartbeats'::regclass
      AND constraint_row.contype = 'c'
      AND pg_get_constraintdef(constraint_row.oid) LIKE '%scope_kind%'
    ORDER BY constraint_row.conname
    LIMIT 1;
    EXECUTE format(
        'ALTER TABLE service_loop_heartbeats DROP CONSTRAINT %I',
        scope_constraint
    );
END
$$;
SQL
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' 'DROP TABLE service_loop_heartbeats;'
} | run_psql

# Exercise the authority-arm upgrade from its exact preceding empty binding
# shape. Applying the reviewed file twice must retain one required column, its
# closed check, and the chain/name/arm exclusion domain.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE surface_bindings
    DROP CONSTRAINT surface_bindings_no_overlap,
    DROP CONSTRAINT surface_bindings_authority_arm_check,
    DROP COLUMN authority_arm;
ALTER TABLE surface_bindings
    ADD CONSTRAINT surface_bindings_no_overlap
    EXCLUDE USING gist (
        logical_name_id WITH =,
        tstzrange(active_from, COALESCE(active_to, 'infinity'::timestamptz), '[)') WITH &&
    )
    WHERE (canonicality_state IN ('canonical', 'safe', 'finalized'));

INSERT INTO manifest_versions (
    manifest_version, namespace, source_family, chain_id, deployment_label,
    rollout_status, normalizer_version, file_path, manifest_payload
) VALUES (
    1, 'schema-v2-check', 'authority-reset-sentinel', 'authority-reset',
    'authority-reset', 'draft', 'test', 'authority-reset-sentinel.toml', '{}'::jsonb
);
INSERT INTO chain_lineage (
    chain_id, block_hash, block_number, block_timestamp, canonicality_state
) VALUES (
    'authority-reset', '0x01', 1, to_timestamp(1), 'canonical'
);
INSERT INTO name_surfaces (
    logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
    namehash, labelhashes, normalizer_version, visibility_state,
    chain_id, block_hash, block_number, canonicality_state
) VALUES (
    'schema-v2-check:0xreset', 'schema-v2-check', 'reset', ARRAY['reset'],
    '\x'::bytea, '0xreset', ARRAY['0xreset'], 'test', 'active',
    'authority-reset', '0x01', 1, 'canonical'
);
INSERT INTO resources (
    resource_id, chain_id, block_hash, block_number, canonicality_state
) VALUES (
    '00000000-0000-0000-0000-000000000001',
    'authority-reset', '0x01', 1, 'canonical'
);
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, derivation_kind, canonicality_state
) VALUES (
    'authority-reset-normalized-sentinel', 'schema-v2-check',
    'SourceManifestUpdated', 'authority-reset-sentinel', 1,
    'authority-reset', 'manifest_sync', 'canonical'
);
SQL
} | run_psql
assert_migration_refusal \
    authority-arm-offline-reset \
    "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
    'surface binding authority-arm upgrade requires the reviewed offline binding reset before migration apply' <<'SQL'
INSERT INTO surface_bindings (
    surface_binding_id, logical_name_id, resource_id, binding_kind,
    active_from, chain_id, block_hash, block_number, canonicality_state
) VALUES (
    '00000000-0000-0000-0000-000000000002',
    'schema-v2-check:0xreset',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', to_timestamp(1),
    'authority-reset', '0x01', 1, 'canonical'
);
SQL
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO surface_bindings (
    surface_binding_id, logical_name_id, resource_id, binding_kind,
    active_from, chain_id, block_hash, block_number, canonicality_state
) VALUES (
    '00000000-0000-0000-0000-000000000002',
    'schema-v2-check:0xreset',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', to_timestamp(1),
    'authority-reset', '0x01', 1, 'canonical'
);
TRUNCATE TABLE
    name_current,
    address_names_current,
    surface_bindings
    CONTINUE IDENTITY RESTRICT;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM surface_bindings) THEN
        RAISE EXCEPTION 'targeted binding reset left historical bindings';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM manifest_versions
        WHERE file_path = 'authority-reset-sentinel.toml'
    ) OR NOT EXISTS (
        SELECT 1 FROM normalized_events
        WHERE event_identity = 'authority-reset-normalized-sentinel'
    ) THEN
        RAISE EXCEPTION 'targeted binding reset removed preserved identity metadata';
    END IF;
END
$$;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
        preceding-shape
    emit_phase_migration \
        "$ROOT/migrations/20260814130000_surface_binding_authority_arm.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    exclusion_definition text;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'surface_bindings'
          AND column_name = 'authority_arm'
          AND is_nullable = 'NO'
    ) THEN
        RAISE EXCEPTION 'authority-arm upgrade did not add the required column';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'surface_bindings'::regclass
          AND conname = 'surface_bindings_authority_arm_check'
    ) THEN
        RAISE EXCEPTION 'authority-arm upgrade did not add its closed check';
    END IF;
    SELECT pg_get_constraintdef(oid)
    INTO exclusion_definition
    FROM pg_constraint
    WHERE conrelid = 'surface_bindings'::regclass
      AND conname = 'surface_bindings_no_overlap';
    IF exclusion_definition NOT LIKE '%chain_id WITH =%'
        OR exclusion_definition NOT LIKE '%logical_name_id WITH =%'
        OR exclusion_definition NOT LIKE '%authority_arm WITH =%'
    THEN
        RAISE EXCEPTION
            'authority-arm exclusion has the wrong domain: %', exclusion_definition;
    END IF;
END
$$;
SQL
} | run_psql

# Exercise the initialized pre-change schema branch: normalized events and
# unrelated data already exist, while the new redo handoff does not.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind, canonicality_state
) VALUES (
    'redo-handoff-upgrade-sentinel', 'schema-v2-check',
    'SourceManifestUpdated', 'schema-check', 1, 'schema-v2-check',
    'manifest_sync', 'finalized'
);
DROP TABLE project_redo_resolver_evidence;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260814120000_project_redo_resolver_evidence.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    constraint_count bigint;
    index_is_ready boolean;
BEGIN
    IF to_regclass(current_schema() || '.project_redo_resolver_evidence') IS NULL THEN
        RAISE EXCEPTION 'initialized-schema upgrade did not create redo handoff';
    END IF;

    SELECT count(*)
    INTO constraint_count
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'project_redo_resolver_evidence'::regclass
      AND constraint_row.contype IN ('p', 'c');
    IF constraint_count <> 4 THEN
        RAISE EXCEPTION
            'initialized-schema redo handoff has % required constraints, expected 4',
            constraint_count;
    END IF;

    SELECT index_state.indisvalid
       AND index_state.indisready
       AND index_state.indislive
    INTO index_is_ready
    FROM pg_class index_relation
    JOIN pg_namespace namespace
      ON namespace.oid = index_relation.relnamespace
    JOIN pg_index index_state
      ON index_state.indexrelid = index_relation.oid
    WHERE namespace.nspname = current_schema()
      AND index_relation.relname = 'project_redo_resolver_evidence_range_idx';
    IF index_is_ready IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'initialized-schema redo handoff index is not ready';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM normalized_events
        WHERE event_identity = 'redo-handoff-upgrade-sentinel'
    ) THEN
        RAISE EXCEPTION 'initialized-schema upgrade changed existing normalized data';
    END IF;
END
$$;
DELETE FROM normalized_events
WHERE event_identity = 'redo-handoff-upgrade-sentinel';
SQL
} | run_psql

# Exercise the initialized pre-change schema branch for the bounded path-expiry
# handoff. Existing normalized events must survive while the schema-migrations
# add the table and then extend its scope to permission resources.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind, canonicality_state
) VALUES (
    'expiry-redo-handoff-upgrade-sentinel', 'schema-v2-check',
    'SourceManifestUpdated', 'schema-check', 1, 'schema-v2-check',
    'manifest_sync', 'finalized'
);
DROP TABLE project_redo_expiry_roots;
SQL
    emit_phase_migration \
        "$ROOT/migrations/20260902140000_project_redo_expiry_roots.sql" \
        preceding-shape
    emit_phase_migration \
        "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
        preceding-shape
    emit_phase_migration \
        "$ROOT/migrations/20260902150000_project_redo_expiry_resources.sql" \
        preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    constraint_count bigint;
    index_is_ready boolean;
    logical_name_is_nullable boolean;
    resource_id_is_nullable boolean;
BEGIN
    IF to_regclass(current_schema() || '.project_redo_expiry_roots') IS NULL THEN
        RAISE EXCEPTION
            'initialized-schema upgrade did not create path-expiry handoff';
    END IF;

    SELECT count(*)
    INTO constraint_count
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'project_redo_expiry_roots'::regclass
      AND constraint_row.contype IN ('p', 'c');
    IF constraint_count <> 4 THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry handoff has % required constraints, expected 4',
            constraint_count;
    END IF;

    SELECT is_nullable = 'YES'
    INTO logical_name_is_nullable
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'project_redo_expiry_roots'
      AND column_name = 'logical_name_id';
    IF logical_name_is_nullable IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry logical name is not nullable';
    END IF;

    SELECT is_nullable = 'YES'
    INTO resource_id_is_nullable
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'project_redo_expiry_roots'
      AND column_name = 'resource_id';
    IF resource_id_is_nullable IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry resource is absent or not nullable';
    END IF;

    SELECT index_state.indisvalid
       AND index_state.indisready
       AND index_state.indislive
    INTO index_is_ready
    FROM pg_class index_relation
    JOIN pg_namespace namespace
      ON namespace.oid = index_relation.relnamespace
    JOIN pg_index index_state
      ON index_state.indexrelid = index_relation.oid
    WHERE namespace.nspname = current_schema()
      AND index_relation.relname = 'project_redo_expiry_roots_range_idx';
    IF index_is_ready IS DISTINCT FROM true THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry handoff index is not ready';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM normalized_events
        WHERE event_identity = 'expiry-redo-handoff-upgrade-sentinel'
    ) THEN
        RAISE EXCEPTION
            'initialized-schema path-expiry handoff changed normalized data';
    END IF;
END
$$;
DELETE FROM normalized_events
WHERE event_identity = 'expiry-redo-handoff-upgrade-sentinel';
SQL
} | run_psql

# Exercise and verify the in-place comment upgrade on an initialized schema.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "COMMENT ON COLUMN service_heartbeats.heartbeat_at IS" \
        "    'This time records the latest completed work unit.';"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814121000_phase_heartbeat_liveness_comment.sql" \
        preceding-shape \
        | run_psql
done
heartbeat_comment_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "SELECT CASE WHEN col_description('service_heartbeats'::regclass," \
        "    (SELECT attnum FROM pg_attribute" \
        "     WHERE attrelid = 'service_heartbeats'::regclass" \
        "       AND attname = 'heartbeat_at')) =" \
        "    'This time records runner liveness, including refreshes during storage-capacity waits.'" \
        "THEN 'heartbeat_liveness_comment_exact'" \
        "ELSE 'heartbeat_liveness_comment_wrong' END;"
} | run_psql)"
if [[ "$heartbeat_comment_check" != *heartbeat_liveness_comment_exact* ]]; then
    printf '%s\n' "heartbeat liveness comment upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema unconfigured-settlement upgrade from its preceding
# shape. The existing row must stay NULL, and both additive migrations must be
# idempotent after the constraint has been validated.
assert_unconfigured_settlement_constraint baseline
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "INSERT INTO chain_phase_state (chain_id, phase_name)" \
        "VALUES ('verify-settlement-upgrade-check', 'verify');" \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_unconfigured_settlement_check," \
        "    DROP COLUMN settled_while_unconfigured;"
} | run_psql
for ignored in 1 2; do
    for migration_file in \
        "$ROOT/migrations/20260814122000_verify_unconfigured_settlement.sql" \
        "$ROOT/migrations/20260814122100_verify_unconfigured_settlement_validate.sql"
    do
        emit_phase_migration "$migration_file" preceding-shape | run_psql
    done
done
assert_unconfigured_settlement_constraint migration
verify_settlement_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    (SELECT settled_while_unconfigured IS NULL
     FROM chain_phase_state
     WHERE chain_id = 'verify-settlement-upgrade-check'
       AND phase_name = 'verify')
    AND (SELECT settled_while_unconfigured IS TRUE
         FROM chain_phase_state
         WHERE chain_id = 'phase-settlement-migration-true'
           AND phase_name = 'ingest')
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'settled_while_unconfigured'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'settled_while_unconfigured'
           AND NOT attisdropped)
    ) = 'True only when startup settled an active phase row for a chain absent from runtime configuration; NULL identifies ordinary phase state.'
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_unconfigured_settlement_check'
          AND convalidated
    )
THEN 'verify_settlement_upgrade_exact'
ELSE 'verify_settlement_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$verify_settlement_upgrade_check" != *verify_settlement_upgrade_exact* ]]; then
    printf '%s\n' "Verify settlement provenance upgrade was not applied exactly" >&2
    exit 1
fi

# Exercise the initialized-schema Ingest redo boundary-marker upgrade from its
# preceding shape, then verify baseline and schema-migration parity.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_ingest_redo_source_boundaries_check," \
        "    DROP COLUMN redo_source_boundary_markers;"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814123000_ingest_redo_source_boundary_markers.sql" \
        preceding-shape \
        | run_psql
done
redo_source_boundary_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'redo_source_boundary_markers'
          AND data_type = 'jsonb'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_ingest_redo_source_boundaries_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%phase_name = ''ingest''%'
          AND pg_get_constraintdef(oid) LIKE '%redo_in_progress%'
          AND pg_get_constraintdef(oid) LIKE '%jsonb_typeof%'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname <> 'chain_phase_state_ingest_redo_source_boundaries_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%redo_previous_phase_status%'
          AND pg_get_constraintdef(oid) LIKE '%redo_from_block_number%'
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'redo_source_boundary_markers'
           AND NOT attisdropped)
    ) = 'This object maps each Ingest source key to a block number and hash returned by a boundary load during the active redo.'
THEN 'redo_source_boundary_upgrade_ok'
ELSE 'redo_source_boundary_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$redo_source_boundary_upgrade_check" != *redo_source_boundary_upgrade_ok* ]]; then
    printf '%s\n' "Ingest redo source-boundary upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema redo-attempt generation upgrade from its
# preceding shape, then verify baseline and schema-migration parity.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_redo_attempt_generation_check," \
        "    DROP COLUMN redo_attempt_generation;"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814124000_redo_attempt_generation.sql" \
        preceding-shape \
        | run_psql
    emit_phase_migration \
        "$ROOT/migrations/20260825041728_redo_attempt_generation_comment.sql" \
        preceding-shape \
        | run_psql
    emit_phase_migration \
        "$ROOT/migrations/20260831140000_discovery_watch_admissions.sql" \
        baseline-first \
        | run_psql
done
redo_attempt_generation_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'redo_attempt_generation'
          AND data_type = 'bigint'
          AND is_nullable = 'NO'
          AND column_default = '0'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_redo_attempt_generation_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%redo_attempt_generation >= 0%'
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'redo_attempt_generation'
           AND NOT attisdropped)
    ) = 'This nonnegative, row-local counter increments when an explicit redo begins, when the phase runner installs or extends required downstream redo, and when the shared required-Ingest installer records genuinely new manifest or discovery demand. Repeated observation of unchanged semantic demand is suppressed before installation and does not advance it.'
THEN 'redo_attempt_generation_upgrade_ok'
ELSE 'redo_attempt_generation_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$redo_attempt_generation_upgrade_check" != *redo_attempt_generation_upgrade_ok* ]]; then
    printf '%s\n' "Redo attempt generation upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema Ingest redo manifest-fingerprint upgrade from
# its preceding shape, then verify baseline and schema-migration parity.
baseline_redo_manifest_authority_constraint="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'chain_phase_state'::regclass
  AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
  AND convalidated;
SQL
} | run_psql)"
if [[ -z "$baseline_redo_manifest_authority_constraint" ]]; then
    printf '%s\n' "Baseline is missing the Ingest redo manifest-fingerprint constraint" >&2
    exit 1
fi
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "ALTER TABLE chain_phase_state" \
        "    DROP CONSTRAINT chain_phase_state_ingest_redo_manifest_authority_check," \
        "    DROP COLUMN redo_manifest_authority_fingerprint;"
} | run_psql
for ignored in 1 2; do
    emit_phase_migration \
        "$ROOT/migrations/20260814125000_ingest_redo_manifest_authority.sql" \
        preceding-shape \
        | run_psql
done
migration_redo_manifest_authority_constraint="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'chain_phase_state'::regclass
  AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
  AND convalidated;
SQL
} | run_psql)"
if [[ "$migration_redo_manifest_authority_constraint" != "$baseline_redo_manifest_authority_constraint" ]]; then
    printf '%s\n' "Baseline and schema-migration Ingest redo evidence constraints differ" >&2
    exit 1
fi
redo_manifest_authority_upgrade_check="$({
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
SELECT CASE WHEN
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'chain_phase_state'
          AND column_name = 'redo_manifest_authority_fingerprint'
          AND data_type = 'text'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chain_phase_state'::regclass
          AND conname = 'chain_phase_state_ingest_redo_manifest_authority_check'
          AND convalidated
          AND pg_get_constraintdef(oid) LIKE '%phase_name = ''ingest''%'
          AND pg_get_constraintdef(oid) LIKE '%redo_in_progress%'
          AND pg_get_constraintdef(oid) LIKE '%redo_manifest_authority_fingerprint ~%'
    )
    AND col_description(
        'chain_phase_state'::regclass,
        (SELECT attnum
         FROM pg_attribute
         WHERE attrelid = 'chain_phase_state'::regclass
           AND attname = 'redo_manifest_authority_fingerprint'
           AND NOT attisdropped)
    ) = 'For an active Ingest redo, this value binds resumable numeric and per-source boundary evidence to the chain''s active manifest rows, excluding normalizer_version.'
THEN 'redo_manifest_authority_upgrade_ok'
ELSE 'redo_manifest_authority_upgrade_wrong' END;
SQL
} | run_psql)"
if [[ "$redo_manifest_authority_upgrade_check" != *redo_manifest_authority_upgrade_ok* ]]; then
    printf '%s\n' "Ingest redo manifest-fingerprint upgrade was not applied" >&2
    exit 1
fi

# Exercise the initialized-schema upgrade against the preceding closed
# vocabularies. Rewrite only the qualified schema name so the checked-in
# schema-migration runs against this isolated scratch namespace.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
ALTER TABLE surface_bindings
    DROP CONSTRAINT surface_bindings_binding_kind_check,
    ADD CONSTRAINT surface_bindings_binding_kind_check
        CHECK (
            binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'migration_rebind',
                'observed_only'
            )
        );
ALTER TABLE name_current
    DROP CONSTRAINT name_current_binding_kind_check,
    ADD CONSTRAINT name_current_binding_kind_check
        CHECK (
            binding_kind IS NULL
            OR binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'migration_rebind',
                'observed_only'
            )
        );
ALTER TABLE address_names_current
    DROP CONSTRAINT address_names_current_binding_kind_check,
    ADD CONSTRAINT address_names_current_binding_kind_check
        CHECK (
            binding_kind IN (
                'declared_registry_path',
                'linked_subregistry_path',
                'resolver_alias_path',
                'observed_wildcard_path',
                'migration_rebind',
                'observed_only'
            )
        );
ALTER TABLE permissions_current
    DROP CONSTRAINT permissions_current_scope_kind_check,
    ADD CONSTRAINT permissions_current_scope_kind_check
        CHECK (
            scope_kind IN (
                'root',
                'registry',
                'resource',
                'resolver',
                'record_manager',
                'migration_derived',
                'transport_derived'
            )
        );
INSERT INTO normalized_events (
    event_identity, namespace, event_kind, source_family, manifest_version,
    chain_id, derivation_kind, after_state
) VALUES (
    'remove-l2-safe-event', 'schema-v2-check', 'PermissionChanged',
    'schema-check', 1, 'schema-v2-check', 'ens_v2_permissions',
    '{"scope":{"kind":"resource"}}'::jsonb
);
INSERT INTO surface_bindings (
    surface_binding_id, logical_name_id, resource_id, binding_kind,
    authority_arm, active_from, chain_id, block_hash, block_number,
    canonicality_state
) VALUES
    ('00000000-0000-0000-0000-000000000030', 'schema-v2-check:0xreset',
     '00000000-0000-0000-0000-000000000001', 'declared_registry_path',
     'ens_v1', to_timestamp(2), 'authority-reset', '0x01', 1, 'canonical'),
    ('00000000-0000-0000-0000-000000000031', 'schema-v2-check:0xreset',
     '00000000-0000-0000-0000-000000000001', 'declared_registry_path',
     'ens_v2', to_timestamp(2), 'authority-reset', '0x01', 1, 'canonical');
INSERT INTO name_current (
    logical_name_id, namespace, raw_name, namehash, surface_binding_id,
    resource_id, binding_kind, support_status, manifest_version
) VALUES (
    'schema-v2-check:0xreset', 'schema-v2-check', 'reset', '0xreset',
    '00000000-0000-0000-0000-000000000031',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', 'supported', 1
);
INSERT INTO address_names_current (
    address, logical_name_id, relation, namespace, raw_name, namehash,
    surface_binding_id, resource_id, binding_kind, support_status,
    manifest_version
) VALUES (
    'remove-l2-address', 'schema-v2-check:0xreset', 'registrant',
    'schema-v2-check', 'reset', '0xreset',
    '00000000-0000-0000-0000-000000000031',
    '00000000-0000-0000-0000-000000000001',
    'declared_registry_path', 'supported', 1
);
INSERT INTO permissions_current (
    resource_id, subject, scope, scope_kind, manifest_version
) VALUES (
    '00000000-0000-0000-0000-000000000001',
    'remove-l2-subject', 'remove-l2-scope', 'resource', 1
);
SQL
} | run_psql

remove_l2_migration="$ROOT/migrations/20260810120000_remove_l2_migration_remnants.sql"
assert_migration_refusal remove-l2-normalized-events "$remove_l2_migration" \
    'cannot remove permission scopes: normalized events still use removed values' <<'SQL'
UPDATE normalized_events SET after_state = '{"scope":{"kind":"migration_derived"}}'
WHERE event_identity = 'remove-l2-safe-event';
SQL
assert_migration_refusal remove-l2-surface-bindings "$remove_l2_migration" \
    'cannot remove migration_rebind: surface bindings still use it' <<'SQL'
UPDATE surface_bindings SET binding_kind = 'migration_rebind'
WHERE surface_binding_id = '00000000-0000-0000-0000-000000000030';
SQL
assert_migration_refusal remove-l2-name-current "$remove_l2_migration" \
    'cannot remove migration_rebind: current names still use it' <<'SQL'
ALTER TABLE name_current ALTER CONSTRAINT
    name_current_surface_binding_id_logical_name_id_resource_i_fkey
    DEFERRABLE INITIALLY DEFERRED;
UPDATE name_current SET binding_kind = 'migration_rebind'
WHERE logical_name_id = 'schema-v2-check:0xreset';
SQL
assert_migration_refusal remove-l2-address-names-current "$remove_l2_migration" \
    'cannot remove migration_rebind: current address-name rows still use it' <<'SQL'
ALTER TABLE address_names_current ALTER CONSTRAINT
    address_names_current_surface_binding_id_logical_name_id_r_fkey
    DEFERRABLE INITIALLY DEFERRED;
UPDATE address_names_current SET binding_kind = 'migration_rebind'
WHERE address = 'remove-l2-address';
SQL
assert_migration_refusal remove-l2-permissions-current "$remove_l2_migration" \
    'cannot remove permission scopes: current rows still use removed values' <<'SQL'
UPDATE permissions_current SET scope_kind = 'migration_derived'
WHERE subject = 'remove-l2-subject';
SQL

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "DELETE FROM address_names_current WHERE address = 'remove-l2-address';" \
        "DELETE FROM name_current WHERE logical_name_id = 'schema-v2-check:0xreset';" \
        "DELETE FROM permissions_current WHERE subject = 'remove-l2-subject';" \
        "DELETE FROM surface_bindings WHERE surface_binding_id IN (" \
        "    '00000000-0000-0000-0000-000000000030'," \
        "    '00000000-0000-0000-0000-000000000031'" \
        ");" \
        "DELETE FROM normalized_events WHERE event_identity = 'remove-l2-safe-event';"
    emit_phase_migration "$remove_l2_migration" preceding-shape
    cat <<'SQL'
DO $$
DECLARE
    removed_vocabulary_count bigint;
BEGIN
    SELECT count(*)
    INTO removed_vocabulary_count
    FROM pg_constraint constraint_row
    WHERE constraint_row.conname IN (
        'surface_bindings_binding_kind_check',
        'name_current_binding_kind_check',
        'address_names_current_binding_kind_check',
        'permissions_current_scope_kind_check'
    )
      AND constraint_row.conrelid IN (
          'surface_bindings'::regclass,
          'name_current'::regclass,
          'address_names_current'::regclass,
          'permissions_current'::regclass
      )
      AND pg_get_constraintdef(constraint_row.oid) ~
          '(migration_rebind|migration_derived|transport_derived)';

    IF removed_vocabulary_count <> 0 THEN
        RAISE EXCEPTION
            'initialized-schema upgrade retained removed vocabulary in % constraints',
            removed_vocabulary_count;
    END IF;
END
$$;
SQL
} | run_psql

# Exercise the ENSv1-to-ENSv2 vocabulary upgrade from the exact preceding
# normalized-event shape. This catches deployed-schema drift independently of
# the idempotent fresh-install baseline check above.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DROP TABLE migration_candidate_discovery_effects;
DROP TABLE migration_candidate_identity_effects;
DROP TABLE migration_discovery_associations;
DROP TABLE migration_event_associations;
ALTER TABLE normalized_events
    DROP CONSTRAINT normalized_events_event_kind_check,
    DROP CONSTRAINT normalized_events_derivation_kind_check,
    DROP CONSTRAINT normalized_events_migration_correlation_ids_check,
    DROP CONSTRAINT normalized_events_consumer_visibility_check,
    DROP CONSTRAINT normalized_events_candidate_correlation_check,
    DROP COLUMN migration_correlation_ids,
    DROP COLUMN consumer_visibility,
    ADD CONSTRAINT normalized_events_event_kind_check
        CHECK (
            event_kind IN (
                'AliasChanged',
                'AuthorityEpochChanged',
                'AuthorityTransferred',
                'ExpiryChanged',
                'ParentChanged',
                'PermissionChanged',
                'PermissionScopeChanged',
                'PreimageObserved',
                'RecordChanged',
                'RecordVersionChanged',
                'RegistrarNameRegistered',
                'RegistrationGranted',
                'RegistrationReleased',
                'RegistrationRenewed',
                'RegistrationReserved',
                'RegistryCreated',
                'ResolverChanged',
                'ReverseChanged',
                'RootPermissionChanged',
                'SourceManifestUpdated',
                'SubregistryChanged',
                'SurfaceBound',
                'SurfaceUnbound',
                'TokenControlTransferred',
                'TokenRegenerated',
                'TokenResourceLinked',
                'Upgraded'
            )
        ),
    ADD CONSTRAINT normalized_events_derivation_kind_check
        CHECK (
            derivation_kind IN (
                'ens_v1_reverse_claim',
                'ens_v1_unwrapped_authority',
                'ens_v2_permissions',
                'ens_v2_registrar',
                'ens_v2_registry_resource_surface',
                'ens_v2_resolver',
                'manifest_sync',
                'proxy_upgrade',
                'raw_log_preimage_observation'
            )
        );
DROP FUNCTION migration_correlation_ids_valid(text[]);
DROP TABLE account_permission_state_current;
DROP INDEX permissions_current_resource_registry_binding_idx;
ALTER TABLE permissions_current_resource_summary
    DROP CONSTRAINT permissions_current_resource_summary_registry_binding_check,
    DROP COLUMN registry_owner,
    DROP COLUMN registry_contract,
    DROP COLUMN registry_binding_provenance,
    DROP COLUMN registry_binding_chain_positions;
SQL
    for migration_file in \
        "$ROOT/migrations/20260811120000_ens_v2_migration_slice_1.sql" \
        "$ROOT/migrations/20260811120100_ens_v2_migration_slice_1_validate.sql" \
        "$ROOT/migrations/20260811120200_ens_v2_migration_slice_1_constraints.sql" \
        "$ROOT/migrations/20260820140000_raw_block_preimage_derivation.sql" \
        "$ROOT/migrations/20260820140100_raw_block_preimage_derivation_validate.sql" \
        "$ROOT/migrations/20260820140200_raw_block_preimage_derivation_swap.sql" \
        "$ROOT/migrations/20260902160000_registry_operator_account_permissions.sql" \
        "$ROOT/migrations/20260902160100_registry_operator_account_permissions_validate.sql" \
        "$ROOT/migrations/20260902160200_registry_operator_account_permissions_swap.sql"
    do
        emit_phase_migration "$migration_file" preceding-shape
    done
} | run_psql

# The production functions intentionally bind their SECURITY DEFINER lookups
# to bigname_phase. Prove that contract before rebinding only this scratch
# schema's copies so the remainder of this isolated harness can exercise them.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    unsafe_function_count bigint;
BEGIN
    SELECT count(*)
    INTO unsafe_function_count
    FROM pg_proc procedure
    JOIN pg_namespace namespace
      ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname = current_schema()
      AND procedure.proname IN (
          'revalidate_resolution_lookup_state',
          'write_resolution_divergence'
      )
      AND procedure.proconfig @>
          ARRAY['search_path=pg_catalog, bigname_phase, pg_temp']::text[];

    IF unsafe_function_count <> 2 THEN
        RAISE EXCEPTION
            'lookup SECURITY DEFINER functions lack the fixed production search path';
    END IF;
END
$$;
SQL
    printf \
        'ALTER FUNCTION "%s".revalidate_resolution_lookup_state(text, bigint, text, jsonb, jsonb, uuid, text, text) SET search_path = pg_catalog, "%s", pg_temp;\n' \
        "$scratch_schema" "$scratch_schema"
    printf \
        'ALTER FUNCTION "%s".write_resolution_divergence(uuid, text, text, text, bigint, text, jsonb, text, text, text, text, jsonb, jsonb, boolean) SET search_path = pg_catalog, "%s", pg_temp;\n' \
        "$scratch_schema" "$scratch_schema"
} | run_psql

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    missing_tables text;
    unexpected_tables text;
    uncommented_tables text;
    uncommented_columns text;
    forbidden_tables text;
    forbidden_columns text;
    forbidden_projection_publication_tables text;
    raw_table_without_hash_key text;
    missing_behavioral_constraints text;
    unbounded_btree_columns text;
BEGIN
    WITH expected(table_name) AS (
        VALUES
            ('account_permission_state_current'),
            ('address_names_current'),
            ('chain_heads'),
            ('chain_header_audit'),
            ('chain_lineage'),
            ('chain_phase_state'),
            ('children_current'),
            ('contract_instance_addresses'),
            ('contract_instances'),
            ('discovery_watch_admissions'),
            ('discovery_edges'),
            ('ens_names'),
            ('ingest_cursors'),
            ('interpret_decode_skips'),
            ('label_preimages'),
            ('manifest_contract_instances'),
            ('manifest_discovery_rules'),
            ('manifest_authority_attestations'),
            ('manifest_versions'),
            ('migration_candidate_discovery_effects'),
            ('migration_candidate_identity_effects'),
            ('migration_discovery_associations'),
            ('migration_event_associations'),
            ('name_current'),
            ('name_surfaces'),
            ('normalized_events'),
            ('project_generation_failures'),
            ('project_redo_child_registration_history'),
            ('project_redo_expiry_roots'),
            ('project_redo_resolver_evidence'),
            ('permissions_current'),
            ('permissions_current_resource_summary'),
            ('primary_names_current'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions'),
            ('record_inventory_current'),
            ('resolution_divergences'),
            ('resolver_current'),
            ('resources'),
            ('service_heartbeats'),
            ('surface_bindings'),
            ('token_lineages')
    )
    SELECT string_agg(expected.table_name, ', ' ORDER BY expected.table_name)
    INTO missing_tables
    FROM expected
    LEFT JOIN information_schema.tables actual
      ON actual.table_schema = current_schema()
     AND actual.table_name = expected.table_name
     AND actual.table_type = 'BASE TABLE'
    WHERE actual.table_name IS NULL;

    IF missing_tables IS NOT NULL THEN
        RAISE EXCEPTION 'missing schema-v2 tables: %', missing_tables;
    END IF;

    IF to_regclass('account_permission_state_current_active_subject_idx') IS NULL
       OR to_regclass('account_permission_state_current_applicability_idx') IS NULL
       OR to_regclass('permissions_current_resource_registry_binding_idx') IS NULL
    THEN
        RAISE EXCEPTION 'registry-operator projection indexes are incomplete';
    END IF;

    WITH expected(table_name) AS (
        VALUES
            ('account_permission_state_current'),
            ('address_names_current'),
            ('chain_heads'),
            ('chain_header_audit'),
            ('chain_lineage'),
            ('chain_phase_state'),
            ('children_current'),
            ('contract_instance_addresses'),
            ('contract_instances'),
            ('discovery_watch_admissions'),
            ('discovery_edges'),
            ('ens_names'),
            ('ingest_cursors'),
            ('interpret_decode_skips'),
            ('label_preimages'),
            ('manifest_contract_instances'),
            ('manifest_discovery_rules'),
            ('manifest_authority_attestations'),
            ('manifest_versions'),
            ('migration_candidate_discovery_effects'),
            ('migration_candidate_identity_effects'),
            ('migration_discovery_associations'),
            ('migration_event_associations'),
            ('name_current'),
            ('name_surfaces'),
            ('normalized_events'),
            ('project_generation_failures'),
            ('project_redo_child_registration_history'),
            ('project_redo_expiry_roots'),
            ('project_redo_resolver_evidence'),
            ('permissions_current'),
            ('permissions_current_resource_summary'),
            ('primary_names_current'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions'),
            ('record_inventory_current'),
            ('resolution_divergences'),
            ('resolver_current'),
            ('resources'),
            ('service_heartbeats'),
            ('surface_bindings'),
            ('token_lineages')
    )
    SELECT string_agg(actual.table_name, ', ' ORDER BY actual.table_name)
    INTO unexpected_tables
    FROM information_schema.tables actual
    LEFT JOIN expected
      ON expected.table_name = actual.table_name
    WHERE actual.table_schema = current_schema()
      AND actual.table_type = 'BASE TABLE'
      AND expected.table_name IS NULL;

    IF unexpected_tables IS NOT NULL THEN
        RAISE EXCEPTION 'unexpected schema-v2 tables: %', unexpected_tables;
    END IF;

    -- Add exact exceptions only after maintainer authorization.
    -- `project_generation_failures` is the contracted name of the append-only
    -- projection-generation failure audit (docs/storage.md, table ownership and
    -- "Projection publication"); it is not retention-generation state.
    WITH maintainer_authorized_allowlist(table_name) AS (
        VALUES ('project_generation_failures')
    )
    SELECT string_agg(actual.table_name, ', ' ORDER BY actual.table_name)
    INTO forbidden_tables
    FROM information_schema.tables actual
    LEFT JOIN maintainer_authorized_allowlist
      ON maintainer_authorized_allowlist.table_name = actual.table_name
    WHERE actual.table_schema = current_schema()
      AND actual.table_type = 'BASE TABLE'
      AND maintainer_authorized_allowlist.table_name IS NULL
      AND (
          actual.table_name ~
              '(coverage|backfill|lease|generation|revision)'
          OR actual.table_name ~
              '(checkpoint|frontier|queue|dead_letter|watermark)'
          OR actual.table_name ~
              '(code_hash|execution_trace|execution_step)'
          OR actual.table_name ~ '(outcome_cache|raw_call|startup)'
          OR actual.table_name ~
              '(fence|epoch|journal|promotion|reconciliation|rederive)'
          OR actual.table_name ~
              '(drift|alert|replay_version|dead_letter|watermark|staging)'
          OR actual.table_name = 'manifest_capability_flags'
      );

    IF forbidden_tables IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 tables: %', forbidden_tables;
    END IF;

    -- Add exact exceptions only after maintainer authorization.
    WITH maintainer_authorized_allowlist(table_name, column_name) AS (
        VALUES
            ('chain_heads', 'lineage_orphaning_epoch'),
            ('discovery_watch_admissions', 'lineage_orphaning_epoch'),
            ('manifest_authority_attestations', 'generation_token'),
            ('chain_phase_state', 'redo_attempt_generation')
    )
    SELECT string_agg(
        format('%I.%I', actual.table_name, actual.column_name),
        ', '
        ORDER BY actual.table_name, actual.ordinal_position
    )
    INTO forbidden_columns
    FROM information_schema.columns actual
    LEFT JOIN maintainer_authorized_allowlist
      ON maintainer_authorized_allowlist.table_name = actual.table_name
     AND maintainer_authorized_allowlist.column_name = actual.column_name
    WHERE actual.table_schema = current_schema()
      AND maintainer_authorized_allowlist.column_name IS NULL
      AND (
          actual.column_name ~
              '(coverage|exhaustiveness|generation|revision)'
          OR actual.column_name ~ '(supersed|repair|capability)'
          OR actual.column_name ~
              '(fence|epoch|journal|promotion|reconciliation|rederive)'
          OR actual.column_name ~
              '(drift|alert|replay_version|dead_letter|watermark|staging)'
          OR actual.column_name = 'code_hash'
      );

    IF forbidden_columns IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 columns: %', forbidden_columns;
    END IF;

    SELECT string_agg(table_name, ', ' ORDER BY table_name)
    INTO forbidden_projection_publication_tables
    FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_type = 'BASE TABLE'
      AND table_name ~ '(_staging|_publication)$';

    IF forbidden_projection_publication_tables IS NOT NULL THEN
        RAISE EXCEPTION
            'schema-v2 contains forbidden projection publication tables: %',
            forbidden_projection_publication_tables;
    END IF;

    SELECT string_agg(class.relname, ', ' ORDER BY class.relname)
    INTO uncommented_tables
    FROM pg_class class
    JOIN pg_namespace namespace
      ON namespace.oid = class.relnamespace
    WHERE namespace.nspname = current_schema()
      AND class.relkind = 'r'
      AND obj_description(class.oid, 'pg_class') IS NULL;

    IF uncommented_tables IS NOT NULL THEN
        RAISE EXCEPTION 'schema-v2 tables without comments: %', uncommented_tables;
    END IF;

    SELECT string_agg(
        format('%I.%I', class.relname, attribute.attname),
        ', '
        ORDER BY class.relname, attribute.attnum
    )
    INTO uncommented_columns
    FROM pg_class class
    JOIN pg_namespace namespace
      ON namespace.oid = class.relnamespace
    JOIN pg_attribute attribute
      ON attribute.attrelid = class.oid
    WHERE namespace.nspname = current_schema()
      AND class.relkind = 'r'
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped
      AND col_description(class.oid, attribute.attnum) IS NULL;

    IF uncommented_columns IS NOT NULL THEN
        RAISE EXCEPTION 'schema-v2 columns without comments: %', uncommented_columns;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'normalized_events'
          AND column_name ~ '(repair|supersed|authority|revision|generation)'
    ) THEN
        RAISE EXCEPTION 'normalized_events contains repair apparatus';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN (
              'name_current',
              'address_names_current',
              'permissions_current',
              'account_permission_state_current',
              'permissions_current_resource_summary',
              'record_inventory_current',
              'resolver_current',
              'children_current',
              'primary_names_current'
          )
          AND column_name IN (
              'coverage',
              'exhaustiveness',
              'enumeration_basis',
              'explicit_gaps'
          )
    ) THEN
        RAISE EXCEPTION 'a current projection contains exhaustiveness accounting';
    END IF;

    WITH required(table_name) AS (
        VALUES
            ('chain_header_audit'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions')
    )
    SELECT string_agg(required.table_name, ', ' ORDER BY required.table_name)
    INTO raw_table_without_hash_key
    FROM required
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        JOIN pg_class class
          ON class.oid = constraint_row.conrelid
        JOIN pg_namespace namespace
          ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = current_schema()
          AND class.relname = required.table_name
          AND constraint_row.contype = 'p'
          AND EXISTS (
              SELECT 1
              FROM unnest(constraint_row.conkey) AS key_column(attnum)
              JOIN pg_attribute attribute
                ON attribute.attrelid = class.oid
               AND attribute.attnum = key_column.attnum
              WHERE attribute.attname = 'block_hash'
          )
    );

    IF raw_table_without_hash_key IS NOT NULL THEN
        RAISE EXCEPTION
            'raw tables without block_hash primary keys: %',
            raw_table_without_hash_key;
    END IF;

    WITH invariant_checks(invariant_name, is_present) AS (
        SELECT
            'canonical surface bindings cannot overlap',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'surface_bindings'::regclass
                  AND conname = 'surface_bindings_no_overlap'
                  AND contype = 'x'
            )
        UNION ALL
        SELECT
            'one address row is active per contract instance',
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'contract_instance_addresses_active_instance_idx'
                  AND indexdef LIKE 'CREATE UNIQUE INDEX%'
                  AND indexdef LIKE '%(contract_instance_id)%'
                  AND indexdef LIKE '%deactivated_at IS NULL%'
            )
        UNION ALL
        SELECT
            'manifest contract declarations cascade on manifest deletion',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'manifest_contract_instances'::regclass
                  AND conname =
                      'manifest_contract_instances_manifest_fkey'
                  AND contype = 'f'
                  AND confdeltype = 'c'
            )
        UNION ALL
        SELECT
            'manifest discovery rules cascade on manifest deletion',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'manifest_discovery_rules'::regclass
                  AND conname = 'manifest_discovery_rules_manifest_fkey'
                  AND contype = 'f'
                  AND confdeltype = 'c'
            )
        UNION ALL
        SELECT
            'migration event associations retain missing normalized-event parents',
            NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'migration_event_associations'::regclass
                  AND confrelid = 'normalized_events'::regclass
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'receipt transaction positions match their transactions',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'raw_receipts'::regclass
                  AND conname = 'raw_receipts_transaction_position_fkey'
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'log transaction positions match their transactions',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'raw_logs'::regclass
                  AND conname = 'raw_logs_transaction_position_fkey'
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'chain lineage canonicality transitions are constrained',
            EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgrelid = 'chain_lineage'::regclass
                  AND tgname =
                      'chain_lineage_enforce_canonicality_transition'
                  AND NOT tgisinternal
            )
        UNION ALL
        SELECT
            'chain heads carry a nonnegative lineage orphaning epoch',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'chain_heads'
                  AND column_name = 'lineage_orphaning_epoch'
                  AND data_type = 'bigint'
                  AND is_nullable = 'NO'
                  AND column_default IN ('0', '0::bigint')
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'chain_heads'::regclass
                  AND conname = 'chain_heads_lineage_orphaning_epoch_check'
                  AND contype = 'c'
            )
        UNION ALL
        SELECT
            'divergence writes guard the compared record inventory row',
            EXISTS (
                SELECT 1
                FROM pg_proc procedure
                JOIN pg_namespace namespace
                  ON namespace.oid = procedure.pronamespace
                WHERE namespace.nspname = current_schema()
                  AND procedure.proname = 'write_resolution_divergence'
                  AND pg_get_function_identity_arguments(procedure.oid) =
                      'compared_resource_id uuid, compared_boundary_key text, compared_row_xmin text, requested_authoritative_chain_id text, requested_authoritative_block_number bigint, requested_authoritative_block_hash text, compared_execution_authority jsonb, requested_logical_name_id text, requested_resolver_chain_id text, requested_resolver_address text, requested_record_key text, compared_positions jsonb, live_answer jsonb, used_ccip_read boolean'
            )
        UNION ALL
        SELECT
            format('%s carries its closed vocabulary', required.table_name),
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = to_regclass(required.table_name)
                  AND conname = required.constraint_name
                  AND contype = 'c'
            )
        FROM (
            VALUES
                (
                    'contract_instances',
                    'contract_instances_contract_kind_check'
                ),
                (
                    'discovery_edges',
                    'discovery_edges_edge_kind_check'
                ),
                (
                    'manifest_discovery_rules',
                    'manifest_discovery_rules_edge_kind_check'
                ),
                (
                    'surface_bindings',
                    'surface_bindings_binding_kind_check'
                ),
                ('name_current', 'name_current_binding_kind_check'),
                (
                    'address_names_current',
                    'address_names_current_binding_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_event_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_derivation_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_migration_correlation_ids_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_consumer_visibility_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_candidate_correlation_check'
                ),
                (
                    'permissions_current',
                    'permissions_current_scope_kind_check'
                ),
                (
                    'address_names_current',
                    'address_names_current_relation_check'
                )
        ) AS required(table_name, constraint_name)
        UNION ALL
        SELECT
            format('%s filters readable history', required.index_name),
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = required.index_name
                  AND indexdef LIKE '% WHERE %'
                  AND indexdef LIKE '%canonicality_state%'
                  AND indexdef LIKE '%''canonical''%'
                  AND indexdef LIKE '%''safe''%'
                  AND indexdef LIKE '%''finalized''%'
            )
        FROM (
            VALUES
                ('surface_bindings_name_idx'),
                ('surface_bindings_resource_idx'),
                ('normalized_events_name_history_idx'),
                ('normalized_events_resource_history_idx'),
                ('normalized_events_subregistry_registration_history_idx')
        ) AS required(index_name)
        UNION ALL
        SELECT
            format('%s has the reviewed delta-driven definition', required.index_name),
            EXISTS (
                SELECT 1
                FROM pg_class AS index_relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = index_relation.relnamespace
                JOIN pg_index AS index_state
                  ON index_state.indexrelid = index_relation.oid
                JOIN pg_class AS table_relation
                  ON table_relation.oid = index_state.indrelid
                JOIN pg_am AS access_method
                  ON access_method.oid = index_relation.relam
                WHERE namespace.nspname = current_schema()
                  AND index_relation.relname = required.index_name
                  AND table_relation.relname = required.table_name
                  AND access_method.amname = 'btree'
                  AND index_state.indisvalid
                  AND index_state.indisready
                  AND index_state.indislive
                  AND index_state.indnatts = index_state.indnkeyatts
                  AND index_state.indnkeyatts =
                      cardinality(required.key_patterns)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM generate_subscripts(required.key_patterns, 1)
                          AS key(ordinal)
                      WHERE pg_get_indexdef(
                                index_relation.oid,
                                key.ordinal,
                                true
                            ) NOT LIKE required.key_patterns[key.ordinal]
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM generate_subscripts(required.key_patterns, 1)
                          AS key(ordinal)
                      WHERE pg_index_column_has_property(
                                index_relation.oid,
                                key.ordinal,
                                'desc'
                            ) IS DISTINCT FROM (
                                required.index_name IN (
                                    'normalized_events_resolver_alias_history_idx',
                                    'normalized_events_resolver_upgrade_history_idx',
                                    'normalized_events_subregistry_registration_history_idx'
                                )
                                AND key.ordinal IN (3, 4)
                            )
                  )
                  AND CASE
                      WHEN required.predicate_patterns IS NULL
                          THEN index_state.indpred IS NULL
                      ELSE index_state.indpred IS NOT NULL
                           AND NOT EXISTS (
                               SELECT 1
                               FROM generate_subscripts(
                                   required.predicate_patterns,
                                   1
                               ) AS predicate(ordinal)
                               WHERE pg_get_expr(
                                         index_state.indpred,
                                         index_state.indrelid,
                                         true
                                     ) NOT LIKE
                                     required.predicate_patterns[predicate.ordinal]
                           )
                  END
            )
        FROM (
            VALUES
                (
                    'normalized_events_chain_block_number_idx',
                    'normalized_events',
                    ARRAY['chain_id', 'block_number'],
                    NULL::text[]
                ),
                (
                    'normalized_events_resolver_alias_history_idx',
                    'normalized_events',
                    ARRAY[
                        'chain_id',
                        '%lower%COALESCE%after_state%resolver%before_state%resolver%raw_fact_ref%emitting_address%',
                        'block_number',
                        'normalized_event_id'
                    ],
                    ARRAY[
                        '%event_kind%AliasChanged%',
                        '%canonicality_state%canonical%safe%finalized%'
                    ]
                ),
                (
                    'normalized_events_resolver_upgrade_history_idx',
                    'normalized_events',
                    ARRAY[
                        'chain_id',
                        '%lower%after_state%proxy_address%',
                        'block_number',
                        'normalized_event_id'
                    ],
                    ARRAY[
                        '%event_kind%Upgraded%',
                        '%canonicality_state%canonical%safe%finalized%'
                    ]
                ),
                (
                    'normalized_events_subregistry_registration_history_idx',
                    'normalized_events',
                    ARRAY[
                        'chain_id',
                        '%after_state%registry_contract_instance_id%',
                        'block_number',
                        'normalized_event_id',
                        'logical_name_id'
                    ],
                    ARRAY[
                        '%event_kind%RegistrationGranted%RegistrationReserved%RegistrationRenewed%RegistrationReleased%',
                        '%source_family%ens_v2_root_l1%ens_v2_registry_l1%',
                        '%canonicality_state%canonical%safe%finalized%',
                        '%logical_name_id%IS NOT NULL%',
                        '%after_state%registry_contract_instance_id%IS NOT NULL%'
                    ]
                ),
                (
                    'name_surfaces_chain_block_number_idx',
                    'name_surfaces',
                    ARRAY['chain_id', 'block_number'],
                    NULL
                ),
                (
                    'surface_bindings_chain_block_number_idx',
                    'surface_bindings',
                    ARRAY['chain_id', 'block_number'],
                    NULL
                ),
                (
                    'resources_chain_block_number_idx',
                    'resources',
                    ARRAY['chain_id', 'block_number'],
                    NULL
                ),
                (
                    'children_current_labelhash_idx',
                    'children_current',
                    ARRAY[
                        'namespace',
                        'lower(labelhash)',
                        'parent_logical_name_id',
                        'child_logical_name_id'
                    ],
                    NULL
                ),
                (
                    'name_current_resolver_idx',
                    'name_current',
                    ARRAY[
                        '%declared_summary%resolver%chain_id%',
                        '%lower%declared_summary%resolver%address%',
                        'logical_name_id'
                    ],
                    ARRAY['%declared_summary%resolver%address%IS NOT NULL%']
                ),
                (
                    'permissions_current_resolver_scope_idx',
                    'permissions_current',
                    ARRAY[
                        '%scope_detail%chain_id%',
                        '%lower%scope_detail%resolver_address%',
                        'resource_id'
                    ],
                    ARRAY[
                        '%scope_kind%resolver%',
                        '%scope_detail%resolver_address%IS NOT NULL%'
                    ]
                ),
                (
                    'record_inventory_current_resolver_idx',
                    'record_inventory_current',
                    ARRAY[
                        '%provenance%chain_id%',
                        '%lower%provenance%resolver_address%',
                        'resource_id'
                    ],
                    ARRAY['%provenance%resolver_address%IS NOT NULL%']
                ),
                (
                    'primary_names_current_reverse_node_idx',
                    'primary_names_current',
                    ARRAY[
                        '%claim_provenance%chain_id%',
                        '%lower%claim_provenance%reverse_node%',
                        'address',
                        'coin_type',
                        'namespace'
                    ],
                    ARRAY['%claim_provenance%reverse_node%IS NOT NULL%']
                ),
                (
                    'permissions_current_resource_wrapper_expiry_idx',
                    'permissions_current_resource_summary',
                    ARRAY[
                        '%provenance%chain_id%',
                        '%provenance%wrapper_expiry_boundary%expiry_seconds%numeric%',
                        'resource_id'
                    ],
                    ARRAY['%provenance%wrapper_expiry_boundary%']
                )
        ) AS required(
            index_name,
            table_name,
            key_patterns,
            predicate_patterns
        )
        UNION ALL
        SELECT
            'normalized event state compaction has its expression index',
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'normalized_events_interpreter_state_history_idx'
                  AND indexdef LIKE '%raw_fact_ref%interpreter_state_key%'
                  AND indexdef LIKE '%digest%sha256%'
                  AND indexdef LIKE '%canonicality_state%'
            )
        UNION ALL
        SELECT
            'name surface visibility requires an explicit decision',
            NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'name_surfaces'
                  AND column_name = 'visibility_state'
                  AND column_default IS NOT NULL
            )
        UNION ALL
        SELECT
            'manifest deployment label has an unambiguous storage name',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'manifest_versions'
                  AND column_name = 'deployment_label'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'manifest_versions'
                  AND column_name = 'deployment_id'
            )
        UNION ALL
        SELECT
            'record inventory has no primary-key duplicate index',
            NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'record_inventory_current_resource_idx'
            )
        UNION ALL
        SELECT
            'children raw labels preserve bytes with optional exact text',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'raw_label'
                  AND data_type = 'bytea'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'decoded_label'
                  AND data_type = 'text'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_label_matches_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_label_requires_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'labelhash'
                  AND is_nullable = 'NO'
            )
        UNION ALL
        SELECT
            'children raw names preserve bytes with optional exact text',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'raw_name'
                  AND data_type = 'bytea'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'decoded_name'
                  AND data_type = 'text'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_name_matches_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_name_requires_raw_check'
                  AND contype = 'c'
            )
    )
    SELECT string_agg(invariant_name, ', ' ORDER BY invariant_name)
    INTO missing_behavioral_constraints
    FROM invariant_checks
    WHERE NOT is_present;

    IF missing_behavioral_constraints IS NOT NULL THEN
        RAISE EXCEPTION
            'missing schema-v2 behavioral constraints: %',
            missing_behavioral_constraints;
    END IF;

    WITH unbounded_input(table_name, column_name) AS (
        VALUES
            ('label_preimages', 'raw_label'),
            ('label_preimages', 'decoded_label'),
            ('ens_names', 'name'),
            ('name_surfaces', 'raw_name'),
            ('name_surfaces', 'raw_labels'),
            ('name_surfaces', 'dns_encoded_name'),
            ('name_surfaces', 'normalization_errors'),
            ('normalized_events', 'before_state'),
            ('normalized_events', 'after_state'),
            ('name_current', 'raw_name'),
            ('children_current', 'raw_name'),
            ('children_current', 'decoded_name'),
            ('children_current', 'raw_label'),
            ('children_current', 'decoded_label'),
            ('record_inventory_current', 'selectors'),
            ('record_inventory_current', 'unsupported_families'),
            ('record_inventory_current', 'last_change'),
            ('record_inventory_current', 'entries'),
            ('address_names_current', 'raw_name'),
            ('primary_names_current', 'raw_claim_name'),
            ('resolution_divergences', 'request_kind')
    )
    SELECT string_agg(
        format('%I.%I in %I', input.table_name, input.column_name, index.oid::regclass),
        ', '
        ORDER BY input.table_name, input.column_name, index.relname
    )
    INTO unbounded_btree_columns
    FROM unbounded_input input
    JOIN pg_namespace namespace
      ON namespace.nspname = current_schema()
    JOIN pg_class relation
      ON relation.relnamespace = namespace.oid
     AND relation.relname = input.table_name
    JOIN pg_attribute attribute
      ON attribute.attrelid = relation.oid
     AND attribute.attname = input.column_name
    JOIN pg_index indexed
      ON indexed.indrelid = relation.oid
     AND attribute.attnum = ANY(indexed.indkey::smallint[])
    JOIN pg_class index ON index.oid = indexed.indexrelid
    JOIN pg_am access_method ON access_method.oid = index.relam
    WHERE access_method.amname = 'btree';

    IF unbounded_btree_columns IS NOT NULL THEN
        RAISE EXCEPTION
            'unbounded externally controlled columns remain in btree indexes: %',
            unbounded_btree_columns;
    END IF;
END
$$;

CREATE FUNCTION assert_exact_zero_migration_behavior() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    zero_case text;
    zero_step integer;
    zero_expected jsonb;
    zero_live jsonb;
    zero_row jsonb;
    zero_written jsonb;
    zero_cleared jsonb;
    manifest_key bigint;
    transition_case record;
    violated_constraint text;
    accepted_projection_identity_mismatches text[] := ARRAY[]::text[];
    accepted_cross_chain_relationships text[] := ARRAY[]::text[];
    accepted_head_invariants text[] := ARRAY[]::text[];
    accepted_projection_relationship_mismatches text[] := ARRAY[]::text[];
    accepted_manifest_mismatches text[] := ARRAY[]::text[];
    accepted_divergence_canonicality text[] := ARRAY[]::text[];
    divergence_guard_xmin text;
    divergence_execution_authority jsonb;
    divergence_write_status text;
    divergence_count bigint;
    oversized_text text;
    oversized_bytes bytea;
BEGIN
    SELECT string_agg(md5(chunk::text), '' ORDER BY chunk)
    INTO oversized_text
    FROM generate_series(1, 96) AS chunks(chunk);
    oversized_bytes := convert_to(oversized_text, 'UTF8');
    IF octet_length(oversized_bytes) <= 2704 THEN
        RAISE EXCEPTION 'oversized-input probe did not exceed the btree row limit';
    END IF;

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        block_number,
        block_timestamp,
        canonicality_state
    )
    VALUES
        (
            'schema-v2-check',
            'block-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        ),
        (
            'schema-v2-check',
            'block-1',
            1,
            '2026-01-01 00:00:01+00',
            'canonical'
        ),
        (
            'schema-v2-other',
            'block-other-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        ),
        (
            'schema-v2-other',
            'orphaned-block-1',
            1,
            '2026-01-01 00:00:01+00',
            'orphaned'
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                ('observed', 'safe'),
                ('observed', 'finalized'),
                ('canonical', 'observed'),
                ('canonical', 'finalized'),
                ('safe', 'observed'),
                ('safe', 'canonical'),
                ('orphaned', 'observed'),
                ('orphaned', 'safe'),
                ('orphaned', 'finalized'),
                ('finalized', 'observed'),
                ('finalized', 'canonical'),
                ('finalized', 'safe'),
                ('finalized', 'orphaned')
        ) AS illegal(from_state, to_state)
    LOOP
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            format(
                'schema-v2-illegal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            ),
            'transition-block',
            0,
            '2026-01-01 00:00:00+00',
            transition_case.from_state::canonicality_state
        );

        BEGIN
            UPDATE chain_lineage
            SET canonicality_state =
                transition_case.to_state::canonicality_state
            WHERE chain_id = format(
                'schema-v2-illegal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            )
              AND block_hash = 'transition-block';
            RAISE EXCEPTION
                'chain_lineage accepted illegal canonicality transition % -> %',
                transition_case.from_state,
                transition_case.to_state;
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM <> format(
                    'illegal chain lineage canonicality transition: %s -> %s',
                    transition_case.from_state,
                    transition_case.to_state
                ) THEN
                    RAISE;
                END IF;
        END;

        DELETE FROM chain_lineage
        WHERE chain_id = format(
            'schema-v2-illegal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';
    END LOOP;

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                ('observed', 'canonical'),
                ('observed', 'orphaned'),
                ('canonical', 'safe'),
                ('canonical', 'orphaned'),
                ('safe', 'finalized'),
                ('safe', 'orphaned'),
                ('orphaned', 'canonical')
        ) AS legal(from_state, to_state)
    LOOP
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            format(
                'schema-v2-legal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            ),
            'transition-block',
            0,
            '2026-01-01 00:00:00+00',
            transition_case.from_state::canonicality_state
        );

        UPDATE chain_lineage
        SET canonicality_state =
            transition_case.to_state::canonicality_state
        WHERE chain_id = format(
            'schema-v2-legal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';

        IF NOT EXISTS (
            SELECT 1
            FROM chain_lineage
            WHERE chain_id = format(
                'schema-v2-legal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            )
              AND block_hash = 'transition-block'
              AND canonicality_state =
                  transition_case.to_state::canonicality_state
        ) THEN
            RAISE EXCEPTION
                'chain_lineage did not apply legal canonicality transition % -> %',
                transition_case.from_state,
                transition_case.to_state;
        END IF;

        DELETE FROM chain_lineage
        WHERE chain_id = format(
            'schema-v2-legal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';
    END LOOP;

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        block_number,
        block_timestamp,
        canonicality_state,
        first_observed_at,
        canonicality_updated_at
    )
    VALUES (
        'schema-v2-transition-timestamp',
        'transition-block',
        0,
        '2026-01-01 00:00:00+00',
        'observed',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-transition-timestamp'
      AND block_hash = 'transition-block';

    IF (
        SELECT canonicality_updated_at
        FROM chain_lineage
        WHERE chain_id = 'schema-v2-transition-timestamp'
          AND block_hash = 'transition-block'
    ) <= TIMESTAMPTZ '2026-01-01 00:00:00+00'
    THEN
        RAISE EXCEPTION
            'chain_lineage did not timestamp its canonicality transition';
    END IF;

    DELETE FROM chain_lineage
    WHERE chain_id = 'schema-v2-transition-timestamp'
      AND block_hash = 'transition-block';

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        parent_hash,
        block_number,
        block_timestamp,
        canonicality_state
    )
    VALUES
        (
            'schema-v2-checkpoint-jump',
            'jump-block-0',
            NULL,
            0,
            '2026-01-01 00:00:00+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-1',
            'jump-block-0',
            1,
            '2026-01-01 00:00:01+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-2',
            'jump-block-1',
            2,
            '2026-01-01 00:00:02+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-3',
            'jump-block-2',
            3,
            '2026-01-01 00:00:03+00',
            'observed'
        );

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-checkpoint-jump';

    UPDATE chain_lineage
    SET canonicality_state = 'safe'
    WHERE chain_id = 'schema-v2-checkpoint-jump'
      AND block_number <= 2;

    UPDATE chain_lineage
    SET canonicality_state = 'finalized'
    WHERE chain_id = 'schema-v2-checkpoint-jump'
      AND block_number <= 1;

    INSERT INTO chain_heads (
        chain_id,
        latest_block_hash,
        latest_block_number,
        safe_block_hash,
        safe_block_number,
        finalized_block_hash,
        finalized_block_number
    )
    VALUES (
        'schema-v2-checkpoint-jump',
        'jump-block-3',
        3,
        'jump-block-2',
        2,
        'jump-block-1',
        1
    );

    IF (
        SELECT lineage_orphaning_epoch
        FROM chain_heads
        WHERE chain_id = 'schema-v2-checkpoint-jump'
    ) <> 0 THEN
        RAISE EXCEPTION 'chain head orphaning epoch did not start at zero';
    END IF;

    BEGIN
        UPDATE chain_heads
        SET lineage_orphaning_epoch = -1
        WHERE chain_id = 'schema-v2-checkpoint-jump';
        RAISE EXCEPTION 'chain head accepted a negative orphaning epoch';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "chain_heads_lineage_orphaning_epoch_check"%'
            THEN
                RAISE;
            END IF;
    END;

    IF (
        SELECT array_agg(
            canonicality_state::text
            ORDER BY block_number
        )
        FROM chain_lineage
        WHERE chain_id = 'schema-v2-checkpoint-jump'
    ) <> ARRAY['finalized', 'finalized', 'safe', 'canonical']
    THEN
        RAISE EXCEPTION
            'ordered checkpoint jump did not preserve adjacent canonicality transitions';
    END IF;

    BEGIN
        INSERT INTO chain_heads (
            chain_id,
            latest_block_hash,
            latest_block_number
        )
        VALUES (
            'schema-v2-other',
            'orphaned-block-1',
            1
        );
        accepted_head_invariants :=
            array_append(accepted_head_invariants, 'orphaned head');
        DELETE FROM chain_heads
        WHERE chain_id = 'schema-v2-other';
    EXCEPTION
        WHEN check_violation OR foreign_key_violation THEN
            IF SQLERRM <>
                'latest head must reference a canonical chain block'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            'schema-v2-check',
            'competing-block-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        );
        accepted_head_invariants :=
            array_append(
                accepted_head_invariants,
                'multiple canonical blocks at one height'
            );
        DELETE FROM chain_lineage
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'competing-block-0';
    EXCEPTION
        WHEN unique_violation THEN
            IF SQLERRM NOT LIKE
                '%chain_lineage_readable_height_idx%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO chain_heads (
        chain_id,
        latest_block_hash,
        latest_block_number
    )
    VALUES (
        'schema-v2-check',
        'block-0',
        0
    );

    BEGIN
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
        accepted_head_invariants :=
            array_append(
                accepted_head_invariants,
                'head demoted to orphaned'
            );
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
    EXCEPTION
        WHEN check_violation OR foreign_key_violation THEN
            IF SQLERRM <>
                'a chain head still references this block state'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_head_invariants) > 0 THEN
        RAISE EXCEPTION
            'chain tables accepted invalid head state: %',
            array_to_string(accepted_head_invariants, ', ');
    END IF;

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        verification_level
    )
    VALUES (
        'schema-v2-check',
        'verify',
        'idle',
        'quick_synced'
    );

    UPDATE chain_phase_state
    SET verification_level = 'cross_checked'
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'verify';

    UPDATE chain_phase_state
    SET verification_level = 'node_checked'
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'verify';

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name
    )
    VALUES (
        'schema-v2-redo-failure',
        'interpret'
    );

    UPDATE chain_phase_state
    SET phase_status = 'running',
        redo_in_progress = true,
        redo_mode = 'redo',
        redo_previous_phase_status = 'idle',
        redo_from_block_number = 0,
        redo_to_block_number = 1,
        last_error = 'deterministic redo failure',
        started_at = now()
    WHERE chain_id = 'schema-v2-redo-failure'
      AND phase_name = 'interpret';

    BEGIN
        INSERT INTO chain_phase_state (
            chain_id,
            phase_name,
            phase_status,
            last_error,
            started_at
        )
        VALUES (
            'schema-v2-invalid-running-error',
            'interpret',
            'running',
            'invalid normal-run error',
            now()
        );
        RAISE EXCEPTION
            'a normal running phase accepted last_error';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    DELETE FROM chain_phase_state
    WHERE chain_id = 'schema-v2-redo-failure'
      AND phase_name = 'interpret';

    BEGIN
        INSERT INTO chain_phase_state (
            chain_id,
            phase_name,
            phase_status,
            verification_level
        )
        VALUES (
            'schema-v2-check',
            'ingest',
            'idle',
            'quick_synced'
        );
        RAISE EXCEPTION
            'a non-verify phase accepted a verification level';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "chain_phase_state_verification_phase_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        started_at
    )
    VALUES (
        'schema-v2-check',
        'ingest',
        'paused',
        now()
    );

    DELETE FROM chain_phase_state
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'ingest';

    INSERT INTO raw_transactions (
        chain_id,
        block_hash,
        block_number,
        transaction_hash,
        transaction_index,
        from_address
    )
    VALUES
        ('schema-v2-check', 'block-0', 0, 'tx-0', 0, 'address-0'),
        ('schema-v2-check', 'block-0', 0, 'tx-1', 1, 'address-1');

    BEGIN
        INSERT INTO raw_receipts (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index
        )
        VALUES ('schema-v2-check', 'block-0', 0, 'tx-0', 1);
        RAISE EXCEPTION
            'raw_receipts accepted a mismatched transaction position';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "raw_receipts_transaction_position_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address
        )
        VALUES (
            'schema-v2-check',
            'block-0',
            0,
            'tx-0',
            1,
            0,
            'address-0'
        );
        RAISE EXCEPTION
            'raw_logs accepted a mismatched transaction position';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "raw_logs_transaction_position_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'root'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'contract'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000003',
        'schema-v2-other',
        'root'
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-other',
            'cross-chain-address'
        );
        RAISE EXCEPTION
            'a contract address accepted a different instance chain';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instance_addresses_instance_chain_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-other',
            'proxy_implementation',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a discovery edge accepted different endpoint chains';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "discovery_edges_from_instance_chain_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO discovery_edges (
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        admission_basis
    )
    VALUES (
        'schema-v2-check',
        'registry_announcement',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000001',
        'RegistryCreated',
        'schema-v2-check'
    );

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-check',
            'resolver',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000001',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a non-announcement discovery edge accepted equal endpoints';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    INSERT INTO contract_instance_addresses (
        contract_instance_id,
        chain_id,
        address,
        active_from_block_number,
        active_to_block_number,
        deactivated_at
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'historical-address-0',
        0,
        100,
        now()
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_to_block_number,
            deactivated_at
        )
        VALUES (
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'historical-address-1',
            50,
            150,
            now()
        );
        RAISE EXCEPTION
            'one contract instance accepted overlapping address ranges';
    EXCEPTION
        WHEN exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instance_addresses_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO contract_instance_addresses (
        contract_instance_id,
        chain_id,
        address,
        active_from_block_number
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'contract-address-0',
        0
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            '00000000-0000-0000-0000-000000000001',
            'schema-v2-check',
            'contract-address-1'
        );
        RAISE EXCEPTION
            'one contract instance accepted two active addresses';
    EXCEPTION
        WHEN unique_violation OR exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%contract_instance_addresses_active_instance_idx%'
                AND SQLERRM NOT LIKE
                    '%constraint "contract_instance_addresses_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO token_lineages (
        token_lineage_id,
        chain_id,
        block_hash,
        block_number
    )
    VALUES
        (
            '00000000-0000-0000-0000-000000000031',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000032',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000033',
            'schema-v2-check',
            'block-0',
            0
        );

    INSERT INTO resources (
        resource_id,
        token_lineage_id,
        chain_id,
        block_hash,
        block_number
    )
    VALUES
        (
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000031',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000013',
            NULL,
            'schema-v2-other',
            'block-other-0',
            0
        );

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'Name',
        ARRAY['Name'],
        '\x044e616d6500',
        'namehash-0',
        ARRAY['labelhash-0'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-2',
        'schema-v2-check',
        'Other',
        ARRAY['Other'],
        '\x054f7468657200',
        'namehash-2',
        ARRAY['labelhash-2'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-missing-visibility',
            'schema-v2-check',
            'Missing',
            ARRAY['Missing'],
            '\x074d697373696e6700',
            'namehash-missing-visibility',
            ARRAY['labelhash-missing-visibility'],
            'check',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted an omitted visibility decision';
    EXCEPTION
        WHEN not_null_violation THEN
            IF SQLERRM NOT LIKE
                '%null value in column "visibility_state"%'
            THEN
                RAISE EXCEPTION
                    'name_surfaces omitted visibility failed with unexpected message: %',
                    SQLERRM;
            END IF;
    END;

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        normalization_errors,
        deactivation_reason,
        deactivated_at,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-shadow',
        'schema-v2-check',
        'Shadow',
        ARRAY['Shadow'],
        '\x06536861646f7700',
        'namehash-shadow',
        ARRAY['labelhash-shadow'],
        'check',
        'shadow',
        '["normalization failed"]'::jsonb,
        'normalization failed',
        '2026-01-01 00:00:01+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-invalid-active',
            'schema-v2-check',
            'Invalid Active',
            ARRAY['Invalid Active'],
            '\x0e496e76616c69642041637469766500',
            'namehash-invalid-active',
            ARRAY['labelhash-invalid-active'],
            'check',
            'active',
            '["unexpected"]'::jsonb,
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted active normalization errors';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_visibility_coherence_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-invalid-shadow',
            'schema-v2-check',
            'Invalid Shadow',
            ARRAY['Invalid Shadow'],
            '\x0e496e76616c696420536861646f7700',
            'namehash-invalid-shadow',
            ARRAY['labelhash-invalid-shadow'],
            'check',
            'shadow',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted an incomplete shadow state';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_visibility_coherence_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:not-the-namehash',
            'schema-v2-check',
            'Other',
            ARRAY['Other'],
            '\x054f7468657200',
            'namehash-1',
            ARRAY['labelhash-1'],
            'check',
            'active',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted a logical ID that is not namespace:namehash';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000014',
            '00000000-0000-0000-0000-000000000033',
            'schema-v2-other',
            'block-other-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'resources.token_lineage_id'
            );
        DELETE FROM resources
        WHERE resource_id = '00000000-0000-0000-0000-000000000014';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "resources_chain_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000023',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000013',
            'declared_registry_path',
            'ens_v1',
            '2026-01-01 00:00:00+00',
            'schema-v2-other',
            'block-other-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'surface_bindings.logical_name_id'
            );
        DELETE FROM surface_bindings
        WHERE surface_binding_id =
            '00000000-0000-0000-0000-000000000023';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_chain_id_logical_name_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000024',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000013',
            'declared_registry_path',
            'ens_v1',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'surface_bindings.resource_id'
            );
        DELETE FROM surface_bindings
        WHERE surface_binding_id =
            '00000000-0000-0000-0000-000000000024';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_chain_id_resource_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'cross-chain-name-event',
            'schema-v2-check',
            'schema-v2-check:namehash-0',
            'ResolverChanged',
            'check',
            1,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.logical_name_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-name-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_chain_id_logical_name_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'cross-chain-resource-event',
            'schema-v2-check',
            '00000000-0000-0000-0000-000000000011',
            'ResolverChanged',
            'check',
            1,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.resource_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-resource-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_chain_id_resource_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_cross_chain_relationships) > 0 THEN
        RAISE EXCEPTION
            'identity tables accepted cross-chain relationships: %',
            array_to_string(accepted_cross_chain_relationships, ', ');
    END IF;

    INSERT INTO record_inventory_current (
        resource_id,
        record_version_boundary_key,
        record_version_boundary,
        selectors,
        unsupported_families,
        entries,
        support_status,
        provenance,
        chain_positions,
        canonicality_summary,
        manifest_version
    ) VALUES (
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        '{"kind": "schema-v2-lookup-guard"}'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        'supported',
        '{}'::jsonb,
        '{}'::jsonb,
        '{}'::jsonb,
        1
    );

    INSERT INTO name_current (
        logical_name_id,
        namespace,
        raw_name,
        namehash,
        declared_summary,
        support_status,
        manifest_version
    ) VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'Name',
        'namehash-0',
        '{
            "topology": {
                "resolver_path": [
                    {
                        "chain_id": "schema-v2-check",
                        "address": "resolver-address-guard"
                    }
                ],
                "version_boundaries": {
                    "record_version_boundary": {
                        "kind": "schema-v2-lookup-guard"
                    }
                }
            }
        }'::jsonb,
        'supported',
        1
    );

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        current_block_number,
        current_block_hash,
        target_block_number,
        target_block_hash,
        input_content_hash,
        started_at,
        finished_at
    ) VALUES (
        'schema-v2-check',
        'project',
        'completed',
        0,
        'block-0',
        0,
        'block-0',
        'schema-v2-check-content',
        now(),
        now()
    );

    INSERT INTO manifest_versions (
        manifest_version,
        namespace,
        source_family,
        chain_id,
        deployment_label,
        rollout_status,
        normalizer_version,
        file_path,
        manifest_payload
    ) VALUES (
        1,
        'schema-v2-check',
        'schema-v2-lookup-guard',
        'schema-v2-check',
        'schema-v2-lookup-guard',
        'active',
        'check',
        'schema-v2-lookup-guard.toml',
        '{}'::jsonb
    ) RETURNING manifest_id INTO manifest_key;

    INSERT INTO manifest_contract_instances (
        manifest_id,
        chain_id,
        declaration_kind,
        declaration_name,
        contract_instance_id,
        declared_address,
        role,
        proxy_kind
    ) VALUES (
        manifest_key,
        'schema-v2-check',
        'contract',
        'schema-v2-lookup-guard',
        '00000000-0000-0000-0000-000000000002',
        'resolver-address-guard',
        'lookup-guard',
        'none'
    );

    SELECT jsonb_build_object(
        'project_row_xmin', phase.xmin::text,
        'logical_name_id', name.logical_name_id,
        'name_row_xmin', name.xmin::text,
        'manifest_authorities', jsonb_build_array(jsonb_build_object(
            'declared_address', declaration.declared_address,
            'manifest_id', manifest.manifest_id::text,
            'manifest_row_xmin', manifest.xmin::text,
            'declaration_id',
                declaration.manifest_contract_instance_id::text,
            'declaration_row_xmin', declaration.xmin::text
        ))
    )
    INTO divergence_execution_authority
    FROM chain_phase_state AS phase
    CROSS JOIN name_current AS name
    JOIN manifest_versions AS manifest
      ON manifest.manifest_id = manifest_key
    JOIN manifest_contract_instances AS declaration
      ON declaration.manifest_id = manifest.manifest_id
    WHERE phase.chain_id = 'schema-v2-check'
      AND phase.phase_name = 'project'
      AND name.logical_name_id = 'schema-v2-check:namehash-0';

    SELECT xmin::text
    INTO divergence_guard_xmin
    FROM record_inventory_current
    WHERE resource_id = '00000000-0000-0000-0000-000000000011'
      AND record_version_boundary_key = 'schema-v2-lookup-guard';

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'addr:60',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "0x01"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'written'
        OR NOT EXISTS (
            SELECT 1
            FROM resolution_divergences
            WHERE resolver_address = 'resolver-address-guard'
              AND indexed_result = '{"status": "not_found"}'::jsonb
              AND live_result =
                  '{"status": "success", "value": "0x01"}'::jsonb
              AND observed_positions -> 'resolver' ->> 'block_hash' =
                  'block-0'
        )
    THEN
        RAISE EXCEPTION
            'guarded divergence write did not store both answers and anchor';
    END IF;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'text:' || oversized_text,
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "oversized"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'written'
        OR NOT EXISTS (
            SELECT 1
            FROM resolution_divergences
            WHERE resolver_address = 'resolver-address-guard'
              AND request_kind_hash =
                  public.digest('text:' || oversized_text, 'sha256')
              AND request_kind = 'text:' || oversized_text
        )
    THEN
        RAISE EXCEPTION
            'oversized divergence request was not findable by its bounded digest';
    END IF;

    SELECT count(*)
    INTO divergence_count
    FROM resolution_divergences;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'addr:60',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "orphaned-agreement",
                "block_number": 1,
                "timestamp": "2026-01-01T00:00:01Z"
            }
        }'::jsonb,
        '{"status": "not_found"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'guard_rejected'
        OR NOT EXISTS (
            SELECT 1
            FROM resolution_divergences
            WHERE resolver_address = 'resolver-address-guard'
              AND request_kind = 'addr:60'
              AND cleared_at IS NULL
        )
    THEN
        RAISE EXCEPTION
            'orphaned agreement cleared an older active divergence';
    END IF;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin || '-stale',
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-guard',
        'text:url',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "https://example.test"}'::jsonb,
        false
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'guard_rejected'
        OR (SELECT count(*) FROM resolution_divergences) <> divergence_count
    THEN
        RAISE EXCEPTION
            'row-unchanged guard accepted a stale projection comparison';
    END IF;

    SELECT write_resolution_divergence(
        '00000000-0000-0000-0000-000000000011',
        'schema-v2-lookup-guard',
        divergence_guard_xmin,
        'schema-v2-check',
        0,
        'block-0',
        divergence_execution_authority,
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-ccip-guard',
        'text:ccip',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"status": "success", "value": "ccip"}'::jsonb,
        true
    ) INTO divergence_write_status;

    IF divergence_write_status <> 'ccip_skipped'
        OR (SELECT count(*) FROM resolution_divergences) <> divergence_count
    THEN
        RAISE EXCEPTION 'CCIP result reached the divergence ledger';
    END IF;

    -- These are SQL evaluator fixtures, separate from the Project component proof.
    FOR zero_case IN SELECT unnest(ARRAY['marked', 'missing', 'empty']) LOOP
        DELETE FROM resolution_divergences
        WHERE logical_name_id = 'schema-v2-check:namehash-0' AND request_kind = 'addr:60';
        UPDATE record_inventory_current
        SET entries = '[{"record_key":"addr:2147483648","record_family":"addr",
                "selector_key":"2147483648","status":"success",
                "value":"0x2222222222222222222222222222222222222222"}]'::jsonb
            || CASE WHEN zero_case = 'missing' THEN '[]'::jsonb ELSE
                '[{"record_key":"addr:60","record_family":"addr",
                  "selector_key":"60","status":"not_found"}]'::jsonb END,
            provenance = '{"read_rules":[{"kind":"ensip19_default_address",
                "source_record_key":"addr:2147483648"}]}'::jsonb
                || CASE WHEN zero_case = 'marked' THEN
                    '{"exact_nonempty_not_found_record_keys":["addr:60"]}'::jsonb
                    ELSE '{}'::jsonb END
        WHERE resource_id = '00000000-0000-0000-0000-000000000011';
        SELECT xmin::text INTO STRICT divergence_guard_xmin FROM record_inventory_current
        WHERE resource_id = '00000000-0000-0000-0000-000000000011';
        zero_expected := CASE WHEN zero_case = 'marked' THEN '{"status":"not_found"}'::jsonb
            ELSE '{"status":"success","value":"0x2222222222222222222222222222222222222222"}'::jsonb END;
        FOR zero_step IN 1..3 LOOP
            zero_live := CASE WHEN zero_step = 1 THEN
                '{"status":"success","value":"0x1111111111111111111111111111111111111111"}'::jsonb
                ELSE zero_expected END;
            SELECT write_resolution_divergence(
                '00000000-0000-0000-0000-000000000011', 'schema-v2-lookup-guard',
                divergence_guard_xmin, 'schema-v2-check', 0, 'block-0',
                divergence_execution_authority, 'schema-v2-check:namehash-0',
                'schema-v2-check', 'resolver-address-guard', 'addr:60',
                '{"resolver":{"chain_id":"schema-v2-check","block_hash":"block-0",
                  "block_number":0,"timestamp":"2026-01-01T00:00:00Z"}}'::jsonb,
                zero_live, false
            ) INTO divergence_write_status;
            IF divergence_write_status <> (ARRAY['written', 'cleared', 'agreement'])[zero_step] THEN
                RAISE EXCEPTION 'exact-zero writer action mismatch: %, %, %', zero_case, zero_step, divergence_write_status;
            END IF;
            SELECT to_jsonb(ledger) INTO STRICT zero_row FROM resolution_divergences ledger
            WHERE logical_name_id = 'schema-v2-check:namehash-0' AND request_kind = 'addr:60';
            IF zero_row->'indexed_result' <> zero_expected
                OR (zero_row->>'cleared_at' IS NULL) <> (zero_step = 1) THEN
                RAISE EXCEPTION 'exact-zero durable answer or active state mismatch: %, %', zero_case, zero_step;
            END IF;
            IF zero_step = 1 THEN
                zero_written := zero_row;
            ELSIF zero_step = 2 THEN
                IF zero_row - 'cleared_at' <> zero_written - 'cleared_at' THEN
                    RAISE EXCEPTION 'exact-zero clear replaced or mutated the retained row';
                END IF;
                zero_cleared := zero_row;
            ELSIF zero_row <> zero_cleared THEN
                RAISE EXCEPTION 'exact-zero agreement mutated the retained cleared row';
            END IF;
        END LOOP;
    END LOOP;

    DELETE FROM name_current
    WHERE logical_name_id = 'schema-v2-check:namehash-0';

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-0',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    BEGIN
        UPDATE chain_lineage
        SET block_timestamp = '2026-01-01 00:00:02+00'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
        RAISE EXCEPTION
            'chain_lineage accepted a block timestamp identity change';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM <> 'chain lineage block identity is immutable' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result,
            first_observed_at,
            last_observed_at,
            cleared_at
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'resolver-address-time-check',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-check",
                    "block_hash": "block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb,
            '2026-01-01 00:00:00+00',
            '2026-01-03 00:00:00+00',
            '2026-01-02 00:00:00+00'
        );
        RAISE EXCEPTION
            'resolution_divergences accepted clearing before last observation';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "resolution_divergences_clearing_time_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-other',
        'resolver-address-reorg-check',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-other",
                "block_hash": "block-other-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    UPDATE chain_lineage
    SET canonicality_state = 'orphaned'
    WHERE chain_id = 'schema-v2-other'
      AND block_hash = 'block-other-0';

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-reorg-check'
          AND cleared_at IS NULL
    ) THEN
        accepted_divergence_canonicality :=
            array_append(
                accepted_divergence_canonicality,
                'active row survived an orphaned observed block'
            );
    END IF;

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-other'
      AND block_hash = 'block-other-0';

    DELETE FROM resolution_divergences
    WHERE resolver_address = 'resolver-address-reorg-check';

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-other',
            'resolver-address-orphan-check',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-other",
                    "block_hash": "orphaned-block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb
        );
        accepted_divergence_canonicality :=
            array_append(
                accepted_divergence_canonicality,
                'active row accepted an orphaned observed block'
            );
        DELETE FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-orphan-check';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM <>
                'active resolution difference position resolver is not canonical'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_divergence_canonicality) > 0 THEN
        RAISE EXCEPTION
            'resolution differences accepted noncanonical positions: %',
            array_to_string(accepted_divergence_canonicality, ', ');
    END IF;

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'resolver-address-0',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-check",
                    "block_hash": "block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb
        );
        RAISE EXCEPTION
            'one request accepted two active divergence rows';
    EXCEPTION
        WHEN unique_violation THEN
            IF SQLERRM NOT LIKE
                '%resolution_divergences_one_active_request_idx%'
            THEN
                RAISE;
            END IF;
    END;

    UPDATE resolution_divergences
    SET cleared_at = now()
    WHERE logical_name_id = 'schema-v2-check:namehash-0'
      AND resolver_chain_id = 'schema-v2-check'
      AND resolver_address = 'resolver-address-0'
      AND request_kind = 'addr';

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-0',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-1",
                "block_number": 1,
                "timestamp": "2026-01-01T00:00:01Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        authority_arm,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000021',
        'schema-v2-check:namehash-0',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'ens_v1',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        authority_arm,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000025',
        'schema-v2-check:namehash-2',
        '00000000-0000-0000-0000-000000000012',
        'declared_registry_path',
        'ens_v1',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'name_current binding'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_surface_binding_id_logical_name_id_resource_i_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-binding-mismatch',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'address_names_current binding'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-binding-mismatch';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_surface_binding_id_logical_name_id_r_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'name_current token lineage'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_resource_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-token-mismatch',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'address_names_current token lineage'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-token-mismatch';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_resource_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_projection_relationship_mismatches) > 0 THEN
        RAISE EXCEPTION
            'projection tables accepted mismatched relationships: %',
            array_to_string(
                accepted_projection_relationship_mismatches,
                ', '
            );
    END IF;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            'supported',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'name_current'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            raw_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-namehash',
            'wrong-namespace',
            convert_to('Child.Name', 'UTF8'),
            convert_to('Child', 'UTF8'),
            'wrong-namehash',
            'wrong-labelhash',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'children_current'
            );
        DELETE FROM children_current
        WHERE parent_logical_name_id = 'schema-v2-check:namehash-0'
          AND child_logical_name_id = 'schema-v2-check:child-namehash';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-clean',
        'schema-v2-check',
        convert_to('Clean.Name', 'UTF8'),
        'Clean.Name',
        convert_to('Clean', 'UTF8'),
        'Clean',
        'child-clean',
        'labelhash-child-clean',
        1
    ), (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-hostile',
        'schema-v2-check',
        decode('ff002e4e616d65', 'hex'),
        NULL,
        decode('ff00', 'hex'),
        NULL,
        'child-hostile',
        'labelhash-child-hostile',
        1
    );

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-topology-only',
        'schema-v2-check',
        NULL,
        NULL,
        NULL,
        NULL,
        'child-topology-only',
        'labelhash-topology-only',
        1
    );

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoded-label-without-raw',
            'schema-v2-check',
            NULL,
            NULL,
            NULL,
            'Synthesized',
            'child-decoded-label-without-raw',
            'labelhash-decoded-label-without-raw',
            1
        );
        RAISE EXCEPTION
            'children_current accepted a decoded label without raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_label_requires_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoded-name-without-raw',
            'schema-v2-check',
            NULL,
            'Synthesized.Name',
            NULL,
            NULL,
            'child-decoded-name-without-raw',
            'labelhash-decoded-name-without-raw',
            1
        );
        RAISE EXCEPTION
            'children_current accepted a decoded name without raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_name_requires_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoding-drift',
            'schema-v2-check',
            convert_to('Raw.Name', 'UTF8'),
            'Raw.Name',
            convert_to('raw', 'UTF8'),
            'different',
            'child-decoding-drift',
            'labelhash-child-decoding-drift',
            1
        );
        RAISE EXCEPTION
            'children_current accepted decoded text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_label_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-name-decoding-drift',
            'schema-v2-check',
            convert_to('Raw.Name', 'UTF8'),
            'Different.Name',
            convert_to('Raw', 'UTF8'),
            'Raw',
            'child-name-decoding-drift',
            'labelhash-child-name-decoding-drift',
            1
        );
        RAISE EXCEPTION
            'children_current accepted decoded name text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_name_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-0',
            'schema-v2-check:namehash-0',
            'registrant',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared_registry_path',
            'supported',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'address_names_current'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-0'
          AND logical_name_id = 'schema-v2-check:namehash-0'
          AND relation = 'registrant';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_projection_identity_mismatches) > 0 THEN
        RAISE EXCEPTION
            'projection tables accepted mismatched logical IDs: %',
            array_to_string(
                accepted_projection_identity_mismatches,
                ', '
            );
    END IF;

    BEGIN
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES (
            '00000000-0000-0000-0000-000000000099',
            'schema-v2-check',
            'registry'
        );
        RAISE EXCEPTION
            'contract_instances accepted an unknown contract kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instances_contract_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-check',
            'proxy',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-check',
            'schema-check'
        );
        RAISE EXCEPTION
            'discovery_edges accepted an unknown edge kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "discovery_edges_edge_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000029',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'ens_v1',
            '2025-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0
        );
        RAISE EXCEPTION
            'surface_bindings accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'invalid-event-kind',
            'schema-v2-check',
            'Check',
            'schema-check',
            1,
            'schema-v2-check',
            'ens_v2_resolver'
        );
        RAISE EXCEPTION
            'normalized_events accepted an unknown event kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_event_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        derivation_kind,
        migration_correlation_ids,
        consumer_visibility
    )
    SELECT
        'valid-migration-event-kind-' || event_kind,
        'schema-v2-check',
        event_kind,
        'ens_v2_migration_l1',
        1,
        'schema-v2-check',
        'ens_v2_migration',
        ARRAY['migration-correlation-check'],
        'candidate'
    FROM unnest(
        ARRAY['ContractDiscovered', 'MigrationApplied']
    ) AS admitted(event_kind);

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        derivation_kind
    )
    SELECT
        'valid-derivation-kind-' || derivation_kind,
        'schema-v2-check',
        'ResolverChanged',
        'schema-check',
        1,
        'schema-v2-check',
        derivation_kind
    FROM unnest(
        ARRAY[
            'ens_v1_reverse_claim',
            'ens_v1_unwrapped_authority',
            'ens_v2_migration',
            'ens_v2_permissions',
            'ens_v2_registrar',
            'ens_v2_registry_resource_surface',
            'ens_v2_resolver',
            'manifest_sync',
            'proxy_upgrade',
            'raw_block_preimage_observation',
            'raw_log_preimage_observation',
            'standard_approval'
        ]
    ) AS admitted(derivation_kind);

    INSERT INTO normalized_events (
        event_identity, namespace, event_kind, source_family,
        manifest_version, chain_id, derivation_kind
    ) VALUES (
        'valid-account-permission-changed', 'schema-v2-check',
        'AccountPermissionChanged', 'ens_v1_registry_l1', 1,
        'schema-v2-check', 'standard_approval'
    );

    FOR transition_case IN
        SELECT * FROM (VALUES
            ('0x00000000000000000000000000000000000000AA',
             '0x00000000000000000000000000000000000000bb', true, '["registry_control"]'::jsonb),
            ('', '0x00000000000000000000000000000000000000bb', true, '["registry_control"]'::jsonb),
            ('0x00000000000000000000000000000000000000aa',
             '0x00000000000000000000000000000000000000bb', true, '[]'::jsonb),
            ('0x00000000000000000000000000000000000000aa',
             '0x00000000000000000000000000000000000000bb', false, '["registry_control"]'::jsonb)
        ) AS invalid(authority_contract, owner, approved, powers)
    LOOP
        BEGIN
            INSERT INTO account_permission_state_current (
                chain_id, authority_kind, authority_contract, authority_contract_instance_id,
                owner, subject, relation_kind, approved, effective_powers, grant_source,
                inheritance_path, transfer_behavior, provenance, chain_positions,
                canonicality_summary, manifest_version
            ) VALUES (
                'schema-v2-check', 'registry', transition_case.authority_contract,
                '00000000-0000-0000-0000-000000000099', transition_case.owner,
                '0x00000000000000000000000000000000000000cc', 'operator',
                transition_case.approved, transition_case.powers, '{}', '[]', '{}', '{}', '{}', '{}', 1
            );
            RAISE EXCEPTION 'account permission state accepted an invalid row';
        EXCEPTION WHEN check_violation THEN NULL;
        END;
    END LOOP;

    BEGIN
        INSERT INTO permissions_current_resource_summary (
            resource_id, registry_owner, registry_contract,
            registry_binding_chain_positions, support_status,
            unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        ) VALUES (
            '00000000-0000-0000-0000-000000000011',
            '0x00000000000000000000000000000000000000aa',
            '0x00000000000000000000000000000000000000bb',
            '{}', 'unsupported', 'registry-binding-probe', '{}', '{}', '{}', 1
        );
        RAISE EXCEPTION 'permission resource summary accepted a partial registry binding';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS violated_constraint = CONSTRAINT_NAME;
        IF violated_constraint <>
            'permissions_current_resource_summary_registry_binding_check'
        THEN
            RAISE EXCEPTION
                'partial registry binding failed through unexpected constraint %',
                violated_constraint;
        END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'invalid-derivation-kind',
            'schema-v2-check',
            'ResolverChanged',
            'schema-check',
            1,
            'schema-v2-check',
            'check'
        );
        RAISE EXCEPTION
            'normalized_events accepted an unknown derivation kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_derivation_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO permissions_current (
            resource_id,
            subject,
            scope,
            scope_kind,
            manifest_version
        )
        VALUES (
            '00000000-0000-0000-0000-000000000011',
            'schema-check-subject',
            'schema-check-scope',
            'check',
            1
        );
        RAISE EXCEPTION
            'permissions_current accepted an unknown scope kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "permissions_current_scope_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        RAISE EXCEPTION
            'name_current accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'invalid-relation',
            'schema-v2-check:namehash-0',
            'owner',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared_registry_path',
            'supported',
            1
        );
        RAISE EXCEPTION
            'address_names_current accepted an unknown relation';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_relation_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'invalid-binding-kind',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        RAISE EXCEPTION
            'address_names_current accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO label_preimages (
        labelhash,
        raw_label,
        decoded_label,
        normalizer_version,
        normalized_under_version,
        normalization_error,
        source_kind,
        source_priority
    )
    VALUES
        (
            'labelhash-normalized',
            convert_to('normalized', 'UTF8'),
            'normalized',
            'check',
            true,
            NULL,
            'schema-check',
            0
        ),
        (
            'labelhash-rejected',
            decode('ff00', 'hex'),
            NULL,
            'check',
            false,
            'normalization failed',
            'schema-check',
            0
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'labelhash-invalid-success',
                    true,
                    'unexpected error'::text
                ),
                (
                    'labelhash-invalid-failure',
                    false,
                    NULL::text
                )
        ) AS invalid(labelhash, normalized, normalization_error)
    LOOP
        BEGIN
            INSERT INTO label_preimages (
                labelhash,
                raw_label,
                decoded_label,
                normalizer_version,
                normalized_under_version,
                normalization_error,
                source_kind,
                source_priority
            )
            VALUES (
                transition_case.labelhash,
                convert_to('invalid', 'UTF8'),
                'invalid',
                'check',
                transition_case.normalized,
                transition_case.normalization_error,
                'schema-check',
                0
            );
            RAISE EXCEPTION
                'label_preimages accepted incoherent normalization state';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE
                    '%constraint "label_preimages_normalization_coherence_check"%'
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    BEGIN
        INSERT INTO label_preimages (
            labelhash,
            raw_label,
            decoded_label,
            normalizer_version,
            normalized_under_version,
            normalization_error,
            source_kind,
            source_priority
        )
        VALUES (
            'labelhash-decoding-drift',
            convert_to('raw', 'UTF8'),
            'different',
            'check',
            false,
            'normalization failed',
            'schema-check',
            0
        );
        RAISE EXCEPTION
            'label_preimages accepted decoded text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "label_preimages_decoded_label_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO label_preimages (
        labelhash,
        raw_label,
        decoded_label,
        normalizer_version,
        normalized_under_version,
        normalization_error,
        source_kind,
        source_priority
    )
    VALUES (
        'labelhash-oversized',
        oversized_bytes,
        oversized_text,
        'check',
        false,
        'normalization failed',
        'schema-check',
        0
    );

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        block_number,
        block_hash,
        raw_fact_ref,
        derivation_kind,
        canonicality_state
    )
    VALUES (
        'event-oversized-interpreter-state-key',
        'schema-v2-check',
        'RecordChanged',
        'schema-check',
        1,
        'schema-v2-check',
        0,
        'block-0',
        jsonb_build_object(
            'interpreter_state_key', oversized_text,
            'state_scope', oversized_text
        ),
        'raw_log_preimage_observation',
        'canonical'
    );

    IF NOT EXISTS (
        SELECT 1
        FROM normalized_events
        WHERE chain_id = 'schema-v2-check'
          AND public.digest(
              COALESCE(
                  raw_fact_ref ->> 'interpreter_state_key',
                  event_identity
              ),
              'sha256'
          ) = public.digest(oversized_text, 'sha256')
          AND raw_fact_ref ->> 'interpreter_state_key' = oversized_text
    ) THEN
        RAISE EXCEPTION
            'oversized interpreter state key was not findable by its bounded digest';
    END IF;

    INSERT INTO ens_names (hash, name)
    VALUES ('hash-oversized', oversized_text);

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-oversized',
        'schema-v2-check',
        oversized_text,
        ARRAY[oversized_text],
        decode('', 'hex'),
        'namehash-oversized',
        ARRAY['labelhash-oversized'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        authority_arm,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000029',
        'schema-v2-check:namehash-oversized',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'ens_v1',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO name_current (
        logical_name_id,
        namespace,
        raw_name,
        namehash,
        surface_binding_id,
        resource_id,
        binding_kind,
        support_status,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-oversized',
        'schema-v2-check',
        oversized_text,
        'namehash-oversized',
        '00000000-0000-0000-0000-000000000029',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'supported',
        1
    );

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:namehash-oversized-child',
        'schema-v2-check',
        oversized_bytes,
        oversized_text,
        oversized_bytes,
        oversized_text,
        'namehash-oversized-child',
        'labelhash-oversized-child',
        1
    );

    INSERT INTO address_names_current (
        address,
        logical_name_id,
        relation,
        namespace,
        raw_name,
        namehash,
        surface_binding_id,
        resource_id,
        binding_kind,
        support_status,
        manifest_version
    )
    VALUES (
        'address-oversized',
        'schema-v2-check:namehash-oversized',
        'registrant',
        'schema-v2-check',
        oversized_text,
        'namehash-oversized',
        '00000000-0000-0000-0000-000000000029',
        '00000000-0000-0000-0000-000000000011',
        'declared_registry_path',
        'supported',
        1
    );

    INSERT INTO primary_names_current (
        address,
        coin_type,
        namespace,
        claim_status,
        raw_claim_name,
        claim_name_is_normalized
    )
    VALUES (
        'primary-oversized',
        '60',
        'schema-v2-check',
        'success',
        oversized_text,
        false
    );

    IF NOT EXISTS (
        SELECT 1 FROM label_preimages
        WHERE labelhash = 'labelhash-oversized' AND raw_label = oversized_bytes
    ) OR NOT EXISTS (
        SELECT 1 FROM ens_names
        WHERE hash = 'hash-oversized' AND name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM name_surfaces
        WHERE namespace = 'schema-v2-check'
          AND visibility_state = 'active'
          AND namehash = 'namehash-oversized'
          AND raw_name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM name_current
        WHERE namespace = 'schema-v2-check'
          AND namehash = 'namehash-oversized'
          AND logical_name_id = 'schema-v2-check:namehash-oversized'
          AND raw_name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM children_current
        WHERE parent_logical_name_id = 'schema-v2-check:namehash-0'
          AND surface_class = 'declared'
          AND namehash = 'namehash-oversized-child'
          AND child_logical_name_id = 'schema-v2-check:namehash-oversized-child'
          AND raw_name = oversized_bytes
          AND raw_label = oversized_bytes
    ) OR NOT EXISTS (
        SELECT 1 FROM address_names_current
        WHERE lower(address) = 'address-oversized'
          AND relation = 'registrant'
          AND namespace = 'schema-v2-check'
          AND namehash = 'namehash-oversized'
          AND logical_name_id = 'schema-v2-check:namehash-oversized'
          AND raw_name = oversized_text
    ) OR NOT EXISTS (
        SELECT 1 FROM primary_names_current
        WHERE namespace = 'schema-v2-check'
          AND coin_type = '60'
          AND address = 'primary-oversized'
          AND claim_status = 'success'
          AND raw_claim_name = oversized_text
    ) THEN
        RAISE EXCEPTION
            'oversized input was not findable through every bounded lookup path';
    END IF;

    INSERT INTO primary_names_current (
        address,
        coin_type,
        namespace,
        claim_status,
        raw_claim_name,
        claim_name_is_normalized,
        unsupported_reason
    )
    VALUES
        (
            'primary-success',
            '60',
            'schema-v2-check',
            'success',
            'name.eth',
            true,
            NULL
        ),
        (
            'primary-not-found',
            '60',
            'schema-v2-check',
            'not_found',
            NULL,
            false,
            NULL
        ),
        (
            'primary-unsupported',
            '60',
            'schema-v2-check',
            'unsupported',
            NULL,
            false,
            'coin type unsupported'
        ),
        (
            'primary-invalid-name',
            '60',
            'schema-v2-check',
            'invalid_name',
            'invalid name',
            false,
            NULL
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'primary-missing-name',
                    'success',
                    NULL::text,
                    false,
                    NULL::text,
                    'primary_names_current_claim_name_check'
                ),
                (
                    'primary-unexpected-name',
                    'not_found',
                    'name.eth',
                    false,
                    NULL::text,
                    'primary_names_current_claim_name_check'
                ),
                (
                    'primary-invalid-normalized',
                    'invalid_name',
                    'invalid name',
                    true,
                    NULL::text,
                    'primary_names_current_normalized_claim_check'
                ),
                (
                    'primary-missing-reason',
                    'unsupported',
                    NULL::text,
                    false,
                    NULL::text,
                    'primary_names_current_unsupported_reason_coherence_check'
                ),
                (
                    'primary-unexpected-reason',
                    'success',
                    'name.eth',
                    true,
                    'unexpected reason',
                    'primary_names_current_unsupported_reason_coherence_check'
                )
        ) AS invalid(
            address,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            unsupported_reason,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO primary_names_current (
                address,
                coin_type,
                namespace,
                claim_status,
                raw_claim_name,
                claim_name_is_normalized,
                unsupported_reason
            )
            VALUES (
                transition_case.address,
                '60',
                'schema-v2-check',
                transition_case.claim_status,
                transition_case.raw_claim_name,
                transition_case.claim_name_is_normalized,
                transition_case.unsupported_reason
            );
            RAISE EXCEPTION
                'primary_names_current accepted incoherent claim state';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    INSERT INTO ingest_cursors (
        chain_id,
        source_key,
        source_kind,
        seed_basis,
        start_block_number,
        next_block_number,
        target_block_number,
        last_processed_block_number,
        last_processed_block_hash
    )
    VALUES (
        'schema-v2-check',
        'valid-source',
        'logs',
        'base_seam',
        10,
        11,
        12,
        10,
        'processed-block-10'
    );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'invalid-next',
                    10::bigint,
                    9::bigint,
                    NULL::bigint,
                    NULL::bigint,
                    NULL::text,
                    'ingest_cursors_next_block_order_check'
                ),
                (
                    'invalid-target',
                    10::bigint,
                    10::bigint,
                    9::bigint,
                    NULL::bigint,
                    NULL::text,
                    'ingest_cursors_target_block_order_check'
                ),
                (
                    'invalid-last-pair',
                    10::bigint,
                    11::bigint,
                    NULL::bigint,
                    10::bigint,
                    NULL::text,
                    'ingest_cursors_last_processed_pair_check'
                ),
                (
                    'invalid-last-order',
                    10::bigint,
                    11::bigint,
                    NULL::bigint,
                    11::bigint,
                    'processed-block-11',
                    'ingest_cursors_last_processed_order_check'
                )
        ) AS invalid(
            source_key,
            start_block_number,
            next_block_number,
            target_block_number,
            last_processed_block_number,
            last_processed_block_hash,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO ingest_cursors (
                chain_id,
                source_key,
                source_kind,
                seed_basis,
                start_block_number,
                next_block_number,
                target_block_number,
                last_processed_block_number,
                last_processed_block_hash
            )
            VALUES (
                'schema-v2-check',
                transition_case.source_key,
                'logs',
                'base_seam',
                transition_case.start_block_number,
                transition_case.next_block_number,
                transition_case.target_block_number,
                transition_case.last_processed_block_number,
                transition_case.last_processed_block_hash
            );
            RAISE EXCEPTION
                'ingest_cursors accepted invalid ordering';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    INSERT INTO service_heartbeats (
        service_name,
        instance_id,
        chain_id,
        phase_name,
        started_at,
        heartbeat_at
    )
    VALUES (
        'indexer',
        'instance-valid',
        'schema-v2-check',
        'project',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:01+00'
    );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    '',
                    'instance-invalid-service',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_service_name_check'
                ),
                (
                    'indexer',
                    '',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_instance_id_check'
                ),
                (
                    'indexer',
                    'instance-invalid-chain',
                    '',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_chain_id_check'
                ),
                (
                    'indexer',
                    'instance-invalid-phase',
                    'schema-v2-check',
                    'publish',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_phase_name_check'
                ),
                (
                    'indexer',
                    'instance-invalid-time',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:01+00'::timestamptz,
                    '2026-01-01 00:00:00+00'::timestamptz,
                    'service_heartbeats_time_order_check'
                )
        ) AS invalid(
            service_name,
            instance_id,
            chain_id,
            phase_name,
            started_at,
            heartbeat_at,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO service_heartbeats (
                service_name,
                instance_id,
                chain_id,
                phase_name,
                started_at,
                heartbeat_at
            )
            VALUES (
                transition_case.service_name,
                transition_case.instance_id,
                transition_case.chain_id,
                transition_case.phase_name,
                transition_case.started_at,
                transition_case.heartbeat_at
            );
            RAISE EXCEPTION
                'service_heartbeats accepted an invalid row shape';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            authority_arm,
            active_from,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            '00000000-0000-0000-0000-000000000022',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000012',
            'declared_registry_path',
            'ens_v1',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'surface_bindings accepted overlapping canonical ranges';
    EXCEPTION
        WHEN exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO manifest_versions (
        manifest_version,
        namespace,
        source_family,
        chain_id,
        deployment_label,
        rollout_status,
        normalizer_version,
        file_path,
        manifest_payload
    )
    VALUES (
        1,
        'schema-v2-check',
        'schema-v2-check',
        'schema-v2-check',
        'schema-v2-check',
        'draft',
        'check',
        'schema-v2-check.toml',
        '{}'::jsonb
    )
    RETURNING manifest_id INTO manifest_key;

    BEGIN
        INSERT INTO manifest_discovery_rules (
            manifest_id,
            edge_kind,
            admission
        )
        VALUES (manifest_key, 'announced', 'declared');
        RAISE EXCEPTION
            'manifest_discovery_rules accepted an unknown edge kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "manifest_discovery_rules_edge_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO manifest_contract_instances (
            manifest_id,
            chain_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            proxy_kind
        )
        VALUES (
            manifest_key,
            'schema-v2-check',
            'root',
            'cross-chain-root',
            '00000000-0000-0000-0000-000000000003',
            'cross-chain-address',
            'none'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'manifest contract chain'
            );
        DELETE FROM manifest_contract_instances
        WHERE manifest_id = manifest_key
          AND declaration_name = 'cross-chain-root';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "manifest_contract_instances_chain_id_contract_instance_id_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            derivation_kind
        )
        VALUES (
            'mismatched-manifest-event',
            'wrong-namespace',
            'ResolverChanged',
            'wrong-source-family',
            99,
            manifest_key,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'normalized event manifest'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'mismatched-manifest-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_source_manifest_id_namespace_source_fami_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    IF cardinality(accepted_manifest_mismatches) > 0 THEN
        RAISE EXCEPTION
            'manifest tables accepted contradictory provenance: %',
            array_to_string(accepted_manifest_mismatches, ', ');
    END IF;

    INSERT INTO manifest_contract_instances (
        manifest_id,
        chain_id,
        declaration_kind,
        declaration_name,
        contract_instance_id,
        declared_address,
        proxy_kind
    )
    VALUES (
        manifest_key,
        'schema-v2-check',
        'root',
        'root',
        '00000000-0000-0000-0000-000000000001',
        'contract-address-0',
        'none'
    );

    INSERT INTO manifest_discovery_rules (
        manifest_id,
        edge_kind,
        admission
    )
    VALUES (manifest_key, 'resolver', 'declared');

    UPDATE contract_instance_addresses
    SET source_manifest_id = manifest_key
    WHERE contract_instance_id =
        '00000000-0000-0000-0000-000000000001'
      AND deactivated_at IS NULL;

    INSERT INTO discovery_edges (
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        admission_basis,
        source_manifest_id
    )
    VALUES (
        'schema-v2-check',
        'proxy_implementation',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'schema-v2-check',
        manifest_key
    );

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        source_manifest_id,
        chain_id,
        derivation_kind
    )
    VALUES (
        'manifest-delete-check',
        'schema-v2-check',
        'ResolverChanged',
        'schema-v2-check',
        1,
        manifest_key,
        'schema-v2-check',
        'ens_v2_resolver'
    );

    DELETE FROM manifest_versions
    WHERE manifest_id = manifest_key;

    IF EXISTS (
        SELECT 1
        FROM manifest_contract_instances
        WHERE manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM manifest_discovery_rules
        WHERE manifest_id = manifest_key
    ) THEN
        RAISE EXCEPTION
            'manifest deletion did not remove child declarations';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM contract_instance_addresses
        WHERE source_manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM discovery_edges
        WHERE source_manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM normalized_events
        WHERE source_manifest_id = manifest_key
    ) THEN
        RAISE EXCEPTION
            'manifest deletion did not clear retained provenance links';
    END IF;
END
$$;
SQL
} | run_psql

# The independent predecessor body and ACL are copied from public #855 f95200b3.
zero_default_migration="$ROOT/migrations/20260906120000_exact_zero_addr60_default_derivation.sql"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE FUNCTION exact_zero_writer_metadata() RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_build_object(
        'definition', replace(pg_get_functiondef(p.oid), current_schema(), '<phase>'),
        'arguments', pg_get_function_identity_arguments(p.oid),
        'return_type', pg_get_function_result(p.oid), 'owner', p.proowner,
        'security_definer', p.prosecdef,
        'config', replace(to_jsonb(p.proconfig)::text, current_schema(), '<phase>')::jsonb,
        'acl', (SELECT jsonb_agg(jsonb_build_array(a.grantor, a.grantee,
                    a.privilege_type, a.is_grantable) ORDER BY a.grantor, a.grantee,
                    a.privilege_type, a.is_grantable)
                FROM aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a)
    ) FROM pg_proc p
    WHERE p.oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure;
$$;
CREATE TEMP TABLE exact_zero_expected_writer AS SELECT exact_zero_writer_metadata() AS metadata;
DO $$
BEGIN
    IF (SELECT metadata->>'security_definer' <> 'true'
        OR metadata->>'return_type' <> 'text'
        OR metadata->'config' <> '["search_path=pg_catalog, <phase>, pg_temp"]'::jsonb
        FROM exact_zero_expected_writer)
        OR EXISTS (SELECT 1 FROM pg_proc p,
            LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
            WHERE p.oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure
              AND a.grantee = 0 AND a.privilege_type = 'EXECUTE') THEN
        RAISE EXCEPTION 'fresh exact-zero writer privilege envelope is invalid';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
SQL
    for pass in 1 2; do
        emit_phase_migration "$zero_default_migration" baseline-first
        cat <<'SQL'
DO $$
BEGIN
    IF exact_zero_writer_metadata() <> (SELECT metadata FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'exact-zero function definition, signature or privilege metadata diverged';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
SQL
    done
    cat <<'SQL'
CREATE OR REPLACE FUNCTION write_resolution_divergence(
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text,
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    compared_execution_authority jsonb,
    requested_logical_name_id text,
    requested_resolver_chain_id text,
    requested_resolver_address text,
    requested_record_key text,
    compared_positions jsonb,
    live_answer jsonb,
    used_ccip_read boolean
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    guard_status text;
    resolver_path jsonb;
    compared_entries jsonb;
    compared_provenance jsonb;
    compared_support_status text;
    selector_family text;
    selector_key text;
    indexed_entry jsonb;
    default_entry jsonb;
    indexed_status text;
    indexed_value jsonb;
    indexed_answer jsonb;
BEGIN
    IF used_ccip_read THEN
        RETURN 'ccip_skipped';
    END IF;

    IF compared_execution_authority ->> 'logical_name_id'
        IS DISTINCT FROM requested_logical_name_id
    THEN
        RETURN 'guard_rejected';
    END IF;

    guard_status := revalidate_resolution_lookup_state(
        requested_authoritative_chain_id,
        requested_authoritative_block_number,
        requested_authoritative_block_hash,
        compared_positions,
        compared_execution_authority,
        compared_resource_id,
        compared_boundary_key,
        compared_row_xmin
    );

    IF guard_status <> 'unchanged' THEN
        RETURN 'guard_rejected';
    END IF;

    CASE
        WHEN requested_record_key = 'avatar' THEN
            selector_family := 'avatar';
            selector_key := NULL;
        WHEN requested_record_key = 'contenthash' THEN
            selector_family := 'contenthash';
            selector_key := NULL;
        WHEN requested_record_key LIKE 'text:%'
            AND length(substr(requested_record_key, 6)) > 0
        THEN
            selector_family := 'text';
            selector_key := substr(requested_record_key, 6);
        WHEN requested_record_key ~ '^addr:(0|[1-9][0-9]*)$' THEN
            BEGIN
                selector_key := substr(requested_record_key, 6);
                IF selector_key::numeric > 18446744073709551615::numeric THEN
                    RETURN 'guard_rejected';
                END IF;
                selector_family := 'addr';
            EXCEPTION
                WHEN data_exception THEN
                    RETURN 'guard_rejected';
            END;
        ELSE
            RETURN 'guard_rejected';
    END CASE;

    SELECT inventory.entries,
           inventory.provenance,
           inventory.support_status,
           name.declared_summary #> '{topology,resolver_path}'
    INTO compared_entries, compared_provenance, compared_support_status, resolver_path
    FROM record_inventory_current AS inventory
    JOIN name_current AS name
      ON name.logical_name_id = requested_logical_name_id
     AND name.support_status = 'supported'
     AND name.declared_summary
            #> '{topology,version_boundaries,record_version_boundary}' =
         inventory.record_version_boundary
    WHERE inventory.resource_id = compared_resource_id
      AND inventory.record_version_boundary_key = compared_boundary_key
      AND inventory.xmin::text = compared_row_xmin
    FOR SHARE OF inventory, name;

    IF NOT FOUND
        OR jsonb_typeof(resolver_path) IS DISTINCT FROM 'array'
        OR jsonb_array_length(resolver_path) = 0
        OR resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'chain_id' <> requested_resolver_chain_id
        OR lower(
            resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'address'
        ) <> lower(requested_resolver_address)
    THEN
        RETURN 'guard_rejected';
    END IF;

    SELECT candidate.entry
    INTO indexed_entry
    FROM jsonb_array_elements(compared_entries)
        WITH ORDINALITY AS candidate(entry, ordinal)
    WHERE candidate.entry ->> 'record_key' = requested_record_key
       OR (
            candidate.entry ->> 'record_family' = selector_family
            AND (candidate.entry ->> 'selector_key')
                IS NOT DISTINCT FROM selector_key
       )
       OR (
            requested_record_key = 'avatar'
            AND candidate.entry ->> 'record_key' = 'text:avatar'
       )
    ORDER BY CASE
        WHEN candidate.entry ->> 'record_key' = 'text:avatar'
            AND requested_record_key = 'avatar'
        THEN 1
        ELSE 0
    END,
    candidate.ordinal
    LIMIT 1;

    IF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
       AND selector_family = 'addr'
       AND (
           selector_key = '60'
           OR selector_key::numeric BETWEEN 2147483649::numeric AND 4294967295::numeric
       )
       AND EXISTS (
           SELECT 1
           FROM jsonb_array_elements(COALESCE(
               compared_provenance -> 'read_rules', '[]'::jsonb
           )) rule
           WHERE rule ->> 'kind' = 'ensip19_default_address'
             AND rule ->> 'source_record_key' = 'addr:2147483648'
       )
    THEN
        IF compared_support_status <> 'supported' THEN
            indexed_entry := jsonb_build_object('status', 'unsupported');
        ELSE
            SELECT candidate.entry
            INTO default_entry
            FROM jsonb_array_elements(compared_entries)
                WITH ORDINALITY AS candidate(entry, ordinal)
            WHERE candidate.entry ->> 'record_key' = 'addr:2147483648'
               OR (
                    candidate.entry ->> 'record_family' = 'addr'
                    AND candidate.entry ->> 'selector_key' = '2147483648'
               )
            ORDER BY candidate.ordinal
            LIMIT 1;

            IF default_entry IS NULL THEN
                indexed_entry := jsonb_build_object('status', 'not_found');
            ELSIF default_entry ->> 'status' IN ('success', 'not_found') THEN
                -- Match the requested getter's verified decode. addr(bytes32) converts
                -- the coin-60 bytes to address(0); multicoin addr(bytes32,uint256)
                -- preserves non-empty bytes, including 20 zero bytes.
                -- (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
                -- (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
                IF selector_key = '60'
                   AND default_entry ->> 'status' = 'success'
                   AND lower(COALESCE(
                       default_entry #>> '{value,value}',
                       default_entry #>> '{value,bytes}',
                       default_entry ->> 'value'
                   )) = '0x0000000000000000000000000000000000000000'
                THEN
                    indexed_entry := jsonb_build_object('status', 'not_found');
                ELSE
                    indexed_entry := default_entry;
                END IF;
            ELSE
                indexed_entry := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;
    ELSIF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
          AND compared_support_status <> 'supported'
    THEN
        indexed_entry := jsonb_build_object('status', 'unsupported');
    END IF;

    IF indexed_entry IS NULL THEN
        indexed_answer := jsonb_build_object('status', 'not_found');
    ELSE
        indexed_status := CASE COALESCE(
            indexed_entry ->> 'status',
            'unsupported'
        )
            WHEN 'failed' THEN 'execution_failed'
            ELSE COALESCE(indexed_entry ->> 'status', 'unsupported')
        END;
        indexed_answer := jsonb_build_object('status', indexed_status);
        IF indexed_status = 'success' THEN
            indexed_value := COALESCE(
                indexed_entry #> '{value,value}',
                indexed_entry #> '{value,bytes}',
                indexed_entry -> 'value'
            );
            IF jsonb_typeof(indexed_value) = 'string' THEN
                indexed_answer := indexed_answer || jsonb_build_object(
                    'value',
                    CASE
                        WHEN selector_family = 'addr'
                            THEN lower(indexed_value #>> '{}')
                        ELSE indexed_value #>> '{}'
                    END
                );
            ELSE
                indexed_answer := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;
    END IF;

    IF indexed_answer = live_answer THEN
        UPDATE resolution_divergences
        SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind = requested_record_key
          AND cleared_at IS NULL;
        IF FOUND THEN
            RETURN 'cleared';
        END IF;
        RETURN 'agreement';
    END IF;

    UPDATE resolution_divergences
    SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
    WHERE logical_name_id = requested_logical_name_id
      AND resolver_chain_id = requested_resolver_chain_id
      AND lower(resolver_address) = lower(requested_resolver_address)
      AND request_kind_hash =
          public.digest(requested_record_key, 'sha256')
      AND request_kind = requested_record_key
      AND observed_positions <> compared_positions
      AND cleared_at IS NULL;

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind <> requested_record_key
    ) THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    ) VALUES (
        requested_logical_name_id,
        requested_resolver_chain_id,
        lower(requested_resolver_address),
        requested_record_key,
        compared_positions,
        indexed_answer,
        live_answer
    )
    ON CONFLICT ON CONSTRAINT resolution_divergences_pkey DO UPDATE
    SET indexed_result = EXCLUDED.indexed_result,
        live_result = EXCLUDED.live_result,
        last_observed_at = GREATEST(
            resolution_divergences.last_observed_at,
            statement_timestamp()
        ),
        cleared_at = NULL
    WHERE resolution_divergences.request_kind = EXCLUDED.request_kind;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    RETURN 'written';
END
$$;

REVOKE ALL ON FUNCTION write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) FROM PUBLIC;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc
        WHERE oid = 'write_resolution_divergence(uuid,text,text,text,bigint,text,jsonb,text,text,text,text,jsonb,jsonb,boolean)'::regprocedure
          AND md5(prosrc) = '6a4678aa84cd76e410d7883daed9f8fa'
          AND proconfig = ARRAY['search_path=pg_catalog, bigname_phase, pg_temp'])
        OR exact_zero_writer_metadata() - 'definition' - 'config'
            <> (SELECT metadata - 'definition' - 'config' FROM exact_zero_expected_writer)
        OR exact_zero_writer_metadata() = (SELECT metadata FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'independent f952 predecessor was not installed';
    END IF;
END
$$;
SQL
    for pass in 1 2; do
        emit_phase_migration "$zero_default_migration" preceding-shape
        cat <<'SQL'
DO $$
BEGIN
    IF exact_zero_writer_metadata() <> (SELECT metadata FROM exact_zero_expected_writer) THEN
        RAISE EXCEPTION 'exact-zero function definition, signature or privilege metadata diverged';
    END IF;
END
$$;
BEGIN;
SELECT assert_exact_zero_migration_behavior();
ROLLBACK;
SQL
    done
    cat <<'SQL'
DROP TABLE exact_zero_expected_writer;
DROP FUNCTION exact_zero_writer_metadata();
DROP FUNCTION assert_exact_zero_migration_behavior();
SQL
} | run_psql
assert_migration_context_count "$zero_default_migration" empty-schema 1
assert_migration_context_count "$zero_default_migration" baseline-first 2
assert_migration_context_count "$zero_default_migration" preceding-shape 2
report_timing exact-zero-default

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO chain_lineage (
    chain_id,
    block_hash,
    block_number,
    block_timestamp,
    canonicality_state
)
VALUES (
    'schema-v2-race',
    'race-block-0',
    0,
    '2026-01-01 00:00:00+00',
    'canonical'
);

INSERT INTO name_surfaces (
    logical_name_id,
    namespace,
    raw_name,
    raw_labels,
    dns_encoded_name,
    namehash,
    labelhashes,
    normalizer_version,
    visibility_state,
    chain_id,
    block_hash,
    block_number,
    canonicality_state
)
VALUES (
    'schema-v2-race:race-namehash',
    'schema-v2-race',
    'race',
    ARRAY['race'],
    decode('00', 'hex'),
    'race-namehash',
    ARRAY['race-labelhash'],
    'schema-v2-check',
    'active',
    'schema-v2-race',
    'race-block-0',
    0,
    'canonical'
);
SQL
} | run_psql

schema_v2_race_log="$(mktemp)"
schema_v2_race_application="schema_v2_race_${PPID}_$$"

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf "SET application_name TO '%s';\n" \
        "$schema_v2_race_application"
    cat <<'SQL'
BEGIN;

INSERT INTO resolution_divergences (
    logical_name_id,
    resolver_chain_id,
    resolver_address,
    request_kind,
    observed_positions,
    indexed_result,
    live_result
)
VALUES (
    'schema-v2-race:race-namehash',
    'schema-v2-race',
    'resolver-address-race',
    'addr',
    '{
        "resolver": {
            "chain_id": "schema-v2-race",
            "block_hash": "race-block-0",
            "block_number": 0,
            "timestamp": "2026-01-01T00:00:00Z"
        }
    }'::jsonb,
    '{"value": "indexed"}'::jsonb,
    '{"value": "live"}'::jsonb
);

SELECT pg_sleep(5);
COMMIT;
SQL
} | run_psql >"$schema_v2_race_log" 2>&1 &
schema_v2_race_pid=$!

if ! wait_for_schema_v2_race_session "$schema_v2_race_application"; then
    printf '%s\n' \
        "concurrent resolution-difference insert did not become ready" >&2
    cat "$schema_v2_race_log" >&2
    exit 1
fi

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
UPDATE chain_lineage
SET canonicality_state = 'orphaned'
WHERE chain_id = 'schema-v2-race'
  AND block_hash = 'race-block-0';
SQL
} | run_psql

if ! wait "$schema_v2_race_pid"; then
    cat "$schema_v2_race_log" >&2
    exit 1
fi
schema_v2_race_pid=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-race'
          AND cleared_at IS NULL
    ) THEN
        RAISE EXCEPTION
            'concurrent reorg left an active resolution difference';
    END IF;
END
$$;
SQL
} | run_psql

rm -f -- "$schema_v2_race_log"
schema_v2_race_log=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO chain_lineage (
    chain_id,
    block_hash,
    block_number,
    block_timestamp,
    canonicality_state
)
VALUES (
    'schema-v2-head-race',
    'head-race-block-0',
    0,
    '2026-01-01 00:00:00+00',
    'canonical'
);
SQL
} | run_psql

schema_v2_race_log="$(mktemp)"
schema_v2_race_application="schema_v2_head_race_${PPID}_$$"

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf "SET application_name TO '%s';\n" \
        "$schema_v2_race_application"
    cat <<'SQL'
BEGIN;

INSERT INTO chain_heads (
    chain_id,
    latest_block_hash,
    latest_block_number
)
VALUES (
    'schema-v2-head-race',
    'head-race-block-0',
    0
);

SELECT pg_sleep(5);
COMMIT;
SQL
} | run_psql >"$schema_v2_race_log" 2>&1 &
schema_v2_race_pid=$!

if ! wait_for_schema_v2_race_session "$schema_v2_race_application"; then
    printf '%s\n' "concurrent chain-head insert did not become ready" >&2
    cat "$schema_v2_race_log" >&2
    exit 1
fi

schema_v2_head_update_output=""
if ! schema_v2_head_update_output="$(
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        cat <<'SQL'
UPDATE chain_lineage
SET canonicality_state = 'orphaned'
WHERE chain_id = 'schema-v2-head-race'
  AND block_hash = 'head-race-block-0';
SQL
    } | run_psql 2>&1
)"; then
    if [[ "$schema_v2_head_update_output" != \
        *"a chain head still references this block state"* ]]; then
        printf '%s\n' "$schema_v2_head_update_output" >&2
        exit 1
    fi
fi

if ! wait "$schema_v2_race_pid"; then
    cat "$schema_v2_race_log" >&2
    exit 1
fi
schema_v2_race_pid=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chain_heads AS head
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE lineage.canonicality_state
            NOT IN ('canonical', 'safe', 'finalized')
    ) THEN
        RAISE EXCEPTION
            'concurrent reorg left a head on a noncanonical block';
    END IF;
END
$$;
SQL
} | run_psql

report_timing specialized-predecessor "$refusal_probe_seconds"
if [ "${SCHEMA_V2_APPLY_CHECK_TIMING:-0}" = 1 ]; then printf 'schema-v2 timing: refusal-probes=%ss\n' "$refusal_probe_seconds"; fi
assert_reviewed_phase_migrations_applied
if [ "$refusal_assertions_passed" -ne "$expected_refusal_assertions" ]; then
    printf '%s\n' \
        "refusal assertions: $refusal_assertions_passed/$expected_refusal_assertions" >&2
    exit 1
fi
report_timing final-assertions
printf '%s\n' \
    "schema-v2 baseline applied twice and passed structural and behavior checks"
printf '%s\n' \
    "schema-migration coverage: expected reviewed phase schema-migrations=$expected_reviewed_phase_migration_count; applied on maintained paths=$unique_successful_migration_count; exact-predecessor-shape proofs=$predecessor_shape_proof_count/$expected_reviewed_phase_migration_count; without exact-predecessor-shape proof=$((expected_reviewed_phase_migration_count - predecessor_shape_proof_count)); total successful applications=$total_successful_migration_applications; intentional skips=$intentional_phase_migration_skip_count; refusal assertions=$refusal_assertions_passed/$expected_refusal_assertions"
