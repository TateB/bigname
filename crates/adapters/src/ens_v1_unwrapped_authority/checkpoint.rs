use super::*;
use crate::ens_v1_subregistry_discovery::ReplayAdapterCheckpointContext;
use anyhow::{Context, Result, bail};
use futures_util::TryStreamExt;
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

const ADAPTER: &str = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY;
const CHECKPOINT_SCOPE: &str = "full_closure";
const SNAPSHOT_VERSION: i64 = 1;
const ITEM_KIND_HISTORY: &str = "name_history";
const ITEM_KIND_REVERSE_HISTORY: &str = "reverse_history";
const ITEM_KIND_KNOWN_NAME: &str = "known_name";
const ITEM_KIND_KNOWN_NAME_REF: &str = "known_name_ref";
const ITEM_KIND_NAMEHASH_LABELHASH: &str = "namehash_labelhash";
const ITEM_KIND_PENDING_OBSERVATIONS: &str = "pending_observations";
const ITEM_KIND_MIGRATED_NODE: &str = "migrated_registry_node";
const CHECKPOINT_ITEM_INSERT_BATCH_SIZE: usize = 250;
const CHECKPOINT_ITEM_DELETE_BATCH_SIZE: usize = 1_000;
const CHECKPOINT_ESCAPED_STRING_KEY: &str =
    "__bigname_unwrapped_authority_checkpoint_string_v1_hex";

#[derive(Default)]
pub(super) struct UnwrappedAuthorityReplayCheckpointDelta {
    pub(super) history_keys: BTreeSet<String>,
    pub(super) reverse_history_keys: BTreeSet<String>,
    pub(super) known_name_keys: BTreeSet<String>,
    pub(super) known_name_ref_keys: BTreeSet<String>,
    pub(super) namehash_labelhash_keys: BTreeSet<String>,
    pub(super) pending_observation_keys: BTreeSet<String>,
    pub(super) migrated_nodes: BTreeSet<String>,
}

impl UnwrappedAuthorityReplayCheckpointDelta {
    pub(super) fn mark_history(&mut self, key: impl Into<String>) {
        self.history_keys.insert(key.into());
    }

    pub(super) fn mark_reverse_history(&mut self, key: impl Into<String>) {
        self.reverse_history_keys.insert(key.into());
    }

    pub(super) fn mark_known_name(&mut self, key: impl Into<String>) {
        self.known_name_keys.insert(key.into());
    }

    pub(super) fn mark_known_name_ref(&mut self, key: impl Into<String>) {
        self.known_name_ref_keys.insert(key.into());
    }

    pub(super) fn mark_namehash_labelhash(&mut self, key: impl Into<String>) {
        self.namehash_labelhash_keys.insert(key.into());
    }

    pub(super) fn mark_pending_observations(&mut self, key: impl Into<String>) {
        self.pending_observation_keys.insert(key.into());
    }

    pub(super) fn mark_migrated_node(&mut self, node: impl Into<String>) {
        self.migrated_nodes.insert(node.into());
    }

    pub(super) fn clear(&mut self) {
        self.history_keys.clear();
        self.reverse_history_keys.clear();
        self.known_name_keys.clear();
        self.known_name_ref_keys.clear();
        self.namehash_labelhash_keys.clear();
        self.pending_observation_keys.clear();
        self.migrated_nodes.clear();
    }
}

pub(super) struct UnwrappedAuthorityReplayCheckpointState {
    pub(super) histories: BTreeMap<String, NameHistory>,
    pub(super) reverse_histories: BTreeMap<String, ReverseClaimSourceHistory>,
    pub(super) known_names_by_namehash: HashMap<String, NameMetadata>,
    pub(super) known_name_refs_by_namehash: HashMap<String, ObservationRef>,
    pub(super) namehash_to_labelhash: HashMap<String, String>,
    pub(super) pending_namehash_observations: HashMap<String, Vec<AuthorityObservation>>,
    pub(super) migrated_registry_nodes: MigratedRegistryNodes,
}

pub(super) struct UnwrappedAuthorityReplayCheckpointStateRef<'a> {
    pub(super) histories: &'a BTreeMap<String, NameHistory>,
    pub(super) reverse_histories: &'a BTreeMap<String, ReverseClaimSourceHistory>,
    pub(super) known_names_by_namehash: &'a HashMap<String, NameMetadata>,
    pub(super) known_name_refs_by_namehash: &'a HashMap<String, ObservationRef>,
    pub(super) namehash_to_labelhash: &'a HashMap<String, String>,
    pub(super) pending_namehash_observations: &'a HashMap<String, Vec<AuthorityObservation>>,
    pub(super) migrated_registry_nodes: &'a MigratedRegistryNodes,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct UnwrappedAuthorityReplayFlushedEvents {
    pub(super) total_count: usize,
    pub(super) inserted_count: usize,
    pub(super) by_kind: BTreeMap<String, usize>,
}

pub(super) struct UnwrappedAuthorityReplayCheckpoint {
    context: ReplayAdapterCheckpointContext,
    chain: String,
    status: String,
    last_block_number: Option<i64>,
    scanned_log_count: usize,
    matched_log_count: usize,
    flushed_events: UnwrappedAuthorityReplayFlushedEvents,
    state_payload: Value,
}

pub async fn clear_replay_adapter_checkpoints(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(ADAPTER)
    .bind(CHECKPOINT_SCOPE)
    .execute(pool)
    .await;
    if is_undefined_table_error(&result) {
        return Ok(());
    }
    result.with_context(|| {
        format!(
            "failed to clear unwrapped-authority replay adapter checkpoints for {deployment_profile}/{chain}/{cursor_kind}"
        )
    })?;
    Ok(())
}

impl UnwrappedAuthorityReplayCheckpoint {
    pub(super) async fn load_or_start(
        pool: &PgPool,
        chain: &str,
        context: &ReplayAdapterCheckpointContext,
    ) -> Result<Self> {
        let existing = load_checkpoint_row(pool, chain, context).await?;
        if existing.as_ref().is_some_and(|checkpoint| {
            checkpoint.context.range_start_block_number != context.range_start_block_number
                || !checkpoint.snapshot_version_is_current()
        }) {
            delete_checkpoint(pool, chain, context).await?;
        }

        if existing.is_none()
            || existing.as_ref().is_some_and(|checkpoint| {
                checkpoint.context.range_start_block_number != context.range_start_block_number
                    || !checkpoint.snapshot_version_is_current()
            })
        {
            sqlx::query(
                r#"
                INSERT INTO normalized_replay_adapter_checkpoints (
                    deployment_profile,
                    chain_id,
                    cursor_kind,
                    adapter,
                    checkpoint_scope,
                    replay_start_block_number,
                    replay_target_block_number,
                    state_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            )
            .bind(&context.deployment_profile)
            .bind(chain)
            .bind(&context.cursor_kind)
            .bind(ADAPTER)
            .bind(CHECKPOINT_SCOPE)
            .bind(context.range_start_block_number)
            .bind(context.target_block_number)
            .bind(json!({ "version": SNAPSHOT_VERSION }))
            .execute(pool)
            .await
            .with_context(|| {
                format!(
                    "failed to start {ADAPTER} replay checkpoint for {}/{}",
                    context.deployment_profile, chain
                )
            })?;
        } else {
            sqlx::query(
                r#"
                UPDATE normalized_replay_adapter_checkpoints
                SET
                    replay_target_block_number = GREATEST(replay_target_block_number, $6),
                    status = CASE
                        WHEN status = 'completed' AND replay_target_block_number < $6 THEN 'running'
                        ELSE status
                    END,
                    completed_at = CASE
                        WHEN status = 'completed' AND replay_target_block_number < $6 THEN NULL
                        ELSE completed_at
                    END,
                    updated_at = now()
                WHERE deployment_profile = $1
                  AND chain_id = $2
                  AND cursor_kind = $3
                  AND adapter = $4
                  AND checkpoint_scope = $5
                "#,
            )
            .bind(&context.deployment_profile)
            .bind(chain)
            .bind(&context.cursor_kind)
            .bind(ADAPTER)
            .bind(CHECKPOINT_SCOPE)
            .bind(context.target_block_number)
            .execute(pool)
            .await
            .with_context(|| {
                format!(
                    "failed to refresh {ADAPTER} replay checkpoint for {}/{}",
                    context.deployment_profile, chain
                )
            })?;
        }

        load_checkpoint_row(pool, chain, context)
            .await?
            .context("started unwrapped-authority replay checkpoint row was not found")
    }

    pub(super) fn completed_summary(&self) -> Result<Option<EnsV1UnwrappedAuthoritySyncSummary>> {
        if self.status != "completed" {
            return Ok(None);
        }
        let summary = self
            .state_payload
            .get("summary")
            .context("completed unwrapped-authority replay checkpoint is missing summary")?;
        Ok(Some(summary_from_payload(summary)?))
    }

    pub(super) fn last_block_number(&self) -> Option<i64> {
        self.last_block_number
    }

    pub(super) fn target_block_number(&self) -> i64 {
        self.context.target_block_number
    }

    pub(super) fn needs_replay_auxiliary_state(&self) -> bool {
        self.last_block_number
            .is_none_or(|last_block_number| last_block_number < self.context.target_block_number)
    }

    pub(super) fn scanned_log_count(&self) -> usize {
        self.scanned_log_count
    }

    pub(super) fn matched_log_count(&self) -> usize {
        self.matched_log_count
    }

    pub(super) fn flushed_events(&self) -> &UnwrappedAuthorityReplayFlushedEvents {
        &self.flushed_events
    }

    pub(super) async fn load_state(
        &self,
        pool: &PgPool,
        include_replay_auxiliary_state: bool,
    ) -> Result<Option<UnwrappedAuthorityReplayCheckpointState>> {
        if self.last_block_number.is_none() {
            return Ok(None);
        }

        let materialization_item_kinds = vec![
            ITEM_KIND_HISTORY.to_owned(),
            ITEM_KIND_REVERSE_HISTORY.to_owned(),
        ];
        let mut rows = sqlx::query(
            r#"
            SELECT item_kind, item_key, item_payload
            FROM normalized_replay_adapter_checkpoint_items
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND NOT (
                  item_kind = 'pending_observations'
                  AND item_payload = '[]'::jsonb
              )
              AND (
                  $6::BOOLEAN
                  OR item_kind = ANY($7::TEXT[])
              )
            ORDER BY item_kind, item_key
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .bind(include_replay_auxiliary_state)
        .bind(&materialization_item_kinds)
        .fetch(pool);

        let mut histories = BTreeMap::new();
        let mut reverse_histories = BTreeMap::new();
        let mut known_names_by_namehash = HashMap::new();
        let mut known_name_refs_by_namehash = HashMap::new();
        let mut namehash_to_labelhash = HashMap::new();
        let mut pending_namehash_observations = HashMap::new();
        let mut migrated_nodes = HashSet::new();

        while let Some(row) = rows.try_next().await.with_context(|| {
            format!(
                "failed to load staged {ADAPTER} replay checkpoint items for {}/{}",
                self.context.deployment_profile, self.chain
            )
        })? {
            let item_kind: String = row.try_get("item_kind")?;
            let item_key: String = row.try_get("item_key")?;
            let item_payload: Value = row.try_get("item_payload")?;
            match item_kind.as_str() {
                ITEM_KIND_HISTORY => {
                    histories.insert(item_key, decode_item(item_payload, ITEM_KIND_HISTORY)?);
                }
                ITEM_KIND_REVERSE_HISTORY => {
                    reverse_histories.insert(
                        item_key,
                        decode_item(item_payload, ITEM_KIND_REVERSE_HISTORY)?,
                    );
                }
                ITEM_KIND_KNOWN_NAME => {
                    known_names_by_namehash
                        .insert(item_key, decode_item(item_payload, ITEM_KIND_KNOWN_NAME)?);
                }
                ITEM_KIND_KNOWN_NAME_REF => {
                    known_name_refs_by_namehash.insert(
                        item_key,
                        decode_item(item_payload, ITEM_KIND_KNOWN_NAME_REF)?,
                    );
                }
                ITEM_KIND_NAMEHASH_LABELHASH => {
                    if let Some(labelhash) = item_payload.get("labelhash").and_then(Value::as_str) {
                        namehash_to_labelhash.insert(item_key, labelhash.to_owned());
                    }
                }
                ITEM_KIND_PENDING_OBSERVATIONS => {
                    let observations = decode_item::<Vec<AuthorityObservation>>(
                        item_payload,
                        ITEM_KIND_PENDING_OBSERVATIONS,
                    )?;
                    if !observations.is_empty() {
                        pending_namehash_observations.insert(item_key, observations);
                    }
                }
                ITEM_KIND_MIGRATED_NODE => {
                    migrated_nodes.insert(item_key);
                }
                _ => {}
            }
        }

        Ok(Some(UnwrappedAuthorityReplayCheckpointState {
            histories,
            reverse_histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            migrated_registry_nodes: MigratedRegistryNodes::from_delta(migrated_nodes),
        }))
    }

    pub(super) async fn save_progress(
        &mut self,
        pool: &PgPool,
        boundary_block_number: i64,
        scanned_log_count: usize,
        matched_log_count: usize,
        state: UnwrappedAuthorityReplayCheckpointStateRef<'_>,
        delta: &UnwrappedAuthorityReplayCheckpointDelta,
        flushed_events: &UnwrappedAuthorityReplayFlushedEvents,
    ) -> Result<()> {
        let item_rows = checkpoint_item_rows(&state, delta)?;
        let pending_observation_delete_keys =
            checkpoint_pending_observation_delete_keys(&state, delta);
        let staged_item_count = state.histories.len() + state.reverse_histories.len();
        let staged_aux_item_count = state.known_names_by_namehash.len()
            + state.known_name_refs_by_namehash.len()
            + state.namehash_to_labelhash.len()
            + state.pending_namehash_observations.len()
            + state.migrated_registry_nodes.delta_nodes().count();
        let state_payload = json!({
            "version": SNAPSHOT_VERSION,
            "last_block_number": boundary_block_number,
            "flushed_normalized_event_count": flushed_events.total_count,
            "flushed_normalized_event_inserted_count": flushed_events.inserted_count,
            "flushed_by_kind": &flushed_events.by_kind,
        });

        let mut transaction = pool
            .begin()
            .await
            .context("failed to start unwrapped-authority replay checkpoint transaction")?;
        delete_checkpoint_items(
            &mut transaction,
            self,
            ITEM_KIND_PENDING_OBSERVATIONS,
            &pending_observation_delete_keys,
        )
        .await?;
        insert_checkpoint_items(&mut transaction, self, &item_rows).await?;
        update_checkpoint_progress(
            &mut transaction,
            self,
            "running",
            Some(boundary_block_number),
            scanned_log_count,
            matched_log_count,
            staged_item_count,
            staged_aux_item_count,
            state_payload.clone(),
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit unwrapped-authority replay checkpoint progress")?;

        self.last_block_number = Some(boundary_block_number);
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        self.flushed_events = flushed_events.clone();
        self.status = "running".to_owned();
        self.state_payload = state_payload;
        Ok(())
    }

    pub(super) async fn mark_stream_complete(
        &mut self,
        pool: &PgPool,
        scanned_log_count: usize,
        matched_log_count: usize,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE normalized_replay_adapter_checkpoints
            SET
                status = 'stream_complete',
                scanned_log_count = $6,
                matched_log_count = $7,
                updated_at = now(),
                last_failure_reason = NULL
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
        .execute(pool)
        .await
        .context("failed to mark unwrapped-authority replay checkpoint stream complete")?;

        self.status = "stream_complete".to_owned();
        self.scanned_log_count = scanned_log_count;
        self.matched_log_count = matched_log_count;
        Ok(())
    }

    pub(super) async fn mark_completed(
        &mut self,
        pool: &PgPool,
        summary: &EnsV1UnwrappedAuthoritySyncSummary,
    ) -> Result<()> {
        let state_payload = json!({
            "version": SNAPSHOT_VERSION,
            "summary": summary_payload(summary),
        });
        sqlx::query(
            r#"
            UPDATE normalized_replay_adapter_checkpoints
            SET
                status = 'completed',
                scanned_log_count = $6,
                matched_log_count = $7,
                state_payload = $8,
                completed_at = now(),
                updated_at = now(),
                last_failure_reason = NULL
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
            "#,
        )
        .bind(&self.context.deployment_profile)
        .bind(&self.chain)
        .bind(&self.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .bind(i64::try_from(summary.scanned_log_count).context("scanned log count overflowed i64")?)
        .bind(i64::try_from(summary.matched_log_count).context("matched log count overflowed i64")?)
        .bind(&state_payload)
        .execute(pool)
        .await
        .context("failed to mark unwrapped-authority replay checkpoint completed")?;

        self.status = "completed".to_owned();
        self.scanned_log_count = summary.scanned_log_count;
        self.matched_log_count = summary.matched_log_count;
        self.flushed_events = UnwrappedAuthorityReplayFlushedEvents::default();
        self.state_payload = state_payload;
        Ok(())
    }

    fn snapshot_version_is_current(&self) -> bool {
        self.state_payload
            .get("version")
            .and_then(Value::as_i64)
            .is_none_or(|version| version == SNAPSHOT_VERSION)
    }
}

fn is_undefined_table_error<T>(result: &std::result::Result<T, sqlx::Error>) -> bool {
    matches!(
        result,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01")
    )
}

async fn load_checkpoint_row(
    pool: &PgPool,
    chain: &str,
    context: &ReplayAdapterCheckpointContext,
) -> Result<Option<UnwrappedAuthorityReplayCheckpoint>> {
    let row = sqlx::query(
        r#"
        SELECT
            replay_start_block_number,
            replay_target_block_number,
            last_block_number,
            scanned_log_count,
            matched_log_count,
            status,
            state_payload
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&context.deployment_profile)
    .bind(chain)
    .bind(&context.cursor_kind)
    .bind(ADAPTER)
    .bind(CHECKPOINT_SCOPE)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load {ADAPTER} replay checkpoint for {}/{}",
            context.deployment_profile, chain
        )
    })?;

    row.map(|row| checkpoint_from_row(chain, context, row))
        .transpose()
}

fn checkpoint_from_row(
    chain: &str,
    context: &ReplayAdapterCheckpointContext,
    row: sqlx::postgres::PgRow,
) -> Result<UnwrappedAuthorityReplayCheckpoint> {
    let state_payload: Value = row.try_get("state_payload")?;
    let flushed_events = flushed_events_from_payload(&state_payload)?;
    Ok(UnwrappedAuthorityReplayCheckpoint {
        context: ReplayAdapterCheckpointContext {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            range_start_block_number: row.try_get("replay_start_block_number")?,
            target_block_number: row.try_get("replay_target_block_number")?,
        },
        chain: chain.to_owned(),
        status: row.try_get("status")?,
        last_block_number: row.try_get("last_block_number")?,
        scanned_log_count: usize::try_from(row.try_get::<i64, _>("scanned_log_count")?)
            .context("checkpoint scanned log count overflowed usize")?,
        matched_log_count: usize::try_from(row.try_get::<i64, _>("matched_log_count")?)
            .context("checkpoint matched log count overflowed usize")?,
        state_payload,
        flushed_events,
    })
}

async fn delete_checkpoint(
    pool: &PgPool,
    chain: &str,
    context: &ReplayAdapterCheckpointContext,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&context.deployment_profile)
    .bind(chain)
    .bind(&context.cursor_kind)
    .bind(ADAPTER)
    .bind(CHECKPOINT_SCOPE)
    .execute(pool)
    .await
    .context("failed to reset stale unwrapped-authority replay checkpoint")?;
    Ok(())
}

fn checkpoint_item_rows(
    state: &UnwrappedAuthorityReplayCheckpointStateRef<'_>,
    delta: &UnwrappedAuthorityReplayCheckpointDelta,
) -> Result<Vec<(&'static str, String, Value)>> {
    let mut rows = Vec::new();
    for key in &delta.history_keys {
        if let Some(value) = state.histories.get(key) {
            rows.push((ITEM_KIND_HISTORY, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.reverse_history_keys {
        if let Some(value) = state.reverse_histories.get(key) {
            rows.push((ITEM_KIND_REVERSE_HISTORY, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.known_name_keys {
        if let Some(value) = state.known_names_by_namehash.get(key) {
            rows.push((ITEM_KIND_KNOWN_NAME, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.known_name_ref_keys {
        if let Some(value) = state.known_name_refs_by_namehash.get(key) {
            rows.push((ITEM_KIND_KNOWN_NAME_REF, key.clone(), encode_item(value)?));
        }
    }
    for key in &delta.namehash_labelhash_keys {
        if let Some(labelhash) = state.namehash_to_labelhash.get(key) {
            rows.push((
                ITEM_KIND_NAMEHASH_LABELHASH,
                key.clone(),
                encode_checkpoint_payload(json!({ "labelhash": labelhash })),
            ));
        }
    }
    for key in &delta.pending_observation_keys {
        if let Some(observations) = state
            .pending_namehash_observations
            .get(key)
            .filter(|observations| !observations.is_empty())
        {
            rows.push((
                ITEM_KIND_PENDING_OBSERVATIONS,
                key.clone(),
                encode_item(observations.as_slice())?,
            ));
        }
    }
    for node in &delta.migrated_nodes {
        rows.push((
            ITEM_KIND_MIGRATED_NODE,
            node.clone(),
            encode_checkpoint_payload(json!({ "node": node })),
        ));
    }
    Ok(rows)
}

fn checkpoint_pending_observation_delete_keys(
    state: &UnwrappedAuthorityReplayCheckpointStateRef<'_>,
    delta: &UnwrappedAuthorityReplayCheckpointDelta,
) -> Vec<String> {
    delta
        .pending_observation_keys
        .iter()
        .filter(|key| {
            state
                .pending_namehash_observations
                .get(*key)
                .is_none_or(Vec::is_empty)
        })
        .cloned()
        .collect()
}

async fn delete_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    item_kind: &str,
    item_keys: &[String],
) -> Result<()> {
    for chunk in item_keys.chunks(CHECKPOINT_ITEM_DELETE_BATCH_SIZE) {
        if chunk.is_empty() {
            continue;
        }
        sqlx::query(
            r#"
            DELETE FROM normalized_replay_adapter_checkpoint_items
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND item_kind = $6
              AND item_key = ANY($7)
            "#,
        )
        .bind(&checkpoint.context.deployment_profile)
        .bind(&checkpoint.chain)
        .bind(&checkpoint.context.cursor_kind)
        .bind(ADAPTER)
        .bind(CHECKPOINT_SCOPE)
        .bind(item_kind)
        .bind(chunk)
        .execute(transaction.as_mut())
        .await
        .context("failed to delete cleared unwrapped-authority replay checkpoint items")?;
    }
    Ok(())
}

async fn insert_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    item_rows: &[(&'static str, String, Value)],
) -> Result<()> {
    for chunk in item_rows.chunks(CHECKPOINT_ITEM_INSERT_BATCH_SIZE) {
        if chunk.is_empty() {
            continue;
        }
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO normalized_replay_adapter_checkpoint_items (
                deployment_profile,
                chain_id,
                cursor_kind,
                adapter,
                checkpoint_scope,
                item_kind,
                item_key,
                item_payload
            )
            "#,
        );
        builder.push_values(
            chunk.iter(),
            |mut row, (item_kind, item_key, item_payload)| {
                row.push_bind(&checkpoint.context.deployment_profile)
                    .push_bind(&checkpoint.chain)
                    .push_bind(&checkpoint.context.cursor_kind)
                    .push_bind(ADAPTER)
                    .push_bind(CHECKPOINT_SCOPE)
                    .push_bind(*item_kind)
                    .push_bind(item_key)
                    .push_bind(item_payload);
            },
        );
        builder.push(
            r#"
            ON CONFLICT (
                deployment_profile,
                chain_id,
                cursor_kind,
                adapter,
                checkpoint_scope,
                item_kind,
                item_key
            ) DO UPDATE
            SET item_payload = EXCLUDED.item_payload,
                updated_at = now()
            "#,
        );
        builder
            .build()
            .execute(transaction.as_mut())
            .await
            .context("failed to upsert unwrapped-authority replay checkpoint items")?;
    }
    Ok(())
}

async fn update_checkpoint_progress(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    checkpoint: &UnwrappedAuthorityReplayCheckpoint,
    status: &str,
    last_block_number: Option<i64>,
    scanned_log_count: usize,
    matched_log_count: usize,
    staged_item_count: usize,
    staged_aux_item_count: usize,
    state_payload: Value,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE normalized_replay_adapter_checkpoints
        SET
            status = $6,
            last_block_number = $7,
            last_transaction_index = CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 0 END,
            last_log_index = CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 0 END,
            last_emitting_address = CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 'block-boundary' END,
            staged_item_count = $8,
            staged_aux_item_count = $9,
            scanned_log_count = $10,
            matched_log_count = $11,
            state_payload = $12,
            updated_at = now(),
            last_failure_reason = NULL
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(&checkpoint.context.deployment_profile)
    .bind(&checkpoint.chain)
    .bind(&checkpoint.context.cursor_kind)
    .bind(ADAPTER)
    .bind(CHECKPOINT_SCOPE)
    .bind(status)
    .bind(last_block_number)
    .bind(i64::try_from(staged_item_count).context("staged item count overflowed i64")?)
    .bind(i64::try_from(staged_aux_item_count).context("staged aux item count overflowed i64")?)
    .bind(i64::try_from(scanned_log_count).context("scanned log count overflowed i64")?)
    .bind(i64::try_from(matched_log_count).context("matched log count overflowed i64")?)
    .bind(state_payload)
    .execute(transaction.as_mut())
    .await
    .context("failed to update unwrapped-authority replay checkpoint progress")?;
    Ok(())
}

fn encode_item<T>(value: &T) -> Result<Value>
where
    T: serde::Serialize + ?Sized,
{
    let value = serde_json::to_value(value)
        .context("failed to encode unwrapped-authority checkpoint item")?;
    Ok(encode_checkpoint_payload(value))
}

fn decode_item<T>(value: Value, item_kind: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let value = decode_checkpoint_payload(value)?;
    serde_json::from_value(value).with_context(|| {
        format!("failed to decode unwrapped-authority checkpoint item {item_kind}")
    })
}

fn encode_checkpoint_payload(value: Value) -> Value {
    match value {
        Value::String(value) if value.contains('\0') => {
            let mut object = Map::new();
            object.insert(
                CHECKPOINT_ESCAPED_STRING_KEY.to_owned(),
                Value::String(hex_encode_utf8(&value)),
            );
            Value::Object(object)
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(encode_checkpoint_payload).collect())
        }
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, encode_checkpoint_payload(value)))
                .collect(),
        ),
        value => value,
    }
}

fn decode_checkpoint_payload(value: Value) -> Result<Value> {
    match value {
        Value::Object(mut object) => {
            if object.len() == 1 {
                match object.remove(CHECKPOINT_ESCAPED_STRING_KEY) {
                    Some(Value::String(encoded)) => {
                        return Ok(Value::String(hex_decode_utf8(&encoded)?));
                    }
                    Some(_) => bail!(
                        "unwrapped-authority checkpoint escaped string payload is not a string"
                    ),
                    None => {}
                }
            }
            let object = object
                .into_iter()
                .map(|(key, value)| Ok((key, decode_checkpoint_payload(value)?)))
                .collect::<Result<Map<_, _>>>()?;
            Ok(Value::Object(object))
        }
        Value::Array(values) => {
            let values = values
                .into_iter()
                .map(decode_checkpoint_payload)
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::Array(values))
        }
        value => Ok(value),
    }
}

fn hex_encode_utf8(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode_utf8(value: &str) -> Result<String> {
    if value.len() % 2 != 0 {
        bail!("unwrapped-authority checkpoint escaped string hex has odd length");
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    String::from_utf8(bytes)
        .context("unwrapped-authority checkpoint escaped string is not valid utf-8")
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("unwrapped-authority checkpoint escaped string hex is invalid"),
    }
}

fn summary_payload(summary: &EnsV1UnwrappedAuthoritySyncSummary) -> Value {
    json!({
        "scanned_log_count": summary.scanned_log_count,
        "matched_log_count": summary.matched_log_count,
        "total_name_surface_count": summary.total_name_surface_count,
        "total_resource_count": summary.total_resource_count,
        "total_surface_binding_count": summary.total_surface_binding_count,
        "total_normalized_event_count": summary.total_normalized_event_count,
        "total_normalized_event_inserted_count": summary.total_normalized_event_inserted_count,
        "by_kind": summary.by_kind,
    })
}

fn summary_from_payload(payload: &Value) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count: usize_field(payload, "scanned_log_count")?,
        matched_log_count: usize_field(payload, "matched_log_count")?,
        total_name_surface_count: usize_field(payload, "total_name_surface_count")?,
        total_resource_count: usize_field(payload, "total_resource_count")?,
        total_surface_binding_count: usize_field(payload, "total_surface_binding_count")?,
        total_normalized_event_count: usize_field(payload, "total_normalized_event_count")?,
        total_normalized_event_inserted_count: usize_field(
            payload,
            "total_normalized_event_inserted_count",
        )?,
        by_kind: serde_json::from_value(
            payload.get("by_kind").cloned().unwrap_or_else(|| json!({})),
        )
        .context("checkpoint summary by_kind is invalid")?,
    })
}

fn flushed_events_from_payload(payload: &Value) -> Result<UnwrappedAuthorityReplayFlushedEvents> {
    Ok(UnwrappedAuthorityReplayFlushedEvents {
        total_count: optional_usize_field(payload, "flushed_normalized_event_count")?,
        inserted_count: optional_usize_field(payload, "flushed_normalized_event_inserted_count")?,
        by_kind: payload
            .get("flushed_by_kind")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .context("checkpoint flushed_by_kind is invalid")?
            .unwrap_or_default(),
    })
}

fn usize_field(payload: &Value, field: &str) -> Result<usize> {
    let value = payload
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("checkpoint summary is missing i64 field {field}"))?;
    usize::try_from(value)
        .with_context(|| format!("checkpoint summary field {field} overflows usize"))
}

fn optional_usize_field(payload: &Value, field: &str) -> Result<usize> {
    let Some(value) = payload.get(field).and_then(Value::as_i64) else {
        return Ok(0);
    };
    usize::try_from(value).with_context(|| format!("checkpoint field {field} overflows usize"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_payload_encoding_round_trips_nul_strings() -> Result<()> {
        let payload = json!({
            "record": "before\0after",
            "nested": [
                { "raw_name": "name\0with\0nuls" },
                "plain"
            ]
        });

        let encoded = encode_checkpoint_payload(payload.clone());
        let encoded_json = serde_json::to_string(&encoded)?;

        assert_ne!(encoded, payload);
        assert!(!encoded_json.contains("\\u0000"));
        assert_eq!(decode_checkpoint_payload(encoded)?, payload);
        Ok(())
    }

    #[test]
    fn checkpoint_payload_encoding_leaves_jsonb_safe_strings_plain() -> Result<()> {
        let payload = json!({
            "record": "ordinary text",
            "nested": ["also ordinary"]
        });

        let encoded = encode_checkpoint_payload(payload.clone());

        assert_eq!(encoded, payload);
        assert_eq!(decode_checkpoint_payload(encoded)?, payload);
        Ok(())
    }

    #[test]
    fn checkpoint_item_rows_prune_empty_pending_observations() -> Result<()> {
        let histories = BTreeMap::<String, NameHistory>::new();
        let reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
        let known_names_by_namehash = HashMap::<String, NameMetadata>::new();
        let known_name_refs_by_namehash = HashMap::<String, ObservationRef>::new();
        let namehash_to_labelhash = HashMap::<String, String>::new();
        let mut pending_namehash_observations = HashMap::<String, Vec<AuthorityObservation>>::new();
        pending_namehash_observations.insert("cleared".to_owned(), Vec::new());
        let migrated_registry_nodes = MigratedRegistryNodes::empty();
        let state = UnwrappedAuthorityReplayCheckpointStateRef {
            histories: &histories,
            reverse_histories: &reverse_histories,
            known_names_by_namehash: &known_names_by_namehash,
            known_name_refs_by_namehash: &known_name_refs_by_namehash,
            namehash_to_labelhash: &namehash_to_labelhash,
            pending_namehash_observations: &pending_namehash_observations,
            migrated_registry_nodes: &migrated_registry_nodes,
        };
        let mut delta = UnwrappedAuthorityReplayCheckpointDelta::default();
        delta.mark_pending_observations("cleared");
        delta.mark_pending_observations("missing");

        let rows = checkpoint_item_rows(&state, &delta)?;
        let delete_keys = checkpoint_pending_observation_delete_keys(&state, &delta);

        assert!(
            rows.iter()
                .all(|(item_kind, _, _)| *item_kind != ITEM_KIND_PENDING_OBSERVATIONS)
        );
        assert_eq!(
            delete_keys,
            vec!["cleared".to_owned(), "missing".to_owned()]
        );
        Ok(())
    }
}
