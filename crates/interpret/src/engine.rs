use std::{collections::HashMap, sync::Mutex, time::Instant};

use bigname_adapters::{SchemaV2AdapterSession, StateCacheCapacity};
use sqlx::PgPool;

use crate::{InterpretError, Result, load, recompute, write};

const CANONICAL_BLOCKS_PER_BATCH: i64 = 500;
pub const DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES: usize = 65_536;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Marker {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RunMode {
    Normal,
    Redo,
    RecomputeFlags,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchRequest {
    pub chain_id: String,
    pub from_block: i64,
    pub to_block: i64,
    pub resume_current: Option<Marker>,
    pub mode: RunMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchOutcome {
    pub current: Marker,
    pub target: Marker,
    pub complete: bool,
    pub estimated_write_bytes: u64,
}

pub struct Engine {
    pool: PgPool,
    state_cache_capacity: StateCacheCapacity,
    prior_sessions: Mutex<HashMap<String, PriorSession>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SessionKey {
    chain_id: String,
    from_block: i64,
    mode: RunMode,
}

struct PriorSession {
    key: SessionKey,
    next_block: i64,
    cache: load::PriorCache,
    adapter_session: SchemaV2AdapterSession,
}

impl Engine {
    pub fn new(pool: PgPool) -> Self {
        Self::with_state_cache_capacity(pool, DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES)
    }

    pub fn with_state_cache_capacity(pool: PgPool, entries: usize) -> Self {
        Self {
            pool,
            state_cache_capacity: StateCacheCapacity::Entries(entries),
            prior_sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run_batch(&self, request: BatchRequest) -> Result<BatchOutcome> {
        let profile = std::env::var_os("BIGNAME_INTERPRET_FOLD_PROFILE").is_some();
        let batch_started = Instant::now();
        validate_request(&request)?;
        let target = load::marker(&self.pool, &request.chain_id, request.to_block)
            .await?
            .map(|(number, hash)| Marker { number, hash })
            .ok_or_else(|| {
                InterpretError::data_integrity(format!(
                    "interpret target block {} for chain {} is not canonical",
                    request.to_block, request.chain_id
                ))
            })?;
        validate_resume(&self.pool, &request, &target).await?;
        let next_block = request
            .resume_current
            .as_ref()
            .map_or(request.from_block, |marker| marker.number.saturating_add(1));
        if next_block > target.number {
            if !matches!(request.mode, RunMode::RecomputeFlags) {
                write::discovery_admission::finalize_empty_completion(
                    &self.pool,
                    &request.chain_id,
                )
                .await?;
            }
            return Ok(BatchOutcome {
                current: target.clone(),
                target,
                complete: true,
                estimated_write_bytes: 0,
            });
        }
        if matches!(request.mode, RunMode::RecomputeFlags) {
            let estimated_write_bytes = recompute::run(
                &self.pool,
                &request.chain_id,
                request.from_block,
                request.to_block,
            )
            .await?;
            return Ok(BatchOutcome {
                current: target.clone(),
                target,
                complete: true,
                estimated_write_bytes,
            });
        }
        let markers = load::canonical_markers(
            &self.pool,
            &request.chain_id,
            next_block,
            target.number,
            CANONICAL_BLOCKS_PER_BATCH,
        )
        .await?;
        validate_contiguous_markers(&request.chain_id, next_block, &markers)?;
        let Some((batch_from, _)) = markers.first() else {
            return Err(InterpretError::data_integrity(format!(
                "interpret range {next_block}..={} for chain {} has no canonical lineage",
                target.number, request.chain_id
            )));
        };
        let (batch_to, batch_hash) = markers.last().expect("non-empty markers");
        let session_key = SessionKey {
            chain_id: request.chain_id.clone(),
            from_block: request.from_block,
            mode: request.mode,
        };
        let phase_started = Instant::now();
        let cached_prior =
            self.take_prior_session(&session_key, *batch_from, request.resume_current.is_some())?;
        profile_phase(profile, "take_prior_session", phase_started, None);
        let phase_started = Instant::now();
        let loaded = load::batch_input(
            &self.pool,
            &request.chain_id,
            *batch_from,
            *batch_to,
            request
                .resume_current
                .as_ref()
                .map(|marker| (marker.number, marker.hash.as_str())),
            cached_prior,
            self.state_cache_capacity,
        )
        .await?;
        let restored_event_count = loaded.restored_event_count;
        profile_phase(
            profile,
            "batch_input",
            phase_started,
            Some(restored_event_count),
        );
        let prior_cache = loaded.prior_cache;
        let expected_orphaning_epoch = prior_cache.validated_orphaning_epoch;
        let adapter_session = loaded.adapter_session;
        let provenance_manifests = loaded.provenance_manifests;
        let input = loaded.input;
        let loaded_markers = input
            .blocks
            .iter()
            .map(|block| (block.block_number, block.block_hash.clone()))
            .collect::<Vec<_>>();
        validate_loaded_lineage(&request.chain_id, &markers, &loaded_markers)?;
        let mut write_lineage = Vec::with_capacity(
            loaded_markers.len() + usize::from(request.resume_current.is_some()),
        );
        if let Some(resume) = &request.resume_current {
            write_lineage.push((resume.number, resume.hash.clone()));
        }
        write_lineage.extend(loaded_markers.iter().cloned());
        let phase_started = Instant::now();
        let prepared = bigname_adapters::prepare_schema_v2_batch_incremental_with_provenance(
            input,
            provenance_manifests,
            adapter_session,
            self.state_cache_capacity,
        )
        .map_err(|error| {
            InterpretError::data_integrity(format!(
                "hash-covered adapter interpretation failed: {error:#}"
            ))
        })?;
        let state_values = load::prior_state_values(
            &self.pool,
            &request.chain_id,
            *batch_from,
            prepared.state_value_requests(),
        )
        .await?;
        let (output, adapter_session) = prepared.finish(state_values).map_err(|error| {
            InterpretError::data_integrity(format!(
                "hash-covered adapter state reload failed: {error:#}"
            ))
        })?;
        profile_phase(
            profile,
            "adapter_restore_and_batch",
            phase_started,
            Some(restored_event_count),
        );
        let phase_started = Instant::now();
        let next_prior_cache = load::fold_prior_cache(prior_cache, &output.normalized_events);
        let retained_state_count = next_prior_cache.pending_dependencies.len();
        profile_phase(
            profile,
            "fold_prior_cache_delta",
            phase_started,
            Some(retained_state_count),
        );
        let complete = *batch_to == target.number;
        let redo_range =
            matches!(request.mode, RunMode::Redo).then_some((request.from_block, request.to_block));
        let prepare_redo_range = redo_range.is_some() && request.resume_current.is_none();
        let phase_started = Instant::now();
        let estimated_write_bytes = write::batch(
            &self.pool,
            &request.chain_id,
            redo_range,
            prepare_redo_range,
            complete,
            expected_orphaning_epoch,
            &write_lineage,
            &output,
        )
        .await?;
        profile_phase(profile, "write_batch", phase_started, None);
        let phase_started = Instant::now();
        self.store_prior_session(
            session_key,
            batch_to.saturating_add(1),
            next_prior_cache,
            adapter_session,
            complete,
        )?;
        profile_phase(
            profile,
            "store_prior_session",
            phase_started,
            Some(retained_state_count),
        );
        let current = Marker {
            number: *batch_to,
            hash: batch_hash.clone(),
        };
        profile_phase(profile, "batch_total", batch_started, None);
        Ok(BatchOutcome {
            complete,
            current,
            target,
            estimated_write_bytes,
        })
    }

    fn take_prior_session(
        &self,
        key: &SessionKey,
        next_block: i64,
        allow_resume: bool,
    ) -> Result<Option<load::CachedPrior>> {
        let mut sessions = self.prior_sessions.lock().map_err(|_| {
            InterpretError::transient("interpret prior-state session lock was poisoned")
        })?;
        // Moving the value out prevents a retained copy from overlapping the active batch and
        // makes the chain ID itself the one-session ownership boundary.
        let session = sessions.remove(&key.chain_id);
        Ok(session
            .filter(|session| {
                allow_resume && session.key == *key && session.next_block == next_block
            })
            .map(|session| load::CachedPrior {
                cache: session.cache,
                adapter_session: session.adapter_session,
            }))
    }

    fn store_prior_session(
        &self,
        key: SessionKey,
        next_block: i64,
        cache: load::PriorCache,
        adapter_session: SchemaV2AdapterSession,
        complete: bool,
    ) -> Result<()> {
        let mut sessions = self.prior_sessions.lock().map_err(|_| {
            InterpretError::transient("interpret prior-state session lock was poisoned")
        })?;
        let chain_id = key.chain_id.clone();
        let session = (!(matches!(key.mode, RunMode::Redo) && complete)).then_some(PriorSession {
            key,
            next_block,
            cache,
            adapter_session,
        });
        update_prior_sessions(&mut sessions, chain_id, session);
        Ok(())
    }
}

fn profile_phase(enabled: bool, phase: &str, started: Instant, retained_events: Option<usize>) {
    if !enabled {
        return;
    }
    let rss_kib = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0);
    eprintln!(
        "interpret-fold-profile phase={phase} elapsed_ms={} retained_events={} rss_kib={rss_kib}",
        started.elapsed().as_millis(),
        retained_events.unwrap_or(0),
    );
}

fn update_prior_sessions<T>(
    sessions: &mut HashMap<String, T>,
    chain_id: String,
    session: Option<T>,
) {
    if let Some(session) = session {
        sessions.insert(chain_id, session);
    } else {
        sessions.remove(&chain_id);
    }
}

fn validate_loaded_lineage(
    chain_id: &str,
    selected: &[(i64, String)],
    loaded: &[(i64, String)],
) -> Result<()> {
    if loaded != selected {
        return Err(InterpretError::transient(format!(
            "interpret batch lineage changed while loading raw facts for chain {chain_id}; retry with a fresh canonical batch"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "engine/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "engine/activation_tests.rs"]
mod activation_tests;

fn validate_contiguous_markers(
    chain_id: &str,
    expected_from: i64,
    markers: &[(i64, String)],
) -> Result<()> {
    for (offset, (actual, _)) in markers.iter().enumerate() {
        let expected = expected_from.saturating_add(i64::try_from(offset).unwrap_or(i64::MAX));
        if *actual != expected {
            return Err(InterpretError::data_integrity(format!(
                "interpret canonical lineage for chain {chain_id} has a gap at block {expected}"
            )));
        }
    }
    Ok(())
}

fn validate_request(request: &BatchRequest) -> Result<()> {
    if request.chain_id.trim().is_empty() {
        return Err(InterpretError::configuration(
            "interpret chain ID must not be empty",
        ));
    }
    if request.from_block < 0 || request.to_block < request.from_block {
        return Err(InterpretError::configuration(format!(
            "invalid interpret range {}..={}",
            request.from_block, request.to_block
        )));
    }
    Ok(())
}

async fn validate_resume(pool: &PgPool, request: &BatchRequest, target: &Marker) -> Result<()> {
    let Some(resume) = &request.resume_current else {
        return Ok(());
    };
    if resume.number > target.number {
        return Err(InterpretError::data_integrity(format!(
            "interpret resume block {} is above target block {}",
            resume.number, target.number
        )));
    }
    let canonical = load::marker(pool, &request.chain_id, resume.number).await?;
    if canonical.as_ref().map(|(_, hash)| hash) != Some(&resume.hash) {
        return Err(InterpretError::data_integrity(format!(
            "interpret resume marker {} {} is no longer canonical",
            resume.number, resume.hash
        )));
    }
    Ok(())
}
