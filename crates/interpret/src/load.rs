use bigname_adapters::SchemaV2AdapterSession;
use bigname_adapters::schema_v2::seam::{ADMISSION_DISCOVERY_EDGE_KINDS, OBSERVATION_KEY};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity,
};
use sqlx::{PgConnection, PgPool, types::Uuid};

use crate::{InterpretError, Result};

mod cache;
mod manifests;
mod migration;
mod prior;
mod resume;

pub(crate) use cache::{PriorCache, fold as fold_prior_cache};
pub(crate) use prior::prior_state_values;

pub(crate) struct CachedPrior {
    pub cache: PriorCache,
    pub adapter_session: SchemaV2AdapterSession,
}

pub(crate) struct LoadedBatch {
    pub input: BatchInput,
    pub provenance_manifests: Vec<ManifestInput>,
    pub prior_cache: PriorCache,
    pub adapter_session: Option<SchemaV2AdapterSession>,
    pub restored_event_count: usize,
}

type RawLogRow = (
    String,
    String,
    i64,
    time::OffsetDateTime,
    String,
    String,
    i64,
    i64,
    String,
    Vec<String>,
    Vec<u8>,
);

pub(crate) async fn batch_input(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    resume_marker: Option<(i64, &str)>,
    cached_prior: Option<CachedPrior>,
    state_cache_capacity: StateCacheCapacity,
) -> Result<LoadedBatch> {
    let mut transaction = pool.begin().await.map_err(|error| {
        InterpretError::database("failed to begin interpret input snapshot", error)
    })?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to configure interpret input snapshot", error)
        })?;
    validate_snapshot_resume_marker(&mut transaction, chain_id, resume_marker).await?;
    let orphaning_epoch = cache::orphaning_epoch(&mut transaction, chain_id).await?;
    let cached = match cached_prior {
        Some(CachedPrior {
            cache: prior_cache,
            adapter_session,
        }) => {
            match cache::revalidate(&mut transaction, chain_id, prior_cache, orphaning_epoch)
                .await?
            {
                Some(prior_cache) => Some((prior_cache, adapter_session)),
                None => {
                    drop(adapter_session);
                    None
                }
            }
        }
        None => None,
    };
    let (manifests, provenance_manifests) = manifests::load(&mut transaction, chain_id).await?;
    if manifests.is_empty() {
        return Err(InterpretError::configuration(format!(
            "chain {chain_id} has no active manifests for interpretation"
        )));
    }
    let discovery_rules = load_discovery_rules(&mut transaction, chain_id).await?;
    let mut admissions = load_admissions(&mut transaction, chain_id, from_block).await?;
    admissions.extend(migration::admissions(&mut transaction, chain_id, from_block).await?);
    let blocks = load_blocks(&mut transaction, chain_id, from_block, to_block).await?;
    let raw_logs = load_raw_logs(&mut transaction, chain_id, from_block, to_block).await?;
    let (prior_cache, adapter_session, restored_event_count) = match cached {
        Some((prior_cache, adapter_session)) => (prior_cache, adapter_session, 0),
        None => {
            let mut restore = bigname_adapters::begin_schema_v2_adapter_restore_with_provenance(
                chain_id.to_owned(),
                manifests.clone(),
                provenance_manifests.clone(),
                discovery_rules.clone(),
                admissions.clone(),
                state_cache_capacity,
            )
            .map_err(|error| {
                InterpretError::data_integrity(format!(
                    "failed to begin adapter state restore: {error:#}"
                ))
            })?;
            let restored_event_count =
                prior::restore_events(&mut transaction, chain_id, from_block, &mut restore).await?;
            let adapter_session =
                resume::finish_restore(&mut transaction, chain_id, from_block, restore).await?;
            (
                cache::freshly_loaded(orphaning_epoch),
                adapter_session,
                restored_event_count,
            )
        }
    };
    transaction.commit().await.map_err(|error| {
        InterpretError::database("failed to commit interpret input snapshot", error)
    })?;
    Ok(LoadedBatch {
        input: BatchInput {
            chain_id: chain_id.to_owned(),
            discovery_rules,
            admissions,
            prior_events: Vec::new(),
            blocks,
            raw_logs,
            manifests,
        },
        provenance_manifests,
        prior_cache,
        adapter_session: Some(adapter_session),
        restored_event_count,
    })
}

async fn validate_snapshot_resume_marker(
    connection: &mut PgConnection,
    chain_id: &str,
    expected: Option<(i64, &str)>,
) -> Result<()> {
    let Some((block_number, block_hash)) = expected else {
        return Ok(());
    };
    let live: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_hash
        ",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| {
        InterpretError::database("failed to validate resume marker in input snapshot", error)
    })?;
    require_unique_live_heights(
        chain_id,
        live.iter().map(|(number, hash)| (*number, hash.as_str())),
    )?;
    if live.as_slice() != [(block_number, block_hash.to_owned())] {
        return Err(InterpretError::transient(format!(
            "interpret resume marker {block_number} {block_hash} changed before the input snapshot for chain {chain_id}"
        )));
    }
    Ok(())
}

async fn load_blocks(
    connection: &mut PgConnection,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<RawBlockInput>> {
    let rows: Vec<(String, String, i64, time::OffsetDateTime, String)> = sqlx::query_as(
        "
        SELECT chain_id, block_hash, block_number, block_timestamp,
               canonicality_state::text
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_number, block_hash
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical block facts", error))?;
    require_unique_live_heights(
        chain_id,
        rows.iter()
            .map(|(_, hash, number, _, _)| (*number, hash.as_str())),
    )?;
    Ok(rows
        .into_iter()
        .map(
            |(chain_id, block_hash, block_number, block_timestamp, canonicality_state)| {
                RawBlockInput {
                    chain_id,
                    block_hash,
                    block_number,
                    block_timestamp,
                    canonicality_state,
                }
            },
        )
        .collect())
}

pub(crate) async fn canonical_markers(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    limit: i64,
) -> Result<Vec<(i64, String)>> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_number, block_hash
        LIMIT $4
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical batch markers", error))?;
    require_unique_live_heights(
        chain_id,
        rows.iter().map(|(number, hash)| (*number, hash.as_str())),
    )?;
    Ok(rows)
}

pub(crate) async fn marker(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
) -> Result<Option<(i64, String)>> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_all(pool)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical target marker", error))?;
    require_unique_live_heights(
        chain_id,
        rows.iter().map(|(number, hash)| (*number, hash.as_str())),
    )?;
    Ok(rows.into_iter().next())
}

fn require_unique_live_heights<'a>(
    chain_id: &str,
    rows: impl IntoIterator<Item = (i64, &'a str)>,
) -> Result<()> {
    let mut hashes = std::collections::BTreeMap::new();
    for (number, hash) in rows {
        if let Some(existing) = hashes.insert(number, hash)
            && existing != hash
        {
            return Err(InterpretError::data_integrity(format!(
                "chain {chain_id} has multiple live-lineage hashes at block {number}: {existing} and {hash}"
            )));
        }
    }
    Ok(())
}

async fn load_discovery_rules(
    connection: &mut PgConnection,
    chain_id: &str,
) -> Result<Vec<DiscoveryRuleInput>> {
    let rows: Vec<(i64, String, Option<String>, String)> = sqlx::query_as(
        "
        SELECT rule.manifest_id, rule.edge_kind, rule.from_role, rule.admission
        FROM manifest_discovery_rules rule
        JOIN manifest_versions manifest
          ON manifest.manifest_id = rule.manifest_id
        WHERE manifest.chain_id = $1
          AND manifest.rollout_status = 'active'
        ORDER BY rule.manifest_id, rule.manifest_discovery_rule_id
        ",
    )
    .bind(chain_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| InterpretError::database("failed to load manifest discovery rules", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(manifest_id, edge_kind, from_role, admission)| DiscoveryRuleInput {
                manifest_id,
                edge_kind,
                from_role,
                admission,
            },
        )
        .collect())
}

#[allow(clippy::type_complexity)]
async fn load_admissions(
    connection: &mut PgConnection,
    chain_id: &str,
    before_block: i64,
) -> Result<Vec<AddressAdmissionInput>> {
    let statement = format!(
        "
        WITH declared AS (
            SELECT lower(address.address) AS address,
                   address.contract_instance_id,
                   manifest.manifest_id AS source_manifest_id,
                   COALESCE(declaration.role, declaration.declaration_name) AS role,
                   NULL::text AS edge_kind,
                   NULL::uuid AS from_contract_instance_id,
                   NULL::text AS observation_key,
                   GREATEST(
                       COALESCE(declaration.start_block_number, 0),
                       COALESCE(address.active_from_block_number, 0)
                   ) AS active_from,
                   address.active_to_block_number AS active_to
            FROM manifest_versions manifest
            JOIN manifest_contract_instances declaration
              ON declaration.manifest_id = manifest.manifest_id
             AND declaration.chain_id = manifest.chain_id
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = declaration.contract_instance_id
             AND address.chain_id = declaration.chain_id
            WHERE manifest.chain_id = $1
              AND manifest.rollout_status = 'active'
              AND (
                  address.deactivated_at IS NULL
                  OR address.active_to_block_number IS NOT NULL
              )
        ),
        discovered AS (
            SELECT lower(address.address) AS address,
                   address.contract_instance_id,
                   edge.source_manifest_id,
                   NULL::text AS role,
                   edge.edge_kind,
                   edge.from_contract_instance_id,
                   edge.provenance ->> '{OBSERVATION_KEY}' AS observation_key,
                   GREATEST(
                       COALESCE(edge.active_from_block_number, 0),
                       COALESCE(address.active_from_block_number, 0)
                   ) AS active_from,
                   CASE
                       WHEN edge.active_to_block_number IS NULL
                           THEN address.active_to_block_number
                       WHEN address.active_to_block_number IS NULL
                           THEN edge.active_to_block_number
                       ELSE LEAST(
                           edge.active_to_block_number,
                           address.active_to_block_number
                       )
                   END AS active_to
            FROM discovery_edges edge
            JOIN manifest_versions manifest
              ON manifest.manifest_id = edge.source_manifest_id
             AND manifest.chain_id = edge.chain_id
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = edge.to_contract_instance_id
             AND address.chain_id = edge.chain_id
            WHERE edge.chain_id = $1
              AND manifest.rollout_status = 'active'
              AND edge.edge_kind = ANY($3::text[])
              AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND COALESCE(edge.active_from_block_number, 0) < $2
              AND (
                  edge.deactivated_at IS NULL
                  OR edge.active_to_block_number IS NOT NULL
              )
        )
        SELECT address,
               contract_instance_id,
               source_manifest_id,
               role,
               edge_kind,
               from_contract_instance_id,
               observation_key,
               active_from,
               active_to
        FROM declared
        UNION ALL
        SELECT address,
               contract_instance_id,
               source_manifest_id,
               role,
               edge_kind,
               from_contract_instance_id,
               observation_key,
               active_from,
               active_to
        FROM discovered
        ORDER BY address, contract_instance_id, edge_kind NULLS FIRST
        "
    );
    let rows: Vec<(
        String,
        Uuid,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<Uuid>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    )> = sqlx::query_as(&statement)
        .bind(chain_id)
        .bind(before_block)
        .bind(
            ADMISSION_DISCOVERY_EDGE_KINDS
                .iter()
                .map(|kind| (*kind).to_owned())
                .collect::<Vec<_>>(),
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            InterpretError::database("failed to load admitted contract ranges", error)
        })?;
    Ok(rows
        .into_iter()
        .map(
            |(
                address,
                contract_instance_id,
                source_manifest_id,
                role,
                discovery_edge_kind,
                discovery_from_contract_instance_id,
                discovery_observation_key,
                active_from_block,
                active_to_block,
            )| AddressAdmissionInput {
                address,
                contract_instance_id,
                source_manifest_id,
                role,
                discovery_edge_kind,
                discovery_from_contract_instance_id,
                discovery_observation_key,
                active_from_block,
                active_to_block,
            },
        )
        .collect())
}

async fn load_raw_logs(
    connection: &mut PgConnection,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<RawLogInput>> {
    let rows: Vec<RawLogRow> = sqlx::query_as(
        "
        SELECT raw.chain_id,
               raw.block_hash,
               raw.block_number,
               lineage.block_timestamp,
               lineage.canonicality_state::text,
               raw.transaction_hash,
               raw.transaction_index,
               raw.log_index,
               lower(raw.emitting_address),
               raw.topics,
               raw.data
        FROM raw_logs raw
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw.chain_id
         AND lineage.block_hash = raw.block_hash
         AND lineage.block_number = raw.block_number
        WHERE raw.chain_id = $1
          AND raw.block_number BETWEEN $2 AND $3
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY raw.block_number,
                 raw.transaction_index,
                 raw.log_index,
                 raw.block_hash
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| InterpretError::database("failed to load canonical raw logs", error))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
            )| RawLogInput {
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
            },
        )
        .collect())
}
