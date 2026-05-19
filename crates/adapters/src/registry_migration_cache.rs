use std::{collections::HashSet, sync::Arc};

#[cfg(not(test))]
use std::{collections::HashMap, sync::OnceLock};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RegistryMigrationMarkerEmitter {
    address: String,
    effective_from_block: i64,
    effective_to_block: i64,
}

impl RegistryMigrationMarkerEmitter {
    pub(crate) fn new(
        address: impl Into<String>,
        effective_from_block: i64,
        effective_to_block: i64,
    ) -> Self {
        Self {
            address: address.into().to_ascii_lowercase(),
            effective_from_block,
            effective_to_block,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MigratedRegistryNodes {
    baseline: Arc<HashSet<String>>,
    delta: HashSet<String>,
}

impl MigratedRegistryNodes {
    pub(crate) fn empty() -> Self {
        Self {
            baseline: Arc::new(HashSet::new()),
            delta: HashSet::new(),
        }
    }

    pub(crate) fn from_delta(delta: HashSet<String>) -> Self {
        Self {
            baseline: Arc::new(HashSet::new()),
            delta,
        }
    }

    fn from_baseline(baseline: HashSet<String>) -> Self {
        Self {
            baseline: Arc::new(baseline),
            delta: HashSet::new(),
        }
    }

    pub(crate) fn contains(&self, node: &str) -> bool {
        self.delta.contains(node) || self.baseline.contains(node)
    }

    pub(crate) fn insert(&mut self, node: String) -> bool {
        self.delta.insert(node)
    }

    pub(crate) fn delta_nodes(&self) -> impl Iterator<Item = &String> {
        self.delta.iter()
    }
}

#[cfg(not(test))]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RegistryMigrationMarkerCacheKey {
    chain: String,
    marker_topic0: String,
    emitters: Vec<RegistryMigrationMarkerEmitter>,
}

#[cfg(not(test))]
#[derive(Debug)]
struct RegistryMigrationMarkerCacheEntry {
    loaded_before_block: i64,
    nodes: HashSet<String>,
}

#[cfg(not(test))]
static REGISTRY_MIGRATION_MARKER_CACHE: OnceLock<
    tokio::sync::Mutex<HashMap<RegistryMigrationMarkerCacheKey, RegistryMigrationMarkerCacheEntry>>,
> = OnceLock::new();

#[cfg(not(test))]
fn registry_migration_marker_cache() -> &'static tokio::sync::Mutex<
    HashMap<RegistryMigrationMarkerCacheKey, RegistryMigrationMarkerCacheEntry>,
> {
    REGISTRY_MIGRATION_MARKER_CACHE.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

pub(crate) async fn load_migrated_registry_nodes_before_block<F>(
    pool: &PgPool,
    chain: &str,
    emitters: &[RegistryMigrationMarkerEmitter],
    before_block: i64,
    marker_topic0: &str,
    decode_node: F,
) -> Result<MigratedRegistryNodes>
where
    F: Fn(&[String]) -> Result<String>,
{
    let mut emitters = emitters.to_vec();
    emitters.sort();
    emitters.dedup();
    if emitters.is_empty() {
        return Ok(MigratedRegistryNodes::empty());
    }

    #[cfg(test)]
    {
        let nodes = load_marker_nodes_between(
            pool,
            chain,
            &emitters,
            0,
            before_block,
            marker_topic0,
            &decode_node,
        )
        .await?;
        return Ok(MigratedRegistryNodes::from_baseline(nodes));
    }

    #[cfg(not(test))]
    {
        let key = RegistryMigrationMarkerCacheKey {
            chain: chain.to_owned(),
            marker_topic0: marker_topic0.to_ascii_lowercase(),
            emitters,
        };
        let key_emitters = key.emitters.clone();
        let mut cache = registry_migration_marker_cache().lock().await;
        let entry = cache
            .entry(key)
            .or_insert_with(|| RegistryMigrationMarkerCacheEntry {
                loaded_before_block: 0,
                nodes: HashSet::new(),
            });

        if before_block > entry.loaded_before_block {
            let loaded_before_block = entry.loaded_before_block;
            let nodes = load_marker_nodes_between(
                pool,
                chain,
                &key_emitters,
                loaded_before_block,
                before_block,
                marker_topic0,
                &decode_node,
            )
            .await?;
            if !nodes.is_empty() {
                entry.nodes.extend(nodes);
            }
            entry.loaded_before_block = before_block;
        }

        return Ok(MigratedRegistryNodes::from_baseline(entry.nodes.clone()));
    }
}

async fn load_marker_nodes_between<F>(
    pool: &PgPool,
    chain: &str,
    emitters: &[RegistryMigrationMarkerEmitter],
    from_block: i64,
    before_block: i64,
    marker_topic0: &str,
    decode_node: &F,
) -> Result<HashSet<String>>
where
    F: Fn(&[String]) -> Result<String>,
{
    if from_block >= before_block {
        return Ok(HashSet::new());
    }

    let addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = emitters
        .iter()
        .map(|emitter| emitter.effective_from_block)
        .collect::<Vec<_>>();
    let to_blocks = emitters
        .iter()
        .map(|emitter| emitter.effective_to_block)
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT topics
        FROM raw_logs
        WHERE chain_id = $1
          AND LOWER(emitting_address) = ANY($2::TEXT[])
          AND block_number >= $7
          AND block_number < $3
          AND topics[1] = $4
          AND EXISTS (
              SELECT 1
              FROM unnest($2::TEXT[], $5::BIGINT[], $6::BIGINT[]) AS watched(
                  address,
                  effective_from_block,
                  effective_to_block
              )
              WHERE watched.address = LOWER(emitting_address)
                AND block_number BETWEEN watched.effective_from_block
                    AND watched.effective_to_block
          )
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(&addresses)
    .bind(before_block)
    .bind(marker_topic0)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(from_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load ENSv1 registry migration markers for blocks {from_block}..{before_block}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let topics = row
                .try_get::<Vec<String>, _>("topics")
                .context("missing topics")?;
            decode_node(&topics)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrated_registry_nodes_snapshots_do_not_learn_later_cache_nodes() {
        let early = MigratedRegistryNodes::from_baseline(HashSet::from(["0x01".to_owned()]));
        let later = MigratedRegistryNodes::from_baseline(HashSet::from([
            "0x01".to_owned(),
            "0x02".to_owned(),
        ]));

        assert!(early.contains("0x01"));
        assert!(!early.contains("0x02"));
        assert!(later.contains("0x02"));
    }
}
