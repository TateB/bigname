use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[cfg(not(test))]
use std::sync::OnceLock;

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RegistryMigrationMarkerCacheKey {
    chain: String,
    marker_topic0: String,
    emitters: Vec<RegistryMigrationMarkerEmitter>,
}

#[derive(Debug)]
struct RegistryMigrationMarkerCacheEntry {
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
        Ok(MigratedRegistryNodes::from_baseline(nodes))
    }

    #[cfg(not(test))]
    {
        let key = RegistryMigrationMarkerCacheKey {
            chain: chain.to_owned(),
            marker_topic0: marker_topic0.to_ascii_lowercase(),
            emitters,
        };
        let mut cache = registry_migration_marker_cache().lock().await;
        return load_migrated_registry_nodes_before_block_with_cache(
            pool,
            chain,
            key,
            before_block,
            marker_topic0,
            &decode_node,
            &mut cache,
        )
        .await;
    }
}

async fn load_migrated_registry_nodes_before_block_with_cache<F>(
    pool: &PgPool,
    chain: &str,
    key: RegistryMigrationMarkerCacheKey,
    before_block: i64,
    marker_topic0: &str,
    decode_node: &F,
    cache: &mut HashMap<RegistryMigrationMarkerCacheKey, RegistryMigrationMarkerCacheEntry>,
) -> Result<MigratedRegistryNodes>
where
    F: Fn(&[String]) -> Result<String>,
{
    let key_emitters = key.emitters.clone();
    let entry = cache
        .entry(key)
        .or_insert_with(|| RegistryMigrationMarkerCacheEntry {
            nodes: HashSet::new(),
        });
    let nodes = load_marker_nodes_between(
        pool,
        chain,
        &key_emitters,
        0,
        before_block,
        marker_topic0,
        decode_node,
    )
    .await?;
    entry.nodes = nodes;

    Ok(MigratedRegistryNodes::from_baseline(entry.nodes.clone()))
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
          AND emitting_address = ANY($2::TEXT[])
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
              WHERE watched.address = emitting_address
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
    use bigname_storage::{
        CanonicalityState, RawBlock, RawLog, default_database_url, upsert_raw_blocks,
        upsert_raw_logs,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for registry migration cache tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!("bn_ad_mig_{}_{}_{}", std::process::id(), sequence, unique);

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for registry migration cache tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for registry migration cache tests")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for registry migration cache tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

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

    #[tokio::test]
    async fn migration_marker_cache_drops_reorged_away_marker_without_restart() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;
        let chain = "ethereum-mainnet";
        let marker_topic0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let migrated_node = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let emitter = RegistryMigrationMarkerEmitter::new(
            "0x00000000000000000000000000000000000000aa",
            0,
            100,
        );
        let key = RegistryMigrationMarkerCacheKey {
            chain: chain.to_owned(),
            marker_topic0: marker_topic0.to_owned(),
            emitters: vec![emitter.clone()],
        };
        let mut cache = HashMap::new();

        upsert_raw_blocks(
            database.pool(),
            &[RawBlock {
                chain_id: chain.to_owned(),
                block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_owned(),
                parent_hash: None,
                block_number: 10,
                block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_010)?,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        upsert_raw_logs(
            database.pool(),
            &[RawLog {
                chain_id: chain.to_owned(),
                block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_owned(),
                block_number: 10,
                transaction_hash:
                    "0xtxcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitter.address.clone(),
                topics: vec![marker_topic0.to_owned(), migrated_node.to_owned()],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;

        let decode_node = |topics: &[String]| {
            topics
                .get(1)
                .cloned()
                .context("marker test log is missing node topic")
        };
        let first = load_migrated_registry_nodes_before_block_with_cache(
            database.pool(),
            chain,
            key.clone(),
            11,
            marker_topic0,
            &decode_node,
            &mut cache,
        )
        .await?;
        assert!(first.contains(migrated_node));

        sqlx::query(
            "UPDATE raw_logs SET canonicality_state = 'orphaned'::canonicality_state WHERE block_hash = $1",
        )
        .bind("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
        .execute(database.pool())
        .await
        .context("failed to orphan cached migration marker")?;

        let reloaded = load_migrated_registry_nodes_before_block_with_cache(
            database.pool(),
            chain,
            key,
            11,
            marker_topic0,
            &decode_node,
            &mut cache,
        )
        .await?;
        assert!(!reloaded.contains(migrated_node));

        database.cleanup().await
    }
}
