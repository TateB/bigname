use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::Row;

use crate::{
    cli::HealthcheckArgs,
    runtime::{ensure_manifest_root_ready, load_manifest_repository},
};

pub(crate) async fn healthcheck(args: HealthcheckArgs) -> Result<()> {
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    ensure_manifest_root_ready(manifest_repository.summary())?;

    let pool = bigname_storage::connect(&args.database).await?;
    verify_database_reachable(&pool).await?;
    verify_migrations_current(&pool).await?;
    println!("ok");
    Ok(())
}

async fn verify_database_reachable(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .context("failed to run indexer health database reachability query")?;
    Ok(())
}

async fn verify_migrations_current(pool: &sqlx::PgPool) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT version, success, checksum
        FROM _sqlx_migrations
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to read applied migrations for indexer healthcheck")?;

    let mut applied = BTreeMap::new();
    for row in rows {
        let version = row
            .try_get::<i64, _>("version")
            .context("applied migration row is missing version")?;
        let success = row
            .try_get::<bool, _>("success")
            .context("applied migration row is missing success flag")?;
        let checksum = row
            .try_get::<Vec<u8>, _>("checksum")
            .context("applied migration row is missing checksum")?;
        applied.insert(version, (success, checksum));
    }

    for migration in bigname_storage::MIGRATOR.iter() {
        let Some((success, checksum)) = applied.remove(&migration.version) else {
            bail!("checked-in migration {} is not applied", migration.version);
        };
        if !success {
            bail!(
                "applied migration {} did not complete successfully",
                migration.version
            );
        }
        if checksum != migration.checksum.as_ref() {
            bail!(
                "applied migration {} checksum does not match checked-in migration",
                migration.version
            );
        }
    }

    if let Some(version) = applied.keys().next() {
        bail!(
            "database has applied migration {version} that is not present in this indexer binary"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        fs,
        path::{Path, PathBuf},
        str::FromStr,
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use bigname_storage::DatabaseConfig;
    use clap::Parser;
    use sqlx::{ConnectOptions, postgres::PgConnectOptions};

    use crate::cli::{Cli, Command};

    struct TestManifestRoot {
        path: PathBuf,
    }

    impl TestManifestRoot {
        fn create() -> Result<Self> {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bigname-indexer-healthcheck-manifests-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            Ok(Self { path })
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestManifestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn database_config(database: &bigname_test_support::TestDatabase) -> Result<DatabaseConfig> {
        let base_url = bigname_test_support::database_url_from_env();
        let database_url = PgConnectOptions::from_str(&base_url)
            .context("failed to parse test database URL")?
            .database(database.database_name())
            .to_url_lossy()
            .to_string();
        Ok(DatabaseConfig {
            database_url: Some(database_url),
            max_connections: 2,
        })
    }

    #[test]
    fn healthcheck_cli_is_available() {
        let cli = Cli::parse_from([
            "bigname-indexer",
            "healthcheck",
            "--manifests-root",
            "manifests/mainnet",
        ]);

        match cli.command {
            Command::Healthcheck(args) => {
                assert_eq!(args.manifests_root, PathBuf::from("manifests/mainnet"));
                assert_eq!(args.database.max_connections, 10);
            }
            other => panic!("expected healthcheck command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn healthcheck_accepts_migrated_database() -> Result<()> {
        let database = bigname_test_support::TestDatabase::create_migrated(
            bigname_test_support::TestDatabaseConfig::new("bigname_indexer_healthcheck_test"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for indexer healthcheck test",
        )
        .await?;
        let manifest_root = TestManifestRoot::create()?;
        let result = healthcheck(HealthcheckArgs {
            database: database_config(&database)?,
            manifests_root: manifest_root.path().to_path_buf(),
        })
        .await;
        database.cleanup().await?;
        result
    }

    #[tokio::test]
    async fn healthcheck_rejects_unmigrated_database() -> Result<()> {
        let database = bigname_test_support::TestDatabase::create(
            bigname_test_support::TestDatabaseConfig::new("bigname_indexer_healthcheck_unmigrated"),
        )
        .await?;
        let manifest_root = TestManifestRoot::create()?;
        let error = healthcheck(HealthcheckArgs {
            database: database_config(&database)?,
            manifests_root: manifest_root.path().to_path_buf(),
        })
        .await
        .expect_err("unmigrated database must fail healthcheck");
        database.cleanup().await?;

        assert!(
            error
                .to_string()
                .contains("failed to read applied migrations"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn healthcheck_rejects_missing_manifest_root() {
        let missing_root = std::env::temp_dir().join(format!(
            "bigname-indexer-healthcheck-missing-{}",
            std::process::id()
        ));
        let error = healthcheck(HealthcheckArgs {
            database: DatabaseConfig::default(),
            manifests_root: missing_root,
        })
        .await
        .expect_err("missing manifest root must fail healthcheck before database access");

        assert!(
            error.to_string().contains("refusing to boot"),
            "unexpected error: {error:#}"
        );
    }
}
