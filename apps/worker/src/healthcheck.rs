use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::Row;

use crate::cli::HealthcheckArgs;

pub(crate) async fn healthcheck(args: HealthcheckArgs) -> Result<()> {
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
        .context("failed to run worker health database reachability query")?;
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
    .context("failed to read applied migrations for worker healthcheck")?;

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
        bail!("database has applied migration {version} that is not present in this worker binary");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use anyhow::Result;
    use bigname_storage::DatabaseConfig;
    use clap::Parser;
    use sqlx::{ConnectOptions, postgres::PgConnectOptions};

    use crate::cli::{Cli, Command};

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
        let cli = Cli::parse_from(["bigname-worker", "healthcheck"]);

        match cli.command {
            Command::Healthcheck(args) => {
                assert_eq!(args.database.max_connections, 10);
            }
            other => panic!("expected healthcheck command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn healthcheck_accepts_migrated_database() -> Result<()> {
        let database = bigname_test_support::TestDatabase::create_migrated(
            bigname_test_support::TestDatabaseConfig::new("bigname_worker_healthcheck_test"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for worker healthcheck test",
        )
        .await?;
        let result = healthcheck(HealthcheckArgs {
            database: database_config(&database)?,
        })
        .await;
        database.cleanup().await?;
        result
    }

    #[tokio::test]
    async fn healthcheck_rejects_unmigrated_database() -> Result<()> {
        let database = bigname_test_support::TestDatabase::create(
            bigname_test_support::TestDatabaseConfig::new("bigname_worker_healthcheck_unmigrated"),
        )
        .await?;
        let error = healthcheck(HealthcheckArgs {
            database: database_config(&database)?,
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
}
