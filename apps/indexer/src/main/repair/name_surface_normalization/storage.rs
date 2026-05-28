use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgRow};

use super::{
    CompatibleNameSurfaceUpdate, NameSurfaceNormalizationFinding, NameSurfaceNormalizationRow,
};

pub(super) async fn load_name_surface_normalization_page(
    pool: &PgPool,
    expected_normalizer_version: &str,
    after_logical_name_id: Option<&str>,
    page_size: i64,
) -> Result<Vec<NameSurfaceNormalizationRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_errors
        FROM name_surfaces
        WHERE normalizer_version <> $1
          AND ($2::TEXT IS NULL OR logical_name_id > $2)
        ORDER BY logical_name_id
        LIMIT $3
        "#,
    )
    .bind(expected_normalizer_version)
    .bind(after_logical_name_id)
    .bind(page_size)
    .fetch_all(pool)
    .await
    .context("failed to load name-surface normalization repair page")?;

    rows.into_iter()
        .map(decode_name_surface_normalization_row)
        .collect()
}

fn decode_name_surface_normalization_row(row: PgRow) -> Result<NameSurfaceNormalizationRow> {
    Ok(NameSurfaceNormalizationRow {
        logical_name_id: row.try_get("logical_name_id")?,
        namespace: row.try_get("namespace")?,
        input_name: row.try_get("input_name")?,
        canonical_display_name: row.try_get("canonical_display_name")?,
        normalized_name: row.try_get("normalized_name")?,
        dns_encoded_name: row.try_get("dns_encoded_name")?,
        namehash: row.try_get("namehash")?,
        labelhashes: row.try_get("labelhashes")?,
        normalizer_version: row.try_get("normalizer_version")?,
        normalization_errors: row.try_get("normalization_errors")?,
    })
}

pub(super) async fn update_compatible_name_surfaces(
    transaction: &mut Transaction<'_, Postgres>,
    updates: &[CompatibleNameSurfaceUpdate],
    expected_normalizer_version: &str,
) -> Result<Vec<String>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }

    let logical_name_ids = updates
        .iter()
        .map(|update| update.logical_name_id.clone())
        .collect::<Vec<_>>();
    let input_names = updates
        .iter()
        .map(|update| update.input_name.clone())
        .collect::<Vec<_>>();
    let canonical_display_names = updates
        .iter()
        .map(|update| update.canonical_display_name.clone())
        .collect::<Vec<_>>();
    let namespaces = updates
        .iter()
        .map(|update| update.namespace.clone())
        .collect::<Vec<_>>();
    let normalized_names = updates
        .iter()
        .map(|update| update.normalized_name.clone())
        .collect::<Vec<_>>();
    let dns_encoded_names = updates
        .iter()
        .map(|update| update.dns_encoded_name.clone())
        .collect::<Vec<_>>();
    let namehashes = updates
        .iter()
        .map(|update| update.namehash.clone())
        .collect::<Vec<_>>();
    let labelhashes = updates
        .iter()
        .map(|update| {
            serde_json::to_string(&update.labelhashes)
                .context("failed to serialize compatible name-surface labelhashes")
        })
        .collect::<Result<Vec<_>>>()?;

    sqlx::query_scalar::<_, String>(
        r#"
        WITH input_rows AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::BYTEA[],
                $7::TEXT[],
                $8::TEXT[]
            ) AS input(
                logical_name_id,
                input_name,
                canonical_display_name,
                namespace,
                normalized_name,
                dns_encoded_name,
                namehash,
                labelhashes
            )
        ),
        updated AS (
            UPDATE name_surfaces surface
            SET
                input_name = input_rows.input_name,
                canonical_display_name = input_rows.canonical_display_name,
                normalizer_version = $9,
                normalization_warnings = '[]'::jsonb,
                normalization_errors = '[]'::jsonb
            FROM input_rows
            WHERE surface.logical_name_id = input_rows.logical_name_id
              AND surface.normalizer_version <> $9
              AND surface.namespace = input_rows.namespace
              AND surface.normalized_name = input_rows.normalized_name
              AND surface.dns_encoded_name = input_rows.dns_encoded_name
              AND surface.namehash = input_rows.namehash
              AND surface.labelhashes = ARRAY(
                    SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb)
                  )
              AND surface.normalization_errors = '[]'::jsonb
            RETURNING surface.logical_name_id
        )
        SELECT logical_name_id FROM updated
        "#,
    )
    .bind(&logical_name_ids)
    .bind(&input_names)
    .bind(&canonical_display_names)
    .bind(&namespaces)
    .bind(&normalized_names)
    .bind(&dns_encoded_names)
    .bind(&namehashes)
    .bind(&labelhashes)
    .bind(expected_normalizer_version)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to update compatible name-surface normalization metadata")
}

pub(super) async fn clear_compatible_name_surface_findings(
    transaction: &mut Transaction<'_, Postgres>,
    updated_logical_name_ids: &[String],
    expected_normalizer_version: &str,
) -> Result<()> {
    if updated_logical_name_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        DELETE FROM name_surface_normalization_repair_findings
        WHERE expected_normalizer_version = $1
          AND logical_name_id = ANY($2::TEXT[])
        "#,
    )
    .bind(expected_normalizer_version)
    .bind(updated_logical_name_ids)
    .execute(&mut **transaction)
    .await
    .context("failed to clear compatible name-surface normalization findings")?;
    Ok(())
}

pub(super) async fn upsert_name_surface_normalization_findings(
    transaction: &mut Transaction<'_, Postgres>,
    findings: &[NameSurfaceNormalizationFinding],
) -> Result<i64> {
    if findings.is_empty() {
        return Ok(0);
    }

    let logical_name_ids = findings
        .iter()
        .map(|finding| finding.logical_name_id.clone())
        .collect::<Vec<_>>();
    let expected_versions = findings
        .iter()
        .map(|finding| finding.expected_normalizer_version.clone())
        .collect::<Vec<_>>();
    let finding_kinds = findings
        .iter()
        .map(|finding| finding.finding_kind.to_owned())
        .collect::<Vec<_>>();
    let current_versions = findings
        .iter()
        .map(|finding| finding.current_normalizer_version.clone())
        .collect::<Vec<_>>();
    let namespaces = findings
        .iter()
        .map(|finding| finding.namespace.clone())
        .collect::<Vec<_>>();
    let input_names = findings
        .iter()
        .map(|finding| finding.input_name.clone())
        .collect::<Vec<_>>();
    let current_normalized_names = findings
        .iter()
        .map(|finding| finding.current_normalized_name.clone())
        .collect::<Vec<_>>();
    let candidate_logical_name_ids = findings
        .iter()
        .map(|finding| finding.candidate_logical_name_id.clone())
        .collect::<Vec<_>>();
    let candidate_normalized_names = findings
        .iter()
        .map(|finding| finding.candidate_normalized_name.clone())
        .collect::<Vec<_>>();
    let error_messages = findings
        .iter()
        .map(|finding| finding.error_message.clone())
        .collect::<Vec<_>>();
    let details = findings
        .iter()
        .map(|finding| finding.details.to_string())
        .collect::<Vec<_>>();

    sqlx::query_scalar::<_, i64>(
        r#"
        WITH input_rows AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::TEXT[],
                $7::TEXT[],
                $8::TEXT[],
                $9::TEXT[],
                $10::TEXT[],
                $11::TEXT[]
            ) AS input(
                logical_name_id,
                expected_normalizer_version,
                finding_kind,
                current_normalizer_version,
                namespace,
                input_name,
                current_normalized_name,
                candidate_logical_name_id,
                candidate_normalized_name,
                error_message,
                details
            )
        ),
        upserted AS (
            INSERT INTO name_surface_normalization_repair_findings (
                logical_name_id,
                expected_normalizer_version,
                finding_kind,
                current_normalizer_version,
                namespace,
                input_name,
                current_normalized_name,
                candidate_logical_name_id,
                candidate_normalized_name,
                error_message,
                details,
                detected_at,
                updated_at
            )
            SELECT
                logical_name_id,
                expected_normalizer_version,
                finding_kind,
                current_normalizer_version,
                namespace,
                input_name,
                current_normalized_name,
                candidate_logical_name_id,
                candidate_normalized_name,
                error_message,
                details::jsonb,
                now(),
                now()
            FROM input_rows
            ON CONFLICT (expected_normalizer_version, logical_name_id) DO UPDATE
            SET
                finding_kind = EXCLUDED.finding_kind,
                current_normalizer_version = EXCLUDED.current_normalizer_version,
                namespace = EXCLUDED.namespace,
                input_name = EXCLUDED.input_name,
                current_normalized_name = EXCLUDED.current_normalized_name,
                candidate_logical_name_id = EXCLUDED.candidate_logical_name_id,
                candidate_normalized_name = EXCLUDED.candidate_normalized_name,
                error_message = EXCLUDED.error_message,
                details = EXCLUDED.details,
                updated_at = now()
            RETURNING logical_name_id
        )
        SELECT COUNT(*)::BIGINT FROM upserted
        "#,
    )
    .bind(&logical_name_ids)
    .bind(&expected_versions)
    .bind(&finding_kinds)
    .bind(&current_versions)
    .bind(&namespaces)
    .bind(&input_names)
    .bind(&current_normalized_names)
    .bind(&candidate_logical_name_ids)
    .bind(&candidate_normalized_names)
    .bind(&error_messages)
    .bind(&details)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to upsert name-surface normalization repair findings")
}

pub(super) async fn count_name_surfaces_with_old_normalizer(
    pool: &PgPool,
    expected_normalizer_version: &str,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM name_surfaces
        WHERE normalizer_version <> $1
        "#,
    )
    .bind(expected_normalizer_version)
    .fetch_one(pool)
    .await
    .context("failed to count name surfaces with old normalizer")
}

#[cfg(test)]
mod tests {
    use super::super::{
        NameSurfaceNormalizationAction, classify_name_surface_normalization, keccak256_hex,
        namehash_hex,
    };
    use super::*;
    use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn guarded_update_clears_only_rows_that_were_stamped() -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| bigname_storage::default_database_url().to_owned());
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .context("failed to connect name-surface normalization storage test database")?;
        create_temp_repair_tables(&pool).await?;
        insert_compatible_surface(&pool).await?;

        let page =
            load_name_surface_normalization_page(&pool, ENS_NORMALIZER_VERSION, None, 1).await?;
        let update = match classify_name_surface_normalization(&page[0], ENS_NORMALIZER_VERSION) {
            NameSurfaceNormalizationAction::Compatible(update) => update,
            NameSurfaceNormalizationAction::Finding(_) => {
                panic!("test fixture should classify as compatible")
            }
        };
        insert_existing_finding(&pool, &update.logical_name_id).await?;

        sqlx::query(
            "UPDATE name_surfaces SET normalized_name = 'alice-mutated.eth' WHERE logical_name_id = $1",
        )
        .bind(&update.logical_name_id)
        .execute(&pool)
        .await
        .context("failed to simulate concurrent name-surface identity change")?;

        let mut transaction = pool
            .begin()
            .await
            .context("failed to open guarded update test transaction")?;
        let updated =
            update_compatible_name_surfaces(&mut transaction, &[update], ENS_NORMALIZER_VERSION)
                .await?;
        clear_compatible_name_surface_findings(&mut transaction, &updated, ENS_NORMALIZER_VERSION)
            .await?;
        transaction
            .commit()
            .await
            .context("failed to commit guarded update test transaction")?;

        assert!(updated.is_empty());
        let old_normalizer = sqlx::query_scalar::<_, String>(
            "SELECT normalizer_version FROM name_surfaces WHERE logical_name_id = 'ens:alice.eth'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(old_normalizer, "ensip15@2026-04-16");
        let finding_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM name_surface_normalization_repair_findings",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(finding_count, 1);

        Ok(())
    }

    async fn create_temp_repair_tables(pool: &PgPool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TEMP TABLE name_surfaces (
                logical_name_id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                input_name TEXT NOT NULL,
                canonical_display_name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                dns_encoded_name BYTEA NOT NULL,
                namehash TEXT NOT NULL,
                labelhashes TEXT[] NOT NULL,
                normalizer_version TEXT NOT NULL,
                normalization_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
                normalization_errors JSONB NOT NULL DEFAULT '[]'::jsonb
            )
            "#,
        )
        .execute(pool)
        .await
        .context("failed to create temp name_surfaces table")?;
        sqlx::query(
            r#"
            CREATE TEMP TABLE name_surface_normalization_repair_findings (
                logical_name_id TEXT NOT NULL,
                expected_normalizer_version TEXT NOT NULL,
                finding_kind TEXT NOT NULL,
                current_normalizer_version TEXT NOT NULL,
                namespace TEXT NOT NULL,
                input_name TEXT NOT NULL,
                current_normalized_name TEXT NOT NULL,
                candidate_logical_name_id TEXT,
                candidate_normalized_name TEXT,
                error_message TEXT,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (expected_normalizer_version, logical_name_id)
            )
            "#,
        )
        .execute(pool)
        .await
        .context("failed to create temp name-surface repair findings table")?;
        Ok(())
    }

    async fn insert_compatible_surface(pool: &PgPool) -> Result<()> {
        let labels = ["alice".to_owned(), "eth".to_owned()];
        let dns_encoded_name = vec![5, b'a', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0];
        let labelhashes = labels
            .iter()
            .map(|label| keccak256_hex(label.as_bytes()))
            .collect::<Vec<_>>();
        sqlx::query(
            r#"
            INSERT INTO name_surfaces (
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                labelhashes,
                normalizer_version,
                normalization_warnings,
                normalization_errors
            )
            VALUES (
                'ens:alice.eth',
                'ens',
                'Alice.eth',
                'alice.eth',
                'alice.eth',
                $1,
                $2,
                $3,
                'ensip15@2026-04-16',
                '[]'::jsonb,
                '[]'::jsonb
            )
            "#,
        )
        .bind(dns_encoded_name)
        .bind(namehash_hex(&labels))
        .bind(labelhashes)
        .execute(pool)
        .await
        .context("failed to insert compatible temp name surface")?;
        Ok(())
    }

    async fn insert_existing_finding(pool: &PgPool, logical_name_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO name_surface_normalization_repair_findings (
                logical_name_id,
                expected_normalizer_version,
                finding_kind,
                current_normalizer_version,
                namespace,
                input_name,
                current_normalized_name,
                error_message
            )
            VALUES (
                $1,
                $2,
                'rejected',
                'ensip15@2026-04-16',
                'ens',
                'Alice.eth',
                'alice.eth',
                'stale prior finding'
            )
            "#,
        )
        .bind(logical_name_id)
        .bind(ENS_NORMALIZER_VERSION)
        .execute(pool)
        .await
        .context("failed to insert existing temp name-surface repair finding")?;
        Ok(())
    }
}
