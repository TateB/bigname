use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page,
};

use super::{
    decode::{decode_permissions_current_full_filter_summary, decode_permissions_current_row},
    reads::{DEFAULT_PERMISSIONS_CURRENT_READ_FILTER, push_permissions_current_filters},
    types::{
        PermissionScope, PermissionsCurrentAccountResourceCursor,
        PermissionsCurrentAccountResourcePage, PermissionsCurrentFullFilterSummary,
        PermissionsCurrentKeysetCursor, PermissionsCurrentPage,
    },
};

/// Load one bounded keyset page for a resource's current permission rows.
pub async fn load_permissions_current_page(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
    cursor: Option<&PermissionsCurrentKeysetCursor>,
    page_size: u64,
) -> Result<PermissionsCurrentPage> {
    let limit = checked_page_limit_i64(
        page_size,
        "permissions_current page_size must be positive",
        "permissions_current page_size is too large",
    )?;
    let page_size_usize = checked_page_size_usize(
        page_size,
        "permissions_current page_size must be positive",
        "permissions_current page_size must fit in usize",
    )?;
    let scope_storage_key = scope.map(PermissionScope::storage_key);

    let page_rows = {
        let mut page_builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                pc.resource_id,
                pc.subject,
                pc.scope,
                pc.scope_kind,
                pc.scope_detail,
                pc.effective_powers,
                pc.grant_source,
                pc.revocation_source,
                pc.inheritance_path,
                pc.transfer_behavior,
                pc.provenance,
                pc.coverage,
                pc.chain_positions,
                pc.canonicality_summary,
                pc.manifest_version,
                pc.last_recomputed_at
            FROM permissions_current pc
            JOIN resources resource
              ON resource.resource_id = pc.resource_id
            WHERE "#,
        );
        push_permissions_current_filters(
            &mut page_builder,
            resource_id,
            subject,
            scope_storage_key.as_deref(),
        );
        push_permissions_current_keyset_cursor(&mut page_builder, cursor);
        page_builder.push(" ORDER BY pc.subject ASC, pc.scope ASC LIMIT ");
        page_builder.push_bind(limit);

        page_builder
            .build()
            .fetch_all(pool)
            .await
            .with_context(|| {
                format!("failed to load permissions_current page for resource_id {resource_id}")
            })?
    };

    let rows = page_rows
        .into_iter()
        .map(decode_permissions_current_row)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size_usize, |row| {
        PermissionsCurrentKeysetCursor::from(row)
    });

    let summary = load_permissions_current_full_filter_summary(
        pool,
        resource_id,
        subject,
        scope_storage_key.as_deref(),
    )
    .await?;

    Ok(PermissionsCurrentPage {
        rows,
        next_cursor,
        summary,
    })
}

/// Load one bounded keyset page for app-facing account/resource role rows.
pub async fn load_permissions_current_account_resource_page(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<PermissionsCurrentAccountResourcePage> {
    load_permissions_current_account_resource_page_with_summary(
        pool,
        subject,
        resource_id,
        cursor,
        page_size,
        AccountResourceSummaryMode::Full,
    )
    .await
}

/// Load one bounded app-facing account/resource role page with count-only summary metadata.
pub async fn load_permissions_current_account_resource_page_count_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<PermissionsCurrentAccountResourcePage> {
    load_permissions_current_account_resource_page_with_summary(
        pool,
        subject,
        resource_id,
        cursor,
        page_size,
        AccountResourceSummaryMode::CountOnly,
    )
    .await
}

#[derive(Clone, Copy)]
enum AccountResourceSummaryMode {
    Full,
    CountOnly,
}

async fn load_permissions_current_account_resource_page_with_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
    summary_mode: AccountResourceSummaryMode,
) -> Result<PermissionsCurrentAccountResourcePage> {
    let limit = checked_page_limit_i64(
        page_size,
        "permissions_current page_size must be positive",
        "permissions_current page_size is too large",
    )?;
    let page_size_usize = checked_page_size_usize(
        page_size,
        "permissions_current page_size must be positive",
        "permissions_current page_size must fit in usize",
    )?;

    let page_rows = {
        let mut page_builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                pc.resource_id,
                pc.subject,
                pc.scope,
                pc.scope_kind,
                pc.scope_detail,
                pc.effective_powers,
                pc.grant_source,
                pc.revocation_source,
                pc.inheritance_path,
                pc.transfer_behavior,
                pc.provenance,
                pc.coverage,
                pc.chain_positions,
                pc.canonicality_summary,
                pc.manifest_version,
                pc.last_recomputed_at
            FROM permissions_current pc
            JOIN resources resource
              ON resource.resource_id = pc.resource_id
            WHERE TRUE
            "#,
        );
        push_permissions_current_account_resource_filters(&mut page_builder, subject, resource_id);
        push_permissions_current_account_resource_cursor(&mut page_builder, cursor);
        page_builder.push(
            r#" ORDER BY pc.subject COLLATE "C" ASC, pc.resource_id ASC, pc.scope COLLATE "C" ASC LIMIT "#,
        );
        page_builder.push_bind(limit);

        page_builder
            .build()
            .fetch_all(pool)
            .await
            .context("failed to load permissions_current account/resource page")?
    };

    let rows = page_rows
        .into_iter()
        .map(decode_permissions_current_row)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size_usize, |row| {
        PermissionsCurrentAccountResourceCursor::from(row)
    });

    let summary = match summary_mode {
        AccountResourceSummaryMode::Full => {
            load_permissions_current_account_resource_summary(pool, subject, resource_id).await?
        }
        AccountResourceSummaryMode::CountOnly => {
            load_permissions_current_account_resource_count_summary(pool, subject, resource_id)
                .await?
        }
    };

    Ok(PermissionsCurrentAccountResourcePage {
        rows,
        next_cursor,
        summary,
    })
}

async fn load_permissions_current_full_filter_summary(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope_storage_key: Option<&str>,
) -> Result<PermissionsCurrentFullFilterSummary> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            COUNT(*)::BIGINT AS row_count,
            COALESCE(jsonb_agg(pc.provenance ORDER BY pc.subject ASC, pc.scope ASC), '[]'::jsonb) AS provenance,
            (jsonb_agg(pc.coverage ORDER BY pc.subject ASC, pc.scope ASC)->0) AS coverage,
            COALESCE(jsonb_agg(pc.chain_positions ORDER BY pc.subject ASC, pc.scope ASC), '[]'::jsonb) AS chain_positions,
            COALESCE(jsonb_agg(pc.canonicality_summary ORDER BY pc.subject ASC, pc.scope ASC), '[]'::jsonb) AS canonicality_summaries,
            MAX(pc.last_recomputed_at) AS last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE "#,
    );
    push_permissions_current_filters(&mut builder, resource_id, subject, scope_storage_key);

    let row = builder.build().fetch_one(pool).await.with_context(|| {
        format!("failed to summarize permissions_current rows for resource_id {resource_id}")
    })?;

    decode_permissions_current_full_filter_summary(row)
}

async fn load_permissions_current_account_resource_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
) -> Result<PermissionsCurrentFullFilterSummary> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            COUNT(*)::BIGINT AS row_count,
            COALESCE(jsonb_agg(pc.provenance ORDER BY pc.subject COLLATE "C" ASC, pc.resource_id ASC, pc.scope COLLATE "C" ASC), '[]'::jsonb) AS provenance,
            (jsonb_agg(pc.coverage ORDER BY pc.subject COLLATE "C" ASC, pc.resource_id ASC, pc.scope COLLATE "C" ASC)->0) AS coverage,
            COALESCE(jsonb_agg(pc.chain_positions ORDER BY pc.subject COLLATE "C" ASC, pc.resource_id ASC, pc.scope COLLATE "C" ASC), '[]'::jsonb) AS chain_positions,
            COALESCE(jsonb_agg(pc.canonicality_summary ORDER BY pc.subject COLLATE "C" ASC, pc.resource_id ASC, pc.scope COLLATE "C" ASC), '[]'::jsonb) AS canonicality_summaries,
            MAX(pc.last_recomputed_at) AS last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE TRUE
        "#,
    );
    push_permissions_current_account_resource_filters(&mut builder, subject, resource_id);

    let row = builder
        .build()
        .fetch_one(pool)
        .await
        .context("failed to summarize permissions_current account/resource rows")?;

    decode_permissions_current_full_filter_summary(row)
}

async fn load_permissions_current_account_resource_count_summary(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
) -> Result<PermissionsCurrentFullFilterSummary> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            COUNT(*)::BIGINT AS row_count,
            '[]'::jsonb AS provenance,
            NULL::jsonb AS coverage,
            '[]'::jsonb AS chain_positions,
            '[]'::jsonb AS canonicality_summaries,
            NULL::TIMESTAMPTZ AS last_recomputed_at
        FROM permissions_current pc
        JOIN resources resource
          ON resource.resource_id = pc.resource_id
        WHERE TRUE
        "#,
    );
    push_permissions_current_account_resource_filters(&mut builder, subject, resource_id);

    let row = builder
        .build()
        .fetch_one(pool)
        .await
        .context("failed to count permissions_current account/resource rows")?;

    decode_permissions_current_full_filter_summary(row)
}

fn push_permissions_current_keyset_cursor<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: Option<&'a PermissionsCurrentKeysetCursor>,
) {
    if let Some(cursor) = cursor {
        builder.push(" AND (pc.subject, pc.scope) > (");
        builder.push_bind(&cursor.subject);
        builder.push(", ");
        builder.push_bind(&cursor.scope);
        builder.push(")");
    }
}

fn push_permissions_current_account_resource_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    subject: Option<&'a str>,
    resource_id: Option<Uuid>,
) {
    if let Some(subject) = subject {
        builder.push(" AND pc.subject = ");
        builder.push_bind(subject);
    }

    if let Some(resource_id) = resource_id {
        builder.push(" AND pc.resource_id = ");
        builder.push_bind(resource_id);
    }

    builder.push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER);
}

fn push_permissions_current_account_resource_cursor<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: Option<&'a PermissionsCurrentAccountResourceCursor>,
) {
    if let Some(cursor) = cursor {
        builder.push(r#" AND (pc.subject COLLATE "C", pc.resource_id, pc.scope COLLATE "C") > ("#);
        builder.push_bind(&cursor.subject);
        builder.push(r#" COLLATE "C", "#);
        builder.push_bind(cursor.resource_id);
        builder.push(", ");
        builder.push_bind(&cursor.scope);
        builder.push(r#" COLLATE "C")"#);
    }
}
