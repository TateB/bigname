use anyhow::{Context, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    LoadedManifest, normalize_address,
    support::{DeclarationKey, PersistedManifestEntry},
};

pub(super) async fn resolve_manifest_entry_contract_instance_id(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
    declared_address: &str,
    existing_entry: Option<&PersistedManifestEntry>,
    contract_kind: &str,
) -> Result<Uuid> {
    if let Some(existing_entry) = existing_entry
        && existing_entry.declared_address == declared_address
    {
        return Ok(existing_entry.contract_instance_id);
    }

    if let Some(previous_entry) =
        load_latest_related_manifest_entry(executor, manifest_id, loaded_manifest, key).await?
        && previous_entry.declared_address == declared_address
    {
        return Ok(previous_entry.contract_instance_id);
    }

    resolve_contract_instance_by_address(
        executor,
        &loaded_manifest.manifest.chain,
        declared_address,
        contract_kind,
        &serde_json::json!({
            "source": "manifest_declaration",
            "manifest_id": manifest_id,
            "declaration_kind": key.declaration_kind,
            "declaration_name": key.declaration_name,
        }),
    )
    .await
}

async fn load_latest_related_manifest_entry(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
) -> Result<Option<PersistedManifestEntry>> {
    let row = sqlx::query(
        r#"
        SELECT
            mci.contract_instance_id,
            mci.declared_address,
            mci.code_hash,
            mci.abi_ref,
            mci.role,
            mci.proxy_kind,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_contract_instances mci
        JOIN manifest_versions mv ON mv.manifest_id = mci.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
          AND mv.chain = $3
          AND mv.deployment_epoch = $4
          AND mci.declaration_kind = $5
          AND mci.declaration_name = $6
          AND mci.manifest_id <> $7
        ORDER BY mv.manifest_version DESC, mci.manifest_contract_instance_id DESC
        LIMIT 1
        "#,
    )
    .bind(&loaded_manifest.manifest.namespace)
    .bind(&loaded_manifest.manifest.source_family)
    .bind(&loaded_manifest.manifest.chain)
    .bind(&loaded_manifest.manifest.deployment_epoch)
    .bind(&key.declaration_kind)
    .bind(&key.declaration_name)
    .bind(manifest_id)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load prior declaration state for {} {}",
            key.declaration_kind, key.declaration_name
        )
    })?;

    row.map(|row| {
        Ok(PersistedManifestEntry {
            key: key.clone(),
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read prior contract_instance_id")?,
            declared_address: row
                .try_get("declared_address")
                .context("failed to read prior declared_address")?,
            code_hash: row
                .try_get("code_hash")
                .context("failed to read prior code_hash")?,
            abi_ref: row
                .try_get("abi_ref")
                .context("failed to read prior abi_ref")?,
            role: row.try_get("role").context("failed to read prior role")?,
            proxy_kind: row
                .try_get("proxy_kind")
                .context("failed to read prior proxy_kind")?,
            implementation_contract_instance_id: row
                .try_get("implementation_contract_instance_id")
                .context("failed to read prior implementation_contract_instance_id")?,
            declared_implementation_address: row
                .try_get("declared_implementation_address")
                .context("failed to read prior declared_implementation_address")?,
        })
    })
    .transpose()
}

pub(crate) async fn resolve_contract_instance_by_address(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
    contract_kind: &str,
    provenance: &serde_json::Value,
) -> Result<Uuid> {
    Ok(resolve_contract_instance_by_address_with_status(
        executor,
        chain,
        address,
        contract_kind,
        provenance,
    )
    .await?
    .0)
}

pub(crate) async fn resolve_contract_instance_by_address_with_status(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
    contract_kind: &str,
    provenance: &serde_json::Value,
) -> Result<(Uuid, bool)> {
    let normalized_address = normalize_address(address);

    if let Some(contract_instance_id) =
        find_contract_instance_by_address(executor, chain, &normalized_address).await?
    {
        return Ok((contract_instance_id, false));
    }

    let contract_instance_id = Uuid::new_v4();
    let provenance = serde_json::to_string(provenance)
        .context("failed to serialize contract-instance provenance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, $3, $4::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(contract_kind)
    .bind(provenance)
    .execute(executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert contract_instance_id {contract_instance_id} for chain {chain} address {normalized_address}"
        )
    })?;

    Ok((contract_instance_id, true))
}

async fn find_contract_instance_by_address(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT contract_instance_id
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND address = $2
        ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(address)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to resolve contract instance for chain {chain} address {address}")
    })
}
