use anyhow::{Context, Result};
use uuid::Uuid;

use crate::{LoadedManifest, support::PersistedManifestEntry};

pub(super) async fn seed_planned_manifest_entry_addresses(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    planned_entries: &[PersistedManifestEntry],
) -> Result<()> {
    for entry in planned_entries {
        ensure_contract_instance_address_seed(
            executor,
            entry.contract_instance_id,
            &loaded_manifest.manifest.chain,
            &entry.declared_address,
            Some(manifest_id),
            &serde_json::json!({
                "source": "manifest_declaration_seed",
                "manifest_id": manifest_id,
                "declaration_kind": entry.key.declaration_kind,
                "declaration_name": entry.key.declaration_name,
                "source_family": loaded_manifest.manifest.source_family,
            }),
        )
        .await?;

        if let (Some(implementation_contract_instance_id), Some(implementation_address)) = (
            entry.implementation_contract_instance_id,
            entry.declared_implementation_address.as_deref(),
        ) {
            ensure_contract_instance_address_seed(
                executor,
                implementation_contract_instance_id,
                &loaded_manifest.manifest.chain,
                implementation_address,
                Some(manifest_id),
                &serde_json::json!({
                    "source": "manifest_proxy_implementation_seed",
                    "manifest_id": manifest_id,
                    "declaration_name": entry.key.declaration_name,
                    "source_family": loaded_manifest.manifest.source_family,
                    "proxy_contract_instance_id": entry.contract_instance_id,
                    "proxy_address": entry.declared_address,
                }),
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) async fn ensure_contract_instance_address_seed(
    executor: &mut sqlx::postgres::PgConnection,
    contract_instance_id: Uuid,
    chain: &str,
    address: &str,
    source_manifest_id: Option<i64>,
    provenance: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, $4, $5::jsonb)
        ON CONFLICT (contract_instance_id)
        WHERE deactivated_at IS NULL
        DO NOTHING
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(address)
    .bind(source_manifest_id)
    .bind(
        serde_json::to_string(provenance)
            .context("failed to serialize contract-instance address seed provenance")?,
    )
    .execute(&mut *executor)
    .await
    .with_context(|| {
        format!(
            "failed to seed contract-instance address row for contract_instance_id {contract_instance_id}"
        )
    })?;

    Ok(())
}
