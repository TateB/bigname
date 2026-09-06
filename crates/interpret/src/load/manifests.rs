use bigname_adapters::schema_v2::ManifestInput;
use sqlx::PgConnection;

use crate::{InterpretError, Result};

type ManifestRow = (
    i64,
    i64,
    String,
    String,
    String,
    String,
    String,
    String,
    bool,
);

pub(super) async fn load(
    connection: &mut PgConnection,
    chain_id: &str,
) -> Result<(Vec<ManifestInput>, Vec<ManifestInput>)> {
    let rows: Vec<ManifestRow> = sqlx::query_as(
        "
        SELECT manifest_id,
               manifest_version,
               namespace,
               source_family,
               chain_id,
               deployment_label,
               normalizer_version,
               manifest_payload::text,
               rollout_status = 'active'
        FROM manifest_versions
        WHERE chain_id = $1
          AND rollout_status IN ('active', 'deprecated')
        ORDER BY namespace, source_family, manifest_version, manifest_id
        ",
    )
    .bind(chain_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| InterpretError::database("failed to load manifest provenance", error))?;
    let provenance = rows
        .into_iter()
        .map(
            |(
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain_id,
                deployment_label,
                normalizer_version,
                payload_json,
                active,
            )| {
                (
                    ManifestInput {
                        manifest_id,
                        manifest_version,
                        namespace,
                        source_family,
                        chain_id,
                        deployment_label,
                        normalizer_version,
                        payload_json,
                    },
                    active,
                )
            },
        )
        .collect::<Vec<_>>();
    let active = provenance
        .iter()
        .filter(|(_, active)| *active)
        .map(|(manifest, _)| manifest.clone())
        .collect();
    Ok((
        active,
        provenance
            .into_iter()
            .map(|(manifest, _)| manifest)
            .collect(),
    ))
}
