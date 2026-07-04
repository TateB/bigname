use super::*;

pub(super) fn sort_and_dedup_blocks(blocks: &mut Vec<RawBlockSnapshot>) {
    blocks.sort_by(|left, right| {
        left.block_number
            .cmp(&right.block_number)
            .then(left.block_hash.cmp(&right.block_hash))
    });
    blocks.dedup_by(|left, right| {
        left.chain_id == right.chain_id
            && left.block_hash == right.block_hash
            && left.block_number == right.block_number
    });
}

pub(super) fn raw_block_snapshot_from_row(row: sqlx::postgres::PgRow) -> Result<RawBlockSnapshot> {
    Ok(RawBlockSnapshot {
        chain_id: sql_row::get(&row, "chain_id")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_number: sql_row::get(&row, "block_number")?,
        block_timestamp: sql_row::get(&row, "block_timestamp")?,
        canonicality_state: sql_row::get(&row, "canonicality_state")?,
    })
}

pub(super) fn json_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("selected registrar replay reference is missing {key}"))
}

pub(super) fn json_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("selected registrar replay reference is missing {key}"))
}

pub(super) fn json_optional_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

pub(super) fn json_optional_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_i64)
}

pub(super) fn raw_log_event_identity(
    raw_log: &AuthorityRawLogRow,
    event_kind: &str,
    identity_prefix: &str,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        event_kind,
        identity_prefix,
        raw_log.block_hash,
        raw_log.transaction_hash,
        raw_log.log_index
    )
}

pub(super) fn name_metadata_from_preload_row(row: &sqlx::postgres::PgRow) -> Result<NameMetadata> {
    Ok(NameMetadata {
        namespace: sql_row::get(&row, "namespace")?,
        logical_name_id: sql_row::get(&row, "logical_name_id")?,
        input_name: sql_row::get(&row, "input_name")?,
        canonical_display_name: sql_row::get(&row, "canonical_display_name")?,
        normalized_name: sql_row::get(&row, "normalized_name")?,
        dns_encoded_name: sql_row::get(&row, "dns_encoded_name")?,
        namehash: sql_row::get::<String>(&row, "namehash")?.to_ascii_lowercase(),
        labelhashes: sql_row::get(&row, "labelhashes")?,
        normalizer_version: sql_row::get(&row, "normalizer_version")?,
    })
}

pub(super) async fn load_name_metadata_by_logical_name_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<HashMap<String, NameMetadata>> {
    if logical_name_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            namespace,
            logical_name_id,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version
        FROM name_surfaces
        WHERE logical_name_id = ANY($1::TEXT[])
          AND canonicality_state {CANONICALITY_STATE_FILTER}
        "#
    ))
    .bind(logical_name_ids)
    .fetch_all(pool)
    .await
    .context("failed to preload selected registrar replay name metadata")?;

    let mut names = HashMap::new();
    for row in rows {
        let name = name_metadata_from_preload_row(&row)?;
        names.insert(name.logical_name_id.clone(), name);
    }
    Ok(names)
}

pub(super) fn observation_ref_from_boundary(
    boundary: &BoundaryRef,
    source_family: Option<String>,
    source_manifest_id: Option<i64>,
    log_index: Option<i64>,
) -> ObservationRef {
    ObservationRef {
        chain_id: boundary.chain_id.clone(),
        block_hash: boundary.block_hash.clone(),
        block_number: boundary.block_number,
        block_timestamp: boundary.block_timestamp,
        transaction_hash: None,
        transaction_index: None,
        log_index,
        canonicality_state: boundary.canonicality_state,
        namespace: boundary.namespace.clone(),
        source_manifest_id: source_manifest_id.unwrap_or(0),
        source_family: source_family
            .unwrap_or_else(|| default_registrar_source_family(&boundary.namespace).to_owned()),
        manifest_version: 1,
    }
}

pub(super) fn provenance_string(provenance: &Value, key: &str) -> Result<String> {
    provenance
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| format!("preloaded authority provenance is missing {key}"))
}

pub(super) fn provenance_i64(provenance: &Value, key: &str) -> Result<i64> {
    provenance
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("preloaded authority provenance is missing integer {key}"))
}

pub(super) fn manifest_id_from_authority_key(authority_key: &str) -> Option<i64> {
    authority_key.split(':').nth(2)?.parse().ok()
}

pub(super) fn log_index_from_authority_key(authority_key: &str) -> Option<i64> {
    authority_key.rsplit(':').next()?.parse().ok()
}
