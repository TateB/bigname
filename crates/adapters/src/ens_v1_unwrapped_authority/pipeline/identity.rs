use super::materialize::AuthorityIdentityBuffers;
use super::*;

pub(super) async fn ensure_binding_authority_identity_rows(
    pool: &PgPool,
    identity: &mut AuthorityIdentityBuffers,
    logical_name_id: &str,
    segment: &BindingSegment,
) -> Result<()> {
    let mut provenance = Map::from_iter([
        (
            "adapter".to_owned(),
            Value::String(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned()),
        ),
        (
            "authority_kind".to_owned(),
            Value::String(segment.authority.kind.as_str().to_owned()),
        ),
        (
            "authority_key".to_owned(),
            Value::String(segment.authority.authority_key.clone()),
        ),
        (
            "logical_name_id".to_owned(),
            Value::String(logical_name_id.to_owned()),
        ),
        (
            "source_event".to_owned(),
            Value::String("surface_binding_authority".to_owned()),
        ),
        (
            "binding_source_family".to_owned(),
            Value::String(segment.authority.binding_source_family.clone()),
        ),
        (
            "binding_manifest_version".to_owned(),
            Value::Number(segment.authority.binding_manifest_version.into()),
        ),
        (
            "binding_manifest_id".to_owned(),
            Value::Number(segment.authority.binding_manifest_id.into()),
        ),
    ]);
    if segment.authority.kind == AuthorityKind::Registrar {
        if let Some(labelhash) =
            registrar_labelhash_from_authority_key(&segment.authority.authority_key)
        {
            provenance.insert("labelhash".to_owned(), Value::String(labelhash));
        }
        if let Some(active_to) = segment.active_to {
            provenance.insert(
                "released_at".to_owned(),
                Value::Number(active_to.unix_timestamp().into()),
            );
            if let Some(expiry) = active_to
                .unix_timestamp()
                .checked_sub(ENS_GRACE_PERIOD_SECS)
            {
                provenance.insert("expiry".to_owned(), Value::Number(expiry.into()));
            }
        }
    }
    let provenance = Value::Object(provenance);

    if let Some(token_lineage_id) = segment.authority.token_lineage_id {
        identity.push_token_lineage(
            build_token_lineage_from_boundary(
                pool,
                token_lineage_id,
                &segment.anchor_ref.chain_id,
                &segment.anchor_ref,
                provenance.clone(),
            )
            .await?,
        );
    }

    identity.push_resource(
        build_resource(
            pool,
            segment.authority.resource_id,
            segment.authority.token_lineage_id,
            &segment.anchor_ref.chain_id,
            &segment.anchor_ref,
            provenance,
        )
        .await?,
    );

    Ok(())
}

pub(super) async fn close_binding_overlaps(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<(usize, u128)> {
    let started = Instant::now();
    let count = close_weaker_overlapping_existing_surface_bindings(pool, bindings).await?;
    Ok((count, started.elapsed().as_millis()))
}
