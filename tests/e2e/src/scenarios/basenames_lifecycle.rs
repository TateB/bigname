use std::collections::BTreeSet;

use alloy_primitives::{Address, U256, keccak256};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::{resolver_records::V2Api, support};
use crate::harness::responses::{exact_name, pointer, primary_name, selector_keys};
use crate::harness::{anvil::Anvil, basenames, ens_v1, repo_root};

const DAY: u64 = 24 * 60 * 60;
const YEAR: u64 = 365 * DAY;
const GRACE_PERIOD: u64 = 90 * DAY;
const MULTICOIN_TYPE: u64 = 0;
const MULTICOIN_BYTES: &[u8] = &[0xde, 0xad, 0xbe, 0xef];
const CONTENTHASH_BYTES: &[u8] = &[0xe3, 0x01, 0x01, 0x70, 0x12, 0x20];

async fn children(run: &support::PipelineRun, parent: &str) -> Result<Vec<Value>> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/names/basenames/{parent}/children"))
        .await?;
    assert_eq!(status, 200, "Basenames children lookup failed: {body}");
    Ok(body
        .pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

async fn compact_records(run: &support::PipelineRun, name: &str, query: &str) -> Result<Value> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/names/basenames/{name}/records{query}"))
        .await?;
    assert_eq!(status, 200, "Basenames records lookup failed: {body}");
    Ok(body)
}

async fn v2_records(api: &V2Api, name: &str, source: &str) -> Result<Value> {
    let response = api
        .client
        .get(format!(
            "{}/v2/names/{name}/records?namespace=basenames&source={source}&keys=addr:60",
            api.base_url
        ))
        .send()
        .await?;
    let status = response.status();
    let body: Value = response.json().await?;
    assert_eq!(status, 200, "v2 Basenames records lookup failed: {body}");
    Ok(body)
}

fn assert_addr60_not_found(body: &Value) {
    assert_eq!(
        pointer(body, "/data/records/addr:60/status"),
        "not_found",
        "Basenames addr:60 should be absent: {body}"
    );
    assert!(body.pointer("/data/records/addr:60/value").is_none());
    assert!(body.pointer("/data/addresses/60").is_none());
    assert!(body.pointer("/data/primary_address").is_none());
}

fn addr60_observation(body: &Value) -> Value {
    json!({
        "record": {
            "status": body.pointer("/data/records/addr:60/status"),
            "value": body.pointer("/data/records/addr:60/value"),
        },
        "address": body.pointer("/data/addresses/60"),
        "primary_address": body.pointer("/data/primary_address"),
    })
}

async fn enable_basenames_verified_route(
    run: &support::PipelineRun,
    logical_name_id: &str,
    l1_resolver: Address,
) -> Result<()> {
    let l1_resolver = format!("{l1_resolver:#x}");
    // #857 workaround: without scenario topology/provenance, verified/auto are unsupported.
    let topology = sqlx::query(
        r#"
        UPDATE name_current name
        SET declared_summary = jsonb_set(
                name.declared_summary,
                '{topology}',
                jsonb_build_object(
                    'registry_path', jsonb_build_array(jsonb_build_object(
                        'logical_name_id', name.logical_name_id,
                        'namespace', name.namespace,
                        'normalized_name', name.raw_name,
                        'canonical_display_name', name.raw_name,
                        'namehash', name.namehash,
                        'resource_id', name.resource_id,
                        'binding_kind', name.binding_kind
                    )),
                    'subregistry_path', '[]'::jsonb,
                    'resolver_path', jsonb_build_array(jsonb_build_object(
                        'logical_name_id', name.logical_name_id,
                        'namespace', name.namespace,
                        'normalized_name', name.raw_name,
                        'canonical_display_name', name.raw_name,
                        'resource_id', name.resource_id,
                        'chain_id', name.declared_summary #>> '{resolver,chain_id}',
                        'address', name.declared_summary #>> '{resolver,address}',
                        'latest_event_kind',
                            name.declared_summary #>> '{resolver,latest_event_kind}'
                    )),
                    'wildcard', jsonb_build_object(
                        'source', NULL, 'matched_labels', '[]'::jsonb
                    ),
                    'alias', jsonb_build_object(
                        'final_target', NULL, 'hops', '[]'::jsonb
                    ),
                    'version_boundaries', jsonb_build_object(
                        'topology_version_boundary',
                            inventory.record_version_boundary,
                        'record_version_boundary',
                            inventory.record_version_boundary
                    ),
                    'transport', jsonb_build_object(
                        'source_chain_id', 'base-mainnet',
                        'target_chain_id', 'ethereum-mainnet',
                        'contract_address', $2,
                        'latest_event_kind', NULL
                    )
                ),
                true
            ),
            provenance = jsonb_set(
                name.provenance,
                '{manifest_versions}',
                COALESCE(name.provenance -> 'manifest_versions', '[]'::jsonb) ||
                    jsonb_build_array(jsonb_build_object(
                        'source_family', 'basenames_execution',
                        'manifest_version', 2,
                        'chain', 'ethereum-mainnet',
                        'deployment_epoch', 'basenames_v1'
                    )),
                true
            )
        FROM record_inventory_current inventory
        WHERE name.logical_name_id = $1
          AND inventory.resource_id = name.resource_id
        "#,
    )
    .bind(logical_name_id)
    .bind(l1_resolver)
    .execute(&run.db.pool)
    .await?;
    ensure!(
        topology.rows_affected() == 1,
        "install Basenames verified topology for {logical_name_id}"
    );
    Ok(())
}

fn inventory_reason(body: &Value, section: &str, family: &str, field: &str) -> Option<String> {
    body.pointer(&format!("/declared_state/record_inventory/{section}"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|entry| entry.get("record_family").and_then(Value::as_str) == Some(family))
        .and_then(|entry| entry.get(field).and_then(Value::as_str))
        .map(str::to_owned)
}

async fn active_registrar_identity(
    run: &support::PipelineRun,
    logical_name_id: &str,
) -> Result<(Uuid, Uuid)> {
    let (resource_id, token_lineage_id): (Uuid, Option<Uuid>) = sqlx::query_as(
        "SELECT binding.resource_id, resource.token_lineage_id \
         FROM surface_bindings binding \
         JOIN resources resource USING (resource_id) \
         WHERE binding.logical_name_id = $1 \
           AND binding.active_to IS NULL \
           AND binding.canonicality_state = 'canonical' \
           AND resource.canonicality_state = 'canonical' \
         ORDER BY binding.active_from DESC, binding.surface_binding_id DESC \
         LIMIT 1",
    )
    .bind(logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    Ok((
        resource_id,
        token_lineage_id.context("active registrar resource has no token lineage")?,
    ))
}

/// The legacy controller's three-field renewal is followed through grace,
/// release at the first later admitted block, and the registrar's expired-token
/// burn/re-mint path.
/// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L497 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/util/Constants.sol:L15 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L438 @ basenames@1809bbc)
#[tokio::test]
async fn renew_release_and_premium_reregistration_rotate_lineage() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);

    basenames::register_base_name(&rpc, &deployment, alice, "phoenix", alice, YEAR).await?;
    let renewal = basenames::renew_base_name(&rpc, &deployment, alice, "phoenix", YEAR).await?;
    rpc.increase_time(2 * YEAR + DAY).await?;

    let renewal_ready_sql = support::canonical_event_ready_sql(
        "basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020",
        "RegistrationRenewed",
        None,
    );
    let grace =
        support::ingest_basenames_and_serve(&base, &deployment, Some(&renewal_ready_sql)).await?;
    let renewal_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND logical_name_id = 'basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020' \
           AND event_kind = 'RegistrationRenewed' \
           AND source_family = 'basenames_base_registrar' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&renewal.tx_hash)
    .fetch_one(&grace.db.pool)
    .await?;
    assert_eq!(
        renewal_events, 1,
        "three-field NameRenewed must decode once"
    );
    let grace_body = exact_name(&grace.api, "basenames", "phoenix.base.eth").await?;
    assert_eq!(
        pointer(&grace_body, "/declared_state/registration/status"),
        "active"
    );
    let grace_expiry = pointer(&grace_body, "/declared_state/registration/expiry")
        .as_u64()
        .context("renewed expiry missing")?;
    assert!(
        u128::from(grace_expiry) < rpc.block_timestamp().await?,
        "the renewed lease should be expired but still represented inside grace"
    );
    let first_identity = active_registrar_identity(
        &grace,
        "basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020",
    )
    .await?;
    grace.db.cleanup().await?;

    rpc.increase_time(GRACE_PERIOD).await?;
    let activity =
        basenames::register_base_name(&rpc, &deployment, alice, "releaseprobe", alice, YEAR)
            .await?;
    let released_ready_sql = support::canonical_event_ready_sql(
        "basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020",
        "RegistrationReleased",
        None,
    );
    let released =
        support::ingest_basenames_and_serve(&base, &deployment, Some(&released_ready_sql)).await?;
    let release_block: i64 = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events \
         WHERE logical_name_id = 'basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020' \
           AND event_kind = 'RegistrationReleased' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&released.db.pool)
    .await?;
    // The release anchors at the sync boundary the triggering activity
    // advances — observed one block before the activity transaction itself
    // (the boundary block), never later than it.
    assert!(
        release_block <= activity.register_block as i64,
        "release must settle no later than the triggering activity: \
         release={release_block}, activity={}",
        activity.register_block
    );
    let released_body = exact_name(&released.api, "basenames", "phoenix.base.eth").await?;
    assert_eq!(
        pointer(&released_body, "/declared_state/registration/status"),
        "released"
    );
    released.db.cleanup().await?;

    let (_, premium) =
        basenames::legacy_base_rent_price(&rpc, &deployment, "phoenix", YEAR).await?;
    assert!(
        premium > U256::ZERO,
        "re-registration must occur during premium decay"
    );
    let reregistered =
        basenames::register_base_name(&rpc, &deployment, bob, "phoenix", bob, YEAR).await?;
    let ready_sql = "SELECT count(*) = 2 FROM normalized_events \
         WHERE logical_name_id = 'basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020' \
           AND event_kind = 'RegistrationGranted' \
           AND canonicality_state = 'canonical'";
    let current = support::ingest_basenames_and_serve(&base, &deployment, Some(ready_sql)).await?;

    let transfer_topic = format!(
        "{:#x}",
        keccak256("Transfer(address,address,uint256)".as_bytes())
    );
    let reregister_transfers: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 \
           AND lower(emitting_address) = $2 \
           AND topics[1] = $3 \
           AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&reregistered.register_tx_hash)
    .bind(format!("{:#x}", deployment.base_registrar.address))
    .bind(&transfer_topic)
    .fetch_one(&current.db.pool)
    .await?;
    assert_eq!(
        reregister_transfers, 2,
        "expired re-registration must retain the burn and re-mint raw facts"
    );

    let grant_identities: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT event.resource_id, resource.token_lineage_id \
         FROM normalized_events event \
         JOIN resources resource USING (resource_id) \
         WHERE event.logical_name_id = 'basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020' \
           AND event.event_kind = 'RegistrationGranted' \
           AND event.canonicality_state = 'canonical' \
         ORDER BY event.block_number, event.log_index",
    )
    .fetch_all(&current.db.pool)
    .await?;
    assert_eq!(grant_identities.len(), 2);
    let second_identity = (
        grant_identities[1].0,
        grant_identities[1]
            .1
            .context("second lease has no token lineage")?,
    );
    assert_eq!(
        grant_identities[0],
        (first_identity.0, Some(first_identity.1))
    );
    assert_ne!(first_identity.0, second_identity.0, "resource must rotate");
    assert_ne!(
        first_identity.1, second_identity.1,
        "token lineage must rotate"
    );

    let renewal_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events \
         WHERE transaction_hash = $1 AND event_kind = 'RegistrationRenewed' \
           AND logical_name_id = 'basenames:0x655849ebdf0fdaad2841db2f076a016bacae6d806f08b1d58f3770c13e2c1020' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&renewal.tx_hash)
    .fetch_one(&current.db.pool)
    .await?;
    assert_eq!(renewal_resource, first_identity.0);

    let body = exact_name(&current.api, "basenames", "phoenix.base.eth").await?;
    assert_eq!(
        pointer(&body, "/data/resource_id"),
        second_identity.0.to_string()
    );
    assert_eq!(
        pointer(&body, "/data/token_lineage_id"),
        second_identity.1.to_string()
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{bob:#x}")
    );

    current.db.cleanup().await?;
    Ok(())
}

/// The task-directed bare ERC1967 proxy initializes the pinned controller,
/// while the local manifest preserves distinct proxy and implementation
/// identities and admits events from the proxy role.
/// (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L300 @ basenames@1809bbc)
/// (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/proxy/ERC1967/ERC1967Proxy.sol:L26 @ basenames@1809bbc)
/// (upstream: .refs/basenames/test/Integration/SwitchToUpgradeableRegistrarController.t.sol:L66 @ basenames@1809bbc)
#[tokio::test]
async fn upgradeable_controller_proxy_registers_and_renews() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let root = repo_root();
    let mut deployment = basenames::deploy_basenames(&rpc, &root).await?;
    basenames::deploy_upgradeable_registrar_controller(&rpc, &root, &mut deployment).await?;
    let alice = rpc.accounts().await?[1];

    let registered =
        basenames::register_upgradeable_base_name(&rpc, &deployment, alice, "proxied", alice, YEAR)
            .await?;
    let renewed =
        basenames::renew_upgradeable_base_name(&rpc, &deployment, alice, "proxied", YEAR).await?;
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'basenames:0xf46aa6e99df01bcdde980e8b64aed31cf0eb7515c42f311555c4f7afbc23af98' \
           AND event_kind = 'RegistrationRenewed' \
           AND transaction_hash = '{}' \
           AND canonicality_state = 'canonical')",
        renewed.tx_hash
    );
    let run = support::ingest_basenames_and_serve(&base, &deployment, Some(&ready_sql)).await?;

    let proxy = deployment
        .upgradeable_registrar_controller
        .as_ref()
        .context("proxy deployment missing")?;
    let implementation = deployment
        .upgradeable_registrar_controller_implementation
        .as_ref()
        .context("implementation deployment missing")?;
    let identity: (Uuid, Uuid, String, String, String, String, String) = sqlx::query_as(
        "SELECT proxy.contract_instance_id, proxy.implementation_contract_instance_id, \
                proxy.declared_address, proxy.declared_implementation_address, \
                proxy.proxy_kind, proxy_instance.provenance->>'source', \
                implementation_instance.provenance->>'source' \
         FROM manifest_contract_instances proxy \
         JOIN manifest_versions manifest USING (manifest_id) \
         JOIN contract_instances proxy_instance \
           ON proxy_instance.contract_instance_id = proxy.contract_instance_id \
         JOIN contract_instances implementation_instance \
           ON implementation_instance.contract_instance_id = proxy.implementation_contract_instance_id \
         WHERE manifest.rollout_status = 'active' \
           AND manifest.source_family = 'basenames_base_registrar' \
           AND proxy.role = 'upgradeable_registrar_controller'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_ne!(identity.0, identity.1);
    assert_eq!(identity.2, format!("{:#x}", proxy.address));
    assert_eq!(identity.3, format!("{:#x}", implementation.address));
    assert_eq!(identity.4, "erc1967");
    assert_eq!(identity.5, "manifest_declaration");
    assert_eq!(identity.6, "manifest_proxy_implementation");

    let proxy_edge: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM discovery_edges \
         WHERE edge_kind = 'proxy_implementation' \
           AND from_contract_instance_id = $1 \
           AND to_contract_instance_id = $2 \
           AND deactivated_at IS NULL",
    )
    .bind(identity.0)
    .bind(identity.1)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(proxy_edge, 1);

    for (transaction_hash, event_kind) in [
        (&registered.register_tx_hash, "RegistrationGranted"),
        (&renewed.tx_hash, "RegistrationRenewed"),
    ] {
        let raw_proxy_logs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raw_logs raw \
             JOIN chain_lineage lineage USING (chain_id, block_hash) \
             WHERE transaction_hash = $1 \
               AND lower(emitting_address) = $2 \
               AND lineage.canonicality_state = 'canonical'",
        )
        .bind(transaction_hash)
        .bind(format!("{:#x}", proxy.address))
        .fetch_one(&run.db.pool)
        .await?;
        assert!(raw_proxy_logs >= 1, "proxy fragment was not retained");
        let derived: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events \
             WHERE transaction_hash = $1 AND event_kind = $2 \
               AND logical_name_id = 'basenames:0xf46aa6e99df01bcdde980e8b64aed31cf0eb7515c42f311555c4f7afbc23af98' \
               AND canonicality_state = 'canonical'",
        )
        .bind(transaction_hash)
        .bind(event_kind)
        .fetch_one(&run.db.pool)
        .await?;
        assert_eq!(derived, 1, "proxy-emitted {event_kind} did not decode");
    }
    let implementation_call_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE transaction_hash IN ($1, $2) \
           AND lower(emitting_address) = $3",
    )
    .bind(&registered.register_tx_hash)
    .bind(&renewed.tx_hash)
    .bind(format!("{:#x}", implementation.address))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        implementation_call_logs, 0,
        "delegatecall registration and renewal logs must be emitted by the proxy"
    );

    let body = exact_name(&run.api, "basenames", "proxied.base.eth").await?;
    assert_eq!(
        pointer(&body, "/declared_state/registration/status"),
        "active"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{alice:#x}")
    );

    run.db.cleanup().await?;
    Ok(())
}

/// Base registry child edges carry a labelhash and owner; a separately learned
/// label preimage improves display without minting an exact-name surface.
/// (upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/Registry.sol:L122 @ basenames@1809bbc)
#[tokio::test]
async fn basenames_subnames_list_preimages_placeholders_and_tombstones() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);

    basenames::register_base_name(&rpc, &deployment, alice, "parent", alice, YEAR).await?;
    basenames::register_base_name(&rpc, &deployment, alice, "knownchild", alice, YEAR).await?;
    basenames::set_base_subnode_owner(
        &rpc,
        &deployment,
        alice,
        "parent.base.eth",
        "knownchild",
        bob,
    )
    .await?;
    basenames::set_base_subnode_owner(
        &rpc,
        &deployment,
        alice,
        "parent.base.eth",
        "opaquechild",
        bob,
    )
    .await?;

    let parent_node = format!("{:#x}", ens_v1::namehash("parent.base.eth"));
    let opaque_labelhash = format!("{:#x}", ens_v1::labelhash("opaquechild"));
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'node' = '{parent_node}' \
           AND after_state->>'labelhash' = '{opaque_labelhash}' \
           AND canonicality_state = 'canonical')"
    );
    let initial = support::ingest_basenames_and_serve(&base, &deployment, Some(&ready_sql)).await?;
    let initial_children = children(&initial, "parent.base.eth").await?;
    assert_eq!(initial_children.len(), 2, "children: {initial_children:?}");

    let known = initial_children
        .iter()
        .find(|entry| {
            entry.get("normalized_name").and_then(Value::as_str)
                == Some("knownchild.parent.base.eth")
        })
        .context("known-label child missing")?;
    assert_eq!(
        known.get("owner").and_then(Value::as_str),
        Some(format!("{bob:#x}").as_str())
    );
    let opaque = initial_children
        .iter()
        .find(|entry| entry.get("labelhash").and_then(Value::as_str) == Some(&opaque_labelhash))
        .context("opaque child missing")?;
    let opaque_name = opaque
        .get("normalized_name")
        .and_then(Value::as_str)
        .context("opaque child name missing")?;
    assert!(
        opaque_name.starts_with('[') && opaque_name.ends_with(".parent.base.eth"),
        "expected bracketed child placeholder, got {opaque_name}"
    );
    let (status, _) = initial
        .api
        .get_json("/v1/names/basenames/knownchild.parent.base.eth")
        .await?;
    assert_eq!(
        status, 404,
        "a label preimage must not mint an exact-name surface"
    );
    initial.db.cleanup().await?;

    basenames::set_base_subnode_owner(
        &rpc,
        &deployment,
        alice,
        "parent.base.eth",
        "opaquechild",
        Address::ZERO,
    )
    .await?;
    let zero_owner = format!("{:#x}", Address::ZERO);
    let tombstone_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'node' = '{parent_node}' \
           AND after_state->>'labelhash' = '{opaque_labelhash}' \
           AND lower(after_state->>'owner') = '{zero_owner}' \
           AND canonicality_state = 'canonical')"
    );
    let current =
        support::ingest_basenames_and_serve(&base, &deployment, Some(&tombstone_sql)).await?;
    let current_children = children(&current, "parent.base.eth").await?;
    assert_eq!(current_children.len(), 1, "children: {current_children:?}");
    assert_eq!(
        current_children[0]
            .get("normalized_name")
            .and_then(Value::as_str),
        Some("knownchild.parent.base.eth")
    );

    current.db.cleanup().await?;
    Ok(())
}

/// The admitted L2Resolver emits text, multicoin, name, and version events;
/// its composed contenthash setter remains outside the Base event admission.
/// (upstream: .refs/basenames/src/L2/resolver/TextResolver.sol:L31 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/resolver/NameResolver.sol:L28 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/resolver/ResolverBase.sol:L35 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32 @ basenames@1809bbc)
#[tokio::test]
async fn l2_resolver_records_clear_and_contenthash_gap() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let alice = rpc.accounts().await?[1];
    let resolver = deployment.l2_resolver.address;

    basenames::register_base_name(&rpc, &deployment, alice, "records", alice, YEAR).await?;
    basenames::set_base_text_record(
        &rpc,
        resolver,
        alice,
        "records.base.eth",
        "description",
        "before-clear",
    )
    .await?;
    basenames::set_base_multicoin_addr_record(
        &rpc,
        resolver,
        alice,
        "records.base.eth",
        MULTICOIN_TYPE,
        MULTICOIN_BYTES,
    )
    .await?;
    basenames::set_base_name_record(
        &rpc,
        resolver,
        alice,
        "records.base.eth",
        "records.base.eth",
    )
    .await?;
    basenames::set_base_contenthash_record(
        &rpc,
        resolver,
        alice,
        "records.base.eth",
        CONTENTHASH_BYTES,
    )
    .await?;

    let initial = support::ingest_basenames_and_serve(
        &base,
        &deployment,
        Some(
            "SELECT count(DISTINCT after_state->>'record_key') = 3 \
             FROM normalized_events \
             WHERE logical_name_id = 'basenames:0xfcb7e9e91917e0b14a681580be903b6135c4e0d763185f81ce807bdd708d70ff' \
               AND event_kind = 'RecordChanged' \
               AND after_state->>'record_key' IN ('text:description', 'addr:0', 'name') \
               AND canonicality_state = 'canonical'",
        ),
    )
    .await?;

    let derived_keys: BTreeSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT after_state->>'record_key' FROM normalized_events \
         WHERE logical_name_id = 'basenames:0xfcb7e9e91917e0b14a681580be903b6135c4e0d763185f81ce807bdd708d70ff' \
           AND event_kind = 'RecordChanged' \
           AND source_family = 'basenames_base_resolver' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_all(&initial.db.pool)
    .await?
    .into_iter()
    .collect();
    assert_eq!(
        derived_keys,
        BTreeSet::from([
            "addr:0".to_owned(),
            "name".to_owned(),
            "text:description".to_owned(),
        ])
    );
    let name_value: String = sqlx::query_scalar(
        "SELECT after_state->>'raw_name' FROM normalized_events \
         WHERE logical_name_id = 'basenames:0xfcb7e9e91917e0b14a681580be903b6135c4e0d763185f81ce807bdd708d70ff' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'name' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&initial.db.pool)
    .await?;
    assert_eq!(name_value, "records.base.eth");

    let contenthash_topic = format!(
        "{:#x}",
        keccak256("ContenthashChanged(bytes32,bytes)".as_bytes())
    );
    let raw_contenthash: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs \
         WHERE lower(emitting_address) = $1 AND topics[1] = $2",
    )
    .bind(format!("{resolver:#x}"))
    .bind(&contenthash_topic)
    .fetch_one(&initial.db.pool)
    .await?;
    // Observed asymmetry with the mainnet pubkey pin: on the watched Base
    // resolver instance the unadmitted contenthash RAW log is retained —
    // the profile gate rejects it before derivation instead of the scan
    // dropping it (mainnet's pubkey write persisted no raw log at all).
    assert_eq!(
        raw_contenthash, 1,
        "unadmitted contenthash raw log is retained at the watched instance"
    );
    let contenthash_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'basenames:0xfcb7e9e91917e0b14a681580be903b6135c4e0d763185f81ce807bdd708d70ff' \
           AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'contenthash'",
    )
    .fetch_one(&initial.db.pool)
    .await?;
    assert_eq!(contenthash_events, 0);

    let initial_exact = exact_name(&initial.api, "basenames", "records.base.eth").await?;
    assert_eq!(
        selector_keys(&initial_exact),
        BTreeSet::from(["addr:0".to_owned(), "text:description".to_owned()])
    );
    assert_eq!(
        inventory_reason(&initial_exact, "explicit_gaps", "contenthash", "gap_reason"),
        None,
        "schema-v2 projections do not synthesize the legacy API's explicit contenthash gap"
    );
    let initial_boundary = pointer(
        &initial_exact,
        "/declared_state/record_inventory/record_version_boundary",
    );
    let initial_records = compact_records(
        &initial,
        "records.base.eth",
        "?texts=description&coin_types=0&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&initial_records, "/data/text_records/description/status"),
        "success"
    );
    assert_eq!(
        pointer(&initial_records, "/data/text_records/description/value"),
        "before-clear"
    );
    assert_eq!(
        pointer(&initial_records, "/data/coin_addresses/0/status"),
        "success"
    );
    assert_eq!(
        pointer(&initial_records, "/data/coin_addresses/0/value"),
        json!("0xdeadbeef")
    );
    initial.db.cleanup().await?;

    basenames::clear_base_records(&rpc, resolver, alice, "records.base.eth").await?;
    let ready_sql = support::canonical_event_ready_sql(
        "basenames:0xfcb7e9e91917e0b14a681580be903b6135c4e0d763185f81ce807bdd708d70ff",
        "RecordVersionChanged",
        None,
    );
    let current = support::ingest_basenames_and_serve(&base, &deployment, Some(&ready_sql)).await?;
    let version_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE logical_name_id = 'basenames:0xfcb7e9e91917e0b14a681580be903b6135c4e0d763185f81ce807bdd708d70ff' \
           AND event_kind = 'RecordVersionChanged' \
           AND source_family = 'basenames_base_resolver' \
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&current.db.pool)
    .await?;
    assert_eq!(version_events, 1);

    let current_exact = exact_name(&current.api, "basenames", "records.base.eth").await?;
    let current_boundary = pointer(
        &current_exact,
        "/declared_state/record_inventory/record_version_boundary",
    );
    assert_ne!(current_boundary, initial_boundary);
    let boundary_block = |boundary: &Value| {
        boundary
            .pointer("/chain_position/block_number")
            .and_then(Value::as_i64)
            .unwrap_or_default()
    };
    assert!(boundary_block(&current_boundary) > boundary_block(&initial_boundary));
    let current_records = compact_records(
        &current,
        "records.base.eth",
        "?texts=description&coin_types=0&mode=declared&meta=full",
    )
    .await?;
    assert_ne!(
        pointer(&current_records, "/data/text_records/description/status"),
        "success"
    );
    assert_ne!(
        pointer(&current_records, "/data/coin_addresses/0/status"),
        "success"
    );

    current.db.cleanup().await?;
    Ok(())
}

/// The legacy helper writes the Base reverse registry hierarchy and calls the
/// admitted resolver, but it is not the declared primary-value emitter.
/// (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
#[tokio::test]
async fn legacy_reverse_registrar_stays_registry_and_raw_record_only() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let alice = rpc.accounts().await?[1];
    let alice_path = format!("{alice:#x}");
    let reverse_node = format!("{:#x}", basenames::base_reverse_node_for(alice));

    let claim = basenames::claim_legacy_base_reverse(
        &rpc,
        &deployment,
        alice,
        alice,
        alice,
        deployment.l2_resolver.address,
    )
    .await?;
    let claim_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'child_node' = '{reverse_node}' \
           AND transaction_hash = '{}' \
           AND canonicality_state = 'canonical')",
        claim.tx_hash
    );
    let claimed =
        support::ingest_basenames_and_serve(&base, &deployment, Some(&claim_ready_sql)).await?;
    let claim_registry_edge: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'SubregistryChanged' \
           AND after_state->>'child_node' = $2 \
           AND source_family = 'basenames_base_registry' \
           AND canonicality_state = 'canonical'",
    )
    .bind(&claim.tx_hash)
    .bind(&reverse_node)
    .fetch_one(&claimed.db.pool)
    .await?;
    assert_eq!(
        claim_registry_edge, 1,
        "claim must derive its child assignment on its own"
    );
    claimed.db.cleanup().await?;

    let named = basenames::set_legacy_base_reverse_name(
        &rpc,
        &deployment,
        alice,
        alice,
        alice,
        deployment.l2_resolver.address,
        "legacy.base.eth",
    )
    .await?;

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'child_node' = '{reverse_node}' \
           AND transaction_hash = '{}' \
           AND canonicality_state = 'canonical')",
        named.tx_hash
    );
    let run = support::ingest_basenames_and_serve(&base, &deployment, Some(&ready_sql)).await?;

    for transaction_hash in [&claim.tx_hash, &named.tx_hash] {
        let registry_edges: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events \
             WHERE transaction_hash = $1 \
               AND event_kind = 'SubregistryChanged' \
               AND after_state->>'child_node' = $2 \
               AND source_family = 'basenames_base_registry' \
               AND canonicality_state = 'canonical'",
        )
        .bind(transaction_hash)
        .bind(&reverse_node)
        .fetch_one(&run.db.pool)
        .await?;
        assert_eq!(
            registry_edges, 1,
            "one-shot replay must retain each canonical reverse-child history event"
        );
    }

    let new_owner_topic = format!(
        "{:#x}",
        keccak256("NewOwner(bytes32,bytes32,address)".as_bytes())
    );
    let new_resolver_topic = format!(
        "{:#x}",
        keccak256("NewResolver(bytes32,address)".as_bytes())
    );
    for (topic, expected) in [(&new_owner_topic, 2_i64), (&new_resolver_topic, 1_i64)] {
        let raw_registry_facts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raw_logs raw \
             JOIN chain_lineage lineage USING (chain_id, block_hash) \
             WHERE transaction_hash IN ($1, $2) \
               AND lower(emitting_address) = $3 \
               AND topics[1] = $4 \
               AND lineage.canonicality_state = 'canonical'",
        )
        .bind(&claim.tx_hash)
        .bind(&named.tx_hash)
        .bind(format!("{:#x}", deployment.registry.address))
        .bind(topic)
        .fetch_one(&run.db.pool)
        .await?;
        assert_eq!(raw_registry_facts, expected);
    }
    let normalized_resolver_changes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 \
           AND event_kind = 'ResolverChanged' \
           AND source_family = 'basenames_base_registry' \
           AND logical_name_id IS NULL \
           AND resource_id IS NULL \
           AND after_state->>'node' = $2 \
           AND canonicality_state = 'canonical'",
    )
    .bind(&claim.tx_hash)
    .bind(&reverse_node)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        normalized_resolver_changes, 0,
        "NewResolver for an unknown reverse node must not recreate resolver discovery history"
    );

    let name_changed_topic = format!("{:#x}", keccak256("NameChanged(bytes32,string)".as_bytes()));
    let raw_name_changed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 \
           AND lower(emitting_address) = $2 \
           AND topics[1] = $3 \
           AND topics[2] = $4 \
           AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&named.tx_hash)
    .bind(format!("{:#x}", deployment.l2_resolver.address))
    .bind(&name_changed_topic)
    .bind(&reverse_node)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(raw_name_changed, 1, "reverse NameChanged raw fact missing");

    let normalized_records: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 AND event_kind = 'RecordChanged'",
    )
    .bind(&named.tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        normalized_records, 1,
        "the admitted resolver record is interpreted, independently of primary-name eligibility"
    );
    let primary_sources: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash = $1 AND event_kind = 'RecordChanged' \
           AND after_state ? 'primary_claim_source'",
    )
    .bind(&named.tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        primary_sources, 0,
        "a generic resolver NameChanged must not mint a primary-name claim"
    );
    let reverse_claims: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash IN ($1, $2) AND event_kind = 'ReverseChanged'",
    )
    .bind(&claim.tx_hash)
    .bind(&named.tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(reverse_claims, 0);

    let primary_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM primary_names_current \
         WHERE address = $1 AND namespace = 'basenames' \
           AND coin_type = '2147492101'",
    )
    .bind(&alice_path)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        primary_rows, 0,
        "helper path must not mint a primary candidate"
    );
    let primary = primary_name(
        &run.api,
        "basenames",
        basenames::BASE_PRIMARY_COIN_TYPE,
        &alice_path,
        "declared",
    )
    .await?;
    assert_eq!(
        pointer(&primary, "/declared_state/claimed_primary_name/status"),
        "not_found"
    );

    let child_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM children_current WHERE namehash = $1")
            .bind(&reverse_node)
            .fetch_one(&run.db.pool)
            .await?;
    assert_eq!(
        child_rows, 0,
        "REVIEW POINT: the unknown reverse parent has no child projection"
    );

    run.db.cleanup().await?;
    Ok(())
}

/// An owner-added controller can call BaseRegistrar `register` or
/// `registerOnly` without a label-bearing controller event.
/// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L203 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L237 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L248 @ basenames@1809bbc)
#[tokio::test]
async fn third_party_controller_registration_degrades_without_label_events() -> Result<()> {
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (controller, alice, bob) = (accounts[3], accounts[1], accounts[2]);

    basenames::add_base_controller(&rpc, &deployment, controller).await?;
    let direct = basenames::direct_register_base_name(
        &rpc,
        &deployment,
        controller,
        "thirdparty",
        alice,
        YEAR,
    )
    .await?;
    let token_only = basenames::direct_register_base_name_only(
        &rpc,
        &deployment,
        controller,
        "tokenonly",
        bob,
        YEAR,
    )
    .await?;
    assert_eq!(
        basenames::base_registry_owner(&rpc, &deployment, "thirdparty.base.eth").await?,
        alice
    );
    assert_eq!(
        basenames::base_registry_owner(&rpc, &deployment, "tokenonly.base.eth").await?,
        Address::ZERO
    );

    let thirdparty_node = format!("{:#x}", ens_v1::namehash("thirdparty.base.eth"));
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'SubregistryChanged' \
           AND after_state->>'child_node' = '{thirdparty_node}' \
           AND transaction_hash = '{}' \
           AND canonicality_state = 'canonical')",
        direct.tx_hash
    );
    let run = support::ingest_basenames_and_serve(&base, &deployment, Some(&ready_sql)).await?;
    let transfer_topic = format!(
        "{:#x}",
        keccak256("Transfer(address,address,uint256)".as_bytes())
    );
    for transaction_hash in [&direct.tx_hash, &token_only.tx_hash] {
        let raw_mints: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raw_logs raw \
             JOIN chain_lineage lineage USING (chain_id, block_hash) \
             WHERE transaction_hash = $1 \
               AND lower(emitting_address) = $2 \
               AND topics[1] = $3 \
               AND lineage.canonicality_state = 'canonical'",
        )
        .bind(transaction_hash)
        .bind(format!("{:#x}", deployment.base_registrar.address))
        .bind(&transfer_topic)
        .fetch_one(&run.db.pool)
        .await?;
        assert_eq!(raw_mints, 1, "registrar mint raw fact missing");
    }

    let direct_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         ORDER BY event_kind",
    )
    .bind(&direct.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        direct_kinds,
        vec![
            "AuthorityTransferred".to_owned(),
            "PermissionChanged".to_owned(),
            "SubregistryChanged".to_owned(),
        ],
        "direct register degraded shape changed"
    );
    let token_only_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE transaction_hash = $1 AND canonicality_state = 'canonical' \
         ORDER BY event_kind",
    )
    .bind(&token_only.tx_hash)
    .fetch_all(&run.db.pool)
    .await?;
    assert!(
        token_only_kinds.is_empty(),
        "registerOnly must derive no named registry or lease facts: {token_only_kinds:?}"
    );

    let direct_registry_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 AND lower(emitting_address) = $2 \
           AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&direct.tx_hash)
    .bind(format!("{:#x}", deployment.registry.address))
    .fetch_one(&run.db.pool)
    .await?;
    let token_only_registry_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs raw \
         JOIN chain_lineage lineage USING (chain_id, block_hash) \
         WHERE transaction_hash = $1 AND lower(emitting_address) = $2 \
           AND lineage.canonicality_state = 'canonical'",
    )
    .bind(&token_only.tx_hash)
    .bind(format!("{:#x}", deployment.registry.address))
    .fetch_one(&run.db.pool)
    .await?;
    assert!(direct_registry_logs >= 1);
    assert_eq!(token_only_registry_logs, 0);

    let named_surfaces: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_surfaces \
         WHERE logical_name_id IN ( \
           'basenames:0xdfa08232ef9f414a648791b8e277797475e1ae7c4a02739377776fe6998564c5', \
           'basenames:0x3f9791eaf26cb507f9d00cb5ea9a3ec77658a401eabca6e68de2e8519771a06e' \
         )",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        named_surfaces, 0,
        "label-less paths must not mint exact surfaces"
    );
    for name in ["thirdparty.base.eth", "tokenonly.base.eth"] {
        let (status, body) = run
            .api
            .get_json(&format!("/v1/names/basenames/{name}"))
            .await?;
        assert_eq!(
            status, 404,
            "label-less registration must not become an exact-name route: {body}"
        );
    }
    let grants: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events \
         WHERE transaction_hash IN ($1, $2) \
           AND event_kind IN ('RegistrationGranted', 'TokenControlTransferred')",
    )
    .bind(&direct.tx_hash)
    .bind(&token_only.tx_hash)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(grants, 0);

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn l2_zero_addr60_uses_stubbed_verified_transport() -> Result<()> {
    let eth = Anvil::spawn().await?;
    let l1_resolver =
        Address::from_slice(&keccak256("bigname-e2e-placeholder:l1_resolver".as_bytes())[12..]);
    // Fixed-answer verified transport stub; mine it before either chain advances.
    eth.client()
        .set_code(
            l1_resolver,
            &[
                0x60, 0x20, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x20, 0x52, 0x60, 0x60, 0x60, 0x00,
                0xf3,
            ],
        )
        .await?;
    eth.client().mine(1).await?;
    let base = Anvil::spawn_base_mainnet().await?;
    let rpc = base.client();
    let ens_deployment = ens_v1::deploy_ens_v1(&eth.client(), &repo_root()).await?;
    let deployment = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, nonzero) = (accounts[1], accounts[2]);
    let name = "zero-address-682.base.eth";
    let node = format!("{:#x}", ens_v1::namehash(name));
    let logical_name_id = format!("basenames:{node}");

    basenames::register_base_name(&rpc, &deployment, alice, "zero-address-682", alice, YEAR)
        .await?;
    basenames::set_base_registry_resolver(
        &rpc,
        &deployment,
        alice,
        name,
        deployment.l2_resolver.address,
    )
    .await?;
    basenames::set_addr_record(&rpc, &deployment, alice, name, nonzero).await?;
    basenames::set_addr_record(&rpc, &deployment, alice, name, Address::ZERO).await?;
    let resolver = deployment.l2_resolver.address;
    ens_v1::set_multicoin_addr_record(&rpc, resolver, alice, name, 2147483648, nonzero.as_slice())
        .await?;
    for (coin, expected) in [
        (None, Address::ZERO.as_slice()),
        (Some(60), Address::ZERO.as_slice()),
        (Some(2147483648), nonzero.as_slice()),
        (Some(2147483649), &[]),
    ] {
        assert_eq!(
            ens_v1::read_addr_record(&rpc, resolver, name, coin).await?,
            expected
        );
    }
    assert!(
        ens_v1::read_addr_record(&rpc, resolver, "fresh-zero-control.base.eth", Some(60))
            .await?
            .is_empty()
    );

    let zero = format!("{:#x}", Address::ZERO);
    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events WHERE after_state->>'node' = '{node}' \
         AND event_kind = 'RecordChanged' AND after_state->>'record_key' = 'addr:60' \
         AND after_state->>'value' = '{zero}' AND canonicality_state = 'canonical')"
    );
    let run = support::ingest_mainnet_composed_and_serve(
        &eth,
        &ens_deployment,
        &base,
        &deployment,
        Some(&ready_sql),
    )
    .await?;
    let rows: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT after_state->>'source_event', transaction_hash, log_index, after_state->>'value' \
         FROM normalized_events WHERE after_state->>'node' = $1 AND event_kind = 'RecordChanged' \
         AND after_state->>'record_key' = 'addr:60' AND after_state->>'value' = $2 \
         AND canonicality_state = 'canonical' ORDER BY log_index",
    )
    .bind(&node)
    .bind(&zero)
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(rows.len(), 2, "zero transaction rows: {rows:?}");
    assert_eq!(rows[0].0, "AddressChanged");
    assert_eq!(rows[1].0, "AddrChanged");
    assert_eq!(rows[0].1, rows[1].1);
    assert_eq!(rows[1].2, rows[0].2 + 1);
    assert!(rows.iter().all(|row| row.3 == zero));

    assert_default_inventory(&run, &logical_name_id, false).await?;
    enable_basenames_verified_route(&run, &logical_name_id, l1_resolver).await?;
    super::resolver_records::normalize_v2_snapshot_timestamp(&run, &logical_name_id).await?;
    let api = super::resolver_records::start_v2_api(
        &run,
        &[
            ("base-mainnet", base.url.as_str()),
            ("ethereum-mainnet", eth.url.as_str()),
        ],
    )
    .await?;
    let indexed = v2_records(&api, name, "indexed").await?;
    let before: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE cleared_at IS NULL) \
         FROM resolution_divergences WHERE logical_name_id = $1 AND request_kind = 'addr:60'",
    )
    .bind(&logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    let verified = v2_records(&api, name, "verified").await?;
    let verified_repeat = v2_records(&api, name, "verified").await?;
    let auto = v2_records(&api, name, "auto").await?;
    let after: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE cleared_at IS NULL) \
         FROM resolution_divergences WHERE logical_name_id = $1 AND request_kind = 'addr:60'",
    )
    .bind(&logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        json!({
            "indexed": addr60_observation(&indexed),
            "verified": addr60_observation(&verified),
            "verified_repeat": addr60_observation(&verified_repeat),
            "auto": addr60_observation(&auto),
            "divergence_before": before, "divergence_after": after
        }),
        json!({
            "indexed":{"record":{"status":"not_found", "value":null}, "address":null, "primary_address":null},
            "verified":{"record":{"status":"not_found", "value":null}, "address":null, "primary_address":null},
            "verified_repeat":{"record":{"status":"not_found", "value":null}, "address":null, "primary_address":null},
            "auto":{"record":{"status":"not_found", "value":null}, "address":null, "primary_address":null},
            "divergence_before":[0,0], "divergence_after":[0,0]
        })
    );
    for body in [&indexed, &verified, &verified_repeat, &auto] {
        assert_addr60_not_found(body);
    }

    super::resolver_records::assert_zero_api_shapes(
        &run,
        &api,
        "basenames",
        name,
        &logical_name_id,
    )
    .await?;
    drop(api);
    run.db.cleanup().await?;
    Ok(())
}

pub(crate) async fn assert_default_inventory(
    run: &support::PipelineRun,
    logical_name: &str,
    fallback: bool,
) -> Result<()> {
    let (classification, provenance, entries): (Value, Value, Value) = sqlx::query_as(
        "SELECT resolver.declared_summary->'classification', inventory.provenance, inventory.entries
        FROM record_inventory_current inventory JOIN resolver_current resolver
        ON resolver.chain_id = inventory.provenance->>'chain_id'
        AND resolver.resolver_address = inventory.provenance->>'resolver_address'
        WHERE inventory.provenance->>'logical_name_id' = $1",
    ).bind(logical_name).fetch_one(&run.db.pool).await?;
    let features = if fallback {
        vec!["ensip19_default_address"]
    } else {
        Vec::new()
    };
    assert_eq!(classification["read_features"], json!(features));
    assert_eq!(
        provenance["read_rules"],
        if fallback {
            json!([{
                "kind":"ensip19_default_address", "source_record_key":"addr:2147483648"
            }])
        } else {
            json!([])
        }
    );
    assert_eq!(
        provenance["exact_nonempty_not_found_record_keys"],
        json!(["addr:60"])
    );
    let default = entries
        .as_array()
        .context("produced entries")?
        .iter()
        .find(|entry| entry["record_key"] == "addr:2147483648")
        .context("produced default")?;
    assert_eq!(default["status"], "success");
    Ok(())
}
