use alloy_primitives::Address;
use anyhow::{Result, ensure};
use serde_json::{Value, json};

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const TEXT_KEY: &str = "description";
const TEXT_VALUE: &str = "selected before the surface";

async fn exercise(ownerless: bool) -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (controller, owner, address_value) = (accounts[1], accounts[2], accounts[3]);
    let label = if ownerless {
        "ownerlesspresurface"
    } else {
        "ownedpresurface"
    };
    let name = format!("{label}.eth");
    let node = format!("{:#x}", ens_v1::namehash(&name));
    let logical_name_id = support::schema_v2_logical_name_id(&format!("ens:{node}"));
    let resolver = deployment.public_resolver.address;

    ens_v1::add_registrar_controller(&rpc, &deployment, controller).await?;
    ens_v1::register_via_registrar(&rpc, &deployment, controller, label, owner, YEAR).await?;
    ens_v1::set_resolver(&rpc, &deployment, owner, &name, resolver).await?;
    ens_v1::set_text_record(&rpc, resolver, owner, &name, TEXT_KEY, TEXT_VALUE).await?;
    ens_v1::set_addr_record(&rpc, resolver, owner, &name, address_value).await?;
    if ownerless {
        ens_v1::set_registry_owner(&rpc, &deployment, owner, &name, Address::ZERO).await?;
    }
    ens_v1::renew_eth_name(&rpc, &deployment, owner, label, YEAR).await?;

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{logical_name_id}' \
           AND event_kind = 'ResolverChanged' \
           AND after_state->>'state_derived' = 'true' \
           AND after_state->>'resolver' = '{resolver:#x}')"
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let (status, name_body) = run.api.get_json(&format!("/v1/names/ens/{name}")).await?;
    assert_eq!(status, 200, "name route failed: {name_body}");
    assert_eq!(name_body["support_status"], json!("supported"));
    assert_eq!(name_body["data"]["namespace"], json!("ens"));
    assert_eq!(
        name_body["declared_state"]["resolver"]["address"],
        format!("{resolver:#x}")
    );
    assert_eq!(
        name_body["declared_state"]["resolver"]["chain_id"],
        "ethereum-mainnet"
    );

    let (status, records) = run
        .api
        .get_json(&format!(
            "/v1/names/ens/{name}/records?texts={TEXT_KEY}&coin_types=60&include=inventory"
        ))
        .await?;
    assert_eq!(status, 200, "records route failed: {records}");
    assert_eq!(
        records["data"]["resolver_address"],
        format!("{resolver:#x}")
    );
    let projected_entries = records["declared_state"]["record_inventory"]["entries"].clone();
    let inventory_keys = projected_entries
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry["record_key"].as_str())
        .collect::<Vec<_>>();
    ensure!(
        inventory_keys.contains(&"text:description") && inventory_keys.contains(&"addr:60"),
        "inventory keys missing: {records}"
    );
    let record_value = |key: &str| {
        projected_entries
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| entry["record_key"] == key)
            .and_then(|entry| entry["value"].as_str())
    };
    let expected_address = format!("{address_value:#x}");
    assert_eq!(record_value("text:description"), Some(TEXT_VALUE));
    assert_eq!(record_value("addr:60"), Some(expected_address.as_str()));
    assert_eq!(
        records["data"]["text_records"][TEXT_KEY]["status"],
        "success"
    );
    assert_eq!(records["data"]["coin_addresses"]["60"]["status"], "success");

    let control_bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings \
         WHERE chain_id = 'ethereum-mainnet' AND logical_name_id = $1 AND active_to IS NULL",
    )
    .bind(&logical_name_id)
    .fetch_one(&run.db.pool)
    .await?;
    if ownerless {
        assert_eq!(
            name_body["declared_state"]["registration"]["status"],
            "unregistered"
        );
        assert_eq!(
            name_body["declared_state"]["control"]["status"],
            "unregistered"
        );
        assert_eq!(name_body["data"]["resource_id"], Value::Null);
        assert_eq!(name_body["data"]["token_lineage_id"], Value::Null);
        assert_eq!(name_body["data"]["binding_kind"], Value::Null);
        for field in [
            "registry_owner",
            "manager",
            "registrant",
            "registration_id",
            "token_id",
        ] {
            assert!(
                name_body["declared_state"]["registration"]
                    .get(field)
                    .is_none_or(Value::is_null)
                    && name_body["declared_state"]["control"]
                        .get(field)
                        .is_none_or(Value::is_null),
                "ownerless field {field} must be absent: {name_body}"
            );
        }
        assert_eq!(control_bindings, 0);
    } else {
        assert_eq!(
            name_body["declared_state"]["registration"]["status"],
            "active"
        );
        assert_eq!(
            name_body["declared_state"]["control"]["registry_owner"],
            format!("{owner:#x}")
        );
        assert_eq!(
            name_body["declared_state"]["control"]["status"],
            Value::Null
        );
        assert!(name_body["data"]["resource_id"].is_string());
        assert_eq!(name_body["data"]["token_lineage_id"], Value::Null);
        assert_eq!(control_bindings, 1);
    }

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn owned_pre_surface_resolver_records_serve_after_late_renewal_without_reselection()
-> Result<()> {
    exercise(false).await
}

#[tokio::test]
async fn ownerless_pre_surface_resolver_records_serve_after_late_renewal_without_reselection()
-> Result<()> {
    exercise(true).await
}
