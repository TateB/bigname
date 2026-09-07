use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};

use super::support;
use crate::harness::responses::{exact_name, pointer, selector_keys};
use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const MULTICOIN_TYPE: u64 = 0;
const ENSIP19_DEFAULT_COIN_TYPE: u64 = 1 << 31;
const MULTICOIN_BYTES: &[u8] = &[0xde, 0xad, 0xbe, 0xef];
const CONTENTHASH_BYTES: &[u8] = &[0xe3, 0x01, 0x01, 0x70, 0x12, 0x20];
const MULTICOIN_HEX: &str = "0xdeadbeef";
const CONTENTHASH_HEX: &str = "0xe30101701220";

fn boundary(body: &Value) -> Result<Value> {
    body.pointer("/declared_state/record_inventory/record_version_boundary")
        .cloned()
        .context("exact-name response is missing record_version_boundary")
}

async fn compact_records(run: &support::PipelineRun, name: &str, query: &str) -> Result<Value> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/names/ens/{name}/records{query}"))
        .await?;
    assert_eq!(status, 200, "records lookup for {name} failed: {body}");
    Ok(body)
}

pub(super) struct V2Api {
    child: Child,
    pub(super) base_url: String,
    pub(super) client: reqwest::Client,
}

impl Drop for V2Api {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(super) async fn start_v2_api(
    run: &support::PipelineRun,
    chain_rpc_urls: &[(&str, &str)],
) -> Result<V2Api> {
    let root = repo_root();
    let cargo = std::env::var_os("BIGNAME_E2E_REAL_CARGO")
        .or_else(|| std::env::var_os("CARGO"))
        .unwrap_or_else(|| "cargo".into());
    let status = tokio::process::Command::new(cargo)
        .current_dir(&root)
        .args("build --locked -p bigname-api --bin=bigname-api".split_whitespace())
        .status()
        .await?;
    ensure!(status.success(), "build real API binary for e2e");
    let target = root.join(std::env::var_os("CARGO_TARGET_DIR").unwrap_or_else(|| "target".into()));
    let binary = target.join("debug/bigname-api");
    ensure!(binary.is_file(), "API binary missing at {binary:?}");
    let ready_timeout_secs = pipeline::ready_timeout_secs()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(ready_timeout_secs);
    let mut last_exit = None;
    for _ in 0..5 {
        let _startup_guard =
            tokio::time::timeout_at(deadline, crate::harness::lock_local_server_start())
                .await
                .context("local-server startup lock exceeded the API readiness deadline")?;
        let bind_addr = free_addr()?;
        let mut command = Command::new(&binary);
        command
            .current_dir(&root)
            .args(["serve", "--bind-addr"])
            .arg(&bind_addr)
            .args(["--metrics-bind-addr", "127.0.0.1:0", "--database-url"])
            .arg(&run.db.url)
            .args(["--max-connections", "4"])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        for (chain, url) in chain_rpc_urls {
            command.args(["--chain-rpc-url", &format!("{chain}={url}")]);
        }
        let mut api = V2Api {
            child: command.spawn()?,
            base_url: format!("http://{bind_addr}"),
            client: reqwest::Client::new(),
        };
        let health_url = format!("{}/healthz", api.base_url);
        loop {
            let ready = matches!(
                tokio::time::timeout_at(deadline, api.client.get(&health_url).send()).await,
                Ok(Ok(response)) if response.status().is_success()
            );
            ensure!(
                tokio::time::Instant::now() < deadline,
                "real API readiness exceeded configured {ready_timeout_secs}s at {}",
                api.base_url
            );
            if let Some(status) = api.child.try_wait()? {
                last_exit = Some(status);
                break;
            }
            if ready {
                return Ok(api);
            }
            let _ =
                tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(100)))
                    .await;
        }
    }
    bail!("real API exited before readiness in 5 attempts; last exit {last_exit:?}")
}

fn free_addr() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.to_string())
}

async fn v2_records(api: &V2Api, name: &str, source: &str) -> Result<Value> {
    let response = api
        .client
        .get(format!(
            "{}/v2/names/{name}/records?namespace=ens&source={source}&keys=addr:60",
            api.base_url
        ))
        .send()
        .await?;
    let status = response.status();
    let body: Value = response.json().await?;
    assert_eq!(status, 200, "v2 records lookup failed: {body}");
    Ok(body)
}

fn assert_addr60_not_found(body: &Value) {
    assert_eq!(
        pointer(body, "/data/records/addr:60/status"),
        "not_found",
        "addr:60 should be absent: {body}"
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

async fn enable_ens_verified_route(
    run: &support::PipelineRun,
    logical_name_id: &str,
    universal: Address,
) -> Result<()> {
    // #857 workaround: without this scenario-local execution manifest, verified/auto are unsupported.
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions \
             (manifest_version, namespace, source_family, chain_id, deployment_label, \
              rollout_status, normalizer_version, file_path, manifest_payload) \
         VALUES (1, 'ens', 'ens_execution', 'ethereum-mainnet', 'ens_v1', \
                 'shadow', 'e2e', 'e2e/ens_execution/v1.toml', \
                 jsonb_build_object('capability_flags', jsonb_build_object( \
                     'verified_resolution', jsonb_build_object('status', 'shadow')))) \
         RETURNING manifest_id",
    )
    .fetch_one(&run.db.pool)
    .await?;
    // #857 workaround: the local execution declaration needs an addressable instance.
    sqlx::query(
        "INSERT INTO contract_instances \
             (contract_instance_id, chain_id, contract_kind) \
         VALUES ('68200000-0000-0000-0000-000000000001', \
                 'ethereum-mainnet', 'contract')",
    )
    .execute(&run.db.pool)
    .await?;
    // #857 workaround: without this role row, verified lookup has no local entrypoint.
    sqlx::query(
        "INSERT INTO manifest_contract_instances \
             (manifest_id, chain_id, declaration_kind, declaration_name, \
              contract_instance_id, declared_address, role, proxy_kind, \
              start_block_number) \
         VALUES ($1, 'ethereum-mainnet', 'contract', 'universal_resolver', \
                 '68200000-0000-0000-0000-000000000001', $2, \
                 'universal_resolver', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(format!("{universal:#x}"))
    .execute(&run.db.pool)
    .await?;
    // #857 workaround: Project does not publish this executable scenario topology.
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
                    'topology_version_boundary', inventory.record_version_boundary,
                    'record_version_boundary', inventory.record_version_boundary
                ),
                'transport', jsonb_build_object(
                    'source_chain_id', NULL, 'target_chain_id', NULL,
                    'contract_address', NULL, 'latest_event_kind', NULL
                )
            ),
            true
        )
        FROM record_inventory_current inventory
        WHERE name.logical_name_id = $1
          AND inventory.resource_id = name.resource_id
        "#,
    )
    .bind(logical_name_id)
    .execute(&run.db.pool)
    .await?;
    ensure!(
        topology.rows_affected() == 1,
        "install ENS verified topology for {logical_name_id}"
    );
    Ok(())
}

fn universal_zero_runtime(resolver: Address) -> Vec<u8> {
    let mut runtime = vec![0x60, 0x40, 0x60, 0x00, 0x52, 0x73];
    runtime.extend_from_slice(resolver.as_slice());
    runtime.extend_from_slice(&[
        0x60, 0x20, 0x52, 0x60, 0x20, 0x60, 0x40, 0x52, 0x60, 0x00, 0x60, 0x60, 0x52, 0x60, 0x80,
        0x60, 0x00, 0xf3,
    ]);
    runtime
}

async fn divergence_counts(
    run: &support::PipelineRun,
    logical_name_id: &str,
) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE cleared_at IS NULL) \
         FROM resolution_divergences \
         WHERE logical_name_id = $1 AND request_kind = 'addr:60'",
    )
    .bind(logical_name_id)
    .fetch_one(&run.db.pool)
    .await?)
}

pub(super) async fn normalize_v2_snapshot_timestamp(
    run: &support::PipelineRun,
    logical_name_id: &str,
) -> Result<()> {
    if logical_name_id.starts_with("basenames:") {
        // #857 workaround: without an Ethereum execution position, verified reads are stale.
        sqlx::query(
            "UPDATE name_current SET chain_positions = chain_positions || jsonb_build_object( \
                 'ethereum', (SELECT jsonb_build_object( \
                     'chain_id', lineage.chain_id, 'block_number', lineage.block_number, \
                     'block_hash', lineage.block_hash, 'timestamp', \
                     to_char(lineage.block_timestamp AT TIME ZONE 'UTC', \
                             'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')) \
                 FROM chain_lineage lineage \
                 WHERE lineage.chain_id = 'ethereum-mainnet' \
                   AND lineage.block_timestamp <= \
                       (name_current.chain_positions #>> '{base,timestamp}')::timestamptz \
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized') \
                 ORDER BY lineage.block_timestamp DESC, lineage.block_number DESC, \
                          lineage.block_hash DESC LIMIT 1)) \
             WHERE logical_name_id = $1",
        )
        .bind(logical_name_id)
        .execute(&run.db.pool)
        .await?;
    }
    Ok(())
}

fn assert_resolver(body: &Value, resolver: Address) {
    assert_eq!(
        pointer(body, "/declared_state/resolver/address"),
        format!("{resolver:#x}"),
        "declared resolver should match current registry binding; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/resolver/chain_id"),
        "ethereum-mainnet"
    );
    assert_eq!(
        pointer(body, "/declared_state/resolver/latest_event_kind"),
        "ResolverChanged"
    );
}

fn assert_no_resolver(body: &Value) {
    assert_eq!(
        pointer(body, "/declared_state/resolver/address"),
        Value::Null,
        "zero resolver should use the supported null resolver shape; body: {body}"
    );
    assert_eq!(
        pointer(body, "/declared_state/resolver/chain_id"),
        Value::Null
    );
}

fn assert_compact_record_not_success(body: &Value, path: &str) {
    assert_eq!(
        pointer(body, &format!("{path}/status")),
        "not_found",
        "old resolver cache must become not_found at {path}; body: {body}"
    );
    assert!(
        body.pointer(&format!("{path}/value")).is_none(),
        "not_found record must omit value at {path}; body: {body}"
    );
}

pub(super) async fn start_split_replay(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    target: u64,
    ready_sql: &str,
) -> Result<(
    HarnessDb,
    support::TempDir,
    pipeline::SequentialFixtureReplay,
)> {
    let root = repo_root();
    let scratch = support::TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let mut replay = pipeline::SequentialFixtureReplay::start_with_chain_rpc_urls(
        &root,
        &db.url,
        &profile.root,
        &chain_rpc_urls,
    )
    .await?;
    replay
        .replay_chain_through(&db.pool, "ethereum-mainnet", target, Some(ready_sql))
        .await?;
    Ok((db, scratch, replay))
}

pub(super) async fn materialize_wrapped_surface(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
    name: &str,
    previous_target: u64,
    db: &HarnessDb,
    replay: &mut pipeline::SequentialFixtureReplay,
) -> Result<u64> {
    let rpc = anvil.client();
    ens_v1::set_name_record_for_node(
        &rpc,
        deployment.public_resolver.address,
        owner,
        ens_v1::namehash(name),
        name,
    )
    .await?;
    ens_v1::set_registry_approval_for_all(
        &rpc,
        deployment,
        owner,
        deployment.name_wrapper.address,
        true,
    )
    .await?;
    ens_v1::wrap_registry_name(&rpc, deployment, owner, name, owner, Address::ZERO).await?;
    rpc.mine(1).await?;
    let target = rpc.block_number().await?;
    replay
        .replay_chain_range(
            &db.pool,
            "ethereum-mainnet",
            previous_target + 1,
            target,
            Some(&format!(
                "SELECT EXISTS (SELECT 1 FROM name_surfaces WHERE logical_name_id = '{}')",
                support::schema_v2_logical_name_id(&format!("ens:{name}"))
            )),
        )
        .await?;
    Ok(target)
}

pub(super) async fn select_wrapped_resolver(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
    selection: (&str, Address),
    previous_target: u64,
    db: &HarnessDb,
    replay: &mut pipeline::SequentialFixtureReplay,
) -> Result<u64> {
    let (name, resolver) = selection;
    let rpc = anvil.client();
    ens_v1::set_wrapped_resolver(&rpc, deployment, owner, name, resolver).await?;
    rpc.mine(1).await?;
    let target = rpc.block_number().await?;
    replay
        .replay_chain_range(
            &db.pool,
            "ethereum-mainnet",
            previous_target + 1,
            target,
            None,
        )
        .await?;
    Ok(target)
}

async fn materialize_and_select_wrapped_resolver(
    anvil: &Anvil,
    deployment: &ens_v1::EnsV1Deployment,
    owner: Address,
    selection: (&str, Address),
    previous_target: u64,
    db: &HarnessDb,
    replay: &mut pipeline::SequentialFixtureReplay,
) -> Result<u64> {
    let surface_target = materialize_wrapped_surface(
        anvil,
        deployment,
        owner,
        selection.0,
        previous_target,
        db,
        replay,
    )
    .await?;
    select_wrapped_resolver(
        anvil,
        deployment,
        owner,
        selection,
        surface_target,
        db,
        replay,
    )
    .await
}

#[tokio::test]
async fn resolver_changes_follow_registry_and_zero_releases() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let second_resolver = ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    let alice = rpc.accounts().await?[1];
    let first_resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "flip", alice, YEAR, first_resolver).await?;

    {
        let ready_sql = support::canonical_event_ready_sql(
            "ens:0x1973cc0d7ca356c07f68eae6cb7ca41dc66e9d1552a607d2fe446fd0d3fc9804",
            "ResolverChanged",
            None,
        );
        let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
        let body = exact_name(&run.api, "ens", "flip.eth").await?;
        assert_resolver(&body, first_resolver);
        run.db.cleanup().await?;
    }

    ens_v1::set_resolver(
        &rpc,
        &deployment,
        alice,
        "flip.eth",
        second_resolver.address,
    )
    .await?;

    {
        let ready_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:0x1973cc0d7ca356c07f68eae6cb7ca41dc66e9d1552a607d2fe446fd0d3fc9804' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical' \
             AND lower(after_state->>'resolver') = '{:#x}')",
            second_resolver.address
        );
        let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
        let body = exact_name(&run.api, "ens", "flip.eth").await?;
        assert_resolver(&body, second_resolver.address);
        run.db.cleanup().await?;
    }

    ens_v1::set_resolver(&rpc, &deployment, alice, "flip.eth", Address::ZERO).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT count(*) >= 3 FROM normalized_events \
             WHERE logical_name_id = 'ens:0x1973cc0d7ca356c07f68eae6cb7ca41dc66e9d1552a607d2fe446fd0d3fc9804' AND event_kind = 'ResolverChanged' \
             AND canonicality_state = 'canonical'",
        ),
    )
    .await?;
    let body = exact_name(&run.api, "ens", "flip.eth").await?;
    assert_no_resolver(&body);

    let records = compact_records(
        &run,
        "flip.eth",
        "?include=resolver_address,content_hash,coins&coin_types=60&content_hash=true",
    )
    .await?;
    assert_eq!(
        pointer(&records, "/data/resolver_address"),
        Value::Null,
        "records route should expose the same null resolver shape after zeroing; body: {records}"
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/60/status"),
        "not_found"
    );
    assert_eq!(pointer(&records, "/data/content_hash/status"), "not_found");

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_newowner_record_serves_after_late_surface() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let alice = rpc.accounts().await?[1];
    let resolver = deployment.public_resolver.address;
    let name = "before.known.eth";
    let node = format!("{:#x}", ens_v1::namehash(name));

    ens_v1::register_eth_name(&rpc, &deployment, "known", alice, YEAR, resolver).await?;
    ens_v1::create_subname(&rpc, &deployment, alice, "known.eth", "before", alice).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        alice,
        name,
        "description",
        "written before the surface",
    )
    .await?;
    let null_link_ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:description' \
           AND lower(after_state->>'node') = lower('{node}') \
           AND logical_name_id IS NULL)"
    );
    let record_target = rpc.block_number().await?;
    let (db, scratch, mut replay) =
        start_split_replay(&anvil, &deployment, record_target, &null_link_ready_sql).await?;
    materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        alice,
        (name, resolver),
        record_target,
        &db,
        &mut replay,
    )
    .await?;
    let run = support::serve_existing_db(db, scratch, &anvil).await?;

    let (linked_name, retained_node, emitting_resolver): (Option<String>, String, String) =
        sqlx::query_as(
            "SELECT logical_name_id, after_state->>'node', \
                    COALESCE(after_state->>'resolver', raw_fact_ref->>'emitting_address') \
             FROM normalized_events \
             WHERE event_kind = 'RecordChanged' \
               AND after_state->>'record_key' = 'text:description' \
               AND lower(after_state->>'node') = lower($1)",
        )
        .bind(&node)
        .fetch_one(&run.db.pool)
        .await?;
    assert_eq!(
        linked_name, None,
        "the interpretation-time name link must stay null"
    );
    assert_eq!(retained_node.to_lowercase(), node);
    assert_eq!(emitting_resolver.to_lowercase(), format!("{resolver:#x}"));

    let exact = exact_name(&run.api, "ens", name).await?;
    assert!(
        selector_keys(&exact).contains("text:description"),
        "late surface must recover the earlier record in inventory: {exact}"
    );
    let records = compact_records(&run, name, "?texts=description&mode=declared&meta=full").await?;
    assert_eq!(
        pointer(&records, "/data/text_records/description/value"),
        "written before the surface"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_record_history_follows_current_resolver_and_version_boundary() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let resolver_two =
        ens_v1::deploy_extra_public_resolver(&rpc, &repo_root(), &deployment).await?;
    let owner = rpc.accounts().await?[1];
    let resolver_one = deployment.public_resolver.address;
    let name = "version.history.eth";
    let node = format!("{:#x}", ens_v1::namehash(name));

    ens_v1::register_eth_name(&rpc, &deployment, "history", owner, YEAR, resolver_one).await?;
    ens_v1::create_subname(&rpc, &deployment, owner, "history.eth", "version", owner).await?;
    ens_v1::set_text_record(&rpc, resolver_one, owner, name, "legacy", "before-version").await?;
    ens_v1::clear_records(&rpc, resolver_one, owner, name).await?;
    ens_v1::set_text_record(&rpc, resolver_one, owner, name, "active", "resolver-one").await?;
    ens_v1::set_text_record(
        &rpc,
        resolver_two.address,
        owner,
        name,
        "active",
        "resolver-two",
    )
    .await?;

    let record_target = rpc.block_number().await?;
    let null_ready = format!(
        "SELECT count(*) >= 4 FROM normalized_events \
         WHERE logical_name_id IS NULL \
           AND event_kind IN ('RecordChanged', 'RecordVersionChanged') \
           AND lower(after_state->>'node') = lower('{node}')"
    );
    let (db, scratch, mut replay) =
        start_split_replay(&anvil, &deployment, record_target, &null_ready).await?;
    let resolver_one_target = materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name, resolver_one),
        record_target,
        &db,
        &mut replay,
    )
    .await?;
    let run = support::serve_existing_db(db, scratch, &anvil).await?;

    let resolver_one_records =
        compact_records(&run, name, "?texts=legacy,active&mode=declared&meta=full").await?;
    assert_compact_record_not_success(&resolver_one_records, "/data/text_records/legacy");
    assert_eq!(
        pointer(&resolver_one_records, "/data/text_records/active/value"),
        "resolver-one"
    );

    let resolver_two_target = select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name, resolver_two.address),
        resolver_one_target,
        &run.db,
        &mut replay,
    )
    .await?;
    let resolver_two_records =
        compact_records(&run, name, "?texts=active&mode=declared&meta=full").await?;
    assert_eq!(
        pointer(&resolver_two_records, "/data/text_records/active/value"),
        "resolver-two"
    );

    select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name, resolver_one),
        resolver_two_target,
        &run.db,
        &mut replay,
    )
    .await?;
    let restored = compact_records(&run, name, "?texts=active&mode=declared&meta=full").await?;
    assert_eq!(
        pointer(&restored, "/data/text_records/active/value"),
        "resolver-one",
        "selecting the first resolver again must restore its pre-pointer value"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_record_attribution_is_node_scoped_and_never_materializes_unknown_names()
-> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let owner = rpc.accounts().await?[1];
    let resolver = deployment.public_resolver.address;
    let (name_one, name_two, unknown_name) = ("one.scope.eth", "two.scope.eth", "orphan");

    ens_v1::register_eth_name(&rpc, &deployment, "scope", owner, YEAR, resolver).await?;
    ens_v1::create_subname(&rpc, &deployment, owner, "scope.eth", "one", owner).await?;
    ens_v1::create_subname(&rpc, &deployment, owner, "scope.eth", "two", owner).await?;
    ens_v1::create_subname(
        &rpc,
        &deployment,
        deployment.deployer,
        "",
        unknown_name,
        owner,
    )
    .await?;
    ens_v1::set_text_record(&rpc, resolver, owner, name_one, "description", "one-only").await?;
    ens_v1::set_text_record(&rpc, resolver, owner, name_two, "description", "two-only").await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        owner,
        unknown_name,
        "description",
        "never-served",
    )
    .await?;

    let record_target = rpc.block_number().await?;
    let nodes =
        [name_one, name_two, unknown_name].map(|name| format!("{:#x}", ens_v1::namehash(name)));
    let null_ready = format!(
        "SELECT count(*) = 3 FROM normalized_events \
         WHERE logical_name_id IS NULL AND event_kind = 'RecordChanged' \
           AND after_state->>'record_key' = 'text:description' \
           AND lower(after_state->>'node') IN (lower('{}'), lower('{}'), lower('{}'))",
        nodes[0], nodes[1], nodes[2]
    );
    let (db, scratch, mut replay) =
        start_split_replay(&anvil, &deployment, record_target, &null_ready).await?;
    let one_pointer = materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name_one, resolver),
        record_target,
        &db,
        &mut replay,
    )
    .await?;
    materialize_and_select_wrapped_resolver(
        &anvil,
        &deployment,
        owner,
        (name_two, resolver),
        one_pointer,
        &db,
        &mut replay,
    )
    .await?;
    let run = support::serve_existing_db(db, scratch, &anvil).await?;

    for (name, expected) in [(name_one, "one-only"), (name_two, "two-only")] {
        let records =
            compact_records(&run, name, "?texts=description&mode=declared&meta=full").await?;
        assert_eq!(
            pointer(&records, "/data/text_records/description/value"),
            expected,
            "shared-resolver attribution leaked across nodes: {records}"
        );
    }

    let (surface_count, name_count, child_count, inventory_count, discovery_count): (
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        "SELECT
           (SELECT count(*) FROM name_surfaces WHERE namehash = $1),
           (SELECT count(*) FROM name_current WHERE namehash = $1),
           (SELECT count(*) FROM children_current WHERE namehash = $1),
           (SELECT count(DISTINCT inventory.resource_id)
            FROM record_inventory_current inventory
            JOIN normalized_events event USING (resource_id)
            WHERE lower(event.after_state->>'node') = lower($1)),
           (SELECT count(*) FROM discovery_edges
            WHERE lower(provenance::text) LIKE '%' || lower($1) || '%')",
    )
    .bind(&nodes[2])
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        (
            surface_count,
            name_count,
            child_count,
            inventory_count,
            discovery_count
        ),
        (0, 0, 0, 0, 0),
        "unknown-node history must remain audit-only"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn records_route_values_and_version_boundaries_follow_current_resolver() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let replacement_resolver =
        ens_v1::deploy_extra_public_resolver(&rpc, &root, &deployment).await?;
    let alice = rpc.accounts().await?[1];
    let resolver_a = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "records", alice, YEAR, resolver_a).await?;
    ens_v1::set_multicoin_addr_record(
        &rpc,
        resolver_a,
        alice,
        "records.eth",
        MULTICOIN_TYPE,
        MULTICOIN_BYTES,
    )
    .await?;
    ens_v1::set_multicoin_addr_record(
        &rpc,
        resolver_a,
        alice,
        "records.eth",
        ENSIP19_DEFAULT_COIN_TYPE,
        alice.as_slice(),
    )
    .await?;
    ens_v1::set_contenthash_record(&rpc, resolver_a, alice, "records.eth", CONTENTHASH_BYTES)
        .await?;

    ens_v1::register_eth_name(&rpc, &deployment, "clearable", alice, YEAR, resolver_a).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver_a,
        alice,
        "clearable.eth",
        "com.twitter",
        "before-clear",
    )
    .await?;

    let initial = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT \
               (SELECT count(DISTINCT after_state->>'record_key') >= 3 FROM normalized_events \
                WHERE logical_name_id = 'ens:0x9407a4f27b24ccf343caeb964a24d93fe04d7851e8fa0813a35c1c3b9eda8574' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' IN ('addr:0', 'addr:2147483648', 'contenthash')) \
             AND \
               EXISTS (SELECT 1 FROM normalized_events \
                WHERE logical_name_id = 'ens:0x129efdee8c82c635f3d70c6f1b7b36923b7419ade3f0c5eb4c1223435cc277dd' AND event_kind = 'RecordChanged' \
                AND canonicality_state = 'canonical' \
                AND after_state->>'record_key' = 'text:com.twitter')",
        ),
    )
    .await?;

    let records_exact = exact_name(&initial.api, "ens", "records.eth").await?;
    let selectors = selector_keys(&records_exact);
    for expected in ["addr:0", "addr:2147483648", "contenthash"] {
        assert!(
            selectors.contains(expected),
            "expected selector {expected} in records.eth inventory; body: {records_exact}"
        );
    }
    let initial_records_boundary = boundary(&records_exact)?;
    let (classification, provenance): (Value, Value) = sqlx::query_as(
        "SELECT resolver.declared_summary->'classification', inventory.provenance
         FROM resolver_current resolver
         JOIN record_inventory_current inventory
           ON inventory.provenance->>'resolver_address' = resolver.resolver_address
         WHERE resolver.chain_id = 'ethereum-mainnet'
           AND resolver.resolver_address = $1
         ORDER BY inventory.resource_id
         LIMIT 1",
    )
    .bind(format!("{resolver_a:#x}"))
    .fetch_one(&initial.db.pool)
    .await?;
    assert_eq!(
        classification["read_features"],
        json!(["ensip19_default_address"])
    );
    assert_eq!(
        provenance["read_rules"],
        json!([{
            "kind": "ensip19_default_address",
            "source_record_key": "addr:2147483648"
        }])
    );

    // `include` replaces the default section set (which is just
    // resolver_address), so it must be named explicitly alongside the
    // record sections.
    let records = compact_records(
        &initial,
        "records.eth",
        "?include=resolver_address,content_hash,coins&content_hash=true&coin_types=0&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&records, "/data/resolver_address"),
        format!("{resolver_a:#x}")
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/0/status"),
        "success"
    );
    assert_eq!(
        pointer(&records, "/data/coin_addresses/0/value"),
        json!(MULTICOIN_HEX)
    );
    assert_eq!(pointer(&records, "/data/content_hash/status"), "success");
    assert_eq!(
        pointer(&records, "/data/content_hash/value"),
        json!({
            "encoding": "hex",
            "bytes": CONTENTHASH_HEX,
        })
    );

    let clearable_records = compact_records(
        &initial,
        "clearable.eth",
        "?texts=com.twitter&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&clearable_records, "/data/text_records/com.twitter/value"),
        "before-clear",
        "clearable.eth should have a cached text value before clearRecords; body: {clearable_records}"
    );
    let clearable_exact = exact_name(&initial.api, "ens", "clearable.eth").await?;
    let initial_clearable_boundary = boundary(&clearable_exact)?;
    initial.db.cleanup().await?;

    ens_v1::set_resolver(
        &rpc,
        &deployment,
        alice,
        "records.eth",
        replacement_resolver.address,
    )
    .await?;
    ens_v1::clear_records(&rpc, resolver_a, alice, "clearable.eth").await?;

    let replacement_addr = format!("{:#x}", replacement_resolver.address);
    let ready_sql = format!(
        "SELECT \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:0x9407a4f27b24ccf343caeb964a24d93fe04d7851e8fa0813a35c1c3b9eda8574' AND event_kind = 'ResolverChanged' \
            AND canonicality_state = 'canonical' \
            AND lower(after_state->>'resolver') = '{replacement_addr}') \
         AND \
           EXISTS (SELECT 1 FROM normalized_events \
            WHERE logical_name_id = 'ens:0x129efdee8c82c635f3d70c6f1b7b36923b7419ade3f0c5eb4c1223435cc277dd' AND event_kind = 'RecordVersionChanged' \
            AND canonicality_state = 'canonical')"
    );
    let current = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;

    let replaced_exact = exact_name(&current.api, "ens", "records.eth").await?;
    assert_resolver(&replaced_exact, replacement_resolver.address);
    // The wire boundary object carries only its chain position; the
    // event-identity fields (event_kind, normalized_event_id) are null.
    // Boundary movement is asserted positionally.
    let replacement_boundary = boundary(&replaced_exact)?;
    assert_ne!(
        replacement_boundary, initial_records_boundary,
        "resolver replacement must move the record-version boundary; body: {replaced_exact}"
    );
    let boundary_block = |value: &Value| -> i64 {
        value
            .pointer("/chain_position/block_number")
            .and_then(Value::as_i64)
            .unwrap_or_default()
    };
    assert!(
        boundary_block(&replacement_boundary) > boundary_block(&initial_records_boundary),
        "replacement boundary should move to a later block; body: {replaced_exact}"
    );

    let replaced_records = compact_records(
        &current,
        "records.eth",
        "?include=resolver_address,content_hash,coins&content_hash=true&coin_types=0&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&replaced_records, "/data/resolver_address"),
        replacement_addr
    );
    assert_compact_record_not_success(&replaced_records, "/data/coin_addresses/0");
    assert_compact_record_not_success(&replaced_records, "/data/content_hash");

    let cleared_exact = exact_name(&current.api, "ens", "clearable.eth").await?;
    let cleared_boundary = boundary(&cleared_exact)?;
    assert!(
        boundary_block(&cleared_boundary) > boundary_block(&initial_clearable_boundary),
        "clearRecords must move the record-version boundary to a later block; body: {cleared_exact}"
    );
    let cleared_records = compact_records(
        &current,
        "clearable.eth",
        "?texts=com.twitter&mode=declared&meta=full",
    )
    .await?;
    assert_compact_record_not_success(&cleared_records, "/data/text_records/com.twitter");

    current.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn shared_resolver_keeps_per_name_records_and_projection_marks_fan_in_unsupported()
-> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "sharedone", alice, YEAR, resolver).await?;
    ens_v1::register_eth_name(&rpc, &deployment, "sharedtwo", bob, YEAR, resolver).await?;
    ens_v1::set_text_record(
        &rpc,
        resolver,
        alice,
        "sharedone.eth",
        "description",
        "one record",
    )
    .await?;
    ens_v1::set_addr_record(&rpc, resolver, bob, "sharedtwo.eth", bob).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT count(*) >= 2 FROM normalized_events \
             WHERE logical_name_id IN ('ens:0x139602c6817eb66c83e55c315be8b13b69f89cf04eae8a9815295e737729eea7', 'ens:0x7e8051d3a865d1f28e73883a890c8a7a8ebfcd91a3e1012ebabcee0d3fa85ed6') \
             AND event_kind = 'RecordChanged' AND canonicality_state = 'canonical'",
        ),
    )
    .await?;

    let one = compact_records(
        &run,
        "sharedone.eth",
        "?texts=description&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&one, "/data/text_records/description/value"),
        "one record",
        "sharedone.eth text record should stay scoped by node; body: {one}"
    );
    assert_eq!(
        pointer(&one, "/data/resolver_address"),
        format!("{resolver:#x}")
    );

    let two = compact_records(
        &run,
        "sharedtwo.eth",
        "?coin_types=60&mode=declared&meta=full",
    )
    .await?;
    assert_eq!(
        pointer(&two, "/data/coin_addresses/60/value"),
        format!("{bob:#x}"),
        "sharedtwo.eth addr record should stay scoped by node; body: {two}"
    );
    assert_eq!(
        pointer(&two, "/data/resolver_address"),
        format!("{resolver:#x}")
    );

    let overview: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = 'ethereum-mainnet' AND resolver_address = $1",
    )
    .bind(format!("{resolver:#x}"))
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        pointer(&overview, "/bindings/status"),
        "unsupported",
        "schema-v2 must not claim ENSv1 resolver-binding enumeration: {overview}"
    );
    assert_eq!(
        pointer(&overview, "/bindings/unsupported_reason"),
        "resolver_binding_enumeration_not_projected",
        "schema-v2 should persist the fan-in unsupported reason: {overview}"
    );
    assert_eq!(pointer(&overview, "/coverage/status"), "projected");
    assert_eq!(
        pointer(&overview, "/coverage/exhaustiveness"),
        "not_asserted"
    );

    run.db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn exact_zero_addr60_uses_stubbed_verified_transport() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, nonzero) = (accounts[1], accounts[2]);
    let name = "zero-address-682.eth";
    let node = format!("{:#x}", ens_v1::namehash(name));
    let logical_name_id = format!("ens:{node}");

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "zero-address-682",
        alice,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_addr_record(
        &rpc,
        deployment.public_resolver.address,
        alice,
        name,
        nonzero,
    )
    .await?;
    let universal = Address::from_slice(
        &alloy_primitives::keccak256("bigname-e2e-placeholder:universal_resolver".as_bytes())[12..],
    );
    // Fixed-answer verified transport stub: it does not read the stored resolver value.
    rpc.set_code(
        universal,
        &universal_zero_runtime(deployment.public_resolver.address),
    )
    .await?;
    ens_v1::set_addr_record(
        &rpc,
        deployment.public_resolver.address,
        alice,
        name,
        Address::ZERO,
    )
    .await?;
    let resolver = deployment.public_resolver.address;
    ens_v1::set_multicoin_addr_record(
        &rpc,
        resolver,
        alice,
        name,
        ENSIP19_DEFAULT_COIN_TYPE,
        nonzero.as_slice(),
    )
    .await?;
    for (coin, expected) in [
        (None, Address::ZERO.as_slice()),
        (Some(60), Address::ZERO.as_slice()),
        (Some(ENSIP19_DEFAULT_COIN_TYPE), nonzero.as_slice()),
        (Some(2147483649), nonzero.as_slice()),
    ] {
        assert_eq!(
            ens_v1::read_addr_record(&rpc, resolver, name, coin).await?,
            expected
        );
    }
    assert!(
        ens_v1::read_addr_record(&rpc, resolver, "fresh-zero-control.eth", Some(60))
            .await?
            .is_empty()
    );

    let ready_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events WHERE after_state->>'node' = '{node}' \
         AND event_kind = 'RecordChanged' AND after_state->>'record_key' = 'addr:60' \
         AND after_state->>'value' = '{ZERO_ADDRESS}' AND canonicality_state = 'canonical')",
        ZERO_ADDRESS = Address::ZERO
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
    super::basenames_lifecycle::assert_default_inventory(&run, &logical_name_id, true).await?;
    enable_ens_verified_route(&run, &logical_name_id, universal).await?;
    let rows: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT after_state->>'source_event', transaction_hash, log_index, after_state->>'value' \
         FROM normalized_events WHERE after_state->>'node' = $1 AND event_kind = 'RecordChanged' \
         AND after_state->>'record_key' = 'addr:60' AND after_state->>'value' = $2 \
         AND canonicality_state = 'canonical' ORDER BY log_index",
    )
    .bind(&node)
    .bind(format!("{:#x}", Address::ZERO))
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(rows.len(), 2, "zero transaction rows: {rows:?}");
    assert_eq!(rows[0].0, "AddressChanged");
    assert_eq!(rows[1].0, "AddrChanged");
    assert_eq!(rows[0].1, rows[1].1);
    assert_eq!(rows[1].2, rows[0].2 + 1);
    assert!(
        rows.iter()
            .all(|row| row.3 == format!("{:#x}", Address::ZERO))
    );

    normalize_v2_snapshot_timestamp(&run, &logical_name_id).await?;
    let api = start_v2_api(&run, &[("ethereum-mainnet", anvil.url.as_str())]).await?;
    let indexed = v2_records(&api, name, "indexed").await?;
    let before = divergence_counts(&run, &logical_name_id).await?;
    let verified = v2_records(&api, name, "verified").await?;
    let verified_repeat = v2_records(&api, name, "verified").await?;
    let auto = v2_records(&api, name, "auto").await?;
    let after = divergence_counts(&run, &logical_name_id).await?;
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

    assert_zero_api_shapes(&run, &api, "ens", name, &logical_name_id).await?;
    drop(api);
    run.db.cleanup().await?;
    Ok(())
}

fn assert_keys(value: &Value, expected: &[&str]) {
    let actual = value
        .as_object()
        .expect("expected a JSON object")
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        actual,
        expected.iter().copied().collect(),
        "unexpected public keys: {value}"
    );
}

fn assert_private_inventory_absent(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                assert!(
                    ![
                        "exact_nonempty_not_found_record_keys",
                        "provenance",
                        "read_rules",
                        "read_features",
                        "derivation",
                        "source_record_key",
                    ]
                    .contains(&key.as_str()),
                    "private inventory field {key}: {value}"
                );
                assert_private_inventory_absent(child);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_private_inventory_absent),
        Value::String(text) => assert!(!text.contains("exact_nonempty_not_found_record_keys")),
        _ => {}
    }
}

async fn zero_api_response(
    request: reqwest::RequestBuilder,
    source: Option<&str>,
) -> Result<Value> {
    let response = request.send().await?;
    let status = response.status();
    let body: Value = response.json().await?;
    assert_eq!(status, 200, "zero-address route failed: {body}");
    assert_keys(&body, &["data", "meta"]);
    assert_private_inventory_absent(&body);
    let mut meta_keys = vec!["as_of", "as_of_token"];
    if let Some(source) = source {
        meta_keys.push("source");
        assert_eq!(body["meta"]["source"], source);
    }
    assert_keys(&body["meta"], &meta_keys);
    for position in body["meta"]["as_of"]
        .as_object()
        .context("snapshot positions")?
        .values()
    {
        assert_keys(position, &["block_number", "block_hash", "timestamp"]);
    }
    Ok(body)
}

fn assert_zero_name_shape(record: &Value, namespace: &str, name: &str, verified: bool) {
    let mut keys = vec![
        "registration_id",
        "registrant",
        "registered_at",
        "created_at",
        "expires_at",
        "registration_status",
        "name",
        "display_name",
        "namespace",
        "namehash",
        "resolver",
        "addresses",
        "chain_id",
        "network",
        "status",
    ];
    if namespace == "ens" {
        keys.push("token_id");
    }
    if verified {
        keys.push("unsupported_fields");
        assert_eq!(
            record["unsupported_fields"],
            json!(["content_hash", "text_records"])
        );
    } else {
        keys.push("text_records");
    }
    assert_keys(record, &keys);
    assert_eq!(record["status"], "ok");
    assert_eq!(record["name"], name);
    assert_eq!(record["namespace"], namespace);
    assert_keys(&record["resolver"], &["chain_id", "address"]);
    assert!(record.pointer("/addresses/60").is_none());
    assert!(record.get("primary_address").is_none());
    assert!(record.get("value").is_none());
}

// These are actual API calls over the existing fixed-answer verified transport.
// They do not establish genuine Universal Resolver or L1-to-Base forwarding.
pub(super) async fn assert_zero_api_shapes(
    run: &support::PipelineRun,
    api: &V2Api,
    namespace: &str,
    name: &str,
    logical_name: &str,
) -> Result<()> {
    for source in ["indexed", "verified", "verified", "auto"] {
        let request = api.client.get(format!(
            "{}/v2/names/{name}/records?namespace={namespace}&source={source}&keys=addr:60",
            api.base_url,
        ));
        let selected_source = if source == "auto" { "indexed" } else { source };
        let body = zero_api_response(request, Some(selected_source)).await?;
        assert_keys(
            &body["data"],
            &[
                "namespace",
                "resolver",
                "addresses",
                "text_records",
                "content_hash",
                "records",
            ],
        );
        assert_keys(&body["data"]["resolver"], &["chain_id", "address"]);
        assert_eq!(body["data"]["namespace"], namespace);
        assert_eq!(body["data"]["addresses"], json!({}));
        assert_eq!(body["data"]["text_records"], json!({}));
        assert_eq!(body["data"]["content_hash"], Value::Null);
        let expected = if source == "verified" {
            json!({"addr:60":{"status":"not_found", "failure_reason":"no_addr_record"}})
        } else {
            json!({"addr:60":{"status":"not_found"}})
        };
        assert_eq!(body["data"]["records"], expected);
        assert_addr60_not_found(&body);
    }
    for source in ["indexed", "verified"] {
        let body = zero_api_response(
            api.client.get(format!(
                "{}/v2/names/{name}?namespace={namespace}&source={source}",
                api.base_url,
            )),
            Some(source),
        )
        .await?;
        assert_zero_name_shape(&body["data"], namespace, name, source == "verified");
    }
    let batch = zero_api_response(
        api.client
            .post(format!("{}/v2/lookup", api.base_url))
            .json(&json!({"namespace":namespace,"inputs":[{"id":"zero","name":name}]})),
        None,
    )
    .await?;
    let results = batch["data"].as_array().context("batch name results")?;
    assert_eq!(results.len(), 1);
    assert_keys(&results[0], &["input", "kind", "status", "record"]);
    assert_eq!(results[0]["input"], json!({"id":"zero","name":name}));
    assert_eq!(results[0]["kind"], "name");
    assert_eq!(results[0]["status"], "ok");
    assert_zero_name_shape(&results[0]["record"], namespace, name, false);

    let diagnostic = zero_api_response(
        api.client.get(format!(
            "{}/v2/diagnostics/names/{name}/records?namespace={namespace}&keys=addr:60",
            api.base_url,
        )),
        None,
    )
    .await?;
    let data = &diagnostic["data"];
    assert_keys(
        data,
        &[
            "record_inventory",
            "record_cache",
            "value_sources",
            "comparison",
        ],
    );
    let inventory = &data["record_inventory"];
    assert_keys(
        inventory,
        &[
            "record_version_boundary",
            "enumeration_basis",
            "selectors",
            "explicit_gaps",
            "unsupported_families",
            "last_change",
        ],
    );
    assert_eq!(
        inventory["enumeration_basis"],
        json!({
            "capability_declared_families":true, "globally_enumerable":false, "observed_selectors":true,
        })
    );
    assert_eq!(inventory["explicit_gaps"], json!([]));
    assert_eq!(inventory["unsupported_families"], json!([]));
    assert_eq!(
        inventory["selectors"],
        json!([
            {"cacheable":true,"record_family":"addr","record_key":"addr:2147483648","selector_key":"2147483648"},
            {"cacheable":true,"record_family":"addr","record_key":"addr:60","selector_key":"60"},
        ])
    );
    assert_keys(
        &inventory["last_change"],
        &["chain_position", "event_kind", "normalized_event_id"],
    );
    assert_keys(
        &inventory["last_change"]["chain_position"],
        &["chain_id", "block_number", "block_hash", "timestamp"],
    );
    let cache = &data["record_cache"];
    assert_keys(cache, &["entries", "record_version_boundary"]);
    assert_eq!(
        cache["record_version_boundary"],
        inventory["record_version_boundary"]
    );
    assert_keys(
        &cache["record_version_boundary"],
        &[
            "chain_position",
            "event_kind",
            "name",
            "namespace",
            "normalized_event_id",
            "registration_id",
        ],
    );
    assert_keys(
        &cache["record_version_boundary"]["chain_position"],
        &["chain_id", "block_number", "block_hash", "timestamp"],
    );
    let entries = cache["entries"]
        .as_array()
        .context("diagnostic cache entries")?;
    assert_eq!(entries.len(), 2);
    assert_keys(
        &entries[0],
        &[
            "record_family",
            "record_key",
            "selector_key",
            "status",
            "value",
        ],
    );
    assert_eq!(entries[0]["record_key"], "addr:2147483648");
    assert_eq!(entries[0]["status"], "success");
    assert_eq!(
        entries[1],
        json!({
            "record_family":"addr", "record_key":"addr:60", "selector_key":"60", "status":"not_found",
        })
    );
    assert_eq!(
        data["comparison"],
        json!({"addr:60":{
            "indexed":{"status":"not_found"},
            "verified":{"status":"not_found","failure_reason":"no_addr_record"},
        }})
    );
    assert_eq!(
        data["value_sources"],
        json!({"addr:60":[
            {"source":"indexed","status":"not_found"},
            {"source":"verified","status":"not_found","failure_reason":"no_addr_record"},
        ]})
    );
    assert_eq!(divergence_counts(run, logical_name).await?, (0, 0));
    Ok(())
}
