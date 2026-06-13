const BASE_PRIMARY_COIN_TYPE: &str = "2147492101";

fn primary_name_supported_coverage(namespace: &str) -> Value {
    let source_classes_considered = match namespace {
        "ens" => json!(["ens_v1_reverse_l1", "ens_execution"]),
        "basenames" => json!(["basenames_base_primary", "basenames_execution"]),
        other => panic!("unsupported test namespace {other}"),
    };

    json!({
        "status": "partial",
        "exhaustiveness": "non_enumerable",
        "source_classes_considered": source_classes_considered,
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": null,
    })
}

fn primary_name_unsupported_coverage() -> Value {
    json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": "primary-name exact-tuple persisted readback is not supported for the requested tuple",
    })
}

fn primary_name_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        primary_name_left_pad_hex("40", 64),
        primary_name_padded_address_hex(
            "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        ),
        primary_name_left_pad_hex("20", 64),
        primary_name_padded_address_hex(address),
    ))
}

fn primary_name_reverse_name_response(name: &str) -> Value {
    let name_hex = name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let padded_name_hex_len = name_hex.len().next_multiple_of(64);
    json!(format!(
        "0x{}{}{}",
        primary_name_left_pad_hex("20", 64),
        primary_name_left_pad_hex(&format!("{:x}", name.len()), 64),
        format!("{name_hex:0<padded_name_hex_len$}"),
    ))
}

fn primary_name_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    primary_name_left_pad_hex(stripped, 64)
}

fn primary_name_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
}

async fn spawn_primary_name_mock_rpc(
    responses: Vec<Value>,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response_result in responses {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock primary-name RPC request")?;
            let request_payload = read_primary_name_mock_rpc_request(&mut socket).await?;
            requests.push(request_payload);
            write_primary_name_mock_rpc_response(&mut socket, response_result).await?;
        }
        Ok(requests)
    });

    Ok((url, handle))
}

async fn read_primary_name_mock_rpc_request(socket: &mut tokio::net::TcpStream) -> Result<Value> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(body_start) = primary_name_mock_header_end(&buffer) {
            let headers = std::str::from_utf8(&buffer[..body_start])
                .context("mock primary-name RPC request headers were not utf8")?;
            let content_length = primary_name_mock_content_length(headers)?;
            break (body_start, content_length);
        }
    };

    while buffer.len() < body_start + content_length {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request body")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }

    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("failed to parse mock primary-name RPC request body")
}

fn primary_name_mock_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn primary_name_mock_content_length(headers: &str) -> Result<usize> {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .context("mock primary-name RPC request content-length was invalid")?
        .with_context(|| "mock primary-name RPC request did not include content-length")
}

async fn write_primary_name_mock_rpc_response(
    socket: &mut tokio::net::TcpStream,
    result: Value,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result,
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock primary-name RPC response")
}

async fn join_primary_name_mock_rpc_requests(
    handle: tokio::task::JoinHandle<Result<Vec<Value>>>,
) -> Result<Vec<Value>> {
    handle
        .await
        .context("mock primary-name RPC task panicked or was cancelled")?
}

#[test]
fn primary_name_response_uses_on_demand_claim_and_verification_for_default_tuple_miss() -> Result<()>
{
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TupleMissing,
        normalized_claim_name: None,
        on_demand_claim: OnDemandPrimaryNameClaimState::Found(OnDemandPrimaryNameClaim {
            normalized_name: "taytems.eth".to_owned(),
            resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
        }),
        on_demand_verified: OnDemandPrimaryNameVerificationState::Verified(json!({
            "status": "success",
            "name": {
                "logical_name_id": "ens:taytems.eth",
                "namespace": "ens",
                "normalized_name": "taytems.eth",
                "canonical_display_name": "taytems.eth",
                "namehash": bigname_execution::ens_namehash_hex("taytems.eth")?,
            },
        })),
        persisted_verified: None,
    };

    let payload = build_primary_name_response(
        "0x8e8db5ccef88cca9d624701db544989c996e3216".to_owned(),
        "ens".to_owned(),
        "60".to_owned(),
        ResolutionMode::Both,
        &lookup_state,
    );

    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:taytems.eth",
                    "namespace": "ens",
                    "normalized_name": "taytems.eth",
                    "canonical_display_name": "taytems.eth",
                    "namehash": bigname_execution::ens_namehash_hex("taytems.eth")?,
                },
            }
        }))
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc", "ens_execution_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );
    Ok(())
}

#[test]
fn primary_name_response_reports_supported_tuple_class_without_persisted_verified_outcome()
-> Result<()> {
    let address = "0x0000000000000000000000000000000000000abc";
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TuplePresent(PrimaryNameCurrentRow {
            address: address.to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: json!({
                "source_family": "ens_v1_reverse_l1",
            }),
        }),
        normalized_claim_name: Some("alice.eth".to_owned()),
        on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
        on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        persisted_verified: None,
    };

    let payload = build_primary_name_response(
        address.to_owned(),
        "ens".to_owned(),
        "60".to_owned(),
        ResolutionMode::Both,
        &lookup_state,
    );

    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));
    Ok(())
}

#[tokio::test]
async fn get_primary_names_uses_configured_on_demand_rpc_for_default_tuple_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        json!(
            "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b74617974656d732e657468000000000000000000000000000000000000000000"
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 2);
    assert_eq!(rpc_requests[0]["method"], "eth_call");
    assert_eq!(
        rpc_requests[0]["params"][0]["to"],
        bigname_execution::ENS_REGISTRY_ADDRESS
    );
    assert_eq!(rpc_requests[0]["params"][1], "latest");
    assert_eq!(rpc_requests[1]["method"], "eth_call");
    assert_eq!(
        rpc_requests[1]["params"][0]["to"],
        "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
    );
    assert_eq!(rpc_requests[1]["params"][1], "latest");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_canonical_coin_type_reaches_on_demand_fallback() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?namespace=ens&coin_type=060")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("canonical coin_type on-demand primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.data,
        json!({
            "address": "0x8e8db5ccef88cca9d624701db544989c996e3216",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );

    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 2);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_canonicalizes_coin_type_before_lookup_and_response() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "ens_v1_reverse_l1",
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(address, "ens", "60", Some("alice.eth"))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=060&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("canonical coin_type primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.data,
        json!({
            "address": address,
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.eth",
                "provenance": {
                    "source_family": "ens_v1_reverse_l1",
                },
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reports_on_demand_unnormalizable_claim_as_invalid_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("alice..eth"),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand invalid-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "invalid_name",
                "raw_claim_name": "alice..eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_verifies_default_tuple_miss_with_on_demand_rpc() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        json!(
            "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b74617974656d732e657468000000000000000000000000000000000000000000"
        ),
        primary_name_universal_resolver_addr60_response(
            "0x8e8db5ccef88cca9d624701db544989c996e3216",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand verified tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:taytems.eth",
                    "namespace": "ens",
                    "normalized_name": "taytems.eth",
                    "canonical_display_name": "taytems.eth",
                    "namehash": bigname_execution::ens_namehash_hex("taytems.eth")?,
                },
            }
        }))
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc", "ens_execution_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 3);
    assert_eq!(
        rpc_requests[2]["params"][0]["to"],
        bigname_execution::ENS_UNIVERSAL_RESOLVER_ADDRESS
    );
    assert_eq!(rpc_requests[2]["params"][1], "latest");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reports_on_demand_forward_addr_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        json!(
            "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b74617974656d732e657468000000000000000000000000000000000000000000"
        ),
        primary_name_universal_resolver_addr60_response(
            "0x0000000000000000000000000000000000000000",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand forward miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc", "ens_execution_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reports_partial_coverage_for_on_demand_rpc_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![json!(
        "0x0000000000000000000000000000000000000000000000000000000000000000"
    )])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    assert_eq!(rpc_requests[0]["method"], "eth_call");
    assert_eq!(
        rpc_requests[0]["params"][0]["to"],
        bigname_execution::ENS_REGISTRY_ADDRESS
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_keeps_tuple_unsupported_when_on_demand_rpc_unconfigured() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unconfigured primary-name fallback request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_unsupported_coverage());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_freezes_bootstrap_mode_envelopes() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let default_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000ABC?namespace=ens&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default primary-name request failed")?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name request failed")?;
    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("both primary-name request failed")?;

    assert_eq!(default_response.status(), StatusCode::OK);
    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let default_payload: PrimaryNameResponse = read_json(default_response).await?;
    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        default_payload.data,
        json!({
            "address": "0x0000000000000000000000000000000000000abc",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(default_payload.data, declared_payload.data);
    assert_eq!(default_payload.data, verified_payload.data);
    assert_eq!(default_payload.data, both_payload.data);

    assert_eq!(
        default_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "declared primary-name claim surface is not yet supported",
            }
        }))
    );
    assert_eq!(
        declared_payload.declared_state,
        default_payload.declared_state
    );
    assert_eq!(default_payload.verified_state, None);
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "declared primary-name claim surface is not yet supported",
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert_eq!(default_payload.coverage, primary_name_unsupported_coverage());
    assert!(default_payload.provenance.is_null());
    assert_eq!(default_payload.chain_positions, json!({}));
    assert_eq!(default_payload.consistency, "head");
    assert!(default_payload.last_updated.ends_with('Z'));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_returns_not_found_for_tuple_miss_when_projection_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "61")
        .await?;

    let other_verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:other.eth",
            "namespace": "ens",
            "normalized_name": "other.eth",
            "canonical_display_name": "other.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
            "resource_id": "00000000-0000-0000-0000-000000000999",
            "binding_kind": "declared_registry_path"
        }
    });
    let other_trace = primary_name_execution_trace(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000031),
        "ens",
        "0x0000000000000000000000000000000000000abc",
        "61",
        other_verified_primary_name.clone(),
        timestamp(1_717_172_301),
    );
    let other_outcome = primary_name_execution_outcome(
        other_trace.execution_trace_id,
        "ens",
        "0x0000000000000000000000000000000000000abc",
        "61",
        other_verified_primary_name,
        timestamp(1_717_172_301),
    );
    upsert_execution_trace(&database.pool, &other_trace).await?;
    upsert_execution_outcome(&database.pool, &other_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert!(payload.provenance.is_null());
    assert_eq!(payload.coverage, primary_name_unsupported_coverage());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_declared_claim_status_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    upsert_normalized_events(
        &database.pool,
        &[
            primary_name_reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000abc",
                "60",
                250,
                0,
                CanonicalityState::Canonical,
            ),
            primary_name_reverse_linked_name_event(
                "record-a-60-success",
                "0x0000000000000000000000000000000000000abc",
                "60",
                Some("Alice.eth"),
                251,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some("0x0000000000000000000000000000000000000abc"),
        Some("ens"),
        Some("60"),
    )
    .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name status request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name status request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.eth",
                "provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-0000000000fa",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }
        }))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(declared_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_basenames_declared_claim_status_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    upsert_normalized_events(
        &database.pool,
        &[
            basenames_primary_name_reverse_changed_event(
                "basenames-reverse-a-60",
                address,
                BASE_PRIMARY_COIN_TYPE,
                260,
                0,
                CanonicalityState::Canonical,
            ),
            basenames_primary_name_reverse_linked_name_event(
                "basenames-record-a-60-success",
                address,
                BASE_PRIMARY_COIN_TYPE,
                Some("Alice.base.eth"),
                261,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some(address),
        Some("basenames"),
        Some(BASE_PRIMARY_COIN_TYPE),
    )
    .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared basenames primary-name status request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames primary-name status request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.base.eth",
                "provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }
        }))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(
        declared_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(
        both_payload.coverage,
        primary_name_supported_coverage("basenames")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_declared_claim_provenance_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "target_reverse",
                "contract_role": "reverse_registrar",
                "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                "emitting_address": "0x00000000000000000000000000000000000000ad",
                "execution_trace_id": "must-be-omitted",
                "verified_primary_name_lookup": {
                    "address": address,
                    "namespace": "ens",
                    "coin_type": "60",
                },
                "verified_primary_name_invalidation": {
                    "claim_status": "success",
                    "primary_claim_source": {
                        "seed": "ignored",
                    },
                },
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(address, "ens", "60", Some("alice.eth"))
        .await?;
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "61",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "sibling_reverse",
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(address, "ens", "61", Some("beta.eth"))
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name provenance request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name provenance request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let expected_claimed_primary_name = json!({
        "status": "success",
        "name": "alice.eth",
        "provenance": {
            "source_family": "target_reverse",
            "contract_role": "reverse_registrar",
            "contract_instance_id": "00000000-0000-0000-0000-000000000123",
            "emitting_address": "0x00000000000000000000000000000000000000ad",
        },
    });

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": expected_claimed_primary_name.clone(),
        }))
    );
    assert_eq!(
        declared_payload
            .declared_state
            .as_ref()
            .and_then(|declared_state| declared_state.get("claimed_primary_name"))
            .and_then(Value::as_object)
            .and_then(|claimed_primary_name| claimed_primary_name.get("name")),
        Some(&json!("alice.eth"))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(declared_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, primary_name_supported_coverage("ens"));

    let claimed_primary_name = declared_payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");
    let provenance = claimed_primary_name
        .get("provenance")
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name provenance must be present");
    assert!(!provenance.contains_key("execution_trace_id"));
    assert!(!provenance.contains_key("verified_primary_name_lookup"));
    assert!(!provenance.contains_key("verified_primary_name_invalidation"));
    assert_eq!(
        provenance.get("source_family"),
        Some(&json!("target_reverse"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_omit_declared_claim_provenance_from_top_level_route_summary()
-> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "normalized_event_ids": [101, 102],
                "raw_fact_refs": [{
                    "kind": "raw_log",
                    "block_number": 101,
                }],
                "manifest_versions": [{
                    "manifest_version": 7,
                    "source_family": "ens_v1_reverse_l1",
                    "source_manifest_id": null,
                }],
                "derivation_kind": "primary_name_projection_rebuild",
                "verified_primary_name_lookup": {
                    "address": address,
                    "namespace": "ens",
                    "coin_type": "60",
                },
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(address, "ens", "60", Some("alice.eth"))
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name route provenance request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name route provenance request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    assert!(declared_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_raw_claim_name_for_invalid_name_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_claim_row(
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
            PrimaryNameClaimStatus::InvalidName,
            Some("alice..eth"),
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed invalid-name primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    let claimed_primary_name = payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");

    assert_eq!(
        claimed_primary_name.get("status"),
        Some(&json!("invalid_name"))
    );
    assert_eq!(
        claimed_primary_name.get("raw_claim_name"),
        Some(&json!("alice..eth"))
    );
    assert_eq!(claimed_primary_name.get("provenance"), Some(&json!({})));
    assert!(
        !claimed_primary_name.contains_key("name"),
        "declared invalid-name readback must not backfill claimed_primary_name.name"
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_invalid_claim_name_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    upsert_normalized_events(
        &database.pool,
        &[
            primary_name_reverse_changed_event(
                "reverse-a-60",
                address,
                "60",
                350,
                0,
                CanonicalityState::Canonical,
            ),
            primary_name_reverse_linked_name_event(
                "record-a-60-invalid-name",
                address,
                "60",
                Some("Ni\u{200d}ck.eth"),
                351,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some(address),
        Some("ens"),
        Some("60"),
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    let claimed_primary_name = payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");

    assert_eq!(
        claimed_primary_name.get("status"),
        Some(&json!("invalid_name"))
    );
    assert_eq!(
        claimed_primary_name.get("raw_claim_name"),
        Some(&json!("Ni\u{200d}ck.eth"))
    );
    assert!(
        !claimed_primary_name.contains_key("name"),
        "invalid raw claims must not publish claimed_primary_name.name in bootstrap mode"
    );
    let provenance = claimed_primary_name
        .get("provenance")
        .and_then(Value::as_object)
        .expect("declared invalid-name provenance must be present");
    assert_eq!(
        provenance.get("source_family"),
        Some(&json!("ens_v1_reverse_l1"))
    );
    assert_eq!(
        provenance.get("contract_role"),
        Some(&json!("reverse_registrar"))
    );
    assert_eq!(
        provenance.get("emitting_address"),
        Some(&json!("0x00000000000000000000000000000000000000ad"))
    );
    assert!(!provenance.contains_key("execution_trace_id"));
    assert!(!provenance.contains_key("verified_primary_name_lookup"));
    assert!(!provenance.contains_key("verified_primary_name_invalidation"));
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_verified_primary_name_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000041);
    let finished_at = timestamp(1_717_172_401);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;
    database
        .insert_primary_name_current_row(address, "ens", "61")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let other_trace = primary_name_execution_trace(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
        "ens",
        address,
        "61",
        json!({
            "status": "mismatch",
            "name": {
                "logical_name_id": "ens:other.eth",
                "namespace": "ens",
                "normalized_name": "other.eth",
                "canonical_display_name": "other.eth",
                "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
                "resource_id": "00000000-0000-0000-0000-000000000999",
                "binding_kind": "declared_registry_path"
            },
            "failure_reason": "resolved_address_mismatch"
        }),
        timestamp(1_717_172_499),
    );
    let other_outcome = primary_name_execution_outcome(
        other_trace.execution_trace_id,
        "ens",
        address,
        "61",
        json!({
            "status": "mismatch",
            "name": {
                "logical_name_id": "ens:other.eth",
                "namespace": "ens",
                "normalized_name": "other.eth",
                "canonical_display_name": "other.eth",
                "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
                "resource_id": "00000000-0000-0000-0000-000000000999",
                "binding_kind": "declared_registry_path"
            },
            "failure_reason": "resolved_address_mismatch"
        }),
        timestamp(1_717_172_499),
    );
    upsert_execution_trace(&database.pool, &other_trace).await?;
    upsert_execution_outcome(&database.pool, &other_outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name persisted readback request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name persisted readback request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    assert_eq!(
        verified_primary_name.get("provenance"),
        Some(&verified_section_provenance)
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("execution_trace_id")),
        Some(&json!(execution_trace_id.to_string())),
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("manifest_versions")),
        Some(&primary_name_execution_manifest_versions()),
    );
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:01Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:01Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_execution_persisted_verified_primary_name_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let expected_data = json!({
        "address": address,
        "namespace": "ens",
        "coin_type": "60",
    });
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000051);
    let finished_at = timestamp(1_717_172_417);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(address, "ens", "60", Some("alice.eth"))
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    bigname_execution::persist_ens_verified_primary_name(
        &database.pool,
        &bigname_execution::PersistEnsVerifiedPrimaryNameRequest {
            trace,
            outcome: outcome.clone(),
        },
    )
    .await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("execution-persisted verified primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });
    assert_eq!(verified_payload.data, expected_data);
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance,
            }
        }))
    );
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:17Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_basenames_verified_primary_name_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004a);
    let finished_at = timestamp(1_717_172_410);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    upsert_normalized_events(
        &database.pool,
        &[
            basenames_primary_name_reverse_changed_event(
                "basenames-reverse-b-60",
                address,
                BASE_PRIMARY_COIN_TYPE,
                360,
                0,
                CanonicalityState::Canonical,
            ),
            basenames_primary_name_reverse_linked_name_event(
                "basenames-record-b-60-success",
                address,
                BASE_PRIMARY_COIN_TYPE,
                Some("Alice.base.eth"),
                361,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some(address),
        Some("basenames"),
        Some(BASE_PRIMARY_COIN_TYPE),
    )
    .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified basenames primary-name persisted readback request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames primary-name persisted readback request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "basenames:alice.base.eth",
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
                    "resource_id": "00000000-0000-0000-0000-000000000654",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.base.eth",
                "provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000168",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(
        verified_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:10Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:10Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_basenames_verified_primary_name_not_found_without_l1_resolver_call()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bce";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004b);
    let finished_at = timestamp(1_717_172_411);
    let verified_primary_name = json!({
        "status": "not_found"
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            BASE_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::NotFound,
            None,
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified basenames not_found primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames not_found primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(
        verified_payload.data,
        json!({
            "address": address,
            "namespace": "basenames",
            "coin_type": BASE_PRIMARY_COIN_TYPE,
        })
    );
    assert_eq!(both_payload.data, verified_payload.data);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(
        verified_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:11Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:11Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_basenames_verified_primary_name_invalid_name_without_l1_resolver_call()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcf";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004c);
    let finished_at = timestamp(1_717_172_412);
    let verified_primary_name = json!({
        "status": "invalid_name",
        "failure_reason": "claim_name_not_normalizable"
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            BASE_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::InvalidName,
            Some("alice..base.eth"),
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified basenames invalid_name primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames invalid_name primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(
        verified_payload.data,
        json!({
            "address": address,
            "namespace": "basenames",
            "coin_type": BASE_PRIMARY_COIN_TYPE,
        })
    );
    assert_eq!(both_payload.data, verified_payload.data);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "invalid_name",
                "raw_claim_name": "alice..base.eth",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    let claimed_primary_name = both_payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("claimed_primary_name must be present");
    assert!(
        !claimed_primary_name.contains_key("name"),
        "invalid_name readback must not backfill claimed_primary_name.name"
    );
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(
        verified_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:12Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:12Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_verified_primary_name_mismatch_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000043);
    let finished_at = timestamp(1_717_172_403);
    let verified_primary_name = json!({
        "status": "mismatch",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        },
        "failure_reason": "resolved_target_mismatch"
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name persisted mismatch request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name persisted mismatch request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "mismatch",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "failure_reason": "resolved_target_mismatch",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    assert_eq!(
        verified_primary_name.get("provenance"),
        Some(&verified_section_provenance)
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("execution_trace_id")),
        Some(&json!(execution_trace_id.to_string())),
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("manifest_versions")),
        Some(&primary_name_execution_manifest_versions()),
    );
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, verified_payload.coverage);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_malformed_persisted_verified_primary_name_section() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000044);
    let finished_at = timestamp(1_717_172_404);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    outcome
        .outcome_payload
        .as_mut()
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("verified_primary_name"))
        .and_then(Value::as_object_mut)
        .expect("verified_primary_name section must be present")
        .insert("legacy_field".to_owned(), json!("unexpected"));

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed persisted verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        format!("persisted verified primary-name payload mismatch for address {address}")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_trace_manifest_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000045);
    let finished_at = timestamp(1_717_172_405);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.manifest_context = json!({
        "manifest_versions": [{
            "manifest_version": 99,
            "source_family": "ens_v1_registry"
        }],
    });
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_trace_tuple_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000050);
    let finished_at = timestamp(1_717_172_416);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.request_key = "ens:0x0000000000000000000000000000000000000def:60".to_owned();
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("tuple-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_persisted_basenames_verified_primary_name_without_basenames_execution_source_family()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bc0";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004d);
    let finished_at = timestamp(1_717_172_413);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "basenames", BASE_PRIMARY_COIN_TYPE)
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    trace.manifest_context = json!({
        "manifest_versions": [{
            "manifest_version": 99,
            "source_family": "basenames_base_primary"
        }],
    });
    trace.request_metadata["cache_identity"]["manifest_versions"] = json!([{
        "manifest_version": 99,
        "source_family": "basenames_base_primary"
    }]);
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    outcome.cache_key.manifest_versions = json!([{
        "manifest_version": 99,
        "source_family": "basenames_base_primary"
    }]);

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("Basenames wrong-source-family verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        format!("persisted verified primary-name provenance mismatch for address {address}")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_cache_boundary_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004e);
    let finished_at = timestamp(1_717_172_414);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    outcome.cache_key.requested_chain_positions = json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_099,
        "block_hash": "0xstaleprimary"
    }]);
    outcome.cache_key.topology_version_boundary = record_inventory_boundary(
        "ens:stale.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000c001),
    );
    outcome.cache_key.record_version_boundary = record_inventory_boundary(
        "ens:stale.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000c002),
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("cache-boundary-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_scans_past_newer_drifted_verified_primary_name_outcome()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let older_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000052);
    let older_finished_at = timestamp(1_717_172_418);
    let older_trace = primary_name_execution_trace(
        older_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        older_finished_at,
    );
    let older_outcome = primary_name_execution_outcome(
        older_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        older_finished_at,
    );
    upsert_execution_trace(&database.pool, &older_trace).await?;
    upsert_execution_outcome(&database.pool, &older_outcome).await?;

    let newer_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000053);
    let newer_finished_at = timestamp(1_717_172_519);
    let newer_trace = primary_name_execution_trace(
        newer_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        newer_finished_at,
    );
    let mut newer_outcome = primary_name_execution_outcome(
        newer_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        newer_finished_at,
    );
    newer_outcome.cache_key.record_version_boundary = record_inventory_boundary(
        "ens:stale.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000c003),
    );
    upsert_execution_trace(&database.pool, &newer_trace).await?;
    upsert_execution_outcome(&database.pool, &newer_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("masked persisted verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": {
                    "manifest_versions": primary_name_execution_manifest_versions(),
                    "execution_trace_id": older_trace_id.to_string(),
                },
            }
        }))
    );
    assert_eq!(payload.last_updated, "2024-05-31T16:20:18Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_manifest_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004f);
    let finished_at = timestamp(1_717_172_415);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.request_metadata["cache_identity"]["manifest_versions"] = json!([{
        "manifest_version": 4,
        "source_family": "ens_execution"
    }]);
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest-drift-as-miss verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_omits_verified_section_provenance_for_unsupported_boundaries()
-> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "60")
        .await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported verified primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported mixed primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    let both_verified_primary_name = both_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");

    assert_eq!(verified_primary_name.get("status"), Some(&json!("not_found")));
    assert!(!verified_primary_name.contains_key("unsupported_reason"));
    assert!(!verified_primary_name.contains_key("provenance"));
    assert_eq!(both_verified_primary_name, verified_primary_name);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_freezes_bootstrap_behavior_for_tuple_present() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "60")
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name tuple present request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_defaults_namespace_and_coin_type() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let default_tuple = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default tuple request failed")?;
    let default_namespace = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default namespace request failed")?;

    assert_eq!(default_tuple.status(), StatusCode::OK);
    assert_eq!(default_namespace.status(), StatusCode::OK);

    let default_tuple_payload: PrimaryNameResponse = read_json(default_tuple).await?;
    let default_namespace_payload: PrimaryNameResponse = read_json(default_namespace).await?;
    assert_eq!(
        default_tuple_payload.data,
        json!({
            "address": "0x0000000000000000000000000000000000000abc",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(default_namespace_payload.data, default_tuple_payload.data);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_malformed_input() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let malformed_address = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/not-an-address?namespace=ens&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed-address request failed")?;
    let malformed_coin_type = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60,61")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed-coin-type request failed")?;
    let overflowing_coin_type = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=18446744073709551616")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("overflowing-coin-type request failed")?;

    assert_eq!(malformed_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(malformed_coin_type.status(), StatusCode::BAD_REQUEST);
    assert_eq!(overflowing_coin_type.status(), StatusCode::BAD_REQUEST);

    let malformed_address_payload: ErrorResponse = read_json(malformed_address).await?;
    let malformed_coin_type_payload: ErrorResponse = read_json(malformed_coin_type).await?;
    let overflowing_coin_type_payload: ErrorResponse = read_json(overflowing_coin_type).await?;
    assert_eq!(malformed_address_payload.error.code, "invalid_input");
    assert_eq!(
        malformed_address_payload.error.message,
        "address must be a 0x-prefixed 20-byte hex string"
    );
    assert_eq!(malformed_coin_type_payload.error.code, "invalid_input");
    assert_eq!(
        malformed_coin_type_payload.error.message,
        "coin_type must contain only decimal digits"
    );
    assert_eq!(overflowing_coin_type_payload.error.code, "invalid_input");
    assert_eq!(
        overflowing_coin_type_payload.error.message,
        "coin_type must fit in an unsigned 64-bit integer"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_returns_not_found_for_unsupported_namespace() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=unknown&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported-namespace primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");

    database.cleanup().await?;
    Ok(())
}
