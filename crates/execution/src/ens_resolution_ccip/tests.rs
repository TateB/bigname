use super::*;

#[test]
fn decodes_offchain_lookup_from_compat_rpc_error_shapes() -> Result<()> {
    let encoded = encoded_offchain_lookup_error();
    let shapes = [
        json!(encoded.clone()),
        json!({ "data": encoded.clone() }),
        json!({ "originalError": { "data": encoded.clone() } }),
        json!({ "error": { "data": encoded } }),
    ];

    for data in shapes {
        let lookup = offchain_lookup_from_rpc_error(&JsonRpcCallError {
            code: Some(3),
            message: "execution reverted".to_owned(),
            data: Some(data),
        })?
        .expect("OffchainLookup error data must decode");
        assert_eq!(lookup.sender, "0x1111111111111111111111111111111111111111");
        assert_eq!(lookup.urls, vec!["https://gateway.example/{data}"]);
        assert_eq!(lookup.call_data, vec![0xab, 0xcd]);
        assert_eq!(lookup.callback_function, [0x12, 0x34, 0x56, 0x78]);
        assert_eq!(lookup.extra_data, vec![0xef]);
    }

    Ok(())
}

#[test]
fn ignores_non_offchain_lookup_revert_data() -> Result<()> {
    let lookup = offchain_lookup_from_rpc_error(&JsonRpcCallError {
        code: Some(3),
        message: "execution reverted".to_owned(),
        data: Some(json!("0x08c379a0")),
    })?;

    assert_eq!(lookup, None);
    Ok(())
}

#[test]
fn decodes_gateway_response_compatibility_shapes() -> Result<()> {
    let bodies: [&[u8]; 3] = [br#"{"data":"0xabcd"}"#, br#""0xabcd""#, b"0xabcd\n"];

    for body in bodies {
        assert_eq!(decode_gateway_response_body(body)?, vec![0xab, 0xcd]);
    }

    Ok(())
}

#[test]
fn encodes_ccip_callback_calldata() {
    assert_eq!(
        hex_string(&ccip_callback_calldata(
            [0x12, 0x34, 0x56, 0x78],
            &[0xab, 0xcd],
            &[0xef],
        )),
        concat!(
            "0x12345678",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "0000000000000000000000000000000000000000000000000000000000000080",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "abcd000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "ef00000000000000000000000000000000000000000000000000000000000000",
        )
    );
}

#[test]
fn encodes_batch_gateway_response() {
    assert_eq!(
        hex_string(&abi_encode_bool_array_and_bytes_array(
            &[false, true],
            &[vec![0xab], vec![0xcd, 0xef]],
        )),
        concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "00000000000000000000000000000000000000000000000000000000000000a0",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "0000000000000000000000000000000000000000000000000000000000000080",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "ab00000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "cdef000000000000000000000000000000000000000000000000000000000000",
        )
    );
}

fn encoded_offchain_lookup_error() -> String {
    hex_string(
        &abi::OffchainLookup {
            sender: Address::repeat_byte(0x11),
            urls: vec!["https://gateway.example/{data}".to_owned()],
            callData: Bytes::copy_from_slice(&[0xab, 0xcd]),
            callbackFunction: alloy_primitives::FixedBytes::from(&[0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::copy_from_slice(&[0xef]),
        }
        .abi_encode(),
    )
}
