use super::*;

#[test]
fn selected_registrar_preload_keeps_future_token_out_of_initial_state() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let labelhash = keccak256_hex(b"marketbubble");
    let namehash = child_namehash_hex(&eth_node(), &labelhash)?;
    let resolver_log = selected_replay_raw_log(
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        vec![new_resolver_topic0(), namehash],
        address_word(0xdd),
        1,
    );
    let token_log = selected_replay_raw_log(
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        vec![
            transfer_topic0(),
            address_topic(0x01),
            address_topic(0x02),
            labelhash,
        ],
        Vec::new(),
        2,
    );

    let identities =
        selected_registrar_event_identities(&[resolver_log, token_log], &event_topics)?;

    assert!(identities.is_empty());
    Ok(())
}

#[test]
fn selected_registrar_preload_keeps_token_when_it_is_first_selected_name_event() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let labelhash = keccak256_hex(b"marketbubble");
    let token_log = selected_replay_raw_log(
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        vec![
            transfer_topic0(),
            address_topic(0x01),
            address_topic(0x02),
            labelhash,
        ],
        Vec::new(),
        2,
    );
    let expected = raw_log_event_identity(
        &token_log,
        EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        "token-transfer",
    );

    let identities = selected_registrar_event_identities(&[token_log], &event_topics)?;

    assert_eq!(identities, vec![expected]);
    Ok(())
}

fn selected_replay_raw_log(
    source_family: &str,
    topics: Vec<String>,
    data: Vec<u8>,
    log_index: i64,
) -> AuthorityRawLogRow {
    AuthorityRawLogRow {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xf3cdbd23213a9334b5635aff853a9b4ef31bbbd34a78ed76826e180e3e60a116".to_owned(),
        block_number: 25_039_752,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_778_115_839)
            .expect("test timestamp must be valid"),
        transaction_hash: "0x58f94372f036f6bea7f9e12f7f138b60437aa4bb589e32f3e9097b62758fe8e5"
            .to_owned(),
        transaction_index: 98,
        log_index,
        emitting_address: "0x0000000000000000000000000000000000000001".to_owned(),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 13,
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 3,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        contract_role: None,
    }
}

fn address_topic(low_byte: u8) -> String {
    let mut word = [0_u8; 32];
    word[31] = low_byte;
    hex_string(&word)
}

fn address_word(low_byte: u8) -> Vec<u8> {
    let mut word = vec![0_u8; 32];
    word[31] = low_byte;
    word
}
