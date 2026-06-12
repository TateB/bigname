use super::*;
use crate::evm_abi;
use alloy_sol_types::{SolEvent, sol};

sol! {
    #[derive(Debug)]
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);

    #[derive(Debug)]
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires);

    #[derive(Debug)]
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires, bytes32 referrer);

    #[derive(Debug)]
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires);

    #[derive(Debug)]
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);

    #[derive(Debug)]
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires, bytes32 referrer);

    #[derive(Debug)]
    event NameRenewed(string name, bytes32 indexed label, uint256 expires);

    #[derive(Debug)]
    event NameChanged(bytes32 indexed node, string name);

    #[derive(Debug)]
    event AddrChanged(bytes32 indexed node, address a);

    #[derive(Debug)]
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);

    #[derive(Debug)]
    event VersionChanged(bytes32 indexed node, uint64 newVersion);

    #[derive(Debug)]
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key);

    #[derive(Debug)]
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);

    #[derive(Debug)]
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);

    #[derive(Debug)]
    event NameUnwrapped(bytes32 indexed node, address owner);

    #[derive(Debug)]
    event FusesSet(bytes32 indexed node, uint32 fuses);

    #[derive(Debug)]
    event ExpiryExtended(bytes32 indexed node, uint64 expiry);

    #[derive(Debug)]
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);

    #[derive(Debug)]
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
}

pub(super) struct RegistrarLabelEventData {
    pub(super) label: String,
    pub(super) expiry: i64,
}

pub(super) struct ResolverAddressChangedData {
    pub(super) coin_type: i64,
    pub(super) address_bytes: Vec<u8>,
}

pub(super) struct TextChangedData {
    pub(super) key: String,
    pub(super) indexed_key_hash: String,
    pub(super) value: Option<String>,
}

pub(super) struct WrapperNameWrappedData {
    pub(super) namehash: String,
    pub(super) dns_name: Vec<u8>,
    pub(super) owner: String,
    pub(super) fuses: i64,
    pub(super) expiry: i64,
}

pub(super) struct WrapperFusesSetData {
    pub(super) namehash: String,
    pub(super) fuses: i64,
}

pub(super) struct WrapperExpiryExtendedData {
    pub(super) namehash: String,
    pub(super) expiry: i64,
}

pub(super) struct WrapperTokenTransferData {
    pub(super) namehash: String,
    pub(super) from_address: String,
    pub(super) to_address: String,
    pub(super) value: i64,
}

pub(super) fn decode_registrar_name_registered_data(
    raw_log: &AuthorityRawLogRow,
    topic0: &str,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<RegistrarLabelEventData>> {
    let source_family = raw_log.source_family.as_str();
    if source_family == SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 {
        if event_topics.matches(NAME_REGISTERED_SIGNATURE, topic0)? {
            let Some(event) =
                decode_event_skip::<NameRegistered_0>(raw_log, "NameRegistered log is malformed")
            else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label: event.name,
                expiry: evm_abi::u256_i64(event.expires, "NameRegistered expiry")?,
            }));
        }
        if event_topics.matches(WRAPPED_NAME_REGISTERED_SIGNATURE, topic0)? {
            let Some(event) = decode_event_skip::<NameRegistered_1>(
                raw_log,
                "wrapped NameRegistered log is malformed",
            ) else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label: event.name,
                expiry: evm_abi::u256_i64(event.expires, "wrapped NameRegistered expiry")?,
            }));
        }
        if event_topics.matches(UNWRAPPED_NAME_REGISTERED_SIGNATURE, topic0)? {
            let Some(event) = decode_event_skip::<NameRegistered_2>(
                raw_log,
                "unwrapped NameRegistered log is malformed",
            ) else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label: event.name,
                expiry: evm_abi::u256_i64(event.expires, "unwrapped NameRegistered expiry")?,
            }));
        }
    }

    if source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        && event_topics.matches(BASENAMES_NAME_REGISTERED_SIGNATURE, topic0)?
    {
        let Some(event) = decode_event_skip::<NameRegistered_3>(
            raw_log,
            "Basenames NameRegistered log is malformed",
        ) else {
            return Ok(None);
        };
        return Ok(Some(RegistrarLabelEventData {
            label: event.name,
            expiry: evm_abi::u256_i64(event.expires, "Basenames NameRegistered expiry")?,
        }));
    }

    Ok(None)
}

pub(super) fn decode_registrar_name_renewed_data(
    raw_log: &AuthorityRawLogRow,
    topic0: &str,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<RegistrarLabelEventData>> {
    let source_family = raw_log.source_family.as_str();
    if source_family == SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 {
        if event_topics.matches(NAME_RENEWED_SIGNATURE, topic0)? {
            let Some(event) =
                decode_event_skip::<NameRenewed_0>(raw_log, "NameRenewed log is malformed")
            else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label: event.name,
                expiry: evm_abi::u256_i64(event.expires, "NameRenewed expiry")?,
            }));
        }
        if event_topics.matches(UNWRAPPED_NAME_RENEWED_SIGNATURE, topic0)? {
            let Some(event) = decode_event_skip::<NameRenewed_1>(
                raw_log,
                "unwrapped NameRenewed log is malformed",
            ) else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label: event.name,
                expiry: evm_abi::u256_i64(event.expires, "unwrapped NameRenewed expiry")?,
            }));
        }
    }

    if source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        && event_topics.matches(BASENAMES_NAME_RENEWED_SIGNATURE, topic0)?
    {
        let Some(event) =
            decode_event_skip::<NameRenewed_2>(raw_log, "Basenames NameRenewed log is malformed")
        else {
            return Ok(None);
        };
        return Ok(Some(RegistrarLabelEventData {
            label: event.name,
            expiry: evm_abi::u256_i64(event.expires, "Basenames NameRenewed expiry")?,
        }));
    }

    Ok(None)
}

pub(super) fn decode_name_changed_data(raw_log: &AuthorityRawLogRow) -> Option<String> {
    decode_event_skip::<NameChanged>(raw_log, "NameChanged log is malformed")
        .map(|event| event.name)
}

pub(super) fn decode_addr_changed_data(raw_log: &AuthorityRawLogRow) -> Option<String> {
    decode_event_skip::<AddrChanged>(raw_log, "AddrChanged log is malformed")
        .map(|event| evm_abi::address_hex(event.a))
}

pub(super) fn decode_address_changed_data(
    raw_log: &AuthorityRawLogRow,
) -> Option<ResolverAddressChangedData> {
    let event = decode_event_skip::<AddressChanged>(raw_log, "AddressChanged log is malformed")?;
    Some(ResolverAddressChangedData {
        coin_type: evm_abi::u256_i64(event.coinType, "AddressChanged coin type").ok()?,
        address_bytes: event.newAddress.to_vec(),
    })
}

pub(super) fn decode_version_changed_data(raw_log: &AuthorityRawLogRow) -> Option<i64> {
    decode_event_skip::<VersionChanged>(raw_log, "VersionChanged log is malformed")
        .and_then(|event| i64::try_from(event.newVersion).ok())
}

pub(super) fn decode_text_changed_data(
    source_family: &str,
    raw_log: &AuthorityRawLogRow,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<TextChangedData>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    if source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        && event_topics.matches(TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE, topic0)?
    {
        let Some(event) =
            decode_event_skip::<TextChanged_0>(raw_log, "TextChanged log is malformed")
        else {
            return Ok(None);
        };
        return Ok(Some(TextChangedData {
            key: event.key,
            indexed_key_hash: evm_abi::hex_string(event.indexedKey.as_slice()),
            value: None,
        }));
    }

    let Some(event) = decode_event_skip::<TextChanged_1>(raw_log, "TextChanged log is malformed")
    else {
        return Ok(None);
    };
    Ok(Some(TextChangedData {
        key: event.key,
        indexed_key_hash: evm_abi::hex_string(event.indexedKey.as_slice()),
        value: Some(event.value),
    }))
}

pub(super) fn decode_wrapper_name_wrapped_data(
    raw_log: &AuthorityRawLogRow,
) -> Result<WrapperNameWrappedData> {
    let event = decode_event::<NameWrapped>(raw_log, "NameWrapped log is malformed")?;
    Ok(WrapperNameWrappedData {
        namehash: evm_abi::hex_string(event.node.as_slice()),
        dns_name: event.name.to_vec(),
        owner: evm_abi::address_hex(event.owner),
        fuses: i64::from(event.fuses),
        expiry: i64::try_from(event.expiry).context("NameWrapped expiry exceeds i64")?,
    })
}

pub(super) fn decode_wrapper_fuses_set_data(
    raw_log: &AuthorityRawLogRow,
) -> Result<WrapperFusesSetData> {
    let event = decode_event::<FusesSet>(raw_log, "FusesSet log is malformed")?;
    Ok(WrapperFusesSetData {
        namehash: evm_abi::hex_string(event.node.as_slice()),
        fuses: i64::from(event.fuses),
    })
}

pub(super) fn decode_wrapper_expiry_extended_data(
    raw_log: &AuthorityRawLogRow,
) -> Result<WrapperExpiryExtendedData> {
    let event = decode_event::<ExpiryExtended>(raw_log, "ExpiryExtended log is malformed")?;
    Ok(WrapperExpiryExtendedData {
        namehash: evm_abi::hex_string(event.node.as_slice()),
        expiry: i64::try_from(event.expiry).context("ExpiryExtended expiry exceeds i64")?,
    })
}

pub(super) fn decode_wrapper_transfer_single_data(
    raw_log: &AuthorityRawLogRow,
) -> Result<WrapperTokenTransferData> {
    let event = decode_event::<TransferSingle>(raw_log, "TransferSingle log is malformed")?;
    Ok(WrapperTokenTransferData {
        namehash: evm_abi::u256_word_hex(event.id),
        from_address: evm_abi::address_hex(event.from),
        to_address: evm_abi::address_hex(event.to),
        value: evm_abi::u256_i64(event.value, "TransferSingle value")?,
    })
}

pub(super) fn decode_wrapper_transfer_batch_data(
    raw_log: &AuthorityRawLogRow,
) -> Result<Vec<WrapperTokenTransferData>> {
    let event = decode_event::<TransferBatch>(raw_log, "TransferBatch log is malformed")?;
    if event.ids.len() != event.values.len() {
        bail!("TransferBatch ids and values length mismatch");
    }
    event
        .ids
        .into_iter()
        .zip(event.values)
        .map(|(id, value)| {
            Ok(WrapperTokenTransferData {
                namehash: evm_abi::u256_word_hex(id),
                from_address: evm_abi::address_hex(event.from),
                to_address: evm_abi::address_hex(event.to),
                value: evm_abi::u256_i64(value, "TransferBatch value")?,
            })
        })
        .collect()
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    evm_abi::normalize_hex_32(value)
}

pub(super) fn decode_owner_address(data: &[u8]) -> Result<String> {
    evm_abi::address_hex_from_word(data)
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    evm_abi::topic_address_hex(value)
}

fn decode_event<E>(raw_log: &AuthorityRawLogRow, context: &'static str) -> Result<E>
where
    E: SolEvent,
{
    evm_abi::decode_event_log::<E>(&raw_log.topics, &raw_log.data, context)
}

fn decode_event_skip<E>(raw_log: &AuthorityRawLogRow, context: &'static str) -> Option<E>
where
    E: SolEvent,
{
    decode_event::<E>(raw_log, context).ok()
}
