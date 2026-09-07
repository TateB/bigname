use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ENSIP19_DEFAULT_COIN_TYPE: u64 = 1 << 31;
pub const ETH_COIN_TYPE: u64 = 60;
pub const ENSIP19_DEFAULT_RECORD_KEY: &str = "addr:2147483648";
const ZERO20_HEX: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolverReadFeature {
    Ensip19DefaultAddress,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolverReadRule {
    Ensip19DefaultAddress { source_record_key: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexedRecordStatus {
    Success,
    NotFound,
    Unsupported,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedRecordDerivation {
    pub rule: ResolverReadFeature,
    pub source_record_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedRecordAnswer {
    pub status: IndexedRecordStatus,
    pub value: Option<Value>,
    pub unsupported_reason: Option<String>,
    pub failure_reason: Option<String>,
    pub derivation: Option<IndexedRecordDerivation>,
}

pub const fn ensip19_chain_from_coin_type(coin_type: u64) -> u32 {
    if coin_type == ETH_COIN_TYPE {
        return 1;
    }
    let candidate = coin_type ^ ENSIP19_DEFAULT_COIN_TYPE;
    if candidate < ENSIP19_DEFAULT_COIN_TYPE {
        candidate as u32
    } else {
        0
    }
}

pub const fn ensip19_default_fallback_target(coin_type: u64) -> bool {
    ensip19_chain_from_coin_type(coin_type) > 0
}

pub fn evaluate_indexed_record(
    entries: &Value,
    provenance: &Value,
    coverage: &Value,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
) -> IndexedRecordAnswer {
    if let Some(entry) = find_entry(entries, record_key, record_family, selector_key) {
        let exact = answer_from_entry(entry, record_family);
        if exact.status != IndexedRecordStatus::NotFound
            || (record_key == "addr:60"
                && provenance["exact_nonempty_not_found_record_keys"]
                    .as_array()
                    .is_some_and(|keys| keys.iter().any(|key| key.as_str() == Some(record_key))))
        {
            if exact.status == IndexedRecordStatus::NotFound && !coverage_is_authoritative(coverage)
            {
                return unsupported("indexed_record_inventory_not_authoritative");
            }
            return exact;
        }
    }

    let eligible_coin_type = (record_family == "addr")
        .then(|| selector_key?.parse::<u64>().ok())
        .flatten()
        .is_some_and(ensip19_default_fallback_target);
    if eligible_coin_type && has_ensip19_rule(provenance) {
        if !coverage_is_authoritative(coverage) {
            return unsupported("ensip19_default_address_source_unavailable");
        }
        let derivation = Some(IndexedRecordDerivation {
            rule: ResolverReadFeature::Ensip19DefaultAddress,
            source_record_key: ENSIP19_DEFAULT_RECORD_KEY.to_owned(),
        });
        if let Some(source) = find_entry(
            entries,
            ENSIP19_DEFAULT_RECORD_KEY,
            "addr",
            Some("2147483648"),
        ) {
            let mut answer = answer_from_entry(source, record_family);
            // A derived answer must use the requested getter's verified decode. The legacy
            // addr(bytes32) path converts the coin-60 bytes to address(0), while the multicoin
            // path returns the non-empty bytes unchanged. Exact stored entries keep their shape.
            // (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
            // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
            if selector_key == Some("60")
                && answer.status == IndexedRecordStatus::Success
                && answer.value.as_ref().and_then(Value::as_str) == Some(ZERO20_HEX)
            {
                answer = not_found();
            }
            return match answer.status {
                IndexedRecordStatus::Success | IndexedRecordStatus::NotFound => {
                    IndexedRecordAnswer {
                        derivation,
                        ..answer
                    }
                }
                IndexedRecordStatus::Unsupported | IndexedRecordStatus::ExecutionFailed => {
                    unsupported("ensip19_default_address_source_unavailable")
                }
            };
        }
        return if coverage_is_authoritative(coverage) {
            IndexedRecordAnswer {
                derivation,
                ..not_found()
            }
        } else {
            unsupported("ensip19_default_address_source_unavailable")
        };
    }

    if coverage_is_authoritative(coverage) {
        not_found()
    } else {
        unsupported("indexed_record_inventory_not_authoritative")
    }
}

impl IndexedRecordStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::ExecutionFailed => "execution_failed",
        }
    }
}

impl IndexedRecordAnswer {
    pub fn comparison_value(&self) -> Value {
        let mut answer = serde_json::json!({"status": self.status.as_str()});
        if let Some(value) = &self.value {
            answer["value"] = value.clone();
        }
        answer
    }
}

fn find_entry<'a>(
    entries: &'a Value,
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
) -> Option<&'a Value> {
    entries.as_array().and_then(|entries| {
        entries
            .iter()
            .find(|entry| {
                entry.get("record_key").and_then(Value::as_str) == Some(record_key)
                    || (entry.get("record_family").and_then(Value::as_str) == Some(record_family)
                        && entry.get("selector_key").and_then(Value::as_str) == selector_key)
            })
            .or_else(|| {
                (record_key == "avatar").then(|| {
                    entries.iter().find(|entry| {
                        entry.get("record_key").and_then(Value::as_str) == Some("text:avatar")
                    })
                })?
            })
    })
}

fn answer_from_entry(entry: &Value, record_family: &str) -> IndexedRecordAnswer {
    match entry.get("status").and_then(Value::as_str) {
        Some("success") => match indexed_value(entry, record_family) {
            Some(value) => IndexedRecordAnswer {
                status: IndexedRecordStatus::Success,
                value: Some(value),
                unsupported_reason: None,
                failure_reason: None,
                derivation: None,
            },
            None => unsupported("indexed_record_value_malformed"),
        },
        Some("not_found") => IndexedRecordAnswer {
            failure_reason: entry
                .get("failure_reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ..not_found()
        },
        Some("unsupported") => unsupported(
            entry
                .get("unsupported_reason")
                .and_then(Value::as_str)
                .unwrap_or("record_not_supported"),
        ),
        Some("execution_failed" | "failed") => IndexedRecordAnswer {
            status: IndexedRecordStatus::ExecutionFailed,
            value: None,
            unsupported_reason: None,
            failure_reason: Some(
                entry
                    .get("failure_reason")
                    .and_then(Value::as_str)
                    .unwrap_or("record_read_failed")
                    .to_owned(),
            ),
            derivation: None,
        },
        _ => unsupported("indexed_record_entry_malformed"),
    }
}

fn indexed_value(entry: &Value, record_family: &str) -> Option<Value> {
    let value = entry.get("value")?;
    let value = value
        .get("value")
        .or_else(|| value.get("bytes"))
        .unwrap_or(value);
    let text = value.as_str()?;
    Some(Value::String(if record_family == "addr" {
        text.to_ascii_lowercase()
    } else {
        text.to_owned()
    }))
}

fn has_ensip19_rule(provenance: &Value) -> bool {
    provenance
        .get("read_rules")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|rule| {
            serde_json::from_value::<ResolverReadRule>(rule.clone()).is_ok_and(|rule| {
                matches!(
                    rule,
                    ResolverReadRule::Ensip19DefaultAddress { ref source_record_key }
                        if source_record_key == ENSIP19_DEFAULT_RECORD_KEY
                )
            })
        })
}

fn coverage_is_authoritative(coverage: &Value) -> bool {
    coverage.get("unsupported_reason").is_none()
        && matches!(
            coverage.get("status").and_then(Value::as_str),
            Some("full" | "projected")
        )
}

fn not_found() -> IndexedRecordAnswer {
    IndexedRecordAnswer {
        status: IndexedRecordStatus::NotFound,
        value: None,
        unsupported_reason: None,
        failure_reason: None,
        derivation: None,
    }
}

fn unsupported(reason: &str) -> IndexedRecordAnswer {
    IndexedRecordAnswer {
        status: IndexedRecordStatus::Unsupported,
        value: None,
        unsupported_reason: Some(reason.to_owned()),
        failure_reason: None,
        derivation: None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn projected() -> Value {
        json!({"status": "projected", "exhaustiveness": "not_asserted"})
    }

    fn rule() -> Value {
        json!({"read_rules": [{
            "kind": "ensip19_default_address",
            "source_record_key": ENSIP19_DEFAULT_RECORD_KEY
        }]})
    }

    #[test]
    fn exact_nonempty_absence_marker_only_blocks_matching_derivation() {
        use IndexedRecordStatus::{NotFound, Success, Unsupported};
        for (exact, marker, expected, derived) in [
            (None, json!([]), Success, true),
            (Some("not_found"), json!([]), Success, true),
            (Some("not_found"), json!(["addr:60"]), NotFound, false),
            (None, json!(["addr:60"]), Success, true),
            (Some("success"), json!(["addr:60"]), Success, false),
            (Some("not_found"), json!("addr:60"), Success, true),
        ] {
            let mut entries = json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,
                "record_family":"addr", "selector_key":"2147483648", "status":"success",
                "value":"0x1111111111111111111111111111111111111111"}]);
            if let Some(status) = exact {
                entries
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"record_key":"addr:60",
                    "record_family":"addr", "selector_key":"60", "status":status,
                    "value":"0x2222222222222222222222222222222222222222"}));
            }
            let mut provenance = rule();
            provenance["exact_nonempty_not_found_record_keys"] = marker;
            let answer = evaluate_indexed_record(
                &entries,
                &provenance,
                &projected(),
                "addr:60",
                "addr",
                Some("60"),
            );
            assert_eq!(answer.status, expected, "{entries}; {provenance}");
            assert_eq!(answer.derivation.is_some(), derived);
            let incomplete = evaluate_indexed_record(
                &entries,
                &provenance,
                &json!({"status":"unsupported"}),
                "addr:60",
                "addr",
                Some("60"),
            );
            if exact == Some("not_found")
                && provenance["exact_nonempty_not_found_record_keys"] == json!(["addr:60"])
            {
                assert_eq!(
                    incomplete.unsupported_reason.as_deref(),
                    Some("indexed_record_inventory_not_authoritative")
                );
            }
            assert_eq!(
                incomplete.status,
                if exact == Some("success") {
                    Success
                } else {
                    Unsupported
                }
            );
        }
    }

    #[test]
    fn ensip19_xor_boundaries_match_chain_from_coin_type() {
        for (coin_type, expected_chain, eligible) in [
            (59, 0, false),
            (60, 1, true),
            (2_147_483_648, 0, false),
            (2_147_483_649, 1, true),
            (4_294_967_295, 2_147_483_647, true),
            (4_294_967_296, 0, false),
            (u64::MAX, 0, false),
        ] {
            assert_eq!(ensip19_chain_from_coin_type(coin_type), expected_chain);
            assert_eq!(ensip19_default_fallback_target(coin_type), eligible);
        }
    }

    #[test]
    fn exact_success_wins_over_default() {
        let answer = evaluate_indexed_record(
            &json!([
                {"record_key":"addr:2147483649","record_family":"addr","selector_key":"2147483649","status":"success","value":"0xEXACT"},
                {"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}
            ]),
            &rule(),
            &projected(),
            "addr:2147483649",
            "addr",
            Some("2147483649"),
        );
        assert_eq!(answer.status, IndexedRecordStatus::Success);
        assert_eq!(answer.value, Some(json!("0xexact")));
        assert_eq!(answer.derivation, None);
    }

    #[test]
    fn missing_or_not_found_exact_uses_default_with_metadata() {
        for entries in [
            json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}]),
            json!([
                {"record_key":"addr:2147483649","record_family":"addr","selector_key":"2147483649","status":"not_found"},
                {"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}
            ]),
        ] {
            let answer = evaluate_indexed_record(
                &entries,
                &rule(),
                &projected(),
                "addr:2147483649",
                "addr",
                Some("2147483649"),
            );
            assert_eq!(answer.status, IndexedRecordStatus::Success);
            assert_eq!(answer.value, Some(json!("0xdefault")));
            assert_eq!(
                answer.derivation,
                Some(IndexedRecordDerivation {
                    rule: ResolverReadFeature::Ensip19DefaultAddress,
                    source_record_key: ENSIP19_DEFAULT_RECORD_KEY.to_owned(),
                })
            );
        }
    }

    #[test]
    fn derived_zero20_matches_the_requested_verified_decode() {
        let zero20 = json!([{
            "record_key":ENSIP19_DEFAULT_RECORD_KEY,
            "record_family":"addr",
            "selector_key":"2147483648",
            "status":"success",
            "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000000"}
        }]);
        let expected_derivation = Some(IndexedRecordDerivation {
            rule: ResolverReadFeature::Ensip19DefaultAddress,
            source_record_key: ENSIP19_DEFAULT_RECORD_KEY.to_owned(),
        });

        let legacy = evaluate_indexed_record(
            &zero20,
            &rule(),
            &projected(),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(legacy.status, IndexedRecordStatus::NotFound);
        assert_eq!(legacy.value, None);
        assert_eq!(legacy.derivation, expected_derivation);

        let multicoin = evaluate_indexed_record(
            &zero20,
            &rule(),
            &projected(),
            "addr:2147483649",
            "addr",
            Some("2147483649"),
        );
        assert_eq!(multicoin.status, IndexedRecordStatus::Success);
        assert_eq!(
            multicoin.value,
            Some(json!("0x0000000000000000000000000000000000000000"))
        );
        assert_eq!(multicoin.derivation, expected_derivation);

        let nonzero = evaluate_indexed_record(
            &json!([{
                "record_key":ENSIP19_DEFAULT_RECORD_KEY,
                "record_family":"addr",
                "selector_key":"2147483648",
                "status":"success",
                "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000def"}
            }]),
            &rule(),
            &projected(),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(nonzero.status, IndexedRecordStatus::Success);
        assert_eq!(
            nonzero.value,
            Some(json!("0x0000000000000000000000000000000000000def"))
        );
        assert_eq!(nonzero.derivation, expected_derivation);
    }

    #[test]
    fn authoritative_default_absence_is_a_derived_miss() {
        let answer = evaluate_indexed_record(
            &json!([]),
            &rule(),
            &projected(),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(answer.status, IndexedRecordStatus::NotFound);
        assert!(answer.derivation.is_some());
    }

    #[test]
    fn incomplete_or_unsupported_default_source_is_nonterminal() {
        let incomplete = evaluate_indexed_record(
            &json!([]),
            &rule(),
            &json!({"status":"unsupported","unsupported_reason":"coverage_incomplete"}),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(incomplete.status, IndexedRecordStatus::Unsupported);

        let unsupported = evaluate_indexed_record(
            &json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"unsupported","unsupported_reason":"value_not_retained"}]),
            &rule(),
            &projected(),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(unsupported.status, IndexedRecordStatus::Unsupported);
    }

    #[test]
    fn exact_unsupported_does_not_assume_empty_storage() {
        let answer = evaluate_indexed_record(
            &json!([
                {"record_key":"addr:60","record_family":"addr","selector_key":"60","status":"unsupported","unsupported_reason":"value_not_retained"},
                {"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}
            ]),
            &rule(),
            &projected(),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(answer.status, IndexedRecordStatus::Unsupported);
        assert_eq!(answer.derivation, None);
    }

    #[test]
    fn default_key_ineligible_and_non_addr_requests_never_derive() {
        for (record_key, family, selector, expected) in [
            (
                ENSIP19_DEFAULT_RECORD_KEY,
                "addr",
                Some("2147483648"),
                IndexedRecordStatus::Success,
            ),
            ("addr:59", "addr", Some("59"), IndexedRecordStatus::NotFound),
            (
                "text:url",
                "text",
                Some("url"),
                IndexedRecordStatus::NotFound,
            ),
        ] {
            let answer = evaluate_indexed_record(
                &json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}]),
                &rule(),
                &projected(),
                record_key,
                family,
                selector,
            );
            assert_eq!(answer.status, expected);
            assert_eq!(answer.derivation, None);
        }
    }
}
