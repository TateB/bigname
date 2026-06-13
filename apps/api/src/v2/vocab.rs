use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    NotFound,
    InvalidName,
    Mismatch,
    Unsupported,
    Stale,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Completeness {
    Full,
    Partial,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Indexed,
    Verified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Finality {
    Latest,
    Safe,
    Finalized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegistrationStatus {
    Active,
    Wrapped,
    Registered,
    Released,
    Unregistered,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Relation {
    Owner,
    Manager,
    Registrant,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Resolver {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    fn assert_wire<T: Serialize>(value: T, expected: &str) {
        let serialized = serde_json::to_value(value).expect("value must serialize");
        assert_eq!(serialized, serde_json::Value::String(expected.to_owned()));
    }

    #[test]
    fn status_variants_use_exact_wire_spelling() {
        assert_wire(Status::Ok, "ok");
        assert_wire(Status::NotFound, "not_found");
        assert_wire(Status::InvalidName, "invalid_name");
        assert_wire(Status::Mismatch, "mismatch");
        assert_wire(Status::Unsupported, "unsupported");
        assert_wire(Status::Stale, "stale");
        assert_wire(Status::Failed, "failed");
    }

    #[test]
    fn completeness_variants_use_exact_wire_spelling() {
        assert_wire(Completeness::Full, "full");
        assert_wire(Completeness::Partial, "partial");
        assert_wire(Completeness::Unsupported, "unsupported");
    }

    #[test]
    fn source_variants_use_exact_wire_spelling() {
        assert_wire(Source::Indexed, "indexed");
        assert_wire(Source::Verified, "verified");
    }

    #[test]
    fn finality_variants_use_exact_wire_spelling() {
        assert_wire(Finality::Latest, "latest");
        assert_wire(Finality::Safe, "safe");
        assert_wire(Finality::Finalized, "finalized");
    }

    #[test]
    fn registration_status_variants_use_exact_wire_spelling() {
        assert_wire(RegistrationStatus::Active, "active");
        assert_wire(RegistrationStatus::Wrapped, "wrapped");
        assert_wire(RegistrationStatus::Registered, "registered");
        assert_wire(RegistrationStatus::Released, "released");
        assert_wire(RegistrationStatus::Unregistered, "unregistered");
    }

    #[test]
    fn relation_variants_use_exact_wire_spelling() {
        assert_wire(Relation::Owner, "owner");
        assert_wire(Relation::Manager, "manager");
        assert_wire(Relation::Registrant, "registrant");
    }
}
