use axum::{
    Json,
    extract::{FromRequestParts, Path, State},
    http::request::Parts,
};
use bigname_storage::{BASENAMES_NAMESPACE, PrimaryNameClaimStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AppState, OnDemandPrimaryNameClaimState, OnDemandPrimaryNameVerificationState,
    PrimaryNameLookupState, PrimaryNameTupleState,
};

use super::{
    Envelope, Meta, RawQueryParams, Source, Status, V2Error, V2Result, api_error_to_v2,
    load_served_head_meta, shared_product_reason,
    v2_exact_name_snapshot_scope_with_resolution_auxiliary,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PrimaryName {
    pub(crate) address: String,
    pub(crate) coin_type: u64,
    pub(crate) namespace: String,
    pub(crate) answers: Vec<PrimaryNameAnswer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) verification: Option<PrimaryNameVerification>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PrimaryNameAnswer {
    pub(crate) source: Source,
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) raw_claim_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PrimaryNameVerification {
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryNameSourceSelection {
    Both,
    Indexed,
    Verified,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PrimaryNameQueryParams {
    pub(crate) namespace: String,
    pub(crate) coin_type: String,
    pub(crate) source: PrimaryNameSourceSelection,
}

struct VerifiedAnswer {
    answer: PrimaryNameAnswer,
    outcome_exists: bool,
}

impl<S> FromRequestParts<S> for PrimaryNameQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let raw = super::parse_raw_query_params_with_allowlist::<RawQueryParams, S>(
            parts,
            state,
            &["coin_type", "namespace", "source"],
        )
        .await?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawQueryParams> for PrimaryNameQueryParams {
    type Error = V2Error;

    fn try_from(raw: RawQueryParams) -> Result<Self, Self::Error> {
        if nonempty(raw.at.as_deref()) {
            return Err(invalid_parameter("at"));
        }
        if nonempty(raw.finality.as_deref()) {
            return Err(invalid_parameter("finality"));
        }

        Ok(Self {
            namespace: crate::parse_primary_name_namespace(
                raw.namespace.as_deref().or(Some("ens")),
            )
            .map_err(api_error_to_v2)?,
            coin_type: crate::parse_primary_name_coin_type(raw.coin_type.as_deref().or(Some("60")))
                .map_err(api_error_to_v2)?,
            source: parse_primary_name_source(raw.source.as_deref())?,
        })
    }
}

impl PrimaryNameSourceSelection {
    pub(crate) const fn resolution_mode(self) -> crate::ResolutionMode {
        match self {
            Self::Both => crate::ResolutionMode::Both,
            Self::Indexed => crate::ResolutionMode::Declared,
            Self::Verified => crate::ResolutionMode::Verified,
        }
    }

    pub(crate) const fn meta_source(self) -> Option<Source> {
        match self {
            Self::Both => None,
            Self::Indexed => Some(Source::Indexed),
            Self::Verified => Some(Source::Verified),
        }
    }
}

pub(crate) async fn get_primary_name(
    Path(address): Path<String>,
    params: PrimaryNameQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<PrimaryName>>> {
    let address = crate::parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let mode = params.source.resolution_mode();
    let coin_type = primary_name_coin_type_number(&params.coin_type)?;
    let mut lookup_state = crate::load_primary_name_lookup_state(
        &state.pool,
        &address,
        &params.namespace,
        &params.coin_type,
        mode,
    )
    .await
    .map_err(api_error_to_v2)?;

    if (mode.includes_declared() || mode.includes_verified())
        && matches!(
            lookup_state.tuple_state,
            PrimaryNameTupleState::TupleMissing
        )
    {
        lookup_state.on_demand_claim = crate::load_on_demand_primary_name_claim(
            &state,
            &address,
            &params.namespace,
            &params.coin_type,
        )
        .await
        .map_err(api_error_to_v2)?;
    }
    if mode.includes_verified()
        && matches!(
            lookup_state.tuple_state,
            PrimaryNameTupleState::TupleMissing
        )
        && let OnDemandPrimaryNameClaimState::Found(claim) = &lookup_state.on_demand_claim
    {
        lookup_state.on_demand_verified = crate::load_on_demand_primary_name_verification(
            &state,
            &address,
            &params.namespace,
            &params.coin_type,
            claim,
        )
        .await
        .map_err(api_error_to_v2)?;
    }

    let mut meta = if matches!(
        lookup_state.on_demand_claim,
        OnDemandPrimaryNameClaimState::NotFound
            | OnDemandPrimaryNameClaimState::InvalidName(_)
            | OnDemandPrimaryNameClaimState::Found(_)
    ) || matches!(
        lookup_state.on_demand_verified,
        OnDemandPrimaryNameVerificationState::Verified(_)
    ) {
        Meta::default()
    } else {
        let snapshot_scope = v2_exact_name_snapshot_scope_with_resolution_auxiliary(
            &state,
            &params.namespace,
            None,
            params.namespace == BASENAMES_NAMESPACE && lookup_state.persisted_verified.is_some(),
        )
        .await?;
        load_served_head_meta(&state.pool, &snapshot_scope).await?
    };
    meta.source = params.source.meta_source();

    Ok(Json(Envelope {
        data: build_primary_name(
            address,
            params.namespace,
            coin_type,
            params.source,
            &lookup_state,
        )?,
        page: None,
        meta,
    }))
}

pub(crate) fn build_primary_name(
    address: String,
    namespace: String,
    coin_type: u64,
    source: PrimaryNameSourceSelection,
    lookup_state: &PrimaryNameLookupState,
) -> V2Result<PrimaryName> {
    let verified = build_verified_answer(&namespace, lookup_state)?;
    let mut answers = Vec::with_capacity(match source {
        PrimaryNameSourceSelection::Both => 2,
        PrimaryNameSourceSelection::Indexed | PrimaryNameSourceSelection::Verified => 1,
    });

    if matches!(
        source,
        PrimaryNameSourceSelection::Both | PrimaryNameSourceSelection::Indexed
    ) {
        answers.push(build_indexed_answer(lookup_state));
    }
    if matches!(
        source,
        PrimaryNameSourceSelection::Both | PrimaryNameSourceSelection::Verified
    ) {
        answers.push(verified.answer.clone());
    }

    Ok(PrimaryName {
        address,
        coin_type,
        namespace,
        answers,
        verification: verified
            .outcome_exists
            .then(|| verification_from_answer(&verified.answer)),
    })
}

fn build_indexed_answer(lookup_state: &PrimaryNameLookupState) -> PrimaryNameAnswer {
    match &lookup_state.tuple_state {
        PrimaryNameTupleState::ProjectionUnavailable => PrimaryNameAnswer::unsupported(
            Source::Indexed,
            "declared primary-name claim surface is not yet supported",
        ),
        PrimaryNameTupleState::TupleMissing => match &lookup_state.on_demand_claim {
            OnDemandPrimaryNameClaimState::Found(claim) => {
                PrimaryNameAnswer::named(Source::Indexed, Status::Ok, &claim.normalized_name)
            }
            OnDemandPrimaryNameClaimState::InvalidName(invalid_claim) => {
                PrimaryNameAnswer::invalid(Source::Indexed, &invalid_claim.raw_name)
            }
            _ => PrimaryNameAnswer::new(Source::Indexed, Status::NotFound),
        },
        PrimaryNameTupleState::TuplePresent(row) => {
            let mut answer =
                PrimaryNameAnswer::new(Source::Indexed, claim_status_to_v2(row.claim_status));
            if row.claim_status == PrimaryNameClaimStatus::Success
                && let Some(name) = lookup_state.normalized_claim_name.as_deref()
            {
                answer.name = Some(name.to_owned());
            }
            if row.claim_status == PrimaryNameClaimStatus::InvalidName {
                answer.raw_claim_name = row.raw_claim_name.clone();
            }
            if row.claim_status == PrimaryNameClaimStatus::Unsupported {
                answer.unsupported_reason = Some(
                    "indexed primary-name claim is not supported for the requested tuple"
                        .to_owned(),
                );
            }
            answer
        }
    }
}

fn build_verified_answer(
    namespace: &str,
    lookup_state: &PrimaryNameLookupState,
) -> V2Result<VerifiedAnswer> {
    if let Some(persisted) = lookup_state.persisted_verified.as_ref() {
        return Ok(VerifiedAnswer {
            answer: verified_answer_from_value(&persisted.verified_primary_name, lookup_state)?,
            outcome_exists: true,
        });
    }

    match &lookup_state.tuple_state {
        PrimaryNameTupleState::TupleMissing => {
            if let OnDemandPrimaryNameVerificationState::Verified(on_demand_verified) =
                &lookup_state.on_demand_verified
            {
                return Ok(VerifiedAnswer {
                    answer: verified_answer_from_value(on_demand_verified, lookup_state)?,
                    outcome_exists: true,
                });
            }
            Ok(VerifiedAnswer {
                answer: PrimaryNameAnswer::new(Source::Verified, Status::NotFound),
                outcome_exists: false,
            })
        }
        PrimaryNameTupleState::TuplePresent(_) if primary_name_supported_namespace(namespace) => {
            Ok(VerifiedAnswer {
                answer: PrimaryNameAnswer::new(Source::Verified, Status::NotFound),
                outcome_exists: false,
            })
        }
        PrimaryNameTupleState::ProjectionUnavailable | PrimaryNameTupleState::TuplePresent(_) => {
            Ok(VerifiedAnswer {
                answer: PrimaryNameAnswer::unsupported(
                    Source::Verified,
                    "verified primary-name entrypoint is not yet supported",
                ),
                outcome_exists: false,
            })
        }
    }
}

fn verified_answer_from_value(
    verified_primary_name: &Value,
    lookup_state: &PrimaryNameLookupState,
) -> V2Result<PrimaryNameAnswer> {
    let status = status_from_v1(
        verified_primary_name
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("execution_failed"),
    );
    let mut answer = PrimaryNameAnswer::new(Source::Verified, status);
    answer.name = primary_name_from_value(verified_primary_name);
    answer.unsupported_reason = primary_name_unsupported_reason(
        str_field(verified_primary_name.get("unsupported_reason")).as_deref(),
        status,
    )?;
    answer.failure_reason = str_field(verified_primary_name.get("failure_reason"))
        .map(|reason| product_primary_name_reason(&reason))
        .transpose()?;
    if status == Status::InvalidName {
        answer.raw_claim_name = str_field(verified_primary_name.get("raw_claim_name"))
            .or_else(|| raw_claim_name(lookup_state));
    }
    Ok(answer)
}

fn verification_from_answer(answer: &PrimaryNameAnswer) -> PrimaryNameVerification {
    PrimaryNameVerification {
        status: answer.status,
        name: answer.name.clone(),
        unsupported_reason: answer.unsupported_reason.clone(),
        failure_reason: answer.failure_reason.clone(),
    }
}

impl PrimaryNameAnswer {
    fn new(source: Source, status: Status) -> Self {
        Self {
            source,
            status,
            name: None,
            raw_claim_name: None,
            unsupported_reason: None,
            failure_reason: None,
        }
    }

    fn named(source: Source, status: Status, name: &str) -> Self {
        Self {
            name: Some(name.to_owned()),
            ..Self::new(source, status)
        }
    }

    fn invalid(source: Source, raw_claim_name: &str) -> Self {
        Self {
            raw_claim_name: Some(raw_claim_name.to_owned()),
            ..Self::new(source, Status::InvalidName)
        }
    }

    fn unsupported(source: Source, reason: &str) -> Self {
        Self {
            unsupported_reason: Some(reason.to_owned()),
            ..Self::new(source, Status::Unsupported)
        }
    }
}

fn claim_status_to_v2(status: PrimaryNameClaimStatus) -> Status {
    match status {
        PrimaryNameClaimStatus::Success => Status::Ok,
        PrimaryNameClaimStatus::NotFound => Status::NotFound,
        PrimaryNameClaimStatus::Unsupported => Status::Unsupported,
        PrimaryNameClaimStatus::InvalidName => Status::InvalidName,
    }
}

fn status_from_v1(status: &str) -> Status {
    match status {
        "success" | "ok" => Status::Ok,
        "not_found" => Status::NotFound,
        "invalid_name" => Status::InvalidName,
        "mismatch" => Status::Mismatch,
        "unsupported" => Status::Unsupported,
        "stale" => Status::Stale,
        "execution_failed" | "failed" => Status::Failed,
        _ => Status::Failed,
    }
}

fn primary_name_unsupported_reason(
    reason: Option<&str>,
    status: Status,
) -> V2Result<Option<String>> {
    if status != Status::Unsupported {
        return Ok(None);
    }

    let reason = reason
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .unwrap_or("unsupported_reason_missing");
    product_primary_name_reason(reason).map(Some)
}

fn product_primary_name_reason(reason: &str) -> V2Result<String> {
    shared_product_reason(
        reason,
        "rejected primary-name reason containing pipeline vocabulary",
        "failed to map primary-name reason vocabulary",
    )
}

fn primary_name_from_value(value: &Value) -> Option<String> {
    match value.get("name")? {
        Value::String(name) => Some(name.clone()),
        Value::Object(name) => name
            .get("normalized_name")
            .and_then(Value::as_str)
            .or_else(|| name.get("name").and_then(Value::as_str))
            .map(str::to_owned),
        _ => None,
    }
}

fn str_field(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(str::to_owned)
}

fn raw_claim_name(lookup_state: &PrimaryNameLookupState) -> Option<String> {
    match &lookup_state.tuple_state {
        PrimaryNameTupleState::TuplePresent(row) => row.raw_claim_name.clone(),
        PrimaryNameTupleState::TupleMissing => match &lookup_state.on_demand_claim {
            OnDemandPrimaryNameClaimState::InvalidName(claim) => Some(claim.raw_name.clone()),
            _ => None,
        },
        PrimaryNameTupleState::ProjectionUnavailable => None,
    }
}

fn primary_name_supported_namespace(namespace: &str) -> bool {
    matches!(namespace, "ens" | "basenames")
}

fn parse_primary_name_source(value: Option<&str>) -> V2Result<PrimaryNameSourceSelection> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(PrimaryNameSourceSelection::Both),
        Some("indexed") => Ok(PrimaryNameSourceSelection::Indexed),
        Some("verified") => Ok(PrimaryNameSourceSelection::Verified),
        Some(_) => Err(invalid_parameter("source")),
    }
}

fn primary_name_coin_type_number(coin_type: &str) -> V2Result<u64> {
    coin_type
        .parse::<u64>()
        .map_err(|_| invalid_parameter("coin_type"))
}

fn nonempty(value: Option<&str>) -> bool {
    value.map(str::trim).is_some_and(|value| !value.is_empty())
}

fn invalid_parameter(parameter: &'static str) -> V2Error {
    V2Error::invalid_input(format!("{parameter} is invalid"))
}

#[cfg(test)]
#[path = "primary_name/tests.rs"]
mod tests;
