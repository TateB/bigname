use async_graphql::{MaybeUndefined, Result};
use sqlx::{Postgres, QueryBuilder};

use bigname_storage::{
    DEFAULT_ADDRESS_NAMES_MEMBERSHIP_JOINS, DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER,
    NameCurrentAddressFilter,
};

use super::generated_filter_ops::{GeneratedDomainFilter, StringFilter};
use super::inputs::DomainEntityFilter;

const TARGETS: &str = r#"JSONB_AGG(
    owner_witness.chain_positions || JSONB_BUILD_OBJECT(
        'chain_id', owner_witness.provenance ->> 'chain_id'
    ) ORDER BY owner_witness.address, owner_witness.relation
)"#;

pub(crate) fn domain_effective_owner_filter(filter: &DomainEntityFilter) -> Result<StringFilter> {
    macro_rules! value {
        ($member:ident) => {
            required_owner_value(
                filter.$member.clone(),
                stringify!($member),
                std::convert::identity,
            )?
        };
    }
    Ok(StringFilter {
        eq: filter.owner.clone().map(Some),
        not: required_owner_value(
            filter.owner_not.clone(),
            "owner_not",
            std::convert::identity,
        )?
        .map(Some),
        gt: value!(owner_gt),
        gte: value!(owner_gte),
        lt: value!(owner_lt),
        lte: value!(owner_lte),
        in_values: filter.owner_in.clone(),
        not_in_values: required_owner_value(
            filter.owner_not_in.clone(),
            "owner_not_in",
            std::convert::identity,
        )?,
        contains: value!(owner_contains),
        contains_nocase: value!(owner_contains_nocase),
        not_contains: value!(owner_not_contains),
        not_contains_nocase: value!(owner_not_contains_nocase),
        starts_with: value!(owner_starts_with),
        starts_with_nocase: value!(owner_starts_with_nocase),
        not_starts_with: value!(owner_not_starts_with),
        not_starts_with_nocase: value!(owner_not_starts_with_nocase),
        ends_with: value!(owner_ends_with),
        ends_with_nocase: value!(owner_ends_with_nocase),
        not_ends_with: value!(owner_not_ends_with),
        not_ends_with_nocase: value!(owner_not_ends_with_nocase),
    })
}

fn required_owner_value<T, U>(
    value: MaybeUndefined<T>,
    member: &str,
    map: impl FnOnce(T) -> U,
) -> Result<Option<U>> {
    match value {
        MaybeUndefined::Undefined => Ok(None),
        MaybeUndefined::Null => Err(async_graphql::Error::new(format!(
            "Domain_filter.{member} must not be null"
        ))),
        MaybeUndefined::Value(value) => Ok(Some(map(value))),
    }
}

pub(crate) fn owner_filter_is_active(filter: &StringFilter) -> bool {
    filter != &StringFilter::default()
}

pub(crate) fn bounded_effective_owner_membership(filter: &StringFilter) -> bool {
    filter.eq.as_ref().is_some_and(Option::is_some) || filter.in_values.is_some()
}

pub(crate) fn active_owner_filter(filter: Option<&GeneratedDomainFilter>) -> Option<&StringFilter> {
    filter
        .map(|filter| &filter.owner)
        .filter(|filter| owner_filter_is_active(filter))
}

pub(crate) fn legacy_address_filter<'a>(
    owner_filter: Option<&StringFilter>,
    address_filter: Option<&'a NameCurrentAddressFilter>,
) -> Option<&'a NameCurrentAddressFilter> {
    owner_filter.is_none().then_some(address_filter).flatten()
}

pub(crate) fn push_effective_owner_cte_predicates<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: Option<&'a StringFilter>,
    chain_ids: Option<&'a [String]>,
) {
    let Some(filter) = filter else { return };
    builder.push(
        "eligible_effective_owner AS NOT MATERIALIZED (SELECT anc.logical_name_id, \
         anc.address, anc.relation, anc.chain_positions, anc.provenance, ",
    );
    push_rejection_match(builder, filter);
    builder.push(" AS rejects_filter, ");
    builder.push(super::name_queries::owner_witness_validation::OWNER_TARGET_AT_HEAD);
    builder.push(" AS at_head FROM bigname_phase.address_names_current anc ");
    builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_JOINS);
    builder.push(
        " WHERE anc.support_status = 'supported' \
           AND anc.relation = 'effective_controller'",
    );
    if let Some(chain_ids) = chain_ids {
        builder
            .push(" AND anc.provenance ->> 'chain_id' = ANY(")
            .push_bind(chain_ids)
            .push(")");
    }
    builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER);
    builder.push("), ");
    if bounded_effective_owner_membership(filter) {
        builder
            .push("effective_owner_membership AS (SELECT owner_witness.logical_name_id, ")
            .push(TARGETS)
            .push(" AS membership_targets FROM eligible_effective_owner owner_witness WHERE TRUE");
        push_owner_predicates(builder, filter);
        builder.push(" GROUP BY owner_witness.logical_name_id), ");
    }
}

pub(crate) fn push_effective_owner_membership_targets(
    builder: &mut QueryBuilder<'_, Postgres>,
    filter: Option<&StringFilter>,
    legacy_address: bool,
) {
    builder.push(if filter.is_some_and(bounded_effective_owner_membership) {
        " effective_owner_membership.membership_targets"
    } else if filter.is_some() {
        " effective_owner_probe.membership_targets"
    } else if legacy_address {
        " address_membership.membership_targets"
    } else {
        " '[]'::JSONB AS membership_targets"
    });
    if filter.is_some_and(has_negative) {
        builder.push(
            " || COALESCE((SELECT JSONB_AGG( \
                 rejected_owner.chain_positions || JSONB_BUILD_OBJECT( \
                   'chain_id', rejected_owner.provenance ->> 'chain_id')) \
               FROM eligible_effective_owner rejected_owner \
               WHERE rejected_owner.logical_name_id = nc.logical_name_id \
                 AND rejected_owner.rejects_filter AND NOT rejected_owner.at_head \
             ), '[]'::JSONB) AS membership_targets",
        );
    }
}

pub(crate) fn push_effective_owner_lateral_join<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: Option<&'a StringFilter>,
) {
    let Some(filter) = filter else { return };
    if bounded_effective_owner_membership(filter) {
        builder.push(
            " JOIN effective_owner_membership \
               ON effective_owner_membership.logical_name_id = nc.logical_name_id",
        );
        return;
    }
    builder.push(" JOIN LATERAL (SELECT ").push(TARGETS).push(
        " AS membership_targets FROM eligible_effective_owner owner_witness \
             WHERE owner_witness.logical_name_id = nc.logical_name_id",
    );
    push_owner_predicates(builder, filter);
    builder.push(" HAVING COUNT(*) > 0) effective_owner_probe ON TRUE");
}

fn push_owner_predicates<'a>(builder: &mut QueryBuilder<'a, Postgres>, filter: &'a StringFilter) {
    if let Some(value) = filter.eq.as_ref() {
        match value {
            Some(value) => push_scalar(builder, "owner_witness.address", " = ", value),
            None => {
                builder.push(" AND FALSE");
            }
        }
    }
    for (value, operator) in [
        (filter.gt.as_ref(), " > "),
        (filter.gte.as_ref(), " >= "),
        (filter.lt.as_ref(), " < "),
        (filter.lte.as_ref(), " <= "),
    ] {
        if let Some(value) = value {
            push_scalar(
                builder,
                "(owner_witness.address COLLATE \"C\")",
                operator,
                value,
            );
        }
    }
    push_membership(builder, filter.in_values.as_deref());

    macro_rules! patterns {
        ($( $member:ident => ($nocase:literal, $kind:ident) ),+ $(,)?) => {
            $(if let Some(value) = filter.$member.as_deref() {
                push_pattern(builder, Pattern::$kind.apply(value), $nocase);
            })+
        };
    }
    patterns! {
        contains => (false, Contains),
        contains_nocase => (true, Contains),
        starts_with => (false, Starts),
        starts_with_nocase => (true, Starts),
        ends_with => (false, Ends),
        ends_with_nocase => (true, Ends),
    }
    if filter.not == Some(None) || filter.not_in_values.as_ref().is_some_and(Vec::is_empty) {
        builder.push(" AND FALSE");
    } else if has_negative(filter) {
        // A rejecting future target must reach Rust validation, even when another
        // current controller satisfies the positive predicates.
        builder.push(
            " AND NOT EXISTS (SELECT 1 FROM eligible_effective_owner rejected_owner \
                 WHERE rejected_owner.logical_name_id = owner_witness.logical_name_id \
                   AND rejected_owner.rejects_filter \
               AND NOT EXISTS (SELECT 1 FROM eligible_effective_owner invalid_owner \
                 WHERE invalid_owner.logical_name_id = owner_witness.logical_name_id \
                   AND invalid_owner.rejects_filter AND NOT invalid_owner.at_head))",
        );
    }
}

fn push_membership<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: Option<&'a [String]>) {
    let Some(values) = values else { return };
    if values.is_empty() {
        builder.push(" AND FALSE");
    } else {
        builder
            .push(" AND owner_witness.address = ANY(")
            .push_bind(values)
            .push("::text[])");
    }
}

fn push_scalar<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    operator: &str,
    value: &'a String,
) {
    builder
        .push(" AND ")
        .push(column)
        .push(operator)
        .push_bind(value);
}

fn push_pattern<'a>(builder: &mut QueryBuilder<'a, Postgres>, pattern: String, nocase: bool) {
    let operator = if nocase { " ILIKE " } else { " LIKE " };
    builder
        .push(" AND (owner_witness.address COLLATE \"C\")")
        .push(operator)
        .push_bind(pattern);
}

#[derive(Clone, Copy)]
enum Pattern {
    Contains,
    Starts,
    Ends,
}

impl Pattern {
    fn apply(self, value: &str) -> String {
        match self {
            Self::Contains if value.starts_with('%') || value.ends_with('%') => value.to_owned(),
            Self::Contains => format!("%{value}%"),
            Self::Starts => format!("{value}%"),
            Self::Ends => format!("%{value}"),
        }
    }
}

fn has_negative(filter: &StringFilter) -> bool {
    filter.not.is_some()
        || filter.not_in_values.is_some()
        || filter.not_contains.is_some()
        || filter.not_contains_nocase.is_some()
        || filter.not_starts_with.is_some()
        || filter.not_starts_with_nocase.is_some()
        || filter.not_ends_with.is_some()
        || filter.not_ends_with_nocase.is_some()
}

fn push_rejection_match<'a>(builder: &mut QueryBuilder<'a, Postgres>, filter: &'a StringFilter) {
    builder.push("(FALSE");
    if let Some(Some(value)) = &filter.not {
        builder.push(" OR anc.address = ").push_bind(value);
    }
    if let Some(values) = &filter.not_in_values {
        builder
            .push(" OR anc.address = ANY(")
            .push_bind(values)
            .push("::text[])");
    }
    for (value, nocase, kind) in [
        (filter.not_contains.as_ref(), false, Pattern::Contains),
        (filter.not_contains_nocase.as_ref(), true, Pattern::Contains),
        (filter.not_starts_with.as_ref(), false, Pattern::Starts),
        (
            filter.not_starts_with_nocase.as_ref(),
            true,
            Pattern::Starts,
        ),
        (filter.not_ends_with.as_ref(), false, Pattern::Ends),
        (filter.not_ends_with_nocase.as_ref(), true, Pattern::Ends),
    ] {
        if let Some(value) = value {
            builder
                .push(" OR (anc.address COLLATE \"C\")")
                .push(if nocase { " ILIKE " } else { " LIKE " })
                .push_bind(kind.apply(value));
        }
    }
    builder.push(")");
}
