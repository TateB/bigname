use sqlx::{Postgres, QueryBuilder};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdFilter {
    pub eq: Option<Option<String>>,
    pub not: Option<Option<String>>,
    pub gt: Option<String>,
    pub gte: Option<String>,
    pub lt: Option<String>,
    pub lte: Option<String>,
    pub in_values: Option<Vec<String>>,
    pub not_in_values: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StringFilter {
    pub eq: Option<Option<String>>,
    pub not: Option<Option<String>>,
    pub gt: Option<String>,
    pub gte: Option<String>,
    pub lt: Option<String>,
    pub lte: Option<String>,
    pub in_values: Option<Vec<String>>,
    pub not_in_values: Option<Vec<String>>,
    pub contains: Option<String>,
    pub contains_nocase: Option<String>,
    pub not_contains: Option<String>,
    pub not_contains_nocase: Option<String>,
    pub starts_with: Option<String>,
    pub starts_with_nocase: Option<String>,
    pub not_starts_with: Option<String>,
    pub not_starts_with_nocase: Option<String>,
    pub ends_with: Option<String>,
    pub ends_with_nocase: Option<String>,
    pub not_ends_with: Option<String>,
    pub not_ends_with_nocase: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GeneratedDomainFilter {
    pub id: IdFilter,
    pub name: StringFilter,
    pub owner: StringFilter,
}

impl GeneratedDomainFilter {
    pub(crate) fn has_bounded_id_predicate(&self) -> bool {
        self.id.eq.as_ref().is_some_and(Option::is_some) || self.id.in_values.is_some()
    }
}

pub fn push_generated_domain_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a GeneratedDomainFilter,
) {
    push_nullable_comparison(builder, "nc.namehash", filter.id.eq.as_ref(), false);
    push_nullable_comparison(builder, "nc.namehash", filter.id.not.as_ref(), true);
    push_id_range_comparisons(
        builder,
        [
            (filter.id.gt.as_ref(), " > "),
            (filter.id.gte.as_ref(), " >= "),
            (filter.id.lt.as_ref(), " < "),
            (filter.id.lte.as_ref(), " <= "),
        ],
    );
    push_membership(
        builder,
        "nc.namehash",
        filter.id.in_values.as_deref(),
        false,
    );
    push_membership(
        builder,
        "nc.namehash",
        filter.id.not_in_values.as_deref(),
        true,
    );
    push_nullable_comparison(
        builder,
        "(nc.raw_name COLLATE \"C\")",
        filter.name.eq.as_ref(),
        false,
    );
    push_nullable_comparison(
        builder,
        "(nc.raw_name COLLATE \"C\")",
        filter.name.not.as_ref(),
        true,
    );
    push_scalar_comparisons(
        builder,
        "(nc.raw_name COLLATE \"C\")",
        [
            (filter.name.gt.as_ref(), " > "),
            (filter.name.gte.as_ref(), " >= "),
            (filter.name.lt.as_ref(), " < "),
            (filter.name.lte.as_ref(), " <= "),
        ],
    );
    push_membership(
        builder,
        "(nc.raw_name COLLATE \"C\")",
        filter.name.in_values.as_deref(),
        false,
    );
    push_membership(
        builder,
        "(nc.raw_name COLLATE \"C\")",
        filter.name.not_in_values.as_deref(),
        true,
    );

    // Contains preserves existing edge `%` and otherwise adds both wildcards.
    // (upstream: .refs/graph_node/store/postgres/src/relational_queries.rs:L1432-L1476 @ graph_node@aefe173)
    for (value, negated) in [
        (filter.name.contains.as_deref(), false),
        (filter.name.not_contains.as_deref(), true),
    ] {
        if let Some(value) = value {
            push_case_sensitive_like(builder, contains_pattern(value), negated);
        }
    }
    for (value, negated) in [
        (filter.name.contains_nocase.as_deref(), false),
        (filter.name.not_contains_nocase.as_deref(), true),
    ] {
        if let Some(value) = value {
            push_nocase_like(builder, contains_pattern(value), negated);
        }
    }
    push_pattern_pairs(
        builder,
        filter.name.starts_with.as_deref(),
        filter.name.not_starts_with.as_deref(),
        filter.name.starts_with_nocase.as_deref(),
        filter.name.not_starts_with_nocase.as_deref(),
        |value| format!("{value}%"),
    );
    push_pattern_pairs(
        builder,
        filter.name.ends_with.as_deref(),
        filter.name.not_ends_with.as_deref(),
        filter.name.ends_with_nocase.as_deref(),
        filter.name.not_ends_with_nocase.as_deref(),
        |value| format!("%{value}"),
    );
}

fn push_id_range_comparisons<'a, const N: usize>(
    builder: &mut QueryBuilder<'a, Postgres>,
    values: [(Option<&'a String>, &'static str); N],
) {
    for (value, operator) in values {
        if let Some(value) = value {
            builder
                .push(" AND ")
                .push(if is_indexed_namehash_operand(value) {
                    "nc.namehash"
                } else {
                    "(nc.namehash COLLATE \"C\")"
                })
                .push(operator)
                .push_bind(value);
        }
    }
}

fn is_indexed_namehash_operand(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn push_nullable_comparison<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    value: Option<&'a Option<String>>,
    negated: bool,
) {
    let Some(value) = value else { return };
    builder.push(" AND ").push(column);
    match value {
        Some(value) => {
            builder
                .push(if negated { " <> " } else { " = " })
                .push_bind(value);
        }
        None => {
            builder.push(if negated { " IS NOT NULL" } else { " IS NULL" });
        }
    }
}

fn push_scalar_comparisons<'a, const N: usize>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: [(Option<&'a String>, &'static str); N],
) {
    for (value, operator) in values {
        if let Some(value) = value {
            builder
                .push(" AND ")
                .push(column)
                .push(operator)
                .push_bind(value);
        }
    }
}

fn push_membership<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: Option<&'a [String]>,
    negated: bool,
) {
    let Some(values) = values else { return };
    if values.is_empty() {
        builder.push(" AND FALSE");
    } else {
        builder.push(" AND ").push(column);
        builder.push(if negated { " <> ALL(" } else { " = ANY(" });
        builder.push_bind(values).push("::text[])");
    }
}

fn push_pattern_pairs<F: Fn(&str) -> String>(
    builder: &mut QueryBuilder<'_, Postgres>,
    positive: Option<&str>,
    negative: Option<&str>,
    positive_nocase: Option<&str>,
    negative_nocase: Option<&str>,
    pattern: F,
) {
    if let Some(value) = positive {
        push_case_sensitive_like(builder, pattern(value), false);
    }
    if let Some(value) = negative {
        push_case_sensitive_like(builder, pattern(value), true);
    }
    if let Some(value) = positive_nocase {
        push_nocase_like(builder, pattern(value), false);
    }
    if let Some(value) = negative_nocase {
        push_nocase_like(builder, pattern(value), true);
    }
}

fn push_case_sensitive_like(
    builder: &mut QueryBuilder<'_, Postgres>,
    pattern: String,
    negated: bool,
) {
    builder
        .push(" AND (nc.raw_name COLLATE \"C\") ")
        .push(if negated { "NOT LIKE " } else { "LIKE " })
        .push_bind(pattern);
}

fn push_nocase_like(builder: &mut QueryBuilder<'_, Postgres>, pattern: String, negated: bool) {
    builder
        .push(" AND (nc.raw_name COLLATE \"C\") ")
        .push(if negated { "NOT ILIKE " } else { "ILIKE " })
        .push_bind(pattern);
}

fn contains_pattern(value: &str) -> String {
    if value.starts_with('%') || value.ends_with('%') {
        value.to_owned()
    } else {
        format!("%{value}%")
    }
}
