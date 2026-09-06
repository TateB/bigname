use sqlx::{Postgres, QueryBuilder};

use super::{EventHistoryReadFilter, paging::push_history_filters, source::push_history_source};

pub(super) fn push_product_history_duplicate_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    // Registry read copies retain the original control-resource representation.
    builder.push(" AND strpos(ne.event_identity, ':ResolverChanged:registry-read:') = 0");
    // Handoffs have no separate original. Pick one matching copy, retaining the
    // sole clear in a resource-scoped request and keeping selection before paging.
    // NOT LIKE also exposes marker selectivity to the global lineage-join planner.
    builder.push(
        r#"
        AND (ne.event_identity NOT LIKE '%:ResolverChanged:registry-fallback-handoff:%'
             OR ne.event_identity = (
                 WITH handoff AS (
                     SELECT ne.chain_id, ne.block_number, ne.block_hash,
                            ne.after_state ->> 'node' AS node,
                            split_part(ne.event_identity,
                                ':ResolverChanged:registry-fallback-handoff:', 1) AS origin
                 )
                 SELECT min(ne.event_identity)
        "#,
    );
    // Handoffs come from RawLogInput, whose chain and block number are required.
    // Equality bounds use the existing chain/block index before matching origin.
    push_history_source(builder, false);
    builder.push(
        r#"
        AND ne.event_kind = 'ResolverChanged'
        AND strpos(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:') > 0
        AND ne.chain_id = (SELECT chain_id FROM handoff)
        AND ne.block_number = (SELECT block_number FROM handoff)
        AND ne.block_hash IS NOT DISTINCT FROM (SELECT block_hash FROM handoff)
        AND ne.after_state ->> 'node' IS NOT DISTINCT FROM (SELECT node FROM handoff)
        AND split_part(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:', 1)
            = (SELECT origin FROM handoff)
        "#,
    );
    push_history_filters(builder, filter, canonical_only);
    builder.push("))");
}

#[cfg(test)]
#[path = "duplicates_tests.rs"]
mod tests;
