use anyhow::{Result, bail};

use super::pagination::CoinbaseSqlLogCursor;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlFilterPack {
    pub(super) chain: String,
    pub(super) from_block: i64,
    pub(super) to_block: i64,
    pub(super) addresses: Vec<String>,
    pub(super) topic0s: Vec<String>,
    pub(super) scan_all_emitters: bool,
    pub(super) source_families: Vec<String>,
}

pub(super) fn build_query(
    pack: &CoinbaseSqlFilterPack,
    cursor: Option<CoinbaseSqlLogCursor>,
    limit: usize,
) -> Result<String> {
    let network = coinbase_sql_network(&pack.chain)?;
    if limit == 0 {
        bail!("Coinbase SQL query limit must be positive");
    }
    if pack.from_block > pack.to_block {
        bail!(
            "Coinbase SQL filter pack start {} is after end {}",
            pack.from_block,
            pack.to_block
        );
    }
    if pack.addresses.is_empty() && !pack.scan_all_emitters {
        bail!("Coinbase SQL filter pack must include addresses unless scan_all_emitters is true");
    }

    let mut final_selection_predicates = Vec::new();
    let mut source_selection_predicates = Vec::new();
    if !pack.scan_all_emitters {
        let address_predicate = format!(
            "l.emitting_address IN ({})",
            sql_string_literals(&pack.addresses)
        );
        final_selection_predicates.push(address_predicate);
        source_selection_predicates.push(format!(
            "l.address IN ({})",
            sql_string_literals(&pack.addresses)
        ));
    }
    if !pack.topic0s.is_empty() {
        let topic_predicate = format!("l.topics[1] IN ({})", sql_string_literals(&pack.topic0s));
        final_selection_predicates.push(topic_predicate.clone());
        source_selection_predicates.push(topic_predicate);
    }
    let final_selection_predicates = if final_selection_predicates.is_empty() {
        "1 = 1".to_owned()
    } else {
        final_selection_predicates.join("\n  AND ")
    };
    let source_selection_predicates = if source_selection_predicates.is_empty() {
        "1 = 1".to_owned()
    } else {
        source_selection_predicates.join("\n  AND ")
    };
    let mut output_predicates = vec![format!(
        "l.block_number BETWEEN {} AND {}",
        pack.from_block, pack.to_block
    )];
    if let Some(cursor) = cursor {
        output_predicates.push(format!(
            "(l.block_number > {} OR (l.block_number = {} AND l.transaction_index > {}) OR (l.block_number = {} AND l.transaction_index = {} AND l.log_index > {}))",
            cursor.block_number,
            cursor.block_number,
            cursor.transaction_index,
            cursor.block_number,
            cursor.transaction_index,
            cursor.log_index
        ));
    }
    let log_action_expr = active_action_expression("l.action");
    let tx_action_expr = active_action_expression("action");

    Ok(format!(
        r#"WITH active_transactions AS (
  SELECT
    block_number,
    block_hash,
    transaction_hash,
    transaction_index
  FROM {network}.transactions
  WHERE block_number BETWEEN {from_block} AND {to_block}
  GROUP BY
    block_number,
    block_hash,
    transaction_hash,
    transaction_index
  HAVING sum({tx_action_expr}) > 0
),
all_log_rows AS (
  SELECT
    l.block_number AS block_number,
    l.block_hash AS block_hash,
    l.transaction_hash AS transaction_hash,
    t.transaction_index AS transaction_index,
    l.log_index AS transaction_log_index,
    l.address AS emitting_address,
    l.topics AS topics,
    {log_action_expr} AS action
  FROM {network}.events l
  JOIN active_transactions t
    ON t.block_number = l.block_number
   AND t.block_hash = l.block_hash
   AND t.transaction_hash = l.transaction_hash
  WHERE l.block_number BETWEEN {from_block} AND {to_block}
    AND {source_selection_predicates}
  UNION ALL
  SELECT
    l.block_number AS block_number,
    l.block_hash AS block_hash,
    l.transaction_hash AS transaction_hash,
    t.transaction_index AS transaction_index,
    l.log_index AS transaction_log_index,
    l.address AS emitting_address,
    l.topics AS topics,
    {log_action_expr} AS action
  FROM {network}.encoded_logs l
  JOIN active_transactions t
    ON t.block_number = l.block_number
   AND t.block_hash = l.block_hash
   AND t.transaction_hash = l.transaction_hash
  WHERE l.block_number BETWEEN {from_block} AND {to_block}
    AND {source_selection_predicates}
),
active_logs AS (
  SELECT
    block_number,
    block_hash,
    transaction_hash,
    transaction_index,
    transaction_log_index,
    emitting_address,
    topics
  FROM all_log_rows
  GROUP BY
    block_number,
    block_hash,
    transaction_hash,
    transaction_index,
    transaction_log_index,
    emitting_address,
    topics
  HAVING sum(action) > 0
),
indexed_logs AS (
  SELECT
    block_number,
    block_hash,
    transaction_hash,
    transaction_index,
    row_number() OVER (
      PARTITION BY block_number, block_hash
      ORDER BY transaction_index, transaction_log_index
    ) - 1 AS log_index,
    emitting_address,
    topics
  FROM active_logs
)
SELECT
  l.block_number AS block_number,
  l.block_hash AS block_hash,
  l.transaction_hash AS transaction_hash,
  l.transaction_index AS transaction_index,
  l.log_index AS log_index,
  l.emitting_address AS emitting_address,
  l.topics AS topics
FROM indexed_logs l
WHERE {output_predicates}
  AND {final_selection_predicates}
ORDER BY l.block_number, l.transaction_index, l.log_index
LIMIT {limit}"#,
        from_block = pack.from_block,
        to_block = pack.to_block,
        tx_action_expr = tx_action_expr,
        log_action_expr = log_action_expr,
        output_predicates = output_predicates.join("\n  AND "),
        source_selection_predicates = source_selection_predicates,
        final_selection_predicates = final_selection_predicates
    ))
}

pub(super) fn build_or_split_filter_pack(
    pack: CoinbaseSqlFilterPack,
    char_limit: usize,
    page_limit: usize,
) -> Result<Vec<CoinbaseSqlFilterPack>> {
    let conservative_limit = char_limit.saturating_sub(500);
    if build_query(&pack, None, page_limit)?.len() <= conservative_limit {
        return Ok(vec![pack]);
    }

    if pack.addresses.len() > 1 {
        let midpoint = pack.addresses.len() / 2;
        let mut left = pack.clone();
        let mut right = pack;
        left.addresses = left.addresses[..midpoint].to_vec();
        right.addresses = right.addresses[midpoint..].to_vec();
        let mut packs = build_or_split_filter_pack(left, char_limit, page_limit)?;
        packs.extend(build_or_split_filter_pack(right, char_limit, page_limit)?);
        return Ok(packs);
    }

    if pack.topic0s.len() > 1 {
        let midpoint = pack.topic0s.len() / 2;
        let mut left = pack.clone();
        let mut right = pack;
        left.topic0s = left.topic0s[..midpoint].to_vec();
        right.topic0s = right.topic0s[midpoint..].to_vec();
        let mut packs = build_or_split_filter_pack(left, char_limit, page_limit)?;
        packs.extend(build_or_split_filter_pack(right, char_limit, page_limit)?);
        return Ok(packs);
    }

    bail!("single Coinbase SQL address/topic query exceeds SQL character budget")
}

fn coinbase_sql_network(chain: &str) -> Result<&'static str> {
    match chain {
        "base-mainnet" | "base" => Ok("base"),
        "base-sepolia" => Ok("base_sepolia"),
        chain => bail!("Coinbase SQL backfill currently supports Base chains only, got {chain}"),
    }
}

fn active_action_expression(column: &str) -> String {
    format!(
        "CASE WHEN toString({column}) IN ('1', 'added') THEN 1 WHEN toString({column}) IN ('-1', 'removed') THEN -1 ELSE 0 END"
    )
}

fn sql_string_literals(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", value.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}
