use anyhow::{Result, bail};

use super::pagination::CoinbaseSqlLogCursor;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlFilterPack {
    pub(super) chain: String,
    pub(super) from_block: i64,
    pub(super) to_block: i64,
    pub(super) addresses: Vec<String>,
    pub(super) topic0s: Vec<String>,
    pub(super) event_signatures: Vec<String>,
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

    let mut event_log_predicates = vec![format!(
        "l.block_number BETWEEN {} AND {}",
        pack.from_block, pack.to_block
    )];
    let mut encoded_log_predicates = event_log_predicates.clone();
    if !pack.scan_all_emitters {
        let address_predicate = format!("l.address IN ({})", sql_string_literals(&pack.addresses));
        event_log_predicates.push(address_predicate.clone());
        encoded_log_predicates.push(address_predicate);
    }
    if !pack.event_signatures.is_empty() {
        let event_signature_predicate = format!(
            "l.event_signature IN ({})",
            sql_string_literals(&pack.event_signatures)
        );
        event_log_predicates.push(event_signature_predicate);
    } else if !pack.topic0s.is_empty() {
        event_log_predicates.push(topic0_predicate(&pack.topic0s));
    }
    if !pack.topic0s.is_empty() {
        encoded_log_predicates.push(topic0_predicate(&pack.topic0s));
    }
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
    t.block_number AS block_number,
    t.block_hash AS block_hash,
    t.transaction_hash AS transaction_hash,
    t.transaction_index AS transaction_index
  FROM (
    SELECT
      block_number,
      block_hash,
      transaction_hash,
      transaction_index,
      sum({tx_action_expr}) AS action_sum
    FROM {network}.transactions
    WHERE block_number BETWEEN {from_block} AND {to_block}
    GROUP BY
      block_number,
      block_hash,
      transaction_hash,
      transaction_index
  ) t
  WHERE t.action_sum > 0
),
decoded_log_rows AS (
  SELECT
    l.block_number AS block_number,
    l.block_hash AS block_hash,
    l.transaction_hash AS transaction_hash,
    t.transaction_index AS transaction_index,
    l.log_index AS log_index,
    l.address AS emitting_address,
    l.event_signature AS event_signature,
    toJSONString(l.parameters) AS parameters,
    l.topics AS topics,
    {log_action_expr} AS action
  FROM {network}.events l
  JOIN active_transactions t
    ON t.block_number = l.block_number
   AND t.block_hash = l.block_hash
   AND t.transaction_hash = l.transaction_hash
  WHERE {event_log_predicates}
),
decoded_log_sums AS (
  SELECT
    l.block_number AS block_number,
    l.block_hash AS block_hash,
    l.transaction_hash AS transaction_hash,
    l.transaction_index AS transaction_index,
    l.log_index AS log_index,
    l.emitting_address AS emitting_address,
    any(l.event_signature) AS event_signature,
    any(l.parameters) AS parameters,
    l.topics AS topics,
    sum(l.action) AS action_sum
  FROM decoded_log_rows l
  GROUP BY
    l.block_number,
    l.block_hash,
    l.transaction_hash,
    l.transaction_index,
    l.log_index,
    l.emitting_address,
    l.topics
),
active_decoded_logs AS (
  SELECT
    e.block_number AS block_number,
    e.block_hash AS block_hash,
    e.transaction_hash AS transaction_hash,
    e.transaction_index AS transaction_index,
    e.log_index AS log_index,
    e.emitting_address AS emitting_address,
    e.event_signature AS event_signature,
    e.parameters AS parameters,
    e.topics AS topics
  FROM decoded_log_sums e
  WHERE e.action_sum > 0
),
encoded_log_rows AS (
  SELECT
    l.block_number AS block_number,
    l.block_hash AS block_hash,
    l.transaction_hash AS transaction_hash,
    t.transaction_index AS transaction_index,
    l.log_index AS log_index,
    l.address AS emitting_address,
    NULL AS event_signature,
    CAST(NULL AS Nullable(String)) AS parameters,
    l.topics AS topics,
    {log_action_expr} AS action
  FROM {network}.encoded_logs l
  JOIN active_transactions t
    ON t.block_number = l.block_number
   AND t.block_hash = l.block_hash
   AND t.transaction_hash = l.transaction_hash
  WHERE {encoded_log_predicates}
),
encoded_log_sums AS (
  SELECT
    l.block_number AS block_number,
    l.block_hash AS block_hash,
    l.transaction_hash AS transaction_hash,
    l.transaction_index AS transaction_index,
    l.log_index AS log_index,
    l.emitting_address AS emitting_address,
    NULL AS event_signature,
    CAST(NULL AS Nullable(String)) AS parameters,
    l.topics AS topics,
    sum(l.action) AS action_sum
  FROM encoded_log_rows l
  GROUP BY
    l.block_number,
    l.block_hash,
    l.transaction_hash,
    l.transaction_index,
    l.log_index,
    l.emitting_address,
    l.topics
),
active_encoded_logs AS (
  SELECT
    e.block_number AS block_number,
    e.block_hash AS block_hash,
    e.transaction_hash AS transaction_hash,
    e.transaction_index AS transaction_index,
    e.log_index AS log_index,
    e.emitting_address AS emitting_address,
    e.event_signature AS event_signature,
    e.parameters AS parameters,
    e.topics AS topics
  FROM encoded_log_sums e
  WHERE e.action_sum > 0
)
SELECT
  l.block_number AS block_number,
  l.block_hash AS block_hash,
  l.transaction_hash AS transaction_hash,
  l.transaction_index AS transaction_index,
  l.log_index AS log_index,
  l.emitting_address AS emitting_address,
  l.event_signature AS event_signature,
  l.parameters AS parameters,
  l.topics AS topics
FROM active_decoded_logs l
WHERE {output_predicates}
UNION ALL
SELECT
  l.block_number AS block_number,
  l.block_hash AS block_hash,
  l.transaction_hash AS transaction_hash,
  l.transaction_index AS transaction_index,
  l.log_index AS log_index,
  l.emitting_address AS emitting_address,
  l.event_signature AS event_signature,
  l.parameters AS parameters,
  l.topics AS topics
FROM active_encoded_logs l
WHERE {output_predicates}
ORDER BY block_number, transaction_index, log_index
LIMIT {limit}"#,
        from_block = pack.from_block,
        to_block = pack.to_block,
        tx_action_expr = tx_action_expr,
        log_action_expr = log_action_expr,
        event_log_predicates = event_log_predicates.join("\n    AND "),
        encoded_log_predicates = encoded_log_predicates.join("\n    AND "),
        output_predicates = output_predicates.join("\n  AND "),
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

    if pack.event_signatures.len() > 1 {
        let midpoint = pack.event_signatures.len() / 2;
        let mut left = pack.clone();
        let mut right = pack;
        left.event_signatures = left.event_signatures[..midpoint].to_vec();
        right.event_signatures = right.event_signatures[midpoint..].to_vec();
        let mut packs = build_or_split_filter_pack(left, char_limit, page_limit)?;
        packs.extend(build_or_split_filter_pack(right, char_limit, page_limit)?);
        return Ok(packs);
    }

    bail!("single Coinbase SQL address/event-signature query exceeds SQL character budget")
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

fn topic0_predicate(topic0s: &[String]) -> String {
    format!("l.topics[1] IN ({})", sql_string_literals(topic0s))
}

fn sql_string_literals(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", value.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}
