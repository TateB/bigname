use anyhow::{Result, bail};

use super::{
    client::{CoinbaseSqlClient, CoinbaseSqlQueryResponse},
    query::{CoinbaseSqlFilterPack, build_query},
    rows::CoinbaseSqlLogRow,
};
use crate::backfill::CoinbaseSqlFetchStats;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlLogCursor {
    pub(super) block_number: i64,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
}

impl From<&CoinbaseSqlLogRow> for CoinbaseSqlLogCursor {
    fn from(row: &CoinbaseSqlLogRow) -> Self {
        Self {
            block_number: row.block_number,
            transaction_index: row.transaction_index,
            log_index: row.log_index,
        }
    }
}

impl CoinbaseSqlLogCursor {
    fn strictly_after(self, previous: Self) -> bool {
        (self.block_number, self.transaction_index, self.log_index)
            > (
                previous.block_number,
                previous.transaction_index,
                previous.log_index,
            )
    }
}

pub(super) struct CoinbaseSqlFetchedPages {
    pub(super) rows: Vec<CoinbaseSqlLogRow>,
    pub(super) stats: CoinbaseSqlFetchStats,
}

pub(super) async fn fetch_all_pages(
    client: &CoinbaseSqlClient,
    pack: &CoinbaseSqlFilterPack,
    page_limit: usize,
    sql_char_limit: usize,
) -> Result<CoinbaseSqlFetchedPages> {
    let mut cursor = None;
    let mut rows = Vec::new();
    let mut stats = CoinbaseSqlFetchStats::default();
    let mut previous_cursor = None;

    loop {
        let sql = build_query(pack, cursor, page_limit)?;
        if sql.len() > sql_char_limit {
            bail!(
                "Coinbase SQL query length {} exceeds configured character limit {}",
                sql.len(),
                sql_char_limit
            );
        }
        let response = client.run_query(&sql).await?;
        record_response_stats(&mut stats, &response);
        let page_len = response.rows.len();

        for row in response.rows {
            let row_cursor = CoinbaseSqlLogCursor::from(&row);
            if let Some(previous) = previous_cursor
                && !row_cursor.strictly_after(previous)
            {
                bail!(
                    "Coinbase SQL rows were not strictly ordered at block {}, transaction index {}, log index {}",
                    row.block_number,
                    row.transaction_index,
                    row.log_index
                );
            }
            previous_cursor = Some(row_cursor);
            rows.push(row);
        }

        if page_len < page_limit {
            break;
        }
        cursor = previous_cursor;
        if cursor.is_none() {
            bail!("Coinbase SQL returned a full page without a cursor row");
        }
    }

    Ok(CoinbaseSqlFetchedPages { rows, stats })
}

fn record_response_stats(stats: &mut CoinbaseSqlFetchStats, response: &CoinbaseSqlQueryResponse) {
    stats.query_count += 1;
    stats.retry_count += response.retry_count;
    stats.record_page(response.rows.len());
}
