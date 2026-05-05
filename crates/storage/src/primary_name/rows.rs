use anyhow::Result;
use sqlx::postgres::PgRow;

use super::types::{PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot};

pub(super) fn decode_primary_name_current_snapshot(
    row: PgRow,
) -> Result<PrimaryNameCurrentSnapshot> {
    Ok(PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: crate::sql_row::get::<String>(&row, "address")?.to_ascii_lowercase(),
            namespace: crate::sql_row::get(&row, "namespace")?,
            coin_type: crate::sql_row::get(&row, "coin_type")?,
            claim_status: PrimaryNameClaimStatus::parse(&crate::sql_row::get::<String>(
                &row,
                "claim_status",
            )?)?,
            raw_claim_name: crate::sql_row::get(&row, "raw_claim_name")?,
            claim_provenance: crate::sql_row::get(&row, "claim_provenance")?,
        },
        normalized_claim_name: crate::sql_row::get(&row, "normalized_claim_name")?,
    })
}
