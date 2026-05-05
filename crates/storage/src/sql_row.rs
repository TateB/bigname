use anyhow::{Context, Result};
use sqlx::{Decode, Postgres, Row, Type, postgres::PgRow};

pub(crate) fn get<'r, T>(row: &'r PgRow, column: &'static str) -> Result<T>
where
    T: Decode<'r, Postgres> + Type<Postgres>,
{
    row.try_get(column)
        .with_context(|| format!("missing {column}"))
}
