use anyhow::{Context, Result, bail};
use sqlx::{
    Decode, Postgres, Type,
    error::BoxDynError,
    postgres::{PgTypeInfo, PgValueRef},
};

use super::reads::{
    ensure_chain_lineage_path_reaches_stop, load_chain_lineage_path,
    load_lineage_snapshots_for_hashes,
};
use super::types::{CanonicalityState, ChainLineageBlock};

impl CanonicalityState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Canonical => "canonical",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
            Self::Orphaned => "orphaned",
        }
    }

    pub(crate) fn promote_to(self, target: Self) -> Self {
        match target {
            Self::Observed => {
                if self == Self::Orphaned {
                    Self::Observed
                } else {
                    self
                }
            }
            Self::Canonical | Self::Safe | Self::Finalized => {
                if self == Self::Orphaned {
                    return target;
                }

                if self.rank() >= target.rank() {
                    self
                } else {
                    target
                }
            }
            Self::Orphaned => Self::Orphaned,
        }
    }

    pub fn merge_observation(self, incoming: Self) -> Self {
        match incoming {
            Self::Orphaned => Self::Orphaned,
            Self::Observed => {
                if self == Self::Orphaned {
                    Self::Observed
                } else {
                    self
                }
            }
            Self::Canonical | Self::Safe | Self::Finalized => {
                if self == Self::Orphaned {
                    incoming
                } else {
                    self.promote_to(incoming)
                }
            }
        }
    }

    pub const fn rank(self) -> u8 {
        match self {
            Self::Observed => 0,
            Self::Canonical => 1,
            Self::Safe => 2,
            Self::Finalized => 3,
            Self::Orphaned => 4,
        }
    }

    pub fn weakest(states: impl IntoIterator<Item = Self>) -> Option<Self> {
        states.into_iter().min_by_key(|state| state.rank())
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "observed" => Ok(Self::Observed),
            "canonical" => Ok(Self::Canonical),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            "orphaned" => Ok(Self::Orphaned),
            _ => bail!("unknown canonicality_state value {value}"),
        }
    }
}

impl Type<Postgres> for CanonicalityState {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <String as Type<Postgres>>::compatible(ty)
    }
}

impl<'r> Decode<'r, Postgres> for CanonicalityState {
    fn decode(value: PgValueRef<'r>) -> std::result::Result<Self, BoxDynError> {
        let value = <String as Decode<Postgres>>::decode(value)?;
        Self::parse(&value).map_err(Into::into)
    }
}

pub(crate) async fn promote_chain_lineage_path(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
    target_state: CanonicalityState,
    require_stop: bool,
) -> Result<Vec<ChainLineageBlock>> {
    let path = load_chain_lineage_path(&mut **executor, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if path.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }
    if require_stop {
        ensure_chain_lineage_path_reaches_stop(chain_id, from_hash, stop_before_hash, &path)?;
    }

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN canonicality_state = 'orphaned'::canonicality_state THEN $3::canonicality_state
            WHEN $3::canonicality_state = 'canonical'::canonicality_state
                AND canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                THEN canonicality_state
            WHEN $3::canonicality_state = 'safe'::canonicality_state
                AND canonicality_state = 'finalized'::canonicality_state
                THEN canonicality_state
            WHEN $3::canonicality_state = 'observed'::canonicality_state
                THEN canonicality_state
            ELSE $3::canonicality_state
        END
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .bind(target_state.as_str())
    .execute(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to promote lineage path for chain {chain_id} starting from block {from_hash}"
        )
    })?;

    load_lineage_snapshots_for_hashes(&mut **executor, chain_id, &block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to reload promoted lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })
}
