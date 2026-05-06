use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::{
    Decode, Postgres, Type,
    error::BoxDynError,
    postgres::{PgTypeInfo, PgValueRef},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

use crate::CanonicalityState;

/// Persisted stable token lineage anchor for one resource ownership history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenLineage {
    pub token_lineage_id: Uuid,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Persisted stable backing-resource anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resource {
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Persisted canonical public-surface identity for one logical name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameSurface {
    pub logical_name_id: String,
    pub namespace: String,
    pub input_name: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub dns_encoded_name: Vec<u8>,
    pub namehash: String,
    pub labelhashes: Vec<String>,
    pub normalizer_version: String,
    pub normalization_warnings: Value,
    pub normalization_errors: Value,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Storage-local binding taxonomy between surfaces and backing resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SurfaceBindingKind {
    DeclaredRegistryPath,
    LinkedSubregistryPath,
    ResolverAliasPath,
    ObservedWildcardPath,
    MigrationRebind,
    ObservedOnly,
}

impl SurfaceBindingKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeclaredRegistryPath => "declared_registry_path",
            Self::LinkedSubregistryPath => "linked_subregistry_path",
            Self::ResolverAliasPath => "resolver_alias_path",
            Self::ObservedWildcardPath => "observed_wildcard_path",
            Self::MigrationRebind => "migration_rebind",
            Self::ObservedOnly => "observed_only",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "declared_registry_path" => Ok(Self::DeclaredRegistryPath),
            "linked_subregistry_path" => Ok(Self::LinkedSubregistryPath),
            "resolver_alias_path" => Ok(Self::ResolverAliasPath),
            "observed_wildcard_path" => Ok(Self::ObservedWildcardPath),
            "migration_rebind" => Ok(Self::MigrationRebind),
            "observed_only" => Ok(Self::ObservedOnly),
            _ => bail!("unknown surface binding kind {value}"),
        }
    }
}

impl Type<Postgres> for SurfaceBindingKind {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <String as Type<Postgres>>::compatible(ty)
    }
}

impl<'r> Decode<'r, Postgres> for SurfaceBindingKind {
    fn decode(value: PgValueRef<'r>) -> std::result::Result<Self, BoxDynError> {
        let value = <String as Decode<Postgres>>::decode(value)?;
        Self::parse(&value).map_err(Into::into)
    }
}

/// Persisted time-ranged mapping from a public surface to a backing resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SurfaceBinding {
    pub surface_binding_id: Uuid,
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub binding_kind: SurfaceBindingKind,
    pub active_from: OffsetDateTime,
    pub active_to: Option<OffsetDateTime>,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub provenance: Value,
    pub canonicality_state: CanonicalityState,
}

/// Counts of identity rows orphaned during one losing-branch repair.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdentityOrphanCounts {
    pub token_lineage_count: u64,
    pub resource_count: u64,
    pub name_surface_count: u64,
    pub surface_binding_count: u64,
}
