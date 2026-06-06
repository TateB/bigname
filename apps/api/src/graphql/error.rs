use async_graphql::{Error, ErrorExtensions};

/// Map a storage/`anyhow` failure into a GraphQL error carrying a stable `code` extension, logging
/// the underlying cause like the REST handlers do. The detailed cause is not surfaced to clients.
pub(super) fn internal_error(operation: &str, error: anyhow::Error) -> Error {
    tracing::error!(operation, error = ?error, "graphql resolver failed");
    Error::new(format!("{operation} failed"))
        .extend_with(|_, ext| ext.set("code", "internal_error"))
}
