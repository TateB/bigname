use anyhow::Result;

use super::types::{PermissionsRawLogRow, ResolverResourceHint};
use super::util::dns_decode;

pub(super) fn resolver_resource_hint(
    raw_log: &PermissionsRawLogRow,
    upstream_resource: String,
    dns_encoded_name: Vec<u8>,
    selector_kind: &str,
    selector_key: Option<String>,
    selector_hash: Option<String>,
) -> Result<ResolverResourceHint> {
    let normalized_name = dns_decode(&dns_encoded_name).ok();
    Ok(ResolverResourceHint {
        upstream_resource,
        logical_name_id: normalized_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .map(|name| format!("{}:{name}", raw_log.namespace)),
        normalized_name,
        dns_encoded_name: Some(dns_encoded_name),
        selector_kind: selector_kind.to_owned(),
        selector_key,
        selector_hash,
        first_ref: raw_log.reference(),
    })
}

pub(super) fn fallback_resource_hint(
    raw_log: &PermissionsRawLogRow,
    upstream_resource: String,
    is_root: bool,
) -> ResolverResourceHint {
    ResolverResourceHint {
        upstream_resource,
        logical_name_id: None,
        normalized_name: None,
        dns_encoded_name: None,
        selector_kind: if is_root { "root" } else { "unknown" }.to_owned(),
        selector_key: None,
        selector_hash: None,
        first_ref: raw_log.reference(),
    }
}
