use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_manifests::{
    ActiveManifestVersion, CapabilitySupportStatus, NamespaceManifestSnapshot,
    load_namespace_manifest_snapshot,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{AppState, ensure_public_namespace};

use super::{
    Completeness, Envelope, Meta, NoQueryParams, V2Error, V2Result, api_error_to_v2,
    numeric_to_slug, slug_to_numeric,
};

const UNSUPPORTED_REASON: &str = "not_supported_for_namespace";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Namespace {
    pub(crate) namespace: String,
    pub(crate) capabilities: BTreeMap<String, NamespaceCapability>,
    pub(crate) networks: Vec<NamespaceNetwork>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceCapability {
    pub(crate) completeness: Completeness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceNetwork {
    pub(crate) network: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chain_id: Option<u64>,
}

pub(crate) async fn get_namespace(
    Path(namespace): Path<String>,
    _no_query: NoQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Namespace>>> {
    ensure_public_namespace(&namespace).map_err(api_error_to_v2)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load v2 namespace metadata"
            );
            V2Error::internal_error(format!(
                "failed to load namespace metadata for namespace {namespace}"
            ))
        })?;

    Ok(Json(Envelope {
        data: build_namespace(namespace, snapshot)?,
        page: None,
        meta: Meta::default(),
    }))
}

fn build_namespace(namespace: String, snapshot: NamespaceManifestSnapshot) -> V2Result<Namespace> {
    Ok(Namespace {
        namespace,
        capabilities: aggregate_capabilities(&snapshot.manifests)?,
        networks: namespace_networks(&snapshot.manifests),
    })
}

fn aggregate_capabilities(
    manifests: &[ActiveManifestVersion],
) -> V2Result<BTreeMap<String, NamespaceCapability>> {
    let mut capability_counts = BTreeMap::<String, (usize, usize)>::new();

    for manifest in manifests {
        for (raw_name, flag) in &manifest.capability_flags {
            let product_name = product_capability_name(raw_name)?.to_owned();
            let (declared_count, supported_count) =
                capability_counts.entry(product_name).or_default();
            *declared_count += 1;
            if flag.status == CapabilitySupportStatus::Supported {
                *supported_count += 1;
            }
        }
    }

    Ok(capability_counts
        .into_iter()
        .map(|(capability, (declared_count, supported_count))| {
            let completeness = if supported_count == declared_count {
                Completeness::Full
            } else if supported_count > 0 {
                Completeness::Partial
            } else {
                Completeness::Unsupported
            };
            let unsupported_reason =
                (completeness == Completeness::Unsupported).then(|| UNSUPPORTED_REASON.to_owned());

            (
                capability,
                NamespaceCapability {
                    completeness,
                    unsupported_reason,
                },
            )
        })
        .collect())
}

fn product_capability_name(raw_name: &str) -> V2Result<&'static str> {
    match raw_name {
        "declared_children" => Ok("subnames"),
        "exact_name_profile" => Ok("name_profile"),
        "name_history" => Ok("name_history"),
        "verified_resolution" => Ok("verified_records"),
        _ => {
            error!(
                service = "api",
                raw_capability = %raw_name,
                "missing v2 product capability mapping"
            );
            Err(V2Error::internal_error(
                "namespace capability mapping is missing",
            ))
        }
    }
}

fn namespace_networks(manifests: &[ActiveManifestVersion]) -> Vec<NamespaceNetwork> {
    manifests
        .iter()
        .map(|manifest| manifest.chain.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(namespace_network)
        .collect()
}

fn namespace_network(chain: &str) -> NamespaceNetwork {
    let chain_id = slug_to_numeric(chain);
    let canonical_slug = chain_id.and_then(numeric_to_slug).unwrap_or(chain);

    NamespaceNetwork {
        network: display_network_slug(canonical_slug).to_owned(),
        chain_id,
    }
}

fn display_network_slug(chain_slug: &str) -> &str {
    match chain_slug {
        bigname_storage::BASE_MAINNET_CHAIN_ID => "base",
        bigname_storage::ETHEREUM_MAINNET_CHAIN_ID => "ethereum",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::{
        extract::{FromRequestParts, Path},
        http::{Request, StatusCode},
        response::IntoResponse,
    };
    use bigname_manifests::{CapabilityFlag, CapabilitySupportStatus};
    use sqlx::PgPool;

    use super::*;

    #[test]
    fn capability_aggregation_maps_full_partial_and_unsupported() {
        let manifests = vec![
            manifest(
                "ens_l1",
                "ethereum-mainnet",
                [
                    ("declared_children", CapabilitySupportStatus::Supported),
                    ("verified_resolution", CapabilitySupportStatus::Supported),
                ],
            ),
            manifest(
                "ens_l2",
                "base-mainnet",
                [
                    ("verified_resolution", CapabilitySupportStatus::Unsupported),
                    ("name_history", CapabilitySupportStatus::Supported),
                ],
            ),
            manifest(
                "ens_l3",
                "ethereum-mainnet",
                [
                    ("exact_name_profile", CapabilitySupportStatus::Shadow),
                    ("name_history", CapabilitySupportStatus::Supported),
                ],
            ),
        ];

        let capabilities =
            aggregate_capabilities(&manifests).expect("capability aggregation must succeed");

        assert_eq!(
            capabilities["subnames"],
            NamespaceCapability {
                completeness: Completeness::Full,
                unsupported_reason: None,
            }
        );
        assert_eq!(
            capabilities["verified_records"],
            NamespaceCapability {
                completeness: Completeness::Partial,
                unsupported_reason: None,
            }
        );
        assert_eq!(
            capabilities["name_history"],
            NamespaceCapability {
                completeness: Completeness::Full,
                unsupported_reason: None,
            }
        );
        assert_eq!(
            capabilities["name_profile"],
            NamespaceCapability {
                completeness: Completeness::Unsupported,
                unsupported_reason: Some(UNSUPPORTED_REASON.to_owned()),
            }
        );
    }

    #[test]
    fn namespace_networks_use_display_slugs_and_numeric_chain_ids() {
        let manifests = vec![
            manifest("base_registry", "base-mainnet", []),
            manifest("ens_registry", "ethereum-mainnet", []),
            manifest("unknown_registry", "future-testnet", []),
        ];

        assert_eq!(
            namespace_networks(&manifests),
            vec![
                NamespaceNetwork {
                    network: "base".to_owned(),
                    chain_id: Some(8453),
                },
                NamespaceNetwork {
                    network: "ethereum".to_owned(),
                    chain_id: Some(1),
                },
                NamespaceNetwork {
                    network: "future-testnet".to_owned(),
                    chain_id: None,
                },
            ]
        );
    }

    #[test]
    fn missing_product_mapping_is_an_internal_error_without_leaking_the_raw_key() {
        let error = product_capability_name("declared_internal_pipeline").expect_err(
            "unmapped capability keys must not be exposed on the product namespace route",
        );

        let envelope = error.envelope();
        assert_eq!(envelope.error.code, "internal_error");
        assert_eq!(
            envelope.error.message,
            "namespace capability mapping is missing"
        );
    }

    #[tokio::test]
    async fn get_namespace_returns_not_found_for_unsupported_namespace() {
        let state = AppState {
            phase: "test",
            pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
                .expect("unsupported namespace validation does not use the database"),
            chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
        };

        let error = get_namespace(Path("unknown".to_owned()), NoQueryParams, State(state))
            .await
            .expect_err("unsupported namespace must return an error");
        let envelope = error.envelope();
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(envelope.error.code, "not_found");
        assert_eq!(envelope.error.message, "namespace unknown is not supported");
    }

    #[tokio::test]
    async fn no_query_params_rejects_namespace_controls() {
        let request = Request::builder()
            .uri("/v2/namespaces/ens?at=2026-06-10T00:00:00Z")
            .body(())
            .expect("request must build");
        let (mut parts, ()) = request.into_parts();

        let error = NoQueryParams::from_request_parts(&mut parts, &())
            .await
            .expect_err("namespace metadata route must reject query params");

        assert_eq!(error.envelope().error.code, "invalid_input");
    }

    fn manifest<const N: usize>(
        source_family: &str,
        chain: &str,
        capabilities: [(&str, CapabilitySupportStatus); N],
    ) -> ActiveManifestVersion {
        ActiveManifestVersion {
            manifest_version: 1,
            source_family: source_family.to_owned(),
            chain: chain.to_owned(),
            deployment_epoch: "test".to_owned(),
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            capability_flags: capabilities
                .into_iter()
                .map(|(name, status)| {
                    (
                        name.to_owned(),
                        CapabilityFlag {
                            status,
                            notes: None,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        }
    }
}
