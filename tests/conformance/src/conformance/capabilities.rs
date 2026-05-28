        #[derive(Clone, Copy, Debug)]
        struct CapabilityCutoverEvidence {
            capability_group: &'static str,
            route_owner: &'static [&'static str],
            conformance_owner: &'static str,
            rollout_gate: &'static [&'static str],
            rollback_gate: &'static [&'static str],
        }

        const RELEASE_SMOKE_GATE: &str = "scripts/release-smoke";
        const ROLLBACK_SMOKE_GATE: &str = "scripts/rollback-smoke";
        const OPENAPI_ROUTE_OWNER_GATE: &str = "OpenAPI route owner guard";
        const CAPABILITY_GOLDEN_FIXTURE_SCOPE: &str = "local_cutover_evidence";
        const FORBIDDEN_GOLDEN_CLAIM_TERMS: &[&str] = &[
            "imported app",
            "external app",
            "first-party app",
            "app call-site",
            "app call site",
            "app_call_site",
            "call-site",
            "call_site",
            "parity",
            "replacement",
            "legacy schema",
            "consumer replacement",
            "consumer-replacement",
        ];

        const CAPABILITY_CUTOVER_EVIDENCE: &[CapabilityCutoverEvidence] = &[
            CapabilityCutoverEvidence {
                capability_group: "exact name profile",
                route_owner: &["/v1/names/{namespace}/{name}", "/v1/coverage/{namespace}/{name}"],
                conformance_owner: "exact_name.rs::exact_name_contract_* and exact_name.rs::coverage_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused exact-name conformance",
                    RELEASE_SMOKE_GATE,
                    "matching manifest support state where applicable",
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "route envelope returns prior supported / unsupported coverage state",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "names owned / controlled by address",
                route_owner: &["/v1/addresses/{address}/names"],
                conformance_owner: "collections.rs::address_names_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused collection conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable cursor / response-shape rollback for address collection",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "names owned / controlled by address with role summary",
                route_owner: &["/v1/addresses/{address}/names?include=role_summary"],
                conformance_owner: "collections.rs::address_names_role_summary_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "role-summary conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "base address collection remains stable without role-summary widening",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "compact names collection",
                route_owner: &["/v1/names"],
                conformance_owner: "apps/api tests::names_collection",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused names collection API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable compact names pagination and explicit unsupported filter behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "declared child subnames and counts",
                route_owner: &["/v1/names/{namespace}/{name}/children"],
                conformance_owner:
                    "collections.rs::name_children_contract_* and apps/api tests::collections child compact default",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused children conformance and compact-default API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable child collection pagination / unsupported behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "record inventory for editing",
                route_owner: &["/v1/profiles/names/{name}", "/v1/names/{namespace}/{name}/records"],
                conformance_owner: "apps/api tests::resolution profile record selection and records compact route",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused profile and records API route tests",
                    "supported resolver-profile evidence",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "explicit unsupported / pending resolver-family state",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "compact name records",
                route_owner: &["/v1/names/{namespace}/{name}/records"],
                conformance_owner: "apps/api tests::records",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused compact records API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable compact record summary and explicit unsupported verified mode",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "verified record reads",
                route_owner: &[
                    "/v1/profiles/names/{name}",
                    "/v1/names/{namespace}/{name}/records",
                    "/v1/explain/resolutions/{namespace}/{name}/execution",
                ],
                conformance_owner: "apps/api tests::resolution profile execution and records verified mode; resolution_and_permissions.rs::resolution_execution_explain_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused profile / records verified API route tests",
                    "execution trace persistence checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "execution cache invalidation / unsupported verified-query behavior remains stable",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "name history",
                route_owner: &["/v1/history/names/{namespace}/{name}"],
                conformance_owner:
                    "history.rs::name_history_contract_* and apps/api tests::history compact view",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused history conformance and compact-view API route tests",
                    "replay stability checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable cursor and canonical-only history behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "address history across names",
                route_owner: &["/v1/history/addresses/{address}"],
                conformance_owner:
                    "history.rs::address_history_contract_* and apps/api tests::history compact view",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused address-history conformance and compact-view API route tests",
                    "replay stability checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable address-anchor selection and pagination behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "event stream",
                route_owner: &["/v1/events"],
                conformance_owner: "apps/api tests::events",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused events API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable canonical event filters, pagination, and reserved-filter errors",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "role holders for a resource",
                route_owner: &["/v1/resources/{resource_id}/permissions"],
                conformance_owner: "resolution_and_permissions.rs::resource_permissions_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused permissions conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable permission envelope and explicit unsupported summaries",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "role rows by filter",
                route_owner: &["/v1/roles"],
                conformance_owner: "apps/api tests::roles",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused roles API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable role filters and missing-primary-filter errors",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "name role holders",
                route_owner: &["/v1/names/{namespace}/{name}/roles"],
                conformance_owner: "apps/api tests::roles name roles",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused name roles API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable name role pagination and resource resolution",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "resource lookup",
                route_owner: &["/v1/resources/lookup"],
                conformance_owner: "apps/api tests::roles resource lookup",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused resource lookup API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable name-current resource identity lookup behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "role change history",
                route_owner: &["/v1/history/resources/{resource_id}"],
                conformance_owner:
                    "history.rs::resource_history_contract_* and apps/api tests::history compact view",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused history conformance and compact-view API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[ROLLBACK_SMOKE_GATE, "stable resource-history cursor behavior"],
            },
            CapabilityCutoverEvidence {
                capability_group: "resolver-centric overview",
                route_owner: &["/v1/resolvers/{chain_id}/{resolver_address}/overview"],
                conformance_owner: "resolution_and_permissions.rs::resolver_overview_contract_* and apps/api tests::resolvers",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused resolver overview conformance",
                    "supported resolver-profile evidence",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "null compact sections and meta.unsupported_fields for pending / unsupported resolver profiles",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "compact resolver overview",
                route_owner: &["/v1/resolvers/{chain_id}/{resolver_address}/overview"],
                conformance_owner: "apps/api tests::resolvers",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused compact resolver overview API route tests",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable compact resolver sections and explicit unprojected fields",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "claimed vs verified primary name",
                route_owner: &["/v1/primary-names/{address}"],
                conformance_owner: "primary_names.rs::primary_names_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused primary-name conformance",
                    "persisted execution readback checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable partial exact-tuple coverage, explicit unsupported out-of-class behavior, and execution-derived verification rollback",
                ],
            },
        ];

        #[derive(Clone, Copy, Debug)]
        struct CapabilityGoldenFixtureDocument {
            fixture_path: &'static str,
            body: &'static str,
        }

        const CAPABILITY_GOLDEN_RESPONSE_FIXTURES: &[CapabilityGoldenFixtureDocument] = &[
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/exact-name-profile.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/exact-name-profile.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/names-owned-controlled-by-address.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/names-owned-controlled-by-address.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path:
                    "fixtures/capabilities/names-owned-controlled-by-address-with-role-summary.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/names-owned-controlled-by-address-with-role-summary.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/compact-names-collection.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/compact-names-collection.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/declared-child-subnames-and-counts.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/declared-child-subnames-and-counts.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/record-inventory-for-editing.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/record-inventory-for-editing.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/compact-name-records.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/compact-name-records.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/verified-record-reads.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/verified-record-reads.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/name-history.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/name-history.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/address-history-across-names.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/address-history-across-names.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/event-stream.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/event-stream.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/role-holders-for-a-resource.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/role-holders-for-a-resource.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/role-rows-by-filter.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/role-rows-by-filter.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/name-role-holders.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/name-role-holders.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/resource-lookup.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/resource-lookup.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/role-change-history.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/role-change-history.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/resolver-centric-overview.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/resolver-centric-overview.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/compact-resolver-overview.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/compact-resolver-overview.json"
                )),
            },
            CapabilityGoldenFixtureDocument {
                fixture_path: "fixtures/capabilities/claimed-vs-verified-primary-name.json",
                body: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/fixtures/capabilities/claimed-vs-verified-primary-name.json"
                )),
            },
        ];

        #[derive(Debug, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct CapabilityGoldenFixture {
            capability_group: String,
            conformance_owner: String,
            fixture_id: String,
            request: CapabilityGoldenFixtureRequest,
            response: Value,
            rollback_gate: Vec<String>,
            rollout_gate: Vec<String>,
            route_owner: Vec<String>,
            scope: String,
        }

        #[derive(Debug, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct CapabilityGoldenFixtureRequest {
            method: String,
            path: String,
            query: BTreeMap<String, String>,
        }

        #[test]
        fn capability_cutover_evidence_is_complete_for_shipped_groups() {
            let expected_groups = [
                "exact name profile",
                "names owned / controlled by address",
                "names owned / controlled by address with role summary",
                "compact names collection",
                "declared child subnames and counts",
                "record inventory for editing",
                "compact name records",
                "verified record reads",
                "name history",
                "address history across names",
                "event stream",
                "role holders for a resource",
                "role rows by filter",
                "name role holders",
                "resource lookup",
                "role change history",
                "resolver-centric overview",
                "compact resolver overview",
                "claimed vs verified primary name",
            ];
            let openapi_owned_paths = OPENAPI_CONFORMANCE_COVERAGE
                .iter()
                .filter_map(|coverage| match coverage.scope {
                    OpenApiConformanceScope::HarnessOwner(_) => Some(coverage.path),
                    OpenApiConformanceScope::OutOfScope(_) => None,
                })
                .collect::<BTreeSet<_>>();
            let mut evidence_by_group = BTreeMap::new();

            for evidence in CAPABILITY_CUTOVER_EVIDENCE {
                assert!(
                    !evidence.capability_group.trim().is_empty(),
                    "capability group must be explicit"
                );
                assert!(
                    evidence_by_group
                        .insert(evidence.capability_group, evidence)
                        .is_none(),
                    "duplicate cutover evidence for {}",
                    evidence.capability_group
                );
                assert!(
                    !evidence.route_owner.is_empty(),
                    "{} must name at least one route owner",
                    evidence.capability_group
                );
                assert!(
                    !evidence.conformance_owner.trim().is_empty(),
                    "{} must name a conformance owner",
                    evidence.capability_group
                );
                assert!(
                    evidence.rollout_gate.contains(&OPENAPI_ROUTE_OWNER_GATE),
                    "{} rollout gate must include the OpenAPI route owner guard",
                    evidence.capability_group
                );
                assert!(
                    evidence.rollout_gate.contains(&RELEASE_SMOKE_GATE),
                    "{} rollout gate must include release smoke",
                    evidence.capability_group
                );
                assert!(
                    evidence.rollback_gate.contains(&ROLLBACK_SMOKE_GATE),
                    "{} rollback gate must include rollback smoke",
                    evidence.capability_group
                );

                for route_owner in evidence.route_owner {
                    let path = route_owner
                        .split_once('?')
                        .map_or(*route_owner, |(path, _query)| path);
                    assert!(
                        openapi_owned_paths.contains(path),
                        "{} references route owner {route_owner}, but {path} has no OpenAPI conformance owner",
                        evidence.capability_group
                    );
                }
            }

            let missing_groups = expected_groups
                .iter()
                .filter(|group| !evidence_by_group.contains_key(**group))
                .collect::<Vec<_>>();
            assert!(
                missing_groups.is_empty(),
                "missing cutover evidence for shipped capability groups: {missing_groups:#?}"
            );

            let unexpected_groups = evidence_by_group
                .keys()
                .filter(|group| !expected_groups.contains(group))
                .collect::<Vec<_>>();
            assert!(
                unexpected_groups.is_empty(),
                "unexpected capability cutover evidence groups: {unexpected_groups:#?}"
            );
        }

        #[test]
        fn capability_golden_response_fixtures_cover_cutover_evidence() -> Result<()> {
            let checked_in_fixture_paths = checked_in_capability_golden_fixture_paths()?;
            let expected_fixture_paths = CAPABILITY_GOLDEN_RESPONSE_FIXTURES
                .iter()
                .map(|document| document.fixture_path.to_owned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(
                checked_in_fixture_paths, expected_fixture_paths,
                "checked-in capability golden fixtures must be exactly the fixture documents covered by the harness"
            );

            let evidence_by_group = CAPABILITY_CUTOVER_EVIDENCE
                .iter()
                .map(|evidence| (evidence.capability_group, evidence))
                .collect::<BTreeMap<_, _>>();
            let mut fixtures_by_group = BTreeMap::new();

            for document in CAPABILITY_GOLDEN_RESPONSE_FIXTURES {
                let fixture = parse_capability_golden_fixture(document)?;
                let raw_fixture: Value = serde_json::from_str(document.body)
                    .with_context(|| format!("failed to parse {}", document.fixture_path))?;
                assert_no_imported_claim_terms(document.fixture_path, &raw_fixture);
                assert_eq!(
                    fixture.scope.as_str(),
                    CAPABILITY_GOLDEN_FIXTURE_SCOPE,
                    "{} must stay scoped to local cutover evidence",
                    document.fixture_path
                );
                assert_eq!(
                    fixture.fixture_id.as_str(),
                    capability_fixture_id(&fixture.capability_group),
                    "{} fixture_id must be derived from capability_group",
                    document.fixture_path
                );
                assert_eq!(
                    document.fixture_path,
                    format!("fixtures/capabilities/{}.json", fixture.fixture_id),
                    "{} fixture path must match fixture_id",
                    document.fixture_path
                );
                assert!(
                    fixture.response.is_object(),
                    "{} response fixture must be a JSON object",
                    document.fixture_path
                );
                assert!(
                    matches!(fixture.response.get("coverage"), Some(Value::Object(_))),
                    "{} response fixture must include a coverage object",
                    document.fixture_path
                );
                assert!(
                    matches!(fixture.response.get("data"), Some(Value::Object(_))),
                    "{} response fixture must include a data object",
                    document.fixture_path
                );
                assert_eq!(
                    fixture.request.method, "GET",
                    "{} golden fixture request must be a read route",
                    document.fixture_path
                );

                let evidence = evidence_by_group
                    .get(fixture.capability_group.as_str())
                    .with_context(|| {
                        format!(
                            "{} references capability group without cutover evidence: {}",
                            document.fixture_path, fixture.capability_group
                        )
                    })?;
                assert_eq!(
                    fixture.route_owner.as_slice(),
                    evidence.route_owner,
                    "{} route owners must match CAPABILITY_CUTOVER_EVIDENCE",
                    document.fixture_path
                );
                assert_eq!(
                    fixture.conformance_owner.as_str(),
                    evidence.conformance_owner,
                    "{} conformance owner must match CAPABILITY_CUTOVER_EVIDENCE",
                    document.fixture_path
                );
                assert_eq!(
                    fixture.rollout_gate.as_slice(),
                    evidence.rollout_gate,
                    "{} rollout gates must match CAPABILITY_CUTOVER_EVIDENCE",
                    document.fixture_path
                );
                assert_eq!(
                    fixture.rollback_gate.as_slice(),
                    evidence.rollback_gate,
                    "{} rollback gates must match CAPABILITY_CUTOVER_EVIDENCE",
                    document.fixture_path
                );
                assert!(
                    fixture
                        .route_owner
                        .contains(&capability_fixture_route_owner(&fixture.request)),
                    "{} request must target one of the declared route owners",
                    document.fixture_path
                );
                assert!(
                    fixtures_by_group
                        .insert(fixture.capability_group.clone(), document.fixture_path)
                        .is_none(),
                    "duplicate golden fixture for {}",
                    fixture.capability_group
                );
            }

            let fixture_groups = fixtures_by_group.keys().cloned().collect::<BTreeSet<_>>();
            let evidence_groups = CAPABILITY_CUTOVER_EVIDENCE
                .iter()
                .map(|evidence| evidence.capability_group.to_owned())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                fixture_groups, evidence_groups,
                "capability golden response fixtures must cover every local cutover evidence group"
            );

            Ok(())
        }

        #[test]
        fn capability_golden_response_fixtures_are_stably_formatted() -> Result<()> {
            for document in CAPABILITY_GOLDEN_RESPONSE_FIXTURES {
                let parsed: Value = serde_json::from_str(document.body)
                    .with_context(|| format!("failed to parse {}", document.fixture_path))?;
                let stable_body = format!("{}\n", serde_json::to_string_pretty(&parsed)?);
                assert_eq!(
                    document.body, stable_body,
                    "{} must stay in serde_json pretty format",
                    document.fixture_path
                );
            }

            Ok(())
        }

        fn parse_capability_golden_fixture(
            document: &CapabilityGoldenFixtureDocument,
        ) -> Result<CapabilityGoldenFixture> {
            serde_json::from_str(document.body)
                .with_context(|| format!("failed to parse {}", document.fixture_path))
        }

        fn checked_in_capability_golden_fixture_paths() -> Result<Vec<String>> {
            let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures")
                .join("capabilities");
            let mut fixture_paths = std::fs::read_dir(&fixture_dir)
                .with_context(|| {
                    format!(
                        "failed to read capability golden fixture directory {}",
                        fixture_dir.display()
                    )
                })?
                .map(|entry| {
                    let entry = entry.with_context(|| {
                        format!(
                            "failed to read capability golden fixture entry in {}",
                            fixture_dir.display()
                        )
                    })?;
                    let file_name = entry.file_name();
                    let file_name = file_name.to_string_lossy();
                    Ok(format!("fixtures/capabilities/{file_name}"))
                })
                .collect::<Result<Vec<_>>>()?;
            fixture_paths.retain(|path| path.ends_with(".json"));
            fixture_paths.sort();

            Ok(fixture_paths)
        }

        fn capability_fixture_id(capability_group: &str) -> String {
            let mut fixture_id = String::new();
            let mut last_was_separator = false;

            for character in capability_group.chars() {
                if character.is_ascii_alphanumeric() {
                    fixture_id.push(character.to_ascii_lowercase());
                    last_was_separator = false;
                } else if !last_was_separator {
                    fixture_id.push('-');
                    last_was_separator = true;
                }
            }

            fixture_id.trim_matches('-').to_owned()
        }

        fn capability_fixture_route_owner(request: &CapabilityGoldenFixtureRequest) -> String {
            if request.query.is_empty() {
                return request.path.clone();
            }

            let query = request
                .query
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&");
            format!("{}?{}", request.path, query)
        }

        fn assert_no_imported_claim_terms(fixture_path: &str, value: &Value) {
            match value {
                Value::Array(items) => {
                    for item in items {
                        assert_no_imported_claim_terms(fixture_path, item);
                    }
                }
                Value::Object(fields) => {
                    for (key, item) in fields {
                        assert_no_forbidden_golden_claim_text(fixture_path, key);
                        assert_no_imported_claim_terms(fixture_path, item);
                    }
                }
                Value::String(text) => {
                    assert_no_forbidden_golden_claim_text(fixture_path, text);
                }
                Value::Bool(_) | Value::Null | Value::Number(_) => {}
            }
        }

        fn assert_no_forbidden_golden_claim_text(fixture_path: &str, text: &str) {
            let text_lowercase = text.to_ascii_lowercase();
            for forbidden_term in FORBIDDEN_GOLDEN_CLAIM_TERMS {
                assert!(
                    !text_lowercase.contains(forbidden_term),
                    "{fixture_path} contains forbidden imported-call-site or parity claim term {forbidden_term:?}: {text:?}"
                );
            }
        }
