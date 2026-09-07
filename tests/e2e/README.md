# bigname end-to-end scenario tests

This package exercises ENSv1, ENSv2, and Basenames contract emissions against
schema-v2 through the production `phase-runner` binary. It does not start the
deleted indexer or worker; two #682 scenarios start the production API. Assertions that
previously read HTTP responses now read schema-v2
[projections](../../docs/glossary.md#projection) and phase state directly
through the test-only `ProjectionReader`.

## Prerequisites

- Foundry v1.7.1 with `anvil` on `PATH`.
- Pinned upstream checkouts from `scripts/sync-refs`.
- An isolated PostgreSQL test server supplied by `scripts/test-db`.

Run the same passed-count gate used by CI:

```sh
PATH=/home/ubuntu/.foundry/bin:$PATH scripts/test-db -- tests/e2e/run-gate
```

If Foundry is already on `PATH`, omit the explicit prefix. For a plain test
run without the count assertion:

```sh
scripts/test-db -- cargo test --manifest-path tests/e2e/Cargo.toml --locked -- --test-threads=8
```

The default gate requires the exact library-test summary `91 passed; 0 failed;
3 ignored; 0 filtered out`. CI shard 1 requires `45 passed; 0 failed; 2
ignored; 47 filtered out`, and shard 2 requires `46 passed; 0 failed; 1
ignored; 47 filtered out`. The gate checks both Cargo's exit status and every
summary count, so a prematurely successful process or an incorrectly filtered
suite cannot satisfy CI.

## Harness design

1. `HarnessDb::create` clones an isolated schema-migration template and runs
   `phase-runner init-schema`. Scenario pools select only `bigname_phase`.
2. Each scenario deploys its local ENS or Basenames topology on Anvil and
   generates a temporary [deployment profile](../../docs/glossary.md#deployment-profile)
   with scenario-local addresses. The checked-in manifest tree is never edited.
3. Most scenarios materialize immutable Anvil blocks, transactions, receipts,
   and logs as [raw facts](../../docs/glossary.md#raw-fact) up front, then
   execute `phase-runner redo` for interpret and project. This is the
   deterministic fixture pattern used by the production phase-runner tests.
   Scenarios that add facts in stages synchronously replay each selected
   snapshot; they do not claim a continuously running service or live
   checkpoint.
4. Provider-fault and intake-path parity scenarios execute the real JSON-RPC
   ingest redo path.
   Truncated JSON, transient errors, delayed responses, omitted logs, and
   partial receipts pass through the production provider implementation.
   The silent-log case explicitly pins pre-existing defect #154: a valid but
   incomplete response is accepted and published, and only the scenario's
   explicit clean redo repairs it. Its green result is evidence of that known
   limitation plus deterministic repair, not automatic containment.
5. Reorg scenarios ingest both forks through that same RPC path, invoke the
   production `phase-runner rewind` command at the stored ancestor, then
   complete the mandatory interpret/project replay stamped by the rewind.
   Completed redo removes superseded losing-fork normalized derivations and
   derives the winning fork. Losing raw facts remain anchored to retained
   orphaned lineage as the audit trail, and winning projections must match a
   clean corpus. The composed Base/ENS case also proves the untouched chain
   does not move.
   The rich-chain case also pins the #305 production history-loader fix: its
   canonical read excludes the losing event before redo and returns the winning
   event after redo through the chain-lineage join.
6. Schema-v2 projections are usually queried directly; the two #682 scenarios start the
   production API, while other helpers avoid legacy public-schema reads.
7. A scenario readiness predicate is evaluated once after its synchronous
   phase-runner commands. A false result fails the scenario instead of being
   treated as an asynchronous retry condition.
8. Deployment-profile-specific phase-runner executables are hard links, not multi-gigabyte
   copies. Direct test runs unlink each one when its last scenario lease is
   dropped. The counted gate retains links only for in-process build sharing
   and removes its run-scoped directory on exit.

## Connected ENSv1→ENSv2 migration

The connected Sepolia fixture starts with the existing local ENSv2 deployment:
`LabelStore`, `RootRegistry`, the `.eth` registry, its rent-price oracle and
registrar, mock payment tokens, and the required root-role grants. It then
deploys the ENSv1→ENSv2 migration address set/namer, `VerifiableFactory`,
`ENSV1Resolver`, `Graveyard`, `WrapperRegistryImpl`, and the unlocked and locked
ENSv1→ENSv2 migration controllers. Their constructor argument order follows
the pinned upstream contracts
(upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L28-L30 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L73-L75 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L70-L89 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L56-L64 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L42-L57 @ ens_v2@a971bd64).
The archived `WrapperRegistryImpl` bytecode has constructor revision skew from
the current pinned source: its fifth address is named
`ApprovedUpgradeGate upgradeGate`, and its metadata includes
`ApprovedUpgradeGate.sol`
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/WrapperRegistryImpl.json:L27-L29 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/WrapperRegistryImpl.json:L3154 @ ens_v2@a971bd64),
while the current source names that fifth address `IAddressSet upgradeSet`
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L70-L80 @ ens_v2@a971bd64).
The harness relies only on the common nine-address order and address ABI type;
these scenarios do not exercise `upgradeToAndCall` or `canUpgradeFrom`.
The archived `PublicResolverSet.json` artifact names its contract
`PermissionedAddressSet`. Its deployed source generation inherits
`EnhancedAccessControl`, `IAddressSet`, and `IContractNamer`, and advertises the
last two plus inherited interfaces
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PublicResolverSet.json:L531 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/utils/PermissionedAddressSet.sol:L25-L26 @ ens_v2_sepolia_20260629@ccaeb58)
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/utils/PermissionedAddressSet.sol:L53-L58 @ ens_v2_sepolia_20260629@ccaeb58).
The current source generation instead implements `IPermissionedAddressSet` plus
`IContractNamer`; `IPermissionedAddressSet` extends `IEnhancedAccessControl` and
`IAddressSet`
(upstream: .refs/ens_v2/contracts/src/utils/PermissionedAddressSet.sol:L21 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/utils/PermissionedAddressSet.sol:L34 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/utils/interfaces/IPermissionedAddressSet.sol:L21 @ ens_v2@a971bd64).
The harness uses only the address-set and contract-namer dependencies shared by
both generations.

Every ENSv2 contract above is deployed from the top-level `bytecode` read by
`load_ens_v2_artifact` from
`.refs/ens_v2/contracts/deployments/sepolia-20260629-r1/<Artifact>.json`; the
harness appends ABI-encoded constructor arguments before sending the Anvil
deployment transaction. It does not run a Solidity build or copy an artifact
into this repository. `ENSV1Resolver` receives a zero gateway-provider address,
and the locked path receives a zero replacement public-resolver address. These
scenarios therefore make no ENSv1 CCIP-read functionality claim.

The fixture generates one composite Sepolia
[deployment profile](../../docs/glossary.md#deployment-profile) containing the
four ENSv1 [source families](../../docs/glossary.md#source-family) except
reverse, the four ordinary ENSv2 source families, and `ens_v2_migration_l1`. It
mirrors every shipped `v*.toml` into a scenario
`TempDir`, then separately substitutes ENSv1, ENSv2, and ENSv1→ENSv2 migration
targets and the ENSv1→ENSv2 migration family's local `NameWrapper` and
`BaseRegistrar` correlation addresses. The ordinary Sepolia generator remains
ENSv2-only and substitutes only roots and contracts. Checked-in `manifests/` is
unchanged.
Of the ENSv1→ENSv2 migration family's eight roles, `ens_v1_renewal_bridge`,
`batch_registrar`, and `migration_helper` are deterministic, no-code
placeholders with start block zero and are inert in this fixture; the other five
roles are locally deployed ENSv1→ENSv2 migration contracts.

Both paths first register and wrap an ENSv1 `.eth` parent, read its live
registrar expiry, and reserve the same label in the ENSv2 `.eth` registry with
zero owner, zero roles, zero subregistry, and zero resolver. Transfer data is
the ordered `LibMigration.Data` tuple `label`, `owner`, `subregistry`, and
`resolver` (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L20-L31 @ ens_v2@a971bd64).
For the unlocked path, the child exists before wrapping; the parent retains an
unset `CANNOT_UNWRAP` bit, transfers to the unlocked controller, and is then
followed by an explicit `Graveyard.clear` for the child. The controller rejects
locked tokens
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L139-L140 @ ens_v2@a971bd64).
For an accepted token it clears the resolver, unwraps to Graveyard, and claims
the reservation
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L165 @ ens_v2@a971bd64).
`Graveyard.clear` processes the supplied names, and its `OWNED` descendant
branch assigns the child to Graveyard with a zero resolver
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L98-L102 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L170-L172 @ ens_v2@a971bd64).

For the locked path, the parent first burns `CANNOT_UNWRAP`; two wrapped
children remain live and ENSv1-owned, but only `bridged` burns
`PARENT_CANNOT_CONTROL`. The ENSv1 wrapper permits that parent-controlled fuse
only after the parent has burned `CANNOT_UNWRAP`
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L963-L975 @ ens_v1@91c966f).
The locked controller transfer moves the parent token to Graveyard, deploys and
initializes a `WrapperRegistry` proxy through `VerifiableFactory`, and registers
the parent with that proxy as subregistry
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L129-L188 @ ens_v2@a971bd64).
An unmigrated child is reachable through that registry only when it has no
successor registration, has `PARENT_CANNOT_CONTROL` set and `IS_DOT_ETH` clear,
and retains a nonzero ENSv1 registry owner
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L84-L89 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18-L19 @ ens_v1@91c966f).

The exact scenarios are:

- `registry_migration::connected_ens_v1_v2_migration_paths_emit_expected_facts`,
  which independently proves both activated `MigrationApplied` paths and their
  [successor bindings](../../docs/glossary.md#migration-authority-transition);
- `cross_protocol::unlocked_parent_hides_retained_ens_v1_children`, which proves
  the retained nonzero-owner ENSv1 child is unreachable after unlocked parent
  ENSv1→ENSv2 migration; and
- `cross_protocol::locked_parent_publishes_only_migratable_ens_v1_children`,
  which proves only the fuse-eligible retained ENSv1 child is reachable.

The two `cross_protocol` scenarios depend on the Project behavior merged in
#821; the `registry_migration` scenario proves the deployment and interpreted
facts independently. The fixture records the Anvil block, transaction, receipt,
and log snapshot as raw facts, then runs the real Interpret and Project phases;
it does not exercise provider selection or RPC log acquisition in Ingest.
Serving assertions call the route-shaped `ProjectionReader`: exact-name paths
read `name_current`, children paths read `children_current`, and a missing exact
projection returns `404`. This is the established projection-serving seam; it
does not exercise network transport or API-process startup.
The unlocked scenario discriminates on the child's logical name identifier in
the children response. It makes no exact-name-route assertion because this
registry-created child remains in
[non-name form](../../docs/glossary.md#non-name-form) in the fixture, so that
route is already absent before ENSv1→ENSv2 migration.

`forge` must be on `PATH` before the 64 Foundry-dependent semantic scenarios are
described as runnable; the other 3 semantic scenarios are retired and ignored.
Together with the two [pre-surface](../../docs/glossary.md#pre-surface) resolver scenarios, these three connected
scenarios produce a counted inventory of 92 tests: 89 runnable and 3 ignored,
split as 44 runnable plus 2 ignored on shard 1 and 45 runnable plus 1 ignored on
shard 2. This coverage changes no production rollout,
deployment file, Docker configuration, environment file, checked-in manifest,
or interpreter source.

The executor-only verified-resolution scenario was deleted with the legacy
execution plane. Public lookup behavior remains covered by API crate tests; no
runnable e2e scenario claims deleted checkpoint or completeness semantics.

## CI shape

The e2e builder in `.github/workflows/ci.yml` builds one shared
`phase-runner` artifact. Two explicit scenario shards independently provision
PostgreSQL and Foundry, verify that artifact, and run `tests/e2e/run-gate` with
eight test threads. A result-only `test (e2e)` job rejects any builder or shard
result other than success, and the aggregate `test` job continues to require
that result.

## Shard assignment and refresh

`tests/e2e/run-gate` checks in two duration-balanced lists of full test names.
Before executing scenarios, each shard discovers the complete library-test set
and ignored subset, then proves that the two lists have no duplicates or
intersection and that their union exactly equals discovery. Any added, removed,
renamed, or newly ignored test therefore fails closed before scenario execution.

To refresh the lists, provision PostgreSQL and Foundry as above, then discover
and time the runnable names from the repository root:

```sh
scripts/test-db -- bash -euo pipefail -c '
  cargo build --locked --package phase-runner --bin phase-runner
  cargo test --manifest-path tests/e2e/Cargo.toml --locked -- --list |
    sed -n "s/: test$//p" | LC_ALL=C sort > /tmp/e2e-all.names
  cargo test --manifest-path tests/e2e/Cargo.toml --locked -- --ignored --list |
    sed -n "s/: test$//p" | LC_ALL=C sort > /tmp/e2e-ignored.names
  LC_ALL=C comm -23 /tmp/e2e-all.names /tmp/e2e-ignored.names \
    > /tmp/e2e-runnable.names
  : > /tmp/e2e-durations.tsv
  while IFS= read -r name; do
    start=$(date +%s%N)
    cargo test --manifest-path tests/e2e/Cargo.toml --locked "$name" -- \
      --exact --test-threads=1
    end=$(date +%s%N)
    awk -v name="$name" -v start="$start" -v end="$end" \
      "BEGIN { printf \"%s\\t%.3f\\n\", name, (end-start)/1000000000 }" \
      >> /tmp/e2e-durations.tsv
  done < /tmp/e2e-runnable.names
'
```

Sort durations descending, seed shard 1 with
`scenarios::cross_protocol::composed_mainnet_profile_serves_both_protocols_without_leakage`
and shard 2 with the second-longest scenario by measured duration. Assign names
to the lower predicted load subject to the required final capacities, except
for an explicitly documented scenario-family grouping. The current inventory
uses one such grouping: the standalone connected ENSv1→ENSv2 migration facts
scenario is on shard 1 and both connected `cross_protocol` reachability
scenarios are on shard 2. The current runnable split is 45 on shard 1 and 46 on
shard 2. Break
equal-duration or equal-load ties by full test name, keep at most five of the
measured top ten on either shard, and keep two ignored tests on shard 1 and one
on shard 2.
Update the lists, expected ignored-name set, counts, and predicted totals
together in the block at the top of `run-gate`, then run its default, shard 1,
and shard 2 modes. The explicit root-workspace build above removes a one-time
canonical `phase-runner` compile from the first measured scenario while leaving
scenario-specific generated builds in the timing sample.

## Coverage ledger

The semantic inventory contains 69 scenario tests:

- 66 retargeted and runnable;
- 3 explicitly retired with one-line reasons.

The 66 runnable scenarios include the #154 known-defect reproduction described
above; it is kept runnable so the provider path and explicit repair remain
observable rather than being hidden as an ignored test.

The crate contains 94 total tests when 25 harness/support checks are included.
The pre-retarget crate contained 88; the net change is +6: obsolete
Cargo-artifact tests for the old indexer, worker, v1 API, and execution plane
were removed, while deployment-profile binary lifecycle and normalized-event
parity-completeness regression tests, the archived-artifact path check, the
three connected ENSv1→ENSv2 migration scenarios, the two pre-surface
resolver scenarios, and the two exact zero-address scenarios were added. The pure in-memory
`catchup_equivalence::primary_route_normalization_preserves_contract_instance_identity`
normalization oracle is counted as support rather than as a contract-backed
semantic scenario. The final worker-coordination stub, verified-resolution
scenario, and stale observed-code-hash admission scenario were removed
explicitly with issue #314.

### Retargeted and runnable (66)

- Basenames:
  `basenames::basenames_declared_state_matrix_end_to_end`;
  `basenames_lifecycle::basenames_subnames_list_preimages_placeholders_and_tombstones`;
  `basenames_lifecycle::l2_resolver_records_clear_and_contenthash_gap`;
  `basenames_lifecycle::l2_zero_addr60_uses_stubbed_verified_transport`;
  `basenames_lifecycle::legacy_reverse_registrar_stays_registry_and_raw_record_only`;
  `basenames_lifecycle::renew_release_and_premium_reregistration_rotate_lineage`;
  `basenames_lifecycle::third_party_controller_registration_degrades_without_label_events`;
  `basenames_lifecycle::upgradeable_controller_proxy_registers_and_renews`.
- Intake-path parity and composition:
  `catchup_equivalence::upfront_facts_match_rpc_ingest_outputs`;
  `catchup_equivalence::upfront_facts_match_rpc_ingest_wrapper_reverse_outputs`;
  `cross_protocol::base_reorg_leaves_ethereum_canonicality_untouched`;
  `cross_protocol::composed_mainnet_profile_serves_both_protocols_without_leakage`;
  `cross_protocol::locked_parent_publishes_only_migratable_ens_v1_children`;
  `cross_protocol::unlocked_parent_hides_retained_ens_v1_children`.
- ENSv2:
  `ens_v2_lifecycle::expiry_passes_then_reregistration_advances_lineage`;
  `ens_v2_lifecycle::renewal_preserves_promoted_coverage_and_registry_edges_follow`;
  `ens_v2_lifecycle::reserved_labels_foreign_registrar_and_token_sale`;
  `ens_v2_lifecycle::resolver_and_subregistry_edges_follow_set_change_zero`;
  `ens_v2_lifecycle::root_apex_attach_and_root_scope_roles`;
  `ens_v2_live_poll::ens_v2_registry_lifecycle_survives_successive_fixture_replays`.
- ENSv1 lifecycle:
  `lifecycle::expire_without_reregistration_releases_and_unlists_registration`;
  `lifecycle::expiry_grace_and_reregistration_rotate_identity`;
  `lifecycle::register_without_resolver_keeps_declared_resolver_empty`;
  `lifecycle::renew_and_transfer_keep_identity`;
  `lifecycle_divergence::transfer_without_reclaim_keeps_registry_owner_divergent`.
- Replay, reorg, and provider faults:
  `perturbations::rich_chain_live_reorg_converges_to_winning_branch`;
  `perturbations::pre_surface_records_converge_fresh_incremental_and_restored`;
  `perturbations::rich_chain_projection_and_normalized_event_replay_are_route_stable`;
  `perturbations::rich_chain_rpc_ingest_normalized_events_match_upfront_facts`;
  `perturbations::rich_chain_successive_fixture_replays_match_single_pass`;
  `pre_surface_resolver::owned_pre_surface_resolver_records_serve_after_late_renewal_without_reselection`;
  `pre_surface_resolver::ownerless_pre_surface_resolver_records_serve_after_late_renewal_without_reselection`;
  `provider_faults::silently_short_logs_are_accepted_until_explicit_refetch_matches_control`;
  `provider_faults::transient_provider_faults_and_partial_receipts_recover_to_control`.
- Registrations and record families:
  `record_families::pubkey_write_on_admitted_resolver_stays_raw_only`;
  `record_families::remaining_record_families_derive_normalized_but_stay_unenumerated`;
  `register_eth_name::register_eth_name_end_to_end`;
  `registration_burst::registration_with_records_reverse_and_referrer_derives_single_burst`.
- Registry and ENSv1→ENSv2 migration:
  `registry_driven_reads::deep_registry_hierarchy_lists_direct_children_only`;
  `registry_driven_reads::registry_driven_reads`;
  `registry_driven_reads::same_label_under_two_parents_keeps_children_distinct`;
  `registry_driven_reads::zero_owner_subname_leaves_default_children_listing`;
  `registry_migration::connected_ens_v1_v2_migration_paths_emit_expected_facts`;
  `registry_migration::registry_migration_legacy_to_current_semantics`;
  `registry_preimages::label_preimage_revealed_later_upgrades_child_listing`.
- Resolver and reverse claims:
  `resolver_records::exact_zero_addr60_uses_stubbed_verified_transport`;
  `resolver_authorization::operator_delegate_writes_match_owner_authorship`;
  `resolver_records::pre_surface_newowner_record_serves_after_late_surface`;
  `resolver_records::pre_surface_record_attribution_is_node_scoped_and_never_materializes_unknown_names`;
  `resolver_records::pre_surface_record_history_follows_current_resolver_and_version_boundary`;
  `resolver_records::records_route_values_and_version_boundaries_follow_current_resolver`;
  `resolver_records::resolver_changes_follow_registry_and_zero_releases`;
  `resolver_records::shared_resolver_keeps_per_name_records_and_projection_marks_fan_in_unsupported`;
  `reverse_primary::reverse_claim_invalid_name_surfaces_raw_claim`;
  `reverse_primary::generic_name_record_set_changed_then_cleared_stays_unadmitted`;
  `reverse_primary_claims::authorised_third_party_generic_name_record_does_not_key_claim`;
  `reverse_primary_claims::claim_without_name_record_keeps_candidate_absent`;
  `reverse_primary_claims::forward_mismatch_keeps_generic_name_record_unadmitted`;
  `reverse_primary_claims::unadmitted_reverse_resolver_keeps_candidate_absent`.
- Authority and wrapping:
  `unadmitted_controller::unadmitted_controller_registration_derives_registry_side_only`;
  `wrapper::wrapper_wrap_fuses_subnames_and_unwrap_restore_identity`;
  `wrapper_registration::born_wrapped_registration_retains_wrapper_authority`;
  `wrapper_registration::parent_burns_pcc_then_extends_existing_child_expiry`;
  `wrapper_registration::wrap_existing_registry_subname_rotates_child_only`;
  `wrapper_renewal_and_transfers::wrapped_erc1155_single_and_batch_transfers_preserve_identity`;
  `wrapper_renewal_and_transfers::wrapped_renewal_tracks_registrar_expiry_without_wrapper_event`.

### Retired with reason (3)

- `provider_faults::transient_get_code_retries_primary_without_using_configured_fallback` — runtime bytecode-hash admission and its `eth_getCode` retry path were deleted in Stage B.
- `provider_faults::pruned_get_code_fails_closed_then_uses_configured_fallback` — the code-hash archive fallback was deleted with runtime bytecode-hash admission.
- `registry_preimages::registry_only_non_eth_tree_derives_declared_state` — generic resolver `NameChanged` no longer materializes an unknown registry-only surface under the schema-v2 identity gate.

### Count deltas

| Measure | Historical baseline | Retargeted suite | Delta |
| --- | ---: | ---: | ---: |
| Total crate tests | 88 | 94 | +6 |
| Semantic scenario inventory | 62 at the retarget base, including one pure helper | 69 | -1 reclassified, -3 deleted, +11 added |
| Runnable passed-count gate | 65 in the historical Anvil gate | 91 | +26 |
| Anvil-backed semantic inventory | 65 historical gate reference | 69 | +4 |
| Runnable Anvil-backed semantic scenarios | 65 historical gate reference | 66 | +1 |

The 65 comparisons are reported because that is the historical gate reference,
but the current passed-count denominator is explicit: 66 runnable Anvil
scenarios and 25 harness/support checks produce 91 passes. Three semantic
scenarios are explicitly ignored with their retired behavior recorded above.

## Diagnostics

- `BIGNAME_E2E_KEEP_DB=1` keeps per-scenario databases and prints their URLs.
- `BIGNAME_E2E_READY_TIMEOUT_SECS` controls readiness waits (default 600s).
- `BIGNAME_E2E_COMMAND_TIMEOUT_SECS` controls bounded phase-runner commands
  (default 600s).
- `BIGNAME_E2E_TEST_THREADS` controls the gate's test thread count (default
  8).
- Command logs are written under the system temporary directory and removed
  after successful completion; failures include their paths and reversed
  tails.
