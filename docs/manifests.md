# Source Manifests

Status: Phase 0 baseline

This document freezes how `bigname` represents source manifests, capability flags, and discovery admission.

## 1. Purpose

Manifests make watched contracts, capability support, and rollout state explicit. They are part of the truth model, not deploy-time configuration only.

## 2. File Format And Location

Manifests live in the repository as TOML:

```text
manifests/<namespace>/<source_family>/<version>.toml
```

Reasons for TOML:

- deterministic diffs
- easy hand-editing during protocol bootstrap
- straightforward Rust parsing

## 3. Required Fields

Each manifest contains:

- `manifest_version`
- `namespace`
- `source_family`
- `chain`
- `deployment_epoch`
- `rollout_status`
- `normalizer_version`
- `capability_flags`
- `roots`
- `contracts`
- `discovery_rules`

### `rollout_status`

- `draft`
- `shadow`
- `active`
- `deprecated`

### `capability_flags`

Capabilities are named and versioned. Each flag records:

- capability name
- status: `unsupported`, `shadow`, `supported`
- optional notes

## 4. Example Shape

```toml
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"
code_hash = "sha256:..."
abi_ref = "abis/ens_v2_root_registry.json"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"
# Omit `implementation` when `proxy_kind = "none"`.

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[capability_flags]
declared_children = "supported"
```

Capability ownership is source-family specific. For ENS verified resolution on Ethereum Mainnet, the authoritative execution manifest is `ens_execution`, not `ens_v1_registry_l1`. Its canonical contract entry is the ENS Universal Resolver.

Relevant manifest fields for that execution family:

```toml
source_family = "ens_execution"
chain = "ethereum-mainnet"

[[contracts]]
role = "universal_resolver"
address = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"

[capability_flags]
verified_resolution = "shadow"
```

That freeze attaches `verified_resolution` ownership to `ens_execution`. It allows shadow execution traces and cache ownership without implying that public verified-resolution reads are already shipped.

ENS reverse-claim intake follows the same source-family discipline. For Ethereum Mainnet, later declared primary-claim intake is anchored to `ens_v1_reverse_l1`, not `ens_v1_registry_l1` or `ens_v1_resolver_l1`. Its canonical contract entry is the Ethereum `addr.reverse` Reverse Registrar.

Relevant manifest fields for that reverse family:

```toml
source_family = "ens_v1_reverse_l1"
chain = "ethereum-mainnet"

[[contracts]]
role = "reverse_registrar"
address = "0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb"
```

That freeze fixes the authoritative reverse entrypoint, source-family owner, and reverse-only intake precedence for later ENS primary-claim support. It does not define a new capability flag, does not add manifest schema, does not authorize fallback to registry-, resolver-, or other claim-setting surfaces, and does not by itself ship graduated public primary-name reads.

Within the claimed-vs-verified primary-name contract, that reverse family owns only the declared claim intake. The truth split stays explicit: `ens_v1_reverse_l1` admits the authoritative reverse claim source, while any verified primary-name result remains execution-derived through the execution owner already frozen above. The current reverse manifest may therefore be `rollout_status = "active"` with no dedicated primary-name capability flag at all. That combination means the reverse claim surface is admitted for declared intake only; it does not imply shipped public primary-name read support, verified-primary support, richer tuple-present route payloads, or graduated public coverage. In Phase 7 it also does not authorize combining the admitted reverse tuple with resolver-backed or execution-derived name identity to fill richer ENS `claimed_primary_name` payloads.

## 5. Contract Instance Admission And Continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not as raw addresses.

A manifest-declared root or contract instance keeps its ID only while the same declared contract address remains admitted on the same chain.

Rules:

- each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted `contract_instance_id`
- `[[roots]]` seed canonical graph expansion and watch-plan expansion, but otherwise follow the same identity and continuity rules as `[[contracts]]`
- reusing the same declared address on the same chain across manifest versions, including after an inactive gap, carries forward the existing `contract_instance_id` and records a new non-overlapping active range
- changing a root or contract entry's own declared address closes the prior instance active range and admits a new `contract_instance_id`; any continuity to the predecessor is expressed by a `migration` edge, not by ID reuse
- when `proxy_kind != "none"`, the proxy address and the `implementation` address refer to separate contract instances linked by a time-ranged proxy / implementation edge
- changing only `implementation` keeps the proxy's `contract_instance_id`; reuse the prior implementation instance if that implementation address reappears, otherwise mint a new implementation instance

Contract addresses remain stored as time-ranged attributes for raw-fact matching and watch-plan expansion.

## 6. Discovery Admission Rules

A discovered contract becomes authoritative only if one of the following is true:

- it is declared directly in an active manifest
- it is reachable from an active manifest root through an allowed discovery rule
- it is explicitly allow-listed by manifest version for a migration epoch

Every admitted discovery edge stores:

- `from_contract_instance_id`
- `to_contract_instance_id`
- source manifest version
- edge kind
- discovery source
- active range
- provenance

Discovery uses raw addresses only as lookup inputs. It resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the canonical edge.

If discovery re-admits an address that was admitted previously on the same chain and later became inactive, it reuses the prior `contract_instance_id` and records a new non-overlapping active range. It mints a new `contract_instance_id` only when that address has never been admitted on that chain.

Manifest-declared and discovered proxy / implementation links use the same edge and active-range rules.

## 7. Manifest Change Propagation

Manifest changes produce normalized events:

- `SourceManifestUpdated`
- `ProxyImplementationChanged`
- `CapabilityChanged`

They also:

- update discovery admission
- invalidate relevant execution cache entries
- trigger projection recomputation where capability boundaries change

## 8. Watch-Plan Expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by `contract_instance_id`.

Rules:

- the chain-intake watch target is the address range attached to each active contract instance at the requested time
- watch rows may denormalize address and code-hash state, but their durable explanation path is `manifest root -> discovery edge(s) -> contract_instance_id`
- address-only watch state is derived and may be rebuilt from manifests, contract-instance address attributes, and active discovery edges

## 9. Capability Policy

Capabilities gate behavior, not public-contract existence.

Rules:

- an unsupported capability must surface as `coverage.unsupported_reason` or a typed error
- shadow capabilities may write facts and traces without being enabled for general reads
- capability ownership attaches to the manifest-declared `source_family`; it is never implied by another family's presence
- ENS verified resolution on Ethereum Mainnet is owned by `ens_execution` through contract role `universal_resolver` at `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`, not by `ens_v1_registry_l1`
- ENS reverse-claim intake on Ethereum Mainnet is anchored to `ens_v1_reverse_l1` through contract role `reverse_registrar` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`, not by `ens_v1_registry_l1` or `ens_v1_resolver_l1`
- ENS primary-name truth on Ethereum Mainnet is intentionally split across those owners: `ens_v1_reverse_l1` owns declared reverse-claim intake, while verification stays execution-derived rather than becoming a second manifest-owned claim surface
- that reverse-family ownership freezes only the current reverse-only ENS claim surface; any later fallback claim-setting surface would need its own manifest-owned source family and a later doc-first contract update
- `rollout_status` and `capability_flags` are source-family-local readiness inputs; they do not by themselves widen ENS claim precedence, combine reverse tuple intake with resolver-backed name payloads, collapse claimed and verified primary-name truth into one manifest capability, or graduate the bootstrap public coverage contract
- adding a new capability is additive if it does not change prior semantics

## 10. Ownership And Workflow

- manifest/discovery owners maintain the TOML files
- adapter owners consume manifest versions as inputs, not hidden configuration
- execution owners depend on manifest versions for cache keys and invalidation
- any manifest schema change requires a doc-first update to this file
