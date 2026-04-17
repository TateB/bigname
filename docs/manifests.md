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
verified_resolution = "shadow"
```

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
- adding a new capability is additive if it does not change prior semantics

## 10. Ownership And Workflow

- manifest/discovery owners maintain the TOML files
- adapter owners consume manifest versions as inputs, not hidden configuration
- execution owners depend on manifest versions for cache keys and invalidation
- any manifest schema change requires a doc-first update to this file
