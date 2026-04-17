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

## 5. Discovery Admission Rules

A discovered contract becomes authoritative only if one of the following is true:

- it is declared directly in an active manifest
- it is reachable from an active manifest root through an allowed discovery rule
- it is explicitly allow-listed by manifest version for a migration epoch

Every admitted discovery edge stores:

- source manifest version
- edge kind
- discovery source
- active range
- provenance

## 6. Manifest Change Propagation

Manifest changes produce normalized events:

- `SourceManifestUpdated`
- `ProxyImplementationChanged`
- `CapabilityChanged`

They also:

- update discovery admission
- invalidate relevant execution cache entries
- trigger projection recomputation where capability boundaries change

## 7. Capability Policy

Capabilities gate behavior, not public-contract existence.

Rules:

- an unsupported capability must surface as `coverage.unsupported_reason` or a typed error
- shadow capabilities may write facts and traces without being enabled for general reads
- adding a new capability is additive if it does not change prior semantics

## 8. Ownership And Workflow

- manifest/discovery owners maintain the TOML files
- adapter owners consume manifest versions as inputs, not hidden configuration
- execution owners depend on manifest versions for cache keys and invalidation
- any manifest schema change requires a doc-first update to this file
