# ADR 0002: Surface And Resource Identity

Status: Accepted
Date: 2026-04-16

## Context

Legacy ENS indexing tends to conflate public name text, node identity, token identity, resolver instance, and control history. ENSv2 and Basenames both break that simplification:

- one public surface may rebind across time
- one resource may appear under multiple public surfaces
- token identifiers may change while backing authority does not
- resolver aliasing and wildcard behavior may create observable surfaces without direct registry entries

## Decision

Use four distinct identity anchors:

- `logical_name_id`: deterministic public-surface identity, stored as `<namespace>:<normalized_name>`
- `resource_id`: opaque stable identity for the backing authority object
- `token_lineage_id`: opaque stable identity for tokenized ownership history
- `contract_instance_id`: opaque stable identity for registry, registrar, resolver, wrapper, or transport instances

Contract-instance rules:

- mint a `contract_instance_id` when a manifest-declared contract or discovery-admitted contract first enters the canonical source graph
- one admitted contract address on one chain maps to one `contract_instance_id` across all manifest and discovery epochs
- reuse the same `contract_instance_id` while the same admitted contract address remains authoritative on the same chain
- if the same admitted contract address becomes active again after an inactive gap, reuse the prior `contract_instance_id` and record a new non-overlapping active range
- treat a change to the watched contract's own admitted address as a new contract instance; close the predecessor's active range and mint a successor ID instead of reusing the old one
- roots follow the same contract-instance rules as ordinary manifest-declared and discovery-admitted contracts
- model proxy contracts and implementation contracts as separate contract instances linked by time-ranged proxy / implementation edges
- represent continuity between distinct contract instances with `migration` edges in the manifest/discovery graph
- resolve discovery and watch-plan lookup from `(chain, address, point in time)` to `contract_instance_id`; raw addresses are attributes used for lookup, not graph identity

Public identity rules:

- exact lookup is surface-first and keyed by `logical_name_id`
- permissions and control are resource-first and keyed by `resource_id`
- token IDs are never treated as logical identity
- a time-ranged `SurfaceBinding` joins `logical_name_id` to `resource_id`

ENSv1 authority-anchor rules:

- `resource_id` is anchored to the current ENSv1 authority object, not to the surface text and not to the current holder address
- for this slice, the relevant ENSv1 authority anchors are direct registry-only control, registrar-backed registration, and wrapper-backed control
- keep the active `resource_id` while the same ENSv1 authority anchor stays authoritative across transfer, renewal, expiry, grace, fuse, resolver changes, or controller changes that do not diverge from the current tokenized holder
- rotate the active `resource_id` when authority moves to a different ENSv1 anchor; wrap, unwrap, re-registration, and live registrar registry-owner divergence are the important cases
- if the exact prior ENSv1 authority anchor becomes authoritative again, reuse its prior `resource_id`; exact means the prior anchor itself, not a different holder / controller and not a released or re-registered lease
- effective permissions and permission history are keyed to the authoritative `resource_id`, not to the surface text
- when the same ENSv1 anchor remains authoritative, resource-centric permission continuity stays on that `resource_id`; when authority moves to a different anchor, resource-centric permission reads do not merge predecessor and successor resources
- direct registry-only control has no active `token_lineage_id`
- registrar-backed and wrapper-backed ENSv1 anchors each carry their own `token_lineage_id`
- keep the active `token_lineage_id` while the same tokenized ENSv1 anchor stays authoritative; rotate it when authority moves to a different tokenized anchor
- if authority returns to the exact prior tokenized anchor, reuse its prior `token_lineage_id`; exact prior-anchor reuse does not resurrect token lineage after release or across mismatched holder / controller authority
- ordinary ENSv1 registry-only control, registrar registration, wrap, unwrap, expiry / grace, transfer, and re-registration all use `SurfaceBinding.binding_kind = declared_registry_path`; those lifecycle changes do not require `migration_rebind`

Resource-centric convenience rule:

- when a resource view needs a single display surface, rank bindings in this order:
  `declared_registry_path`
  `linked_subregistry_path`
  `migration_rebind`
  `resolver_alias_path`
  `observed_wildcard_path`
  `observed_only`
- `migration_rebind` ranks after direct declared paths and before alias- or observation-derived paths
- ties break by earliest active binding, then lexical `normalized_name`

## Consequences

- address collections return surfaces by default
- clients may opt into `dedupe_by=resource`, but that is never the default truth model
- history must support `scope=surface|resource|both`
- wrapping, migration, token regeneration, and aliasing can be represented without identity distortion

## Worked Examples

### ENSv1 authority-anchor lifecycle

| Case | Continuity result |
| --- | --- |
| Registry-only control for `ens:sub.alice.eth` | mint one registry-anchored `resource_id`; keep it across registry-owner or controller changes; no active `token_lineage_id`; `binding_kind` is `declared_registry_path` |
| Registrar registration for `ens:alice.eth` | mint one registrar-anchored `resource_id` and one registrar `token_lineage_id`; keep both while that same lease remains authoritative; `binding_kind` is `declared_registry_path` |
| Registry owner diverges from the live registrar holder for `ens:alice.eth` | close the registrar binding; mint one registry-anchored `resource_id` with no active `token_lineage_id`; the successor binding is still `declared_registry_path` |
| Diverged registry owner returns to the same live registrar holder before release via registry-side `setOwner` or registrar `reclaim` | keep `logical_name_id`; close the registry-only binding; reactivate the prior registrar `resource_id` and prior registrar `token_lineage_id`; the successor binding is still `declared_registry_path` |
| Registry owner returns after release or to a different holder / controller | keep `logical_name_id`; do not reactivate the prior registrar `resource_id` or prior registrar `token_lineage_id`; the active authority remains on a distinct registry-only or later registrar anchor; `binding_kind` is `declared_registry_path` |
| Wrap `ens:alice.eth` | keep `logical_name_id`; close the registrar binding; mint a wrapper-anchored `resource_id` and wrapper `token_lineage_id`; the successor binding is still `declared_registry_path` |
| Unwrap `ens:alice.eth` before the lease ends | keep `logical_name_id`; close the wrapper binding; reactivate the prior registrar `resource_id` and prior registrar `token_lineage_id`; the successor binding is still `declared_registry_path` |
| `ens:alice.eth` enters expiry or grace while the same authority anchor remains in force | keep the current `resource_id` and current `token_lineage_id`; only status and expiry facts change; `binding_kind` stays `declared_registry_path` |
| `ens:alice.eth` transfers while the same authority anchor remains in force | keep the current `resource_id` and current `token_lineage_id`; no new binding row is needed if the anchor did not change; `binding_kind` stays `declared_registry_path` |
| `ens:alice.eth` fully lapses and is later re-registered | keep `logical_name_id`; once the old authority ends, its binding closes; a later registration mints a new registrar `resource_id` and a new registrar `token_lineage_id`; the new binding is `declared_registry_path` |

Resource-centric permissions follow the same lifecycle: while one ENSv1 authority anchor remains authoritative, effective permission continuity stays on that anchor's `resource_id`; wrap, unwrap, or re-registration do not cause the API to stitch different `resource_id` values into one permission collection. Exact prior-anchor reuse applies to that prior anchor becoming authoritative again, such as unwrap or registry-side convergence back to the same live unreleased registrar lease, not to post-release resurrection or convergence through a different holder / controller.

### ENSv2 linked surfaces

Two public surfaces may bind to the same `resource_id`. Permissions and role history stay attached to the resource; surface-specific reads keep their own binding provenance.

### Token regeneration with stable authority

Token regeneration does not change `logical_name_id`, and it does not require a new `resource_id` when the backing authority is the same. Token attributes change within the token-lineage history rather than becoming the primary identity.

### Proxy implementation upgrade

The proxy contract keeps the same `contract_instance_id`. The old proxy / implementation edge closes and a new edge opens to the implementation contract instance for the new implementation address. If a prior implementation address returns later, its prior `contract_instance_id` is reused.

### Declared contract replacement

If a manifest changes a watched contract's own address, the prior contract instance ends and a new `contract_instance_id` begins for the successor deployment. Any continuity is represented with a `migration` edge, not by reusing the predecessor's ID. If the predecessor address returns later, its prior `contract_instance_id` is reused with a new active range.
