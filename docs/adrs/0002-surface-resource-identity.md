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

### ENSv1 wrap or unwrap

`ens:test.eth` keeps the same `logical_name_id`. If the authority anchor changes, a new `SurfaceBinding` may point to a different `resource_id`, but the public surface history remains continuous.

### ENSv2 linked surfaces

Two public surfaces may bind to the same `resource_id`. Permissions and role history stay attached to the resource; surface-specific reads keep their own binding provenance.

### Token regeneration

Token regeneration does not change `logical_name_id`, and it does not require a new `resource_id` when the backing authority is the same. Token attributes change within the token-lineage history rather than becoming the primary identity.

### Proxy implementation upgrade

The proxy contract keeps the same `contract_instance_id`. The old proxy / implementation edge closes and a new edge opens to the implementation contract instance for the new implementation address. If a prior implementation address returns later, its prior `contract_instance_id` is reused.

### Declared contract replacement

If a manifest changes a watched contract's own address, the prior contract instance ends and a new `contract_instance_id` begins for the successor deployment. Any continuity is represented with a `migration` edge, not by reusing the predecessor's ID. If the predecessor address returns later, its prior `contract_instance_id` is reused with a new active range.
