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

Public identity rules:

- exact lookup is surface-first and keyed by `logical_name_id`
- permissions and control are resource-first and keyed by `resource_id`
- token IDs are never treated as logical identity
- a time-ranged `SurfaceBinding` joins `logical_name_id` to `resource_id`

Resource-centric convenience rule:

- when a resource view needs a single display surface, rank bindings in this order:
  `declared_registry_path`
  `linked_subregistry_path`
  `resolver_alias_path`
  `observed_wildcard_path`
  `observed_only`
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

