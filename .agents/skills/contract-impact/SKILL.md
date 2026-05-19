---
name: contract-impact
description: Classify a bigname request or diff before coding. Use whenever work may affect public API semantics, coverage or exhaustiveness, shared IDs or enums, manifests, migrations, work ownership, `crates/domain`, replay behavior, upstream citations, or consumer replacement/parity.
metadata:
  kind: playbook
---

# Contract Impact

Decide whether the work is implementation-only, doc-first semantic, shared-interface, manifest/admission, storage/replay, or upstream-citation work.

## Read only what is needed

- API or coverage: `docs/api-v1.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md`
- Identity or shared IDs: `docs/adrs/0002-surface-resource-identity.md`
- Storage, migrations, replay: `docs/storage.md`, `docs/projections.md`, `docs/execution.md`
- Manifests or authority: `docs/manifests.md`
- Upstream claims: `AGENTS.md` § Upstream anchors, `.refs/MANIFEST.toml`, `docs/upstream.md`
- Ownership: `docs/internal/workstreams.md`

## Output

Produce a short note:

1. `class`: one of `implementation-only`, `doc-first semantic`, `shared-interface`, `manifest/admission`, `storage/replay`, `upstream-citation`
2. `docs_or_artifacts`: exact docs, OpenAPI, fixtures, manifests, or migrations that must move with the change
3. `owner`: owning boundary or directory
4. `blockers`: unresolved semantic questions or required upstream citations

## Rules

- Public behavior, coverage meaning, shared IDs/enums, manifest schema, source authority, replay semantics, or replacement meaning require docs in the same change.
- ENSv1, ENSv2, and Basenames behavior claims require pinned `.refs/` citations; unsupported claims must not be written.
- Hard-stop if the change would make adapters write projections, API read raw facts for normal reads, execution depend on adapter internals, or unsupported behavior become implicit.
