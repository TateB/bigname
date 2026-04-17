# Storage Strategy

Status: Phase 0 baseline

This document freezes the internal persistence strategy enough for storage, intake, projection, and execution work to proceed in parallel.

## 1. Invariants

- raw facts are immutable
- projections are disposable and rebuildable
- canonicality is explicit, never inferred from "latest row wins"
- verified execution artifacts are durable facts, not ephemeral cache only
- one write owner exists per storage family

## 2. Storage Layers

The system of record is split into six layers:

1. `chain_lineage`: block ancestry, fork points, hash-first reconciliation, head promotion
2. `raw_facts`: blocks, transactions, receipts, logs, code hashes, fetched call snapshots
3. `manifests_and_discovery`: source manifests, discovered edges, rollout flags
4. `identity_and_events`: `NameSurface`, `SurfaceBinding`, resources, token lineage, normalized events
5. `projections`: current-state and collection read models
6. `execution`: traces, cache entries, persisted verified outcomes

Only layers 1 through 5 are required to rebuild current declared state. Layer 6 is required to replay verified answers and explain them.

## 3. ID Strategy

### Deterministic text IDs

- `logical_name_id = "<namespace>:<normalized_name>"`

This is stable, human-auditable, and can be derived without database lookup.

### Opaque stable IDs

Use `uuid` for:

- `resource_id`
- `token_lineage_id`
- `contract_instance_id`
- `surface_binding_id`
- `execution_trace_id`

Rules:

- UUID values are internal identities, not user-generated strings
- `resource_id` and `token_lineage_id` must survive projection rebuilds
- token IDs, node hashes, and resolver addresses are attributes, not identity anchors

### Append-only event IDs

Use `bigint generated always as identity` for:

- raw fact rows
- normalized event rows
- projection job rows

## 4. Table Families And Write Ownership

| Family | Write owner | Notes |
| --- | --- | --- |
| `chain_*` | intake | lineage and canonical block graph |
| `raw_*` | intake | immutable blockchain and execution inputs |
| `manifest_*` | manifests/discovery | source manifests and capability versions |
| `discovery_*` | manifests/discovery | canonical reachable contract graph |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | stable identity anchors |
| `normalized_events` | adapters | append-only normalized protocol events |
| `projection_*` | projection workers | disposable read models |
| `execution_*` | execution workers | traces, cached answers, invalidations |

The API process is read-only against storage.

## 5. Partitioning Baseline

Start with partitioning on the highest-volume append-only tables:

- `raw_blocks`
- `raw_transactions`
- `raw_receipts`
- `raw_logs`
- `normalized_events`
- `execution_steps`

Partition keys:

- `chain_id`
- block-number range

Current-state projection tables start unpartitioned unless measurements prove otherwise.

## 6. Canonicality Model

`chain_lineage` persists the recent reconciled block window keyed by `(chain_id, block_hash)` and carries the fields needed to recover canonical ancestry:

- `parent_hash`
- `block_number`
- `timestamp`
- checkpoint-promotion state
- integrity fields needed for audits and replay

Every fact-derived row that can be invalidated by reorg carries:

- `chain_id`
- `block_number`
- `block_hash`
- `canonicality_state`
- `observed_at`

`canonicality_state` values:

- `observed`
- `canonical`
- `safe`
- `finalized`
- `orphaned`

Rules:

- block hash is the identity anchor; block number is position only
- fork detection marks affected rows `orphaned`; it does not delete them
- projection rebuilds read rows that are `canonical`, `safe`, or `finalized` by default
- history and audit tools may opt into `observed` and `orphaned` rows explicitly
- safe and finalized promotion is monotonic per chain

## 7. Projection Storage Rules

Every current-state projection row carries:

- provenance pointers
- manifest version
- relevant chain positions
- canonicality summary
- last recomputed timestamp

Projection tables may be truncated and rebuilt from canonical facts plus normalized events.

## 8. Execution Artifact Storage

Persist small execution payloads inline in Postgres:

- request metadata
- response digests
- decoded final values
- failure reasons

Persist large payloads in object storage addressed by SHA-256 digest:

- CCIP payload bodies
- large metadata responses
- trace attachments

Postgres stores the digest, size, content type, and object key.

## 9. Migration Rules

- schema changes land through checked-in migrations only
- append-only tables prefer additive changes over destructive rewrites
- projection tables may be recreated when the rebuild path already exists
- migrations that change a shared interface require the companion doc update first

## 10. Repository Ownership Implications

To keep parallel work safe:

- storage owns migrations and query primitives
- adapters own inserts into identity and normalized-event tables
- projection workers own materialized read models
- execution workers own trace and cache tables
- API code must not query raw-fact tables directly except for explicit audit endpoints
