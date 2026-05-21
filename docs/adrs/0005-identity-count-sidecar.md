# ADR 0005: Identity Reverse Count Sidecar

Status: Accepted
Date: 2026-05-20

## Context

The partner-facing identity reverse routes must return `pagination.total_count` on the hot feed path. Counting the full reverse relation universe at request time was too expensive for 100 to 1000 input batches, especially when the page itself only needs one or a few records per address.

## Decision

Maintain `address_names_current_identity_counts` as a storage-owned sidecar updated by triggers on `address_names_current` plus the supporting identity-anchor and `name_current` readability rows. This is a deliberate exception to the usual projection-worker-only write rule.

The sidecar is not a source of protocol truth and does not publish new semantics. It is an operational summary of the reverse identity page universe for `(address, roles)`, using the same canonical/read-safe filters as the reverse page query and excluding relation rows whose `name_current` record is absent or not readable.

## Replay And Rebuild Semantics

Projection workers remain the only writers of current projection families. The sidecar follows those projection rows through database triggers and can be rebuilt deterministically by truncating it and replaying the migration's grouped count query over the current projection tables.

If a supporting row is invalidated, orphaned, repaired, or deleted, the trigger recomputes affected addresses. If trigger state is suspected stale after manual repair, operators may rebuild only the sidecar without replaying protocol facts because it is disposable derived state.

`/v1/status/indexing` continues to report readiness from chain checkpoints, active/shadow manifest chains, projection apply cursors, and pending invalidations. The count sidecar itself is not a readiness source; stale projections or pending invalidations keep the route degraded or stale before the sidecar result is trusted.

## Consequences

This keeps partner-required count reads off the request hot path while making the ownership exception explicit and bounded. The cost is extra trigger work on current projection writes and a small amount of additional rebuild/runbook surface.
