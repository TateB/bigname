# ADR 0005: Identity Reverse Feed Sidecars

Status: Accepted
Date: 2026-05-20

## Context

The partner-facing identity reverse routes must return `pagination.total_count` on the hot feed path. The compact feed route must also return one representative display row per address under partner-1 latency targets. Counting the full reverse relation universe and selecting the first readable row at request time was too expensive for 100 to 1000 input batches, especially when the page itself only needs one compact record per address.

## Decision

Maintain `address_names_current_identity_counts` and `address_names_current_identity_feed` as storage-owned sidecars updated by triggers on `address_names_current`, `primary_names_current`, plus the supporting identity-anchor and `name_current` readability rows. This is a deliberate exception to the usual projection-worker-only write rule.

The sidecars are not a source of protocol truth and do not publish new semantics. They are operational summaries of the reverse identity page universe: counts for `(address, roles)` and compact feed rows for `(address, roles, coin_type)`, using the same canonical/read-safe filters as the reverse page query and excluding relation rows whose `name_current` record is absent or not readable.

## Replay And Rebuild Semantics

Projection workers remain the only writers of current projection families. The sidecars follow those projection rows through database triggers and can be rebuilt deterministically by truncating them and replaying the migration's grouped count/feed queries over the current projection tables.

If a supporting row is invalidated, orphaned, repaired, or deleted, triggers recompute affected addresses. Recomputes are serialized with the same address-level advisory lock so counts and compact display rows cannot interleave stale delete/insert windows for one address. If trigger state is suspected stale after manual repair, operators may rebuild only the sidecars without replaying protocol facts because they are disposable derived state.

`/v1/status` continues to report readiness from chain checkpoints, active/shadow manifest chains, projection apply cursors, and pending invalidations. The sidecars themselves are not readiness sources; stale projections or pending invalidations keep the route degraded or stale before sidecar results are trusted.

## Consequences

This keeps partner-required count and representative-row reads off the request hot path while making the ownership exception explicit and bounded. The cost is extra trigger work on current projection writes and a small amount of additional rebuild/runbook surface.
