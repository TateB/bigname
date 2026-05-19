---
name: consumer-slice
description: Scope or implement one end-to-end bigname consumer capability. Use when work maps a first-party or public capability to routes, projections, execution behavior, manifests, tests, rollout criteria, or any parity/replacement claim.
metadata:
  kind: playbook
---

# Consumer Slice

Start with `docs/consumer-capabilities.md`, then read only the API, projection, execution, manifest, and storage docs needed for the capability.

## Slice contract

For one capability, state:

1. consumer capability and explicit non-goals
2. route or routes and response mode
3. declared vs verified responsibility
4. storage/projection/execution path
5. manifest or authority assumptions
6. coverage and unsupported behavior
7. tests, fixtures, and rollout/rollback evidence

## Guardrails

- Prefer one cohesive capability over scattered parity work.
- Do not claim replacement until docs, route behavior, fixtures, and conformance evidence exist.
- Claims about ENSv1, ENSv2, or Basenames behavior require `.refs/` citations or an explicit bigname divergence in `docs/upstream.md`.
- If the slice changes public semantics, run `$contract-impact` first and keep docs in the same change.
