---
name: upstream-evidence
description: Gather pinned `.refs/` evidence for ENSv1, ENSv2, Basenames, admitted app metadata, reference-indexer comparisons, or reference execution-client comparisons. Use when docs, manifests, tests, comments, or task output make upstream behavior claims or when a change may introduce a divergence.
metadata:
  kind: playbook
---

# Upstream Evidence

Read `AGENTS.md` § Upstream anchors, `.refs/MANIFEST.toml`, and `docs/upstream.md` first. Then read only the pinned `.refs/<key>/` files needed for the claim.

## Output

Produce a claim ledger:

1. claim
2. upstream citation in `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)` format
3. whether the citation supports the claim directly or only constrains it
4. any bigname narrowing, widening, or divergence that must be documented in `docs/upstream.md`

## Rules

- Do not cite memory, external URLs, or unpinned checkouts for governed upstream behavior.
- If the relevant ref is missing, run `scripts/sync-refs` only when the user/task permits write-side setup; otherwise report the blocker.
- Manifest address changes and new source families cite upstream deployment metadata or Solidity.
- Unsupported or uncited upstream claims should be removed, not softened.
