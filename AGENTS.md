# bigname

bigname is a versioned indexing and read API for ENS, ENSv2, and Basenames. The checked-in docs are the source of truth for semantics; agent process stays in this file and the repo-local skills.

## Guardrails

- Public-contract docs that constrain agent work: `docs/architecture.md`, `docs/api-v1.md` (plus `docs/api-v1-routes.md`), `docs/storage.md`, `docs/manifests.md`, `docs/consumer-capabilities.md`, `docs/adrs/0001-stack.md`, `docs/adrs/0002-surface-resource-identity.md`.
- If a task changes public semantics, shared IDs or enums, coverage meaning, manifest schema, workstream ownership, or replacement meaning, update the relevant docs first or in the same change.
- Prefer cohesive end-to-end slices — a full capability with its tests and wiring, not a commit-sized edge. Do not build disguised legacy API parity or new planning docs unless semantics changed.

## Boundaries

- Adapters write identity rows and normalized events, not projection rows.
- API code reads projections and execution output only, except explicit audit endpoints and documented on-demand verified-resolution cache-miss writes through execution persistence.
- Execution code uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.
- Raw facts are immutable. Projections are rebuildable. Canonicality is explicit. Execution artifacts are durable. Unsupported behavior must be explicit.

## Upstream anchors

The canonical ENSv1, ENSv2, and Basenames codebases are pinned under `.refs/`. Agents read from the pinned checkouts; they do not guess or paraphrase upstream behavior from memory.

- `.refs/ens_v1/` — canonical ENSv1 Solidity
- `.refs/ens_v2/` — ENSv2 contracts
- `.refs/basenames/` — canonical Basenames Solidity
- `.refs/ens_subgraph/`, `.refs/ensnode/` — reference indexers for cross-check only
- `.refs/ens_app_v3/` — ENS app known-resolver metadata for first-party app admission rows only
- `.refs/ponder/`, `.refs/graph_node/` — reference indexers for chain-intake cross-check only
- `.refs/reth/` — reference Ethereum execution client for node-level chain-intake cross-check only

Pins live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`; verify with `scripts/sync-refs --check`. Rotation policy and known divergences live in `docs/upstream.md`.

Citation rules:

- Any claim about ENSv1, ENSv2, Basenames, admitted upstream app metadata, reference-indexer comparison, or reference execution-client comparison behavior — in docs, manifests, ADRs, code comments, task writeups, or agent output — must cite the upstream source as `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`.
- "Upstream says X" without a `.refs/` citation is unsupported and should be rejected in review.
- When upstream disagrees with our docs or manifests, the disagreement is a doc-first task. We may intentionally narrow, widen, or reshape upstream semantics; the divergence must be stated explicitly in the doc that carries our rule and listed in `docs/upstream.md` § Known divergences.
- Manifest address changes and new source families cite the upstream deployment metadata or Solidity file rather than relying on external URLs.

## High Conflict

- Keep `crates/domain` narrow.
- Coordinate migrations carefully.
- Treat fixture updates as cross-workstream review points.
- Inspect dirty state before staging. Stage explicit paths only, and never stage unrelated user or agent work.

## Rust File Size

- Hand-written production `.rs` files normally target <=500 LOC.
- The script emits advisory warnings for hand-written production files >500 LOC.
- Hand-written production files >600 LOC require an explicit entry in `scripts/rust-file-size-baseline.toml`; the file is now an oversized-file allowlist, not a full production-file ratchet list.
- Every allowlist entry must match the current file size and include a justification. Entries >900 LOC also require explicit review justification.
- Newly allowlisted hand-written production files may not exceed 1200 LOC. Existing allowlist allowances may not increase over the base allowance.
- Remove allowlist entries once files shrink to <=600 LOC. Omitting a base entry is OK only when the current file is no longer oversized.
- Generated code, bindings, typegen, constants, fixtures, tests, and equivalent non-production files remain excluded from the gate.
- `lib.rs` and `main.rs` are wiring files: target <=300 LOC, with hard review and an allowlist entry required above 500 LOC.
- The CI/script gate lives in `scripts/check-rust-file-size`.

## Core Skills

- `$contract-impact`: classify implementation-only vs doc-first/shared-interface work before coding.
- `$upstream-evidence`: gather pinned `.refs/` citations and divergence notes for upstream behavior claims.
- `$consumer-slice`: scope one end-to-end consumer capability with docs, behavior, tests, and explicit deferrals.
- `$manifest-authority`: plan or review manifests, discovery, admission, capability flags, and watch-plan authority.
- `$replay-safety`: review raw facts, normalized events, canonicality, projection rebuilds, invalidation, execution artifacts, and migrations.
- `$verify-loop`: user-invoked reviewer/fix loop that spawns a fresh `verification_reviewer`, confirms real findings with failing tests or checks, fixes them, and repeats until clean.

## Core Agents

- `evidence_reader`, `contract_editor`, `slice_builder`, and `verification_reviewer` are defined in `.codex/agents/`.
- Use subagents only for bounded work with a clear output contract. Do not run autonomous "keep shipping" loops without a named capability target and review gate.
