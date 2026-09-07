# Deployment

The deployable runtime consists of the API and the phase runner. The phase
runner implements `ingest`, `interpret`, `project`, read-only `verify`, and
continuous `live` follow. The deleted indexer, worker, legacy execution crate,
and their operational commands are not present in the image.

## Container contents

The image contains these runnable binaries:

- `bigname-api`
- `phase-runner`

The entrypoint selectors are:

```sh
docker run --rm ghcr.io/ensdomains/bigname:latest api
docker run --rm ghcr.io/ensdomains/bigname:latest phases-migrate
docker run --rm ghcr.io/ensdomains/bigname:latest phases
```

The one-time `phases-migrate` command invokes `phase-runner init-schema` and
requires an empty `bigname_phase` schema. It refuses every nonempty phase schema
because initialized-schema upgrades are applied separately through reviewed
versioned schema-migrations, or through the replacement procedure when an
in-place change cannot preserve durable state. `phases` then invokes
`phase-runner run` with `bigname_phase` as its search path. It can
persist ingest-through-project output and continuously follow provider heads,
including reorg-driven downstream redo and canonical-head hydration. Its
read-only verification phase can compare Base's Coinbase-loaded range with dRPC
through the `48,428,000` ingest seam and Ethereum Mainnet with local reth. Only a distinct [verification-only](glossary.md#source-role) reference earns an independent level, and the target-covering intake cursor records
`quick_synced` without one. V2, GraphQL, and operational paths consume its
phase projections and lookup output. Apply append-only SQLx schema-migrations
through deployment automation; there is no application schema-migration command
in the image. A release may also carry explicitly reviewed additive baseline
indexes whose exact `CREATE INDEX CONCURRENTLY` statements and validity checks
are listed in the release runbook. Those exceptional indexes are applied and
recorded as a separate pre-deploy step rather than entered in `_sqlx_migrations`.

## Server Compose

`docker-compose.server.yml` starts PostgreSQL, the API, and the phase runner.

```sh
cp .env.server.example .env.server
# Set credentials, the image, a positive disk floor and an absolute probe path.
# Complete the capacity preflight linked below before starting services.
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

Server Compose requires nonempty `BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES`
and `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`; missing or empty values fail rendering.
Choose a positive reserve for the actual deployment, and pre-create a dedicated
writable sibling on PostgreSQL's filesystem. The same absolute path is used on
the Docker daemon host and inside the runner. Do not expose database files or
change the existing PostgreSQL volume. Complete the [capacity preflight](runbooks/production-docker.md#capacity-preflight)
for every active overlay before recreating the runner. A successful render alone
does not prove filesystem identity, permissions or protection.

Before the full `up`, apply reviewed versioned schema-migrations with the
schema-migration runner — `sqlx migrate run --source migrations` from the
deployed commit, named
step by step in
[`runbooks/production-docker.md`](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
— then initialize `bigname_phase` only for a fresh database. Retain an existing
namespace when its in-place schema-migrations pass; replace it as described
below only when a reviewed schema-migration cannot preserve its durable state.
Then provision the non-owner `bigname_api` login. Set
`BIGNAME_API_DATABASE_URL` to that login; Compose deliberately does not fall
back to `BIGNAME_DATABASE_URL` for the API. The phase runner and
schema-migration automation use the writer URL.

Preflight every release with `sqlx migrate info --source migrations` against
the writer URL and confirm no version is pending. Also complete any explicitly
listed manual concurrent baseline-index step and verify each named index before
starting the new artifact. Neither the API nor the phase runner reports the
applied schema version. Missing lookup DDL checked by API startup produces the
diagnostic described under [Surviving services](#surviving-services); other
forgotten schema-migrations or release-specific index steps surface only as
runtime query failures or unacceptable query plans.

The API binds to the configured `BIGNAME_API_HOST` and
`BIGNAME_API_PORT`; `/healthz` remains its local readiness endpoint. Current
runtime configuration is documented in
[`production.md`](production.md) and [`development.md`](development.md).
A directly launched API can configure its metrics listener with
`BIGNAME_API_METRICS_BIND_ADDR`. The server Compose file instead fixes that
container listener at `0.0.0.0:9464`; `BIGNAME_API_METRICS_HOST` and
`BIGNAME_API_METRICS_PORT` change only its host port mapping.

## Phase-runner configuration

The implemented phases use:

- `BIGNAME_DATABASE_URL`
- `BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL`
- `BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT`
- `BIGNAME_PHASE_RUNNER_CHAINS`
- `BIGNAME_PHASE_RUNNER_SOURCES`
- `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS`
- `BIGNAME_PHASE_RUNNER_INSTANCE_ID`
- `BIGNAME_PHASE_RUNNER_INTERPRETER_STATE_CACHE_ENTRIES`
- `BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES` — required server-Compose floor
- `BIGNAME_PHASE_RUNNER_WRITABLE_PATH` — required server-Compose probe directory
- `BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES` — optional logical database ceiling
- `BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR`
- `BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR`
- `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS`

The optional ceiling is omitted when unset; an explicitly empty value is invalid.
A configured ceiling must parse as an unsigned 64-bit integer; zero is a limit,
not a way to disable it. The floor must also parse as an unsigned 64-bit integer.
Operational admission rejects floor zero: Compose only enforces presence, and
the unchanged CLI accepts zero. Direct CLI defaults remain floor zero, path `.`
and no ceiling; the existing capacity poll interval remains five seconds.
These settings are read at startup. Shell values override `--env-file`, so inspect
both interpolation inputs and the effective service/container configuration.

`BIGNAME_DATABASE_URL` is the writer credential. Supervised `run` and a
`verify` redo also require
`BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL`, pointing at the same
database with a different login. The verifier rejects that login unless it has
USAGE on `bigname_phase`, SELECT on every relation there, no write privilege on
an application relation, no database/schema creation authority, no elevated
role attributes, and no role memberships. The URL must authenticate that login
directly: startup rejects a writer session that assumes the reader role. A
reader is accepted only when its PostgreSQL system identifier, database OID,
and database name match the writer connection. A non-verification redo does
not require the reader URL.

`BIGNAME_PHASE_RUNNER_INTERPRETER_STATE_CACHE_ENTRIES` bounds the number of
persisted per-key interpreter values held by each active [interpreter
session](glossary.md#interpreter-session). It defaults to 65,536 entries. Lower
values reduce process memory and cause more indexed reads from
`normalized_events`; zero is valid and forces every required pre-batch value
through that read path. The setting does not change stored output or the
[interpreter content hash](glossary.md#interpreter-content-hash).

`BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR` configures the Prometheus listener for
a directly launched runner and defaults to `127.0.0.1:9465`. The server Compose
file fixes the container listener at `0.0.0.0:9465` and publishes it on host
loopback by default; `BIGNAME_PHASE_RUNNER_METRICS_HOST` and
`BIGNAME_PHASE_RUNNER_METRICS_PORT` change only that Compose port mapping. The
redo command deliberately ignores that runner variable: its separate
`BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR` defaults to the ephemeral
`127.0.0.1:0`. Its info-level startup event records the selected port; set
`RUST_LOG=info` to display it. Set the redo variable or pass
`--metrics-bind-addr` when a stable, unique repair target will be scraped. Each
listener serves `GET /metrics`. Every five seconds it reads phase progress,
heartbeats, verification, unfinished repair work, and the published chain head
from the runner-owned tables. It also reports an in-process heartbeat for the
runner loop of each supervised chain or the active one-shot repair chain, plus
the process-start timestamp used to detect repeated restarts.
It does not write metric state to PostgreSQL. Missing block positions and phase
heartbeats are exported as `-1`, rather than being silently omitted. See the
[pipeline monitoring runbook](runbooks/pipeline-monitoring.md) for the checked-in
Prometheus rules and Grafana dashboard.

`BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` is exported for the checked-in
phase and runner-loop heartbeat alerts. It defaults to 900 seconds. Set it
above the slowest healthy batch or inter-phase transition observed in the
deployment: heartbeats record completed work opportunities between batches,
not proof that a long batch is still executing. Rebuild batches during a
planned [re-derivation boundary](glossary.md#re-derivation-boundary) have
historically exceeded eight minutes, so calibrate this threshold before the
full source re-walk rather than after its first false page. The alerts then
require the configured age to remain exceeded for another two minutes before
paging.

Point both database URLs at the writer primary. Never point the verification
URL at a replica, standby, physical basebackup clone, or a pooler that can route
it to one. Physical copies retain the system identifier, database OID, and
database name, so a lagging copy can pass the identity check and then cause a
spurious fatal mismatch because recent stored rows are absent. A logical
restore has a new identity: repoint both URLs to its primary together so both
connections observe that new identity. A mixed old/restored pair fails the
startup check, as intended.

Provision the login after `phase-runner init-schema` (substitute the database,
role, and secret through the normal secret-management path):

```sql
CREATE ROLE bigname_verify
    LOGIN PASSWORD '<secret>'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
REVOKE CREATE ON DATABASE bigname FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE bigname TO bigname_verify;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO bigname_verify;
GRANT USAGE ON SCHEMA bigname_phase TO bigname_verify;
GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase TO bigname_verify;
```

The role provisioning is an operational database grant, not schema-v2
schema-migration authority. Reapply and revalidate the SELECT grant after every
approved phase-schema rebuild or additive schema-migration that creates a
table. In particular, after applying the attestation-audit schema-migration for
the [manifest-authority marker](glossary.md#manifest-authority-marker), run the
`GRANT SELECT ON ALL TABLES` statement again before
starting the runner. Stop every old phase-runner and one-shot redo process
before applying that schema-migration, and keep them stopped until the new
binary is ready. An old binary recognizes the marker prefix but does not bind
its boolean attestation to the new generation token or write the durable audit
row. The
[projection generation failure](glossary.md#projection-generation-failure)
audit schema-migration creates a table the same way and needs the same regrant,
without that stop-the-runner requirement. PostgreSQL does not extend an earlier
all-tables grant to tables created later. Do not reuse the writer credential in
the verification URL:
setting a writer session's default transaction to read-only does not remove
that role's write authority, and startup rejects it.

The phase runner accepts each
`BIGNAME_PHASE_RUNNER_SOURCES` entry in the form
`CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV`; omission of `ROLE` defaults to `both`. Role tokens are exact: use `verification-only`, not `verification_only`; source-kind normalization does not apply to roles. The named
environment variable contains the provider URL. Under the ratified contract,
before Ingest can make its first provider write, the runner persists each
[intake-capable source](glossary.md#source-role)'s cursor row with its kind, seed basis,
and start block and with empty progress fields. On every chain, changing a
source's normalized kind after that row exists is a data-integrity error checked
before Ingest runs. Case-only changes, surrounding whitespace, and
hyphen/underscore spelling changes are equivalent. Before a runnable Ingest
phase contacts a provider, each row's seed basis and start block must also match
the runtime source. A restart that skips an already-completed Ingest phase still
requires every configured intake-capable source's persisted key, kind, seed basis, and start
block to match, and the configured intake-capable source-key set must exactly match the persisted cursor keys. Standalone Interpret and Project redo require the complete intake-capable descriptor set and perform that exact-key check without contacting ingest providers; in an `all` redo, Ingest performs the check before Interpret replays. Any change to a persisted identity field—source key,
normalized kind, seed basis, or start block—requires an explicitly reviewed
reset that removes the cursor and every durable Ingest output that may have come
from the source, followed by a [full source
re-walk](glossary.md#re-derivation-boundary); never relabel the row in place.
Changing only the provider endpoint is allowed because endpoints are not part
of persisted source identity and will not trigger the runtime reset guard.

Each chain must have exactly one block-provider intake source that Live follows; the Coinbase SQL historical source is not a block provider.
Adding a second such source is not failover configuration: before configuring
it, define how Live selects one source, because after the Ingest handoff
Interpret fails closed rather than choosing between sources.

**Endpoint-rotation gate:** never reuse an endpoint that served intake during
the retained walk as `verification-only`, even under a different key. The
stronger level covers only facts retained since the last full source re-walk
under the current endpoint-and-role configuration. A former intake endpoint
requires the reviewed affected-chain reset and full source re-walk before it can
serve as the independent reference; current endpoint inequality is not a
substitute. Record and review endpoint-rotation history outside the database,
because phase-runner does not persist it.

Retained raw facts, chain lineage, or header-audit rows block initialization of
any missing configured source row. Lineage and header rows can remain after a
range with no watched transactions, receipts, or logs, and none of this output
identifies its provider. The runner therefore cannot distinguish a safe source
addition from replacement of the source that supplied it. The
[verification-mismatch repair](#verification-mismatch-repair) section describes
the state that a reviewed repair must cover, but it is not an executable reset
authorization. For the Issue #411 source-role transition, only the
[owner-ratified rollout gate](#owner-ratified-sepolia-source-role-rollout), its
applicable reviewed reset and preservation procedure, and the owner-approved
rollback and restoration plan authorize the reset. An ordinary redo is not that
reset.
Capacity, retry, and polling controls use the
`BIGNAME_PHASE_RUNNER_*` names exposed by `phase-runner --help`.
For that rollout, [source roles](glossary.md#source-role) are `intake`, `verification-only`, and `both`; omission defaults to `both`. Only intake-capable keys receive cursors or Ingest/Live requests, and only verification-only sources earn `cross_checked` or `node_checked`; `both` falls back to `quick_synced`. The runner rejects dRPC endpoints with the same parsed URL identity and reth paths that share the configured datadir or any provider-opened storage root (`db`, `static_files`, or `rocksdb`) by filesystem device and inode, without exposing either value. This catches symlink and bind-mount aliases; a missing or inaccessible root falls back individually to canonical or lexical spelling identity. Intake-membership changes require reset. Stronger levels are downgraded after provider-trusted revalidation, while `quick_synced` is not auto-upgraded.
Sepolia's from-zero sources for the Issue #411 rollout are `ethereum-sepolia:sepolia-intake:drpc:ethereum_head:0:intake=SEPOLIA_INTAKE_RPC_URL` and `ethereum-sepolia:sepolia-verify:drpc:ethereum_head:0:verification-only=SEPOLIA_VERIFY_RPC_URL`.
The server Compose file forwards the documented `RETH_DATA_DIR` source and the
hydration URL map. Its reth overlay bind-mounts `RETH_DATA_DIR` read-only at the
same container path. Add any differently named provider environment variable
to the phase-runner service explicitly; `docker compose --env-file` supplies
interpolation values but does not expose arbitrary variables to a container.
Base intake requires Coinbase history
plus the target-covering dRPC at block `48,428,000`. An optional distinct
verification-only dRPC records `cross_checked` through that seam; without one,
the intake dRPC records `quick_synced`. A moved verification source start or
comparison redo above the seam is rejected before redo state is created. Base
with `reth_db` is also rejected during configuration validation:
the pinned reader uses reth's Ethereum node type and Ethereum transaction and
receipt primitives (upstream: .refs/reth/crates/ethereum/node/src/node.rs:L121 @ reth@88505c7f)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L27 @ reth@88505c7f)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L51 @ reth@88505c7f). Bigname does not
implement a separate OP Stack transaction and receipt reader.
Base-aware local database verification is tracked by
[issue #433](https://github.com/ensdomains/bigname/issues/433).
An explicit verification-only Ethereum Mainnet
`reth_db` records `node_checked`; intake-capable reth alone records
`quick_synced`. `ethereum-sepolia` requires exactly one `drpc` intake source at
block zero. A distinct verification-only dRPC records `cross_checked`;
otherwise Verify records `quick_synced` when the intake cursor matches its
configuration and covers the finalized target. That binding and coverage are
checked when verification completes, and the returned final block-number/hash
marker must equal the frozen target before completion is recorded or Live can
run. A later reorg may orphan the retained cursor tip above that target, but
the stored parent chain must still reach the exact frozen target hash; a fork
at or below the target is rejected. The runner validates this exact Sepolia
source shape before Ingest creates the source cursor or contacts the provider.
The runner always completes a provider-trusted Verify plan before starting Live, including reference-less Base, Ethereum Mainnet, and Sepolia. A Compared Base plan remains paired unless Base is listed in `verify-before-live`; Ethereum-head intake keeps Mainnet and Sepolia serial for either plan shape. For a provider-trusted completed row, Verify
checks the current configuration and target-covering intake cursor against the
completion-time target without changing the recorded extent as Live finality
moves. A generic RPC
kind is not accepted as Base verification authority
because it does not identify the ratified independent provider.
Each completed dRPC comparison batch logs its actual request count, including
transport retries, range-splitting attempts, and target-marker checks. The count
is log-only: `chain_phase_state` does not persist it. At sweep time, copy every
structured `INFO` event with
`message="stored history verification batch matched its reference"` and fields
`chain_id`, `source_key`, `reference_kind`,
`reference_verification_level`, `reported_verification_level`, `from_block`,
`to_block`, and `reference_rpc_request_count` into the durable operational
record alongside the provider's billed volume. If those events are lost, phase
state cannot reconstruct the count. The measured dRPC cost remains a required
D3 cutover input; D1/D7 tooling must close this durable-accounting gap before
automating the evidence capture.
For every configured chain on which canonical-head hydration runs (currently
`ethereum-mainnet`), `BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` must contain a
`CHAIN=HTTP_URL` entry. A missing entry is a fatal project-phase configuration
error. The check runs before event-derived project publication or hydration
writes, so previously hydrated values remain intact while the chain is stopped
for configuration repair.

The retained ENS chain set is the union of chains in ENS [name
surfaces](glossary.md#surface-name-surface) and active ENS manifests. Later
`run` and `redo` synchronization allows an empty retained set only when the
incoming [deployment profile](glossary.md#deployment-profile) has zero or one
ENS chain. When retained state exists, exactly one retained ENS chain must equal
the single incoming ENS chain. Every other combination is refused before
manifest versions, contract instances, discovery rules, or normalized
`SourceManifestUpdated` events change. Multi-chain ENS deployment profiles need
an explicit contract change; this guard does not allow them.

Validation and the complete manifest-mutation transaction run on the same
PostgreSQL session that holds the startup advisory lock, so losing that session
also aborts the transaction. Concurrent runners wait and validate against the
manifests installed by the preceding synchronization. Use a separate
database/schema for the other ENS deployment.

Ordinary redo and `recompute-flags` operations do not authorize an in-place ENS
chain switch. Only the explicitly reviewed [full phase-schema replacement
procedure](#replacing-an-initialized-phase-schema) can do so when its documented
preconditions apply.

An empty retained set means that neither ENS name surfaces nor active ENS
manifests supply persisted-chain evidence to this guard. Deprecated ENS
manifests and raw facts without an active ENS manifest or ENS name surface are
outside this startup predicate.

One-shot finite phase work is available through `phase-runner redo` for
`ingest`, `interpret`, `project`, `verify`, and `recompute-flags`.
`--phase all` runs ingest through verify for each selected chain, and
`--all-chains` discovers active manifest chains before dispatching the same
per-chain path. A chain failure stops its remaining phases but does not prevent
later selected chains from running; the command still exits nonzero with the
collected failures. Interpret's effective replay range is handed to Project
through the downstream redo stamp. `--phase all` refuses a chain with any
already-pending redo rather than absorbing that work. If one of its phases
fails, the error lists every pending phase-specific recovery command in
dependency order, including a required Verify redo created by Ingest. The
operator must complete those durable markers before rerunning `--phase all`.
Verify redo checks its source
and SELECT-only database configuration before phase initialization, locking,
or redo-state publication.
It rechecks only a range inside the recorded verification extent: the range
end cannot exceed the current verify cursor. Each batch is additionally
constrained to finalized lineage. Blocks above the verify cursor are covered
by normal verification resume, never by redo.
Completion restores the pre-redo normal extent. A partial redo keeps the weaker of the retained full-extent level
and the level available from the current source roles, while a redo covering
the full retained extent can establish the current plan's level. An interrupted attempt keeps the normal resumable
redo marker and must be rerun with the same range.
Historical `live` redo is rejected because live follows only the current head.
Live does not advance the finite per-source ingest cursors. Interpret redo
checks each source only through its persisted finite target and separately
requires readable lineage at every height through the effective redo end. That
cursor coverage and lineage prove only the facts selected by the [watch
plan](glossary.md#watch-plan--watched-tuple) active when each block was loaded,
not facts required by a later watch plan.

The [manifest-authority marker](glossary.md#manifest-authority-marker) records
the active authority set's fingerprint.
The interpreter content hash and the manifest-authority fingerprint are independent deploy gates.
The interpreter hash covers inputs that can change
Interpret or Project output, including manifest `[[abi.events]]` declarations;
when it changes, complete the full-history Interpret redo and the stamped
Project redo before deploying the matching API.
`read_features` can change the manifest-authority fingerprint while the interpreter content hash remains byte-identical.
On an initialized chain, that authority change still blocks
ordinary derived work until the exact token-attested full-range Interpret redo
and downstream stamped Project redo complete; if it widened the watch plan,
complete the stamped Ingest redo first.
When the active Ethereum Mainnet `basenames_execution` authority changes,
Ethereum Mainnet follows the rule above and, in addition, the Base Project phase
is invalidated on its own ([cross-chain
exception](manifests.md#discovery-admission)): complete an explicit full-range
Project redo on `base-mainnet` (the runner prints the required range); it needs
no stamp, no attestation token, and no Base Interpret redo, and it does not
appear in the pending-redo listing.
An unchanged interpreter hash therefore does not waive authority-transition re-derivation.

Manifest synchronization records a [manifest-authority
marker](glossary.md#manifest-authority-marker) when its authority changes. Every
Interpret redo that would discharge that marker uses this operator flow:

1. If the change widened the watch plan, complete the [mandatory historical
   fetch for the affected
   range](manifests.md#mandatory-historical-fetch-after-watch-plan-widening).
   Otherwise, confirm that the change widened nothing.
2. Copy the invalidation token printed by the fence error and re-run the redo
   with `--attest-watch-set-coverage <token>`. For a multi-chain redo, repeat
   `--attest-watch-set-coverage <chain>=<token>` for each affected chain.

Without the flag, the redo fails closed. With it, the runner logs an error-level
structured event from an immutable audit row containing the chain, phase, redo
range, authority fingerprint, invalidation token, runner instance ID, and
attestation time. The audit row is committed in the same transaction that
begins the marker-discharging redo and is unique for that chain, phase, and
invalidation generation. A restart re-emits it only after the locked begin
matches and commits the same active redo; rerunning the same token-valued
command is valid only for that exact active, audited redo. If a binary upgrade
changes the [interpreter content hash](glossary.md#interpreter-content-hash)
while that redo is interrupted, use the same token and exact audited range. The
locked begin keeps the audit association but discards progress written under
the prior hash, so Interpret restarts the range from its beginning under the
new hash. Later interruptions under the new hash resume normally. The locked
begin rejects a stale token, including one from an earlier transition to the
same authority. Manifest synchronization detects manifest-authored watch-plan
widening over retained Ingest coverage and stamps the exact required Ingest
range. Normal phase execution stops and prints the `ingest` redo chain, phase,
and range command prefix plus an instruction to append configured sources; it
never performs the potentially expensive historical fetch automatically.
Complete that command before the attested Interpret redo.
Until it completes, the runner also refuses the start of any explicit
Interpret, Project, or recompute-flags redo and repeats the required Ingest
command prefix and source instruction. Supplying
`--attest-watch-set-coverage` does not override this refusal: the attestation
describes retained-fact coverage, while the durable Ingest stamp records an
uncompleted historical-fetch obligation.
Narrowing, a same-set sync, and a newly admitted chain with no Ingest coverage
do not stamp Ingest. The attestation remains the operator's responsibility for
the whole authority transition. Do not edit cursors. An interpreter content hash
rotation with neither a current manifest-authority marker nor an active audited
redo remains flagless. When a full-history Interpret redo for an interpreter
content hash rotation starts at the finite ingest bounds after Live has
advanced, the runner extends Interpret
through its recorded head and stamps the range onto Project clipped to
Project's own recorded head — the same range unless a crash between the two
phases' live-cycle advances left Project one block behind. Run or resume
the stamped Project range exactly as recorded. Project hash adoption uses its
recorded head rather than narrowing the stamp to the older ingest handoff, and
an interrupted attempt keeps the live-extended range. When that interruption
belongs to an attested Interpret redo from the prior interpreter content hash,
restart the same audited range with its token; the range restarts from its
beginning rather than resuming the cursor written under the prior interpreter
content hash.

The first manifest sync under the binary that adds `_bigname_compiled_watch`
rewrites every stored active payload. For every chain with existing derived
output, that rewrite mints a manifest-authority marker. Schedule the resulting
mandatory full attested Interpret redo across the already-derived fleet at the
planned walk-from-zero re-derivation boundary. A fresh or partially initialized
chain with no derived output receives no marker and derives normally; do not
supply an attestation for it. This rollout does not cause a spurious Ingest
refetch: when the prior payload lacks the compiled field, watch comparison
compiles that side from the same TOML under this binary. Once that snapshot
exists, a later binary-policy widening is detectable.

The binary that adds the manifest namespace to stored family-emitter entries
likewise enriches legacy `_bigname_compiled_watch` payloads from their enclosing
manifest. On chains with derived output, that payload rewrite mints a
manifest-authority marker and requires the same full attested Interpret redo
and downstream Project redo even when the TOML is unchanged. It stamps no
Ingest redo when namespace enrichment reveals no actual watch-plan widening.

`recompute-flags` recalculates label and name-surface normalization metadata
under the current normalizer and refreshes the scoped primary-name projection.
Names that remain active or remain shadow complete without replay. Names that
cross between active and shadow are reported and merged into the ordinary
Interpret and Project redo markers; only that replay path may create or retract
their bindings. After a shadow-to-active recompute commits, the surface has
active visibility while bindings and projections remain at their pre-transition
class. The API serves that conservative pre-transition projection state, and
the stamped markers block normal Interpret work. Run the stamped redo to make
transitions visible; until then, affected names serve their pre-transition
state. On completion the command writes one JSON object to standard output with
the same-class and transition counts plus every stamped phase range; this report
does not depend on `RUST_LOG`. After a normalizer-version bump (a change to the
`ENS_NORMALIZER_VERSION` constant), run `recompute-flags` per chain over the
chain's full retained range (`--from-block`/`--to-block` are required and a
bounded range skips labels whose only selection arm is range-scoped) and then a
full-range Project redo per chain over the chain's full retained range — the
same full-range redo the [rainbow-table import](storage.md#rainbow-table-preimage-import)
requires: label verdicts gate what Project composes into served names, and a
verdict flip on a label with no name surface stamps no redo of its own — only
surface visibility-class transitions do — so the full-range redo is the
required sequence to move a surface-less flip into served names. An interrupted
recompute resumes from its durable
marker; the
scoped Project refresh marker created by the command is likewise distinguishable
and resumable. A completed scoped refresh stays marked as "Interpret flags
pending" until Interpret completion clears or replaces it atomically, so a
restart in that handoff resumes the same command without repeating Project. An
unrelated ordinary Project redo that was already pending is widened or
preserved, never completed by the recompute session. This split
deliberately narrows the simplification plan's
bare statement that the mode runs without replay: shadow names suppress
bindings, so a class transition requires normal binding derivation or
retraction rather than a direct flag write. Project redo,
`recompute-flags`, `--phase all`, and an interpret-to-project cascade use
`BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS` (or
`--hydration-rpc CHAIN=HTTP_URL`) for the same current-head enrichment as the
supervised project phase. `phase-runner rewind` moves the
published latest marker to an exact stored readable ancestor and uses normal
head publication to orphan the suffix, clear affected divergence observations,
and stamp downstream redo. If the rewind makes the end of an uncompleted
required Ingest redo unreadable, the next supervised run first uses Live intake
to publish the winning suffix and then repeats the pending command prefix and
source instruction. When
finite Ingest was interrupted before it recorded a handoff, that recovery-only
Live pass anchors at the published readable ancestor.

`phase-runner inspect block-canonicality`, `stored-lineage`, and `raw-events`
provide the three read-only bounded schema-v2 operator windows. They do not
expose API routes. No drift, cache, execution-trace, or watch-plan inspection
surface is ported to the phase runner.

Before these schema-v2 operator commands are first used, run
`phase-runner init-schema` once. The phase runner owns the `bigname_phase`
namespace in that database. Head publication atomically marks phase lineage
orphaned, clears affected resolution-divergence observations, and stamps
downstream redo within that namespace.

## Verification mismatch repair

A [stored-history verification](glossary.md#stored-history-verification)
mismatch stops only the affected chain and is not retried.
`chain_phase_state.last_error` on the `verify` row records the block number,
field, stored value, and reference value. If verification was paired with live
follow, the `live` row records the same stop reason. The other configured chain
continues.

Treat the mismatch as a data-integrity incident. Preserve the recorded context
for diagnosis. Then wipe the affected chain's schema-v2 data, including its
`chain_phase_state` and `ingest_cursors` rows, ingest it again from the
configured sources, rebuild interpretation and projections, and rerun
verification from an empty verify cursor. Do not edit immutable raw rows in
place and do not mark the phase complete manually. A raw-data-only wipe is
unsafe: normal verification resumes at one block above its last successful
cursor and does not re-verify the re-ingested prefix below that cursor. If an
approved repair procedure intentionally preserves phase state, run verify redo
from the durable ingest start through the retained verified extent (the current
verify cursor). That range satisfies the full-extent condition and records the
current plan's level again. Normal verification resume then covers
the re-ingested blocks above the cursor. A mismatch in the first-ever verify
batch leaves no recorded verification extent, so no verify redo range is
expressible and a full phase-state reset is the only repair. Under the
state-preserving alternative, a failed verify redo retains its marker and is
resumed by rerunning the same redo command after repair. After a full
phase-state reset, rerun the normal pipeline instead.

## Surviving services

The API uses one `bigname_phase` request pool plus a reserved readiness
connection. GraphQL, `/v2/status`, snapshot selection,
[verified lookup](glossary.md#verified-lookup), and all projection reads use
phase relations. The `/v2/status` phase-runner heartbeat
threshold uses `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` (60 seconds by
default). V2 record lookup may perform only the guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) write;
v2 primary-name lookup writes nothing. The API database role therefore needs
`USAGE` on `bigname_phase`, `SELECT` on only the serving relations enumerated
below, and `EXECUTE` on the guarded functions below. These fixed-`search_path`,
security-definer functions are owned
by their schema owner; their installers revoke default `PUBLIC` execution.
Grant them only to the API role, and do not grant that role `CREATE` on
`bigname_phase` or `public`. In particular, the API receives no direct `INSERT`
or `UPDATE` on
`resolution_divergences` and no `UPDATE` on the guarded head, lineage, or
projection relations.

API startup tolerates a wholly absent phase schema so `/v2/status` can return
its empty, `degraded` response. Once the phase schema exists, startup checks
every phase-schema relation, function, and type its serving paths read:
relations by name, both guarded functions by exact signature, and the
`canonicality_state` type. If any are missing, the API refuses to start and its
diagnostic names every missing identity.

After the phase schema exists, the schema owner provisions the dedicated login
with these privileges (substitute
database, role, and secret through the normal secret-management path):

```sql
CREATE ROLE bigname_api
    LOGIN PASSWORD '<secret>'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE bigname TO bigname_api;
GRANT USAGE ON SCHEMA bigname_phase TO bigname_api;
GRANT SELECT ON TABLE
    bigname_phase.chain_heads,
    bigname_phase.chain_header_audit,
    bigname_phase.chain_lineage,
    bigname_phase.chain_phase_state,
    bigname_phase.service_heartbeats,
    bigname_phase.normalized_events,
    bigname_phase.migration_event_associations,
    bigname_phase.name_current,
    bigname_phase.address_names_current,
    bigname_phase.children_current,
    bigname_phase.permissions_current,
    bigname_phase.permissions_current_resource_summary,
    bigname_phase.resolver_current,
    bigname_phase.name_surfaces,
    bigname_phase.resources,
    bigname_phase.surface_bindings,
    bigname_phase.token_lineages,
    bigname_phase.record_inventory_current,
    bigname_phase.primary_names_current,
    bigname_phase.manifest_versions,
    bigname_phase.manifest_contract_instances
TO bigname_api;
GRANT EXECUTE ON FUNCTION bigname_phase.revalidate_resolution_lookup_state(
    text, bigint, text, jsonb, jsonb, uuid, text, text
) TO bigname_api;
GRANT EXECUTE ON FUNCTION bigname_phase.write_resolution_divergence(
    uuid, text, text, text, bigint, text, jsonb, text, text, text,
    text, jsonb, jsonb, boolean
) TO bigname_api;
```

This role cannot read raw facts, discovery state, the divergence table, or
unrelated operational tables directly. Reapply these explicit relation and
function grants after a reviewed phase-schema replacement; do not use ownership
or schema-wide write grants as a shortcut.

### Replacing an initialized phase schema

The current installer cannot upgrade a nonempty `bigname_phase` schema. When a
reviewed versioned schema-migration cannot preserve an existing initialized
database, the cutover requires an offline replacement and full pipeline walk:
This procedure is not used for
`20260814130000_surface_binding_authority_arm.sql`; that shared boundary must
preserve sequence-assigned manifest IDs and instead uses the targeted binding
reset in the production runbook.

1. Build `phase-runner` and `bigname-api` from the same commit. Stop the
   phase runner and every API process that can open the phase schema, and retain
   a database backup.
2. As the phase-schema owner, move the old namespace aside and create the empty
   target expected by the installer:

   ```sql
   BEGIN;
   ALTER SCHEMA bigname_phase RENAME TO bigname_phase_pre_c2;
   CREATE SCHEMA bigname_phase AUTHORIZATION <phase_owner>;
   COMMIT;
   ```

3. Run `sqlx migrate run --source migrations --database-url
   "$BIGNAME_DATABASE_URL"` from the deployed commit while the replacement
   namespace is empty, then run the new binary's `phase-runner init-schema`
   with the same database URL. This order lets an append-numbered schema-migration
   record its version when its phase table is absent before the fresh baseline
   creates the current table shape. Reapply the verification-role `USAGE`/`SELECT`
   grants and the exact API-role relation/function grant block above; schema
   rename and replacement do not carry those grants to the new namespace.
4. Run the configured `phase-runner run` from each admitted source's historical
   start through the current head. Wait for ingest, interpretation, projection,
   and stored-history verification to complete and for live follow to catch up.
   Do not copy phase tables from `bigname_phase_pre_c2` into the new schema.
5. Validate the rebuilt projections and grants, deploy the same-commit API, and
   only then retire the archived schema under the normal backup-retention
   policy.

The expected cost is one complete historical ingest-through-verification walk,
the associated provider traffic and projection work, and temporary storage for
both schemas. The v2 lookup writer is not admitted before this cutover, so the
old [resolution divergence ledger](glossary.md#resolution-divergence-ledger) is
expected to contain no rows and nothing from it is copied. After cutover,
ledger rows are not reconstructable from raw facts: once any row exists, a
future schema upgrade must use a separately reviewed schema-migration or lossless
export/import mechanism rather than this replacement procedure.
Schema-migration `20260831120000_retire_direct_divergences_for_null_resolver.sql` is
such an additive upgrade: it preserves the populated ledger, installs the
trigger that runs when Project publishes a null exact resolver, and marks
already-active observations stale where the current ENS Mainnet exact resolver
is null.

The project-at-head guard also binds the API's compiled interpreter content
hash. `bigname-api` and `phase-runner` must therefore come from the same commit.
After any interpreter content hash rotation, deploy the new phase runner and
finish its required re-walk before deploying the matching API; deploying the
API first makes all v2 snapshot-selected reads return `409 stale` until the new
project generation is published. This includes indexed reads because snapshot
selection itself requires the matching project publication before any
projection row is admitted.

The [complete-group](glossary.md#complete-group) ENSv1→ENSv2 activation is such a walk gate. Its manifest
profiles and generated watch plans do not change, so no historical fetch or
manifest-authority attestation is introduced. Deploy the new phase runner,
complete the retained-range Interpret redo under the new interpreter content
hash, run the stamped Project range, and evaluate the proof-scoped integrity
assertions for both configured ENS deployment profiles (Mainnet and Sepolia)
before `publish::swap`. Only after that Project generation publishes may the
matching API be deployed. Independent unproven Sepolia ENSv1/ENSv2 overlap
remains a per-name refusal rather than a generation failure. The connected
wrapped and locked publication prerequisite is recorded in
[PR #852](https://github.com/ensdomains/bigname/pull/852).
An interrupted walk resumes only from its existing exact phase
[redo-marker scope](glossary.md#redo-marker-scope). Interpret separately
validates the normalized arm-wide replay preimage, keeps its named replacement
binding closed, and reopens only the other matching bindings in that authority
arm. An activated boundary reopens only its recorded ENSv1 predecessor.
Activation does not create, infer, widen, or relax the phase marker or that
replay evidence.

Configure
`BIGNAME_API_CHAIN_RPC_URLS` for status and verified lookup as described in the
API docs. The request pool uses `BIGNAME_DATABASE_MAX_CONNECTIONS`; together
with the reserved readiness connection, one API process can open at most
`BIGNAME_DATABASE_MAX_CONNECTIONS + 1` PostgreSQL connections.

## Owner-ratified Sepolia source-role rollout

Do not begin this destructive rollout until the Issue #411 part-2 release
artifact, two distinct endpoint secrets, and an owner-approved rollback and
restoration procedure are available; a binary-only rollback is insufficient.
No narrower per-chain reset procedure is checked in. The only checked-in reset
broad enough to remove complete intake and source-identity state,
[Replacing an initialized phase schema](#replacing-an-initialized-phase-schema),
replaces the entire `bigname_phase` namespace and rebuilds every configured
chain, not Sepolia alone. Its authorization is limited to a reviewed
schema-migration that cannot preserve an initialized namespace; a source-role
transition does not meet that condition. It therefore does not authorize this
rollout, and using it would incorrectly give a nominally single-chain Sepolia
transition whole-schema downtime, all-chain rebuild scope, and public-identity
and audit-preservation obligations. The rollout must stop until part 3 supplies
a reviewed per-chain reset and lossless preservation procedure. Never improvise
a reset, data transfer, or rollback. Once that procedure exists, the
owner-ratified from-zero Sepolia source-role rollout is: stop old runners and
redo processes; deploy the part-2 binary and distinct secrets; configure and
validate `sepolia-intake` as intake and `sepolia-verify` as verification-only;
perform the reviewed per-chain reset; run Ingest through Verify before Live.
Confirm only the intake cursor exists, Verify reaches its frozen target with
`cross_checked`, match logs name `sepolia-verify`, and provider/operator request
accounting shows zero Ingest/Live requests for that key. Do not substitute an
ordinary redo. The required per-chain reset and preservation procedure is
part-3 work.

## Removed operational surfaces

This source tree has no command for the deleted indexer or worker planes,
including:

- old-indexer startup, live polling, or head-following
- persisted `backfill_*` job creation, leasing, advancement, or repair
- normalized-event catch-up, adapter startup synchronization, supersession, or
  coverage recovery
- the Base drop-and-rederive correction
- resolver-profile reconciliation or authority-journal draining
- old raw-code and name-normalization indexer repair commands
- legacy projection replay, hydration, migration, or inspection commands
- persisted legacy execution-cache or trace inspection

The corresponding SQL migrations remain immutable history, followed by the
append-only migration that drops their `public`-schema tables. Existing rows
are not current readiness or replay authority during the planned transition.
