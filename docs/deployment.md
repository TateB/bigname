# Deployment

The production container image contains the three runnable bigname binaries:

- `bigname-api`
- `bigname-indexer`
- `bigname-worker`

The image entrypoint accepts one service selector:

```sh
docker run --rm ghcr.io/tateb/bigname:latest api
docker run --rm ghcr.io/tateb/bigname:latest indexer
docker run --rm ghcr.io/tateb/bigname:latest worker
docker run --rm ghcr.io/tateb/bigname:latest migrate
```

The default command is `api`. Raw binary invocations are also supported:

```sh
docker run --rm ghcr.io/tateb/bigname:latest bigname-api print-openapi
docker run --rm ghcr.io/tateb/bigname:latest bigname-worker inspect watch-plan --json
```

## Fresh Server Compose

1. Install Docker and Docker Compose.
2. Copy `.env.server.example` to `.env.server` and change the placeholder passwords.
3. Set `BIGNAME_IMAGE` to the image tag to run.
4. Start the stack:

```sh
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The server compose file starts PostgreSQL, a one-shot migration service, the API,
the indexer, and the worker. The API listens on the host port from
`BIGNAME_API_PORT` and answers readiness at `/healthz`. Set
`BIGNAME_API_HOST` to control the host bind address; production public-edge
deployments normally set it to `127.0.0.1` and expose traffic through the Caddy
override documented in `docs/production.md`.

The indexer and worker healthcheck commands verify that applied database
migrations exactly match the migration set compiled into the running binary.
They fail closed for missing, failed, checksum-mismatched, or newer unknown
migrations. During rolling upgrades, running `migrate` before recreating old
service containers can therefore mark those old indexer/worker containers
unhealthy until they are replaced with the matching image; treat that as version
skew, not evidence that PostgreSQL is down.

The indexer loads exactly one manifest profile root. Use `/app/manifests/mainnet`
for the mainnet profile or `/app/manifests/sepolia` for the ENSv2 Sepolia
profile. Do not point one runtime at both manifest roots.

If `BIGNAME_INDEXER_CHAIN_RPC_URLS` is unset, the indexer still syncs
manifest/watch state, but provider-backed live ingestion remains idle. Current
bootstrap RPC support accepts `http://` and `https://` endpoints.

The API service also needs its own Ethereum JSON-RPC provider for live ENS
verified resolution and the ENS/60 primary-name on-demand reverse/forward RPC
fallback, configured as
`BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`. `GET /v1/profiles/names/{name}`
in `mode=verified|both`, `GET /v1/names/{namespace}/{name}/records` when it
needs verified values, `GET /v2/names/{name}?source=verified`, and
`GET /v2/names/{name}/records` in `source=verified|auto` first use matching
persisted execution output; when supported ENS verified-resolution selectors are
missing from execution storage, the API executes them against the selected
exact-name snapshot, persists the trace/outcome, and then returns the result.
With no `at` or `chain_positions` selector, that target is `consistency=head` at
the latest stored Ethereum checkpoint, not provider latest. Missing API provider
configuration or a provider that cannot serve the selected block must fail
closed; v1 returns `409 stale`, while v2 product routes return their documented
in-band `status=stale`/`failure_reason` envelope. Neither generation may fall
back to declared record cache for verified values. The indexer RPC setting and
Reth DB source settings do not satisfy this API live-execution provider
requirement by themselves.

The primary-name fallback is deliberately softer than verified resolution: when
`GET /v1/primary-names/{address}` defaults to `namespace=ens&coin_type=60` and
the persisted tuple is missing, a configured API provider lets the route read
the current Ethereum Mainnet reverse resolver and, in verified modes, validate
the claimed name's current `addr:60` value through the ENS Universal Resolver.
A zero resolver, empty name, wrong namespace, unnormalizable reverse name, or
empty forward `addr` is a supported fallback miss. Missing provider configuration
or reverse-provider failure is logged and suppresses the fallback, leaving the
route to return the persisted/no-fallback response instead of failing the
request. Forward-verification provider failure after a reverse claim returns
`verified_primary_name.status=execution_failed`.

The worker may use the same provider shape for projection-owned ENSv1 text
hydration, configured as
`BIGNAME_WORKER_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`. During automatic
all-current projection replay, the first worker handoff to continuous apply, and
continuous `record_inventory_current` invalidation apply, the worker hydrates
legacy `text:<key>` values only after the normalized-event row has been rebuilt
and only at the stored chain checkpoint selected for the current projection.
Missing worker RPC configuration leaves those projection cache entries explicit
`unsupported`; it must not make the worker query provider `latest` or mutate
normalized events.

The same worker RPC setting enables projection-owned legacy ENSv1 reverse
resolver hydration for `primary_names_current`. The built-in configured legacy
reverse resolver set can be extended with
`BIGNAME_INDEXER_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES` on the indexer and
`BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_RESOLVER_ADDRESSES` on the worker;
deployment-specific additions must use the same comma-delimited address list in
both places so live direct-call observation and worker hydration stay aligned.
Operators may tune the Multicall3 target and batch size with
`BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_MULTICALL3_ADDRESS` and
`BIGNAME_WORKER_PRIMARY_NAME_LEGACY_REVERSE_HYDRATION_BATCH_SIZE`. The pass runs
after replay/bootstrap and during continuous apply, evaluates reverse-claim,
resolver, claim-name, and retained direct-call inputs at or behind the stored
Ethereum Mainnet checkpoint, and writes only `primary_names_current`.

Deployments with a same-host Reth database can layer
`docker-compose.reth-db.yml` on top of the server compose file. Set
`BIGNAME_INDEXER_RETH_DATADIR_HOST` to the host Reth datadir,
`BIGNAME_INDEXER_RETH_DATADIR_CONTAINER` to the in-container mount path, and
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` to comma-delimited `<chain>=<path>`
entries that use that in-container path. `BIGNAME_INDEXER_CHAIN_RPC_URLS` may
still provide RPC sources for other active watched chains, for example
Base Mainnet while Ethereum Mainnet uses same-host Reth. Do not configure the
same chain in both settings; duplicate provider sources fail at startup. Reth
DB sources remain operational intake sources; they do not replace bigname raw
facts or normalized-event `raw_fact_ref` identities.
The repository Dockerfile builds `bigname-indexer` with the
`bigname-indexer/reth-db` Cargo feature so this override keeps the Reth provider
path available. Custom images that omit that feature fail fast when
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` is set, with a rebuild instruction
instead of silently falling back to JSON-RPC or dropping the provider.
The indexer opens the Reth database through Reth's read-only provider API, but
the container mount is writable because MDBX cooperative read-only opens still
need writable lock/coordination files in the datadir.
The override defaults the indexer to `BIGNAME_INDEXER_RETH_DB_USER=0:0` because
container-managed Reth datadirs are commonly `root:root`; operators may set a
less-privileged UID/GID after granting that identity write access to
the Reth datadir's MDBX lock files. The override also raises `nofile` because
Reth's read-only RocksDB provider can keep thousands of SST files open.
It uses the host PID/IPC namespaces and bypasses the image's `tini` entrypoint
so the indexer process owns PID 1; Reth's live MDBX read-only open can fail
from the default `tini` child process.

The Reth DB reader has ignored live verification tests for code-hash decode
checks against the local Reth datadir, JSON-RPC `eth_getCode`, and stored
`raw_code_hashes`. They are not CI tests because they require the live Ethereum
Mainnet archive node and bigname storage. These tests only read provider and
storage data. The green windowed test defaults to the 688-row known-correct
island sample and is overrideable with comma-separated block numbers in
`BIGNAME_INDEXER_TEST_RETH_CODE_HASH_COMPARE_BLOCKS`. The latest-row-per-watched
address check is the post-remediation acceptance gate for the supervised padded
`raw_code_hashes` correction run; before that run, failures reflect the known
padded corpus rather than a new reader regression. The full-table audit is
outside this harness and must also be green after remediation. Local observation,
not an upstream Reth guarantee: on this host, fresh read-only verifier opens have
lagged the node persistence horizon by roughly 1-1.5k blocks, so near-head
compare blocks can fail spuriously.

The raw code-hash correction's mandatory per-run RPC oracle is `eth_getCode` by
block hash, which the tool uses to compare both bytecode hash and byte length
against the Reth DB re-derived value. Keep this host's archive Reth historical
proof window widened for the optional `eth_getProof` spot-check when the node
can serve it. The corpus reaches roughly 470,000 blocks behind head, so set
`--rpc.eth-proof-window` to at least 500,000; pinned Reth exposes that argument
and caps it at `MAX_ETH_PROOF_WINDOW` 1,209,600 blocks
`(upstream: .refs/reth/crates/node/core/src/args/rpc_server.rs:L601 @ reth@88505c7)`
`(upstream: .refs/reth/crates/node/core/src/args/rpc_server.rs:L603 @ reth@88505c7)`
`(upstream: .refs/reth/crates/rpc/rpc-server-types/src/constants.rs:L69 @ reth@88505c7)`.
Edit `/home/ubuntu/eth-archive-node/docker-compose.yml` under
`services.reth.command`:

```yaml
      - --http.api=eth,net,web3,txpool,debug,trace
      - --rpc.eth-proof-window=500000
      - --ws
```

Then restart the Reth service:

```sh
cd /home/ubuntu/eth-archive-node
docker compose up -d reth
```

Local observation, not an upstream Reth guarantee: even with
`--rpc.eth-proof-window=500000`, deep `eth_getProof` calls on this node have
taken roughly 120-240 seconds or more per call while Reth computes historical
state roots, with long-lived MDBX reads and `historical_sp` warnings in the
node logs. A 1% proof sample is therefore infeasible here. The correction tool
records whether the small proof spot-check verified, timed out, or hit a
provider-serving error; timeout or provider-serving failure is non-fatal after
the mandatory `eth_getCode` sample succeeds.

Run the ignored cargo live-verification tests from a one-off Rust container
attached to the `bigname_default` Docker network so `postgres` resolves to the
PostgreSQL service and `host.docker.internal` reaches the host-published Reth
RPC port. The live Reth datadir on this host is
`/home/ubuntu/eth-archive-node/data/reth`; it is mounted at `/reth-data` because
the Reth reader may need writable MDBX/RocksDB coordination files even for
read-only verification.

```sh
docker run --rm \
  --network bigname_default \
  --add-host host.docker.internal:host-gateway \
  -v /home/ubuntu/bigname-worktrees/ws-code-hash-fix:/workspace:ro \
  -v /home/ubuntu/eth-archive-node/data/reth:/reth-data:rw \
  -w /workspace \
  rust:1.93.1-bookworm \
  bash -lc '
    apt-get update &&
    apt-get install -y --no-install-recommends clang libclang-dev &&
    export CARGO_HOME=/tmp/cargo CARGO_TARGET_DIR=/tmp/bigname-target &&
    export BIGNAME_INDEXER_TEST_RETH_CODE_HASH_COMPARE_BLOCKS=25287255,25287268 &&
    export BIGNAME_INDEXER_TEST_RETH_DB_DATADIR=/reth-data &&
    export BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL=http://host.docker.internal:8545 &&
    export BIGNAME_INDEXER_TEST_RETH_CODE_HASH_DATABASE_URL=postgres://bigname:bigname@postgres:5432/bigname &&
    cargo test -p bigname-indexer --features reth-db \
      reth_db_provider_matches_rpc_and_stored_for_known_correct_code_hash_window \
      -- --ignored --nocapture
  '
```

The post-remediation latest-row check uses the same three live inputs and must
pass after the padded-row remediation:

```sh
docker run --rm \
  --network bigname_default \
  --add-host host.docker.internal:host-gateway \
  -v /home/ubuntu/bigname-worktrees/ws-code-hash-fix:/workspace:ro \
  -v /home/ubuntu/eth-archive-node/data/reth:/reth-data:rw \
  -w /workspace \
  rust:1.93.1-bookworm \
  bash -lc '
    apt-get update &&
    apt-get install -y --no-install-recommends clang libclang-dev &&
    export CARGO_HOME=/tmp/cargo CARGO_TARGET_DIR=/tmp/bigname-target &&
    export BIGNAME_INDEXER_TEST_RETH_DB_DATADIR=/reth-data &&
    export BIGNAME_INDEXER_TEST_ETHEREUM_RPC_URL=http://host.docker.internal:8545 &&
    export BIGNAME_INDEXER_TEST_RETH_CODE_HASH_DATABASE_URL=postgres://bigname:bigname@postgres:5432/bigname &&
    cargo test -p bigname-indexer --features reth-db \
      reth_db_provider_latest_rows_match_consensus \
      -- --ignored --nocapture
  '
```

The supervised correction run itself is a two-step operator action and must run
with the indexer stopped; otherwise live intake can race the audited window.
Expect roughly 1 GB of additional RAM and hours-scale wall time on the current
corpus. Use an `--observed-before` value at least two hours behind now because
the Reth DB reader can lag the node persistence horizon; after the main pass,
run a later tail pass from that first upper bound to a fresh two-hours-behind
upper bound.

```sh
cd /home/ubuntu/bigname
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  stop indexer

export OBSERVED_BEFORE="$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%SZ)"
export BIGNAME_IMAGE="${BIGNAME_IMAGE:-ghcr.io/tateb/bigname:latest}"

docker run --rm \
  --network bigname_default \
  --add-host host.docker.internal:host-gateway \
  --user 0:0 \
  --pid host \
  --ipc host \
  --ulimit nofile=1048576:1048576 \
  -v /home/ubuntu/eth-archive-node/data/reth:/reth-data:rw \
  -e BIGNAME_INDEXER_RAW_CODE_HASH_CORRECTION_RETH_DB_SOURCE=ethereum-mainnet=/reth-data \
  -e BIGNAME_INDEXER_RAW_CODE_HASH_CORRECTION_RPC_URL=ethereum-mainnet=http://host.docker.internal:8545 \
  "$BIGNAME_IMAGE" \
  bigname-indexer repair raw-code-hashes \
  --database-url postgres://bigname:bigname@postgres:5432/bigname \
  --chain ethereum-mainnet \
  --observed-before "$OBSERVED_BEFORE" \
  --dry-run
```

After the dry-run census matches the ratified correction scope, rerun the same
`docker run` command without `--dry-run`. The command logs the census and
per-address breakdown before the RPC-verification and out-of-family gates,
verifies its mandatory `eth_getCode` RPC sample before writing, records the
best-effort `eth_getProof` spot-check status, skips rows whose block hash is
orphaned or absent from retained lineage, and rewrites only
`raw_code_hashes.code_hash` and `raw_code_hashes.code_byte_length` in guarded
batches. The dry-run census after the write must show zero remaining
correctable non-orphan rows and report the expected `orphaned_skipped` bucket.
This repository change ships the tool and record only; it does not execute the
supervised correction.

For the tail pass, wait until the previous `OBSERVED_BEFORE` is safely behind
the Reth reader horizon, then repeat the dry-run/write sequence with:

```sh
export OBSERVED_FROM="$OBSERVED_BEFORE"
export OBSERVED_BEFORE="$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%SZ)"
# add these arguments to the same docker run command:
#   --observed-from "$OBSERVED_FROM" --observed-before "$OBSERVED_BEFORE"
```

Before the supervised Basenames Base registry-only derivation repair, apply
checked-in migrations and wait for the concurrent `resources` provenance-index
builds to finish. Do not run the registrar-family EXPLAIN gate or repair until
both `resources_basenames_registry_authority_key_idx` and
`resources_basenames_registry_logical_labelhash_idx` are present, ready, and
valid:

```sql
WITH expected(index_name) AS (
    VALUES
        ('resources_basenames_registry_authority_key_idx'),
        ('resources_basenames_registry_logical_labelhash_idx')
)
SELECT
    expected.index_name,
    pg_index.indisready,
    pg_index.indisvalid
FROM expected
LEFT JOIN pg_class
  ON pg_class.oid = to_regclass(expected.index_name)
LEFT JOIN pg_index
  ON pg_index.indexrelid = pg_class.oid;
```

The archived registrar-family `EXPLAIN (ANALYZE, BUFFERS)` must then show index
scans on those two indexes for the stale before-key resource proof and the
current registry-only counterpart lookup. A plan with repeated `resources`
sequential scans is still a hard stop. For 10k-row preflight batches, run the
EXPLAIN in a rolled-back transaction; a bounded `SET LOCAL work_mem = '256MB'`
may be used to keep the materialized proof CTEs in memory, but it is not a
substitute for the required index scans.

High-volume bootstrap defaults to
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC=auto`. In `auto` mode,
hash-pinned backfill chunks use the manifest-declared/raw catch-up scope while
the indexer is catching up, live polling keeps new block-derived events current,
and the indexer also runs automatic bounded raw-fact normalized-event replay
from its `normalized_replay_*` cursor until historical normalized events reach
the persisted raw-log head. Broad manifest-observation, discovery-refresh, and
discovery-emitter adapter sync stay outside the live tailer. Operators may set
`raw-only` to defer live normalized sync manually, or `inline` to replay each
chunk immediately for small ranges and enable broad runtime refreshes.

```sh
BIGNAME_INDEXER_RETH_DATADIR_HOST=/home/ubuntu/eth-archive-node/data/reth \
BIGNAME_INDEXER_RETH_DATADIR_CONTAINER=/reth-data \
BIGNAME_INDEXER_CHAIN_RPC_URLS=base-mainnet=http://host.docker.internal:9545 \
BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES=ethereum-mainnet=/reth-data \
BIGNAME_INDEXER_RETH_DB_USER=0:0 \
BIGNAME_INDEXER_RETH_DB_NOFILE_SOFT=1048576 \
BIGNAME_INDEXER_RETH_DB_NOFILE_HARD=1048576 \
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d indexer
```

RPC requirements are per selected profile and active watched chain. An
Ethereum-only run may omit Base entirely. If the selected profile includes Base
but no Base RPC is configured, Base provider-backed intake, automatic bootstrap,
backfill catch-up, and live head following stay idle with an explicit
`no_provider` / unavailable operational state; startup for configured chains
must not fail solely because Base is missing. A provider entry for a chain that
is not part of the selected manifest root is invalid.

Startup bootstrap creates finite backfill jobs from each eligible target's
manifest/discovery admitted start through the provider head observed at job
creation time. It does not cap work to a recent window. This is still
operational intake work: completing bootstrap alone is not consumer-replacement
or route-coverage evidence without the relevant projection, route, conformance,
and rollout gates.

Bootstrap backfill identity is tied to the selected deployment profile, chain,
finite range, and source identity, not the manifest root path used by a given
host. Moving an unchanged manifest corpus between directories must not make the
indexer reread historical ranges.

Automatic bootstrap partitions large job segments into child range leases for
internal workers. `BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_WORKERS=0` selects an
automatic worker count capped at 4; set a positive value to pin the count.
`BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_RANGE_BLOCKS` controls the child range size
and defaults to `50000` blocks. The worker pool is inside one normal
`bigname-indexer run` process; operators do not need to launch extra indexer
containers for parallel bootstrap. Parallel bootstrap applies to the effective
raw-only startup path used by `auto` / `raw-only`; explicit `inline` adapter sync
keeps startup bootstrap sequential so normalized-event writes remain ordered.

Hash-pinned backfill execution batches each reserved range into
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS`-sized chunks. The default
server profile uses `1024` blocks. Larger chunks reduce checkpoint churn and RPC
round trips during long historical bootstrap, while also increasing the amount
of range work retried after a failed chunk. Raw-only sparse backfill also caps
each materialized push with
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_PUSH` so dense log spans are
split before transaction and receipt fetch/persist work. The older
`BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_RANGE` name is still accepted
as a fallback.

Manual Base historical backfills can select Coinbase CDP SQL with
`BIGNAME_INDEXER_BACKFILL_SOURCE=coinbase-sql` or allow Base-only automatic
selection with `BIGNAME_INDEXER_BACKFILL_SOURCE=auto` plus
`BIGNAME_INDEXER_COINBASE_SQL_URLS=base-mainnet=default`. The `default` URL is
the CDP SQL `/run` endpoint; custom URLs must use `https://` because the runner
sends generated bearer JWTs. Configure `COINBASE_CDP_SQL_API_KEY_ID` and
`COINBASE_CDP_SQL_API_KEY_SECRET`, or override the env var names with
`BIGNAME_INDEXER_COINBASE_SQL_API_KEY_ID_ENV` and
`BIGNAME_INDEXER_COINBASE_SQL_API_KEY_SECRET_ENV`. The runner keeps the Secret
API Key material in env and generates a fresh CDP REST bearer JWT for each SQL
request, so operators should not paste a short-lived JWT into the server
configuration. The generated JWT is passed to the HTTPS client through a
0600-permission temporary config file and removed after the request; operators
should still treat process temp directories as sensitive because a crash can
leave a short-lived JWT behind until normal temp cleanup. This source is
backfill-only and finite-range-only: it is
unavailable to `run` live following, `ops-catchup`, repair, chain-head
promotion, and checkpoint promotion. Operators must still configure
`BIGNAME_INDEXER_CHAIN_RPC_URLS` or `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` for
the same Base chain so the validation provider owns block hashes, headers,
canonicality evidence, selected-log-emitter code observations, and
transaction/receipt fills. The Coinbase SQL runner respects
`BIGNAME_INDEXER_COINBASE_SQL_PAGE_LIMIT`,
`BIGNAME_INDEXER_COINBASE_SQL_QUERY_CHAR_LIMIT`,
`BIGNAME_INDEXER_COINBASE_SQL_QUERY_TIMEOUT_SECS`, and
`BIGNAME_INDEXER_COINBASE_SQL_RATE_LIMIT_QPS`; query length and timeout defaults
track the stricter currently published CDP SQL limits, while the row default
uses bigname's conservative 10,000-row effective cap because Coinbase's
[SQL FAQ](https://docs.cdp.coinbase.com/data/sql-api/faq) and
[REST reference](https://docs.cdp.coinbase.com/api-reference/v2/rest-api/onchain-data/run-sql-query)
currently publish different row/query-length ceilings. If the configured page
limit is above the effective result cap, pagination and window tuning use the
cap. The QPS default is a conservative per-process guardrail and remains
operator-configurable if product limits change. The default validation mode is
`full`, so the validation provider fetches the same address/topic log span and
fails the range if Coinbase SQL omitted or added a selected log identity.
Coinbase SQL reads decoded event rows and undecoded encoded-log rows; undecoded
rows require validation-provider payload fill because Coinbase SQL supplies only
the log identity and topics for them. Empty Coinbase SQL windows do not force
code observations for every selected address. `sample` uses Coinbase SQL
identities for selected logs, resolves and hash-checks only the returned log
blocks with the validation provider, fills exact logs from those block bundles,
and then uses the same validation provider for canonicality evidence, selected
transaction/receipt fills, and selected-log-emitter code observations.

Coinbase SQL backfills can split a finite range into concurrent range workers
with `BIGNAME_INDEXER_COINBASE_SQL_WORKERS`; the default `1` keeps one worker.
`BIGNAME_INDEXER_COINBASE_SQL_RANGE_BLOCKS` chooses the fixed range size used by
that concurrent mode. The default `0` keeps the normal adaptive resumable range
planner. Basenames authority Coinbase SQL backfills remain raw-only in both the
single-worker and concurrent paths so ordered normalized-event replay owns
authority closure materialization.

JSON-RPC validation providers can tune batch shape with
`BIGNAME_INDEXER_JSON_RPC_BATCH_ITEM_LIMIT` and
`BIGNAME_INDEXER_JSON_RPC_BATCH_CONCURRENCY`. The defaults favor conservative
provider compatibility; higher values reduce round trips but can trip provider
payload or rate limits. Receipt-heavy backfills can configure
`BIGNAME_INDEXER_CHAIN_RPC_RECEIPT_FALLBACK_URLS` as comma-separated
`<chain>=<url>` entries. The provider uses those URLs only for transaction
receipt fallback work; block hashes, headers, canonicality, and primary payload
validation still come from the chain's configured primary validation provider.

Automatic normalized-event replay catch-up keeps its block cursor, but also caps
each replay chunk with `BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK`
so sparse eras can move in large block jumps while dense spans are bounded by
the number of persisted canonical raw-log event candidates considered. For adapters classified
`stateless_raw_fact`, the cap bounds each cursor chunk. For implemented
`stateful_closure_required` and `contextual_dependency_required` full-closure
replay, the same cap bounds physical adapter pages while preserving whole-block
boundaries, and adapter routing may then filter those pages down to the watched
or generic source events consumed by the closure pass. If one block exceeds the
cap, that whole block is still replayed as one page. Adapter implementations may also use a larger scan guard to bound one
database range probe, but that guard is not a fixed replay window and should not
force sparse eras through 512-block pages. The automatic cursor is one all-source
chain cursor over persisted canonical raw facts, and chunk/log caps are only IO
hints; they are not semantic adapter-state or dependency-closure snapshots. The
ENSv1 unwrapped-authority closure implementation keeps its database scan guard
at the catch-up chunk-block scale so sparse eras normally page by the configured
candidate-log cap instead of a small fixed block window. Because those pages can
carry many more derived events than the old small-window pages, the implementation
flushes and checkpoints the adapter-private closure snapshot after each physical
page. The
runner fails closed for
stateful/contextual adapters that do not have a documented closure/dependency
replay session, rather than advancing the cursor over possibly divergent
transition rows. Source-scoped replay is reserved for explicit repair/backfill
runs and is not deterministic stateful/contextual regeneration unless the
selection is closure-complete. After automatic replay catch-up completes, the
post-replay live-adapter backlog cursor pages already-persisted raw-log blocks
with `BIGNAME_INDEXER_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE`. The default `1`
keeps the handoff conservative; values above the internal cap are clamped.
Use `RUST_LOG=info,sqlx::query=error` for these runs; otherwise SQLx slow-query
warnings can print huge generated INSERT statements for dense chunks and waste
time on logging instead of ingest.

The 2026-07-03 ratified Base normalized-event corpus correction is supervised
and off by default. It exists only for the comprehensive Basenames Base
drop-and-full-closure-rederive window documented in [`storage.md`](storage.md).
Do not run it against a live indexer or worker process, and do not treat it as a
projection rebuild. The execute step deletes Base current-projection rows because
those rows have foreign keys into the identity rows being dropped; the API must
not serve during the destructive and replay/rebuild window. The delete is
batched and resumable under a session advisory lock, with durable progress in
`base_normalized_rederive_runs` and `base_normalized_rederive_run_batches`.
The passed deployment profile must already own a
`base-mainnet/raw_fact_normalized_events` replay cursor because the delete scope
is global to Base while replay reset is profile-scoped. Updated indexer and
worker runtimes and write-capable one-shot commands hold a shared advisory lock
while running; the execute path takes the exclusive form of that lock and also
refuses visible `bigname-indexer`/`bigname-worker` sessions before it writes.
Those guarded writers also refuse to start while any Base rederive run is still
incomplete, even if a crashed execute container released its session advisory
lock. Apply this release's checked-in migrations before starting the correction;
the worker migration command remains a guarded writer because checked-in
migrations can include replay-adjacent data repairs. If a crash occurs before
the abort-status migration is installed, resume/complete the run or restore the
database to a consistent pre-run snapshot before running migrations or writers.
Guarded writer processes require at least two database pool connections so the
held advisory lock connection cannot starve the writer work.

1. Stop the indexer and worker services, leaving PostgreSQL and the API online
   for dry-run review if desired.
2. Run the dry-run census and capture stdout for maintainer review:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --no-deps indexer \
       bigname-indexer drop-and-rederive-base-normalized-events \
       --deployment-profile mainnet \
       --run-id base-normalized-rederive-2026-07-03 \
       --batch-size 100000
   ```

3. Review the printed derivation-kind/source-family partition, including the
   re-derivable delete count and explicitly kept nonreplay pairs such as
   `raw_log_preimage_observation` and non-closure source families;
   identity/projection/change-log delete counts; deferred raw-fact safety line,
   including that its canonical Base raw-log head is the reviewed replay target;
   the `ratified_dropped_orphan_emitters` line, which must report
   exactly 3,939,502 legacy Basenames `ReverseRegistrar`
   `0x79ea96012eea67a83431f1701b3dff7e37f9e282` rows under
   `2026-07-05 option A` for `ens_v1_reverse_claim` /
   `basenames_base_primary` with `ReverseChanged` / `BaseReverseClaimed`,
   coin type `60`, and blocks `17575714..46903158`
   (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
   (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
   (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f);
   active replay target snapshot row count and digest;
   active manifest snapshot row count and digest; both replay cursor counts;
   affected current-projection replay marker count; run id, batch size, batch
   count/order; max affected block; replay target floor; and the replay reset target
   `mainnet/base-mainnet/raw_fact_normalized_events: 17571485..=<validated replay target>`.
   The `delete_census` fields are exact and execute-gated by the matching
   `--expected-*` arguments. The derivation-kind/source-family partition,
   ratified dropped-emitter section, cursor-census breakdown, estimated batch
   counts, and deferred raw-fact safety line are informational review aids.
   `resolver_current` and `primary_names_current` are not per-row projection
   delete counts for this correction; they are covered by the exact
   `current_projection_replay_status` reset count because the later
   all-current-projections replay rebuilds those families.
   That ratified dropped-emitter class remains part of the normalized-event
   delete count and is deliberately not re-derived; any other orphaned emitter
   still indicates an unsafe dry run and must hard-stop before execute.
4. Execute only after review, passing the dry-run counts back as exact
   `--expected-*` arguments and a reviewed `--replay-target-block` so the tool
   refuses drift between review and write. Use the dry-run's reported head, or
   another reviewed value that is at least the printed replay target floor and
   not above the current canonical raw-log head. On a rerun after the drop, the
   floor includes any still-pending prior reset raw replay cursor target, so the
   target cannot be shrunk while replay is still pending. Immediately before
   this step, drain or stop the `api` service and keep it unavailable until the
   replay, projection rebuild, and verification steps complete. This is total
   API impact for the stack, including Ethereum name reads, not only Basenames
   reads:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     stop api

   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --no-deps indexer \
       bigname-indexer drop-and-rederive-base-normalized-events \
       --deployment-profile mainnet \
       --run-id base-normalized-rederive-2026-07-03 \
       --batch-size 100000 \
       --replay-target-block <dry-run-target-block> \
       --execute \
       --confirm-ratified-2026-07-03 \
       --expected-normalized-events <dry-run-value> \
       --expected-resources <dry-run-value> \
       --expected-token-lineages <dry-run-value> \
       --expected-name-surfaces <dry-run-value> \
       --expected-surface-bindings <dry-run-value> \
       --expected-name-current <dry-run-value> \
       --expected-address-names-current <dry-run-value> \
       --expected-children-current <dry-run-value> \
       --expected-permissions-current <dry-run-value> \
       --expected-record-inventory-current <dry-run-value> \
       --expected-projection-normalized-event-changes <dry-run-value> \
       --expected-current-projection-replay-status <dry-run-value> \
       --expected-replay-cursor-rows <dry-run-value> \
       --expected-adapter-checkpoint-rows <dry-run-value> \
       --expected-adapter-checkpoint-item-rows <dry-run-value> \
       --expected-active-replay-target-snapshot-digest <dry-run-value> \
       --expected-active-manifest-snapshot-digest <dry-run-value>
   ```

5. Monitor batch progress while execute is running from another SQL session:

   ```sql
   SELECT run_id, status, current_step, deleted_counts, updated_at
   FROM base_normalized_rederive_runs
   WHERE run_id = 'base-normalized-rederive-2026-07-03';

   SELECT step, count(*) AS batches, sum(row_count) AS rows
   FROM base_normalized_rederive_run_batches
   WHERE run_id = 'base-normalized-rederive-2026-07-03'
   GROUP BY step
   ORDER BY min(batch_sequence);
   ```

   The default `--batch-size 100000` bounds WAL and row locks per commit while
   leaving row-level sidecar delete triggers enabled. Lower the batch size only
   if per-commit WAL or lock duration needs more headroom.
6. If the execute container dies before completion, keep the API drained and run
   the same execute command again with the same `--run-id`, `--batch-size`,
   `--replay-target-block`, and expected counts. The command resumes only when
   recorded deleted counts plus the remaining live census still equal the
   reviewed dry-run census, the current active replay target/range snapshot
   and active manifest snapshot still hash to the reviewed digests stored in
   the run row, and retained raw facts remain complete and unchanged for the
   stored target.
   Do not run replay until the run row is `status='completed'`; before that
   final state, replay cursors and projection markers are intentionally
   untouched. Normal indexer, worker, and guarded one-shot writers also refuse
   to start while the run remains incomplete. If the operator decides not to
   resume, restore the database to a consistent pre-run snapshot first, then
   explicitly mark the run aborted so guarded writers may start:

   ```sql
   UPDATE base_normalized_rederive_runs
   SET status = 'aborted',
       current_step = 'aborted',
       updated_at = now()
   WHERE run_id = 'base-normalized-rederive-2026-07-03'
     AND status <> 'completed';
   ```

   Do not mark a half-deleted database aborted merely to unblock writers; either
   complete the rederive run or restore the database before aborting.
7. Run only the catch-up indexer with normalized replay catch-up enabled and
   `--hash-pinned-adapter-sync auto` so the reset cursor runs full-closure
   replay from block `17571485` through the reviewed target block. Keep the API
   drained. Run this from the same reviewed manifest image/root used for dry-run
   and execute. The catch-up path rebuilds the current active Base replay
   target/range snapshot and active manifest snapshot, then compares their
   digests with the reviewed digests stored in the completed run row before
   replaying. If either digest differs, catch-up bails before re-emitting rows.
   While this completed correction reset cursor is still pending replay, the
   indexer skips repository manifest sync, so normal repository sync cannot
   rotate the active manifest tables during full-closure re-derivation. The
   skipped repository refresh remains marked for retry, so the same
   long-running indexer syncs normally once the pending reset replay cursor
   completes. The final reset already
   validated that the retained canonical Base raw-log floor equals block
   `17571485`; catch-up repeats that check while the reset cursor is pending, so
   normal cursor refresh cannot widen this correction replay below the delete
   boundary on the reviewed deployment. The command also clears any stale
   `post_replay_live_adapter_backlog` cursor for the same Base deployment. The
   2026-07-05 option A dropped legacy Basenames reverse-registrar rows are not
   expected to return during this replay; after the following projection rebuild,
   `primary_names_current` should reflect only the ENS Base `L2ReverseRegistrar`
   declared primary-name authority for Base
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f).
   This mode can re-enable live
   adapter sync after replay catches up, so do not let it
   overlap the projection rebuild:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --name bigname-base-normalized-replay indexer \
       bigname-indexer run \
       --hash-pinned-adapter-sync auto \
       --normalized-replay-catchup-enabled \
       --normalized-replay-defer-projection-indexes
   ```

8. After the normalized replay cursor reaches the reviewed target, stop the
   catch-up indexer before rebuilding projections. If the command above is still
   running because live sync resumed after catch-up, stop that container first:

   ```sh
   docker stop bigname-base-normalized-replay
   ```

9. Rebuild all current projections:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     run --rm --no-deps worker \
       bigname-worker replay all-current-projections
   ```

10. Verify the conflict block, the `linkerman` and `harsh007` one-timeline checks,
   and the identity-10k sample before restoring the API, worker, and normal
   indexer service.

Operational catch-up to finalized head should be run as bounded idempotent
backfill chunks. Before every chunk starts range work, check current Postgres
size, writable free disk, and any configured object-cache budget. Capacity
shortage should pause or fail the chunk explicitly instead of silently retaining
less selected replay data or retaining full payload bundles for empty historical
blocks.

## GHCR Image

The repository publishes the image to:

```text
ghcr.io/tateb/bigname
```

The GitHub Actions workflow publishes only after the full CI workflow succeeds
for a push to `main`. Successful main pushes publish `latest` and the short
commit SHA tag. Release-tag image publishing is deferred and is not automatic.

Manual publish from an authenticated checkout:

```sh
docker buildx build --platform linux/amd64 \
  -t ghcr.io/tateb/bigname:latest \
  -t ghcr.io/tateb/bigname:$(git rev-parse --short HEAD) \
  --push .
```
