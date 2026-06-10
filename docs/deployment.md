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

The server compose file starts PostgreSQL, MinIO, a one-shot migration service,
the API, the indexer, and the worker. The API listens on the host port from
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
verified resolution and the ENS/60 primary-name on-demand reverse/forward RPC fallback, configured as
`BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`. `GET /v1/profiles/names/{name}`
in `mode=verified|both`, and `GET /v1/names/{namespace}/{name}/records` when it
needs verified values, first use matching persisted execution output; when
supported ENS verified-resolution selectors are missing from execution storage,
the API executes them against the selected exact-name snapshot, persists the
trace/outcome, and then returns the result. With no `at` or `chain_positions`
selector, that target is `consistency=head` at the latest stored Ethereum
checkpoint, not provider latest. Missing API provider
configuration or a provider that cannot serve the selected block must fail
closed with `409 stale` plus a configuration message; it must not fall back
to declared record cache. The indexer RPC setting and Reth DB source settings do
not satisfy this API live-execution provider requirement by themselves.

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
BIGNAME_INDEXER_RETH_DATADIR_HOST=/var/lib/reth \
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
