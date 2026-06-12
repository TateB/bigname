# Development

Local development uses Docker Compose for PostgreSQL plus host-side Rust binaries for the API, indexer, and worker.

## Bootstrap

1. Copy `.env.example` to `.env`.
2. Run `docker compose up -d`.
3. Apply the checked-in migration with `./scripts/migrate`.
4. Boot the API, indexer, and worker together with `./scripts/dev-up`.

The compose stack starts:

- PostgreSQL on `127.0.0.1:5432` with database `bigname` and credentials `bigname` / `bigname`

Stop the local services with `docker compose down`. Add `-v` if you also want to remove the local data volumes.

## Database-backed tests

Run DB-backed Rust tests through the local test harness:

```sh
./scripts/test-db
```

Do not run DB-backed tests directly with `cargo test` unless
`BIGNAME_DATABASE_URL` or `DATABASE_URL` already points at a reachable local
PostgreSQL server. Direct runs otherwise sit on the default development URL and
usually fail with an admin-pool timeout; rerun the same command through
`./scripts/test-db -- ...`.

The harness starts or reuses an isolated `postgres:16-alpine` container named
`bigname-test-postgres` on `127.0.0.1:55432`, exports both
`BIGNAME_DATABASE_URL` and `DATABASE_URL`, and then runs the requested command.
It intentionally does not source `.env`, so server-oriented values such as
`postgres:5432` do not leak into host-side cargo test runs.

Pass a focused command after `--`:

```sh
./scripts/test-db -- cargo test -p bigname-worker projection_apply -- --nocapture
```

Set `BIGNAME_TEST_DATABASE_URL` to point at an already-running PostgreSQL server
instead of using Docker. That server must allow the configured user to create
and drop temporary test databases.

## Bootstrap Migration Hygiene

During bootstrap, bigname has no active deployments or shared production
databases that must preserve data across every intermediate schema. Migration
findings that only affect historical data moving between pre-deployment schemas
should be tracked as bootstrap cleanup unless a shared/staging database is
explicitly declared non-rebuildable.

Before the first stateful deployment, collapse the checked-in SQL history into a
small baseline migration set. When collapsing, remove obsolete transition-only
steps or re-audit them for hard preflight checks before destructive drops, such
as the pre-deployment `raw_blocks` to `chain_header_audit` transition.

## Live Indexing Configuration

`./scripts/dev-up` sources `.env`, applies migrations, starts the API, starts
`bigname-indexer run`, and starts the worker. On startup the indexer loads the
selected manifest root, syncs manifest state into PostgreSQL, rebuilds the
stored watch plan, creates persisted chain checkpoint rows for active watched
chains, and then polls configured provider sources.

Set `BIGNAME_INDEXER_MANIFESTS_ROOT` to select one runtime profile. The default
is `manifests/mainnet` for the shipped mainnet profile. Use `manifests/sepolia`
only when running the ENSv2 Sepolia profile; do not load it beside
`manifests/mainnet` in the same local database.

Set `BIGNAME_INDEXER_CHAIN_RPC_URLS` to a comma-delimited list of
`<chain>=<url>` entries matching active watched chains in the selected profile:

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545,base-mainnet=http://127.0.0.1:9545
```

If both provider source settings are unset, `./scripts/dev-up` still boots the
processes and the indexer still syncs manifest/watch state, but provider-backed
head fetch and live ingestion stay idle. Current bootstrap RPC support accepts
`http://` and `https://` endpoints.

## Live API Execution Configuration

`GET /v1/profiles/names/{name}` in `mode=verified|both`, and
`GET /v1/names/{namespace}/{name}/records` when it needs verified values, may
execute supported ENS verified-resolution selectors on demand when matching
persisted execution output is absent. That live execution uses the selected
exact-name snapshot: no `at` and no `chain_positions` means `consistency=head`
and the latest stored Ethereum checkpoint, and the API call targets that
selected block rather than provider latest.

Configure `BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>` for the API
process before relying on live ENS verified resolution or the ENS/60
primary-name on-demand reverse/forward RPC fallback. This is separate from
`BIGNAME_INDEXER_CHAIN_RPC_URLS`, which feeds indexer intake and checkpoint
state only. If the API Ethereum provider is not configured, supported live ENS
verified selectors fail closed with `409 stale` and a configuration message
instead of falling back to declared record cache. For
`GET /v1/primary-names/{address}` defaulting to `namespace=ens&coin_type=60`,
missing provider configuration or provider failure logs a warning and suppresses
only the route-local fallback; successful fallback misses still return
`claimed_primary_name.status=not_found` with ENS reverse-RPC partial coverage.
When `mode=verified|both` and the reverse claim succeeds, the API also uses the
same provider for live `addr:60` verification through the ENS Universal Resolver.

Deployments with a local Reth database can also set
`BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` to a comma-delimited list of
`<chain>=<reth-datadir>` entries. Configure at most one source per chain. The
Reth source is optional and operational: it must feed the same raw-fact intake
contract as JSON-RPC, and Reth-local table references do not replace bigname raw
fact refs or Postgres replay facts.
Native Reth database support is compiled only when the indexer is built with
the `reth-db` feature, for example
`cargo check -p bigname-indexer --features reth-db`. That opt-in build requires
Clang/libclang development headers for Reth's RocksDB/MDBX bindings. Default
workspace checks do not build those native dependencies.

`BIGNAME_INDEXER_POLL_INTERVAL_SECS` controls the local indexer poll interval
and defaults to `5`.

## Private Readiness Endpoint

The API process exposes `GET /healthz` on the same bind address as
`cargo api -- serve` and `./scripts/dev-up`. The default local address is
`http://127.0.0.1:3000/healthz`.

`/healthz` is a private operator endpoint. It is not part of the versioned
`/v1` read API and should not be treated as a consumer compatibility surface.

The endpoint separates process readiness from database readiness:

- Healthy database: `200 OK`, top-level `status` is `ready`,
  `process.status` is `running`, `database.status` is `reachable`,
  `database.reachable` is `true`, `database.check` is `select_1`, and
  `database.error` is `null`.
- Unreachable database or pool: `503 Service Unavailable`, top-level `status`
  is `degraded`, `process.status` remains `running`, `database.status` is
  `unreachable`, `database.reachable` is `false`, `database.check` remains
  `select_1`, and `database.error` is `database readiness query failed`.

Database reachability is checked with `SELECT 1` through the configured
PostgreSQL pool. A degraded response means the API process handled the request,
but the configured database pool could not satisfy the readiness query.
