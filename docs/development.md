# Development

Local development uses Docker Compose for PostgreSQL and S3-compatible object storage, matching the baseline in `docs/adrs/0001-stack.md`.

## Bootstrap

1. Copy `.env.example` to `.env`.
2. Run `docker compose up -d`.
3. Apply the checked-in migration with `./scripts/migrate`.
4. Boot the API, indexer, and worker together with `./scripts/dev-up`.

The compose stack starts:

- PostgreSQL on `127.0.0.1:5432` with database `bigname` and credentials `bigname` / `bigname`
- MinIO S3 API on `127.0.0.1:9000`
- MinIO console on `127.0.0.1:9001`
- a one-shot bootstrap container that creates the `bigname-dev` bucket by default

Stop the local services with `docker compose down`. Add `-v` if you also want to remove the local data volumes.

## Live Indexing Configuration

`./scripts/dev-up` sources `.env`, applies migrations, starts the API, starts
`bigname-indexer run`, and starts the worker. On startup the indexer loads the
selected manifest root, syncs manifest state into PostgreSQL, rebuilds the
stored watch plan, creates persisted chain checkpoint rows for active watched
chains, and then polls configured provider sources.

Set `BIGNAME_INDEXER_MANIFESTS_ROOT` to select one runtime profile. The default
is `manifests` for the shipped mainnet profile. Use `manifests-sepolia-dev` only
when running the ENSv2 Sepolia dev profile; do not load it beside `manifests` in
the same local database.

Set `BIGNAME_INDEXER_CHAIN_RPC_URLS` to a comma-delimited list of
`<chain>=<url>` entries matching active watched chains in the selected profile:

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545,base-mainnet=http://127.0.0.1:9545
```

If both provider source settings are unset, `./scripts/dev-up` still boots the
processes and the indexer still syncs manifest/watch state, but provider-backed
head fetch and live ingestion stay idle. Current bootstrap RPC support accepts
`http://` endpoints only; use a local node or local HTTP proxy for hosted RPC
providers that expose only HTTPS.

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
