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
