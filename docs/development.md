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
