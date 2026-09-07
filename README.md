<h1 align="center">
  <img src="docs/assets/bigname-lockup-capheight.svg" alt="bigname" width="100%">
</h1>

A replayable, auditable indexing and read API for ENS, ENSv2, and Basenames.

bigname turns onchain state from Ethereum and Base into a versioned REST API. Its `v2`
routes cover the supported portions of [exact-name profiles](docs/glossary.md), name and
address collections, resolver records and overviews, primary names, history, permissions,
and verified record reads; see the [consumer capability matrix](docs/consumer-capabilities.md)
for the exact boundaries. Partial and unsupported results are reported explicitly.
[Raw facts](docs/glossary.md) are immutable; [projections](docs/glossary.md) are
rebuildable; v2 verified reads use the schema-v2 lookup engine without writing reusable
outcomes or durable [execution traces](docs/glossary.md).

## What's here

- `apps/api` — the read API (`/v2/...`, `/graphql`, `/healthz`)
- `apps/phase-runner` — the ingest, interpret, project, verify, and live phase
  supervisor
- `crates/` — domain types, storage, manifests, schema-v2 adapters, ingest,
  interpret, lookup, and projection behavior
- `manifests/` — checked-in profile roots such as `mainnet` and `sepolia`, split by chain combo
- `migrations/` — Postgres schema
- `schema-v2/` — the fresh phase-runner schema baseline
- `docs/` — how it works

## Local development

```sh
cp .env.example .env                       # optional, for custom ports/creds
docker compose up -d                       # PostgreSQL
cargo phase -- init-schema                 # initialize bigname_phase once
./scripts/dev-up                           # boot api + configured phase runner
```

The API binds to `127.0.0.1:3000` by default. Use `/v2` routes for REST,
`POST /graphql` for the narrow compatibility surface, and `/healthz` for
readiness. The API and phase runner use `bigname_phase` in the same database.
Initialize that namespace once with `cargo phase -- init-schema`.

Useful one-shots:

- `cargo api -- serve`
- `cargo phase -- init-schema`
- `cargo phase -- redo --help`

Set `BIGNAME_API_CHAIN_RPC_URLS` for schema-v2 verified ENS resolution and
ENS/60 primary-name lookup. The phase runner owns ingest, interpret, project,
verify, and continuous live follow. See [`docs/development.md`](docs/development.md).

## Container

Published as `ghcr.io/ensdomains/bigname`. The image entrypoint takes a service
name (`api`, `phases`, or `phases-migrate`). The one-time `phases-migrate`
command installs schema-v2 into an empty `bigname_phase` namespace in that
same database.

For server deployment:

```sh
cp .env.server.example .env.server         # configure credentials, image and capacity inputs
# Set a positive disk floor and pre-create the dedicated probe directory.
# Complete docs/runbooks/production-docker.md#capacity-preflight before starting.
docker compose --env-file .env.server -f docker-compose.server.yml up -d
```

The compose file leaves `api` and `phase-runner` as long-running services.
Apply reviewed versioned migrations at the deployment boundary before starting
the new image; the deleted worker migration command is no longer available.

See [`docs/deployment.md`](docs/deployment.md) and [`docs/production.md`](docs/production.md) for the public-edge stack.

## Reading the docs

Start with [`docs/architecture.md`](docs/architecture.md) for the model — with [`docs/glossary.md`](docs/glossary.md) beside it for any project-specific term — then dive into the area you care about:

- [`docs/api-v2.md`](docs/api-v2.md) — the read contract; per-route reference in [`docs/api-v2-routes.md`](docs/api-v2-routes.md)
- [`docs/storage.md`](docs/storage.md) — schema and write ownership
- [`docs/manifests.md`](docs/manifests.md) — source manifests and discovery
- [`docs/chain-intake.md`](docs/chain-intake.md) — block intake, lineage, reorgs, backfill
- [`docs/projections.md`](docs/projections.md) — current-state read models
- [`docs/execution.md`](docs/execution.md) — verified resolution and primary names
- [`docs/consumer-capabilities.md`](docs/consumer-capabilities.md) — what each capability covers
- [`docs/development.md`](docs/development.md), [`docs/deployment.md`](docs/deployment.md), [`docs/production.md`](docs/production.md), [`docs/runbooks/`](docs/runbooks/) — running it
- [`docs/upstream.md`](docs/upstream.md) — pinned upstream refs and intentional divergences
- [`docs/adrs/`](docs/adrs/) — architecture decisions

Internal planning notes (implementation sequencing, parallel workstreams) live under [`docs/internal/`](docs/internal/) and are not required reading to use or deploy bigname.

## Guardrails

- schema-v2 `interpret` writes identity rows, discovery edges, and normalized
  events; adapters provide interpretation behavior, not projection writes
- the API reads projections and request-scoped schema-v2 lookup output, not raw facts
- raw facts are immutable, projections are rebuildable, and provider lookup responses are not retained
- update the relevant doc before changing public semantics, shared IDs, manifest schema, or coverage meaning
