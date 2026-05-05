# bigname

A replayable, auditable indexing and read API for ENS, ENSv2, and Basenames.

bigname turns onchain state from Ethereum and Base into a versioned `v1` REST contract that answers point-in-time, provenance-tagged questions about names, addresses, resolvers, primary names, and verified resolution. Onchain events become immutable raw facts; read models rebuild from those facts; verified onchain calls are recorded with their inputs so any answer can be re-checked.

## Layout

| Path | What it is |
|---|---|
| `apps/api` | The HTTP API (`/v1/...`, `/healthz`, `/docs`) |
| `apps/indexer` | Chain intake, manifest sync, backfill, head-following |
| `apps/worker` | Projections, replay, verified execution, inspection commands |
| `crates/` | Domain types, storage, manifests, adapters (ENSv1, ENSv2, Basenames), execution |
| `manifests/` | Mainnet source manifests for ENS and Basenames |
| `manifests-sepolia-dev/` | ENSv2 dev profile (selected at runtime, not loaded together) |
| `migrations/` | Postgres schema |
| `tests/conformance/` | Rust conformance harness |
| `docs/` | How it works |

## Local development

```sh
cp .env.example .env                       # optional, for custom ports/creds
docker compose up -d                       # Postgres + MinIO
./scripts/migrate                          # apply migrations
./scripts/dev-up                           # boot api + indexer + worker
```

The API binds to `127.0.0.1:3000`. Hit `http://127.0.0.1:3000/docs` for OpenAPI, `/healthz` for readiness.

For live ingest and live ENS verified resolution, set `BIGNAME_INDEXER_CHAIN_RPC_URLS` and `BIGNAME_API_CHAIN_RPC_URLS`. See [`docs/operations.md`](docs/operations.md).

## Container

```sh
docker run --rm ghcr.io/tateb/bigname:latest api       # default
docker run --rm ghcr.io/tateb/bigname:latest indexer
docker run --rm ghcr.io/tateb/bigname:latest worker
docker run --rm ghcr.io/tateb/bigname:latest migrate
```

For server deployment with Postgres, MinIO, and the public Caddy edge, see [`docs/operations.md`](docs/operations.md).

## Docs

Start with [`docs/architecture.md`](docs/architecture.md) for the model. Then jump to whichever surface you need.

| Doc | Covers |
|---|---|
| [`architecture.md`](docs/architecture.md) | Identities, namespaces, source families, resolution model, support classes |
| [`api-v1.md`](docs/api-v1.md) | Conventions, snapshot selection, response envelope, shared objects, errors |
| [`api-v1-routes.md`](docs/api-v1-routes.md) | Per-route reference |
| [`api-v1.openapi.json`](docs/api-v1.openapi.json) | Machine-readable contract |
| [`consumer-capabilities.md`](docs/consumer-capabilities.md) | What's served, route mapping, what's out of scope |
| [`storage.md`](docs/storage.md) | Persistence layout, IDs, retention modes, write ownership |
| [`manifests.md`](docs/manifests.md) | Source-family TOML schema and per-namespace ownership |
| [`chain-intake.md`](docs/chain-intake.md) | Block intake, lineage, reorgs, backfill, replay |
| [`projections.md`](docs/projections.md) | Read models — families, replay, invalidation |
| [`execution.md`](docs/execution.md) | Verified resolution and primary-name verification |
| [`operations.md`](docs/operations.md) | Local dev, server deploy, public edge, inspection commands |
| [`upstream.md`](docs/upstream.md) | Pinned upstream refs, citation format, intentional divergences |
| [`runbooks/`](docs/runbooks/) | Release and rollback gates |

Internal planning notes (sequencing, parallel workstreams) are under [`docs/internal/`](docs/internal/) and aren't required reading to use or deploy bigname.

## Guardrails

- Adapters write identity rows and normalised events. They never write projection rows.
- The API reads projections and execution output. It never reads raw facts directly (except documented audit endpoints).
- Public-contract docs lead the implementation: changing public semantics, shared identifiers/enums, coverage meaning, manifest schema, or replacement meaning means updating the relevant doc first or in the same change.
- Claims about ENSv1, ENSv2, or Basenames behaviour cite pinned upstream sources via `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`. See [`docs/upstream.md`](docs/upstream.md).
