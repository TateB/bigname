# Operations

Local development, server deployment, and the public edge for bigname. Container build is `ghcr.io/tateb/bigname`. Three runnable binaries — `bigname-api`, `bigname-indexer`, `bigname-worker` — plus a one-shot `migrate` entrypoint.

Companion docs: [`chain-intake.md`](chain-intake.md) for provider-source semantics, [`storage.md`](storage.md) for retention modes and inspection tooling, [`projections.md`](projections.md) for replay and rebuild commands, [`runbooks/`](runbooks/) for release and rollback gates.

## Local development

Start Postgres and MinIO, apply migrations, and boot the three processes.

```sh
cp .env.example .env                       # optional, for custom ports/creds
docker compose up -d                       # Postgres + MinIO
./scripts/migrate                          # apply migrations
./scripts/dev-up                           # boot api + indexer + worker
```

The API binds to `127.0.0.1:3000` by default. Hit `http://127.0.0.1:3000/docs` for OpenAPI, `/healthz` for readiness.

The compose stack starts:

| Service | Where |
|---|---|
| PostgreSQL | `127.0.0.1:5432`, db `bigname`, creds `bigname` / `bigname` |
| MinIO S3 API | `127.0.0.1:9000` |
| MinIO console | `127.0.0.1:9001` |
| MinIO bootstrap (one-shot) | creates the `bigname-dev` bucket |

`docker compose down` stops the local services. `-v` also drops the data volumes.

Useful one-shots from the workspace root:

```sh
cargo api -- serve
cargo indexer -- run
cargo worker -- run
cargo worker -- migrate
```

### Bootstrap migration hygiene

bigname has no shared production databases that must preserve every intermediate schema. Migration findings that only affect historical data between pre-deployment schemas are bootstrap cleanup unless a shared/staging database is explicitly declared non-rebuildable.

Before the first stateful deployment, collapse the checked-in SQL history into a small baseline migration set. When collapsing, remove obsolete transition-only steps or re-audit them for hard preflight checks before destructive drops (e.g. the pre-deployment `raw_blocks` → `chain_header_audit` transition).

### Live indexing configuration

`./scripts/dev-up` sources `.env`, applies migrations, starts the API, starts `bigname-indexer run`, and starts the worker. On startup the indexer loads the selected manifest root, syncs manifest state into Postgres, rebuilds the stored watch plan, creates persisted chain checkpoint rows for active watched chains, and then polls configured provider sources.

Pick the profile with `BIGNAME_INDEXER_MANIFESTS_ROOT`. Default is `manifests` (mainnet); use `manifests-sepolia-dev` only when running the ENSv2 Sepolia dev profile. Don't load both into the same database.

Wire RPC providers with `BIGNAME_INDEXER_CHAIN_RPC_URLS` — comma-delimited `<chain>=<url>` matching active watched chains:

```sh
BIGNAME_INDEXER_CHAIN_RPC_URLS=ethereum-mainnet=http://127.0.0.1:8545,base-mainnet=http://127.0.0.1:9545
```

If both provider settings are unset, `./scripts/dev-up` still boots the processes and the indexer still syncs manifest/watch state, but provider-backed head fetch and live ingestion stay idle. Bootstrap RPC accepts `http://` endpoints only — use a local node or local HTTP proxy for hosted providers that expose only HTTPS.

`BIGNAME_INDEXER_POLL_INTERVAL_SECS` controls the indexer poll interval (default `5`).

### Live API execution configuration

`GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}` in `mode=verified|both` may execute supported ENS verified-resolution selectors on demand when matching persisted output is absent. Live execution targets the selected exact-name snapshot — `consistency=head` and the latest stored Ethereum checkpoint when no `at` or `chain_positions` is supplied — not provider latest.

Configure `BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>` for the API process before relying on live ENS verified resolution. This is separate from `BIGNAME_INDEXER_CHAIN_RPC_URLS`, which feeds indexer intake and checkpoint state only. If the API Ethereum provider isn't configured, supported live selectors fail with `409 stale` and a configuration message — never declared cache fallback.

### Reth DB source (optional)

Deployments with a local Reth database can also set `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` to `<chain>=<reth-datadir>` entries. At most one source per chain. The Reth source is operational and optional — it must feed the same raw-fact intake contract as JSON-RPC, and Reth-local table references don't replace bigname raw-fact refs or Postgres replay facts.

Native Reth support is compiled only with the `reth-db` feature, e.g. `cargo check -p bigname-indexer --features reth-db`. The opt-in build needs Clang/libclang for Reth's RocksDB/MDBX bindings; default workspace checks don't build those native dependencies.

### Readiness endpoint

The API serves `GET /healthz` on the same bind address as `cargo api -- serve` and `./scripts/dev-up`. Default local: `http://127.0.0.1:3000/healthz`.

`/healthz` is a private operator endpoint — not part of the versioned `/v1` API and not a consumer compatibility surface.

| State | HTTP | Body summary |
|---|---|---|
| Healthy | `200 OK` | `status=ready`, `process.status=running`, `database.status=reachable`, `database.reachable=true`, `database.check=select_1`, `database.error=null` |
| DB unreachable | `503 Service Unavailable` | `status=degraded`, `database.status=unreachable`, `database.reachable=false`, `database.error="database readiness query failed"` |

Database reachability is checked with `SELECT 1` through the configured Postgres pool.

## Server deployment

The image entrypoint accepts one service selector:

```sh
docker run --rm ghcr.io/tateb/bigname:latest api
docker run --rm ghcr.io/tateb/bigname:latest indexer
docker run --rm ghcr.io/tateb/bigname:latest worker
docker run --rm ghcr.io/tateb/bigname:latest migrate
```

Default command is `api`. Raw binary invocations also work:

```sh
docker run --rm ghcr.io/tateb/bigname:latest bigname-api print-openapi
docker run --rm ghcr.io/tateb/bigname:latest bigname-worker inspect watch-plan --json
```

### Fresh server compose

1. Install Docker + Compose.
2. `cp .env.server.example .env.server`, replace placeholder passwords.
3. Set `BIGNAME_IMAGE` to the image tag to run.
4. `docker compose --env-file .env.server -f docker-compose.server.yml up -d`

The server compose file starts Postgres, MinIO, a one-shot migration service, the API, the indexer, and the worker. The API listens on `BIGNAME_API_PORT`. Set `BIGNAME_API_HOST` to control the host bind — production public-edge deployments normally set it to `127.0.0.1` and expose traffic through the Caddy override.

Manifest root: `/app/manifests` for the mainnet profile or `/app/manifests-sepolia-dev` for the ENSv2 Sepolia dev profile. Don't point one runtime at both.

If `BIGNAME_INDEXER_CHAIN_RPC_URLS` is unset, the indexer still syncs manifest/watch state, but provider-backed live ingestion stays idle.

The API process needs its own Ethereum RPC for live ENS verified resolution: `BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<http-url>`. Indexer RPC settings and Reth DB sources do **not** satisfy the API's live-execution provider requirement.

RPC requirements are per selected profile and active watched chain. An Ethereum-only run may omit Base entirely. If the selected profile includes Base but no Base RPC is configured, Base provider-backed intake stays idle with `no_provider`; startup for the configured chains must not fail solely because Base is missing. A provider for a chain not part of the selected manifest root is invalid.

### Reth DB compose override

Layer `docker-compose.reth-db.yml` on top of the server compose file when running with a same-host Reth datadir.

```sh
BIGNAME_INDEXER_RETH_DATADIR_HOST=/var/lib/reth \
BIGNAME_INDEXER_RETH_DATADIR_CONTAINER=/reth-data \
BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES=ethereum-mainnet=/reth-data \
BIGNAME_INDEXER_RETH_DB_USER=0:0 \
BIGNAME_INDEXER_RETH_DB_NOFILE_SOFT=1048576 \
BIGNAME_INDEXER_RETH_DB_NOFILE_HARD=1048576 \
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d indexer
```

Notes:

- The override clears `BIGNAME_INDEXER_CHAIN_RPC_URLS` for the indexer so each chain still has only one provider source.
- The indexer opens the Reth database through Reth's read-only provider API, but the container mount is writable because MDBX cooperative read-only opens still need writable lock/coordination files in the datadir.
- The override defaults `BIGNAME_INDEXER_RETH_DB_USER=0:0` because container-managed Reth datadirs are commonly `root:root`. Operators may set a less-privileged UID/GID after granting that identity write access to the MDBX lock files.
- The `nofile` raise covers Reth's read-only RocksDB provider, which can keep thousands of SST files open.
- It uses host PID/IPC namespaces and bypasses the image's `tini` entrypoint so the indexer process owns PID 1; Reth's live MDBX read-only open can fail from the default `tini` child process.
- The repository Dockerfile builds `bigname-indexer` with the `bigname-indexer/reth-db` Cargo feature so this override keeps the Reth provider path available. Custom images that omit that feature fail fast when `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` is set.

### Bootstrap and catch-up tuning

Hash-pinned backfill defaults to `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC=auto`. In `auto` mode, hash-pinned chunks use the manifest-declared / raw catch-up scope while the indexer is catching up, live polling keeps new block-derived events current, and the indexer also runs automatic bounded raw-fact normalised-event replay from its `normalized_replay_*` cursor until historical normalised events reach the persisted raw-log head. Operators may set `raw-only` to defer live normalised sync, or `inline` to replay each chunk immediately for small ranges.

Bootstrap partitioning into child range leases:

| Variable | Purpose | Default |
|---|---|---|
| `BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_WORKERS` | Worker pool size; `0` selects an automatic count capped at 4 | `0` |
| `BIGNAME_INDEXER_BOOTSTRAP_BACKFILL_RANGE_BLOCKS` | Child range size in blocks | `50000` |
| `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_CHUNK_BLOCKS` | Chunk size for hash-pinned backfill execution | `1024` (server profile) |
| `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_MAX_LOGS_PER_PUSH` | Cap on materialised push for raw-only sparse backfill (older `…_PER_RANGE` still accepted) | — |
| `BIGNAME_INDEXER_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK` | Cap on each automatic normalised-event replay chunk | — |

Use `RUST_LOG=info,sqlx::query=error` for these runs; otherwise SQLx slow-query warnings can print huge generated INSERT statements for dense chunks.

Operational catch-up to finalized head runs as bounded idempotent backfill chunks. Each chunk preflights Postgres size, writable free disk, and any configured object-cache budget; capacity shortage pauses or fails the chunk explicitly rather than silently retaining less data.

Startup bootstrap creates finite backfill jobs from each eligible target's manifest/discovery admitted start through the provider head observed at job creation. It does not cap work to a recent window. Completing bootstrap alone is operational intake readiness — not consumer-replacement or route-coverage evidence without the relevant projection, route, conformance, and rollout gates.

### GHCR image

Published as `ghcr.io/tateb/bigname`. The GitHub Actions workflow publishes `latest` on the default branch and a short commit SHA tag on every push to `main`. Tags pushed to the repository are also published with the same tag name.

Manual publish from an authenticated checkout:

```sh
docker buildx build --platform linux/amd64 \
  -t ghcr.io/tateb/bigname:latest \
  -t ghcr.io/tateb/bigname:$(git rev-parse --short HEAD) \
  --push .
```

## Public edge

Production deploys use Caddy in front of the API. The current production hostname is `bigname.taytems.xyz`.

The Caddy stack is defined by `docker-compose.public.yml` and `docker/caddy/Caddyfile`. Caddy forwards to the internal API at `api:3000`. The public edge exposes the same read-only surface as `bigname-api`:

- `GET /docs`
- `GET /openapi.json`
- `GET /healthz`
- `GET /v1/...`

There are no public admin or mutation routes. Worker, migration, Postgres, MinIO, and indexer control surfaces aren't routed through Caddy.

### Environment

In addition to the server settings, the public edge needs:

```sh
BIGNAME_IMAGE=ghcr.io/tateb/bigname:<tag>
BIGNAME_API_HOST=127.0.0.1
BIGNAME_API_PORT=3000
BIGNAME_PUBLIC_SITE_ADDRESS=api.example.com
BIGNAME_PUBLIC_HTTP_PORT=80
BIGNAME_PUBLIC_HTTPS_PORT=443
```

`BIGNAME_API_HOST=127.0.0.1` keeps direct host access to the API on localhost only. Public access goes through Caddy on 80 and 443.

Temporary HTTP-only deployment before DNS is ready:

```sh
BIGNAME_PUBLIC_SITE_ADDRESS=:80
```

When `BIGNAME_PUBLIC_SITE_ADDRESS` is a hostname with public DNS pointing at the server, Caddy automatically obtains and renews TLS certificates.

### Start

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d
```

For a local image build on a server checkout, replace `BIGNAME_IMAGE` with `bigname:local` in the environment used for the command.

### Verify

```sh
# internal API
curl -fsS http://127.0.0.1:3000/healthz

# public edge
curl -fsS -I http://127.0.0.1/docs
curl -fsS -I http://127.0.0.1/openapi.json
```

For hostname/TLS deployments, replace `127.0.0.1` with the public hostname and `http` with `https`.

### Operations notes

- Keep Postgres and MinIO unexposed at the host/network edge.
- Keep JSON-RPC providers reachable only from the containers that need them.
- Use host firewall or cloud security groups to allow public `80/tcp` and `443/tcp`. Allow `443/udp` when HTTP/3 should be available. Don't publish database, object-store, or execution-node admin ports.
- Caddy data lives in the `caddy-data` Docker volume. Preserve it across container recreates so certificate state survives restarts.
- Caddy sends HSTS and advertises HTTP/3 when the UDP port is published. The docs page and OpenAPI JSON are cacheable for a short window; `v1` API responses aren't edge-cached by this configuration.

## Inspection commands

Worker-owned, read-only operational tooling. None expose public routes; none mutate truth.

| Command | What it shows |
|---|---|
| `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` | Single-block lineage, parent hash, canonicality state, fact and event counts |
| `bigname-worker inspect stored-lineage-range --chain-id <id> --from <block> --to <block>` | Stored lineage rows for a finite block range |
| `bigname-worker inspect backfill-job --backfill-job-id <id>` | One persisted job and its child ranges |
| `bigname-worker inspect execution-trace --execution-trace-id <id>` | Stored execution trace and steps |
| `bigname-worker inspect manifest-drift --json` | Already-persisted alert observations (read-only) |
| `bigname-worker inspect watch-plan --json` | Active watched contracts |
| `bigname-worker manifest-drift audit --json` | Computes live drift candidates, persists alert observations, renders the durable view. Add `--fail-on-alert` to exit non-zero on actionable persisted alerts. |
| `bigname-worker raw-facts compact-log-staging` | Manual compaction boundary for minimal raw-log retention. Refuses to compact unless replay is caught up and failure-free. |
| `bigname-worker replay all-current-projections --json` | One-shot rebuild summary across current-state projection families. |

Each `<family>-current rebuild` subcommand exists for point or family-scoped rebuilds:

```sh
bigname-worker name-current rebuild
bigname-worker address-names-current rebuild
bigname-worker children-current rebuild
bigname-worker permissions-current rebuild
bigname-worker primary-names-current rebuild
bigname-worker resolver-current rebuild
bigname-worker record-inventory-current rebuild
bigname-worker record-inventory-current hydrate-text-values
```

Execution cache invalidation commands (operational; for incident response):

```sh
bigname-worker execution invalidate-verified-resolution-manifest
bigname-worker execution invalidate-verified-resolution-topology-boundary
bigname-worker execution invalidate-verified-resolution-record-boundary
bigname-worker execution invalidate-verified-primary-name-manifest
bigname-worker execution invalidate-verified-primary-name-topology-boundary
bigname-worker execution invalidate-verified-primary-name-record-boundary
```

Indexer subcommands beyond `run`:

```sh
bigname-indexer backfill --source-family <family>           # source-scoped
bigname-indexer backfill --watch-target <contract_instance_id>
bigname-indexer ops-catchup [--follow]                      # finalized catch-up chunks
bigname-indexer replay normalized-events                    # bounded replay over canonical raw facts
bigname-indexer repair ens-v1-text-records                  # narrow adapter repair
```

## Decisions captured here

A handful of operational choices that earlier ADRs documented:

- **Stack:** Rust modular monolith. `tokio` for async, `axum` for HTTP, `sqlx` + Postgres for storage, `serde` for wire/persistence, `clap` for binaries, `tracing` (OpenTelemetry-compatible) for observability, S3-compatible object storage for large execution and metadata artifacts.
- **Background work:** No separate message bus. Workers coordinate through the database plus polling. Introduce a queue only when measured load demands it.
- **Local development:** Docker Compose for Postgres and object storage, checked-in migrations, one command (`./scripts/dev-up`) to boot all three processes.
- **Testing:** `cargo fmt`, `cargo clippy`, `cargo test`/`cargo nextest`, migration verification in CI, plus the Rust conformance harness under `tests/conformance/`.

## Decision-making

Doc-first. AGENTS.md spells it out: when a change touches public semantics, shared identifiers/enums, coverage meaning, manifest schema, workstream ownership, or replacement meaning, the relevant doc in `docs/` is updated first or in the same change. Reviewers reject implementations that drift from those docs.

There's no separate ADR folder. Significant constraints live near the surfaces they govern (e.g. identity rules in [`storage.md`](storage.md), upstream-divergence policy in [`upstream.md`](upstream.md)) so they stay close to the code that has to follow them.
