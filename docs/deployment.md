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
`BIGNAME_API_PORT` and answers readiness at `/healthz`.

The indexer loads exactly one manifest root. Use `/app/manifests` for the
mainnet profile or `/app/manifests-sepolia-dev` for the ENSv2 Sepolia dev
profile. Do not point one runtime at both manifest roots.

If `BIGNAME_INDEXER_CHAIN_RPC_URLS` is unset, the indexer still syncs
manifest/watch state, but provider-backed live ingestion remains idle. Current
bootstrap RPC support accepts `http://` endpoints.

## GHCR Image

The repository publishes the image to:

```text
ghcr.io/tateb/bigname
```

The GitHub Actions workflow publishes `latest` on the default branch and a short
commit SHA tag on every push to `main`. Tags pushed to the repository are also
published with the same tag name.

Manual publish from an authenticated checkout:

```sh
docker buildx build --platform linux/amd64 \
  -t ghcr.io/tateb/bigname:latest \
  -t ghcr.io/tateb/bigname:$(git rev-parse --short HEAD) \
  --push .
```
