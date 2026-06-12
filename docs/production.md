# Production

This page documents the public single-host deployment shape used for bigname.

The current production hostname is `bigname.taytems.xyz`.

## Public Edge

Public traffic terminates at Caddy, defined by `docker-compose.public.yml` and
`docker/caddy/Caddyfile`. Caddy forwards requests to the internal API service at
`api:3000`.

The public edge exposes the same read-only API surface that the `bigname-api`
process serves:

- `GET /`
- `GET /docs`
- `GET /openapi.json`
- `GET /healthz`
- `GET /v1/...`

There are no public admin or mutation routes in the API process. Worker,
migration, PostgreSQL, and indexer control surfaces are not routed
through Caddy.

## Environment

Use the normal server environment from `.env.server`, plus these production edge
settings:

```sh
BIGNAME_IMAGE=ghcr.io/tateb/bigname:<tag>
BIGNAME_API_HOST=127.0.0.1
BIGNAME_API_PORT=3000
BIGNAME_PUBLIC_SITE_ADDRESS=api.example.com
BIGNAME_PUBLIC_HTTP_PORT=80
BIGNAME_PUBLIC_HTTPS_PORT=443
```

`BIGNAME_API_HOST=127.0.0.1` keeps direct host access to the API on localhost
only. Public access goes through Caddy on ports 80 and 443.

For a temporary HTTP-only deployment before DNS is ready, set:

```sh
BIGNAME_PUBLIC_SITE_ADDRESS=:80
```

When `BIGNAME_PUBLIC_SITE_ADDRESS` is a hostname with public DNS pointing at the
server, Caddy automatically obtains and renews TLS certificates.

## Start

Start or refresh the public stack with:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d
```

For a local image build on a server checkout, replace `BIGNAME_IMAGE` with
`bigname:local` in the environment used for the command.

## Verify

Check the internal API:

```sh
curl -fsS http://127.0.0.1:3000/healthz
```

Check the public edge:

```sh
curl -fsS -I http://127.0.0.1/
curl -fsS -I http://127.0.0.1/docs
curl -fsS -I http://127.0.0.1/openapi.json
```

For hostname/TLS deployments, replace `127.0.0.1` with the public hostname and
`http` with `https`.

## Operations Notes

- Keep PostgreSQL unexposed at the host/network edge.
- Keep JSON-RPC providers reachable only from the containers that need them.
- Use [`runbooks/production-docker.md`](runbooks/production-docker.md) for
  current-host Docker operations, monitoring, pause/resume, and recovery
  checklists.
- Use host firewall or cloud security groups to allow public `80/tcp` and
  `443/tcp`. Allow `443/udp` when HTTP/3 should be available. Do not publish
  database or execution-node admin ports.
- Caddy data lives in the `caddy-data` Docker volume. Preserve it across
  container recreates so certificate state survives restarts.
- Caddy sends HSTS and advertises HTTP/3 when the UDP port is published. The
  docs page and OpenAPI JSON are cacheable for a short window; the `v1` API
  responses are not edge-cached by this configuration.
