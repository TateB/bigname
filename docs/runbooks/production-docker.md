# Production Docker Runbook

This runbook is for operating the single-host Docker production stack from a
server checkout. General deployment shape is documented in
[`../deployment.md`](../deployment.md), public edge details in
[`../production.md`](../production.md), and current non-secret host notes in
[`../production-current-host.md`](../production-current-host.md).

Do not commit `.env.server`, provider URLs, passwords, tokens, private IPs, or
cloud credentials.

## Compose Command

Run commands from the repository root:

```sh
cd /home/ubuntu/bigname
```

For the current same-host Reth setup, use both compose files:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  ps
```

If operating the public edge, add `-f docker-compose.public.yml` to commands
that need Caddy.

## Preflight

Before long bootstrap, replay, migration, or rebuild work:

```sh
df -h / /var/lib/docker /home/ubuntu/bigname
free -h
docker system df
docker stats --no-stream
docker inspect -f '{{.Name}} {{.RestartCount}} {{.State.Status}} {{.State.OOMKilled}} {{.State.Error}}' \
  bigname-api-1 bigname-indexer-1 bigname-worker-1 bigname-postgres-1
```

The current host notes define a `100G` free-space floor for writable disk during
bootstrap and finalized catch-up. Pause range work before crossing that floor.

## Start Or Refresh

For the configured image tag in `.env.server`:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d
```

For a local emergency build from the checkout:

```sh
docker build -t bigname:local .

BIGNAME_IMAGE=bigname:local docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up --force-recreate migrate

BIGNAME_IMAGE=bigname:local docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d --force-recreate api worker indexer
```

Prefer committing or clearly recording the source revision before running a
local image on production. A local tag is convenient, but it is not a durable
release identifier by itself.

## Pause And Resume

To pause chain intake and normalized replay:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  stop indexer
```

To freeze projection writes as well:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  stop worker
```

Leave Postgres running unless the maintenance requires a database stop. Leave
Reth and Lighthouse running when possible so the node does not need a long
catch-up later. The API may stay up for reads if Postgres is healthy and the
operator is comfortable serving the current projection state.

Resume:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  up -d worker indexer
```

## Monitoring

Process and resource checks:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  ps

docker stats --no-stream \
  bigname-api-1 bigname-indexer-1 bigname-worker-1 bigname-postgres-1 \
  eth-archive-node-reth-1 eth-archive-node-lighthouse-1

curl -fsS http://127.0.0.1:3000/healthz
```

Recent logs:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  logs --tail=200 indexer

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  logs --tail=200 worker
```

Database activity:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T postgres psql -U bigname -d bigname -P pager=off -c "
SELECT pid, now() - query_start AS age, state, wait_event_type, wait_event,
       left(regexp_replace(query, '\s+', ' ', 'g'), 240) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND state <> 'idle'
ORDER BY query_start;"
```

Chain checkpoints:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T postgres psql -U bigname -d bigname -P pager=off -c "
SELECT chain_id, canonical_block_number, safe_block_number,
       finalized_block_number, updated_at
FROM chain_checkpoints
ORDER BY chain_id;"
```

Normalized replay cursor:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T postgres psql -U bigname -d bigname -P pager=off -c "
SELECT deployment_profile, chain_id, cursor_kind,
       next_block_number, target_block_number, last_completed_block_number,
       last_replayed_at, last_failure_at,
       left(last_failure_reason, 240) AS last_failure_reason
FROM normalized_replay_cursors
ORDER BY deployment_profile, chain_id, cursor_kind;"
```

Name-surface normalizer repair status:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T postgres psql -U bigname -d bigname -P pager=off -c "
SELECT normalizer_version, count(*)
FROM name_surfaces
GROUP BY normalizer_version
ORDER BY count(*) DESC;

SELECT expected_normalizer_version, finding_kind, count(*)
FROM name_surface_normalization_repair_findings
GROUP BY expected_normalizer_version, finding_kind
ORDER BY expected_normalizer_version, finding_kind;"
```

After deploying a normalizer change and running migrations, apply only compatible
retained-surface metadata from the existing indexer container:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T indexer bigname-indexer repair name-surface-normalization \
    --apply-compatible \
    --record-findings
```

This command does not rewrite `logical_name_id` or identity-defining hash fields.
Rows that reject or remap under the active normalizer remain unchanged and are
recorded in `name_surface_normalization_repair_findings` for a separate
semantic repair decision.

Because compatible repair may refresh retained surface display metadata, replay
all current projections before declaring the repair complete:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T worker bigname-worker replay all-current-projections
```

The live identity validation checks for stale display rows in the API-readable
current projections and identity-feed sidecar after this replay.

Backfill and projection apply:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  exec -T postgres psql -U bigname -d bigname -P pager=off -c "
SELECT status, count(*) FROM backfill_jobs GROUP BY status ORDER BY status;
SELECT status, count(*) FROM backfill_ranges GROUP BY status ORDER BY status;
SELECT cursor_name, last_change_id, updated_at FROM projection_apply_cursors;
SELECT projection,
       count(*) FILTER (WHERE claim_token IS NULL) AS pending,
       count(*) FILTER (WHERE claim_token IS NOT NULL) AS claimed,
       count(*) FILTER (WHERE last_failure_reason IS NOT NULL) AS failed
FROM projection_invalidations
GROUP BY projection
ORDER BY projection;"
```

## Synced Criteria

Treat the stack as fully synced only when all of these are true:

- indexer and worker are running without fresh restarts or OOM kills
- chain checkpoints continue advancing near the configured provider or Reth head
- bootstrap/backfill ranges have no unexpected `failed`, `reserved`, or long
  running stale rows
- normalized replay has `next_block_number > target_block_number` and no
  `last_failure_reason`
- projection invalidations have drained, with no failed invalidations
- API health is ready
- disk, RAM, swap, and CPU remain within the host budget

After a repair, keep monitoring for at least one to two hours after all of the
above are true, not merely after the first successful restart.

## Failure Handling

When a failure appears, capture the broad state before restarting:

```sh
docker inspect -f '{{.Name}} {{.RestartCount}} {{.State.Status}} {{.State.OOMKilled}} {{.State.Error}} {{.State.FinishedAt}}' \
  bigname-api-1 bigname-indexer-1 bigname-worker-1 bigname-postgres-1

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  logs --tail=300 indexer

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.reth-db.yml \
  logs --tail=300 worker
```

For normalized-event mismatch failures, stop the indexer before retry loops
generate noise. Decide whether the fix is code, a data repair migration, or
both. If a migration changes normalized event canonicality or identity fields,
it must also enqueue projection invalidation through
`projection_normalized_event_changes`.
