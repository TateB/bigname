# Pipeline Monitoring

This runbook adds the phase runner to an existing Prometheus and Grafana stack.
It uses only checked-in configuration. Applying it restarts or reloads the
operator's monitoring services and recreates the phase-runner container; it
does not add database writes for metrics. Runner startup can settle active
`running` or `paused` rows with no unfinished explicit repair for chains that
are no longer configured, as described below.

The artifacts are:

- [`ops/monitoring/prometheus/phase-runner.yml`](../../ops/monitoring/prometheus/phase-runner.yml)
  — scrape job and rule-file reference;
- [`ops/monitoring/prometheus/phase-runner-alerts.yml`](../../ops/monitoring/prometheus/phase-runner-alerts.yml)
  — paging rules;
- [`ops/monitoring/prometheus/phase-runner-alerts.test.yml`](../../ops/monitoring/prometheus/phase-runner-alerts.test.yml)
  — rule-evaluation fixtures for every checked-in paging rule; and
- [`ops/monitoring/grafana/dashboards/phase-runner.json`](../../ops/monitoring/grafana/dashboards/phase-runner.json)
  — importable dashboard.

## Apply the runner endpoint

Deploy the image built from this change and validate the tracked Compose file
before recreating anything:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml config
```

The tracked configuration binds the container listener to `0.0.0.0:9465` and
publishes it as `127.0.0.1:9465` on the host. The loopback-only host mapping is
useful for a manual check and does not make the endpoint public. Recreate the
runner with the same image and all overlays used by the deployment:

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  up -d --no-deps phase-runner

curl -fsS http://127.0.0.1:9465/metrics | \
  grep '^phase_runner_metrics_refresh_success 1$'
```

If `9465` is already assigned, set the Compose-only
`BIGNAME_PHASE_RUNNER_METRICS_PORT` to a free host port. Prometheus does not use
that host port in the recommended container-network setup below; it connects to
the container listener at `phase-runner:9465`.

Before enabling paging, set
`BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` above the slowest healthy
batch or inter-phase transition measured on this deployment. The checked-in
default is 900 seconds. The runner writes heartbeats between batches, so a
single healthy batch longer than 900 seconds will page at the default. Rebuild
batches during a planned [re-derivation
boundary](../glossary.md#re-derivation-boundary) have historically exceeded
eight minutes. Calibrate the threshold before the full source re-walk, not
after its first false page.

## Connect Prometheus

The scrape artifact expects Prometheus to resolve `phase-runner` on the
application's `bigname_default` Docker network. Add that existing network to the
Prometheus service in the host's monitoring Compose file:

```yaml
services:
  prometheus:
    networks:
      - default
      - bigname

networks:
  bigname:
    external: true
    name: bigname_default
```

Do not expose Prometheus or the metrics listener at the public edge. If the
monitoring stack deliberately uses host networking instead, change the checked
scrape target as it is copied into the host configuration to
`127.0.0.1:9465`.

Merge the `rule_files` and `scrape_configs` entries from
`phase-runner.yml` into the host's Prometheus configuration. Copy or mount
`phase-runner-alerts.yml` beside that configuration so the relative rule-file
path resolves. Keep the job name `bigname-phase-runner`; the dashboard and
alerts select that exact label.

A directly launched one-shot redo binds an ephemeral loopback listener by
default, so concurrent repairs do not contend for the supervisor port. Its
info-level startup event records the selected address when `RUST_LOG=info`.
That default is not a stable Prometheus target. To apply the checked-in
repair-mode alerts during an operator redo, pass a unique stable
`--metrics-bind-addr` or set
`BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR`, then add that address under the
same job name. The redo command deliberately ignores
`BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR`, which remains the supervised runner's
setting. Remove the temporary target and reload Prometheus immediately when the
redo exits, then confirm it is absent before the two-minute
`BignamePhaseRunnerDown` hold elapses.

The container-restart rule uses the runner endpoint's process-start gauge. It
does not depend on cAdvisor container labels, which are unavailable from some
cAdvisor and Docker storage-driver combinations. Confirm that the runner target
exports the gauge:

```sh
curl --get --fail --silent --show-error \
  'http://127.0.0.1:9090/api/v1/query' \
  --data-urlencode \
  'query=phase_runner_process_start_timestamp_milliseconds{job="bigname-phase-runner"}'
```

The result must contain one sample per runner target. Prometheus keeps the
target's `job` and `instance` labels stable across container replacements, so
each new process-start value is observable as a change on the same time series.
No cAdvisor or additional scrape configuration is needed for this rule.
When planned Ingest, Interpret, and Project one-shot repairs precede a supervisor
restart inside ten minutes, schedule a maintenance silence scoped only to
`alertname="BignamePhaseRunnerContainerRestarting"`, `job="bigname-phase-runner"`,
and the maintained target's exact `instance`;
keep every other alert active and end the silence after the restarted supervisor is healthy.

Validate the fully assembled host files before reloading Prometheus:

```sh
promtool check rules /path/to/phase-runner-alerts.yml
promtool check config /path/to/prometheus.yml
promtool test rules /path/to/phase-runner-alerts.test.yml
```

Use the monitoring stack's ordinary reload or targeted Compose recreation.
Then check the target and rules without changing application state:

```sh
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22bigname-phase-runner%22%7D'
curl -fsS 'http://127.0.0.1:9090/api/v1/rules'
```

The first result must contain a sample value of `1`. The second must list the
`bigname-phase-runner` rule group. Route the checked-in `severity=page` label
through the host's existing notification policy; these files do not contain a
receiver, token, or destination.

## Import the Grafana dashboard

Import `phase-runner.json` through Grafana's dashboard import screen and choose
the host Prometheus data source when prompted. For file provisioning, mount the
JSON in the existing dashboard-provisioning directory and use the monitoring
stack's normal Grafana reload or targeted recreation. The stable dashboard UID
is `bigname-phase-runner`, so a later import updates the same dashboard.

## Panels

| Panel | What it means |
| --- | --- |
| Phase lifecycle state | The current `idle`, `running`, `paused`, `completed`, or `failed` row for every chain phase. A value of `1` is the active state. |
| Phase progress | The latest processed block and current target. `-1` means the runner has not recorded that position. |
| Heartbeat age | Seconds since the newest heartbeat for the chain phase, plus the in-process age since each supervised or active one-shot repair chain last crossed a phase or batch boundary. A phase value of `-1` means no database heartbeat exists. Compare both with the configured stale threshold, which defaults to 900 seconds and must exceed the slowest healthy batch or inter-phase transition. |
| Head lag in blocks | Observed provider target minus the phase's processed block. For Live, the target is the provider head observed at the start of its latest batch. The paging rule applies to Live because historical phases can be far behind during an expected rebuild. |
| Verification level | The stored `quick_synced`, `cross_checked`, or `node_checked` result. A value of `1` identifies the recorded level. |
| Repair and reinterpretation state | The active marker and progress for unfinished repair work, plus whether Interpret still needs a repair run because its stored [interpreter content hash](../glossary.md#interpreter-content-hash) differs. Starting the required repair adopts the new hash and clears the requirement gauge; `phase_runner_redo_in_progress` stays at `1` until that work finishes. |
| Phase cursor non-progress | Committed [work-bearing batches](../glossary.md#work-bearing-batch) confirmed at the next resume to have left the [durable composite cursor](../glossary.md#durable-composite-cursor) unchanged, and the age of that sequence. Normal, redo, and recompute-flags work remain separate. |
| Exporter health | Whether Prometheus can scrape the runner and whether the latest read of PostgreSQL state succeeded. |

## Alerts

| Alert | Threshold | Plain-language meaning |
| --- | --- | --- |
| `BignamePhaseFailed` | A phase reports `failed` on one rule evaluation. | This intentionally trades pages during retryable transient backoff for guaranteed visibility of terminal errors and crash loops. Use the logs and subsequent state to distinguish them. |
| `BignamePhaseRunnerDown` | The target reports `up=0`, or no `up` series exists for the job, continuously for 2 minutes. | The runner process, metrics listener, or Prometheus target definition stayed unavailable. A successful scrape resets the timer, so this rule does not catch a flapping crash loop. The absent-target branch has only the `job` label because no target exists to supply an `instance`. |
| `BignamePhaseRunnerCapacityPaused` | A phase remains continuously `paused` for 15 minutes. | Storage capacity has stopped pipeline work for the named chain phase. Short capacity waits do not page, while the runner continues refreshing its liveness signals during the wait. |
| `BignamePhaseRunnerContainerRestarting` | The runner's process-start value changes at least 3 times within 10 minutes. | The container is crash-looping, including fresh-deployment failures that happen before a phase failure can be stored. Prometheus must successfully scrape each start that it counts. |
| `BignamePhaseRunnerHeartbeatThresholdMissing` | The configured heartbeat-threshold series is absent for 2 minutes. | The runner image and rules are incompatible, so the age-based alerts cannot be evaluated safely. |
| `BignamePhaseRunnerLoopHeartbeatMissing` | The runner is scrapeable and exports the heartbeat threshold, but its runner-loop heartbeat series is absent for 2 minutes. | The runner image predates the loop-liveness rule, so between-phase stalls cannot be evaluated safely. |
| `BignamePhaseRunnerHeartbeatStale` | An active phase has no database liveness heartbeat, or exceeds `BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS` (900 seconds by default), for 2 minutes. | The runner stopped refreshing phase liveness. Capacity waits keep this heartbeat fresh and instead page through `BignamePhaseRunnerCapacityPaused` after 15 minutes. |
| `BignamePhaseRunnerLoopStale` | A supervised or active one-shot repair chain crosses no phase or batch boundary for the heartbeat threshold, plus 2 minutes. | The process is scrapeable, but work for that chain may be wedged while every supervised phase row rests. |
| `BignamePhaseRunnerHeadLagHigh` | Live lag exceeds 30 blocks and Live is observed running at least once in every 2-minute window for 10 minutes. | The chain is persistently falling behind new blocks; brief completed-state zeroes do not reset the alert, while a failed or resting phase does not keep it active without new running samples. |
| `BignamePhaseRunnerMetricsRefreshStale` | The database read fails, or the last successful refresh becomes older than 60 seconds, for 2 minutes. | The endpoint is reachable but is serving an old view of pipeline state. |
| `BignamePhaseRunnerPhaseNonProgress` | Three confirmed unchanged-cursor work batches, or two whose sequence is at least 10 minutes old, remain present for 2 minutes. | The named phase and mode is repeatedly committing work without changing its durable resume position. It does not require the phase to be sampled as `running`; a failed phase remains owned by `BignamePhaseFailed`. |
| `BignamePhaseRunnerProgressMetricsMissing` | Either non-progress metric family is absent from a scrapeable runner that exports the heartbeat threshold for 2 minutes. | The runner image and rules are incompatible, so cursor-progress paging fails closed. |

The two hand-detected failure cases from issue #327 map directly to paging: a
terminal phase error triggers `BignamePhaseFailed` while the runner remains
reachable, or `BignamePhaseRunnerDown` if the process exits and remains down
for 2 minutes. Repeated exits with successful scrapes between them trigger
`BignamePhaseRunnerContainerRestarting` reliably for restart periods under
about three minutes, intermittently for periods from roughly three to five
minutes, and not for periods of five minutes or longer. A stalled batch that
crosses the deployment's configured maximum healthy duration triggers
`BignamePhaseRunnerHeartbeatStale`.

A slow crash loop remains unpaged when its restart period is five minutes or
longer, each outage lasts less than two minutes, and no phase failure has yet
been stored; one concrete case is a fresh deployment that is OOM-killed several
minutes into every startup.

## Phase cursor non-progress response

`phase_runner_phase_batches_since_cursor_advance` counts consecutive successful
work-bearing commits that the next durable resume confirms left the cursor
unchanged. `phase_runner_phase_cursor_stall_age_seconds` measures from the first
such commit. Both gauges use only `chain`, `phase`, and `mode`; `mode` is
`normal`, `redo`, or `recompute_flags`. Normal progress uses
`phase_runner_phase_current_block`; repair work uses
`phase_runner_redo_current_block` and `phase_runner_redo_mode`.

The cursor comparison includes block hashes. Ingest additionally compares its
sorted per-source next block, last processed block, and loaded redo boundary.
Thus a reorg, a hash replacement, or one Ingest source moving resets the
sequence even when the displayed summary block is unchanged. Target/head
movement does not reset it. A single Project boundary replay can reach count
`1` but cannot page. Caught-up Live polls that claim no cursor movement, no-head/empty completions, idle polls,
capacity pauses, and completed Verify revalidation clear or bypass the detector.
An idle poll clears earlier evidence, so pinned commits separated by idle polls
do not accumulate; this is accepted because an idle poll reports no indexing or
repair work. Evidence expires after the configured heartbeat-stale interval
without another successful work-bearing commit.
In-process retries of the same requested repair range retain accumulated evidence across attempt restarts; a different range or a process restart starts fresh.

With the checked-in 15-second rule interval, the two-minute hold pages no later
than 3 minutes after the third confirmed pinned completion. The age path
pages no later than 13 minutes after the second. A single long-running batch
still belongs to the heartbeat alert. Preserve the 13-minute two-batch bound by
configuring `--heartbeat-stale-after-secs` (or
`BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS`) to at least 900 seconds;
a lower expiry can clear that evidence before the age-based rule holds, leaving
the three-batch path as the remaining non-progress page.

When `BignamePhaseRunnerPhaseNonProgress` fires:

1. Record the alert's `chain`, `phase`, and `mode`.
2. Inspect both non-progress gauges, the applicable normal or repair current
   block, the target, recent phase logs, and provider quota/database use.
3. Confirm that the count is increasing or already at threshold while the
   durable cursor remains pinned.
4. If quota or database budget is at risk, stop only the phase runner with the
   exact Compose overlays deployed on the host:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     stop phase-runner
   ```

   Preserve every additional deployed `-f` overlay in the real command.
5. Do not edit `chain_phase_state`, repair fields, or Ingest cursors to silence
   the alert. Capture cursor/target hashes from logs, count, age, image SHA, and
   provider/database evidence before remediation.
6. After deploying the corrective change, resume with the same image and
   overlays:

   ```sh
   docker compose --env-file .env.server \
     -f docker-compose.server.yml \
     up -d phase-runner
   ```

7. Confirm both gauges return to zero and the applicable normal or repair
   cursor advances.

## Removing a configured chain

Before removal, use the normal runner or reviewed recovery procedure to recover
every `failed` phase to `completed` and finish every explicit repair. If either
condition cannot be met, keep the chain configured and escalate to the
phase-runner and storage owners for a separately reviewed decommission cleanup.

Then stop the runner, remove the chain from configuration, and restart it. At
startup, the runner acquires the same per-phase lock used for normal recovery
and changes any `running` or `paused` row with no unfinished explicit repair for
an unconfigured chain to `completed`; it logs the chain and phase it settled and
never starts work for that chain. Failed rows and unfinished repair markers are
deliberately not rewritten, which is why they must be resolved before removal.
Do not update statuses, clear repair markers, or delete rows merely to silence
an alert.

If the chain is configured again later, the runner resumes from preserved
cursors and re-runs any Ingest or Verify phase whose completion evidence is
incomplete, rather than trusting the rewritten `completed` status alone. It
resumes Ingest when the current block or live handoff does not match the target,
and resumes Verify unless it has a matching current/target block pair and a
[verification level](../glossary.md#verification-level). Settling an active
Ingest row clears its live handoff but preserves its source cursors, so re-adding
the chain resumes even if an older runner stopped between its formerly separate
summary and cursor writes.

## First-response checks

Keep diagnosis read-only until the failure is understood:

```sh
curl -fsS http://127.0.0.1:9465/metrics | \
  grep -E 'phase_runner_(phase_status|heartbeat_age_seconds|loop_heartbeat_age_seconds|head_lag_blocks|phase_batches_since_cursor_advance|phase_cursor_stall_age_seconds)'

docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  logs --tail=200 phase-runner
```

For a failed phase, record the chain, phase, error log, current block, and
target block before choosing a recovery procedure. For stale heartbeats, check
whether block progress is also flat and whether the exporter refresh remains
healthy. For head lag, compare Live progress with its observed provider target
and check provider and database latency. Follow
[`production-docker.md`](production-docker.md#recovery-plays) for recovery; do
not clear phase rows or mark work complete by hand.

For `BignamePhaseRunnerCapacityPaused`:

1. Find the runner warning `phase paused until storage capacity recovers` and
   read its `breach_reasons` and `free_disk_bytes` fields to identify which
   bound stopped work.
2. If `breach_reasons` contains `database_size`, review actual storage headroom
   before increasing `BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES`. Server Compose
   forwards an explicitly configured ceiling; leaving it unset omits it and
   disables that ceiling. Empty, malformed or overflowing values are invalid;
   ceiling zero is a limit, not disabled protection. Recreate the runner after
   changing settings because they are read only at startup.
3. For `free_disk`, free space on the filesystem containing the configured
   `BIGNAME_PHASE_RUNNER_WRITABLE_PATH`. Follow the [capacity preflight](production-docker.md#capacity-preflight)
   to verify it is PostgreSQL's actual filesystem and is writable without
   exposing database files. The required server-Compose floor is operator-selected;
   missing/empty values fail rendering, while zero must be rejected operationally.
   The unchanged CLI still accepts zero. Do not derive a production reserve from
   an old host observation or lower the floor merely to clear an alert.
   The guard adds the preceding batch's reserved-write estimate to the floor;
   that estimate starts at zero for a new batch loop and is not a reservation.
   Capacity is checked before batches, not before all startup work or every write.
   Heartbeats and unrelated writes can continue during a pause, so this is not
   an absolute ENOSPC guarantee. A probe permission error is a retryable phase
   failure, not an ordinary capacity breach.
4. Once the capacity check clears, the phase resumes from its stored cursor.
   No phase-state edit or manual phase restart is needed.
