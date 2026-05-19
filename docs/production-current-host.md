# Current Production Host

This doc records non-secret operational notes for the current bigname production
host. General production setup remains in [`production.md`](production.md).
Use [`runbooks/production-docker.md`](runbooks/production-docker.md) for
copy-pastable Docker operations, pause/resume commands, monitoring queries, and
failure handling.

Do not add provider URLs, passwords, tokens, private IPs, wallet keys, cloud
credentials, or `.env.server` values here. Commit only capacity observations,
container names, and operator-safe commands.

## Storage Budget

On 2026-05-07 UTC, the Docker-backed filesystem reported:

| Mount | Total | Used | Available | Use |
| --- | ---: | ---: | ---: | ---: |
| `/` / `/var/lib/docker` | `3.5T` | `3.1T` | `181G` | `95%` |

`docker system df` reported:

| Type | Total | Active | Size | Reclaimable |
| --- | ---: | ---: | ---: | ---: |
| Images | `6` | `2` | `23.65G` | `860M` |
| Containers | `2` | `2` | `1.216G` | `0B` |
| Local volumes | `6` | `0` | `505.2G` | `505.2G` |
| Build cache | `71` | `0` | `18.37G` | `18.37G` |

The running chain-node containers at observation time were:

- `eth-archive-node-lighthouse-1`
- `eth-archive-node-reth-1`

Bootstrap and finalized catch-up on this host profile must keep at least `100G`
writable free disk after estimated write amplification. With `181G` observed
free, the current headroom above that floor is about `81G`.

Large chunks, audit-header retention, object-cache growth, or full-payload
retention must pause or be resized before crossing the free-space floor.
Docker-reported reclaimable volume space is not budget until an operator
confirms the volumes are unrelated and safe to remove.

## Capacity Check

Before long bootstrap or finalized catch-up runs, record:

```sh
df -h / /var/lib/docker /home/ubuntu/bigname
docker system df
docker stats --no-stream
```

If the preflight estimate would cross the free-space floor, pause range work.
Do not drop selected replay facts, retain fewer lineage records, or silently
switch retention modes to make a chunk fit.
