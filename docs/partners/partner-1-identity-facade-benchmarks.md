# partner-1 Identity Façade Benchmark Notes

Date: 2026-05-19

Environment:

- API/Postgres host-local measurement via `http://127.0.0.1:3000`.
- Public route smoke-tested through `https://bigname.taytems.xyz`.
- Ethereum Mainnet indexing was synced at measurement time with `projection_lag_blocks=0`.
- Base remains unconfigured by operator choice; `/v1/status/indexing` is therefore `degraded` while active/shadow Base manifests have no checkpoint/source-intake readiness.

## Changes Measured

- Forward and reverse identity record loading no longer reads large `name_current.provenance` or unused record-inventory JSON fields.
- Reverse batch defaults to `page_size=1`, matching feed rendering. Reverse single keeps the profile-style `page_size=100` default.
- Reverse `total_count` is read from `address_names_current_identity_counts`, an indexed sidecar maintained from `address_names_current` and readable `name_current` eligibility, rather than counted on the request path.
- ENS token IDs fall back to the current surface labelhash as a uint256 string for second-level `.eth` names when projected authority/registration/control summaries do not carry a token ID.
- `POST /v1/identity/addresses:feed` is the compact feed DTO for latency-sensitive identity rendering. It returns one display record per address plus `total_count`, skips full `NameRecord` hydration, and is backed by indexed count/display sidecars.

## Before / After

The "before" sample was taken before the slim identity read path and feed-default reverse batch. The "after" sample used the live Docker API after redeploy.

| Case | Before p95 | After p95 | Notes |
| --- | ---: | ---: | --- |
| forward single `taytems.eth` | 2.30 ms | 1.33 ms | Full `NameRecord`. |
| forward batch 100 | 29.04 ms | 14.58 ms | Full `NameRecord` per input. |
| forward batch 250 | 66.51 ms | 23.92 ms | Full `NameRecord` per input. |
| forward batch 1000 | 264.54 ms | 107.85 ms | 0.91 MB response. |
| reverse batch 100, default page | n/a | 46.81 ms | Default is now one record per input; `total_count` present. |
| reverse batch 250, default page | n/a | 57.17 ms | Default is now one record per input; `total_count` present. |
| reverse batch 1000, default page | n/a | 250.91 ms | 1.90 MB response. |
| reverse batch 100, `page_size=10` | 718.39 ms | 151.73 ms | Full `ReverseNameRecord`; 1.01 MB response. |
| reverse batch 250, `page_size=10` | 1304.36 ms | 332.54 ms | Full `ReverseNameRecord`; 2.53 MB response. |
| reverse batch 1000, `page_size=10` | 11420.88 ms | 1415.95 ms | Full `ReverseNameRecord`; 10.26 MB response. |

## 2026-05-21 Compact Feed Update

Measured against the live Docker API on `http://127.0.0.1:3000` using one keep-alive HTTP connection, 30 warmup requests per case, and 200 measured requests per case except the 1000-input case, which used 80 measured requests.

| Case | Response size | p95 |
| --- | ---: | ---: |
| `POST /v1/identity/addresses:feed`, 1 input | 0.41 KB | 1.12 ms |
| `POST /v1/identity/addresses:feed`, 10 inputs | 4.11 KB | 1.28 ms |
| `POST /v1/identity/addresses:feed`, 25 inputs | 10.12 KB | 0.89 ms |
| `POST /v1/identity/addresses:feed`, 50 inputs | 20.29 KB | 1.29 ms |
| `POST /v1/identity/addresses:feed`, 100 inputs | 40.58 KB | 2.21 ms |
| `POST /v1/identity/addresses:feed`, 250 inputs | 100.88 KB | 5.03 ms |
| `POST /v1/identity/addresses:feed`, 1000 inputs | 407.09 KB | 18.79 ms |

## Interpretation

The compact feed route meets an under-10 ms p95 local/server-side proxy for the latency-sensitive tens-to-hundreds feed-rendering band measured here through 250 inputs. This measurement uses the readable-row-safe `address_names_current_identity_feed` sidecar, not the earlier live first-row shortcut. It is not a substitute for a final AWS `us-east` vantage measurement in the deployed environment, but it demonstrates that the API and database path are under the budget on the running Docker stack. The 1000-input ceiling remains supported for bulk/batch compatibility, but it is not the latency target: the compact response is already about 407 KB at 1000 inputs, and p95 is dominated by response size and serialization. Full shared-record reverse batches remain the profile/detail path and are intentionally not the feed latency contract.

## Unsupported Fields Sample

A 100-name ENS sample from live projections returned:

- `success`: 100
- unsupported `manager_address`: 14
- unsupported `owner_address`: 14

For `taytems.eth`, the live façade returned no unsupported fields after the token-id fallback and slim record-inventory path.
