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

## Interpretation

The optimized façade is substantially faster and supports 1000-input batches, but full shared-record batch responses still do not meet a 10 ms p95 target. The remaining cost is dominated by the partner-required full `NameRecord` payload shape, JSON serialization, and response size. A 10 ms p95 feed path at hundreds of addresses likely needs a negotiated compact feed DTO or a dedicated materialized response projection with a smaller response shape.

## Unsupported Fields Sample

A 100-name ENS sample from live projections returned:

- `success`: 100
- unsupported `manager_address`: 14
- unsupported `owner_address`: 14

For `taytems.eth`, the live façade returned no unsupported fields after the token-id fallback and slim record-inventory path.
