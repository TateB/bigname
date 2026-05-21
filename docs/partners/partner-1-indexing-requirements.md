# ENS Indexing Requirements for partner-1

**Source date:** 2026-05-06
**Partner alias:** `partner-1`
**Audience:** engineering agent or implementation planner
**Purpose:** scope whether an ENS Labs hosted indexer can satisfy partner-1's indexed name-data requirements across L1 ENS, ENSv2, and Basenames.

This document is a partner-supplied requirements snapshot. Protocol-like names, event names, coin-type examples, and coverage examples should be treated as partner input, not bigname support claims or upstream protocol claims, unless a statement carries an explicit pinned `.refs/` citation.

## Agent task

Evaluate the current ENS Labs indexing/API stack against this requirements document and produce an implementation plan that separates:

1. already-supported functionality,
2. small API/schema extensions,
3. net-new build work,
4. unresolved product or protocol questions.

The output should be concrete enough for engineering planning: list affected services, expected endpoint/schema changes, indexing gaps, data-model changes, migration/shadow-testing support, and any latency or operational risks.

## Context

partner-1 currently uses an internal indexing service to serve ENS and Basenames data to internal consumers. partner-1 wants to explore whether an ENS Labs hosted indexer can fully cover its needs across:

- L1 ENS canonical names on Ethereum Mainnet,
- ENSv2 names with onchain records,
- Basenames on Base Mainnet.

If ENS Labs can cover the required surface, partner-1 intends to retire its internal indexing service and use ENS as the canonical source for indexed name-data reads.

This document covers only indexing-backed flows. Live resolution for payment flows in partner-1 Exchange and the Base App is handled separately through the ENS Universal Resolver path being landed in `go-ens` and `ens-resolver`.

## Non-goals

Do not scope partner-specific identity composition into the ENS indexer. For example, partner-1 may choose a default identity when a user has identities from multiple sources, but that composition belongs in the partner-1 shim layer, not in the ENS API.

Do not assume partner-1 will maintain a cache or hot standby in front of ENS. ENS is intended to be the sole source for indexed reads. During an ENS outage, identity surfaces should fall back to live resolution through the Universal Resolver path.

## Use cases

partner-1 Base App and adjacent partner-1 products use indexed ENS and Basenames data for the following flows.

### 1. Identity rendering in feeds and timelines

Render human-readable names instead of addresses in social feeds, transaction lists, and profile cards.

Requirements:

- high volume,
- latency sensitive,
- target p95 latency: under 10 ms from AWS `us-east` vantage points.

### 2. Profile aggregation per user

List every name an address owns or manages, including the primary name from reverse resolution.

Primary consumers:

- profile pages,
- account switchers,
- "names I own" surfaces.

### 3. Per-name detail lookup

When a user clicks a specific name, fetch its full indexed record for display.

Required details include:

- text records,
- primary address,
- expiration,
- owner,
- manager.

### 4. Batched identity resolution

Resolve identities for many addresses in one round trip, typically for list or feed rendering.

Requirements:

- support tens to hundreds of addresses per feed render,
- support up to 1000 inputs per batch unless ENS chooses a different batch ceiling,
- preserve per-input grouping in batched reverse responses.

## Required namespace and chain coverage

This table records partner-1's requested product coverage and wording. It is not an upstream protocol assertion, a bigname support claim, or evidence that the corresponding source families are admitted in this branch.

| Domain | Required | Notes |
| --- | --- | --- |
| L1 ENS canonical names | Yes | partner-1 requests Ethereum Mainnet coverage. |
| ENSv2 names with onchain records | Yes | partner-1 requests coverage for records on L1 or on ENSv2 L2 destination chains once those source families are admitted. |
| Basenames | Yes | partner-1 requests Base Mainnet coverage. |

The caller should not need to fan out by namespace. A single API call should be able to span L1 ENS, ENSv2, and Basenames where applicable.

## Traffic and latency targets

| Metric | Target / observed value |
| --- | --- |
| p95 latency | Under 10 ms for latency-sensitive identity rendering, measured from AWS `us-east` vantage points. |
| Nominal throughput | Roughly 200 requests per second. |
| Burst throughput | Historical bursts have approached 3x nominal throughput. |
| Batch ceiling | partner-1 currently supports 1000 inputs per batch; ENS may define a different ceiling. |

The agent should explicitly assess whether the current ENS API/indexer architecture can meet these targets without a partner-side cache. The 10 ms p95 target should be interpreted as a regional/server-side target from AWS `us-east`, not as a global end-user latency target.

## API requirements

The required surface consists of two functional primitives, each with single and batched variants:

1. forward resolution: name to indexed record,
2. reverse resolution: address plus coin type to indexed names.

The transport is not prescribed. REST or GraphQL is acceptable. The operation semantics and fields are the requirement.

## Shared record shape

Forward resolution returns this record shape. Reverse resolution returns the same shape with additional reverse-specific metadata.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | `string` | Yes | Full human-readable name. Example: `vitalik.eth`, `someone.base.eth`. |
| `namehash` | `bytes32` / hex string | Yes | ENS namehash. |
| `owner_address` | `string` | Yes | Wallet address holding the NFT. |
| `manager_address` | `string` | Yes | Address with manager/controller permissions. |
| `primary_address` | `string` | Yes | Reverse-record target for the relevant name. |
| `coin_type_addresses` | `map[uint64]bytes` | Yes | partner-1 expects a map from coin type to address bytes. |
| `text_records` | `map[string]string` | Yes | All currently set text records, such as `avatar`, `description`, `url`, etc. |
| `resolver_address` | `string` | Yes | Active resolver contract. |
| `expiration` | Unix timestamp seconds | Yes | Expiration timestamp. |
| `token_id` | `uint256` | Yes where applicable | NFT token ID for wrapped names and Basenames ERC-721 names. |
| `network` | `string` | Yes | Chain where the canonical record lives, such as `ethereum`, `base`, etc. |

## Primitive 1: forward resolution

Resolve a fully qualified name to its indexed record.

### Input

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | `string` | Yes | Fully qualified name, for example `vitalik.eth` or `someone.base.eth`. |

### Response

Return one shared name record.

### Primary use cases

- name detail page,
- profile rendering after click-through,
- fetching many name details through the batched form.

## Primitive 2: reverse resolution

Resolve an address and coin type to indexed names.

partner-1 requires reverse resolution to be scoped by coin type. The same address may have different primary-name results per requested coin type in partner-1's expected model, so `coin_type` is required.

### Input

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `address` | `string` | Yes | EVM address. |
| `coin_type` | `uint64` | Yes | Coin type that scopes reverse resolution in partner-1's expected model. Examples supplied by partner-1 include `60` for ETH mainnet and `2147492101` for Base. |
| `roles` | enum | No | Filter by `OWNED`, `MANAGED`, or both. |
| `page_size` | `uint` | No | Pagination size. |
| `page_cursor` | `string` | No | Pagination cursor. |

### Response

Return a paginated list of shared name records. Each record must also include:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `is_primary` | `bool` | Yes | True when the row matches partner-1's requested primary-name result for the requested coin type. |

Pagination metadata:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `next_page_cursor` | `string` | Yes when more results exist | Cursor for the next page. |
| `total_count` | `uint` | Preferred | Total result count. |
| `has_more` | `bool` | Yes | Whether additional pages exist. |

ENS may either hoist the primary name to index `0` or only mark it with `is_primary`; the `is_primary` marker is the requirement.

### Primary use cases

- profile aggregation per user,
- account switcher,
- owned/managed name surfaces,
- batched identity rendering for address lists.

## Batched variants

### Batched forward resolution

Input:

```ts
type ForwardBatchInput = {
  names: string[];
};
```

Output:

```ts
type ForwardBatchOutput = {
  records: NameRecord[];
};
```

Requirements:

- input order should be preserved where practical,
- not-found behavior must be defined consistently,
- suitable for rendering many name details at once.

### Batched reverse resolution

Input:

```ts
type ReverseBatchInput = {
  inputs: Array<{
    address: string;
    coin_type: number;
    roles?: "OWNED" | "MANAGED" | "BOTH";
    page_size?: number;
    page_cursor?: string;
  }>;
};
```

Output:

```ts
type ReverseBatchOutput = {
  results: Array<{
    input: {
      address: string;
      coin_type: number;
    };
    records: ReverseNameRecord[];
    pagination: {
      next_page_cursor?: string;
      total_count?: number;
      has_more: boolean;
    };
  }>;
};
```

Requirements:

- preserve grouping per input,
- require `coin_type` per input, not per batch,
- support feed-render scenarios where most inputs use the same coin type,
- define or negotiate a batch ceiling; partner-1 currently uses a 1000-input cap.

## Behavior requirements to confirm

The agent should verify whether the current ENS implementation matches these behaviors. If behavior differs, document the difference and propose a compatibility strategy.

### Empty vs. not-found

partner-1 currently returns empty responses for not-registered cases, such as empty lists or empty records, rather than typed errors.

Confirm whether ENS should:

- preserve empty-response semantics,
- return typed not-found errors,
- support an adapter-compatible mode in the partner-1 shim.

### Indexing lag and staleness metadata

Confirm block-to-served-record lag per chain.

Determine whether ENS can expose either:

- a staleness guarantee,
- `as_of_block` metadata,
- `as_of_timestamp` metadata,
- per-chain indexing heads in health/status endpoints.

### Dynamic resolver detection

Confirm whether the indexer automatically detects resolver changes to contracts that were not previously indexed.

Required behavior:

- when a name switches to a new resolver contract, the indexer should detect it,
- the indexer should begin ingesting relevant resolver events, including `TextChanged` and `AddressChanged`,
- the resulting records should not remain stale because the resolver was not in a preconfigured allowlist.

This is a known source of staleness in partner-1's current implementation.

### Cross-namespace query shape

Confirm whether a single API call can span:

- L1 ENS,
- Basenames,
- ENSv2 onchain records on L1 and partner-1-requested L2 destinations.

The desired behavior is no per-namespace fan-out by the caller for either forward or reverse resolution.

## Migration working model

If ENS can satisfy the required surface, partner-1 expects this migration model.

### Phase 1: shim

partner-1 stands up a thin partner-owned shim that translates its existing internal contract into ENS API calls.

Internal consumers should not need to change at cutover.

### Phase 2: shadow comparison

Run a production shadow-comparison harness:

1. shim queries both ENS and partner-1's internal indexing service,
2. shim returns partner-1's existing internal result,
3. shim logs per-operation diffs,
4. consuming teams validate diff classes against predefined tolerances.

### Phase 3: cutover

Flip primary reads to ENS per operation, soak, and then decommission partner-1's internal indexing service.

## Open questions for ENS Labs

The agent should answer or route these questions.

1. What is the current ENS API surface? Provide the GraphQL schema or REST endpoint catalog.
2. What namespace and chain coverage already exists?
3. What is net-new for this scope, especially Basenames and ENSv2 onchain records on partner-1-requested L2 destinations?
4. Can forward and reverse queries span L1 ENS, Basenames, and ENSv2 without caller-side fan-out?
5. What is the buildout timeline for missing functionality?
6. Should the API be exposed as a public good, and if so, what operational/SLA boundaries are needed?

## Agent deliverables

Produce a concise implementation assessment with the following sections.

### 1. Coverage matrix

For each required field and behavior, mark:

- supported now,
- partially supported,
- missing,
- unknown.

Include source files, services, endpoints, schemas, and indexer components where relevant.

### 2. API proposal

If current APIs do not directly satisfy the requirements, propose the smallest REST or GraphQL surface that does.

Include:

- endpoint or operation names,
- request shapes,
- response shapes,
- pagination behavior,
- not-found behavior,
- batch limits,
- staleness metadata.

### 3. Indexing gap analysis

Call out required changes for:

- L1 ENS,
- ENSv2,
- Basenames,
- resolver event indexing,
- reverse resolution by coin type,
- ownership vs management roles,
- expiration and token ID extraction,
- cross-chain/cross-namespace canonicalization.

### 4. Operational risk assessment

Assess whether ENS can meet:

- p95 under 10 ms from AWS `us-east`,
- roughly 200 rps nominal,
- burst traffic near 3x nominal,
- no partner-side cache,
- acceptable indexing lag per chain.

### 5. Migration support plan

Describe how ENS can support partner-1's shim and shadow-comparison rollout.

Include:

- diff-friendly response determinism,
- stable ordering rules,
- explicit missing/null/empty semantics,
- per-response staleness metadata,
- logging or trace IDs useful for comparing ENS vs partner-1 internal results.

## Acceptance criteria

The requirement is satisfied when an agent can produce a scoped engineering plan that answers:

1. whether the current ENS API/indexer can replace partner-1's internal indexed name-data service,
2. what needs to be built or changed,
3. how the migration can be validated safely through shadow comparison,
4. whether latency, throughput, staleness, and cross-namespace requirements are realistic without a partner-side cache.
