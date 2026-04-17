# Consumer Capability Baseline

Status: Phase 0 baseline

This document is the checked-in replacement contract for first-party consumers until the apps monorepo is imported and mapped call-site by call-site.

## 1. Capability Groups

| Capability | Example consumer surface | Native `v1` responsibility |
| --- | --- | --- |
| exact name profile | profile pages, record editing, registration views | `Name.registration` + `Resolution` |
| names owned or controlled by address | dashboards and search flows | `Address.names` |
| names with role summary | dashboard lists | `Address.names` with `include=role_summary` |
| declared child subnames and counts | subname pages and creation flows | `Name.children` |
| record inventory for editing | profile and records screens | `Resolution.record_inventory` |
| verified record reads | profile, send, and address-resolution flows | `Resolution.verified_queries` |
| name history | profile history pages | `History(scope=both)` |
| address history across names | address activity views | `Address.history` |
| role holders for a resource | roles pages | `Permissions.by_resource` |
| role change history | roles history pages | `History(filter=permissions)` |
| resolver-centric overview | resolver pages | `Resolver` |
| claimed vs verified primary name | dashboard and profile | `PrimaryName` |

## 2. Current Status

- this is the working baseline for Phase 0
- when the apps monorepo is imported, add app-by-app call-site mappings rather than replacing this table
- any capability required by a first-party consumer that is not covered here must be added here before code claims parity

## 3. Exit Condition For Consumer Cutover

Before first-party cutover:

- each capability must map to one or more concrete app call sites
- each capability must have contract tests
- each capability must have rollout and rollback criteria
