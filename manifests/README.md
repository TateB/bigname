# Repository Manifests

Checked-in source manifests live here using the layout frozen in `docs/manifests.md`:

```text
manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml
```

Runtime profile roots are the profile directories, not the top-level `manifests/` directory:

```text
manifests/mainnet/ethereum/<namespace>/<source_family>/v1.toml
manifests/mainnet/base/<namespace>/<source_family>/v1.toml
manifests/sepolia/ethereum/<namespace>/<source_family>/v1.toml
manifests/sepolia/base/<namespace>/<source_family>/v1.toml
```

Bootstrap seed manifests are checked in for the first ENS, ENSv2 Sepolia, and Basenames source families.

Current policy:

- active manifests should contain only authoritative contract addresses we are ready to watch
- draft manifests may reserve shape for future source families without activating intake
- manifest changes must stay within the schema frozen in `docs/manifests.md`
- a runtime selects exactly one manifest profile root; `manifests/mainnet/` and `manifests/sepolia/` must not be loaded into the same canonical corpus, watch plan, discovery graph, or projection set
