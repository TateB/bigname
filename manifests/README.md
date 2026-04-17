# Repository Manifests

Checked-in source manifests live here using the layout frozen in `docs/manifests.md`:

```text
manifests/<namespace>/<source_family>/<version>.toml
```

Bootstrap seed manifests are now checked in for the first ENS and Basenames source families.

Current policy:

- active manifests should contain only authoritative contract addresses we are ready to watch
- draft manifests may reserve shape for future source families without activating intake
- manifest changes must stay within the schema frozen in `docs/manifests.md`
