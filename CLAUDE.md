# fire_duck_ext — project instructions

## Keep `description.yml` in sync with user-facing changes

`description.yml` is the DuckDB community-extensions metadata (the public listing:
`hello_world` examples, `extended_description`, auth/feature docs). Whenever a
**user-facing** change is made to the extension, review and update `description.yml`
to match. User-facing changes include:

- new or changed table/scalar functions or `CALL` procedures,
- new or changed secret parameters or authentication modes,
- changed defaults or behavior (e.g. `show_missing`, pushdown rules),
- new platform support (e.g. WebAssembly),
- new type mappings.

Keep `description.yml` consistent with `README.md`. Bump `extension.version` and
update `repo.ref` as part of the release/submission, not on every change.
