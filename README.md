# mutinynet-ssp

Self-hosted Spark Service Provider (SSP) in Rust, wired to `ldk-server`.

- Serves the SSP GraphQL surface the `@buildonspark/spark-sdk` `SspClient` expects
  (`graphql/spark/rc` + `graphql/spark/2025-03-19`).
- Default build uses fake Lightning data so all SDK flows work with no funds.
  Enable live LDK with `cargo build --features ldk` (needs a running `ldk-server`).
- Works with any network via env (`REGTEST` today, MutinyNet signet at deploy).

## Run (regtest e2e)

```sh
./e2e/e2e.sh   # compose up, fund sidecar, SDK e2e (deposit, swap, transfer, quote)
curl http://127.0.0.1:5000/health
```

Point the SDK at it via `sspClientOptions` (`baseUrl` + `schemaEndpoint`
`graphql/spark/rc` + the `ssp_identity_pubkey` from `/health`).

## Deploy

See `docs/DEPLOY.md` (first-boot order, liquidity ops, secrets) plus
`deploy/` (Caddy edge, env template, LDK channel script).

## Docs

- `docs/SSP_API_COVERAGE.md` – op-by-op status.
- `docs/LDK_GAPS.md` – missing `ldk-server-client` APIs + fake-data swap map.
- `docs/SO_SETUP_MUTINYNET.md` – SO + MutinyNet deploy path.
