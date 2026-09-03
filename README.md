# mutinynet-ssp

Self-hosted Spark Service Provider (SSP) in Rust, wired to `ldk-server`.

- Serves the SSP GraphQL surface the `@buildonspark/spark-sdk` `SspClient` expects
  (`graphql/spark/rc` + `graphql/spark/2025-03-19`).
- Live Lightning is selected at run time when `LDK_GRPC_ADDR`, an API key,
  and the TLS certificate are configured. Fake Lightning requires the explicit
  development-only setting `SSP_ALLOW_FAKE_LN=1`.
- Works with any network via env (`REGTEST` today, MutinyNet signet at deploy).

## Run (regtest e2e)

```sh
./e2e/e2e.sh   # compose up, fund sidecar, SDK e2e (deposit, swap, transfer, quote)
curl http://127.0.0.1:5000/health
```

Point the SDK at it via `sspClientOptions` (`baseUrl` + `schemaEndpoint`
`graphql/spark/rc` + the `ssp_identity_pubkey` from `/health`).

The current Lightning receive quote uses a JSON compatibility manifest. The
stock SDK expects a protobuf `TransferManifest`, so use the raw GraphQL receive
path until the protobuf serializer and signing digest are implemented.

## Deploy

See `docs/DEPLOY.md` (first-boot order, liquidity ops, secrets) plus
`deploy/` (Caddy edge, env template, LDK channel script).

## Docs

- `docs/SSP_API_COVERAGE.md` – op-by-op status.
- `docs/LDK_GAPS.md` – missing `ldk-server-client` APIs + fake-data swap map.
- `docs/SO_SETUP_MUTINYNET.md` – SO + MutinyNet deploy path.
