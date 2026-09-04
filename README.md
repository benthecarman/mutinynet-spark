# mutinynet-ssp

`mutinynet-ssp` is a self-hosted Spark Service Provider written in Rust. One
process provides the SSP GraphQL API, owns the Spark liquidity wallet, fills
Swap V3 requests, and settles Lightning payments through `ldk-server`.

The service uses the Breez Spark Rust SDK for its embedded wallet. Spark
Operators remain separate services and use the existing Spark protocol. The
operator build must expose the authenticated counter-swap RPC used by the SSP.

## Supported flows

- Wallet challenge authentication and durable 24-hour sessions.
- Partial Spark transfers through atomic Swap V3 counter transfers.
- BOLT11 Lightning sends backed by a verified Spark preimage-swap transfer.
- BOLT11 Lightning receives with wallet-created preimage shares, an atomic
  operator swap, and a Spark payout before the Lightning claim.
- Durable payment state, event-stream reconnect, and payment reconciliation.
- Authenticated Spark liquidity deposits and exact-leaf funding.

Static-deposit quotes are test-only on regtest. Cooperative exits, instant
static deposits, receive quotes, request history pagination, and wallet
webhooks are not production-complete. See
[SSP API coverage](docs/SSP_API_COVERAGE.md) for the exact operation status.

## HTTP endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Basic process health (`{ "status": "ok" }`) |
| `GET /identity` | Public SSP identity discovery |
| `GET /status` | Spark wallet, liquidity, and LDK status (admin bearer token required) |
| `POST /graphql/spark/rc` | Current Spark SDK GraphQL endpoint |
| `POST /graphql/spark/2025-03-19` | Dated Spark SDK endpoint |
| `POST /graphql` | GraphQL compatibility alias |
| `POST /admin/spark/deposit-address` | Create a Spark deposit address |
| `POST /admin/spark/claim-deposit` | Claim a confirmed deposit output |

The status and admin endpoints require
`Authorization: Bearer <SPARK_ADMIN_TOKEN>`.

## Client configuration

Read `identityPublicKey` from `/identity` and use it as the SSP identity:

```json
{
  "baseUrl": "https://ssp.example.com",
  "schemaEndpoint": "graphql/spark/rc",
  "identityPublicKey": "<identityPublicKey>"
}
```

The wallet network, operator set, and SSP must use the same Bitcoin network.
For MutinyNet, use `SIGNET`. The pinned JavaScript SDK fork fixes SIGNET
network mapping and accepts the shared TESTNET/SIGNET `lntb` invoice prefix.

## Local end-to-end test

The Lightning acceptance stack contains bitcoind, a local Electrs Esplora
service, PostgreSQL, three Spark Operators, two `ldk-server` nodes, two SSP
instances, and two Breez SDK wallets. It needs Docker Compose, Rust, and
checkouts of the pinned Spark and `ldk-server` revisions. The
revisions are listed in
[the fixture README](e2e/upstream/README.md).

With the two checkouts at their default paths, run:

```sh
./e2e/ln-e2e.sh
```

The script always creates a separate Compose project with empty volumes. It
funds exact SSP leaves, creates bidirectional Lightning liquidity, bootstraps
both Breez wallets through standard BOLT11 receives, and then makes two Breez
payments in opposite directions. It verifies each Spark balance, each Breez
payment record, both LDK payment records, and the received preimages.

Run `./e2e/e2e.sh` for the supplemental API, idempotency, failure, reconnect,
restart, concurrency, and shutdown checks.

## Deployment

Use [the deployment runbook](docs/DEPLOY.md) for service requirements,
configuration, startup, liquidity, backups, and monitoring. Current Lightning
backend limits are in [ldk-server compatibility](docs/LDK_GAPS.md). The
`deploy/` directory contains a local Caddy edge example and an LDK command
helper. It is not a production topology. The production Compose definition is
in [MutinyWallet/mutiny-net](https://github.com/MutinyWallet/mutiny-net).
