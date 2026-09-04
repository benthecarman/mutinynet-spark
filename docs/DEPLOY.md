# Deployment runbook

This runbook defines the service contract for a production SSP deployment.
Keep host names, secrets, volumes, and image policy in the deployment
repository.

## Required services

- A Bitcoin node for the selected network.
- At least two Spark Operators and their signers.
- PostgreSQL databases used by the Spark Operators.
- `ldk-server` with usable inbound and outbound channel capacity.
- `mutinynet-ssp` with persistent storage mounted at `/data`.

The Spark Operator build must include the authenticated Swap V3 counter RPC.
The SSP calls existing operator consensus code through this RPC.

## Configuration

| Variable | Requirement |
|---|---|
| `SSP_NETWORK` | `REGTEST`, `SIGNET`, `TESTNET`, or `MAINNET`; must match every dependency |
| `SSP_LISTEN_ADDR` | Listen address; use `0.0.0.0:5000` in a container |
| `SSP_PUBLIC_URL` | SSP URL reachable from the embedded wallet |
| `SSP_DATA_DIR` | SQLite directory; use persistent storage |
| `SPARK_MNEMONIC_FILE` | Persistent BIP39 mnemonic path |
| `SPARK_MNEMONIC_REQUIRED` | Set to `1` after the wallet file exists |
| `SSP_IDENTITY_PUBKEY` | Optional guard for the identity derived from the mnemonic |
| `SO_HOSTS` | Ordered, comma-separated operator gRPC addresses |
| `SO_IDENTITY_PUBKEYS` | Ordered operator identity keys; must match `SO_HOSTS` |
| `SO_CERT_FILES` | Empty for public trust, or one ordered certificate file per operator |
| `SSP_FROST_OPERATORS` | JSON operator IDs, identifiers, and identity keys for receive shares |
| `SSP_FROST_THRESHOLD` | Operator signing threshold |
| `SPARK_ADMIN_TOKEN` | Bearer token for the liquidity endpoints |
| `LDK_GRPC_ADDR` | `ldk-server` gRPC address without a URL scheme |
| `LDK_API_KEY` | Hex API key; use this or `LDK_API_KEY_FILE` |
| `LDK_API_KEY_FILE` | Mounted raw `ldk-server` API-key file |
| `LDK_TLS_CERT_FILE` | Mounted `ldk-server` TLS certificate |
| `SSP_SWAP_FEE_SATS` | Flat leaf-swap fee |
| `MAX_SWAP_TOTAL_SATS` | Maximum value accepted by one swap; `0` removes the cap |
| `SSP_CORS_ORIGINS` | Optional comma-separated browser origins |
| `RUST_LOG` | Optional tracing filter; use `info` unless more detail is needed |

Production must not set `SPARK_ADMIN_ALLOW_NO_AUTH=1` or
`SSP_ALLOW_FAKE_LN=1`.

`SSP_FROST_OPERATORS` has this shape:

```json
[
  {
    "id": 0,
    "identifier": "<64-character operator identifier>",
    "identityPublicKey": "<compressed operator identity key>"
  }
]
```

Operator IDs must be contiguous and start at zero. Each identifier is the
64-character hexadecimal form of `id + 1`. The number of entries must be at
least the threshold, and every identity key must match its operator.

## Wallet initialization

For a new empty volume, start one SSP instance with
`SPARK_MNEMONIC_REQUIRED=0`. The process creates the mnemonic with mode `0600`.
Back up the file, stop the process, set `SPARK_MNEMONIC_REQUIRED=1`, and start
the service normally.

Never start two SSP instances against an empty shared mnemonic path. The
mnemonic controls the SSP identity and all Spark liquidity.

## TLS checks

Each private Spark Operator certificate must be a server certificate. It must
not be a CA certificate. Check every mounted certificate before SSP startup:

```sh
openssl x509 -in /path/to/server.crt -noout -ext basicConstraints
```

The result must contain `CA:FALSE`. `SO_CERT_FILES` must use the certificates
that belong to the currently running operator instances.

## Startup

1. Start Bitcoin, PostgreSQL, the Spark Operators, and their signers.
2. Wait for every operator to have signing keyshares and stable TLS files.
3. Start `ldk-server` and verify its node information and channels.
4. Start the SSP and wait for `/health`.
5. Query authenticated `/status` and verify that `spark_error` is `null` and
   `ldk_mode` is `live`.
6. Verify that `/identity`'s `identityPublicKey` equals `/status`'s
   `ssp_identity_pubkey`, `spark.identity_pubkey`, and the configured client
   identity.
7. Fund the exact Spark leaf denominations needed by the service.

Start or recreate the SSP only after operator certificates are ready. This
prevents the wallet from keeping connections to replaced certificates.

## Liquidity

Swap fills and Lightning receives need exact SSP-owned Spark leaves. Monitor
these authenticated `/status` fields:

- `spark.available_sats`: spendable leaves visible on the operators.
- `spark.owned_sats`: all wallet-owned leaves, including temporarily missing
  leaves.
- `spark.needs_topup`: `true` after an exact-match or balance failure.

The funding helper obtains deposit addresses from the authenticated admin API,
funds them through Bitcoin RPC, waits for confirmation, and submits each raw
transaction to the SSP:

```sh
SPARK_ADMIN_TOKEN=<token> \
FUND_LADDER=1000,2000,4000,8000 \
FUND_MULTIPLICITY=12 \
node e2e/fund-ssp.mjs
```

Use `SSP_BASE_URL`, `BITCOIN_RPC_URL`, `BITCOIN_RPC_USER`, and
`BITCOIN_RPC_PASSWORD` when the services are not at the local defaults. Do not
put quotes inside values passed through Docker's `--env-file` option.

Keep `MAX_SWAP_TOTAL_SATS` below the amount of liquidity that the operator can
safely expose. Add small denominations before `needs_topup` becomes true.

## Lightning

Lightning receives need inbound channel capacity. Lightning sends need
outbound capacity. The SSP keeps the server-streaming event RPC open without a
unary deadline, reconnects with capped exponential backoff, and reconciles
payment state every 30 seconds.

Use `/health` for basic process liveness. Treat authenticated `/status` as
ready only when `ldk_mode` is `live` and `ldk_node_id` is the expected node.
Alert on repeated event-stream reconnects or reconciliation errors.

See [ldk-server compatibility](LDK_GAPS.md) for supported calls and current
backend gaps.

## Upgrade and rollback

1. Back up the mnemonic file and the complete `SSP_DATA_DIR` volume.
2. Pull the selected SSP image.
3. Recreate only the SSP service and wait for its health check.
4. Check identity, Spark balance, and LDK mode before client traffic resumes.

The process handles `SIGTERM` and closes cleanly. Do not delete or replace the
data volume during an image rollback.

## Secrets and backups

- Do not commit the Spark mnemonic, `SPARK_ADMIN_TOKEN`, or LDK API key.
- Restrict `/status` and the admin endpoints at the network edge as well as
  with the bearer token.
- Stop the SSP or use a SQLite-aware backup before copying its data directory.
- Back up `spark.mnemonic` and the complete SQLite data together.
- Test restore procedures with a wallet identity check before adding funds.
