# Deploy runbook

## First boot

1. Start bitcoind, the Spark operators, and `ldk-server`.
2. Set `SO_HOSTS`, `SO_IDENTITY_PUBKEYS`, and `SO_CERT_FILES` for the same
   ordered operator set. Set `SSP_FROST_THRESHOLD` to its signing threshold.
3. Mount durable SSP storage at `/data` and set `SPARK_MNEMONIC_FILE` to
   `/data/spark.mnemonic`. For an upgrade, copy the old `sidecar.mnemonic`
   into that location before startup. This keeps the SSP identity and all
   funded leaves. Set `SPARK_MNEMONIC_REQUIRED=1` after the file exists.
4. Set `SPARK_ADMIN_TOKEN`, start the SSP, and read its identity from
   `/health`. Put that key in client `sspClientOptions.identityPublicKey`.
5. Fund exact Spark leaves through the authenticated endpoints:
   `POST /admin/spark/deposit-address`, then
   `POST /admin/spark/claim-deposit` with the confirmed transaction hex and
   output index. The regtest helper `node e2e/fund-ssp.mjs` does both steps.
6. Fund `ldk-server`, connect peers, and open channels. Lightning receives
   need inbound capacity. Lightning sends need outbound capacity.

The operator image must include the small public Swap V3 counter RPC wrapper
until buildonspark/spark issue 150 is complete. The wrapper calls the existing
operator consensus code. It does not change the Spark protocol.

## Liquidity operations

- Swap fills use exact denominations and never start a recursive SSP swap.
  An exact-match failure returns `NEEDS_TOPUP` before the SSP spends leaves.
- Lightning receives also spend SSP leaves. Keep exact leaves for common
  invoice amounts.
- Monitor `spark.available_sats`, `spark.owned_sats`, and
  `spark.needs_topup` in `/health`.
- Add small denominations more often than large denominations. For regtest:
  `FUND_LADDER=1000,2000,4000,8000 FUND_MULTIPLICITY=12
  node e2e/fund-ssp.mjs`.
- Keep `MAX_SWAP_TOTAL_SATS` set to a safe value for the available liquidity.

## Secrets and backups

- Do not commit `SPARK_ADMIN_TOKEN`, `LDK_API_KEY`, or the BIP39 mnemonic.
- Back up the mnemonic and the SSP SQLite volume. The mnemonic controls the
  SSP identity and Spark funds.
- The admin token protects endpoints that can create and claim deposits. Do
  not expose them without authentication.

## Health checks

The container uses `mutinynet-ssp healthcheck`. It does not need curl, wget,
Node.js, or Python. `/health` reports the embedded wallet identity and balance,
plus the live or fake Lightning mode.

## Residual risks

- `ldk-server` must support the deployed custom signet. The SSP refuses fake
  Lightning unless `SSP_ALLOW_FAKE_LN=1` is explicit.
- Leaf lifetimes are block based. Keep the SSP online so the embedded wallet
  can sync and claim transfers.
- Wallet webhooks are not implemented. Clients must poll transfer state.
