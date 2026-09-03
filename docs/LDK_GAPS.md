# LDK-server gaps vs Spark SSP needs — decisions (2026-09-03)

1. Fee estimator: none exists -> assume 0 fee. Estimates return 0.
2. Preimages: SSP generates its own for SSP-minted invoices (stored
   hash->preimage); wallet-supplied hashes stay wallet-owned (hodl, claim on
   reveal via `reveal_and_claim`). Implemented in `FakeLdkBackend`, maps 1:1
   to `Bolt11ReceiveForHash` / `Bolt11ClaimForHash` / `Bolt11FailForHash`.
3. Receives stay BOLT11 only (no BOLT12 hodl in ldk-server).
4. Send only inits (`Bolt11Send`); final status comes from `SubscribeEvents`
   (PaymentSuccessful/Failed) via `apply_ln_event`; wallets poll
   Transfers/UserRequest. Fake mode simulates the event with a 2s flip.
5. ldk-server points at bitcoind (regtest compose proves it: node
   `02b7ed...`, gRPC 3536, chain syncs; fee-estimate warnings are benign
   regtest fallbacks).
6. SSP subscribes to `SubscribeEvents` internally; no outbound webhooks,
   internal API only.
7. Autopilot/rebalance/liquidity automation: out of scope (manual channels).

Live-mode RPC map is in `src/ldk.rs` (`live` module, `--features ldk`).

Source: `ldk-server-ref` (`api.proto`, `client.rs`) vs
`spark-ref` SSP schema + `spark-sdk/src/graphql/client.ts`.

Live mode is behind `cargo build --features ldk`. Default build uses
`FakeLdkBackend` (fake data) so SDK flows work with no funds.

## Covered by ldk-server today

- `Bolt11Send` + `GetPaymentDetails` / `ListPayments` -> `RequestLightningSend`.
- `Bolt11ReceiveForHash` + `SubscribeEvents::PaymentClaimable` + `Bolt11ClaimForHash` /
  `Bolt11FailForHash` -> `RequestLightningReceive` hold flow.
- `DecodeInvoice` -> fee-quote decode helper.
- `OnchainReceive` / `OnchainSend` -> coop-exit payout + faucet helpers.
- `ListChannels` / `OpenChannel` / `CloseChannel` / `ConnectPeer` -> ops.

## Missing, faked in v1

1. **Fee estimator RPC** – none exists. Fake: `fee_ppm` from `SSP_LN_FEE_PPM`
   (default 2500 = 0.25%% like Lightspark). Live: `DecodeInvoice` + ppm heuristic.
   Needed upstream: `EstimateRouteFee(invoice, amount)` using pathfinding scores.
2. **Preimage lookup by hash** – `PaymentDetails` echoes preimage only for known
   payments. SSP hold flow needs lookup on SO proof path. Fake: random preimage.
   Needed: `GetPreimage(payment_hash)` or expose claimable preimage in event.
3. **BOLT12 hold / invoice state machine** – only BOLT11 has ReceiveForHash/Claim/Fail.
   Fake: BOLT12 receive returns fake string. Needed: BOLT12 hold RPCs or document
   BOLT11-only SSP v1.
4. **Outbound cancel (`abandon_payment`)** – no RPC to drop a stuck outbound before
   claim. Fake: mark SUCCEEDED instantly. Needed for timeout path in
   `RequestLightningSend`.
5. **Custom signet (MutinyNet) genesis** – `Network` enum is fixed
   (BITCOIN/TESTNET/TESTNET4/SIGNET/REGTEST); no custom genesis/challenge field.
   Workaround: point bitcoind backend at MutinyNet node + run ldk-server with
   `SIGNET` and matching bitcoind RPC; breaks if genesis differs. Needed upstream:
   `signet_genesis_hash` / `challenge` config in `ldk-server --custom-signet`.
6. **Webhooks** – only `SubscribeEvents` stream. SSP webhook CRUD is stubbed empty.
   Needed in SSP: poll task bridging `SubscribeEvents` -> stored webhook queue.
7. **Channel liquidity automation** – no autopilot/rebalance/LSPS1. Manual
   `OpenChannel` + JIT-receive only. For MutinyNet deploy, open channels by hand.

## Fake-data map (where to swap in live calls)

- `src/ldk.rs::FakeLdkBackend::pay_invoice` -> `Bolt11Send`.
- `::create_invoice` -> `Bolt11ReceiveForHash`.
- `::fee_estimate_msat` -> `DecodeInvoice` + ppm.
- `static_deposit_quote` credit_amount -> esplora/bitcoind UTXO lookup
  (signature is already real ECDSA via `sign_with_ssp`).
- `request_coop_exit` raw tx fields -> PSBT build + `OnchainSend`.
- `request_swap` swapLeaves/inbound_transfer -> SSP liquidity wallet:
  the SSP must own funded Spark leaves and send a real SO transfer to the
  user; the SDK rejects empty `swapLeaves` and unknown inbound ids by design.

## E2E regtest status (2026-09-03, `docker-compose.regtest.yml` + `e2e/e2e.mjs`)

PASS through the real JS SDK against local SOs: wallet init, L1 fund +
mine, claim deposit (100000 sats), full-balance Spark transfer A->B with
background claim, static-deposit quote with SSP signature verified
cryptographically against the SSP identity pubkey. Partial transfers that
need an SSP leaf swap fail as the SDK requires (gap above).
