# LDK-server gaps vs Spark SSP needs — decisions (2026-09-03)

1. Fee estimator: none exists -> assume 0 fee. Estimates return 0.
2. Preimages: receives use hodl invoices. The SSP mints and stores the
   preimage first, then creates one invoice for that hash. On an LDK event,
   it sends an idempotent Spark payout to the user before it claims the
   Lightning HTLC. It reconciles both legs after stream gaps.
3. Receives stay BOLT11 only (no BOLT12 hodl in ldk-server). Sends accept
   BOLT11 and BOLT12 offers (`lno1…` routes to `bolt12_send`).
4. Send only inits (`Bolt11Send`); final status comes from `SubscribeEvents`
   (PaymentSuccessful/Failed) via `apply_ln_event`; wallets poll
   Transfers/UserRequest. Fake mode simulates the event with a 2s flip.
5. ldk-server points at bitcoind (regtest compose proves it: node
   `02b7ed...`, gRPC 3536, chain syncs; fee-estimate warnings are benign
   regtest fallbacks).
6. SSP subscribes to `SubscribeEvents` internally; no outbound webhooks,
   internal API only. The streaming RPC has no unary deadline. The SSP
   reconnects it with capped exponential backoff and reconciles payment state.
7. Autopilot/rebalance/liquidity automation: out of scope (manual channels).

The live-mode RPC map is in `src/ldk.rs`. Live mode is selected at run time.

Source: `ldk-server-ref` (`api.proto`, `client.rs`) vs
`spark-ref` SSP schema + `spark-sdk/src/graphql/client.ts`.

Set `LDK_GRPC_ADDR`, `LDK_API_KEY` or `LDK_API_KEY_FILE`, and
`LDK_TLS_CERT_FILE` for live mode. Fake mode is development-only and requires
`SSP_ALLOW_FAKE_LN=1`.

## Covered by ldk-server today

- `Bolt11Send` + `GetPaymentDetails` / `ListPayments` -> `RequestLightningSend`.
- `Bolt11ReceiveForHash` + `SubscribeEvents::PaymentClaimable` + `Bolt11ClaimForHash` /
  `Bolt11FailForHash` -> `RequestLightningReceive` hold flow.
- `DecodeInvoice` -> fee-quote decode helper.
- `OnchainReceive` / `OnchainSend` -> coop-exit payout + faucet helpers.
- `ListChannels` / `OpenChannel` / `CloseChannel` / `ConnectPeer` -> ops.

## Missing, faked in v1

1. **Fee estimator RPC** – none exists. Both backends return 0 by policy.
   Needed upstream: `EstimateRouteFee(invoice, amount)` using pathfinding scores.
2. **Preimage lookup by hash** – `PaymentDetails` echoes preimage only for known
   payments. The SSP hold flow uses its own persistent preimage store.
   Needed: `GetPreimage(payment_hash)` or expose claimable preimage in event.
3. **BOLT12 hold / invoice state machine** – only BOLT11 has
   ReceiveForHash/Claim/Fail. Receives are BOLT11-only.
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

## E2E regtest status (2026-09-03)

PASS through the real JS SDK against local SOs: wallet init, L1 fund +
mine, claim deposit (100000 sats), full-balance Spark transfer A->B with
background claim, and a static-deposit quote with a non-empty SSP signature.
The Lightning suite also proves public SDK send and receive, exact Spark debit
and credit, funding proof, idempotency, expiry, a 95-second idle stream, LDK
restart recovery, missed-event reconciliation after an SSP restart, and
concurrent receives. The test does not independently verify the static quote
signature.
