# ldk-server compatibility

The SSP uses `ldk-server-client` for Lightning and keeps all Spark settlement
state in the SSP. This document lists the current integration boundary and
verified limitations.

## Supported production path

### BOLT11 send

The SSP verifies that the wallet funded the payment with a matching Spark
preimage-swap transfer. It then calls `Bolt11Send`. Payment success or failure
comes from `SubscribeEvents` and is also recovered through
`GetPaymentDetails` and `ListPayments`. On success, the SSP gives the Lightning
preimage to the Spark transfer.

Retries with the same wallet, invoice, transfer ID, and idempotency key return
the stored request and do not start a second Lightning payment.

If the encoded BOLT11 invoice exactly matches an open receive request created
by the same SSP, internal settlement is available only for the explicit
SSP-owned HODL extension, where the SSP already holds the preimage. The SSP
claims the payer's Spark funding before paying the receiver. A persistent
reservation prevents a competing Lightning payment from claiming the same
receive. Pending settlements resume after a retry or process restart;
expired or returned funding produces a failed send.

Standard wallet-created invoices from the same SSP are rejected during fee
estimation and send validation. Recovering their preimage requires an
irreversible receiver payout, while the payer's funding can expire before it
is claimed. Supporting these invoices safely requires an operator operation
that commits both transfers atomically. They can still be paid from an
external Lightning wallet. Existing pending internal requests without a
reservation cannot reuse a receive that has already started or completed.

### BOLT11 receive

The wallet creates the preimage and stores encrypted threshold shares with the
Spark Operators through the existing Spark SDK receive flow. The SSP calls
`Bolt11ReceiveForHash` for that hash and waits for `PaymentClaimable`. It then
prepares an SSP-to-wallet transfer and calls `InitiatePreimageSwapV3` with
`REASON_RECEIVE`. The operators commit the transfer and return the reconstructed
preimage. The SSP verifies the preimage hash before it calls
`Bolt11ClaimForHash`.

The Spark commit and returned preimage are stored before the Lightning claim.
Retries and process restarts therefore do not create a second Spark transfer.
The SSP calls `Bolt11FailForHash` when an unfunded swap cannot complete or the
hold invoice expires.

### BOLT12 send

The SSP verifies a completed standard Spark transfer from the wallet and then
calls `Bolt12Send`. It verifies the final payment hash and preimage. A final
Lightning failure starts a deterministic Spark refund. Reconciliation can
repeat the refund without creating a second transfer.

This is a prepaid flow, not an atomic swap. BOLT12 offers do not expose the
payment hash before the invoice-request exchange, so the wallet cannot create
the BOLT11 hash-locked funding transfer.

### BOLT12 receive

The SSP calls `Bolt12Receive` to create a fixed-value offer. After
`PaymentReceived`, it sends the offer amount to the wallet with a deterministic
transfer ID. Reconciliation can repeat the payout safely after a restart.

`ldk-server` claims the payment before the SSP makes the Spark transfer. This
is not an atomic receive. The limitation comes from the missing BOLT12 hold
API described below.

### Event recovery

The `SubscribeEvents` server stream has no per-request deadline. Only stream
connection setup has a 15-second timeout. If the stream ends, the SSP
reconnects with jittered exponential backoff capped at approximately 30
seconds. It also reconciles durable payment state through `ListPayments` every
30 seconds.

## ldk-server API gaps

### Route-fee estimation

`ldk-server` can decode an invoice but does not expose the route-fee estimate
needed by the SSP API. `lightning_send_fee_estimate` therefore returns zero.
Callers must not interpret this value as a routing guarantee.

Needed capability: an estimate RPC that uses the same routing data and limits
as the payment call.

### BOLT12 hold invoices

`ldk-server` does not expose a BOLT12 equivalent of
`Bolt11ReceiveForHash`, `Bolt11ClaimForHash`, and `Bolt11FailForHash`.
The SSP can receive BOLT12 payments, but it cannot hold them until the Spark
payout completes.

Needed capability: create, claim, and fail APIs for a BOLT12 payment that uses
a caller-supplied hash.

### Outbound cancellation

The client API does not expose an abandon or cancel operation for an outbound
payment. The SSP can observe a final success or failure, but it cannot force a
stuck payment into a terminal state.

Needed capability: an idempotent payment-cancel RPC with a clear result when
the payment is already final.

### Channel liquidity automation

`ldk-server` exposes peer and channel operations, but the deployment has no
automatic inbound-liquidity, rebalance, or channel-replacement policy. An
operator must monitor and maintain channel capacity.

Needed capability: deployment automation or an external liquidity controller;
this does not require a change to the SSP protocol.

## SSP gaps that are not ldk-server gaps

These limitations are in the SSP and must not be attributed to `ldk-server`:

- `lightning_receive_quote` does not emit the protobuf `TransferManifest`
  required by the SDK quote flow.
- Wallet webhook handlers do not persist subscriptions or deliver events.
- Cooperative exits and instant static deposits return compatibility data but
  do not complete their financial operations.

See [SSP API coverage](SSP_API_COVERAGE.md) for the operation-level status.

## Deployment requirements

Production must provide `LDK_GRPC_ADDR`, either `LDK_API_KEY` or
`LDK_API_KEY_FILE`, and `LDK_TLS_CERT_FILE`. Authenticated `/status` must report
`ldk_mode: "live"` and the expected `ldk_node_id`; `/health` reports only basic
process liveness.

Do not enable `SSP_ALLOW_FAKE_LN` in production. Fake mode is only for isolated
development tests.
