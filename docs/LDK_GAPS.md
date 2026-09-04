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

### BOLT11 receive

The tested receive flow uses an SSP-owned preimage. The SSP calls
`Bolt11ReceiveForHash`, waits for `PaymentClaimable`, pays the Spark recipient,
and calls `Bolt11ClaimForHash`. It calls `Bolt11FailForHash` when the hold
invoice expires or setup fails.

The SSP stores its preimage before invoice creation and stores encrypted FROST
shares with the Spark Operators. This lets it complete the Spark payout before
it releases the Lightning preimage.

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
Lightning receive is therefore BOLT11-only.

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
- Automatic receive uses the SSP-owned preimage extension. A wallet-owned
  preimage must be sent through `reveal_preimage` before the SSP can claim it.
- BOLT12 send is not wired into the SSP. Funding verification calls the
  BOLT11-only `DecodeInvoice`, and final Spark settlement matches only BOLT11
  payment records. `ldk-server` already provides `Bolt12Send` and exposes the
  BOLT12 hash and preimage in payment state.
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
