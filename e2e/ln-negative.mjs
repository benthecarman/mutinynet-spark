// Failure checks that must not move Lightning or Spark funds.
import { randomUUID } from "node:crypto";
import {
  GRAPHQL_URL,
  authenticatedRaw,
  cleanupWallet,
  fetchJson,
  initializeWallet,
  ldkJson,
  mintInvoicePreimage,
  paymentByHash,
  poll,
} from "./ln-test-helpers.mjs";

const LDK1 = process.env.LDK1_CONTAINER;
const LDK2 = process.env.LDK2_CONTAINER;
if (!LDK1 || !LDK2) throw new Error("set LDK1_CONTAINER and LDK2_CONTAINER");

const SEND_MUTATION = `
  mutation RequestLightningSend(
    $encoded_invoice: String!
    $amount_sats: Long
    $user_outbound_transfer_external_id: UUID
  ) {
    request_lightning_send(input: {
      encoded_invoice: $encoded_invoice
      amount_sats: $amount_sats
      user_outbound_transfer_external_id: $user_outbound_transfer_external_id
    }) { request { id status } }
  }
`;

async function mustReject(label, action, pattern) {
  try {
    await action();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!pattern.test(message)) {
      throw new Error(`${label} returned the wrong error: ${message}`);
    }
    return;
  }
  throw new Error(`${label} did not reject`);
}

let wallet;
try {
  ({ wallet } = await initializeWallet());
  const startingBalance = (await wallet.getBalance()).balance;

  const unauthorized = await fetch(GRAPHQL_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: "query UserRequest($request_id:ID!){ user_request(request_id:$request_id){ id } }",
      variables: { request_id: randomUUID() },
      operationName: "UserRequest",
    }),
    signal: AbortSignal.timeout(15_000),
  });
  const unauthorizedBody = await unauthorized.json();
  if (
    unauthorizedBody.data != null ||
    !/unauthorized/i.test(unauthorizedBody.errors?.[0]?.message ?? "")
  ) {
    throw new Error(`unauthorized GraphQL request was accepted: ${JSON.stringify(unauthorizedBody)}`);
  }

  await mustReject(
    "malformed payment hash",
    () =>
      wallet.createLightningHodlInvoice({
        amountSats: 1000,
        paymentHash: "not-a-hash",
      }),
    /invalid payment hash/i,
  );

  const invoiceResult = ldkJson(LDK2, "bolt11-receive", "700sat", "-d", "ssp-negative");
  const paymentHash = invoiceResult.payment_hash.toLowerCase();
  await mustReject(
    "missing Spark funding transfer",
    () =>
      authenticatedRaw(wallet, SEND_MUTATION, {
        encoded_invoice: invoiceResult.invoice,
        amount_sats: null,
        user_outbound_transfer_external_id: null,
      }),
    /user_outbound_transfer_external_id is required/i,
  );
  await mustReject(
    "unknown Spark funding transfer",
    () =>
      authenticatedRaw(wallet, SEND_MUTATION, {
        encoded_invoice: invoiceResult.invoice,
        amount_sats: null,
        user_outbound_transfer_external_id: randomUUID(),
      }),
    /matching preimage swap was not found/i,
  );
  if (paymentByHash(LDK2, paymentHash, "INBOUND")?.status === "SUCCEEDED") {
    throw new Error("an unfunded request paid the Lightning invoice");
  }
  if (paymentByHash(LDK1, paymentHash, "OUTBOUND")) {
    throw new Error("an unfunded request reached the SSP Lightning node");
  }

  const expiringHash = await mintInvoicePreimage(wallet);
  const expiring = await wallet.createLightningHodlInvoice({
    amountSats: 800,
    paymentHash: expiringHash,
    memo: "ssp-expiry",
    expirySeconds: 1,
  });
  await poll(
    "expired hold invoice failure",
    async () => {
      const response = await authenticatedRaw(
        wallet,
        "query UserRequest($request_id:ID!){ user_request(request_id:$request_id){ __typename ... on LightningReceiveRequest { status } } }",
        { request_id: expiring.id },
      );
      return response.user_request?.status === "HTLC_FAILED" ? response.user_request : undefined;
    },
    { timeoutMs: 90_000 },
  );
  await wallet.experimental_syncWallet();
  if ((await wallet.getBalance()).balance !== startingBalance) {
    throw new Error("a rejected or expired request changed the Spark balance");
  }

  console.log("[ln-negative] PASS auth, validation, funding proof, expiry");
} finally {
  await cleanupWallet(wallet);
}
