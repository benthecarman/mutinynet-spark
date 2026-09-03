// LN receive through SparkWallet.createLightningHodlInvoice. The SSP pays an
// exact Spark transfer, then claims the hold invoice with its preimage.
import {
  assertPayment,
  cleanupWallet,
  initializeWallet,
  ldkJson,
  mintInvoicePreimage,
  paymentByHash,
  poll,
} from "./ln-test-helpers.mjs";

const LDK1 = process.env.LDK1_CONTAINER;
const LDK2 = process.env.LDK2_CONTAINER;
if (!LDK1 || !LDK2) throw new Error("set LDK1_CONTAINER and LDK2_CONTAINER");

const AMOUNT_SATS = Number(process.env.LN_RECEIVE_AMOUNT_SATS ?? "5000");
let wallet;
try {
  const initialized = await initializeWallet();
  ({ wallet } = initialized);
  const startingBalance = (await wallet.getBalance()).balance;
  const receiverIdentity = await wallet.getIdentityPublicKey();
  const paymentHash = await mintInvoicePreimage(wallet);
  const request = await wallet.createLightningHodlInvoice({
    amountSats: AMOUNT_SATS,
    paymentHash,
    memo: "ssp-sdk-receive",
    expirySeconds: 300,
  });
  if (request.status !== "INVOICE_CREATED") {
    throw new Error(`unexpected initial receive request: ${JSON.stringify(request)}`);
  }
  if (request.invoice.paymentHash.toLowerCase() !== paymentHash) {
    throw new Error("SDK invoice payment hash does not match the SSP preimage hash");
  }

  const send = ldkJson(LDK2, "bolt11-send", request.invoice.encodedInvoice);
  if (!send.payment_id) throw new Error(`LDK send has no payment ID: ${JSON.stringify(send)}`);

  const finalRequest = await poll("SSP Lightning receive success", async () => {
    const current = await wallet.getLightningReceiveRequest(request.id);
    if (current?.status === "HTLC_FAILED" || current?.status === "TRANSFER_FAILED") {
      throw new Error(`SSP receive failed: ${JSON.stringify(current)}`);
    }
    return current?.status === "TRANSFER_COMPLETED" ? current : undefined;
  });
  if (finalRequest.invoice.paymentHash.toLowerCase() !== paymentHash) {
    throw new Error("SSP receive request changed its payment hash");
  }
  const transferId = finalRequest.transfer?.sparkId;
  if (!transferId) {
    throw new Error(`completed receive has no Spark transfer: ${JSON.stringify(finalRequest)}`);
  }

  await poll("Spark receive balance credit", async () => {
    await wallet.experimental_syncWallet();
    return (await wallet.getBalance()).balance === startingBalance + BigInt(AMOUNT_SATS);
  });
  const sparkTransfer = await wallet.getTransfer(transferId);
  assertPayment(sparkTransfer, {
    id: transferId,
    transferDirection: "INCOMING",
    totalValue: AMOUNT_SATS,
    senderIdentityPublicKey: initialized.health.ssp_identity_pubkey,
    receiverIdentityPublicKey: receiverIdentity,
  });

  const [inbound, outbound] = await Promise.all([
    poll("SSP LDK inbound payment success", () => {
      const payment = paymentByHash(LDK1, paymentHash, "INBOUND");
      return payment?.status === "SUCCEEDED" ? payment : undefined;
    }),
    poll("counterparty LDK outbound payment success", () => {
      const payment = paymentByHash(LDK2, paymentHash, "OUTBOUND");
      return payment?.status === "SUCCEEDED" ? payment : undefined;
    }),
  ]);
  assertPayment(inbound, {
    direction: "INBOUND",
    status: "SUCCEEDED",
    amount_msat: AMOUNT_SATS * 1000,
  });
  assertPayment(outbound, {
    id: send.payment_id,
    direction: "OUTBOUND",
    status: "SUCCEEDED",
    amount_msat: AMOUNT_SATS * 1000,
  });

  console.log(`[ln-receive] PASS hash=${paymentHash} request=${request.id}`);
} finally {
  await cleanupWallet(wallet);
}
