// Standard LN receive through SparkWallet.createLightningInvoice. The wallet
// creates and stores the operator preimage shares; the SSP redeems them while
// it atomically transfers Spark value, then claims the held Lightning payment.
import {
  assertPayment,
  cleanupWallet,
  initializeWallet,
  ldkJson,
  paymentByHash,
  poll,
} from "./ln-test-helpers.mjs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";

const LDK1 = process.env.LDK1_CONTAINER;
const LDK2 = process.env.LDK2_CONTAINER;
const PAYER = process.env.LN_RECEIVE_PAYER ?? "ldk-server";
if (!LDK1 || (PAYER === "ldk-server" && !LDK2)) {
  throw new Error("set LDK1_CONTAINER and set LDK2_CONTAINER for the ldk-server payer");
}
if (PAYER !== "ldk-server" && PAYER !== "mutinynet-cli") {
  throw new Error("LN_RECEIVE_PAYER must be ldk-server or mutinynet-cli");
}

const AMOUNT_SATS = Number(process.env.LN_RECEIVE_AMOUNT_SATS ?? "5000");
let wallet;
try {
  const initialized = await initializeWallet();
  ({ wallet } = initialized);
  const startingBalance = (await wallet.getBalance()).balance;
  const receiverIdentity = await wallet.getIdentityPublicKey();
  const request = await wallet.createLightningInvoice({
    amountSats: AMOUNT_SATS,
    memo: "ssp-sdk-receive",
    expirySeconds: 300,
  });
  const paymentHash = request.invoice.paymentHash.toLowerCase();
  if (request.status !== "INVOICE_CREATED") {
    throw new Error(`unexpected initial receive request: ${JSON.stringify(request)}`);
  }
  let send;
  if (PAYER === "mutinynet-cli") {
    execFileSync(
      process.env.MUTINYNET_CLI_PATH ?? "mutinynet-cli",
      ["lightning", request.invoice.encodedInvoice],
      {
        encoding: "utf8",
        stdio: "inherit",
        timeout: Number(process.env.E2E_MUTINYNET_TIMEOUT_MS ?? "120000"),
      },
    );
  } else {
    send = ldkJson(LDK2, "bolt11-send", request.invoice.encodedInvoice);
    if (!send.payment_id) {
      throw new Error(`LDK send has no payment ID: ${JSON.stringify(send)}`);
    }
  }

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

  const inbound = await poll("SSP LDK inbound payment success", () => {
    const payment = paymentByHash(LDK1, paymentHash, "INBOUND");
    return payment?.status === "SUCCEEDED" ? payment : undefined;
  });
  assertPayment(inbound, {
    direction: "INBOUND",
    status: "SUCCEEDED",
    amount_msat: AMOUNT_SATS * 1000,
  });
  if (PAYER === "ldk-server") {
    const outbound = await poll("counterparty LDK outbound payment success", () => {
      const payment = paymentByHash(LDK2, paymentHash, "OUTBOUND");
      return payment?.status === "SUCCEEDED" ? payment : undefined;
    });
    assertPayment(outbound, {
      id: send.payment_id,
      direction: "OUTBOUND",
      status: "SUCCEEDED",
      amount_msat: AMOUNT_SATS * 1000,
    });
  }
  const claimedPreimage =
    inbound.kind?.kind?.bolt11?.preimage ?? inbound.kind?.bolt11?.preimage;
  if (!/^[0-9a-f]{64}$/i.test(claimedPreimage ?? "")) {
    throw new Error("settled inbound payment has no 32-byte preimage");
  }
  const claimedHash = createHash("sha256")
    .update(Buffer.from(claimedPreimage, "hex"))
    .digest("hex");
  if (claimedHash !== paymentHash) {
    throw new Error("LDK claimed with a preimage that does not match the wallet hash");
  }

  console.log(`[ln-receive] PASS hash=${paymentHash} request=${request.id}`);
} finally {
  await cleanupWallet(wallet);
}
