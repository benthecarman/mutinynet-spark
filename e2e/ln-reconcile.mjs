// Create a hold invoice, stop the SSP before payment, and verify that its
// startup reconciler claims the payment after restart without the live event.
import { execFileSync } from "node:child_process";
import {
  assertLiveHealth,
  cleanupWallet,
  initializeWallet,
  ldkJson,
  mintInvoicePreimage,
  paymentByHash,
  poll,
} from "./ln-test-helpers.mjs";

const LDK1 = process.env.LDK1_CONTAINER;
const LDK2 = process.env.LDK2_CONTAINER;
const SSP = process.env.SSP_CONTAINER;
if (!LDK1 || !LDK2 || !SSP) {
  throw new Error("set LDK1_CONTAINER, LDK2_CONTAINER, and SSP_CONTAINER");
}
const AMOUNT_SATS = Number(process.env.LN_RECEIVE_AMOUNT_SATS ?? "5000");

let wallet;
let sspStopped = false;
try {
  ({ wallet } = await initializeWallet());
  const startingBalance = (await wallet.getBalance()).balance;
  const paymentHash = await mintInvoicePreimage(wallet);
  const request = await wallet.createLightningHodlInvoice({
    amountSats: AMOUNT_SATS,
    paymentHash,
    memo: "ssp-reconcile",
    expirySeconds: 300,
  });

  execFileSync("docker", ["stop", "--time", "10", SSP], { timeout: 20_000 });
  sspStopped = true;
  const send = ldkJson(LDK2, "bolt11-send", request.invoice.encodedInvoice);
  if (!send.payment_id) throw new Error("counterparty did not start the held payment");

  await poll("held payment while SSP is offline", () => {
    const inbound = paymentByHash(LDK1, paymentHash, "INBOUND");
    return inbound && inbound.status !== "SUCCEEDED" ? inbound : undefined;
  });

  execFileSync("docker", ["start", SSP], { timeout: 20_000 });
  sspStopped = false;
  await poll("SSP restart", () => assertLiveHealth(), { timeoutMs: 120_000 });

  await poll(
    "missed-event reconciliation",
    async () => {
      const [requestState, inbound, outbound] = await Promise.all([
        wallet.getLightningReceiveRequest(request.id).catch(() => undefined),
        Promise.resolve(paymentByHash(LDK1, paymentHash, "INBOUND")),
        Promise.resolve(paymentByHash(LDK2, paymentHash, "OUTBOUND")),
      ]);
      return requestState?.status === "TRANSFER_COMPLETED" &&
        requestState.transfer?.sparkId &&
        inbound?.status === "SUCCEEDED" &&
        outbound?.status === "SUCCEEDED";
    },
    { timeoutMs: 180_000 },
  );

  await poll("reconciled Spark receive balance", async () => {
    await wallet.experimental_syncWallet();
    return (await wallet.getBalance()).balance === startingBalance + BigInt(AMOUNT_SATS);
  });

  console.log(`[ln-reconcile] PASS hash=${paymentHash} request=${request.id}`);
} finally {
  if (sspStopped) {
    execFileSync("docker", ["start", SSP], { timeout: 20_000 });
  }
  await cleanupWallet(wallet);
}
