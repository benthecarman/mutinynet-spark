// LN send through the public Spark SDK. This verifies the Spark debit, SSP
// state, exact LDK payment, and retry idempotency for one payment hash.
import { sendToAddress, mineAndWait } from "./faucet.mjs";
import {
  assertPayment,
  cleanupWallet,
  initializeWallet,
  ldkJson,
  paymentByHash,
  paymentsByHash,
  poll,
} from "./ln-test-helpers.mjs";

const LDK1 = process.env.LDK1_CONTAINER;
const LDK2 = process.env.LDK2_CONTAINER;
if (!LDK1 || !LDK2) throw new Error("set LDK1_CONTAINER and LDK2_CONTAINER");

const AMOUNT_SATS = Number(process.env.LN_SEND_AMOUNT_SATS ?? "3000");
const FUND_SATS = BigInt(process.env.LN_SEND_FUND_SATS ?? String(AMOUNT_SATS));

let wallet;
let generateTransferId;
try {
  ({ wallet, generateTransferId } = await initializeWallet());

  const depositAddress = await wallet.getSingleUseDepositAddress();
  const deposit = await sendToAddress(depositAddress, FUND_SATS);
  await mineAndWait(3, [deposit.id]);
  await wallet.claimDeposit(deposit.id);
  await poll("Spark send wallet funding", async () => {
    await wallet.experimental_syncWallet();
    return (await wallet.getBalance()).balance === FUND_SATS;
  });

  const invoiceResult = ldkJson(
    LDK2,
    "bolt11-receive",
    `${AMOUNT_SATS}sat`,
    "-d",
    "ssp-sdk-send",
  );
  const invoice = invoiceResult.invoice;
  const paymentHash = invoiceResult.payment_hash?.toLowerCase();
  if (!invoice || !/^[0-9a-f]{64}$/.test(paymentHash ?? "")) {
    throw new Error(`invalid LDK invoice response: ${JSON.stringify(invoiceResult)}`);
  }

  const transferIdObject = generateTransferId();
  const transferId = transferIdObject.toString();
  const request = await wallet.payLightningInvoice({
    invoice,
    maxFeeSats: 0,
    transferId: transferIdObject,
  });
  if (!request?.id || request.status !== "LIGHTNING_PAYMENT_INITIATED") {
    throw new Error(`unexpected initial send request: ${JSON.stringify(request)}`);
  }

  const terminal = await poll("SSP Lightning send success", async () => {
    const current = await wallet.getLightningSendRequest(request.id);
    if (current?.status === "LIGHTNING_PAYMENT_FAILED") {
      throw new Error(`SSP payment failed: ${request.id}`);
    }
    return current?.status === "LIGHTNING_PAYMENT_SUCCEEDED" ? current : undefined;
  });
  if (terminal.idempotencyKey !== transferId) {
    throw new Error(`SSP idempotency key ${terminal.idempotencyKey} does not match ${transferId}`);
  }

  const [outbound, inbound] = await Promise.all([
    poll("SSP LDK payment success", () => {
      const payment = paymentByHash(LDK1, paymentHash, "OUTBOUND");
      return payment?.status === "SUCCEEDED" ? payment : undefined;
    }),
    poll("LDK counterparty payment success", () => {
      const payment = paymentByHash(LDK2, paymentHash, "INBOUND");
      return payment?.status === "SUCCEEDED" ? payment : undefined;
    }),
  ]);
  assertPayment(outbound, {
    direction: "OUTBOUND",
    status: "SUCCEEDED",
    amount_msat: AMOUNT_SATS * 1000,
  });
  assertPayment(inbound, {
    direction: "INBOUND",
    status: "SUCCEEDED",
    amount_msat: AMOUNT_SATS * 1000,
  });

  const expectedBalance = FUND_SATS - BigInt(AMOUNT_SATS);
  await poll("Spark send balance debit", async () => {
    await wallet.experimental_syncWallet();
    return (await wallet.getBalance()).balance === expectedBalance;
  });

  // Replay the SDK's SSP request with the same funded transfer. The public
  // pay helper cannot run again after it has spent the local leaf because it
  // performs local coin selection before it reaches the SSP.
  const retry = await wallet.getSspClient().requestLightningSend({
    encodedInvoice: invoice,
    userOutboundTransferExternalId: transferId,
  });
  if (retry.id !== request.id) {
    throw new Error(`idempotent retry returned ${retry.id}; expected ${request.id}`);
  }
  if (
    paymentsByHash(LDK1, paymentHash, "OUTBOUND").length !== 1 ||
    paymentsByHash(LDK2, paymentHash, "INBOUND").length !== 1
  ) {
    throw new Error("idempotent retry produced more than one matching LDK payment");
  }
  if ((await wallet.getBalance()).balance !== expectedBalance) {
    throw new Error("idempotent retry changed the Spark balance twice");
  }

  console.log(`[ln-send] PASS hash=${paymentHash} request=${request.id}`);
} finally {
  await cleanupWallet(wallet);
}
