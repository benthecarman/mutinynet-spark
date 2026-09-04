// Provision exact Spark leaves through the SSP's authenticated funding API.
// The production binary owns the wallet. This helper only funds on-chain
// addresses and gives confirmed transactions back to that wallet.
import { mineAndWait, rawTransaction, sendToAddress } from "./faucet.mjs";

const SSP_URL = process.env.SSP_BASE_URL ?? "http://127.0.0.1:5000";
const TOKEN = process.env.SPARK_ADMIN_TOKEN ?? process.env.SIDECAR_TOKEN ?? "";
if (!TOKEN) throw new Error("set SPARK_ADMIN_TOKEN");

const multiplicity = Number(process.env.FUND_MULTIPLICITY ?? "3");
const denoms = (process.env.FUND_LADDER ??
  "1000,2000,4000,8000,16000,32000,64000,128000,256000,512000,1024000,2048000,4096000,8192000")
  .split(",")
  .map(BigInt);
const ladder = denoms.flatMap((value) => Array(multiplicity).fill(value));

async function admin(path, body) {
  const response = await fetch(`${SSP_URL}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error ?? `${path}: HTTP ${response.status}`);
  return value;
}

const deposits = [];
for (const amount of ladder) {
  const { address } = await admin("/admin/spark/deposit-address");
  const sent = await sendToAddress(address, amount);
  deposits.push({ address, amount, txid: sent.id });
}
console.log(`sent ${deposits.length} Spark deposits`);
await mineAndWait(3, deposits.map(({ txid }) => txid));

for (const deposit of deposits) {
  const transaction = await rawTransaction(deposit.txid);
  const output = transaction.vout.find(
    ({ scriptPubKey }) =>
      scriptPubKey.address === deposit.address ||
      (scriptPubKey.addresses ?? []).includes(deposit.address),
  );
  if (!output) throw new Error(`deposit output not found in ${deposit.txid}`);
  await admin("/admin/spark/claim-deposit", {
    transaction_hex: transaction.hex,
    vout: output.n,
  });
}

const status = await (
  await fetch(`${SSP_URL}/status`, {
    headers: { Authorization: `Bearer ${TOKEN}` },
  })
).json();
console.log(
  `funded: ${status.spark.available_sats} sats across ${deposits.length} exact leaves`,
);
