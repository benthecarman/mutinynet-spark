// SSP swap-fill sidecar: a funded Spark wallet that completes leaf swaps
// with REAL money movement.
//
// The SDK's swap flow only requires (services/swap.ts executeSingleSwap):
//   - request.swapLeaves non-empty (content is not consumed downstream)
//   - request.inboundTransfer.sparkId resolving on the SO (user claims it)
// This sidecar transfers SUM(targets) to the session owner's Spark address
// via a real SO transfer and returns its id. The user's outbound leaves
// (already sent to the SSP identity by sendSwapTransfer) stay with the SSP;
// the sidecar spends pre-funded liquidity. Fund with fund.mjs first.
//
// Env: SPARK_SDK_DIST, SSP_URL (+ SSP_IDENTITY_PUBKEY/SCHEMA as needed),
// SO_HOSTS (comma host:port, default localhost:8535,8536,8537),
// SIDECAR_MNEMONIC (generated + printed on first boot if missing),
// SIDECAR_TOKEN (bearer for /swap-fill), PORT.
import http from "node:http";
import fs from "node:fs";
import { sparkAddressFromIdentityPubkey } from "./address.mjs";

const SDK_DIST = process.env.SPARK_SDK_DIST;
if (!SDK_DIST) throw new Error("set SPARK_SDK_DIST");
const { SparkWallet } = await import(SDK_DIST);

const TOKEN = process.env.SIDECAR_TOKEN ?? "";
const NETWORK = process.env.SPARK_NETWORK ?? "LOCAL";
const SSP_URL = process.env.SSP_URL ?? "http://127.0.0.1:5000";
// SSP identity is self-published on /health; retry while the SSP boots.
// (Only needed for the sidecar's own SSP calls; fills don't use it.)
let SSP_IDENTITY = process.env.SSP_IDENTITY_PUBKEY ?? "";
if (!SSP_IDENTITY) {
  for (let i = 0; i < 24; i++) {
    try {
      const h = await (await fetch(`${SSP_URL}/health`)).json();
      if (h.ssp_identity_pubkey) { SSP_IDENTITY = h.ssp_identity_pubkey; break; }
    } catch {}
    await new Promise((r) => setTimeout(r, 5000));
  }
  console.log("SSP identity:", SSP_IDENTITY || "(none yet)");
}
const SO_HOSTS = (process.env.SO_HOSTS ?? "127.0.0.1:8535,127.0.0.1:8536,127.0.0.1:8537").split(",");
const MNEMONIC_FILE = process.env.SIDECAR_MNEMONIC_FILE ?? "./sidecar.mnemonic";

const LOCAL_PUBKEYS = (process.env.SO_IDENTITY_PUBKEYS ??
  [
    "0322ca18fc489ae25418a0e768273c2c61cabb823edfb14feb891e9bec62016510",
    "0341727a6c41b168f07eb50865ab8c397a53c7eef628ac1020956b705e43b6cb27",
    "0305ab8d485cc752394de4981f8a5ae004f2becfea6f432c9a59d5022d8764f0a6",
    "0352aef4d49439dedd798ac4aef1e7ebef95f569545b647a25338398c1247ffdea",
    "02c05c88cc8fc181b1ba30006df6a4b0597de6490e24514fbdd0266d2b9cd3d0ba",
  ].join(",")
).split(",");

function signingOperators() {
  const ops = {};
  SO_HOSTS.forEach((address, i) => {
    const identifier = `000000000000000000000000000000000000000000000000000000000000000${i + 1}`;
    ops[identifier] = { id: i, identifier, address: `https://${address}`, identityPublicKey: LOCAL_PUBKEYS[i] };
  });
  return ops;
}

let mnemonic = process.env.SIDECAR_MNEMONIC ?? "";
if (!mnemonic && fs.existsSync(MNEMONIC_FILE)) mnemonic = fs.readFileSync(MNEMONIC_FILE, "utf8").trim();

const initOpts = {
  network: NETWORK,
  signingOperators: signingOperators(),
  threshold: 2,
  sspClientOptions: {
    baseUrl: SSP_URL,
    identityPublicKey: SSP_IDENTITY,
    schemaEndpoint: "graphql/spark/rc",
  },
  optimizationOptions: { auto: false, multiplicity: 0 },
};

let wallet;
if (mnemonic) {
  ({ wallet } = await SparkWallet.initialize({ mnemonicOrSeed: mnemonic, options: initOpts }));
} else {
  const created = await SparkWallet.initialize({ options: initOpts });
  wallet = created.wallet;
  fs.writeFileSync(MNEMONIC_FILE, created.mnemonic, { mode: 0o600 });
  console.log("generated sidecar mnemonic; FUND IT, then restart");
  console.log("sidecar address:", await wallet.getSparkAddress());
}
console.log("sidecar wallet:", await wallet.getSparkAddress());

const server = http.createServer(async (req, res) => {
  const send = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
  };
  if (req.url === "/health" && req.method === "GET") {
    const bal = await wallet.getBalance().catch(() => ({ balance: "0", satsBalance: {} }));
    return send(200, { status: "ok", address: await wallet.getSparkAddress(), identityPubkey: await wallet.getIdentityPublicKey(), balance: String(bal.balance), breakdown: {
      available: String(bal.satsBalance?.available ?? "?"),
      owned: String(bal.satsBalance?.owned ?? "?"),
      incoming: String(bal.satsBalance?.incoming ?? "?"),
    } });
  }
  if (req.url === "/sign" && req.method === "POST") {
    if (TOKEN && req.headers.authorization !== `Bearer ${TOKEN}`) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      // Sign with the sidecar (SSP identity) key via the wallet's public API
      // (sha256(message) + DER signature, hex). Matches what verifiers check.
      const { message } = JSON.parse(body);
      if (typeof message !== "string" || !message) return send(400, { error: "message string required" });
      const signature = await wallet.signMessageWithIdentityKey(message);
      return send(200, { signature });
    } catch (e) {
      return send(500, { error: e.message });
    }
  }
  if (req.url === "/swap-fill" && req.method === "POST") {
    if (TOKEN && req.headers.authorization !== `Bearer ${TOKEN}`) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      const { ownerIdentityPubkey, targetAmountsSats, totalAmountSats, idempotencyKey } = JSON.parse(body);
      const targets = (targetAmountsSats ?? []).map(Number).filter((n) => n > 0);
      if (!ownerIdentityPubkey || targets.length === 0) return send(400, { error: "ownerIdentityPubkey + targetAmountsSats required" });
      // Return the FULL locked total (targets + change). The SDK re-selects
      // its targets from the claimed inbound leaves and keeps the rest.
      const total = Number(totalAmountSats ?? 0) || targets.reduce((a, b) => a + b, 0);
      if (total < targets.reduce((a, b) => a + b, 0)) return send(400, { error: "total < targets" });
      const receiver = sparkAddressFromIdentityPubkey(ownerIdentityPubkey, NETWORK);
      // Refresh leaf cache: transfer() selects from cached leaves.
      await wallet.experimental_syncWallet();
      // One real SO transfer of the summed targets; the user claims it as
      // the swap inbound. swapLeaves entries are structurally valid but only
      // null-checked by the SDK (services/swap.ts).
      // The SDK re-selects its exact targets from the claimed inbound, so
      // send one transfer per target amount plus a change transfer. The first
      // transfer id is the inbound; the rest (change) is claimed by the
      // owner's background sync.
      const change = total - targets.reduce((a, b) => a + b, 0);
      const amounts = [...targets];
      if (change > 0) amounts.push(change);
      const txs = [];
      for (const amount of amounts) {
        await wallet.experimental_syncWallet();
        txs.push(await wallet.transfer({ amountSats: amount, receiverSparkAddress: receiver }));
      }
      const tx = txs[0];
      const leaves = (tx.leaves ?? [])
        .map((l) => l.leaf?.id)
        .filter(Boolean)
        .map((leaf_id) => ({
          leaf_id,
          raw_unsigned_refund_transaction: "",
          adaptor_signed_signature: "",
          direct_raw_unsigned_refund_transaction: "",
          direct_adaptor_signed_signature: "",
          direct_from_cpfp_raw_unsigned_refund_transaction: "",
          direct_from_cpfp_adaptor_signed_signature: "",
        }));
      // swapLeaves content is only null-checked by the SDK; keep real ids,
      // fall back to a synthetic entry if the transfer shape lacks them.
      if (leaves.length === 0) {
        leaves.push({
          leaf_id: crypto.randomUUID(),
          raw_unsigned_refund_transaction: "",
          adaptor_signed_signature: "",
          direct_raw_unsigned_refund_transaction: "",
          direct_adaptor_signed_signature: "",
          direct_from_cpfp_raw_unsigned_refund_transaction: "",
          direct_from_cpfp_adaptor_signed_signature: "",
        });
      }
      console.log(`swap-fill ${idempotencyKey}: ${total} sats -> ${receiver.slice(0, 20)}... tx=${tx.id}`);
      return send(200, { inboundTransferSparkId: tx.id, swapLeaves: leaves });
    } catch (e) {
      console.error("swap-fill failed:", e.message);
      return send(500, { error: e.message });
    }
  }
  return send(404, { error: "not found" });
});

server.listen(Number(process.env.PORT ?? 5001), () => console.log("sidecar on :5001"));
