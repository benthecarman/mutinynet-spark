// SSP liquidity sidecar: a funded Spark wallet that pays Lightning receives
// and completes leaf swaps with real money movement.
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
import crypto from "node:crypto";
import { sparkAddressFromIdentityPubkey } from "./address.mjs";

const SDK_DIST = process.env.SPARK_SDK_DIST;
if (!SDK_DIST) throw new Error("set SPARK_SDK_DIST");
const { SparkWallet } = await import(SDK_DIST);

const TOKEN = process.env.SIDECAR_TOKEN ?? "";
// Fail closed: an unset token opens /swap-fill (liquidity drain), /sign
// (signing oracle) and /store-shares to anyone who reaches the port.
// Explicit opt-out only: SIDECAR_ALLOW_NO_AUTH=1 (local dev).
if (!TOKEN && process.env.SIDECAR_ALLOW_NO_AUTH !== "1") {
  console.error("refusing to start: set SIDECAR_TOKEN");
  process.exit(1);
}

function authorized(req) {
  if (!TOKEN) return true;
  const got = (req.headers.authorization ?? "").replace(/^Bearer /, "");
  const a = crypto.createHash("sha256").update(got).digest();
  const b = crypto.createHash("sha256").update(TOKEN).digest();
  return crypto.timingSafeEqual(a, b);
}
const NETWORK = process.env.SPARK_NETWORK ?? "LOCAL";
const SSP_URL = process.env.SSP_URL ?? "http://127.0.0.1:5000";
// The sidecar wallet is the SSP identity. When no fixed identity is set, the
// wallet is initialized once to derive that identity and then reinitialized
// with the correct SSP verification key. This avoids a startup cycle in which
// the SSP and sidecar each wait for the other service to publish the key.
let SSP_IDENTITY = process.env.SSP_IDENTITY_PUBKEY ?? "";
const SO_HOSTS = (process.env.SO_HOSTS ?? "127.0.0.1:8535,127.0.0.1:8536,127.0.0.1:8537").split(",");
const MNEMONIC_FILE = process.env.SIDECAR_MNEMONIC_FILE ?? "./sidecar.mnemonic";
const FILL_RECEIPTS_FILE = process.env.SIDECAR_FILL_RECEIPTS_FILE ?? "./swap-fills.json";

let fillReceipts = {};
if (fs.existsSync(FILL_RECEIPTS_FILE)) {
  fillReceipts = JSON.parse(fs.readFileSync(FILL_RECEIPTS_FILE, "utf8"));
}

function persistFillReceipts() {
  const temp = `${FILL_RECEIPTS_FILE}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(fillReceipts), { mode: 0o600 });
  fs.renameSync(temp, FILL_RECEIPTS_FILE);
}

const fillsInProgress = new Set();
let liquidityTail = Promise.resolve();

async function withLiquidityLock(action) {
  let release;
  const previous = liquidityTail;
  liquidityTail = new Promise((resolve) => {
    release = resolve;
  });
  await previous;
  try {
    return await action();
  } finally {
    release();
  }
}

function publicKeyHex(value) {
  if (typeof value === "string") return value.toLowerCase();
  return Buffer.from(value ?? []).toString("hex").toLowerCase();
}

function validHash(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function transferIdForPaymentHash(paymentHash) {
  const bytes = Buffer.from(paymentHash.slice(0, 32), "hex");
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function validateReceiveTransfer(transfer, transferId, ownerIdentityPubkey, amountSats) {
  if (!transfer || transfer.id !== transferId) {
    throw new Error("Lightning receive transfer has the wrong id");
  }
  if (publicKeyHex(transfer.senderIdentityPublicKey) !== SSP_IDENTITY.toLowerCase()) {
    throw new Error("Lightning receive transfer has the wrong sender");
  }
  const directReceiver = publicKeyHex(transfer.receiverIdentityPublicKey);
  const receiverMatches =
    directReceiver === ownerIdentityPubkey.toLowerCase() ||
    (transfer.receivers ?? []).some(
      (receiver) =>
        publicKeyHex(receiver.identityPublicKey) === ownerIdentityPubkey.toLowerCase() &&
        Number(receiver.amountSats) === amountSats,
    );
  if (!receiverMatches || Number(transfer.totalValue) !== amountSats) {
    throw new Error("Lightning receive transfer has the wrong receiver or amount");
  }
}

async function findPreimageSwap(outboundTransferId, paymentHash) {
  const response = await wallet.queryHTLC({
    paymentHashes: [paymentHash],
    transferIds: [outboundTransferId],
    limit: 10,
  });
  return (response.preimageRequests ?? []).find(
    (request) =>
      request.transfer?.id === outboundTransferId &&
      publicKeyHex(request.paymentHash) === paymentHash,
  );
}

async function signingMessageAllowed(message) {
  if (/^[0-9a-f]{64}:[0-9]{1,10}:[0-9]{1,20}$/.test(message)) return true;
  try {
    const manifest = JSON.parse(Buffer.from(message, "base64").toString("utf8"));
    return (
      typeof manifest.transfer_id === "string" &&
      /^[0-9a-f-]{36}$/.test(manifest.transfer_id) &&
      Number.isSafeInteger(manifest.amount_sats) &&
      manifest.amount_sats > 0 &&
      typeof manifest.network === "string" &&
      manifest.ssp_identity_pubkey === await wallet.getIdentityPublicKey()
    );
  } catch {
    return false;
  }
}

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
// See fund.mjs: custom chains must set ELECTRS_URL for any L1 reads.
if (process.env.ELECTRS_URL) initOpts.electrsUrl = process.env.ELECTRS_URL;

let initialized = await SparkWallet.initialize({
  ...(mnemonic ? { mnemonicOrSeed: mnemonic } : {}),
  options: initOpts,
});
if (!mnemonic) {
  mnemonic = initialized.mnemonic;
  fs.writeFileSync(MNEMONIC_FILE, mnemonic, { mode: 0o600 });
  console.log("generated sidecar mnemonic; FUND IT, then restart");
}
let wallet = initialized.wallet;
if (!SSP_IDENTITY) {
  SSP_IDENTITY = await wallet.getIdentityPublicKey();
  await wallet.cleanup();
  initOpts.sspClientOptions.identityPublicKey = SSP_IDENTITY;
  initialized = await SparkWallet.initialize({ mnemonicOrSeed: mnemonic, options: initOpts });
  wallet = initialized.wallet;
}
console.log("SSP identity:", SSP_IDENTITY);
console.log("sidecar wallet:", await wallet.getSparkAddress());

let needsTopup = false;

const server = http.createServer(async (req, res) => {
  const send = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
  };
  if (req.url === "/health" && req.method === "GET") {
    const bal = await wallet.getBalance().catch(() => ({ balance: "0", satsBalance: {} }));
    return send(200, { status: "ok", address: await wallet.getSparkAddress(), identityPubkey: await wallet.getIdentityPublicKey(), needsTopup, balance: String(bal.balance), breakdown: {
      available: String(bal.satsBalance?.available ?? "?"),
      owned: String(bal.satsBalance?.owned ?? "?"),
      incoming: String(bal.satsBalance?.incoming ?? "?"),
    } });
  }
  if (req.url === "/sign" && req.method === "POST") {
    if (!authorized(req)) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      // Sign with the sidecar (SSP identity) key via the wallet's public API
      // (sha256(message) + DER signature, hex). Matches what verifiers check.
      // Constrained shape: base64 manifests or hex-colon quote payloads only.
      const { message } = JSON.parse(body);
      if (
        typeof message !== "string" ||
        message.length > 8192 ||
        !(await signingMessageAllowed(message))
      ) {
        return send(400, { error: "message shape rejected" });
      }
      const signature = await wallet.signMessageWithIdentityKey(message);
      return send(200, { signature });
    } catch (e) {
      return send(500, { error: e.message });
    }
  }
  if (req.url === "/store-shares" && req.method === "POST") {
    if (!authorized(req)) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      // Store SSP-split preimage shares via the coordinator (same call the
      // SDK wallet makes; owner = SSP identity so attestor == holder).
      // { paymentHashHex, shares: {identifier: hex}, threshold, invoiceString, ownerIdentityPubkeyHex }
      const { paymentHashHex, shares, threshold, invoiceString, ownerIdentityPubkeyHex } = JSON.parse(body);
      const encryptedPreimageShares = {};
      for (const [identifier, hex] of Object.entries(shares ?? {})) {
        encryptedPreimageShares[identifier] = Uint8Array.from(Buffer.from(hex, "hex"));
      }
      const client = await wallet.connectionManager.createSparkClient(
        wallet.config.getCoordinatorAddress(),
      );
      await client.store_preimage_share_v2({
        paymentHash: Uint8Array.from(Buffer.from(paymentHashHex, "hex")),
        encryptedPreimageShares,
        threshold,
        invoiceString,
        userIdentityPublicKey: Uint8Array.from(Buffer.from(ownerIdentityPubkeyHex, "hex")),
      });
      return send(200, { ok: true });
    } catch (e) {
      return send(500, { error: e.message });
    }
  }
  if (req.url === "/verify-lightning-send" && req.method === "POST") {
    if (!authorized(req)) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      const {
        ownerIdentityPubkey,
        outboundTransferId,
        paymentHash,
        totalAmountSats,
      } = JSON.parse(body);
      const total = Number(totalAmountSats);
      if (
        !/^[0-9a-f]{66}$/i.test(ownerIdentityPubkey ?? "") ||
        typeof outboundTransferId !== "string" ||
        outboundTransferId.length === 0 ||
        !validHash(paymentHash) ||
        !Number.isSafeInteger(total) ||
        total <= 0
      ) {
        return send(400, { error: "invalid Lightning send funding proof" });
      }
      const request = await findPreimageSwap(outboundTransferId, paymentHash);
      if (!request?.transfer) {
        return send(404, { error: "matching preimage swap was not found" });
      }
      if (publicKeyHex(request.senderIdentityPubkey) !== ownerIdentityPubkey.toLowerCase()) {
        return send(403, { error: "preimage swap sender does not match session owner" });
      }
      const transferTotal = Number(request.transfer.totalValue);
      if (!Number.isSafeInteger(transferTotal) || transferTotal !== total) {
        return send(409, {
          error: `preimage swap has ${request.transfer.totalValue} sats; expected ${total}`,
        });
      }
      if (request.status !== 0 || (request.preimage?.length ?? 0) !== 0) {
        return send(409, { error: "preimage swap is not waiting for payment" });
      }
      return send(200, { ok: true });
    } catch (e) {
      return send(500, { error: e.message });
    }
  }
  if (req.url === "/settle-lightning-send" && req.method === "POST") {
    if (!authorized(req)) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      const { outboundTransferId, paymentHash, preimage } = JSON.parse(body);
      if (
        typeof outboundTransferId !== "string" ||
        outboundTransferId.length === 0 ||
        !validHash(paymentHash) ||
        !validHash(preimage) ||
        crypto.createHash("sha256").update(Buffer.from(preimage, "hex")).digest("hex") !== paymentHash
      ) {
        return send(400, { error: "invalid Lightning settlement proof" });
      }
      const request = await findPreimageSwap(outboundTransferId, paymentHash);
      if (!request?.transfer) {
        return send(404, { error: "matching preimage swap was not found" });
      }
      if (request.status === 1 && (request.preimage?.length ?? 0) > 0) {
        return send(200, { ok: true, transferId: outboundTransferId });
      }
      if (request.status !== 0) {
        return send(409, { error: "preimage swap can no longer be settled" });
      }
      const transfer = await wallet.claimHTLC(preimage);
      if (transfer?.id !== outboundTransferId) {
        throw new Error("settled transfer id does not match the funded transfer");
      }
      return send(200, { ok: true, transferId: transfer.id });
    } catch (e) {
      return send(500, { error: e.message });
    }
  }
  if (req.url === "/settle-lightning-receive" && req.method === "POST") {
    if (!authorized(req)) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    let receiptKey;
    try {
      const { ownerIdentityPubkey, paymentHash, amountSats } = JSON.parse(body);
      const amount = Number(amountSats);
      if (
        !/^[0-9a-f]{66}$/i.test(ownerIdentityPubkey ?? "") ||
        !validHash(paymentHash) ||
        !Number.isSafeInteger(amount) ||
        amount <= 0
      ) {
        return send(400, { error: "invalid Lightning receive settlement" });
      }
      receiptKey = `lightning-receive:${paymentHash}`;
      const transferId = transferIdForPaymentHash(paymentHash);
      const existing = fillReceipts[receiptKey];
      if (
        existing &&
        (existing.ownerIdentityPubkey !== ownerIdentityPubkey || existing.amountSats !== amount)
      ) {
        return send(409, { error: "payment hash is already bound to another Lightning receive" });
      }
      if (existing?.status === "complete") return send(200, existing.result);
      if (fillsInProgress.has(receiptKey)) {
        return send(409, { error: "Lightning receive settlement is already in progress" });
      }
      fillsInProgress.add(receiptKey);

      const receiverSparkAddress = sparkAddressFromIdentityPubkey(ownerIdentityPubkey, NETWORK);
      fillReceipts[receiptKey] = {
        status: "transferring",
        ownerIdentityPubkey,
        amountSats: amount,
        transferId,
      };
      persistFillReceipts();

      const transfer = await withLiquidityLock(async () => {
        await wallet.experimental_syncWallet();
        let current = await wallet.getTransfer(transferId);
        if (!current) {
          if (typeof wallet.transferInternal !== "function") {
            throw new Error("idempotent Spark transfer is unavailable");
          }
          try {
            current = await wallet.transferInternal(
              { amountSats: amount, receiverSparkAddress },
              transferId,
            );
          } catch (error) {
            current = await wallet.getTransfer(transferId);
            if (!current) throw error;
          }
        }
        return current;
      });
      validateReceiveTransfer(transfer, transferId, ownerIdentityPubkey, amount);
      const result = { ok: true, transferId };
      fillReceipts[receiptKey] = {
        status: "complete",
        ownerIdentityPubkey,
        amountSats: amount,
        transferId,
        result,
      };
      persistFillReceipts();
      needsTopup = false;
      console.log(
        `lightning-receive ${paymentHash}: paid ${amount} sats -> ${receiverSparkAddress.slice(0, 20)}... tx=${transferId}`,
      );
      return send(200, result);
    } catch (e) {
      const needsTopupErr = /available balance|NEEDS_TOPUP/.test(e.message);
      if (needsTopupErr) needsTopup = true;
      console.error(
        `lightning-receive failed${needsTopupErr ? " (NEEDS_TOPUP)" : ""}:`,
        e.message,
      );
      return send(needsTopupErr ? 507 : 500, { error: e.message, needsTopup: needsTopupErr });
    } finally {
      if (receiptKey) fillsInProgress.delete(receiptKey);
    }
  }
  if (req.url === "/swap-fill" && req.method === "POST") {
    if (!authorized(req)) return send(401, { error: "unauthorized" });
    let body = "";
    for await (const chunk of req) body += chunk;
    try {
      const {
        ownerIdentityPubkey,
        outboundTransferId,
        targetAmountsSats,
        receivedTotalAmountSats,
        payoutTotalAmountSats,
        idempotencyKey,
      } = JSON.parse(body);
      const targets = (targetAmountsSats ?? []).map(Number);
      const receivedTotal = Number(receivedTotalAmountSats);
      const payoutTotal = Number(payoutTotalAmountSats);
      if (!ownerIdentityPubkey || !outboundTransferId || targets.length === 0) {
        return send(400, { error: "ownerIdentityPubkey, outboundTransferId, and targetAmountsSats are required" });
      }
      if (
        !targets.every((n) => Number.isSafeInteger(n) && n > 0) ||
        !Number.isSafeInteger(receivedTotal) || receivedTotal <= 0 ||
        !Number.isSafeInteger(payoutTotal) || payoutTotal <= 0
      ) {
        return send(400, { error: "swap amounts must be positive safe integers" });
      }
      const targetTotal = targets.reduce((sum, amount) => sum + amount, 0);
      if (!Number.isSafeInteger(targetTotal) || targetTotal > payoutTotal || payoutTotal > receivedTotal) {
        return send(400, { error: "invalid target, payout, or received total" });
      }
      const existing = fillReceipts[outboundTransferId];
      if (existing && (existing.ownerIdentityPubkey !== ownerIdentityPubkey || existing.receivedTotal !== receivedTotal)) {
        return send(409, { error: "outbound transfer is already bound to another fill" });
      }
      if (existing?.status === "complete") return send(200, existing.result);
      if (fillsInProgress.has(outboundTransferId)) {
        return send(409, { error: "swap fill is already in progress" });
      }
      fillsInProgress.add(outboundTransferId);
      const receiver = sparkAddressFromIdentityPubkey(ownerIdentityPubkey, NETWORK);
      try {
        if (!existing || existing.status === "claiming") {
          fillReceipts[outboundTransferId] = {
            status: "claiming",
            ownerIdentityPubkey,
            receivedTotal,
          };
          persistFillReceipts();
          const svc = wallet.transferService ?? wallet._transferService;
          if (!svc) throw new Error("transfer service unavailable");
          const pending = await svc.queryPendingTransfers({ transferIds: [outboundTransferId] });
          let inbound = pending?.transfers?.find((transfer) => transfer.id === outboundTransferId);
          if (!inbound) {
            await wallet.experimental_syncWallet();
            inbound = await wallet.getTransfer(outboundTransferId);
            if (inbound?.status !== "TRANSFER_STATUS_COMPLETED") {
              throw new Error("outbound swap transfer was not found for the sidecar");
            }
          }
          if (publicKeyHex(inbound.senderIdentityPublicKey) !== ownerIdentityPubkey.toLowerCase()) {
            throw new Error("outbound swap transfer sender does not match the session owner");
          }
          const receiverIdentity = publicKeyHex(inbound.receiverIdentityPublicKey);
          const receiverMatches =
            receiverIdentity === SSP_IDENTITY.toLowerCase() ||
            (inbound.receivers ?? []).some(
              (receiver) =>
                publicKeyHex(receiver.identityPublicKey) === SSP_IDENTITY.toLowerCase() &&
                Number(receiver.amountSats) === receivedTotal,
            );
          if (!receiverMatches) {
            throw new Error("outbound swap transfer receiver does not match the sidecar");
          }
          if (Number(inbound.totalValue) !== receivedTotal) {
            throw new Error(`outbound swap transfer has ${inbound.totalValue} sats; expected ${receivedTotal}`);
          }
          if (inbound.status !== "TRANSFER_STATUS_COMPLETED" && inbound.status !== 5) {
            const claimedLeaves = await svc.claimTransfer(inbound);
            const claimedTotal = claimedLeaves.reduce(
              (sum, leaf) => sum + Number(leaf.value ?? 0),
              0,
            );
            if (claimedTotal !== receivedTotal) {
              throw new Error(`claimed ${claimedTotal} sats; expected ${receivedTotal}`);
            }
          }
          fillReceipts[outboundTransferId].status = "funded";
          persistFillReceipts();
        }

        await wallet.experimental_syncWallet();
        if (typeof wallet.transferV2 !== "function") {
          throw new Error("atomic multi-receiver transferV2 is unavailable");
        }
        const change = payoutTotal - targetTotal;
        const amounts = [...targets];
        if (change > 0) amounts.push(change);
        const tx = await wallet.transferV2({
          receivers: amounts.map((amountSats) => ({
            amountSats,
            receiverSparkAddress: receiver,
          })),
        });
        const leaves = (tx.leaves ?? [])
          .map((leaf) => leaf.leaf?.id)
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
        const result = { inboundTransferSparkId: tx.id, swapLeaves: leaves };
        fillReceipts[outboundTransferId] = {
          status: "complete",
          ownerIdentityPubkey,
          receivedTotal,
          result,
        };
        persistFillReceipts();
        console.log(`swap-fill ${idempotencyKey}: received ${receivedTotal}, paid ${payoutTotal} sats -> ${receiver.slice(0, 20)}... tx=${tx.id}`);
        needsTopup = false;
        return send(200, result);
      } finally {
        fillsInProgress.delete(outboundTransferId);
      }
    } catch (e) {
      // A depleted ladder leaves the verified inbound in the sidecar wallet
      // with a funded receipt. The same request can resume after a top-up.
      const needsTopupErr = /available balance|NEEDS_TOPUP/.test(e.message);
      if (needsTopupErr) needsTopup = true;
      console.error(`swap-fill failed${needsTopupErr ? " (NEEDS_TOPUP)" : ""}:`, e.message);
      return send(needsTopupErr ? 507 : 500, { error: e.message, needsTopup: needsTopupErr });
    }
  }
  return send(404, { error: "not found" });
});

server.listen(Number(process.env.PORT ?? 5001), () => console.log("sidecar on :5001"));
