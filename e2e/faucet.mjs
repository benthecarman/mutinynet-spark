// Minimal faucet over bitcoind RPC (no SDK test-utils needed).
// Regtest: MINING=1 mines via generatetoaddress (default, preserves old behavior).
// Signet/custom chains: unset MINING and pass txids; mineAndWait polls the
// node until the tx reaches the confirmation threshold instead.
const URL = process.env.BITCOIN_RPC_URL ?? "http://127.0.0.1:8332";
const USER = process.env.BITCOIN_RPC_USER ?? "testutil";
const PASS = process.env.BITCOIN_RPC_PASSWORD ?? "testutilpassword";
const WALLET = process.env.BITCOIN_RPC_WALLET ?? "default";
// MINING=1 mines via generatetoaddress (regtest only). Anything else polls
// for confirmations (signet/custom chains with an external miner).
const MINING = process.env.MINING ?? "";
const CONFS = Number(process.env.FUND_CONFS ?? "3");

let id = 0;
async function rpc(method, params = [], wallet) {
  const endpoint = wallet ? `${URL}/wallet/${wallet}` : URL;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${Buffer.from(`${USER}:${PASS}`).toString("base64")}`,
    },
    body: JSON.stringify({ jsonrpc: "1.0", id: ++id, method, params }),
  });
  const body = await res.json();
  if (body.error) throw new Error(`bitcoind ${method}: ${JSON.stringify(body.error)}`);
  return body.result;
}

export async function sendToAddress(address, sats) {
  const txid = await rpc("sendtoaddress", [address, Number(sats) / 1e8], WALLET);
  return { id: txid };
}

export async function mine(n) {
  if (MINING !== "1") throw new Error("generatetoaddress needs MINING=1 (regtest only)");
  const addr = await rpc("getnewaddress", [], WALLET);
  await rpc("generatetoaddress", [n, addr]);
}

async function waitForConfirmations(txids, confs, timeoutMs = 30 * 60 * 1000) {
  const start = Date.now();
  const pending = new Set(txids);
  while (pending.size) {
    for (const txid of [...pending]) {
      try {
        const tx = await rpc("getrawtransaction", [txid, true]);
        if (tx && (tx.confirmations ?? 0) >= confs) pending.delete(txid);
      } catch {
        // Not yet visible; keep polling.
      }
    }
    if (!pending.size) break;
    if (Date.now() - start > timeoutMs) {
      throw new Error(`timeout waiting for ${confs} confs: ${[...pending].join(",")}`);
    }
    await new Promise((r) => setTimeout(r, 15000));
  }
}

export async function mineAndWait(n, txids = []) {
  if (MINING === "1") {
    await mine(n);
  } else if (txids.length) {
    await waitForConfirmations(txids, CONFS);
  }
  // Chain watcher + SO need a moment to ingest.
  await new Promise((r) => setTimeout(r, 4000));
}
