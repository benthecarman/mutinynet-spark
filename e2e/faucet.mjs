// Minimal regtest faucet over bitcoind RPC (no SDK test-utils needed).
const URL = process.env.BITCOIN_RPC_URL ?? "http://127.0.0.1:8332";
const USER = process.env.BITCOIN_RPC_USER ?? "testutil";
const PASS = process.env.BITCOIN_RPC_PASSWORD ?? "testutilpassword";
const WALLET = process.env.BITCOIN_RPC_WALLET ?? "default";

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
  const addr = await rpc("getnewaddress", [], WALLET);
  await rpc("generatetoaddress", [n, addr]);
}

export async function mineAndWait(n) {
  await mine(n);
  // Chain watcher + SO need a moment to ingest.
  await new Promise((r) => setTimeout(r, 4000));
}
