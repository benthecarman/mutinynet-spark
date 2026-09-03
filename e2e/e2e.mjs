// E2E: self-hosted SSP + local regtest SOs, driven through the real JS SDK.
// Run: SPARK_SDK_DIST=/tmp/.../dist/index.node.js node e2e.mjs
// (or ./e2e.sh which sets everything). Regtest-only: mines its own blocks.
process.env.MINING ??= "1";

const SDK_DIST = process.env.SPARK_SDK_DIST;
if (!SDK_DIST) throw new Error("set SPARK_SDK_DIST to the built spark-sdk node entry");

const { SparkWallet } = await import(SDK_DIST);
import { sendToAddress, mineAndWait } from "./faucet.mjs";

const health = await (await fetch("http://127.0.0.1:5000/health")).json();
if (health.status !== "ok") throw new Error("SSP unhealthy");
console.log(`[e2e 0] SSP health: network=${health.network} ldk_mode=${health.ldk_mode} identity=${health.ssp_identity_pubkey}`);
const SSP = {
  baseUrl: process.env.SSP_BASE_URL ?? "http://127.0.0.1:5000",
  identityPublicKey: process.env.SSP_IDENTITY_PUBKEY ?? health.ssp_identity_pubkey,
  schemaEndpoint: "graphql/spark/rc",
};

const opts = () => ({
  network: "LOCAL",
  sspClientOptions: { ...SSP },
  optimizationOptions: { auto: false, multiplicity: 0 },
  tokenOptimizationOptions: { enabled: false },
});

const step = (n, msg) => console.log(`[e2e ${n}] ${msg}`);

const a = await SparkWallet.initialize({ options: opts() });
step(1, `wallet A ready: ${await a.wallet.getSparkAddress()}`);

// Sidecar address derivation must match the SDK exactly.
const { sparkAddressFromIdentityPubkey } = await import("../swap-sidecar/address.mjs");
const derived = sparkAddressFromIdentityPubkey(await a.wallet.getIdentityPublicKey(), "LOCAL");
if (derived !== (await a.wallet.getSparkAddress())) {
  throw new Error(`address derivation drift: ${derived}`);
}
step(1.1, "sidecar address derivation matches SDK");

const depositAddr = await a.wallet.getSingleUseDepositAddress();
step(2, `deposit address: ${depositAddr}`);

const sent = await sendToAddress(depositAddr, 100_000n);
step(3, `funded ${sent.id}`);
await mineAndWait(3);
step(4, "mined 3 blocks");

await a.wallet.claimDeposit(sent.id);
const balA = await a.wallet.getBalance();
step(5, `A balance: ${balA.balance} sats`);
if (balA.balance !== 100_000n) throw new Error(`unexpected A balance ${balA.balance}`);

const b = await SparkWallet.initialize({ options: opts() });
const addrB = await b.wallet.getSparkAddress();
step(6, `wallet B ready: ${addrB}`);

await a.wallet.transfer({ amountSats: 10_000, receiverSparkAddress: addrB });
step(7, "transferred 10000 A -> B (partial: SSP leaf swap via sidecar)");

const balB = await b.wallet.getBalance();
step(8, `B balance before sync: ${balB.balance} sats`);
await b.wallet.experimental_syncWallet();
// Background claim (event stream / periodic task) settles the inbound leaf.
async function pollBalance(w, want, tag) {
  for (let i = 0; i < 12; i++) {
    await new Promise((r) => setTimeout(r, 5000));
    const b = (await w.getBalance()).balance;
    step(tag, `balance poll ${i}: ${b} sats`);
    if (b === want) return b;
  }
  throw new Error(`balance never reached ${want}`);
}
await pollBalance(b.wallet, 10_000n, 8.1);
// Swap change (90000) arrives as a second inbound, claimed via background sync.
await pollBalance(a.wallet, 90_000n, 8.2);

// Full-balance direct transfer of what remains (no swap needed).
const balA3 = (await a.wallet.getBalance()).balance;
await a.wallet.transfer({ amountSats: Number(balA3), receiverSparkAddress: addrB });
step(8.3, `transferred remainder ${balA3} A -> B (direct)`);
await pollBalance(b.wallet, 100_000n, 8.4);

// SSP-specific: static-deposit quote through OUR ssp (exercises quote signing).
const staticAddr = await a.wallet.getStaticDepositAddress();
const sent2 = await sendToAddress(staticAddr, 200_000n);
await mineAndWait(3);
const quote = await a.wallet.getClaimStaticDepositQuote(sent2.id);
step(9, `static quote: credit=${quote.creditAmountSats} sig=${quote.signature.slice(0, 16)}...`);
if (!quote.signature) throw new Error("empty SSP quote signature");

console.log("E2E PASS");
process.exit(0);
