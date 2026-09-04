// E2E: self-hosted SSP + local regtest SOs, driven through the real JS SDK.
// Run: SPARK_SDK_DIST=/tmp/.../dist/index.node.js node e2e.mjs
// (or ./e2e.sh which sets everything). Regtest-only: mines its own blocks.
process.env.MINING ??= "1";

const SDK_DIST = process.env.SPARK_SDK_DIST;
if (!SDK_DIST) throw new Error("set SPARK_SDK_DIST to the built spark-sdk node entry");

const { SparkWallet } = await import(SDK_DIST);
import { sendToAddress, mineAndWait } from "./faucet.mjs";

const SSP_BASE_URL = process.env.SSP_BASE_URL ?? "http://127.0.0.1:5000";
const SPARK_ADMIN_TOKEN = process.env.SPARK_ADMIN_TOKEN;
if (!SPARK_ADMIN_TOKEN) throw new Error("set SPARK_ADMIN_TOKEN");
const statusOptions = {
  headers: { Authorization: `Bearer ${SPARK_ADMIN_TOKEN}` },
};
const health = await (await fetch(`${SSP_BASE_URL}/health`)).json();
if (health.status !== "ok") throw new Error("SSP unhealthy");
const identity = await (await fetch(`${SSP_BASE_URL}/identity`)).json();
if (!identity.identityPublicKey) throw new Error("SSP identity unavailable");
const status = await (await fetch(`${SSP_BASE_URL}/status`, statusOptions)).json();
console.log(`[e2e 0] SSP healthy: network=${status.network} ldk_mode=${status.ldk_mode}`);
const sparkBalanceBefore = BigInt(status.spark.available_sats);
const SSP = {
  baseUrl: SSP_BASE_URL,
  identityPublicKey: process.env.SSP_IDENTITY_PUBKEY ?? identity.identityPublicKey,
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

// A partial spend of a single 100,000-sat leaf must use RequestSwap and the
// funded SSP wallet. A full-leaf transfer would bypass the path under test.
await a.wallet.transfer({ amountSats: 50_000, receiverSparkAddress: addrB });
step(7, "transferred 50000 A -> B through a leaf swap");

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
await pollBalance(b.wallet, 50_000n, 8.1);
await pollBalance(a.wallet, 50_000n, 8.2);

for (let i = 0; i < 30; i++) {
  const current = await (await fetch(`${SSP_BASE_URL}/status`, statusOptions)).json();
  const available = BigInt(current.spark.available_sats);
  step(8.3, `SSP Spark balance poll ${i}: ${available} sats`);
  if (available === sparkBalanceBefore) break;
  if (i === 29) {
    throw new Error(
      `SSP did not reclaim swap input: ${available}; expected ${sparkBalanceBefore}`,
    );
  }
  await new Promise((resolve) => setTimeout(resolve, 2000));
}

// SSP-specific: static-deposit quote through OUR ssp (exercises quote signing).
const staticAddr = await a.wallet.getStaticDepositAddress();
const sent2 = await sendToAddress(staticAddr, 200_000n);
await mineAndWait(3);
const quote = await a.wallet.getClaimStaticDepositQuote(sent2.id);
step(9, `static quote: credit=${quote.creditAmountSats} sig=${quote.signature.slice(0, 16)}...`);
if (!quote.signature) throw new Error("empty SSP quote signature");

console.log("E2E PASS");
process.exit(0);
