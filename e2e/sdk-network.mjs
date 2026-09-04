const SDK_DIST = process.env.SPARK_SDK_DIST;
if (!SDK_DIST) throw new Error("set SPARK_SDK_DIST");

const { Network, SparkWallet } = await import(SDK_DIST);
const cases = [
  ["MAINNET", "MAINNET"],
  ["TESTNET", "TESTNET"],
  ["SIGNET", "SIGNET"],
  ["REGTEST", "REGTEST"],
  ["LOCAL", "REGTEST"],
];

for (const [walletNetwork, expected] of cases) {
  const actual = SparkWallet.prototype.toBitcoinNetwork.call({
    config: { getNetwork: () => Network[walletNetwork] },
  });
  if (actual !== expected) {
    throw new Error(`${walletNetwork} maps to ${actual}; expected ${expected}`);
  }
}

console.log("Spark SDK Bitcoin network mapping PASS");
