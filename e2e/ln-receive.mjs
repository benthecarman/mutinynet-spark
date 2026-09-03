// LN RECEIVE e2e: SSP-minted hash -> hodl invoice -> node2 pays ->
// SSP auto-claims on PaymentClaimable -> settled.
// Asserts: send ok + node1 sees the payment by hash.
import { execSync } from "node:child_process";
import { secp256k1 } from "/tmp/opencode/spark-ref/sdks/js/node_modules/@noble/curves/secp256k1.js";
import { sha256 } from "/tmp/opencode/spark-ref/sdks/js/node_modules/@noble/hashes/sha2.js";
import { bytesToHex } from "/tmp/opencode/spark-ref/sdks/js/node_modules/@noble/curves/utils.js";
import { randomBytes } from "node:crypto";

const B = process.env.SSP_BASE_URL ?? "http://127.0.0.1:5000";
const GQL = `${B}/graphql/spark/rc`;
const gql = (body, tok) =>
  fetch(GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(tok ? { Authorization: "Bearer " + tok } : {}) },
    body: JSON.stringify(body),
  }).then((r) => r.json());

const cli2 = (args) =>
  execSync(
    `docker exec mutinynet-spark-ldk-server-2-1 sh -c 'ldk-server-cli --base-url localhost:3536 --api-key $(od -A n -t x1 /data/regtest/api_key | tr -d '"'"' \\n'"'"') --tls-cert /data/tls.crt ${args}'`,
    { encoding: "utf8", shell: "/bin/bash" },
  );
const cli1 = (args) =>
  execSync(
    `docker exec mutinynet-spark-ldk-server-1 sh -c 'ldk-server-cli --base-url localhost:3536 --api-key $(od -A n -t x1 /data/regtest/api_key | tr -d '"'"' \\n'"'"') --tls-cert /data/tls.crt ${args}'`,
    { encoding: "utf8", shell: "/bin/bash" },
  );

const priv = randomBytes(32);
const pub = bytesToHex(secp256k1.getPublicKey(priv, true));
const ch = await gql({
  query: "mutation GetChallenge($public_key: PublicKey!){ get_challenge(input:{public_key:$public_key}){ protected_challenge } }",
  variables: { public_key: pub },
  operationName: "GetChallenge",
});
const pc = ch.data.get_challenge.protected_challenge;
const sig = secp256k1.sign(sha256(Buffer.from(pc, "base64")), priv);
const vv = await gql({
  query: "mutation VerifyChallenge($protected_challenge:String! $signature:String! $identity_public_key:PublicKey!){ verify_challenge(input:{protected_challenge:$protected_challenge signature:$signature identity_public_key:$identity_public_key}){ session_token } }",
  variables: {
    protected_challenge: pc,
    signature: Buffer.from(sig.toDERRawBytes()).toString("base64"),
    identity_public_key: pub,
  },
  operationName: "VerifyChallenge",
});
const tok = vv.data.verify_challenge.session_token;

const mint = await gql(
  {
    query: "mutation MintInvoicePreimage { mint_invoice_preimage { payment_hash } }",
    variables: {},
    operationName: "MintInvoicePreimage",
  },
  tok,
);
const H = mint.data.mint_invoice_preimage.payment_hash;
console.log("[ln-receive] minted hash:", H.slice(0, 16) + "...");

const r = await gql(
  {
    query: "mutation RequestLightningReceive($network:BitcoinNetwork! $amount_sats:Long! $payment_hash:Hash32!){ request_lightning_receive(input:{network:$network amount_sats:$amount_sats payment_hash:$payment_hash}){ request { id invoice { encoded_invoice } } } }",
    variables: { network: "REGTEST", amount_sats: 5000, payment_hash: H },
    operationName: "RequestLightningReceive",
  },
  tok,
);
const inv = r.data.request_lightning_receive.request.invoice.encoded_invoice;
console.log("[ln-receive] invoice:", inv.slice(0, 40) + "...");

console.log("[ln-receive] node2 paying...");
cli2(`bolt11-send '${inv}'`);
console.log("[ln-receive] send accepted, waiting for settle...");

// Poll node1 for the payment by hash (auto-claimed by SSP).
let settled = false;
for (let i = 0; i < 24; i++) {
  await new Promise((r) => setTimeout(r, 5000));
  const out = cli1("list-payments");
  if (out.toLowerCase().includes(H.slice(0, 32).toLowerCase())) {
    settled = true;
    break;
  }
}
if (!settled) throw new Error("node1 never saw the payment settle");
console.log("[ln-receive] SETTLED on node1");
process.exit(0);
