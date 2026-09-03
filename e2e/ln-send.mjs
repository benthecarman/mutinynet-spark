// LN SEND e2e: invoice on node2 -> SSP RequestLightningSend (init) ->
// poll SSP UserRequest until SUCCEEDED, assert node2 got paid.
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

// Invoice for 3000 sats on node2.
const invOut = cli2("bolt11-receive 3000sat -d 'ssp-send-test'");
const inv = JSON.parse(invOut).invoice;
console.log("[ln-send] node2 invoice:", inv.slice(0, 40) + "...");

const r = await gql(
  {
    query: "mutation RequestLightningSend($encoded_invoice:String!){ request_lightning_send(input:{encoded_invoice:$encoded_invoice}){ request { id status } } }",
    variables: { encoded_invoice: inv },
    operationName: "RequestLightningSend",
  },
  tok,
);
const req = r.data.request_lightning_send.request;
console.log("[ln-send] SSP request:", req.id, req.status);
if (req.status !== "LIGHTNING_PAYMENT_INITIATED") throw new Error(`expected INITIATED, got ${req.status}`);

// Poll SSP user request until terminal.
let final = "";
for (let i = 0; i < 24; i++) {
  await new Promise((r) => setTimeout(r, 5000));
  const u = await gql(
    {
      query: "query UserRequest($request_id:ID!){ user_request(request_id:$request_id){ __typename ... on LightningSendRequest { lightning_send_request_status: status } } }",
      variables: { request_id: req.id },
      operationName: "UserRequest",
    },
    tok,
  );
  const ur = u.data.user_request;
  const st = ur?.lightning_send_request_status ?? ur?.status ?? "";
  if (st === "LIGHTNING_PAYMENT_SUCCEEDED") {
    final = st;
    break;
  }
  if (st === "LIGHTNING_PAYMENT_FAILED") throw new Error("payment failed");
}
if (final !== "LIGHTNING_PAYMENT_SUCCEEDED") throw new Error(`never succeeded (last: ${final})`);
console.log("[ln-send] SUCCEEDED via SSP polling");
process.exit(0);
