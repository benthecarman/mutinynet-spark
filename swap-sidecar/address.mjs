// Derive a Spark address from an identity pubkey with the same libraries
// the SDK uses (@bufbuild/protobuf BinaryWriter + @scure/base bech32m).
// Wire format: bech32m(hrp, proto SparkAddress{identity_public_key: bytes}).
// Self-test in e2e compares this against wallet.getSparkAddress().
import { BinaryWriter } from "@bufbuild/protobuf/wire";
import { bech32m } from "@scure/base";

const HRP = { MAINNET: "spark", TESTNET: "sparkt", REGTEST: "sparkrt", SIGNET: "sparks", LOCAL: "sparkl" };

export function sparkAddressFromIdentityPubkey(identityPubkeyHex, network = "LOCAL") {
  const pk = Buffer.from(identityPubkeyHex, "hex");
  if (pk.length !== 33 || (pk[0] !== 0x02 && pk[0] !== 0x03)) {
    throw new Error("identity pubkey must be 33-byte compressed secp256k1");
  }
  const w = new BinaryWriter();
  w.uint32(10).bytes(pk);
  const words = bech32m.toWords(w.finish());
  return bech32m.encode(HRP[network] ?? "sparkl", words, 1024);
}
