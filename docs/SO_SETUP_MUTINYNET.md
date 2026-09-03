# SO setup (any network, then MutinyNet)

The SSP needs the open-source Spark Operator (SO) + signer running.
This repo does NOT vendor the SO; use `spark-ref` in `/tmp/opencode/spark-ref`.

The keys in `e2e/upstream/` are test fixtures. Never reuse them on a network
that carries value.

## 1. Regtest first (proves SSP wiring)

```sh
cd /tmp/opencode/spark-ref
mise trust && mise install
./run-everything.sh   # starts bitcoind regtest + postgres + 3 SOs + signer, tmux `operator`
```

SO endpoints: check `so_config.yaml` / `docker/operator.config.yaml`.
Faucet: `request_regtest_funds` SSP mutation (stubbed here) or repo script.

## 2. Point the JS SDK at your SSP

In the SDK network config (`dev-regtest-config.json`):

```json
"sspClientOptions": {
  "baseUrl": "http://127.0.0.1:5000",
  "schemaEndpoint": "graphql/spark/rc",
  "identityPublicKey": "<SSP_IDENTITY_PUBKEY from your SSP /health>"
}
```

`identityPublicKey` MUST equal your SSP key or SO rejects transfers
(`receiver == sspIdentityPubkey` check in `spark-wallet.ts`).

## 3. MutinyNet (custom signet)

1. Get MutinyNet connection details: signet challenge, esplora/electrum URL,
   faucet. MutinyNet is a custom signet, so the genesis differs from public signet.
2. Run bitcoind with MutinyNet `-signet -signetchallenge=... -addnode=...`,
   plus ZMQ `zmqpubrawblock`.
3. Configure SO for that chain: copy `so.template.config.yaml`, set network +
   bitcoind host/user/pass + postgres DBs. Run one SO + signer per container
   (`docker-compose.yml` has the pattern; duplicate the operator block per SO).
4. ldk-server gap: no custom-signet flag today (see `docs/LDK_GAPS.md`).
   Use bitcoind backend pointed at your MutinyNet bitcoind; if ldk-server
   rejects the genesis, patch `Network` handling or run SSP in fake mode until
   the patch lands.
5. Set `SSP_NETWORK=SIGNET` (or `MUTINYNET` label your SDK config understands;
   wire value must stay a valid `BitcoinNetwork` for the SO: use `SIGNET`).

## 4. Verify

```sh
SSP_LISTEN_ADDR=127.0.0.1:5000 cargo run
curl http://127.0.0.1:5000/health
# spark-sdk CLI: getChallenge -> verify -> lightningReceiveQuote -> requestReceive
# -> pay invoice from second wallet -> getTransferFromSsp
```
