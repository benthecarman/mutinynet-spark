# Deploy runbook

## First boot order

1. `docker compose -f docker-compose.regtest.yml up -d` (or your MutinyNet
   equivalent providing bitcoind + SOs).
2. SSP generates `<SSP_DATA_DIR>/ssp.key` on first boot; read the matching
   `ssp_identity_pubkey` from `/health` and publish it as wallets'
   `sspClientOptions.identityPublicKey`. Back up `ssp-data` (key + sqlite).
3. Fund the sidecar liquidity wallet:
   `docker compose -f docker-compose.regtest.yml run --rm sidecar-fund`
   (binary ladder, dust-safe floor 1000 sats, multiplicity 3). Verify
   `/swap-sidecar:5001/health` shows available = owned.
4. LDK channels (`deploy/channels.sh`): `connect` to peers, `open` channels.
   Receives need inbound capacity (or JIT), sends need outbound. ldk-server
   uses the bitcoind backend; fund its on-chain wallet first
   (`onchain-receive` via the CLI).
5. Put Caddy in front (`deploy/docker-compose.edge.yml`) for TLS.

## Liquidity ops (recurring)

- Sidecar ladder denoms deplete as fills consume exact matches. Monitor
  `/swap-sidecar:5001/health` (`available` vs `owned`); re-run `sidecar-fund`
  to top up. Without exact denoms, fills fail and user swaps reject.
- LDK: rebalance/close via `deploy/channels.sh` (`list-channels`,
  `close-channel`). No autopilot by decision.

## Secrets

- `SSP_SECRET_KEY_HEX` (or `ssp-data/ssp.key`), `SIDECAR_TOKEN`,
  `LDK_API_KEY`, sidecar mnemonic (`sidecar-data/sidecar.mnemonic`): never
  commit, inject via env/files, back up the volumes.
- Rotate `SIDECAR_TOKEN` by restarting both ssp and swap-sidecar.

## Residual risks (not fixable in this repo)

- ldk-server has no custom-signet flag. If MutinyNet's genesis differs from
  public signet, ldk-server refuses it until patched upstream. The SSP runs
  in fake Lightning mode until live connects (see `/health ldk_mode`).
- SO leaf lifetimes are block-driven: on fast chains, wallets need regular
  syncs; the sidecar self-syncs per fill.
- No webhook push: wallets poll Transfers/UserRequest (by decision).
