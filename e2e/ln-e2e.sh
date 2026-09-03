#!/bin/bash
# LN send+receive e2e through OUR SSP on regtest.
# Topology: ldk-server (SSP backend) <-> channel <-> ldk-server-2 (payer/payee).
# Requires: compose stack up (ldk-server + ldk-server-2), bitcoind funded.
set -e
cd "$(dirname "$0")/.."

CLI1() { docker exec mutinynet-spark-ldk-server-1 sh -c "ldk-server-cli --base-url localhost:3536 --api-key \$(od -A n -t x1 /data/regtest/api_key | tr -d ' \n') --tls-cert /data/tls.crt $*"; }
CLI2() { docker exec mutinynet-spark-ldk-server-2-1 sh -c "ldk-server-cli --base-url localhost:3536 --api-key \$(od -A n -t x1 /data/regtest/api_key | tr -d ' \n') --tls-cert /data/tls.crt $*"; }
BTC() { docker exec mutinynet-spark-bitcoind-1 bitcoin-cli -regtest -rpcuser=testutil -rpcpassword=testutilpassword -rpcport=8332 "$@"; }

echo "=== node ids ==="
ID1=$(CLI1 get-node-info | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.parse(s).node_id))")
ID2=$(CLI2 get-node-info | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.parse(s).node_id))")
echo "node1: $ID1"; echo "node2: $ID2"

echo "=== fund both on-chain ==="
A1=$(CLI1 onchain-receive | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.parse(s).address))")
A2=$(CLI2 onchain-receive | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.parse(s).address))")
WALLET_BAL=$(BTC -rpcwallet=default getbalance)
echo "miner wallet: $WALLET_BAL"
BTC -rpcwallet=default sendtoaddress "$A1" 2 > /dev/null
BTC -rpcwallet=default sendtoaddress "$A2" 1 > /dev/null
MINER_ADDR=$(BTC -rpcwallet=default getnewaddress)
BTC generatetoaddress 6 "$MINER_ADDR" > /dev/null
echo "funded + mined 6, waiting for ldk wallets to sync..."
for i in $(seq 1 30); do
  B1=$(CLI1 get-balances 2>/dev/null | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.stringify(JSON.parse(s)))}catch{console.log('')}})" | grep -o '[0-9]\{7,\}' | head -1)
  if [ -n "$B1" ] && [ "$B1" -gt 100000000 ]; then echo "node1 onchain synced"; break; fi
  sleep 10
done

echo "=== connect + open channel 1->2 (2M sats, skip if exists) ==="
CLI1 connect-peer "$ID2" ldk-server-2:9735 --persist 2>&1 | head -2 || true
HAS_CH=$(CLI1 list-channels 2>/dev/null | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).channels?.length??0)}catch{console.log(0)}})")
if [ "${HAS_CH:-0}" = "0" ]; then
  CLI1 open-channel "$ID2" ldk-server-2:9735 2000000sat 2>&1 | head -3
  BTC generatetoaddress 6 "$MINER_ADDR" > /dev/null
  echo "channel opened + mined 6, waiting for ready..."
else
  echo "channel exists, waiting for ready..."
fi
for i in $(seq 1 30); do
  READY=$(CLI1 list-channels 2>/dev/null | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{const c=JSON.parse(s).channels??[];console.log(c.length&&c[0].is_channel_ready?'yes':'no')}catch{console.log('no')}})")
  [ "$READY" = "yes" ] && break
  sleep 10
done
[ "$READY" = "yes" ] || { echo "channel not ready"; exit 1; }
echo "channel ready"

echo "=== ensure node2 has outbound (500k sats) ==="
N2LN=$(CLI2 get-balances 2>/dev/null | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).total_lightning_balance_sats??0)}catch{console.log(0)}})")
if [ "${N2LN:-0}" -lt 500000 ]; then
  echo "node2 LN balance $N2LN, topping up..."
  INV2=$(CLI2 bolt11-receive 500000sat | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.parse(s).invoice))")
  CLI1 bolt11-send "$INV2" > /dev/null
  sleep 5
fi
echo "node2 funded"

echo "=== LN RECEIVE via SSP (mint -> hodl invoice -> pay -> auto-claim) ==="
export SSP_BASE_URL="${SSP_BASE_URL:-http://127.0.0.1:5000}"
node e2e/ln-receive.mjs

echo "=== LN SEND via SSP (init -> event -> SUCCEEDED) ==="
node e2e/ln-send.mjs

echo "LN E2E PASS"
