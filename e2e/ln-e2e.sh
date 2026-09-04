#!/bin/bash
# Full Lightning test suite for the SSP. The compose stack must be running.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker-compose.regtest.yml)
export SPARK_ADMIN_TOKEN="${SPARK_ADMIN_TOKEN:-regtest-spark-admin-token}"
export SSP_BASE_URL="${SSP_BASE_URL:-http://127.0.0.1:5000}"
export SPARK_DANGEROUSLY_DISABLE_TLS_VERIFICATION="${SPARK_DANGEROUSLY_DISABLE_TLS_VERIFICATION:-1}"
export SPARK_REF="${SPARK_REF:-/tmp/opencode/spark-ref}"
export SDK_REF="${SDK_REF:-$SPARK_REF}"
export SPARK_SDK_DIST="${SPARK_SDK_DIST:-$SDK_REF/sdks/js/packages/spark-sdk/dist/index.node.js}"
export LN_RECEIVE_AMOUNT_SATS="${LN_RECEIVE_AMOUNT_SATS:-5000}"
CONCURRENCY=${LN_RECEIVE_CONCURRENCY:-2}
RECEIVE_CASES=$((CONCURRENCY + 4))
if [ "$LN_RECEIVE_AMOUNT_SATS" -lt 1000 ]; then
  echo "LN_RECEIVE_AMOUNT_SATS must be at least 1000 for a Spark leaf" >&2
  exit 1
fi

LDK1=$("${COMPOSE[@]}" ps -q ldk-server)
LDK2=$("${COMPOSE[@]}" ps -q ldk-server-2)
BITCOIND=$("${COMPOSE[@]}" ps -q bitcoind)
SSP=$("${COMPOSE[@]}" ps -q ssp)
if [ -z "$LDK1" ] || [ -z "$LDK2" ] || [ -z "$BITCOIND" ] || [ -z "$SSP" ]; then
  echo "compose services are not running" >&2
  exit 1
fi
if [ ! -f "$SPARK_SDK_DIST" ]; then
  echo "Spark SDK is not built at $SPARK_SDK_DIST" >&2
  exit 1
fi
export LDK1_CONTAINER="$LDK1"
export LDK2_CONTAINER="$LDK2"
export SSP_CONTAINER="$SSP"

failure_logs() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "=== Lightning e2e failure logs ===" >&2
    "${COMPOSE[@]}" logs --tail=150 ssp ldk-server ldk-server-2 >&2 || true
  fi
  exit "$status"
}
trap failure_logs EXIT

ldk_key() {
  docker exec "$1" sh -c "od -A n -t x1 /data/regtest/api_key | tr -d ' \\n'"
}
LDK1_KEY=$(ldk_key "$LDK1")
LDK2_KEY=$(ldk_key "$LDK2")
CLI1=(timeout 60 docker exec "$LDK1" ldk-server-cli --base-url localhost:3536 --api-key "$LDK1_KEY" --tls-cert /data/tls.crt)
CLI2=(timeout 60 docker exec "$LDK2" ldk-server-cli --base-url localhost:3536 --api-key "$LDK2_KEY" --tls-cert /data/tls.crt)
BTC=(timeout 60 docker exec "$BITCOIND" bitcoin-cli -regtest -rpcuser=testutil -rpcpassword=testutilpassword -rpcport=8332)

json_field() {
  local expression=$1
  shift
  node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);const v=($expression);process.stdout.write(String(v??''))})" "$@"
}

wait_until() {
  local label=$1
  local attempts=$2
  shift 2
  for _ in $(seq 1 "$attempts"); do
    if "$@"; then
      echo "$label"
      return 0
    fi
    sleep 5
  done
  echo "timed out: $label" >&2
  return 1
}

node_info_ready() {
  "${CLI1[@]}" get-node-info >/dev/null 2>&1
}

node1_funded() {
  local balance
  balance=$("${CLI1[@]}" get-balances 2>/dev/null | json_field 'j.spendable_onchain_balance_sats') || return 1
  [ "${balance:-0}" -gt 100000000 ]
}

channel_ready() {
  "${CLI1[@]}" list-channels 2>/dev/null | node -e '
    let s="";
    process.stdin.on("data", d => s += d).on("end", () => {
      const peer = process.argv[1];
      const channels = JSON.parse(s).channels ?? [];
      process.exit(channels.some(c => c.counterparty_node_id === peer && c.is_channel_ready) ? 0 : 1);
    });
  ' "$ID2"
}

node2_outbound_ready() {
  local balance
  balance=$("${CLI2[@]}" get-balances 2>/dev/null | json_field 'j.total_lightning_balance_sats') || return 1
  [ "${balance:-0}" -ge 500000 ]
}

echo "=== prepare Lightning nodes ==="
ID1=$("${CLI1[@]}" get-node-info | json_field 'j.node_id')
ID2=$("${CLI2[@]}" get-node-info | json_field 'j.node_id')
[ -n "$ID1" ] && [ -n "$ID2" ] || { echo "LDK node IDs are missing" >&2; exit 1; }
echo "node1: $ID1"
echo "node2: $ID2"

A1=$("${CLI1[@]}" onchain-receive | json_field 'j.address')
A2=$("${CLI2[@]}" onchain-receive | json_field 'j.address')
"${BTC[@]}" -rpcwallet=default sendtoaddress "$A1" 2 >/dev/null
"${BTC[@]}" -rpcwallet=default sendtoaddress "$A2" 1 >/dev/null
MINER_ADDR=$("${BTC[@]}" -rpcwallet=default getnewaddress)
"${BTC[@]}" generatetoaddress 6 "$MINER_ADDR" >/dev/null
wait_until "node1 on-chain balance is ready" 60 node1_funded

"${CLI1[@]}" connect-peer "$ID2" ldk-server-2:9735 --persist >/dev/null 2>&1 || true
CHANNEL_COUNT=$("${CLI1[@]}" list-channels | json_field '(j.channels??[]).filter(c=>c.counterparty_node_id===process.argv[1]).length' "$ID2")
if [ "${CHANNEL_COUNT:-0}" -eq 0 ]; then
  "${CLI1[@]}" open-channel "$ID2" ldk-server-2:9735 2000000sat >/dev/null
  "${BTC[@]}" generatetoaddress 6 "$MINER_ADDR" >/dev/null
fi
wait_until "exact node1-to-node2 channel is ready" 60 channel_ready

if ! node2_outbound_ready; then
  INV2=$("${CLI2[@]}" bolt11-receive 500000sat -d bootstrap | json_field 'j.invoice')
  "${CLI1[@]}" bolt11-send "$INV2" >/dev/null
fi
wait_until "node2 outbound Lightning balance is ready" 60 node2_outbound_ready

echo "=== provision exact Spark receive liquidity ==="
FUND_LADDER="$LN_RECEIVE_AMOUNT_SATS" \
  FUND_MULTIPLICITY="$RECEIVE_CASES" \
  node e2e/fund-ssp.mjs

echo "=== public SDK receive ==="
node e2e/ln-receive.mjs

echo "=== public SDK send and idempotent retry ==="
node e2e/ln-send.mjs

echo "=== authorization, validation, funding, and expiry failures ==="
node e2e/ln-negative.mjs

IDLE_SECONDS=${LN_STREAM_IDLE_SECONDS:-95}
echo "=== event stream idle test (${IDLE_SECONDS}s) ==="
sleep "$IDLE_SECONDS"
node e2e/ln-receive.mjs

echo "=== LDK restart and stream reconnect ==="
docker restart "$LDK1" >/dev/null
wait_until "LDK server is ready after restart" 60 node_info_ready
wait_until "channel is ready after LDK restart" 60 channel_ready
node e2e/ln-receive.mjs

echo "=== missed-event reconciliation after SSP restart ==="
node e2e/ln-reconcile.mjs

echo "=== concurrent receive requests ==="
pids=()
for _ in $(seq 1 "$CONCURRENCY"); do
  node e2e/ln-receive.mjs &
  pids+=("$!")
done
for pid in "${pids[@]}"; do
  wait "$pid"
done

echo "LN E2E PASS"
