#!/bin/bash
# Bring up the regtest stack, fund the swap sidecar, run the SDK e2e.
# Stack stays up on success.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker-compose.regtest.yml)

failure_logs() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "=== E2E failure logs ===" >&2
    "${COMPOSE[@]}" ps -a >&2 || true
    "${COMPOSE[@]}" logs --tail=150 \
      bitcoind bitcoin-init spark-operator-0 spark-operator-1 \
      spark-operator-2 ldk-server ldk-server-2 ssp swap-sidecar >&2 || true
  fi
  exit "$status"
}
trap failure_logs EXIT

export SPARK_REF="${SPARK_REF:-/tmp/opencode/spark-ref}"
export SPARK_DANGEROUSLY_DISABLE_TLS_VERIFICATION=1
export MINING=1
export SIDECAR_TOKEN="${SIDECAR_TOKEN:-regtest-sidecar-token}"
if [ -z "${BITCOIN_RPC_PORT:-}" ]; then
  BITCOIN_RPC_PORT=$(node -e 'const net=require("net");const s=net.createServer();s.listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close()})')
fi
export BITCOIN_RPC_PORT
export BITCOIN_RPC_URL="${BITCOIN_RPC_URL:-http://127.0.0.1:$BITCOIN_RPC_PORT}"
export BITCOIN_RPC_USER="${BITCOIN_RPC_USER:-testutil}"
export BITCOIN_RPC_PASSWORD="${BITCOIN_RPC_PASSWORD:-testutilpassword}"

echo "=== compose up ==="
"${COMPOSE[@]}" up --build -d

echo "=== wait for SOs (8535-8537) ==="
for i in $(seq 1 60); do
  if (echo > /dev/tcp/127.0.0.1/8535) 2>/dev/null && (echo > /dev/tcp/127.0.0.1/8536) 2>/dev/null && (echo > /dev/tcp/127.0.0.1/8537) 2>/dev/null; then
    echo "SO ports open"; break
  fi
  if [ "$i" = 60 ]; then echo "SOs did not come up"; exit 1; fi
  sleep 5
done

echo "=== wait for SO signing keyshares ==="
for attempt in $(seq 1 120); do
  keyshares_ready=1
  for index in 0 1 2; do
    count=""
    if ! count=$("${COMPOSE[@]}" exec -T postgres psql \
      -U postgres -d "sparkoperator_${index}" -tAc \
      "SELECT count(*) FROM signing_keyshares WHERE status = 'AVAILABLE';" \
      2>/dev/null); then
      keyshares_ready=0
      break
    fi
    count=$(echo "$count" | tr -d '[:space:]')
    case "$count" in
      ''|*[!0-9]*|0) keyshares_ready=0; break ;;
    esac
  done
  if [ "$keyshares_ready" = "1" ]; then
    echo "SO signing keyshares ready"
    break
  fi
  if [ "$attempt" = 120 ]; then
    echo "SO signing keyshares did not become ready"
    exit 1
  fi
  sleep 5
done

echo "=== wait for SSP ==="
for i in $(seq 1 90); do
  if curl -sf http://127.0.0.1:5000/health > /dev/null; then break; fi
  if [ "$i" = 90 ]; then echo "SSP not healthy"; exit 1; fi
  sleep 4
done
curl -s http://127.0.0.1:5000/health; echo

echo "=== swap sidecar up + funded ==="
"${COMPOSE[@]}" up -d swap-sidecar
SIDECAR_JSON=""
for i in $(seq 1 60); do
  if SIDECAR_JSON=$(curl --fail --silent --show-error --max-time 15 \
    http://127.0.0.1:5001/health); then
    break
  fi
  if [ "$i" = 60 ]; then echo "swap sidecar not healthy"; exit 1; fi
  sleep 5
done
SIDECAR_BAL=$(echo "$SIDECAR_JSON" | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{const j=JSON.parse(s);console.log(j.breakdown?.available ?? j.balance ?? 0)}catch{console.log(0)}})") || SIDECAR_BAL=0
SIDECAR_TOPUP=$(echo "$SIDECAR_JSON" | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).needsTopup===true?'yes':'no')}catch{console.log('no')}})") || SIDECAR_TOPUP=no
case "$SIDECAR_BAL" in
  ''|*[!0-9]*) SIDECAR_BAL=0 ;;
esac
echo "sidecar available: $SIDECAR_BAL topup_flag: $SIDECAR_TOPUP"
# Ladder denoms deplete as fills consume exact matches; top up well before
# empty (failed fills lock leaves SO-side and strand liquidity).
if [ "${SIDECAR_BAL:-0}" = "0" ] || [ "${SIDECAR_BAL:-0}" = "null" ] || [ "${SIDECAR_BAL:-0}" -lt 10000000 ] || [ "$SIDECAR_TOPUP" = "yes" ]; then
  echo "funding/topping up sidecar liquidity wallet..."
  "${COMPOSE[@]}" run --rm sidecar-fund
fi

# SSP resolves the sidecar identity in the background. Verify alignment.
SIDECAR_ID=$(curl -s http://127.0.0.1:5001/health | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).identityPubkey??'')}catch{console.log('')}})")
echo "sidecar identity: $SIDECAR_ID"
for i in $(seq 1 30); do
  SSP_ID=$(curl -s http://127.0.0.1:5000/health | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).ssp_identity_pubkey??'')}catch{console.log('')}})")
  if [ -n "$SSP_ID" ] && [ "$SSP_ID" = "$SIDECAR_ID" ]; then echo "SSP identity aligned: $SSP_ID"; break; fi
  if [ "$i" = 30 ]; then echo "SSP identity mismatch: ssp=$SSP_ID sidecar=$SIDECAR_ID"; exit 1; fi
  sleep 4
done

SDK_DIST="$SPARK_REF/sdks/js/packages/spark-sdk/dist/index.node.js"
if [ ! -f "$SDK_DIST" ]; then
  echo "SDK not built at $SDK_DIST (run yarn build:sdk in sdks/js first)"; exit 1
fi
npm ci --prefix swap-sidecar --omit=dev --no-audit --no-fund

echo "=== run e2e ==="
SPARK_SDK_DIST="$SDK_DIST" node e2e/e2e.mjs

echo "=== run Lightning e2e ==="
SPARK_SDK_DIST="$SDK_DIST" ./e2e/ln-e2e.sh
