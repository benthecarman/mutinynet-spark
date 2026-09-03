#!/bin/bash
# Bring up the regtest stack, fund the swap sidecar, run the SDK e2e.
# Stack stays up on success.
set -e
cd "$(dirname "$0")/.."

export SPARK_REF="${SPARK_REF:-/tmp/opencode/spark-ref}"
export SPARK_DANGEROUSLY_DISABLE_TLS_VERIFICATION=1
export BITCOIN_RPC_URL="${BITCOIN_RPC_URL:-http://127.0.0.1:8332}"
export BITCOIN_RPC_USER="${BITCOIN_RPC_USER:-testutil}"
export BITCOIN_RPC_PASSWORD="${BITCOIN_RPC_PASSWORD:-testutilpassword}"

echo "=== compose up ==="
docker compose -f docker-compose.regtest.yml up --build -d

echo "=== wait for SOs (8535-8537) ==="
for i in $(seq 1 60); do
  if (echo > /dev/tcp/127.0.0.1/8535) 2>/dev/null && (echo > /dev/tcp/127.0.0.1/8536) 2>/dev/null && (echo > /dev/tcp/127.0.0.1/8537) 2>/dev/null; then
    echo "SO ports open"; break
  fi
  if [ "$i" = 60 ]; then echo "SOs did not come up"; docker compose -f docker-compose.regtest.yml logs --tail=30; exit 1; fi
  sleep 5
done

echo "=== wait for SSP ==="
for i in $(seq 1 30); do
  if curl -sf http://127.0.0.1:5000/health > /dev/null; then break; fi
  if [ "$i" = 30 ]; then echo "SSP not healthy"; exit 1; fi
  sleep 2
done
curl -s http://127.0.0.1:5000/health; echo

echo "=== swap sidecar up + funded ==="
docker compose -f docker-compose.regtest.yml up -d swap-sidecar
sleep 5
SIDECAR_BAL=$(curl -s http://127.0.0.1:5001/health | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).balance??0)}catch{console.log(0)}})") || SIDECAR_BAL=0
echo "sidecar balance: $SIDECAR_BAL"
if [ "${SIDECAR_BAL:-0}" = "0" ] || [ "${SIDECAR_BAL:-0}" = "null" ]; then
  echo "funding sidecar liquidity wallet..."
  docker compose -f docker-compose.regtest.yml run --rm sidecar-fund
fi

SDK_DIST="$SPARK_REF/sdks/js/packages/spark-sdk/dist/index.node.js"
if [ ! -f "$SDK_DIST" ]; then
  echo "SDK not built at $SDK_DIST (run yarn build:sdk in sdks/js first)"; exit 1
fi

echo "=== run e2e ==="
SPARK_SDK_DIST="$SDK_DIST" node e2e/e2e.mjs
