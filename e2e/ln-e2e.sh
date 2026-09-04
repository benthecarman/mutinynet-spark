#!/bin/bash
# Hermetic Lightning E2E with two Breez SDK wallets and two SSP instances.
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT_NAME=${BREEZ_E2E_PROJECT_NAME:-mutinynet-ssp-breez-e2e}
COMPOSE=(docker compose -p "$PROJECT_NAME" -f docker-compose.regtest.yml)
export COMPOSE_PROGRESS=${COMPOSE_PROGRESS:-plain}
export COMPOSE_BAKE=${COMPOSE_BAKE:-false}
export SPARK_ADMIN_TOKEN=${SPARK_ADMIN_TOKEN:-regtest-spark-admin-token}
SPARK_SOURCE_REF=${SPARK_REF:-/tmp/opencode/spark-ref}
export LDK_SERVER_REF=${LDK_SERVER_REF:-/tmp/opencode/ldk-server-ref}
export BITCOIN_RPC_USER=${BITCOIN_RPC_USER:-testutil}
export BITCOIN_RPC_PASSWORD=${BITCOIN_RPC_PASSWORD:-testutilpassword}
export BITCOIN_RPC_WALLET=${BITCOIN_RPC_WALLET:-default}
export BITCOIN_RPC_URL=${BITCOIN_RPC_URL:-http://127.0.0.1:8332}
export BREEZ_CHAIN_SERVICE_URL=${BREEZ_CHAIN_SERVICE_URL:-http://127.0.0.1:30000}

command -v docker >/dev/null
command -v git >/dev/null
command -v cargo >/dev/null
test -d "$SPARK_SOURCE_REF"
test -d "$LDK_SERVER_REF"

TLS_DIR=$(mktemp -d)
SPARK_WORKTREE_ROOT=$(mktemp -d)
SPARK_CLEAN_REF="$SPARK_WORKTREE_ROOT/spark"
SPARK_COMMIT=${SPARK_OPERATOR_COMMIT:-HEAD}
SPARK_WORKTREE_READY=0
KEEP_STACK=${KEEP_BREEZ_E2E_STACK:-0}

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "=== Breez Lightning E2E failure logs ===" >&2
    "${COMPOSE[@]}" ps -a >&2 || true
    "${COMPOSE[@]}" logs --tail=250 \
      spark-operator-0 spark-operator-1 spark-operator-2 \
      ldk-server ldk-server-2 ssp ssp-2 >&2 || true
  fi
  rm -rf "$TLS_DIR"
  if [ "$KEEP_STACK" != "1" ]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  if [ "$SPARK_WORKTREE_READY" = "1" ]; then
    git -C "$SPARK_SOURCE_REF" worktree remove --force \
      "$SPARK_CLEAN_REF" >/dev/null 2>&1 || true
  fi
  rmdir "$SPARK_WORKTREE_ROOT" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

git -C "$SPARK_SOURCE_REF" worktree add --detach \
  "$SPARK_CLEAN_REF" "$SPARK_COMMIT" >/dev/null
SPARK_WORKTREE_READY=1
export SPARK_REF=$SPARK_CLEAN_REF

echo "=== reset isolated regtest stack ==="
echo "Spark operator commit: $(git -C "$SPARK_REF" rev-parse HEAD)"
"${COMPOSE[@]}" down -v --remove-orphans

echo "=== start Bitcoin, operators, and Lightning nodes ==="
"${COMPOSE[@]}" up --build -d \
  postgres bitcoind bitcoin-init bitcoin-miner electrs cert-init \
  spark-operator-0 spark-operator-1 spark-operator-2 \
  ldk-server ldk-server-2

echo "=== wait for the local chain service ==="
for attempt in $(seq 1 120); do
  if curl --fail --silent "$BREEZ_CHAIN_SERVICE_URL/blocks/tip/height" >/dev/null; then
    break
  fi
  if [ "$attempt" = "120" ]; then
    echo "Electrs did not become ready" >&2
    exit 1
  fi
  sleep 2
done

echo "=== wait for Spark signing keyshares ==="
for attempt in $(seq 1 120); do
  ready=1
  for index in 0 1 2; do
    count=$("${COMPOSE[@]}" exec -T postgres psql \
      -U postgres -d "sparkoperator_${index}" -tAc \
      "SELECT count(*) FROM signing_keyshares WHERE status = 'AVAILABLE';" \
      2>/dev/null || true)
    count=$(echo "$count" | tr -d '[:space:]')
    case "$count" in
      ''|*[!0-9]*|0) ready=0 ;;
    esac
  done
  if [ "$ready" = "1" ]; then
    break
  fi
  if [ "$attempt" = "120" ]; then
    echo "Spark signing keyshares did not become ready" >&2
    exit 1
  fi
  sleep 5
done

echo "=== start both SSP instances ==="
"${COMPOSE[@]}" build ssp
"${COMPOSE[@]}" up --no-build --no-deps -d ssp ssp-2
for port in 5000 5001; do
  for attempt in $(seq 1 90); do
    if curl --fail --silent "http://127.0.0.1:${port}/health" >/dev/null; then
      break
    fi
    if [ "$attempt" = "90" ]; then
      echo "SSP on port ${port} did not become healthy" >&2
      exit 1
    fi
    sleep 4
  done
done

LDK1=$("${COMPOSE[@]}" ps -q ldk-server)
LDK2=$("${COMPOSE[@]}" ps -q ldk-server-2)
if [ -z "$LDK1" ] || [ -z "$LDK2" ]; then
  echo "Lightning containers are not running" >&2
  exit 1
fi
export LDK1_CONTAINER=$LDK1
export LDK2_CONTAINER=$LDK2

echo "=== copy operator trust certificates ==="
for index in 0 1 2; do
  "${COMPOSE[@]}" cp "cert-init:/tls/server_${index}.crt" "$TLS_DIR/server_${index}.crt"
done
export BREEZ_OPERATOR_CERT_DIR=$TLS_DIR

echo "=== run Breez SDK send and receive ==="
cargo run --locked --manifest-path e2e/breez/Cargo.toml
