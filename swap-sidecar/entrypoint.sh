#!/bin/sh
# Bundle every SO server cert found under /tls as the trust root the SDK
# loads at /tmp/minikube-ca.pem when SPARK_LOCAL_INGRESS_HOST is set
# (spark-sdk wallet-config.ts). Works with server_0.crt and server.crt names.
set -e
: > /tmp/minikube-ca.pem
found=0
for f in $(find /tls -maxdepth 2 -name '*.crt' 2>/dev/null | sort); do
  # Skip CA bundles that are not server certs is unnecessary; concat all.
  cat "$f" >> /tmp/minikube-ca.pem
  found=1
done
if [ "$found" = "1" ] && [ -s /tmp/minikube-ca.pem ]; then
  echo "CA bundle ready"
else
  echo "WARNING: no SO certs under /tls; TLS to operators will fail"
fi
exec "$@"
