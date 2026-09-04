# Self-contained swap sidecar: builds the pinned spark-sdk inside, then runs
# the SSP sidecar server. Override the source and pin with build arguments.
ARG SPARK_REPO=https://github.com/benthecarman/spark
ARG SPARK_REF=5a1fc3201c0d8611479d3bafdfb43b026cc1e876

FROM node:22-bookworm AS sdk-build
ARG SPARK_REPO
ARG SPARK_REF
RUN apt-get update && apt-get install -y git clang lld python3 make g++ && rm -rf /var/lib/apt/lists/*
RUN corepack enable && corepack prepare yarn@4.13.0 --activate
RUN git clone ${SPARK_REPO} /spark \
 && cd /spark && git checkout ${SPARK_REF}
WORKDIR /spark/sdks/js
RUN yarn install && yarn build:sdk \
 && yarn workspaces focus @buildonspark/spark-sdk --production \
 && rm -rf /spark/sdks/js/node_modules/.cache 2>/dev/null || true

FROM node:22-bookworm-slim AS final
WORKDIR /app
COPY swap-sidecar/package.json swap-sidecar/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY swap-sidecar/server.mjs swap-sidecar/address.mjs swap-sidecar/fund.mjs ./
COPY e2e/faucet.mjs ./faucet.mjs
COPY swap-sidecar/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY --from=sdk-build /spark/sdks/js/packages/spark-sdk/dist /sdk
COPY --from=sdk-build /spark/sdks/js/node_modules /node_modules
RUN install -d -o node -g node /app/data
ENV SPARK_SDK_DIST=/sdk/index.node.js
USER node
EXPOSE 5001
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:5001/health').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
ENTRYPOINT ["/entrypoint.sh"]
CMD ["node", "server.mjs"]
