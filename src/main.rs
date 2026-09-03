use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tracing::info;

mod auth;
mod config;
mod db;
mod graphql;
mod ldk;

use config::Config;
use db::Db;
use ldk::{Backend, LdkBackend};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Arc<Db>,
    pub ldk: Arc<Backend>,
    /// Hex secret (resolved) + pubkey for SSP signatures.
    pub ssp_secret_hex: String,
    pub ssp_pubkey_hex: String,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub identity_pubkey: String,
    pub valid_until: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct GraphqlRequest {
    pub query: String,
    #[serde(default)]
    pub variables: serde_json::Value,
    #[serde(default)]
    #[allow(non_snake_case)]
    pub operationName: Option<String>,
}

#[derive(Debug, Serialize)]
struct GraphqlResponse {
    data: serde_json::Value,
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "network": state.config.network,
        "ssp_identity_pubkey": state.ssp_pubkey_hex,
        "ldk_mode": if state.ldk.live_node_id().is_some() { "live" } else { "fake" },
        "ldk_node_id": state.ldk.live_node_id(),
    }))
}

async fn graphql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // The SDK's Requester deflate-compresses bodies >1024 bytes
    // (CompressionStream exists in Node 18+), so decode first.
    let raw: Vec<u8> = if headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .contains("deflate")
    {
        match inflate_raw_deflate(&body) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("deflate decode failed: {e}");
                let err = serde_json::json!({ "errors": [{ "message": "bad deflate body" }] });
                return (StatusCode::OK, Json(err)).into_response();
            }
        }
    } else {
        body.to_vec()
    };
    let body = match String::from_utf8(raw) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("non-utf8 graphql body: {e}");
            let err = serde_json::json!({ "errors": [{ "message": "bad graphql body" }] });
            return (StatusCode::OK, Json(err)).into_response();
        }
    };
    let req: GraphqlRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                "unparseable graphql body: {e}; head={:?}",
                &body[..body.len().min(300)]
            );
            let err =
                serde_json::json!({ "errors": [{ "message": format!("bad graphql body: {e}") }] });
            return (StatusCode::OK, Json(err)).into_response();
        }
    };
    let op = detect_operation(&req);
    info!(op = %op, "ssp graphql call");
    match graphql::dispatch(state, &headers, &op, &req).await {
        Ok(mut data) => {
            // The SDK's generated documents alias every field
            // (`foo_bar: foo`). Real GraphQL servers echo the alias names;
            // our canonical resolver output uses raw schema names, so rewrite
            // keys per the query's own `alias: field` pairs.
            graphql::apply_query_aliases(&mut data, &req.query);
            (StatusCode::OK, Json(GraphqlResponse { data })).into_response()
        }
        Err(e) => {
            let body = serde_json::json!({ "errors": [{ "message": e.to_string() }] });
            (StatusCode::OK, Json(body)).into_response()
        }
    }
}

/// Inflate what CompressionStream("deflate") emits (zlib-wrapped deflate).
/// Falls back to raw deflate for other producers.
fn inflate_raw_deflate(input: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut d = flate2::read::ZlibDecoder::new(input);
    let mut out = Vec::with_capacity(input.len() * 2);
    match d.read_to_end(&mut out) {
        Ok(_) => Ok(out),
        Err(e) => {
            let mut r = flate2::read::DeflateDecoder::new(input);
            let mut raw = Vec::with_capacity(input.len() * 2);
            r.read_to_end(&mut raw).map_err(|_| e.to_string())?;
            Ok(raw)
        }
    }
}

/// Fetch the sidecar wallet identity (with retries; sidecar may boot later).
async fn fetch_sidecar_identity(sidecar_url: &str, token: &str) -> Result<String, String> {
    let mut req = reqwest::Client::new().get(format!("{sidecar_url}/health"));
    if !token.is_empty() {
        req = req.bearer_auth(token);
    }
    let mut last = "unreachable".to_string();
    for _ in 0..24 {
        match req
            .try_clone()
            .unwrap()
            .send()
            .await
            .map_err(|e| e.to_string())
        {
            Ok(resp) => {
                let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
                if let Some(pk) = body.get("identityPubkey").and_then(|v| v.as_str()) {
                    if !pk.is_empty() {
                        return Ok(pk.to_string());
                    }
                }
                last = "sidecar /health has no identityPubkey yet".to_string();
            }
            Err(e) => last = e,
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    Err(last)
}

/// Detect the GraphQL operation from operationName or query text.
/// The JS SDK sends raw documents like `mutation RequestLightningSend(...)`.
fn detect_operation(req: &GraphqlRequest) -> String {
    if let Some(name) = &req.operationName {
        if !name.is_empty() {
            return name.clone();
        }
    }
    // Fallback: first `query Foo` / `mutation Foo` token in the document.
    let q = req.query.replace(['\n', '\r', '{', '(', ')'], " ");
    let mut prev = "";
    for tok in q.split_whitespace() {
        if prev == "query" || prev == "mutation" {
            return tok.to_string();
        }
        prev = tok;
    }
    // Last resort substring match (covers aliased/minified docs).
    for known in [
        "GetChallenge",
        "VerifyChallenge",
        "LightningReceiveQuote",
        "RequestLightningReceive",
        "RequestLightningSend",
        "RequestSwap",
        "LeavesSwapFeeEstimate",
        "LightningSendFeeEstimate",
        "StaticDepositQuote",
        "ClaimStaticDeposit",
        "CreateInstantStaticDepositQuote",
        "CreateClaimInstantStaticDeposit",
        "RequestCoopExit",
        "CompleteCoopExit",
        "CoopExitFeeEstimates",
        "CoopExitFeeQuote",
        "Transfers",
        "UserRequest",
        "FetchCurrentUserToUserRequestsConnection",
        "RegisterWalletWebhook",
        "DeleteWalletWebhook",
        "ListSparkWalletWebhooks",
    ] {
        if req.query.contains(known) {
            return known.to_string();
        }
    }
    "Unknown".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::from_env();
    let (secret_hex, mut pubkey_hex) = config
        .resolve_signing_key()
        .map_err(|e| format!("signing key: {e}"))?;
    // When a swap sidecar is configured, IT owns the SSP identity (receives
    // swap outbounds and signs quotes via /sign): publish its pubkey so all
    // three agree. Falls back to the local key if the sidecar is down.
    if !config.sidecar_url.is_empty() {
        match fetch_sidecar_identity(&config.sidecar_url, &config.sidecar_token).await {
            Ok(pk) => {
                info!("SSP identity from sidecar: {pk}");
                pubkey_hex = pk;
            }
            Err(e) => tracing::warn!(
                "sidecar identity unavailable ({e}); publishing local key {pubkey_hex}"
            ),
        }
    }
    // Env identity wins when set; otherwise the resolved key's pubkey is used
    // (first boot publishes it via /health for sspClientOptions).
    let pubkey_hex = if config.ssp_identity_pubkey.is_empty() {
        pubkey_hex
    } else {
        if pubkey_hex != config.ssp_identity_pubkey {
            return Err(format!(
                "resolved key pubkey {pubkey_hex} != SSP_IDENTITY_PUBKEY {}",
                config.ssp_identity_pubkey
            )
            .into());
        }
        pubkey_hex
    };
    let db = Arc::new(Db::open(&config.data_dir).map_err(|e| format!("db: {e}"))?);
    let backend = Arc::new(Backend::select(&config, db.clone()).await);
    // Live event pump (fake backend ignores it).
    {
        let backend = backend.clone();
        tokio::spawn(async move { backend.run_event_loop().await });
    }
    let addr: SocketAddr = config.listen_addr.parse()?;
    let state = AppState {
        config: config.clone(),
        db,
        ldk: backend,
        ssp_secret_hex: secret_hex,
        ssp_pubkey_hex: pubkey_hex.clone(),
    };
    info!(
        "SSP listening on {} (network={}, identity={})",
        addr, config.network, pubkey_hex
    );
    let app = Router::new()
        .route("/health", get(health))
        .route("/", get(health))
        // Both schema endpoints the SDK uses:
        // default "graphql/spark/2025-03-19", LOCAL override "graphql/spark/rc".
        .route("/graphql/spark/2025-03-19", post(graphql_handler))
        .route("/graphql/spark/rc", post(graphql_handler))
        .route("/graphql", post(graphql_handler))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
