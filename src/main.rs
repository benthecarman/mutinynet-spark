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
mod frost;
mod graphql;
mod ldk;

use config::Config;
use db::Db;
use ldk::{Backend, LdkBackend, LdkGrpcBackend};

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Arc<Db>,
    pub ldk: Arc<tokio::sync::RwLock<Backend>>,
    /// Hex secret (resolved) for local signing fallback.
    pub ssp_secret_hex: String,
    /// Published SSP identity. None while the sidecar identity is pending;
    /// identity-dependent ops reject until it resolves.
    pub identity: Arc<tokio::sync::RwLock<Option<String>>>,
    /// Shared HTTP client (timeouts + pooling) for sidecar calls.
    pub http: reqwest::Client,
}

/// One shared client: 15s total, 5s connect. Never build per-request clients.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("http client builds")
}

pub async fn backend(state: &AppState) -> tokio::sync::RwLockReadGuard<'_, Backend> {
    state.ldk.read().await
}

pub async fn ssp_identity(state: &AppState) -> Result<String, String> {
    state
        .identity
        .read()
        .await
        .clone()
        .ok_or_else(|| "ssp identity pending (sidecar unreachable)".to_string())
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
    #[serde(default, rename = "operationName")]
    pub operation_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct GraphqlResponse {
    data: serde_json::Value,
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let backend = state.ldk.read().await;
    Json(serde_json::json!({
        "status": "ok",
        "network": state.config.network,
        "ssp_identity_pubkey": state.identity.read().await.clone(),
        "identity_source": identity_source(&state.config),
        "ldk_mode": if backend.live_node_id().is_some() { "live" } else { "fake" },
        "ldk_node_id": backend.live_node_id(),
    }))
}

fn identity_source(config: &Config) -> &'static str {
    if config.sidecar_url.is_empty() {
        "local"
    } else {
        "sidecar"
    }
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
            let head: String = body.chars().take(300).collect();
            tracing::warn!("unparseable graphql body: {e}; head={head:?}");
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
/// Falls back to raw deflate for other producers. Capped: a 2 MB compressed
/// body must never expand past MAX_INFLATED_BYTES (M3).
const MAX_INFLATED_BYTES: u64 = 8 * 1024 * 1024;

fn read_capped<R: std::io::Read>(r: R) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut out = Vec::new();
    r.take(MAX_INFLATED_BYTES + 1)
        .read_to_end(&mut out)
        .map_err(|e| e.to_string())?;
    if out.len() as u64 > MAX_INFLATED_BYTES {
        return Err("deflate body exceeds limit".to_string());
    }
    Ok(out)
}

fn inflate_raw_deflate(input: &[u8]) -> Result<Vec<u8>, String> {
    match read_capped(flate2::read::ZlibDecoder::new(input)) {
        Ok(out) => Ok(out),
        Err(_) => read_capped(flate2::read::DeflateDecoder::new(input)),
    }
}

/// Fetch the sidecar wallet identity (with retries; sidecar may boot later).
async fn fetch_sidecar_identity(sidecar_url: &str, token: &str) -> Result<String, String> {
    let mut req = http_client().get(format!("{sidecar_url}/health"));
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
    if let Some(name) = &req.operation_name {
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
        "MintInvoicePreimage",
        "RevealPreimage",
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
    let (secret_hex, local_pubkey_hex) = config
        .resolve_signing_key()
        .map_err(|e| format!("signing key: {e}"))?;
    // Env identity wins when set; otherwise the resolved key's pubkey is used
    // (first boot publishes it via /health for sspClientOptions).
    if !config.ssp_identity_pubkey.is_empty() && local_pubkey_hex != config.ssp_identity_pubkey {
        return Err(format!(
            "resolved key pubkey {local_pubkey_hex} != SSP_IDENTITY_PUBKEY {}",
            config.ssp_identity_pubkey
        )
        .into());
    }
    // Identity starts as the local key (or pinned env key). With a sidecar
    // configured it is replaced by the sidecar identity once reachable; until
    // then identity-dependent ops reject with a clear error and /health shows
    // a null pubkey. The listener binds immediately (M16).
    let initial_identity = if config.sidecar_url.is_empty() {
        Some(if config.ssp_identity_pubkey.is_empty() {
            local_pubkey_hex.clone()
        } else {
            config.ssp_identity_pubkey.clone()
        })
    } else {
        None
    };
    let db = Arc::new(Db::open(&config.data_dir).map_err(|e| format!("db: {e}"))?);
    // Fake Lightning is never silent: refuse unless explicitly allowed.
    let allow_fake = std::env::var("SSP_ALLOW_FAKE_LN").unwrap_or_default() == "1";
    let backend = Arc::new(tokio::sync::RwLock::new(
        Backend::select(&config, db.clone()).await,
    ));
    if backend.read().await.live_node_id().is_none() && !allow_fake {
        return Err("ldk-server unreachable and SSP_ALLOW_FAKE_LN!=1; refusing fake mode".into());
    }
    // Live event pump + recovery: if we started fake, keep retrying connect
    // and swap the backend live when ldk-server answers.
    {
        let backend = backend.clone();
        if let Backend::Live(live) = backend.read().await.clone() {
            tokio::spawn(async move { Backend::run_event_pump(live).await });
        }
        let backend = backend.clone();
        let config = config.clone();
        let db = db.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                if backend.read().await.live_node_id().is_some() {
                    continue;
                }
                match LdkGrpcBackend::connect(&config, db.clone()).await {
                    Ok(live) => {
                        tracing::info!("ldk-server reachable; switching to live mode");
                        tokio::spawn(Backend::run_event_pump(live.clone()));
                        *backend.write().await = Backend::Live(live);
                    }
                    Err(e) => tracing::warn!("ldk-server still unreachable: {e}"),
                }
            }
        });
    }
    // Periodic prune: expired sessions + challenges older than 1h.
    {
        let db = db.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                let now = chrono::Utc::now();
                let _ = db.prune_expired_sessions(&now.to_rfc3339()).await;
                let _ = db
                    .prune_challenges(&(now - chrono::Duration::hours(1)).to_rfc3339())
                    .await;
            }
        });
    }
    let addr: SocketAddr = config.listen_addr.parse()?;
    let identity = Arc::new(tokio::sync::RwLock::new(initial_identity));
    // Sidecar identity resolution runs in the background; ops needing it fail
    // closed until it resolves.
    if !config.sidecar_url.is_empty() {
        let identity = identity.clone();
        let config = config.clone();
        tokio::spawn(async move {
            loop {
                match fetch_sidecar_identity(&config.sidecar_url, &config.sidecar_token).await {
                    Ok(pk) => {
                        info!("SSP identity from sidecar: {pk}");
                        *identity.write().await = Some(pk);
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("sidecar identity unavailable ({e}); retrying in 10s");
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                }
            }
        });
    }
    let state = AppState {
        config: config.clone(),
        db,
        ldk: backend,
        ssp_secret_hex: secret_hex,
        identity: identity.clone(),
        http: http_client(),
    };
    info!("SSP listening on {} (network={})", addr, config.network);
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
