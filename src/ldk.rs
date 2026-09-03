use std::sync::Arc;

use ldk_server_client::{
    client::LdkServerClient,
    ldk_server_grpc::{
        api::{
            Bolt11ClaimForHashRequest, Bolt11FailForHashRequest, Bolt11ReceiveForHashRequest,
            Bolt11SendRequest, GetPaymentDetailsRequest,
        },
        events::event_envelope::Event as LdkRawEvent,
        types::{Bolt11InvoiceDescription, PaymentStatus},
    },
};
use sha2::{Digest, Sha256};

use crate::{config::Config, db::Db};

/// What the SSP needs from Lightning. BOLT11 only (no BOLT12 hold support in
/// ldk-server, and receives stay BOLT11 by decision).
///
/// Receive model (hodl, mirrors ldk-server RPCs):
/// - Wallet supplies `payment_hash` (Spark preimage-swap flow): SSP registers
///   a pending hodl invoice via `Bolt11ReceiveForHash`, no preimage yet.
/// - SSP-minted invoices: `create_invoice_with_new_preimage` generates the
///   preimage, stores hash->preimage, registers the hodl invoice.
/// - On SO proof (user reveals preimage when claiming leaves):
///   `reveal_and_claim` stores it and fires `Bolt11ClaimForHash`.
/// - On expiry: `fail_hold` fires `Bolt11FailForHash`.
/// - Inbound state arrives via `SubscribeEvents` (PaymentClaimable/Received);
///   see `apply_ln_event`.
///
/// Send model: `pay_invoice` only INITS (`Bolt11Send`). Final status comes
/// from `SubscribeEvents` (PaymentSuccessful/PaymentFailed) via
/// `apply_ln_event`; wallets poll it through Transfers/UserRequest.
#[async_trait::async_trait]
pub trait LdkBackend: Send + Sync {
    async fn fee_estimate_msat(&self, invoice: &str, amount_sats: Option<u64>) -> u64;
    async fn pay_invoice(&self, invoice: &str, amount_sats: Option<u64>) -> PayResult;
    async fn payment_status(&self, payment_id: &str) -> String;
    async fn create_invoice(
        &self,
        amount_sats: u64,
        payment_hash_hex: &str,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateInvoiceResult, String>;
    /// SSP-minted invoice path (hodl with SSP-held preimage). Used by live
    /// receive flows once the SDK requests it; kept exact to the RPC shape.
    #[allow(dead_code)]
    async fn create_invoice_with_new_preimage(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<NewInvoiceResult, String>;
    /// Called when the SO/user reveals a preimage for a pending hodl invoice.
    /// Wired to Bolt11ClaimForHash in live mode.
    #[allow(dead_code)]
    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool;
    /// Expiry path for hodl invoices (Bolt11FailForHash in live mode).
    #[allow(dead_code)]
    async fn fail_hold(&self, payment_hash_hex: &str) -> bool;
    /// SSP-held preimage lookup (None when the wallet owns the preimage).
    #[allow(dead_code)]
    async fn preimage_for(&self, payment_hash_hex: &str) -> Option<String>;
    async fn apply_ln_event(&self, event: LnEvent);
    fn live_node_id(&self) -> Option<String>;
}

#[derive(Clone, Debug)]
pub struct PayResult {
    pub payment_id: String,
    pub status: String,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CreateInvoiceResult {
    pub invoice: String,
    #[allow(dead_code)]
    pub payment_hash: String,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct NewInvoiceResult {
    pub invoice: String,
    #[allow(dead_code)]
    pub payment_hash: String,
}

/// Minimal SSP view of ldk-server SubscribeEvents payloads.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum LnEvent {
    OutboundSucceeded { payment_id: String },
    OutboundFailed { payment_id: String },
    InboundClaimable { payment_hash: String },
    InboundReceived { payment_hash: String },
}

/// Runtime backend: live ldk-server when configured and reachable, else fake.
pub enum Backend {
    Live(LdkGrpcBackend),
    Fake(FakeLdkBackend),
}

impl Backend {
    /// Select live when LDK_GRPC_ADDR + credentials resolve and the node
    /// answers `get_node_info`; otherwise fake (with a loud log).
    pub async fn select(config: &Config, db: Arc<Db>) -> Self {
        match LdkGrpcBackend::connect(config, db.clone()).await {
            Ok(live) => {
                tracing::info!(
                    "LDK live mode: node {}",
                    live.node_id.clone().unwrap_or_default()
                );
                Backend::Live(live)
            }
            Err(e) => {
                tracing::warn!("LDK fake mode ({e}); set LDK_GRPC_ADDR + credentials for live");
                Backend::Fake(FakeLdkBackend::new(config.clone(), db))
            }
        }
    }

    /// SubscribeEvents pump for live mode. Reconnects forever.
    pub async fn run_event_loop(&self) {
        if let Backend::Live(live) = self {
            loop {
                match live.client.subscribe_events().await {
                    Ok(mut stream) => {
                        tracing::info!("ldk event stream connected");
                        while let Some(msg) = stream.next_message().await {
                            match msg {
                                Ok(env) => {
                                    for ev in map_envelope(env) {
                                        live.apply_ln_event(ev).await;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("ldk event stream error: {e}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => tracing::warn!("ldk subscribe_events failed: {e}; retry in 5s"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

#[async_trait::async_trait]
impl LdkBackend for Backend {
    async fn fee_estimate_msat(&self, invoice: &str, amount_sats: Option<u64>) -> u64 {
        match self {
            Backend::Live(b) => b.fee_estimate_msat(invoice, amount_sats).await,
            Backend::Fake(b) => b.fee_estimate_msat(invoice, amount_sats).await,
        }
    }
    async fn pay_invoice(&self, invoice: &str, amount_sats: Option<u64>) -> PayResult {
        match self {
            Backend::Live(b) => b.pay_invoice(invoice, amount_sats).await,
            Backend::Fake(b) => b.pay_invoice(invoice, amount_sats).await,
        }
    }
    async fn payment_status(&self, payment_id: &str) -> String {
        match self {
            Backend::Live(b) => b.payment_status(payment_id).await,
            Backend::Fake(b) => b.payment_status(payment_id).await,
        }
    }
    async fn create_invoice(
        &self,
        amount_sats: u64,
        payment_hash_hex: &str,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateInvoiceResult, String> {
        match self {
            Backend::Live(b) => {
                b.create_invoice(amount_sats, payment_hash_hex, memo, expiry_secs)
                    .await
            }
            Backend::Fake(b) => {
                b.create_invoice(amount_sats, payment_hash_hex, memo, expiry_secs)
                    .await
            }
        }
    }
    /// SSP-minted invoice path (hodl with SSP-held preimage). Used by live
    /// receive flows once the SDK requests it; kept exact to the RPC shape.
    #[allow(dead_code)]
    async fn create_invoice_with_new_preimage(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<NewInvoiceResult, String> {
        match self {
            Backend::Live(b) => {
                b.create_invoice_with_new_preimage(amount_sats, memo, expiry_secs)
                    .await
            }
            Backend::Fake(b) => {
                b.create_invoice_with_new_preimage(amount_sats, memo, expiry_secs)
                    .await
            }
        }
    }
    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        match self {
            Backend::Live(b) => b.reveal_and_claim(payment_hash_hex, preimage_hex).await,
            Backend::Fake(b) => b.reveal_and_claim(payment_hash_hex, preimage_hex).await,
        }
    }
    async fn fail_hold(&self, payment_hash_hex: &str) -> bool {
        match self {
            Backend::Live(b) => b.fail_hold(payment_hash_hex).await,
            Backend::Fake(b) => b.fail_hold(payment_hash_hex).await,
        }
    }
    async fn preimage_for(&self, payment_hash_hex: &str) -> Option<String> {
        match self {
            Backend::Live(b) => b.preimage_for(payment_hash_hex).await,
            Backend::Fake(b) => b.preimage_for(payment_hash_hex).await,
        }
    }
    async fn apply_ln_event(&self, event: LnEvent) {
        match self {
            Backend::Live(b) => b.apply_ln_event(event).await,
            Backend::Fake(b) => b.apply_ln_event(event).await,
        }
    }
    fn live_node_id(&self) -> Option<String> {
        match self {
            Backend::Live(b) => b.live_node_id(),
            Backend::Fake(_) => None,
        }
    }
}

fn map_envelope(env: ldk_server_client::ldk_server_grpc::events::EventEnvelope) -> Vec<LnEvent> {
    let mut out = Vec::new();
    let Some(event) = env.event else { return out };
    let payment_of = |p: Option<ldk_server_client::ldk_server_grpc::types::Payment>| p;
    match event {
        LdkRawEvent::PaymentSuccessful(e) => {
            if let Some(p) = payment_of(e.payment) {
                out.push(LnEvent::OutboundSucceeded { payment_id: p.id });
            }
        }
        LdkRawEvent::PaymentFailed(e) => {
            if let Some(p) = payment_of(e.payment) {
                out.push(LnEvent::OutboundFailed { payment_id: p.id });
            }
        }
        LdkRawEvent::PaymentClaimable(e) => {
            if let Some(hash) = bolt11_hash(payment_of(e.payment)) {
                out.push(LnEvent::InboundClaimable { payment_hash: hash });
            }
        }
        LdkRawEvent::PaymentReceived(e) => {
            if let Some(hash) = bolt11_hash(payment_of(e.payment)) {
                out.push(LnEvent::InboundReceived { payment_hash: hash });
            }
        }
        _ => {}
    }
    out
}

fn bolt11_hash(p: Option<ldk_server_client::ldk_server_grpc::types::Payment>) -> Option<String> {
    let p = p?;
    let kind = p.kind?;
    match kind.kind? {
        ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt11(b) => Some(b.hash),
        _ => None,
    }
}

pub struct LdkGrpcBackend {
    pub client: LdkServerClient,
    pub node_id: Option<String>,
    db: Arc<Db>,
}

impl LdkGrpcBackend {
    pub async fn connect(config: &Config, db: Arc<Db>) -> Result<Self, String> {
        if config.ldk_grpc_addr.is_empty() {
            return Err("LDK_GRPC_ADDR unset".to_string());
        }
        let api_key = if !config.ldk_api_key.is_empty() {
            config.ldk_api_key.clone()
        } else if !config.ldk_api_key_file.is_empty() {
            // On-disk key is raw bytes; ldk-server hex-encodes before HMAC.
            let raw = std::fs::read(&config.ldk_api_key_file)
                .map_err(|e| format!("read LDK_API_KEY_FILE: {e}"))?;
            hex::encode(raw).trim().to_string()
        } else {
            return Err("LDK_API_KEY or LDK_API_KEY_FILE required for live mode".to_string());
        };
        if api_key.is_empty() {
            return Err("empty LDK api key".to_string());
        }
        let cert_pem = std::fs::read(&config.ldk_tls_cert_file)
            .map_err(|e| format!("read LDK_TLS_CERT_FILE {}: {e}", config.ldk_tls_cert_file))?;
        let client = LdkServerClient::new(config.ldk_grpc_addr.clone(), api_key, &cert_pem)?;
        let info = client
            .get_node_info(ldk_server_client::ldk_server_grpc::api::GetNodeInfoRequest {})
            .await
            .map_err(|e| format!("get_node_info: {e}"))?;
        Ok(Self {
            client,
            node_id: Some(info.node_id.clone()),
            db,
        })
    }
}

fn description_of(memo: &str) -> Option<Bolt11InvoiceDescription> {
    use ldk_server_client::ldk_server_grpc::types::bolt11_invoice_description::Kind;
    if memo.is_empty() {
        return None;
    }
    Some(Bolt11InvoiceDescription {
        kind: Some(Kind::Direct(memo.to_string())),
    })
}

#[async_trait::async_trait]
impl LdkBackend for LdkGrpcBackend {
    // Decision: 0 fee.
    async fn fee_estimate_msat(&self, _invoice: &str, _amount_sats: Option<u64>) -> u64 {
        0
    }

    // Send only inits; finality via SubscribeEvents.
    async fn pay_invoice(&self, invoice: &str, amount_sats: Option<u64>) -> PayResult {
        let req = Bolt11SendRequest {
            invoice: invoice.to_string(),
            amount_msat: amount_sats.map(|s| s * 1000),
            route_parameters: None,
        };
        match self.client.bolt11_send(req).await {
            Ok(resp) => {
                let _ = self.db.set_payment(&resp.payment_id, "PENDING").await;
                PayResult {
                    payment_id: resp.payment_id,
                    status: "PENDING".to_string(),
                }
            }
            Err(e) => PayResult {
                payment_id: format!("init-failed: {e}"),
                status: "FAILED".to_string(),
            },
        }
    }

    async fn payment_status(&self, payment_id: &str) -> String {
        if payment_id.starts_with("init-failed:") {
            return "FAILED".to_string();
        }
        let cached = self.db.payment_status(payment_id).await.unwrap_or_default();
        match self
            .client
            .get_payment_details(GetPaymentDetailsRequest {
                payment_id: payment_id.to_string(),
            })
            .await
        {
            Ok(resp) => match resp.payment {
                Some(p) if p.status == PaymentStatus::Succeeded as i32 => "SUCCEEDED".to_string(),
                Some(p) if p.status == PaymentStatus::Failed as i32 => "FAILED".to_string(),
                Some(_) => {
                    if cached.is_empty() || cached == "UNKNOWN" {
                        "PENDING".to_string()
                    } else {
                        cached
                    }
                }
                None => cached,
            },
            Err(_) => cached,
        }
    }

    async fn create_invoice(
        &self,
        amount_sats: u64,
        payment_hash_hex: &str,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateInvoiceResult, String> {
        let resp = self
            .client
            .bolt11_receive_for_hash(Bolt11ReceiveForHashRequest {
                amount_msat: Some(amount_sats * 1000),
                description: description_of(memo),
                expiry_secs,
                payment_hash: payment_hash_hex.to_string(),
            })
            .await
            .map_err(|e| e.to_string())?;
        Ok(CreateInvoiceResult {
            invoice: resp.invoice,
            payment_hash: payment_hash_hex.to_string(),
        })
    }

    /// SSP-minted invoice path (hodl with SSP-held preimage). Used by live
    /// receive flows once the SDK requests it; kept exact to the RPC shape.
    #[allow(dead_code)]
    async fn create_invoice_with_new_preimage(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<NewInvoiceResult, String> {
        let preimage: [u8; 32] = rand::random();
        let hash = hex::encode(Sha256::digest(preimage));
        let inv = self
            .create_invoice(amount_sats, &hash, memo, expiry_secs)
            .await?;
        self.db
            .save_preimage(&hash, &hex::encode(preimage))
            .await
            .map_err(|e| e)?;
        Ok(NewInvoiceResult {
            invoice: inv.invoice,
            payment_hash: hash,
        })
    }

    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        let digest = hex::encode(Sha256::digest(
            hex::decode(preimage_hex).unwrap_or_default(),
        ));
        if digest != payment_hash_hex.to_lowercase() {
            return false;
        }
        if self
            .db
            .save_preimage(payment_hash_hex, preimage_hex)
            .await
            .is_err()
        {
            return false;
        }
        self.client
            .bolt11_claim_for_hash(Bolt11ClaimForHashRequest {
                payment_hash: Some(payment_hash_hex.to_string()),
                claimable_amount_msat: None,
                preimage: preimage_hex.to_string(),
            })
            .await
            .is_ok()
    }

    async fn fail_hold(&self, payment_hash_hex: &str) -> bool {
        self.client
            .bolt11_fail_for_hash(Bolt11FailForHashRequest {
                payment_hash: payment_hash_hex.to_string(),
            })
            .await
            .is_ok()
    }

    async fn preimage_for(&self, payment_hash_hex: &str) -> Option<String> {
        self.db.get_preimage(payment_hash_hex).await.unwrap_or(None)
    }

    async fn apply_ln_event(&self, event: LnEvent) {
        match event {
            LnEvent::OutboundSucceeded { payment_id } => {
                let _ = self.db.set_payment(&payment_id, "SUCCEEDED").await;
            }
            LnEvent::OutboundFailed { payment_id } => {
                let _ = self.db.set_payment(&payment_id, "FAILED").await;
            }
            LnEvent::InboundClaimable { .. } | LnEvent::InboundReceived { .. } => {}
        }
    }

    fn live_node_id(&self) -> Option<String> {
        self.node_id.clone()
    }
}

pub struct FakeLdkBackend {
    pub config: Config,
    db: Arc<Db>,
}

impl FakeLdkBackend {
    pub fn new(config: Config, db: Arc<Db>) -> Self {
        Self { config, db }
    }
}

#[async_trait::async_trait]
impl LdkBackend for FakeLdkBackend {
    async fn fee_estimate_msat(&self, _invoice: &str, _amount_sats: Option<u64>) -> u64 {
        0
    }

    // Send only inits. Simulates the event path with a delayed flip so the
    // INITIATED -> SUCCEEDED polling works end to end without funds.
    async fn pay_invoice(&self, _invoice: &str, _amount_sats: Option<u64>) -> PayResult {
        let payment_id = uuid::Uuid::new_v4().to_string();
        let _ = self.db.set_payment(&payment_id, "PENDING").await;
        let db = self.db.clone();
        let pid = payment_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let _ = db.set_payment(&pid, "SUCCEEDED").await;
        });
        PayResult {
            payment_id,
            status: "PENDING".to_string(),
        }
    }

    async fn payment_status(&self, payment_id: &str) -> String {
        self.db.payment_status(payment_id).await.unwrap_or_default()
    }

    async fn create_invoice(
        &self,
        amount_sats: u64,
        payment_hash_hex: &str,
        _memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateInvoiceResult, String> {
        Ok(CreateInvoiceResult {
            invoice: format!(
                "lnbc{}n1ssp_hold_{}_exp{}",
                amount_sats,
                &payment_hash_hex[..std::cmp::min(8, payment_hash_hex.len())],
                expiry_secs
            ),
            payment_hash: payment_hash_hex.to_string(),
        })
    }

    /// SSP-minted invoice path (hodl with SSP-held preimage). Used by live
    /// receive flows once the SDK requests it; kept exact to the RPC shape.
    #[allow(dead_code)]
    async fn create_invoice_with_new_preimage(
        &self,
        amount_sats: u64,
        _memo: &str,
        expiry_secs: u32,
    ) -> Result<NewInvoiceResult, String> {
        let preimage: [u8; 32] = rand::random();
        let hash = hex::encode(Sha256::digest(preimage));
        self.db
            .save_preimage(&hash, &hex::encode(preimage))
            .await
            .map_err(|e| e)?;
        Ok(NewInvoiceResult {
            invoice: format!(
                "lnbc{}n1ssp_new_{}_exp{}",
                amount_sats,
                &hash[..8],
                expiry_secs
            ),
            payment_hash: hash,
        })
    }

    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        let digest = hex::encode(Sha256::digest(
            hex::decode(preimage_hex).unwrap_or_default(),
        ));
        if digest != payment_hash_hex.to_lowercase() {
            return false;
        }
        self.db
            .save_preimage(payment_hash_hex, preimage_hex)
            .await
            .is_ok()
    }

    async fn fail_hold(&self, _payment_hash_hex: &str) -> bool {
        true
    }

    async fn preimage_for(&self, payment_hash_hex: &str) -> Option<String> {
        self.db.get_preimage(payment_hash_hex).await.unwrap_or(None)
    }

    async fn apply_ln_event(&self, event: LnEvent) {
        match event {
            LnEvent::OutboundSucceeded { payment_id } => {
                let _ = self.db.set_payment(&payment_id, "SUCCEEDED").await;
            }
            LnEvent::OutboundFailed { payment_id } => {
                let _ = self.db.set_payment(&payment_id, "FAILED").await;
            }
            LnEvent::InboundClaimable { .. } | LnEvent::InboundReceived { .. } => {}
        }
    }

    fn live_node_id(&self) -> Option<String> {
        None
    }
}
