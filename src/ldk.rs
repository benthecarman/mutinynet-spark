use std::sync::Arc;

use ldk_server_client::{
    client::LdkServerClient,
    ldk_server_grpc::{
        api::{
            Bolt11ClaimForHashRequest, Bolt11FailForHashRequest, Bolt11ReceiveForHashRequest,
            Bolt11SendRequest, Bolt12SendRequest, DecodeInvoiceRequest, GetPaymentDetailsRequest,
            ListPaymentsRequest,
        },
        events::event_envelope::Event as LdkRawEvent,
        types::{Bolt11InvoiceDescription, Payment, PaymentDirection, PaymentStatus},
    },
};
use sha2::{Digest, Sha256};

use crate::{config::Config, db::Db, spark::SparkService};

/// What the SSP needs from Lightning. BOLT11 only (no BOLT12 hold support in
/// ldk-server, and receives stay BOLT11 by decision).
///
/// Receive model (hodl, SSP-owned preimage): wallets mint a hash via
/// mint_preimage first and use it in createLightningHodlInvoice. The SSP
/// holds the preimage before payment (compliant: attestor == holder per
/// the SO's own rule) and auto-claims on LN arrival. The SO binds invoice
/// hash deliberately, so the SSP never substitutes hashes.
///
/// Send model: `pay_invoice` only INITS (`Bolt11Send`). Final status comes
/// from `SubscribeEvents` (PaymentSuccessful/PaymentFailed) via
/// `apply_ln_event`; wallets poll it through Transfers/UserRequest.
#[async_trait::async_trait]
pub trait LdkBackend: Send + Sync {
    async fn fee_estimate_msat(&self, invoice: &str, amount_sats: Option<u64>) -> u64;
    async fn verify_lightning_send_funding(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        invoice: &str,
        amount_sats: Option<u64>,
    ) -> Result<(), String>;
    async fn pay_invoice(&self, invoice: &str, amount_sats: Option<u64>) -> PayResult;
    async fn payment_status(&self, payment_id: &str) -> String;
    async fn create_invoice(
        &self,
        amount_sats: u64,
        payment_hash_hex: &str,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateInvoiceResult, String>;
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

/// Minimal SSP view of ldk-server SubscribeEvents payloads.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum LnEvent {
    OutboundSucceeded { payment: Payment },
    OutboundFailed { payment_id: String },
    InboundClaimable { payment_hash: String },
    InboundReceived { payment_hash: String },
}

/// Runtime backend: live ldk-server when configured and reachable, else fake.
#[derive(Clone)]
pub enum Backend {
    Live(Arc<LdkGrpcBackend>),
    Fake(Arc<FakeLdkBackend>),
}

impl Backend {
    /// Select live when LDK_GRPC_ADDR + credentials resolve and the node
    /// answers `get_node_info`; otherwise fake (with a loud log).
    pub async fn select(config: &Config, db: Arc<Db>, spark: Arc<SparkService>) -> Self {
        match LdkGrpcBackend::connect(config, db.clone(), spark).await {
            Ok(live) => {
                tracing::info!(
                    "LDK live mode: node {}",
                    live.node_id.clone().unwrap_or_default()
                );
                Backend::Live(Arc::new(live))
            }
            Err(e) => {
                tracing::warn!("LDK fake mode ({e}); set LDK_GRPC_ADDR + credentials for live");
                Backend::Fake(Arc::new(FakeLdkBackend::new(config.clone(), db)))
            }
        }
    }

    /// SubscribeEvents pump for a live backend. The upstream streaming client
    /// does not set a `grpc-timeout` header. Reconnect with capped exponential
    /// backoff when the server, proxy, or HTTP/2 connection ends the stream.
    pub async fn run_event_pump(live: Arc<LdkGrpcBackend>) {
        let mut failures = 0u32;
        loop {
            let connected_at = std::time::Instant::now();
            let mut received_event = false;
            // Bound only the connection and response-header phase. Do not put
            // a deadline on the returned server stream.
            match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                live.client.subscribe_events(),
            )
            .await
            {
                Ok(Ok(mut stream)) => {
                    tracing::info!("ldk event stream connected");
                    while let Some(msg) = stream.next_message().await {
                        match msg {
                            Ok(env) => {
                                received_event = true;
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
                    tracing::warn!("ldk event stream ended; reconnecting");
                }
                Ok(Err(e)) => tracing::warn!("ldk subscribe_events failed: {e}"),
                Err(_) => tracing::warn!("ldk subscribe_events connection timed out"),
            }
            if received_event || connected_at.elapsed() >= std::time::Duration::from_secs(30) {
                failures = 0;
            } else {
                failures = failures.saturating_add(1);
            }
            let delay = reconnect_delay(failures);
            tracing::info!(?delay, "waiting before ldk event stream reconnect");
            tokio::time::sleep(delay).await;
        }
    }

    /// Recover events lost during a stream gap from the durable payment list.
    pub async fn run_reconciler(live: Arc<LdkGrpcBackend>) {
        loop {
            if let Err(e) = live.reconcile_payments().await {
                tracing::warn!("ldk payment reconcile failed: {e}");
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }
}

fn reconnect_delay(failures: u32) -> std::time::Duration {
    use rand::Rng;
    let exponent = failures.saturating_sub(1).min(5);
    let base_secs = (1u64 << exponent).min(30);
    let jitter_ms = rand::thread_rng().gen_range(0..=base_secs * 250);
    std::time::Duration::from_millis(base_secs * 1000 + jitter_ms)
}

#[async_trait::async_trait]
impl LdkBackend for Backend {
    async fn fee_estimate_msat(&self, invoice: &str, amount_sats: Option<u64>) -> u64 {
        match self {
            Backend::Live(b) => b.fee_estimate_msat(invoice, amount_sats).await,
            Backend::Fake(b) => b.fee_estimate_msat(invoice, amount_sats).await,
        }
    }
    async fn verify_lightning_send_funding(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        invoice: &str,
        amount_sats: Option<u64>,
    ) -> Result<(), String> {
        match self {
            Backend::Live(b) => {
                b.verify_lightning_send_funding(owner, outbound_transfer_id, invoice, amount_sats)
                    .await
            }
            Backend::Fake(b) => {
                b.verify_lightning_send_funding(owner, outbound_transfer_id, invoice, amount_sats)
                    .await
            }
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
    match event {
        LdkRawEvent::PaymentSuccessful(e) => {
            if let Some(p) = e.payment {
                out.push(LnEvent::OutboundSucceeded { payment: p });
            }
        }
        LdkRawEvent::PaymentFailed(e) => {
            if let Some(p) = e.payment {
                out.push(LnEvent::OutboundFailed { payment_id: p.id });
            }
        }
        LdkRawEvent::PaymentClaimable(e) => {
            if let Some(hash) = bolt11_hash(e.payment) {
                out.push(LnEvent::InboundClaimable { payment_hash: hash });
            }
        }
        LdkRawEvent::PaymentReceived(e) => {
            if let Some(hash) = bolt11_hash(e.payment) {
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

#[derive(Clone)]
pub struct LdkGrpcBackend {
    pub client: LdkServerClient,
    pub node_id: Option<String>,
    db: Arc<Db>,
    spark: Arc<SparkService>,
}

impl LdkGrpcBackend {
    pub async fn connect(
        config: &Config,
        db: Arc<Db>,
        spark: Arc<SparkService>,
    ) -> Result<Self, String> {
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
        let info = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            client.get_node_info(ldk_server_client::ldk_server_grpc::api::GetNodeInfoRequest {}),
        )
        .await
        .map_err(|_| "get_node_info timed out".to_string())?
        .map_err(|e| format!("get_node_info: {e}"))?;
        Ok(Self {
            client,
            node_id: Some(info.node_id.clone()),
            db,
            spark,
        })
    }

    async fn settle_succeeded_payment(&self, payment: &Payment) -> Result<(), String> {
        let payment_id = payment.id.clone();
        let Some((_owner, outbound_transfer_id)) =
            self.db.lightning_send_for_payment(&payment_id).await?
        else {
            return Err(format!(
                "no Lightning send request for payment {payment_id}"
            ));
        };
        let Some(kind) = payment.kind.as_ref().and_then(|kind| kind.kind.as_ref()) else {
            return Err(format!("payment {payment_id} has no payment kind"));
        };
        let ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt11(bolt11) = kind
        else {
            return Err("only BOLT11 Spark settlement is supported".to_string());
        };
        let preimage = bolt11
            .preimage
            .as_deref()
            .ok_or_else(|| format!("payment {payment_id} succeeded without a preimage"))?;
        self.spark
            .settle_lightning_send(&outbound_transfer_id, &bolt11.hash, preimage)
            .await?;
        self.db.set_payment(&payment_id, "SUCCEEDED").await
    }

    async fn is_managed_outbound(&self, payment_id: &str) -> Result<bool, String> {
        Ok(self
            .db
            .lightning_send_for_payment(payment_id)
            .await?
            .is_some())
    }

    async fn fund_lightning_receive(&self, payment_hash: &str) -> Result<bool, String> {
        let Some((request_id, owner, amount_sats)) =
            self.db.lightning_receive_for_hash(payment_hash).await?
        else {
            return Ok(false);
        };
        if self.preimage_for(payment_hash).await.is_none() {
            return Ok(false);
        }
        let transfer_id = self
            .spark
            .settle_lightning_receive(&owner, payment_hash, amount_sats)
            .await?;
        self.db
            .insert_transfer(
                &transfer_id,
                &request_id,
                "LIGHTNING_RECEIVE",
                "TRANSFER_COMPLETED",
                &owner,
            )
            .await?;
        Ok(true)
    }

    async fn claim_funded_receive(&self, payment_hash: &str) -> Result<bool, String> {
        if !self.fund_lightning_receive(payment_hash).await? {
            return Ok(false);
        }
        let preimage = self
            .preimage_for(payment_hash)
            .await
            .ok_or_else(|| format!("preimage disappeared for Lightning receive {payment_hash}"))?;
        self.client
            .bolt11_claim_for_hash(Bolt11ClaimForHashRequest {
                payment_hash: Some(payment_hash.to_string()),
                claimable_amount_msat: None,
                preimage,
            })
            .await
            .map_err(|error| format!("claim Lightning receive {payment_hash}: {error}"))?;
        Ok(true)
    }

    async fn finish_received_payment(&self, payment_hash: &str) -> Result<bool, String> {
        if !self.fund_lightning_receive(payment_hash).await? {
            return Ok(false);
        }
        self.db
            .set_receive_status(payment_hash, "TRANSFER_COMPLETED")
            .await?;
        self.db.delete_preimage(payment_hash).await?;
        Ok(true)
    }

    async fn reconcile_payments(&self) -> Result<(), String> {
        let mut page_token = None;
        for _ in 0..100 {
            let page = self
                .client
                .list_payments(ListPaymentsRequest { page_token })
                .await
                .map_err(|e| e.to_string())?;
            for payment in page.payments {
                if payment.direction == PaymentDirection::Outbound as i32 {
                    if !self.is_managed_outbound(&payment.id).await? {
                        continue;
                    }
                    match payment.status {
                        value if value == PaymentStatus::Succeeded as i32 => {
                            if let Err(error) = self.settle_succeeded_payment(&payment).await {
                                tracing::warn!(
                                    payment_id = %payment.id,
                                    "Lightning paid but Spark settlement is pending: {error}"
                                );
                                self.db.set_payment(&payment.id, "SETTLING").await?;
                            }
                        }
                        value if value == PaymentStatus::Failed as i32 => {
                            self.db.set_payment(&payment.id, "FAILED").await?;
                        }
                        _ => self.db.set_payment(&payment.id, "PENDING").await?,
                    }
                    continue;
                }
                let Some(payment_hash) = bolt11_hash(Some(payment.clone())) else {
                    continue;
                };
                match payment.status {
                    value if value == PaymentStatus::Succeeded as i32 => {
                        if let Err(error) = self.finish_received_payment(&payment_hash).await {
                            tracing::warn!(
                                payment_hash,
                                "Lightning received but Spark payout is pending: {error}"
                            );
                        }
                    }
                    value if value == PaymentStatus::Failed as i32 => {
                        if self
                            .db
                            .lightning_receive_for_hash(&payment_hash)
                            .await?
                            .is_some()
                        {
                            self.db
                                .set_receive_status(&payment_hash, "HTLC_FAILED")
                                .await?;
                            self.db.delete_preimage(&payment_hash).await?;
                        }
                    }
                    _ => match self.claim_funded_receive(&payment_hash).await {
                        Ok(true) => {
                            self.db
                                .set_receive_status(&payment_hash, "HTLC_RECEIVED")
                                .await?;
                        }
                        Ok(false) => {}
                        Err(error) => tracing::warn!(
                            payment_hash,
                            "Spark payout or Lightning claim is pending: {error}"
                        ),
                    },
                }
            }
            page_token = page.next_page_token;
            if page_token.is_none() {
                break;
            }
        }
        if page_token.is_some() {
            return Err("ldk payment reconciliation exceeded 100 pages".to_string());
        }
        for payment_hash in self
            .db
            .expired_receive_hashes(chrono::Utc::now().timestamp())
            .await?
        {
            if self
                .client
                .bolt11_fail_for_hash(Bolt11FailForHashRequest {
                    payment_hash: payment_hash.clone(),
                })
                .await
                .is_ok()
            {
                self.db
                    .set_receive_status(&payment_hash, "HTLC_FAILED")
                    .await?;
                self.db.delete_preimage(&payment_hash).await?;
            }
        }
        Ok(())
    }
}

fn sats_to_msats(sats: u64) -> Result<u64, String> {
    sats.checked_mul(1000)
        .ok_or_else(|| "amount_sats is too large".to_string())
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

    async fn verify_lightning_send_funding(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        invoice: &str,
        amount_sats: Option<u64>,
    ) -> Result<(), String> {
        let decoded = self
            .client
            .decode_invoice(DecodeInvoiceRequest {
                invoice: invoice.to_string(),
            })
            .await
            .map_err(|error| format!("decode invoice: {error}"))?;
        let amount_msat = match decoded.amount_msat {
            Some(value) => {
                if amount_sats.is_some() {
                    return Err("amount_sats is only valid for zero-amount invoices".to_string());
                }
                value
            }
            None => sats_to_msats(
                amount_sats.ok_or_else(|| "zero-amount invoice needs amount_sats".to_string())?,
            )?,
        };
        let total_sats = amount_msat
            .checked_add(999)
            .ok_or_else(|| "invoice amount is too large".to_string())?
            / 1000;
        if total_sats == 0 {
            return Err("Lightning send amount must be positive".to_string());
        }
        self.spark
            .verify_lightning_send(
                owner,
                outbound_transfer_id,
                &decoded.payment_hash.to_lowercase(),
                total_sats,
            )
            .await
    }

    // Send only inits; finality via SubscribeEvents.
    // BOLT12 offers (lno1…) route to bolt12_send; everything else to bolt11_send.
    async fn pay_invoice(&self, invoice: &str, amount_sats: Option<u64>) -> PayResult {
        let amount_msat = match amount_sats.map(sats_to_msats).transpose() {
            Ok(amount) => amount,
            Err(e) => {
                return PayResult {
                    payment_id: format!("init-failed: {e}"),
                    status: "FAILED".to_string(),
                };
            }
        };
        if invoice.to_lowercase().starts_with("lno1") {
            let req = Bolt12SendRequest {
                offer: invoice.to_string(),
                amount_msat,
                quantity: None,
                payer_note: None,
                route_parameters: None,
            };
            match self.client.bolt12_send(req).await {
                Ok(resp) => {
                    let _ = self.db.set_payment(&resp.payment_id, "PENDING").await;
                    return PayResult {
                        payment_id: resp.payment_id,
                        status: "PENDING".to_string(),
                    };
                }
                Err(e) => {
                    return PayResult {
                        payment_id: format!("init-failed: {e}"),
                        status: "FAILED".to_string(),
                    }
                }
            }
        }
        let req = Bolt11SendRequest {
            invoice: invoice.to_string(),
            amount_msat,
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
                Some(p) if p.status == PaymentStatus::Succeeded as i32 => {
                    if cached == "SUCCEEDED" {
                        cached
                    } else {
                        match self.settle_succeeded_payment(&p).await {
                            Ok(()) => "SUCCEEDED".to_string(),
                            Err(error) => {
                                tracing::warn!(
                                    payment_id,
                                    "Lightning paid but Spark settlement is pending: {error}"
                                );
                                let _ = self.db.set_payment(payment_id, "SETTLING").await;
                                "SETTLING".to_string()
                            }
                        }
                    }
                }
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
        // Hodl receive, SSP-owned preimage model: the invoice carries the
        // hash the wallet registered (minted by the SSP via mint_preimage,
        // or wallet-supplied). The SSP holds the preimage before payment
        // and claims on arrival. The SO binds invoice hash deliberately
        // (ErrPaymentHashMismatch), so the SSP never substitutes hashes.
        let resp = self
            .client
            .bolt11_receive_for_hash(Bolt11ReceiveForHashRequest {
                amount_msat: Some(sats_to_msats(amount_sats)?),
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

    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        let digest = hex::encode(Sha256::digest(
            hex::decode(preimage_hex).unwrap_or_default(),
        ));
        if digest != payment_hash_hex.to_lowercase() {
            return false;
        }
        let claimed = self
            .client
            .bolt11_claim_for_hash(Bolt11ClaimForHashRequest {
                payment_hash: Some(payment_hash_hex.to_string()),
                claimable_amount_msat: None,
                preimage: preimage_hex.to_string(),
            })
            .await
            .is_ok();
        if claimed {
            self.db
                .save_preimage(
                    payment_hash_hex,
                    preimage_hex,
                    "",
                    &chrono::Utc::now().to_rfc3339(),
                )
                .await
                .is_ok()
        } else {
            false
        }
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
            LnEvent::OutboundSucceeded { payment } => {
                match self.is_managed_outbound(&payment.id).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        tracing::warn!(
                            payment_id = %payment.id,
                            "could not classify outbound Lightning payment: {error}"
                        );
                        return;
                    }
                }
                if let Err(error) = self.settle_succeeded_payment(&payment).await {
                    tracing::warn!(
                        payment_id = %payment.id,
                        "Lightning paid but Spark settlement is pending: {error}"
                    );
                    let _ = self.db.set_payment(&payment.id, "SETTLING").await;
                }
            }
            LnEvent::OutboundFailed { payment_id } => {
                match self.is_managed_outbound(&payment_id).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        tracing::warn!(
                            payment_id,
                            "could not classify outbound Lightning payment: {error}"
                        );
                        return;
                    }
                }
                let _ = self.db.set_payment(&payment_id, "FAILED").await;
            }
            // Self-settling: SSP-minted invoices carry an SSP-held preimage,
            // so claim immediately (claim_for_hash). Wallet-hash invoices
            // have no preimage here until revealed via the reveal_preimage
            // mutation; they wait (expiry fails them back).
            LnEvent::InboundClaimable { payment_hash } => {
                match self.claim_funded_receive(&payment_hash).await {
                    Ok(true) => {
                        let _ = self
                            .db
                            .set_receive_status(&payment_hash, "HTLC_RECEIVED")
                            .await;
                        tracing::info!("funded and claimed hodl invoice {payment_hash}");
                    }
                    Ok(false) => {}
                    Err(error) => tracing::warn!(
                        payment_hash,
                        "Spark payout or Lightning claim is pending: {error}"
                    ),
                }
            }
            LnEvent::InboundReceived { payment_hash } => {
                if let Err(error) = self.finish_received_payment(&payment_hash).await {
                    tracing::warn!(
                        payment_hash,
                        "Lightning received but Spark payout is pending: {error}"
                    );
                }
            }
        }
    }

    fn live_node_id(&self) -> Option<String> {
        self.node_id.clone()
    }
}

#[derive(Clone)]
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

    async fn verify_lightning_send_funding(
        &self,
        _owner: &str,
        outbound_transfer_id: &str,
        _invoice: &str,
        _amount_sats: Option<u64>,
    ) -> Result<(), String> {
        if outbound_transfer_id.is_empty() {
            Err("user_outbound_transfer_external_id is required".to_string())
        } else {
            Ok(())
        }
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

    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        let digest = hex::encode(Sha256::digest(
            hex::decode(preimage_hex).unwrap_or_default(),
        ));
        if digest != payment_hash_hex.to_lowercase() {
            return false;
        }
        self.db
            .save_preimage(
                payment_hash_hex,
                preimage_hex,
                "",
                &chrono::Utc::now().to_rfc3339(),
            )
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
            LnEvent::OutboundSucceeded { payment } => {
                let _ = self.db.set_payment(&payment.id, "SUCCEEDED").await;
            }
            LnEvent::OutboundFailed { payment_id } => {
                let _ = self.db.set_payment(&payment_id, "FAILED").await;
            }
            LnEvent::InboundClaimable { payment_hash } => {
                let _ = self
                    .db
                    .set_receive_status(&payment_hash, "HTLC_RECEIVED")
                    .await;
            }
            LnEvent::InboundReceived { payment_hash } => {
                let _ = self
                    .db
                    .set_receive_status(&payment_hash, "LIGHTNING_PAYMENT_RECEIVED")
                    .await;
            }
        }
    }

    fn live_node_id(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_is_bounded() {
        for failures in 0..100 {
            let delay = reconnect_delay(failures);
            assert!(delay >= std::time::Duration::from_secs(1));
            assert!(delay <= std::time::Duration::from_millis(37_500));
        }
    }

    #[test]
    fn millisatoshi_conversion_rejects_overflow() {
        assert_eq!(
            sats_to_msats(21_000_000 * 100_000_000),
            Ok(2_100_000_000_000_000_000)
        );
        assert!(sats_to_msats(u64::MAX).is_err());
    }
}
