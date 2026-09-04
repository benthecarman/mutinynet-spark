use std::{future::Future, str::FromStr, sync::Arc, time::Duration};

use ldk_server_client::{
    client::LdkServerClient,
    ldk_server_grpc::{
        api::{
            Bolt11ClaimForHashRequest, Bolt11FailForHashRequest, Bolt11ReceiveForHashRequest,
            Bolt11SendRequest, Bolt12ReceiveRequest, Bolt12SendRequest, DecodeInvoiceRequest,
            GetPaymentDetailsRequest, ListPaymentsRequest,
        },
        events::event_envelope::Event as LdkRawEvent,
        types::{Bolt11InvoiceDescription, Payment, PaymentDirection, PaymentStatus},
    },
};
use sha2::{Digest, Sha256};

use crate::{
    config::Config,
    db::Db,
    spark::{LightningReceiveSwap, SparkService},
};

/// What the SSP needs from Lightning. BOLT11 only (no BOLT12 hold support in
/// ldk-server, and receives stay BOLT11 by decision).
///
/// Standard receives use a wallet-created preimage. The wallet stores its
/// threshold shares with the Spark Operators, and the SSP redeems them only
/// through InitiatePreimageSwapV3(REASON_RECEIVE) after PaymentClaimable.
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
    async fn create_bolt12_offer(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateOfferResult, String>;
    /// Called when the SO/user reveals a preimage for a pending hodl invoice.
    /// Wired to Bolt11ClaimForHash in live mode.
    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool;
    /// Expiry path for hodl invoices (Bolt11FailForHash in live mode).
    async fn fail_hold(&self, payment_hash_hex: &str) -> bool;
    /// SSP-held preimage lookup (None when the wallet owns the preimage).
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
pub struct CreateInvoiceResult {
    pub invoice: String,
}

#[derive(Clone, Debug)]
pub struct CreateOfferResult {
    pub offer: String,
    pub offer_id: String,
}

/// Minimal SSP view of ldk-server SubscribeEvents payloads.
#[derive(Clone, Debug)]
pub enum LnEvent {
    OutboundSucceeded {
        payment: Payment,
    },
    OutboundFailed {
        payment_id: String,
    },
    InboundClaimable {
        payment_hash: String,
        amount_msat: Option<u64>,
    },
    InboundReceived {
        payment_hash: String,
    },
    InboundBolt12Received {
        offer_id: String,
        payment_hash: String,
        amount_msat: Option<u64>,
    },
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
    async fn create_bolt12_offer(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateOfferResult, String> {
        match self {
            Backend::Live(b) => b.create_bolt12_offer(amount_sats, memo, expiry_secs).await,
            Backend::Fake(b) => b.create_bolt12_offer(amount_sats, memo, expiry_secs).await,
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
            if let Some(payment) = e.payment {
                let amount_msat = payment.amount_msat;
                if let Some(hash) = bolt11_hash(Some(payment)) {
                    out.push(LnEvent::InboundClaimable {
                        payment_hash: hash,
                        amount_msat,
                    });
                }
            }
        }
        LdkRawEvent::PaymentReceived(e) => {
            if let Some(hash) = bolt11_hash(e.payment.clone()) {
                out.push(LnEvent::InboundReceived { payment_hash: hash });
            } else if let Some((offer_id, payment_hash)) = bolt12_offer_ids(e.payment.clone()) {
                out.push(LnEvent::InboundBolt12Received {
                    offer_id,
                    payment_hash,
                    amount_msat: e.payment.and_then(|payment| payment.amount_msat),
                });
            }
        }
        _ => {}
    }
    out
}

fn bolt12_offer_ids(
    p: Option<ldk_server_client::ldk_server_grpc::types::Payment>,
) -> Option<(String, String)> {
    let p = p?;
    let kind = p.kind?;
    match kind.kind? {
        ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt12Offer(offer) => {
            Some((offer.offer_id, offer.hash?))
        }
        _ => None,
    }
}

fn bolt11_hash(p: Option<ldk_server_client::ldk_server_grpc::types::Payment>) -> Option<String> {
    let p = p?;
    let kind = p.kind?;
    match kind.kind? {
        ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt11(b) => Some(b.hash),
        _ => None,
    }
}

#[async_trait::async_trait]
trait ReceiveSpark: Send + Sync {
    async fn swap_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        invoice: &str,
        amount_sats: u64,
    ) -> Result<LightningReceiveSwap, String>;
}

#[async_trait::async_trait]
impl ReceiveSpark for SparkService {
    async fn swap_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        invoice: &str,
        amount_sats: u64,
    ) -> Result<LightningReceiveSwap, String> {
        self.swap_for_lightning_receive(owner, payment_hash, invoice, amount_sats, 0)
            .await
    }
}

#[async_trait::async_trait]
trait SettleSpark: Send + Sync {
    async fn settle_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        amount_sats: u64,
    ) -> Result<String, String>;
}

#[async_trait::async_trait]
impl SettleSpark for SparkService {
    async fn settle_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        amount_sats: u64,
    ) -> Result<String, String> {
        self.settle_lightning_receive(owner, payment_hash, amount_sats)
            .await
    }
}

#[async_trait::async_trait]
trait ReceiveLdk: Send + Sync {
    async fn claim_receive(
        &self,
        payment_hash: &str,
        amount_msat: u64,
        preimage: &str,
    ) -> Result<(), String>;
    async fn fail_receive(&self, payment_hash: &str) -> Result<(), String>;
}

#[async_trait::async_trait]
impl ReceiveLdk for LdkServerClient {
    async fn claim_receive(
        &self,
        payment_hash: &str,
        amount_msat: u64,
        preimage: &str,
    ) -> Result<(), String> {
        self.bolt11_claim_for_hash(Bolt11ClaimForHashRequest {
            payment_hash: Some(payment_hash.to_string()),
            claimable_amount_msat: Some(amount_msat),
            preimage: preimage.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(|e| format!("claim Lightning receive {payment_hash}: {e}"))
    }

    async fn fail_receive(&self, payment_hash: &str) -> Result<(), String> {
        self.bolt11_fail_for_hash(Bolt11FailForHashRequest {
            payment_hash: payment_hash.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(|e| format!("fail Lightning receive {payment_hash}: {e}"))
    }
}

const RECEIVE_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];
const RECEIVE_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);

async fn retry_bounded<T, F, Fut>(mut operation: F, delays: &[Duration]) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let mut last_error = None;
    for attempt in 0..=delays.len() {
        match tokio::time::timeout(RECEIVE_OPERATION_TIMEOUT, operation()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => last_error = Some("receive operation timed out".to_string()),
        }
        if let Some(delay) = delays.get(attempt) {
            tokio::time::sleep(*delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| "operation failed without an error".to_string()))
}

fn validate_preimage(payment_hash: &str, preimage: &str) -> Result<(), String> {
    let bytes = hex::decode(preimage).map_err(|e| format!("preimage is not hex: {e}"))?;
    if bytes.len() != 32 {
        return Err("preimage must be 32 bytes".to_string());
    }
    let digest = hex::encode(Sha256::digest(bytes));
    if digest != payment_hash.to_lowercase() {
        return Err("preimage does not match the Lightning payment hash".to_string());
    }
    Ok(())
}

fn is_definitive_swap_failure(error: &str) -> bool {
    [
        "Insufficient",
        "insufficient",
        "Unselectable",
        "unselectable",
        "NEEDS_TOPUP",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

async fn fail_unfunded_receive<L: ReceiveLdk + ?Sized>(
    db: &Db,
    ldk: &L,
    payment_hash: &str,
    delays: &[Duration],
) {
    match retry_bounded(|| ldk.fail_receive(payment_hash), delays).await {
        Ok(()) => {
            let _ = db.set_receive_status(payment_hash, "HTLC_FAILED").await;
        }
        Err(error) => tracing::error!(
            payment_hash,
            "could not fail unfunded Lightning receive: {error}"
        ),
    }
}

/// Process one claimable receive under a lock shared by the event stream and
/// reconciler. The database checkpoint separates the operator commit from the
/// LDK claim, so an LDK retry never repeats the Spark transfer.
async fn process_standard_receive<S, L>(
    db: &Db,
    receive_lock: &tokio::sync::Mutex<()>,
    spark: &S,
    ldk: &L,
    payment_hash: &str,
    amount_msat: Option<u64>,
    delays: &[Duration],
) -> Result<bool, String>
where
    S: ReceiveSpark + ?Sized,
    L: ReceiveLdk + ?Sized,
{
    let _guard = receive_lock.lock().await;
    let Some(mut receive) = db.lightning_receive_for_hash(payment_hash).await? else {
        return Ok(false);
    };
    if receive.status == "TRANSFER_COMPLETED" || receive.status == "HTLC_FAILED" {
        return Ok(true);
    }
    let expected_msat = receive
        .amount_sats
        .checked_mul(1000)
        .ok_or_else(|| "Lightning receive amount is too large".to_string())?;
    let actual_msat =
        amount_msat.ok_or_else(|| "claimable Lightning payment has no amount".to_string())?;
    if actual_msat != expected_msat {
        fail_unfunded_receive(db, ldk, payment_hash, delays).await;
        return Err(format!(
            "claimable amount is {actual_msat} msat; expected {expected_msat} msat"
        ));
    }
    db.mark_receive_claimable(payment_hash, actual_msat).await?;

    if receive.transfer_id.is_none() || receive.preimage.is_none() {
        let swap = match retry_bounded(
            || {
                spark.swap_receive(
                    &receive.receiver,
                    payment_hash,
                    &receive.invoice,
                    receive.amount_sats,
                )
            },
            delays,
        )
        .await
        {
            Ok(swap) => swap,
            Err(error) => {
                db.set_receive_status(payment_hash, "TRANSFER_CREATION_FAILED")
                    .await?;
                // A connection error can hide a successful operator commit.
                // Leave that HTLC held for reconciliation. Only fail now when
                // no transfer could have been committed.
                if is_definitive_swap_failure(&error) {
                    fail_unfunded_receive(db, ldk, payment_hash, delays).await;
                }
                return Err(format!("Spark receive swap failed: {error}"));
            }
        };
        if let Err(error) = validate_preimage(payment_hash, &swap.preimage) {
            db.set_receive_status(payment_hash, "PAYMENT_PREIMAGE_RECOVERING_FAILED")
                .await?;
            return Err(error);
        }
        db.commit_lightning_receive_swap(
            payment_hash,
            &swap.transfer_id,
            &swap.preimage,
            &receive.request_id,
            &receive.owner,
        )
        .await?;
        receive.transfer_id = Some(swap.transfer_id);
        receive.preimage = Some(swap.preimage);
    }

    if receive.claim_submitted {
        return Ok(true);
    }
    let preimage = receive
        .preimage
        .as_deref()
        .ok_or_else(|| "committed Spark receive has no preimage".to_string())?;
    validate_preimage(payment_hash, preimage)?;
    retry_bounded(
        || ldk.claim_receive(payment_hash, expected_msat, preimage),
        delays,
    )
    .await?;
    db.mark_receive_claim_submitted(payment_hash).await?;
    Ok(true)
}

/// SSP-owned HODL receive: the SSP minted the preimage, so it pays Spark
/// from its own wallet and claims Lightning with the held preimage. The
/// claimable Lightning amount is validated before any Spark value moves —
/// the payout is irreversible, so a mismatched amount fails the hold (the
/// payer is refunded) instead of funding first and erroring later.
async fn process_ssp_owned_receive<S, L>(
    db: &Db,
    spark: &S,
    ldk: &L,
    payment_hash: &str,
    amount_msat: Option<u64>,
    delays: &[Duration],
    preimage: Option<&str>,
) -> Result<bool, String>
where
    S: SettleSpark + ?Sized,
    L: ReceiveLdk + ?Sized,
{
    let Some(receive) = db.lightning_receive_for_hash(payment_hash).await? else {
        return Ok(false);
    };
    if receive.status == "TRANSFER_COMPLETED" || receive.status == "HTLC_FAILED" {
        return Ok(true);
    }
    let expected_msat = receive
        .amount_sats
        .checked_mul(1000)
        .ok_or_else(|| "Lightning receive amount is too large".to_string())?;
    let actual_msat =
        amount_msat.ok_or_else(|| "claimable Lightning payment has no amount".to_string())?;
    if actual_msat != expected_msat {
        fail_unfunded_receive(db, ldk, payment_hash, delays).await;
        return Err(format!(
            "claimable amount is {actual_msat} msat; expected {expected_msat} msat"
        ));
    }
    let preimage = preimage
        .ok_or_else(|| format!("preimage disappeared for Lightning receive {payment_hash}"))?;
    validate_preimage(payment_hash, preimage)?;

    let transfer_id = spark
        .settle_receive(&receive.receiver, payment_hash, receive.amount_sats)
        .await?;
    db.insert_transfer(
        &transfer_id,
        &receive.request_id,
        "LIGHTNING_RECEIVE",
        "TRANSFER_COMPLETED",
        &receive.owner,
    )
    .await?;
    retry_bounded(
        || ldk.claim_receive(payment_hash, expected_msat, preimage),
        delays,
    )
    .await?;
    Ok(true)
}

#[derive(Clone)]
pub struct LdkGrpcBackend {
    pub client: LdkServerClient,
    pub node_id: Option<String>,
    db: Arc<Db>,
    spark: Arc<SparkService>,
    receive_lock: Arc<tokio::sync::Mutex<()>>,
    invoice_network: bitcoin::Network,
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
            receive_lock: Arc::new(tokio::sync::Mutex::new(())),
            invoice_network: invoice_network(&config.network)?,
        })
    }

    async fn settle_succeeded_payment(&self, payment: &Payment) -> Result<(), String> {
        let payment_id = payment.id.clone();
        let Some(send) = self.db.lightning_send_for_payment(&payment_id).await? else {
            return Err(format!(
                "no Lightning send request for payment {payment_id}"
            ));
        };
        let Some(kind) = payment.kind.as_ref().and_then(|kind| kind.kind.as_ref()) else {
            return Err(format!("payment {payment_id} has no payment kind"));
        };
        match kind {
            ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt11(bolt11) => {
                let preimage = bolt11
                    .preimage
                    .as_deref()
                    .ok_or_else(|| format!("payment {payment_id} succeeded without a preimage"))?;
                self.spark
                    .settle_lightning_send(&send.outbound_transfer_id, &bolt11.hash, preimage)
                    .await?;
            }
            ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt12Offer(offer)
                if send.payment_kind == "BOLT12" =>
            {
                let hash = offer
                    .hash
                    .as_deref()
                    .ok_or_else(|| format!("payment {payment_id} succeeded without a hash"))?;
                let preimage = offer
                    .preimage
                    .as_deref()
                    .ok_or_else(|| format!("payment {payment_id} succeeded without a preimage"))?;
                validate_preimage(hash, preimage)?;
            }
            _ => {
                return Err(format!(
                    "payment {payment_id} has an unexpected payment kind"
                ))
            }
        }
        self.db.set_payment(&payment_id, "SUCCEEDED").await
    }

    async fn fail_managed_payment(&self, payment_id: &str) -> Result<(), String> {
        let Some(send) = self.db.lightning_send_for_payment(payment_id).await? else {
            return Ok(());
        };
        if send.payment_kind == "BOLT12" {
            self.db.set_payment(payment_id, "REFUNDING").await?;
            self.spark
                .refund_bolt12_send(&send.owner, &send.outbound_transfer_id, send.amount_sats)
                .await?;
        }
        self.db.set_payment(payment_id, "FAILED").await
    }

    async fn finish_bolt12_receive(
        &self,
        offer_id: &str,
        payment_hash: &str,
        amount_msat: Option<u64>,
    ) -> Result<(), String> {
        let Some(receive) = self.db.lightning_receive_for_hash(offer_id).await? else {
            return Ok(());
        };
        if receive.status == "TRANSFER_COMPLETED" {
            return Ok(());
        }
        let expected_msat = receive
            .amount_sats
            .checked_mul(1000)
            .ok_or_else(|| "BOLT12 receive amount is too large".to_string())?;
        if !amount_msat.is_some_and(|amount| amount >= expected_msat) {
            return Err(format!(
                "BOLT12 receive has {amount_msat:?} msat; expected at least {expected_msat}"
            ));
        }
        let transfer_id = self
            .spark
            .settle_lightning_receive(&receive.receiver, payment_hash, receive.amount_sats)
            .await?;
        self.db
            .commit_bolt12_receive(
                offer_id,
                payment_hash,
                &transfer_id,
                &receive.request_id,
                &receive.owner,
            )
            .await
    }

    async fn is_managed_outbound(&self, payment_id: &str) -> Result<bool, String> {
        Ok(self
            .db
            .lightning_send_for_payment(payment_id)
            .await?
            .is_some())
    }

    async fn claim_ssp_owned_receive(
        &self,
        payment_hash: &str,
        amount_msat: Option<u64>,
    ) -> Result<bool, String> {
        let preimage = self.preimage_for(payment_hash).await;
        process_ssp_owned_receive(
            self.db.as_ref(),
            self.spark.as_ref(),
            &self.client,
            payment_hash,
            amount_msat,
            &RECEIVE_RETRY_DELAYS,
            preimage.as_deref(),
        )
        .await
    }

    async fn finish_received_payment(&self, payment_hash: &str) -> Result<bool, String> {
        let Some(receive) = self.db.lightning_receive_for_hash(payment_hash).await? else {
            return Ok(false);
        };
        let transfer_id = match receive.transfer_id {
            Some(id) => id,
            None => self
                .db
                .transfer_for_request(&receive.request_id, &receive.owner)
                .await?
                .ok_or_else(|| {
                    format!("Lightning receive {payment_hash} settled before its Spark transfer")
                })?,
        };
        self.db
            .insert_transfer(
                &transfer_id,
                &receive.request_id,
                "LIGHTNING_RECEIVE",
                "TRANSFER_COMPLETED",
                &receive.owner,
            )
            .await?;
        self.db
            .set_receive_status(payment_hash, "TRANSFER_COMPLETED")
            .await?;
        Ok(true)
    }

    async fn process_inbound_claimable(
        &self,
        payment_hash: &str,
        amount_msat: Option<u64>,
    ) -> Result<bool, String> {
        // Keep the explicit SSP-minted HODL extension isolated. Standard SDK
        // receives never put their preimage in this table.
        if self.preimage_for(payment_hash).await.is_some() {
            return self
                .claim_ssp_owned_receive(payment_hash, amount_msat)
                .await;
        }
        process_standard_receive(
            self.db.as_ref(),
            self.receive_lock.as_ref(),
            self.spark.as_ref(),
            &self.client,
            payment_hash,
            amount_msat,
            &RECEIVE_RETRY_DELAYS,
        )
        .await
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
                            self.fail_managed_payment(&payment.id).await?;
                        }
                        _ => self.db.set_payment(&payment.id, "PENDING").await?,
                    }
                    continue;
                }
                if let Some((offer_id, payment_hash)) = bolt12_offer_ids(Some(payment.clone())) {
                    if payment.status == PaymentStatus::Succeeded as i32 {
                        if let Err(error) = self
                            .finish_bolt12_receive(&offer_id, &payment_hash, payment.amount_msat)
                            .await
                        {
                            tracing::warn!(
                                offer_id,
                                payment_hash,
                                "BOLT12 Spark payout is pending: {error}"
                            );
                        }
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
                        }
                    }
                    _ => match self
                        .process_inbound_claimable(&payment_hash, payment.amount_msat)
                        .await
                    {
                        Ok(true) => {}
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
            if retry_bounded(
                || self.client.fail_receive(&payment_hash),
                &RECEIVE_RETRY_DELAYS,
            )
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

fn invoice_network(network: &str) -> Result<bitcoin::Network, String> {
    match network.to_ascii_uppercase().as_str() {
        "MAINNET" => Ok(bitcoin::Network::Bitcoin),
        "TESTNET" => Ok(bitcoin::Network::Testnet),
        "SIGNET" => Ok(bitcoin::Network::Signet),
        "REGTEST" | "LOCAL" => Ok(bitcoin::Network::Regtest),
        _ => Err(format!("unsupported Lightning invoice network {network}")),
    }
}

fn validate_created_invoice(
    invoice: &str,
    payment_hash: &str,
    amount_sats: u64,
    network: bitcoin::Network,
) -> Result<(), String> {
    let invoice = lightning_invoice::Bolt11Invoice::from_str(invoice)
        .map_err(|e| format!("decode created BOLT11 invoice: {e}"))?;
    if invoice.payment_hash().to_string() != payment_hash.to_lowercase() {
        return Err("created invoice payment hash does not match the wallet hash".to_string());
    }
    if invoice.amount_milli_satoshis() != Some(sats_to_msats(amount_sats)?) {
        return Err("created invoice amount does not match the requested amount".to_string());
    }
    if invoice.network() != network {
        return Err("created invoice network does not match the SSP network".to_string());
    }
    Ok(())
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
        if invoice.to_ascii_lowercase().starts_with("lno1") {
            let amount_sats =
                amount_sats.ok_or_else(|| "BOLT12 sends require amount_sats".to_string())?;
            if amount_sats == 0 {
                return Err("Lightning send amount must be positive".to_string());
            }
            return self
                .spark
                .verify_bolt12_send(owner, outbound_transfer_id, amount_sats)
                .await;
        }
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
            return match self.fail_managed_payment(payment_id).await {
                Ok(()) => "FAILED".to_string(),
                Err(error) => {
                    tracing::warn!(payment_id, "BOLT12 refund is pending: {error}");
                    "REFUNDING".to_string()
                }
            };
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
                Some(p) if p.status == PaymentStatus::Failed as i32 => {
                    match self.fail_managed_payment(&p.id).await {
                        Ok(()) => "FAILED".to_string(),
                        Err(error) => {
                            tracing::warn!(payment_id, "BOLT12 refund is pending: {error}");
                            "REFUNDING".to_string()
                        }
                    }
                }
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
        let request = Bolt11ReceiveForHashRequest {
            amount_msat: Some(sats_to_msats(amount_sats)?),
            description: description_of(memo),
            expiry_secs,
            payment_hash: payment_hash_hex.to_string(),
        };
        let resp = retry_bounded(
            || {
                let request = request.clone();
                async {
                    self.client
                        .bolt11_receive_for_hash(request)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            &RECEIVE_RETRY_DELAYS,
        )
        .await?;
        validate_created_invoice(
            &resp.invoice,
            payment_hash_hex,
            amount_sats,
            self.invoice_network,
        )?;
        Ok(CreateInvoiceResult {
            invoice: resp.invoice,
        })
    }

    async fn create_bolt12_offer(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateOfferResult, String> {
        let response = self
            .client
            .bolt12_receive(Bolt12ReceiveRequest {
                description: memo.to_string(),
                amount_msat: Some(sats_to_msats(amount_sats)?),
                expiry_secs: Some(expiry_secs),
                quantity: None,
            })
            .await
            .map_err(|e| format!("create BOLT12 offer: {e}"))?;
        Ok(CreateOfferResult {
            offer: response.offer,
            offer_id: response.offer_id.to_lowercase(),
        })
    }

    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        if validate_preimage(payment_hash_hex, preimage_hex).is_err() {
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
                if let Err(error) = self.fail_managed_payment(&payment_id).await {
                    tracing::warn!(payment_id, "BOLT12 refund is pending: {error}");
                }
            }
            LnEvent::InboundClaimable {
                payment_hash,
                amount_msat,
            } => {
                match self
                    .process_inbound_claimable(&payment_hash, amount_msat)
                    .await
                {
                    Ok(true) => {
                        tracing::info!(
                            "committed Spark receive and submitted LDK claim {payment_hash}"
                        );
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
            LnEvent::InboundBolt12Received {
                offer_id,
                payment_hash,
                amount_msat,
            } => {
                if let Err(error) = self
                    .finish_bolt12_receive(&offer_id, &payment_hash, amount_msat)
                    .await
                {
                    tracing::warn!(
                        offer_id,
                        payment_hash,
                        "BOLT12 Spark payout is pending: {error}"
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
        })
    }

    async fn create_bolt12_offer(
        &self,
        _amount_sats: u64,
        _memo: &str,
        _expiry_secs: u32,
    ) -> Result<CreateOfferResult, String> {
        Err("BOLT12 receive requires live Lightning".to_string())
    }

    async fn reveal_and_claim(&self, payment_hash_hex: &str, preimage_hex: &str) -> bool {
        if validate_preimage(payment_hash_hex, preimage_hex).is_err() {
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
            LnEvent::InboundClaimable { payment_hash, .. } => {
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
            LnEvent::InboundBolt12Received { offer_id, .. } => {
                let _ = self
                    .db
                    .set_receive_status(&offer_id, "TRANSFER_COMPLETED")
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex as StdMutex,
    };

    #[derive(Default)]
    struct MockSpark {
        calls: AtomicUsize,
        failures: AtomicUsize,
        preimage: String,
        log: Arc<StdMutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl ReceiveSpark for MockSpark {
        async fn swap_receive(
            &self,
            _owner: &str,
            _payment_hash: &str,
            _invoice: &str,
            _amount_sats: u64,
        ) -> Result<LightningReceiveSwap, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.log.lock().unwrap().push("spark");
            if self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                return Err("operator unavailable".to_string());
            }
            Ok(LightningReceiveSwap {
                transfer_id: "00000000-0000-4000-8000-000000000001".to_string(),
                preimage: self.preimage.clone(),
            })
        }
    }

    #[derive(Default)]
    struct MockSettle {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SettleSpark for MockSettle {
        async fn settle_receive(
            &self,
            _owner: &str,
            _payment_hash: &str,
            _amount_sats: u64,
        ) -> Result<String, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok("00000000-0000-4000-8000-000000000009".to_string())
        }
    }

    #[derive(Default)]
    struct MockLdk {
        claims: AtomicUsize,
        failures: AtomicUsize,
        failed_holds: AtomicUsize,
        log: Arc<StdMutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl ReceiveLdk for MockLdk {
        async fn claim_receive(
            &self,
            _payment_hash: &str,
            _amount_msat: u64,
            _preimage: &str,
        ) -> Result<(), String> {
            self.claims.fetch_add(1, Ordering::SeqCst);
            self.log.lock().unwrap().push("claim");
            if self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                Err("ldk unavailable".to_string())
            } else {
                Ok(())
            }
        }

        async fn fail_receive(&self, _payment_hash: &str) -> Result<(), String> {
            self.failed_holds.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn receive_fixture() -> (Db, std::path::PathBuf, String, String) {
        let dir = std::env::temp_dir().join(format!("open-ssp-receive-{}", uuid::Uuid::new_v4()));
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        let preimage = "01".repeat(32);
        let payment_hash = hex::encode(Sha256::digest(hex::decode(&preimage).unwrap()));
        db.insert_request(
            "request",
            "LIGHTNING_RECEIVE",
            "request-owner",
            &chrono::Utc::now().to_rfc3339(),
            &serde_json::json!({
                "payment_hash": payment_hash,
                "amount_sats": 5_000,
                "invoice": "ln-invoice",
                "receiver_identity_pubkey": "receiver",
                "expiry_secs": 300,
            }),
            None,
        )
        .await
        .unwrap();
        db.set_receive_status(&payment_hash, "INVOICE_CREATED")
            .await
            .unwrap();
        (db, dir, payment_hash, preimage)
    }

    fn mock_spark(preimage: String, log: Arc<StdMutex<Vec<&'static str>>>) -> MockSpark {
        MockSpark {
            preimage,
            log,
            ..Default::default()
        }
    }

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

    #[test]
    fn bolt12_receive_event_keeps_offer_and_payment_ids() {
        use ldk_server_client::ldk_server_grpc::{
            events::{EventEnvelope, PaymentReceived},
            types::{payment_kind, Bolt12Offer, PaymentKind},
        };

        let payment = Payment {
            id: "payment-id".to_string(),
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt12Offer(Bolt12Offer {
                    hash: Some("payment-hash".to_string()),
                    offer_id: "offer-id".to_string(),
                    ..Default::default()
                })),
            }),
            amount_msat: Some(1_001_000),
            ..Default::default()
        };
        let events = map_envelope(EventEnvelope {
            event: Some(LdkRawEvent::PaymentReceived(PaymentReceived {
                payment: Some(payment),
                custom_records: Vec::new(),
            })),
        });

        assert!(matches!(
            events.as_slice(),
            [LnEvent::InboundBolt12Received {
                offer_id,
                payment_hash,
                amount_msat: Some(1_001_000),
            }] if offer_id == "offer-id" && payment_hash == "payment-hash"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wallet_created_receive_commits_and_claims() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let log = Arc::new(StdMutex::new(Vec::new()));
        let spark = mock_spark(preimage, log.clone());
        let ldk = MockLdk {
            log: log.clone(),
            ..Default::default()
        };
        let lock = tokio::sync::Mutex::new(());

        assert!(
            process_standard_receive(&db, &lock, &spark, &ldk, &hash, Some(5_000_000), &[],)
                .await
                .unwrap()
        );
        let receive = db.lightning_receive_for_hash(&hash).await.unwrap().unwrap();
        assert!(receive.transfer_id.is_some());
        assert_eq!(receive.preimage, Some("01".repeat(32)));
        assert!(receive.claim_submitted);
        assert_eq!(*log.lock().unwrap(), vec!["spark", "claim"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mismatched_operator_preimage_is_not_claimed() {
        let (db, dir, hash, _) = receive_fixture().await;
        let spark = mock_spark("02".repeat(32), Arc::default());
        let ldk = MockLdk::default();

        let error = process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .unwrap_err();
        assert!(error.contains("does not match"));
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_claimable_event_does_not_repeat_transfer_or_claim() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk::default();
        let lock = tokio::sync::Mutex::new(());

        for _ in 0..2 {
            process_standard_receive(&db, &lock, &spark, &ldk, &hash, Some(5_000_000), &[])
                .await
                .unwrap();
        }
        assert_eq!(spark.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_after_spark_commit_resumes_only_ldk_claim() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        db.commit_lightning_receive_swap(
            &hash,
            "00000000-0000-4000-8000-000000000001",
            &preimage,
            "request",
            "request-owner",
        )
        .await
        .unwrap();
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk::default();

        process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn operator_failure_keeps_hold_without_claiming() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = MockSpark {
            failures: AtomicUsize::new(1),
            ..mock_spark(preimage, Arc::default())
        };
        let ldk = MockLdk::default();

        assert!(process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .is_err());
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ldk_claim_retries_without_repeating_spark_transfer() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk {
            failures: AtomicUsize::new(2),
            ..Default::default()
        };
        let delays = [Duration::ZERO, Duration::ZERO];

        process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &delays,
        )
        .await
        .unwrap();
        assert_eq!(spark.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 3);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn claimable_amount_must_match_invoice_amount() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk::default();

        assert!(process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(4_999_000),
            &[],
        )
        .await
        .is_err());
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn receive_invoice_networks_are_explicit() {
        assert_eq!(
            invoice_network("MAINNET").unwrap(),
            bitcoin::Network::Bitcoin
        );
        assert_eq!(
            invoice_network("TESTNET").unwrap(),
            bitcoin::Network::Testnet
        );
        assert_eq!(invoice_network("SIGNET").unwrap(), bitcoin::Network::Signet);
        assert_eq!(invoice_network("LOCAL").unwrap(), bitcoin::Network::Regtest);
        assert!(invoice_network("unknown").is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ssp_owned_receive_rejects_amount_mismatch_before_funding() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = MockSettle::default();
        let ldk = MockLdk::default();

        let error = process_ssp_owned_receive(
            &db,
            &spark,
            &ldk,
            &hash,
            Some(4_999_000),
            &[],
            Some(&preimage),
        )
        .await
        .unwrap_err();
        assert!(error.contains("expected"));
        // No Spark value moved and the hold is failed so the payer refunds.
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 1);
        assert_eq!(
            db.transfer_for_request("request", "request-owner")
                .await
                .unwrap(),
            None
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ssp_owned_receive_funds_and_claims_exact_amount() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = MockSettle::default();
        let ldk = MockLdk::default();

        assert!(process_ssp_owned_receive(
            &db,
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
            Some(&preimage),
        )
        .await
        .unwrap());
        assert_eq!(spark.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 1);
        assert_eq!(
            db.transfer_for_request("request", "request-owner")
                .await
                .unwrap(),
            Some("00000000-0000-4000-8000-000000000009".to_string())
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ssp_owned_receive_requires_its_preimage_before_funding() {
        let (db, dir, hash, _) = receive_fixture().await;
        let spark = MockSettle::default();
        let ldk = MockLdk::default();

        assert!(
            process_ssp_owned_receive(&db, &spark, &ldk, &hash, Some(5_000_000), &[], None,)
                .await
                .is_err()
        );
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
