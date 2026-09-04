use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use bip39::{Language, Mnemonic};
use bitcoin::{consensus::deserialize, Transaction};
use serde::Serialize;
use sha2::{Digest, Sha256};
use spark_wallet::{
    DefaultSigner, Network, OperatorPoolConfig, Preimage, PreimageRequestRole,
    PreimageRequestStatus, SparkAddress, SparkSigner, SparkSignerAdapter, SparkWallet,
    SparkWalletConfig, TransferId, TransferStatus, WalletTransfer,
};

use crate::config::Config;

#[derive(Debug, Serialize)]
pub struct SparkHealth {
    pub address: String,
    pub identity_pubkey: String,
    pub available_sats: u64,
    pub owned_sats: u64,
    pub needs_topup: bool,
}

#[derive(Debug)]
pub struct SwapFill {
    pub transfer_id: String,
    pub leaf_ids: Vec<String>,
}

pub struct SparkService {
    wallet: Arc<SparkWallet>,
    network: Network,
    identity: spark_wallet::PublicKey,
    liquidity_lock: tokio::sync::Mutex<()>,
    needs_topup: AtomicBool,
}

impl SparkService {
    pub async fn connect(config: &Config) -> Result<Arc<Self>, String> {
        let network = parse_network(&config.network)?;
        let mnemonic =
            load_or_create_mnemonic(&config.spark_mnemonic_file, config.spark_mnemonic_required)?;
        let seed = mnemonic.to_seed("");
        let signer = Arc::new(DefaultSigner::new(&seed, network).map_err(|e| e.to_string())?);
        let signer = Arc::new(SparkSignerAdapter::new(signer));
        let identity = signer
            .get_identity_public_key()
            .await
            .map_err(|e| e.to_string())?;
        if !config.ssp_identity_pubkey.is_empty()
            && identity.to_string() != config.ssp_identity_pubkey.to_lowercase()
        {
            return Err(format!(
                "embedded Spark identity {identity} does not match SSP_IDENTITY_PUBKEY {}",
                config.ssp_identity_pubkey
            ));
        }

        let hosts = csv(&config.so_hosts);
        let pubkeys = csv(&config.so_identity_pubkeys);
        if hosts.is_empty() || hosts.len() != pubkeys.len() {
            return Err(
                "SO_HOSTS and SO_IDENTITY_PUBKEYS must have the same nonzero length".to_string(),
            );
        }
        let cert_files = csv(&config.so_cert_files);
        if !cert_files.is_empty() && cert_files.len() != hosts.len() {
            return Err("SO_CERT_FILES must be empty or match SO_HOSTS".to_string());
        }
        let mut operators = Vec::with_capacity(hosts.len());
        for (index, (host, pubkey)) in hosts.iter().zip(&pubkeys).enumerate() {
            let cert = if cert_files.is_empty() || cert_files[index].is_empty() {
                None
            } else {
                Some(
                    std::fs::read(&cert_files[index])
                        .map_err(|e| format!("read SO certificate {}: {e}", cert_files[index]))?,
                )
            };
            let address = if host.contains("://") {
                host.clone()
            } else {
                format!("https://{host}")
            };
            operators.push(
                SparkWalletConfig::create_operator_config(
                    index,
                    &format!("{:064x}", index + 1),
                    &address,
                    cert.as_deref(),
                    pubkey,
                )
                .map_err(|e| format!("operator {index}: {e}"))?,
            );
        }

        let mut wallet_config = SparkWalletConfig::default_config(network);
        wallet_config.operator_pool =
            OperatorPoolConfig::new(0, operators).map_err(|e| e.to_string())?;
        wallet_config.service_provider_config = SparkWalletConfig::create_service_provider_config(
            &config.ssp_public_url,
            &identity.to_string(),
            Some("graphql/spark/rc".to_string()),
        )
        .map_err(|e| e.to_string())?;
        wallet_config.split_secret_threshold = config.frost_threshold as u32;
        wallet_config.leaf_auto_optimize_enabled = false;

        let wallet = Arc::new(
            SparkWallet::connect(wallet_config, signer)
                .await
                .map_err(|e| format!("connect embedded Spark wallet: {e}"))?,
        );
        Ok(Arc::new(Self {
            wallet,
            network,
            identity,
            liquidity_lock: tokio::sync::Mutex::new(()),
            needs_topup: AtomicBool::new(false),
        }))
    }

    pub fn identity(&self) -> String {
        self.identity.to_string()
    }

    pub async fn start_background_processing(&self) {
        self.wallet.start_background_processing().await;
    }

    pub async fn health(&self) -> Result<SparkHealth, String> {
        let leaves = self.wallet.list_leaves().await.map_err(|e| e.to_string())?;
        let available_sats = leaves.available.iter().map(|leaf| leaf.value).sum();
        let owned_sats = available_sats
            + leaves
                .available_missing_from_operators
                .iter()
                .map(|leaf| leaf.value)
                .sum::<u64>();
        let address = self
            .wallet
            .get_spark_address()
            .and_then(|address| {
                address
                    .to_address_string()
                    .map_err(|e| spark_wallet::SparkWalletError::Generic(e.to_string()))
            })
            .map_err(|e| e.to_string())?;
        Ok(SparkHealth {
            address,
            identity_pubkey: self.identity(),
            available_sats,
            owned_sats,
            needs_topup: available_sats == 0 || self.needs_topup.load(Ordering::Relaxed),
        })
    }

    pub async fn generate_deposit_address(&self) -> Result<String, String> {
        self.wallet
            .generate_deposit_address()
            .await
            .map(|result| result.address.to_string())
            .map_err(|e| e.to_string())
    }

    pub async fn claim_deposit(
        &self,
        transaction_hex: &str,
        vout: u32,
    ) -> Result<Vec<u64>, String> {
        let bytes = hex::decode(transaction_hex).map_err(|e| format!("transaction hex: {e}"))?;
        let tx: Transaction = deserialize(&bytes).map_err(|e| format!("transaction: {e}"))?;
        self.wallet
            .claim_deposit(tx, vout)
            .await
            .map(|leaves| leaves.into_iter().map(|leaf| leaf.value).collect())
            .map_err(|e| e.to_string())
    }

    pub async fn sign_message(&self, message: &str) -> Result<String, String> {
        self.wallet
            .sign_message(message)
            .await
            .map(|signature| hex::encode(signature.serialize_der()))
            .map_err(|e| e.to_string())
    }

    pub async fn store_preimage_shares(
        &self,
        payment_hash: Vec<u8>,
        shares: HashMap<String, Vec<u8>>,
        threshold: u32,
        invoice: String,
    ) -> Result<(), String> {
        self.wallet
            .store_preimage_shares(payment_hash, shares, threshold, invoice, self.identity)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn verify_lightning_send(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        payment_hash: &str,
        amount_sats: u64,
    ) -> Result<(), String> {
        let request = self
            .find_htlc(outbound_transfer_id, payment_hash)
            .await?
            .ok_or_else(|| "matching preimage swap was not found".to_string())?;
        let transfer = request
            .transfer
            .ok_or_else(|| "preimage swap has no transfer".to_string())?;
        if transfer.sender_identity_public_key.to_string() != owner.to_lowercase() {
            return Err("preimage swap sender does not match session owner".to_string());
        }
        if transfer.receiver_identity_public_key != self.identity {
            return Err("preimage swap receiver does not match the SSP".to_string());
        }
        if transfer.total_value != amount_sats {
            return Err(format!(
                "preimage swap has {} sats; expected {amount_sats}",
                transfer.total_value
            ));
        }
        if request.status != PreimageRequestStatus::WaitingForPreimage || request.preimage.is_some()
        {
            return Err("preimage swap is not waiting for payment".to_string());
        }
        Ok(())
    }

    pub async fn settle_lightning_send(
        &self,
        outbound_transfer_id: &str,
        payment_hash: &str,
        preimage_hex: &str,
    ) -> Result<(), String> {
        let preimage = Preimage::from_hex(preimage_hex).map_err(|e| e.to_string())?;
        if preimage.compute_hash().to_string() != payment_hash.to_lowercase() {
            return Err("preimage does not match the payment hash".to_string());
        }
        let request = self
            .find_htlc(outbound_transfer_id, payment_hash)
            .await?
            .ok_or_else(|| "matching preimage swap was not found".to_string())?;
        if request.status == PreimageRequestStatus::PreimageShared && request.preimage.is_some() {
            return Ok(());
        }
        if request.status != PreimageRequestStatus::WaitingForPreimage {
            return Err("preimage swap can no longer be settled".to_string());
        }
        let transfer = self
            .wallet
            .claim_htlc(&preimage)
            .await
            .map_err(|e| e.to_string())?;
        if transfer.id.to_string() != outbound_transfer_id {
            return Err("settled transfer id does not match the funded transfer".to_string());
        }
        Ok(())
    }

    pub async fn settle_lightning_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        amount_sats: u64,
    ) -> Result<String, String> {
        let _guard = self.liquidity_lock.lock().await;
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        let transfer_id = payment_transfer_id(payment_hash)?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        let receiver = SparkAddress::new(owner_key, self.network, None);
        let transfer = match self.find_transfer(&transfer_id).await? {
            Some(transfer) => transfer,
            None => self
                .wallet
                .transfer(amount_sats, &receiver, Some(transfer_id.clone()))
                .await
                .map_err(|e| self.liquidity_error(e.to_string()))?,
        };
        validate_transfer(&transfer, self.identity, owner_key, amount_sats)?;
        self.needs_topup.store(false, Ordering::Relaxed);
        Ok(transfer.id.to_string())
    }

    pub async fn fill_swap(
        self: &Arc<Self>,
        owner: &str,
        outbound_transfer_id: &str,
        adaptor_pubkey: &str,
        targets: &[u64],
        received_total_sats: u64,
        payout_total_sats: u64,
    ) -> Result<SwapFill, String> {
        if targets.is_empty() || targets.contains(&0) {
            return Err("swap targets must be positive".to_string());
        }
        if payout_total_sats != received_total_sats {
            return Err("Swap V3 fees are not supported by the operator protocol".to_string());
        }
        let target_total = targets.iter().try_fold(0u64, |sum, value| {
            sum.checked_add(*value)
                .ok_or_else(|| "swap target total overflow".to_string())
        })?;
        if target_total > payout_total_sats {
            return Err("swap targets exceed the payout".to_string());
        }

        let _guard = self.liquidity_lock.lock().await;
        let primary_id = TransferId::from_str(outbound_transfer_id)?;
        let primary = self.wait_for_transfer(&primary_id).await?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        validate_transfer(&primary, owner_key, self.identity, received_total_sats)?;

        let counter_id = counter_transfer_id(outbound_transfer_id);
        let counter = match self.find_transfer(&counter_id).await? {
            Some(transfer) => transfer,
            None => {
                self.wallet.sync().await.map_err(|e| e.to_string())?;
                let adaptor =
                    spark_wallet::PublicKey::from_str(adaptor_pubkey).map_err(|e| e.to_string())?;
                let receiver = SparkAddress::new(owner_key, self.network, None);
                let mut amounts = targets.to_vec();
                let change = received_total_sats - target_total;
                if change > 0 {
                    amounts.push(change);
                }
                self.wallet
                    .transfer_swap_counter(amounts, &receiver, primary_id, adaptor, counter_id)
                    .await
                    .map_err(|e| self.liquidity_error(e.to_string()))?
            }
        };
        validate_transfer(&counter, self.identity, owner_key, received_total_sats)?;
        let leaf_ids = counter
            .leaves
            .iter()
            .map(|leaf| leaf.leaf.id.to_string())
            .collect::<Vec<_>>();
        if leaf_ids.is_empty() {
            return Err("counter transfer has no leaves".to_string());
        }
        self.needs_topup.store(false, Ordering::Relaxed);
        let service = self.clone();
        let primary_id = TransferId::from_str(outbound_transfer_id)?;
        tokio::spawn(async move { service.reconcile_swap_claim(primary_id).await });
        Ok(SwapFill {
            transfer_id: counter.id.to_string(),
            leaf_ids,
        })
    }

    async fn find_htlc(
        &self,
        transfer_id: &str,
        payment_hash: &str,
    ) -> Result<Option<spark_wallet::PreimageRequestWithTransfer>, String> {
        let result = self
            .wallet
            .query_htlc(
                vec![transfer_id.to_string()],
                vec![payment_hash.to_lowercase()],
                None,
                PreimageRequestRole::Receiver,
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(result.items.into_iter().find(|request| {
            request.payment_hash.to_string() == payment_hash.to_lowercase()
                && request
                    .transfer
                    .as_ref()
                    .is_some_and(|transfer| transfer.id.to_string() == transfer_id)
        }))
    }

    async fn wait_for_transfer(&self, id: &TransferId) -> Result<WalletTransfer, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match self.find_transfer(id).await {
                Ok(Some(transfer)) => return Ok(transfer),
                Ok(None) => {}
                Err(error) => tracing::debug!(%error, %id, "Spark transfer lookup retry"),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("outbound swap transfer was not found for the SSP".to_string());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn find_transfer(&self, id: &TransferId) -> Result<Option<WalletTransfer>, String> {
        self.wallet
            .get_transfer(id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn reconcile_swap_claim(self: Arc<Self>, primary_id: TransferId) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let mut delay = Duration::from_millis(250);
        while tokio::time::Instant::now() < deadline {
            match self.find_transfer(&primary_id).await {
                Ok(Some(transfer)) if transfer.status == TransferStatus::Completed => return,
                Ok(Some(transfer))
                    if matches!(
                        transfer.status,
                        TransferStatus::SenderKeyTweaked
                            | TransferStatus::ReceiverKeyTweaked
                            | TransferStatus::ReceiverRefundSigned
                    ) =>
                {
                    match self.wallet.process_transfer(&transfer).await {
                        Ok(_) => return,
                        Err(error) => tracing::warn!(%error, %primary_id, "swap claim retry"),
                    }
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, %primary_id, "swap lookup retry"),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
        tracing::error!(%primary_id, "swap claim reconciliation timed out");
    }

    fn liquidity_error(&self, error: String) -> String {
        if error.contains("available balance")
            || error.contains("Insufficient")
            || error.contains("Target amounts")
        {
            self.needs_topup.store(true, Ordering::Relaxed);
            format!("NEEDS_TOPUP: {error}")
        } else {
            error
        }
    }
}

fn validate_transfer(
    transfer: &WalletTransfer,
    sender: spark_wallet::PublicKey,
    receiver: spark_wallet::PublicKey,
    total_sats: u64,
) -> Result<(), String> {
    if transfer.sender_id != sender {
        return Err("transfer sender does not match".to_string());
    }
    if transfer.receiver_id != receiver {
        return Err("transfer receiver does not match".to_string());
    }
    if transfer.total_value_sat != total_sats {
        return Err(format!(
            "transfer has {} sats; expected {total_sats}",
            transfer.total_value_sat
        ));
    }
    Ok(())
}

fn csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_network(value: &str) -> Result<Network, String> {
    match value.to_ascii_uppercase().as_str() {
        "MAINNET" => Ok(Network::Mainnet),
        "TESTNET" => Ok(Network::Testnet),
        "SIGNET" => Ok(Network::Signet),
        "LOCAL" | "REGTEST" => Ok(Network::Regtest),
        _ => Err(format!("unsupported Spark network {value}")),
    }
}

fn load_or_create_mnemonic(path: &str, required: bool) -> Result<Mnemonic, String> {
    match std::fs::read_to_string(path) {
        Ok(value) => {
            return Mnemonic::parse_in_normalized(Language::English, value.trim())
                .map_err(|e| format!("parse Spark mnemonic: {e}"));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "Spark mnemonic {path} is required; restore the funded wallet mnemonic"
            ));
        }
        Err(error) => return Err(format!("read Spark mnemonic {path}: {error}")),
    }
    let mnemonic = Mnemonic::generate_in(Language::English, 12)
        .map_err(|e| format!("generate Spark mnemonic: {e}"))?;
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create mnemonic directory: {e}"))?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("create Spark mnemonic {}: {e}", path.display()))?;
    writeln!(file, "{mnemonic}").map_err(|e| format!("write Spark mnemonic: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("sync Spark mnemonic: {e}"))?;
    Ok(mnemonic)
}

fn payment_transfer_id(payment_hash: &str) -> Result<TransferId, String> {
    let bytes = hex::decode(payment_hash).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("payment hash must be 32 bytes".to_string());
    }
    deterministic_transfer_id(&bytes[..16])
}

fn counter_transfer_id(primary_id: &str) -> TransferId {
    let digest = Sha256::digest(format!("swap-counter:{primary_id}").as_bytes());
    deterministic_transfer_id(&digest[..16]).expect("sha256 prefix is 16 bytes")
}

fn deterministic_transfer_id(source: &[u8]) -> Result<TransferId, String> {
    let mut bytes: [u8; 16] = source
        .try_into()
        .map_err(|_| "transfer id source must be 16 bytes".to_string())?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(TransferId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_all_supported_networks() {
        assert_eq!(parse_network("MAINNET").unwrap(), Network::Mainnet);
        assert_eq!(parse_network("TESTNET").unwrap(), Network::Testnet);
        assert_eq!(parse_network("SIGNET").unwrap(), Network::Signet);
        assert_eq!(parse_network("LOCAL").unwrap(), Network::Regtest);
    }

    #[test]
    fn payment_ids_match_the_previous_sidecar_format() {
        let hash = "00112233445566778899aabbccddeeff00000000000000000000000000000000";
        assert_eq!(
            payment_transfer_id(hash).unwrap().to_string(),
            "00112233-4455-4677-8899-aabbccddeeff"
        );
    }

    #[test]
    fn required_mnemonic_does_not_create_a_new_identity() {
        let path = std::env::temp_dir().join(format!(
            "mutinynet-ssp-required-mnemonic-{}",
            std::process::id()
        ));
        let error = load_or_create_mnemonic(path.to_str().unwrap(), true).unwrap_err();
        assert!(error.contains("is required"));
        assert!(!path.exists());
    }
}
