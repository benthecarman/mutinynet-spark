use serde::{Deserialize, Serialize};

/// Runtime config. All values have sane regtest defaults.
/// Set via env; works for any network (regtest/signet/testnet/mainnet/custom MutinyNet).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    pub network: String,
    /// Compressed secp256k1 hex, 33 bytes. This is the `identityPublicKey`
    /// wallets put in `sspClientOptions`. SO sends Spark transfers to this key.
    pub ssp_identity_pubkey: String,
    /// Hex secret for signing static-deposit quotes + receive manifests.
    /// Unset = generate on first boot into <SSP_DATA_DIR>/ssp.key.
    pub ssp_secret_key_hex: String,
    /// Directory for sqlite + key file (volume-mount in compose).
    pub data_dir: String,
    /// Live LDK backend (host:port WITHOUT scheme, e.g. "ldk-server:3536").
    /// Empty = fake mode.
    pub ldk_grpc_addr: String,
    pub ldk_api_key: String,
    pub ldk_api_key_file: String,
    pub ldk_tls_cert_file: String,
    pub fee_ppm_lightning_send: u64,
    pub fee_flat_sats_swap: u64,
    /// Swap-fill sidecar (funded Spark wallet). Unset = swaps return empty
    /// swapLeaves, which the SDK rejects by design.
    pub sidecar_url: String,
    pub sidecar_token: String,
}

impl Config {
    pub fn from_env() -> Self {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            listen_addr: get("SSP_LISTEN_ADDR", "127.0.0.1:5000"),
            network: get("SSP_NETWORK", "REGTEST"),
            // Empty = use the resolved signing key's pubkey (first boot
            // generates into <SSP_DATA_DIR>/ssp.key; publish via /health).
            ssp_identity_pubkey: get("SSP_IDENTITY_PUBKEY", ""),
            ssp_secret_key_hex: get("SSP_SECRET_KEY_HEX", ""),
            data_dir: get("SSP_DATA_DIR", "./data"),
            ldk_grpc_addr: get("LDK_GRPC_ADDR", ""),
            ldk_api_key: get("LDK_API_KEY", ""),
            ldk_api_key_file: get("LDK_API_KEY_FILE", ""),
            ldk_tls_cert_file: get("LDK_TLS_CERT_FILE", ""),
            // Decision: 0 fee. Kept as a knob for later.
            fee_ppm_lightning_send: get("SSP_LN_FEE_PPM", "0").parse().unwrap_or(0),
            fee_flat_sats_swap: get("SSP_SWAP_FEE_SATS", "0").parse().unwrap_or(0),
            sidecar_url: get("SIDECAR_URL", ""),
            sidecar_token: get("SIDECAR_TOKEN", ""),
        }
    }

    /// Resolve the signing key: env, else key file, else generate + persist.
    /// Returns (secret_hex, pubkey_hex).
    pub fn resolve_signing_key(&self) -> Result<(String, String), String> {
        use secp256k1::{Secp256k1, SecretKey};
        if !self.ssp_secret_key_hex.is_empty() {
            let secret = SecretKey::from_slice(
                &hex::decode(self.ssp_secret_key_hex.trim()).map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
            let pubkey =
                secp256k1::PublicKey::from_secret_key(&Secp256k1::new(), &secret).to_string();
            if !self.ssp_identity_pubkey.is_empty() && pubkey != self.ssp_identity_pubkey {
                return Err(format!(
                    "SSP_SECRET_KEY_HEX does not match SSP_IDENTITY_PUBKEY (derived {pubkey})"
                ));
            }
            return Ok((self.ssp_secret_key_hex.clone(), pubkey));
        }
        let key_path = std::path::Path::new(&self.data_dir).join("ssp.key");
        if key_path.exists() {
            let secret_hex = std::fs::read_to_string(&key_path).map_err(|e| e.to_string())?;
            let secret =
                SecretKey::from_slice(&hex::decode(secret_hex.trim()).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            let pubkey =
                secp256k1::PublicKey::from_secret_key(&Secp256k1::new(), &secret).to_string();
            return Ok((secret_hex.trim().to_string(), pubkey));
        }
        // First boot: generate. Caller must publish the pubkey as identityPublicKey.
        let secret_bytes: [u8; 32] = rand::random();
        let secret = SecretKey::from_slice(&secret_bytes).map_err(|e| e.to_string())?;
        let secret_hex = hex::encode(secret_bytes);
        let pubkey = secp256k1::PublicKey::from_secret_key(&Secp256k1::new(), &secret).to_string();
        std::fs::create_dir_all(&self.data_dir).map_err(|e| e.to_string())?;
        std::fs::write(&key_path, &secret_hex).map_err(|e| e.to_string())?;
        Ok((secret_hex, pubkey))
    }
}
