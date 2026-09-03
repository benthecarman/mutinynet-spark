use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use secp256k1::{Message, Secp256k1};
use sha2::{Digest, Sha256};

use crate::AppState;

/// Create a challenge for a wallet identity pubkey.
/// Mirrors `mutation GetChallenge(public_key)`. Single-use, 5-minute expiry.
pub async fn get_challenge(state: &AppState, identity_pubkey: &str) -> Result<String, String> {
    if identity_pubkey.trim().is_empty() {
        return Err("public_key required".to_string());
    }
    let raw = format!(
        "spark-ssp-challenge:{}:{}",
        identity_pubkey,
        uuid::Uuid::new_v4()
    );
    let protected = B64.encode(raw.as_bytes());
    state
        .db
        .save_challenge(
            identity_pubkey,
            &protected,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await?;
    Ok(protected)
}

/// Verify `signature` over sha256(base64-decoded protected_challenge) with the
/// wallet identity key. Returns a session token on success.
/// Signature: base64 (SDK) or hex (curl), DER or compact.
pub async fn verify_challenge(
    state: &AppState,
    identity_pubkey: &str,
    protected_challenge: &str,
    signature_hex: &str,
) -> Result<(String, chrono::DateTime<chrono::Utc>), String> {
    let secp = Secp256k1::new();
    let pubkey_bytes = hex::decode(identity_pubkey).map_err(|e| e.to_string())?;
    let pubkey = secp256k1::PublicKey::from_slice(&pubkey_bytes).map_err(|e| e.to_string())?;
    // The challenge must be one we issued, unused, and fresh. Consumed
    // atomically: replays and foreign signatures fail closed here.
    let fresh = state
        .db
        .consume_challenge(
            identity_pubkey,
            protected_challenge,
            chrono::Utc::now().timestamp(),
            300,
        )
        .await?;
    if !fresh {
        return Err("unknown, reused, or expired challenge".to_string());
    }
    let sig_bytes = B64
        .decode(signature_hex.trim())
        .or_else(|_| hex::decode(signature_hex.trim()))
        .map_err(|_| "malformed signature".to_string())?;
    let sig = secp256k1::ecdsa::Signature::from_der(&sig_bytes)
        .or_else(|_| secp256k1::ecdsa::Signature::from_compact(&sig_bytes))
        .map_err(|_| "malformed signature".to_string())?;
    // SDK signs sha256 of the DECODED challenge bytes (client.ts authenticate()).
    let challenge_bytes = B64
        .decode(protected_challenge.trim())
        .map_err(|_| "malformed challenge".to_string())?;
    // Domain separation: challenges we issue start with this prefix.
    if !challenge_bytes.starts_with(b"spark-ssp-challenge:") {
        return Err("foreign challenge".to_string());
    }
    let digest = Sha256::digest(&challenge_bytes);
    let msg = Message::from_digest(*digest.as_ref());
    secp.verify_ecdsa(&msg, &sig, &pubkey)
        .map_err(|e| format!("bad challenge signature: {e}"))?;

    let token = uuid::Uuid::new_v4().to_string();
    let valid_until = chrono::Utc::now() + chrono::Duration::hours(24);
    state
        .db
        .save_session(&token, identity_pubkey, &valid_until.to_rfc3339())
        .await?;
    Ok((token, valid_until))
}

/// Extract bearer session. `get_challenge` allows unauthenticated access;
/// everything else requires it.
pub async fn require_session(state: &AppState, headers: &HeaderMap) -> Result<String, String> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth.strip_prefix("Bearer ").unwrap_or("").trim();
    if token.is_empty() {
        return Err("UNAUTHORIZED: missing bearer session token".to_string());
    }
    match state
        .db
        .session_owner(token, &chrono::Utc::now().to_rfc3339())
        .await?
    {
        Some(owner) => Ok(owner),
        None => Err("UNAUTHORIZED: bad or expired session".to_string()),
    }
}
