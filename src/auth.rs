use axum::http::HeaderMap;
use base64::{
    engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD},
    Engine,
};
use secp256k1::{ecdsa::Signature, Message, Secp256k1};
use sha2::{Digest, Sha256};

use crate::AppState;

/// Create a challenge for a wallet identity pubkey.
/// Mirrors `mutation GetChallenge(public_key)`. Single-use, 5-minute expiry.
pub async fn get_challenge(state: &AppState, identity_pubkey: &str) -> Result<String, String> {
    let public_key =
        hex::decode(identity_pubkey).map_err(|_| "malformed public_key".to_string())?;
    secp256k1::PublicKey::from_slice(&public_key)
        .map_err(|_| "malformed public_key".to_string())?;
    let raw = format!(
        "spark-ssp-challenge:{}:{}",
        identity_pubkey,
        uuid::Uuid::new_v4()
    );
    let protected = URL_SAFE_NO_PAD.encode(raw.as_bytes());
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
    let sig = decode_signature(signature_hex)?;
    // SDK signs sha256 of the DECODED challenge bytes (client.ts authenticate()).
    let challenge_bytes = URL_SAFE_NO_PAD
        .decode(protected_challenge.trim())
        .or_else(|_| B64.decode(protected_challenge.trim()))
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

fn decode_signature(encoded: &str) -> Result<Signature, String> {
    let encoded = encoded.trim();
    let parse = |bytes: Vec<u8>| {
        Signature::from_der(&bytes)
            .or_else(|_| Signature::from_compact(&bytes))
            .ok()
    };

    URL_SAFE_NO_PAD
        .decode(encoded)
        .ok()
        .and_then(&parse)
        .or_else(|| B64.decode(encoded).ok().and_then(&parse))
        .or_else(|| hex::decode(encoded).ok().and_then(parse))
        .ok_or_else(|| "malformed signature".to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_signature() -> Signature {
        let secp = Secp256k1::new();
        let secret_key = secp256k1::SecretKey::from_slice(&[1; 32]).unwrap();
        let message = Message::from_digest([2; 32]);
        secp.sign_ecdsa(&message, &secret_key)
    }

    #[test]
    fn decodes_sdk_url_safe_signature() {
        let signature = test_signature();
        let encoded = URL_SAFE_NO_PAD.encode(signature.serialize_der());

        assert_eq!(decode_signature(&encoded).unwrap(), signature);
    }

    #[test]
    fn keeps_standard_base64_and_hex_compatibility() {
        let signature = test_signature();
        let der = signature.serialize_der();

        assert_eq!(decode_signature(&B64.encode(der)).unwrap(), signature);
        assert_eq!(decode_signature(&hex::encode(der)).unwrap(), signature);
    }
}
