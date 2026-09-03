//! Feldman VSS share splitting for SSP-held preimages.
//!
//! Mirrors `signer/spark-frost/src/vss.rs` exactly (same crates, same layout)
//! so SO verifiers accept our shares: Shamir split via vsss-rs Feldman,
//! proofs = compressed SEC1 coefficient commitments (no generator prefix),
//! share = 32-byte big-endian scalar, 1-based index.
//!
//! Wire: each share is proto-encoded as `SecretShare{secretShare, proofs}`
//! then ECIES-encrypted to the owning operator's identity key, and stored
//! via `store_preimage_share_v2` with owner = SSP identity (attestor).

use k256::{elliptic_curve::PrimeField, ProjectivePoint, Scalar};
use rand::rngs::OsRng;
use vsss_rs::{feldman, FeldmanVerifierSet, IdentifierPrimeField, Share, ValueGroup};

/// A share with Feldman proofs, byte-oriented like the SDK's
/// `WasmVerifiableSecretShare` ({threshold, index, share, proofs}).
#[derive(Clone, Debug)]
pub struct VerifiableSecretShare {
    #[allow(dead_code)]
    pub threshold: usize,
    pub index: u32,
    pub share: Vec<u8>,
    /// Compressed SEC1 pubkeys (33 bytes each), one per coefficient.
    pub proofs: Vec<Vec<u8>>,
}

type VsssShare = (IdentifierPrimeField<Scalar>, IdentifierPrimeField<Scalar>);
type VsssVerifier = ValueGroup<ProjectivePoint>;

fn scalar_from_bytes(bytes: &[u8]) -> Result<Scalar, String> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("scalar must be 32 bytes, got {}", bytes.len()))?;
    Option::from(Scalar::from_repr(k256::FieldBytes::from(arr)))
        .ok_or_else(|| "invalid scalar encoding".to_string())
}

fn scalar_to_bytes(s: &Scalar) -> Vec<u8> {
    s.to_bytes().to_vec()
}

fn point_to_compressed(p: &ProjectivePoint) -> Vec<u8> {
    use k256::elliptic_curve::sec1::ToEncodedPoint;
    p.to_affine().to_encoded_point(true).as_bytes().to_vec()
}

fn scalar_to_index(s: &Scalar) -> u32 {
    let bytes = s.to_bytes();
    u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]])
}

/// vsss-rs Vec layout: [generator, v0, v1, ..., v_{t-1}]; proofs drop the generator.
fn verifier_set_to_proofs(verifier_set: &Vec<VsssVerifier>) -> Vec<Vec<u8>> {
    <Vec<VsssVerifier> as FeldmanVerifierSet<VsssShare, VsssVerifier>>::verifiers(verifier_set)
        .iter()
        .map(|v| point_to_compressed(&v.0))
        .collect()
}

/// Split `secret` into `num_shares` verifiable shares (threshold >= 2).
pub fn split_secret_with_proofs(
    secret: &[u8],
    threshold: usize,
    num_shares: usize,
) -> Result<Vec<VerifiableSecretShare>, String> {
    if threshold < 2 {
        return Err(format!("threshold must be >= 2, got {threshold}"));
    }
    if num_shares < threshold {
        return Err(format!(
            "num_shares must be >= threshold, got num_shares={num_shares}, threshold={threshold}"
        ));
    }
    let secret_scalar = scalar_from_bytes(secret)?;
    let (shares, verifier_set): (Vec<VsssShare>, Vec<VsssVerifier>) = feldman::split_secret(
        threshold,
        num_shares,
        &IdentifierPrimeField(secret_scalar),
        None,
        OsRng,
    )
    .map_err(|e| format!("vsss split_secret failed: {e:?}"))?;
    let proofs = verifier_set_to_proofs(&verifier_set);
    Ok(shares
        .iter()
        .map(|s| VerifiableSecretShare {
            threshold,
            index: scalar_to_index(&s.identifier().0),
            share: scalar_to_bytes(&s.value().0),
            proofs: proofs.clone(),
        })
        .collect())
}

/// Verify one share against Feldman commitments:
/// share*G == sum(proofs[i] * index^i).
pub fn validate_share(share: &[u8], index: u32, proofs: &[Vec<u8>]) -> Result<(), String> {
    use k256::elliptic_curve::sec1::FromEncodedPoint;
    let share_scalar = scalar_from_bytes(share)?;
    let mut expected = ProjectivePoint::IDENTITY;
    let mut power = Scalar::ONE;
    let idx = Scalar::from(index as u64);
    for proof_bytes in proofs {
        if proof_bytes.len() != 33 {
            return Err("malformed proof length".to_string());
        }
        let commitment = Option::<k256::AffinePoint>::from(k256::AffinePoint::from_encoded_point(
            &k256::EncodedPoint::from_bytes(proof_bytes.as_slice())
                .map_err(|_| "malformed proof encoding".to_string())?,
        ))
        .map(ProjectivePoint::from)
        .ok_or_else(|| "malformed proof encoding".to_string())?;
        expected += commitment * power;
        power *= idx;
    }
    let actual = ProjectivePoint::GENERATOR * share_scalar;
    if expected == actual {
        Ok(())
    } else {
        Err("share fails Feldman verification".to_string())
    }
}

/// Proto-encode `SecretShare{bytes secretShare = 1, repeated bytes proofs = 2}`
/// (matches the SDK's SecretShareProto: field 1 length-delimited, field 2 repeated).
pub fn encode_secret_share_proto(share: &[u8], proofs: &[Vec<u8>]) -> Vec<u8> {
    fn varint(mut v: usize, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }
    fn field(out: &mut Vec<u8>, num: u8, data: &[u8]) {
        out.push((num << 3) | 2);
        varint(data.len(), out);
        out.extend_from_slice(data);
    }
    let mut out = Vec::new();
    field(&mut out, 1, share);
    for p in proofs {
        field(&mut out, 2, p);
    }
    out
}

/// ECIES-encrypt share bytes to an operator identity pubkey
/// (compressed secp256k1, same `ecies` crate as the signer).
pub fn encrypt_share_to_operator(
    share_proto_bytes: &[u8],
    operator_pubkey_hex: &str,
) -> Result<Vec<u8>, String> {
    let pk = hex::decode(operator_pubkey_hex.trim()).map_err(|e| e.to_string())?;
    ecies::encrypt(&pk, share_proto_bytes).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_recover_roundtrip() {
        let secret = Sha256Preimage::fake();
        for threshold in [2usize, 3] {
            let shares = split_secret_with_proofs(&secret, threshold, 5).unwrap();
            assert_eq!(shares.len(), 5);
            assert_eq!(shares[0].proofs.len(), threshold);
            // Every share validates against the commitments.
            for s in &shares {
                validate_share(&s.share, s.index, &s.proofs).unwrap();
            }
            // Any `threshold` shares recover the secret (Lagrange over indices).
            let recovered = recover_subset(&shares[..threshold], threshold).unwrap();
            assert_eq!(recovered, secret);
            // Tampered share fails validation.
            let mut bad = shares[0].share.clone();
            bad[0] ^= 1;
            assert!(validate_share(&bad, shares[0].index, &shares[0].proofs).is_err());
        }
    }

    #[test]
    fn proto_encoding_shape() {
        let enc = encode_secret_share_proto(&[1u8; 32], &[vec![2u8; 33], vec![3u8; 33]]);
        // field 1 (0x0a), len 32; then two field-2 (0x12) len-33 entries.
        assert_eq!(enc[0], 0x0a);
        assert_eq!(enc[1], 32);
        assert_eq!(enc[34], 0x12);
        assert_eq!(enc[35], 33);
        assert_eq!(enc.len(), 2 + 32 + 2 * (2 + 33));
    }

    struct Sha256Preimage;
    impl Sha256Preimage {
        fn fake() -> Vec<u8> {
            use sha2::{Digest, Sha256};
            Sha256::digest(b"ssp-frost-test").to_vec()
        }
    }

    fn recover_subset(
        shares: &[VerifiableSecretShare],
        threshold: usize,
    ) -> Result<Vec<u8>, String> {
        let vsss: Vec<VsssShare> = shares
            .iter()
            .map(|s| {
                let value = scalar_from_bytes(&s.share)?;
                let id = IdentifierPrimeField(Scalar::from(s.index as u64));
                Ok((id, IdentifierPrimeField(value)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if vsss.len() < threshold {
            return Err("not enough shares".to_string());
        }
        let recovered: IdentifierPrimeField<Scalar> =
            vsss_rs::ReadableShareSet::combine(&vsss).map_err(|e| format!("{e:?}"))?;
        Ok(scalar_to_bytes(&recovered.0))
    }
}
