use anyhow::{Context, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use std::collections::HashMap;

/// Decodes a base64 string trying the common URL-safe and standard alphabets,
/// both with and without padding.
///
/// This makes signature/key exchange tolerant of client-side base64 flavor
/// choices while staying strict about binary size.
fn decode_base64_flexible(value: &str) -> anyhow::Result<Vec<u8>> {
    let trimmed = value.trim();
    for engine in [&URL_SAFE_NO_PAD, &URL_SAFE, &STANDARD_NO_PAD, &STANDARD] {
        if let Ok(bytes) = engine.decode(trimmed) {
            return Ok(bytes);
        }
    }
    Err(anyhow!("value is not valid base64"))
}

/// Verifies an Ed25519 signature over the service id (the registration
/// challenge) using the public key provisioned for that service.
///
/// Returns the verification error message on failure so HTTP handlers can
/// surface precise client-facing errors.
pub fn verify_service_key_pair(
    service_id: &str,
    signature_b64: &str,
    public_keys: &HashMap<String, String>,
) -> anyhow::Result<()> {
    let public_key_b64 = public_keys
        .get(service_id)
        .ok_or_else(|| anyhow!("no public key provisioned for service '{service_id}'"))?;

    let public_key_bytes = decode_base64_flexible(public_key_b64)
        .with_context(|| format!("provisioned public key for service '{service_id}' is invalid"))?;
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes.try_into().map_err(|_| {
        anyhow!(
            "provisioned public key for service '{service_id}' has wrong length; expected 32 bytes"
        )
    })?)
    .with_context(|| format!("provisioned public key for service '{service_id}' is invalid"))?;

    let signature_bytes = decode_base64_flexible(signature_b64)
        .context("registration signature is not valid base64")?;
    let Ok(signature_bytes): Result<[u8; 64], _> = signature_bytes.try_into() else {
        bail!("registration signature must be 64 bytes");
    };

    verifying_key
        .verify(
            service_id.as_bytes(),
            &Signature::from_bytes(&signature_bytes),
        )
        .map_err(|err| anyhow!("signature verification failed for service '{service_id}': {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as TEST_ENGINE;
    use ed25519_dalek::{Signer, SigningKey};

    fn provisioned_key(signing_key: &SigningKey) -> String {
        TEST_ENGINE.encode(signing_key.verifying_key().to_bytes())
    }

    #[test]
    fn verifies_signature_over_service_id() {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let service_id = "payments";
        let signature = signing_key.sign(service_id.as_bytes());

        let mut keys = HashMap::new();
        keys.insert(service_id.to_string(), provisioned_key(&signing_key));

        verify_service_key_pair(service_id, &TEST_ENGINE.encode(signature.to_bytes()), &keys)
            .expect("valid signature must verify");
    }

    #[test]
    fn rejects_signature_from_wrong_key() {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let other_signing_key = SigningKey::from_bytes(&[9u8; 32]);
        let service_id = "payments";
        let signature = other_signing_key.sign(service_id.as_bytes());

        let mut keys = HashMap::new();
        keys.insert(service_id.to_string(), provisioned_key(&signing_key));

        let err =
            verify_service_key_pair(service_id, &TEST_ENGINE.encode(signature.to_bytes()), &keys)
                .expect_err("mismatched key must fail");
        assert!(err.to_string().contains("signature verification failed"));
    }

    #[test]
    fn rejects_missing_public_key() {
        let err = verify_service_key_pair("ghost", "", &HashMap::new())
            .expect_err("unknown service must fail");
        assert!(err.to_string().contains("no public key provisioned"));
    }

    #[test]
    fn rejects_malformed_signature() {
        let mut keys = HashMap::new();
        keys.insert(
            "payments".to_string(),
            provisioned_key(&SigningKey::from_bytes(&[1u8; 32])),
        );
        let err = verify_service_key_pair("payments", "not-base64!@#", &keys)
            .expect_err("malformed signature must fail");
        assert!(err.to_string().contains("not valid base64"));
    }
}
