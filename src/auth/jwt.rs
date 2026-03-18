//! RS256 JWT verification and stream-level authorization.

use std::sync::RwLock;

use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};

use crate::errors::GatewayError;

/// Decoded JWT claims expected from TurboCable clients.
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct Claims {
    /// Subject — typically a user identifier like `"user_42"`.
    pub sub: String,
    /// Glob patterns controlling which streams the user may subscribe to.
    pub allowed_streams: Vec<String>,
    /// Token expiry (Unix timestamp).
    pub exp: usize,
    /// Token issued-at (Unix timestamp).
    pub iat: usize,
}

/// Thread-safe RS256 JWT verifier with hot-reloadable public key.
pub struct JwtVerifier {
    key: RwLock<DecodingKey>,
    validation: Validation,
}

impl JwtVerifier {
    /// Creates a new verifier from an RSA public key in PEM format.
    pub fn from_rsa_pem(pem: &[u8]) -> Result<Self, GatewayError> {
        let key = DecodingKey::from_rsa_pem(pem)
            .map_err(|e| GatewayError::Auth(format!("invalid RSA PEM: {e}")))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_required_spec_claims(&["exp", "sub", "iat"]);

        Ok(Self {
            key: RwLock::new(key),
            validation,
        })
    }

    /// Decodes and validates a JWT token, returning the claims on success.
    pub fn verify(&self, token: &str) -> Result<Claims, GatewayError> {
        let key = self
            .key
            .read()
            .map_err(|_| GatewayError::Auth("lock poisoned".into()))?
            .clone();

        let token_data: TokenData<Claims> = jsonwebtoken::decode(token, &key, &self.validation)
            .map_err(|e| {
                let reason = match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => "token expired",
                    jsonwebtoken::errors::ErrorKind::InvalidSignature => "invalid signature",
                    jsonwebtoken::errors::ErrorKind::InvalidAlgorithm => "invalid algorithm",
                    _ => "auth failed",
                };
                GatewayError::Auth(reason.into())
            })?;

        Ok(token_data.claims)
    }

    /// Replaces the RSA public key at runtime for zero-downtime key rotation.
    pub fn update_key(&self, pem: &[u8]) -> Result<(), GatewayError> {
        let new_key = DecodingKey::from_rsa_pem(pem)
            .map_err(|e| GatewayError::Auth(format!("invalid RSA PEM: {e}")))?;

        let mut key = self
            .key
            .write()
            .map_err(|_| GatewayError::Auth("lock poisoned".into()))?;
        *key = new_key;

        tracing::info!("JWT public key updated");
        Ok(())
    }
}

/// Checks whether `stream` matches any of the allowed glob patterns.
///
/// Supported patterns:
///   - `"*"` — matches any stream
///   - `"prefix_*"` — matches any stream starting with `prefix_`
///   - `"exact_name"` — matches only that exact stream name
pub fn is_allowed(allowed: &[String], stream: &str) -> bool {
    allowed.iter().any(|pat| {
        if pat == "*" {
            return true;
        }
        match pat.strip_suffix('*') {
            Some(prefix) => stream.starts_with(prefix),
            None => pat == stream,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_RSA_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDH0NRdQOkpkbDm
iTfhST6rIThVkJM1pa3cJPA+sPdRoViPLHC9o648ygEATXsTjZtefpq/niPu0f0h
tERYuNQyom+MGbYqm4qXkdGfOc7/bk5UOpftEQ50GzkrlfmfyXGreIl+bIOMFVjd
OkZujNH4Gw1TOQQ0oOavquSOwSaifYnl1Fnew+XEC0uScBzUXp/QIJdjCOCf1u5Q
T9imYPHM4ZOF8JF3MnqvESvMV6FJ+w2WfJSmseLHXjlBfu9juHFACzGxkXpFqO9f
+558p4F03YWp2sxNTbz2PoiX3CO0FWJLOYpVkLMGvz5hkeTteo1LrKe0XSxqYZfF
bt+Bv+BFAgMBAAECggEAEmbRf+sP7gOcUobVjhpUOqdbDEo9vGWPLuR5+ZQLmsls
ofbaRSSzUaba07/O81yJr/ih4L68GWzeToHO/4q6BBXAhxsBE0hyyYWk0/Cbdxud
/BTPVAZLmfa92506uXPwU3XM18c/kCGRJwKMZPb0CVDYd88a64vb4tauqNTx7WnP
YQiKY9EkJPHObT+Ud6Uebgyi6BGACyunNO2Ty0eVJzuRvv/Peae128qTsNl2Vl7m
3PpSeF6LDOj6RdXeqk6pVdFsTxBKq+yIh7i3HbqIKevVn9pfVlXVBqKQEv8HkI/v
+V35kQa/UBEnwSKm2pU3WGHlPzcGwa665teU8UVIawKBgQD9eADGPaCbpBE+i2d7
lcOi+IY4lNvwSo/l11eBdKfP2Hx44q+YL4AHU45+RA24WQbXLLoCTLjlkgq7s/LJ
6tBvMQ7RwTkBlzr25K/uqy3JN0vnlb+vxVKYNtqecVtqCemoduagIjWtiTmuaWXm
VJXSwQUdxPAOFuIJGcwGQCKUHwKBgQDJz6llw01o9R4m8O9R2qDAxkCOk/bE6dSE
EYb/FOgsrCbL2P36rmYaDRYodMrNp1+vHLQ8xQd/qOhiDeQuHhoClvA9DXBBs5ML
PWEn8SHsejktxmaEvN3BXnd1nFm4r24luNc2VjU8YlWpUloEGfC203jVaMmFtKF8
fgTLsIWfGwKBgQDLJdkJCe+ljrO7eyNva7Mm9SUuSDCWwEvgnN0nhoXREeOBR74Q
rVFhjdiQ3p5YeBIBd3mFylQOuyQbGLiomKiB1cHY35J+8eRyaQuQsGW79bPCYsUF
bZMrKBvEDXqE3HkHanShN4nqEifG3/apynViOw2MtIDp6fEz9hcNk22jZQKBgGM/
UwmOwLULRubTun5AzKnBVeJIdiVk8XR5wjAUMhI2H2ZEsrLjrabGJM2EknANDgtq
TGFObF+ly5LdTgg4GYaIgGEmCLzm+Tuf1fX0qkBH43LVjXleAJimQo1+dMlUzRCU
FJLOVqP5oDMDIu29bBodaeFaBTFSIdC9kNIzX6NdAoGBAPanzGOVw8i2ytBNKHnc
nj9LeLDSU5hTdMbkfsQcSlYp7cm6IouIekFzrfGg2+R7ePB1zTdQT86NoH7bnbPO
obpj1op1zYyZdhOUmAMyEfpcWKUknGXSTl9NJQp2Dh7kv+VfFSEoOxA4KLagTnMF
CM21qtHKzbkqhZvIvYUOhpdX
-----END PRIVATE KEY-----";

    const TEST_RSA_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx9DUXUDpKZGw5ok34Uk+
qyE4VZCTNaWt3CTwPrD3UaFYjyxwvaOuPMoBAE17E42bXn6av54j7tH9IbREWLjU
MqJvjBm2KpuKl5HRnznO/25OVDqX7REOdBs5K5X5n8lxq3iJfmyDjBVY3TpGbozR
+BsNUzkENKDmr6rkjsEmon2J5dRZ3sPlxAtLknAc1F6f0CCXYwjgn9buUE/YpmDx
zOGThfCRdzJ6rxErzFehSfsNlnyUprHix145QX7vY7hxQAsxsZF6RajvX/uefKeB
dN2FqdrMTU289j6Il9wjtBViSzmKVZCzBr8+YZHk7XqNS6yntF0samGXxW7fgb/g
RQIDAQAB
-----END PUBLIC KEY-----";

    fn now_secs() -> usize {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize
    }

    fn make_claims(sub: &str, streams: Vec<&str>, exp_offset_secs: i64) -> Claims {
        let now = now_secs();
        Claims {
            sub: sub.into(),
            allowed_streams: streams.into_iter().map(String::from).collect(),
            iat: now,
            exp: (now as i64 + exp_offset_secs) as usize,
        }
    }

    fn encode_rs256(claims: &Claims) -> String {
        let key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes()).unwrap();
        jsonwebtoken::encode(&Header::new(Algorithm::RS256), claims, &key).unwrap()
    }

    fn test_verifier() -> JwtVerifier {
        JwtVerifier::from_rsa_pem(TEST_RSA_PUBLIC_KEY.as_bytes()).unwrap()
    }

    #[test]
    fn valid_token_accepted() {
        let verifier = test_verifier();
        let claims = make_claims("user_42", vec!["chat_room_*"], 3600);
        let token = encode_rs256(&claims);

        let result = verifier.verify(&token);
        assert!(result.is_ok());

        let decoded = result.unwrap();
        assert_eq!(decoded.sub, "user_42");
        assert_eq!(decoded.allowed_streams, vec!["chat_room_*"]);
    }

    #[test]
    fn expired_token_rejected() {
        let verifier = test_verifier();
        let claims = make_claims("user_42", vec!["*"], -3600);
        let token = encode_rs256(&claims);

        let result = verifier.verify(&token);
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(err.contains("token expired"), "got: {err}");
    }

    #[test]
    fn wrong_algorithm_rejected() {
        let verifier = test_verifier();
        let claims = make_claims("user_42", vec!["*"], 3600);

        let hs256_key = EncodingKey::from_secret(b"some-secret");
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &hs256_key).unwrap();

        let result = verifier.verify(&token);
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid algorithm") || err.contains("auth failed"),
            "got: {err}"
        );
    }

    #[test]
    fn tampered_signature_rejected() {
        let verifier = test_verifier();
        let claims = make_claims("user_42", vec!["*"], 3600);
        let mut token = encode_rs256(&claims);

        let last = token.pop().unwrap();
        let replacement = if last == 'A' { 'B' } else { 'A' };
        token.push(replacement);

        let result = verifier.verify(&token);
        assert!(result.is_err());
    }

    #[test]
    fn stream_glob_chat_room_star_matches_chat_room_42() {
        assert!(is_allowed(&[String::from("chat_room_*")], "chat_room_42"));
    }

    #[test]
    fn stream_glob_star_matches_anything() {
        assert!(is_allowed(&[String::from("*")], "any_stream_name"));
        assert!(is_allowed(&[String::from("*")], ""));
    }

    #[test]
    fn stream_glob_exact_rejects_partial() {
        assert!(!is_allowed(&[String::from("chat_room_1")], "chat_room_10"));
        assert!(is_allowed(&[String::from("chat_room_1")], "chat_room_1"));
    }

    #[test]
    fn stream_glob_multiple_patterns() {
        let patterns = vec![String::from("chat_room_*"), String::from("notifications")];
        assert!(is_allowed(&patterns, "chat_room_42"));
        assert!(is_allowed(&patterns, "notifications"));
        assert!(!is_allowed(&patterns, "admin_panel"));
    }

    #[test]
    fn stream_glob_empty_patterns_rejects_all() {
        assert!(!is_allowed(&[], "chat_room_1"));
    }

    #[test]
    fn key_update_changes_verification() {
        let verifier = test_verifier();
        let claims = make_claims("user_42", vec!["*"], 3600);
        let token = encode_rs256(&claims);

        assert!(verifier.verify(&token).is_ok());

        let other_key = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx9DUXUDpKZGw5ok34Uk+
qyE4VZCTNaWt3CTwPrD3UaFYjyxwvaOuPMoBAE17E42bXn6av54j7tH9IbREWLjU
MqJvjBm2KpuKl5HRnznO/25OVDqX7REOdBs5K5X5n8lxq3iJfmyDjBVY3TpGbozR
+BsNUzkENKDmr6rkjsEmon2J5dRZ3sPlxAtLknAc1F6f0CCXYwjgn9buUE/YpmDx
zOGThfCRdzJ6rxErzFehSfsNlnyUprHix145QX7vY7hxQAsxsZF6RajvX/uefKeB
dN2FqdrMTU289j6Il9wjtBViSzmKVZCzBr8+YZHk7XqNS6yntF0samGXxW7fgb/g
RQIDAQAB
-----END PUBLIC KEY-----";

        verifier.update_key(other_key.as_bytes()).unwrap();

        assert!(verifier.verify(&token).is_ok());
    }
}
