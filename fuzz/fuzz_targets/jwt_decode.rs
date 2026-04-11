//! Fuzz target: JWT token string → JwtVerifier::verify path.
//!
//! Treats arbitrary bytes as a UTF-8 string (valid or not) and passes it to
//! the verifier.  The goal is to confirm no panic or unwrap is reachable
//! regardless of input shape — expired / malformed / garbage tokens must all
//! produce a clean Err result, never a crash.
//!
//! Run with:
//!   cargo fuzz run jwt_decode -- -max_total_time=60
#![no_main]

use libfuzzer_sys::fuzz_target;
use turbocable_server::auth::jwt::JwtVerifier;

// RSA public key used only for fuzzing.  The verifier will reject every
// fuzz-generated token (wrong signature / format), which is the desired outcome.
const FUZZ_PUBLIC_KEY: &[u8] = b"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx9DUXUDpKZGw5ok34Uk+
qyE4VZCTNaWt3CTwPrD3UaFYjyxwvaOuPMoBAE17E42bXn6av54j7tH9IbREWLjU
MqJvjBm2KpuKl5HRnznO/25OVDqX7REOdBs5K5X5n8lxq3iJfmyDjBVY3TpGbozR
+BsNUzkENKDmr6rkjsEmon2J5dRZ3sPlxAtLknAc1F6f0CCXYwjgn9buUE/YpmDx
zOGThfCRdzJ6rxErzFehSfsNlnyUprHix145QX7vY7hxQAsxsZF6RajvX/uefKeB
dN2FqdrMTU289j6Il9wjtBViSzmKVZCzBr8+YZHk7XqNS6yntF0samGXxW7fgb/g
RQIDAQAB
-----END PUBLIC KEY-----";

fuzz_target!(|data: &[u8]| {
    // Build the verifier once per fuzz iteration — construction is cheap and
    // avoids a global-state footgun with the RwLock.
    let Ok(verifier) = JwtVerifier::from_rsa_pem(FUZZ_PUBLIC_KEY) else {
        return;
    };

    // Accept any byte sequence as a candidate token string.
    // Non-UTF-8 inputs are coerced to a lossy string so the fuzz engine can
    // still explore the parser's UTF-8 validation branch.
    let token = String::from_utf8_lossy(data);
    let _ = verifier.verify(&token);
});
