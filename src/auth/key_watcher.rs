//! NATS KV-based public key loading and hot-reload watcher.

use std::sync::Arc;

use futures::StreamExt;

use crate::auth::jwt::JwtVerifier;
use crate::errors::GatewayError;

/// Connects to NATS KV, loads the public key from `TC_PUBKEYS.rails_public_key`,
/// creates a [`JwtVerifier`], and spawns a background watcher for hot-reload.
pub async fn load_and_watch_from_nats(nats_url: &str) -> Result<Arc<JwtVerifier>, GatewayError> {
    let client = async_nats::connect(nats_url)
        .await
        .map_err(|e| GatewayError::Auth(format!("NATS connect for key watcher failed: {e}")))?;

    let jetstream = async_nats::jetstream::new(client);

    let kv = jetstream
        .get_key_value("TC_PUBKEYS")
        .await
        .map_err(|e| GatewayError::Auth(format!("failed to open TC_PUBKEYS bucket: {e}")))?;

    let pem = match kv.get("rails_public_key").await {
        Ok(Some(entry)) => entry,
        Ok(None) => {
            return Err(GatewayError::Auth(
                "TC_PUBKEYS.rails_public_key not found in NATS KV".into(),
            ));
        }
        Err(e) => {
            return Err(GatewayError::Auth(format!(
                "failed to fetch rails_public_key from NATS KV: {e}"
            )));
        }
    };

    let verifier = Arc::new(JwtVerifier::from_rsa_pem(&pem)?);
    tracing::info!("loaded JWT public key from NATS KV (TC_PUBKEYS.rails_public_key)");

    let watcher = kv
        .watch("rails_public_key")
        .await
        .map_err(|e| GatewayError::Auth(format!("failed to watch TC_PUBKEYS: {e}")))?;

    tokio::spawn(watch_loop(watcher, verifier.clone()));
    tracing::info!("NATS KV key watcher started for TC_PUBKEYS.rails_public_key");

    Ok(verifier)
}

/// Starts watching a NATS KV key for updates and hot-reloads the verifier key.
/// Used when the initial key is loaded from a file but NATS KV is also available.
pub async fn start_nats_key_watcher(
    nats_url: &str,
    verifier: Arc<JwtVerifier>,
) -> Result<(), GatewayError> {
    let client = async_nats::connect(nats_url)
        .await
        .map_err(|e| GatewayError::Auth(format!("NATS connect for key watcher failed: {e}")))?;

    let jetstream = async_nats::jetstream::new(client);

    let kv = jetstream
        .get_key_value("TC_PUBKEYS")
        .await
        .map_err(|e| GatewayError::Auth(format!("failed to open TC_PUBKEYS bucket: {e}")))?;

    let watcher = kv
        .watch("rails_public_key")
        .await
        .map_err(|e| GatewayError::Auth(format!("failed to watch TC_PUBKEYS: {e}")))?;

    tokio::spawn(watch_loop(watcher, verifier));
    tracing::info!("NATS KV key watcher started for TC_PUBKEYS.rails_public_key");

    Ok(())
}

async fn watch_loop(mut watcher: async_nats::jetstream::kv::Watch, verifier: Arc<JwtVerifier>) {
    while let Some(result) = watcher.next().await {
        match result {
            Ok(entry) => {
                if let Err(e) = verifier.update_key(&entry.value) {
                    tracing::error!(error = %e, "failed to hot-reload JWT key from NATS KV");
                } else {
                    tracing::info!("JWT public key hot-reloaded from NATS KV");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "NATS KV watch error");
            }
        }
    }
    tracing::warn!("NATS KV key watcher stream ended");
}
