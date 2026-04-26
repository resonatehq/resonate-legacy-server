//! HTTP transport — send messages via HTTP POST to webhook URLs.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::Client;
use tokio::sync::Mutex;

use super::HttpAddress;
use crate::config::{HttpPushAuthConfig, HttpPushAuthMode};
use crate::metrics;

// ---------------------------------------------------------------------------
// Outbound auth
// ---------------------------------------------------------------------------

pub enum Auth {
    None,
    StaticBearer {
        header: String,
        value: String,
    },
    OidcIdToken {
        header: String,
        provider: OidcIdTokenProvider,
    },
}

impl Auth {
    pub fn from_config(config: &HttpPushAuthConfig, client: Client) -> Self {
        match config.mode {
            HttpPushAuthMode::None => Auth::None,
            HttpPushAuthMode::Bearer => {
                let token = config.token.clone().unwrap_or_default();
                Auth::StaticBearer {
                    header: config.header.clone(),
                    value: format!("Bearer {token}"),
                }
            }
            HttpPushAuthMode::OidcIdToken => Auth::OidcIdToken {
                header: config.header.clone(),
                provider: OidcIdTokenProvider::new(client, config.audience.clone()),
            },
        }
    }

    async fn resolve(&self, target_url: &str) -> Option<(String, String)> {
        match self {
            Auth::None => None,
            Auth::StaticBearer { header, value } => Some((header.clone(), value.clone())),
            Auth::OidcIdToken { header, provider } => {
                match provider.get_token(target_url).await {
                    Ok(token) => Some((header.clone(), format!("Bearer {token}"))),
                    Err(err) => {
                        tracing::warn!(
                            target_url = %target_url,
                            error = %err,
                            "OIDC ID token mint failed; sending request unauthenticated"
                        );
                        None
                    }
                }
            }
        }
    }
}

const REFRESH_BUFFER: Duration = Duration::from_secs(60);

struct CachedToken {
    value: String,
    expires_at: Instant,
}

pub struct OidcIdTokenProvider {
    client: Client,
    fixed_audience: Option<String>,
    cache: Mutex<HashMap<String, CachedToken>>,
}

impl OidcIdTokenProvider {
    fn new(client: Client, fixed_audience: Option<String>) -> Self {
        Self { client, fixed_audience, cache: Mutex::new(HashMap::new()) }
    }

    async fn get_token(&self, target_url: &str) -> Result<String, String> {
        let audience = self.fixed_audience.as_deref().unwrap_or(target_url);

        let mut cache = self.cache.lock().await;
        if let Some(entry) = cache.get(audience) {
            if entry.expires_at > Instant::now() + REFRESH_BUFFER {
                return Ok(entry.value.clone());
            }
        }

        let token = self
            .client
            .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity")
            .query(&[("audience", audience), ("format", "full")])
            .header("Metadata-Flavor", "Google")
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| format!("metadata server request: {e}"))?
            .text()
            .await
            .map_err(|e| format!("metadata server body: {e}"))?;

        let expires_at = jwt_exp_instant(&token);
        cache.insert(audience.to_string(), CachedToken { value: token.clone(), expires_at });

        Ok(token)
    }
}

fn jwt_exp_instant(token: &str) -> Instant {
    let payload = token.split('.').nth(1);
    let exp = payload.and_then(|p| {
        let decoded = URL_SAFE_NO_PAD.decode(p).ok()?;
        let v: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
        v["exp"].as_u64()
    });
    match exp {
        Some(exp) => {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Instant::now() + Duration::from_secs(exp.saturating_sub(now_unix))
        }
        None => Instant::now() + Duration::from_secs(3600),
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

pub struct HttpPushTransport {
    client: Client,
    auth: Auth,
}

impl HttpPushTransport {
    pub fn new(connect_timeout: Duration, request_timeout: Duration, auth: Auth) -> Self {
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .expect("failed to build HTTP client");
        Self { client, auth }
    }

    pub async fn send(&self, address: &HttpAddress, payload: &serde_json::Value) {
        let auth_header = self.auth.resolve(&address.url).await;

        let mut request = self
            .client
            .post(&address.url)
            .header("Content-Type", "application/json")
            .json(payload);

        if let Some((name, value)) = auth_header {
            request = request.header(name, value);
        }

        match request.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if resp.status().is_success() {
                    tracing::debug!(address = %address.url, status, "HTTP push delivery succeeded");
                    metrics::DELIVERIES_TOTAL.with_label_values(&["success"]).inc();
                } else {
                    tracing::warn!(address = %address.url, status, "HTTP push delivery rejected by target");
                    metrics::DELIVERIES_TOTAL.with_label_values(&["error"]).inc();
                }
            }
            Err(e) => {
                tracing::warn!(
                    address = %address.url,
                    error = %e,
                    error_kind = if e.is_connect() { "connect" } else if e.is_timeout() { "timeout" } else { "other" },
                    "HTTP push delivery failed"
                );
                metrics::DELIVERIES_TOTAL.with_label_values(&["error"]).inc();
            }
        }
    }
}
