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
    GcpIdToken {
        header: String,
        provider: GcpIdTokenProvider,
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
            HttpPushAuthMode::Gcp => Auth::GcpIdToken {
                header: config.header.clone(),
                provider: GcpIdTokenProvider::new(client, config.audience.clone(), GCP_METADATA_URL.to_string()),
            },
        }
    }

    async fn resolve(&self, target_url: &str) -> Option<(String, String)> {
        match self {
            Auth::None => None,
            Auth::StaticBearer { header, value } => Some((header.clone(), value.clone())),
            Auth::GcpIdToken { header, provider } => match provider.get_token(target_url).await {
                Ok(token) => Some((header.clone(), format!("Bearer {token}"))),
                Err(err) => {
                    tracing::warn!(
                        target_url = %target_url,
                        error = %err,
                        "OIDC ID token mint failed; sending request unauthenticated"
                    );
                    None
                }
            },
        }
    }
}

const REFRESH_BUFFER: Duration = Duration::from_secs(60);
const GCP_METADATA_URL: &str = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity";

struct CachedToken {
    value: String,
    expires_at: Instant,
}

pub struct GcpIdTokenProvider {
    client: Client,
    fixed_audience: Option<String>,
    metadata_url: String,
    cache: Mutex<HashMap<String, CachedToken>>,
}

impl GcpIdTokenProvider {
    fn new(client: Client, fixed_audience: Option<String>, metadata_url: String) -> Self {
        Self {
            client,
            fixed_audience,
            metadata_url,
            cache: Mutex::new(HashMap::new()),
        }
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
            .get(&self.metadata_url)
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
        cache.insert(
            audience.to_string(),
            CachedToken {
                value: token.clone(),
                expires_at,
            },
        );

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
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["success"])
                        .inc();
                } else {
                    tracing::warn!(address = %address.url, status, "HTTP push delivery rejected by target");
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                }
            }
            Err(e) => {
                tracing::warn!(
                    address = %address.url,
                    error = %e,
                    error_kind = if e.is_connect() { "connect" } else if e.is_timeout() { "timeout" } else { "other" },
                    "HTTP push delivery failed"
                );
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["error"])
                    .inc();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Router};
    use crate::transport::HttpAddress;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    /// Spawns a minimal Axum server on a random port that captures each
    /// request's headers and forwards them over the returned channel.
    async fn spawn_capture_server() -> (String, mpsc::Receiver<axum::http::HeaderMap>) {
        let (tx, rx) = mpsc::channel::<axum::http::HeaderMap>(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tx = Arc::new(tx);

        let app = Router::new()
            .route("/", post(capture_handler))
            .with_state(tx);

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://127.0.0.1:{}", addr.port()), rx)
    }

    async fn capture_handler(
        State(tx): State<Arc<mpsc::Sender<axum::http::HeaderMap>>>,
        req: axum::extract::Request,
    ) -> axum::http::StatusCode {
        let _ = tx.send(req.headers().clone()).await;
        axum::http::StatusCode::OK
    }

    /// Spawns a fake GCP metadata server that returns `fake_token` for every
    /// request and forwards the query parameters to the returned channel.
    async fn spawn_meta_server(fake_token: String) -> (String, mpsc::Receiver<HashMap<String, String>>) {
        let (tx, rx) = mpsc::channel::<HashMap<String, String>>(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tx = Arc::new(tx);

        tokio::spawn(async move {
            let app = Router::new().route(
                "/",
                axum::routing::get(
                    move |axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>| {
                        let token = fake_token.clone();
                        let tx = tx.clone();
                        async move {
                            let _ = tx.send(params).await;
                            token
                        }
                    },
                ),
            );
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://127.0.0.1:{}/", addr.port()), rx)
    }

    fn fake_jwt() -> String {
        let payload = URL_SAFE_NO_PAD.encode(r#"{"exp":9999999999}"#);
        format!("header.{payload}.sig")
    }

    fn make_transport(auth: Auth) -> HttpPushTransport {
        HttpPushTransport::new(Duration::from_secs(5), Duration::from_secs(5), auth)
    }

    #[tokio::test]
    async fn no_auth_omits_authorization_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::None)
            .send(&HttpAddress { url }, &serde_json::json!({}))
            .await;
        let headers = rx.recv().await.expect("server received no request");
        assert!(
            !headers.contains_key("authorization"),
            "expected no Authorization header but found one"
        );
    }

    #[tokio::test]
    async fn bearer_auth_sends_token_in_authorization_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::StaticBearer {
            header: "Authorization".to_string(),
            value: "Bearer secret-token".to_string(),
        })
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
        let headers = rx.recv().await.expect("server received no request");
        assert_eq!(
            headers
                .get("authorization")
                .expect("expected Authorization header")
                .to_str()
                .unwrap(),
            "Bearer secret-token"
        );
    }

    #[tokio::test]
    async fn bearer_auth_with_custom_header_uses_that_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::StaticBearer {
            header: "X-Custom-Auth".to_string(),
            value: "Bearer custom-token".to_string(),
        })
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
        let headers = rx.recv().await.expect("server received no request");
        assert_eq!(
            headers
                .get("x-custom-auth")
                .expect("expected X-Custom-Auth header")
                .to_str()
                .unwrap(),
            "Bearer custom-token",
        );
        assert!(
            !headers.contains_key("authorization"),
            "expected no standard Authorization header"
        );
    }

    #[tokio::test]
    async fn gcp_auth_fetches_token_from_metadata_server_and_sends_it() {
        let fake_token = fake_jwt();
        let (meta_url, mut meta_rx) = spawn_meta_server(fake_token.clone()).await;
        let (url, mut rx) = spawn_capture_server().await;

        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            provider: GcpIdTokenProvider::new(reqwest::Client::new(), None, meta_url),
        })
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;

        meta_rx.recv().await.expect("metadata server was not called");

        assert_eq!(
            rx.recv().await.expect("delivery target received no request")
                .get("authorization")
                .expect("expected Authorization header")
                .to_str()
                .unwrap(),
            format!("Bearer {fake_token}"),
        );
    }

    #[tokio::test]
    async fn gcp_auth_fixed_audience_is_sent_to_metadata_server() {
        let (meta_url, mut meta_rx) = spawn_meta_server(fake_jwt()).await;
        let (url, _rx) = spawn_capture_server().await;

        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            provider: GcpIdTokenProvider::new(
                reqwest::Client::new(),
                Some("https://my-audience.example.com".to_string()),
                meta_url,
            ),
        })
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;

        let params = meta_rx.recv().await.expect("metadata server was not called");
        assert_eq!(
            params.get("audience").map(String::as_str),
            Some("https://my-audience.example.com"),
        );
    }

    #[tokio::test]
    async fn gcp_auth_with_custom_header_sends_token_in_that_header() {
        let fake_token = fake_jwt();
        let (meta_url, _meta_rx) = spawn_meta_server(fake_token.clone()).await;
        let (url, mut rx) = spawn_capture_server().await;

        make_transport(Auth::GcpIdToken {
            header: "X-Goog-Token".to_string(),
            provider: GcpIdTokenProvider::new(reqwest::Client::new(), None, meta_url),
        })
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;

        let headers = rx.recv().await.expect("delivery target received no request");
        assert_eq!(
            headers
                .get("x-goog-token")
                .expect("expected X-Goog-Token header")
                .to_str()
                .unwrap(),
            format!("Bearer {fake_token}"),
        );
        assert!(
            !headers.contains_key("authorization"),
            "expected no standard Authorization header"
        );
    }
}
