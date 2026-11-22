//! Typed client for the runtime's canonical `/api/v2` REST surface.
//!
//! Every method unwraps the v2 response envelope (`data`) and maps failures to
//! [`ClientError`]. [`ROUTES`] lists each path the client calls; `tests/contract.rs`
//! checks them against the checked-in `openapi.v2.json`.

use std::fmt;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tantra_runtime_api::strategy_loop::{
    ExpiryHedgeRequest, ExpiryHedgeResponse, StrategyLoopRequest, StrategyLoopResponse,
    StrategySweepRequest, StrategySweepResponse,
};
use tantra_runtime_api::{ErrorCode, ScreenerQuery};

/// Version prefix of every path the client calls.
pub const API_PREFIX: &str = "/api/v2";

/// Default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Every `(METHOD, openapi path template)` the client calls, relative to `/api/v2`.
pub const ROUTES: &[(&str, &str)] = &[
    ("GET", "/bars/{id}"),
    ("GET", "/screener/scans"),
    ("POST", "/studies/strategy-loop/evaluate"),
    ("POST", "/studies/strategy-loop/sweep"),
    ("POST", "/studies/strategy-loop/recommend-hedge"),
    ("GET", "/health"),
    ("GET", "/acp"),
    ("GET", "/stream"),
];

/// Failure of a runtime call.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ClientError {
    /// The server could not be reached (connect, timeout, TLS, ...).
    #[error("unreachable: {0}")]
    Unreachable(String),
    /// The server answered non-2xx; fields come from the v2 error envelope.
    #[error("HTTP {status}{}: {message}", fmt_code(.code))]
    Http {
        status: u16,
        code: Option<ErrorCode>,
        message: String,
        /// `error.context.reason`, when present.
        reason: Option<String>,
    },
    /// A 2xx body that is not a valid envelope / payload.
    #[error("bad body: {0}")]
    BadBody(String),
    /// The base URL is not an `http(s)://` URL.
    #[error("invalid base url: {0}")]
    InvalidBase(String),
}

fn fmt_code(code: &Option<ErrorCode>) -> String {
    match code {
        Some(c) => format!(" ({c:?})"),
        None => String::new(),
    }
}

fn normalize_base(base: &str) -> Result<String, ClientError> {
    let b = base.trim().trim_end_matches('/');
    if (b.starts_with("http://") || b.starts_with("https://"))
        && b.split_once("://").is_some_and(|(_, r)| !r.is_empty())
    {
        Ok(b.to_string())
    } else {
        Err(ClientError::InvalidBase(base.to_string()))
    }
}

/// Percent-encode one path segment (RFC 3986 unreserved set kept).
pub fn encode_path_segment(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn url_for(base: &str, path: &str) -> String {
    format!("{base}{API_PREFIX}{path}")
}

fn ws_url_for(base: &str, path: &str) -> String {
    let ws = if let Some(r) = base.strip_prefix("https://") {
        format!("wss://{r}")
    } else if let Some(r) = base.strip_prefix("http://") {
        format!("ws://{r}")
    } else {
        base.to_string()
    };
    url_for(&ws, path)
}

fn bars_query(tf: &str, from: Option<&str>, to: Option<&str>) -> Vec<(&'static str, String)> {
    let mut q = vec![("tf", tf.to_string())];
    if let Some(f) = from {
        q.push(("from", f.to_string()));
    }
    if let Some(t) = to {
        q.push(("to", t.to_string()));
    }
    q
}

fn transport(e: reqwest::Error) -> ClientError {
    let mut chain = String::new();
    let mut source = std::error::Error::source(&e);
    while let Some(s) = source {
        chain.push_str(&format!(": {s}"));
        source = s.source();
    }
    ClientError::Unreachable(format!("{}{chain}", e.without_url()))
}

/// Decode a response: non-2xx -> `Http` from the error envelope; 2xx -> `data`.
fn decode_value(status: u16, text: &str) -> Result<Value, ClientError> {
    let parsed: Result<Value, _> = serde_json::from_str(text);
    if !(200..300).contains(&status) {
        let err = parsed.as_ref().ok().and_then(|v| v.get("error"));
        let code = err
            .and_then(|e| e.get("code"))
            .and_then(|c| serde_json::from_value::<ErrorCode>(c.clone()).ok());
        let message = err
            .and_then(|e| e.get("message"))
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| text.chars().take(200).collect());
        let reason = err
            .and_then(|e| e.get("context"))
            .and_then(|c| c.get("reason"))
            .and_then(Value::as_str)
            .map(String::from);
        return Err(ClientError::Http {
            status,
            code,
            message,
            reason,
        });
    }
    let env = parsed.map_err(|e| ClientError::BadBody(e.to_string()))?;
    match env {
        Value::Object(mut m) => m
            .remove("data")
            .ok_or_else(|| ClientError::BadBody("envelope has no `data`".into())),
        _ => Err(ClientError::BadBody(
            "response is not an envelope object".into(),
        )),
    }
}

fn decode<T: DeserializeOwned>(status: u16, text: &str) -> Result<T, ClientError> {
    serde_json::from_value(decode_value(status, text)?)
        .map_err(|e| ClientError::BadBody(e.to_string()))
}

fn encode_body<B: Serialize>(b: &B) -> Result<Vec<u8>, ClientError> {
    serde_json::to_vec(b).map_err(|e| ClientError::BadBody(e.to_string()))
}

/// Async runtime client.
#[derive(Clone)]
pub struct RuntimeClient {
    base: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl fmt::Debug for RuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeClient")
            .field("base", &self.base)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl RuntimeClient {
    /// New client with the default 30 s timeout.
    pub fn new(base: &str, token: Option<String>) -> Result<Self, ClientError> {
        Self::with_timeout(base, token, DEFAULT_TIMEOUT)
    }

    /// New client with an explicit request timeout.
    pub fn with_timeout(
        base: &str,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        let base = normalize_base(base)?;
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(transport)?;
        Ok(Self { base, token, http })
    }

    /// `ws(s)://…/api/v2/acp`.
    pub fn acp_ws_url(&self) -> String {
        ws_url_for(&self.base, "/acp")
    }

    /// `ws(s)://…/api/v2/stream`.
    pub fn stream_ws_url(&self) -> String {
        ws_url_for(&self.base, "/stream")
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<(u16, String), ClientError> {
        let req = match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        };
        let resp = req.send().await.map_err(transport)?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(transport)?;
        Ok((status, text))
    }

    async fn get_value(&self, path: &str, query: &[(&str, String)]) -> Result<Value, ClientError> {
        let (status, text) = self
            .send(self.http.get(url_for(&self.base, path)).query(query))
            .await?;
        decode_value(status, &text)
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let req = self
            .http
            .post(url_for(&self.base, path))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encode_body(body)?);
        let (status, text) = self.send(req).await?;
        decode(status, &text)
    }

    /// `GET /api/v2/bars/{id}`.
    pub async fn bars(
        &self,
        id: &str,
        tf: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Value, ClientError> {
        let path = format!("/bars/{}", encode_path_segment(id));
        self.get_value(&path, &bars_query(tf, from, to)).await
    }

    /// `GET /api/v2/screener/scans`.
    pub async fn screener_scans(&self, q: &ScreenerQuery) -> Result<Value, ClientError> {
        let (status, text) = self
            .send(
                self.http
                    .get(url_for(&self.base, "/screener/scans"))
                    .query(q),
            )
            .await?;
        decode_value(status, &text)
    }

    /// `POST /api/v2/studies/strategy-loop/evaluate`.
    pub async fn strategy_loop_evaluate(
        &self,
        req: &StrategyLoopRequest,
    ) -> Result<StrategyLoopResponse, ClientError> {
        self.post("/studies/strategy-loop/evaluate", req).await
    }

    /// `POST /api/v2/studies/strategy-loop/sweep`.
    pub async fn strategy_loop_sweep(
        &self,
        req: &StrategySweepRequest,
    ) -> Result<StrategySweepResponse, ClientError> {
        self.post("/studies/strategy-loop/sweep", req).await
    }

    /// `POST /api/v2/studies/strategy-loop/recommend-hedge`.
    pub async fn recommend_hedge(
        &self,
        req: &ExpiryHedgeRequest,
    ) -> Result<ExpiryHedgeResponse, ClientError> {
        self.post("/studies/strategy-loop/recommend-hedge", req)
            .await
    }

    /// `GET /api/v2/health`.
    pub async fn health(&self) -> Result<Value, ClientError> {
        self.get_value("/health", &[]).await
    }
}

#[cfg(feature = "blocking")]
pub use blocking::BlockingRuntimeClient;

#[cfg(feature = "blocking")]
mod blocking {
    use super::*;

    /// Blocking twin of [`RuntimeClient`] for synchronous callers.
    #[derive(Clone)]
    pub struct BlockingRuntimeClient {
        base: String,
        token: Option<String>,
        http: reqwest::blocking::Client,
    }

    impl fmt::Debug for BlockingRuntimeClient {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("BlockingRuntimeClient")
                .field("base", &self.base)
                .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
                .finish()
        }
    }

    impl BlockingRuntimeClient {
        /// New client with the default 30 s timeout.
        pub fn new(base: &str, token: Option<String>) -> Result<Self, ClientError> {
            Self::with_timeout(base, token, DEFAULT_TIMEOUT)
        }

        /// New client with an explicit request timeout.
        pub fn with_timeout(
            base: &str,
            token: Option<String>,
            timeout: Duration,
        ) -> Result<Self, ClientError> {
            let base = normalize_base(base)?;
            let http = reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(transport)?;
            Ok(Self { base, token, http })
        }

        /// `ws(s)://…/api/v2/acp`.
        pub fn acp_ws_url(&self) -> String {
            ws_url_for(&self.base, "/acp")
        }

        /// `ws(s)://…/api/v2/stream`.
        pub fn stream_ws_url(&self) -> String {
            ws_url_for(&self.base, "/stream")
        }

        fn send(
            &self,
            req: reqwest::blocking::RequestBuilder,
        ) -> Result<(u16, String), ClientError> {
            let req = match &self.token {
                Some(t) => req.bearer_auth(t),
                None => req,
            };
            let resp = req.send().map_err(transport)?;
            let status = resp.status().as_u16();
            let text = resp.text().map_err(transport)?;
            Ok((status, text))
        }

        fn post<B: Serialize, T: DeserializeOwned>(
            &self,
            path: &str,
            body: &B,
        ) -> Result<T, ClientError> {
            let req = self
                .http
                .post(url_for(&self.base, path))
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(encode_body(body)?);
            let (status, text) = self.send(req)?;
            decode(status, &text)
        }

        /// `GET /api/v2/bars/{id}`.
        pub fn bars(
            &self,
            id: &str,
            tf: &str,
            from: Option<&str>,
            to: Option<&str>,
        ) -> Result<Value, ClientError> {
            let path = format!("/bars/{}", encode_path_segment(id));
            let (status, text) = self.send(
                self.http
                    .get(url_for(&self.base, &path))
                    .query(&bars_query(tf, from, to)),
            )?;
            decode_value(status, &text)
        }

        /// `GET /api/v2/screener/scans`.
        pub fn screener_scans(&self, q: &ScreenerQuery) -> Result<Value, ClientError> {
            let (status, text) = self.send(
                self.http
                    .get(url_for(&self.base, "/screener/scans"))
                    .query(q),
            )?;
            decode_value(status, &text)
        }

        /// `POST /api/v2/studies/strategy-loop/evaluate`.
        pub fn strategy_loop_evaluate(
            &self,
            req: &StrategyLoopRequest,
        ) -> Result<StrategyLoopResponse, ClientError> {
            self.post("/studies/strategy-loop/evaluate", req)
        }

        /// `POST /api/v2/studies/strategy-loop/sweep`.
        pub fn strategy_loop_sweep(
            &self,
            req: &StrategySweepRequest,
        ) -> Result<StrategySweepResponse, ClientError> {
            self.post("/studies/strategy-loop/sweep", req)
        }

        /// `POST /api/v2/studies/strategy-loop/recommend-hedge`.
        pub fn recommend_hedge(
            &self,
            req: &ExpiryHedgeRequest,
        ) -> Result<ExpiryHedgeResponse, ClientError> {
            self.post("/studies/strategy-loop/recommend-hedge", req)
        }

        /// `GET /api/v2/health`.
        pub fn health(&self) -> Result<Value, ClientError> {
            let (status, text) = self.send(self.http.get(url_for(&self.base, "/health")))?;
            decode_value(status, &text)
        }
    }
}
