//! Async supervised client for the Python stdio MCP server `tantra-library-mcp`.
//!
//! The server speaks newline-delimited JSON-RPC 2.0 over stdin/stdout. No Rust
//! crate imports Python: the process boundary is the contract.
//!
//! [`LibraryMcpClient::spawn`] starts the supervisor task eagerly (it must be
//! called inside a Tokio runtime). The supervisor owns the child process,
//! restarts it with exponential backoff plus jitter when it exits, and fails
//! in-flight requests with [`LibraryError::Unavailable`] while it is down.

use rand::Rng;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot};

/// Errors returned by [`LibraryMcpClient`].
#[derive(Debug, thiserror::Error)]
pub enum LibraryError {
    /// The child process is down, restarting, or failed.
    #[error("library server unavailable: {0}")]
    Unavailable(String),
    /// The request queue is full (callers map this to HTTP 429).
    #[error("library server busy")]
    Busy,
    /// No response arrived within the request timeout.
    #[error("library request timed out")]
    Timeout,
    /// The server violated the JSON-RPC / MCP protocol.
    #[error("library protocol error: {0}")]
    Protocol(String),
    /// The tool reported an error (`isError: true`).
    #[error("library tool error {code}: {message}")]
    Tool {
        /// Machine-readable error code from the tool.
        code: String,
        /// Human-readable message from the tool.
        message: String,
    },
    /// A response line exceeded the configured maximum size.
    #[error("library response exceeded maximum line size")]
    PayloadTooLarge,
}

/// Configuration for [`LibraryMcpClient`].
#[derive(Debug, Clone)]
pub struct LibraryClientConfig {
    /// Executable to launch.
    pub command: String,
    /// Arguments passed to the executable.
    pub args: Vec<String>,
    /// Extra environment variables for the child.
    pub env: Vec<(String, String)>,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Request queue capacity (also bounds in-flight requests).
    pub queue_capacity: usize,
    /// Maximum accepted response line length in bytes.
    pub max_line_bytes: usize,
    /// Initial restart backoff.
    pub restart_backoff_initial: Duration,
    /// Maximum restart backoff.
    pub restart_backoff_max: Duration,
    /// A child must stay up this long before the restart backoff resets.
    pub stable_after: Duration,
}

impl Default for LibraryClientConfig {
    fn default() -> Self {
        Self {
            command: "tantra-library-mcp".to_string(),
            args: Vec::new(),
            env: Vec::new(),
            request_timeout: Duration::from_secs(30),
            queue_capacity: 64,
            max_line_bytes: 8 * 1024 * 1024,
            restart_backoff_initial: Duration::from_millis(200),
            restart_backoff_max: Duration::from_secs(30),
            stable_after: Duration::from_secs(30),
        }
    }
}

impl LibraryClientConfig {
    /// Build a config from the process environment (composition boundary).
    ///
    /// Reads `TANTRA_LIBRARY_MCP_CMD`, `TANTRA_LIBRARY_MCP_ARGS`
    /// (whitespace-split) and passes `TANTRA_LIBRARY_LAKEHOUSE_ROOT`
    /// (from `lakehouse_root`) and `TANTRA_LIBRARY_DATA_ROOT` (if set) to the child.
    #[must_use]
    pub fn from_env(lakehouse_root: Option<&Path>) -> Self {
        let mut cfg = Self::default();
        if let Ok(cmd) = std::env::var("TANTRA_LIBRARY_MCP_CMD")
            && !cmd.trim().is_empty()
        {
            cfg.command = cmd;
        }
        if let Ok(args) = std::env::var("TANTRA_LIBRARY_MCP_ARGS") {
            cfg.args = args.split_whitespace().map(str::to_string).collect();
        }
        if let Some(root) = lakehouse_root {
            cfg.env.push((
                "TANTRA_LIBRARY_LAKEHOUSE_ROOT".to_string(),
                root.to_string_lossy().into_owned(),
            ));
        }
        if let Ok(v) = std::env::var("TANTRA_LIBRARY_DATA_ROOT") {
            cfg.env.push(("TANTRA_LIBRARY_DATA_ROOT".to_string(), v));
        }
        cfg
    }
}

type Reply = oneshot::Sender<Result<Value, LibraryError>>;

struct Req {
    method: String,
    params: Value,
    reply: Reply,
}

enum Entry {
    Init,
    Caller(Reply),
}

/// Async client for the `tantra-library-mcp` stdio server.
#[derive(Debug)]
pub struct LibraryMcpClient {
    tx: mpsc::Sender<Req>,
    healthy: Arc<AtomicBool>,
    timeout: Duration,
}

impl LibraryMcpClient {
    /// Spawn the supervisor task eagerly. Must be called inside a Tokio runtime.
    #[must_use]
    pub fn spawn(config: LibraryClientConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(config.queue_capacity.max(1));
        let healthy = Arc::new(AtomicBool::new(false));
        let timeout = config.request_timeout;
        tokio::spawn(supervise(config, rx, healthy.clone()));
        Arc::new(Self {
            tx,
            healthy,
            timeout,
        })
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, LibraryError> {
        let (reply, rx) = oneshot::channel();
        let req = Req {
            method: method.to_string(),
            params,
            reply,
        };
        self.tx.try_send(req).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => LibraryError::Busy,
            mpsc::error::TrySendError::Closed(_) => {
                LibraryError::Unavailable("supervisor stopped".into())
            }
        })?;
        match tokio::time::timeout(self.timeout, rx).await {
            Err(_) => Err(LibraryError::Timeout),
            Ok(Err(_)) => Err(LibraryError::Unavailable("request dropped".into())),
            Ok(Ok(r)) => r,
        }
    }

    /// Call an MCP tool and return the parsed JSON of `content[0].text`.
    ///
    /// # Errors
    /// See [`LibraryError`].
    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<Value, LibraryError> {
        let result = self
            .request("tools/call", json!({"name": name, "arguments": arguments}))
            .await?;
        let text = result
            .get("content")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("text"))
            .and_then(Value::as_str)
            .ok_or_else(|| LibraryError::Protocol("missing content[0].text".into()))?;
        let body: Value = serde_json::from_str(text)
            .map_err(|e| LibraryError::Protocol(format!("invalid tool JSON: {e}")))?;
        if result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let field =
                |k: &str, d: &str| body.get(k).and_then(Value::as_str).unwrap_or(d).to_string();
            return Err(LibraryError::Tool {
                code: field("error", "unknown"),
                message: field("message", ""),
            });
        }
        Ok(body)
    }

    /// Send a JSON-RPC `ping`.
    ///
    /// # Errors
    /// See [`LibraryError`].
    pub async fn ping(&self) -> Result<(), LibraryError> {
        match self.request("ping", json!({})).await {
            Ok(_) => {
                self.healthy.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.healthy.store(false, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Whether the child is alive and initialized (for readiness probes).
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}

enum Line {
    Ok(String),
    TooLong,
}

enum SessionEnd {
    Shutdown,
    Ended,
}

fn jittered(d: Duration) -> Duration {
    let max = u64::try_from(d.as_millis() / 4).unwrap_or(0);
    let extra = if max == 0 {
        0
    } else {
        rand::thread_rng().gen_range(0..=max)
    };
    d + Duration::from_millis(extra)
}

async fn supervise(
    config: LibraryClientConfig,
    mut rx: mpsc::Receiver<Req>,
    healthy: Arc<AtomicBool>,
) {
    let mut next_id: u64 = 1;
    let mut backoff = config.restart_backoff_initial;
    loop {
        let started = tokio::time::Instant::now();
        match run_session(&config, &mut rx, &healthy, &mut next_id).await {
            Ok(SessionEnd::Shutdown) => return,
            Ok(SessionEnd::Ended) => {
                if started.elapsed() >= config.stable_after {
                    backoff = config.restart_backoff_initial;
                }
            }
            Err(e) => tracing::warn!(error = %e, "library MCP child failed to start"),
        }
        healthy.store(false, Ordering::Relaxed);
        let sleep = tokio::time::sleep(jittered(backoff));
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => break,
                r = rx.recv() => match r {
                    None => return,
                    Some(req) => {
                        let _ = req.reply.send(Err(LibraryError::Unavailable("restarting".into())));
                    }
                },
            }
        }
        backoff = (backoff * 2).min(config.restart_backoff_max);
    }
}

fn fail_all(pending: &mut HashMap<u64, Entry>, mk: impl Fn() -> LibraryError) {
    for (_, e) in pending.drain() {
        if let Entry::Caller(tx) = e {
            let _ = tx.send(Err(mk()));
        }
    }
}

async fn write_req(
    stdin: &mut ChildStdin,
    id: u64,
    method: &str,
    params: &Value,
    timeout: Duration,
) -> std::io::Result<()> {
    let mut line =
        json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string();
    line.push('\n');
    let write = async {
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await
    };
    tokio::time::timeout(timeout, write)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "stdin write timed out"))?
}

async fn run_session(
    config: &LibraryClientConfig,
    rx: &mut mpsc::Receiver<Req>,
    healthy: &AtomicBool,
    next_id: &mut u64,
) -> std::io::Result<SessionEnd> {
    let mut cmd = Command::new(&config.command);
    cmd.args(&config.args)
        .envs(config.env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child: Child = cmd.spawn()?;
    let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(std::io::Error::other("child stdio unavailable"));
    };

    let max = config.max_line_bytes;
    let (line_tx, mut line_rx) = mpsc::channel::<Line>(16);
    let reader = tokio::spawn(async move {
        let mut r = BufReader::new(stdout);
        loop {
            let mut buf = Vec::new();
            let n = match (&mut r)
                .take(max as u64 + 1)
                .read_until(b'\n', &mut buf)
                .await
            {
                Ok(n) => n,
                Err(_) => return,
            };
            if n == 0 {
                return;
            }
            if buf.last() != Some(&b'\n') {
                if buf.len() > max {
                    let _ = line_tx.send(Line::TooLong).await;
                }
                return;
            }
            buf.pop();
            if line_tx
                .send(Line::Ok(String::from_utf8_lossy(&buf).into_owned()))
                .await
                .is_err()
            {
                return;
            }
        }
    });

    let mut pending: HashMap<u64, Entry> = HashMap::new();
    let mut was_healthy = false;
    let init_id = *next_id;
    *next_id += 1;
    write_req(
        &mut stdin,
        init_id,
        "initialize",
        &json!({"protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": {"name": "tantra-runtime", "version": "0"}}),
        config.request_timeout,
    )
    .await?;
    pending.insert(init_id, Entry::Init);

    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let init_deadline = tokio::time::Instant::now() + config.request_timeout;
    let end = loop {
        pending.retain(|_, e| match e {
            Entry::Caller(tx) => !tx.is_closed(),
            Entry::Init => true,
        });
        let can_recv = pending.len() < config.queue_capacity.max(1);
        tokio::select! {
            r = rx.recv(), if can_recv => match r {
                None => break SessionEnd::Shutdown,
                Some(req) => {
                    let id = *next_id;
                    *next_id += 1;
                    match write_req(&mut stdin, id, &req.method, &req.params, config.request_timeout).await {
                        Ok(()) => { pending.insert(id, Entry::Caller(req.reply)); }
                        Err(e) => {
                            let _ = req.reply.send(Err(LibraryError::Unavailable(format!("write failed: {e}"))));
                            fail_all(&mut pending, || LibraryError::Unavailable("child write failed".into()));
                            break SessionEnd::Ended;
                        }
                    }
                }
            },
            l = line_rx.recv() => match l {
                None => {
                    fail_all(&mut pending, || LibraryError::Unavailable("child exited".into()));
                    break SessionEnd::Ended;
                }
                Some(Line::TooLong) => {
                    fail_all(&mut pending, || LibraryError::PayloadTooLarge);
                    break SessionEnd::Ended;
                }
                Some(Line::Ok(line)) => {
                    let Ok(v) = serde_json::from_str::<Value>(&line) else {
                        tracing::warn!("library MCP: unparsable line");
                        continue;
                    };
                    let Some(id) = v.get("id").and_then(Value::as_u64) else { continue };
                    let res = match v.get("error") {
                        Some(err) => Err(LibraryError::Protocol(
                            err.get("message").and_then(Value::as_str).unwrap_or("rpc error").to_string(),
                        )),
                        None => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
                    };
                    match pending.remove(&id) {
                        Some(Entry::Init) => {
                            if res.is_ok() {
                                was_healthy = true;
                                healthy.store(true, Ordering::Relaxed);
                            }
                        }
                        Some(Entry::Caller(tx)) => { let _ = tx.send(res); }
                        None => {}
                    }
                }
            },
            _ = child.wait() => {
                fail_all(&mut pending, || LibraryError::Unavailable("child exited".into()));
                break SessionEnd::Ended;
            }
            () = tokio::time::sleep_until(init_deadline), if !was_healthy => {
                fail_all(&mut pending, || LibraryError::Unavailable("initialize timed out".into()));
                break SessionEnd::Ended;
            }
            _ = tick.tick() => {}
        }
    };
    reader.abort();
    let _ = child.kill().await;
    Ok(end)
}
