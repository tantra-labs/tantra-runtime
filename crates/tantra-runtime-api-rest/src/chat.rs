//! Local-LLM-backed chat WebSocket bridge (`GET /ws/chat`).
//!
//! Bridges the `tantra-agent-chat` conversational engine (session/turn/event model) to this
//! server over a WebSocket, backed by `tantra-agent-llm`'s local Candle-GGUF provider (or an
//! OpenAI/Ollama-compatible fallback) and the read-only tool ports in
//! `tantra_runtime_api::chat_adapters`.
//!
//! `GET /ws/chat?workspace=<id>` scopes the session to that workspace ([`ChatQuery`]) — connecting
//! again with the same `workspace` resumes its most recent session rather than starting a new
//! one (`SessionSelector::Continue`). Client -> server messages are `{"text": "..."}`; everything
//! the agent emits for the turn streams back as the chat engine's own `SessionEvent` JSON, one
//! WebSocket text frame per event.
//!
//! v1 scope (see `docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md`):
//! - Sessions persist to the configured database via
//!   [`tantra_runtime_persistence::SqlxSessionStore`] (embedded migrations run at boot) when a
//!   database is attached ([`AppState::with_database`]); without one they fall back to
//!   in-memory (`InMemorySessionStore`) and a server restart loses them.
//! - Portfolio-backed tools read the runtime's shared ledger repositories
//!   ([`AppState::shared_ledger`], docs/gaps/0006 design decision) through
//!   [`LedgerPortfolioReader`]: the `rest-api` account, its FIFO positions and tax lots, its
//!   orders and realized P&L — populated from the real `POST /orders`/fill flow, so an empty
//!   answer genuinely means "no trading activity in this process", never fabricated data.
//!
//! Market/screener tools read through live data via [`crate::chat_market::FeedMarketReader`]/
//! [`crate::chat_market::FeedSnapshotSource`]; items 1-5, 11-12 of
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md` are now closed
//! (projection bridges, indicators, display names, regime/risk metrics), leaving `market_depth`
//! as the remaining explicit gap (see that document). The price-alert tools write the caller's
//! `Profile.alerts` document through the profiles
//! store ([`crate::chat_alerts::ProfileAlertStore`], agent plan 0006 Phase 2). The news/filings
//! content tools (agent plan 0006 Phase 3) read filings and news through the `tantra-data`
//! projections via [`crate::chat_content::FeedContentReader`] (see
//! `tantra-data/docs/gaps/0006-news-ingestion.md`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::market_feed::ports::{BarReader, DepthReader, InstrumentMaster, QuoteReader};
use tantra_agent_chat::{
    ChatAgent, ChatErrorView, InMemorySessionStore, SessionEvent, SessionSelector, ToolRegistry,
    TurnId, TurnOutcome,
};
use tantra_agent_chat_tools::ports::alert::{AlertReader, AlertWriter};
use tantra_agent_chat_tools::ports::market::MarketReader;
use tantra_agent_chat_tools::ports::portfolio::PortfolioReader;
use tantra_agent_core::{RuntimeMode, SessionScope, SessionStore};
use tantra_agent_llm::candle_gguf::{CandleGgufConfig, CandleGgufProvider, ENV_GGUF_PATH};
use tantra_agent_llm::config::openai_provider_from_env;
use tantra_agent_llm::openai_compat::OpenAiCompatibleProvider;
use tantra_agent_llm::{LlmError, LlmProvider, MessagePart, Usage};
use tantra_risk::RiskEngine;
use tantra_runtime_api::{LedgerPortfolioReader, SharedLedger};

use crate::AppState;
use crate::chat_market::{FeedMarketReader, FeedSnapshotSource};

/// Whether either LLM backend is configured via environment variables (`TANTRA_GGUF_PATH` for
/// the local Candle-GGUF provider, `TANTRA_LLM_BASE_URL` for the OpenAI/Ollama-compatible one).
fn llm_configured() -> bool {
    std::env::var(ENV_GGUF_PATH).is_ok() || std::env::var("TANTRA_LLM_BASE_URL").is_ok()
}

/// Resolves the server-default `LlmProvider` from environment variables: a local GGUF model
/// (`TANTRA_GGUF_PATH`) takes priority; otherwise falls back to an OpenAI/Ollama-compatible
/// endpoint (`TANTRA_LLM_BASE_URL`/`TANTRA_LLM_MODEL`/`TANTRA_LLM_API_KEY`).
fn resolve_llm_provider() -> Result<Arc<dyn LlmProvider>, LlmError> {
    if let Ok(path) = std::env::var(ENV_GGUF_PATH) {
        let config = CandleGgufConfig::new(path);
        let provider = CandleGgufProvider::new(config);
        Ok(Arc::new(provider) as Arc<dyn LlmProvider>)
    } else {
        let provider = openai_provider_from_env()?;
        Ok(Arc::new(provider) as Arc<dyn LlmProvider>)
    }
}

/// The set of LLM providers a chat session can route a turn to: the server default plus any
/// per-turn `model` overrides registered at startup (gap 0009 — T17.2).
///
/// A per-turn override that matches no registered provider — and no GGUF file / remote base
/// URL that could serve it — resolves to an explicit [`LlmError::Capability`] notice; the
/// caller surfaces that notice in the chat reply instead of silently falling back.
#[derive(Clone)]
pub struct LlmProviderRegistry {
    /// The server-default provider (used whenever no `model` override is sent, and for
    /// `model` overrides equal to the default model name).
    default: Arc<dyn LlmProvider>,
    /// Extra providers keyed by exact model name, resolvable per turn.
    overrides: HashMap<String, Arc<dyn LlmProvider>>,
}

impl LlmProviderRegistry {
    /// Builds the registry from environment variables: the default provider
    /// ([`resolve_llm_provider`]), plus:
    /// - each extra local GGUF model declared as `TANTRA_GGUF_MODEL_<NAME>` (NAME = the model
    ///   name uppercased with non-alphanumerics mapped to `_`), served by an in-process
    ///   Candle-GGUF provider;
    /// - every model named in `TANTRA_LLM_MODELS` (comma-separated), served by the
    ///   OpenAI-compatible endpoint configured in `TANTRA_LLM_BASE_URL`.
    pub fn from_env() -> Result<Self, LlmError> {
        let default = resolve_llm_provider()?;
        let mut overrides: HashMap<String, Arc<dyn LlmProvider>> = HashMap::new();
        for (key, value) in std::env::vars() {
            if let Some(name) = key.strip_prefix("TANTRA_GGUF_MODEL_") {
                if name.is_empty() {
                    continue;
                }
                let model = name.replace('_', "-");
                let provider =
                    CandleGgufProvider::new(CandleGgufConfig::new(value).with_model_name(&model));
                overrides.insert(model, Arc::new(provider) as Arc<dyn LlmProvider>);
            }
        }
        if let Ok(base_url) = std::env::var("TANTRA_LLM_BASE_URL") {
            if let Ok(models) = std::env::var("TANTRA_LLM_MODELS") {
                let api_key = std::env::var("TANTRA_LLM_API_KEY").ok();
                for model in models.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    if overrides.contains_key(model) {
                        continue;
                    }
                    let provider = OpenAiCompatibleProvider::new(
                        base_url.clone(),
                        api_key.clone(),
                        model.to_string(),
                    );
                    overrides.insert(
                        model.to_string(),
                        Arc::new(provider) as Arc<dyn LlmProvider>,
                    );
                }
            }
        }
        Ok(Self { default, overrides })
    }

    /// Builds a registry from explicit providers (tests and embedders inject fakes here;
    /// the production path is [`LlmProviderRegistry::from_env`]).
    pub fn new(
        default: Arc<dyn LlmProvider>,
        overrides: HashMap<String, Arc<dyn LlmProvider>>,
    ) -> Self {
        Self { default, overrides }
    }

    /// The server-default provider.
    pub fn default(&self) -> Arc<dyn LlmProvider> {
        self.default.clone()
    }

    /// The server-default model name.
    pub fn default_model(&self) -> &str {
        self.default.model_name()
    }

    /// Resolves the provider for a per-turn `model` override.
    ///
    /// - `None`/blank or equal to the default model name → the default provider (the
    ///   server-default path stays byte-identical).
    /// - a registered override → that provider;
    /// - otherwise, when `TANTRA_LLM_BASE_URL` is configured, the requested model is routed
    ///   through the OpenAI/Ollama-compatible endpoint (the free-tier router backend);
    /// - otherwise → [`LlmError::Capability`] naming the requested model (never a silent
    ///   fallback to another model).
    pub fn resolve(&self, model: Option<&str>) -> Result<Arc<dyn LlmProvider>, LlmError> {
        match model.map(str::trim).filter(|s| !s.is_empty()) {
            None => Ok(self.default.clone()),
            Some(m) if m.eq_ignore_ascii_case(self.default.model_name()) => {
                Ok(self.default.clone())
            }
            Some(m) => {
                if let Some(provider) = self.overrides.get(m) {
                    return Ok(provider.clone());
                }
                if let Some((_, provider)) = self
                    .overrides
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(m))
                {
                    return Ok(provider.clone());
                }
                if let Ok(base_url) = std::env::var("TANTRA_LLM_BASE_URL") {
                    let api_key = std::env::var("TANTRA_LLM_API_KEY").ok();
                    let provider = OpenAiCompatibleProvider::new(base_url, api_key, m.to_string());
                    return Ok(Arc::new(provider) as Arc<dyn LlmProvider>);
                }
                Err(LlmError::Capability(format!(
                    "model '{m}' is not available locally — no GGUF file is configured for it \
                     and no remote LLM base URL (TANTRA_LLM_BASE_URL) is set; the server \
                     default is '{}'",
                    self.default.model_name()
                )))
            }
        }
    }
}

/// Loads (or creates) the runtime jobs registry the chat trade-intent sink enqueues into
/// (docs/gaps/0007 item 17): `TANTRA_JOBS_PATH` when set, else `~/.tantra/jobs.json`. A
/// registry that cannot be opened leaves the sink answering its explicit gap citation rather
/// than failing the chat build.
fn jobs_registry() -> Option<Arc<std::sync::Mutex<tantra_runtime_jobs::FileJobRegistry>>> {
    let path = std::env::var("TANTRA_JOBS_PATH")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("HOME").ok().map(|home| {
                std::path::PathBuf::from(home)
                    .join(".tantra")
                    .join("jobs.json")
            })
        })?;
    match tantra_runtime_jobs::FileJobRegistry::load_or_create(path) {
        Ok(registry) => Some(Arc::new(std::sync::Mutex::new(registry))),
        Err(e) => {
            tracing::warn!("jobs registry not available for the chat trade-intent sink: {e}");
            None
        }
    }
}

/// The chat-agent runtime held by [`AppState`] once configured: the built conversational agent,
/// ready to open one fresh session per WebSocket connection, plus the per-turn `model`
/// override registry (gap 0009 — T17.2).
pub struct ChatRuntime {
    agent: Arc<ChatAgent>,
    providers: LlmProviderRegistry,
}

impl ChatRuntime {
    /// Resolves the provider for a per-turn `model` override (see [`LlmProviderRegistry::resolve`]):
    /// the server default when no override is sent, the matching registered provider when one
    /// differs from the default, and an explicit [`LlmError::Capability`] notice when the
    /// requested model is not available locally (no GGUF file / remote base URL for it).
    pub fn provider_for(&self, model: Option<&str>) -> Result<Arc<dyn LlmProvider>, LlmError> {
        self.providers.resolve(model)
    }

    /// The server-default model name, as reported in the `SessionOpened` event.
    pub fn default_model(&self) -> &str {
        self.providers.default_model()
    }

    /// Builds a [`ChatRuntime`] from an explicit provider registry with a chat agent carrying
    /// no read-only tool ports and an in-memory session store. This is the injection point for
    /// tests and lightweight embedders; the production env-var path is
    /// [`ChatRuntime::from_env`].
    pub fn with_provider_registry(providers: LlmProviderRegistry) -> anyhow::Result<Self> {
        let registry = ToolRegistry::new();
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let agent = ChatAgent::builder(providers.default(), registry, session_store).build()?;
        Ok(Self {
            agent: Arc::new(agent),
            providers,
        })
    }
    /// Builds a [`ChatRuntime`] from environment variables if either a local GGUF model or an
    /// OpenAI-compatible endpoint is configured; returns `Ok(None)` when neither is set, so the
    /// caller leaves `AppState::chat` as `None` instead of failing server boot.
    ///
    /// `instruments`/`bars`/`quotes`/`risk_engine` are the same ports `AppState` already serves
    /// `/market-data/*` from — this is how `FeedMarketReader`/`FeedSnapshotSource` answer the
    /// chat tool catalog's market/screener tools with real data (closes the market/screener
    /// stand-in of docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md).
    /// `profiles` is the same store `/profile` routes serve: the price-alert tools read and
    /// write the caller's `Profile.alerts` document through it (see
    /// [`crate::chat_alerts::ProfileAlertStore`]). `shared_ledger` is [`AppState`]'s
    /// ledger-repository handle (docs/gaps/0006 design decision): the portfolio and tax tools
    /// read the real `rest-api` account/positions/orders/lots/P&L through
    /// [`LedgerPortfolioReader`]. `database` is the attached database from
    /// [`AppState::with_database`]: when set, the agent persists sessions to it via
    /// [`tantra_runtime_persistence::SqlxSessionStore`] (the embedded migrations that create
    /// `agent_chat_session`/`agent_chat_message` run at boot) instead of keeping them
    /// in-memory (docs/gaps/0006). `data_root` points at the data directory
    /// whose `catalog/filings`, `catalog/corporate_actions`, `catalog/block_deals`,
    /// `catalog/news`, `catalog/institutional_flows`, `catalog/delivery` and
    /// `catalog/economic_calendar` projections plus `open_interest` lakehouse `tantra-data`
    /// maintains — they back the filings, corporate-actions, block/bulk-deal, news,
    /// OI-buildup, institutional-flow, delivery, economic-calendar and macro-series chat
    /// tools (the institutional-flow/delivery/economic-calendar bridges close
    /// `tantra-data` gaps 0017/0019/0018 and `docs/gaps/0007` items 3b/11/12; see the
    /// `with_*` builders on [`crate::chat_market::FeedMarketReader`] and
    /// [`crate::chat_calendar::FeedCalendarReader`]).
    #[allow(clippy::too_many_arguments)]
    pub fn from_env(
        instruments: Arc<dyn InstrumentMaster>,
        bars: Arc<dyn BarReader>,
        quotes: Arc<dyn QuoteReader>,
        depth: Arc<dyn DepthReader>,
        risk_engine: Arc<Mutex<RiskEngine>>,
        profiles: Arc<crate::profile_store::ProfileStore>,
        shared_ledger: Arc<SharedLedger>,
        database: Option<Arc<tantra_runtime_persistence::Database>>,
        data_root: &std::path::Path,
    ) -> anyhow::Result<Option<Self>> {
        if !llm_configured() {
            return Ok(None);
        }
        // The registry carries the server default plus every per-turn model override declared
        // via environment variables (`TANTRA_GGUF_MODEL_<NAME>` / `TANTRA_LLM_MODELS`).
        let providers = LlmProviderRegistry::from_env()?;
        let provider = providers.default();

        let mut registry = ToolRegistry::new();
        // Portfolio-backed tools (docs/gaps/0006, now closed): `AppState`'s `shared_ledger`
        // handle implements all four ledger-repository traits, so `LedgerPortfolioReader`
        // answers real accounts/positions/tax lots/orders/cash/realized P&L — the same state
        // the `POST /orders`/fill flow records — instead of the old explicit gap error.
        let portfolio: Arc<dyn PortfolioReader> = Arc::new(LedgerPortfolioReader::new(
            shared_ledger.clone(),
            shared_ledger.clone(),
            shared_ledger.clone(),
            shared_ledger.clone(),
        ));
        let market: Arc<dyn MarketReader> = Arc::new(
            FeedMarketReader::new(
                instruments.clone(),
                bars.clone(),
                quotes.clone(),
                risk_engine.clone(),
            )
            .with_block_deals(data_root)
            .with_corporate_actions(data_root)
            .with_oi_store(data_root)
            .with_institutional_flows(data_root)
            .with_delivery(data_root)
            .with_economic_calendar(data_root)
            .with_depth_reader(depth),
        );
        // The market reader deliberately wires no live `TradeReader`: no broker trade feed
        // exists in this runtime yet (real feeds remain `tantra-market` gaps 0001/0002), so
        // `quote` honestly falls back to the bid/ask midpoint until one is attached.
        let mut snapshot =
            FeedSnapshotSource::new(instruments.clone()).with_risk_engine(risk_engine);
        if let Some(regime_instrument) = std::env::var("TANTRA_REGIME_INSTRUMENT")
            .ok()
            .and_then(|raw| crate::chat_market::parse_instrument(&raw))
        {
            snapshot = snapshot.with_regime(bars.clone(), regime_instrument);
        }
        let snapshots: Arc<dyn tantra_agent_chat_tools::ports::snapshot::SnapshotSource> =
            Arc::new(snapshot);
        // News/filings content tools (agent plan 0006 Phase 3): filings and news read
        // through the `tantra-data` projections (`{data_root}/catalog/filings` and
        // `{data_root}/catalog/news`).
        let content: Arc<dyn tantra_agent_chat_tools::ports::content::ContentReader> = Arc::new(
            crate::chat_content::FeedContentReader::with_filings(data_root).with_news(data_root),
        );
        // Economic-calendar tool (agent plan 0007 Track 2 Phase 5, docs/gaps/0007 item 3b):
        // the calendar is exported Python-side to `{root}/catalog/economic_calendar` and read
        // through `EconomicCalendarProjectionReader` (closes `tantra-data` gap 0018; see
        // `crate::chat_calendar`).
        let calendar: Arc<dyn tantra_agent_chat_tools::ports::calendar::CalendarReader> = Arc::new(
            crate::chat_calendar::FeedCalendarReader::new().with_economic_calendar(data_root),
        );
        // Fundamentals / mutual-fund / IPO tools (agent plan 0007 Track 3 Phases 6-8): all
        // three readers are wired to the `tantra-data` projections landed 2026-11-14 —
        // fundamentals (`{root}/catalog/fundamentals` + `fundamental_ratios`, gap 0020, item
        // 14), mutual funds + IPO (`{root}/catalog/mutual_funds` + `mf_nav` + `mf_returns` +
        // `ipos` + `ipo_subscriptions`, gaps 0021/0022, items 15/16). Fund *holdings* remain
        // unsourced (no free verified holdings feed), so `fund_holdings` keeps its explicit
        // gap citation (see `crate::chat_fund`).
        let fundamental: Arc<dyn tantra_agent_chat_tools::ports::fundamental::FundamentalReader> =
            Arc::new(
                crate::chat_fundamental::FeedFundamentalReader::new().with_fundamentals(data_root),
            );
        let fund: Arc<dyn tantra_agent_chat_tools::ports::fund::FundReader> =
            Arc::new(crate::chat_fund::FeedFundReader::new().with_funds(data_root));
        let ipo: Arc<dyn tantra_agent_chat_tools::ports::ipo::IpoReader> =
            Arc::new(crate::chat_ipo::FeedIpoReader::new().with_ipos(data_root));
        // Tax / ledger-statement tools (plan 0007 Track 5 Phase 14, docs/gaps/0007 item 19):
        // `tantra-ledger-portfolio` matched lots are recorded by the fill flow and exposed
        // through the shared ledger's closed-lot history, so `tax_report`/`ledger_statement`
        // answer real realized P&L per account/date range (see `crate::chat_tax`).
        let tax: Arc<dyn tantra_agent_chat_tools::ports::tax::TaxReportReader> =
            Arc::new(crate::chat_tax::FeedTaxReportReader::new(shared_ledger));
        let study: Arc<dyn tantra_agent_chat_tools::ports::study::StudyReader> =
            Arc::new(crate::experimental_analysis::FeedStudyReader::new(
                instruments,
                bars,
                quotes,
                data_root.to_path_buf(),
            ));
        tantra_agent_chat_tools::catalog::register_read_only_tools_with_studies(
            &mut registry,
            portfolio,
            market,
            content,
            calendar,
            fundamental,
            fund,
            ipo,
            tax,
            Some(snapshots),
            Some(study),
        );
        // Safe trade tools (plan 0007 Track 5 Phases 12-13, docs/gaps/0007 items 17-18): the
        // trade-intent sink enqueues validated intents into the `FileJobRegistry`-backed
        // approval queue (PendingApproval human gate; `x-papertrading` marks paper intents),
        // and the strategy store answers `strategy_manage` CRUD from an in-memory store (see
        // `crate::chat_trade` / `crate::chat_strategy_store`).
        let trade_sink: Arc<dyn tantra_agent_chat_tools::ports::trade_intent::TradeIntentSink> =
            Arc::new(crate::chat_trade::FeedTradeIntentSink::new(jobs_registry()));
        let strategy_store: Arc<dyn tantra_agent_chat_tools::ports::strategy_store::StrategyStore> =
            Arc::new(crate::chat_strategy_store::FeedStrategyStore::new());
        tantra_agent_chat_tools::catalog::register_trade_tools(
            &mut registry,
            trade_sink,
            strategy_store,
        );

        // Price alerts write the user's profile document (`ToolRisk::LocalWrite`; agent plan
        // 0006 Phase 2) — the same `Profile.alerts` value tantra-ui-web's alerts-store.ts
        // syncs via the /profile routes (docs/gaps/0004's profile-schema precedent).
        let alerts = Arc::new(crate::chat_alerts::ProfileAlertStore::new(profiles));
        let alert_reader: Arc<dyn AlertReader> = alerts.clone();
        let alert_writer: Arc<dyn AlertWriter> = alerts.clone();
        tantra_agent_chat_tools::catalog::register_alert_tools(
            &mut registry,
            alert_reader,
            alert_writer,
        );

        // Sessions persist to the configured database via `SqlxSessionStore` (the embedded
        // migrations creating `agent_chat_session`/`agent_chat_message` run at boot); with no
        // database attached they fall back to in-memory (docs/gaps/0006).
        let session_store: Arc<dyn SessionStore> = match database {
            Some(db) => Arc::new(tantra_runtime_persistence::SqlxSessionStore::new(
                db.as_ref(),
            )),
            None => Arc::new(InMemorySessionStore::new()),
        };

        let agent = ChatAgent::builder(provider, registry, session_store).build()?;
        Ok(Some(Self {
            agent: Arc::new(agent),
            providers,
        }))
    }
}

/// Incoming client message shape: `{"text": "...", "model": "…", "attachments": [...]}`.
///
/// `text` is required; `model` (per-turn override) and `attachments` (multimodal image
/// parts) are optional and default to unset, so the legacy `{ "text": ... }` payloads the
/// v1 web client sends keep parsing unchanged (gap 0009).
#[derive(Debug, Clone, Deserialize)]
pub struct ChatClientMessage {
    /// User text prompt.
    pub text: String,
    /// Optional per-turn model override. When it differs from the server default the runtime
    /// routes the turn through the matching [`LlmProviderRegistry`] entry (or an explicit
    /// capability notice is returned when the model is not available locally).
    #[serde(default)]
    pub model: Option<String>,
    /// Optional base64 image attachments (`image/png` / `image/jpeg`) for vision-capable
    /// providers (e.g. `Capture area for Quant` chart snapshots).
    #[serde(default)]
    pub attachments: Option<Vec<ChatAttachmentInput>>,
}

/// A base64-encoded image attachment submitted on a chat turn.
///
/// `data` carries the raw base64 payload unmodified (the pipeline never decodes or re-encodes
/// it); only the MIME type is validated (`image/png` / `image/jpeg`).
#[derive(Debug, Clone, Deserialize)]
pub struct ChatAttachmentInput {
    /// Image MIME type (`image/png`, `image/jpeg`).
    pub mime_type: String,
    /// Base64-encoded image bytes.
    pub data: String,
}

/// Query params accepted on `GET /ws/chat`.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatQuery {
    /// Workspace identifier the session is scoped to. Each distinct workspace gets its own
    /// session (and, once `SqlxSessionStore` lands per `docs/gaps/0006`, its own persisted
    /// history) — connections with the same `workspace` resume the most recent session for it
    /// rather than starting a new one every time (`SessionSelector::Continue`, not `New`).
    /// Omitted/blank falls back to a fixed `"default"` scope, matching pre-workspace-scoping
    /// behavior for any caller that doesn't pass one yet.
    workspace: Option<String>,
}

/// Upgrades an HTTP connection to a WebSocket bound to one chat-agent session, scoped to the
/// requested workspace (`?workspace=<id>`).
pub async fn chat_ws_handler(
    ws: WebSocketUpgrade,
    State(app_state): State<Arc<AppState>>,
    Query(query): Query<ChatQuery>,
) -> Response {
    let scope_name = query
        .workspace
        .filter(|w| !w.trim().is_empty())
        .unwrap_or_else(|| "default".to_string());
    ws.on_upgrade(move |socket| handle_chat_socket(socket, app_state, scope_name))
}

/// Handles one chat WebSocket connection: client text frames are submitted as turns, and every
/// session event the agent emits streams back as a JSON text frame.
async fn handle_chat_socket(socket: WebSocket, state: Arc<AppState>, scope_name: String) {
    let (mut sender, mut receiver) = socket.split();

    let Some(chat) = state.chat.clone() else {
        let err = serde_json::json!({
            "type": "error",
            "message": "chat not configured — set TANTRA_GGUF_PATH or TANTRA_LLM_BASE_URL",
        });
        let _ = sender.send(Message::Text(err.to_string())).await;
        return;
    };

    let (handle, mut events) = match chat
        .agent
        .open(SessionSelector::Continue(SessionScope::new(
            &scope_name,
            RuntimeMode::Live,
        )))
        .await
    {
        Ok(pair) => pair,
        Err(e) => {
            warn!("chat session open failed: {e}");
            let err = serde_json::json!({"type": "error", "message": e.to_string()});
            let _ = sender.send(Message::Text(err.to_string())).await;
            return;
        }
    };

    loop {
        tokio::select! {
            biased;

            client_msg = receiver.next() => {
                let Some(msg) = client_msg else {
                    debug!("chat WebSocket client disconnected");
                    break;
                };
                match msg {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<ChatClientMessage>(&text) {
                            Ok(ChatClientMessage {
                                text,
                                model,
                                attachments,
                            }) => {
                                // Validate attachment MIME types up front: only base64
                                // `image/png` / `image/jpeg` are accepted (gap 0009 — T17.2).
                                let mut attachment_parts: Vec<MessagePart> = Vec::new();
                                let mut unsupported_mimes: Vec<String> = Vec::new();
                                for attachment in attachments.unwrap_or_default() {
                                    match attachment.mime_type.as_str() {
                                        "image/png" | "image/jpeg" => {
                                            attachment_parts.push(MessagePart::Image {
                                                mime_type: attachment.mime_type,
                                                data: attachment.data,
                                            });
                                        }
                                        other => unsupported_mimes.push(other.to_string()),
                                    }
                                }
                                if !unsupported_mimes.is_empty() {
                                    send_capability_notice(
                                        &mut sender,
                                        &format!(
                                            "unsupported attachment MIME type(s): {} (supported: image/png, image/jpeg)",
                                            unsupported_mimes.join(", ")
                                        ),
                                    )
                                    .await;
                                    continue;
                                }

                                // Per-turn model override routing: the default provider when no
                                // override is sent, the matching provider when one differs from
                                // the default, or an explicit capability notice when the
                                // requested model is not available locally.
                                let provider = match chat.provider_for(model.as_deref()) {
                                    Ok(provider) => provider,
                                    Err(e) => {
                                        send_capability_notice(&mut sender, &e.to_string()).await;
                                        continue;
                                    }
                                };

                                // Tell the client when a per-turn override actually switched
                                // models (the session otherwise reports the server default).
                                if model.is_some() && provider.model_name() != chat.default_model()
                                {
                                    let event = SessionEvent::ProviderChanged {
                                        provider: None,
                                        model: Some(provider.model_name().to_string()),
                                    };
                                    match serde_json::to_string(&event) {
                                        Ok(json) => {
                                            if sender.send(Message::Text(json)).await.is_err() {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            warn!("failed to serialize chat session event: {e}")
                                        }
                                    }
                                }

                                if let Err(e) = handle
                                    .submit_turn(text, attachment_parts, Some(provider))
                                    .await
                                {
                                    warn!("chat submit failed: {e}");
                                }
                            }
                            Err(e) => debug!("ignoring malformed chat client message: {e}"),
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => {
                        debug!("chat WebSocket connection closed");
                        break;
                    }
                    _ => {}
                }
            }

            event = events.next() => {
                let Some(event) = event else {
                    debug!("chat session event stream closed");
                    break;
                };
                match serde_json::to_string(&event) {
                    Ok(json) => {
                        if sender.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => warn!("failed to serialize chat session event: {e}"),
                }
            }
        }
    }
}

/// Streams an explicit, honest capability notice to the client as a short assistant-style
/// turn (`TurnStarted` / `TextDelta` / `TurnEnded`), so a missing capability — an unavailable
/// per-turn model override, an unsupported attachment MIME type, a text-only model receiving
/// an image — is visible in the chat transcript instead of being silently dropped or falling
/// back to another model. The notice turn uses [`TurnId`] `0` (the session actor numbers its
/// own turns from 1); no provider is invoked and nothing is persisted.
async fn send_capability_notice(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    notice: &str,
) {
    let turn = TurnId(0);
    let events = [
        SessionEvent::TurnStarted {
            turn,
            queued_behind: 0,
        },
        SessionEvent::TextDelta {
            turn,
            round: 1,
            text: notice.to_string(),
        },
        SessionEvent::TurnEnded {
            turn,
            outcome: TurnOutcome::Failed {
                error: ChatErrorView {
                    kind: "capability".into(),
                    message: notice.to_string(),
                },
            },
            usage: Usage::default(),
            rounds: 1,
            persisted: false,
        },
    ];
    for event in events {
        match serde_json::to_string(&event) {
            Ok(json) => {
                if sender.send(Message::Text(json)).await.is_err() {
                    return;
                }
            }
            Err(e) => warn!("failed to serialize chat session event: {e}"),
        }
    }
}
