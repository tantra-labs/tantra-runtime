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
use std::sync::{Arc, Mutex, OnceLock, Weak};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
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
use tantra_agent_llm::{
    CatalogError, ConfigCatalog, ConfigError, LlmError, LlmProvider, MessagePart, ModelCatalog,
    Usage,
};
use tantra_risk::RiskEngine;
use tantra_runtime_api::{LedgerPortfolioReader, SharedLedger};

use crate::AppState;
use crate::caller::Caller;
use crate::chat_market::{FeedMarketReader, FeedSnapshotSource};

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
    /// Sorted configured model ids (for error messages and listings).
    ids: Vec<String>,
}

impl LlmProviderRegistry {
    /// Builds the registry from a [`ModelCatalog`] (loaded from `tantra.jsonc`): the default is
    /// the catalog default; every catalog id (`provider/model`) is registered, plus the upstream
    /// `model_name()` as an alias when it does not collide with another key.
    pub fn from_catalog(catalog: &dyn ModelCatalog) -> Result<Self, CatalogError> {
        let default = catalog.provider(&catalog.default_model())?;
        let mut overrides: HashMap<String, Arc<dyn LlmProvider>> = HashMap::new();
        let mut ids: Vec<String> = Vec::new();
        let mut aliases: Vec<(String, Arc<dyn LlmProvider>)> = Vec::new();
        for info in catalog.models() {
            let provider = catalog.provider(&info.id)?;
            aliases.push((provider.model_name().to_string(), provider.clone()));
            ids.push(info.id.clone());
            overrides.insert(info.id, provider);
        }
        for (alias, provider) in aliases {
            overrides.entry(alias).or_insert(provider);
        }
        ids.sort();
        Ok(Self {
            default,
            overrides,
            ids,
        })
    }

    /// The configured model ids, sorted.
    pub fn models(&self) -> Vec<String> {
        self.ids.clone()
    }

    /// Builds a registry from explicit providers (tests and embedders inject fakes here;
    /// the production path is [`LlmProviderRegistry::from_catalog`]).
    pub fn new(
        default: Arc<dyn LlmProvider>,
        overrides: HashMap<String, Arc<dyn LlmProvider>>,
    ) -> Self {
        let mut ids: Vec<String> = overrides.keys().cloned().collect();
        ids.sort();
        Self {
            default,
            overrides,
            ids,
        }
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
                Err(LlmError::Capability(format!(
                    "model '{m}' is not configured — available: {}; add it to tantra.jsonc \
                     (see tantra-agent/docs/design/provider-config.md)",
                    self.ids.join(", ")
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
    /// Late-bound weak handle to the served [`AppState`] used by the in-process strategy-loop
    /// port (weak to avoid an `AppState -> ChatRuntime -> port -> AppState` cycle).
    loop_state: Arc<OnceLock<Weak<AppState>>>,
    /// ACP (`GET /api/v1/acp`) agent host shared by all ACP connections (agent plan 0019 Phase 2).
    acp: Arc<AcpBridge>,
}

/// Maximum concurrent ACP WebSocket connections.
const ACP_MAX_CONNECTIONS: usize = 64;

/// System prompt base for the ACP agent host.
pub const ACP_SYSTEM_PROMPT_BASE: &str = "You are Tantra, an intelligent quantitative finance and algorithmic trading assistant for Indian equities (NSE/BSE). Use the tools to fetch real data; never invent prices or statistics.";

/// The ACP agent host plus a connection-cap semaphore.
pub(crate) struct AcpBridge {
    host: Arc<tantra_agent_acp::AgentHost>,
    slots: Arc<tokio::sync::Semaphore>,
}

impl AcpBridge {
    fn new(
        tools: Arc<tantra_agent_chat::ToolSet>,
        store: Arc<dyn SessionStore>,
        catalog: Arc<dyn tantra_agent_acp::ModelCatalog>,
    ) -> Self {
        let host = tantra_agent_acp::AgentHost {
            system_prompt: format!(
                "{ACP_SYSTEM_PROMPT_BASE}\n\n{}",
                tantra_agent_chat_tools::PRESENT_VIEW_GUIDANCE
            ),
            tools,
            store,
            catalog,
            turn_config: tantra_agent_chat::TurnConfig::default(),
            max_sessions: 8,
        };
        Self {
            host: Arc::new(host),
            slots: Arc::new(tokio::sync::Semaphore::new(ACP_MAX_CONNECTIONS)),
        }
    }
}

impl ChatRuntime {
    /// Binds the served state for in-process strategy-loop tools (first bind wins).
    pub(crate) fn bind_state(&self, state: &Arc<AppState>) {
        let _ = self.loop_state.set(Arc::downgrade(state));
    }

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
    /// [`ChatRuntime::from_config`].
    pub fn with_provider_registry(providers: LlmProviderRegistry) -> anyhow::Result<Self> {
        let mut registry = ToolRegistry::new();
        tantra_agent_chat_tools::catalog::register_presentation_tools(&mut registry);
        let acp_tools = Arc::new(registry.snapshot());
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let acp_catalog: Arc<dyn tantra_agent_acp::ModelCatalog> =
            Arc::new(tantra_agent_acp::StaticCatalog::new().with_model(
                "default",
                "Default",
                providers.default(),
            ));
        let acp = Arc::new(AcpBridge::new(
            acp_tools,
            Arc::clone(&session_store),
            acp_catalog,
        ));
        let agent = ChatAgent::builder(providers.default(), registry, session_store).build()?;
        Ok(Self {
            agent: Arc::new(agent),
            providers,
            loop_state: Arc::new(OnceLock::new()),
            acp,
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
    pub fn from_config(
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
        let loaded = match tantra_agent_llm::load_config(None) {
            Ok(l) => l,
            Err(ConfigError::NotFound { searched }) => {
                tracing::info!(
                    "chat disabled: no tantra.jsonc provider config found (searched {searched:?})"
                );
                return Ok(None);
            }
            Err(e) => return Err(anyhow::anyhow!("invalid LLM provider config: {e}")),
        };
        let catalog = ConfigCatalog::build(&loaded, &|k| std::env::var(k).ok())
            .map_err(|e| anyhow::anyhow!("invalid LLM provider config: {e}"))?;
        let providers = LlmProviderRegistry::from_catalog(&catalog)
            .map_err(|e| anyhow::anyhow!("invalid LLM provider config: {e}"))?;
        tracing::info!(
            default = %catalog.default_model(),
            models = catalog.models().len(),
            "chat LLM providers loaded from {:?}",
            loaded.sources
        );
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
        let strategy_runner: Arc<dyn tantra_agent_chat_tools::ports::strategy::StrategyRunner> =
            Arc::new(crate::strategy_loop::RuntimeStrategyRunner::new(
                instruments.clone(),
                bars.clone(),
                quotes.clone(),
                Some(data_root.to_path_buf()),
            ));
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

        // Strategy execution, bounded tuning & trade-suggestion tools (Plan 0015 Phase 3):
        // backed by real `RuntimeStrategyRunner` over market catalog, historical bars, and
        // live option chains (replacing `DefaultStrategyRunner` hardcoded stubs).
        tantra_agent_chat_tools::catalog::register_strategy_tools(
            &mut registry,
            Some(strategy_runner),
        );

        // Strategy-loop eval/sweep/hedge + chart-annotation tools (root plan 0015 T21.6 /
        // agent plan 0018): call the `/studies/strategy-loop/*` handlers in-process; the state
        // is bound late by `api_router_with_config` via `ChatRuntime::bind_state`.
        let loop_cell: Arc<OnceLock<Weak<AppState>>> = Arc::new(OnceLock::new());
        tantra_agent_chat_tools::catalog::register_strategy_loop_tools(
            &mut registry,
            Arc::new(crate::strategy_loop::RuntimeStrategyLoopPort::new(
                Arc::clone(&loop_cell),
            )),
        );

        // Inline `present_view` tool (agent plan 0019 Phase 2); also served on `/ws/chat`.
        tantra_agent_chat_tools::catalog::register_presentation_tools(&mut registry);
        let acp_tools = Arc::new(registry.snapshot());

        // Sessions persist to the configured database via `SqlxSessionStore` (the embedded
        // migrations creating `agent_chat_session`/`agent_chat_message` run at boot); with no
        // database attached they fall back to in-memory (docs/gaps/0006).
        let session_store: Arc<dyn SessionStore> = match database {
            Some(db) => Arc::new(tantra_runtime_persistence::SqlxSessionStore::new(
                db.as_ref(),
            )),
            None => Arc::new(InMemorySessionStore::new()),
        };

        let acp = Arc::new(AcpBridge::new(
            acp_tools,
            Arc::clone(&session_store),
            Arc::new(catalog),
        ));
        let agent = ChatAgent::builder(provider, registry, session_store).build()?;
        Ok(Some(Self {
            agent: Arc::new(agent),
            providers,
            loop_state: loop_cell,
            acp,
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
            "message": "chat not configured — add an LLM provider to tantra.jsonc (see tantra-agent/docs/design/provider-config.md)",
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

/// `GET /api/v1/acp` — serves the Tantra chat agent over the Agent Client Protocol (newline-delimited
/// JSON-RPC, one message per WebSocket text frame) with the runtime's full chat tool set.
///
/// Extracting [`Caller`] makes the route fail closed when auth is enforced.
pub async fn acp_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    caller: Caller,
) -> Response {
    let Some(chat) = state.chat.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "error": "chat not configured — add an LLM provider to tantra.jsonc"
            })),
        )
            .into_response();
    };
    let bridge = Arc::clone(&chat.acp);
    let Ok(permit) = Arc::clone(&bridge.slots).try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "error": "too many ACP connections" })),
        )
            .into_response();
    };
    tracing::info!(caller = %caller.0, "acp connection");
    ws.max_message_size(1 << 20)
        .max_frame_size(1 << 20)
        .on_upgrade(move |socket| async move {
            run_acp_socket(socket, bridge).await;
            drop(permit);
        })
}

/// Bridges one WebSocket to an in-process ACP `serve` loop over an in-memory duplex pipe.
async fn run_acp_socket(socket: WebSocket, bridge: Arc<AcpBridge>) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (srv, cli) = tokio::io::duplex(256 * 1024);
    let (srv_read, srv_write) = tokio::io::split(srv);
    let (cli_read, mut cli_write) = tokio::io::split(cli);
    let (mut sender, mut receiver) = socket.split();

    let host = Arc::clone(&bridge.host);
    let mut agent = tokio::spawn(async move {
        if let Err(e) = tantra_agent_acp::serve(host, srv_read, srv_write).await {
            debug!("acp serve ended: {e}");
        }
    });

    let mut inbound = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(t) => {
                    let mut line = t.replace(['\r', '\n'], " ");
                    line.push('\n');
                    if cli_write.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        let _ = cli_write.shutdown().await;
    });

    let mut outbound = tokio::spawn(async move {
        let mut reader = BufReader::new(cli_read);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if sender
                        .send(Message::Text(trimmed.to_owned()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = &mut inbound => {}
        _ = &mut outbound => {}
        _ = &mut agent => {}
    }
    inbound.abort();
    outbound.abort();
    agent.abort();
}
