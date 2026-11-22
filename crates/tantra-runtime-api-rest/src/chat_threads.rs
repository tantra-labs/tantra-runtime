//! Caller- and workspace-scoped `/api/v2/chat/threads` CRUD endpoints
//! (`docs/gaps/0014-chat-trace-streaming-and-thread-persistence.md`).
//!
//! Backed by [`crate::AppState::chat_session_store`] (`SqlxSessionStore` when a database is
//! attached or `InMemorySessionStore` otherwise). Every route requires an authenticated
//! [`Caller`] (failing closed when auth is enabled) and isolates threads strictly by
//! `(caller_id, workspace)` using [`crate::chat::chat_scope_name`].

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tantra_agent_core::ports::{
    NewSession, PersistedMessage, RuntimeMode, SessionFilter, SessionId, SessionInfo, StoreError,
    StoreErrorKind,
};
use tantra_runtime_api::{ErrorCode, ErrorDetail};

use crate::caller::Caller;
use crate::chat::chat_scope_name;
use crate::market::{ApiQuery, failure, success};
use crate::profile_store::DEFAULT_USER_ID;
use crate::route_table::{RouteEntry, RouteTable};
use crate::{ApiJson, AppState, RouteScope};

/// Maximum number of threads returned by `GET /api/v2/chat/threads`.
const MAX_THREAD_LIST_LIMIT: usize = 200;

/// Default number of threads returned when `limit` is omitted.
const DEFAULT_THREAD_LIST_LIMIT: usize = 50;

/// Query parameters accepted by `GET /api/v2/chat/threads`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListChatThreadsQuery {
    /// Optional workspace identifier (`default` when omitted if filtering by single workspace).
    #[serde(default)]
    pub workspace: Option<String>,
    /// Optional filter on archived status (`true` = only archived, `false` = only active).
    #[serde(default)]
    pub archived: Option<bool>,
    /// Maximum threads to return (clamped to `200`, defaults to `50`).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Optional workspace scope query parameter on `/:id` thread routes.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ThreadScopeQuery {
    /// Workspace identifier; when supplied, the thread must belong to this exact workspace.
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/v2/chat/threads`.
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateChatThreadRequest {
    /// Target workspace identifier (defaults to `"default"` when omitted or blank).
    #[serde(default)]
    pub workspace: Option<String>,
    /// Initial human-readable thread title.
    #[serde(default)]
    pub title: Option<String>,
    /// Initial model identifier (falls back to server default when omitted or blank).
    #[serde(default)]
    pub model: Option<String>,
}

/// Request body for `PATCH /api/v2/chat/threads/:id`.
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchChatThreadRequest {
    /// Updated thread title (must not be whitespace-only).
    #[serde(default)]
    pub title: Option<String>,
    /// Archive or unarchive the thread.
    #[serde(default)]
    pub archived: Option<bool>,
    /// Updated model name associated with the thread (must not be whitespace-only).
    #[serde(default)]
    pub model: Option<String>,
}

/// Wire summary of a single persisted chat thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChatThreadDto {
    /// Session / thread UUID string.
    pub id: String,
    /// Workspace identifier with any caller prefix stripped.
    pub workspace: String,
    /// Thread title (defaults to `"New Quant session"` when unset or blank).
    pub title: String,
    /// Active LLM model name.
    pub model: String,
    /// LLM provider identifier.
    pub provider: String,
    /// Whether the thread is archived.
    pub archived: bool,
    /// Creation timestamp (RFC 3339).
    pub created_at: String,
    /// Last updated timestamp (RFC 3339).
    pub updated_at: String,
}

/// Response payload for `GET /api/v2/chat/threads`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChatThreadsListDto {
    /// Caller-scoped threads ordered newest first.
    pub threads: Vec<ChatThreadDto>,
}

/// Single persisted turn/message item returned by `GET /api/v2/chat/threads/:id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChatThreadMessageDto {
    /// Strictly increasing message sequence number within the thread.
    pub seq: u64,
    /// Message role (`user`, `assistant`, `tool`, or `system`).
    pub role: String,
    /// Structured JSON message payload (`{"text": ...}`, `tool_calls`, `results`, etc.).
    pub content: serde_json::Value,
    /// Prompt token count recorded on this message, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u32>,
    /// Completion token count recorded on this message, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u32>,
    /// Message creation timestamp (RFC 3339).
    pub created_at: String,
}

/// Response payload for `GET /api/v2/chat/threads/:id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChatThreadDetailDto {
    /// Thread summary metadata.
    pub thread: ChatThreadDto,
    /// Chronologically ordered persisted messages for this thread.
    pub messages: Vec<ChatThreadMessageDto>,
}

/// Response payload for `DELETE /api/v2/chat/threads/:id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DeleteChatThreadResponse {
    /// Always `true` when deletion succeeded.
    pub deleted: bool,
    /// Deleted thread UUID string.
    pub id: String,
}

/// Routes mounted only under `/api/v2`.
pub(crate) fn v2_only_routes() -> Router<Arc<AppState>> {
    v2_only_table().finish().0
}

/// Manifest of the v2-only `/chat/threads*` routes (`:param` spelling).
pub(crate) fn v2_only_manifest() -> Vec<RouteEntry> {
    v2_only_table().finish().1
}

fn v2_only_table() -> RouteTable {
    const AUTH: RouteScope = RouteScope::Authenticated;
    RouteTable::new_v2_only()
        .get("/chat/threads", get_threads, AUTH)
        .post("/chat/threads", post_thread, AUTH)
        .get("/chat/threads/:id", get_thread, AUTH)
        .patch("/chat/threads/:id", patch_thread, AUTH)
        .delete("/chat/threads/:id", delete_thread, AUTH)
}

fn normalized_workspace(workspace: Option<&str>) -> Option<&str> {
    workspace.map(str::trim).filter(|w| !w.is_empty())
}

fn caller_owns_scope(caller_id: &str, profile: &str, requested_workspace: Option<&str>) -> bool {
    if let Some(ws) = normalized_workspace(requested_workspace) {
        return profile == chat_scope_name(caller_id, Some(ws));
    }
    if caller_id == DEFAULT_USER_ID {
        !profile.contains('/')
    } else {
        profile
            .strip_prefix(caller_id)
            .is_some_and(|rest| rest.starts_with('/'))
    }
}

fn workspace_from_profile(caller_id: &str, profile: &str) -> String {
    if caller_id == DEFAULT_USER_ID {
        profile.to_owned()
    } else {
        let prefix = format!("{caller_id}/");
        profile.strip_prefix(&prefix).unwrap_or(profile).to_owned()
    }
}

fn thread_to_dto(caller_id: &str, info: SessionInfo) -> ChatThreadDto {
    let title = info
        .title
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .unwrap_or("New Quant session")
        .to_owned();
    ChatThreadDto {
        id: info.id.to_string(),
        workspace: workspace_from_profile(caller_id, &info.profile),
        title,
        model: info.model,
        provider: info.provider,
        archived: info.archived,
        created_at: info.created_at.to_rfc3339(),
        updated_at: info.updated_at.to_rfc3339(),
    }
}

fn message_to_dto(msg: PersistedMessage) -> ChatThreadMessageDto {
    ChatThreadMessageDto {
        seq: msg.seq,
        role: msg.role,
        content: msg.content,
        input_tokens: msg.input_tokens,
        output_tokens: msg.output_tokens,
        created_at: msg.created_at.to_rfc3339(),
    }
}

fn not_found(id_label: &str) -> Response {
    failure(
        StatusCode::NOT_FOUND,
        ErrorDetail::new(
            ErrorCode::NotFound,
            format!("thread '{id_label}' not found"),
        ),
    )
}

fn store_failure(err: StoreError) -> Response {
    match err.kind {
        StoreErrorKind::NotFound => failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(ErrorCode::NotFound, err.message),
        ),
        StoreErrorKind::Conflict => failure(
            StatusCode::CONFLICT,
            ErrorDetail::new(ErrorCode::ValidationInvalidRequest, err.message),
        ),
        StoreErrorKind::Busy => failure(
            StatusCode::TOO_MANY_REQUESTS,
            ErrorDetail::new(ErrorCode::RateLimited, err.message),
        ),
        StoreErrorKind::Unavailable | StoreErrorKind::Corrupt | StoreErrorKind::Other => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorDetail::new(ErrorCode::ServiceUnavailable, err.message),
        ),
    }
}

fn parse_thread_id(raw: &str) -> Result<SessionId, Box<Response>> {
    raw.trim()
        .parse::<SessionId>()
        .map_err(|_| Box::new(not_found(raw)))
}

async fn load_owned_thread(
    state: &AppState,
    caller_id: &str,
    id: SessionId,
    workspace: Option<&str>,
) -> Result<SessionInfo, Box<Response>> {
    let store = state.chat_session_store();
    let info = store
        .get(id)
        .await
        .map_err(|e| Box::new(store_failure(e)))?
        .ok_or_else(|| Box::new(not_found(&id.to_string())))?;
    if info.mode != RuntimeMode::Live || !caller_owns_scope(caller_id, &info.profile, workspace) {
        return Err(Box::new(not_found(&id.to_string())));
    }
    Ok(info)
}

/// `GET /api/v2/chat/threads` — lists chat threads owned by [`Caller`].
pub async fn get_threads(
    Caller(caller_id): Caller,
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<ListChatThreadsQuery>,
) -> Response {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_THREAD_LIST_LIMIT)
        .clamp(1, MAX_THREAD_LIST_LIMIT);
    let requested_ws = normalized_workspace(query.workspace.as_deref());
    let filter = SessionFilter {
        profile: requested_ws.map(|ws| chat_scope_name(&caller_id, Some(ws))),
        mode: Some(RuntimeMode::Live),
        archived: query.archived,
        limit: if requested_ws.is_some() {
            Some(limit)
        } else {
            Some(MAX_THREAD_LIST_LIMIT * 2)
        },
    };
    let store = state.chat_session_store();
    let rows = match store.list(filter).await {
        Ok(rows) => rows,
        Err(err) => return store_failure(err),
    };
    let threads = rows
        .into_iter()
        .filter(|info| caller_owns_scope(&caller_id, &info.profile, requested_ws))
        .take(limit)
        .map(|info| thread_to_dto(&caller_id, info))
        .collect();
    success(ChatThreadsListDto { threads })
}

/// `POST /api/v2/chat/threads` — creates a new caller- and workspace-scoped chat thread.
pub async fn post_thread(
    Caller(caller_id): Caller,
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<CreateChatThreadRequest>,
) -> Response {
    let scope = chat_scope_name(&caller_id, body.workspace.as_deref());
    let id = SessionId::new();
    let title = body
        .title
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(ToOwned::to_owned);
    let default_model = state
        .chat
        .as_ref()
        .map(|chat| chat.default_model().to_owned())
        .unwrap_or_else(|| "default".to_owned());
    let model = body
        .model
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .unwrap_or(&default_model)
        .to_owned();
    let provider = if state.chat.is_some() {
        "local".to_owned()
    } else {
        "unconfigured".to_owned()
    };

    let store = state.chat_session_store();
    if let Err(err) = store
        .create(NewSession {
            id,
            profile: scope,
            mode: RuntimeMode::Live,
            provider,
            model,
            title,
        })
        .await
    {
        return store_failure(err);
    }

    match store.get(id).await {
        Ok(Some(info)) => success(thread_to_dto(&caller_id, info)),
        Ok(None) => not_found(&id.to_string()),
        Err(err) => store_failure(err),
    }
}

/// `GET /api/v2/chat/threads/:id` — returns metadata and persisted messages for an owned thread.
pub async fn get_thread(
    Caller(caller_id): Caller,
    Path(raw_id): Path<String>,
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<ThreadScopeQuery>,
) -> Response {
    let id = match parse_thread_id(&raw_id) {
        Ok(id) => id,
        Err(resp) => return *resp,
    };
    let info = match load_owned_thread(&state, &caller_id, id, query.workspace.as_deref()).await {
        Ok(info) => info,
        Err(resp) => return *resp,
    };
    let store = state.chat_session_store();
    let rows = match store.messages(id).await {
        Ok(rows) => rows,
        Err(err) => return store_failure(err),
    };
    let messages = rows
        .into_iter()
        .map(|(_, msg)| message_to_dto(msg))
        .collect();
    success(ChatThreadDetailDto {
        thread: thread_to_dto(&caller_id, info),
        messages,
    })
}

/// `PATCH /api/v2/chat/threads/:id` — updates title, archived status, or model on an owned thread.
pub async fn patch_thread(
    Caller(caller_id): Caller,
    Path(raw_id): Path<String>,
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<ThreadScopeQuery>,
    ApiJson(body): ApiJson<PatchChatThreadRequest>,
) -> Response {
    let id = match parse_thread_id(&raw_id) {
        Ok(id) => id,
        Err(resp) => return *resp,
    };

    if let Some(ref raw_title) = body.title
        && raw_title.trim().is_empty()
    {
        return failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "title must not be blank",
            ),
        );
    }
    if let Some(ref raw_model) = body.model
        && raw_model.trim().is_empty()
    {
        return failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "model must not be blank",
            ),
        );
    }

    let info = match load_owned_thread(&state, &caller_id, id, query.workspace.as_deref()).await {
        Ok(info) => info,
        Err(resp) => return *resp,
    };
    let store = state.chat_session_store();

    if let Some(title) = body.title.as_deref().map(str::trim)
        && let Err(err) = store.rename(id, title).await
    {
        return store_failure(err);
    }
    if let Some(archived) = body.archived
        && let Err(err) = store.set_archived(id, archived).await
    {
        return store_failure(err);
    }
    if let Some(model) = body.model.as_deref().map(str::trim)
        && let Err(err) = store.update_provider(id, &info.provider, model).await
    {
        return store_failure(err);
    }

    match store.get(id).await {
        Ok(Some(updated)) => success(thread_to_dto(&caller_id, updated)),
        Ok(None) => not_found(&id.to_string()),
        Err(err) => store_failure(err),
    }
}

/// `DELETE /api/v2/chat/threads/:id` — permanently deletes an owned thread and its messages.
pub async fn delete_thread(
    Caller(caller_id): Caller,
    Path(raw_id): Path<String>,
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<ThreadScopeQuery>,
) -> Response {
    let id = match parse_thread_id(&raw_id) {
        Ok(id) => id,
        Err(resp) => return *resp,
    };
    if let Err(resp) = load_owned_thread(&state, &caller_id, id, query.workspace.as_deref()).await {
        return *resp;
    }
    let store = state.chat_session_store();
    if let Err(err) = store.delete(id).await {
        return store_failure(err);
    }
    success(DeleteChatThreadResponse {
        deleted: true,
        id: id.to_string(),
    })
}
