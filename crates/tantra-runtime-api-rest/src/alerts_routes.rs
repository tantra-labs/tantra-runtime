//! Caller-scoped price alerts as a first-class `/api/v2/alerts` resource (design 0006 R2c).
//!
//! Alerts live on the caller's `Profile.alerts` document via [`ProfileAlertStore`]; the owner is
//! always the authenticated [`Caller`], never a request parameter.

use axum::extract::{Path, State};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::str::FromStr;
use std::sync::Arc;
use tantra_agent_chat_tools::ports::alert::{AlertReader, AlertStatus, AlertWriter, PriceAlert};
use tantra_agent_chat_tools::ports::error::{PortError, PortErrorKind};
use tantra_core::Price;
use tantra_runtime_api::{ErrorCode, ErrorDetail};

use axum::http::StatusCode;
use axum::response::Response;

use crate::caller::Caller;
use crate::chat_alerts::ProfileAlertStore;
use crate::market::{ApiQuery, error, failure, success};
use crate::route_table::{RouteEntry, RouteTable};
use crate::{ApiJson, AppState, RouteScope};

pub(crate) fn v2_only_routes() -> axum::Router<Arc<AppState>> {
    table().finish().0
}

/// Manifest of the v2-only alert routes (`:param` spelling).
pub(crate) fn v2_only_manifest() -> Vec<RouteEntry> {
    table().finish().1
}

fn table() -> RouteTable {
    const AUTHED: RouteScope = RouteScope::Authenticated;
    RouteTable::new_v2_only()
        .get("/alerts", list_alerts, AUTHED)
        .post("/alerts", create_alert, AUTHED)
        .delete("/alerts/:id", delete_alert, AUTHED)
}

/// Query parameters of `GET /api/v2/alerts`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAlertsQuery {
    /// Only alerts of this workspace.
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Body of `POST /api/v2/alerts`.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateAlertRequest {
    /// Alert id (`al_<unix_ms>_<8 hex>` minted when omitted).
    #[serde(default)]
    pub id: Option<String>,
    /// Workspace the alert belongs to.
    pub workspace_id: String,
    /// Instrument identifier, e.g. `NSE:RELIANCE`.
    pub instrument_id: String,
    /// Positive target price as a decimal string.
    pub price: String,
    /// `active` (default) or `paused`; `triggered` is server-set only.
    #[serde(default)]
    pub status: Option<String>,
}

fn invalid(message: impl Into<String>, field: &str) -> Response {
    error(
        ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
            .with_context(json!({ "field": field })),
    )
}

fn port_failure(e: PortError) -> Response {
    match e.kind {
        PortErrorKind::Unavailable => {
            error(ErrorDetail::new(ErrorCode::ServiceUnavailable, e.message))
        }
        PortErrorKind::InvalidArgument if e.message.contains("already exists") => {
            error(ErrorDetail::new(ErrorCode::Conflict, e.message))
        }
        PortErrorKind::InvalidArgument => error(ErrorDetail::new(
            ErrorCode::ValidationInvalidRequest,
            e.message,
        )),
        _ => error(ErrorDetail::new(ErrorCode::InternalError, e.message)),
    }
}

fn store(state: &AppState, caller: String) -> Result<ProfileAlertStore, Box<Response>> {
    match &state.profiles {
        Some(p) => Ok(ProfileAlertStore::for_user(Arc::clone(p), caller)),
        None => Err(Box::new(error(ErrorDetail::new(
            ErrorCode::ServiceUnavailable,
            "profile store is not configured",
        )))),
    }
}

pub(crate) async fn list_alerts(
    State(state): State<Arc<AppState>>,
    Caller(caller): Caller,
    ApiQuery(q): ApiQuery<ListAlertsQuery>,
) -> Response {
    let store = match store(&state, caller) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    match store.list_alerts(q.workspace.as_deref()).await {
        Ok(alerts) => success(json!({ "alerts": alerts })),
        Err(e) => port_failure(e),
    }
}

pub(crate) async fn create_alert(
    State(state): State<Arc<AppState>>,
    Caller(caller): Caller,
    ApiJson(req): ApiJson<CreateAlertRequest>,
) -> Response {
    let store = match store(&state, caller) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let price = match Decimal::from_str(req.price.trim()) {
        Ok(d) if d > Decimal::ZERO => Price::new(d),
        _ => return invalid("price must be a positive decimal string", "price"),
    };
    let status = match req.status.as_deref() {
        None | Some("active") => AlertStatus::Active,
        Some("paused") => AlertStatus::Paused,
        Some(_) => return invalid("status must be `active` or `paused`", "status"),
    };
    if req.workspace_id.trim().is_empty() {
        return invalid("workspaceId must not be empty", "workspaceId");
    }
    let now = chrono::Utc::now();
    let id = match req.id {
        Some(id) if !id.trim().is_empty() => id,
        Some(_) => return invalid("id must not be empty", "id"),
        None => {
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            format!("al_{}_{}", now.timestamp_millis(), &suffix[..8])
        }
    };
    let alert = PriceAlert {
        id,
        workspace_id: req.workspace_id,
        instrument_id: req.instrument_id,
        price,
        status,
        created_at: now.format("%Y-%m-%d").to_string(),
        triggered_at: None,
    };
    match store.add_alert(alert).await {
        Ok(created) => {
            let mut resp = success(created);
            *resp.status_mut() = StatusCode::CREATED;
            resp
        }
        Err(e) => port_failure(e),
    }
}

pub(crate) async fn delete_alert(
    State(state): State<Arc<AppState>>,
    Caller(caller): Caller,
    Path(id): Path<String>,
) -> Response {
    let store = match store(&state, caller) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    match store.delete_alert(&id).await {
        Ok(Some(deleted)) => success(deleted),
        Ok(None) => failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(ErrorCode::NotFound, format!("alert `{id}` not found")),
        ),
        Err(e) => port_failure(e),
    }
}
