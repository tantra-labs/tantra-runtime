//! Route registry builder (design 0006 R1a): every route is registered through [`RouteTable`],
//! and each builder call *requires* the route's auth scope, so a route cannot be added without
//! one. Registering a route records a [`RouteEntry`] (method, path, scope, body cap) next to the
//! axum route itself, which makes the registry the single source of truth for auth scope and
//! request-body cap: `scope_for` and the drift guards read the entries, and the body cap is
//! applied as a layer on the very route it is recorded for.
//!
//! GET and DELETE routes carry no body cap; POST, PUT and PATCH routes always do
//! ([`DEFAULT_BODY_BYTES`] unless the `*_capped` variant overrides it).

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path};
use axum::handler::Handler;
use axum::http::{HeaderName, HeaderValue, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::{delete, get, patch, post, put};

use crate::{AppState, RouteScope};

/// Request-body cap applied to every POST, PUT and PATCH route unless overridden.
pub(crate) const DEFAULT_BODY_BYTES: usize = 1024 * 1024;

/// How `/api/v2` treats a route (design 0006 R2a).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum V2Disposition {
    /// Served as is.
    Canonical,
    /// Under `/api/v2` the route answers `308` to this canonical successor path.
    Redirect(&'static str),
    /// Served under `/api/v2` with `Deprecation` and successor `Link` headers.
    Deprecated(&'static str),
}

/// One registered route.
#[derive(Clone, Copy, Debug)]
pub struct RouteEntry {
    /// HTTP method, upper case.
    pub method: &'static str,
    /// Path with the router's own `:param` spelling, relative to the version prefix.
    pub path: &'static str,
    /// The scope a caller needs (or `Public` / `Authenticated`).
    pub scope: RouteScope,
    /// Request-body cap in bytes; `Some` for POST, PUT and PATCH, `None` otherwise.
    pub body_limit: Option<usize>,
    /// Whether the route is mounted only under `/api/v2`.
    pub v2_only: bool,
    /// How `/api/v2` treats this route.
    pub v2: V2Disposition,
}

/// A router under construction plus the entries registered on it.
pub(crate) struct RouteTable {
    router: Router<Arc<AppState>>,
    v2_router: Router<Arc<AppState>>,
    pending: V2Disposition,
    entries: Vec<RouteEntry>,
    v2_only: bool,
}

/// Percent-encodes a decoded path segment for a `Location` header.
fn encode_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `308 Permanent Redirect` to `/api/v2{successor}` with `:param`s filled by name from the
/// matched path and the query string preserved. Method and body semantics are identical, so
/// the client replays the request as is.
pub(crate) fn redirect_response(
    successor: &str,
    params: Option<HashMap<String, String>>,
    uri: &Uri,
) -> Response {
    let params = params.unwrap_or_default();
    let filled: Vec<String> = successor
        .split('/')
        .map(|seg| match seg.strip_prefix(':') {
            Some(name) => params
                .get(name)
                .map(|v| encode_segment(v))
                .unwrap_or_else(|| seg.to_owned()),
            None => seg.to_owned(),
        })
        .collect();
    let mut location = format!("/api/v2{}", filled.join("/"));
    if let Some(q) = uri.query() {
        location.push('?');
        location.push_str(q);
    }
    let mut res = Response::new(Body::empty());
    *res.status_mut() = StatusCode::PERMANENT_REDIRECT;
    if let Ok(v) = HeaderValue::from_str(&location) {
        res.headers_mut().insert(header::LOCATION, v);
    }
    res
}

macro_rules! redirect_handler {
    ($next:expr) => {{
        let next: &'static str = $next;
        move |p: Option<Path<HashMap<String, String>>>, uri: Uri| async move {
            redirect_response(next, p.map(|Path(m)| m), &uri)
        }
    }};
}

/// Adds `Deprecation: true` and a `rel="successor-version"` `Link` to every response.
fn deprecate(
    route: axum::routing::MethodRouter<Arc<AppState>>,
    successor: &'static str,
) -> axum::routing::MethodRouter<Arc<AppState>> {
    let link = format!("</api/v2{}>; rel=\"successor-version\"", braced(successor));
    route.layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let link = link.clone();
            async move {
                let mut res = next.run(req).await;
                res.headers_mut().insert(
                    HeaderName::from_static("deprecation"),
                    HeaderValue::from_static("true"),
                );
                if let Ok(v) = HeaderValue::from_str(&link) {
                    res.headers_mut().insert(header::LINK, v);
                }
                res
            }
        },
    ))
}

/// `:id` placeholders spelled `{id}`.
fn braced(path: &str) -> String {
    path.split('/')
        .map(|seg| match seg.strip_prefix(':') {
            Some(name) => format!("{{{name}}}"),
            None => seg.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

macro_rules! bodyless {
    ($name:ident, $verb:literal, $func:ident) => {
        pub(crate) fn $name<H, T>(
            mut self,
            path: &'static str,
            handler: H,
            scope: RouteScope,
        ) -> Self
        where
            H: Handler<T, Arc<AppState>>,
            T: 'static,
        {
            let v2 = match self.pending {
                V2Disposition::Redirect(next) => $func(redirect_handler!(next)),
                V2Disposition::Deprecated(next) => deprecate($func(handler.clone()), next),
                V2Disposition::Canonical => $func(handler.clone()),
            };
            self.v2_router = self.v2_router.route(path, v2);
            self.router = self.router.route(path, $func(handler));
            self.record($verb, path, scope, None);
            self
        }
    };
}

macro_rules! with_body {
    ($name:ident, $capped:ident, $verb:literal, $func:ident) => {
        pub(crate) fn $name<H, T>(self, path: &'static str, handler: H, scope: RouteScope) -> Self
        where
            H: Handler<T, Arc<AppState>>,
            T: 'static,
        {
            self.$capped(path, handler, scope, DEFAULT_BODY_BYTES)
        }

        #[allow(dead_code)]
        pub(crate) fn $capped<H, T>(
            mut self,
            path: &'static str,
            handler: H,
            scope: RouteScope,
            cap: usize,
        ) -> Self
        where
            H: Handler<T, Arc<AppState>>,
            T: 'static,
        {
            let v2 = match self.pending {
                V2Disposition::Redirect(next) => $func(redirect_handler!(next)),
                V2Disposition::Deprecated(next) => deprecate(
                    $func(handler.clone()).layer(DefaultBodyLimit::max(cap)),
                    next,
                ),
                V2Disposition::Canonical => {
                    $func(handler.clone()).layer(DefaultBodyLimit::max(cap))
                }
            };
            self.v2_router = self.v2_router.route(path, v2);
            self.router = self
                .router
                .route(path, $func(handler).layer(DefaultBodyLimit::max(cap)));
            self.record($verb, path, scope, Some(cap));
            self
        }
    };
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        Self {
            router: Router::new(),
            v2_router: Router::new(),
            pending: V2Disposition::Canonical,
            entries: Vec::new(),
            v2_only: false,
        }
    }

    /// A table whose routes are mounted only under `/api/v2`.
    pub(crate) fn new_v2_only() -> Self {
        Self {
            v2_only: true,
            ..Self::new()
        }
    }

    fn record(
        &mut self,
        method: &'static str,
        path: &'static str,
        scope: RouteScope,
        body_limit: Option<usize>,
    ) {
        self.entries.push(RouteEntry {
            method,
            path,
            scope,
            body_limit,
            v2_only: self.v2_only,
            v2: std::mem::replace(&mut self.pending, V2Disposition::Canonical),
        });
    }

    /// The next route registered answers `308` to `successor` under `/api/v2`.
    pub(crate) fn v2_redirect(mut self, successor: &'static str) -> Self {
        self.pending = V2Disposition::Redirect(successor);
        self
    }

    /// The next route stays served under `/api/v2` but carries deprecation headers.
    pub(crate) fn v2_deprecated(mut self, successor: &'static str) -> Self {
        self.pending = V2Disposition::Deprecated(successor);
        self
    }

    bodyless!(get, "GET", get);
    bodyless!(delete, "DELETE", delete);
    with_body!(post, post_capped, "POST", post);
    with_body!(put, put_capped, "PUT", put);
    with_body!(patch, patch_capped, "PATCH", patch);

    pub(crate) fn finish(self) -> (Router<Arc<AppState>>, Vec<RouteEntry>) {
        (self.router, self.entries)
    }

    /// The v1 router, the `/api/v2` router (redirects and deprecation applied) and the entries.
    pub(crate) fn finish_both(
        self,
    ) -> (
        Router<Arc<AppState>>,
        Router<Arc<AppState>>,
        Vec<RouteEntry>,
    ) {
        (self.router, self.v2_router, self.entries)
    }
}
