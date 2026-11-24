//! Route registry builder (design 0006 R1a): every route is registered through [`RouteTable`],
//! and each builder call *requires* the route's auth scope, so a route cannot be added without
//! one. Registering a route records a [`RouteEntry`] (method, path, scope, body cap) next to the
//! axum route itself, which makes the registry the single source of truth for auth scope and
//! request-body cap: `scope_for` and the drift guards read the entries, and the body cap is
//! applied as a layer on the very route it is recorded for.
//!
//! GET and DELETE routes carry no body cap; POST, PUT and PATCH routes always do
//! ([`DEFAULT_BODY_BYTES`] unless the `*_capped` variant overrides it).

use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::handler::Handler;
use axum::routing::{delete, get, patch, post, put};

use crate::{AppState, RouteScope};

/// Request-body cap applied to every POST, PUT and PATCH route unless overridden.
pub(crate) const DEFAULT_BODY_BYTES: usize = 1024 * 1024;

/// One registered route.
#[derive(Clone, Copy, Debug)]
pub struct RouteEntry {
    /// HTTP method, upper case.
    pub method: &'static str,
    /// Path with the router's own `:param` spelling, relative to `/api/v2` (or to the server
    /// root for a [`root`](Self::root) route).
    pub path: &'static str,
    /// The scope a caller needs (or `Public` / `Authenticated`).
    pub scope: RouteScope,
    /// Request-body cap in bytes; `Some` for POST, PUT and PATCH, `None` otherwise.
    pub body_limit: Option<usize>,
    /// Whether the route is mounted at the server root (documentation UI) rather than under
    /// `/api/v2`.
    pub root: bool,
}

/// A router under construction plus the entries registered on it.
pub(crate) struct RouteTable {
    router: Router<Arc<AppState>>,
    entries: Vec<RouteEntry>,
    root: bool,
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
            entries: Vec::new(),
            root: false,
        }
    }

    /// A table whose routes are mounted at the server root instead of under `/api/v2`.
    pub(crate) fn new_root() -> Self {
        Self {
            root: true,
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
            root: self.root,
        });
    }

    bodyless!(get, "GET", get);
    bodyless!(delete, "DELETE", delete);
    with_body!(post, post_capped, "POST", post);
    with_body!(put, put_capped, "PUT", put);
    with_body!(patch, patch_capped, "PATCH", patch);

    pub(crate) fn finish(self) -> (Router<Arc<AppState>>, Vec<RouteEntry>) {
        (self.router, self.entries)
    }
}
