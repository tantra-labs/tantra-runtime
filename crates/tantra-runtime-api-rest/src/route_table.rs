//! Recording route table: every route registered through it is also written to a manifest, so
//! the auth drift guard can compare the routes the router really serves with `ROUTE_SCOPES`
//! (design 0006 R0) instead of a hand-kept list.

use std::sync::Arc;

use axum::Router;
use axum::handler::Handler;
use axum::routing::{delete, get, patch, post, put};

use crate::AppState;

/// `(METHOD, path)` pairs, with the router's own `:param` spelling.
pub(crate) type Manifest = Vec<(&'static str, &'static str)>;

/// A router under construction plus the manifest of what was registered on it.
pub(crate) struct RouteTable {
    router: Router<Arc<AppState>>,
    manifest: Manifest,
}

macro_rules! method {
    ($name:ident, $verb:literal, $func:ident) => {
        pub(crate) fn $name<H, T>(mut self, path: &'static str, handler: H) -> Self
        where
            H: Handler<T, Arc<AppState>>,
            T: 'static,
        {
            self.router = self.router.route(path, $func(handler));
            self.manifest.push(($verb, path));
            self
        }
    };
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        Self {
            router: Router::new(),
            manifest: Vec::new(),
        }
    }

    method!(get, "GET", get);
    method!(post, "POST", post);
    method!(put, "PUT", put);
    method!(patch, "PATCH", patch);
    method!(delete, "DELETE", delete);

    /// Transforms the router built so far (e.g. to apply a layer to the routes added until now).
    pub(crate) fn map(
        mut self,
        f: impl FnOnce(Router<Arc<AppState>>) -> Router<Arc<AppState>>,
    ) -> Self {
        self.router = f(self.router);
        self
    }

    pub(crate) fn finish(self) -> (Router<Arc<AppState>>, Manifest) {
        (self.router, self.manifest)
    }
}
