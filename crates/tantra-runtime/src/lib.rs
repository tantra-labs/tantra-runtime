//! Facade of the `tantra-runtime` bounded context (House).
//!
//! Re-exports the member crates under their short names so consumers import the
//! whole runtime context from a single crate (facade-crate convention, root
//! plan 0003 Phase 4 / gap 0007).

pub use tantra_runtime_actors as actors;
pub use tantra_runtime_api as api;
pub use tantra_runtime_api_rest as api_rest;
pub use tantra_runtime_backtest as backtest;
pub use tantra_runtime_config as config;
pub use tantra_runtime_enhancer as enhancer;
pub use tantra_runtime_jobs as jobs;
pub use tantra_runtime_persistence as persistence;
pub use tantra_runtime_strategy as strategy;
pub use tantra_runtime_testing as testing;
