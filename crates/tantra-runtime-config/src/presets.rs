//! Canonical embedded trade-plan presets (Plan 0005, slice P3).
//!
//! The four canonical presets — `qullamaggie`, `ee_plus`, `atr_swing`,
//! `pattern_breakout` — are embedded, versioned TOML specs: the single source
//! of truth consumed by the FFI, strategies, runtime, and agent (JSON is the
//! wire form). Zero-I/O: the TOML is embedded at compile time via
//! `include_str!` and parsed with the pure-Rust `toml` crate (WASM-safe).
//!
//! Preset grounding:
//! - `qullamaggie` — root plan 0009 §3.1: market entry, day-extreme stop with
//!   the fixed 1×ADR% guard, SMA10 daily-close trail, ½ at 2R + runner,
//!   `FixedFractional` 0.5% risk, `min_reward_risk` 2.0.
//! - `ee_plus` — EE+ intraday: market entry, ATR stop, ATR trail, 3R primary +
//!   5R runner targets, 6-bar time stop, `FixedFractional` 1%.
//! - `atr_swing` — ATR-multiple stop, ATR trail, runner-only (no fixed legs),
//!   `FixedFractional` 1%.
//! - `pattern_breakout` — plan 0005 §2a: pattern-breakout entry,
//!   pattern-invalidation SL, ½ at `MeasuredMove(1.0)` (fallback `AtR(2.0)`) +
//!   runner on a Chandelier trail, `FixedFractional` 1%.
//!
//! Placed here (not `tantra-core`) per the T12 placement decision: the kernel
//! owns the wire contracts (`TradePlanSpec`/`TradePlanner`), the run-config
//! crate owns the preset *data* consumed by runtime, FFI, and strategies.

use tantra_core::{PlanError, TradePlanSpec};
use thiserror::Error;

/// Why a preset failed to load.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PresetError {
    /// The preset id is not one of the canonical ids.
    #[error("unknown preset: {0}")]
    UnknownPreset(String),
    /// The embedded TOML failed to deserialize (a build-shipping invariant).
    #[error("preset parse error: {0}")]
    Parse(String),
}

/// The canonical preset identifiers, in stable order.
pub const PRESET_IDS: [&str; 4] = ["qullamaggie", "ee_plus", "atr_swing", "pattern_breakout"];

/// The canonical preset identifiers, in stable order.
#[must_use]
pub fn preset_ids() -> [&'static str; 4] {
    PRESET_IDS
}

/// Load a canonical preset spec by id.
///
/// # Errors
/// [`PresetError::UnknownPreset`] for an unknown id;
/// [`PresetError::Parse`] when an embedded TOML fails to parse (a
/// build-shipping invariant that must never fire).
pub fn preset(id: &str) -> Result<TradePlanSpec, PresetError> {
    let raw = match id {
        "qullamaggie" => include_str!("presets/qullamaggie.toml"),
        "ee_plus" => include_str!("presets/ee_plus.toml"),
        "atr_swing" => include_str!("presets/atr_swing.toml"),
        "pattern_breakout" => include_str!("presets/pattern_breakout.toml"),
        _ => return Err(PresetError::UnknownPreset(id.to_string())),
    };
    let spec: TradePlanSpec =
        toml::from_str(raw).map_err(|err| PresetError::Parse(err.to_string()))?;
    Ok(spec)
}

/// Legacy alias for [`preset`] keeping the `PlanError`-shaped error contract
/// for callers that only need the "missing" branch.
///
/// # Errors
/// [`PlanError::MissingInput("preset")`] on any failure (unknown id or parse).
pub fn preset_plan_error(id: &str) -> Result<TradePlanSpec, PlanError> {
    preset(id).map_err(|_| PlanError::MissingInput("preset"))
}
