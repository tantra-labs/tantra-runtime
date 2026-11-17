//! Tests for the canonical embedded trade-plan presets (Plan 0005, P3) that
//! live here in `tantra-runtime-config` (the run-config crate owns preset data;
//! `tantra-core` owns the wire contracts). Golden vectors assert a preset →
//! `TradePlanner::plan` produces the byte-for-byte committed `TradePlan`.

use std::fs;

use rust_decimal::Decimal;
use serde_json::Value;
use tantra_core::{
    ChartPattern, EntryRule, Exchange, InstrumentId, MaKind, Money, OrderSide, PatternKind,
    PatternPivot, PatternStage, PlanSubject, PlannerInputs, Quantity, RiskBudget, SignalDirection,
    SizingMethod, StopRule, TimeStop, Timeframe, TradePlan, TradePlanSpec, TradePlanner, TrailRule,
};
use tantra_runtime_config::presets::{PresetError, preset, preset_ids};

fn dec(s: &str) -> Decimal {
    s.parse().unwrap()
}

fn ts() -> tantra_core::chrono::DateTime<tantra_core::chrono::Utc> {
    use tantra_core::chrono::{TimeZone, Utc};
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}

fn price(s: &str) -> tantra_core::Price {
    tantra_core::Price::from_rupees(s).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "RELIANCE")
}

fn budget() -> RiskBudget {
    RiskBudget::new(
        Money::inr(dec("100000.00")),
        dec("0.01"),
        Quantity::from_u64(1),
    )
}

/// Base planner inputs shared by the preset goldens.
fn preset_inputs() -> PlannerInputs {
    PlannerInputs {
        subject: PlanSubject::Direct,
        instrument: instrument(),
        side: OrderSide::Buy,
        last_price: price("1000.00"),
        tick_size: price("0.05"),
        atr: Some(dec("10.00")),
        adr_pct: Some(dec("3.00")),
        swing_low: None,
        swing_high: None,
        day_low: None,
        day_high: None,
        ma_values: vec![],
        adv20: None,
        level_map: None,
        timestamp: ts(),
    }
}

/// A valid bullish flag pattern used by the `pattern_breakout` preset golden.
fn bull_pattern() -> ChartPattern {
    ChartPattern::new(
        PatternKind::Flag,
        SignalDirection::Bullish,
        Timeframe::M5,
        PatternStage::FormingArmed,
        vec![PatternPivot {
            bar_ts: ts(),
            price: price("990.00"),
            kind: "high".to_string(),
        }],
        price("1005.00"),
        price("975.00"),
        Some(price("1060.00")),
        80,
        ts(),
        ts(),
        None,
    )
    .unwrap()
}

#[test]
fn preset_parse_roundtrip_and_unknown_id() {
    assert_eq!(
        preset_ids(),
        ["qullamaggie", "ee_plus", "atr_swing", "pattern_breakout"]
    );
    assert_eq!(
        preset("nope").unwrap_err(),
        PresetError::UnknownPreset("nope".to_string())
    );

    for id in preset_ids() {
        let spec = preset(id).unwrap();
        assert_eq!(spec.preset_id, id);
        assert_eq!(spec.version, 1);
        // JSON is the wire form: the spec must round-trip through it.
        let json = serde_json::to_string(&spec).unwrap();
        let back: TradePlanSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    // Grounded preset structure.
    let qm = preset("qullamaggie").unwrap();
    assert_eq!(qm.entry, EntryRule::Market);
    assert_eq!(qm.stop, StopRule::DayExtreme);
    assert_eq!(
        qm.trail.rule,
        TrailRule::CloseMa {
            kind: MaKind::Sma,
            period: 10
        }
    );
    assert_eq!(qm.targets.legs.len(), 1);
    assert!(qm.targets.runner);
    assert_eq!(qm.min_reward_risk, Some(dec("2.0")));
    assert_eq!(
        qm.sizing.method,
        SizingMethod::FixedFractional {
            risk_pct: dec("0.005")
        }
    );

    let ee = preset("ee_plus").unwrap();
    assert_eq!(ee.stop, StopRule::AtrMultiple { mult: dec("2.0") });
    assert_eq!(ee.trail.rule, TrailRule::Atr { mult: dec("3.0") });
    assert_eq!(ee.targets.legs.len(), 2);
    assert_eq!(
        ee.trail.time_stop,
        Some(TimeStop {
            bars: 6,
            min_progress_r: dec("1.0"),
            timeframe: Timeframe::M5
        })
    );

    let swing = preset("atr_swing").unwrap();
    assert!(swing.targets.legs.is_empty());
    assert!(swing.targets.runner);

    let pattern = preset("pattern_breakout").unwrap();
    assert_eq!(
        pattern.entry,
        EntryRule::PatternBreakout { buffer_ticks: 2 }
    );
    assert_eq!(
        pattern.stop,
        StopRule::PatternInvalidation {
            buffer_atr: Some(dec("0.5"))
        }
    );
    assert_eq!(
        pattern.trail.rule,
        TrailRule::Chandelier {
            period: 10,
            mult: dec("3.0")
        }
    );
}

/// Assert one embedded preset's plan is byte-for-byte the committed vector.
fn assert_preset_golden(id: &str, inputs: &PlannerInputs) {
    let json = fs::read_to_string(format!("tests/golden/trade_plan_{id}_golden.json")).unwrap();
    let fixture: Value = serde_json::from_str(&json).unwrap();

    let spec = preset(id).unwrap();
    let plan: TradePlan = TradePlanner::plan(&spec, inputs, &budget()).unwrap();

    let produced = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        produced, fixture,
        "{id} preset plan drifted from its golden vector"
    );

    let loaded: TradePlan = serde_json::from_str(&json).unwrap();
    assert_eq!(loaded, plan);
}

#[test]
fn golden_preset_qullamaggie() {
    let mut inputs = preset_inputs();
    inputs.day_low = Some(price("980.00"));
    inputs.day_high = Some(price("1005.00"));
    inputs.ma_values = vec![(MaKind::Sma, 10, dec("1005.00"))];
    assert_preset_golden("qullamaggie", &inputs);

    // The golden doubles as the grounded contract: market entry, day-extreme
    // stop, SMA10 close trail, ½ @ 2R + runner, 0.5% risk.
    let spec = preset("qullamaggie").unwrap();
    let plan: TradePlan = TradePlanner::plan(&spec, &inputs, &budget()).unwrap();
    assert_eq!(plan.entry, price("1000.00"));
    assert_eq!(plan.initial_stop, price("980.00"));
    assert_eq!(plan.quantity, Quantity::from_u64(25));
    assert_eq!(plan.targets[0].price, price("1040.00"));
    assert_eq!(
        plan.trail.rule,
        TrailRule::CloseMa {
            kind: MaKind::Sma,
            period: 10
        }
    );
}

#[test]
fn golden_preset_ee_plus() {
    assert_preset_golden("ee_plus", &preset_inputs());
}

#[test]
fn golden_preset_atr_swing() {
    assert_preset_golden("atr_swing", &preset_inputs());
}

#[test]
fn golden_preset_pattern_breakout() {
    let mut inputs = preset_inputs();
    inputs.subject = PlanSubject::ChartPattern(bull_pattern());
    inputs.adr_pct = Some(dec("5.00"));
    assert_preset_golden("pattern_breakout", &inputs);
}
