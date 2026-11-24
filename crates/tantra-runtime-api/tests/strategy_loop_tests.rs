//! Serialization contract tests for strategy-loop and `OverlaySceneDto` wire types.

use rust_decimal::Decimal;
use tantra_runtime_api::strategy_loop::{
    ChartPointDto, ChartSeriesDto, HedgeLegSideDto, HedgeLevelOverlayDto, HedgeOptionKindDto,
    HintAnnotationDto, HintMetricDto, HintSeverityDto, LevelLineDto, LevelLineStyleDto,
    MarkerKindDto, MarkerSideDto, OverlaySceneDto, SceneLayerMaskDto, SignalMarkerDto, StopKindDto,
    StopLineDto, TradeConnectorRayDto, ZoneBandDto,
};

#[test]
fn overlay_scene_dto_serializes_expected_wire_keys() {
    let mut scene = OverlaySceneDto {
        strategy_id: "elephant_edge_v3".to_string(),
        symbol: "NSE:NIFTYBEES".to_string(),
        timeframe: "1d".to_string(),
        layers: SceneLayerMaskDto::default(),
        signals: vec![SignalMarkerDto {
            price: Decimal::new(26550, 2),
            index: 12,
            side: MarkerSideDto::Buy,
            kind: MarkerKindDto::SweepSaved,
        }],
        connectors: vec![TradeConnectorRayDto {
            from_index: 12,
            from_price: Decimal::new(26550, 2),
            to_index: 17,
            to_price: Decimal::new(27100, 2),
            side: MarkerSideDto::Buy,
            realized_r: Decimal::new(24, 1),
            stopped_out: false,
            label: Some("+2.4R".to_string()),
        }],
        bands: vec![ZoneBandDto {
            low: Decimal::new(26200, 2),
            high: Decimal::new(26450, 2),
            x0: 0,
            x1: 20,
            fill_index: 0,
            label: Some("S1".to_string()),
        }],
        levels: vec![LevelLineDto {
            price: Decimal::new(26600, 2),
            x0: 0,
            x1: 20,
            label: Some("Equilibrium".to_string()),
            style: LevelLineStyleDto::default(),
        }],
        stops: vec![StopLineDto {
            price: Decimal::new(26050, 2),
            x0: 12,
            x1: 17,
            kind: StopKindDto::Initial,
            previous: Some(Decimal::new(26325, 2)),
        }],
        indicators: vec![ChartSeriesDto {
            name: "EMA(20)".to_string(),
            points: vec![ChartPointDto {
                x: Decimal::from(12),
                y: Decimal::new(26500, 2),
            }],
        }],
        hints: vec![HintAnnotationDto {
            bar_index: 12,
            price: Decimal::new(26550, 2),
            severity: HintSeverityDto::Positive,
            title: "Sweep Saved".to_string(),
            detail: "Outer buffer survived".to_string(),
            metrics: vec![HintMetricDto {
                key: "Net R".to_string(),
                value: "+2.40R".to_string(),
            }],
        }],
        hedges: vec![HedgeLevelOverlayDto {
            role: "Short OTM PE".to_string(),
            side: HedgeLegSideDto::Sell,
            option_kind: HedgeOptionKindDto::Put,
            strike: Decimal::from(260),
            x0: 0,
            x1: 20,
            breakeven: Some(Decimal::new(25850, 2)),
            max_loss: Some(Decimal::from(1200)),
            summary: Some("Bull Put Spread".to_string()),
        }],
    };
    scene.layers.hedges = true;
    let json = serde_json::to_value(&scene).expect("scene serializes");
    assert_eq!(json["signals"][0]["kind"], "sweep_saved");
    assert_eq!(json["stops"][0]["kind"], "initial");
    assert_eq!(json["stops"][0]["previous"], "263.25");
    assert_eq!(json["hedges"][0]["option_kind"], "put");

    let decoded: OverlaySceneDto = serde_json::from_value(json).expect("round-trip");
    assert_eq!(decoded, scene);
}
