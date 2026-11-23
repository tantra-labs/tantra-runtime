//! Handler for `GET /api/v2/options/chain` (gap 0016).
//!
//! Serves live multi-expiry option-chain snapshots by joining [`InstrumentChainReader`]
//! (contracts, top-of-book bid/ask quotes, and optional [`OpenInterestStore`] snapshots)
//! with `tantra-market-pricing::BlackScholesModel` for implied volatility & Greeks.
//! When no live contracts exist for an underlying, falls back deterministically to a
//! Black-Scholes strike ladder over the latest spot price with `bid`/`ask` strictly `null`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use chrono::{DateTime, Datelike, Days, Duration, NaiveDate, Utc};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use tantra_core::{Exchange, InstrumentId, OptionType, Price};
use tantra_market_pricing::{BlackScholesModel, PriceParams, PricingModel};
use tantra_market_store::{ChainContract, ChainReader as _, InstrumentChainReader};
use tantra_runtime_api::options_chain::{
    OptionChainRowDto, OptionChainSource, OptionExpiryDto, OptionLegSnapshotDto, OptionsChainQuery,
    OptionsChainResponse, build_tuesday_expiry_schedule, compute_atm_strike,
    compute_max_pain_strike, compute_pcr, format_contract_symbol, format_expiry_entry,
    iv_regime_label, strike_step_for,
};

use crate::AppState;
use crate::experimental_analysis::read_live_spot;
use crate::market::{ApiQuery, instrument_not_found, port_failure, success, unprocessable};
use crate::market_feed::ports::{BarRequest, TradeReader as _};
use crate::market_feed::wire::{
    Bar, BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};

const DEFAULT_HALF_STRIKE_STEPS: i32 = 14;
const RISK_FREE_RATE: Decimal = Decimal::from_parts(65, 0, 0, false, 3); // 0.065

fn to_dec(val: f64) -> Decimal {
    Decimal::from_f64_retain(val).unwrap_or(Decimal::ZERO)
}

fn default_lot_size(fno_root: &str) -> Decimal {
    match fno_root {
        "NIFTY" => Decimal::from(75),
        "BANKNIFTY" => Decimal::from(30),
        "FINNIFTY" => Decimal::from(65),
        "MIDCPNIFTY" => Decimal::from(50),
        _ => Decimal::from(1),
    }
}

fn build_underlying_candidates(canonical: &InstrumentId, fno_root: &str) -> Vec<InstrumentId> {
    let mut cands = vec![canonical.clone()];
    let sym = canonical.symbol.as_str();
    if !sym.ends_with("-EQ") && !sym.ends_with("-INDEX") && !sym.ends_with("-FUT") {
        cands.push(InstrumentId::new(canonical.exchange, format!("{sym}-EQ")));
        cands.push(InstrumentId::new(
            canonical.exchange,
            format!("{sym}-INDEX"),
        ));
    }
    if canonical.symbol != fno_root {
        cands.push(InstrumentId::new(canonical.exchange, fno_root));
        cands.push(InstrumentId::new(
            canonical.exchange,
            format!("{fno_root}-EQ"),
        ));
        cands.push(InstrumentId::new(
            canonical.exchange,
            format!("{fno_root}-INDEX"),
        ));
    }
    cands
}

async fn resolve_underlying_instrument(
    state: &AppState,
    canonical: &InstrumentId,
    fno_root: &str,
) -> Result<Option<Instrument>, crate::market_feed::ports::PortError> {
    for cand in build_underlying_candidates(canonical, fno_root) {
        if let Some(inst) = state.instruments.get_instrument(&cand).await? {
            return Ok(Some(inst));
        }
    }
    // Fall back to case-insensitive symbol/root scan across listed instruments.
    let all = state.instruments.list_instruments().await?;
    Ok(all.into_iter().find(|inst| {
        inst.kind() != tantra_core::InstrumentKind::Option
            && (inst.id().symbol.eq_ignore_ascii_case(&canonical.symbol)
                || inst.id().symbol.eq_ignore_ascii_case(fno_root)
                || inst
                    .id()
                    .symbol
                    .eq_ignore_ascii_case(&format!("{fno_root}-EQ"))
                || inst
                    .id()
                    .symbol
                    .eq_ignore_ascii_case(&format!("{fno_root}-INDEX")))
    }))
}

async fn load_underlying_bars(
    state: &AppState,
    candidates: &[InstrumentId],
    as_of: NaiveDate,
) -> Result<Vec<Bar>, crate::market_feed::ports::PortError> {
    let from_utc: DateTime<Utc> = (as_of - Duration::days(120))
        .and_hms_opt(0, 0, 0)
        .unwrap_or_default()
        .and_utc();
    let to_utc: DateTime<Utc> = (as_of + Duration::days(2))
        .and_hms_opt(0, 0, 0)
        .unwrap_or_default()
        .and_utc();

    for spec in [
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    ] {
        for cand in candidates {
            let Ok(req) = BarRequest::new(
                cand.clone(),
                spec,
                Some(UnixNanos::from_datetime(from_utc)),
                Some(UnixNanos::from_datetime(to_utc)),
            ) else {
                continue;
            };
            match state.bars.read_bars(&req).await {
                Ok(bars) if !bars.is_empty() => return Ok(bars),
                Ok(_) => {}
                Err(crate::market_feed::ports::PortError::Unsupported(_)) => {}
                Err(err) => return Err(err),
            }
        }
    }
    Ok(Vec::new())
}

fn estimate_realized_vol_pct(bars: &[Bar]) -> (Decimal, Decimal) {
    if bars.len() < 3 {
        return (Decimal::new(1400, 2), Decimal::new(5000, 2));
    }
    let closes: Vec<f64> = bars
        .iter()
        .map(Bar::close)
        .filter(|c| c.is_finite() && *c > 0.0)
        .collect();
    if closes.len() < 3 {
        return (Decimal::new(1400, 2), Decimal::new(5000, 2));
    }
    let returns: Vec<f64> = closes
        .windows(2)
        .map(|w| (w[1] / w[0]).ln())
        .filter(|r| r.is_finite())
        .collect();
    if returns.len() < 2 {
        return (Decimal::new(1400, 2), Decimal::new(5000, 2));
    }

    let mean = returns.iter().sum::<f64>() / (returns.len() as f64);
    let var =
        returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / ((returns.len() - 1) as f64);
    let ann_vol_pct = (var.sqrt() * 252_f64.sqrt() * 100.0).clamp(6.0, 85.0);

    // Rolling 20-bar RV percentile over available history.
    let mut rolling_vols = Vec::new();
    if returns.len() >= 10 {
        let win = returns.len().min(20);
        for slice in returns.windows(win) {
            let m = slice.iter().sum::<f64>() / (slice.len() as f64);
            let v = slice.iter().map(|r| (r - m).powi(2)).sum::<f64>() / ((slice.len() - 1) as f64);
            rolling_vols.push(v.sqrt() * 252_f64.sqrt() * 100.0);
        }
    }
    let percentile = if rolling_vols.len() >= 2 {
        let below = rolling_vols.iter().filter(|v| **v <= ann_vol_pct).count();
        ((below as f64) / (rolling_vols.len() as f64) * 100.0).clamp(0.0, 100.0)
    } else {
        50.0
    };

    let iv = Decimal::from_f64(ann_vol_pct)
        .unwrap_or_else(|| Decimal::new(1400, 2))
        .round_dp(2);
    let pct = Decimal::from_f64(percentile)
        .unwrap_or_else(|| Decimal::new(5000, 2))
        .round_dp(2);
    (iv, pct)
}

fn bs_greeks_for_leg(
    spot: Decimal,
    strike: Decimal,
    dte: u32,
    fallback_iv_pct: Decimal,
    option_type: OptionType,
    observed_ltp: Option<Decimal>,
) -> (Decimal, Decimal, Decimal, Decimal, Decimal, Decimal) {
    let model = BlackScholesModel;
    let time_to_expiry = Decimal::from(dte.max(1)) / Decimal::from(365);
    let fallback_vol = (fallback_iv_pct / Decimal::from(100)).max(Decimal::new(4, 2));

    let base_params = PriceParams {
        spot: Price::new(spot),
        strike: Price::new(strike),
        time_to_expiry,
        risk_free_rate: RISK_FREE_RATE,
        dividend_yield: Decimal::ZERO,
        volatility: fallback_vol,
        option_type,
    };

    let (eff_vol, eff_ltp) = match observed_ltp {
        Some(target) if target > Decimal::ZERO => {
            let solved = model
                .implied_volatility(&base_params, target)
                .ok()
                .filter(|v| *v > Decimal::ZERO)
                .unwrap_or(fallback_vol);
            (solved, target)
        }
        _ => {
            let fair = model
                .price(&base_params)
                .unwrap_or_else(|_| Decimal::new(5, 2))
                .max(Decimal::new(5, 2));
            (fallback_vol, fair)
        }
    };

    let greek_params = PriceParams {
        volatility: eff_vol,
        ..base_params
    };
    let (delta, gamma, theta, vega) = match model.greeks(&greek_params) {
        Ok(g) => (
            g.delta.round_dp(3),
            g.gamma.round_dp(5),
            (g.theta / Decimal::from(365)).round_dp(2),
            (g.vega / Decimal::from(100)).round_dp(2),
        ),
        Err(_) => (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
    };

    (
        eff_ltp.round_dp(2),
        (eff_vol * Decimal::from(100)).round_dp(2),
        delta,
        gamma,
        theta,
        vega,
    )
}

fn skew_iv_pct(atm_iv_pct: Decimal, strike: Decimal, spot: Decimal, step: Decimal) -> Decimal {
    let safe_step = if step > Decimal::ZERO {
        step
    } else {
        Decimal::ONE
    };
    let offset = (strike - spot) / safe_step;
    let abs_off = offset.abs();
    let put_skew = if offset < Decimal::ZERO {
        abs_off * Decimal::new(18, 2)
    } else {
        Decimal::ZERO
    };
    (atm_iv_pct + abs_off * Decimal::new(22, 2) + put_skew)
        .max(Decimal::new(600, 2))
        .round_dp(2)
}

async fn resolve_oi_with_change(
    state: &AppState,
    contract_id: &InstrumentId,
    direct_oi: Option<Decimal>,
    as_of: NaiveDate,
) -> (Decimal, Decimal) {
    let Some(store) = state.oi_store() else {
        return (direct_oi.unwrap_or(Decimal::ZERO), Decimal::ZERO);
    };
    let from = as_of - Duration::days(14);
    match store.history(contract_id, from, Some(as_of)).await {
        Ok(snaps) if !snaps.is_empty() => {
            let latest = snaps
                .last()
                .map(|s| s.open_interest.value())
                .or(direct_oi)
                .unwrap_or(Decimal::ZERO);
            let change_pct = if snaps.len() >= 2 {
                let prev = snaps[snaps.len() - 2].open_interest.value();
                if prev > Decimal::ZERO {
                    (((latest - prev) / prev) * Decimal::from(100)).round_dp(2)
                } else {
                    Decimal::ZERO
                }
            } else {
                Decimal::ZERO
            };
            (latest, change_pct)
        }
        _ => (direct_oi.unwrap_or(Decimal::ZERO), Decimal::ZERO),
    }
}

async fn build_live_leg(
    state: &AppState,
    contract: &ChainContract,
    spot: Decimal,
    dte: u32,
    fallback_iv_pct: Decimal,
    as_of: NaiveDate,
) -> OptionLegSnapshotDto {
    let bid = contract.bid.map(|p| p.value().round_dp(2));
    let ask = contract.ask.map(|p| p.value().round_dp(2));

    let mut observed_trade_ltp = contract.last_price.map(|p| p.value());
    let mut volume = Decimal::ZERO;
    let mut ltp_change_pct = Decimal::ZERO;

    if let Some(live) = state.live_quotes() {
        if let Ok(Some(trade)) = live.read_trade_snapshot(&contract.instrument).await {
            let last = to_dec(trade.last.price());
            if last > Decimal::ZERO {
                observed_trade_ltp = Some(last);
            }
            let day_vol = to_dec(trade.day_volume);
            if day_vol > Decimal::ZERO {
                volume = day_vol.round_dp(0);
            }
        }
        if let Some(ltp_val) = observed_trade_ltp
            && let Ok(Some(prev)) = live.read_fallback(&contract.instrument).await
        {
            let prev_px = to_dec(prev.bid_price());
            if prev_px > Decimal::ZERO {
                ltp_change_pct = (((ltp_val - prev_px) / prev_px) * Decimal::from(100)).round_dp(2);
            }
        }
    }

    let quote_mid = match (bid, ask) {
        (Some(b), Some(a)) if b > Decimal::ZERO && a > Decimal::ZERO => {
            Some((b + a) / Decimal::from(2))
        }
        (Some(b), _) if b > Decimal::ZERO => Some(b),
        (_, Some(a)) if a > Decimal::ZERO => Some(a),
        _ => None,
    };

    let observed_ltp = observed_trade_ltp.or(quote_mid);
    let live_quoted = observed_trade_ltp.is_some() || bid.is_some() || ask.is_some();

    let (ltp, iv_pct, delta, gamma, theta, vega) = bs_greeks_for_leg(
        spot,
        contract.strike.value(),
        dte,
        fallback_iv_pct,
        contract.option_type,
        observed_ltp,
    );

    let (oi, oi_change_pct) = resolve_oi_with_change(
        state,
        &contract.instrument,
        contract.open_interest.map(|q| q.value()),
        as_of,
    )
    .await;

    OptionLegSnapshotDto {
        contract_symbol: contract.instrument.to_string(),
        ltp,
        ltp_change_pct,
        bid,
        ask,
        oi,
        oi_change_pct,
        volume,
        iv_pct,
        delta,
        gamma,
        theta,
        vega,
        live_quoted,
    }
}

fn build_theoretical_leg(
    fno_root: &str,
    expiry: NaiveDate,
    strike: Decimal,
    spot: Decimal,
    dte: u32,
    leg_iv_pct: Decimal,
    option_type: OptionType,
) -> OptionLegSnapshotDto {
    let is_call = option_type == OptionType::Call;
    let (ltp, iv_pct, delta, gamma, theta, vega) =
        bs_greeks_for_leg(spot, strike, dte, leg_iv_pct, option_type, None);

    OptionLegSnapshotDto {
        contract_symbol: format_contract_symbol(fno_root, expiry, strike, is_call),
        ltp,
        ltp_change_pct: Decimal::ZERO,
        bid: None,
        ask: None,
        oi: Decimal::ZERO,
        oi_change_pct: Decimal::ZERO,
        volume: Decimal::ZERO,
        iv_pct,
        delta,
        gamma,
        theta,
        vega,
        live_quoted: false,
    }
}

/// `GET /api/v2/options/chain`: multi-expiry Call/Put chain with Greeks, IV, OI, PCR, and Max Pain.
pub(crate) async fn get_options_chain(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<OptionsChainQuery>,
) -> Response {
    let resolved = match query.resolve() {
        Ok(r) => r,
        Err(detail) => return unprocessable(detail),
    };
    let as_of = resolved.as_of.unwrap_or_else(|| Utc::now().date_naive());

    let underlying_meta =
        match resolve_underlying_instrument(&state, &resolved.instrument_id, &resolved.fno_root)
            .await
        {
            Ok(inst) => inst,
            Err(err) => return port_failure(err),
        };

    let canonical_id = underlying_meta
        .as_ref()
        .map(|inst| inst.id().clone())
        .unwrap_or_else(|| resolved.instrument_id.clone());

    let bar_candidates = build_underlying_candidates(&canonical_id, &resolved.fno_root);
    let bars = match load_underlying_bars(&state, &bar_candidates, as_of).await {
        Ok(b) => b,
        Err(err) => return port_failure(err),
    };

    // Query InstrumentChainReader across both canonical id and NFO root (since contracts store `underlying = "NIFTY"`).
    let mut chain_reader =
        InstrumentChainReader::new(state.instruments.clone(), state.quotes.clone());
    if let Some(oi_store) = state.oi_store() {
        chain_reader = chain_reader.with_oi_store(oi_store.clone());
    }

    let mut probe_ids = vec![
        canonical_id.clone(),
        InstrumentId::new(Exchange::Nfo, &resolved.fno_root),
    ];
    if canonical_id.symbol != resolved.fno_root {
        probe_ids.push(InstrumentId::new(canonical_id.exchange, &resolved.fno_root));
    }

    let mut contracts: Vec<ChainContract> = Vec::new();
    let mut seen_contracts = BTreeSet::new();
    for probe in &probe_ids {
        match chain_reader.options_chain(probe, None, Some(1_000)).await {
            Ok(found) => {
                for c in found {
                    let key = (
                        c.instrument.clone(),
                        c.expiry,
                        c.strike.value(),
                        c.option_type == OptionType::Call,
                    );
                    if seen_contracts.insert(key) {
                        contracts.push(c);
                    }
                }
            }
            Err(crate::market_feed::ports::PortError::Unsupported(_)) => {}
            Err(err) => return port_failure(err),
        }
    }

    // Determine spot price & session delta without fabricating values.
    let mut spot_price: Option<Decimal> = None;
    for cand in &bar_candidates {
        if let Some(s) = read_live_spot(&state, cand).await {
            spot_price = Some(s);
            break;
        }
    }
    if spot_price.is_none()
        && let Some(last_bar) = bars.last()
    {
        let c = to_dec(last_bar.close()).round_dp(2);
        if c > Decimal::ZERO {
            spot_price = Some(c);
        }
    }

    // If we have neither an underlying record, bars, live quote, nor any option contracts -> 404.
    if underlying_meta.is_none() && spot_price.is_none() && contracts.is_empty() {
        return instrument_not_found(&resolved.instrument_id);
    }

    // If contracts exist without a spot bar/quote, anchor spot to the median listed strike so
    // Black-Scholes never runs on a zero/invalid spot.
    let spot_price = spot_price.unwrap_or_else(|| {
        if !contracts.is_empty() {
            let mut strikes: Vec<Decimal> = contracts.iter().map(|c| c.strike.value()).collect();
            strikes.sort();
            strikes[strikes.len() / 2]
        } else {
            Decimal::from(1000)
        }
    });

    let mut prev_close: Option<Decimal> = None;
    if let Some(live) = state.live_quotes() {
        for cand in &bar_candidates {
            if let Ok(Some(fb)) = live.read_fallback(cand).await {
                let p = to_dec(fb.bid_price()).round_dp(2);
                if p > Decimal::ZERO && p != spot_price {
                    prev_close = Some(p);
                    break;
                }
            }
        }
    }
    if prev_close.is_none() && bars.len() >= 2 {
        let p = to_dec(bars[bars.len() - 2].close()).round_dp(2);
        if p > Decimal::ZERO {
            prev_close = Some(p);
        }
    }

    let (spot_change, spot_change_pct) = match prev_close {
        Some(prev) if prev > Decimal::ZERO => {
            let chg = (spot_price - prev).round_dp(2);
            let pct = ((chg / prev) * Decimal::from(100)).round_dp(2);
            (chg, pct)
        }
        _ => (Decimal::ZERO, Decimal::ZERO),
    };

    let (atm_iv_pct, iv_percentile) = estimate_realized_vol_pct(&bars);
    let iv_percentile_regime = iv_regime_label(iv_percentile);

    let currency = underlying_meta
        .as_ref()
        .map(|inst| inst.currency().to_string())
        .unwrap_or_else(|| "INR".to_string());

    let meta_lot_size = underlying_meta
        .as_ref()
        .map(|inst| to_dec(inst.lot_size()).round_dp(0))
        .filter(|l| *l > Decimal::ONE);
    let contract_lot_size = contracts
        .iter()
        .map(|c| c.lot_size.value().round_dp(0))
        .find(|l| *l > Decimal::ZERO);
    let lot_size = contract_lot_size
        .or(meta_lot_size)
        .unwrap_or_else(|| default_lot_size(&resolved.fno_root));

    let step = strike_step_for(&resolved.fno_root, spot_price);

    let (expiries, active_expiry_date, rows, source, live_contract_count, availability_note) =
        if !contracts.is_empty() {
            let expiry_dates: BTreeSet<NaiveDate> = contracts.iter().map(|c| c.expiry).collect();
            let expiries: Vec<OptionExpiryDto> = expiry_dates
                .iter()
                .map(|dt| {
                    let is_monthly = dt
                        .checked_add_days(Days::new(7))
                        .is_some_and(|next| next.month() != dt.month());
                    format_expiry_entry(*dt, as_of, !is_monthly)
                })
                .collect();
            let active_expiry = resolved
                .expiry
                .or_else(|| expiry_dates.iter().copied().next())
                .unwrap_or(as_of);
            let dte = u32::try_from(active_expiry.signed_duration_since(as_of).num_days().max(1))
                .unwrap_or(1);

            let filtered: Vec<&ChainContract> = contracts
                .iter()
                .filter(|c| c.expiry == active_expiry)
                .collect();

            let mut grouped: BTreeMap<Decimal, (Option<&ChainContract>, Option<&ChainContract>)> =
                BTreeMap::new();
            for c in &filtered {
                let entry = grouped.entry(c.strike.value()).or_insert((None, None));
                match c.option_type {
                    OptionType::Call => entry.0 = Some(*c),
                    OptionType::Put => entry.1 = Some(*c),
                }
            }

            let mut built_rows = Vec::with_capacity(grouped.len());
            let mut any_live_quote_or_oi = false;
            let mut quoted_leg_count = 0usize;

            for (strike, (call_opt, put_opt)) in grouped {
                let fallback_iv = skew_iv_pct(atm_iv_pct, strike, spot_price, step);
                let call = match call_opt {
                    Some(c) => {
                        let leg =
                            build_live_leg(&state, c, spot_price, dte, fallback_iv, as_of).await;
                        if leg.live_quoted {
                            any_live_quote_or_oi = true;
                            quoted_leg_count += 1;
                        }
                        if c.open_interest.is_some() || leg.oi > Decimal::ZERO {
                            any_live_quote_or_oi = true;
                        }
                        leg
                    }
                    None => build_theoretical_leg(
                        &resolved.fno_root,
                        active_expiry,
                        strike,
                        spot_price,
                        dte,
                        fallback_iv,
                        OptionType::Call,
                    ),
                };
                let put = match put_opt {
                    Some(p) => {
                        let leg =
                            build_live_leg(&state, p, spot_price, dte, fallback_iv, as_of).await;
                        if leg.live_quoted {
                            any_live_quote_or_oi = true;
                            quoted_leg_count += 1;
                        }
                        if p.open_interest.is_some() || leg.oi > Decimal::ZERO {
                            any_live_quote_or_oi = true;
                        }
                        leg
                    }
                    None => build_theoretical_leg(
                        &resolved.fno_root,
                        active_expiry,
                        strike,
                        spot_price,
                        dte,
                        fallback_iv,
                        OptionType::Put,
                    ),
                };

                built_rows.push(OptionChainRowDto {
                    strike,
                    is_atm: false,
                    call_itm: strike < spot_price,
                    put_itm: strike > spot_price,
                    call,
                    put,
                });
            }

            let source = if any_live_quote_or_oi {
                OptionChainSource::LiveChain
            } else {
                OptionChainSource::TheoreticalBs
            };
            let note = if quoted_leg_count > 0 {
                format!(
                    "Live chain from InstrumentChainReader ({} contracts, {} quoted legs for {}); BS Greeks solved from observed prices.",
                    filtered.len(),
                    quoted_leg_count,
                    active_expiry.format("%Y-%m-%d")
                )
            } else {
                format!(
                    "Contracts indexed for {} ({} contracts) without live bid/ask quotes; theoretical BS pricing shown and bid/ask kept null.",
                    active_expiry.format("%Y-%m-%d"),
                    filtered.len()
                )
            };
            (
                expiries,
                active_expiry,
                built_rows,
                source,
                filtered.len(),
                note,
            )
        } else {
            let expiries = build_tuesday_expiry_schedule(as_of);
            let active_expiry = resolved
                .expiry
                .or_else(|| {
                    expiries
                        .first()
                        .and_then(|e| NaiveDate::parse_from_str(&e.expiry, "%Y-%m-%d").ok())
                })
                .unwrap_or(as_of);
            let dte = u32::try_from(active_expiry.signed_duration_since(as_of).num_days().max(1))
                .unwrap_or(1);

            let safe_step = if step > Decimal::ZERO {
                step
            } else {
                Decimal::ONE
            };
            let center = (spot_price / safe_step)
                .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
                * safe_step;

            let mut built_rows = Vec::new();
            for i in -DEFAULT_HALF_STRIKE_STEPS..=DEFAULT_HALF_STRIKE_STEPS {
                let strike = center + safe_step * Decimal::from(i);
                if strike <= Decimal::ZERO {
                    continue;
                }
                let leg_iv = skew_iv_pct(atm_iv_pct, strike, spot_price, safe_step);
                let call = build_theoretical_leg(
                    &resolved.fno_root,
                    active_expiry,
                    strike,
                    spot_price,
                    dte,
                    leg_iv,
                    OptionType::Call,
                );
                let put = build_theoretical_leg(
                    &resolved.fno_root,
                    active_expiry,
                    strike,
                    spot_price,
                    dte,
                    leg_iv,
                    OptionType::Put,
                );
                built_rows.push(OptionChainRowDto {
                    strike,
                    is_atm: false,
                    call_itm: strike < spot_price,
                    put_itm: strike > spot_price,
                    call,
                    put,
                });
            }

            (
                expiries,
                active_expiry,
                built_rows,
                OptionChainSource::TheoreticalBs,
                0usize,
                "Live option feed offline (no contracts in InstrumentChainReader); theoretical Black-Scholes fair values shown with null bid/ask.".to_string(),
            )
        };

    let atm_strike = compute_atm_strike(&rows, spot_price);
    let mut rows = rows;
    for r in &mut rows {
        r.is_atm = r.strike == atm_strike;
    }

    let pcr = compute_pcr(&rows);
    let max_pain_strike = compute_max_pain_strike(&rows);

    success(OptionsChainResponse {
        instrument_id: canonical_id.to_string(),
        currency,
        spot_price: spot_price.round_dp(2),
        spot_change,
        spot_change_pct,
        lot_size,
        expiries,
        active_expiry: active_expiry_date.format("%Y-%m-%d").to_string(),
        pcr,
        max_pain_strike,
        atm_strike,
        atm_iv_pct,
        iv_percentile,
        iv_percentile_regime,
        source,
        live_contract_count,
        availability_note,
        rows,
    })
}
