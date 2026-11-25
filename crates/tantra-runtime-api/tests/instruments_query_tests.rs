//! Contract tests for `InstrumentsQuery` resolution (`q` fuzzy query + `limit`)
//! and pure fuzzy instrument scoring (`fuzzy_score_instrument`).

use std::str::FromStr;

use tantra_core::{Exchange, InstrumentId};
use tantra_runtime_api::{InstrumentsQuery, MAX_INSTRUMENTS_LIMIT, fuzzy_score_instrument};

fn id(exchange: &str, symbol: &str) -> InstrumentId {
    InstrumentId::try_new(Exchange::from_str(exchange).unwrap(), symbol).unwrap()
}

#[test]
fn resolve_accepts_empty_and_valid_queries() {
    let q = InstrumentsQuery::default();
    let resolved = q.resolve().unwrap();
    assert_eq!(resolved.exchange, None);
    assert_eq!(resolved.symbol, None);
    assert_eq!(resolved.q, None);
    assert_eq!(resolved.limit, None);

    let q2 = InstrumentsQuery {
        exchange: Some("NSE".to_string()),
        symbol: None,
        q: Some("  rel  ".to_string()),
        limit: Some(25),
    };
    let resolved2 = q2.resolve().unwrap();
    assert_eq!(resolved2.exchange, Some("NSE"));
    assert_eq!(resolved2.q, Some("rel"));
    assert_eq!(resolved2.limit, Some(25));
}

#[test]
fn resolve_treats_whitespace_only_q_as_none() {
    let q = InstrumentsQuery {
        q: Some("   ".to_string()),
        ..Default::default()
    };
    assert_eq!(q.resolve().unwrap().q, None);
}

#[test]
fn resolve_rejects_out_of_range_limit() {
    for bad in [0, MAX_INSTRUMENTS_LIMIT + 1] {
        let q = InstrumentsQuery {
            limit: Some(bad),
            ..Default::default()
        };
        let err = q.resolve().unwrap_err();
        assert_eq!(err.context.as_ref().unwrap()["field"], "limit");
        assert_eq!(err.context.as_ref().unwrap()["reason"], "out_of_range");
    }
}

#[test]
fn fuzzy_score_ranks_exact_over_prefix_over_substring_over_subsequence() {
    let exact = id("NSE", "REL");
    let prefix = id("NSE", "RELIANCE");
    let substr = id("NSE", "COREL");
    let subseq = id("NSE", "RECLTD");
    let non_match = id("NSE", "INFY");

    let s_exact = fuzzy_score_instrument("rel", &exact).unwrap();
    let s_prefix = fuzzy_score_instrument("rel", &prefix).unwrap();
    let s_substr = fuzzy_score_instrument("rel", &substr).unwrap();
    let s_subseq = fuzzy_score_instrument("rel", &subseq).unwrap();

    assert!(s_exact > s_prefix, "{s_exact} <= {s_prefix}");
    assert!(s_prefix > s_substr, "{s_prefix} <= {s_substr}");
    assert!(s_substr > s_subseq, "{s_substr} <= {s_subseq}");
    assert_eq!(fuzzy_score_instrument("rel", &non_match), None);
}

#[test]
fn fuzzy_score_matches_subsequence_and_multi_token_queries() {
    let reliance = id("NSE", "RELIANCE");
    let tcs_bse = id("BSE", "TCS");
    let tcs_nse = id("NSE", "TCS");

    assert!(fuzzy_score_instrument("rlnc", &reliance).is_some());
    assert!(fuzzy_score_instrument("nse rel", &reliance).is_some());
    assert!(fuzzy_score_instrument("bse tcs", &tcs_bse).is_some());
    assert_eq!(fuzzy_score_instrument("bse tcs", &tcs_nse), None);
}
