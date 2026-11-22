//! Each `ErrorCode` maps to exactly one HTTP status (design 0006 R1c).

use tantra_runtime_api::ErrorCode;

const ALL: &[ErrorCode] = &[
    ErrorCode::ValidationInvalidRequest,
    ErrorCode::Unauthorized,
    ErrorCode::Forbidden,
    ErrorCode::NotFound,
    ErrorCode::Conflict,
    ErrorCode::NotImplemented,
    ErrorCode::Unsupported,
    ErrorCode::InternalError,
    ErrorCode::Timeout,
    ErrorCode::RiskMaxNotionalExceeded,
    ErrorCode::RiskTradingHalted,
    ErrorCode::RiskReduceOnlyViolation,
    ErrorCode::RiskPriceCollar,
    ErrorCode::RiskLotMultipleViolation,
    ErrorCode::RiskTickSizeViolation,
    ErrorCode::RiskInstrumentUnknown,
    ErrorCode::RateLimited,
    ErrorCode::ServiceUnavailable,
    ErrorCode::BadGateway,
];

/// Exhaustive: adding a variant fails to compile until it is listed here and in `ALL`.
fn expected(code: ErrorCode) -> u16 {
    match code {
        ErrorCode::ValidationInvalidRequest
        | ErrorCode::Unsupported
        | ErrorCode::RiskMaxNotionalExceeded
        | ErrorCode::RiskTradingHalted
        | ErrorCode::RiskReduceOnlyViolation
        | ErrorCode::RiskPriceCollar
        | ErrorCode::RiskLotMultipleViolation
        | ErrorCode::RiskTickSizeViolation
        | ErrorCode::RiskInstrumentUnknown => 422,
        ErrorCode::Unauthorized => 401,
        ErrorCode::Forbidden => 403,
        ErrorCode::NotFound => 404,
        ErrorCode::Conflict => 409,
        ErrorCode::RateLimited => 429,
        ErrorCode::InternalError => 500,
        ErrorCode::NotImplemented => 501,
        ErrorCode::BadGateway => 502,
        ErrorCode::ServiceUnavailable => 503,
        ErrorCode::Timeout => 504,
    }
}

#[test]
fn every_code_has_its_status() {
    for c in ALL {
        assert_eq!(c.http_status(), expected(*c), "{c}");
    }
}

#[test]
fn from_status_round_trips_except_many_to_one() {
    for c in ALL {
        let many_to_one = *c == ErrorCode::Unsupported || c.as_str().starts_with("risk_");
        let back = ErrorCode::from_status(c.http_status());
        if many_to_one {
            assert_eq!(back, ErrorCode::ValidationInvalidRequest, "{c}");
        } else {
            assert_eq!(back, *c, "{c}");
        }
    }
}

#[test]
fn forbidden_wire_spelling() {
    assert_eq!(
        serde_json::to_string(&ErrorCode::Forbidden).unwrap(),
        "\"forbidden\""
    );
    assert!(!ErrorCode::Forbidden.retryable());
}
