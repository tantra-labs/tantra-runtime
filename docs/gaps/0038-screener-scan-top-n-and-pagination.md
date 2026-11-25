# 0038 — Screener scan has no top-N or pagination

**Status**: open

## What's stubbed or hardcoded

`GET /api/v2/screener/scans` accepts a universe of up to `MAX_SCREENER_UNIVERSE` (1000)
instruments but answers a 422 `too_many_rows` once more than `MAX_SCREENER_ROWS` (500) match
(`tantra-runtime-api/src/screener.rs`, `tantra-runtime-api-rest/src/screener.rs`). There is no
`limit`/sort or cursor, so an unfiltered or loose scan over a large universe can never succeed.

The `tantra-ui-web` screener works around it by sending only the first 500 catalog instruments
(`src/lib/tantra-screener-client.ts`, `MAX_SCREENER_UNIVERSE = 500`): the local catalog holds
~3,768 NSE equities, so most of the market is never screened.

## Why

Before the workaround, every unfiltered screener load (1000-instrument universe) was a 422,
and the web client fell back to its offline sample rows (the "Live → Sample" badge flip).

## How to close it

- Add a bounded `limit` (≤ `MAX_SCREENER_ROWS`) plus a deterministic sort key, or a cursor
  over instrument-id order, to `ScreenerQuery`; return the first page instead of a 422 when the
  match count exceeds the page size, and report `truncated`/`next_cursor` in `ScreenerResponse`.
- Let the universe be selected server-side (index/classification) so clients need not ship
  1000 ids in a query string.
- Then raise or remove the web client's 500 cap and drop its citation of this gap.
