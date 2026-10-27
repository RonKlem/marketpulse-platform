# ADR-0003: Use Yahoo Finance's chart endpoint for initial historical ingestion

- **Status:** Accepted
- **Date:** 2026-10-27
- **Deciders:** Ron Klem

## Status

Accepted

## Context

Phase 1 needs a historical daily OHLCV source for six symbols (SPY, QQQ, DIA,
AAPL, MSFT, NVDA), reachable without a paid account or an API key, so that a
contributor can clone this repository and run ingestion the same way they
already run everything else in it (`CONTRIBUTING.md`: "no JVM, no Docker, no
cloud account"). ADR-0001 already established the standard this must meet:
reproducibility outranks data quality, because a vendor-gated dataset cannot
be independently inspected.

What a free, no-auth, historical-equity-OHLCV source needs to supply for Phase
1 specifically:

| Requirement | Why |
|---|---|
| No signup or API key | A contributor must be able to run `marketpulse ingest` immediately after cloning |
| No tight rate ceiling | Six symbols, re-fetched repeatedly during development, must not exhaust a daily quota |
| Daily OHLCV with reasonable history depth | The six-symbol demonstration universe needs enough history to exercise validation meaningfully |
| Stable enough to script against | Not necessarily documented, but response shape must hold still long enough for a fixture-based test to stay valid |

The candidates actually available at the time of this decision:

| Source | Signup/key | Free-tier limit | Equity/ETF OHLCV |
|---|---|---|---|
| Yahoo Finance (undocumented chart endpoint) | None | None published; empirically generous for low request volume | Yes |
| Alpha Vantage | API key required | 25 requests/day (or 5/minute on some plans) | Yes |
| Polygon.io | API key required | End-of-day only, 2-year lookback, 5 calls/minute | Yes |
| FRED | API key required | Generous, but no equity/ETF series at all | No (rates, CPI, macro indicators only) |
| IEX Cloud | Previously free | Free developer tier discontinued | Yes (no longer free) |
| Paid vendors (Bloomberg, Databento, etc.) | Commercial contract | N/A | Yes |

## Decision

Fetch historical daily OHLCV bars from Yahoo Finance's undocumented
`https://query1.finance.yahoo.com/v8/finance/chart/{symbol}` JSON endpoint,
using the standard library's `urllib.request` with a browser-like
`User-Agent` header (Yahoo returns HTTP 429 or an empty body to the default
urllib identifier). No authentication. Transient failures (HTTP 429/5xx,
connection errors, timeouts) are retried with bounded exponential backoff;
authentication/authorization and not-found responses (401/403/404) fail
immediately without retry, since no retry count makes those succeed.

The community `yfinance` package, which wraps this same endpoint, is not
used: it depends on Pandas, NumPy, and `requests`, all of which are banned or
absent from this platform's runtime dependency set for the reasons in
ADR-0002 (the wheel installs onto Databricks clusters that pin their own
versions of these libraries).

### The range/granularity finding, discovered by live testing

The initial design requested `range=max` (Yahoo's parameter for "all
available history") with `interval=1d`, on the reasonable assumption that an
explicit `interval` parameter governs bar spacing regardless of how much
history is requested. Live testing against the real endpoint for all six
configured symbols refuted that assumption:

| Symbol | `range` | `meta.dataGranularity` returned | Rows |
|---|---|---|---|
| SPY | `max` | `1mo` | 406 (33 years at monthly spacing) |
| SPY | `20y` | `1d` | 5,030 (genuinely daily) |
| QQQ | `max` | `1mo` | 332 |
| DIA | `max` | `1mo` | 346 |
| AAPL | `max` | `3mo` | 169 |
| MSFT | `max` | `3mo` | 164 |
| NVDA | `max` | `1mo` | 334 |

For a symbol with enough history, Yahoo silently serves coarser bars than
the requested `interval` --- with no error, no warning, and no change in
response shape that would make the row data itself look wrong. The only
observable signal is the response's own `meta.dataGranularity` field, which
names the granularity Yahoo actually used.

This is corrected two ways, not one, because fixing only the symptom would
leave the underlying hazard for the next range value or the next symbol
added to the universe:

1. `IngestionConfig.range` defaults to `"20y"`, confirmed by the measurement
   above to return genuine daily bars for every symbol in the current
   universe --- substantially more history than a Phase 1 demonstration
   needs, while avoiding the silent-downgrade range.
2. `marketpulse.ingestion.yahoo.fetch_chart_payload` reads
   `meta.dataGranularity` from every response and raises `IngestionError` if
   it does not match the requested `interval`, so a future range change, a
   newly added symbol, or a change in Yahoo's own threshold fails loudly
   rather than silently landing mislabeled data.

## Consequences

### Positive

- Zero cost, zero signup, works today for all six symbols, satisfying
  ADR-0001's reproducibility requirement exactly.
- No new runtime dependency: `urllib.request` is standard library, so
  `tests/architecture/test_repository_conventions.py::test_runtime_dependencies_stay_minimal`
  (which pins runtime dependencies to exactly `{polars, duckdb, pyarrow}`)
  remains satisfied.
- No rate ceiling tight enough to interfere with normal development
  iteration against six symbols.

### Negative

- **No documented API contract.** Yahoo can change the response shape, add
  authentication requirements, or block a request pattern without notice.
  This connector's fixture-based tests will catch a shape change; they cannot
  prevent one.
- **No authenticated SLA or guaranteed uptime.** There is no support channel
  if the endpoint degrades or disappears.
- **The browser-like `User-Agent` is a fragile workaround**, not a
  documented requirement. It is observed to be necessary today; it could stop
  being sufficient, or Yahoo could start requiring a session cookie or crumb
  token this connector does not implement, in which case
  `fetch_chart_payload` fails fast with an `IngestionError` rather than
  hanging in a retry loop (401/403 are treated as non-retryable).
- **The adjusted-close methodology is undocumented.** `adjclose` values are
  used as supplied; this platform does not independently verify Yahoo's
  split/dividend adjustment logic.
- **Requested granularity is not guaranteed, confirmed by measurement.**
  `range=max` silently returns monthly or quarterly bars instead of daily
  for a sufficiently long history (see above). The granularity guard turns
  this into a loud failure rather than a silent corruption, but it does not
  make `range=max` usable; `20y` is a measured, not principled, substitute,
  and the exact threshold at which Yahoo downgrades granularity is unknown
  and could shift.

### Neutral

- Vendor-specific response parsing lives entirely in
  `src/marketpulse/ingestion/yahoo.py`; no other module depends on Yahoo's
  response shape, so a future source swap touches one file.

## Alternatives Considered

### Alternative: Alpha Vantage

**Rejected because** its free tier requires an API key (breaking the
zero-setup clone-and-run property) and caps usage low enough (25
requests/day on the standard free tier) that routine development iteration
against even six symbols would be constrained.

### Alternative: Polygon.io

**Rejected because** its free tier is end-of-day only with a 2-year lookback
and a 5-calls/minute ceiling; full historical depth requires a paid plan,
which fails ADR-0001's reproducibility standard.

### Alternative: FRED

**Rejected because** FRED has no equity or ETF OHLCV series at all --- it
publishes macroeconomic and rates data (already planned for a different
purpose elsewhere in this platform, per ADR-0001). It cannot supply this
requirement regardless of its other merits.

### Alternative: IEX Cloud

**Rejected because** its free developer tier, which would otherwise have
been a strong candidate, has been discontinued.

### Alternative: Paid market data vendors (Bloomberg, Databento, etc.)

**Rejected for the same reason ADR-0001 already rejected paid vendors
generally:** no third party can clone and run this repository against data
gated behind a commercial contract, which fails the reproducibility standard
this platform is held to.

## Revisit When

- **Yahoo blocks the request pattern persistently** --- 403/429 responses
  that survive the configured retry budget routinely, rather than
  occasionally --- indicating the endpoint can no longer be relied on for
  development or CI. At that point, substitute a different free source or
  adopt an API-key-based one with documented setup instructions.
- **The connector's fixture-based tests fail from a genuine response-shape
  change,** not a test bug. An isolated occurrence is a parser fix; a
  recurring pattern indicates the source itself has become unstable enough to
  warrant reconsidering it.
- **Intraday or tick-level data is needed.** Out of scope for this ADR --- a
  Phase 4 streaming-source decision, not a revision of this one.
- **A production SLA or support obligation emerges** for this dataset, which
  an unauthenticated, undocumented endpoint cannot provide. Revisit with a
  paid vendor at that point, accepting the reproducibility tradeoff
  deliberately rather than by default.
- **The granularity guard starts rejecting `range=20y` for a currently-passing
  symbol,** or a newly added symbol fails it immediately. That is a signal
  Yahoo's silent-downgrade threshold has moved; re-measure across the
  configured universe (the table above) and adjust `range` accordingly rather
  than loosening or removing the guard.

## References

- [ADR-0001](0001-why-a-financial-market-platform.md) --- the reproducibility
  requirement this decision satisfies, and its treatment of paid vendors
- [ADR-0002](0002-polars-duckdb-for-local-development.md) --- the dependency
  minimalism this decision's rejection of `yfinance` is consistent with
- [`project_vision.md`](../../project_vision.md) --- Yahoo Finance named
  among the platform's planned data sources
- [`docs/data-contracts/market_prices.md`](../data-contracts/market_prices.md)
  --- the schema landed by this connector
