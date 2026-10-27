# Data Contract: `market_prices` (raw equity/ETF daily bars)

This platform's first formal data contract. Covers `data/raw/<symbol>.parquet`
--- one flat file per configured symbol, landed by `marketpulse ingest` via
`src/marketpulse/ingestion/`. See
[ADR-0003](../adr/0003-why-yahoo-finance-for-initial-ingestion.md) for why
Yahoo Finance, and [`architecture.md`](../../architecture.md) §4.3 for how
`raw/` fits the broader medallion layout.

---

## Scope

| | |
|---|---|
| Location | `data/raw/<symbol lower-cased>.parquet` (one file per symbol, e.g. `data/raw/spy.parquet`) |
| Producer | `src/marketpulse/ingestion/` (`yahoo.py` fetches and parses, `storage.py` lands) |
| Cadence | On demand, via `marketpulse ingest` |
| Source | Yahoo Finance chart endpoint, `range=20y`, `interval=1d` |

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `symbol` | `Utf8` | No | Ticker, upper-cased. Part of the business key. |
| `trade_date` | `Date` | No | UTC calendar date of the bar. Part of the business key. |
| `open` | `Float64` | No | Opening price. |
| `high` | `Float64` | No | High price. |
| `low` | `Float64` | No | Low price. |
| `close` | `Float64` | No | Closing price. |
| `volume` | `Int64` | No | Shares traded. |
| `adj_close` | `Float64` | **Yes** | Split/dividend-adjusted close. Yahoo omits this for some symbol/date combinations; the only column this contract allows to be null. |
| `source_system` | `Utf8` | No | Constant `"yahoo_finance"`. Operational metadata, present for Bronze-layer compatibility. |
| `ingestion_timestamp` | `Datetime(UTC)` | No | When this row's fetch ran. One value per file per run, not per trade date --- it answers "when was this landed," which is identical for every row a single run produces. |

## Business key

`(symbol, trade_date)`. Enforced by the duplicate-detection check in
`src/marketpulse/ingestion/validation.py`; a violation raises
`DataQualityError`.

## Data ownership

`src/marketpulse/ingestion/yahoo.py` owns parsing Yahoo's response shape into
this schema. Downstream readers must treat `raw/` as unconformed and
source-shaped --- not deduplicated against prior runs, not cross-checked
against another source --- never as a cleaned table. Conforming and
cross-source validation belongs to the Bronze/Silver transformations of
Phase 2, not to this layer.

## Quality expectations

Enforced by `src/marketpulse/ingestion/validation.py::validate_bars`, run
before every write. All four are **structural** checks (a malformed response
or transport bug), not semantic ones (a plausible but wrong value) --- see
"Known limitations" below for why semantic validation is explicitly out of
scope here.

| Check | Catches | Raises |
|---|---|---|
| Schema | A missing column or wrong dtype | `DataQualityError` with `missing` or `mismatched` context |
| Nulls | A null in any column except `adj_close` | `DataQualityError` with `null_counts` |
| Duplicates | More than one row for the same `(symbol, trade_date)` | `DataQualityError` with `duplicate_rows` |
| Date range | A `trade_date` in the future, before 1990-01-01, or out of order | `DataQualityError` |
| Granularity | A response whose `meta.dataGranularity` does not match the requested `interval` | `IngestionError` with `requested`/`actual` context, raised in `yahoo.py` before parsing |

The granularity check exists because of a confirmed Yahoo behaviour: for a
symbol with decades of history, `range=max` combined with `interval=1d`
silently returns **monthly**-spaced bars with no error of any kind --- the
row data alone looks structurally valid, so none of the four structural
checks above would catch it. The response's own `meta.dataGranularity`
field is the only signal that reveals the mismatch. This is why `range=20y`
is the configured default rather than `"max"`: it was confirmed live to
return genuine daily bars for every symbol in this contract's universe.

A row that fails validation is not written: the symbol's prior file (if any)
is left untouched, and `marketpulse ingest` reports that symbol as `failed`
without affecting the other configured symbols.

## Known limitations

**Each run is a full-refresh overwrite, not an incremental append.** Every
`marketpulse ingest` invocation re-fetches the full configured range and
overwrites the symbol's entire file. This is a deliberate Phase 1 simplification, in real
tension with this platform's own stated principle that `raw/` is immutable
--- "the only record of what a source actually said on a given date"
([ADR-0001](../adr/0001-why-a-financial-market-platform.md)). Equity bars
from Yahoo do not carry FRED's revision problem day-to-day, which is why this
tradeoff was accepted rather than built around; it is flagged here explicitly
as a gap rather than silently assumed away. See "Future recommendations"
below for the mitigation this contract defers.

**No independent verification of adjustment methodology.** `adj_close`
values are used as Yahoo supplies them; this platform does not recompute or
cross-check split/dividend adjustments.

**No authenticated SLA.** See [ADR-0003](../adr/0003-why-yahoo-finance-for-initial-ingestion.md).

## Future partitioning recommendations

Once promoted into a true multi-symbol Bronze dataset, partition by
`symbol` --- matching the existing guidance in
[`docs/architecture/local-analytics.md`](../architecture/local-analytics.md#partitioning),
including its explicit warning against partitioning on a high-cardinality
column such as a per-date key. To resolve the full-refresh-versus-immutability
tension above, consider landing each run under a secondary
`ingestion_date`-stamped path (e.g. `data/raw/_history/<symbol>/<ingestion_date>.parquet`)
so that a true append-only landing history exists alongside the flat
"current" file this contract defines today.

## Future Delta Lake considerations

Per [ADR-0002](../adr/0002-polars-duckdb-for-local-development.md)'s
promotion boundary: once this dataset moves to Databricks, it becomes a Delta
table under Unity Catalog governance, gaining time travel and `MERGE`
semantics that would directly address the full-refresh limitation above. No
local change is required to prepare for that promotion --- the zone name
(`raw`) and column set are already chosen to carry across unchanged.
