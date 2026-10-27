# Local Analytics Architecture

How the local execution layer works, why it is two engines, and how to choose
between them. The decision to build on Polars and DuckDB rather than Spark is
recorded in [ADR-0002](../adr/0002-polars-duckdb-for-local-development.md); this
document covers the mechanics.

All measurements here were taken on the reference machine — Intel Core Ultra 7
265H (6 P-cores / 10 E-cores, 16 threads), 64 GB RAM, Windows 11, Python 3.14.5,
Polars 1.44.2, DuckDB 1.5.6 — against a 20,160,000-row OHLCV dataset
(4,000 symbols x 20 years of daily bars).

---

## The dataset this layer is sized for

| Property | Value |
|---|---|
| Rows | 20,160,000 |
| Columns | 4 (symbol, trade_date, close, volume) |
| In-memory, Arrow | 456 MB |
| On disk, Parquet + zstd | 30.1 MB |
| On disk, Parquet + snappy | 102.7 MB |
| Grouped aggregation (4,000 groups) | 60 ms |

456 MB is 0.7% of this machine's RAM. That ratio is the premise of the entire
local layer: there is no distributed-systems problem here to solve.

---

## Component model

```text
 ┌─────────────────────────────────────────────────────────────────┐
 │ marketpulse.config                                             │
 │  Layered TOML + env vars → frozen, validated PlatformConfig    │
 │  Resolved once, at startup, before any resource is acquired    │
 └────────────────────────────┬────────────────────────────────────┘
                              │ PlatformConfig
                              ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │ marketpulse.analytics.AnalyticsSession                         │
 │                                                                 │
 │   ┌──────────────────┐    Arrow buffers    ┌────────────────┐  │
 │   │ Polars           │◄───  zero-copy  ───►│ DuckDB         │  │
 │   │                  │                     │                │  │
 │   │ • scan_parquet   │                     │ • SQL optimiser│  │
 │   │ • LazyFrame      │   register(): 0 MB  │ • joins        │  │
 │   │ • expressions    │   .pl():  Arrow out │ • aggregations │  │
 │   │ • window fns     │                     │ • Spark-like   │  │
 │   └────────┬─────────┘                     └───────┬────────┘  │
 │            │                                       │           │
 └────────────┼───────────────────────────────────────┼───────────┘
              │                                       │
              ▼                                       ▼
   ┌─────────────────────────────────────────────────────────────┐
   │  Local lakehouse — Hive-partitioned Parquet (zstd)          │
   │  data/{raw,bronze,silver,gold}/<dataset>/<partition>/*.parquet│
   └─────────────────────────────────────────────────────────────┘
```

---

## The Arrow boundary

This is the mechanism that makes a two-engine local stack cost nothing, and the
reason the architecture is not simply "pick one."

Polars and DuckDB both represent data in memory as Apache Arrow: contiguous,
typed, columnar buffers with a documented layout. DuckDB's **replacement scan**
resolves an unqualified identifier in a SQL query against Python objects in
scope. For an Arrow-backed object it reads those buffers in place rather than
ingesting a copy.

### Measured, not assumed

On the 456 MB frame above:

| Operation | Wall time | DuckDB buffer-pool memory |
|---|---|---|
| `session.register("bars", frame)` | 166 ms | **0 MB** |
| `CREATE TABLE copied AS SELECT * FROM bars` | 184 ms | **1,130 MB** |
| `SELECT symbol, avg(close) ... GROUP BY symbol` over registered frame | **58 ms** | — |
| Same query over the DuckDB-native table | 112 ms | — |

Two things to take from this.

**Registration is genuinely free.** Exposing 456 MB of Polars data to the SQL
engine adds nothing to DuckDB's buffer pool. Materialising the same data as a
native table costs 1,130 MB — more than twice the Arrow footprint, because
DuckDB builds its own row-group structures and compression state.

**The registered frame is faster to query than the native table** (58 ms vs
112 ms) in this case. The native table pays for its own buffer management on
each scan; the registered frame is read directly from the allocation Polars
already holds. This is counter to the usual expectation that a native table
should be faster, and it is the strongest argument against copying data between
the engines.

### Reproduce it

```python
import duckdb, polars as pl


def buffer_pool_mb(con):
    return (con.sql("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0] or 0) / 1e6


frame = pl.read_parquet("data/bronze/equity_daily_bars/**/*.parquet")
con = duckdb.connect()

con.register("bars", frame)
print(buffer_pool_mb(con))  # 0.0

con.execute("CREATE TABLE copied AS SELECT * FROM bars")
print(buffer_pool_mb(con))  # ~1130.0
```

### What breaks it

The zero-copy property is a consequence of both sides speaking Arrow. It is lost
the moment data passes through something that does not:

```python
# Breaks zero-copy — two full copies and a dtype round-trip.
con.register("bars", frame.to_pandas())

# Zero-copy.
con.register("bars", frame)
```

This is why `pandas` and `numpy` are banned from `src/` by Ruff's `banned-api`
rule and by an architecture test. The ban is about preserving a measured
property, not about taste.

---

## Laziness and pushdown

`AnalyticsSession.scan()` returns a `pl.LazyFrame`, never an eager
`pl.DataFrame`. The distinction is not stylistic.

A `LazyFrame` builds a query plan. Polars' optimiser rewrites that plan before
execution, pushing projections (column selection) and predicates (row filters)
down into the Parquet reader itself. The reader then uses each row group's
column statistics to skip groups whose min/max range excludes the predicate —
so filtered data is never decompressed, let alone materialised.

Measured: filtering to 1 symbol out of 4,000 (5,040 of 20.1M rows) returns in
**under 10 ms**, against ~30 ms for a full warm read of all 20.1M rows. The
filtered query does strictly less I/O, not the same I/O followed by a filter.

```python
# Eager: reads all 20.1M rows, then discards 99.97% of them.
frame = pl.read_parquet(path)
spy = frame.filter(pl.col("symbol") == "SPY")

# Lazy: the filter reaches the Parquet reader; irrelevant row groups are skipped.
spy = pl.scan_parquet(path).filter(pl.col("symbol") == "SPY").collect()
```

Both produce identical results, which is precisely the hazard: **a missing
pushdown is a performance regression with no visible symptom.** Inspect the
optimised plan rather than trusting it:

```python
print(
    session.scan(MedallionLayer.SILVER, "equity_daily_bars")
    .filter(pl.col("symbol") == "SPY")
    .explain(optimized=True)
)
```

An integration test asserts pushdown appears in the plan, because correctness
tests alone cannot detect its absence.

---

## Thread configuration

`AnalyticsConfig.threads` defaults to `os.process_cpu_count()` — the full
available CPU count. This default **replaced an incorrect one**, and the
correction is worth understanding.

The original default was `logical // 2`, approximating physical cores, on the
theory that scheduling onto E-cores or SMT siblings would drag a hash join's
slowest partition. Measurement on the grouped aggregation (median of 5 runs):

| threads | median | speedup |
|---|---|---|
| 1 | 0.538 s | 1.00x |
| 2 | 0.283 s | 1.91x |
| 4 | 0.140 s | 3.84x |
| 6 | 0.096 s | 5.59x |
| 8 | 0.076 s | 7.06x |
| **12** | **0.060 s** | **8.95x** |
| 16 | 0.064 s | 8.40x |
| 32 | 0.064 s | 8.40x |

The theory was wrong twice over. Scaling continues well past the 6 physical
P-cores and peaks near 12 threads, so capping at 8 forfeits ~20% of throughput.
And oversubscription **plateaus rather than regressing** — 32 threads on 16
logical CPUs costs nothing.

The mechanism is DuckDB's **morsel-driven parallelism**: work is split into
small units pulled dynamically from a queue, so a slower core completes fewer
morsels instead of stalling a partition, and total concurrency is bounded by the
scheduler rather than by contending OS threads. The original intuition is
correct for engines that *statically* partition work — it simply does not apply
here.

`os.process_cpu_count()` is used rather than `os.cpu_count()` because it
respects cgroup quotas and CPU affinity masks. In a container or on a CI runner
with a quota below the host's core count, `os.cpu_count()` reports the host's
number and would oversubscribe the quota.

Pin the value when benchmarking, so runs are comparable:

```toml
# config/override.toml
[analytics]
threads = 12
```

---

## Memory limits and spilling

`memory_limit` is a ceiling on DuckDB's buffer pool, not a hard process limit.

Below the ceiling, hash tables and sort buffers stay in memory. Above it, DuckDB
**spills to `temp_directory`** — slower, but it completes. Without a ceiling, a
large join can grow until the OS OOM-killer terminates the process, which
produces no diagnostics, no partial result, and no stack trace.

The tradeoff is therefore: *a limit well below physical RAM means some queries
spill; no limit means some queries die.* Spilling is recoverable. Set the limit.

Note DuckDB reports the value back in binary units: a requested `8GB`
(8 x 10⁹ bytes) is echoed as `7.4 GiB`. Same budget, different notation. The
CLI's `doctor` command compares parsed byte counts rather than strings so that
it only warns on an actual clamp.

Point `temp_directory` at a fast local SSD. Spilling to a network mount will
dominate query time.

```bash
marketpulse doctor      # shows what the engine actually accepted
```

`doctor` reads settings back from `duckdb_settings()` rather than echoing the
configuration, because DuckDB clamps or ignores some values. When a query spills
unexpectedly or runs single-threaded, that discrepancy is the first thing to
check.

---

## Choosing between the engines

The handoff is free, so choose per operation.

### Use Polars for

- **Transformation chains.** Composable, type-checked, inspectable via
  `explain()`.
- **Window functions over partitioned time series** — rolling means, lagged
  returns, cumulative aggregates by symbol. `over()` is clearer than SQL's
  `OVER (PARTITION BY ... ORDER BY ...)` for chained derivations.
- **Reading the lakehouse.** `scan_parquet` is where pushdown happens.
- **Schema-aware work** where a typo in a column name should be an error before
  execution rather than a SQL failure at runtime.

```python
returns = (
    session.scan(MedallionLayer.SILVER, "equity_daily_bars")
    .sort("symbol", "trade_date")
    .with_columns(
        daily_return=(pl.col("close") / pl.col("close").shift(1).over("symbol") - 1),
        ma_20=pl.col("close").rolling_mean(window_size=20).over("symbol"),
    )
)
```

### Use DuckDB for

- **Multi-table joins.** Its optimiser handles join ordering; hand-ordered
  Polars joins do not.
- **Aggregations expressed more naturally as SQL,** especially with `GROUP BY`
  combined with `HAVING` and window functions.
- **Anything destined for Spark.** DuckDB SQL is close to Spark SQL, so the
  query ports with minimal change. This is the main reason DuckDB is carried at
  all rather than using Polars alone (ADR-0002).
- **Ad-hoc exploration**, via `marketpulse query`.

```python
with session.registered(bars=bars, metadata=metadata) as scoped:
    result = scoped.sql(
        """
        SELECT m.sector, count(DISTINCT b.symbol) AS symbols, avg(b.close) AS avg_close
        FROM bars b JOIN metadata m USING (symbol)
        GROUP BY m.sector
        HAVING count(DISTINCT b.symbol) > 5
        ORDER BY avg_close DESC
        """
    )
```

### Combine them freely

The common shape is Polars to read and reshape, DuckDB to join and aggregate,
Polars to consume the result — with no copy at either boundary:

```python
bars = session.scan(MedallionLayer.SILVER, "equity_daily_bars").filter(
    pl.col("trade_date") >= date(2026, 1, 1)
)
with session.registered(bars=bars) as scoped:
    summary = scoped.sql("SELECT symbol, avg(close) a FROM bars GROUP BY symbol")
session.write(summary, MedallionLayer.GOLD, "symbol_summary", partition_by="symbol")
```

---

## Lakehouse layout

```text
data/
├── raw/        Source bytes, unmodified. Immutable — never rewritten.
├── bronze/     Parsed and typed. Still source-shaped.
├── silver/     Conformed, deduplicated, validated.
└── gold/       Modelled for consumption.

data/silver/equity_daily_bars/
├── symbol=SPY/part-0.parquet
├── symbol=QQQ/part-0.parquet
└── symbol=IWM/part-0.parquet
```

Zone names match the Unity Catalog schema names used on the distributed side, so
a transformation's input and output identifiers do not change when it is
promoted to Databricks.

**`raw/` is immutable for a domain-specific reason.** Economic series are
*revised* after publication (see [ADR-0001](../adr/0001-why-a-financial-market-platform.md)),
so `raw/` is the only record of what a source actually said on a given date.
Overwriting it destroys the ability to reconstruct a point-in-time view, which
is what makes honest ML evaluation possible.

### Partitioning

Partition on the columns queries filter on. Partition values live in directory
names, so a reader can skip entire directories before opening a file.

```python
session.write(frame, MedallionLayer.SILVER, "equity_daily_bars", partition_by="symbol")
```

Hive partitioning must be enabled on read or **the partition column is silently
absent from the schema** — the value is in the directory name, not inside the
Parquet file. `AnalyticsSession.scan()` enables it; an integration test asserts
the column survives the round trip.

Avoid partitioning on a high-cardinality column. One directory per symbol for
4,000 symbols is reasonable; one per symbol-date for 20M rows produces 20M tiny
files, and per-file open overhead will exceed the scan itself.

### zstd over snappy

Default compression is `zstd`, not the more common `snappy`. Measured on the
20.1M-row frame:

| Codec | Size | Write time |
|---|---|---|
| snappy | 102.7 MB | 0.12 s |
| **zstd** | **30.1 MB** | 0.16 s |

**3.4x smaller for 40 ms more write time.** Financial time series compress
exceptionally well — adjacent prices are near-identical, so delta and
dictionary encoding are highly effective. Local analytics is usually I/O-bound,
so a 3.4x smaller file is a direct read-time win that more than repays the write
cost.

---

## Concurrency

`AnalyticsSession` is **not thread-safe.** A `DuckDBPyConnection` serialises
internally, but the registration namespace the session manages is shared mutable
state, so concurrent `register()` calls on one session can race.

Take a cursor per thread. `cursor_session()` creates an independent connection
handle against the same database, with its own transaction and registration
state:

```python
def worker(symbol: str) -> pl.DataFrame:
    with session.cursor_session() as scoped:
        return scoped.sql("SELECT * FROM bars WHERE symbol = $s", s=symbol)


with ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(in_current_context(worker), symbols))
```

Note `in_current_context`. `ThreadPoolExecutor` does **not** propagate
`ContextVar`s — a worker starts with an empty logging context, so a `run_id`
bound on the submitting thread would be missing from every record the worker
emits. (An `asyncio.Task` *does* inherit context; the thread pool does not.
A test asserts both behaviours so the distinction stays documented.)

Before reaching for threads: DuckDB already parallelises a single query across
`threads` workers, and that parallelism is more effective than Python-level
fan-out for most analytical work. Use a pool for genuinely independent queries
or for overlapping I/O, not to speed up one aggregation.

---

## SQL safety

Values are bound as parameters; identifiers are validated against a whitelist.

```python
# Correct — bound as a value, cannot alter the parsed statement.
session.sql("SELECT * FROM bars WHERE symbol = $s", s=user_input)

# Never do this.
session.sql(f"SELECT * FROM bars WHERE symbol = '{user_input}'")
```

Registration names **cannot** be parameterised, because DuckDB interpolates them
into a view definition. They are therefore restricted to
`[A-Za-z_][A-Za-z0-9_]*`, ASCII, under 128 characters. Restricting the charset
removes the injection path entirely rather than relying on quoting being correct
at every call site. Dataset names are validated the same way, plus a traversal
check, because they arrive from config files and CLI arguments.

---

## Observability

Every operation emits a structured event under the `marketpulse` logger
namespace:

| Event | Fields |
|---|---|
| `analytics.session.opened` | `database`, `threads`, `memory_limit` |
| `analytics.sql.completed` | `query`, `rows`, `columns`, `duration_ms` |
| `analytics.sql.failed` | `query`, exception |
| `analytics.dataset.written` | `layer`, `dataset`, `path`, `partitioned_by` |
| `analytics.setting.rejected` | `setting`, `value`, `duckdb_version` |

`analytics.setting.rejected` exists because DuckDB renames and retires
configuration options across minor versions. A session that refuses to open
because one advisory setting is unrecognised is worse than one that runs with
that setting at its default — so the failure is logged, not raised.

Bind a `run_id` once at the top of a pipeline and it appears on every downstream
record without being threaded through call signatures:

```python
with log_context(run_id=run_id, dataset="equity_daily_bars"):
    ...
```

---

## When this layer is the wrong tool

Stop using it, and promote to Databricks, when any of the following holds. The
full set of thresholds is in
[ADR-0002](../adr/0002-polars-duckdb-for-local-development.md#revisit-when).

- Working set above **~25 GB** — single-node memory becomes the binding
  constraint and ordinary operations spill.
- A single transformation above **~10 minutes** locally — cluster startup
  overhead is then cheaper than the wait.
- Fault tolerance **within** a job is required.
- Delta semantics are load-bearing: concurrent ACID writers, `MERGE`, time
  travel.
- Structured Streaming with checkpointing and exactly-once delivery is needed.

Below those thresholds, this layer is not a compromise — it is faster,
measurably, by eliminating serialisation, JVM startup, and network shuffle that
a single machine does not need.
