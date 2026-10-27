# ADR-0002: Use Polars and DuckDB for local development, and defer Spark

- **Status:** Accepted
- **Date:** 2026-10-26
- **Deciders:** Ron Klem

## Status

Accepted

## Context

MarketPulse must demonstrate distributed data processing on Databricks. The
reflexive conclusion is that the codebase should therefore be built on PySpark
throughout, with a local Spark session for development. That conclusion is
wrong, and this record states why.

### The actual data volume

The figure that drives the decision. Daily OHLCV bars for a 4,000-symbol
universe over twenty years:

```
4,000 symbols x 252 trading days x 20 years = 20,160,000 rows
```

Measured on the reference development machine (Intel Core Ultra 7 265H,
16 threads, 64 GB RAM; Polars 1.44, DuckDB 1.5.6, Python 3.14):

| Property | Measured |
|---|---|
| Rows | 20,160,000 |
| In-memory (Arrow, 4 columns) | 456 MB |
| On disk, Parquet + zstd | 30.1 MB |
| On disk, Parquet + snappy | 102.7 MB |
| Grouped aggregation, 4,000 groups | 60 ms |
| Full Parquet read | ~30 ms warm |

**456 MB is 0.7% of this machine's RAM.** It is roughly 2% of the RAM on a
single modest cloud instance. The entire twenty-year history of the US equity
universe at daily granularity fits in the L3-adjacent working set of a laptop,
and a full grouped aggregation over it completes in 60 milliseconds.

A distributed execution engine solves the problem of a dataset that does not fit
on one machine. This dataset is four orders of magnitude from that problem.

### What Spark costs when the data fits

Spark's architecture is the source of its value at scale and its cost below it.
These are structural properties, not implementation defects:

- **JVM startup and session construction.** A local `SparkSession` takes
  seconds to initialise. Multiplied across a test suite, the feedback loop moves
  from sub-second to minutes.
- **The Python-JVM boundary.** PySpark serialises data across a socket to the
  JVM. A Python UDF additionally pays per-row pickling in both directions.
  Polars and DuckDB execute in-process, in compiled code, over the same Arrow
  buffers Python already holds.
- **A query planner tuned for a different cost model.** Catalyst optimises for
  minimising network shuffle, because shuffle dominates distributed execution.
  On one machine there is no network, so that optimisation target is
  mis-specified: partition counts, broadcast thresholds, and shuffle partitions
  all need tuning away from defaults chosen for a cluster.
- **Operational weight in CI.** A Spark job in CI needs a JVM, a compatible
  Java version, Hadoop native libraries on Windows (`winutils.exe`), and
  minutes of wall-clock per run. A contributor must install and configure all of
  it before their first test passes.

None of this is a criticism of Spark. It is the correct cost of fault-tolerant
execution across a cluster. It is simply not a cost worth paying to aggregate
456 MB.

### The feedback loop is the real constraint

The binding constraint on this project is not throughput. It is **iteration
speed**, because development time is the scarce resource and a slow test suite
is not run.

The full local test suite — 167 tests including real Polars and DuckDB
execution against the filesystem — runs in **under 3 seconds**. The equivalent
suite on a local Spark session would be dominated by session startup. That
difference changes behaviour: a 3-second suite is run on every save, and a
3-minute suite is run before a commit, if then.

### Why two local engines rather than one

Polars and DuckDB overlap substantially, and carrying both needs justification.
It is justified because **the handoff between them is free**, which was measured
rather than assumed.

Both use Apache Arrow as their in-memory columnar format. DuckDB's replacement
scan resolves an unqualified SQL identifier against Python objects in scope,
reading an Arrow-backed object's buffers in place. Measured on the 20.1M-row,
456 MB frame above:

| Operation | Wall time | DuckDB buffer-pool memory |
|---|---|---|
| `register()` a Polars frame | 166 ms | **0 MB** |
| `CREATE TABLE AS SELECT` (a real copy) | 184 ms | **1,130 MB** |
| Aggregate over the registered frame | **58 ms** | — |
| Aggregate over the DuckDB-native table | 112 ms | — |

Registration adds nothing to DuckDB's buffer pool, while materialising the same
data as a native table costs 1,130 MB. The registered frame is also *faster* to
query here, because the native table pays for its own buffer management while
the registered frame is read directly from Polars' existing allocation.

That result is what makes the two-engine choice cost-free, and it means the
engine can be selected per operation rather than per project:

- **Polars** — expression-based transformation, window functions over
  partitioned time series, row-wise feature derivation against a known schema.
  Its lazy API pushes projections and predicates into the Parquet reader, so a
  filter on one of 4,000 symbols skips row groups entirely rather than
  materialising 20M rows and discarding most of them.
- **DuckDB** — multi-table joins, aggregations, and anything more legible as
  SQL. Critically, its SQL dialect is close to Spark SQL, so a query developed
  locally ports to the distributed platform with minimal change. That is a
  direct contributor to the promotion path below.

### The thread-count finding, which overturned an assumption

This decision originally included an incorrect claim. It is recorded here
because the correction is the substance.

The assumption was that DuckDB's thread count should default to *half* the
logical CPU count, approximating physical cores — the reasoning being that on a
hybrid CPU (6 P-cores, 10 E-cores) or under SMT, scheduling work onto slower
cores would drag a hash join's slowest partition and regress wall-clock time.

Measured on the aggregation above (median of 5 runs):

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

The assumption was **wrong in both respects**. Scaling stays near-linear well
past the 6 physical P-cores and peaks around 12 threads, so capping at 8 would
forfeit ~20% of available throughput. And oversubscription does not regress: at
32 threads on 16 logical CPUs, time is unchanged.

The mechanism is DuckDB's morsel-driven parallelism. Work is divided into small
units assigned dynamically from a queue, so a slower core completes fewer
morsels rather than holding up a partition, and concurrency is bounded by the
scheduler rather than by spawning contending OS threads. The original intuition
holds for engines that *statically* partition work — which is why it is worth
recording as refuted rather than quietly dropped.

## Decision

**All local development, transformation, analytics, validation, and testing will
use Polars and DuckDB. PySpark will be introduced only for workloads that meet
an explicit threshold.**

Specifically:

1. **Polars is the default transformation engine.** Lazy (`scan_*` /
   `LazyFrame`) by default, so predicate and projection pushdown apply.
2. **DuckDB is the SQL engine.** Used for joins, aggregations, and queries
   whose SQL will later port to Spark SQL.
3. **The Polars/DuckDB boundary is Arrow and must stay zero-copy.** Frames are
   registered, never copied through an intermediate format. Pandas and NumPy
   are banned from `src/` by Ruff's `banned-api` rules and by an architecture
   test, because reintroducing them reintroduces an eager, single-threaded,
   row-oriented data path and breaks the zero-copy property measured above.
4. **PySpark lives in `platform/databricks/` and is never imported by
   `src/marketpulse/`.** Enforced by an architecture test over the AST of every
   module, not by convention.
5. **DuckDB thread count defaults to the full available CPU count** —
   `os.process_cpu_count()`, which respects cgroup quotas and affinity masks —
   per the measurements above. No cap, because a ceiling would silently
   throttle a large workstation.
6. **Promotion to Spark requires a measured justification** against the Revisit
   When thresholds. "It will need to scale eventually" is not one.

### The promotion path

The separation is only valuable if crossing it is cheap. Three properties keep
it so:

- **Identical medallion zone names** on both sides. Local Parquet directories
  (`data/silver/equity_daily_bars/`) and Unity Catalog schemas
  (`silver.equity_daily_bars`) are named identically, so a transformation's
  input and output identifiers do not change when it moves.
- **SQL-first transformation logic** where practical. DuckDB SQL and Spark SQL
  overlap enough that much of it ports textually.
- **Hive-partitioned Parquet locally, Delta on the platform.** Both support
  predicate pushdown over partitions, so the partitioning strategy transfers
  even though the table format does not.

## Consequences

### Positive

- **A sub-3-second test suite** against real engines, which means tests are
  actually run during development rather than before a commit.
- **Zero-setup contribution.** `pip install -e . --group dev` and the full
  platform runs. No JVM, no Docker, no cluster, no cloud account.
- **Free CI.** Tests run on standard GitHub Actions runners (2 vCPU, 7 GB) in
  seconds. A Spark-based suite would need a larger runner and minutes per run.
- **Zero-copy interop, measured:** 0 MB of buffer-pool overhead to expose a
  456 MB frame to SQL, versus 1,130 MB to copy it.
- **Correct engine per operation** at no integration cost.
- **The decision itself is the artifact.** Demonstrating that Spark was
  deliberately deferred, with measurements, shows more engineering judgment than
  adopting it by default would.

### Negative

- **Two engines means two APIs to know.** A contributor must understand both
  Polars expressions and SQL, and must know which to reach for. Mitigated by
  `docs/architecture/local-analytics.md`, but it is real cognitive load.
- **Transformations written locally are not automatically Spark-ready.** Polars
  expression code must be rewritten for Spark; only SQL ports cleanly. This is
  the central cost of the decision, and the promotion-path properties above
  reduce it without eliminating it.
- **Behavioural divergence between engines is a real hazard.** Null ordering in
  sorts, floating-point aggregation order, string collation, and timestamp
  precision differ between DuckDB and Spark. A transformation validated locally
  can produce different output on the platform. This must be caught by
  reconciliation tests at the promotion boundary, not assumed away.
- **Single-node is a hard ceiling.** When tick data arrives (Phase 4), the local
  stack cannot process it. That is anticipated, not a surprise, but it means the
  local path has a known expiry for some workloads.
- **Spark expertise is demonstrated in a smaller surface area.** Spark code
  concentrates in `platform/databricks/` rather than pervading the repository.
  Accepted deliberately: a repository that uses Spark for 456 MB demonstrates
  unfamiliarity with Spark's cost model, not expertise in Spark.

### Neutral

- Delta Lake features (time travel, `MERGE`, schema enforcement) are unavailable
  locally. The local lakehouse is Parquet with a directory convention.
- Unity Catalog governance has no local equivalent; the filesystem layout stands
  in for it.
- `duckdb` and `polars` are pinned to compatible major ranges in
  `pyproject.toml` rather than exact versions, so engine upgrades surface as CI
  failures with `filterwarnings = ["error"]` rather than during a later upgrade.

## Alternatives Considered

### Alternative: PySpark everywhere, with a local SparkSession

**Rejected because** it imposes every cost enumerated in Context with no
corresponding benefit at 456 MB. The decisive factor is the feedback loop: a
test suite dominated by JVM startup is a test suite that does not get run. It
would also require every contributor to install and configure a JVM, a
compatible Java version, and on Windows `winutils.exe`, before a single test
passes.

A secondary objection: it would be *dishonest* about scale. Running Spark on a
dataset that fits in 0.7% of RAM demonstrates that the author reaches for Spark
reflexively rather than that they understand when it is warranted.

### Alternative: Spark Connect against a remote Databricks cluster

**Rejected because** it makes local development depend on network latency, an
always-on cluster, and a cloud bill. A contributor without a Databricks
workspace could not run the tests at all, which breaks the reproducibility
premise in ADR-0001. Spark Connect is genuinely useful for interactive work
against production-scale data; it is the wrong tool for a unit test.

### Alternative: Polars only, no DuckDB

**Rejected** on two grounds, one of which is the stronger.

The weaker ground is ergonomics: multi-table joins and aggregations are more
legible in SQL than in chained expressions.

The stronger ground is the **promotion path**. DuckDB SQL is close to Spark SQL;
Polars expression code is close to nothing else. Writing transformation logic as
SQL where practical means it ports to Databricks with minimal rewriting. A
Polars-only local stack would make every promotion a full rewrite, which would
in turn discourage promotion — and the promotion path is a primary thing this
repository exists to demonstrate.

Cost of carrying both: measured at 0 MB and 166 ms (see Context).

### Alternative: DuckDB only, no Polars

**Rejected because** SQL is a poor fit for the iterative, composable
transformation chains that feature engineering produces. Window-function-heavy
derivations over partitioned time series are substantially clearer as Polars
expressions, and the lazy API's pushdown into the Parquet reader is both
explicit and inspectable via `explain()`. Python-side DataFrame manipulation
also remains necessary for connector and test code regardless.

### Alternative: Pandas for local development

**Rejected on mechanics, not preference.** Pandas is eager (no predicate
pushdown into Parquet, so a filtered read materialises everything first),
single-threaded for most operations (no use of the 8.95x parallel speedup
measured above), and its row-oriented-adjacent block manager means a
DuckDB handoff requires a conversion rather than a zero-copy buffer share. On
the 20.1M-row frame, the zero-copy property that makes the two-engine
architecture free would be lost. Banned in `src/` via Ruff `banned-api`.

### Alternative: Dask

**Rejected because** it occupies an awkward middle position here. It adds a
distributed scheduler's complexity without Spark's maturity, ecosystem, or — the
decisive point — relevance to this project's target platform. MarketPulse
demonstrates Databricks; Dask experience does not transfer, so it would be
complexity spent on a capability the project is not trying to show.

## Revisit When

Any of the following should trigger reconsideration. They are stated as
thresholds so the answer is measurable rather than arguable.

**Introduce Spark for a workload when:**

1. **The working set exceeds ~25 GB.** At that point a single machine's memory
   becomes the binding constraint and DuckDB spills to disk for ordinary
   operations. Concretely: tick-level data for a broad universe, or intraday
   bars across a multi-year history (Phase 4).
2. **A single transformation exceeds ~10 minutes locally.** Below that, cluster
   startup overhead exceeds the savings.
3. **Fault tolerance within a single job becomes a requirement.** If a job's
   failure and restart cost is unacceptable, Spark's stage-level retry is
   genuinely needed; DuckDB has no equivalent.
4. **Delta Lake semantics become load-bearing** — concurrent writers requiring
   ACID, `MERGE` for upserts, or time travel as a production requirement rather
   than a convenience.
5. **Structured Streaming is needed** for continuous ingestion with
   checkpointing and exactly-once semantics (Phase 4). This is the most likely
   trigger.

**Reconsider the engine choice itself when:**

6. **Polars or DuckDB stops being maintained,** or a major version breaks the
   Arrow zero-copy interop that makes the two-engine architecture free. The
   0 MB / 1,130 MB measurement above is the thing to re-run.
7. **DuckDB's SQL dialect diverges materially from Spark SQL,** eroding the
   promotion path that justifies carrying DuckDB over Polars alone.
8. **Reconciliation tests at the promotion boundary start failing routinely** on
   engine-semantic differences (null ordering, floating-point aggregation,
   collation). That would indicate the local/distributed equivalence assumption
   is costing more than the fast feedback loop is worth.

**Re-measure the thread default when:**

9. **DuckDB's scheduler changes,** or the reference hardware changes
   substantially. The 8.95x-at-12-threads result is specific to DuckDB 1.5.6 on
   a 16-thread hybrid Intel part. The benchmark in
   `AnalyticsConfig.with_host_defaults` should be re-run, not assumed.

## References

- [ADR-0001](0001-why-a-financial-market-platform.md) — the domain, whose volume
  characteristics this decision depends on
- [`docs/architecture/local-analytics.md`](../architecture/local-analytics.md) —
  execution model, pushdown behaviour, and engine selection guidance
- [DuckDB: morsel-driven parallelism](https://duckdb.org/2022/03/07/aggregate-hashtable.html)
  — the mechanism behind the thread-scaling result
- Benchmarks in this record were measured on Intel Core Ultra 7 265H
  (6 P-cores / 10 E-cores, 16 threads), 64 GB RAM, Windows 11, Python 3.14.5,
  Polars 1.44.2, DuckDB 1.5.6. Reproduce with
  `AnalyticsConfig.with_host_defaults`'s documented query.
