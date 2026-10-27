# MarketPulse Platform Architecture

This document describes the platform as a whole: its layers, boundaries, and the
reasoning behind each. Decisions that were expensive to make are recorded as
[ADRs](docs/adr/); the local execution layer has its own detailed treatment in
[`docs/architecture/local-analytics.md`](docs/architecture/local-analytics.md).

Sections marked **(Planned)** describe phases not yet implemented. They are
included because the boundaries they imply constrain what is being built now —
the local layer is designed around the promotion path, not retrofitted to it.

---

## 1. Design principles

Five principles, each with a consequence that shows up in the code.

### 1.1 Use the lightest tool that solves the problem

The dataset this platform is sized for — twenty years of daily bars for 4,000
symbols — is 20.1M rows and **456 MB in memory**. That is 0.7% of the reference
machine's RAM. A distributed execution engine solves the problem of data that
does not fit on one machine; this data is four orders of magnitude from that
problem.

**Consequence:** Polars and DuckDB locally, PySpark only above measured
thresholds ([ADR-0002](docs/adr/0002-polars-duckdb-for-local-development.md)).
The local test suite runs in under 3 seconds against real engines.

### 1.2 Boundaries are enforced by tests, not by convention

A documented rule that nothing checks is a rule that erodes. Nobody *intends* to
import PySpark into the local stack; it happens on a busy week when a reviewer
is reading a diff rather than the whole tree.

**Consequence:** `tests/architecture/` asserts the invariants over the AST of
every module — `core` imports nothing internal, no module imports
PySpark/Pandas/NumPy, no library module configures logging or calls `print`,
every ADR has a reversal condition. These fail CI.

### 1.3 Fail at startup, not mid-pipeline

A pipeline that fails after an expensive read has wasted the read and produced a
diagnosis problem. The earlier a failure surfaces, the cheaper it is.

**Consequence:** configuration is resolved and fully validated before any
resource is acquired. Unknown keys are errors, not warnings — a silently ignored
setting is the most expensive class of config bug, because the operator believes
it is in effect.

### 1.4 Production fails closed; development fails open

The same condition warrants opposite responses depending on environment. A
missing directory locally is a convenience to repair; in production it is a
misconfiguration that would otherwise produce a second, ungoverned, unmonitored
copy of a dataset.

**Consequence:** `storage.create_missing` must be `false` in `prod`, enforced by
a cross-field invariant that rejects the combination at load time.

### 1.5 Record the reasoning, including when it was wrong

**Consequence:** ADR-0002 documents a thread-count default that measurement
*refuted* — the original `logical // 2` heuristic forfeited ~20% of throughput —
and explains the mechanism (DuckDB's morsel-driven parallelism) that makes the
intuition wrong. The refutation is more useful than the conclusion.

---

## 2. System context

```text
         ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐
         │ Yahoo Finance│  │     FRED     │  │ Simulated trade /  │
         │ equity OHLCV │  │ rates, CPI,  │  │ quote event stream │
         │              │  │ indicators   │  │                    │
         └───────┬──────┘  └───────┬──────┘  └─────────┬──────────┘
                 │  batch, daily   │  batch, revised   │  continuous
                 └─────────────────┴───────────────────┘
                                   ▼
            ╔══════════════════════════════════════════════════╗
            ║              MARKETPULSE PLATFORM                ║
            ║                                                  ║
            ║   Ingestion → Lakehouse → Analytics → ML         ║
            ║   Governance · Observability · IaC · CI/CD       ║
            ╚══════════════════════════════════════════════════╝
                                   ▼
          ┌──────────────┬──────────────────┬──────────────────┐
          │  Analysts    │  ML models       │  Operators       │
          │  (SQL)       │  (features)      │  (metrics, logs) │
          └──────────────┴──────────────────┴──────────────────┘
```

FRED is the architecturally significant source. Its series are **revised** after
publication, which makes point-in-time correctness a platform requirement rather
than a modelling detail — see
[ADR-0001](docs/adr/0001-why-a-financial-market-platform.md).

---

## 3. The promotion boundary

The single most important structural decision. Local development and distributed
execution are separated deliberately, and the separation is physical.

```text
┌───────────────────────────────── LOCAL ─────────────────────────────────┐
│                                                                         │
│  src/marketpulse/            Polars · DuckDB · PyArrow                  │
│                              Installable wheel, no JVM                  │
│  data/{raw,bronze,silver,gold}/    Hive-partitioned Parquet (zstd)      │
│                                                                         │
│  Feedback loop: milliseconds.  Full suite: < 3 s.  Cost: $0.            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
         ══════════════ PROMOTION BOUNDARY (ADR-0002) ═══════════════
         Crossed when: working set > ~25 GB, a transformation > ~10 min,
         in-job fault tolerance needed, Delta semantics load-bearing,
         or Structured Streaming required.
                                    │
┌─────────────────────────────── DISTRIBUTED ─────────────────────────────┐
│                                                                         │
│  platform/databricks/        PySpark · Delta Lake · Unity Catalog       │
│                              Job definitions, NOT an importable package │
│  s3://.../{raw,bronze,silver,gold}/    Delta tables                     │
│                                                                         │
│  Feedback loop: minutes.  Purpose: scale and operability.               │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why `src/` is importable and `platform/` is not

`src/marketpulse/` is a Python package that ships as a wheel and is installed on
a Databricks cluster. `platform/` holds deployment artifacts — job definitions,
Terraform modules — that *consume* that wheel.

This is what makes the "no Spark in the local stack" guarantee structural rather
than aspirational: there is no `platform/__init__.py`, so Spark code is not
importable from `src/`. An architecture test asserts the absence of that file.

### What makes crossing the boundary cheap

A separation is only valuable if promotion is not a rewrite. Three properties:

| Property | Local | Distributed |
|---|---|---|
| Zone names | `data/silver/equity_daily_bars/` | `silver.equity_daily_bars` |
| Partitioning | Hive-partitioned Parquet | Delta partitioned |
| Query language | DuckDB SQL | Spark SQL |

Zone names are **identical**, so a transformation's input and output identifiers
do not change. DuckDB SQL and Spark SQL overlap enough that SQL-expressed logic
ports textually — which is the primary reason DuckDB is carried alongside Polars
rather than using Polars alone.

### The honest cost

Polars *expression* code does not port; it must be rewritten for Spark. And
engine semantics diverge in ways that are easy to miss: null ordering in sorts,
floating-point aggregation order, string collation, timestamp precision. A
transformation validated locally can produce different output on the platform.

This is the central risk of the architecture. It is mitigated by reconciliation
tests at the promotion boundary, not assumed away — and if those tests start
failing routinely, that is a stated trigger to revisit ADR-0002.

---

## 4. Local layer (Phase 1-2)

Covered in depth in
[`docs/architecture/local-analytics.md`](docs/architecture/local-analytics.md).
The essentials:

### 4.1 Package structure

```text
src/marketpulse/
├── core/           Errors, structured logging.  Depends on NOTHING internal.
├── config/         Layered TOML + env → frozen validated PlatformConfig.
├── analytics/      AnalyticsSession: the Polars/DuckDB Arrow boundary.
├── cli.py          Entry point. The only module that configures logging.
├── ingestion/      Source connectors.                        (Phase 1)
├── transform/      Medallion transformations.                 (Phase 2)
├── quality/        Data quality expectations.                 (Phase 2)
├── streaming/      Event generation and ingestion.            (Phase 4)
├── observability/  Metrics and health.                        (Phase 6)
└── ml/             Features, training, registry.              (Phase 7)
```

`core` sits at the bottom of the dependency graph and `config` must not import
`analytics` — configuration resolves before any engine is constructed. Both are
enforced by tests, because an inverted dependency surfaces as an `ImportError`
whose traceback points at an innocent module.

### 4.2 The Arrow boundary, measured

Polars and DuckDB share Arrow's memory layout, so a frame registered with DuckDB
is read in place. On the 456 MB / 20.1M-row reference dataset:

| Operation | Wall | DuckDB buffer pool |
|---|---|---|
| `register()` a Polars frame | 166 ms | **0 MB** |
| `CREATE TABLE AS SELECT` (real copy) | 184 ms | **1,130 MB** |
| Aggregate over registered frame | **58 ms** | — |
| Aggregate over native table | 112 ms | — |

Zero buffer-pool cost to expose the data, and querying the registered frame is
*faster* than the native table. This measurement is what makes a two-engine
local stack free, and it is why Pandas and NumPy are banned from `src/` — routing
through either destroys the property.

### 4.3 Medallion layout

```text
data/
├── raw/        Source bytes, unmodified. IMMUTABLE.
├── bronze/     Parsed and typed. Still source-shaped.
├── silver/     Conformed, deduplicated, validated.
└── gold/       Modelled for consumption.
```

`raw/` is immutable because FRED revises published values. It is the only record
of what a source said on a given date, and therefore the only basis for
reconstructing a point-in-time training set. Overwriting it silently destroys
the ability to evaluate a model without look-ahead bias.

---

## 5. Configuration

Six layers, lowest precedence first:

```text
1. Dataclass defaults            src/marketpulse/config/models.py
2. config/base.toml              every environment
3. config/{environment}.toml     per-environment, committed
4. config/override.toml          developer-local, GIT-IGNORED, local env ONLY
5. MARKETPULSE_* env vars        MARKETPULSE_ANALYTICS__THREADS=12
6. Explicit kwargs               tests
```

Three deliberate properties:

**Layer 4 is restricted to the `local` environment.** A git-ignored file that
exists on one machine and is absent in CI is a reproducibility hazard. It is
named `override.toml` rather than `local.toml` specifically so it cannot be
confused with — or loaded twice alongside — the committed `local.toml`
environment layer.

**Unknown keys are startup errors with a `did_you_mean` hint.** A warning would
be ignored; the operator would believe the setting was in effect.

**Resolved objects are frozen.** A config can be handed to a thread pool or
captured in a closure with no question of downstream mutation.

Relative paths anchor to the directory containing `pyproject.toml`, not the
working directory, so `data` means the same place from any subdirectory. When
installed as a wheel there is no checkout, and the anchor falls back to the
working directory — never `site-packages`, which an earlier version did,
silently pointing the lakehouse inside the installed package. `MARKETPULSE_ROOT`
states the anchor explicitly where a deployment needs it.

Why frozen dataclasses rather than Pydantic: the wheel is installed on
Databricks clusters, which pin their own `pydantic` (v1 or v2 depending on DBR
version). A library declaring an incompatible range either fails to resolve or
shadows the runtime's copy for every other library on the cluster. Sixty lines
of hand-written validation is cheaper than that blast radius.

---

## 6. Observability

### 6.1 Structured logging

Built on stdlib `logging`, not `structlog` or `loguru`. The reason is
environmental: this code runs on a laptop, a GitHub runner, and a Databricks
cluster. On a cluster, the runtime installs its own root handlers and ships
records to its log sinks — a framework that bypasses `logging` (as `loguru`
does) loses those records.

| Property | Implementation |
|---|---|
| Renderers | `console` (aligned, for humans), `json` (one object per line) |
| Default | `console` on a TTY, `json` otherwise — CI and jobs get JSON |
| Timestamps | **Always UTC.** Market data spans exchanges; CI runners vary. |
| Context | `ContextVar`, bound once per pipeline, attached at emit time |
| Namespace | `marketpulse` logger, `propagate = False` |

`propagate = False` means the library never touches the root logger, so an
application embedding this package keeps control of its own logging.

**Context does not cross a thread boundary implicitly.** `ContextVar` gives
*isolation* (each thread sees its own value, so concurrent work on different
symbols cannot contaminate each other's records), not propagation. An
`asyncio.Task` inherits a context copy; `ThreadPoolExecutor` does not — a worker
starts empty. `in_current_context()` carries bindings across explicitly. Tests
assert both behaviours, since the asymmetry is easy to assume wrongly.

### 6.2 Reading settings back from the engine

`marketpulse doctor` reports DuckDB settings from `duckdb_settings()` rather
than echoing the configuration, because **DuckDB clamps and normalises values**.
A requested `8GB` is reported as `7.4 GiB` — the same budget in binary units.
When a query spills unexpectedly or runs single-threaded, the gap between
requested and effective is the first thing worth checking.

`doctor` compares parsed byte counts, not strings, so it warns only on a genuine
clamp.

---

## 7. Testing

Three tiers, separated by what each is permitted to touch:

| Tier | Touches | Speed | Purpose |
|---|---|---|---|
| `tests/unit/` | Nothing external | ms | Logic, validation, precedence |
| `tests/integration/` | Real engines + `tmp_path` | ~2 s | Engine behaviour, Arrow handoff, pushdown |
| `tests/architecture/` | The repository tree | ms | Structural invariants, ADR conventions |

**Engines are not mocked.** A mock asserts only that the code calls the API it
was written to call. It cannot catch a DuckDB release that renames a setting, a
Polars change in pushdown behaviour, or an Arrow schema that does not survive
the handoff — which are precisely the failures that matter. Running the real
engines against `tmp_path` costs ~2 seconds.

**No test reaches the network.** A test that calls a market data API fails when
the market is closed, when a vendor rate-limits CI, and on a plane. Connectors
are tested against recorded fixtures; live calls are separately marked.

**`filterwarnings = ["error"]`.** A `DeprecationWarning` from Polars or DuckDB
fails the build that introduces it, rather than becoming a surprise breakage
during a later upgrade.

Two tests deserve specific mention:

- **Pushdown is asserted on the optimised plan,** not on results. A missing
  pushdown produces *correct output* and a silent performance regression, so
  correctness assertions cannot detect it.
- **Thread-count behaviour is asserted against `os.process_cpu_count()`,**
  encoding the measured default so that a regression to the refuted
  `logical // 2` heuristic fails.

---

## 8. CI

`.github/workflows/ci.yml`, on every pull request:

| Job | Checks |
|---|---|
| `lint` | `ruff check`, `ruff format --check` |
| `types` | `mypy --strict` |
| `test` | `pytest` — Python 3.12/3.13, Linux and Windows |
| `coverage` | Gate at 90% |
| `architecture` | Convention tests |

**Windows is in the matrix** because the platform is developed on Windows and
`pathlib` semantics, path separators, and line endings diverge there. A
Linux-only suite validates a configuration no contributor is using.

CI runs with `MARKETPULSE_ENV=ci`, which pins `threads=2` and
`memory_limit=4GB` to match a standard runner's 2 vCPU / 7 GB, and forces JSON
logging (enforced by a cross-field invariant). Deriving threads from the
runner's advertised CPU count would oversubscribe its actual cgroup quota.

---

## 9. Distributed platform (Phase 3) — Planned

```text
platform/databricks/
├── jobs/           PySpark job entry points
├── workflows/      Databricks Workflows definitions
└── catalog/        Unity Catalog schemas, grants, external locations
```

Unity Catalog provides what the local filesystem cannot: column-level lineage,
masking, grants, and audit. Zone names mirror the local layout exactly.

The wheel built from `src/marketpulse/` is installed on the cluster, which is
why its runtime dependencies are held to exactly three — every additional
dependency is a potential conflict with a version DBR already pins, and that
conflict affects every library on the cluster.

---

## 10. Streaming (Phase 4) — Planned

Simulated trade and quote events via Kinesis into Structured Streaming with
checkpointing and exactly-once semantics. **This is the most likely trigger for
crossing the promotion boundary**, since the local stack has no equivalent.

The reconciliation question is the interesting one: do streaming aggregates
agree with the batch daily bars describing the same reality? That is a genuine
correctness problem the domain supplies for free.

---

## 11. Infrastructure (Phase 5) — Planned

```text
platform/terraform/
├── modules/        Reusable: storage, catalog, compute, networking
└── environments/   dev, prod
```

Phase 5 is where `storage.create_missing = false` in `prod` becomes meaningful:
locations are provisioned by Terraform and governed by Unity Catalog external
locations, so a missing path at runtime is a misconfiguration to surface rather
than silently repair.

---

## 12. ML platform (Phase 7) — Planned

The platform concern is **point-in-time correctness**, not model accuracy.

Training on revised FRED figures and evaluating against history produces
look-ahead bias: the model appears to work and would have been unusable in
production. Reconstructing a training set *as it was known* on a given date
requires platform support — bitemporal storage in `silver`, an immutable `raw`
zone, and feature lineage — not a notebook convention.

This is the capability ADR-0001 selected the domain to exercise.

---

## 13. Open questions

Recorded rather than hidden:

1. **Reconciliation at the promotion boundary.** Comparing DuckDB and Spark
   output needs a documented tolerance for floating-point aggregation order and
   a decision on null-ordering normalisation. Not yet designed.
2. **Local Delta.** `delta-rs` would allow time travel and `MERGE` locally
   without Spark, at the cost of a fourth runtime dependency on the wheel.
   Deferred until a concrete need appears.
3. **Bitemporal schema.** Whether revisions are modelled as SCD2 rows or as a
   separate vintage dimension. Affects every downstream query; warrants its own
   ADR before implementation.
4. **Secrets.** Environment variables suffice for Phase 1. Phase 3+ needs a
   decision between Databricks secret scopes and AWS Secrets Manager.

---

## Reference

| Document | Contents |
|---|---|
| [`README.md`](README.md) | Quickstart and orientation |
| [`docs/architecture/local-analytics.md`](docs/architecture/local-analytics.md) | Local execution model, benchmarks, engine selection |
| [`docs/adr/0001`](docs/adr/0001-why-a-financial-market-platform.md) | Why this domain |
| [`docs/adr/0002`](docs/adr/0002-polars-duckdb-for-local-development.md) | Why Polars/DuckDB, with promotion thresholds |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Workflow, engine policy, when an ADR is required |
| [`project_vision.md`](project_vision.md) | Objectives and phase roadmap |
