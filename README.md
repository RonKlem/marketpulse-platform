# MarketPulse

[![CI](https://github.com/ronklem/marketpulse-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/ronklem/marketpulse-platform/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A financial market data and machine learning platform built to demonstrate how
modern data platforms are actually engineered: local-first development, a
deliberate escalation path to distributed execution, and an explicit written
record of why each boundary sits where it does.

MarketPulse is a **platform** project, not a pipeline or a model. The artifacts
that matter are the architecture, the decision records, and the operational
scaffolding — not a dashboard.

---

## The central architectural decision

**Most data platforms reach for Spark too early.** Spark is the correct answer
to a specific problem: a dataset larger than one machine's memory, or a
workload that must survive partial failure across a cluster. Below that
threshold it imposes JVM startup, serialization overhead, a cluster to pay for,
and a 10-second feedback loop on a unit test.

MarketPulse separates the two concerns explicitly:

| | Local development | Distributed platform |
|---|---|---|
| **Engines** | Polars, DuckDB, PyArrow | PySpark, Delta Lake |
| **Runs on** | A laptop, a CI runner | Databricks |
| **Governance** | Filesystem layout | Unity Catalog |
| **Feedback loop** | Milliseconds | Minutes |
| **Purpose** | Build and prove correctness | Prove scale and operability |

Transformations are developed and tested against Polars and DuckDB, where the
test suite runs in under a second and a contributor needs no cloud account.
They are promoted to Spark when — and only when — scale or operational
requirements justify it.

The reasoning, including the specific conditions that justify crossing the
boundary, is in **[ADR-002](docs/adr/0002-polars-duckdb-for-local-development.md)**.

### Why the two local engines are not redundant

Polars and DuckDB both use Apache Arrow as their in-memory columnar format,
which means a frame can move between them **without being copied or
serialized**. They are kept side by side because they are good at different
things:

- **Polars** — expression-based transformation, window functions, and
  row-wise feature derivation over a known schema.
- **DuckDB** — the SQL optimizer: multi-table joins, aggregations, and queries
  whose dialect is close enough to Spark SQL to port with minimal change.

Because the handoff costs nothing, the engine can be chosen per operation
rather than per project. See
[`docs/architecture/local-analytics.md`](docs/architecture/local-analytics.md)
for the execution model and the memory mechanics behind it.

---

## Quickstart

Requires Python 3.12 or newer. No cloud account, no Docker, no cluster.

```bash
git clone https://github.com/ronklem/marketpulse-platform.git
cd marketpulse-platform

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -e . --group dev
```

Verify the installation:

```bash
marketpulse doctor          # environment and engine diagnostics
marketpulse config show     # resolved configuration and its source layers
pytest                      # full suite
ruff check . && ruff format --check .
mypy
```

`marketpulse doctor` is the single command to run first. It reports the
resolved configuration, the DuckDB settings the engine actually accepted (which
is not always what was requested), and the host's thread allocation.

---

## Repository layout

```text
marketpulse-platform/
├── src/marketpulse/            # Installable package — the platform itself
│   ├── core/                   # Errors and structured logging (no internal deps)
│   ├── config/                 # Layered, validated, immutable configuration
│   ├── analytics/              # Polars + DuckDB session, the Arrow boundary
│   ├── ingestion/              # Yahoo Finance connector, historical OHLCV ingestion
│   ├── transform/              # Medallion transformations      (Phase 2)
│   ├── quality/                # Data quality expectations      (Phase 2)
│   ├── streaming/              # Event generation and ingestion (Phase 4)
│   ├── observability/          # Metrics and health             (Phase 6)
│   └── ml/                     # Features, training, registry   (Phase 7)
│
├── platform/                   # Deployment surfaces — not importable
│   ├── databricks/             # PySpark jobs, Workflows, Unity Catalog  (Phase 3)
│   └── terraform/              # AWS and Databricks IaC                  (Phase 5)
│
├── config/                     # TOML configuration layers
├── docs/
│   ├── adr/                    # Architecture Decision Records
│   └── architecture/           # Architecture and execution-model docs
├── tests/
│   ├── unit/                   # Pure, hermetic, no I/O
│   ├── integration/            # Real engines against temp filesystems
│   └── architecture/           # Enforce repo and ADR conventions in CI
└── scripts/                    # Developer entry points
```

Two boundaries in that tree are load-bearing:

**`src/` is importable; `platform/` is not.** Everything under `src/marketpulse`
is a Python package that ships as a wheel and is installed on a Databricks
cluster. Everything under `platform/` is a deployment artifact — a job
definition, a Terraform module — that *consumes* the wheel. Keeping them apart
means no Spark import can leak into the local stack, and the import graph
enforces it rather than a convention.

**`src/marketpulse/core` depends on nothing internal.** Errors and logging sit
at the bottom of the graph; config and analytics sit above. This is checked by a
test, not by reviewer vigilance.

---

## Architecture

```text
   Yahoo Finance        FRED        Simulated trade events
         │               │                   │
         └───────────────┴───────────────────┘
                         ▼
          ┌──────────────────────────────┐
          │   Ingestion (Python/Polars)  │   Source-shaped, unmodified bytes
          └──────────────────────────────┘
                         ▼
        RAW ──────▶ BRONZE ──────▶ SILVER ──────▶ GOLD      Parquet, local
      immutable     typed        conformed     modelled
                         │
                         ▼
          ┌──────────────────────────────┐
          │  DuckDB — local SQL engine   │   Zero-copy over Arrow
          └──────────────────────────────┘
                         │
      ═══════════════════╪═══════════════════  promotion boundary (ADR-002)
                         ▼
          ┌──────────────────────────────┐
          │  Databricks / PySpark        │   Delta Lake + Unity Catalog
          └──────────────────────────────┘
                         ▼
            Streaming  ·  Feature Store  ·  ML Platform
```

The same medallion zone names appear on both sides of the promotion boundary:
local Parquet directories and Unity Catalog schemas are named identically, so a
transformation's input and output identifiers do not change when it moves.

Full detail in [`architecture.md`](architecture.md).

---

## Architecture Decision Records

ADRs are the primary deliverable of this repository. Code demonstrates that
something was built; an ADR demonstrates that the alternatives were understood
and the tradeoff was made deliberately.

| ADR | Decision | Status |
|-----|----------|--------|
| [0001](docs/adr/0001-why-a-financial-market-platform.md) | Why a financial market platform? | Accepted |
| [0002](docs/adr/0002-polars-duckdb-for-local-development.md) | Why Polars and DuckDB for local development? | Accepted |

Every ADR records the alternatives that were rejected and **the conditions
under which the decision should be revisited**. A decision record without a
reversal condition is an opinion.

Conventions and the template are in [`docs/adr/README.md`](docs/adr/README.md).
A test in `tests/architecture/` fails CI if an ADR is missing required
sections, so the records cannot silently rot.

---

## Configuration

Configuration resolves in layers, lowest precedence first:

1. Dataclass defaults in `src/marketpulse/config/models.py`
2. `config/base.toml` — true in every environment
3. `config/{environment}.toml` — per-environment
4. `config/local.toml` — developer-local, git-ignored, **only** loaded in `local`
5. `MARKETPULSE_*` environment variables
6. Explicit keyword overrides (tests)

```bash
MARKETPULSE_ENV=ci \
MARKETPULSE_ANALYTICS__THREADS=4 \
MARKETPULSE_LOGGING__LEVEL=DEBUG \
  marketpulse config show
```

Nesting uses a double underscore, because field names themselves contain single
underscores (`duckdb_database`).

Three variables steer resolution rather than supplying a value:

| Variable | Effect |
|---|---|
| `MARKETPULSE_ENV` | Selects the environment layer |
| `MARKETPULSE_CONFIG_DIR` | Overrides where layers are read from |
| `MARKETPULSE_ROOT` | Anchors relative paths (`storage.root`, the config dir) |

`MARKETPULSE_ROOT` matters for deployments. In a source checkout, relative paths
anchor to the directory containing `pyproject.toml` — so `data` means the same
place from any subdirectory. Installed as a wheel (a Databricks cluster) there
is no checkout, and paths anchor to the working directory. Set
`MARKETPULSE_ROOT`, or an absolute `storage.root`, when a job's location must be
explicit.

Three properties are deliberate:

- **Unknown keys are an error, not a warning.** A silently ignored setting is
  the most expensive class of configuration bug: the operator believes it is in
  effect and the process behaves as though it were never written. Near-misses
  get a `did_you_mean` hint.
- **Resolution is eager.** The whole tree is validated at startup, before any
  resource is acquired — not lazily at first use, which in a pipeline means
  after the expensive read.
- **Resolved objects are frozen.** A config can be handed to a thread pool or
  captured in a closure with no question of downstream mutation.

`config/local.toml` is restricted to the `local` environment on purpose: a file
that exists on one machine and is absent in CI is a reproducibility hazard.

---

## Testing

```bash
pytest                              # everything
pytest tests/unit -q                # hermetic, no I/O
pytest -m architecture              # convention enforcement
pytest --cov --cov-report=term      # coverage, gated at 90%
```

The suite is tiered by what it is allowed to touch:

- **`tests/unit/`** — no filesystem, no network, no engine. Milliseconds.
- **`tests/integration/`** — real Polars and DuckDB against `tmp_path`. Still
  no network: a test that depends on a market data API is a test that fails
  when the market is closed or the vendor rate-limits CI.
- **`tests/architecture/`** — asserts structural invariants: `core` imports
  nothing internal, no module imports PySpark or Pandas, every ADR has its
  required sections. These catch the drift that code review misses on a busy
  week.

`filterwarnings = ["error"]` is set in `pyproject.toml`. A `DeprecationWarning`
from Polars or DuckDB fails the build on the release that introduces it, rather
than becoming a breakage during a future upgrade.

---

## CI

`.github/workflows/ci.yml` runs on every pull request:

| Job | Purpose |
|---|---|
| `lint` | `ruff check` and `ruff format --check` |
| `types` | `mypy --strict` |
| `test` | `pytest` on Python 3.12 and 3.13, Linux and Windows |
| `coverage` | Gate at 90% |
| `architecture` | Convention tests |

The matrix includes Windows because the platform is developed on Windows and
`pathlib` behaviour, path separators, and line endings diverge there. A CI
suite that only runs on Linux validates a configuration no contributor is
using.

---

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Foundation: structure, config, logging, ingestion, CI | **In progress** |
| 2 | Local lakehouse: medallion layers, data quality | Planned |
| 3 | Databricks: PySpark, Delta Lake, Unity Catalog | Planned |
| 4 | Streaming: Kinesis, Structured Streaming | Planned |
| 5 | Infrastructure: Terraform, AWS | Planned |
| 6 | Observability: metrics, dashboards, alerting | Planned |
| 7 | ML platform: features, MLflow, monitoring | Planned |
| 8 | Staff patterns: capacity, DR, cost, governance | Planned |

Detail in [`project_vision.md`](project_vision.md).

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development workflow, the
engine selection policy, and when a change requires an ADR.

## License

[MIT](LICENSE)
