# Contributing to MarketPulse

MarketPulse is a platform engineering portfolio project. Contributions are
welcome, and the bar is deliberately specific: **a change should leave the
reasoning behind it discoverable.** Code that works but cannot be justified is
harder to maintain than code that is slightly worse and explained.

---

## Setup

Requires Python 3.12+. No JVM, no Docker, no cloud account.

```bash
git clone https://github.com/ronklem/marketpulse-platform.git
cd marketpulse-platform

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -e . --group dev
pre-commit install
```

Verify:

```bash
marketpulse doctor     # engine versions and settings actually in force
pytest                 # full suite, < 3 s
ruff check . && ruff format --check . && mypy
```

### Use a virtual environment, not your global interpreter

This is an installable package with a `src/` layout and a console entry point.
`pip install -e .` writes `marketpulse` into whichever `site-packages` it finds,
and installing into a global interpreter shares `polars`, `duckdb`, and `pytest`
with every other project on the machine. When another project needs a different
Polars major version, this one breaks — and the failure appears as an
`AttributeError` deep in a query, not as a dependency conflict.

**There is no `requirements.txt`, deliberately.** Dependency ranges live in
`pyproject.toml` (what the code is compatible with); exact versions belong in a
lockfile. A `requirements.txt` alongside `pyproject.toml` is a second source of
truth that drifts silently. If you prefer `uv`:

```bash
uv sync --group dev
```

---

## Workflow

```bash
git switch -c descriptive-branch-name
# ... change things ...
pytest
ruff check . && ruff format . && mypy
git commit            # pre-commit runs the same checks
```

Open a PR against `main`. CI runs lint, types, tests on Python 3.12/3.13 across
Linux and Windows, coverage, and the architecture tests.

### Commit messages

Explain **why**, not what — the diff shows what.

```
Default DuckDB threads to full CPU count, not logical//2

Benchmarked on 20.1M rows: scaling stays near-linear past the 6
physical P-cores and peaks at 12 threads (8.95x). The previous
logical//2 cap forfeited ~20% of throughput. Oversubscription
plateaus rather than regressing because DuckDB uses morsel-driven
parallelism.
```

---

## Engine selection policy

From [ADR-0002](docs/adr/0002-polars-duckdb-for-local-development.md). This is
the policy most likely to affect a contribution.

### Polars and DuckDB locally; PySpark only above threshold

| Task | Engine |
|---|---|
| Transformation chains, window functions, feature derivation | **Polars** |
| Reading the lakehouse | **Polars** (`scan_*`, lazy) |
| Multi-table joins, aggregations, anything destined for Spark | **DuckDB** |
| Distributed batch, streaming, Delta semantics | **PySpark** (`platform/`) |

PySpark requires meeting an
[explicit threshold](docs/adr/0002-polars-duckdb-for-local-development.md#revisit-when):
working set above ~25 GB, a transformation above ~10 minutes locally, in-job
fault tolerance, Delta semantics, or Structured Streaming. *"It will need to
scale eventually"* is not a threshold.

### Pandas and NumPy are banned in `src/`

Enforced by Ruff's `banned-api` rule and an architecture test. This is not
stylistic.

The two-engine local architecture is free because Polars and DuckDB share
Arrow's memory layout — registering a 456 MB frame with DuckDB costs **0 MB** of
buffer pool, while copying it costs **1,130 MB** (measured; see
[local-analytics.md](docs/architecture/local-analytics.md#the-arrow-boundary)).
Routing through Pandas destroys that property and reintroduces an eager,
single-threaded data path.

If a third-party library genuinely requires one of them, convert at the call
site, document why, and keep it out of the data path.

### Prefer lazy

```python
# Good — filter reaches the Parquet reader; irrelevant row groups are skipped.
pl.scan_parquet(path).filter(pl.col("symbol") == "SPY").collect()

# Bad — materialises 20.1M rows, then discards 99.97% of them.
pl.read_parquet(path).filter(pl.col("symbol") == "SPY")
```

Both return identical results, which is the hazard: **a missing pushdown is a
performance regression with no visible symptom.** Verify with
`.explain(optimized=True)` rather than trusting it.

---

## Code standards

### Typing

`mypy --strict` passes with no `# type: ignore` unless accompanied by a comment
explaining the specific third-party gap being worked around.

### Logging

Library modules call `get_logger(__name__)` and nothing else. Only `cli.py`
calls `configure_logging` — a library that installs handlers steals control from
its host, and on a Databricks cluster it loses records for every other library
too.

Use lazy formatting and structured fields:

```python
# Good
_log.info("dataset.written", extra={"layer": layer.value, "rows": frame.height})

# Bad — eager f-string, and the fields are unparseable downstream
_log.info(f"wrote {frame.height} rows to {layer}")
```

`print()` outside `cli.py` fails an architecture test. stdout is reserved for
machine-readable CLI output so that `marketpulse config show --json | jq` works.

### Errors

Raise a subclass of `MarketPulseError` with structured context:

```python
raise AnalyticsError("Dataset contains no Parquet files", layer=layer.value, dataset=dataset)
```

The distinction between `AnalyticsError` and `DataQualityError` matters
operationally: an analytics error is an engineering defect; a data quality error
means the engine worked and the *data* is wrong. Different runbooks.

### Comments and docstrings

Document **why**, not what. A comment restating the code is noise that rots.

```python
# Good
# zstd over snappy: 3.4x smaller on financial series at comparable write cost,
# and local analytics is usually I/O-bound.

# Bad
# Write the frame to parquet
```

Public functions get Google-style docstrings with `Args`, `Returns`, `Raises`.
Non-obvious engineering decisions belong in the docstring, with the measurement
that justifies them.

### SQL safety

Bind values as parameters. Identifiers cannot be parameterised, so they are
whitelisted to `[A-Za-z_][A-Za-z0-9_]*`:

```python
session.sql("SELECT * FROM bars WHERE symbol = $s", s=user_input)  # correct
session.sql(f"SELECT * FROM bars WHERE symbol = '{user_input}'")  # never
```

---

## Testing

### Pick the right tier

| Tier | May touch | Budget |
|---|---|---|
| `tests/unit/` | Nothing external | milliseconds |
| `tests/integration/` | Real engines, `tmp_path` | ~2 s total |
| `tests/architecture/` | The repository tree | milliseconds |

A unit test that opens DuckDB is an integration test in the wrong tier, and an
architecture test enforces that — the tiering only has value if the fast tier
stays fast.

### Do not mock the engines

A mock asserts that the code calls the API it was written to call. It cannot
catch a DuckDB release that renames a setting, a Polars change in pushdown
behaviour, or an Arrow schema that fails to survive the handoff. Those are the
failures that matter here, and the real engines cost ~2 seconds against
`tmp_path`.

### Never reach the network

Enforced by an architecture test. A test that calls a market data API fails when
the market is closed, when a vendor rate-limits CI, and offline. Use recorded
fixtures; mark live contract tests separately.

### Do not mutate the real environment

`load_config` takes `environ` and `config_dir` parameters so tests inject them:

```python
# Good
load_config(config_dir=config_dir, environ={"MARKETPULSE_ANALYTICS__THREADS": "2"})

# Bad — leaks into every test that runs after, order-dependently
os.environ["MARKETPULSE_ANALYTICS__THREADS"] = "2"
```

### Write the test name as the assertion

```python
def test_override_layer_ignored_outside_local(self) -> None:
def test_context_does_not_cross_a_thread_boundary_implicitly(self) -> None:
```

A reader should learn the invariant from the name alone.

### Assert behaviour that is easy to assume wrongly

The most valuable tests here encode surprising truths. Two examples already in
the suite: `ThreadPoolExecutor` does *not* propagate `ContextVar`s (an
`asyncio.Task` does), and Hive partition columns vanish on read unless
`hive_partitioning=True`, because the value lives in the directory name rather
than in the file. Both were discovered by a failing test contradicting a
docstring.

---

## When a change needs an ADR

Write one ([template](docs/adr/template.md)) when the change:

- is **hard to reverse** — a storage format, table layout, execution engine, or
  public interface;
- **constrains future work** — a boundary others must respect;
- has **credible alternatives** a competent engineer would have chosen;
- was **contentious or surprising**, including a decision *not* to adopt
  something expected.

Do not write one for a cheaply reversible choice. An ADR for a log format
dilutes the set, and a diluted set stops being read.

Required sections, enforced by CI: `Status`, `Context`, `Decision`,
`Consequences`, `Alternatives Considered`, `Revisit When`.

**`Revisit When` is mandatory because a decision record without a reversal
condition is an opinion.** Every decision is correct relative to a context, and
contexts change. State what evidence would overturn it.

**Accepted ADRs are superseded, never edited to change a conclusion.** Editing
destroys the record of what was believed and why, which is the only thing an ADR
is for. Typo fixes are fine.

```bash
cp docs/adr/template.md docs/adr/0003-your-decision.md
# add it to the index table in docs/adr/README.md
pytest -m architecture
```

---

## Performance claims must be measured

This is the house rule that most distinguishes this repository.

If a change is justified by performance, **include the measurement** — in the
docstring, the ADR, or the commit message. State the hardware, the engine
versions, the dataset size, and the methodology (median of N runs).

The thread-count default exists in its current form because a benchmark
*refuted* the original reasoning: the `logical // 2` heuristic, justified by a
plausible argument about E-cores dragging hash-join partitions, turned out to
forfeit ~20% of throughput. The argument was coherent and wrong.

Unmeasured performance reasoning is a guess. Label it as such, or measure it.

---

## Reviewing

Useful review questions:

1. Is the reasoning discoverable in six months without this PR thread?
2. Does a performance claim carry a measurement?
3. Is the engine choice consistent with ADR-0002?
4. Is the test in the right tier, and does its name state the invariant?
5. Does a hard-to-reverse decision have an ADR?
6. Would a boundary this change relies on survive without a test enforcing it?

---

## Reporting issues

For a bug, include `marketpulse doctor` output — it reports engine versions and
the settings DuckDB actually accepted, which is not always what was configured.

For a performance issue, include the dataset size, the query, and
`.explain(optimized=True)` for a Polars plan or `EXPLAIN ANALYZE` for DuckDB.

## License

Contributions are licensed under the [MIT License](LICENSE).
