## What and why

<!--
What changes, and the reasoning. The diff shows what; this should explain why.
If it fixes something, describe the failure, not just the fix.
-->

## Type of change

- [ ] Bug fix
- [ ] New capability
- [ ] Refactor (no behaviour change)
- [ ] Documentation or ADR
- [ ] Infrastructure / CI

## Checklist

- [ ] `pytest` passes
- [ ] `ruff check . && ruff format --check . && mypy` pass
- [ ] Tests are in the correct tier (`unit` touches nothing external;
      `integration` may use real engines and `tmp_path`; neither touches the
      network)
- [ ] New or changed behaviour is covered by a test whose **name states the
      invariant**

## Engine selection

<!-- Delete if not applicable. -->

- [ ] Local work uses Polars or DuckDB per
      [ADR-0002](../docs/adr/0002-polars-duckdb-for-local-development.md)
- [ ] No Pandas or NumPy in `src/` — these break the measured zero-copy Arrow
      boundary (0 MB to register vs 1,130 MB to copy a 456 MB frame)
- [ ] Lakehouse reads are lazy (`scan_*`), and pushdown was verified with
      `.explain(optimized=True)` rather than assumed
- [ ] If this introduces PySpark: which
      [threshold](../docs/adr/0002-polars-duckdb-for-local-development.md#revisit-when)
      does the workload meet, and what is the measurement?

## Performance claims

<!--
Delete if this change makes no performance claim. Otherwise: measurements, not
reasoning. State hardware, engine versions, dataset size, and methodology
(median of N runs).

The thread-count default in this repository exists because a benchmark refuted
a coherent-sounding argument. Unmeasured performance reasoning is a guess.
-->

## Architectural impact

- [ ] This change is cheaply reversible
- [ ] This change is hard to reverse, constrains future work, or had credible
      alternatives — **and an ADR is included**

<!--
An ADR is needed for storage formats, table layouts, execution engines, public
interfaces, or any decision not to adopt something expected.

Required sections, enforced by CI: Status, Context, Decision, Consequences,
Alternatives Considered, Revisit When. The last one is mandatory — a decision
record without a reversal condition is an opinion.
-->

## Notes for reviewers

<!--
Anything that would be hard to see from the diff: a tradeoff you went back and
forth on, an alternative you tried and abandoned, a boundary you were unsure
about.
-->
