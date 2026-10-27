# Deployment surfaces

Everything here is a **deployment artifact**, not Python to import.

```text
platform/
├── databricks/     PySpark jobs, Workflows, Unity Catalog   (Phase 3)
└── terraform/      AWS and Databricks infrastructure        (Phase 5)
```

## Why this is not an importable package

There is deliberately **no `__init__.py`** in this tree, and an architecture
test asserts its absence.

`src/marketpulse/` is a Python package that ships as a wheel and is installed on
a Databricks cluster. The code here *consumes* that wheel: a job definition, a
Terraform module. Keeping the two apart is what makes the "no Spark in the local
stack" guarantee structural rather than aspirational — a PySpark import cannot
leak into `src/` because there is nothing importable to leak from.

That boundary is the subject of
[ADR-0002](../docs/adr/0002-polars-duckdb-for-local-development.md), which also
records the thresholds a workload must meet before it belongs here:

- working set above ~25 GB;
- a single transformation above ~10 minutes locally;
- fault tolerance required *within* a job;
- Delta semantics load-bearing (concurrent ACID writers, `MERGE`, time travel);
- Structured Streaming with checkpointing and exactly-once delivery.

*"It will need to scale eventually"* is not one of them.

## Naming

Medallion zone names match the local layout exactly — local
`data/silver/equity_daily_bars/` corresponds to Unity Catalog
`silver.equity_daily_bars`. A transformation's input and output identifiers
therefore do not change when it is promoted, which is what keeps the promotion
path from being a rewrite.
