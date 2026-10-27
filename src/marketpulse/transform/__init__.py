"""Medallion transformations: raw to bronze to silver to gold.

Status: not yet implemented (Phase 2).

Transformations are Polars-first and lazy, so predicates and projections push
into the Parquet reader. Logic that will later run on Spark is expressed as SQL
where practical, because DuckDB SQL ports to Spark SQL and Polars expressions do
not (ADR-0002).
"""

from __future__ import annotations

__all__: list[str] = []
