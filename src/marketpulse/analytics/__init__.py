"""Local analytics engines: Polars for transformation, DuckDB for SQL.

See ``docs/architecture/local-analytics.md`` for the execution model and
ADR-002 for why these engines rather than Spark.
"""

from __future__ import annotations

from marketpulse.analytics.session import AnalyticsSession, open_session

__all__ = ["AnalyticsSession", "open_session"]
