"""MarketPulse: a local-first financial market data and ML platform.

The package is deliberately split along the boundary described in ADR-002:

* :mod:`marketpulse.config` resolves layered configuration into immutable,
  validated objects.
* :mod:`marketpulse.core` holds cross-cutting concerns (errors, logging) that
  every other subsystem depends on and that depend on nothing themselves.
* :mod:`marketpulse.analytics` owns the local execution engines (Polars and
  DuckDB) and the Arrow boundary between them.

Nothing in this package imports PySpark. Distributed execution lives in
``platform/databricks`` and consumes this package as a dependency, which keeps
the wheel installable on a Databricks cluster without dependency conflicts.
"""

from __future__ import annotations

from marketpulse.core.errors import (
    AnalyticsError,
    ConfigurationError,
    MarketPulseError,
)

__all__ = [
    "AnalyticsError",
    "ConfigurationError",
    "MarketPulseError",
    "__version__",
]

__version__ = "0.1.0"
