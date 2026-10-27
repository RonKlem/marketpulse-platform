"""Cross-cutting platform primitives: errors and structured logging.

This subpackage sits at the bottom of the dependency graph. It must not import
from :mod:`marketpulse.config` or :mod:`marketpulse.analytics`, because both of
those import from here.
"""

from __future__ import annotations

from marketpulse.core.errors import (
    AnalyticsError,
    ConfigurationError,
    DataQualityError,
    MarketPulseError,
)
from marketpulse.core.logging import (
    bind,
    configure_logging,
    get_logger,
    in_current_context,
    log_context,
)

__all__ = [
    "AnalyticsError",
    "ConfigurationError",
    "DataQualityError",
    "MarketPulseError",
    "bind",
    "configure_logging",
    "get_logger",
    "in_current_context",
    "log_context",
]
