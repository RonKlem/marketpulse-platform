"""Exception hierarchy for the MarketPulse platform.

A single root exception lets operational code distinguish "the platform
rejected this" from "the platform has a bug". Orchestration layers catch
:class:`MarketPulseError` to emit a structured failure and exit non-zero;
anything else propagates as an unhandled crash and should page someone.
"""

from __future__ import annotations

from typing import Any


class MarketPulseError(Exception):
    """Base class for every error this platform raises deliberately.

    Carries an optional ``context`` mapping so that failures arrive at the log
    aggregator with the same structured fields as successful operations,
    instead of a prose message a human has to parse.

    Args:
        message: Human-readable description of the failure.
        **context: Structured key/value detail attached to the error.
    """

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = context

    def __str__(self) -> str:
        """Render the message with sorted context fields appended."""
        if not self.context:
            return self.message
        detail = " ".join(f"{k}={v!r}" for k, v in sorted(self.context.items()))
        return f"{self.message} ({detail})"


class ConfigurationError(MarketPulseError):
    """Configuration is missing, malformed, or semantically invalid.

    Raised during startup resolution, never mid-pipeline. A process that fails
    this way should exit before acquiring any resources.
    """


class AnalyticsError(MarketPulseError):
    """A local analytics operation could not be completed.

    Covers engine-level failures in the Polars/DuckDB layer: an unregistered
    relation, a rejected SQL statement, an unreadable dataset path.
    """


class DataQualityError(MarketPulseError):
    """Data violated a declared expectation.

    Distinct from :class:`AnalyticsError` because the engine worked correctly
    and the *data* is wrong. This distinction drives different runbooks: an
    analytics error is an engineering defect, a data quality error is a
    producer or upstream-source problem.
    """
