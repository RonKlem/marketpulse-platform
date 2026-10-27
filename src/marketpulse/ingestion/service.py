"""Per-symbol ingestion orchestration: fetch, parse, validate, land.

Isolates failures per symbol so one bad ticker does not abort a run across
the rest of the configured universe.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from marketpulse.core.errors import DataQualityError, IngestionError
from marketpulse.core.logging import get_logger
from marketpulse.ingestion import storage, validation, yahoo

if TYPE_CHECKING:
    from marketpulse.config.models import PlatformConfig

__all__ = ["SymbolIngestionResult", "ingest_symbol", "run_ingestion"]

_log = get_logger(__name__)

# This module calls through the `yahoo` module object (`yahoo.fetch_chart_payload(...)`)
# rather than importing the function by name. A `from ... import fetch_chart_payload`
# binds a local name at import time that `monkeypatch.setattr(
# "marketpulse.ingestion.yahoo.fetch_chart_payload", fake)` would not affect; the
# module-qualified call looks the attribute up at call time, so a test's patch
# takes effect. This is the seam that lets `marketpulse ingest` be exercised
# through its real CLI dispatch path without reaching the network.


@dataclass(frozen=True, slots=True, kw_only=True)
class SymbolIngestionResult:
    """Outcome of ingesting one symbol.

    Attributes:
        symbol: Ticker ingested.
        status: ``"ok"`` if fetched, validated, and landed; ``"failed"``
            otherwise.
        rows: Row count written. Zero on failure.
        path: Path written to, or ``None`` on failure.
        error: Failure message, or ``None`` on success.
    """

    symbol: str
    status: Literal["ok", "failed"]
    rows: int = 0
    path: Path | None = None
    error: str | None = None


def ingest_symbol(symbol: str, config: PlatformConfig) -> SymbolIngestionResult:
    """Fetch, validate, and land one symbol's historical bars.

    Args:
        symbol: Ticker to ingest.
        config: Platform configuration.

    Returns:
        The outcome. A connector or validation failure is caught and
        reported as a ``"failed"`` result rather than raised, so a caller
        iterating over several symbols can isolate one bad ticker. An
        unexpected exception (anything other than
        :class:`~marketpulse.core.errors.IngestionError` or
        :class:`~marketpulse.core.errors.DataQualityError`) is a defect, not
        an operational failure, and propagates uncaught.
    """
    try:
        payload = yahoo.fetch_chart_payload(symbol, config.ingestion)
        frame = yahoo.parse_chart_response(payload, symbol=symbol)
        validation.validate_bars(frame, symbol=symbol)
        path = storage.write_raw_bars(frame, symbol=symbol, config=config)
    except (IngestionError, DataQualityError) as exc:
        _log.error("ingestion.symbol.failed", extra={"symbol": symbol, "error": str(exc)})
        return SymbolIngestionResult(symbol=symbol, status="failed", error=str(exc))

    return SymbolIngestionResult(symbol=symbol, status="ok", rows=frame.height, path=path)


def run_ingestion(config: PlatformConfig) -> list[SymbolIngestionResult]:
    """Ingest every symbol in ``config.ingestion.symbols``.

    Args:
        config: Platform configuration.

    Returns:
        One result per configured symbol, in configured order.
    """
    return [ingest_symbol(symbol, config) for symbol in config.ingestion.symbols]
