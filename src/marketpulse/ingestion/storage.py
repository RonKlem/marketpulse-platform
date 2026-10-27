"""Landing OHLCV bars as flat per-symbol Parquet files in data/raw/.

Writes directly with Polars rather than through
:class:`~marketpulse.analytics.session.AnalyticsSession`, which always nests
a write under a dataset subdirectory
(``data/raw/<dataset>/part-0.parquet`` or Hive-partitioned). The target layout
here is a flat file at the zone root (``data/raw/spy.parquet``), so this
module calls :meth:`~marketpulse.config.models.StorageConfig.layer_path`
directly. A useful side effect: ``ingestion`` never constructs a DuckDB
session, so it depends on ``config`` and ``core`` only, never ``analytics``.

Each run performs a full-refresh overwrite of a symbol's file, not an
incremental append -- see docs/data-contracts/market_prices.md for why this
is a documented Phase 1 simplification rather than a design this module
tries to paper over.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from marketpulse.config.models import MedallionLayer
from marketpulse.core.errors import IngestionError
from marketpulse.core.logging import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from marketpulse.config.models import PlatformConfig

__all__ = ["raw_bars_path", "write_raw_bars"]

_log = get_logger(__name__)


def raw_bars_path(config: PlatformConfig, symbol: str) -> Path:
    """Return the flat Parquet path for one symbol's raw bars.

    Args:
        config: Platform configuration, for the storage root.
        symbol: Ticker to resolve a path for.

    Returns:
        ``data/raw/<symbol lower-cased>.parquet``. Lower-cased deliberately:
        Windows filesystems are case-insensitive and Linux is
        case-sensitive, so a mixed-case name risks a write on one platform
        silently failing to be found by a read on the other, and this
        platform's CI matrix runs both.
    """
    directory = config.storage.layer_path(MedallionLayer.RAW)
    return directory / f"{symbol.strip().lower()}.parquet"


def write_raw_bars(frame: pl.DataFrame, *, symbol: str, config: PlatformConfig) -> Path:
    """Write a validated OHLCV frame to its flat Parquet path.

    Args:
        frame: Frame to write, already validated by
            :func:`marketpulse.ingestion.validation.validate_bars`.
        symbol: Ticker the frame was fetched for.
        config: Platform configuration, for the storage root.

    Returns:
        The path written to.

    Raises:
        IngestionError: If the write fails. In ``prod``
            (``storage.create_missing = False``), a missing ``data/raw/``
            directory surfaces here as a clear, structured failure rather
            than being silently created -- ``StorageConfig`` already fails
            closed correctly; this just reports it under the right error
            type.
    """
    path = raw_bars_path(config, symbol)
    try:
        frame.write_parquet(path, compression="zstd")
    except (OSError, pl.exceptions.PolarsError) as exc:
        msg = "Failed to write raw bars"
        raise IngestionError(msg, symbol=symbol, path=str(path), reason=str(exc)) from exc

    _log.info(
        "ingestion.write.completed",
        extra={"symbol": symbol, "path": str(path), "rows": frame.height},
    )
    return path
