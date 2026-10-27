"""Structural validation for landed OHLCV bars.

These four checks -- schema, nulls, duplicates, date range -- are structural:
they catch a response shape or transport bug, not a semantically wrong but
structurally valid row (a split-adjusted price next to unadjusted volume, for
instance). That distinction matters for where this code lives.
:mod:`marketpulse.quality`'s own docstring frames Phase 2 data quality as
*semantic*, domain-aware expectations that schema validation cannot catch --
exactly the category these checks are not. Implementing them here, as plain
functions raising the already-defined
:class:`~marketpulse.core.errors.DataQualityError`, avoids guessing at a
Phase 2 expectations API with no Phase 2 design yet to anchor it.
"""

from __future__ import annotations

import datetime as dt
from typing import Final

import polars as pl

from marketpulse.core.errors import DataQualityError

__all__ = ["validate_bars"]

_EARLIEST_SANE_DATE: Final = dt.date(1990, 1, 1)

_EXPECTED_SCHEMA: Final[dict[str, pl.DataType]] = {
    "symbol": pl.String(),
    "trade_date": pl.Date(),
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "volume": pl.Int64(),
    "adj_close": pl.Float64(),
    "source_system": pl.String(),
    "ingestion_timestamp": pl.Datetime("us", "UTC"),
}

_BUSINESS_KEY: Final = ("symbol", "trade_date")

# adj_close is the one column Yahoo legitimately omits for some symbol/date
# combinations; every other column must always be populated.
_NOT_NULLABLE: Final = tuple(col for col in _EXPECTED_SCHEMA if col != "adj_close")


def _check_schema(frame: pl.DataFrame, symbol: str) -> None:
    missing = sorted(set(_EXPECTED_SCHEMA) - set(frame.schema))
    if missing:
        msg = "Missing required columns"
        raise DataQualityError(msg, symbol=symbol, missing=missing)

    mismatched = {
        column: (str(frame.schema[column]), str(expected))
        for column, expected in _EXPECTED_SCHEMA.items()
        if frame.schema[column] != expected
    }
    if mismatched:
        msg = "Column dtype mismatch"
        raise DataQualityError(msg, symbol=symbol, mismatched=mismatched)


def _check_nulls(frame: pl.DataFrame, symbol: str) -> None:
    counts = frame.select(pl.col(c).null_count() for c in _NOT_NULLABLE).row(0, named=True)
    nonzero = {column: count for column, count in counts.items() if count > 0}
    if nonzero:
        msg = "Null values in required columns"
        raise DataQualityError(msg, symbol=symbol, null_counts=nonzero)


def _check_duplicates(frame: pl.DataFrame, symbol: str) -> None:
    duplicate_rows = frame.height - frame.unique(subset=list(_BUSINESS_KEY)).height
    if duplicate_rows:
        msg = "Duplicate business keys"
        raise DataQualityError(msg, symbol=symbol, duplicate_rows=duplicate_rows)


def _check_date_range(frame: pl.DataFrame, symbol: str) -> None:
    today = dt.datetime.now(tz=dt.UTC).date()
    dates = frame.get_column("trade_date")

    out_of_bounds = frame.filter(
        (pl.col("trade_date") > today) | (pl.col("trade_date") < _EARLIEST_SANE_DATE)
    )
    if out_of_bounds.height:
        msg = "trade_date outside sane bounds"
        raise DataQualityError(
            msg,
            symbol=symbol,
            earliest_allowed=str(_EARLIEST_SANE_DATE),
            latest_allowed=str(today),
            offending_rows=out_of_bounds.height,
        )

    if not dates.is_sorted():
        msg = "trade_date is not monotonically increasing"
        raise DataQualityError(msg, symbol=symbol)


def validate_bars(frame: pl.DataFrame, *, symbol: str) -> None:
    """Validate a parsed OHLCV frame before it is landed.

    Args:
        frame: Frame produced by
            :func:`marketpulse.ingestion.yahoo.parse_chart_response`.
        symbol: Ticker the frame was fetched for, attached to every error as
            context.

    Raises:
        DataQualityError: If the frame fails any of the four structural
            checks: schema, null values in a required column, a duplicate
            ``(symbol, trade_date)`` business key, or a ``trade_date`` that is
            out of sane bounds or not sorted.
    """
    _check_schema(frame, symbol)
    _check_nulls(frame, symbol)
    _check_duplicates(frame, symbol)
    _check_date_range(frame, symbol)
