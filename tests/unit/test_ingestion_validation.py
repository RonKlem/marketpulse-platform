"""Structural validation of landed OHLCV bars -- hermetic, no I/O."""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from marketpulse.core.errors import DataQualityError
from marketpulse.ingestion.validation import validate_bars


def _valid_frame(**overrides: object) -> pl.DataFrame:
    """A small, schema-correct, two-row frame for one symbol."""
    data: dict[str, object] = {
        "symbol": ["SPY", "SPY"],
        "trade_date": pl.Series(["2026-01-02", "2026-01-05"]).str.to_date(),
        "open": [472.10, 473.55],
        "high": [473.90, 475.20],
        "low": [471.50, 472.80],
        "close": [472.65, 475.10],
        "volume": [74_120_000, 68_905_000],
        "adj_close": [472.65, 475.10],
        "source_system": ["yahoo_finance", "yahoo_finance"],
        "ingestion_timestamp": [
            dt.datetime(2026, 1, 9, tzinfo=dt.UTC),
            dt.datetime(2026, 1, 9, tzinfo=dt.UTC),
        ],
    }
    data.update(overrides)
    return pl.DataFrame(
        data,
        schema_overrides={
            "symbol": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "adj_close": pl.Float64,
            "source_system": pl.String,
        },
    )


class TestValidFrame:
    def test_passes_without_raising(self) -> None:
        validate_bars(_valid_frame(), symbol="SPY")


class TestSchemaValidation:
    def test_missing_column_is_rejected(self) -> None:
        frame = _valid_frame().drop("volume")
        with pytest.raises(DataQualityError, match="Missing required columns"):
            validate_bars(frame, symbol="SPY")

    def test_wrong_dtype_is_rejected(self) -> None:
        frame = _valid_frame().with_columns(pl.col("close").cast(pl.Utf8))
        with pytest.raises(DataQualityError, match="dtype mismatch"):
            validate_bars(frame, symbol="SPY")


class TestNullValidation:
    def test_null_in_required_column_is_rejected(self) -> None:
        frame = _valid_frame(volume=[74_120_000, None])
        with pytest.raises(DataQualityError, match="Null values in required columns"):
            validate_bars(frame, symbol="SPY")

    def test_null_in_adj_close_is_tolerated(self) -> None:
        """Yahoo legitimately omits adj_close for some symbol/date combinations."""
        validate_bars(_valid_frame(adj_close=[None, None]), symbol="SPY")


class TestDuplicateDetection:
    def test_duplicate_business_key_is_rejected(self) -> None:
        frame = _valid_frame(
            trade_date=pl.Series(["2026-01-02", "2026-01-02"]).str.to_date(),
        )
        with pytest.raises(DataQualityError, match="Duplicate business keys"):
            validate_bars(frame, symbol="SPY")


class TestDateValidation:
    def test_future_trade_date_is_rejected(self) -> None:
        far_future = (dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=3650)).date()
        frame = _valid_frame(trade_date=[dt.date(2026, 1, 2), far_future])
        with pytest.raises(DataQualityError, match="outside sane bounds"):
            validate_bars(frame, symbol="SPY")

    def test_trade_date_before_1990_is_rejected(self) -> None:
        frame = _valid_frame(trade_date=[dt.date(1980, 1, 1), dt.date(2026, 1, 5)])
        with pytest.raises(DataQualityError, match="outside sane bounds"):
            validate_bars(frame, symbol="SPY")

    def test_unsorted_trade_date_is_rejected(self) -> None:
        frame = _valid_frame(trade_date=[dt.date(2026, 1, 5), dt.date(2026, 1, 2)])
        with pytest.raises(DataQualityError, match="not monotonically increasing"):
            validate_bars(frame, symbol="SPY")
