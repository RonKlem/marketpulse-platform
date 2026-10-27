"""Ingestion service orchestration against a real filesystem, no network.

The network boundary (``marketpulse.ingestion.yahoo.fetch_chart_payload``) is
patched at the module level -- the seam ``service.py`` is deliberately
written to expose by calling through the ``yahoo`` module object rather than
importing the function by name. Everything downstream of that patch (JSON
parsing is skipped since the fake returns a frame-shaped payload via the real
parser, validation, and the Parquet write) runs for real against ``tmp_path``.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from marketpulse.core.errors import IngestionError
from marketpulse.ingestion import storage, yahoo
from marketpulse.ingestion.service import ingest_symbol, run_ingestion
from marketpulse.ingestion.yahoo import parse_chart_response

if TYPE_CHECKING:
    from marketpulse.config.models import PlatformConfig

_FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "yahoo_chart_spy.json"


@pytest.fixture
def spy_payload() -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    return payload


@pytest.fixture
def ingestion_config(config: PlatformConfig) -> PlatformConfig:
    """The shared `config` fixture, with a small symbol list for these tests."""
    return dataclasses.replace(
        config,
        ingestion=dataclasses.replace(config.ingestion, symbols=("SPY", "QQQ")),
    )


class TestRunIngestion:
    def test_lands_a_parquet_file_per_configured_symbol(
        self,
        ingestion_config: PlatformConfig,
        spy_payload: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(yahoo, "fetch_chart_payload", lambda _symbol, _cfg: spy_payload)

        results = run_ingestion(ingestion_config)

        assert [r.status for r in results] == ["ok", "ok"]
        for symbol in ("spy", "qqq"):
            path = ingestion_config.storage.root / "raw" / f"{symbol}.parquet"
            assert path.is_file()

    def test_written_parquet_round_trips_through_polars(
        self,
        ingestion_config: PlatformConfig,
        spy_payload: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(yahoo, "fetch_chart_payload", lambda _symbol, _cfg: spy_payload)

        run_ingestion(ingestion_config)

        path = ingestion_config.storage.root / "raw" / "spy.parquet"
        frame = pl.read_parquet(path)
        assert frame.height == 5
        assert set(frame["symbol"]) == {"SPY"}

    def test_one_symbol_failing_does_not_abort_the_run(
        self,
        ingestion_config: PlatformConfig,
        spy_payload: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def flaky_fetch(symbol: str, cfg: object) -> dict[str, Any]:
            if symbol == "SPY":
                msg = "simulated upstream failure"
                raise IngestionError(msg, symbol=symbol)
            return spy_payload

        monkeypatch.setattr(yahoo, "fetch_chart_payload", flaky_fetch)

        results = run_ingestion(ingestion_config)

        by_symbol = {r.symbol: r for r in results}
        assert by_symbol["SPY"].status == "failed"
        assert by_symbol["SPY"].error is not None
        assert by_symbol["QQQ"].status == "ok"
        assert not (ingestion_config.storage.root / "raw" / "spy.parquet").exists()
        assert (ingestion_config.storage.root / "raw" / "qqq.parquet").is_file()

    def test_validation_failure_lands_no_file(
        self,
        ingestion_config: PlatformConfig,
        spy_payload: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broken_payload = json.loads(json.dumps(spy_payload))
        # Two identical trade_date entries trip the duplicate-business-key check.
        broken_payload["chart"]["result"][0]["timestamp"][1] = broken_payload["chart"]["result"][0][
            "timestamp"
        ][0]

        monkeypatch.setattr(yahoo, "fetch_chart_payload", lambda _symbol, _cfg: broken_payload)

        result = ingest_symbol("SPY", ingestion_config)

        assert result.status == "failed"
        assert result.error is not None
        assert not (ingestion_config.storage.root / "raw" / "spy.parquet").exists()


class TestWriteRawBars:
    def test_write_failure_raises_ingestion_error(
        self, ingestion_config: PlatformConfig, spy_payload: dict[str, Any]
    ) -> None:
        """A write that fails at the OS level must surface as IngestionError.

        Writing to a path that is itself a directory is a reliable way to
        provoke an OSError without depending on platform-specific permission
        behaviour.
        """
        frame = parse_chart_response(spy_payload, symbol="SPY")
        path = storage.raw_bars_path(ingestion_config, "SPY")
        path.mkdir(parents=True)

        with pytest.raises(IngestionError, match="Failed to write raw bars"):
            storage.write_raw_bars(frame, symbol="SPY", config=ingestion_config)
