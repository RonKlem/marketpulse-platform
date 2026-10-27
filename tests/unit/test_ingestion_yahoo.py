"""Yahoo chart response parsing, fetching, and the retry helper -- hermetic.

``parse_chart_response`` is a pure function, so it is tested directly against
a recorded fixture and against inline malformed-payload literals, with no
transport mocking. ``fetch_chart_payload`` is tested with
``urllib.request.urlopen`` itself patched -- the actual network boundary --
so no real socket is opened and no transport library needs mocking.
"""

from __future__ import annotations

import datetime as dt
from email.message import Message
import json
from pathlib import Path
from typing import Any
import urllib.error
import urllib.request

import polars as pl
import pytest

from marketpulse.config.models import IngestionConfig
from marketpulse.core.errors import IngestionError
from marketpulse.ingestion.retry import with_retry
from marketpulse.ingestion.yahoo import fetch_chart_payload, parse_chart_response

_FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "yahoo_chart_spy.json"


@pytest.fixture
def spy_payload() -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    return payload


class TestParseChartResponse:
    def test_row_count_matches_the_timestamp_array(self, spy_payload: dict[str, Any]) -> None:
        frame = parse_chart_response(spy_payload, symbol="SPY")
        assert frame.height == 5

    def test_column_set_matches_the_data_contract(self, spy_payload: dict[str, Any]) -> None:
        frame = parse_chart_response(spy_payload, symbol="SPY")
        assert set(frame.columns) == {
            "symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adj_close",
            "source_system",
            "ingestion_timestamp",
        }

    def test_dtypes_match_the_data_contract(self, spy_payload: dict[str, Any]) -> None:
        frame = parse_chart_response(spy_payload, symbol="SPY")
        assert frame.schema["symbol"] == pl.String
        assert frame.schema["trade_date"] == pl.Date
        assert frame.schema["open"] == pl.Float64
        assert frame.schema["volume"] == pl.Int64
        assert frame.schema["adj_close"] == pl.Float64

    def test_symbol_is_upper_cased(self, spy_payload: dict[str, Any]) -> None:
        frame = parse_chart_response(spy_payload, symbol="spy")
        assert set(frame["symbol"]) == {"SPY"}

    def test_source_system_is_stamped(self, spy_payload: dict[str, Any]) -> None:
        frame = parse_chart_response(spy_payload, symbol="SPY")
        assert set(frame["source_system"]) == {"yahoo_finance"}

    def test_ingestion_timestamp_is_utc_and_constant_across_rows(
        self, spy_payload: dict[str, Any]
    ) -> None:
        """Checked via dtype/n_unique, not to_list(): materialising a tz-aware
        column to Python objects requires the IANA tzdata package, which this
        platform does not depend on and which Windows does not ship by
        default."""
        frame = parse_chart_response(spy_payload, symbol="SPY")
        assert frame.schema["ingestion_timestamp"] == pl.Datetime("us", "UTC")
        assert frame["ingestion_timestamp"].n_unique() == 1

    def test_trade_dates_are_decoded_from_unix_seconds(self, spy_payload: dict[str, Any]) -> None:
        frame = parse_chart_response(spy_payload, symbol="SPY")
        assert frame["trade_date"].to_list()[0] == dt.date(2026, 1, 2)
        assert frame["trade_date"].to_list()[-1] == dt.date(2026, 1, 8)

    def test_null_in_a_quote_field_is_preserved_not_dropped(
        self, spy_payload: dict[str, Any]
    ) -> None:
        """A null in a source array must survive parsing as a null row, not be dropped."""
        payload = json.loads(json.dumps(spy_payload))
        payload["chart"]["result"][0]["indicators"]["quote"][0]["volume"][3] = None

        frame = parse_chart_response(payload, symbol="SPY")

        assert frame.height == 5
        assert frame["volume"].null_count() == 1

    def test_missing_adjclose_block_falls_back_to_nulls(self, spy_payload: dict[str, Any]) -> None:
        payload = json.loads(json.dumps(spy_payload))
        del payload["chart"]["result"][0]["indicators"]["adjclose"]
        frame = parse_chart_response(payload, symbol="SPY")
        assert frame["adj_close"].null_count() == frame.height

    @pytest.mark.parametrize(
        ("mutate", "match"),
        [
            (lambda p: p["chart"]["result"][0].__delitem__("timestamp"), "timestamp"),
            (
                lambda p: p["chart"]["result"][0]["indicators"].__delitem__("quote"),
                "quote",
            ),
            (lambda p: p.__delitem__("chart"), "chart"),
        ],
    )
    def test_missing_required_field_raises_ingestion_error(
        self, spy_payload: dict[str, Any], mutate: Any, match: str
    ) -> None:
        payload = json.loads(json.dumps(spy_payload))
        mutate(payload)
        with pytest.raises(IngestionError, match=match):
            parse_chart_response(payload, symbol="SPY")

    def test_chart_error_raises_ingestion_error(self) -> None:
        payload = {
            "chart": {
                "result": None,
                "error": {
                    "code": "Not Found",
                    "description": "No data found, symbol may be delisted",
                },
            }
        }
        with pytest.raises(IngestionError, match="result"):
            parse_chart_response(payload, symbol="DELISTED")

    def test_empty_result_list_raises_ingestion_error(self) -> None:
        payload: dict[str, Any] = {"chart": {"result": [], "error": None}}
        with pytest.raises(IngestionError):
            parse_chart_response(payload, symbol="SPY")

    def test_empty_quote_list_raises_ingestion_error(self, spy_payload: dict[str, Any]) -> None:
        payload = json.loads(json.dumps(spy_payload))
        payload["chart"]["result"][0]["indicators"]["quote"] = []
        with pytest.raises(IngestionError, match="no quote entries"):
            parse_chart_response(payload, symbol="SPY")

    def test_mismatched_array_lengths_raise_ingestion_error(
        self, spy_payload: dict[str, Any]
    ) -> None:
        payload = json.loads(json.dumps(spy_payload))
        payload["chart"]["result"][0]["indicators"]["quote"][0]["close"].pop()
        with pytest.raises(IngestionError, match="disagree in length"):
            parse_chart_response(payload, symbol="SPY")


class _FakeResponse:
    """Minimal stand-in for the context manager urlopen returns."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def _ingestion_config(**overrides: Any) -> IngestionConfig:
    defaults: dict[str, Any] = {
        "symbols": ("SPY",),
        "max_retries": 2,
        "backoff_base_seconds": 0.001,
        "backoff_max_seconds": 0.001,
    }
    defaults.update(overrides)
    return IngestionConfig(**defaults)


class TestFetchChartPayload:
    """``urllib.request.urlopen`` is mocked at the transport boundary -- no socket
    is opened. This is the one seam the connector exposes for a transport-level
    test; everything above it (status classification, JSON decoding, retry) runs
    for real."""

    def test_successful_fetch_returns_decoded_json(
        self, monkeypatch: pytest.MonkeyPatch, spy_payload: dict[str, Any]
    ) -> None:
        body = json.dumps(spy_payload).encode()
        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResponse(body))

        result = fetch_chart_payload("SPY", _ingestion_config())

        assert result == spy_payload

    @pytest.mark.parametrize("status", [401, 403, 404])
    def test_non_retryable_http_status_fails_immediately(
        self, monkeypatch: pytest.MonkeyPatch, status: int
    ) -> None:
        attempts = {"n": 0}

        def raise_http_error(*_args: object, **_kwargs: object) -> None:
            attempts["n"] += 1
            raise urllib.error.HTTPError("url", status, "rejected", Message(), None)

        monkeypatch.setattr(urllib.request, "urlopen", raise_http_error)

        with pytest.raises(IngestionError, match="rejected the request"):
            fetch_chart_payload("SPY", _ingestion_config())
        assert attempts["n"] == 1

    def test_retryable_http_status_is_retried_then_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        attempts = {"n": 0}

        def raise_http_error(*_args: object, **_kwargs: object) -> None:
            attempts["n"] += 1
            raise urllib.error.HTTPError("url", 503, "unavailable", Message(), None)

        monkeypatch.setattr(urllib.request, "urlopen", raise_http_error)

        with pytest.raises(IngestionError, match="Exhausted retries"):
            fetch_chart_payload("SPY", _ingestion_config())
        assert attempts["n"] == 3  # max_retries=2 -> 3 total attempts

    def test_unrecognised_http_status_raises_ingestion_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_http_error(*_args: object, **_kwargs: object) -> None:
            raise urllib.error.HTTPError("url", 418, "teapot", Message(), None)

        monkeypatch.setattr(urllib.request, "urlopen", raise_http_error)

        with pytest.raises(IngestionError, match="Unexpected HTTP status"):
            fetch_chart_payload("SPY", _ingestion_config())

    def test_connection_failure_is_retried_then_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_url_error(*_args: object, **_kwargs: object) -> None:
            raise urllib.error.URLError("connection refused")

        monkeypatch.setattr(urllib.request, "urlopen", raise_url_error)

        with pytest.raises(IngestionError, match="Exhausted retries"):
            fetch_chart_payload("SPY", _ingestion_config())

    def test_non_json_body_raises_ingestion_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResponse(b"not json"))

        with pytest.raises(IngestionError, match="not valid JSON"):
            fetch_chart_payload("SPY", _ingestion_config())

    def test_chart_error_in_body_raises_ingestion_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = json.dumps(
            {"chart": {"result": None, "error": {"code": "Not Found", "description": "no data"}}}
        ).encode()
        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResponse(body))

        with pytest.raises(IngestionError, match="reported an error"):
            fetch_chart_payload("SPY", _ingestion_config())

    def test_missing_result_in_body_raises_ingestion_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = json.dumps({"chart": {"result": [], "error": None}}).encode()
        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResponse(body))

        with pytest.raises(IngestionError, match="no chart result"):
            fetch_chart_payload("SPY", _ingestion_config())

    def test_granularity_mismatch_raises_ingestion_error(
        self, monkeypatch: pytest.MonkeyPatch, spy_payload: dict[str, Any]
    ) -> None:
        """Yahoo silently serves coarser bars than requested for some range/symbol
        combinations (confirmed live for range=max on a long-lived symbol). The
        response's own meta.dataGranularity field is the only way to detect this;
        row data alone looks plausible either way."""
        payload = json.loads(json.dumps(spy_payload))
        payload["chart"]["result"][0]["meta"]["dataGranularity"] = "1mo"
        body = json.dumps(payload).encode()
        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResponse(body))

        with pytest.raises(IngestionError, match="different granularity than requested"):
            fetch_chart_payload("SPY", _ingestion_config())

    def test_missing_granularity_field_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch, spy_payload: dict[str, Any]
    ) -> None:
        """An older or stripped-down response without the field should not block a fetch."""
        payload = json.loads(json.dumps(spy_payload))
        del payload["chart"]["result"][0]["meta"]["dataGranularity"]
        body = json.dumps(payload).encode()
        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResponse(body))

        result = fetch_chart_payload("SPY", _ingestion_config())
        assert result == payload


class TestWithRetry:
    def test_succeeds_on_the_first_attempt_without_sleeping(self) -> None:
        sleeps: list[float] = []
        result = with_retry(
            lambda: "ok",
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            retry_on=(ValueError,),
            sleep=sleeps.append,
        )
        assert result == "ok"
        assert sleeps == []

    def test_retries_the_configured_number_of_times(self) -> None:
        calls = {"n": 0}

        def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("transient")
            return "ok"

        sleeps: list[float] = []
        result = with_retry(
            flaky,
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            retry_on=(ValueError,),
            sleep=sleeps.append,
        )
        assert result == "ok"
        assert calls["n"] == 3
        assert len(sleeps) == 2

    def test_backoff_delays_double_and_are_capped(self) -> None:
        sleeps: list[float] = []

        def always_fails() -> str:
            raise ValueError("transient")

        with pytest.raises(ValueError, match="transient"):
            with_retry(
                always_fails,
                max_attempts=5,
                base_delay=1.0,
                max_delay=3.0,
                retry_on=(ValueError,),
                sleep=sleeps.append,
            )
        assert sleeps == [1.0, 2.0, 3.0, 3.0]

    def test_exhausting_attempts_reraises_the_last_exception(self) -> None:
        with pytest.raises(ValueError, match="always fails"):
            with_retry(
                lambda: (_ for _ in ()).throw(ValueError("always fails")),
                max_attempts=2,
                base_delay=0.01,
                max_delay=0.01,
                retry_on=(ValueError,),
                sleep=lambda _: None,
            )

    def test_exception_outside_retry_on_propagates_immediately(self) -> None:
        sleeps: list[float] = []

        def raises_type_error() -> str:
            raise TypeError("not retryable")

        with pytest.raises(TypeError, match="not retryable"):
            with_retry(
                raises_type_error,
                max_attempts=3,
                base_delay=1.0,
                max_delay=10.0,
                retry_on=(ValueError,),
                sleep=sleeps.append,
            )
        assert sleeps == []

    def test_max_attempts_below_one_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            with_retry(
                lambda: "unreachable",
                max_attempts=0,
                base_delay=1.0,
                max_delay=10.0,
                retry_on=(ValueError,),
            )
