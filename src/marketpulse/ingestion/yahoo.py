"""Yahoo Finance connector: fetch and parse daily OHLCV bars.

Uses Yahoo's undocumented ``/v8/finance/chart/{symbol}`` JSON endpoint
directly via stdlib ``urllib.request`` rather than the community ``yfinance``
package, which pulls Pandas, NumPy, and ``requests`` into the dependency
closure -- all banned or absent from this stack for the reasons in ADR-002.
See ADR-0003 for why Yahoo Finance was chosen over alternatives, and its
operational limitations.

Fetching and parsing are deliberately separate functions. Parsing is a pure
function over a plain mapping, so it is unit-testable against a recorded
fixture with no transport mocking; fetching is the only function that touches
the network, and is the one seam tests patch.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any, Final
import urllib.error
import urllib.request

import polars as pl

from marketpulse.core.errors import IngestionError
from marketpulse.core.logging import get_logger
from marketpulse.ingestion.retry import with_retry

if TYPE_CHECKING:
    from collections.abc import Mapping

    from marketpulse.config.models import IngestionConfig

__all__ = ["fetch_chart_payload", "parse_chart_response"]

_log = get_logger(__name__)

_SOURCE_SYSTEM: Final = "yahoo_finance"

_RETRYABLE_STATUS_CODES: Final = frozenset({429, 500, 502, 503, 504})
_NON_RETRYABLE_STATUS_CODES: Final = frozenset({401, 403, 404})


class _TransientFetchError(Exception):
    """A fetch failure worth retrying. Never escapes this module."""


def _build_request(symbol: str, config: IngestionConfig) -> urllib.request.Request:
    """Construct the chart-endpoint request for one symbol."""
    params = f"range={config.range}&interval={config.interval}&includeAdjustedClose=true"
    url = f"{config.base_url}/{symbol}?{params}"
    return urllib.request.Request(  # noqa: S310 -- url is built from a validated https base_url
        url,
        headers={"User-Agent": config.user_agent, "Accept": "application/json"},
    )


def _attempt_fetch(symbol: str, config: IngestionConfig) -> dict[str, Any]:
    """Perform one fetch attempt, raising for classification by the caller."""
    request = _build_request(symbol, config)
    try:
        with urllib.request.urlopen(  # noqa: S310 -- scheme is restricted to https by config validation
            request, timeout=config.request_timeout_seconds
        ) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        # HTTPError wraps the error response body in a file-like object that
        # is never otherwise closed on this path, which raises a
        # ResourceWarning on garbage collection; this platform's
        # filterwarnings = ["error"] turns that into a test failure, and in
        # production it is a genuine (if small) resource leak.
        exc.close()
        if exc.code in _NON_RETRYABLE_STATUS_CODES:
            msg = "Yahoo Finance rejected the request"
            raise IngestionError(msg, symbol=symbol, status=exc.code, reason=exc.reason) from exc
        if exc.code in _RETRYABLE_STATUS_CODES:
            msg = f"Transient HTTP {exc.code} from Yahoo Finance"
            raise _TransientFetchError(msg) from exc
        msg = "Unexpected HTTP status from Yahoo Finance"
        raise IngestionError(msg, symbol=symbol, status=exc.code, reason=exc.reason) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        msg = f"Transport failure contacting Yahoo Finance: {exc}"
        raise _TransientFetchError(msg) from exc

    try:
        payload: dict[str, Any] = json.loads(body)
    except json.JSONDecodeError as exc:
        msg = "Yahoo Finance returned a response that is not valid JSON"
        raise IngestionError(msg, symbol=symbol, reason=str(exc)) from exc

    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        msg = "Yahoo Finance reported an error for this symbol"
        raise IngestionError(
            msg,
            symbol=symbol,
            code=error.get("code"),
            description=error.get("description"),
        )
    if not chart.get("result"):
        msg = "Yahoo Finance returned no chart result for this symbol"
        raise IngestionError(msg, symbol=symbol)

    _check_granularity(chart["result"][0], symbol=symbol, requested=config.interval)

    return payload


def _check_granularity(result: Mapping[str, Any], *, symbol: str, requested: str) -> None:
    """Reject a response whose actual granularity does not match what was asked.

    Yahoo silently serves coarser bars than requested for some range/symbol
    combinations -- confirmed live for ``range=max`` on a long-lived symbol,
    which returns monthly bars under ``interval=1d`` with no error of any
    kind. The row data alone cannot reveal this; the response's own
    ``meta.dataGranularity`` field can. Landing mismatched data silently,
    mislabeled as daily, is worse than failing the fetch.
    """
    actual = result.get("meta", {}).get("dataGranularity")
    if actual is not None and actual != requested:
        msg = "Yahoo Finance returned a different granularity than requested"
        raise IngestionError(msg, symbol=symbol, requested=requested, actual=actual)


def fetch_chart_payload(symbol: str, config: IngestionConfig) -> dict[str, Any]:
    """Fetch the raw chart JSON for one symbol, retrying transient failures.

    Args:
        symbol: Ticker to request.
        config: Ingestion settings -- endpoint, timeouts, and retry/backoff
            parameters.

    Returns:
        The decoded JSON response body.

    Raises:
        IngestionError: If the request is rejected outright (unknown symbol,
            authentication/authorisation failure, an unrecognised response
            shape), or if retries are exhausted on a transient failure.
    """
    try:
        return with_retry(
            lambda: _attempt_fetch(symbol, config),
            max_attempts=config.max_retries + 1,
            base_delay=config.backoff_base_seconds,
            max_delay=config.backoff_max_seconds,
            retry_on=(_TransientFetchError,),
        )
    except _TransientFetchError as exc:
        msg = "Exhausted retries contacting Yahoo Finance"
        raise IngestionError(msg, symbol=symbol, reason=str(exc)) from exc


def _require(mapping: Mapping[str, Any], key: str, *, symbol: str) -> Any:
    """Fetch a required key, raising IngestionError rather than KeyError.

    A malformed upstream payload is an anticipated operational failure, not a
    defect, and must surface through the same error hierarchy as every other
    connector failure rather than as a raw traceback.
    """
    if key not in mapping:
        msg = "Chart response is missing a required field"
        raise IngestionError(msg, symbol=symbol, field=key)
    return mapping[key]


def parse_chart_response(payload: Mapping[str, Any], *, symbol: str) -> pl.DataFrame:
    """Parse a Yahoo chart payload into the ingestion schema.

    Pure function: no I/O, so it is directly unit-testable against a recorded
    fixture or an inline malformed-payload literal.

    Args:
        payload: Decoded JSON response body, as returned by
            :func:`fetch_chart_payload`.
        symbol: Ticker the payload was fetched for. Used for the ``symbol``
            column and in error context; not read from the payload itself,
            since Yahoo's ``meta.symbol`` is not guaranteed present.

    Returns:
        A frame with columns ``symbol``, ``trade_date``, ``open``, ``high``,
        ``low``, ``close``, ``volume``, ``adj_close``, ``source_system``,
        ``ingestion_timestamp``. See
        ``docs/data-contracts/market_prices.md`` for the schema contract.

    Raises:
        IngestionError: If a required field is missing, or the per-row arrays
            disagree in length.
    """
    chart = _require(payload, "chart", symbol=symbol)
    results = _require(chart, "result", symbol=symbol)
    if not results:
        msg = "Chart response has no result entries"
        raise IngestionError(msg, symbol=symbol)
    result = results[0]

    timestamps = _require(result, "timestamp", symbol=symbol)
    indicators = _require(result, "indicators", symbol=symbol)
    quotes = _require(indicators, "quote", symbol=symbol)
    if not quotes:
        msg = "Chart response has no quote entries"
        raise IngestionError(msg, symbol=symbol)
    quote = quotes[0]

    open_ = _require(quote, "open", symbol=symbol)
    high = _require(quote, "high", symbol=symbol)
    low = _require(quote, "low", symbol=symbol)
    close = _require(quote, "close", symbol=symbol)
    volume = _require(quote, "volume", symbol=symbol)

    # adjclose is sometimes absent entirely; fall back to nulls for every row
    # rather than treating its absence as a parse failure.
    adjclose_entries = indicators.get("adjclose") or [{}]
    adj_close = adjclose_entries[0].get("adjclose") or [None] * len(timestamps)

    lengths = {
        "timestamp": len(timestamps),
        "open": len(open_),
        "high": len(high),
        "low": len(low),
        "close": len(close),
        "volume": len(volume),
        "adjclose": len(adj_close),
    }
    if len(set(lengths.values())) > 1:
        msg = "Chart response arrays disagree in length"
        raise IngestionError(msg, symbol=symbol, lengths=lengths)

    symbol_upper = symbol.strip().upper()
    ingestion_timestamp = dt.datetime.now(tz=dt.UTC)
    trade_dates = [
        dt.datetime.fromtimestamp(ts, tz=dt.UTC).date() if ts is not None else None
        for ts in timestamps
    ]

    return (
        pl.DataFrame(
            {
                "trade_date": trade_dates,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "adj_close": adj_close,
            },
            schema_overrides={
                "trade_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "adj_close": pl.Float64,
            },
        )
        .with_columns(
            symbol=pl.lit(symbol_upper),
            source_system=pl.lit(_SOURCE_SYSTEM),
            ingestion_timestamp=pl.lit(ingestion_timestamp),
        )
        .select(
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
        )
    )
