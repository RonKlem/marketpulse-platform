"""Shared fixtures.

Two rules govern fixtures in this repository.

**No test touches the real environment.** ``load_config`` accepts an
``environ`` mapping and a ``config_dir`` precisely so that tests inject them
rather than mutating ``os.environ``. A test that sets a real environment
variable leaks into every test that runs after it, and the resulting failure
depends on execution order, which makes it nearly impossible to reproduce from
a CI log.

**No test reaches the network.** A test that calls a market data API fails when
the market is closed, when the vendor rate-limits a CI runner, and on an
aeroplane. Source connectors are tested against recorded fixtures; the live
call is exercised by a separate, explicitly-marked contract test.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from marketpulse.analytics.session import AnalyticsSession, open_session
from marketpulse.config.models import (
    AnalyticsConfig,
    Environment,
    LoggingConfig,
    PlatformConfig,
    StorageConfig,
)
from marketpulse.config.settings import clear_config_cache
from marketpulse.core.logging import ROOT_LOGGER_NAME, clear_context

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(autouse=True)
def _isolate_global_state() -> Iterator[None]:
    """Reset process-global state around every test.

    Autouse because the state reset here is invisible at the call site: a test
    that passes alone and fails in a full run is almost always caused by one of
    these three leaks, and relying on each test to remember them does not scale.
    """
    clear_config_cache()
    clear_context()
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    saved_handlers = list(logger.handlers)
    saved_level = logger.level
    try:
        yield
    finally:
        clear_config_cache()
        clear_context()
        logger.handlers = saved_handlers
        logger.setLevel(saved_level)


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    """Return an empty configuration directory.

    Tests write only the layers they care about, so a resolution test exercises
    exactly the precedence it asserts rather than inheriting the repository's
    committed defaults.
    """
    directory = tmp_path / "config"
    directory.mkdir()
    return directory


@pytest.fixture
def storage_root(tmp_path: Path) -> Path:
    """Return a lakehouse root under ``tmp_path``."""
    root = tmp_path / "lakehouse"
    root.mkdir()
    return root


@pytest.fixture
def config(storage_root: Path) -> PlatformConfig:
    """Return a deterministic configuration for engine tests.

    ``threads=2`` and a small ``memory_limit`` are fixed rather than derived
    from the host so that behaviour does not vary between a 16-core workstation
    and a 2-vCPU CI runner. ``preserve_insertion_order=True`` makes row-order
    assertions meaningful.
    """
    return PlatformConfig(
        environment=Environment.CI,
        storage=StorageConfig(root=storage_root, create_missing=True),
        analytics=AnalyticsConfig(
            threads=2,
            memory_limit="512MB",
            preserve_insertion_order=True,
            enable_progress_bar=False,
        ),
        logging=LoggingConfig(level="DEBUG", format="json"),
    )


@pytest.fixture
def session(config: PlatformConfig) -> Iterator[AnalyticsSession]:
    """Yield an open analytics session backed by a temporary lakehouse."""
    with open_session(config) as open_handle:
        yield open_handle


@pytest.fixture
def bars() -> pl.DataFrame:
    """Return a small OHLCV frame.

    Timestamps are timezone-aware UTC. Naive datetimes are banned by the ``DTZ``
    lint rules because equity bars are only interpretable against an exchange
    session, and a naive timestamp silently adopts whatever timezone the
    reader's machine is set to.
    """
    return pl.DataFrame(
        {
            "symbol": ["SPY", "SPY", "QQQ", "QQQ", "IWM"],
            "trade_date": pl.Series(
                ["2026-01-02", "2026-01-05", "2026-01-02", "2026-01-05", "2026-01-02"],
            ).str.to_date(),
            "close": [472.65, 475.10, 402.33, 405.88, 198.42],
            "volume": [74_120_000, 68_905_000, 41_002_000, 39_887_000, 28_440_000],
        }
    )


@pytest.fixture
def log_stream() -> Iterator[Any]:
    """Yield a text buffer that captures configured log output.

    ``caplog`` attaches to the root logger, but this package sets
    ``propagate = False`` on the ``marketpulse`` logger deliberately, so
    ``caplog`` sees nothing. Capturing the configured handler's own stream is
    what actually verifies the rendered output, including the JSON shape that
    downstream log aggregators depend on.
    """
    import io

    buffer = io.StringIO()
    yield buffer
    buffer.close()
