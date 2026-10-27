"""Structured logging: rendering, context propagation, and handler hygiene."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import io  # noqa: TC003  -- io.StringIO is instantiated at runtime in a fixture
import json
import logging
from typing import Any

import pytest

from marketpulse.core.logging import (
    ROOT_LOGGER_NAME,
    bind,
    clear_context,
    configure_logging,
    current_context,
    get_logger,
    in_current_context,
    log_context,
)


def _records(buffer: io.StringIO) -> list[dict[str, Any]]:
    """Parse captured JSON log lines."""
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


@pytest.fixture
def json_log(log_stream: io.StringIO) -> io.StringIO:
    """Configure JSON logging into a capture buffer."""
    configure_logging(level="DEBUG", fmt="json", stream=log_stream)
    return log_stream


@pytest.fixture
def console_log(log_stream: io.StringIO) -> io.StringIO:
    """Configure console logging into a capture buffer."""
    configure_logging(level="DEBUG", fmt="console", stream=log_stream)
    return log_stream


class TestJsonRendering:
    def test_one_object_per_line(self, json_log: io.StringIO) -> None:
        """Aggregators parse line-delimited JSON; a multiline object breaks them."""
        log = get_logger("test")
        log.info("first.event")
        log.info("second.event")
        lines = json_log.getvalue().strip().splitlines()
        assert len(lines) == 2
        assert all(json.loads(line) for line in lines)

    def test_required_fields_are_present(self, json_log: io.StringIO) -> None:
        get_logger("test").info("ingest.started")
        record = _records(json_log)[0]
        assert record["event"] == "ingest.started"
        assert record["level"] == "INFO"
        assert record["logger"] == f"{ROOT_LOGGER_NAME}.test"
        assert "timestamp" in record
        assert "source" in record

    def test_timestamp_is_utc(self, json_log: io.StringIO) -> None:
        """Local-time logs cannot be correlated across exchanges and CI runners."""
        get_logger("test").info("event")
        assert _records(json_log)[0]["timestamp"].endswith("Z")

    def test_extra_fields_are_promoted_to_top_level(self, json_log: io.StringIO) -> None:
        get_logger("test").info("bars.fetched", extra={"symbol": "SPY", "rows": 251})
        record = _records(json_log)[0]
        assert record["symbol"] == "SPY"
        assert record["rows"] == 251

    def test_unserialisable_field_does_not_raise(self, json_log: io.StringIO) -> None:
        """Losing a log line is bad; failing a pipeline over one is worse."""

        class Opaque:
            def __repr__(self) -> str:
                return "<opaque>"

        get_logger("test").info("event", extra={"thing": Opaque()})
        assert _records(json_log)[0]["thing"] == "<opaque>"

    def test_exception_includes_traceback(self, json_log: io.StringIO) -> None:
        log = get_logger("test")
        try:
            raise ValueError("boom")
        except ValueError:
            log.exception("operation.failed")
        record = _records(json_log)[0]
        assert "ValueError: boom" in record["exception"]
        assert record["level"] == "ERROR"


class TestConsoleRendering:
    def test_event_and_fields_appear(self, console_log: io.StringIO) -> None:
        get_logger("test").info("bars.fetched", extra={"symbol": "SPY"})
        output = console_log.getvalue()
        assert "bars.fetched" in output
        assert "symbol='SPY'" in output

    def test_output_is_not_json(self, console_log: io.StringIO) -> None:
        get_logger("test").info("event")
        with pytest.raises(json.JSONDecodeError):
            json.loads(console_log.getvalue().strip())


class TestContextPropagation:
    def test_bound_fields_appear_on_records(self, json_log: io.StringIO) -> None:
        with log_context(run_id="r-1"):
            get_logger("test").info("event")
        assert _records(json_log)[0]["run_id"] == "r-1"

    def test_context_is_removed_after_the_block(self, json_log: io.StringIO) -> None:
        with log_context(run_id="r-1"):
            pass
        get_logger("test").info("event")
        assert "run_id" not in _records(json_log)[0]

    def test_contexts_nest(self, json_log: io.StringIO) -> None:
        with log_context(run_id="r-1"), log_context(symbol="SPY"):
            get_logger("test").info("event")
        record = _records(json_log)[0]
        assert record["run_id"] == "r-1"
        assert record["symbol"] == "SPY"

    def test_inner_context_does_not_leak_outward(self, json_log: io.StringIO) -> None:
        log = get_logger("test")
        with log_context(run_id="r-1"):
            with log_context(symbol="SPY"):
                pass
            log.info("event")
        assert "symbol" not in _records(json_log)[0]

    def test_context_is_restored_after_an_exception(self, json_log: io.StringIO) -> None:
        """Exception safety matters: a leaked binding mislabels later records."""
        with pytest.raises(RuntimeError), log_context(run_id="r-1"):
            raise RuntimeError("boom")
        assert current_context() == {}

    def test_explicit_extra_beats_ambient_context(self, json_log: io.StringIO) -> None:
        """The more specific value at the call site should win."""
        with log_context(symbol="SPY"):
            get_logger("test").info("event", extra={"symbol": "QQQ"})
        assert _records(json_log)[0]["symbol"] == "QQQ"

    def test_context_applies_to_loggers_obtained_before_binding(
        self, json_log: io.StringIO
    ) -> None:
        """Context is captured at emit time, not at logger-construction time."""
        log = get_logger("test")
        with log_context(run_id="r-1"):
            log.info("event")
        assert _records(json_log)[0]["run_id"] == "r-1"

    def test_context_does_not_cross_a_thread_boundary_implicitly(
        self, json_log: io.StringIO
    ) -> None:
        """ThreadPoolExecutor workers start with an empty context.

        Unlike an asyncio.Task, a pool worker does not inherit the submitting
        thread's context variables. Asserted explicitly because the opposite is
        a common and plausible-sounding assumption, and a test that encodes the
        real semantics is what keeps the documentation honest.
        """
        log = get_logger("test")

        def emit() -> None:
            log.info("worker.event")

        with log_context(run_id="r-1"), ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: emit(), range(2)))

        records = _records(json_log)
        assert len(records) == 2
        assert all("run_id" not in record for record in records)

    def test_in_current_context_carries_bindings_into_workers(self, json_log: io.StringIO) -> None:
        """The explicit wrapper is how a run_id survives a fan-out."""
        log = get_logger("test")

        def emit(symbol: str) -> str:
            log.info("worker.event", extra={"symbol": symbol})
            return symbol

        with log_context(run_id="r-1"), ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(in_current_context(emit), ["SPY", "QQQ"])) == [
                "QQQ",
                "SPY",
            ]

        records = _records(json_log)
        assert len(records) == 2
        assert all(record["run_id"] == "r-1" for record in records)
        assert {record["symbol"] for record in records} == {"SPY", "QQQ"}

    def test_wrapper_does_not_leak_bindings_back_to_the_caller(self, json_log: io.StringIO) -> None:
        """A worker that binds its own fields must not mutate the caller's context."""
        log = get_logger("test")

        def emit() -> None:
            bind(worker_local=True)
            log.info("worker.event")

        with log_context(run_id="r-1"), ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(in_current_context(emit)).result()
            assert "worker_local" not in current_context()

    def test_clear_context_drops_all_fields(self) -> None:
        bind(run_id="r-1")
        clear_context()
        assert current_context() == {}

    def test_current_context_returns_a_copy(self) -> None:
        """A caller must not be able to mutate the live context by accident."""
        with log_context(run_id="r-1"):
            snapshot = current_context()
            snapshot["injected"] = True
            assert "injected" not in current_context()


class TestConfiguration:
    def test_repeated_calls_do_not_stack_handlers(self, log_stream: io.StringIO) -> None:
        """A per-test fixture calls this repeatedly; duplicates would double output."""
        for _ in range(3):
            configure_logging(level="INFO", fmt="json", stream=log_stream)
        assert len(logging.getLogger(ROOT_LOGGER_NAME).handlers) == 1

    def test_package_logger_does_not_propagate(self, json_log: io.StringIO) -> None:
        """The application owns the root logger; this library must not touch it."""
        assert logging.getLogger(ROOT_LOGGER_NAME).propagate is False

    def test_root_logger_is_left_alone(self, log_stream: io.StringIO) -> None:
        before = list(logging.getLogger().handlers)
        configure_logging(level="INFO", fmt="json", stream=log_stream)
        assert logging.getLogger().handlers == before

    def test_level_filters_records(self, log_stream: io.StringIO) -> None:
        configure_logging(level="WARNING", fmt="json", stream=log_stream)
        log = get_logger("test")
        log.debug("suppressed")
        log.info("suppressed")
        log.warning("emitted")
        records = _records(log_stream)
        assert [record["event"] for record in records] == ["emitted"]

    def test_level_can_be_numeric(self, log_stream: io.StringIO) -> None:
        configure_logging(level=logging.ERROR, fmt="json", stream=log_stream)
        assert logging.getLogger(ROOT_LOGGER_NAME).level == logging.ERROR

    def test_env_var_supplies_the_level(
        self, log_stream: io.StringIO, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MARKETPULSE_LOG_LEVEL", "ERROR")
        configure_logging(fmt="json", stream=log_stream)
        assert logging.getLogger(ROOT_LOGGER_NAME).level == logging.ERROR

    def test_env_var_supplies_the_format(
        self, log_stream: io.StringIO, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MARKETPULSE_LOG_FORMAT", "json")
        configure_logging(level="INFO", stream=log_stream)
        get_logger("test").info("event")
        assert _records(log_stream)[0]["event"] == "event"

    @pytest.mark.parametrize("level", ["CHATTY", "verbose"])
    def test_unknown_level_is_rejected(self, level: str, log_stream: io.StringIO) -> None:
        with pytest.raises(ValueError, match="Unknown log level"):
            configure_logging(level=level, stream=log_stream)

    def test_unknown_format_is_rejected(
        self, log_stream: io.StringIO, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MARKETPULSE_LOG_FORMAT", "xml")
        with pytest.raises(ValueError, match="Unknown log format"):
            configure_logging(level="INFO", stream=log_stream)


class TestLoggerNaming:
    def test_module_name_is_mapped_into_the_namespace(self) -> None:
        """One level threshold must govern the whole platform."""
        assert get_logger("analytics.session").name == f"{ROOT_LOGGER_NAME}.analytics.session"

    def test_already_namespaced_name_is_not_doubled(self) -> None:
        name = f"{ROOT_LOGGER_NAME}.analytics"
        assert get_logger(name).name == name

    @pytest.mark.parametrize("name", [None, "", ROOT_LOGGER_NAME])
    def test_empty_name_returns_the_namespace_root(self, name: str | None) -> None:
        assert get_logger(name).name == ROOT_LOGGER_NAME
