"""Structured logging for the MarketPulse platform.

Why stdlib ``logging`` rather than ``structlog`` or ``loguru``
-------------------------------------------------------------
This module is imported by code that runs in three places: a developer laptop,
a GitHub Actions runner, and a Databricks cluster. On a Databricks cluster the
driver and executor log streams are owned by the runtime, which installs its
own handlers on the root logger and ships records to the cluster's log sinks.
A third-party logging framework that bypasses ``logging`` (as ``loguru`` does)
loses those records. Building on ``logging`` means the platform's structured
events are captured by whatever handler the host already installed, so the same
code path works unchanged in all three environments and adds no runtime
dependency to the wheel.

Design
------
* Two renderers. ``console`` is aligned and human-scannable for local work;
  ``json`` emits one object per line for log aggregators. CI and any deployed
  surface use ``json``.
* Context lives in a :class:`~contextvars.ContextVar`, so a ``run_id`` or
  ``symbol`` bound once at the top of a pipeline appears on every downstream
  record without being threaded through call signatures.

  ``ContextVar`` is used rather than a module-level dict for isolation, not for
  propagation. Each thread and each :class:`asyncio.Task` sees its own value, so
  a binding made while handling one symbol cannot contaminate a record emitted
  concurrently for another. A plain dict would be shared mutable state and would
  do exactly that.

  Propagation differs by concurrency primitive, and the difference is easy to
  get wrong: an :class:`asyncio.Task` inherits a copy of the context at creation
  time, but :class:`~concurrent.futures.ThreadPoolExecutor` and
  :class:`threading.Thread` do **not** — a worker starts with an empty context.
  Use :func:`in_current_context` to carry bindings across a thread boundary
  explicitly.
* Configuration attaches handlers to the ``marketpulse`` logger and sets
  ``propagate = False``. The library never touches the root logger, so an
  application embedding this package keeps control of its own logging.
"""

from __future__ import annotations

import contextlib
import contextvars
import datetime as dt
import functools
import json
import logging
import os
import sys
import types
from typing import TYPE_CHECKING, Any, Final, Literal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

__all__ = [
    "LogFormat",
    "bind",
    "clear_context",
    "configure_logging",
    "current_context",
    "get_logger",
    "in_current_context",
    "log_context",
]

LogFormat = Literal["console", "json"]

ROOT_LOGGER_NAME: Final = "marketpulse"
"""Namespace this package logs under. Never the root logger."""

_ENV_LEVEL: Final = "MARKETPULSE_LOG_LEVEL"
_ENV_FORMAT: Final = "MARKETPULSE_LOG_FORMAT"

_EMPTY_CONTEXT: Final[Mapping[str, Any]] = types.MappingProxyType({})
"""Immutable default. A mutable ``{}`` default is shared across every context.

``ContextVar`` does not copy its default, so a dict default would be one object
visible to every thread and task. Any code that mutated it in place would
silently alter the baseline for the whole process.
"""

_context: contextvars.ContextVar[Mapping[str, Any]] = contextvars.ContextVar(
    "marketpulse_log_context",
    default=_EMPTY_CONTEXT,
)

# Attributes present on every LogRecord. Anything outside this set arrived via
# ``logger.info(..., extra={...})`` and is treated as a structured field.
# Derived from logging.LogRecord rather than hand-copied so it cannot drift as
# the stdlib evolves.
_RESERVED: Final[frozenset[str]] = frozenset(
    logging.LogRecord(
        name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
    ).__dict__
) | {"message", "asctime", "taskName"}


def current_context() -> dict[str, Any]:
    """Return a copy of the structured fields currently bound to this context."""
    return dict(_context.get())


def bind(**fields: Any) -> contextvars.Token[Mapping[str, Any]]:
    """Merge fields into the ambient logging context.

    Prefer :func:`log_context` where the scope is lexical. Use this directly
    only at a process or task entry point, where the binding should outlive the
    enclosing function.

    Args:
        **fields: Structured fields to attach to subsequent log records.

    Returns:
        A token that restores the previous context when passed to
        ``contextvars.ContextVar.reset``.
    """
    return _context.set({**_context.get(), **fields})


def clear_context() -> contextvars.Token[Mapping[str, Any]]:
    """Drop all bound fields from the ambient logging context.

    Returns:
        A token that restores the previous context.
    """
    return _context.set(_EMPTY_CONTEXT)


@contextlib.contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind structured fields for the duration of a block.

    Nests correctly and is exception-safe: the previous context is restored on
    the way out regardless of how the block exits.

    Args:
        **fields: Structured fields to attach to records emitted in the block.

    Yields:
        ``None``. The binding is ambient, not passed to the caller.

    Example:
        >>> configure_logging(level="CRITICAL")
        >>> log = get_logger(__name__)
        >>> with log_context(run_id="r-1"):
        ...     log.info("ingest.started", extra={"symbol": "SPY"})
    """
    token = bind(**fields)
    try:
        yield
    finally:
        _context.reset(token)


def in_current_context[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """Wrap a callable so it runs with the caller's logging context.

    ``ThreadPoolExecutor.submit`` and ``threading.Thread`` do not copy
    context variables: the worker begins with an empty context, so a ``run_id``
    bound on the submitting thread would be absent from every record the worker
    emits. Wrapping the callable captures the context at wrap time and re-enters
    it inside the worker.

    Args:
        func: Callable to wrap. Must be wrapped on the thread whose context
            should be carried, not inside the worker.

    Returns:
        A callable that runs ``func`` under a copy of the captured context.

    Example:
        Wrap on the submitting thread, then hand the wrapper to the pool::

            with log_context(run_id="r-1"), ThreadPoolExecutor(4) as pool:
                pool.map(in_current_context(fetch_bars), symbols)
    """
    captured = contextvars.copy_context()

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        # copy_context() returns a snapshot that cannot be entered twice
        # concurrently, so each invocation runs in its own copy.
        return captured.copy().run(func, *args, **kwargs)

    return wrapper


class _ContextFilter(logging.Filter):
    """Copy the ambient context onto each record as it is emitted.

    Implemented as a filter rather than a ``LoggerAdapter`` so that context is
    captured at emit time on records originating anywhere in the tree,
    including from loggers obtained before the context was bound.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Attach ambient context to ``record`` without filtering anything out."""
        ctx = _context.get()
        if ctx:
            # Explicit ``extra=`` on the call site is more specific than ambient
            # context, so it wins.
            for key, value in ctx.items():
                if key not in record.__dict__:
                    record.__dict__[key] = value
        return True


def _record_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Extract the structured (non-stdlib) fields carried by a record."""
    return {k: v for k, v in record.__dict__.items() if k not in _RESERVED}


def _format_timestamp(created: float) -> str:
    """Render a record timestamp as an RFC 3339 UTC string.

    Always UTC. Market data spans exchange timezones and CI runners are
    configured arbitrarily; local-time logs cannot be correlated across them.
    """
    return (
        dt.datetime.fromtimestamp(created, tz=dt.UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class JsonFormatter(logging.Formatter):
    """Render records as single-line JSON objects.

    One object per line, so the output is directly ingestible by CloudWatch
    Logs Insights or the Databricks log sink without a multiline parser.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Serialise ``record`` to a single-line JSON object."""
        payload: dict[str, Any] = {
            "timestamp": _format_timestamp(record.created),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(_record_fields(record))
        payload["source"] = f"{record.module}.{record.funcName}:{record.lineno}"

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        # default=str keeps the logging path from ever raising on an
        # unserialisable field. Losing a log record is bad; failing a pipeline
        # because a log field was not JSON-serialisable is worse.
        return json.dumps(payload, default=str, separators=(",", ":"))


class ConsoleFormatter(logging.Formatter):
    """Render records as aligned, human-scannable single lines."""

    _TEMPLATE: Final = "{ts} {level:<8} {logger:<38} {event}"

    def format(self, record: logging.LogRecord) -> str:
        """Render ``record`` for terminal consumption."""
        line = self._TEMPLATE.format(
            ts=_format_timestamp(record.created),
            level=record.levelname,
            logger=record.name,
            event=record.getMessage(),
        )
        if fields := _record_fields(record):
            line += "  " + " ".join(f"{k}={v!r}" for k, v in sorted(fields.items()))
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        if record.stack_info:
            line += "\n" + self.formatStack(record.stack_info)
        return line


def _resolve_level(level: int | str | None) -> int:
    """Resolve an effective log level from an argument or the environment."""
    raw = level if level is not None else os.environ.get(_ENV_LEVEL, "INFO")
    if isinstance(raw, int):
        return raw
    resolved = logging.getLevelNamesMapping().get(raw.strip().upper())
    if resolved is None:
        msg = f"Unknown log level {raw!r}"
        raise ValueError(msg)
    return resolved


def _resolve_format(fmt: LogFormat | None) -> LogFormat:
    """Resolve the renderer, defaulting to JSON anywhere that is not a TTY.

    A TTY implies a human is reading; anything else (CI, a Databricks job, a
    container) implies a machine is, and should get JSON.
    """
    raw = fmt if fmt is not None else os.environ.get(_ENV_FORMAT)
    if raw is None:
        return "console" if sys.stderr.isatty() else "json"
    normalised = raw.strip().lower()
    if normalised not in ("console", "json"):
        msg = f"Unknown log format {raw!r}; expected 'console' or 'json'"
        raise ValueError(msg)
    return normalised  # type: ignore[return-value]


def configure_logging(
    *,
    level: int | str | None = None,
    fmt: LogFormat | None = None,
    stream: Any | None = None,
) -> logging.Logger:
    """Install handlers on the ``marketpulse`` logger. Idempotent.

    Call once from an entry point (CLI, job driver, test fixture). Library
    modules must only call :func:`get_logger`; a library that configures
    handlers steals control from the application embedding it.

    Repeated calls replace existing handlers rather than stacking them, which
    makes this safe to call from a per-test fixture without producing duplicate
    output.

    Args:
        level: Threshold as a name or numeric value. Defaults to
            ``$MARKETPULSE_LOG_LEVEL``, then ``INFO``.
        fmt: ``"console"`` or ``"json"``. Defaults to ``$MARKETPULSE_LOG_FORMAT``,
            then ``console`` on a TTY and ``json`` otherwise.
        stream: Destination stream. Defaults to ``sys.stderr`` so that stdout
            stays clean for machine-readable command output.

    Returns:
        The configured ``marketpulse`` logger.

    Raises:
        ValueError: If ``level`` or ``fmt`` is not recognised.
    """
    resolved_level = _resolve_level(level)
    resolved_fmt = _resolve_format(fmt)

    logger = logging.getLogger(ROOT_LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonFormatter() if resolved_fmt == "json" else ConsoleFormatter())
    handler.addFilter(_ContextFilter())

    logger.addHandler(handler)
    logger.setLevel(resolved_level)
    # The application owns the root logger, not this package.
    logger.propagate = False
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger inside the ``marketpulse`` namespace.

    Args:
        name: Typically ``__name__``. A module's own dotted name is mapped into
            the package namespace so that a single level threshold governs the
            whole platform. ``None`` returns the namespace root.

    Returns:
        A standard library logger.
    """
    if not name or name == ROOT_LOGGER_NAME:
        return logging.getLogger(ROOT_LOGGER_NAME)
    if name.startswith(f"{ROOT_LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")
