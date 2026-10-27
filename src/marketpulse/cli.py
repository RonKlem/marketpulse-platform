"""Command-line entry point for the MarketPulse platform.

Built on ``argparse`` rather than Click or Typer. The CLI's job is to resolve
configuration, install logging, and dispatch — it is not where the platform's
value lives, and a third-party dependency that ships with the wheel onto a
Databricks cluster has to earn its place. ``argparse`` is in the standard
library and is sufficient for this surface.

Conventions
-----------
* Machine-readable output goes to stdout; logs and diagnostics go to stderr.
  That separation is what makes ``marketpulse config show --json | jq`` work
  while logging is still visible in the same terminal.
* Exit codes are meaningful: ``0`` success, ``1`` a deliberate platform error,
  ``2`` argument misuse (argparse's own convention), ``130`` interrupted.
* A :class:`~marketpulse.core.errors.MarketPulseError` is reported as a clean
  one-line failure. Anything else propagates with its traceback intact, because
  an unexpected exception is a defect and hiding the traceback makes it harder
  to fix.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING, Any, Final

from marketpulse import __version__
from marketpulse.config.models import Environment, MedallionLayer
from marketpulse.config.settings import describe_layers, load_config
from marketpulse.core.errors import MarketPulseError
from marketpulse.core.logging import configure_logging, get_logger, log_context

if TYPE_CHECKING:
    from collections.abc import Sequence

    from marketpulse.config.models import PlatformConfig

__all__ = ["main"]

_log = get_logger(__name__)

EXIT_OK: Final = 0
EXIT_ERROR: Final = 1
EXIT_INTERRUPTED: Final = 130


def _to_jsonable(value: Any) -> Any:
    """Convert a config object tree into JSON-serialisable primitives."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: _to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Environment | MedallionLayer):
        return value.value
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_to_jsonable(v) for v in value]
    return value


def _print_table(rows: Sequence[tuple[str, str]], *, indent: str = "  ") -> None:
    """Print aligned key/value rows to stdout."""
    if not rows:
        return
    width = max(len(key) for key, _ in rows)
    for key, value in rows:
        print(f"{indent}{key.ljust(width)}  {value}")


def _flatten(prefix: str, value: Any) -> list[tuple[str, str]]:
    """Flatten a nested mapping into sorted dotted-key rows."""
    if isinstance(value, dict):
        rows: list[tuple[str, str]] = []
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten(child, value[key]))
        return rows
    rendered = "<unset>" if value is None else str(value)
    return [(prefix, rendered)]


def _cmd_config(args: argparse.Namespace, config: PlatformConfig) -> int:
    """Print the resolved configuration and the layers it came from."""
    payload = _to_jsonable(config)

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return EXIT_OK

    print(f"environment: {config.environment.value}")
    print("\nresolved configuration:")
    _print_table(_flatten("", payload))

    print("\nconfiguration layers (applied in order):")
    layer_rows = [
        (str(path), "loaded" if exists else "absent")
        for path, exists in describe_layers(config.environment)
    ]
    _print_table(layer_rows)
    print(
        "\nEnvironment variables prefixed MARKETPULSE_ override all files."
        "\nNesting uses a double underscore: MARKETPULSE_ANALYTICS__THREADS=4"
    )
    return EXIT_OK


def _cmd_doctor(args: argparse.Namespace, config: PlatformConfig) -> int:
    """Report environment, engine versions, and settings the engine accepted.

    The reason this command exists rather than being a documented set of
    one-liners: DuckDB silently clamps or ignores some settings, so the value
    requested in a config file is not reliably the value in force. Reading them
    back from ``duckdb_settings()`` is the only way to know, and that
    discrepancy is the first thing worth checking when a query spills or runs
    single-threaded unexpectedly.
    """
    import duckdb
    import polars as pl
    import pyarrow as pa

    # Imported lazily and locally: this is the one command that needs engine
    # version introspection, and `marketpulse --help` should not pay for
    # importing three columnar engines.
    from marketpulse.analytics.session import open_session
    from marketpulse.config.models import available_cpus

    rows: list[tuple[str, str]] = [
        ("marketpulse", __version__),
        ("python", sys.version.split()[0]),
        ("platform", sys.platform),
        ("polars", pl.__version__),
        ("duckdb", duckdb.__version__),
        ("pyarrow", pa.__version__),
        ("environment", config.environment.value),
        ("available cpus", str(available_cpus())),
        ("storage root", str(config.storage.root)),
        ("storage root exists", str(config.storage.root.exists())),
    ]
    print("platform:")
    _print_table(rows)

    with open_session(config) as session:
        effective = session.settings()

    print("\nduckdb settings in force (read back from the engine):")
    _print_table([(k, str(v)) for k, v in sorted(effective.items())])

    requested = config.analytics.memory_limit
    applied = effective.get("memory_limit")
    if requested and applied and not _memory_matches(requested, applied):
        print(
            f"\nnote: requested memory_limit {requested!r} but the engine reports "
            f"{applied!r}. DuckDB normalises and clamps this value.",
            file=sys.stderr,
        )

    if args.json:
        print(json.dumps({"platform": dict(rows), "duckdb": dict(effective)}, indent=2))
    return EXIT_OK


_SIZE_UNITS: Final[dict[str, int]] = {
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "TB": 10**12,
    "KIB": 2**10,
    "MIB": 2**20,
    "GIB": 2**30,
    "TIB": 2**40,
}


def _parse_size(value: str) -> float | None:
    """Parse a size string such as ``8GB`` or ``7.4 GiB`` into bytes.

    Returns:
        The size in bytes, or ``None`` if the string is not a recognised size.
    """
    match = re.fullmatch(r"\s*([\d.]+)\s*([KMGT]i?B)\s*", value, re.IGNORECASE)
    if match is None:
        return None
    magnitude, unit = match.groups()
    return float(magnitude) * _SIZE_UNITS[unit.upper()]


def _memory_matches(requested: str, applied: str) -> bool:
    """Report whether two DuckDB size strings denote the same budget.

    DuckDB stores ``memory_limit`` in bytes and echoes it back in binary units,
    so a requested ``8GB`` (8e9 bytes, decimal) is reported as ``7.4 GiB``.
    Those are the same budget, and comparing the strings would flag a
    discrepancy on every correctly configured session. Comparing parsed byte
    counts with a 1% tolerance absorbs the rounding in DuckDB's one-decimal
    display while still catching an actual clamp.
    """
    want, got = _parse_size(requested), _parse_size(applied)
    if want is None or got is None:
        return requested.strip().casefold() == applied.strip().casefold()
    return abs(want - got) <= 0.01 * want


def _cmd_query(args: argparse.Namespace, config: PlatformConfig) -> int:
    """Run a SQL query against the local lakehouse."""
    from marketpulse.analytics.session import open_session

    with open_session(config) as session:
        if args.dataset:
            layer = MedallionLayer(args.layer)
            session.register(args.dataset, session.scan(layer, args.dataset))
        frame = session.sql(args.sql)

    if args.json:
        print(json.dumps(frame.to_dicts(), indent=2, default=str))
    else:
        print(frame)
    return EXIT_OK


def _build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser."""
    parser = argparse.ArgumentParser(
        prog="marketpulse",
        description="MarketPulse platform CLI.",
        epilog=(
            "Machine-readable output goes to stdout and logs go to stderr, so "
            "'marketpulse config show --json | jq' works with logging enabled."
        ),
    )
    parser.add_argument("--version", action="version", version=f"marketpulse {__version__}")
    parser.add_argument(
        "--env",
        dest="environment",
        choices=[e.value for e in Environment],
        help="Target environment. Defaults to $MARKETPULSE_ENV, then 'local'.",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Override the configured log level for this invocation.",
    )
    parser.add_argument(
        "--log-format",
        choices=["console", "json"],
        default=None,
        help="Override the log renderer. Defaults to console on a TTY, json otherwise.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    config_parser = subparsers.add_parser(
        "config",
        help="Show resolved configuration and its source layers.",
    )
    config_parser.add_argument(
        "action",
        nargs="?",
        default="show",
        choices=["show"],
        help="Reserved for future subcommands such as 'validate'.",
    )
    config_parser.add_argument("--json", action="store_true", help="Emit JSON.")
    config_parser.set_defaults(handler=_cmd_config)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Report environment, engine versions, and settings actually in force.",
    )
    doctor_parser.add_argument("--json", action="store_true", help="Also emit JSON.")
    doctor_parser.set_defaults(handler=_cmd_doctor)

    query_parser = subparsers.add_parser("query", help="Run SQL against the local lakehouse.")
    query_parser.add_argument("sql", help="SQL to execute.")
    query_parser.add_argument(
        "--dataset",
        help="Register a medallion dataset under its own name before querying.",
    )
    query_parser.add_argument(
        "--layer",
        default=MedallionLayer.GOLD.value,
        choices=[layer.value for layer in MedallionLayer],
        help="Medallion zone for --dataset. Defaults to gold.",
    )
    query_parser.add_argument("--json", action="store_true", help="Emit JSON rows.")
    query_parser.set_defaults(handler=_cmd_query)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI.

    Args:
        argv: Argument vector excluding the program name. Defaults to
            ``sys.argv[1:]``. Injectable so that tests exercise the real
            dispatch path rather than calling handlers directly.

    Returns:
        A process exit code.
    """
    args = _build_parser().parse_args(argv)

    try:
        config = load_config(environment=args.environment)
    except MarketPulseError as exc:
        # Logging is not configured yet, because its level comes from the
        # configuration that just failed to load. Write directly to stderr.
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_ERROR

    configure_logging(
        level=args.log_level or config.logging.level,
        fmt=args.log_format or config.logging.format,  # type: ignore[arg-type]
    )

    try:
        with log_context(command=args.command, environment=config.environment.value):
            return int(args.handler(args, config))
    except MarketPulseError as exc:
        _log.error("cli.failed", extra={"error": str(exc), "error_type": type(exc).__name__})
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
