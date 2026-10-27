"""Typed configuration objects.

Why frozen dataclasses instead of Pydantic
------------------------------------------
Pydantic would shorten this file, and on a greenfield service it would be the
right call. Two constraints here push the other way.

1. The wheel is installed on Databricks clusters. Databricks Runtime pins its
   own ``pydantic`` (DBR 14-16 ship v1 or v2 depending on the version), and a
   library that declares an incompatible range either fails to resolve or
   silently shadows the runtime's copy for every other library on the cluster.
   Validation logic is not worth that blast radius.
2. ``pydantic-core`` is a compiled extension. Keeping the runtime dependency
   set to the three libraries that do real work (Polars, DuckDB, PyArrow) keeps
   cold-start install time on an ephemeral job cluster measurable.

The cost is roughly sixty lines of hand-written validation and no automatic
coercion from strings, which :mod:`marketpulse.config.settings` handles
explicitly at the environment-variable boundary. That is an acceptable trade
for a config module that is read far more often than it is edited.

Every object here is ``frozen=True`` and ``slots=True``. Immutability means a
resolved config can be passed to a thread pool or captured in a closure without
anyone wondering whether a downstream caller mutated it; ``slots`` makes
attribute typos an ``AttributeError`` at assignment rather than a silently
ignored field.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
import logging
import os
from pathlib import Path
import re
from typing import Final, Self

from marketpulse.core.errors import ConfigurationError

__all__ = [
    "AnalyticsConfig",
    "Environment",
    "LoggingConfig",
    "MedallionLayer",
    "PlatformConfig",
    "StorageConfig",
    "available_cpus",
]

_MEMORY_LIMIT_PATTERN: Final = re.compile(
    r"^\d+(\.\d+)?\s*(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$", re.IGNORECASE
)

_MAX_REASONABLE_THREADS: Final = 256
"""Guards against a typo (``threads = 640``) oversubscribing the box."""


def available_cpus() -> int:
    """Return the CPU count this process is actually allowed to use.

    ``os.process_cpu_count`` is preferred because it respects CPU affinity
    masks and cgroup quotas: in a container or on a CI runner allocated fewer
    CPUs than the host has, ``os.cpu_count`` reports the host's number and
    would oversubscribe the quota.

    It was added in Python 3.13, and this package supports 3.12, so the
    fallback is required rather than defensive. ``sched_getaffinity`` gives the
    same affinity-aware answer on Linux 3.12; ``os.cpu_count`` is the last
    resort on Windows and macOS 3.12, where it over-reports under a quota but
    is the only thing available.

    Returns:
        A positive CPU count, defaulting to 4 if the platform reports nothing.
    """
    if hasattr(os, "process_cpu_count"):
        count = os.process_cpu_count()
    elif hasattr(os, "sched_getaffinity"):
        count = len(os.sched_getaffinity(0))
    else:
        count = os.cpu_count()
    return count or 4


class Environment(StrEnum):
    """Deployment target a process is running against.

    Drives defaults that should never be guessed: ``LOCAL`` tolerates a missing
    dataset and renders logs for humans, ``CI`` must be hermetic and
    machine-readable, ``PROD`` must fail closed.
    """

    LOCAL = "local"
    CI = "ci"
    DEV = "dev"
    PROD = "prod"

    @property
    def is_ephemeral(self) -> bool:
        """Whether state in this environment is expected to be thrown away."""
        return self in (Environment.LOCAL, Environment.CI)


class MedallionLayer(StrEnum):
    """Zones of the medallion architecture, in promotion order.

    ``RAW`` is the immutable landing zone: bytes as the source delivered them,
    never rewritten. ``BRONZE`` is the same data parsed into a typed, columnar
    format. ``SILVER`` is conformed and deduplicated. ``GOLD`` is modelled for
    consumption.
    """

    RAW = "raw"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass(frozen=True, slots=True, kw_only=True)
class StorageConfig:
    """Filesystem layout of the local lakehouse.

    Local paths here are the counterpart of Unity Catalog external locations in
    the distributed stack (ADR-002). Keeping the zone names identical in both
    places means a transformation's input and output identifiers do not change
    when it is promoted to Databricks.

    Attributes:
        root: Base directory containing every medallion zone. Relative paths
            resolve against the repository root, not the working directory, so
            behaviour does not depend on where a command was invoked.
        create_missing: Whether to create zone directories on access. True
            locally, false in ``PROD`` where storage is provisioned by
            Terraform and an unexpected directory indicates a misconfiguration.
    """

    root: Path = Path("data")
    create_missing: bool = True

    def layer_path(self, layer: MedallionLayer) -> Path:
        """Return the directory for a medallion zone.

        Args:
            layer: Zone to resolve.

        Returns:
            Absolute path to the zone directory.
        """
        path = self.root / layer.value
        if self.create_missing:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def dataset_path(self, layer: MedallionLayer, dataset: str) -> Path:
        """Return the directory for one dataset within a zone.

        Args:
            layer: Zone the dataset belongs to.
            dataset: Dataset identifier, e.g. ``equity_daily_bars``.

        Returns:
            Absolute path to the dataset directory.

        Raises:
            ConfigurationError: If ``dataset`` is empty or contains a path
                separator or ``..``. Dataset names reach this method from
                config files and CLI arguments, so a traversal check belongs
                here rather than at every call site.
        """
        if not dataset or dataset != dataset.strip():
            msg = "Dataset name must be a non-empty string without surrounding whitespace"
            raise ConfigurationError(msg, dataset=dataset)
        if "/" in dataset or "\\" in dataset or ".." in dataset:
            msg = "Dataset name must not contain path separators or parent references"
            raise ConfigurationError(msg, dataset=dataset)

        path = self.layer_path(layer) / dataset
        if self.create_missing:
            path.mkdir(parents=True, exist_ok=True)
        return path


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalyticsConfig:
    """Execution limits for the local Polars and DuckDB engines.

    These are the knobs that decide whether a local query completes or dies
    with an out-of-memory error, so they are configuration rather than
    constants.

    Attributes:
        duckdb_database: Path to the DuckDB database file, or ``None`` for an
            in-memory database. In-memory is the default: the local lakehouse
            is Parquet on disk and DuckDB reads it directly, so a persistent
            database file would be a redundant second copy. Set a path only
            when persisting views, macros, or a query cache across sessions.
        threads: Worker threads for DuckDB. Defaults to ``None``, which is
            resolved from the host at load time rather than hardcoded, because
            this value is wrong by construction on any machine other than the
            one it was written for. See
            :meth:`AnalyticsConfig.with_host_defaults` for the policy and the
            measurements behind it.
        memory_limit: DuckDB memory budget as a size string, e.g. ``"24GB"``.
            Below this ceiling DuckDB keeps hash tables and sort buffers in
            memory; above it, it spills to ``temp_directory``. A limit well
            under physical RAM is deliberate: exceeding it degrades to disk,
            whereas omitting it risks the OS OOM-killer terminating the
            process with no diagnostics.
        temp_directory: Spill location for operators that exceed
            ``memory_limit``. ``None`` uses the DuckDB default.
        preserve_insertion_order: When false, DuckDB may reorder result rows,
            which lets it stream large scans without buffering to preserve
            order. Safe for pipelines whose output is explicitly sorted or
            written partitioned; set true only when an unordered result would
            be incorrect.
        enable_progress_bar: DuckDB progress bar for interactive sessions.
    """

    duckdb_database: Path | None = None
    threads: int | None = None
    memory_limit: str | None = None
    temp_directory: Path | None = None
    preserve_insertion_order: bool = False
    enable_progress_bar: bool = False

    def __post_init__(self) -> None:
        """Validate execution limits.

        Raises:
            ConfigurationError: If ``threads`` or ``memory_limit`` is invalid.
        """
        if self.threads is not None and not 1 <= self.threads <= _MAX_REASONABLE_THREADS:
            msg = f"threads must be between 1 and {_MAX_REASONABLE_THREADS}"
            raise ConfigurationError(msg, threads=self.threads)

        if self.memory_limit is not None and not _MEMORY_LIMIT_PATTERN.match(self.memory_limit):
            msg = "memory_limit must be a size string such as '24GB' or '512MB'"
            raise ConfigurationError(msg, memory_limit=self.memory_limit)

    def with_host_defaults(self) -> Self:
        """Fill unset execution limits from the host's CPU topology.

        Thread count defaults to the full available CPU count, not a halved
        approximation of physical cores.

        That choice is empirical, and it overturned the opposite assumption.
        Measured on a 16-thread hybrid Intel Core Ultra 7 265H (6 P-cores,
        10 E-cores) against a 20.1M-row Parquet dataset, with a grouped
        aggregation over 4,000 groups (median of 5 runs, DuckDB 1.5.6):

        =========  ========  =========
        threads    median    speedup
        =========  ========  =========
        1          0.538 s   1.00x
        2          0.283 s   1.91x
        4          0.140 s   3.84x
        6          0.096 s   5.59x
        8          0.076 s   7.06x
        12         0.060 s   8.95x
        16         0.064 s   8.40x
        32         0.064 s   8.40x
        =========  ========  =========

        Two results matter. Scaling stays near-linear well past the physical
        core count and peaks around 12 threads, so capping at ``logical // 2``
        (8 here) would forfeit roughly 20% of achievable throughput. And
        oversubscription *plateaus* rather than regressing — at 32 threads on
        16 logical CPUs the time is unchanged, because DuckDB's scheduler
        bounds concurrent pipeline work by its own morsel queue rather than
        spawning contending OS threads per task.

        The intuition this replaces — that E-cores and SMT siblings would drag
        a hash join's slowest partition and regress wall-clock time — is wrong
        for DuckDB specifically. Morsel-driven parallelism assigns small work
        units dynamically, so a slower core simply completes fewer morsels
        instead of holding up a partition. That intuition does hold for engines
        that statically partition work, which is why it is worth stating as
        rejected rather than leaving unsaid.

        The count comes from :func:`available_cpus`, which is affinity- and
        cgroup-aware so that a container or CI runner with a quota below the
        host's core count is not oversubscribed.

        No cap is applied beyond the validation ceiling. A lower limit here
        would silently throttle a large workstation, and the point at which
        local analytics stops being the right tool is a function of working-set
        size, not core count — that threshold belongs to ADR-002, not to a
        thread default.

        Returns:
            A copy with ``threads`` populated if it was ``None``, otherwise
            ``self`` unchanged.
        """
        if self.threads is not None:
            return self
        return replace(self, threads=max(1, min(available_cpus(), _MAX_REASONABLE_THREADS)))


@dataclass(frozen=True, slots=True, kw_only=True)
class LoggingConfig:
    """Observability settings for the process.

    Attributes:
        level: Threshold name, e.g. ``INFO``.
        format: ``console`` for humans, ``json`` for aggregators, or ``None``
            to detect from whether stderr is a TTY.
    """

    level: str = "INFO"
    format: str | None = None

    def __post_init__(self) -> None:
        """Validate the level name and renderer.

        Raises:
            ConfigurationError: If either value is not recognised. Checked here
                rather than at handler-installation time so that a typo in a
                config file fails at startup, not on the first log call.
        """
        if self.level.upper() not in logging.getLevelNamesMapping():
            msg = "Unknown log level"
            raise ConfigurationError(msg, level=self.level)
        if self.format is not None and self.format not in ("console", "json"):
            msg = "format must be 'console', 'json', or unset"
            raise ConfigurationError(msg, format=self.format)


@dataclass(frozen=True, slots=True, kw_only=True)
class PlatformConfig:
    """Fully resolved platform configuration.

    Produced by :func:`marketpulse.config.settings.load_config`. Construct this
    directly only in tests, where bypassing file and environment resolution is
    the point.

    Attributes:
        environment: Deployment target.
        storage: Local lakehouse layout.
        analytics: Local engine execution limits.
        logging: Observability settings.
        extras: Validated-as-present-but-not-typed config. Lets a subsystem
            under active development carry settings without every experiment
            forcing a change to this class. Promote a key to a typed field once
            its shape is stable.
    """

    environment: Environment = Environment.LOCAL
    storage: StorageConfig = field(default_factory=StorageConfig)
    analytics: AnalyticsConfig = field(default_factory=AnalyticsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    extras: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Apply cross-field invariants that single fields cannot express.

        Raises:
            ConfigurationError: If the combination of settings is unsafe for
                the target environment.
        """
        if self.environment is Environment.PROD and self.storage.create_missing:
            msg = (
                "storage.create_missing must be false in prod: object storage is "
                "provisioned by Terraform, so a missing location is a "
                "misconfiguration to surface rather than silently repair"
            )
            raise ConfigurationError(msg, environment=self.environment.value)

        if self.environment is Environment.CI and self.logging.format == "console":
            msg = (
                "logging.format must be 'json' in ci so that failures are "
                "machine-parseable in build logs"
            )
            raise ConfigurationError(msg, environment=self.environment.value)
