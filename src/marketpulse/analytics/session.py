"""The local analytics session: a configured DuckDB connection with Arrow interop.

The Arrow boundary
------------------
Polars and DuckDB both use Apache Arrow as their in-memory columnar format.
DuckDB's replacement scan mechanism resolves an unqualified identifier in a SQL
query against Python objects in scope, and for an Arrow-backed object it reads
the buffers directly. A ``pl.DataFrame`` registered through this session is
therefore visible to SQL without serialisation: no copy, no Python-object
round-trip, and no Pandas intermediary. The reverse direction
(:meth:`AnalyticsSession.sql` returning a ``pl.DataFrame``) goes through
``duckdb.DuckDBPyRelation.pl()``, which hands Polars the Arrow result buffers.

This is the reason the local stack is two engines rather than one. Polars'
expression API is better for row-wise feature derivation and window functions
over a known schema; DuckDB's optimiser is better for multi-table joins and
aggregations expressed as SQL, and its SQL dialect is close enough to Spark SQL
that queries developed locally port with minimal change (ADR-002). Because the
handoff is zero-copy, choosing per operation costs nothing.

Laziness
--------
Dataset reads return ``pl.LazyFrame``, not ``pl.DataFrame``.
``pl.scan_parquet`` defers execution so that Polars' optimiser can push
projections and filters into the Parquet reader and skip row groups whose
column statistics exclude them. Returning an eager frame would materialise
every row and column before the caller's first filter, which on a partitioned
dataset is the difference between reading one file and reading all of them.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
import time
from typing import TYPE_CHECKING, Any, Literal, Self

import duckdb
import polars as pl

from marketpulse.config.models import MedallionLayer, PlatformConfig
from marketpulse.config.settings import load_config
from marketpulse.core.errors import AnalyticsError
from marketpulse.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

    import pyarrow as pa

__all__ = ["AnalyticsSession", "ParquetCompression", "open_session"]

ParquetCompression = Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"]
"""Codecs Polars accepts for a Parquet write.

Mirrors Polars' own literal rather than taking a bare ``str``, so an invalid
codec is a type error at the call site instead of a Polars exception raised
after the frame has already been computed.
"""

_log = get_logger(__name__)

_IDENTIFIER_MAX_LENGTH = 128


@dataclass(slots=True)
class AnalyticsSession:
    """A configured DuckDB connection plus helpers for the local lakehouse.

    Not thread-safe. A ``duckdb.DuckDBPyConnection`` serialises access
    internally, but the registration namespace this class manages is shared
    mutable state, so concurrent ``register`` calls on one session can race.
    Use :meth:`cursor_session` to obtain an independent handle per thread.

    Construct via :func:`open_session` rather than directly, so that the
    connection is configured and closed deterministically.

    Attributes:
        config: The configuration this session was opened with.
        connection: The live DuckDB connection.
    """

    config: PlatformConfig
    connection: duckdb.DuckDBPyConnection
    _registered: set[str]

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> Self:
        """Return self; the connection is already open."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Close the connection."""
        self.close()

    def close(self) -> None:
        """Close the DuckDB connection. Idempotent."""
        with contextlib.suppress(duckdb.ConnectionException):
            self.connection.close()

    # -- SQL ---------------------------------------------------------------

    def sql(self, query: str, **parameters: Any) -> pl.DataFrame:
        """Execute SQL and return the result as an eager Polars frame.

        Args:
            query: SQL text. Registered Polars frames are addressable by their
                registration name.
            **parameters: Named prepared-statement parameters, bound as
                ``$name``. Always prefer these to string interpolation: DuckDB
                binds them as values, so a parameter cannot alter the parsed
                statement.

        Returns:
            The result as a ``pl.DataFrame``, transferred over Arrow.

        Raises:
            AnalyticsError: If DuckDB rejects or fails to execute the query.
        """
        started = time.perf_counter()
        try:
            relation = (
                self.connection.sql(query, params=parameters)
                if parameters
                else self.connection.sql(query)
            )
            # A statement with no result set (CREATE, SET, INSERT) yields None.
            frame = pl.DataFrame() if relation is None else relation.pl()
        except duckdb.Error as exc:
            _log.exception("analytics.sql.failed", extra={"query": _truncate(query)})
            msg = "SQL execution failed"
            raise AnalyticsError(msg, query=_truncate(query), reason=str(exc)) from exc

        _log.debug(
            "analytics.sql.completed",
            extra={
                "query": _truncate(query),
                "rows": frame.height,
                "columns": frame.width,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
        )
        return frame

    def execute(self, statement: str, **parameters: Any) -> None:
        """Run a statement for its side effect, discarding any result.

        Args:
            statement: SQL DDL or DML.
            **parameters: Named prepared-statement parameters.

        Raises:
            AnalyticsError: If DuckDB rejects or fails to execute it.
        """
        try:
            if parameters:
                self.connection.execute(statement, parameters)
            else:
                self.connection.execute(statement)
        except duckdb.Error as exc:
            _log.exception("analytics.execute.failed", extra={"sql": _truncate(statement)})
            msg = "Statement execution failed"
            raise AnalyticsError(msg, sql=_truncate(statement), reason=str(exc)) from exc

    # -- Arrow boundary ----------------------------------------------------

    def register(self, name: str, frame: pl.DataFrame | pl.LazyFrame | pa.Table) -> Self:
        """Make an in-memory frame queryable by SQL, without copying it.

        DuckDB reads the object's Arrow buffers in place. A ``LazyFrame`` is
        collected first, because SQL needs a materialised relation to scan.

        Args:
            name: Identifier to use in SQL. Validated, because it is
                interpolated into a ``CREATE VIEW`` statement by DuckDB and
                cannot be parameterised.
            frame: Polars frame or Arrow table to expose.

        Returns:
            ``self``, to allow chaining.

        Raises:
            AnalyticsError: If ``name`` is not a safe identifier, or
                registration fails.
        """
        _validate_identifier(name)
        payload = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        try:
            self.connection.register(name, payload)
        except duckdb.Error as exc:
            msg = "Failed to register frame"
            raise AnalyticsError(msg, name=name, reason=str(exc)) from exc
        self._registered.add(name)
        _log.debug("analytics.registered", extra={"relation": name})
        return self

    def unregister(self, name: str) -> Self:
        """Remove a previously registered relation.

        Args:
            name: Registration name.

        Returns:
            ``self``, to allow chaining.
        """
        _validate_identifier(name)
        with contextlib.suppress(duckdb.Error):
            self.connection.unregister(name)
        self._registered.discard(name)
        return self

    @contextlib.contextmanager
    def registered(self, **frames: pl.DataFrame | pl.LazyFrame) -> Iterator[Self]:
        """Register frames for the duration of a block, then remove them.

        Scoping registrations prevents a long-lived session from pinning Arrow
        buffers that the caller believes are garbage: a registered frame is
        referenced by DuckDB and will not be collected while the registration
        stands.

        Args:
            **frames: Frames to register, keyed by SQL identifier.

        Yields:
            ``self``, with the frames queryable.

        Example:
            >>> import polars as pl
            >>> with open_session() as session:
            ...     bars = pl.DataFrame({"symbol": ["SPY"], "close": [500.0]})
            ...     with session.registered(bars=bars) as scoped:
            ...         scoped.sql("SELECT symbol FROM bars").to_dicts()
            [{'symbol': 'SPY'}]
        """
        for name, frame in frames.items():
            self.register(name, frame)
        try:
            yield self
        finally:
            for name in frames:
                self.unregister(name)

    @property
    def registrations(self) -> frozenset[str]:
        """Names currently registered on this session."""
        return frozenset(self._registered)

    # -- lakehouse access --------------------------------------------------

    def scan(self, layer: MedallionLayer, dataset: str) -> pl.LazyFrame:
        """Lazily scan a Parquet dataset from a medallion zone.

        Args:
            layer: Zone to read from.
            dataset: Dataset identifier.

        Returns:
            A ``pl.LazyFrame`` over the dataset's Parquet files. Lazy so that
            the caller's subsequent filters and projections are pushed into the
            Parquet reader rather than applied after a full materialisation.

        Raises:
            AnalyticsError: If the dataset directory contains no Parquet files.
                Returning an empty frame instead would let a typo in a dataset
                name propagate as a silently empty result.
        """
        directory = self.config.storage.dataset_path(layer, dataset)
        if not any(directory.rglob("*.parquet")):
            msg = "Dataset contains no Parquet files"
            raise AnalyticsError(
                msg,
                layer=layer.value,
                dataset=dataset,
                path=str(directory),
            )
        # Hive partitioning is enabled because the write path partitions by
        # date; without it the partition columns would be absent from the schema.
        return pl.scan_parquet(
            directory / "**" / "*.parquet",
            hive_partitioning=True,
        )

    def write(
        self,
        frame: pl.DataFrame | pl.LazyFrame,
        layer: MedallionLayer,
        dataset: str,
        *,
        partition_by: str | list[str] | None = None,
        compression: ParquetCompression = "zstd",
    ) -> Path:
        """Write a frame to a medallion zone as Parquet.

        Args:
            frame: Data to write. A ``LazyFrame`` is sunk via
                :meth:`polars.LazyFrame.sink_parquet` where possible, which
                streams batches to disk instead of materialising the whole
                result in memory first.
            layer: Destination zone.
            dataset: Dataset identifier.
            partition_by: Column(s) to Hive-partition by. Partitioning on the
                columns that queries filter on is what makes predicate
                pushdown able to skip files entirely.
            compression: Parquet codec. ``zstd`` is the default rather than
                ``snappy`` because it compresses financial time series
                substantially better at comparable decompression speed, and
                local analytics is usually I/O-bound.

        Returns:
            The dataset directory written to.

        Raises:
            AnalyticsError: If the write fails.
        """
        directory = self.config.storage.dataset_path(layer, dataset)
        keys = [partition_by] if isinstance(partition_by, str) else partition_by

        try:
            if keys:
                eager = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
                eager.write_parquet(
                    directory,
                    partition_by=keys,
                    compression=compression,
                )
            elif isinstance(frame, pl.LazyFrame):
                frame.sink_parquet(directory / "part-0.parquet", compression=compression)
            else:
                frame.write_parquet(directory / "part-0.parquet", compression=compression)
        except (OSError, pl.exceptions.PolarsError) as exc:
            msg = "Failed to write dataset"
            raise AnalyticsError(
                msg,
                layer=layer.value,
                dataset=dataset,
                path=str(directory),
                reason=str(exc),
            ) from exc

        _log.info(
            "analytics.dataset.written",
            extra={
                "layer": layer.value,
                "dataset": dataset,
                "path": str(directory),
                "partitioned_by": keys,
            },
        )
        return directory

    # -- concurrency -------------------------------------------------------

    @contextlib.contextmanager
    def cursor_session(self) -> Iterator[AnalyticsSession]:
        """Yield an independent session sharing this one's database.

        ``DuckDBPyConnection.cursor`` creates a new connection handle against
        the same database instance, with its own transaction and registration
        state. This is the supported way to run queries from multiple threads:
        each thread takes its own cursor rather than sharing one handle.

        Yields:
            A session wrapping the cursor. Closed on exit; the parent is not.
        """
        cursor = self.connection.cursor()
        child = AnalyticsSession(config=self.config, connection=cursor, _registered=set())
        try:
            yield child
        finally:
            child.close()

    def settings(self) -> Mapping[str, str]:
        """Return the DuckDB settings this session actually applied.

        Reads back from ``duckdb_settings()`` rather than echoing the
        configuration, so the result reflects what the engine accepted. DuckDB
        clamps or ignores some values (a ``memory_limit`` below its minimum,
        for instance), and the difference matters when diagnosing a spill.

        Returns:
            Mapping of setting name to its current value.
        """
        rows = self.sql(
            """
            SELECT name, value
            FROM duckdb_settings()
            WHERE name IN (
                'threads', 'memory_limit', 'temp_directory',
                'preserve_insertion_order', 'enable_progress_bar'
            )
            ORDER BY name
            """
        )
        return dict(zip(rows["name"], rows["value"], strict=True))


def _truncate(text: str, limit: int = 300) -> str:
    """Collapse whitespace and cap length, for logging a query safely."""
    collapsed = " ".join(text.split())
    return collapsed if len(collapsed) <= limit else f"{collapsed[:limit]}..."


def _validate_identifier(name: str) -> None:
    """Reject anything that is not a plain SQL identifier.

    Registration names cannot be passed as prepared-statement parameters,
    because DuckDB interpolates them into a view definition. Restricting them
    to ``[A-Za-z_][A-Za-z0-9_]*`` removes any possibility of injection through
    this path rather than relying on quoting.

    Args:
        name: Candidate identifier.

    Raises:
        AnalyticsError: If the name is not a safe identifier.
    """
    if not name or not name.isascii() or len(name) > _IDENTIFIER_MAX_LENGTH:
        msg = "Relation name must be a non-empty ASCII identifier"
        raise AnalyticsError(msg, name=name)
    if not (name[0].isalpha() or name[0] == "_"):
        msg = "Relation name must start with a letter or underscore"
        raise AnalyticsError(msg, name=name)
    if not all(char.isalnum() or char == "_" for char in name):
        msg = "Relation name must contain only letters, digits, and underscores"
        raise AnalyticsError(msg, name=name)


def _configure(connection: duckdb.DuckDBPyConnection, config: PlatformConfig) -> None:
    """Apply execution limits to a fresh connection.

    Settings are applied individually and tolerantly: DuckDB renames and
    retires configuration options across minor versions, and a session that
    refuses to open because one advisory setting is unrecognised is worse than
    one that runs with that setting at its default.
    """
    analytics = config.analytics
    desired: list[tuple[str, object]] = [
        ("preserve_insertion_order", analytics.preserve_insertion_order),
        ("enable_progress_bar", analytics.enable_progress_bar),
    ]
    if analytics.threads is not None:
        desired.append(("threads", analytics.threads))
    if analytics.memory_limit is not None:
        desired.append(("memory_limit", analytics.memory_limit))
    if analytics.temp_directory is not None:
        analytics.temp_directory.mkdir(parents=True, exist_ok=True)
        desired.append(("temp_directory", str(analytics.temp_directory)))

    for key, value in desired:
        try:
            connection.execute(f"SET {key} = ?", [value])
        except duckdb.Error:
            _log.warning(
                "analytics.setting.rejected",
                extra={"setting": key, "value": value, "duckdb_version": duckdb.__version__},
            )


@contextlib.contextmanager
def open_session(config: PlatformConfig | None = None) -> Iterator[AnalyticsSession]:
    """Open a configured analytics session and close it on exit.

    A context manager rather than a plain constructor because a DuckDB
    connection holds a file lock when backed by a database file, and an
    in-memory database holds its buffer pool until closed. Leaking either in a
    long-running process is a real cost, so release is made structural.

    Args:
        config: Platform configuration. Resolved via
            :func:`~marketpulse.config.settings.load_config` when omitted.

    Yields:
        The open session.

    Raises:
        AnalyticsError: If the connection cannot be established.
    """
    resolved = config if config is not None else load_config()
    database = str(resolved.analytics.duckdb_database or ":memory:")

    if resolved.analytics.duckdb_database is not None:
        resolved.analytics.duckdb_database.parent.mkdir(parents=True, exist_ok=True)

    try:
        connection = duckdb.connect(database)
    except duckdb.Error as exc:
        msg = "Could not open DuckDB connection"
        raise AnalyticsError(msg, database=database, reason=str(exc)) from exc

    _configure(connection, resolved)
    _log.debug(
        "analytics.session.opened",
        extra={
            "database": database,
            "threads": resolved.analytics.threads,
            "memory_limit": resolved.analytics.memory_limit,
        },
    )

    session = AnalyticsSession(config=resolved, connection=connection, _registered=set())
    try:
        yield session
    finally:
        session.close()
        _log.debug("analytics.session.closed", extra={"database": database})
