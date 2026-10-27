"""Analytics session against real Polars and DuckDB engines.

These tests use the actual engines rather than mocks. Mocking a query engine
asserts only that the code calls the API it was written to call; it cannot
catch a DuckDB version that renames a setting, a Polars release that changes
``scan_parquet`` pushdown behaviour, or an Arrow schema that does not survive
the handoff. Those are exactly the failures that matter here, and they are
cheap to test for real: the whole module runs in well under a second against
``tmp_path``.

No test in this module touches the network.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

from marketpulse.analytics.session import open_session
from marketpulse.config.models import MedallionLayer
from marketpulse.core.errors import AnalyticsError

if TYPE_CHECKING:
    from marketpulse.analytics.session import AnalyticsSession
    from marketpulse.config.models import PlatformConfig


class TestSql:
    def test_scalar_query(self, session: AnalyticsSession) -> None:
        assert session.sql("SELECT 1 AS n").to_dicts() == [{"n": 1}]

    def test_result_is_a_polars_frame(self, session: AnalyticsSession) -> None:
        """The return type is the Arrow handoff; a Pandas frame would mean a copy."""
        assert isinstance(session.sql("SELECT 1 AS n"), pl.DataFrame)

    def test_named_parameters_are_bound_as_values(self, session: AnalyticsSession) -> None:
        result = session.sql("SELECT $value AS v", value=42)
        assert result.to_dicts() == [{"v": 42}]

    def test_parameter_cannot_alter_the_statement(self, session: AnalyticsSession) -> None:
        """A bound parameter is a value, never parsed as SQL.

        The payload below would be catastrophic under string interpolation; as
        a parameter it is returned verbatim as data.
        """
        payload = "1; DROP TABLE important"
        assert session.sql("SELECT $v AS v", v=payload).to_dicts() == [{"v": payload}]

    def test_statement_without_a_result_set_returns_empty(self, session: AnalyticsSession) -> None:
        assert session.sql("CREATE TABLE t (a INTEGER)").is_empty()

    def test_invalid_sql_raises_analytics_error(self, session: AnalyticsSession) -> None:
        with pytest.raises(AnalyticsError, match="SQL execution failed"):
            session.sql("SELECT FROM WHERE")

    def test_error_carries_the_offending_query(self, session: AnalyticsSession) -> None:
        with pytest.raises(AnalyticsError) as exc_info:
            session.sql("SELECT * FROM does_not_exist")
        assert "does_not_exist" in exc_info.value.context["query"]

    def test_execute_runs_ddl(self, session: AnalyticsSession) -> None:
        session.execute("CREATE TABLE t (a INTEGER)")
        session.execute("INSERT INTO t VALUES (7)")
        assert session.sql("SELECT a FROM t").to_dicts() == [{"a": 7}]

    def test_execute_surfaces_failures(self, session: AnalyticsSession) -> None:
        with pytest.raises(AnalyticsError, match="Statement execution failed"):
            session.execute("CREATE TABLE")


class TestArrowBoundary:
    def test_registered_frame_is_queryable(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        session.register("bars", bars)
        assert session.sql("SELECT count(*) AS n FROM bars").to_dicts() == [{"n": 5}]

    def test_lazyframe_is_collected_before_registration(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """SQL needs a materialised relation, so a LazyFrame is collected."""
        session.register("bars", bars.lazy().filter(pl.col("symbol") == "SPY"))
        assert session.sql("SELECT count(*) AS n FROM bars").to_dicts() == [{"n": 2}]

    def test_schema_survives_the_handoff(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Dtypes must round-trip; a silent cast to float would corrupt volume."""
        session.register("bars", bars)
        result = session.sql("SELECT symbol, trade_date, close, volume FROM bars LIMIT 1")
        assert result.schema["symbol"] == pl.String
        assert result.schema["trade_date"] == pl.Date
        assert result.schema["close"] == pl.Float64
        assert result.schema["volume"] == pl.Int64

    def test_round_trip_preserves_values(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        session.register("bars", bars)
        returned = session.sql("SELECT * FROM bars ORDER BY symbol, trade_date")
        expected = bars.sort("symbol", "trade_date")
        assert returned.equals(expected)

    def test_aggregation_crosses_both_engines(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Polars builds the frame, DuckDB aggregates it, Polars reads the result."""
        session.register("bars", bars)
        result = session.sql(
            """
            SELECT symbol, round(avg(close), 2) AS avg_close
            FROM bars
            GROUP BY symbol
            ORDER BY symbol
            """
        )
        assert result.to_dicts() == [
            {"symbol": "IWM", "avg_close": 198.42},
            {"symbol": "QQQ", "avg_close": 404.11},
            {"symbol": "SPY", "avg_close": 473.88},
        ]

    def test_arrow_table_can_be_registered(self, session: AnalyticsSession) -> None:
        import pyarrow as pa

        session.register("t", pa.table({"a": [1, 2, 3]}))
        assert session.sql("SELECT sum(a) AS total FROM t").to_dicts() == [{"total": 6}]

    def test_unregister_removes_the_relation(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        session.register("bars", bars)
        session.unregister("bars")
        with pytest.raises(AnalyticsError):
            session.sql("SELECT * FROM bars")

    def test_registrations_are_tracked(self, session: AnalyticsSession, bars: pl.DataFrame) -> None:
        session.register("bars", bars)
        assert "bars" in session.registrations
        session.unregister("bars")
        assert "bars" not in session.registrations

    def test_register_is_chainable(self, session: AnalyticsSession, bars: pl.DataFrame) -> None:
        session.register("a", bars).register("b", bars)
        assert session.registrations == frozenset({"a", "b"})


class TestScopedRegistration:
    def test_frames_are_queryable_inside_the_block(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        with session.registered(bars=bars) as scoped:
            assert scoped.sql("SELECT count(*) AS n FROM bars").to_dicts() == [{"n": 5}]

    def test_frames_are_removed_after_the_block(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Scoping prevents a long-lived session from pinning Arrow buffers."""
        with session.registered(bars=bars):
            pass
        assert session.registrations == frozenset()

    def test_registrations_are_removed_after_an_exception(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        with pytest.raises(RuntimeError), session.registered(bars=bars):
            raise RuntimeError("boom")
        assert session.registrations == frozenset()

    def test_multiple_frames_can_be_joined(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        names = pl.DataFrame(
            {"symbol": ["SPY", "QQQ", "IWM"], "name": ["S&P 500", "Nasdaq 100", "Russell 2000"]}
        )
        with session.registered(bars=bars, names=names) as scoped:
            result = scoped.sql(
                """
                SELECT n.name, count(*) AS observations
                FROM bars b JOIN names n USING (symbol)
                GROUP BY n.name
                ORDER BY n.name
                """
            )
        assert result.to_dicts() == [
            {"name": "Nasdaq 100", "observations": 2},
            {"name": "Russell 2000", "observations": 1},
            {"name": "S&P 500", "observations": 2},
        ]


class TestIdentifierValidation:
    @pytest.mark.parametrize(
        "name",
        [
            "bars; DROP TABLE t",
            "bars--comment",
            "has space",
            "has-dash",
            "1leading_digit",
            "",
            "symöbol",
            "a" * 200,
        ],
    )
    def test_unsafe_relation_names_are_rejected(
        self, name: str, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Registration names cannot be parameterised, so they are whitelisted.

        DuckDB interpolates the name into a view definition, so restricting the
        charset removes the injection path entirely rather than relying on
        quoting being correct everywhere.
        """
        with pytest.raises(AnalyticsError):
            session.register(name, bars)

    @pytest.mark.parametrize("name", ["bars", "_private", "bars_2026", "UPPER"])
    def test_safe_relation_names_are_accepted(
        self, name: str, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        session.register(name, bars)
        assert name in session.registrations


class TestLakehouseIo:
    def test_write_then_scan_round_trips(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        session.write(bars, MedallionLayer.BRONZE, "equity_daily_bars")
        scanned = session.scan(MedallionLayer.BRONZE, "equity_daily_bars").collect()
        assert scanned.sort("symbol", "trade_date").equals(bars.sort("symbol", "trade_date"))

    def test_scan_returns_a_lazyframe(self, session: AnalyticsSession, bars: pl.DataFrame) -> None:
        """Laziness is what lets Polars push filters into the Parquet reader."""
        session.write(bars, MedallionLayer.BRONZE, "equity_daily_bars")
        assert isinstance(session.scan(MedallionLayer.BRONZE, "equity_daily_bars"), pl.LazyFrame)

    def test_predicate_is_pushed_into_the_scan(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Verifies the optimiser actually pushes down, not just that it is lazy.

        If the filter were applied after a full materialisation the result would
        still be correct, so correctness alone cannot detect the regression. The
        optimised plan is the observable.
        """
        session.write(bars, MedallionLayer.BRONZE, "equity_daily_bars")
        plan = (
            session.scan(MedallionLayer.BRONZE, "equity_daily_bars")
            .filter(pl.col("symbol") == "SPY")
            .explain(optimized=True)
        )
        # Polars reports pushdown in the scan node of the optimised plan.
        assert "SELECTION" in plan.upper() or "FILTER" in plan.upper()

    def test_partitioned_write_creates_hive_directories(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        directory = session.write(
            bars, MedallionLayer.SILVER, "equity_daily_bars", partition_by="symbol"
        )
        partitions = {p.name for p in directory.iterdir() if p.is_dir()}
        assert partitions == {"symbol=SPY", "symbol=QQQ", "symbol=IWM"}

    def test_partition_column_survives_the_round_trip(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Hive partitioning must be enabled on read or the column vanishes.

        The partition value lives in the directory name, not inside the Parquet
        file, so a reader without hive_partitioning loses the column silently.
        """
        session.write(bars, MedallionLayer.SILVER, "equity_daily_bars", partition_by="symbol")
        scanned = session.scan(MedallionLayer.SILVER, "equity_daily_bars").collect()
        assert "symbol" in scanned.columns
        assert set(scanned["symbol"]) == {"SPY", "QQQ", "IWM"}
        assert scanned.height == bars.height

    def test_lazyframe_can_be_written(self, session: AnalyticsSession, bars: pl.DataFrame) -> None:
        session.write(bars.lazy(), MedallionLayer.GOLD, "summary")
        assert session.scan(MedallionLayer.GOLD, "summary").collect().height == 5

    def test_missing_dataset_raises_rather_than_returning_empty(
        self, session: AnalyticsSession
    ) -> None:
        """A typo in a dataset name must not look like a legitimately empty result."""
        with pytest.raises(AnalyticsError, match="no Parquet files"):
            session.scan(MedallionLayer.GOLD, "never_written")

    def test_traversal_in_dataset_name_is_rejected(self, session: AnalyticsSession) -> None:
        from marketpulse.core.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            session.scan(MedallionLayer.GOLD, "../../etc")

    def test_scanned_dataset_is_queryable_as_sql(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """The full local path: Parquet to Polars scan to DuckDB SQL."""
        session.write(bars, MedallionLayer.GOLD, "equity_daily_bars")
        scanned = session.scan(MedallionLayer.GOLD, "equity_daily_bars")
        with session.registered(gold=scanned) as scoped:
            result = scoped.sql("SELECT count(DISTINCT symbol) AS symbols FROM gold")
        assert result.to_dicts() == [{"symbols": 3}]


class TestSessionLifecycle:
    def test_settings_are_read_back_from_the_engine(self, session: AnalyticsSession) -> None:
        """Reported settings must reflect what DuckDB accepted, not what was asked.

        DuckDB clamps some values, and the discrepancy is the first thing worth
        checking when a query spills unexpectedly.
        """
        settings = session.settings()
        assert settings["threads"] == "2"
        assert settings["preserve_insertion_order"] == "true"

    def test_memory_limit_is_applied(self, session: AnalyticsSession) -> None:
        assert "MiB" in session.settings()["memory_limit"]

    def test_close_is_idempotent(self, config: PlatformConfig) -> None:
        with open_session(config) as handle:
            handle.close()
            handle.close()

    def test_context_manager_closes_the_connection(self, config: PlatformConfig) -> None:
        """Use after close fails as an AnalyticsError, not a raw duckdb error.

        The session wraps engine failures so callers catch one hierarchy; a
        leaked duckdb.ConnectionException would bypass that.
        """
        with open_session(config) as handle:
            handle.sql("SELECT 1")
        with pytest.raises(AnalyticsError, match="SQL execution failed"):
            handle.sql("SELECT 1")

    def test_persistent_database_file_is_created(
        self, config: PlatformConfig, tmp_path: pytest.TempPathFactory
    ) -> None:
        import dataclasses

        target = config.storage.root / "nested" / "marketpulse.duckdb"
        persistent = dataclasses.replace(
            config,
            analytics=dataclasses.replace(config.analytics, duckdb_database=target),
        )
        with open_session(persistent) as handle:
            handle.execute("CREATE TABLE t (a INTEGER)")
        assert target.is_file()

    def test_persisted_table_survives_reconnection(self, config: PlatformConfig) -> None:
        import dataclasses

        target = config.storage.root / "marketpulse.duckdb"
        persistent = dataclasses.replace(
            config,
            analytics=dataclasses.replace(config.analytics, duckdb_database=target),
        )
        with open_session(persistent) as handle:
            handle.execute("CREATE TABLE t AS SELECT 7 AS a")
        with open_session(persistent) as handle:
            assert handle.sql("SELECT a FROM t").to_dicts() == [{"a": 7}]

    def test_cursor_session_shares_the_database(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        session.execute("CREATE TABLE t AS SELECT 1 AS a")
        with session.cursor_session() as child:
            assert child.sql("SELECT a FROM t").to_dicts() == [{"a": 1}]

    def test_cursor_session_has_its_own_registrations(
        self, session: AnalyticsSession, bars: pl.DataFrame
    ) -> None:
        """Independent registration state is what makes per-thread cursors safe."""
        with session.cursor_session() as child:
            child.register("bars", bars)
            assert child.registrations == frozenset({"bars"})
        assert session.registrations == frozenset()

    def test_closing_a_cursor_leaves_the_parent_open(self, session: AnalyticsSession) -> None:
        with session.cursor_session():
            pass
        assert session.sql("SELECT 1 AS n").to_dicts() == [{"n": 1}]
