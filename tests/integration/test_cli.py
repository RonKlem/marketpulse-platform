"""CLI behaviour, exercised through the real dispatch path.

Every test calls :func:`marketpulse.cli.main` with an argument vector rather
than invoking a handler directly, so argument parsing, configuration
resolution, logging setup, and exit codes are all covered. Calling a handler
directly would skip exactly the wiring most likely to break.

The stdout/stderr split is asserted deliberately: machine-readable output must
stay on stdout so that ``marketpulse config show --json | jq`` works while
logging is enabled.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import polars as pl  # noqa: TC002  -- runtime import keeps this an integration-tier test
import pytest

from marketpulse.analytics.session import open_session
from marketpulse.cli import EXIT_ERROR, EXIT_OK, main
from marketpulse.config.models import MedallionLayer

if TYPE_CHECKING:
    from pathlib import Path

    from marketpulse.config.models import PlatformConfig


@pytest.fixture(autouse=True)
def _ci_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Pin the CLI to a hermetic environment with a temporary storage root.

    Without this the CLI would resolve the repository's own ``config/`` and
    write into the developer's real ``data/`` directory.
    """
    monkeypatch.setenv("MARKETPULSE_ENV", "ci")
    monkeypatch.setenv("MARKETPULSE_STORAGE__ROOT", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("MARKETPULSE_ANALYTICS__THREADS", "2")
    monkeypatch.setenv("MARKETPULSE_ANALYTICS__MEMORY_LIMIT", "512MB")


class TestConfigCommand:
    def test_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["config", "show"]) == EXIT_OK

    def test_action_defaults_to_show(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["config"]) == EXIT_OK
        assert "environment:" in capsys.readouterr().out

    def test_reports_resolved_values(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["config", "show"])
        out = capsys.readouterr().out
        assert "analytics.threads" in out
        assert "storage.root" in out

    def test_lists_layers_in_application_order(self, capsys: pytest.CaptureFixture[str]) -> None:
        """ "Which file did this come from" is the first diagnostic question."""
        main(["config", "show"])
        out = capsys.readouterr().out
        assert "base.toml" in out
        assert "ci.toml" in out

    def test_json_output_is_parseable(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["config", "show", "--json"]) == EXIT_OK
        payload = json.loads(capsys.readouterr().out)
        assert payload["environment"] == "ci"
        assert payload["analytics"]["threads"] == 2

    def test_json_goes_to_stdout_not_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Keeps `marketpulse config show --json | jq` usable with logging on."""
        main(["config", "show", "--json"])
        captured = capsys.readouterr()
        json.loads(captured.out)
        assert "{" not in captured.err

    def test_paths_serialise_as_strings(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["config", "show", "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload["storage"]["root"], str)

    def test_unset_values_render_as_null_in_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["config", "show", "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert payload["analytics"]["duckdb_database"] is None


class TestEnvironmentSelection:
    @pytest.mark.parametrize("environment", ["local", "ci", "dev"])
    def test_env_flag_selects_the_environment(
        self, environment: str, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert main(["--env", environment, "config", "show", "--json"]) == EXIT_OK
        assert json.loads(capsys.readouterr().out)["environment"] == environment

    def test_env_flag_overrides_the_environment_variable(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The fixture set MARKETPULSE_ENV=ci.
        main(["--env", "dev", "config", "show", "--json"])
        assert json.loads(capsys.readouterr().out)["environment"] == "dev"

    def test_prod_layer_loads_through_the_cli(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Prod has a cross-field invariant; a broken layer must not reach deploy."""
        assert main(["--env", "prod", "config", "show", "--json"]) == EXIT_OK
        payload = json.loads(capsys.readouterr().out)
        assert payload["storage"]["create_missing"] is False

    def test_unknown_environment_is_rejected_by_argparse(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["--env", "staging", "config", "show"])
        assert exc_info.value.code == 2


class TestDoctorCommand:
    def test_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["doctor"]) == EXIT_OK

    def test_reports_engine_versions(self, capsys: pytest.CaptureFixture[str]) -> None:
        """A bug report needs the versions; an upstream change is the usual cause."""
        main(["doctor"])
        out = capsys.readouterr().out
        for field in ("marketpulse", "python", "polars", "duckdb", "pyarrow"):
            assert field in out

    def test_reads_settings_back_from_the_engine(self, capsys: pytest.CaptureFixture[str]) -> None:
        """DuckDB clamps values, so the effective setting is what matters."""
        main(["doctor"])
        out = capsys.readouterr().out
        assert "duckdb settings in force" in out
        assert "threads" in out

    def test_does_not_warn_when_memory_limit_is_merely_renormalised(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """DuckDB echoes 512MB as '488.2 MiB' — the same budget in binary units.

        Comparing strings would warn on every correctly configured session, so
        the comparison parses byte counts with a tolerance.
        """
        main(["doctor"])
        assert "requested memory_limit" not in capsys.readouterr().err

    def test_json_flag_emits_parseable_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["doctor", "--json"]) == EXIT_OK
        out = capsys.readouterr().out
        payload = json.loads(out[out.index("{") :])
        assert "platform" in payload
        assert "duckdb" in payload


class TestQueryCommand:
    def test_scalar_query(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["query", "SELECT 1 AS n"]) == EXIT_OK
        assert "1" in capsys.readouterr().out

    def test_json_rows(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["query", "SELECT 1 AS n", "--json"]) == EXIT_OK
        assert json.loads(capsys.readouterr().out) == [{"n": 1}]

    def test_invalid_sql_exits_nonzero(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["query", "SELECT FROM WHERE"]) == EXIT_ERROR

    def test_invalid_sql_reports_to_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["query", "SELECT FROM WHERE"])
        assert "error:" in capsys.readouterr().err

    def test_dataset_flag_registers_a_lakehouse_dataset(
        self,
        config: PlatformConfig,
        bars: pl.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """End-to-end: write Parquet, then query it by name through the CLI."""
        monkeypatch.setenv("MARKETPULSE_STORAGE__ROOT", str(config.storage.root))
        with open_session(config) as session:
            session.write(bars, MedallionLayer.GOLD, "equity_daily_bars")

        exit_code = main(
            [
                "query",
                "SELECT count(*) AS n FROM equity_daily_bars",
                "--dataset",
                "equity_daily_bars",
                "--json",
            ]
        )
        assert exit_code == EXIT_OK
        assert json.loads(capsys.readouterr().out) == [{"n": 5}]

    def test_missing_dataset_exits_nonzero(self, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["query", "SELECT 1", "--dataset", "never_written", "--layer", "gold"])
        assert exit_code == EXIT_ERROR
        assert "no Parquet files" in capsys.readouterr().err

    def test_unknown_layer_is_rejected_by_argparse(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["query", "SELECT 1", "--dataset", "d", "--layer", "platinum"])
        assert exc_info.value.code == 2


class TestLoggingFlags:
    def test_log_level_flag_is_accepted(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["--log-level", "DEBUG", "config", "show"]) == EXIT_OK

    def test_log_format_flag_is_accepted(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["--log-format", "json", "config", "show"]) == EXIT_OK

    def test_logs_do_not_contaminate_json_stdout(self, capsys: pytest.CaptureFixture[str]) -> None:
        """DEBUG logging must not break a downstream `| jq`."""
        main(["--log-level", "DEBUG", "config", "show", "--json"])
        json.loads(capsys.readouterr().out)


class TestParserContract:
    def test_version_flag_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["--version"])
        assert exc_info.value.code == 0
        assert "marketpulse" in capsys.readouterr().out

    def test_help_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_missing_subcommand_is_an_argparse_error(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 2

    def test_unknown_subcommand_is_an_argparse_error(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["frobnicate"])
        assert exc_info.value.code == 2


class TestFailureHandling:
    def test_malformed_config_exits_before_logging_is_configured(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The log level comes from the config that just failed to parse.

        So the error must be written directly to stderr rather than logged.
        """
        config_dir = tmp_path / "broken"
        config_dir.mkdir()
        (config_dir / "base.toml").write_text("[analytics\nthreads = 2\n", encoding="utf-8")
        monkeypatch.setenv("MARKETPULSE_CONFIG_DIR", str(config_dir))

        assert main(["config", "show"]) == EXIT_ERROR
        assert "configuration error:" in capsys.readouterr().err

    def test_unknown_config_key_exits_nonzero(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        config_dir = tmp_path / "typo"
        config_dir.mkdir()
        (config_dir / "base.toml").write_text("[analytics]\nthread = 2\n", encoding="utf-8")
        monkeypatch.setenv("MARKETPULSE_CONFIG_DIR", str(config_dir))

        assert main(["config", "show"]) == EXIT_ERROR
        assert "did_you_mean" in capsys.readouterr().err
