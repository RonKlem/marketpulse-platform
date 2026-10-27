"""Configuration resolution, precedence, and validation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from marketpulse.config.models import (
    AnalyticsConfig,
    Environment,
    IngestionConfig,
    LoggingConfig,
    MedallionLayer,
    PlatformConfig,
    StorageConfig,
    available_cpus,
)
from marketpulse.config.settings import (
    ENV_PREFIX,
    OVERRIDE_LAYER_NAME,
    RESOLUTION_CONTROL_VARS,
    anchor_dir,
    describe_layers,
    load_config,
    repo_root,
    resolve_layers,
)
from marketpulse.core.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.fixture
def write_layer(config_dir: Path) -> Callable[[str, str], Path]:
    """Return a helper that writes a named TOML layer."""

    def _write(name: str, body: str) -> Path:
        path = config_dir / name
        path.write_text(body, encoding="utf-8")
        return path

    return _write


class TestPrecedence:
    def test_defaults_apply_with_no_layers(self, config_dir: Path) -> None:
        resolved = load_config(config_dir=config_dir, environ={})
        assert resolved.environment is Environment.LOCAL
        assert resolved.logging.level == "INFO"

    def test_environment_layer_overrides_base(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", '[logging]\nlevel = "WARNING"\n')
        write_layer("dev.toml", '[logging]\nlevel = "ERROR"\n')
        resolved = load_config(environment="dev", config_dir=config_dir, environ={})
        assert resolved.logging.level == "ERROR"

    def test_env_var_overrides_every_file_layer(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", "[analytics]\nthreads = 4\n")
        write_layer("local.toml", "[analytics]\nthreads = 6\n")
        resolved = load_config(
            config_dir=config_dir,
            environ={f"{ENV_PREFIX}ANALYTICS__THREADS": "2"},
        )
        assert resolved.analytics.threads == 2

    def test_explicit_kwargs_override_env_vars(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", "[analytics]\nthreads = 4\n")
        resolved = load_config(
            config_dir=config_dir,
            environ={f"{ENV_PREFIX}ANALYTICS__THREADS": "2"},
            analytics={"threads": 7},
        )
        assert resolved.analytics.threads == 7

    def test_merge_is_section_aware_not_wholesale(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        """An overlay that sets one key must not drop the section's siblings.

        This is the specific bug a naive ``dict.update`` introduces, and it is
        silent: the process starts with a default memory limit nobody chose.
        """
        write_layer("base.toml", '[analytics]\nthreads = 4\nmemory_limit = "16GB"\n')
        write_layer("dev.toml", "[analytics]\nthreads = 8\n")
        resolved = load_config(environment="dev", config_dir=config_dir, environ={})
        assert resolved.analytics.threads == 8
        assert resolved.analytics.memory_limit == "16GB"


class TestOverrideLayerScoping:
    def test_override_layer_applies_in_local(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", "[analytics]\nthreads = 4\n")
        write_layer(OVERRIDE_LAYER_NAME, "[analytics]\nthreads = 5\n")
        resolved = load_config(environment="local", config_dir=config_dir, environ={})
        assert resolved.analytics.threads == 5

    @pytest.mark.parametrize("environment", ["ci", "dev", "prod"])
    def test_override_layer_ignored_outside_local(
        self,
        environment: str,
        config_dir: Path,
        write_layer: Callable[[str, str], Path],
    ) -> None:
        """A git-ignored file must not change behaviour in a shared environment.

        If it did, a process would depend on a file that exists on one laptop
        and nowhere else, which is unreproducible by construction.
        """
        write_layer("base.toml", "[analytics]\nthreads = 4\n")
        write_layer(f"{environment}.toml", "[storage]\ncreate_missing = false\n")
        write_layer(OVERRIDE_LAYER_NAME, "[analytics]\nthreads = 99\n")
        resolved = load_config(environment=environment, config_dir=config_dir, environ={})
        assert resolved.analytics.threads == 4

    def test_override_layer_does_not_collide_with_an_environment_layer(self) -> None:
        """``local.toml`` is the committed environment layer, not the override.

        Collapsing the two would load the same file twice in the local
        environment and make the git-ignore rule ambiguous.
        """
        environment_layers = {f"{environment.value}.toml" for environment in Environment}
        assert OVERRIDE_LAYER_NAME not in environment_layers

    def test_describe_layers_reports_no_duplicates(self) -> None:
        paths = [path for path, _ in describe_layers(Environment.LOCAL)]
        assert len(paths) == len(set(paths))


class TestUnknownKeys:
    def test_unknown_section_key_is_rejected(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", "[analytics]\nthread = 4\n")
        with pytest.raises(ConfigurationError, match="Unknown configuration key"):
            load_config(config_dir=config_dir, environ={})

    def test_unknown_root_key_is_rejected(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", '[unexpected]\nvalue = "x"\n')
        with pytest.raises(ConfigurationError, match="Unknown configuration key"):
            load_config(config_dir=config_dir, environ={})

    def test_near_miss_gets_a_suggestion(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        """A typo should produce a one-line fix, not a list to search."""
        write_layer("base.toml", "[analytics]\nthread = 4\n")
        with pytest.raises(ConfigurationError) as exc_info:
            load_config(config_dir=config_dir, environ={})
        assert exc_info.value.context["did_you_mean"] == {"thread": "threads"}

    def test_section_must_be_a_table(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", 'analytics = "not-a-table"\n')
        with pytest.raises(ConfigurationError, match="must be a table"):
            load_config(config_dir=config_dir, environ={})


class TestEnvironmentVariableCoercion:
    @pytest.mark.parametrize(("raw", "expected"), [("4", 4), ("1", 1), (" 6 ", 6)])
    def test_integers_are_coerced_from_strings(
        self, raw: str, expected: int, config_dir: Path
    ) -> None:
        resolved = load_config(
            config_dir=config_dir,
            environ={f"{ENV_PREFIX}ANALYTICS__THREADS": raw},
        )
        assert resolved.analytics.threads == expected

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("true", True),
            ("TRUE", True),
            ("yes", True),
            ("on", True),
            ("1", True),
            ("false", False),
            ("off", False),
            ("0", False),
        ],
    )
    def test_booleans_are_coerced_from_strings(
        self, raw: str, expected: bool, config_dir: Path
    ) -> None:
        """Environment variables are untyped strings; the dataclass wants bool.

        ``1`` and ``0`` must land as booleans here even though they also parse
        as integers, because the target field is boolean-typed.
        """
        resolved = load_config(
            config_dir=config_dir,
            environ={f"{ENV_PREFIX}ANALYTICS__ENABLE_PROGRESS_BAR": raw},
        )
        assert resolved.analytics.enable_progress_bar is expected

    def test_path_values_are_coerced_to_path(self, config_dir: Path, tmp_path: Path) -> None:
        target = tmp_path / "spill"
        resolved = load_config(
            config_dir=config_dir,
            environ={f"{ENV_PREFIX}ANALYTICS__TEMP_DIRECTORY": str(target)},
        )
        assert resolved.analytics.temp_directory == target
        assert isinstance(resolved.analytics.temp_directory, Path)

    def test_empty_field_name_after_delimiter_is_rejected(self, config_dir: Path) -> None:
        with pytest.raises(ConfigurationError, match="empty field name"):
            load_config(
                config_dir=config_dir,
                environ={f"{ENV_PREFIX}ANALYTICS__": "4"},
            )

    def test_unrelated_env_vars_are_ignored(self, config_dir: Path) -> None:
        resolved = load_config(
            config_dir=config_dir,
            environ={"PATH": "/usr/bin", "OTHER_APP_THREADS": "99"},
        )
        assert resolved.analytics.threads is not None

    @pytest.mark.parametrize("variable", sorted(RESOLUTION_CONTROL_VARS))
    def test_resolution_control_vars_are_not_read_as_config_keys(
        self, variable: str, config_dir: Path, tmp_path: Path
    ) -> None:
        """These steer resolution and carry no ``__`` nesting delimiter.

        Without an explicit exclusion the override parser reads
        ``MARKETPULSE_ROOT`` as a root-level key named ``root`` and rejects it
        as unknown, so setting it would be a startup error rather than a
        working deployment knob. Parametrised over the whole set so a future
        control variable cannot reintroduce the bug.
        """
        # MARKETPULSE_ENV takes an environment name; the other two take a path.
        value = "ci" if variable.endswith("_ENV") else str(tmp_path)
        assert load_config(config_dir=config_dir, environ={variable: value}) is not None

    def test_double_underscore_is_required_for_nesting(self, config_dir: Path) -> None:
        """Single-underscore names cannot nest, because field names contain them.

        ``MARKETPULSE_ANALYTICS_THREADS`` is ambiguous: it could mean section
        ``analytics`` field ``threads``, or a root field named
        ``analytics_threads``. It is treated as the latter and rejected.
        """
        with pytest.raises(ConfigurationError, match="Unknown configuration key"):
            load_config(
                config_dir=config_dir,
                environ={f"{ENV_PREFIX}ANALYTICS_THREADS": "4"},
            )


class TestValidation:
    def test_malformed_toml_names_the_file(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        path = write_layer("base.toml", "[analytics\nthreads = 4\n")
        with pytest.raises(ConfigurationError) as exc_info:
            load_config(config_dir=config_dir, environ={})
        assert exc_info.value.context["path"] == str(path)

    def test_unknown_environment_lists_valid_values(self, config_dir: Path) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            load_config(environment="staging", config_dir=config_dir, environ={})
        assert "local" in exc_info.value.context["valid"]

    @pytest.mark.parametrize("threads", [0, -1, 9999])
    def test_thread_count_is_bounded(self, threads: int) -> None:
        with pytest.raises(ConfigurationError, match="threads must be between"):
            AnalyticsConfig(threads=threads)

    @pytest.mark.parametrize("limit", ["8 gigabytes", "lots", "8", "-8GB"])
    def test_memory_limit_must_be_a_size_string(self, limit: str) -> None:
        with pytest.raises(ConfigurationError, match="memory_limit must be"):
            AnalyticsConfig(memory_limit=limit)

    @pytest.mark.parametrize("limit", ["8GB", "512MB", "24 GiB", "1.5TB"])
    def test_valid_memory_limits_are_accepted(self, limit: str) -> None:
        assert AnalyticsConfig(memory_limit=limit).memory_limit == limit

    def test_unknown_log_level_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="Unknown log level"):
            LoggingConfig(level="CHATTY")

    def test_unknown_log_format_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="format must be"):
            LoggingConfig(format="xml")


class TestCrossFieldInvariants:
    def test_prod_rejects_create_missing(self) -> None:
        """Prod must fail closed on a missing storage location.

        Silently creating it would produce a second, ungoverned copy of a
        dataset that no lineage graph knows about and nothing backs up.
        """
        with pytest.raises(ConfigurationError, match="create_missing must be false in prod"):
            PlatformConfig(
                environment=Environment.PROD,
                storage=StorageConfig(create_missing=True),
            )

    def test_prod_accepts_create_missing_false(self) -> None:
        resolved = PlatformConfig(
            environment=Environment.PROD,
            storage=StorageConfig(create_missing=False),
        )
        assert resolved.storage.create_missing is False

    def test_ci_rejects_console_logging(self) -> None:
        with pytest.raises(ConfigurationError, match="must be 'json' in ci"):
            PlatformConfig(
                environment=Environment.CI,
                logging=LoggingConfig(format="console"),
            )

    def test_committed_prod_layer_satisfies_its_own_invariant(self) -> None:
        """The committed prod layer must actually load.

        A config file that violates a cross-field invariant would only be
        discovered on a prod deployment, which is the worst place to find out.
        """
        resolved = load_config(environment="prod", environ={})
        assert resolved.environment is Environment.PROD
        assert resolved.storage.create_missing is False

    @pytest.mark.parametrize("environment", ["local", "ci", "dev", "prod"])
    def test_every_committed_layer_loads(self, environment: str) -> None:
        assert load_config(environment=environment, environ={}) is not None


class TestImmutability:
    def test_config_is_frozen(self) -> None:
        resolved = PlatformConfig()
        with pytest.raises((AttributeError, TypeError)):
            resolved.environment = Environment.PROD  # type: ignore[misc]

    def test_sections_are_frozen(self) -> None:
        analytics = AnalyticsConfig(threads=4)
        with pytest.raises((AttributeError, TypeError)):
            analytics.threads = 8  # type: ignore[misc]


class TestHostDefaults:
    def test_threads_are_derived_when_unset(self) -> None:
        resolved = AnalyticsConfig().with_host_defaults()
        assert resolved.threads is not None
        assert resolved.threads >= 1

    def test_explicit_threads_are_preserved(self) -> None:
        """A deliberate value must survive. Benchmarks depend on it."""
        assert AnalyticsConfig(threads=3).with_host_defaults().threads == 3

    def test_derived_threads_match_the_available_cpu_quota(self) -> None:
        """Full CPU count, not a halved approximation of physical cores.

        Measured on a 16-thread hybrid CPU, DuckDB's grouped aggregation scales
        near-linearly past the physical core count and peaks around 12 threads;
        capping at ``logical // 2`` would forfeit roughly 20% of throughput.
        See AnalyticsConfig.with_host_defaults for the measurements.
        """
        assert AnalyticsConfig().with_host_defaults().threads == min(available_cpus(), 256)

    def test_available_cpus_is_positive_on_every_supported_python(self) -> None:
        """os.process_cpu_count is 3.13+; this package supports 3.12.

        The fallback chain is required, not defensive, so it is asserted
        directly rather than left to the 3.13 path in CI.
        """
        assert available_cpus() >= 1

    def test_derived_threads_respect_the_validation_ceiling(self) -> None:
        """The derived value must satisfy the same bound as a configured one."""
        resolved = AnalyticsConfig().with_host_defaults()
        assert AnalyticsConfig(threads=resolved.threads).threads == resolved.threads


class TestStoragePaths:
    def test_relative_root_anchors_to_the_checkout_not_cwd(self, config_dir: Path) -> None:
        """Behaviour must not depend on the directory a command was run from."""
        resolved = load_config(config_dir=config_dir, environ={}, storage={"root": "data"})
        assert resolved.storage.root.is_absolute()
        assert resolved.storage.root == anchor_dir() / "data"

    def test_anchor_dir_is_the_checkout_root_in_a_source_tree(self) -> None:
        assert anchor_dir() == repo_root()

    def test_repo_root_finds_the_pyproject_directory(self) -> None:
        checkout = repo_root()
        assert checkout is not None
        assert (checkout / "pyproject.toml").is_file()

    def test_anchor_never_resolves_inside_site_packages(self) -> None:
        """A wheel install must not write its lakehouse into site-packages.

        repo_root() previously fell back to parents[2], which resolved to
        site-packages when installed as a wheel: storage.root = "data" pointed
        inside the installed package and the committed config layers were
        reported absent. It now returns None and anchor_dir() falls back to the
        working directory.
        """
        assert "site-packages" not in anchor_dir().parts

    def test_absolute_root_is_left_alone(self, config_dir: Path, tmp_path: Path) -> None:
        resolved = load_config(
            config_dir=config_dir,
            environ={},
            storage={"root": str(tmp_path)},
        )
        assert resolved.storage.root == tmp_path

    def test_layer_path_includes_zone_name(self, storage_root: Path) -> None:
        storage = StorageConfig(root=storage_root)
        assert storage.layer_path(MedallionLayer.BRONZE).name == "bronze"

    def test_dataset_path_is_created_when_enabled(self, storage_root: Path) -> None:
        storage = StorageConfig(root=storage_root, create_missing=True)
        path = storage.dataset_path(MedallionLayer.SILVER, "equity_daily_bars")
        assert path.is_dir()

    def test_dataset_path_is_not_created_when_disabled(self, storage_root: Path) -> None:
        storage = StorageConfig(root=storage_root, create_missing=False)
        path = storage.dataset_path(MedallionLayer.SILVER, "equity_daily_bars")
        assert not path.exists()

    @pytest.mark.parametrize(
        "dataset",
        ["../escape", "nested/path", "back\\slash", "..", " padded ", ""],
    )
    def test_traversal_and_blank_dataset_names_are_rejected(
        self, dataset: str, storage_root: Path
    ) -> None:
        """Dataset names arrive from config files and CLI arguments.

        Validating centrally means no call site has to remember to.
        """
        storage = StorageConfig(root=storage_root)
        with pytest.raises(ConfigurationError):
            storage.dataset_path(MedallionLayer.RAW, dataset)


class TestLayerIntrospection:
    def test_layers_are_reported_in_application_order(self) -> None:
        names = [path.name for path, _ in describe_layers(Environment.LOCAL)]
        assert names == ["base.toml", "local.toml", OVERRIDE_LAYER_NAME]

    def test_override_layer_absent_for_non_local(self) -> None:
        names = [path.name for path, _ in describe_layers(Environment.PROD)]
        assert OVERRIDE_LAYER_NAME not in names

    def test_resolve_layers_returns_untyped_mapping(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        write_layer("base.toml", "[analytics]\nthreads = 4\n")
        merged = resolve_layers(Environment.LOCAL, config_dir=config_dir, environ={})
        assert merged == {"analytics": {"threads": 4}}


class TestEnvironmentEnum:
    @pytest.mark.parametrize(
        ("environment", "ephemeral"),
        [
            (Environment.LOCAL, True),
            (Environment.CI, True),
            (Environment.DEV, False),
            (Environment.PROD, False),
        ],
    )
    def test_ephemerality(self, environment: Environment, ephemeral: bool) -> None:
        assert environment.is_ephemeral is ephemeral

    def test_env_var_selects_environment(self, config_dir: Path) -> None:
        resolved = load_config(config_dir=config_dir, environ={f"{ENV_PREFIX}ENV": "dev"})
        assert resolved.environment is Environment.DEV

    def test_explicit_argument_beats_env_var(self, config_dir: Path) -> None:
        resolved = load_config(
            environment="ci",
            config_dir=config_dir,
            environ={f"{ENV_PREFIX}ENV": "dev"},
        )
        assert resolved.environment is Environment.CI

    def test_environment_cannot_be_set_from_a_toml_layer(
        self, config_dir: Path, write_layer: Callable[[str, str], Path]
    ) -> None:
        """A layer that could rename its own environment would be a trap."""
        write_layer("base.toml", 'environment = "prod"\n')
        resolved = load_config(config_dir=config_dir, environ={})
        assert resolved.environment is Environment.LOCAL


class TestMedallionLayer:
    def test_zones_are_declared_in_promotion_order(self) -> None:
        assert [layer.value for layer in MedallionLayer] == [
            "raw",
            "bronze",
            "silver",
            "gold",
        ]


class TestIngestionConfig:
    def test_default_symbols_are_the_phase_one_universe(self) -> None:
        assert set(IngestionConfig().symbols) == {"SPY", "QQQ", "DIA", "AAPL", "MSFT", "NVDA"}

    def test_empty_symbols_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="must not be empty"):
            IngestionConfig(symbols=())

    def test_blank_symbol_entry_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="blank entries"):
            IngestionConfig(symbols=("SPY", "   "))

    def test_case_insensitive_duplicate_symbol_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="duplicates"):
            IngestionConfig(symbols=("SPY", "spy"))

    @pytest.mark.parametrize("max_retries", [-1, 11])
    def test_max_retries_out_of_bounds_is_rejected(self, max_retries: int) -> None:
        with pytest.raises(ConfigurationError, match="max_retries must be between"):
            IngestionConfig(symbols=("SPY",), max_retries=max_retries)

    def test_non_positive_timeout_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="request_timeout_seconds must be positive"):
            IngestionConfig(symbols=("SPY",), request_timeout_seconds=0)

    def test_backoff_max_below_base_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="backoff_max_seconds must be"):
            IngestionConfig(symbols=("SPY",), backoff_base_seconds=5.0, backoff_max_seconds=1.0)

    def test_non_https_base_url_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="must use https"):
            IngestionConfig(symbols=("SPY",), base_url="http://example.com")

    def test_ingestion_section_resolves_through_load_config(self, config_dir: Path) -> None:
        (config_dir / "base.toml").write_text(
            '[ingestion]\nsymbols = ["SPY", "QQQ"]\n', encoding="utf-8"
        )
        resolved = load_config(config_dir=config_dir, environ={})
        assert resolved.ingestion.symbols == ["SPY", "QQQ"]

    def test_committed_base_layer_ingestion_section_loads(self) -> None:
        resolved = load_config(environment="local", environ={})
        assert set(resolved.ingestion.symbols) == {
            "SPY",
            "QQQ",
            "DIA",
            "AAPL",
            "MSFT",
            "NVDA",
        }
