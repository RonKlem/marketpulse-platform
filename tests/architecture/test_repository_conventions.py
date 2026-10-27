"""Enforce the structural invariants that make this repository's claims true.

These tests exist because the properties they check are the ones that erode
silently. Nobody intends to import PySpark into the local stack or to merge an
ADR with no reversal condition; both happen on a busy week when a reviewer is
reading a diff rather than the whole tree. A failing test is a cheaper
correction than a design conversation six months later.

Each test here corresponds to a claim made in README.md or an ADR. If a claim
is not worth enforcing, it should be deleted from the documentation rather than
left as an aspiration.
"""

from __future__ import annotations

import ast
from pathlib import Path  # noqa: TC003  -- used at runtime to build module-level paths
import re
import tomllib
from typing import TYPE_CHECKING

import pytest

from marketpulse.config.settings import repo_root

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.architecture

_CHECKOUT = repo_root()
if _CHECKOUT is None:  # pragma: no cover - these tests require a source checkout
    pytest.skip(
        "Architecture tests inspect the repository tree and need a source checkout.",
        allow_module_level=True,
    )
ROOT = _CHECKOUT
SRC = ROOT / "src" / "marketpulse"
ADR_DIR = ROOT / "docs" / "adr"

# Engines that must never appear in the local stack. PySpark because the whole
# point of ADR-002 is that local development does not require a JVM; Pandas and
# NumPy because Polars supersedes them here and a stray import reintroduces an
# eager, single-threaded, row-oriented data path (ADR-002).
FORBIDDEN_IMPORTS = {
    "pyspark": "PySpark belongs in platform/databricks, not the local stack (ADR-002).",
    "pandas": "Use Polars. Pandas is eager and single-threaded (ADR-002).",
    "numpy": "Use Polars expressions (ADR-002).",
}


def python_modules(directory: Path) -> Iterator[Path]:
    """Yield every Python module under a directory."""
    return (path for path in directory.rglob("*.py") if "__pycache__" not in path.parts)


def imported_modules(path: Path) -> set[str]:
    """Return the top-level module names a file imports."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module.split(".")[0])
    return names


def marketpulse_imports(path: Path) -> set[str]:
    """Return the ``marketpulse.*`` submodules a file imports."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module.startswith("marketpulse"):
                names.add(node.module)
        elif isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names if alias.name.startswith("marketpulse"))
    return names


class TestEngineBoundary:
    """The local stack must not depend on distributed engines (ADR-002)."""

    @pytest.mark.parametrize("module", sorted(FORBIDDEN_IMPORTS))
    def test_forbidden_engine_is_absent_from_src(self, module: str) -> None:
        offenders = [
            path.relative_to(ROOT)
            for path in python_modules(SRC)
            if module in imported_modules(path)
        ]
        assert not offenders, f"{FORBIDDEN_IMPORTS[module]} Found in: {offenders}"

    def test_runtime_dependencies_stay_minimal(self) -> None:
        """The wheel is installed on Databricks clusters.

        Every extra runtime dependency is a potential conflict with a version
        the Databricks Runtime already pins, and that conflict affects every
        other library on the cluster, not just this one.
        """
        manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        declared = {
            re.split(r"[><=!\[]", spec, maxsplit=1)[0].strip().lower()
            for spec in manifest["project"]["dependencies"]
        }
        assert declared == {"polars", "duckdb", "pyarrow"}

    def test_no_dev_dependency_leaks_into_runtime(self) -> None:
        manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        runtime = " ".join(manifest["project"]["dependencies"]).lower()
        for tool in ("pytest", "ruff", "mypy", "pre-commit"):
            assert tool not in runtime


class TestLayering:
    """``core`` is the bottom of the dependency graph; nothing may invert it."""

    def test_core_imports_nothing_internal(self) -> None:
        """Errors and logging are imported by config and analytics.

        If core imported either of them back, the cycle would surface as an
        ImportError whose traceback points at an innocent module.
        """
        for path in python_modules(SRC / "core"):
            internal = {
                name
                for name in marketpulse_imports(path)
                if not name.startswith("marketpulse.core") and name != "marketpulse"
            }
            assert not internal, f"{path.relative_to(ROOT)} imports {internal}"

    def test_config_does_not_import_analytics(self) -> None:
        """Configuration must resolve before any engine is constructed."""
        for path in python_modules(SRC / "config"):
            offenders = {
                name
                for name in marketpulse_imports(path)
                if name.startswith("marketpulse.analytics")
            }
            assert not offenders, f"{path.relative_to(ROOT)} imports {offenders}"

    def test_package_root_exposes_the_error_hierarchy(self) -> None:
        import marketpulse

        for name in ("MarketPulseError", "ConfigurationError", "AnalyticsError"):
            assert hasattr(marketpulse, name)


class TestPackagingLayout:
    """``src/`` ships as a wheel; ``platform/`` is a deployment artifact."""

    def test_platform_directory_is_not_an_importable_package(self) -> None:
        """A top-level __init__.py under platform/ would make Spark code importable.

        The separation is what guarantees a Spark import cannot leak into the
        local stack, so it is enforced rather than documented.
        """
        platform_dir = ROOT / "platform"
        if platform_dir.is_dir():
            assert not (platform_dir / "__init__.py").exists()

    def test_only_the_src_package_is_distributed(self) -> None:
        manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        packages = manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
        assert packages == ["src/marketpulse"]

    def test_every_source_subpackage_has_an_init(self) -> None:
        for directory in SRC.rglob("*"):
            if not directory.is_dir() or "__pycache__" in directory.parts:
                continue
            if any(directory.glob("*.py")):
                assert (directory / "__init__.py").is_file(), directory.relative_to(ROOT)


class TestLoggingDiscipline:
    """A library configures no handlers and prints nothing."""

    def test_only_entry_points_configure_logging(self) -> None:
        """A library that installs handlers steals control from its host.

        On a Databricks cluster the runtime owns the log sinks; a library that
        reconfigures them loses records for every other library too.
        """
        allowed = {SRC / "cli.py", SRC / "core" / "logging.py"}
        for path in python_modules(SRC):
            if path in allowed:
                continue
            source = path.read_text(encoding="utf-8")
            assert "configure_logging(" not in source, path.relative_to(ROOT)

    def test_library_code_does_not_print(self) -> None:
        """stdout is reserved for machine-readable CLI output."""
        for path in python_modules(SRC):
            if path == SRC / "cli.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            calls = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "print"
            ]
            assert not calls, f"{path.relative_to(ROOT)} calls print()"

    def test_no_module_touches_the_root_logger(self) -> None:
        for path in python_modules(SRC):
            source = path.read_text(encoding="utf-8")
            assert "logging.basicConfig" not in source, path.relative_to(ROOT)


class TestConfigurationLayers:
    """Every committed layer must parse and load."""

    def test_base_layer_exists(self) -> None:
        assert (ROOT / "config" / "base.toml").is_file()

    @pytest.mark.parametrize("environment", ["local", "ci", "dev", "prod"])
    def test_environment_layer_exists_and_parses(self, environment: str) -> None:
        path = ROOT / "config" / f"{environment}.toml"
        assert path.is_file(), f"missing config/{environment}.toml"
        tomllib.loads(path.read_text(encoding="utf-8"))

    def test_override_template_is_committed(self) -> None:
        """A contributor needs a documented starting point for personal settings."""
        assert (ROOT / "config" / "override.example.toml").is_file()

    def test_personal_override_is_not_committed(self) -> None:
        """config/override.toml is git-ignored; committing it breaks reproducibility."""
        tracked = (ROOT / ".gitignore").read_text(encoding="utf-8")
        assert "config/override.toml" in tracked

    def test_committed_local_layer_is_not_gitignored(self) -> None:
        """local.toml is the shared environment layer and must be tracked."""
        ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        assert not re.search(r"^config/local\.toml\s*$", ignore, re.MULTILINE)


class TestDocumentation:
    def test_required_documents_exist(self) -> None:
        for name in ("README.md", "architecture.md", "CONTRIBUTING.md", "LICENSE"):
            assert (ROOT / name).is_file(), f"missing {name}"

    def test_local_analytics_document_exists(self) -> None:
        assert (ROOT / "docs" / "architecture" / "local-analytics.md").is_file()

    def test_adr_index_exists(self) -> None:
        assert (ADR_DIR / "README.md").is_file()

    def test_adr_template_exists(self) -> None:
        assert (ADR_DIR / "template.md").is_file()


def adr_files() -> list[Path]:
    """Return the numbered ADR files, excluding the index and template."""
    if not ADR_DIR.is_dir():
        return []
    return sorted(path for path in ADR_DIR.glob("[0-9][0-9][0-9][0-9]-*.md"))


class TestArchitectureDecisionRecords:
    """ADRs are the primary deliverable, so their conventions are enforced."""

    def test_the_required_adrs_are_present(self) -> None:
        numbers = {path.name[:4] for path in adr_files()}
        assert {"0001", "0002"} <= numbers

    def test_adr_numbers_are_unique(self) -> None:
        numbers = [path.name[:4] for path in adr_files()]
        assert len(numbers) == len(set(numbers))

    def test_adr_numbers_are_contiguous(self) -> None:
        """A gap means a record was deleted rather than superseded.

        Superseding preserves the reasoning; deleting discards it, which defeats
        the purpose of keeping decision records at all.
        """
        numbers = sorted(int(path.name[:4]) for path in adr_files())
        assert numbers == list(range(1, len(numbers) + 1))

    @pytest.mark.parametrize("adr", adr_files(), ids=lambda path: path.name)
    def test_adr_has_the_required_sections(self, adr: Path) -> None:
        body = adr.read_text(encoding="utf-8")
        for heading in ("## Status", "## Context", "## Decision", "## Consequences"):
            assert heading in body, f"{adr.name} is missing {heading!r}"

    @pytest.mark.parametrize("adr", adr_files(), ids=lambda path: path.name)
    def test_adr_records_rejected_alternatives(self, adr: Path) -> None:
        """A decision with no alternatives considered is an assumption.

        The alternatives are what demonstrate the decision was made rather than
        defaulted into.
        """
        body = adr.read_text(encoding="utf-8")
        assert "## Alternatives Considered" in body, f"{adr.name} records no alternatives"

    @pytest.mark.parametrize("adr", adr_files(), ids=lambda path: path.name)
    def test_adr_states_a_reversal_condition(self, adr: Path) -> None:
        """A decision record without a reversal condition is an opinion.

        Stating in advance what evidence would change the decision is what makes
        it reviewable later, when the original context is gone.
        """
        body = adr.read_text(encoding="utf-8")
        assert "## Revisit When" in body, f"{adr.name} states no reversal condition"

    @pytest.mark.parametrize("adr", adr_files(), ids=lambda path: path.name)
    def test_adr_declares_a_recognised_status(self, adr: Path) -> None:
        body = adr.read_text(encoding="utf-8")
        statuses = ("Proposed", "Accepted", "Deprecated", "Superseded", "Rejected")
        section = body.split("## Status", 1)[1].split("##", 1)[0]
        assert any(status in section for status in statuses), f"{adr.name} has no valid status"

    @pytest.mark.parametrize("adr", adr_files(), ids=lambda path: path.name)
    def test_adr_filename_matches_its_title(self, adr: Path) -> None:
        """Keeps the index, the filename, and the heading from drifting apart."""
        first_line = adr.read_text(encoding="utf-8").splitlines()[0]
        assert first_line.startswith("# ADR-"), f"{adr.name} has no '# ADR-NNNN' heading"
        assert adr.name[:4] in first_line, f"{adr.name} heading number does not match filename"

    @pytest.mark.parametrize("adr", adr_files(), ids=lambda path: path.name)
    def test_adr_is_linked_from_the_index(self, adr: Path) -> None:
        index = (ADR_DIR / "README.md").read_text(encoding="utf-8")
        assert adr.name in index, f"{adr.name} is not linked from docs/adr/README.md"


class TestTestSuiteStructure:
    def test_the_three_tiers_exist(self) -> None:
        for tier in ("unit", "integration", "architecture"):
            assert (ROOT / "tests" / tier).is_dir(), f"missing tests/{tier}"

    def test_unit_tests_do_not_import_the_engines(self) -> None:
        """A unit test that opens DuckDB is an integration test in the wrong tier.

        The tiering only has value if the fast tier is actually fast.
        """
        for path in python_modules(ROOT / "tests" / "unit"):
            imports = imported_modules(path)
            assert "duckdb" not in imports, f"{path.relative_to(ROOT)} imports duckdb"

    def test_no_test_reaches_the_network(self) -> None:
        """A test that hits a market data API fails when the market is closed."""
        forbidden = {"requests", "httpx", "urllib3", "yfinance", "aiohttp"}
        for path in python_modules(ROOT / "tests"):
            offenders = imported_modules(path) & forbidden
            assert not offenders, f"{path.relative_to(ROOT)} imports {offenders}"

    def test_warnings_are_errors(self) -> None:
        """A DeprecationWarning must fail the build that introduces it.

        Otherwise it is discovered during a future upgrade, as a breakage,
        without the context of the change that caused it.
        """
        manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        assert "error" in manifest["tool"]["pytest"]["ini_options"]["filterwarnings"]

    def test_coverage_gate_is_configured(self) -> None:
        manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        assert manifest["tool"]["coverage"]["report"]["fail_under"] >= 85


class TestContinuousIntegration:
    def test_ci_workflow_exists(self) -> None:
        assert (ROOT / ".github" / "workflows" / "ci.yml").is_file()

    def test_ci_runs_on_pull_requests(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "pull_request" in workflow

    def test_ci_runs_lint_types_and_tests(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        for command in ("ruff check", "ruff format", "mypy", "pytest"):
            assert command in workflow, f"CI does not run {command!r}"

    def test_ci_matrix_includes_windows(self) -> None:
        """The platform is developed on Windows; pathlib behaviour diverges there.

        A Linux-only CI suite validates a configuration no contributor uses.
        """
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "windows-latest" in workflow
