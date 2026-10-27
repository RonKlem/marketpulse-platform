"""Configuration resolution: files, then environment, then explicit overrides.

Precedence (lowest to highest)
------------------------------
1. Dataclass defaults in :mod:`marketpulse.config.models`.
2. ``config/base.toml`` — settings true in every environment.
3. ``config/{environment}.toml`` — per-environment settings, committed.
4. ``config/override.toml`` — developer-local, git-ignored, never loaded
   outside :attr:`~marketpulse.config.models.Environment.LOCAL`.
5. ``MARKETPULSE_*`` environment variables.
6. Keyword overrides passed to :func:`load_config`, used by tests.

This ordering is the standard twelve-factor arrangement, with one deliberate
restriction: ``override.toml`` is ignored outside ``LOCAL``. A file that exists
on a developer's machine and is absent in CI is a reproducibility hazard, so it
is scoped to the only environment where that asymmetry is acceptable. The file
is named ``override.toml`` rather than ``local.toml`` so that it cannot be
confused with — or collide with — the committed ``local.toml`` environment
layer.

Why TOML plus environment variables rather than TOML alone
----------------------------------------------------------
Files express structure legibly and are reviewable in a pull request; they are
also baked into an image or a repository checkout. Environment variables are
the only layer available to a Databricks job cluster or a GitHub Actions
runner whose settings are injected at launch. Supporting both means the same
resolution code path works in all three places, and secrets never need to
exist in a file.

Why resolution is eager and cached rather than lazy
---------------------------------------------------
``load_config`` validates the whole tree at process start and caches the
result. A lazily-validated config fails at first use, which in a pipeline means
after the expensive read. Failing before any resource is acquired is cheaper
and far easier to diagnose.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
import difflib
from functools import lru_cache
import os
from pathlib import Path
import tomllib
from typing import TYPE_CHECKING, Any, Final, get_args, get_type_hints

from marketpulse.config.models import (
    AnalyticsConfig,
    Environment,
    LoggingConfig,
    PlatformConfig,
    StorageConfig,
)
from marketpulse.core.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "CONFIG_DIR_NAME",
    "ENV_PREFIX",
    "OVERRIDE_LAYER_NAME",
    "anchor_dir",
    "clear_config_cache",
    "describe_layers",
    "load_config",
    "repo_root",
    "resolve_layers",
]

ENV_PREFIX: Final = "MARKETPULSE_"
"""Prefix claimed by this platform. Variables use ``__`` to nest sections."""

ENV_NESTING_DELIMITER: Final = "__"
"""``MARKETPULSE_ANALYTICS__THREADS`` sets ``analytics.threads``.

A double underscore is required because section and field names contain single
underscores (``duckdb_database``), so a single underscore cannot distinguish a
nesting boundary from part of a name.
"""

CONFIG_DIR_NAME: Final = "config"

OVERRIDE_LAYER_NAME: Final = "override.toml"
"""Git-ignored developer layer, applied last and only in ``LOCAL``.

Deliberately not named ``local.toml``: that name is already taken by the
committed environment layer for ``Environment.LOCAL``, and a file serving as
both would be loaded twice and be ambiguous to reason about.
"""

ENV_VAR_ENVIRONMENT: Final = f"{ENV_PREFIX}ENV"
_ENV_VAR_CONFIG_DIR: Final = f"{ENV_PREFIX}CONFIG_DIR"
_ENV_VAR_ROOT: Final = f"{ENV_PREFIX}ROOT"

RESOLUTION_CONTROL_VARS: Final[frozenset[str]] = frozenset(
    {ENV_VAR_ENVIRONMENT, _ENV_VAR_CONFIG_DIR, _ENV_VAR_ROOT}
)
"""Variables that steer resolution itself rather than supplying a config value.

These carry no ``__``, so without this exclusion the override parser would read
``MARKETPULSE_ROOT`` as a root-level config key named ``root`` and reject it as
unknown. Any future control variable must be added here, or setting it becomes
a startup error.
"""

_SECTION_TYPES: Final[dict[str, type]] = {
    "storage": StorageConfig,
    "analytics": AnalyticsConfig,
    "logging": LoggingConfig,
}

_TRUE_VALUES: Final = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES: Final = frozenset({"0", "false", "no", "off"})


def repo_root() -> Path | None:
    """Return the source checkout root, or ``None`` when installed as a wheel.

    Resolved by walking up from this module to the directory containing
    ``pyproject.toml``. Deriving it from the module's own location rather than
    ``Path.cwd()`` means relative paths mean the same thing whether a command
    runs from the repository root, a subdirectory, or a notebook.

    Returns ``None`` rather than guessing when no ``pyproject.toml`` is found.
    That is the normal situation on a Databricks cluster, where the wheel is
    installed into ``site-packages`` and there is no checkout. An earlier
    version returned ``parents[2]`` as a fallback, which silently resolved to
    ``site-packages`` — so ``storage.root = "data"`` pointed inside the
    installed package, and the committed config layers were reported as absent.
    A deployment writing its lakehouse into ``site-packages`` is a failure that
    must be explicit.

    Returns:
        The directory containing ``pyproject.toml``, or ``None``.
    """
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return None


def anchor_dir() -> Path:
    """Return the directory that relative configuration paths resolve against.

    Precedence:

    1. ``$MARKETPULSE_ROOT``, for a deployment that needs to state it outright.
    2. The source checkout root, when running from one.
    3. The current working directory, as the last resort.

    Falling back to the working directory keeps a wheel install usable — a job
    writes under wherever it was launched — while never writing into
    ``site-packages``. A deployment that cares about the location should set
    ``MARKETPULSE_ROOT`` or an absolute ``storage.root``.

    Returns:
        An absolute directory path.
    """
    if override := os.environ.get(_ENV_VAR_ROOT):
        return Path(override).expanduser().resolve()
    return repo_root() or Path.cwd().resolve()


def _config_dir() -> Path:
    """Return the directory holding TOML layers."""
    if override := os.environ.get(_ENV_VAR_CONFIG_DIR):
        return Path(override).expanduser().resolve()
    return anchor_dir() / CONFIG_DIR_NAME


def _read_toml(path: Path) -> dict[str, Any]:
    """Parse one TOML layer.

    Args:
        path: File to read. A missing file yields an empty mapping, because
            every layer above ``base.toml`` is optional by design.

    Returns:
        The parsed document.

    Raises:
        ConfigurationError: If the file exists but is not valid TOML, or cannot
            be read. Both are startup failures that must not be swallowed.
    """
    if not path.is_file():
        return {}
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        msg = "Configuration file is not valid TOML"
        raise ConfigurationError(msg, path=str(path), reason=str(exc)) from exc
    except OSError as exc:
        msg = "Configuration file could not be read"
        raise ConfigurationError(msg, path=str(path), reason=str(exc)) from exc


def _deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Merge ``overlay`` onto ``base``, recursing into nested mappings.

    Section-aware rather than top-level: an environment layer that sets only
    ``analytics.threads`` must not discard ``analytics.memory_limit`` from the
    base layer. Scalars and lists are replaced wholesale, since a partial list
    merge has no unambiguous meaning.

    Args:
        base: Lower-precedence mapping.
        overlay: Higher-precedence mapping.

    Returns:
        A new merged mapping. Neither input is modified.
    """
    merged = dict(base)
    for key, value in overlay.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def _coerce_scalar(raw: str) -> Any:
    """Interpret an environment variable string as a TOML-ish scalar.

    Environment variables are untyped strings, but the dataclasses expect
    ``int``, ``bool``, and ``Path``. Rather than guess per field, this applies
    one consistent widening: booleans, then integers, then floats, then the
    string unchanged. Field-level validation in the dataclass catches anything
    that slips through with the wrong type.

    Args:
        raw: Raw environment variable value.

    Returns:
        A ``bool``, ``int``, ``float``, ``None``, or the original string.
    """
    value = raw.strip()
    lowered = value.lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    if lowered in ("", "null", "none"):
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _env_overrides(environ: Mapping[str, str]) -> dict[str, Any]:
    """Build a nested override mapping from ``MARKETPULSE_*`` variables.

    Only variables whose prefix-stripped name maps to a known section are
    nested; everything else lands at the top level, where unknown-key
    validation will reject it. Silently dropping an unrecognised variable would
    let a typo like ``MARKETPULSE_ANALYTIC__THREADS`` appear to work.

    Args:
        environ: Environment mapping to read.

    Returns:
        A nested mapping shaped like the TOML layers.
    """
    overrides: dict[str, Any] = {}
    for key, raw in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        name = key.removeprefix(ENV_PREFIX)
        if not name or key in RESOLUTION_CONTROL_VARS:
            continue

        if ENV_NESTING_DELIMITER in name:
            section, _, leaf = name.partition(ENV_NESTING_DELIMITER)
            section_key = section.lower()
            leaf_key = leaf.lower()
            if not leaf_key:
                msg = "Environment variable has an empty field name after the delimiter"
                raise ConfigurationError(msg, variable=key)
            overrides.setdefault(section_key, {})[leaf_key] = _coerce_scalar(raw)
        else:
            overrides[name.lower()] = _coerce_scalar(raw)
    return overrides


def resolve_layers(
    environment: Environment,
    *,
    config_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Merge every configuration layer into one mapping, without validating it.

    Exposed separately from :func:`load_config` so that tests and a
    ``config show --layers`` style diagnostic can inspect the merge result
    before it is turned into dataclasses.

    Args:
        environment: Environment whose layer to apply.
        config_dir: Directory holding the TOML layers. Defaults to
            ``$MARKETPULSE_CONFIG_DIR``, then ``<repo>/config``.
        environ: Environment mapping. Defaults to ``os.environ``.

    Returns:
        The merged, still-untyped configuration mapping.
    """
    directory = config_dir if config_dir is not None else _config_dir()
    env = environ if environ is not None else os.environ

    layer_files: list[Path] = [
        directory / "base.toml",
        directory / f"{environment.value}.toml",
    ]
    # override.toml is git-ignored, so loading it anywhere but LOCAL would make
    # a process's behaviour depend on a file that does not exist in CI or prod.
    if environment is Environment.LOCAL:
        layer_files.append(directory / OVERRIDE_LAYER_NAME)

    merged: dict[str, Any] = {}
    for path in layer_files:
        merged = _deep_merge(merged, _read_toml(path))
    return _deep_merge(merged, _env_overrides(env))


def _known_field_names(cls: type) -> frozenset[str]:
    """Return the field names of a dataclass."""
    if not is_dataclass(cls):  # pragma: no cover - guarded by _SECTION_TYPES
        msg = f"{cls!r} is not a dataclass"
        raise TypeError(msg)
    return frozenset(f.name for f in fields(cls))


def _reject_unknown(
    section: str,
    payload: Mapping[str, Any],
    known: frozenset[str],
) -> None:
    """Fail on configuration keys that no field consumes.

    An ignored key is the most expensive kind of configuration bug: the
    operator believes a setting is in effect and the process behaves as though
    it were never written. Suggesting a near-miss turns a silent no-op into a
    one-line fix.

    Args:
        section: Section name, for the error message.
        payload: Keys supplied for the section.
        known: Field names the section accepts.

    Raises:
        ConfigurationError: If ``payload`` contains an unknown key.
    """
    unknown = sorted(set(payload) - known)
    if not unknown:
        return

    hints = {
        key: match[0]
        for key in unknown
        if (match := difflib.get_close_matches(key, sorted(known), n=1, cutoff=0.6))
    }
    msg = f"Unknown configuration key(s) in section {section!r}"
    raise ConfigurationError(
        msg,
        unknown=unknown,
        did_you_mean=hints or None,
        valid_keys=sorted(known),
    )


def _path_field_names(cls: type[Any]) -> frozenset[str]:
    """Return the fields of ``cls`` that accept a :class:`~pathlib.Path`.

    Resolved from the class's type hints rather than by string-matching
    ``Field.type``, which is an unresolved string under
    ``from __future__ import annotations`` and would silently stop matching if
    a field were ever annotated as ``os.PathLike`` or imported under an alias.
    """
    resolved = get_type_hints(cls)
    return frozenset(
        name for name, hint in resolved.items() if hint is Path or Path in get_args(hint)
    )


_PATH_FIELDS: Final[dict[str, frozenset[str]]] = {
    name: _path_field_names(cls) for name, cls in _SECTION_TYPES.items()
}
"""Path-typed field names per section, resolved once at import.

Computed eagerly rather than memoised per call because the section types are a
fixed, closed set. ``get_type_hints`` is not cheap, and doing it at import
keeps it off the configuration-load path entirely.
"""


def _build_section[T](name: str, cls: type[T], payload: Mapping[str, Any]) -> T:
    """Instantiate one configuration section from a raw mapping.

    Args:
        name: Section name, for error messages.
        cls: Target dataclass.
        payload: Raw values for the section.

    Returns:
        The constructed section.

    Raises:
        ConfigurationError: If a key is unknown or a value is the wrong type.
    """
    _reject_unknown(name, payload, _known_field_names(cls))

    # Path-typed fields arrive as strings from TOML and the environment, so they
    # are converted once here rather than defensively at every consumer.
    path_fields = _PATH_FIELDS.get(name) or _path_field_names(cls)
    converted: dict[str, Any] = {
        key: Path(value).expanduser() if key in path_fields and isinstance(value, str) else value
        for key, value in payload.items()
    }

    try:
        return cls(**converted)
    except ConfigurationError:
        raise
    except TypeError as exc:
        msg = f"Invalid value type in section {name!r}"
        raise ConfigurationError(msg, reason=str(exc)) from exc


def _resolve_environment(
    explicit: Environment | str | None,
    environ: Mapping[str, str],
) -> Environment:
    """Determine the active environment.

    Args:
        explicit: Caller-supplied environment, if any.
        environ: Environment mapping to fall back on.

    Returns:
        The resolved environment, defaulting to ``LOCAL``.

    Raises:
        ConfigurationError: If the value is not a known environment.
    """
    raw = explicit if explicit is not None else environ.get(ENV_VAR_ENVIRONMENT, "local")
    if isinstance(raw, Environment):
        return raw
    try:
        return Environment(str(raw).strip().lower())
    except ValueError as exc:
        msg = "Unknown environment"
        raise ConfigurationError(
            msg,
            environment=raw,
            valid=[e.value for e in Environment],
        ) from exc


def _absolutise(storage: StorageConfig) -> StorageConfig:
    """Anchor a relative storage root to :func:`anchor_dir`.

    In a source checkout this is the repository root, so a relative ``data``
    means the same directory regardless of where a command was invoked from.
    Resolving against the working directory instead would silently produce an
    empty dataset rather than an error when run from a subdirectory.

    Args:
        storage: Section to normalise.

    Returns:
        A copy whose ``root`` is absolute, or ``storage`` if it already was.
    """
    if storage.root.is_absolute():
        return storage
    return replace(storage, root=(anchor_dir() / storage.root).resolve())


def load_config(
    *,
    environment: Environment | str | None = None,
    config_dir: Path | None = None,
    environ: Mapping[str, str] | None = None,
    **overrides: Any,
) -> PlatformConfig:
    """Resolve, validate, and return the platform configuration.

    Args:
        environment: Target environment. Defaults to ``$MARKETPULSE_ENV``, then
            ``local``.
        config_dir: Directory holding TOML layers. Defaults to
            ``$MARKETPULSE_CONFIG_DIR``, then ``<repo>/config``.
        environ: Environment mapping. Defaults to ``os.environ``. Injectable so
            that tests never mutate the real process environment.
        **overrides: Highest-precedence values, nested by section, e.g.
            ``analytics={"threads": 2}``.

    Returns:
        A fully validated, immutable configuration.

    Raises:
        ConfigurationError: If any layer is malformed, any key is unknown, or
            any cross-field invariant fails.
    """
    env = environ if environ is not None else os.environ
    resolved_env = _resolve_environment(environment, env)

    merged = _deep_merge(
        resolve_layers(resolved_env, config_dir=config_dir, environ=env),
        overrides,
    )
    # The environment is settled before merging and is not a configurable field;
    # a TOML layer that could rename its own environment would be a trap.
    merged.pop("env", None)
    merged.pop("environment", None)

    extras = merged.pop("extras", {})
    if not isinstance(extras, dict):
        msg = "extras must be a table"
        raise ConfigurationError(msg, extras_type=type(extras).__name__)

    sections: dict[str, Any] = {}
    for name, cls in _SECTION_TYPES.items():
        payload = merged.pop(name, {})
        if not isinstance(payload, dict):
            msg = f"Section {name!r} must be a table"
            raise ConfigurationError(msg, section=name, got=type(payload).__name__)
        sections[name] = _build_section(name, cls, payload)

    _reject_unknown("<root>", merged, frozenset(_SECTION_TYPES) | {"extras"})

    storage: StorageConfig = _absolutise(sections["storage"])
    analytics: AnalyticsConfig = sections["analytics"].with_host_defaults()

    return PlatformConfig(
        environment=resolved_env,
        storage=storage,
        analytics=analytics,
        logging=sections["logging"],
        extras=dict(extras),
    )


@lru_cache(maxsize=1)
def _cached_config(environment: str | None) -> PlatformConfig:
    """Memoised backing store for :func:`get_config`."""
    return load_config(environment=environment)


def get_config(environment: Environment | str | None = None) -> PlatformConfig:
    """Return the process-wide configuration, resolving it at most once.

    Use this from code that needs configuration but is not an entry point.
    Entry points should call :func:`load_config` explicitly and pass the result
    down, which keeps the dependency visible in signatures rather than hidden
    in a module-level lookup.

    Args:
        environment: Target environment, forwarded to :func:`load_config` on
            the first call.

    Returns:
        The cached configuration.
    """
    key = environment.value if isinstance(environment, Environment) else environment
    return _cached_config(key)


def clear_config_cache() -> None:
    """Drop the memoised configuration.

    Required in any test that changes the environment or config directory,
    since :func:`get_config` would otherwise return a value resolved under the
    previous conditions.
    """
    _cached_config.cache_clear()


def describe_layers(
    environment: Environment,
    *,
    config_dir: Path | None = None,
) -> Sequence[tuple[Path, bool]]:
    """Report which configuration files exist for an environment.

    Supports the ``marketpulse config`` diagnostic. "Which file did this value
    come from" is the first question asked when a setting is not what an
    operator expected.

    Args:
        environment: Environment to describe.
        config_dir: Directory holding TOML layers.

    Returns:
        Layer paths in application order, each paired with whether it exists.
    """
    directory = config_dir if config_dir is not None else _config_dir()
    paths = [directory / "base.toml", directory / f"{environment.value}.toml"]
    if environment is Environment.LOCAL:
        paths.append(directory / OVERRIDE_LAYER_NAME)
    return [(p, p.is_file()) for p in paths]
