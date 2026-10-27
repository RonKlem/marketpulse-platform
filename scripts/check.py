#!/usr/bin/env python
"""Run the checks CI runs, in CI's order, locally.

Exists because there is no ``make`` on a stock Windows machine and this project
is developed on Windows. A shell script or Makefile would work for contributors
on Linux and silently not for everyone else, so the runner is Python — the one
interpreter every contributor is guaranteed to have.

    python scripts/check.py           # lint, format, types, tests
    python scripts/check.py --fix     # apply lint and format fixes first
    python scripts/check.py --fast    # skip coverage and the architecture tier

Ordering matters: lint and format are cheapest and catch the most common
mistakes, so they run before the type checker and the suite. Every step runs
even after one fails, because fixing three reported problems in one pass beats
three edit-run cycles.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time

Step = tuple[str, list[str]]


def build_steps(*, fix: bool, fast: bool) -> list[Step]:
    """Return the ordered checks to run."""
    lint: list[str] = ["ruff", "check", "."]
    fmt: list[str] = ["ruff", "format", "."]
    if fix:
        lint.append("--fix")
    else:
        fmt.extend(["--check", "--diff"])

    steps: list[Step] = [
        ("lint", lint),
        ("format", fmt),
        ("types", ["mypy"]),
    ]
    if fast:
        steps.append(("tests", ["pytest", "-q", "-m", "not architecture"]))
    else:
        steps.append(("tests", ["pytest", "-q"]))
        steps.append(("coverage", ["pytest", "-q", "--cov", "--cov-report=term-missing"]))
    return steps


def run(name: str, command: list[str]) -> tuple[bool, float]:
    """Run one step, streaming its output. Returns success and elapsed seconds."""
    # Invoked via `python -m` so the step uses the interpreter running this
    # script. Calling the bare executable would silently use whichever copy is
    # first on PATH, which on a machine with a global install is often not the
    # virtual environment's.
    argv = [sys.executable, "-m", *command]
    print(f"\n=== {name}: {' '.join(command)} ===", flush=True)
    started = time.perf_counter()
    completed = subprocess.run(argv, check=False)  # noqa: S603 - fixed argv, no shell
    return completed.returncode == 0, time.perf_counter() - started


def main() -> int:
    """Run every check and report a summary."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--fix", action="store_true", help="Apply lint and format fixes.")
    parser.add_argument("--fast", action="store_true", help="Skip coverage and architecture.")
    args = parser.parse_args()

    if shutil.which(sys.executable) is None:  # pragma: no cover - defensive
        print("Cannot locate the running interpreter.", file=sys.stderr)
        return 1

    results: list[tuple[str, bool, float]] = []
    for name, command in build_steps(fix=args.fix, fast=args.fast):
        ok, elapsed = run(name, command)
        results.append((name, ok, elapsed))

    print("\n=== summary ===")
    width = max(len(name) for name, _, _ in results)
    for name, ok, elapsed in results:
        print(f"  {name.ljust(width)}  {'pass' if ok else 'FAIL'}  {elapsed:6.2f}s")

    failed = [name for name, ok, _ in results if not ok]
    if failed:
        print(f"\n{len(failed)} step(s) failed: {', '.join(failed)}")
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
