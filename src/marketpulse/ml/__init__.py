"""Feature pipelines, training, registry, and model monitoring.

Status: not yet implemented (Phase 7).

The platform concern is point-in-time correctness, not model accuracy.

FRED revises published figures, so training on revised values and evaluating
against history produces look-ahead bias -- the model appears to work and would
have been unusable in production. Reconstructing a training set as it was known
on a given date needs platform support, not a notebook convention (ADR-0001).
"""

from __future__ import annotations

__all__: list[str] = []
