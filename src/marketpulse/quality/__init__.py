"""Data quality expectations and validation results.

Status: not yet implemented (Phase 2).

Market data failures are semantic rather than structural: a structurally
perfect row carrying a split-adjusted price next to an unadjusted volume, a
stale quote repeated through a halt, a close outside the day's high-low range.

Schema validation cannot catch these, which is why expectations here are
domain-aware. A violation raises
:class:`~marketpulse.core.errors.DataQualityError` rather than
:class:`~marketpulse.core.errors.AnalyticsError` -- the engine worked and the
data is wrong, which is a different runbook.
"""

from __future__ import annotations

__all__: list[str] = []
