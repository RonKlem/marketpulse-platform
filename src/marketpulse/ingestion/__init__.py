"""Source connectors: Yahoo Finance, FRED, simulated events.

Status: not yet implemented (Phase 1).

Each connector owns one source's quirks -- adjustment conventions, timestamp
semantics, pagination -- and exposes a uniform interface so that a vendor
changing its API breaks one connector rather than the platform.

Connectors land source-shaped bytes in the ``raw`` zone without modification.
Parsing happens in :mod:`marketpulse.transform`, because ``raw`` is the only
record of what a source actually said on a given date (ADR-0001).
"""

from __future__ import annotations

__all__: list[str] = []
