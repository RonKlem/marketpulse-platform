"""Trade and quote event generation and ingestion.

Status: not yet implemented (Phase 4).

The most likely trigger for crossing the promotion boundary: the local stack
has no equivalent to Structured Streaming's checkpointing and exactly-once
semantics (ADR-0002).

The interesting correctness question is reconciliation -- whether streaming
aggregates agree with the batch daily bars describing the same reality.
"""

from __future__ import annotations

__all__: list[str] = []
