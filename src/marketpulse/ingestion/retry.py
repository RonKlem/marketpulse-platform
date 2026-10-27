"""Exponential backoff retry for transient connector failures.

Lives in ``ingestion`` rather than ``core`` because exactly one consumer
exists today (:mod:`marketpulse.ingestion.yahoo`). Promoting it to ``core``
ahead of a second connector would be exactly the kind of speculative,
built-for-the-future abstraction this platform avoids; it is cheap to move
later once a real second caller appears.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from marketpulse.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["with_retry"]

_log = get_logger(__name__)


def with_retry[T](
    func: Callable[[], T],
    *,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
    retry_on: tuple[type[Exception], ...],
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``func``, retrying on ``retry_on`` exceptions with capped backoff.

    Args:
        func: Zero-argument callable to invoke. Takes no arguments so the
            caller closes over whatever per-attempt state it needs, keeping
            this helper ignorant of what it is retrying.
        max_attempts: Total attempts, including the first. ``1`` disables
            retrying entirely.
        base_delay: Delay before the second attempt, in seconds. Each
            subsequent delay doubles: ``min(base_delay * 2**n, max_delay)``.
        max_delay: Ceiling on the computed delay, regardless of attempt count.
        retry_on: Exception types that warrant a retry. Anything else
            propagates immediately.
        sleep: Delay function. Injectable so a test can assert the attempt
            count and backoff sequence without a real wall-clock wait, the
            same injection pattern :func:`marketpulse.config.settings.load_config`
            uses for ``environ``.

    Returns:
        Whatever ``func`` returns on a successful attempt.

    Raises:
        ValueError: If ``max_attempts`` is less than 1.
        Exception: The last exception raised by ``func``, once every attempt
            is exhausted, or immediately if it is not a member of
            ``retry_on``.
    """
    if max_attempts < 1:
        msg = "max_attempts must be at least 1"
        raise ValueError(msg)

    for attempt in range(max_attempts - 1):
        try:
            return func()
        except retry_on as exc:
            delay = min(base_delay * (2**attempt), max_delay)
            _log.warning(
                "ingestion.retry",
                extra={
                    "attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "delay_seconds": delay,
                    "reason": str(exc),
                },
            )
            sleep(delay)

    return func()
