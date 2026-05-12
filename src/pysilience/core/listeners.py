"""Shared helpers for pattern event listeners (observability callbacks)."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def notify_listeners(listeners: list[Callable[[T], None]], event: T) -> None:
    """Invoke each listener with ``event``; swallow exceptions so callbacks cannot break callers."""
    for listener in listeners:
        with contextlib.suppress(Exception):
            listener(event)
