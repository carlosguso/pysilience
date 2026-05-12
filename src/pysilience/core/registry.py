"""Registry of named resilience pattern instances (for composition, testing, and tooling)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any

_lock = threading.Lock()
_instances: dict[tuple[str, str], Any] = {}


def register(kind: str, name: str, instance: object) -> None:
    """Register a pattern instance under ``(kind, name)`` (e.g. ``("retry", "api")``).

    Later registrations for the same key replace the previous instance.
    """
    if not name:
        raise ValueError("name must be non-empty")
    with _lock:
        _instances[(kind, name)] = instance


def unregister(kind: str, name: str) -> None:
    """Remove ``(kind, name)`` from the registry if present."""
    with _lock:
        _instances.pop((kind, name), None)


def get(kind: str, name: str) -> Any | None:
    """Return the registered instance, or ``None``."""
    with _lock:
        return _instances.get((kind, name))


def clear() -> None:
    """Remove all registrations (intended for tests)."""
    with _lock:
        _instances.clear()


def iter_by_kind(kind: str) -> Iterator[tuple[str, Any]]:
    """Yield ``(name, instance)`` pairs for a given pattern kind."""
    with _lock:
        snapshot = [(n, inst) for (k, n), inst in _instances.items() if k == kind]
    yield from snapshot


def all_instances() -> dict[tuple[str, str], Any]:
    """Return a shallow copy of the full registry mapping."""
    with _lock:
        return dict(_instances)
