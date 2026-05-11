"""Tests for ``pysilience.core`` registry and shared helpers."""

from __future__ import annotations

import pytest

from pysilience import clear_registry, create_retry, get_registered, register, unregister
from pysilience.core import all_instances, clear, get, iter_by_kind, notify_listeners
from pysilience.retry import Retry, RetryConfig


def test_register_get_replace() -> None:
    clear()
    r1 = Retry(RetryConfig(max_attempts=2), name="x")
    r2 = Retry(RetryConfig(max_attempts=3), name="x")
    register("retry", "api", r1)
    assert get("retry", "api") is r1
    register("retry", "api", r2)
    assert get("retry", "api") is r2
    clear()


def test_unregister() -> None:
    clear()
    r = Retry(name="n")
    register("retry", "n", r)
    assert get("retry", "n") is r
    unregister("retry", "n")
    assert get("retry", "n") is None
    clear()


def test_iter_by_kind() -> None:
    clear()
    register("retry", "a", object())
    register("retry", "b", object())
    register("timeout", "c", object())
    names = {n for n, _ in iter_by_kind("retry")}
    assert names == {"a", "b"}
    clear()


def test_all_instances_snapshot() -> None:
    clear()
    register("retry", "z", Retry(name="z"))
    snap = all_instances()
    assert ("retry", "z") in snap
    clear()
    assert snap  # snapshot is independent


def test_register_empty_name_raises() -> None:
    clear()
    with pytest.raises(ValueError, match="non-empty"):
        register("retry", "", Retry(name="bad"))
    clear()


def test_notify_listeners_swallows_callback_errors() -> None:
    events: list[int] = []

    def bad(_: int) -> None:
        raise RuntimeError("listener boom")

    def good(e: int) -> None:
        events.append(e)

    notify_listeners([bad, good], 42)
    assert events == [42]


def test_package_aliases_match_core() -> None:
    clear_registry()
    inst = create_retry(RetryConfig(max_attempts=1), name="pkg", register=True)
    assert get_registered("retry", "pkg") is inst
    clear_registry()

