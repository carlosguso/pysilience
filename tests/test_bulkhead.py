"""
Tests for the Bulkhead pattern.

Run with: pytest tests/test_bulkhead.py -v
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from pysilience.bulkhead import (
    Bulkhead,
    BulkheadConfig,
    BulkheadEvent,
    BulkheadEventType,
    BulkheadRejected,
    bulkhead,
)


class TestBulkheadConfig:
    def test_default_values(self) -> None:
        config = BulkheadConfig()
        assert config.max_concurrent == 10
        assert config.max_wait == 0.0

    def test_invalid_max_concurrent(self) -> None:
        with pytest.raises(ValueError, match="max_concurrent must be >= 1"):
            BulkheadConfig(max_concurrent=0)

    def test_invalid_max_wait(self) -> None:
        with pytest.raises(ValueError, match="max_wait must be non-negative"):
            BulkheadConfig(max_wait=-0.1)

    def test_frozen(self) -> None:
        config = BulkheadConfig()
        with pytest.raises(AttributeError):
            config.max_concurrent = 5  # type: ignore[misc]


class TestSyncBulkhead:
    def test_execute_success(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=3), name="t")
        assert bh.execute(lambda: 7) == 7

    def test_decorator_success(self) -> None:
        @bulkhead(max_concurrent=2, name="d")
        def f(x: int) -> int:
            return x + 1

        assert f(1) == 2

    def test_rejects_when_full(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2, max_wait=0.0), name="full")
        started = threading.Semaphore(0)
        hold = threading.Event()

        def occupy() -> None:
            def inner() -> None:
                started.release()
                hold.wait()

            bh.execute(inner)

        t1 = threading.Thread(target=occupy)
        t2 = threading.Thread(target=occupy)
        t1.start()
        t2.start()
        assert started.acquire(timeout=2.0)
        assert started.acquire(timeout=2.0)

        errors: list[BaseException] = []

        def third() -> None:
            try:

                def inner() -> None:
                    pass

                bh.execute(inner)
            except BaseException as e:
                errors.append(e)

        t3 = threading.Thread(target=third)
        t3.start()
        t3.join(timeout=2.0)
        assert not t3.is_alive()
        hold.set()
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)
        assert len(errors) == 1
        assert isinstance(errors[0], BulkheadRejected)

    def test_waits_then_acquires(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=2.0), name="wait")
        hold = threading.Event()
        order: list[int] = []

        def first() -> None:
            order.append(1)
            hold.wait()

        def second() -> None:
            order.append(2)

        t1 = threading.Thread(target=lambda: bh.execute(first))
        t1.start()
        time.sleep(0.05)
        t2 = threading.Thread(target=lambda: bh.execute(second))
        t2.start()
        time.sleep(0.05)
        hold.set()
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)
        assert order == [1, 2]

    def test_inner_exception_still_releases(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=1.0))

        def boom() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            bh.execute(boom)
        assert bh.execute(lambda: "ok") == "ok"


class TestAsyncBulkhead:
    @pytest.mark.asyncio
    async def test_execute_async_success(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=4), name="a")

        async def factory() -> int:
            return 42

        assert await bh.execute_async(factory) == 42

    @pytest.mark.asyncio
    async def test_decorator_async(self) -> None:
        @bulkhead(max_concurrent=2)
        async def g() -> str:
            return "x"

        assert await g() == "x"

    @pytest.mark.asyncio
    async def test_async_rejects_when_full(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=2, max_wait=0.0))
        hold = asyncio.Event()

        async def occupy() -> None:
            await hold.wait()

        t1 = asyncio.create_task(bh.execute_async(lambda: occupy()))
        t2 = asyncio.create_task(bh.execute_async(lambda: occupy()))
        await asyncio.sleep(0.05)
        with pytest.raises(BulkheadRejected):
            await bh.execute_async(lambda: asyncio.sleep(0))
        hold.set()
        await asyncio.gather(t1, t2)

    @pytest.mark.asyncio
    async def test_async_waits(self) -> None:
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=1.0))
        gate = asyncio.Event()
        order: list[str] = []

        async def first() -> None:
            order.append("a")
            await gate.wait()

        async def second() -> None:
            order.append("b")

        t1 = asyncio.create_task(bh.execute_async(lambda: first()))
        await asyncio.sleep(0.05)
        t2 = asyncio.create_task(bh.execute_async(lambda: second()))
        await asyncio.sleep(0.05)
        gate.set()
        await asyncio.gather(t1, t2)
        assert order == ["a", "b"]


class TestBulkheadEvents:
    def test_sync_events(self) -> None:
        events: list[BulkheadEvent] = []
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=0.0), name="ev")
        bh.on_event(events.append)
        bh.execute(lambda: None)
        assert len(events) == 1
        assert events[0].event_type == BulkheadEventType.SUCCESS
        assert events[0].name == "ev"

    def test_sync_error_event(self) -> None:
        events: list[BulkheadEvent] = []
        bh = Bulkhead(BulkheadConfig(max_concurrent=1), name="e")
        bh.on_event(events.append)
        def bad() -> None:
            raise ValueError("x")

        with pytest.raises(ValueError):
            bh.execute(bad)
        assert any(e.event_type == BulkheadEventType.ERROR for e in events)

    @pytest.mark.asyncio
    async def test_async_rejected_event(self) -> None:
        events: list[BulkheadEvent] = []
        bh = Bulkhead(BulkheadConfig(max_concurrent=1, max_wait=0.0))
        bh.on_event(events.append)
        hold = asyncio.Event()

        async def block() -> None:
            await hold.wait()

        t = asyncio.create_task(bh.execute_async(block))
        await asyncio.sleep(0.05)
        with pytest.raises(BulkheadRejected):
            await bh.execute_async(lambda: None)
        hold.set()
        await t
        assert any(e.event_type == BulkheadEventType.REJECTED for e in events)


class TestBulkheadDecoratorBare:
    def test_bare_decorator(self) -> None:
        @bulkhead
        def f() -> int:
            return 3

        assert f() == 3
