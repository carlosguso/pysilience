"""
Basic usage examples for pysilience.

Run with: python examples/basic_usage.py
"""

import asyncio
import time

from pysilience import timeout, Timeout, TimeoutConfig, OperationTimeout


def example_basic_timeout() -> None:
    """Basic timeout decorator usage."""
    print("\n=== Basic Timeout Example ===")

    @timeout(duration=2.0)
    def slow_operation() -> str:
        time.sleep(0.5)
        return "Operation completed!"

    result = slow_operation()
    print(f"Result: {result}")


def example_timeout_exceeded() -> None:
    """Example of timeout being exceeded."""
    print("\n=== Timeout Exceeded Example ===")

    @timeout(duration=0.5)
    def very_slow_operation() -> str:
        time.sleep(2.0)
        return "This won't be returned"

    try:
        very_slow_operation()
    except OperationTimeout as e:
        print(f"Caught timeout: {e}")
        print(f"  Duration limit: {e.duration}s")
        print(f"  Elapsed: {e.elapsed:.2f}s")


def example_configured_timeout() -> None:
    """Using TimeoutConfig for more control."""
    print("\n=== Configured Timeout Example ===")

    config = TimeoutConfig(duration=5.0)
    t = Timeout(config, name="api-timeout")

    @t
    def api_call() -> dict:
        time.sleep(0.1)
        return {"status": "ok", "data": [1, 2, 3]}

    result = api_call()
    print(f"API Response: {result}")


def example_with_events() -> None:
    """Using timeout events for observability."""
    print("\n=== Timeout Events Example ===")

    from pysilience.timeout import TimeoutEvent, TimeoutEventType

    t = Timeout(TimeoutConfig(duration=1.0), name="monitored-op")

    def event_handler(event: TimeoutEvent) -> None:
        print(f"  Event: {event.event_type.name}")
        print(f"  Elapsed: {event.elapsed:.4f}s")

    t.on_event(event_handler)

    @t
    def monitored_operation() -> str:
        time.sleep(0.1)
        return "done"

    print("Calling monitored operation...")
    monitored_operation()


async def example_async_timeout() -> None:
    """Async function timeout."""
    print("\n=== Async Timeout Example ===")

    @timeout(duration=2.0)
    async def async_operation() -> str:
        await asyncio.sleep(0.5)
        return "Async operation completed!"

    result = await async_operation()
    print(f"Result: {result}")


async def example_async_timeout_exceeded() -> None:
    """Async timeout exceeded."""
    print("\n=== Async Timeout Exceeded Example ===")

    @timeout(duration=0.5)
    async def slow_async() -> str:
        await asyncio.sleep(2.0)
        return "Won't return"

    try:
        await slow_async()
    except OperationTimeout as e:
        print(f"Caught async timeout: {e}")


def main() -> None:
    """Run all examples."""
    print("=" * 50)
    print("Pysilience Timeout Examples")
    print("=" * 50)

    # Sync examples
    example_basic_timeout()
    example_timeout_exceeded()
    example_configured_timeout()
    example_with_events()

    # Async examples
    print("\n--- Async Examples ---")
    asyncio.run(example_async_timeout())
    asyncio.run(example_async_timeout_exceeded())

    print("\n" + "=" * 50)
    print("All examples completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
