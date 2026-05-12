# Bulkhead

Limits concurrent executions so a failing or slow dependency cannot exhaust all available threads or tasks. Inspired by bulkhead compartments in ship design.

## API

### Decorator

```python
from pysilience import bulkhead

@bulkhead(max_concurrent=4)
def call_api():
    ...

@bulkhead(max_concurrent=8, max_wait=2.0)
async def call_api_async():
    ...
```

### Class (imperative)

```python
from pysilience import Bulkhead, BulkheadConfig

bh = Bulkhead(BulkheadConfig(max_concurrent=5, max_wait=1.0), name="db-pool")
result = bh.execute(lambda: query_database())
result = await bh.execute_async(lambda: async_query())
```

### Factory

```python
from pysilience import create_bulkhead

bh = create_bulkhead(
    BulkheadConfig(max_concurrent=10, max_wait=5.0),
    name="payment-service",
    register=True,
)
```

## Configuration

`BulkheadConfig` is a frozen dataclass:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_concurrent` | `int` | `10` | Maximum simultaneous executions. Must be >= 1. |
| `max_wait` | `float` | `0.0` | Seconds to wait for a slot before rejecting. `0.0` = reject immediately when full. |

## Exception

```python
from pysilience import BulkheadRejected

try:
    result = call_api()
except BulkheadRejected as e:
    print(e.name)            # Instance name
    print(e.max_concurrent)  # Configured limit
```

## Events

```python
from pysilience import Bulkhead, BulkheadConfig, BulkheadEvent, BulkheadEventType

bh = Bulkhead(BulkheadConfig(max_concurrent=5), name="api")
bh.on_event(lambda e: log(e.event_type.name))
```

`BulkheadEventType` values:

| Type | Description |
|------|-------------|
| `SUCCESS` | Operation completed successfully within concurrency limit |
| `REJECTED` | No slot available within `max_wait` |
| `ERROR` | Operation raised an exception (slot was released) |

`BulkheadEvent` fields: `event_type`, `name`, `max_concurrent`, `exception`.

## Behavior

- **Sync**: Uses a `threading.Semaphore` to limit concurrent threads.
- **Async**: Uses an asyncio-based counter with `asyncio.Event` for safe coordination (avoids `asyncio.Condition` pitfalls).
- Use one `Bulkhead` instance per dependency to isolate failure domains.
- When `max_wait=0.0`, calls are rejected immediately if the bulkhead is full.
- Thread-safe: safe to share a `Bulkhead` instance across threads for sync workloads.
