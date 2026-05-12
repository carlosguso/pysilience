# Timeout

Limits how long a sync or async operation may run. When the duration is exceeded, an `OperationTimeout` exception is raised.

## API

### Decorator

```python
from pysilience import timeout

@timeout(duration=5.0)
def slow_function():
    ...

@timeout(duration=10.0)
async def slow_async_function():
    ...
```

### Class (imperative)

```python
from pysilience import Timeout, TimeoutConfig

t = Timeout(TimeoutConfig(duration=5.0), name="api-call")
result = t.execute(lambda: expensive_computation())
result = await t.execute_async(lambda: async_fetch())
```

### Factory

```python
from pysilience import create_timeout

t = create_timeout(TimeoutConfig(duration=5.0), name="db-query", register=True)
```

## Configuration

`TimeoutConfig` is a frozen dataclass with the following fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `duration` | `float` | `30.0` | Maximum time allowed in seconds. Must be > 0. |
| `cancel_running_future` | `bool` | `True` | For async operations, whether to cancel the underlying task on timeout. |
| `use_signals` | `bool` | `False` | For sync operations on Unix, use `SIGALRM` (main thread only) instead of threading-based timeout. |

## Exception

```python
from pysilience import OperationTimeout

try:
    result = slow_function()
except OperationTimeout as e:
    print(e.name)      # Name of the timeout instance
    print(e.duration)  # Configured limit in seconds
    print(e.elapsed)   # Actual elapsed time
```

## Events

```python
from pysilience import Timeout, TimeoutConfig, TimeoutEvent, TimeoutEventType

t = Timeout(TimeoutConfig(duration=5.0), name="http")
t.on_event(lambda e: log(e))
```

`TimeoutEventType` values:

| Type | Description |
|------|-------------|
| `SUCCESS` | Operation completed within time limit |
| `TIMEOUT` | Operation exceeded time limit |
| `ERROR` | Operation raised an exception (not a timeout) |

`TimeoutEvent` fields: `event_type`, `name`, `duration`, `elapsed`, `exception`.

## Behavior

- **Sync**: By default uses a background thread with `concurrent.futures`. Set `use_signals=True` on Unix (main thread only) for signal-based timeout.
- **Async**: Uses `asyncio.wait_for`. When `cancel_running_future=True` (default), the wrapped coroutine is cancelled on timeout.
- **Thread-safe**: The `Timeout` instance can be shared across threads.
