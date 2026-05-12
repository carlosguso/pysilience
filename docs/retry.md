# Retry

Automatically retries failed operations with configurable backoff strategies.

## API

### Decorator

```python
from pysilience import retry

@retry(max_attempts=3, initial_interval=0.5, multiplier=2.0)
def flaky_call():
    ...

@retry(max_attempts=5, initial_interval=0.1, jitter=True)
async def flaky_async():
    ...
```

### Class (imperative)

```python
from pysilience import Retry, RetryConfig

r = Retry(RetryConfig(max_attempts=3, initial_interval=0.5), name="http")
result = r.execute(lambda: fetch_data())
result = await r.execute_async(lambda: async_fetch_data())
```

### Factory

```python
from pysilience import create_retry

r = create_retry(
    RetryConfig(max_attempts=5, initial_interval=1.0),
    name="payment-api",
    register=True,
)
```

## Configuration

`RetryConfig` is a frozen dataclass:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_attempts` | `int` | `3` | Total number of attempts (including the first). Must be >= 1. |
| `initial_interval` | `float` | `0.0` | Base wait time in seconds before the first retry. |
| `multiplier` | `float` | `2.0` | Factor applied between attempts for exponential backoff. Use `1.0` for fixed delay. |
| `max_interval` | `float \| None` | `None` | Upper bound on wait time between attempts. `None` = no cap. |
| `jitter` | `bool` | `False` | Add random jitter to wait times to prevent thundering herd. |
| `jitter_ratio` | `float` | `0.1` | Half-width of the jitter band (0 to 1). Ignored if `jitter=False`. |
| `retry_on` | `tuple[type[BaseException], ...]` | `(Exception,)` | Exception types that trigger a retry. |
| `abort_on` | `tuple[type[BaseException], ...]` | `()` | Exception types that are never retried (checked first). |

### Backoff formula

```
wait(k) = min(initial_interval * multiplier^k, max_interval) * jitter_factor
```

Where `k` is the retry number (0-indexed) and `jitter_factor` is uniform in `[1 - jitter_ratio, 1 + jitter_ratio]`.

## Exception

```python
from pysilience import RetriesExhausted

try:
    result = flaky_call()
except RetriesExhausted as e:
    print(e.name)            # Instance name
    print(e.attempts)        # Number of attempts made
    print(e.last_exception)  # The final underlying error
```

## Events

```python
from pysilience import Retry, RetryConfig, RetryEvent, RetryEventType

r = Retry(RetryConfig(max_attempts=3), name="api")
r.on_event(lambda e: log(f"{e.event_type.name} attempt={e.attempt}"))
```

`RetryEventType` values:

| Type | Description |
|------|-------------|
| `SUCCESS` | Operation succeeded (possibly after retries) |
| `ATTEMPT_FAILURE` | Failure that will be retried after a wait |
| `EXHAUSTED` | All attempts failed |
| `NON_RETRYABLE` | Failure that is not retried (`abort_on` or not in `retry_on`) |

`RetryEvent` fields: `event_type`, `name`, `attempt`, `max_attempts`, `wait_before_next`, `exception`.

## Behavior

- **Sync**: Sleeps with `time.sleep()` between attempts.
- **Async**: Sleeps with `asyncio.sleep()` between attempts.
- Exceptions matching `abort_on` are raised immediately without consuming attempts.
- Exceptions not matching `retry_on` are also raised immediately.
- `RetriesExhausted` wraps the last exception after all attempts fail.
