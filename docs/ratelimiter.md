# Rate Limiter

Controls the rate at which operations are executed using configurable algorithms.

## API

### Decorator

```python
from pysilience import rate_limiter

@rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
def call_api():
    ...

@rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=2.0)
async def call_api_async():
    ...
```

### Class (imperative)

```python
from pysilience import RateLimiter, RateLimiterConfig, RateLimitAlgorithm

rl = RateLimiter(
    RateLimiterConfig(
        limit_for_period=10,
        limit_refresh_period=1.0,
        algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
    ),
    name="external-api",
)
result = rl.execute(lambda: fetch_data())
result = await rl.execute_async(lambda: async_fetch_data())
```

### Factory

```python
from pysilience import create_rate_limiter

rl = create_rate_limiter(
    RateLimiterConfig(limit_for_period=100, limit_refresh_period=60.0),
    name="stripe",
    register=True,
)
```

## Configuration

`RateLimiterConfig` is a frozen dataclass:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `limit_for_period` | `int` | `50` | Number of permits available per period. Must be >= 1. |
| `limit_refresh_period` | `float` | `0.5` | Duration of one period in seconds. Must be > 0. |
| `timeout_duration` | `float` | `5.0` | Max time (seconds) to wait for a permit. `0.0` = reject immediately. |
| `algorithm` | `RateLimitAlgorithm` | `TOKEN_BUCKET` | Rate limiting algorithm. |

## Algorithms

`RateLimitAlgorithm` enum:

| Algorithm | Description |
|-----------|-------------|
| `TOKEN_BUCKET` | Continuous token refill up to capacity. Allows bursting from idle state. |
| `LEAKY_BUCKET` | Enforces smooth, evenly-spaced requests. No bursting even after idle. |
| `FIXED_WINDOW` | Counter resets at fixed period boundaries. Simple but allows 2x burst at window edges. |
| `SLIDING_WINDOW` | Weighted blend of current and previous windows. Smooths boundary burst of fixed window. |

### Choosing an algorithm

- **TOKEN_BUCKET** (default): Best for APIs where short bursts are acceptable but sustained rate must be limited.
- **LEAKY_BUCKET**: Best for producing smooth, evenly-spaced outgoing requests (e.g. upstream rate limits with no burst allowance).
- **FIXED_WINDOW**: Simplest implementation. Suitable when approximate rate limiting is acceptable.
- **SLIDING_WINDOW**: Good balance between accuracy and simplicity. Reduces boundary burst without per-request state.

## Exception

```python
from pysilience import RateLimitExceeded

try:
    result = call_api()
except RateLimitExceeded as e:
    print(e.name)              # Instance name
    print(e.available_permits) # Always 0
    print(e.wait_time)         # Seconds until next permit (or None)
```

## Events

```python
from pysilience import RateLimiter, RateLimiterConfig, RateLimiterEvent, RateLimiterEventType

rl = RateLimiter(RateLimiterConfig(limit_for_period=10), name="api")
rl.on_event(lambda e: log(e.event_type.name))
```

`RateLimiterEventType` values:

| Type | Description |
|------|-------------|
| `SUCCESS` | Permit acquired; operation executed |
| `REJECTED` | No permit available within timeout |
| `ERROR` | Operation raised an exception after acquiring permit |

`RateLimiterEvent` fields: `event_type`, `name`, `available_permits`, `wait_time`, `exception`.

## Behavior

- **Sync**: Blocks the calling thread with `time.sleep()` while waiting for permits.
- **Async**: Uses `asyncio.sleep()` while waiting for permits.
- Thread-safe: internal state is protected with `threading.Lock`.
- When `timeout_duration=0.0`, calls are rejected immediately if no permit is available.
