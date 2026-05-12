# Pysilience

A lightweight fault tolerance library for Python, inspired by [resilience4j](https://github.com/resilience4j/resilience4j).

## Features

- **Minimal dependencies**: Core patterns use only the Python standard library; optional backends (e.g. Redis) are installed separately
- **Async-first**: Every pattern works with both sync and async functions out of the box
- **Type-safe**: Full typing support with `ParamSpec` and `TypeVar` generics
- **Composable**: Patterns share a core registry and event system for observation and tooling
- **Decorator & imperative APIs**: Use `@decorator` for convenience or instantiate classes directly for full control

## Installation

```bash
pip install pysilience

# With Redis cache backend
pip install pysilience[redis]
```

## Patterns

| Pattern | Description |
|---------|-------------|
| [Timeout](docs/timeout.md) | Limits execution time of sync/async operations |
| [Retry](docs/retry.md) | Retries failed operations with configurable backoff |
| [Circuit Breaker](docs/circuitbreaker.md) | Prevents cascading failures with three-state state machine |
| [Rate Limiter](docs/ratelimiter.md) | Controls the rate of operations (token bucket, leaky bucket, fixed/sliding window) |
| [Bulkhead](docs/bulkhead.md) | Limits concurrent executions to isolate failures |
| [Fallback](docs/fallback.md) | Provides alternative results when operations fail |
| [Cache](docs/cache.md) | Caches function results with LRU eviction, TTL, and pluggable backends |

## Quick Start

```python
from pysilience import timeout, retry, rate_limiter, bulkhead, fallback, cache

@timeout(duration=5.0)
@retry(max_attempts=3, initial_interval=0.5)
@rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
def call_external_api(endpoint: str) -> dict:
    ...

@fallback(action=lambda exc: {"status": "degraded"})
@timeout(duration=10.0)
async def async_api_call(user_id: int) -> dict:
    ...

@cache(max_size=256, ttl=60.0)
def get_user(user_id: int) -> dict:
    ...
```

## Architecture

All patterns follow a consistent structure:

```
┌─────────────────────────────────────────────────────┐
│  @decorator(...)          Decorator factory         │
│  PatternClass(config)     Imperative class          │
│  PatternConfig(...)       Frozen dataclass          │
│  create_pattern(name=...) Factory + registry        │
│  PatternEvent / Type      Observability events      │
│  PatternException         Pattern-specific errors   │
└─────────────────────────────────────────────────────┘
         │                           │
         ▼                           ▼
┌─────────────────┐       ┌────────────────────┐
│  core.registry  │       │  core.listeners    │
│  register/get   │       │  notify_listeners  │
└─────────────────┘       └────────────────────┘
```

### Registry

Named instances can be registered for retrieval by other parts of your application:

```python
from pysilience import create_retry, get_registered

r = create_retry(name="payment-api", register=True)

# Elsewhere in the app
r = get_registered("retry", "payment-api")
```

### Event Listeners

Every pattern emits typed events for observability:

```python
from pysilience import Retry, RetryConfig, RetryEvent

r = Retry(RetryConfig(max_attempts=3), name="http")
r.on_event(lambda e: print(f"{e.event_type.name}: attempt {e.attempt}"))
```

## Documentation

- **Pattern guides**: [`docs/`](docs/) — one file per pattern with full API reference
- **Examples**: [`docs/examples/`](docs/examples/) — real-world usage with FastAPI, Celery, and plain Python

## Requirements

- Python 3.10+
- No runtime dependencies (core patterns)
- `redis>=5.0` for the Redis cache backend (`pip install pysilience[redis]`)

## License

MIT
