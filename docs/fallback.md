# Fallback

Provides an alternative result when the primary operation fails. The fallback action receives the exception and can return a degraded response, default value, or data from an alternative source.

## API

### Decorator

```python
from pysilience import fallback

@fallback(action=lambda exc: "default")
def risky_call():
    ...

@fallback(action=lambda exc: {"status": "degraded"})
async def risky_async():
    ...
```

### Class (imperative)

```python
from pysilience import Fallback, FallbackConfig

fb = Fallback(
    config=FallbackConfig(fallback_on=(IOError, TimeoutError)),
    action=lambda exc: cached_response(),
    name="user-service",
)
result = fb.execute(lambda: fetch_user(42))
result = await fb.execute_async(lambda: async_fetch_user(42))
```

### Factory

```python
from pysilience import create_fallback

fb = create_fallback(
    config=FallbackConfig(fallback_on=(Exception,), raise_on=(KeyboardInterrupt,)),
    action=lambda exc: None,
    name="notifications",
    register=True,
)
```

## Configuration

`FallbackConfig` is a frozen dataclass:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `fallback_on` | `tuple[type[BaseException], ...]` | `(Exception,)` | Exception types that trigger the fallback action. |
| `raise_on` | `tuple[type[BaseException], ...]` | `()` | Exception types that bypass fallback and propagate immediately. Checked before `fallback_on`. |

### Action callable

The `action` parameter is a callable `(BaseException) -> R` that receives the caught exception and returns the fallback value. It can also be an async callable for async usage.

## Events

```python
from pysilience import Fallback, FallbackConfig, FallbackEvent, FallbackEventType

fb = Fallback(
    config=FallbackConfig(),
    action=lambda exc: "fallback",
    name="api",
)
fb.on_event(lambda e: log(e.event_type.name))
```

`FallbackEventType` values:

| Type | Description |
|------|-------------|
| `SUCCESS` | Primary operation succeeded without needing fallback |
| `FALLBACK` | Primary failed; fallback action provided the result |
| `FALLBACK_ERROR` | Both primary and fallback action failed |

`FallbackEvent` fields: `event_type`, `name`, `exception`.

## Behavior

- **Sync & Async**: Works identically for both; the fallback action is called synchronously for sync paths and can be either sync or async for async paths.
- `raise_on` is checked before `fallback_on`. If an exception matches `raise_on`, it propagates immediately regardless of `fallback_on`.
- If the fallback action itself raises, a `FALLBACK_ERROR` event is emitted and the fallback action's exception propagates.
- The original exception is passed to the action callable so it can make decisions based on the failure type.
