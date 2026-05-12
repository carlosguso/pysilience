# Circuit Breaker

Prevents cascading failures by stopping calls to a dependency that is likely down. Tracks recent outcomes in a sliding window and transitions between three states.

## State Machine

```
         failure rate >= threshold
  CLOSED ──────────────────────────► OPEN
    ▲                                  │
    │                                  │ wait_duration expires
    │ failure rate < threshold         ▼
    └────────────────────────── HALF_OPEN
         (after probes complete)
```

- **CLOSED**: Normal operation. Failures are recorded; the failure rate is evaluated after each call.
- **OPEN**: Every call is immediately rejected with `CircuitBreakerOpen`. After `wait_duration_in_open_state` seconds, transitions to HALF_OPEN.
- **HALF_OPEN**: Up to `permitted_number_of_calls_in_half_open_state` probe calls are allowed. Once all probes complete, the failure rate is re-evaluated.

## API

### Decorator

```python
from pysilience import circuit_breaker

@circuit_breaker(failure_rate_threshold=0.5)
def call_service():
    ...

@circuit_breaker(failure_rate_threshold=0.5, wait_duration_in_open_state=30.0)
async def call_service_async():
    ...
```

### Class (imperative)

```python
from pysilience import CircuitBreaker, CircuitBreakerConfig

cb = CircuitBreaker(
    CircuitBreakerConfig(failure_rate_threshold=0.5, sliding_window_size=10),
    name="payment-service",
)
result = cb.execute(lambda: payment_api.charge(order))
result = await cb.execute_async(lambda: async_payment_api.charge(order))
```

### Factory

```python
from pysilience import create_circuit_breaker, CircuitBreakerConfig

cb = create_circuit_breaker(
    CircuitBreakerConfig(
        failure_rate_threshold=0.5,
        wait_duration_in_open_state=30.0,
    ),
    name="user-service",
    register=True,
)
```

## Configuration

`CircuitBreakerConfig` is a frozen dataclass:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `failure_rate_threshold` | `float` | `0.5` | Failure ratio (0.0–1.0] at which the circuit opens. 0.5 = 50%. |
| `sliding_window_size` | `int` | `10` | Number of most-recent outcomes in the CLOSED-state window. |
| `minimum_number_of_calls` | `int` | `5` | Minimum outcomes recorded before the failure rate is evaluated. Must be <= `sliding_window_size`. |
| `wait_duration_in_open_state` | `float` | `60.0` | Seconds the circuit stays OPEN before transitioning to HALF_OPEN. |
| `permitted_number_of_calls_in_half_open_state` | `int` | `5` | Number of probe calls allowed in HALF_OPEN. |
| `record_exceptions` | `tuple[type[BaseException], ...]` | `(Exception,)` | Exception types counted as failures. |
| `ignore_exceptions` | `tuple[type[BaseException], ...]` | `()` | Exception types never counted as failures (checked before `record_exceptions`). |

## Exception

```python
from pysilience import CircuitBreakerOpen

try:
    result = call_service()
except CircuitBreakerOpen as e:
    print(e.name)            # Instance name
    print(e.remaining_wait)  # Seconds until HALF_OPEN (or None in HALF_OPEN)
```

## States

```python
from pysilience import CircuitBreakerState

cb.state           # CircuitBreakerState.CLOSED / OPEN / HALF_OPEN
cb.failure_rate    # Current failure rate (float)
cb.reset()         # Force back to CLOSED state
```

## Events

```python
from pysilience import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerEvent, CircuitBreakerEventType

cb = CircuitBreaker(CircuitBreakerConfig(), name="api")
cb.on_event(lambda e: log(f"{e.event_type.name} state={e.state.name}"))
```

`CircuitBreakerEventType` values:

| Type | Description |
|------|-------------|
| `SUCCESS` | Call completed successfully |
| `ERROR` | Call failed; counted as failure |
| `IGNORED_ERROR` | Call failed but matches `ignore_exceptions`; not counted |
| `REJECTED` | Call rejected because circuit is OPEN/HALF_OPEN at capacity |
| `STATE_TRANSITION` | Circuit changed state (check `from_state` and `to_state`) |

`CircuitBreakerEvent` fields: `event_type`, `name`, `state`, `exception`, `from_state`, `to_state`.

## Behavior

- **Sliding window**: Uses a `deque(maxlen=N)` in CLOSED state for O(1) failure rate tracking.
- **HALF_OPEN evaluation**: After all `permitted_number_of_calls_in_half_open_state` probes complete, the failure rate is checked. Below threshold → CLOSED; at or above → OPEN.
- **`ignore_exceptions`** takes precedence over `record_exceptions`. A matching ignored exception is neither counted as failure nor success.
- **Thread-safe**: Protected with `threading.Lock`.
- **Reset**: `cb.reset()` forces the circuit back to CLOSED and clears the sliding window.
