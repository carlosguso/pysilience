# Circuit Breaker — Examples

## Python-only

### Basic circuit breaker

```python
from pysilience import circuit_breaker, CircuitBreakerOpen

@circuit_breaker(failure_rate_threshold=0.5, sliding_window_size=10)
def call_payment_api(order_id: str) -> dict:
    return payment_gateway.charge(order_id)

try:
    result = call_payment_api("order-123")
except CircuitBreakerOpen as e:
    print(f"Circuit is open, retry after {e.remaining_wait:.0f}s")
```

### Monitoring state transitions

```python
from pysilience import (
    CircuitBreaker, CircuitBreakerConfig,
    CircuitBreakerEvent, CircuitBreakerEventType,
)

cb = CircuitBreaker(
    CircuitBreakerConfig(
        failure_rate_threshold=0.5,
        sliding_window_size=20,
        minimum_number_of_calls=10,
        wait_duration_in_open_state=30.0,
    ),
    name="user-service",
)

def on_circuit_event(event: CircuitBreakerEvent):
    if event.event_type == CircuitBreakerEventType.STATE_TRANSITION:
        print(f"Circuit '{event.name}': {event.from_state.name} → {event.to_state.name}")
    elif event.event_type == CircuitBreakerEventType.REJECTED:
        print(f"Call rejected by circuit '{event.name}'")

cb.on_event(on_circuit_event)
```

### Ignoring specific exceptions

```python
from pysilience import circuit_breaker

@circuit_breaker(
    failure_rate_threshold=0.5,
    record_exceptions=(IOError, TimeoutError),
    ignore_exceptions=(ValueError, KeyError),
)
def process_request(data: dict) -> dict:
    """ValueError/KeyError won't trip the circuit; only network errors count."""
    validated = validate(data)  # ValueError won't count as circuit failure
    return fetch(validated)     # IOError will count
```

### Manual reset

```python
from pysilience import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState

cb = CircuitBreaker(
    CircuitBreakerConfig(failure_rate_threshold=0.5),
    name="db",
)

# After deploying a fix, manually reset the circuit
if cb.state == CircuitBreakerState.OPEN:
    cb.reset()
    print("Circuit reset to CLOSED")
```

---

## FastAPI

### Protecting an upstream dependency

```python
from fastapi import FastAPI, HTTPException
from pysilience import (
    create_circuit_breaker, CircuitBreakerConfig,
    CircuitBreakerOpen, CircuitBreakerState,
    get_registered,
)

app = FastAPI()

@app.on_event("startup")
def setup():
    create_circuit_breaker(
        CircuitBreakerConfig(
            failure_rate_threshold=0.5,
            sliding_window_size=20,
            wait_duration_in_open_state=30.0,
        ),
        name="recommendation-engine",
        register=True,
    )

@app.get("/recommendations/{user_id}")
async def get_recommendations(user_id: str):
    cb = get_registered("circuit_breaker", "recommendation-engine")
    try:
        return await cb.execute_async(
            lambda: recommendation_client.get(user_id)
        )
    except CircuitBreakerOpen:
        raise HTTPException(
            status_code=503,
            detail="Recommendation service temporarily unavailable",
        )
```

### Health check endpoint exposing circuit state

```python
from fastapi import FastAPI
from pysilience import get_registered, CircuitBreakerState

app = FastAPI()

@app.get("/health/circuits")
async def circuit_health():
    cb = get_registered("circuit_breaker", "recommendation-engine")
    return {
        "recommendation-engine": {
            "state": cb.state.name,
            "failure_rate": cb.failure_rate,
        }
    }
```

### Combining circuit breaker with fallback

```python
from fastapi import FastAPI
from pysilience import circuit_breaker, fallback, CircuitBreakerOpen

app = FastAPI()

@fallback(
    action=lambda exc: {"recommendations": [], "source": "fallback"},
    config={"fallback_on": (CircuitBreakerOpen, IOError)},
)
@circuit_breaker(failure_rate_threshold=0.5, wait_duration_in_open_state=15.0)
async def get_recommendations(user_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://rec-engine.internal/users/{user_id}")
        resp.raise_for_status()
        return resp.json()

@app.get("/feed/{user_id}")
async def user_feed(user_id: str):
    return await get_recommendations(user_id)
```

---

## Celery

### Circuit breaker on external API calls within tasks

```python
from celery import Celery
from pysilience import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpen

app = Celery("tasks", broker="redis://localhost:6379/0")

shipping_cb = CircuitBreaker(
    CircuitBreakerConfig(
        failure_rate_threshold=0.5,
        sliding_window_size=20,
        wait_duration_in_open_state=120.0,
    ),
    name="shipping-provider",
)

@app.task(bind=True, max_retries=5)
def create_shipment(self, order_id: str, address: dict):
    try:
        return shipping_cb.execute(
            lambda: shipping_api.create(order_id, address)
        )
    except CircuitBreakerOpen as e:
        self.retry(countdown=e.remaining_wait or 60)
```

### Shared circuit state across task executions

```python
from celery import Celery
from pysilience import (
    create_circuit_breaker, CircuitBreakerConfig, CircuitBreakerOpen,
    get_registered,
)

app = Celery("tasks", broker="redis://localhost:6379/0")

@app.on_after_configure.connect
def setup_resilience(sender, **kwargs):
    create_circuit_breaker(
        CircuitBreakerConfig(
            failure_rate_threshold=0.3,
            sliding_window_size=50,
            minimum_number_of_calls=20,
            wait_duration_in_open_state=60.0,
        ),
        name="email-provider",
        register=True,
    )

@app.task(bind=True, max_retries=3)
def send_transactional_email(self, to: str, template: str, context: dict):
    cb = get_registered("circuit_breaker", "email-provider")
    try:
        return cb.execute(lambda: email_service.send(to, template, context))
    except CircuitBreakerOpen:
        self.retry(countdown=120)
```
