# Retry — Examples

## Python-only

### Basic retry with exponential backoff

```python
import httpx
from pysilience import retry, RetriesExhausted

@retry(max_attempts=3, initial_interval=1.0, multiplier=2.0)
def fetch_data(endpoint: str) -> dict:
    resp = httpx.get(endpoint)
    resp.raise_for_status()
    return resp.json()

try:
    data = fetch_data("https://api.example.com/data")
except RetriesExhausted as e:
    print(f"All {e.attempts} attempts failed: {e.last_exception}")
```

### Selective retry with jitter

```python
from pysilience import retry

@retry(
    max_attempts=5,
    initial_interval=0.5,
    multiplier=2.0,
    max_interval=10.0,
    jitter=True,
    retry_on=(IOError, TimeoutError),
    abort_on=(ValueError, KeyError),
)
def call_external_service() -> dict:
    """Only retries on IO/Timeout errors. ValueError propagates immediately."""
    ...
```

### Async retry

```python
import httpx
from pysilience import retry

@retry(max_attempts=3, initial_interval=0.2)
async def async_fetch(url: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()
```

### Imperative usage with event listener

```python
from pysilience import Retry, RetryConfig, RetryEvent

r = Retry(
    RetryConfig(max_attempts=4, initial_interval=1.0, multiplier=2.0),
    name="payment",
)

def log_retry(event: RetryEvent):
    if event.wait_before_next:
        print(f"Attempt {event.attempt} failed, retrying in {event.wait_before_next:.1f}s")

r.on_event(log_retry)
result = r.execute(lambda: payment_gateway.charge(order))
```

---

## FastAPI

### Endpoint with retry on upstream failures

```python
from fastapi import FastAPI, HTTPException
import httpx
from pysilience import retry, RetriesExhausted

app = FastAPI()

@retry(max_attempts=3, initial_interval=0.5, retry_on=(httpx.HTTPStatusError,))
async def call_upstream(path: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://upstream.example.com{path}")
        resp.raise_for_status()
        return resp.json()

@app.get("/proxy/{path:path}")
async def proxy(path: str):
    try:
        return await call_upstream(f"/{path}")
    except RetriesExhausted:
        raise HTTPException(status_code=502, detail="Upstream unavailable")
```

### Registered retry instance shared across routes

```python
from fastapi import FastAPI
from pysilience import create_retry, RetryConfig, get_registered

app = FastAPI()

@app.on_event("startup")
def setup_resilience():
    create_retry(
        RetryConfig(max_attempts=3, initial_interval=1.0, jitter=True),
        name="email-service",
        register=True,
    )

@app.post("/notifications/email")
async def send_email(to: str, body: str):
    r = get_registered("retry", "email-service")
    return await r.execute_async(lambda: email_client.send(to, body))
```

---

## Celery

### Retry within a Celery task (fine-grained control)

```python
from celery import Celery
from pysilience import retry, RetriesExhausted

app = Celery("tasks", broker="redis://localhost:6379/0")

@app.task(bind=True)
def sync_inventory(self, product_id: str):
    @retry(max_attempts=5, initial_interval=2.0, multiplier=2.0, max_interval=30.0)
    def push_to_warehouse(product_id: str):
        warehouse_api.sync(product_id)

    try:
        push_to_warehouse(product_id)
    except RetriesExhausted:
        self.retry(countdown=60, max_retries=2)
```

### Combining pysilience retry with Celery's built-in retry

```python
from celery import Celery
from pysilience import Retry, RetryConfig

app = Celery("tasks", broker="redis://localhost:6379/0")

fast_retry = Retry(
    RetryConfig(max_attempts=3, initial_interval=0.5),
    name="fast-retry",
)

@app.task(bind=True, max_retries=3)
def process_webhook(self, payload: dict):
    """Fast retries for transient errors; Celery retry for longer outages."""
    try:
        fast_retry.execute(lambda: downstream.process(payload))
    except Exception:
        self.retry(countdown=300)
```
