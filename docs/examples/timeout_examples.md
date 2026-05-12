# Timeout — Examples

## Python-only

### Basic HTTP call with timeout

```python
import urllib.request
from pysilience import timeout, OperationTimeout

@timeout(duration=5.0)
def fetch_url(url: str) -> str:
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode()

try:
    html = fetch_url("https://api.example.com/data")
except OperationTimeout:
    print("Request took too long")
```

### Async timeout with explicit class

```python
import asyncio
import httpx
from pysilience import Timeout, TimeoutConfig, OperationTimeout

http_timeout = Timeout(TimeoutConfig(duration=3.0), name="http-client")

async def get_user(user_id: int) -> dict:
    async with httpx.AsyncClient() as client:
        return await http_timeout.execute_async(
            lambda: client.get(f"https://api.example.com/users/{user_id}")
        )
```

### Composing timeout with retry

```python
from pysilience import timeout, retry

@retry(max_attempts=3, initial_interval=1.0)
@timeout(duration=2.0)
def unreliable_service() -> dict:
    """Each attempt gets 2 seconds; up to 3 attempts total."""
    return call_external_service()
```

---

## FastAPI

### Endpoint with timeout

```python
from fastapi import FastAPI, HTTPException
from pysilience import timeout, OperationTimeout

app = FastAPI()

@timeout(duration=5.0)
async def fetch_from_upstream(item_id: int) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://upstream.example.com/items/{item_id}")
        resp.raise_for_status()
        return resp.json()

@app.get("/items/{item_id}")
async def get_item(item_id: int):
    try:
        return await fetch_from_upstream(item_id)
    except OperationTimeout:
        raise HTTPException(status_code=504, detail="Upstream timed out")
```

### Shared timeout instance with observability

```python
from fastapi import FastAPI
from pysilience import create_timeout, TimeoutConfig, TimeoutEvent
import structlog

app = FastAPI()
logger = structlog.get_logger()

db_timeout = create_timeout(
    TimeoutConfig(duration=2.0), name="database", register=True
)
db_timeout.on_event(lambda e: logger.info("timeout_event", **vars(e)))

@app.get("/users/{user_id}")
async def get_user(user_id: int):
    return await db_timeout.execute_async(lambda: db.fetch_user(user_id))
```

---

## Celery

### Timeout on individual task steps

```python
from celery import Celery
from pysilience import timeout, OperationTimeout

app = Celery("tasks", broker="redis://localhost:6379/0")

@app.task(bind=True, max_retries=3)
def process_payment(self, order_id: str):
    @timeout(duration=10.0)
    def charge_card(order_id: str) -> dict:
        return payment_gateway.charge(order_id)

    try:
        result = charge_card(order_id)
    except OperationTimeout:
        self.retry(countdown=5)
    return result
```
