# Fallback — Examples

## Python-only

### Return a default on failure

```python
from pysilience import fallback

@fallback(action=lambda exc: {"status": "unknown", "source": "fallback"})
def get_service_status() -> dict:
    return monitoring_api.get_status()
```

### Fallback to a secondary data source

```python
from pysilience import Fallback, FallbackConfig

def primary_fetch(user_id: int) -> dict:
    return primary_db.get_user(user_id)

def fallback_fetch(exc: Exception) -> dict:
    return replica_db.get_user(user_id)

fb = Fallback(
    config=FallbackConfig(fallback_on=(IOError, TimeoutError)),
    action=fallback_fetch,
    name="user-lookup",
)
user = fb.execute(lambda: primary_fetch(42))
```

### Selective exception handling

```python
from pysilience import fallback

@fallback(
    action=lambda exc: None,
    config={"fallback_on": (IOError, ConnectionError), "raise_on": (ValueError,)},
)
def parse_and_fetch(url: str) -> dict:
    """ValueError (bad URL) propagates; network errors return None."""
    validated = validate_url(url)  # raises ValueError
    return fetch(validated)        # raises IOError
```

### Async fallback

```python
from pysilience import fallback

@fallback(action=lambda exc: {"items": [], "cached": True})
async def fetch_catalog() -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://catalog.example.com/items")
        resp.raise_for_status()
        return resp.json()
```

---

## FastAPI

### Graceful degradation endpoint

```python
from fastapi import FastAPI
from pysilience import fallback

app = FastAPI()

@fallback(action=lambda exc: {"recommendations": [], "reason": "service_unavailable"})
async def get_recommendations(user_id: int) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://rec-engine.internal/users/{user_id}")
        resp.raise_for_status()
        return resp.json()

@app.get("/users/{user_id}/feed")
async def user_feed(user_id: int):
    recs = await get_recommendations(user_id)
    return {"user_id": user_id, "recommendations": recs}
```

### Fallback with structured error response

```python
from fastapi import FastAPI
from pysilience import Fallback, FallbackConfig, FallbackEvent, FallbackEventType
import structlog

app = FastAPI()
logger = structlog.get_logger()

def pricing_fallback(exc: Exception) -> dict:
    logger.warning("pricing_fallback_triggered", error=str(exc))
    return {"price": None, "currency": "USD", "source": "fallback"}

pricing_fb = Fallback(
    config=FallbackConfig(fallback_on=(IOError, TimeoutError)),
    action=pricing_fallback,
    name="pricing",
)

@app.get("/products/{product_id}/price")
async def get_price(product_id: str):
    return await pricing_fb.execute_async(
        lambda: pricing_service.get_price(product_id)
    )
```

---

## Celery

### Fallback in task processing

```python
from celery import Celery
from pysilience import Fallback, FallbackConfig

app = Celery("tasks", broker="redis://localhost:6379/0")

notification_fb = Fallback(
    config=FallbackConfig(fallback_on=(Exception,)),
    action=lambda exc: {"sent": False, "queued_for_retry": True},
    name="notification",
)

@app.task
def send_notification(user_id: str, message: str) -> dict:
    return notification_fb.execute(
        lambda: push_service.send(user_id, message)
    )
```

### Composing fallback with other patterns

```python
from celery import Celery
from pysilience import timeout, retry, fallback

app = Celery("tasks", broker="redis://localhost:6379/0")

@app.task
def enrich_profile(user_id: str) -> dict:
    @fallback(action=lambda exc: {"enriched": False})
    @retry(max_attempts=2, initial_interval=1.0)
    @timeout(duration=5.0)
    def do_enrichment(uid: str) -> dict:
        return enrichment_api.enrich(uid)

    return do_enrichment(user_id)
```
