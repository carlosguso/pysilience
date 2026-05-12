# Rate Limiter — Examples

## Python-only

### Basic rate limiting

```python
from pysilience import rate_limiter, RateLimitExceeded

@rate_limiter(limit_for_period=10, limit_refresh_period=1.0)
def call_api(endpoint: str) -> dict:
    """At most 10 calls per second."""
    return httpx.get(endpoint).json()

try:
    results = [call_api("/data") for _ in range(20)]
except RateLimitExceeded as e:
    print(f"Rate limit hit. Wait {e.wait_time:.1f}s for next permit.")
```

### Different algorithms

```python
from pysilience import rate_limiter, RateLimitAlgorithm

# Smooth output - no bursting
@rate_limiter(
    limit_for_period=5,
    limit_refresh_period=1.0,
    algorithm=RateLimitAlgorithm.LEAKY_BUCKET,
)
def send_notification(user_id: str, msg: str):
    notification_service.send(user_id, msg)

# Sliding window for smoother rate enforcement
@rate_limiter(
    limit_for_period=100,
    limit_refresh_period=60.0,
    algorithm=RateLimitAlgorithm.SLIDING_WINDOW,
)
def query_database(sql: str) -> list:
    return db.execute(sql)
```

### Async rate limiter with waiting

```python
from pysilience import rate_limiter

@rate_limiter(limit_for_period=5, limit_refresh_period=1.0, timeout_duration=10.0)
async def call_rate_limited_api(payload: dict) -> dict:
    """Waits up to 10s for a permit rather than rejecting immediately."""
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://api.example.com", json=payload)
        return resp.json()
```

### Imperative usage with metrics

```python
from pysilience import RateLimiter, RateLimiterConfig, RateLimiterEvent, RateLimiterEventType

rl = RateLimiter(
    RateLimiterConfig(limit_for_period=50, limit_refresh_period=1.0),
    name="stripe-api",
)

def track_metrics(event: RateLimiterEvent):
    if event.event_type == RateLimiterEventType.REJECTED:
        metrics.increment("rate_limit.rejected", tags={"service": event.name})

rl.on_event(track_metrics)
```

---

## FastAPI

### Per-endpoint rate limiting

```python
from fastapi import FastAPI, HTTPException, Request
from pysilience import RateLimiter, RateLimiterConfig, RateLimitExceeded

app = FastAPI()

search_limiter = RateLimiter(
    RateLimiterConfig(limit_for_period=20, limit_refresh_period=60.0, timeout_duration=0.0),
    name="search",
)

@app.get("/search")
async def search(q: str):
    try:
        return await search_limiter.execute_async(
            lambda: search_engine.query(q)
        )
    except RateLimitExceeded:
        raise HTTPException(
            status_code=429,
            detail="Too many requests",
            headers={"Retry-After": "60"},
        )
```

### Protecting an upstream API

```python
from fastapi import FastAPI
from pysilience import create_rate_limiter, RateLimiterConfig, RateLimitAlgorithm

app = FastAPI()

@app.on_event("startup")
def setup():
    create_rate_limiter(
        RateLimiterConfig(
            limit_for_period=100,
            limit_refresh_period=60.0,
            timeout_duration=5.0,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
        ),
        name="openai",
        register=True,
    )

@app.post("/chat")
async def chat(message: str):
    from pysilience import get_registered
    rl = get_registered("rate_limiter", "openai")
    return await rl.execute_async(lambda: openai_client.chat(message))
```

---

## Celery

### Rate-limited task execution

```python
from celery import Celery
from pysilience import RateLimiter, RateLimiterConfig, RateLimitExceeded

app = Celery("tasks", broker="redis://localhost:6379/0")

email_limiter = RateLimiter(
    RateLimiterConfig(
        limit_for_period=50,
        limit_refresh_period=60.0,
        timeout_duration=30.0,
    ),
    name="email-provider",
)

@app.task(bind=True, max_retries=5)
def send_email(self, to: str, subject: str, body: str):
    try:
        email_limiter.execute(lambda: smtp.send(to, subject, body))
    except RateLimitExceeded:
        self.retry(countdown=60)
```

### Batch processing with rate control

```python
from celery import Celery
from pysilience import RateLimiter, RateLimiterConfig, RateLimitAlgorithm

app = Celery("tasks", broker="redis://localhost:6379/0")

api_limiter = RateLimiter(
    RateLimiterConfig(
        limit_for_period=10,
        limit_refresh_period=1.0,
        timeout_duration=60.0,
        algorithm=RateLimitAlgorithm.LEAKY_BUCKET,
    ),
    name="vendor-api",
)

@app.task
def process_batch(items: list[dict]):
    results = []
    for item in items:
        result = api_limiter.execute(lambda: vendor_api.process(item))
        results.append(result)
    return results
```
