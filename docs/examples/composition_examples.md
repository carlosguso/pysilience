# Composing Patterns — Examples

Pysilience patterns are designed to be composed. Stack decorators or nest imperative calls to build layered resilience strategies.

## Decorator composition order

Decorators execute outside-in. The **outermost** decorator wraps the entire call chain:

```python
from pysilience import timeout, retry, rate_limiter, bulkhead, fallback, cache

@fallback(action=lambda exc: {"status": "degraded"})  # 1. catch final failures
@retry(max_attempts=3, initial_interval=0.5)           # 2. retry transient errors
@timeout(duration=5.0)                                 # 3. time-limit each attempt
@rate_limiter(limit_for_period=10, limit_refresh_period=1.0)  # 4. throttle outgoing calls
@bulkhead(max_concurrent=5)                            # 5. limit concurrency
def call_external_api(endpoint: str) -> dict:
    return httpx.get(endpoint).json()
```

**Read bottom-up** to understand execution flow: bulkhead limits concurrency, rate limiter throttles, timeout limits each attempt, retry handles transient failures, fallback catches everything else.

---

## Common composition patterns

### Retry + Timeout (per-attempt timeout)

```python
from pysilience import retry, timeout

@retry(max_attempts=3, initial_interval=1.0)
@timeout(duration=2.0)
def fetch_with_per_attempt_timeout(url: str) -> dict:
    """Each attempt gets 2s; if it times out, retry kicks in."""
    return httpx.get(url).json()
```

### Timeout + Retry (total timeout)

```python
from pysilience import timeout, retry

@timeout(duration=10.0)
@retry(max_attempts=5, initial_interval=0.5)
def fetch_with_total_timeout(url: str) -> dict:
    """All retries must complete within 10s total."""
    return httpx.get(url).json()
```

### Cache + Fallback (stale cache on failure)

```python
from pysilience import Cache, CacheConfig, Fallback, FallbackConfig

fresh_cache = Cache(CacheConfig(max_size=100, ttl=30.0), name="fresh")
stale_cache = Cache(CacheConfig(max_size=100, ttl=3600.0), name="stale")

def get_data(key: str) -> dict:
    def fetch():
        result = api.get(key)
        stale_cache.execute(key, lambda: result)
        return result

    try:
        return fresh_cache.execute(key, fetch)
    except Exception:
        return stale_cache.execute(key, lambda: api.get(key))
```

### Rate Limiter + Bulkhead (protect upstream)

```python
from pysilience import rate_limiter, bulkhead

@rate_limiter(limit_for_period=100, limit_refresh_period=60.0)
@bulkhead(max_concurrent=10)
def call_payment_gateway(order: dict) -> dict:
    """Rate limit overall throughput AND limit concurrent connections."""
    return stripe.charge(order)
```

---

## FastAPI full-stack example

```python
from fastapi import FastAPI, HTTPException
from pysilience import (
    create_timeout, create_retry, create_rate_limiter, create_bulkhead,
    TimeoutConfig, RetryConfig, RateLimiterConfig, BulkheadConfig,
    OperationTimeout, RetriesExhausted, RateLimitExceeded, BulkheadRejected,
    get_registered,
)
import structlog

app = FastAPI()
logger = structlog.get_logger()

@app.on_event("startup")
def setup_resilience():
    t = create_timeout(TimeoutConfig(duration=3.0), name="upstream", register=True)
    r = create_retry(RetryConfig(max_attempts=3, initial_interval=0.5), name="upstream", register=True)
    rl = create_rate_limiter(
        RateLimiterConfig(limit_for_period=50, limit_refresh_period=1.0),
        name="upstream", register=True,
    )
    bh = create_bulkhead(BulkheadConfig(max_concurrent=10), name="upstream", register=True)

    for instance in [t, r, rl, bh]:
        instance.on_event(lambda e: logger.info("resilience_event", **vars(e)))

@app.get("/data/{key}")
async def get_data(key: str):
    bh = get_registered("bulkhead", "upstream")
    rl = get_registered("rate_limiter", "upstream")
    r = get_registered("retry", "upstream")
    t = get_registered("timeout", "upstream")

    async def fetch():
        return await t.execute_async(lambda: upstream_client.get(key))

    try:
        return await bh.execute_async(
            lambda: rl.execute_async(
                lambda: r.execute_async(fetch)
            )
        )
    except BulkheadRejected:
        raise HTTPException(503, "Service at capacity")
    except RateLimitExceeded:
        raise HTTPException(429, "Too many requests")
    except (RetriesExhausted, OperationTimeout):
        raise HTTPException(502, "Upstream unavailable")
```

---

## Celery full-stack example

```python
from celery import Celery
from pysilience import (
    Timeout, TimeoutConfig,
    Retry, RetryConfig,
    RateLimiter, RateLimiterConfig,
    Bulkhead, BulkheadConfig,
    Fallback, FallbackConfig,
    OperationTimeout, RetriesExhausted,
)

app = Celery("tasks", broker="redis://localhost:6379/0")

t = Timeout(TimeoutConfig(duration=10.0), name="vendor")
r = Retry(RetryConfig(max_attempts=3, initial_interval=2.0, jitter=True), name="vendor")
rl = RateLimiter(RateLimiterConfig(limit_for_period=20, limit_refresh_period=1.0), name="vendor")
bh = Bulkhead(BulkheadConfig(max_concurrent=5, max_wait=10.0), name="vendor")
fb = Fallback(
    config=FallbackConfig(fallback_on=(RetriesExhausted, OperationTimeout)),
    action=lambda exc: {"status": "queued_for_retry"},
    name="vendor",
)

@app.task(bind=True, max_retries=2)
def call_vendor(self, payload: dict) -> dict:
    def attempt():
        return t.execute(lambda: vendor_api.submit(payload))

    def with_retry():
        return r.execute(attempt)

    def with_rate_limit():
        return rl.execute(with_retry)

    def with_bulkhead():
        return bh.execute(with_rate_limit)

    result = fb.execute(with_bulkhead)
    if result.get("status") == "queued_for_retry":
        self.retry(countdown=300)
    return result
```
