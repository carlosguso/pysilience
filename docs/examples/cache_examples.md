# Cache — Examples

## Python-only

### Basic caching with TTL

```python
from pysilience import cache

@cache(max_size=256, ttl=60.0)
def get_user(user_id: int) -> dict:
    """Cached for 60 seconds, up to 256 entries."""
    return db.query(f"SELECT * FROM users WHERE id = {user_id}")
```

### Async caching

```python
from pysilience import cache

@cache(max_size=100, ttl=30.0)
async def fetch_product(product_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://api.example.com/products/{product_id}")
        return resp.json()
```

### Explicit key control

```python
from pysilience import Cache, CacheConfig

user_cache = Cache(CacheConfig(max_size=500, ttl=120.0), name="users")

def get_user(user_id: int) -> dict:
    return user_cache.execute(f"user:{user_id}", lambda: db.get_user(user_id))

def invalidate_user(user_id: int):
    user_cache.invalidate(f"user:{user_id}")
```

### Redis backend for distributed caching

The default `JsonSerializer` is a good fit for dict/list API and database responses — no extra setup required.

```python
import redis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend

redis_client = redis.Redis(host="localhost", port=6379, db=0)
backend = RedisBackend(sync_client=redis_client, prefix="myapp:users:")

user_cache = Cache(CacheConfig(ttl=300), backend=backend, name="users")

def get_user(user_id: int) -> dict:
    return user_cache.execute(f"user:{user_id}", lambda: db.get_user(user_id))
```

### Redis with datetime values (JsonSerializer)

`JsonSerializer` handles `datetime`, `date`, `time`, and `bytes` out of the box:

```python
from datetime import datetime

import redis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend

backend = RedisBackend(sync_client=redis.Redis(), prefix="myapp:events:")
event_cache = Cache(CacheConfig(ttl=600), backend=backend, name="events")

def get_event(event_id: str) -> dict:
    return event_cache.execute(
        f"event:{event_id}",
        lambda: {"id": event_id, "created_at": datetime.utcnow(), "payload": {...}},
    )
```

### Redis with arbitrary Python objects (HmacPickleSerializer)

When you need to cache custom classes or other non-JSON types, use `HmacPickleSerializer`. It signs pickle payloads with HMAC-SHA256 so a compromised Redis server cannot inject malicious pickle data.

```python
import os
import redis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend
from pysilience.cache_serializer_hmac import HmacPickleSerializer

# Pass secret explicitly, or set PYSILIENCE_CACHE_SECRET in the environment
serializer = HmacPickleSerializer(secret=os.environ["PYSILIENCE_CACHE_SECRET"])
backend = RedisBackend(sync_client=redis.Redis(), prefix="myapp:models:", serializer=serializer)

model_cache = Cache(CacheConfig(ttl=300), backend=backend, name="models")

def get_profile(user_id: int) -> UserProfile:
    return model_cache.execute(
        f"profile:{user_id}",
        lambda: UserProfile.load_from_db(user_id),
    )
```

### Redis with MessagePack (compact binary)

Requires `pip install pysilience[msgpack]`. Register custom types as msgpack extension types; optional helpers for datetime types are in `cache_serializer_msgpack_builtins`.

```python
from datetime import datetime

import redis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend
from pysilience.cache_serializer_msgpack import MsgpackSerializer
from pysilience.cache_serializer_msgpack_builtins import pack_datetime, unpack_datetime

serializer = MsgpackSerializer()
serializer.register_type(datetime, type_id=64, pack=pack_datetime, unpack=unpack_datetime)

backend = RedisBackend(sync_client=redis.Redis(), prefix="myapp:metrics:", serializer=serializer)
metrics_cache = Cache(CacheConfig(ttl=60), backend=backend, name="metrics")
```

### Async Redis backend

```python
import redis.asyncio as aioredis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend

async_redis = aioredis.Redis(host="localhost", port=6379, db=0)
backend = RedisBackend(async_client=async_redis, prefix="myapp:products:")

product_cache = Cache(CacheConfig(ttl=60), backend=backend, name="products")

async def get_product(product_id: str) -> dict:
    return await product_cache.execute_async(
        f"product:{product_id}",
        lambda: fetch_product_from_api(product_id),
    )
```

### Cache with event monitoring

```python
from pysilience import Cache, CacheConfig, CacheEvent, CacheEventType

c = Cache(CacheConfig(max_size=100, ttl=60.0), name="api-cache")

hit_count = 0
miss_count = 0

def track_cache(event: CacheEvent):
    global hit_count, miss_count
    if event.event_type == CacheEventType.HIT:
        hit_count += 1
    elif event.event_type == CacheEventType.MISS:
        miss_count += 1

c.on_event(track_cache)
```

---

## FastAPI

### Caching expensive computations

```python
from fastapi import FastAPI
from pysilience import cache

app = FastAPI()

@cache(max_size=64, ttl=300.0)
async def compute_analytics(org_id: str) -> dict:
    return await analytics_engine.aggregate(org_id)

@app.get("/orgs/{org_id}/analytics")
async def get_analytics(org_id: str):
    return await compute_analytics(org_id)
```

### Redis-backed cache with invalidation endpoint

```python
from fastapi import FastAPI
import redis.asyncio as aioredis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend

app = FastAPI()
redis_pool = aioredis.Redis(host="localhost", port=6379, db=0)
backend = RedisBackend(async_client=redis_pool, prefix="app:users:")
user_cache = Cache(CacheConfig(ttl=600), backend=backend, name="users")

@app.get("/users/{user_id}")
async def get_user(user_id: int):
    return await user_cache.execute_async(
        f"user:{user_id}",
        lambda: db.fetch_user(user_id),
    )

@app.put("/users/{user_id}")
async def update_user(user_id: int, data: dict):
    await db.update_user(user_id, data)
    user_cache.invalidate(f"user:{user_id}")
    return {"status": "updated"}
```

### Shared cache instance via registry

```python
from fastapi import FastAPI
from pysilience import create_cache, CacheConfig, get_registered

app = FastAPI()

@app.on_event("startup")
def setup():
    create_cache(CacheConfig(max_size=1000, ttl=60.0), name="products", register=True)

@app.get("/products/{product_id}")
async def get_product(product_id: str):
    c = get_registered("cache", "products")
    return await c.execute_async(
        f"product:{product_id}",
        lambda: catalog_service.get(product_id),
    )
```

---

## Celery

### Caching task results to avoid recomputation

```python
from celery import Celery
from pysilience import Cache, CacheConfig

app = Celery("tasks", broker="redis://localhost:6379/0")

report_cache = Cache(CacheConfig(max_size=50, ttl=3600.0), name="reports")

@app.task
def generate_report(report_id: str) -> dict:
    return report_cache.execute(
        f"report:{report_id}",
        lambda: expensive_report_generation(report_id),
    )
```

### Redis cache shared across Celery workers

```python
import redis
from celery import Celery
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend

app = Celery("tasks", broker="redis://localhost:6379/0")

redis_client = redis.Redis(host="localhost", port=6379, db=1)
backend = RedisBackend(sync_client=redis_client, prefix="celery:cache:")
shared_cache = Cache(CacheConfig(ttl=600), backend=backend, name="shared")

@app.task
def fetch_external_data(entity_id: str) -> dict:
    """All workers share the same Redis cache — avoids duplicate API calls."""
    return shared_cache.execute(
        f"entity:{entity_id}",
        lambda: external_api.fetch(entity_id),
    )
```

### Cache with fallback on miss

```python
from celery import Celery
from pysilience import Cache, CacheConfig, fallback

app = Celery("tasks", broker="redis://localhost:6379/0")

pricing_cache = Cache(CacheConfig(max_size=200, ttl=30.0), name="pricing")

@app.task
def get_price(product_id: str) -> float:
    @fallback(action=lambda exc: get_last_known_price(product_id))
    def fetch_price() -> float:
        return pricing_cache.execute(
            f"price:{product_id}",
            lambda: pricing_api.current_price(product_id),
        )

    return fetch_price()
```
