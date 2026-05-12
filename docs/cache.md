# Cache

Caches function results with LRU eviction, optional TTL, and pluggable storage backends. Ships with an in-memory backend and supports Redis for distributed caching.

## API

### Decorator

```python
from pysilience import cache

@cache(max_size=128, ttl=60.0)
def fetch_user(user_id: int) -> dict:
    ...

@cache(max_size=64, ttl=30.0)
async def fetch_user_async(user_id: int) -> dict:
    ...

# Bare @cache uses defaults (max_size=128, no TTL)
@cache
def compute(x: int) -> int:
    return x * 2
```

### Class (imperative)

```python
from pysilience import Cache, CacheConfig

c = Cache(CacheConfig(max_size=100, ttl=60.0), name="users")
result = c.execute("user:42", lambda: fetch_user(42))
result = await c.execute_async("user:42", lambda: async_fetch_user(42))
```

### Factory

```python
from pysilience import create_cache, CacheConfig

c = create_cache(CacheConfig(max_size=256, ttl=120.0), name="products", register=True)
```

## Configuration

`CacheConfig` is a frozen dataclass:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_size` | `int` | `128` | Maximum entries before LRU eviction. Must be >= 1. Only applies to `MemoryBackend`. |
| `ttl` | `float \| None` | `None` | Time-to-live in seconds. `None` = entries never expire by time. |

## Backends

### MemoryBackend (default)

In-process LRU cache using `collections.OrderedDict`. Created automatically when no backend is specified.

```python
from pysilience.cache import MemoryBackend

backend = MemoryBackend(max_size=256, ttl=60.0)
c = Cache(CacheConfig(), backend=backend, name="custom")
```

### RedisBackend

Distributed cache backed by Redis. Requires `pip install pysilience[redis]`.

```python
import redis
from pysilience import Cache, CacheConfig
from pysilience.cache_redis import RedisBackend

# Sync
backend = RedisBackend(sync_client=redis.Redis(), prefix="myapp:")
c = Cache(CacheConfig(ttl=300), backend=backend, name="users")
result = c.execute("user:42", lambda: fetch_user(42))

# Async
import redis.asyncio as aioredis
backend = RedisBackend(async_client=aioredis.Redis(), prefix="myapp:")
result = await c.execute_async("user:42", lambda: async_fetch_user(42))
```

`RedisBackend` notes:
- Values are serialised with `pickle` (protocol 5).
- `max_size` is **not** enforced — Redis manages its own memory/eviction.
- `ttl` is applied natively via Redis `SETEX`.
- `clear()` uses `SCAN` to avoid blocking Redis.

### Custom Backends

Implement the `CacheBackend` protocol:

```python
from pysilience.cache import CacheBackend

class MyBackend:
    def get(self, key): ...
    def put(self, key, value, ttl=None): ...
    def delete(self, key) -> bool: ...
    def clear(self): ...
    async def aget(self, key): ...
    async def aput(self, key, value, ttl=None): ...
    async def adelete(self, key) -> bool: ...
    async def aclear(self): ...
```

Return the `_MISS` sentinel (importable from `pysilience.cache`) from `get`/`aget` to distinguish cache misses from cached `None` values.

## Invalidation

```python
c.invalidate("user:42")   # Remove a single key
c.invalidate_all()         # Clear all entries
```

## Thundering-herd protection

Concurrent callers requesting the same key are coalesced — only one invocation of the underlying function runs while others wait for its result. This applies to both sync (threading) and async (asyncio) paths.

## Events

```python
from pysilience import Cache, CacheConfig, CacheEvent, CacheEventType

c = Cache(CacheConfig(max_size=100, ttl=60.0), name="api")
c.on_event(lambda e: log(f"{e.event_type.name} key={e.key}"))
```

`CacheEventType` values:

| Type | Description |
|------|-------------|
| `HIT` | Value found in cache |
| `MISS` | Value not in cache; function was called and result cached |
| `ERROR` | Function raised an exception (result not cached) |

`CacheEvent` fields: `event_type`, `name`, `key`, `exception`.

## Behavior

- **Decorator keys**: Derived from function arguments (must be hashable). A `TypeError` is raised for unhashable arguments.
- **LRU eviction**: Least-recently-used entries are evicted when `max_size` is exceeded (MemoryBackend only).
- **TTL**: Entries older than `ttl` seconds are treated as misses on access (lazy expiration).
- **Thread-safe**: Internal state is protected with `threading.Lock` / `threading.Condition`.
