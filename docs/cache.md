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

# Sync (default JsonSerializer — safe for untrusted Redis data)
backend = RedisBackend(sync_client=redis.Redis(), prefix="myapp:")
c = Cache(CacheConfig(ttl=300), backend=backend, name="users")
result = c.execute("user:42", lambda: fetch_user(42))

# Async
import redis.asyncio as aioredis
backend = RedisBackend(async_client=aioredis.Redis(), prefix="myapp:")
result = await c.execute_async("user:42", lambda: async_fetch_user(42))
```

`RedisBackend` notes:
- Values are serialised with a pluggable `CacheSerializer` (defaults to `JsonSerializer`).
- Cache keys are hashed with SHA-256 of `repr(key)` — stable across Python versions and fixed-length in Redis.
- Invalid or corrupted entries are logged as a warning and treated as a cache miss.
- `max_size` is **not** enforced — Redis manages its own memory/eviction.
- `ttl` is applied natively via Redis `SETEX`.
- `clear()` uses `SCAN` to avoid blocking Redis. Keys inserted between scan pages may not be removed; for production, consider prefix rotation instead.

#### Serializers

Pass a `serializer` to `RedisBackend` to control how values are stored. All serializers implement the `CacheSerializer` protocol (`dumps` / `loads`); `loads` raises `ValueError` on invalid data, which the backend treats as a miss.

| Serializer | Module | Use when |
|------------|--------|----------|
| `JsonSerializer` (default) | `pysilience.cache_serializer_json` | JSON-native data (`dict`, `list`, `str`, …). Safe to deserialize from an untrusted Redis server. Built-in support for `datetime`, `date`, `time`, and `bytes`. |
| `HmacPickleSerializer` | `pysilience.cache_serializer_hmac` | Arbitrary Python objects. Pickle payloads are signed with HMAC-SHA256 so tampered data is rejected before unpickling. **Requires a signing secret.** |
| `MsgpackSerializer` | `pysilience.cache_serializer_msgpack` | Compact binary encoding. Requires `pip install pysilience[msgpack]`. Custom types must be registered as msgpack extension types. |

```python
from pysilience.cache_redis import RedisBackend, JsonSerializer, HmacPickleSerializer
from pysilience.cache_serializer_msgpack import MsgpackSerializer

# Default — JSON (explicit, same as omitting serializer=)
backend = RedisBackend(sync_client=redis.Redis(), serializer=JsonSerializer())

# HMAC-signed pickle for arbitrary Python objects
serializer = HmacPickleSerializer(secret=b"my-secret-key")
backend = RedisBackend(sync_client=redis.Redis(), serializer=serializer)

# Or set PYSILIENCE_CACHE_SECRET in the environment instead of passing secret=
# Generate a value: python -c "import secrets; print(secrets.token_hex(32))"

# MessagePack (compact binary)
serializer = MsgpackSerializer()
backend = RedisBackend(sync_client=redis.Redis(), serializer=serializer)
```

`JsonSerializer` wraps non-JSON types in a type envelope and supports custom types via `register()`:

```python
from pysilience.cache_serializer_json import JsonSerializer

JsonSerializer.register(
    UserProfile,
    encode=lambda u: {"name": u.name},
    decode=lambda d: UserProfile(d["name"]),
)
```

`MsgpackSerializer` uses extension type IDs you choose; optional handlers for `datetime` / `date` / `time` live in `pysilience.cache_serializer_msgpack_builtins`.

Custom serializers are also supported — implement `dumps(value) -> bytes` and `loads(raw) -> Any`.

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
