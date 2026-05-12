"""
Pysilience - Fault Tolerance Library for Python
================================================

A lightweight fault tolerance library inspired by resilience4j. Patterns share
a small core (event notification, instance registry) for composition and tools.

Patterns:
    - timeout: Limits execution time of operations
    - retry: Automatically retries failed operations
    - bulkhead: Limits concurrent executions
    - fallback: Provides alternative results on failure
    - cache: Caches function results with LRU eviction and TTL
    - circuitbreaker: Prevents cascading failures (coming soon)
    - ratelimiter: Limits rate of operations

Basic usage:
    >>> from pysilience import timeout
    >>>
    >>> @timeout(duration=5.0)
    ... def slow_operation():
    ...     ...

Registry and core utilities:
    >>> from pysilience import register, get_registered, create_retry
    >>> r = create_retry(name="api", register=True)  # doctest: +SKIP

Submodules: ``pysilience.timeout``, ``pysilience.retry``, ``pysilience.bulkhead``,
``pysilience.fallback``, ``pysilience.cache``, ``pysilience.core``.
"""

from pysilience._version import __version__
from pysilience.bulkhead import (
    Bulkhead,
    BulkheadConfig,
    BulkheadEvent,
    BulkheadEventType,
    BulkheadRejected,
    bulkhead,
    create_bulkhead,
)
from pysilience.cache import (
    Cache,
    CacheBackend,
    CacheConfig,
    CacheEvent,
    CacheEventType,
    MemoryBackend,
    cache,
    create_cache,
)
from pysilience.core import (
    clear as clear_registry,
)
from pysilience.core import (
    get as get_registered,
)
from pysilience.core import (
    register,
    unregister,
)
from pysilience.fallback import (
    Fallback,
    FallbackConfig,
    FallbackEvent,
    FallbackEventType,
    create_fallback,
    fallback,
)
from pysilience.ratelimiter import (
    RateLimitAlgorithm,
    RateLimiter,
    RateLimiterConfig,
    RateLimiterEvent,
    RateLimiterEventType,
    RateLimitExceeded,
    create_rate_limiter,
    rate_limiter,
)
from pysilience.retry import (
    RetriesExhausted,
    Retry,
    RetryConfig,
    RetryEvent,
    RetryEventType,
    create_retry,
    retry,
)
from pysilience.timeout import (
    OperationTimeout,
    Timeout,
    TimeoutConfig,
    TimeoutEvent,
    TimeoutEventType,
    create_timeout,
    timeout,
)

__all__ = [
    # Version
    "__version__",
    # Core registry
    "register",
    "get_registered",
    "unregister",
    "clear_registry",
    # Timeout
    "timeout",
    "Timeout",
    "TimeoutConfig",
    "OperationTimeout",
    "TimeoutEvent",
    "TimeoutEventType",
    "create_timeout",
    # Retry
    "retry",
    "Retry",
    "RetryConfig",
    "RetriesExhausted",
    "RetryEvent",
    "RetryEventType",
    "create_retry",
    # Rate Limiter
    "rate_limiter",
    "RateLimiter",
    "RateLimiterConfig",
    "RateLimitAlgorithm",
    "RateLimitExceeded",
    "RateLimiterEvent",
    "RateLimiterEventType",
    "create_rate_limiter",
    # Bulkhead
    "bulkhead",
    "Bulkhead",
    "BulkheadConfig",
    "BulkheadRejected",
    "BulkheadEvent",
    "BulkheadEventType",
    "create_bulkhead",
    # Fallback
    "fallback",
    "Fallback",
    "FallbackConfig",
    "FallbackEvent",
    "FallbackEventType",
    "create_fallback",
    # Cache
    "cache",
    "Cache",
    "CacheBackend",
    "CacheConfig",
    "CacheEvent",
    "CacheEventType",
    "MemoryBackend",
    "create_cache",
]
