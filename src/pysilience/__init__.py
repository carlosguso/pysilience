"""
Pysilience - Fault Tolerance Library for Python
================================================

A lightweight fault tolerance library inspired by resilience4j. Patterns share
a small core (event notification, instance registry) for composition and tools.

Patterns:
    - timeout: Limits execution time of operations
    - retry: Automatically retries failed operations
    - circuitbreaker: Prevents cascading failures with three-state state machine
    - ratelimiter: Limits rate of operations
    - bulkhead: Limits concurrent executions
    - fallback: Provides alternative results on failure
    - cache: Caches function results with LRU eviction and TTL

Basic usage:
    >>> from pysilience import timeout, retry, fallback
    >>>
    >>> @fallback(action=lambda exc: "default")
    ... @retry(max_attempts=3, initial_interval=0.5)
    ... @timeout(duration=5.0)
    ... def resilient_operation():
    ...     ...

Registry and core utilities:
    >>> from pysilience import register, get_registered, create_retry
    >>> r = create_retry(name="api", register=True)  # doctest: +SKIP

Submodules: ``pysilience.timeout``, ``pysilience.retry``, ``pysilience.circuitbreaker``,
``pysilience.ratelimiter``, ``pysilience.bulkhead``, ``pysilience.fallback``,
``pysilience.cache``, ``pysilience.cache_redis``, ``pysilience.core``.
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
from pysilience.cache_serializer import CacheSerializer
from pysilience.cache_serializer_json import JsonSerializer
from pysilience.circuitbreaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerEvent,
    CircuitBreakerEventType,
    CircuitBreakerOpen,
    CircuitBreakerState,
    circuit_breaker,
    create_circuit_breaker,
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
    # Circuit Breaker
    "circuit_breaker",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpen",
    "CircuitBreakerState",
    "CircuitBreakerEvent",
    "CircuitBreakerEventType",
    "create_circuit_breaker",
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
    "CacheSerializer",
    "JsonSerializer",
    "MemoryBackend",
    "create_cache",
]
