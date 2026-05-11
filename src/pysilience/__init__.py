"""
Pysilience - Fault Tolerance Library for Python
================================================

A lightweight fault tolerance library inspired by resilience4j. Patterns share
a small core (event notification, instance registry) for composition and tools.

Patterns:
    - timeout: Limits execution time of operations
    - retry: Automatically retries failed operations
    - bulkhead: Limits concurrent executions
    - circuitbreaker: Prevents cascading failures (coming soon)
    - ratelimiter: Limits rate of operations (coming soon)

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
``pysilience.core``.
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
    # Bulkhead
    "bulkhead",
    "Bulkhead",
    "BulkheadConfig",
    "BulkheadRejected",
    "BulkheadEvent",
    "BulkheadEventType",
    "create_bulkhead",
]
