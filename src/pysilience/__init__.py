"""
Pysilience - Fault Tolerance Library for Python
================================================

A lightweight fault tolerance library inspired by resilience4j.
Each pattern is self-contained and can be copied directly into your project.

Patterns:
    - timeout: Limits execution time of operations
    - retry: Automatically retries failed operations (coming soon)
    - circuitbreaker: Prevents cascading failures (coming soon)
    - ratelimiter: Limits rate of operations (coming soon)
    - bulkhead: Limits concurrent executions (coming soon)

Basic Usage:
    >>> from pysilience import timeout
    >>> 
    >>> @timeout(duration=5.0)
    ... def slow_operation():
    ...     ...

Each pattern can also be imported individually:
    >>> from pysilience.timeout import Timeout, TimeoutConfig, OperationTimeout
"""

from pysilience._version import __version__
from pysilience.timeout import (
    OperationTimeout,
    Timeout,
    TimeoutConfig,
    TimeoutEvent,
    TimeoutEventType,
    timeout,
)

__all__ = [
    # Version
    "__version__",
    # Timeout
    "timeout",
    "Timeout",
    "TimeoutConfig",
    "OperationTimeout",
    "TimeoutEvent",
    "TimeoutEventType",
]
