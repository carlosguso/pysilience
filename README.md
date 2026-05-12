# Pysilience

A lightweight fault tolerance library for Python, inspired by [resilience4j](https://github.com/resilience4j/resilience4j).

## Features

- **Minimal dependencies**: Core patterns use only the Python standard library; optional backends (e.g. Redis) are installed separately
- **Copy-paste friendly**: Each pattern is self-contained in a single file
- **Async support**: Works with both sync and async functions
- **Type hints**: Full typing support for better IDE experience
- **Decorator-based**: Simple `@decorator` API

## Installation

```bash
pip install pysilience
# or
uv add pysilience
```

Or just copy the file you need directly into your project!

## Quick Start

```python
from pysilience import timeout

@timeout(duration=5.0)
def call_external_api():
    # Your code here
    ...

@timeout(duration=10.0)
async def async_api_call():
    # Your async code here
    ...
```

## Patterns

| Pattern | Status | Description |
|---------|--------|-------------|
| Timeout | ✅ Ready | Limits execution time |
| Retry | 🚧 Coming | Retries failed operations |
| Circuit Breaker | 🚧 Coming | Prevents cascading failures |
| Rate Limiter | 🚧 Coming | Limits operation rate |
| Bulkhead | 🚧 Coming | Limits concurrency |

## Copy-Paste Usage

Each pattern is designed to work standalone. Just copy the file:

```bash
curl -O https://raw.githubusercontent.com/yourusername/pysilience/main/src/pysilience/timeout.py
```

Then use it directly:

```python
from timeout import timeout

@timeout(duration=5.0)
def my_function():
    ...
```

## License

MIT
