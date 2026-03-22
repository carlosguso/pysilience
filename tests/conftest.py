"""Pytest configuration and fixtures."""

import pytest


# Configure pytest-asyncio
def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "slow: marks tests as slow")
