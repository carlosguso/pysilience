"""
Pysilience - HMAC-Signed Pickle Cache Serializer
=================================================
A :class:`~pysilience.cache_serializer.CacheSerializer` using signed pickle.

License: MIT
"""

from __future__ import annotations

import hashlib
import hmac
import os
import pickle
from typing import Any

from pysilience.cache_serializer import CacheSerializer

__all__ = ["HmacPickleSerializer"]

# SHA-256 produces a 32-byte digest; we prepend it to every stored value.
_HMAC_DIGEST_SIZE = 32


def _sign(data: bytes, secret: bytes) -> bytes:
    """Return *data* prefixed with its HMAC-SHA256 signature."""
    sig = hmac.new(secret, data, hashlib.sha256).digest()
    return sig + data


def _verify_and_load(raw: bytes, secret: bytes) -> Any:
    """Verify the HMAC-SHA256 signature and unpickle the payload.

    Raises :class:`ValueError` if the signature is absent, too short, or
    does not match — indicating corruption, tampering, or unsigned legacy data.
    """
    if len(raw) < _HMAC_DIGEST_SIZE:
        raise ValueError(
            "Cache entry is too short to contain a signature "
            "(possible corruption or unsigned legacy data)"
        )
    sig, payload = raw[:_HMAC_DIGEST_SIZE], raw[_HMAC_DIGEST_SIZE:]
    expected = hmac.new(secret, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise ValueError(
            "Cache entry signature mismatch — possible tampering or secret rotation"
        )
    return pickle.loads(payload)  # noqa: S301


class HmacPickleSerializer(CacheSerializer):
    """Pickle (protocol 5) serialisation with HMAC-SHA256 integrity protection.

    Values are serialised with :mod:`pickle` and prefixed with a 32-byte
    HMAC-SHA256 signature.  :meth:`loads` verifies the signature before
    unpickling, so a compromised Redis server cannot achieve remote code
    execution through crafted payloads.

    Args:
        secret: HMAC signing secret (``bytes`` or ``str``).  If omitted the
            ``PYSILIENCE_CACHE_SECRET`` environment variable is used.  A
            :class:`ValueError` is raised at construction time if neither
            source provides a non-empty value.

    Example::

        from pysilience.cache_redis import RedisBackend
        from pysilience.cache_serializer_hmac import HmacPickleSerializer

        serializer = HmacPickleSerializer(secret=b"my-secret-key")
        backend = RedisBackend(sync_client=redis.Redis(), serializer=serializer)
    """

    __slots__ = ("_secret",)

    def __init__(self, *, secret: bytes | str | None = None) -> None:
        _secret: bytes | str | None = secret
        if not _secret:
            env_val = os.environ.get("PYSILIENCE_CACHE_SECRET", "")
            if env_val:
                _secret = env_val
        if not _secret:
            raise ValueError(
                "HmacPickleSerializer requires a signing secret to prevent unsafe "
                "deserialization. Pass secret=b'...' or set the "
                "PYSILIENCE_CACHE_SECRET environment variable. "
                "Generate a suitable value with:\n"
                "    python -c \"import secrets; print(secrets.token_hex(32))\""
            )
        self._secret: bytes = (
            _secret if isinstance(_secret, bytes) else _secret.encode()
        )

    def dumps(self, value: Any) -> bytes:
        """Pickle *value* and return HMAC-signed bytes."""
        return _sign(pickle.dumps(value, protocol=5), self._secret)

    def loads(self, raw: bytes) -> Any:
        """Verify the HMAC signature and unpickle *raw*."""
        return _verify_and_load(raw, self._secret)
