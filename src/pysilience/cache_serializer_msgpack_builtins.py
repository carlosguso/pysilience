"""
Pysilience - MessagePack Built-in Type Handlers
================================================
Optional pack/unpack helpers for common Python types.

These are **not** registered automatically.  Choose extension type IDs and
wire them up on your :class:`~pysilience.cache_serializer_msgpack.MsgpackSerializer`::

    from datetime import datetime

    from pysilience.cache_serializer_msgpack import MsgpackSerializer
    from pysilience.cache_serializer_msgpack_builtins import (
        pack_date,
        pack_datetime,
        pack_time,
        unpack_date,
        unpack_datetime,
        unpack_time,
    )

    serializer = MsgpackSerializer()
    serializer.register_type(datetime, type_id=64, pack=pack_datetime, unpack=unpack_datetime)
    serializer.register_type(date, type_id=65, pack=pack_date, unpack=unpack_date)
    serializer.register_type(time, type_id=66, pack=pack_time, unpack=unpack_time)

``bytes`` needs no handler — msgpack stores binary data natively.

License: MIT
"""

from __future__ import annotations

from datetime import date, datetime, time

__all__ = [
    "pack_date",
    "pack_datetime",
    "pack_time",
    "unpack_date",
    "unpack_datetime",
    "unpack_time",
]


def pack_datetime(value: datetime) -> bytes:
    """Serialize *value* as an ISO-8601 UTF-8 string."""
    return value.isoformat().encode()


def unpack_datetime(data: bytes) -> datetime:
    """Restore a :class:`datetime.datetime` from :func:`pack_datetime` output."""
    return datetime.fromisoformat(data.decode())


def pack_date(value: date) -> bytes:
    """Serialize *value* as an ISO-8601 UTF-8 string."""
    return value.isoformat().encode()


def unpack_date(data: bytes) -> date:
    """Restore a :class:`datetime.date` from :func:`pack_date` output."""
    return date.fromisoformat(data.decode())


def pack_time(value: time) -> bytes:
    """Serialize *value* as an ISO-8601 UTF-8 string."""
    return value.isoformat().encode()


def unpack_time(data: bytes) -> time:
    """Restore a :class:`datetime.time` from :func:`pack_time` output."""
    return time.fromisoformat(data.decode())
