"""
Tests for the MessagePack cache serializer.

Requires: pip install pysilience[msgpack]  (included in dev extras)

Run with: pytest tests/test_cache_serializer_msgpack.py -v
"""

from __future__ import annotations

import struct
from datetime import date, datetime, time

import msgpack
import pytest

from pysilience.cache_serializer import CacheSerializer
from pysilience.cache_serializer_msgpack import MsgpackSerializer
from pysilience.cache_serializer_msgpack_builtins import (
    pack_date,
    pack_datetime,
    pack_time,
    unpack_date,
    unpack_datetime,
    unpack_time,
)


def _register_iso_builtins(
    serializer: MsgpackSerializer,
    *,
    datetime_id: int = 64,
    date_id: int = 65,
    time_id: int = 66,
) -> None:
    """Test helper: wire optional ISO handlers at caller-chosen type IDs."""
    serializer.register_type(
        datetime, type_id=datetime_id, pack=pack_datetime, unpack=unpack_datetime
    )
    serializer.register_type(date, type_id=date_id, pack=pack_date, unpack=unpack_date)
    serializer.register_type(time, type_id=time_id, pack=pack_time, unpack=unpack_time)


class TestMsgpackSerializer:
    def test_implements_cache_serializer_protocol(self) -> None:
        assert isinstance(MsgpackSerializer(), CacheSerializer)

    def test_round_trip(self) -> None:
        serializer = MsgpackSerializer()
        value = {"hello": "world", "n": 42, "ok": True, "empty": None}
        assert serializer.loads(serializer.dumps(value)) == value

    def test_datetime_requires_registration(self) -> None:
        serializer = MsgpackSerializer()
        with pytest.raises(TypeError, match="not serializable"):
            serializer.dumps(datetime(2025, 6, 15, 14, 30, 0))

    def test_datetime_round_trip(self) -> None:
        serializer = MsgpackSerializer()
        _register_iso_builtins(serializer)
        value = datetime(2025, 6, 15, 14, 30, 0)
        assert serializer.loads(serializer.dumps(value)) == value

    def test_date_round_trip(self) -> None:
        serializer = MsgpackSerializer()
        _register_iso_builtins(serializer)
        value = date(2025, 6, 15)
        assert serializer.loads(serializer.dumps(value)) == value

    def test_time_round_trip(self) -> None:
        serializer = MsgpackSerializer()
        _register_iso_builtins(serializer)
        value = time(14, 30, 0)
        assert serializer.loads(serializer.dumps(value)) == value

    def test_nested_datetime_in_dict(self) -> None:
        serializer = MsgpackSerializer()
        _register_iso_builtins(serializer)
        ts = datetime(2025, 1, 1, 12, 0, 0)
        value = {"user": "alice", "created_at": ts}
        assert serializer.loads(serializer.dumps(value)) == value

    def test_override_registered_handler(self) -> None:
        serializer = MsgpackSerializer()
        _register_iso_builtins(serializer)
        original = serializer.loads(serializer.dumps(datetime(2025, 1, 1, 12, 0, 0)))

        serializer.register_type(
            datetime,
            pack=lambda d: str(d.timestamp()).encode(),
            unpack=lambda b: datetime.fromtimestamp(float(b.decode())),
        )
        try:
            value = datetime(2025, 6, 15, 14, 30, 0)
            assert serializer.loads(serializer.dumps(value)) == value
        finally:
            serializer.register_type(
                datetime, pack=pack_datetime, unpack=unpack_datetime
            )

        assert serializer.loads(serializer.dumps(original)) == original

    def test_bytes_round_trip(self) -> None:
        serializer = MsgpackSerializer()
        value = b"\x00\x01\xff"
        assert serializer.loads(serializer.dumps(value)) == value

    def test_custom_type_via_register(self) -> None:
        serializer = MsgpackSerializer()

        @serializer.register(type_id=64)
        class GeoPoint:
            __slots__ = ("lat", "lon")

            def __init__(self, lat: float, lon: float) -> None:
                self.lat = lat
                self.lon = lon

            def __eq__(self, other: object) -> bool:
                return (
                    isinstance(other, GeoPoint)
                    and self.lat == other.lat
                    and self.lon == other.lon
                )

            def __pack__(self) -> bytes:
                return struct.pack("dd", self.lat, self.lon)

            @classmethod
            def __unpack__(cls, data: bytes) -> GeoPoint:
                lat, lon = struct.unpack("dd", data)
                return cls(lat, lon)

        point = GeoPoint(40.7128, -74.0060)
        assert serializer.loads(serializer.dumps(point)) == point

        nested = {"origin": point, "label": "NYC"}
        assert serializer.loads(serializer.dumps(nested)) == nested

    def test_register_rejects_occupied_type_id(self) -> None:
        serializer = MsgpackSerializer()

        @serializer.register(type_id=1)
        class First:
            def __pack__(self) -> bytes:
                return b"a"

            @classmethod
            def __unpack__(cls, data: bytes) -> First:
                return cls()

        with pytest.raises(ValueError, match="unregister_type"):

            @serializer.register(type_id=1)
            class Second:
                def __pack__(self) -> bytes:
                    return b"b"

                @classmethod
                def __unpack__(cls, data: bytes) -> Second:
                    return cls()

        serializer.unregister_type(First)

        @serializer.register(type_id=1)
        class Second:
            def __pack__(self) -> bytes:
                return b"b"

            @classmethod
            def __unpack__(cls, data: bytes) -> Second:
                return cls()

        assert isinstance(serializer.loads(serializer.dumps(Second())), Second)

    def test_register_rejects_instance_unpack(self) -> None:
        serializer = MsgpackSerializer()
        with pytest.raises(TypeError, match="@classmethod __unpack__"):

            @serializer.register(type_id=64)
            class BadUnpack:
                def __pack__(self) -> bytes:
                    return b""

                def __unpack__(self, data: bytes) -> BadUnpack:
                    return self

    def test_unregistered_type_raises(self) -> None:
        serializer = MsgpackSerializer()

        class Unregistered:
            pass

        with pytest.raises(TypeError, match="not serializable"):
            serializer.dumps(Unregistered())

    def test_unknown_extension_raises(self) -> None:
        serializer = MsgpackSerializer()
        payload = msgpack.packb(msgpack.ExtType(99, b"orphan"), use_bin_type=True)
        with pytest.raises(ValueError, match="Unknown msgpack extension type"):
            serializer.loads(payload)

    def test_invalid_payload_raises(self) -> None:
        serializer = MsgpackSerializer()
        with pytest.raises(ValueError):
            serializer.loads(b"\xff\xfe not msgpack")

    def test_import_error_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Missing msgpack should surface the install extra hint."""
        import builtins
        import importlib

        real_import = builtins.__import__

        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "msgpack":
                raise ImportError("No module named 'msgpack'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match="pysilience\\[msgpack\\]"):
            importlib.reload(
                importlib.import_module("pysilience.cache_serializer_msgpack")
            )


class TestMsgpackBuiltins:
    def test_iso_datetime_round_trip(self) -> None:
        value = datetime(2025, 6, 15, 14, 30, 0)
        assert unpack_datetime(pack_datetime(value)) == value

    def test_iso_date_round_trip(self) -> None:
        value = date(2025, 6, 15)
        assert unpack_date(pack_date(value)) == value

    def test_iso_time_round_trip(self) -> None:
        value = time(14, 30, 0)
        assert unpack_time(pack_time(value)) == value

    def test_user_chosen_type_ids(self) -> None:
        """Same handlers work at any extension ID the caller chooses."""
        serializer = MsgpackSerializer()
        _register_iso_builtins(serializer, datetime_id=100, date_id=101, time_id=102)
        ts = datetime(2025, 1, 1, 12, 0, 0)
        assert serializer.loads(serializer.dumps(ts)) == ts
