"""
Tests for the MessagePack cache serializer.

Requires: pip install pysilience[msgpack]  (included in dev extras)

Run with: pytest tests/test_cache_serializer_msgpack.py -v
"""

from __future__ import annotations

import struct

import msgpack
import pytest

from pysilience.cache_serializer import CacheSerializer
from pysilience.cache_serializer_msgpack import MsgpackSerializer


class TestMsgpackSerializer:
    def test_implements_cache_serializer_protocol(self) -> None:
        assert isinstance(MsgpackSerializer(), CacheSerializer)

    def test_round_trip(self) -> None:
        serializer = MsgpackSerializer()
        value = {"hello": "world", "n": 42, "ok": True, "empty": None}
        assert serializer.loads(serializer.dumps(value)) == value

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

    def test_register_rejects_invalid_type_id(self) -> None:
        serializer = MsgpackSerializer()
        with pytest.raises(ValueError, match="type_id must be between"):

            @serializer.register(type_id=63)
            class BadId:
                def __pack__(self) -> bytes:
                    return b""

                @classmethod
                def __unpack__(cls, data: bytes) -> BadId:
                    return cls()

    def test_register_rejects_instance_unpack(self) -> None:
        serializer = MsgpackSerializer()
        with pytest.raises(TypeError, match="@classmethod __unpack__"):

            @serializer.register(type_id=66)
            class BadUnpack:
                def __pack__(self) -> bytes:
                    return b""

                def __unpack__(self, data: bytes) -> BadUnpack:
                    return self

    def test_register_rejects_duplicate_type_id(self) -> None:
        serializer = MsgpackSerializer()

        @serializer.register(type_id=65)
        class First:
            def __pack__(self) -> bytes:
                return b"a"

            @classmethod
            def __unpack__(cls, data: bytes) -> First:
                return cls()

        with pytest.raises(ValueError, match="already registered"):

            @serializer.register(type_id=65)
            class Second:
                def __pack__(self) -> bytes:
                    return b"b"

                @classmethod
                def __unpack__(cls, data: bytes) -> Second:
                    return cls()

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
