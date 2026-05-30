"""
Tests for the MessagePack cache serializer.

Requires: pip install pysilience[msgpack]  (included in dev extras)

Run with: pytest tests/test_cache_serializer_msgpack.py -v
"""

from __future__ import annotations

from datetime import datetime

import pytest

from pysilience.cache_serializer import CacheSerializer
from pysilience.cache_serializer_json import JsonSerializer
from pysilience.cache_serializer_msgpack import MsgpackSerializer

_MSGPACK = MsgpackSerializer()


class TestMsgpackSerializer:
    def test_implements_cache_serializer_protocol(self) -> None:
        assert isinstance(MsgpackSerializer(), CacheSerializer)

    def test_round_trip(self) -> None:
        value = {"hello": "world", "n": 42, "ok": True, "empty": None}
        assert _MSGPACK.loads(_MSGPACK.dumps(value)) == value

    def test_datetime_round_trip(self) -> None:
        value = datetime(2025, 6, 15, 14, 30, 0)
        assert _MSGPACK.loads(_MSGPACK.dumps(value)) == value

    def test_bytes_round_trip(self) -> None:
        value = b"\x00\x01\xff"
        assert _MSGPACK.loads(_MSGPACK.dumps(value)) == value

    def test_shared_type_registry(self) -> None:
        class Widget:
            __slots__ = ("id",)

            def __init__(self, id: int) -> None:
                self.id = id

            def __eq__(self, other: object) -> bool:
                return isinstance(other, Widget) and self.id == other.id

        JsonSerializer.register(
            Widget,
            encode=lambda w: w.id,
            decode=lambda i: Widget(i),
        )
        try:
            widget = Widget(99)
            assert _MSGPACK.loads(_MSGPACK.dumps(widget)) == widget
        finally:
            JsonSerializer.unregister(Widget)

    def test_invalid_payload_raises(self) -> None:
        with pytest.raises(ValueError):
            _MSGPACK.loads(b"\xff\xfe not msgpack")

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
