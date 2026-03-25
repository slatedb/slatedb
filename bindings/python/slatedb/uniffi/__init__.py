"""Public Python wrapper for the generated SlateDB UniFFI bindings."""

from importlib import import_module

_generated = import_module("._slatedb_uniffi.slatedb", __name__)
from ._slatedb_uniffi import *  # noqa: E402,F403

__all__ = _generated.__all__
