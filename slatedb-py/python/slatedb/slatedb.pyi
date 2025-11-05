"""
Typing stubs for the ``slatedb`` module.

These stubs describe the Python API exposed by the Rust extension and are
used by type checkers and IDEs for autocompletion and linting.
"""

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Literal, TypedDict

Isolation = Literal["si", "ssi", "snapshot", "serializable", "serializable_snapshot"]


class TransactionError(Exception): ...
class ClosedError(Exception): ...
class UnavailableError(Exception): ...
class InvalidError(Exception): ...
class DataError(Exception): ...
class InternalError(Exception): ...


class SlateDB:
    """Read/write interface to a SlateDB database."""

    def __init__(
        self,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
        settings: str | None = None,
    ) -> None: ...

    @classmethod
    async def open_async(
        cls,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
        settings: str | None = None,
    ) -> SlateDB: ...

    def snapshot(self) -> SlateDBSnapshot: ...
    async def snapshot_async(self) -> SlateDBSnapshot: ...

    def begin(self, isolation: Isolation | None = None) -> SlateDBTransaction: ...
    async def begin_async(self, isolation: Isolation | None = None) -> SlateDBTransaction: ...

    def put(self, key: bytes, value: bytes) -> None: ...
    async def put_async(self, key: bytes, value: bytes) -> None: ...
    def get(self, key: bytes) -> bytes | None: ...
    async def get_async(self, key: bytes) -> bytes | None: ...
    def delete(self, key: bytes) -> None: ...
    async def delete_async(self, key: bytes) -> None: ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    def scan_with_options(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...
    async def scan_with_options_async(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...

    def merge(self, key: bytes, value: bytes) -> None: ...
    async def merge_async(self, key: bytes, value: bytes) -> None: ...
    def close(self) -> None: ...
    async def close_async(self) -> None: ...


class SlateDBSnapshot:
    def get(self, key: bytes) -> bytes | None: ...
    async def get_async(self, key: bytes) -> bytes | None: ...
    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    def scan_with_options(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...
    async def scan_with_options_async(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...
    def close(self) -> None: ...
    async def close_async(self) -> None: ...


class SlateDBTransaction:
    def get(self, key: bytes) -> bytes | None: ...
    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    def scan_with_options(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...
    def put(self, key: bytes, value: bytes) -> None: ...
    def delete(self, key: bytes) -> None: ...
    def merge(self, key: bytes, value: bytes) -> None: ...
    def commit(self) -> None: ...
    async def commit_async(self) -> None: ...
    def rollback(self) -> None: ...


class SlateDBReader:
    def __init__(
        self,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        checkpoint_id: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
    ) -> None: ...

    @classmethod
    async def open_async(
        cls,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        checkpoint_id: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
    ) -> SlateDBReader: ...

    def get(self, key: bytes) -> bytes | None: ...
    async def get_async(self, key: bytes) -> bytes | None: ...
    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator: ...
    def scan_with_options(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...
    async def scan_with_options_async(
        self,
        start: bytes,
        end: bytes | None = None,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
        read_ahead_bytes: int | None = None,
        cache_blocks: bool | None = None,
        max_fetch_tasks: int | None = None,
    ) -> DbIterator: ...
    def close(self) -> None: ...
    async def close_async(self) -> None: ...


class DbIterator(Iterator[tuple[bytes, bytes]], AsyncIterator[tuple[bytes, bytes]]):
    def __iter__(self) -> DbIterator: ...
    def __next__(self) -> tuple[bytes, bytes]: ...
    def __aiter__(self) -> DbIterator: ...
    async def __anext__(self) -> tuple[bytes, bytes]: ...
    def seek(self, key: bytes) -> None: ...
    async def seek_async(self, key: bytes) -> None: ...


class Checkpoint(TypedDict):
    id: str
    manifest_id: int
    expire_time: int | None
    create_time: int


class CheckpointCreateResult(TypedDict):
    id: str
    manifest_id: int


class SlateDBAdmin:
    def __init__(self, path: str, url: str | None = None, env_file: str | None = None) -> None: ...
    def create_checkpoint(
        self,
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult: ...
    async def create_checkpoint_async(
        self,
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult: ...
    def list_checkpoints(self) -> list[Checkpoint]: ...
    async def list_checkpoints_async(self) -> list[Checkpoint]: ...
