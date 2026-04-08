from __future__ import annotations

import asyncio
import inspect
import threading
import uuid
from contextlib import asynccontextmanager
from typing import Any, Callable

from slatedb.uniffi import (
    DbBuilder,
    DbIterator,
    DbReaderBuilder,
    DurabilityLevel,
    FlushOptions,
    FlushType,
    KeyValue,
    LogCallback,
    LogRecord,
    MergeOperator,
    MergeOptions,
    ObjectStore,
    PutOptions,
    ReadOptions,
    ReadSources,
    ReaderOptions,
    RowEntry,
    RowEntryKind,
    ScanOptions,
    Ttl,
    WalFileIterator,
    WriteOptions,
)

TEST_DB_PATH = "test-db"


def new_memory_store() -> ObjectStore:
    return ObjectStore.resolve("memory:///")


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def read_options() -> ReadOptions:
    return ReadOptions(
        durability_filter=DurabilityLevel.MEMORY,
        dirty=False,
        cache_blocks=True,
        read_sources=ReadSources(sources=[]),
    )


def scan_options(read_ahead_bytes: int, cache_blocks: bool, max_fetch_tasks: int) -> ScanOptions:
    return ScanOptions(
        durability_filter=DurabilityLevel.MEMORY,
        dirty=False,
        read_ahead_bytes=read_ahead_bytes,
        cache_blocks=cache_blocks,
        max_fetch_tasks=max_fetch_tasks,
        read_sources=ReadSources(sources=[]),
    )


def reader_options(skip_wal_replay: bool) -> ReaderOptions:
    return ReaderOptions(
        manifest_poll_interval_ms=100,
        checkpoint_lifetime_ms=1_000,
        max_memtable_bytes=64 * 1024 * 1024,
        skip_wal_replay=skip_wal_replay,
    )


def write_options() -> WriteOptions:
    return WriteOptions(await_durable=True)


def put_options() -> PutOptions:
    return PutOptions(ttl=Ttl.DEFAULT())


def merge_options() -> MergeOptions:
    return MergeOptions(ttl=Ttl.DEFAULT())


@asynccontextmanager
async def open_db(
    store: ObjectStore,
    path: str = TEST_DB_PATH,
    configure: Callable[[DbBuilder], Any] | None = None,
):
    builder = DbBuilder(path, store)
    if configure is not None:
        await _maybe_await(configure(builder))
    db = await builder.build()
    try:
        yield db
    finally:
        await db.shutdown()


@asynccontextmanager
async def open_reader(
    store: ObjectStore,
    path: str = TEST_DB_PATH,
    configure: Callable[[DbReaderBuilder], Any] | None = None,
):
    builder = DbReaderBuilder(path, store)
    if configure is not None:
        await _maybe_await(configure(builder))
    reader = await builder.build()
    try:
        yield reader
    finally:
        await reader.shutdown()


async def drain_iterator(iterator: DbIterator) -> list[KeyValue]:
    rows: list[KeyValue] = []
    while True:
        row = await iterator.next()
        if row is None:
            return rows
        rows.append(row)


async def drain_wal_iterator(iterator: WalFileIterator) -> list[RowEntry]:
    rows: list[RowEntry] = []
    while True:
        row = await iterator.next()
        if row is None:
            return rows
        rows.append(row)


def require_rows(rows: list[KeyValue], want_keys: list[str], want_values: list[str]) -> None:
    assert len(rows) == len(want_keys) == len(want_values)
    for index, row in enumerate(rows):
        assert row.key == want_keys[index].encode()
        assert row.value == want_values[index].encode()


def require_wal_row(row: RowEntry, kind: RowEntryKind, key: str, value: str | None) -> None:
    assert row.kind == kind
    assert row.key == key.encode()
    if value is None:
        assert row.value is None
    else:
        assert row.value == value.encode()


async def wait_until(
    check: Callable[[], Any],
    timeout: float = 60,
    step: float = 0.025,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    last_error: BaseException | None = None

    while True:
        try:
            if await _maybe_await(check()):
                return
            last_error = None
        except Exception as error:  # pragma: no cover - helper for polling assertions
            last_error = error

        if asyncio.get_running_loop().time() >= deadline:
            break

        await asyncio.sleep(step)

    if last_error is not None:
        raise AssertionError(f"timed out after {timeout} seconds") from last_error
    raise AssertionError(f"timed out after {timeout} seconds")


class ConcatMergeOperator(MergeOperator):
    def merge(self, key: bytes, existing_value: bytes | None, operand: bytes) -> bytes:
        del key
        return (existing_value or b"") + operand


def clone_log_record(record: LogRecord) -> LogRecord:
    return LogRecord(
        level=record.level,
        target=record.target,
        message=record.message,
        module_path=record.module_path,
        file=record.file,
        line=record.line,
    )


class LogCollector(LogCallback):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: list[LogRecord] = []

    def log(self, record: LogRecord) -> None:
        with self._lock:
            self._records.append(clone_log_record(record))

    def matching_record(self, predicate: Callable[[LogRecord], bool]) -> LogRecord | None:
        with self._lock:
            for record in self._records:
                if predicate(record):
                    return clone_log_record(record)
        return None


async def seed_wal_files(store: ObjectStore) -> None:
    async with open_db(
        store,
        configure=lambda builder: builder.with_merge_operator(ConcatMergeOperator()),
    ) as db:
        await db.put(b"a", b"1")
        await db.put(b"b", b"2")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.WAL))

        await db.delete(b"a")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.WAL))

        await db.merge(b"m", b"x")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.WAL))


def unique_path(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"
