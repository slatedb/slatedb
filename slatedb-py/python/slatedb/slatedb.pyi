"""
SlateDB Python API typing stubs.

This module describes the public Python interface for SlateDB, a
cloud-native, embeddable, transactional key–value database optimized for
object storage. The stubs are used by type checkers and IDEs for
autocompletion, static analysis, and inline documentation.

Overview:
- ``SlateDB`` provides read/write access, transactions, and snapshots.
- ``SlateDBReader`` provides read-only access, optionally pinned to a
  checkpoint.
- ``SlateDBAdmin`` exposes administrative functionality for manifests,
  checkpoints, cloning, and garbage collection.
- Iteration is available via ``scan(...)`` returning ``DbIterator`` that
  supports both sync and async iteration.

See also:
- Project website: https://slatedb.io
- Root README: README.md (project root)
"""

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Literal, TypedDict

# Isolation levels for transactions.
Isolation = Literal["si", "ssi", "snapshot", "serializable", "serializable_snapshot"]


class TransactionError(Exception):
    """Raised when a transaction conflict occurs (retryable)."""


class ClosedError(Exception):
    """Raised when an operation targets a closed or fenced resource."""


class UnavailableError(Exception):
    """Raised when the underlying object store or network is unavailable."""


class InvalidError(Exception):
    """Raised for invalid arguments, configuration, or method usage."""


class DataError(Exception):
    """Raised for invalid or corrupted on-disk/object-store data."""


class InternalError(Exception):
    """Raised for unexpected internal errors."""


class SlateDB:
    """
    Read/write interface to a SlateDB database.

    Use this class to open a database, read and write keys, and run
    transactions. For read-only access (optionally at a checkpoint), see
    ``SlateDBReader``.
    """

    def __init__(
        self,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
        settings: str | None = None,
    ) -> None:
        """
        Create or open a SlateDB database.

        Args:
            path: Local path (or logical DB name) for the database.
            url: Optional object store URL (e.g. "s3://bucket/prefix",
                "memory:///"), overrides env_file if provided.
            env_file: Optional path to a file containing environment variables
                to configure the object store when ``url`` is omitted.
            merge_operator: Optional Python callable implementing a merge
                function with signature ``merge(existing: Optional[bytes], value: bytes) -> bytes``.
            settings: Optional path to a SlateDB settings TOML file.

        Examples:
            Open a DB using an env file:

            >>> db = SlateDB("/tmp/mydb", env_file=".env")

            Configure a merge operator:

            >>> def last_write_wins(existing: bytes | None, value: bytes) -> bytes:
            ...     return value
            >>> db = SlateDB("/tmp/mydb", merge_operator=last_write_wins)
        """
        ...

    @classmethod
    async def open_async(
        cls,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
        settings: str | None = None,
    ) -> SlateDB:
        """
        Async constructor that opens the database and returns a SlateDB.

        Args:
            path: Database path.
            url: Optional object store URL.
            env_file: Optional env file for object store config.
            merge_operator: Optional merge function.
            settings: Optional path to settings TOML file.

        Returns:
            An initialized SlateDB instance.

        Examples:
            >>> db = await SlateDB.open_async("/tmp/mydb", env_file=".env")
        """
        ...

    def snapshot(self) -> SlateDBSnapshot:
        """
        Create a read-only snapshot at the current committed state.

        Returns:
            SlateDBSnapshot providing a consistent, read-only view.

        Example:
            >>> snap = db.snapshot()
            >>> _ = snap.get(b"k")
        """
        ...

    async def snapshot_async(self) -> SlateDBSnapshot:
        """
        Async variant of ``snapshot``.

        Returns:
            A SlateDBSnapshot instance.

        Examples:
            >>> snap = await db.snapshot_async()
        """
        ...

    def begin(self, isolation: Isolation | None = None) -> SlateDBTransaction:
        """
        Begin a transaction.

        Args:
            isolation: Isolation level. Aliases:
                - "si" or "snapshot" for Snapshot Isolation
                - "ssi", "serializable", or "serializable_snapshot" for Serializable Snapshot Isolation

        Returns:
            SlateDBTransaction handle.

        Example:
            >>> txn = db.begin("ssi")
            >>> txn.put(b"k", b"v")
            >>> txn.commit()
        """
        ...

    async def begin_async(self, isolation: Isolation | None = None) -> SlateDBTransaction:
        """
        Async variant of ``begin``.

        Args:
            isolation: Optional isolation level string.

        Returns:
            A SlateDBTransaction instance.

        Examples:
            >>> txn = await db.begin_async("si")
        """
        ...

    def put(self, key: bytes, value: bytes) -> None:
        """
        Store a key-value pair.

        Args:
            key: Non-empty key.
            value: Value bytes.

        Example:
            >>> db.put(b"k", b"v")
        """
        ...

    async def put_async(self, key: bytes, value: bytes) -> None:
        """
        Async variant of ``put``.

        Args:
            key: Non-empty key.
            value: Value bytes.

        Examples:
            >>> await db.put_async(b"k", b"v")
        """
        ...

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value by key.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Example:
            >>> db.get(b"missing") is None
            True
        """
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """
        Async variant of ``get``.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Examples:
            >>> await db.put_async(b"k", b"v")
            >>> await db.get_async(b"k")
            b'v'
        """
        ...

    def get_with_options(
        self,
        key: bytes,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
    ) -> bytes | None:
        """
        Get a value with read options.

        Args:
            key: Non-empty key.
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include uncommitted/dirty data if True.

        Returns:
            Value bytes or ``None`` if not found.

        Example:
            >>> db.get_with_options(b"k", durability_filter="memory")
        """
        ...

    async def get_with_options_async(
        self,
        key: bytes,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
    ) -> bytes | None:
        """
        Async variant of ``get_with_options``.

        Args:
            key: Non-empty key.
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include uncommitted/dirty data if True.

        Returns:
            Value bytes or ``None`` if not found.

        Examples:
            >>> await db.get_with_options_async(b"k", durability_filter="remote")
        """
        ...

    def delete(self, key: bytes) -> None:
        """
        Delete a key.

        Args:
            key: Non-empty key to remove.

        Examples:
            >>> db.put(b"temp", b"1")
            >>> db.delete(b"temp")
        """
        ...

    async def delete_async(self, key: bytes) -> None:
        """
        Async variant of ``delete``.

        Args:
            key: Non-empty key to remove.

        Examples:
            >>> await db.put_async(b"temp", b"1")
            >>> await db.delete_async(b"temp")
        """
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Iterate over a key range.

        Args:
            start: Start key (inclusive, non-empty).
            end: Optional end key (exclusive). If ``None``, a bound is derived
                to iterate keys sharing the ``start`` prefix.

        Returns:
            DbIterator yielding ``(key, value)`` sorted by key.

        Examples:
            Iterate synchronously:

            >>> for k, v in db.scan(b"a", b"z"):
            ...     _ = (k, v)
        """
        ...

    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Async variant of ``scan``.

        Args:
            start: Start key (inclusive, non-empty).
            end: Optional end key (exclusive) or ``None`` for prefix range.

        Returns:
            A DbIterator that supports ``async for``.

        Example:
            >>> it = await db.scan_async(b"prefix")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

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
    ) -> DbIterator:
        """
        Iterate with advanced scan options.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include unflushed data if ``True``.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range.

        Examples:
            >>> it = db.scan_with_options(b"a", b"c", read_ahead_bytes=1_000_000)
            >>> next(it)  # doctest: +SKIP
        """
        ...

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
    ) -> DbIterator:
        """
        Async variant of ``scan_with_options``.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include unflushed data if ``True``.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range that supports ``async for``.

        Examples:
            >>> it = await db.scan_with_options_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """
        Merge a value using the configured merge operator.

        Requires that the DB was opened with a ``merge_operator``.

        Args:
            key: Non-empty key.
            value: Value bytes to merge.

        Examples:
            >>> def last_write_wins(existing: bytes | None, value: bytes) -> bytes:
            ...     return value
            >>> db = SlateDB("/tmp/mydb", merge_operator=last_write_wins)
            >>> db.merge(b"k", b"v")
        """
        ...

    async def merge_async(self, key: bytes, value: bytes) -> None:
        """
        Async variant of ``merge``.

        Args:
            key: Non-empty key.
            value: Value bytes to merge.

        Examples:
            >>> await db.merge_async(b"k", b"v")
        """
        ...

    def put_with_options(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
        await_durable: bool | None = None,
    ) -> None:
        """
        Store a key-value pair with per-op options.

        Args:
            key: Non-empty key.
            value: Value bytes.
            ttl: Optional logical TTL units for this write (depends on logical clock).
            await_durable: If False, do not wait for durable write.

        Example:
            >>> db.put_with_options(b"k", b"v", await_durable=False)
        """
        ...

    def delete_with_options(self, key: bytes, *, await_durable: bool | None = None) -> None:
        """
        Delete a key with write options.

        Args:
            key: Non-empty key.
            await_durable: If False, do not wait for durable write.
        """
        ...

    def merge_with_options(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
        await_durable: bool | None = None,
    ) -> None:
        """
        Merge a value using the configured merge operator with per-op options.

        Args:
            key: Non-empty key.
            value: Value bytes to merge.
            ttl: Optional logical TTL units for this write.
            await_durable: If False, do not wait for durable write.

        Examples:
            >>> db.merge_with_options(b"k", b"v", ttl=60_000)
        """
        ...

    async def put_with_options_async(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
        await_durable: bool | None = None,
    ) -> None:
        """
        Async variant of ``put_with_options``.

        Args:
            key: Non-empty key.
            value: Value bytes.
            ttl: Optional logical TTL units.
            await_durable: If False, do not wait for durable write.

        Examples:
            >>> await db.put_with_options_async(b"k", b"v", await_durable=False)
        """
        ...

    async def delete_with_options_async(self, key: bytes, *, await_durable: bool | None = None) -> None:
        """
        Async variant of ``delete_with_options``.

        Args:
            key: Non-empty key.
            await_durable: If False, do not wait for durable write.

        Examples:
            >>> await db.delete_with_options_async(b"k", await_durable=False)
        """
        ...

    async def merge_with_options_async(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
        await_durable: bool | None = None,
    ) -> None:
        """
        Async variant of ``merge_with_options``.

        Args:
            key: Non-empty key.
            value: Value bytes to merge.
            ttl: Optional logical TTL units.
            await_durable: If False, do not wait for durable write.

        Examples:
            >>> await db.merge_with_options_async(b"k", b"v", ttl=10_000)
        """
        ...

    def write(self, batch: WriteBatch) -> None:
        """
        Atomically apply a batch of writes (puts/deletes/merges).

        Args:
            batch: A WriteBatch with queued operations.

        Example:
            >>> wb = WriteBatch()
            >>> wb.put(b"k1", b"v1")
            >>> wb.delete(b"k2")
            >>> db.write(wb)
        """
        ...

    def write_with_options(self, batch: WriteBatch, *, await_durable: bool | None = None) -> None:
        """
        Atomically apply a batch with write options.

        Args:
            batch: A WriteBatch with queued operations.
            await_durable: If False, do not wait for durable write.

        Examples:
            >>> wb = WriteBatch()
            >>> wb.put(b"k", b"v")
            >>> db.write_with_options(wb, await_durable=False)
        """
        ...

    async def write_async(self, batch: WriteBatch) -> None:
        """
        Async variant of ``write``.

        Args:
            batch: A WriteBatch with queued operations.

        Examples:
            >>> wb = WriteBatch(); wb.put(b"k", b"v")
            >>> await db.write_async(wb)
        """
        ...

    async def write_with_options_async(self, batch: WriteBatch, *, await_durable: bool | None = None) -> None:
        """
        Async variant of ``write_with_options``.

        Args:
            batch: A WriteBatch with queued operations.
            await_durable: If False, do not wait for durable write.

        Examples:
            >>> wb = WriteBatch(); wb.put(b"k", b"v")
            >>> await db.write_with_options_async(wb, await_durable=False)
        """
        ...

    def flush(self) -> None:
        """
        Flush in-memory writes to durable storage.

        Examples:
            >>> db.flush()
        """
        ...

    async def flush_async(self) -> None:
        """
        Async variant of ``flush``.

        Examples:
            >>> await db.flush_async()
        """
        ...

    def flush_with_options(self, flush_type: Literal["wal", "memtable"]) -> None:
        """
        Flush with explicit type.

        Args:
            flush_type: "wal" to flush write-ahead log or "memtable" to flush in-memory table.

        Examples:
            >>> db.flush_with_options("wal")
        """
        ...

    async def flush_with_options_async(self, flush_type: Literal["wal", "memtable"]) -> None:
        """
        Async variant of ``flush_with_options``.

        Args:
            flush_type: "wal" or "memtable".

        Examples:
            >>> await db.flush_with_options_async("memtable")
        """
        ...

    def metrics(self) -> dict[str, int]:
        """
        Return current SlateDB metrics as a dictionary.

        Returns:
            A mapping of metric name to value. Keys are metric names (for example,
            ``"db/get_requests"``) and values are integers.

        Examples:
            >>> m = db.metrics()
            >>> isinstance(m, dict)
            True
        """
        ...

    def create_checkpoint(
        self,
        scope: Literal["all", "durable"] = "all",
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult:
        """
        Create a writer checkpoint that includes data per the requested scope.

        Args:
            scope: "all" to include recent writes, "durable" to include only durable data.
            lifetime: Optional lifetime in milliseconds.
            source: Optional existing checkpoint UUID to base from.

        Returns:
            Dict with ``id`` (UUID string) and ``manifest_id`` (int).

        Examples:
            >>> res = db.create_checkpoint()
            >>> list(res.keys()) == ["id", "manifest_id"]
            True
        """
        ...

    async def create_checkpoint_async(
        self,
        scope: Literal["all", "durable"] = "all",
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult:
        """
        Async variant of ``create_checkpoint``.

        Args:
            scope: "all" or "durable".
            lifetime: Optional lifetime in milliseconds.
            source: Optional checkpoint id to base from.

        Returns:
            A mapping with ``id`` and ``manifest_id``.

        Examples:
            >>> res = await db.create_checkpoint_async()
            >>> "id" in res and "manifest_id" in res
            True
        """
        ...

    def close(self) -> None:
        """
        Close the database and release resources.

        Examples:
            >>> db.close()
        """
        ...

    async def close_async(self) -> None:
        """
        Async variant of ``close``.

        Examples:
            >>> await db.close_async()
        """
        ...


class SlateDBSnapshot:
    """Read-only view of a consistent point in time."""

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value from the snapshot by key.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if the key does not exist at the snapshot.

        Examples:
            >>> snap = db.snapshot()
            >>> _ = snap.get(b"k")
        """
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """
        Async variant of ``get``.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None``.

        Examples:
            >>> snap = await db.snapshot_async()
            >>> await snap.get_async(b"k")
        """
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Iterate a range within the snapshot.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive). ``None`` derives a prefix bound.

        Returns:
            DbIterator over ``(key, value)`` pairs at the snapshot.

        Examples:
            >>> it = snap.scan(b"a")
            >>> next(it)  # doctest: +SKIP
        """
        ...

    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Async variant of ``scan``.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).

        Returns:
            DbIterator for async iteration at the snapshot.

        Examples:
            >>> it = await snap.scan_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

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
    ) -> DbIterator:
        """
        Iterate a range with advanced options within the snapshot.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Ignored for snapshots (reads are committed-only).
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range.

        Examples:
            >>> it = snap.scan_with_options(b"a", cache_blocks=True)
            >>> next(it)  # doctest: +SKIP
        """
        ...

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
    ) -> DbIterator:
        """
        Async variant of ``scan_with_options``.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Ignored for snapshots.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range for async iteration.

        Examples:
            >>> it = await snap.scan_with_options_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

    def close(self) -> None:
        """
        Close the snapshot handle.

        Examples:
            >>> snap.close()
        """
        ...

    async def close_async(self) -> None:
        """
        Async variant of ``close``.

        Examples:
            >>> await snap.close_async()
        """
        ...


class SlateDBTransaction:
    """Read/write transaction handle."""

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value within the transaction (reads see prior writes).

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Examples:
            >>> txn = db.begin()
            >>> txn.get(b"k") is None or True
            True
        """
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """
        Async variant of ``get`` within the transaction.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None``.

        Examples:
            >>> txn = await db.begin_async()
            >>> await txn.get_async(b"k")
        """
        ...

    def get_with_options(
        self,
        key: bytes,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
    ) -> bytes | None:
        """
        Get a value with read options within the transaction.

        Args:
            key: Non-empty key.
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include uncommitted/dirty data if True.

        Returns:
            Value bytes or ``None``.

        Examples:
            >>> txn = db.begin()
            >>> txn.get_with_options(b"k", dirty=True)
        """
        ...

    async def get_with_options_async(
        self,
        key: bytes,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
    ) -> bytes | None:
        """
        Async variant of ``get_with_options`` within the transaction.

        Args:
            key: Non-empty key.
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include uncommitted/dirty data if True.

        Returns:
            Value bytes or ``None``.

        Examples:
            >>> txn = await db.begin_async()
            >>> await txn.get_with_options_async(b"k", dirty=True)
        """
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Iterate a range within the transaction.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).

        Returns:
            DbIterator over ``(key, value)`` pairs, reflecting in-transaction writes.

        Examples:
            >>> txn = db.begin()
            >>> for k, v in txn.scan(b"a"):
            ...     _ = (k, v)
        """
        ...

    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Async variant of ``scan`` returning a DbIterator.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).

        Returns:
            DbIterator for async iteration reflecting in-transaction writes.

        Examples:
            >>> txn = await db.begin_async()
            >>> it = await txn.scan_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

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
    ) -> DbIterator:
        """
        Iterate a range with advanced options within the transaction.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include unflushed data if ``True``.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range.

        Examples:
            >>> txn = db.begin()
            >>> it = txn.scan_with_options(b"a", read_ahead_bytes=1_000)
            >>> next(it)  # doctest: +SKIP
        """
        ...

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
    ) -> DbIterator:
        """
        Async variant of ``scan_with_options`` returning a DbIterator.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include unflushed data if ``True``.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range for async iteration.

        Examples:
            >>> txn = await db.begin_async()
            >>> it = await txn.scan_with_options_async(b"a", cache_blocks=True)
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

    def put(self, key: bytes, value: bytes) -> None:
        """
        Buffer a put in the transaction.

        Args:
            key: Non-empty key.
            value: Value bytes.

        Examples:
            >>> txn = db.begin(); txn.put(b"k", b"v")
        """
        ...

    def put_with_options(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
    ) -> None:
        """
        Buffer a put with per-op options.

        Args:
            key: Non-empty key.
            value: Value bytes.
            ttl: Optional logical TTL units for this write.

        Examples:
            >>> txn = db.begin(); txn.put_with_options(b"k", b"v", ttl=60_000)
        """
        ...

    def delete(self, key: bytes) -> None:
        """
        Buffer a delete in the transaction.

        Args:
            key: Non-empty key.

        Examples:
            >>> txn = db.begin(); txn.delete(b"k")
        """
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """
        Buffer a merge in the transaction (requires merge operator).

        Args:
            key: Non-empty key.
            value: Value bytes to merge.

        Examples:
            >>> txn = db.begin(); txn.merge(b"k", b"v")
        """
        ...

    def merge_with_options(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
    ) -> None:
        """
        Buffer a merge with per-op options (e.g., TTL).

        Args:
            key: Non-empty key.
            value: Value bytes to merge.
            ttl: Optional logical TTL units.

        Examples:
            >>> txn = db.begin(); txn.merge_with_options(b"k", b"v", ttl=10_000)
        """
        ...

    def commit(self) -> None:
        """
        Commit buffered writes atomically.

        Examples:
            >>> txn = db.begin(); txn.put(b"k", b"v"); txn.commit()
        """
        ...

    async def commit_async(self) -> None:
        """
        Async variant of ``commit``.

        Examples:
            >>> txn = await db.begin_async()
            >>> await txn.put_with_options(b"k", b"v")  # doctest: +SKIP
            >>> await txn.commit_async()  # doctest: +SKIP
        """
        ...

    def rollback(self) -> None:
        """
        Discard buffered writes without committing.

        Examples:
            >>> txn = db.begin(); txn.put(b"k", b"v"); txn.rollback()
        """
        ...


class SlateDBReader:
    def __init__(
        self,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        checkpoint_id: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
        manifest_poll_interval: int | None = None,
        checkpoint_lifetime: int | None = None,
        max_memtable_bytes: int | None = None,
    ) -> None:
        """
        Create a read-only reader.

        Args:
            path: Database path.
            url: Optional object store URL.
            env_file: Optional env file for object store config.
            checkpoint_id: Optional checkpoint UUID string to read at.
            merge_operator: Optional merge operator for reads.
            manifest_poll_interval: Optional poll interval in milliseconds to refresh manifests and replay new WALs when no explicit checkpoint is supplied. Must be <= checkpoint_lifetime/2.
            checkpoint_lifetime: Optional checkpoint lifetime in milliseconds for implicit reader checkpoints. Must be >= 1000 and > 2x manifest_poll_interval.
            max_memtable_bytes: Optional maximum size in bytes of the internal immutable memtable used when replaying WALs.

        Examples:
            >>> reader = SlateDBReader("/tmp/mydb", env_file=".env")
        """
        ...

    @classmethod
    async def open_async(
        cls,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        checkpoint_id: str | None = None,
        *,
        merge_operator: Callable[[bytes | None, bytes], bytes] | None = None,
        manifest_poll_interval: int | None = None,
        checkpoint_lifetime: int | None = None,
        max_memtable_bytes: int | None = None,
    ) -> SlateDBReader:
        """
        Async constructor for a read-only reader.

        Args:
            path: Database path.
            url: Optional object store URL.
            env_file: Optional env file for object store config.
            checkpoint_id: Optional checkpoint UUID to read at.
            merge_operator: Optional merge operator for reads.
            manifest_poll_interval: Optional manifest poll interval (ms).
            checkpoint_lifetime: Optional checkpoint lifetime (ms).
            max_memtable_bytes: Optional max size of internal immutable memtable when replaying WALs.

        Returns:
            A SlateDBReader instance.

        Examples:
            >>> reader = await SlateDBReader.open_async("/tmp/mydb", env_file=".env")
        """
        ...

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value by key from the reader.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Examples:
            >>> reader.get(b"k") is None or True
            True
        """
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """
        Async variant of ``get``.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Examples:
            >>> await reader.get_async(b"k")
        """
        ...

    def get_with_options(
        self,
        key: bytes,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
    ) -> bytes | None:
        """
        Get a value by key with read options.

        Args:
            key: Non-empty key.
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include uncommitted/dirty data if True (ignored by readers; reads are committed-only).

        Returns:
            Value bytes or ``None`` if not found.

        Example:
            >>> reader.get_with_options(b"k", durability_filter="memory")
        """
        ...

    async def get_with_options_async(
        self,
        key: bytes,
        *,
        durability_filter: Literal["remote", "memory"] | None = None,
        dirty: bool | None = None,
    ) -> bytes | None:
        """
        Async variant of ``get_with_options``.

        Args:
            key: Non-empty key.
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Include uncommitted/dirty data if True (ignored by readers).

        Returns:
            Value bytes or ``None`` if not found.

        Examples:
            >>> await reader.get_with_options_async(b"k", durability_filter="remote")
        """
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Iterate a range using the reader.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive). If ``None``, a bound is derived to iterate keys sharing the ``start`` prefix.

        Returns:
            DbIterator over ``(key, value)`` pairs.

        Examples:
            >>> it = reader.scan(b"a")
            >>> next(it)  # doctest: +SKIP
        """
        ...

    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """
        Async variant of ``scan``.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).

        Returns:
            DbIterator for async iteration.

        Examples:
            >>> it = await reader.scan_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

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
    ) -> DbIterator:
        """
        Iterate a range with advanced options using the reader.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Ignored by readers; reads are committed-only.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range.

        Examples:
            >>> it = reader.scan_with_options(b"a", read_ahead_bytes=1_000)
            >>> next(it)  # doctest: +SKIP
        """
        ...

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
    ) -> DbIterator:
        """
        Async variant of ``scan_with_options``.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources ("remote" or "memory").
            dirty: Ignored by readers.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            DbIterator over the requested range for async iteration.

        Examples:
            >>> it = await reader.scan_with_options_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

    def close(self) -> None:
        """
        Close the reader and release resources.

        Examples:
            >>> reader.close()
        """
        ...

    async def close_async(self) -> None:
        """
        Async variant of ``close``.

        Examples:
            >>> await reader.close_async()
        """
        ...


class DbIterator(Iterator[tuple[bytes, bytes]], AsyncIterator[tuple[bytes, bytes]]):
    """Iterator over ``(key, value)`` tuples returned by scans."""

    def __iter__(self) -> DbIterator:
        """
        Return ``self`` to support iteration in ``for`` loops.

        Returns:
            The iterator itself.

        Examples:
            >>> it = db.scan(b"a")
            >>> iter(it) is it
            True
        """
        ...

    def __next__(self) -> tuple[bytes, bytes]:
        """
        Return the next ``(key, value)`` pair.

        Returns:
            Tuple of ``(key, value)`` bytes.

        Examples:
            >>> it = db.scan(b"a")
            >>> isinstance(next(it), tuple)
            True
        """
        ...

    def __aiter__(self) -> DbIterator:
        """
        Return ``self`` to support ``async for`` loops.

        Returns:
            The iterator itself.

        Examples:
            >>> it = await db.scan_async(b"a")
            >>> aiter = it.__aiter__()
            >>> aiter is it
            True
        """
        ...

    async def __anext__(self) -> tuple[bytes, bytes]:
        """
        Await and return the next ``(key, value)`` pair.

        Returns:
            Tuple of ``(key, value)`` bytes.

        Examples:
            >>> it = await db.scan_async(b"a")
            >>> async for k, v in it:
            ...     _ = (k, v)
        """
        ...

    def seek(self, key: bytes) -> None:
        """
        Seek forward to the first key ``>= key`` within the original range.

        Args:
            key: Non-empty key to seek to.

        Examples:
            >>> it = db.scan(b"a")
            >>> it.seek(b"b")
        """
        ...

    async def seek_async(self, key: bytes) -> None:
        """
        Async variant of ``seek``.

        Args:
            key: Non-empty key to seek to.

        Examples:
            >>> it = await db.scan_async(b"a")
            >>> await it.seek_async(b"b")
        """
        ...


class Checkpoint(TypedDict):
    """Checkpoint metadata returned by the admin API."""

    id: str
    manifest_id: int
    expire_time: int | None
    create_time: int


class CheckpointCreateResult(TypedDict):
    """Result from ``create_checkpoint`` containing the checkpoint id and manifest id."""

    id: str
    manifest_id: int


class WriteBatch:
    """Accumulates atomic write operations for ``SlateDB.write(...)``."""

    def __init__(self) -> None:
        """
        Create an empty write batch.

        Examples:
            >>> wb = WriteBatch()
            >>> isinstance(wb, WriteBatch)
            True
        """
        ...

    def put(self, key: bytes, value: bytes) -> None:
        """
        Queue a put operation.

        Args:
            key: Non-empty key.
            value: Value bytes.

        Examples:
            >>> wb = WriteBatch(); wb.put(b"k", b"v")
        """
        ...

    def put_with_options(self, key: bytes, value: bytes, *, ttl: int | None = None) -> None:
        """
        Queue a put with per-op options (e.g., TTL).

        Args:
            key: Non-empty key.
            value: Value bytes.
            ttl: Optional logical TTL units for this write.

        Examples:
            >>> wb = WriteBatch(); wb.put_with_options(b"k", b"v", ttl=1000)
        """
        ...

    def delete(self, key: bytes) -> None:
        """
        Queue a delete operation.

        Args:
            key: Non-empty key.

        Examples:
            >>> wb = WriteBatch(); wb.delete(b"k")
        """
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """
        Queue a merge operation.

        Args:
            key: Non-empty key.
            value: Value bytes to merge.

        Examples:
            >>> wb = WriteBatch(); wb.merge(b"k", b"v")
        """
        ...

    def merge_with_options(self, key: bytes, value: bytes, *, ttl: int | None = None) -> None:
        """
        Queue a merge with per-op options (e.g., TTL).

        Args:
            key: Non-empty key.
            value: Value bytes to merge.
            ttl: Optional logical TTL units for this write.

        Examples:
            >>> wb = WriteBatch(); wb.merge_with_options(b"k", b"v", ttl=10_000)
        """
        ...

class SlateDBAdmin:
    """Administrative interface for managing manifests, checkpoints, and GC."""

    def __init__(
        self,
        path: str,
        url: str | None = None,
        env_file: str | None = None,
        *,
        wal_url: str | None = None,
    ) -> None:
        """
        Create an admin handle for a database path/object store.

        Args:
            path: Database path.
            url: Optional object store URL.
            env_file: Optional env file for object store config.
            wal_url: Optional WAL object store URL override.

        Examples:
            >>> admin = SlateDBAdmin("/tmp/mydb", env_file=".env")
        """
        ...

    def read_manifest(self, id: int | None = None) -> str | None:
        """
        Read the latest or a specific manifest as a JSON string.

        Args:
            id: Optional manifest id to read. If ``None``, reads the latest.

        Returns:
            JSON string of the manifest, or ``None`` if no manifests exist.

        Examples:
            >>> s = admin.read_manifest()
            >>> s is None or s.startswith("{")
            True
        """
        ...

    async def read_manifest_async(self, id: int | None = None) -> str | None:
        """
        Async variant of ``read_manifest``.

        Args:
            id: Optional manifest id to read.

        Returns:
            JSON string of the manifest, or ``None``.

        Examples:
            >>> await admin.read_manifest_async()
        """
        ...

    def list_manifests(self, start: int | None = None, end: int | None = None) -> str:
        """
        List manifests within an optional [start, end) range as JSON.

        Args:
            start: Optional inclusive start id.
            end: Optional exclusive end id.

        Returns:
            JSON string containing a list of manifest metadata.

        Examples:
            >>> s = admin.list_manifests()
            >>> s.startswith("[")
            True
        """
        ...

    async def list_manifests_async(self, start: int | None = None, end: int | None = None) -> str:
        """
        Async variant of ``list_manifests``.

        Args:
            start: Optional inclusive start id.
            end: Optional exclusive end id.

        Returns:
            JSON string containing a list of manifest metadata.

        Examples:
            >>> s = await admin.list_manifests_async()
            >>> s.startswith("[")
            True
        """
        ...

    def create_checkpoint(
        self,
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult:
        """
        Create a detached checkpoint.

        Args:
            lifetime: Optional checkpoint lifetime in milliseconds.
            source: Optional source checkpoint UUID string to extend/refresh.

        Returns:
            Dict with ``id`` (UUID string) and ``manifest_id`` (int).

        Example:
            >>> admin = SlateDBAdmin("/tmp/mydb", env_file=".env")
            >>> res = admin.create_checkpoint()
            >>> list(res.keys()) == ["id", "manifest_id"]
            True
        """
        ...

    async def create_checkpoint_async(
        self,
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult:
        """
        Async variant of ``create_checkpoint``.

        Args:
            lifetime: Optional checkpoint lifetime in milliseconds.
            source: Optional source checkpoint UUID string to extend/refresh.

        Returns:
            Dict with ``id`` (UUID string) and ``manifest_id`` (int).

        Examples:
            >>> await admin.create_checkpoint_async()  # doctest: +SKIP
        """
        ...

    def list_checkpoints(self) -> list[Checkpoint]:
        """
        List known checkpoints for the database path/object store.

        Returns:
            A list of checkpoint metadata dicts.

        Examples:
            >>> cps = admin.list_checkpoints()
            >>> isinstance(cps, list)
            True
        """
        ...

    async def list_checkpoints_async(self) -> list[Checkpoint]:
        """
        Async variant of ``list_checkpoints``.

        Returns:
            A list of checkpoint metadata dicts.

        Examples:
            >>> await admin.list_checkpoints_async()
        """
        ...

    def refresh_checkpoint(self, id: str, lifetime: int | None = None) -> None:
        """
        Refresh a checkpoint's lifetime.

        Args:
            id: Checkpoint UUID string.
            lifetime: New lifetime in milliseconds.

        Examples:
            >>> admin.refresh_checkpoint("00000000-0000-0000-0000-000000000000", lifetime=60000)  # doctest: +SKIP
        """
        ...

    async def refresh_checkpoint_async(self, id: str, lifetime: int | None = None) -> None:
        """
        Async variant of ``refresh_checkpoint``.

        Args:
            id: Checkpoint UUID string.
            lifetime: New lifetime in milliseconds.

        Examples:
            >>> await admin.refresh_checkpoint_async("00000000-0000-0000-0000-000000000000", lifetime=60000)  # doctest: +SKIP
        """
        ...

    def delete_checkpoint(self, id: str) -> None:
        """
        Delete a checkpoint by id.

        Args:
            id: Checkpoint UUID string.

        Examples:
            >>> admin.delete_checkpoint("00000000-0000-0000-0000-000000000000")  # doctest: +SKIP
        """
        ...

    async def delete_checkpoint_async(self, id: str) -> None:
        """
        Async variant of ``delete_checkpoint``.

        Args:
            id: Checkpoint UUID string.

        Examples:
            >>> await admin.delete_checkpoint_async("00000000-0000-0000-0000-000000000000")  # doctest: +SKIP
        """
        ...

    def get_timestamp_for_sequence(self, seq: int, *, round_up: bool = False) -> int | None:
        """
        Return timestamp millis for a sequence, or ``None`` if unknown.

        Args:
            seq: Sequence number.
            round_up: If True, round up to next known timestamp when exact is missing.

        Returns:
            Milliseconds since epoch, or ``None``.

        Examples:
            >>> ts = admin.get_timestamp_for_sequence(42)
            >>> ts is None or isinstance(ts, int)
            True
        """
        ...

    async def get_timestamp_for_sequence_async(self, seq: int, *, round_up: bool = False) -> int | None:
        """
        Async variant of ``get_timestamp_for_sequence``.

        Args:
            seq: Sequence number.
            round_up: If True, round up to next known timestamp when exact is missing.

        Returns:
            Milliseconds since epoch, or ``None``.

        Examples:
            >>> await admin.get_timestamp_for_sequence_async(42)
        """
        ...

    def get_sequence_for_timestamp(self, ts_millis: int, *, round_up: bool = False) -> int | None:
        """
        Return sequence for timestamp millis, or ``None`` if unknown.

        Args:
            ts_millis: Milliseconds since epoch.
            round_up: If True, round up to next known sequence when exact is missing.

        Returns:
            Sequence number, or ``None``.

        Examples:
            >>> seq = admin.get_sequence_for_timestamp(1_700_000_000_000)
            >>> seq is None or isinstance(seq, int)
            True
        """
        ...

    async def get_sequence_for_timestamp_async(self, ts_millis: int, *, round_up: bool = False) -> int | None:
        """
        Async variant of ``get_sequence_for_timestamp``.

        Args:
            ts_millis: Milliseconds since epoch.
            round_up: If True, round up to next known sequence when exact is missing.

        Returns:
            Sequence number, or ``None``.

        Examples:
            >>> await admin.get_sequence_for_timestamp_async(1_700_000_000_000)
        """
        ...

    def create_clone(self, parent_path: str, parent_checkpoint: str | None = None) -> None:
        """
        Create a clone DB at this admin's path from a parent path/checkpoint.

        Args:
            parent_path: Parent database path to clone from.
            parent_checkpoint: Optional checkpoint id to clone from; latest if ``None``.

        Examples:
            >>> admin.create_clone("/tmp/parent", parent_checkpoint=None)  # doctest: +SKIP
        """
        ...

    async def create_clone_async(self, parent_path: str, parent_checkpoint: str | None = None) -> None:
        """
        Async variant of ``create_clone``.

        Args:
            parent_path: Parent database path to clone from.
            parent_checkpoint: Optional checkpoint id to clone from.

        Examples:
            >>> await admin.create_clone_async("/tmp/parent")  # doctest: +SKIP
        """
        ...

    def run_gc_once(
        self,
        *,
        manifest_interval: int | None = None,
        manifest_min_age: int | None = None,
        wal_interval: int | None = None,
        wal_min_age: int | None = None,
        compacted_interval: int | None = None,
        compacted_min_age: int | None = None,
    ) -> None:
        """
        Run a single garbage-collection pass.

        Args:
            manifest_interval: Minimum interval between manifest deletions (ms).
            manifest_min_age: Minimum age before manifests are GC'd (ms).
            wal_interval: Minimum interval between WAL deletions (ms).
            wal_min_age: Minimum age before WALs are GC'd (ms).
            compacted_interval: Minimum interval for compacted file deletions (ms).
            compacted_min_age: Minimum age before compacted files are GC'd (ms).

        Examples:
            >>> admin.run_gc_once(manifest_min_age=3_600_000)  # doctest: +SKIP
        """
        ...

    async def run_gc_once_async(
        self,
        *,
        manifest_interval: int | None = None,
        manifest_min_age: int | None = None,
        wal_interval: int | None = None,
        wal_min_age: int | None = None,
        compacted_interval: int | None = None,
        compacted_min_age: int | None = None,
    ) -> None:
        """
        Async variant of ``run_gc_once``.

        Args:
            manifest_interval: Minimum interval between manifest deletions (ms).
            manifest_min_age: Minimum age before manifests are GC'd (ms).
            wal_interval: Minimum interval between WAL deletions (ms).
            wal_min_age: Minimum age before WALs are GC'd (ms).
            compacted_interval: Minimum interval for compacted file deletions (ms).
            compacted_min_age: Minimum age before compacted files are GC'd (ms).

        Examples:
            >>> await admin.run_gc_once_async(manifest_min_age=3_600_000)  # doctest: +SKIP
        """
        ...

    def run_gc(
        self,
        *,
        manifest_interval: int | None = None,
        manifest_min_age: int | None = None,
        wal_interval: int | None = None,
        wal_min_age: int | None = None,
        compacted_interval: int | None = None,
        compacted_min_age: int | None = None,
    ) -> None:
        """
        Continuously run garbage-collection with the provided thresholds.

        Args:
            manifest_interval: Minimum interval between manifest deletions (ms).
            manifest_min_age: Minimum age before manifests are GC'd (ms).
            wal_interval: Minimum interval between WAL deletions (ms).
            wal_min_age: Minimum age before WALs are GC'd (ms).
            compacted_interval: Minimum interval for compacted file deletions (ms).
            compacted_min_age: Minimum age before compacted files are GC'd (ms).

        Examples:
            >>> # Blocks until cancelled; run in a background task
            >>> # admin.run_gc(manifest_min_age=3_600_000)  # doctest: +SKIP
        """
        ...

    async def run_gc_async(
        self,
        *,
        manifest_interval: int | None = None,
        manifest_min_age: int | None = None,
        wal_interval: int | None = None,
        wal_min_age: int | None = None,
        compacted_interval: int | None = None,
        compacted_min_age: int | None = None,
    ) -> None:
        """
        Async variant of ``run_gc``.

        Args:
            manifest_interval: Minimum interval between manifest deletions (ms).
            manifest_min_age: Minimum age before manifests are GC'd (ms).
            wal_interval: Minimum interval between WAL deletions (ms).
            wal_min_age: Minimum age before WALs are GC'd (ms).
            compacted_interval: Minimum interval for compacted file deletions (ms).
            compacted_min_age: Minimum age before compacted files are GC'd (ms).

        Examples:
            >>> # Typically run in an asyncio Task
            >>> # await admin.run_gc_async(manifest_min_age=3_600_000)  # doctest: +SKIP
        """
        ...
