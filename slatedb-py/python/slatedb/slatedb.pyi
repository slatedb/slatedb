"""
Typing stubs for the ``slatedb`` module.

These stubs describe the Python API exposed by the Rust extension and are
used by type checkers and IDEs for autocompletion and linting.
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

        Raises:
            InvalidError: Invalid arguments or configuration.
            UnavailableError: Object store unavailable.
            DataError: Persisted data is invalid or incompatible.
            InternalError: Unexpected internal error.

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

        Raises:
            InvalidError | UnavailableError | DataError | InternalError

        Examples:
            >>> db = await SlateDB.open_async("/tmp/mydb", env_file=".env")
        """
        ...

    def snapshot(self) -> SlateDBSnapshot:
        """
        Create a read-only snapshot at the current committed state.

        Returns:
            SlateDBSnapshot providing a consistent, read-only view.

        Raises:
            UnavailableError | InternalError

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

        Example:
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

        Raises:
            InvalidError: Invalid isolation string.
            ClosedError | UnavailableError | InternalError

        Example:
            >>> txn = db.begin("ssi")
            >>> txn.put(b"k", b"v")
            >>> txn.commit()
        """
        ...

    async def begin_async(self, isolation: Isolation | None = None) -> SlateDBTransaction:
        """
        Async variant of ``begin``.

        Returns:
            A SlateDBTransaction instance.

        Example:
            >>> txn = await db.begin_async("si")
        """
        ...

    def put(self, key: bytes, value: bytes) -> None:
        """
        Store a key-value pair.

        Args:
            key: Non-empty key.
            value: Value bytes.

        Raises:
            InvalidError: Empty key.
            TransactionError | ClosedError | UnavailableError | DataError | InternalError

        Example:
            >>> db.put(b"k", b"v")
        """
        ...

    async def put_async(self, key: bytes, value: bytes) -> None:
        """Async variant of ``put``."""
        ...

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value by key.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Raises:
            InvalidError | TransactionError | ClosedError | UnavailableError | DataError | InternalError

        Example:
            >>> db.get(b"missing") is None
            True
        """
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """Async variant of ``get``."""
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

        Raises:
            InvalidError | TransactionError | ClosedError | UnavailableError | DataError | InternalError

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
        """Async variant of ``get_with_options``."""
        ...

    def delete(self, key: bytes) -> None:
        """
        Delete a key.

        Args:
            key: Non-empty key to remove.

        Raises:
            InvalidError | TransactionError | ClosedError | UnavailableError | DataError | InternalError
        """
        ...

    async def delete_async(self, key: bytes) -> None:
        """Async variant of ``delete``."""
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
        """Async variant of ``scan_with_options``."""
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """
        Merge a value using the configured merge operator.

        Requires that the DB was opened with a ``merge_operator``.
        """
        ...

    async def merge_async(self, key: bytes, value: bytes) -> None:
        """Async variant of ``merge``."""
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

        Raises:
            InvalidError | TransactionError | ClosedError | UnavailableError | DataError | InternalError

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
        """Async variant of ``put_with_options``."""
        ...

    async def delete_with_options_async(self, key: bytes, *, await_durable: bool | None = None) -> None:
        """Async variant of ``delete_with_options``."""
        ...

    async def merge_with_options_async(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: int | None = None,
        await_durable: bool | None = None,
    ) -> None:
        """Async variant of ``merge_with_options``."""
        ...

    def write(self, batch: WriteBatch) -> None:
        """
        Atomically apply a batch of writes (puts/deletes/merges).

        Args:
            batch: A WriteBatch with queued operations.

        Raises:
            TransactionError | ClosedError | UnavailableError | DataError | InternalError

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
        """
        ...

    async def write_async(self, batch: WriteBatch) -> None:
        """Async variant of ``write``."""
        ...

    async def write_with_options_async(self, batch: WriteBatch, *, await_durable: bool | None = None) -> None:
        """Async variant of ``write_with_options``."""
        ...

    def flush(self) -> None:
        """
        Flush in-memory writes to durable storage.
        """
        ...

    async def flush_async(self) -> None:
        """Async variant of ``flush``."""
        ...

    def flush_with_options(self, flush_type: Literal["wal", "memtable"]) -> None:
        """
        Flush with explicit type.

        Args:
            flush_type: "wal" to flush write-ahead log or "memtable" to flush in-memory table.
        """
        ...

    async def flush_with_options_async(self, flush_type: Literal["wal", "memtable"]) -> None:
        """Async variant of ``flush_with_options``."""
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
        """
        ...

    async def create_checkpoint_async(
        self,
        scope: Literal["all", "durable"] = "all",
        *,
        lifetime: int | None = None,
        source: str | None = None,
    ) -> CheckpointCreateResult:
        """Async variant of ``create_checkpoint``."""
        ...

    def close(self) -> None:
        """Close the database and release resources."""
        ...

    async def close_async(self) -> None:
        """Async variant of ``close``."""
        ...


class SlateDBSnapshot:
    """Read-only view of a consistent point in time."""

    def get(self, key: bytes) -> bytes | None:
        """Get a value from the snapshot by key."""
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """Async variant of ``get``."""
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """Iterate a range within the snapshot."""
        ...

    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """Async variant of ``scan``."""
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
        """Iterate a range with advanced options within the snapshot."""
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
        """Async variant of ``scan_with_options``."""
        ...

    def close(self) -> None:
        """Close the snapshot handle."""
        ...

    async def close_async(self) -> None:
        """Async variant of ``close``."""
        ...


class SlateDBTransaction:
    """Read/write transaction handle."""

    def get(self, key: bytes) -> bytes | None:
        """Get a value within the transaction (reads see prior writes)."""
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """Iterate a range within the transaction."""
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
        """Iterate a range with advanced options within the transaction."""
        ...

    def put(self, key: bytes, value: bytes) -> None:
        """Buffer a put in the transaction."""
        ...

    def delete(self, key: bytes) -> None:
        """Buffer a delete in the transaction."""
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """Buffer a merge in the transaction (requires merge operator)."""
        ...

    def commit(self) -> None:
        """Commit buffered writes atomically."""
        ...

    async def commit_async(self) -> None:
        """Async variant of ``commit``."""
        ...

    def rollback(self) -> None:
        """Discard buffered writes without committing."""
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
    ) -> None:
        """
        Create a read-only reader.

        Args:
            path: Database path.
            url: Optional object store URL.
            env_file: Optional env file for object store config.
            checkpoint_id: Optional checkpoint UUID string to read at.
            merge_operator: Optional merge operator for reads.
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
    ) -> SlateDBReader:
        """
        Async constructor for a read-only reader.

        Returns:
            A SlateDBReader instance.

        Example:
            >>> reader = await SlateDBReader.open_async("/tmp/mydb", env_file=".env")
        """
        ...

    def get(self, key: bytes) -> bytes | None:
        """Get a value by key from the reader."""
        ...

    async def get_async(self, key: bytes) -> bytes | None:
        """Async variant of ``get``."""
        ...

    def scan(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """Iterate a range using the reader."""
        ...

    async def scan_async(self, start: bytes, end: bytes | None = None) -> DbIterator:
        """Async variant of ``scan``."""
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
        """Iterate a range with advanced options using the reader."""
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
        """Async variant of ``scan_with_options``."""
        ...

    def close(self) -> None:
        """Close the reader and release resources."""
        ...

    async def close_async(self) -> None:
        """Async variant of ``close``."""
        ...


class DbIterator(Iterator[tuple[bytes, bytes]], AsyncIterator[tuple[bytes, bytes]]):
    """Iterator over ``(key, value)`` tuples returned by scans."""

    def __iter__(self) -> DbIterator:
        """Return ``self`` to support iteration in ``for`` loops."""
        ...

    def __next__(self) -> tuple[bytes, bytes]:
        """
        Return the next ``(key, value)`` pair.

        Raises:
            StopIteration: When the iterator is exhausted.

        Example:
            >>> it = db.scan(b"a")
            >>> next(it)
            (b"a1", b"v1")
        """
        ...

    def __aiter__(self) -> DbIterator:
        """Return ``self`` to support ``async for`` loops."""
        ...

    async def __anext__(self) -> tuple[bytes, bytes]:
        """
        Await and return the next ``(key, value)`` pair.

        Raises:
            StopAsyncIteration: When the iterator is exhausted.

        Example:
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
        """
        ...

    async def seek_async(self, key: bytes) -> None:
        """Async variant of ``seek``."""
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

    def __init__(self) -> None: ...

    def put(self, key: bytes, value: bytes) -> None:
        """Queue a put operation."""
        ...

    def put_with_options(self, key: bytes, value: bytes, *, ttl: int | None = None) -> None:
        """Queue a put with per-op options (e.g., TTL)."""
        ...

    def delete(self, key: bytes) -> None:
        """Queue a delete operation."""
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """Queue a merge operation."""
        ...

    def merge_with_options(self, key: bytes, value: bytes, *, ttl: int | None = None) -> None:
        """Queue a merge with per-op options (e.g., TTL)."""
        ...

class SlateDBAdmin:
    """Administrative interface for managing checkpoints and metadata."""

    def __init__(self, path: str, url: str | None = None, env_file: str | None = None) -> None:
        """Create an admin handle for a database path/object store."""
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

        Raises:
            InvalidError | UnavailableError | DataError | InternalError

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
        """Async variant of ``create_checkpoint``."""
        ...

    def list_checkpoints(self) -> list[Checkpoint]:
        """List known checkpoints for the database path/object store."""
        ...

    async def list_checkpoints_async(self) -> list[Checkpoint]:
        """Async variant of ``list_checkpoints``."""
        ...
