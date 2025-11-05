"""
Typing stubs for the ``slatedb`` module.

These stubs describe the Python API exposed by the Rust extension and are
used by type checkers and IDEs for autocompletion and linting.
"""

from typing import Optional, List, Tuple, TypedDict, Iterator, AsyncIterator, Literal, Callable


class TransactionError(Exception): ...
class ClosedError(Exception): ...
class UnavailableError(Exception): ...
class InvalidError(Exception): ...
class DataError(Exception): ...
class InternalError(Exception): ...


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
        url: Optional[str] = None,
        env_file: Optional[str] = None,
        *,
        merge_operator: Optional[Callable[[Optional[bytes], bytes], bytes]] = None,
        settings: Optional[str] = None,
    ) -> None:
        """
        Create or open a SlateDB database.

        Args:
            path: Local path (or logical DB name) for the database.
            url: Optional object store URL (e.g. ``"s3://bucket/prefix"``,
                ``"memory:///"``). If omitted, object store is resolved from
                ``env_file`` when provided, otherwise in-memory.
            env_file: Optional path to a file containing environment variables
                to configure the object store.
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

            >>> def last_write_wins(existing: Optional[bytes], value: bytes) -> bytes:
            ...     return value
            >>> db = SlateDB("/tmp/mydb", merge_operator=last_write_wins)
        """
        ...

    @classmethod
    async def open_async(
        cls,
        path: str,
        url: Optional[str] = None,
        env_file: Optional[str] = None,
        *,
        merge_operator: Optional[Callable[[Optional[bytes], bytes], bytes]] = None,
        settings: Optional[str] = None,
    ) -> "SlateDB":
        """
        Async constructor that opens the database and returns a ``SlateDB``.

        Args:
            path: Database path.
            url: Optional object store URL.
            env_file: Optional env file for object store config.
            merge_operator: Optional merge function.
            settings: Optional path to settings TOML file.

        Returns:
            An initialized ``SlateDB`` instance.

        Raises:
            InvalidError | UnavailableError | DataError | InternalError

        Examples:
            >>> db = await SlateDB.open_async("/tmp/mydb", env_file=".env")
        """
        ...

    def snapshot(self) -> "SlateDBSnapshot":
        """
        Create a read-only snapshot at the current committed state.

        Returns:
            ``SlateDBSnapshot`` providing a consistent, read-only view.

        Raises:
            UnavailableError | InternalError

        Example:
            >>> snap = db.snapshot()
            >>> snap.get(b"k")
        """
        ...
    async def snapshot_async(self) -> "SlateDBSnapshot":
        """
        Async variant of ``snapshot``.

        Example:
            >>> snap = await db.snapshot_async()
        """
        ...

    def begin(self, isolation: Literal["si", "ssi", "snapshot", "serializable", "serializable_snapshot"] | None = None) -> "SlateDBTransaction":
        """
        Begin a transaction.

        Args:
            isolation: Isolation level. Aliases:
                - ``"si"`` or ``"snapshot"`` for Snapshot Isolation
                - ``"ssi"``, ``"serializable"``, or ``"serializable_snapshot"`` for Serializable Snapshot Isolation

        Returns:
            ``SlateDBTransaction`` handle.

        Raises:
            InvalidError: Invalid isolation string.
            ClosedError | UnavailableError | InternalError

        Example:
            >>> txn = db.begin("ssi")
            >>> txn.put(b"k", b"v")
            >>> txn.commit()
        """
        ...
    async def begin_async(self, isolation: Literal["si", "ssi", "snapshot", "serializable", "serializable_snapshot"] | None = None) -> "SlateDBTransaction":
        """Async variant of ``begin``.

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
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Get a value by key.

        Args:
            key: Non-empty key.

        Returns:
            Value bytes or ``None`` if not found.

        Raises:
            InvalidError | TransactionError | ClosedError | UnavailableError | DataError | InternalError
        """
        ...
    async def get_async(self, key: bytes) -> Optional[bytes]:
        """Async variant of ``get``."""
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

    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """
        Iterate over a key range.

        Args:
            start: Start key (inclusive, non-empty).
            end: Optional end key (exclusive). If ``None``, a bound is derived
                to iterate keys sharing the ``start`` prefix.

        Returns:
            ``DbIterator`` yielding ``(key, value)`` sorted by key.

        Example:
            >>> for k, v in db.scan(b"a", b"z"):
            ...     pass
        """
        ...
    async def scan_async(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """
        Async variant of ``scan``. Returns an async-capable iterator.

        Example:
            >>> it = await db.scan_async(b"a")
            >>> async for k, v in it:
            ...     pass
        """
        ...
    def scan_with_options(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
        """
        Iterate with advanced scan options.

        Args:
            start: Start key (inclusive).
            end: Optional end key (exclusive).
            durability_filter: Restrict sources (``"remote"`` or ``"memory"``).
            dirty: Include unflushed data if ``True``.
            read_ahead_bytes: Read-ahead size hint.
            cache_blocks: Cache blocks during iteration if ``True``.
            max_fetch_tasks: Limit background fetch task count.

        Returns:
            ``DbIterator`` over the requested range.
        """
        ...
    async def scan_with_options_async(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
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
    def close(self) -> None:
        """Close the database and release resources."""
        ...
    async def close_async(self) -> None:
        """Async variant of ``close``."""
        ...


class SlateDBSnapshot:
    """Read-only view of a consistent point in time."""

    def get(self, key: bytes) -> Optional[bytes]:
        """Get a value from the snapshot by key."""
        ...

    async def get_async(self, key: bytes) -> Optional[bytes]:
        """Async variant of ``get``."""
        ...

    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """Iterate a range within the snapshot."""
        ...

    async def scan_async(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """Async variant of ``scan``."""
        ...

    def scan_with_options(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
        """Iterate a range with advanced options within the snapshot."""
        ...
    async def scan_with_options_async(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
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

    def get(self, key: bytes) -> Optional[bytes]:
        """Get a value within the transaction (reads see prior writes)."""
        ...

    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """Iterate a range within the transaction."""
        ...

    def scan_with_options(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
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
        url: Optional[str] = None,
        env_file: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
        *,
        merge_operator: Optional[Callable[[Optional[bytes], bytes], bytes]] = None,
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
        url: Optional[str] = None,
        env_file: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
        *,
        merge_operator: Optional[Callable[[Optional[bytes], bytes], bytes]] = None,
    ) -> "SlateDBReader":
        """
        Async constructor for a read-only reader.

        Example:
            >>> reader = await SlateDBReader.open_async("/tmp/mydb", env_file=".env")
        """
        ...

    def get(self, key: bytes) -> Optional[bytes]:
        """Get a value by key from the reader."""
        ...
    async def get_async(self, key: bytes) -> Optional[bytes]:
        """Async variant of ``get``."""
        ...
    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """Iterate a range using the reader."""
        ...
    async def scan_async(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """Async variant of ``scan``."""
        ...
    def scan_with_options(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
        """Iterate a range with advanced options using the reader."""
        ...
    async def scan_with_options_async(
        self,
        start: bytes,
        end: Optional[bytes] = None,
        *,
        durability_filter: Optional[Literal["remote", "memory"]] = None,
        dirty: Optional[bool] = None,
        read_ahead_bytes: Optional[int] = None,
        cache_blocks: Optional[bool] = None,
        max_fetch_tasks: Optional[int] = None,
    ) -> "DbIterator":
        """Async variant of ``scan_with_options``."""
        ...
    def close(self) -> None:
        """Close the reader and release resources."""
        ...
    async def close_async(self) -> None:
        """Async variant of ``close``."""
        ...


class DbIterator(Iterator[Tuple[bytes, bytes]], AsyncIterator[Tuple[bytes, bytes]]):
    """Iterator over ``(key, value)`` tuples returned by scans."""

    def __iter__(self) -> "DbIterator":
        """Return ``self`` to support iteration in ``for`` loops."""
        ...

    def __next__(self) -> Tuple[bytes, bytes]:
        """
        Return the next ``(key, value)`` pair.

        Raises:
            StopIteration: When the iterator is exhausted.
        """
        ...

    def __aiter__(self) -> "DbIterator":
        """Return ``self`` to support ``async for`` loops."""
        ...

    async def __anext__(self) -> Tuple[bytes, bytes]:
        """
        Await and return the next ``(key, value)`` pair.

        Raises:
            StopAsyncIteration: When the iterator is exhausted.
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
    expire_time: Optional[int]
    create_time: int


class CheckpointCreateResult(TypedDict):
    """Result from ``create_checkpoint`` containing the checkpoint id and manifest id."""

    id: str
    manifest_id: int


class SlateDBAdmin:
    """Administrative interface for managing checkpoints and metadata."""

    def __init__(self, path: str, url: Optional[str] = None, env_file: Optional[str] = None) -> None:
        """Create an admin handle for a database path/object store."""
        ...

    def create_checkpoint(self, *, lifetime: Optional[int] = None, source: Optional[str] = None) -> CheckpointCreateResult:
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
            >>> admin.create_checkpoint()
        """
        ...

    async def create_checkpoint_async(self, *, lifetime: Optional[int] = None, source: Optional[str] = None) -> CheckpointCreateResult:
        """Async variant of ``create_checkpoint``."""
        ...

    def list_checkpoints(self) -> List[Checkpoint]:
        """List known checkpoints for the database path/object store."""
        ...

    async def list_checkpoints_async(self) -> List[Checkpoint]:
        """Async variant of ``list_checkpoints``."""
        ...
