"""
Python stub file for slatedb module.

This module provides a Python interface to SlateDB, a key-value database built in Rust.
"""

from typing import Optional, List, Tuple, TypedDict, Iterator, Literal

class TransactionError(Exception):
    """Raised when a transaction conflict occurs (retry or drop the operation)."""
    ...

class ClosedError(Exception):
    """Raised when the database/reader is closed or fenced; create a new instance."""
    ...

class UnavailableError(Exception):
    """Raised when storage or network is unavailable; retry or drop the operation."""
    ...

class InvalidError(Exception):
    """Raised for invalid arguments, configuration, or method usage."""
    ...

class DataError(Exception):
    """Raised for on-disk/object-store data corruption or incompatible versions."""
    ...

class InternalError(Exception):
    """Raised for unexpected internal errors; consider reporting an issue."""
    ...

class SlateDB:
    """
    A Python interface to SlateDB, a key-value database.
    
    SlateDB is a high-performance key-value store that provides ACID transactions
    and is built with Rust for safety and performance.
    """
    
    def __init__(self, path: str, url: Optional[str] = None, env_file: Optional[str] = None, **kwargs) -> None:
        """
        Create a new SlateDB instance.

        Args:
            path: The path where the database will be stored
            url: Optional object store URL (e.g., "s3://bucket/prefix", "memory:///")
            env_file: Optional environment file for object store configuration

        Raises:
            InvalidError: If configuration or arguments are invalid
            UnavailableError: If the object store is unavailable
            DataError: If persisted data is invalid
            InternalError: For unexpected internal errors
        """
        ...
    
    def put(self, key: bytes, value: bytes) -> None:
        """
        Store a key-value pair in the database.
        
        Args:
            key: The key as bytes (cannot be empty)
            value: The value as bytes
            
        Raises:
            InvalidError: If the key is empty or arguments are invalid
            TransactionError | ClosedError | UnavailableError | DataError | InternalError: On DB errors
        """
        ...

    def snapshot(self) -> "SlateDBSnapshot":
        """
        Create a read-only snapshot of the database at the current committed state.

        Returns:
            A SlateDBSnapshot that provides a consistent, read-only view of data.

        Notes:
            The snapshot is independent from subsequent writes to the database. Call
            `close()` on the snapshot to release resources promptly, or let it be
            dropped by Python’s GC.
        """
        ...

    def begin(self, isolation: Literal["si", "ssi"] = "si") -> "SlateDBTransaction":
        """
        Begin a transaction.

        Args:
            isolation: Isolation level. "si" for Snapshot Isolation, "ssi" for Serializable Snapshot Isolation.

        Returns:
            A SlateDBTransaction handle for performing read/write operations and commit/rollback.
        """
        ...

    async def begin_async(self, isolation: Literal["si", "ssi"] = "si") -> "SlateDBTransaction":
        """Async variant of begin()."""
        ...

    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve a value by key from the database.
        
        Args:
            key: The key to look up as bytes (cannot be empty)
            
        Returns:
            The value as bytes if found, None if not found
            
        Raises:
            InvalidError: If the key is empty or arguments are invalid
            TransactionError | ClosedError | UnavailableError | DataError | InternalError: On DB errors
        """
        ...

    
    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """
        Create an iterator over key-value pairs within a range.

        Args:
            start: The start key to scan from as bytes (cannot be empty)
            end: The end key to stop at as bytes, exclusive (optional)
                 if None, scan with auto-generated end (start + 0xFF)

        Returns:
            A DbIterator yielding (key, value) tuples sorted by key.
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
        """Create an iterator with advanced scan options."""
        ...
    
    def delete(self, key: bytes) -> None:
        """
        Delete a key-value pair from the database.
        
        Args:
            key: The key to delete as bytes (cannot be empty)
            
        Raises:
            InvalidError: If the key is empty or arguments are invalid
            TransactionError | ClosedError | UnavailableError | DataError | InternalError: On DB errors
        """
        ...

    async def put_async(self, key: bytes, value: bytes) -> None:
        """
        Store a key-value pair in the database asynchronously.
        """
        ...

    async def get_async(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve a value by key from the database asynchronously.
        """
        ...

    async def delete_async(self, key: bytes) -> None:
        """
        Delete a key-value pair from the database asynchronously.
        """
        ...


    def close(self) -> None:
        """
        Close the database connection.
        
        Raises:
            UnavailableError | InternalError: On errors during close
        """
        ...

class SlateDBSnapshot:
    """A consistent, read-only view of the database created via SlateDB.snapshot()."""

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve a value by key from the snapshot.

        Args:
            key: The key to look up as bytes (cannot be empty)

        Returns:
            The value as bytes if found, None if not found
        """
        ...

class SlateDBTransaction:
    """
    A read-write transaction handle created by ``SlateDB.begin()``.

    A transaction provides read-your-writes semantics: reads issued after a
    buffered ``put``/``delete``/``merge`` within the same transaction will
    observe those uncommitted changes. Use ``commit()`` to atomically persist
    the buffered writes or ``rollback()`` to discard them.
    """

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Get a value within the transaction.

        Reads will observe your own uncommitted writes within this transaction.

        Args:
            key: The key to look up as bytes (must be non-empty).

        Returns:
            The value as bytes if found, otherwise ``None`` if the key does not exist.

        Raises:
            InvalidError: If ``key`` is empty or otherwise invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """
        Iterate key/value pairs within a range in this transaction.

        Args:
            start: Start of the range (bytes, non-empty). Inclusive.
            end: Optional end of the range (bytes). Exclusive. If ``None``, an
                end bound is derived to iterate keys sharing the ``start`` prefix.

        Returns:
            A ``DbIterator`` yielding ``(key, value)`` pairs in key order.

        Raises:
            InvalidError: If ``start`` is empty or bounds are invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
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
        Iterate a key range with advanced scan options within this transaction.

        Args:
            start: Start of the range (bytes, non-empty). Inclusive.
            end: Optional end of the range (bytes). Exclusive.
            durability_filter: Optionally restrict sources consulted during the scan
                (e.g., ``"remote"`` or ``"memory"``). If ``None``, uses the default.
            dirty: If specified, controls inclusion of dirty (in-memory/unflushed)
                data. If ``None``, uses the default for the transaction.
            read_ahead_bytes: Optional read-ahead size hint in bytes.
            cache_blocks: Optional toggle to cache blocks read during iteration.
            max_fetch_tasks: Optional limit on concurrent/background fetch tasks.

        Returns:
            A ``DbIterator`` yielding ``(key, value)`` pairs in key order.

        Raises:
            InvalidError: If bounds or options are invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def put(self, key: bytes, value: bytes) -> None:
        """
        Buffer a put in the transaction's write set.

        The change becomes visible to reads in this transaction and is durably
        applied when ``commit()`` succeeds.

        Args:
            key: Key to set (bytes, must be non-empty).
            value: Value to associate with ``key`` (bytes).

        Raises:
            InvalidError: If ``key`` is empty or arguments are invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def delete(self, key: bytes) -> None:
        """
        Buffer a delete in the transaction's write set.

        The removal becomes visible to reads in this transaction and is applied
        when ``commit()`` succeeds.

        Args:
            key: Key to remove (bytes, must be non-empty).

        Raises:
            InvalidError: If ``key`` is empty or arguments are invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def merge(self, key: bytes, value: bytes) -> None:
        """
        Buffer a merge operation in the transaction.

        Requires a merge operator to be configured for the database.

        Args:
            key: Key to merge into (bytes, must be non-empty).
            value: Operand value supplied to the merge operator (bytes).

        Raises:
            InvalidError: If ``key`` is empty, a merge operator is not configured, or arguments are invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def commit(self) -> None:
        """
        Commit the transaction, atomically applying buffered writes.

        Raises:
            TransactionError: If the commit detects a conflict and is aborted.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def rollback(self) -> None:
        """
        Discard all buffered operations without committing any changes.

        This is also performed automatically when the transaction is garbage-collected
        if neither ``commit()`` nor ``rollback()`` has been called.

        Raises:
            ClosedError: If the transaction has already been closed.
            UnavailableError: If the underlying storage is unavailable.
            InternalError: For unexpected internal errors.
        """
        ...

    async def get_async(self, key: bytes) -> Optional[bytes]:
        """
        Asynchronously get a value within the transaction.

        Args:
            key: The key to look up as bytes (must be non-empty).

        Returns:
            The value as bytes if found, otherwise ``None`` if the key does not exist.

        Raises:
            InvalidError: If ``key`` is empty or otherwise invalid.
            ClosedError: If the transaction has been closed or the database is fenced.
            UnavailableError: If the underlying storage is unavailable.
            DataError: If persisted data is corrupted or incompatible.
            InternalError: For unexpected internal errors.
        """
        ...

    def close(self) -> None:
        """
        Close the transaction and release any associated resources.

        Raises:
            UnavailableError: If the underlying storage is unavailable.
            InternalError: For unexpected internal errors.
        """
        ...

class SlateDBReader:
    """
    A read-only Python interface to SlateDB.
    
    SlateDBReader provides read-only access to a SlateDB database,
    optionally at a specific checkpoint.
    """
    
    def __init__(
        self,
        path: str,
        url: Optional[str] = None,
        env_file: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> None:
        """
        Create a new SlateDBReader instance.
        
        Args:
            path: The path where the database is stored
            url: Optional object store URL (e.g., "s3://bucket/prefix", "memory:///")
            env_file: Optional environment file for object store configuration
            checkpoint_id: Optional checkpoint ID (UUID string) to read from
            
        Raises:
            InvalidError: If checkpoint_id is invalid or arguments are invalid
            UnavailableError: If the object store is unavailable
            DataError: If persisted data is invalid
            InternalError: For unexpected internal errors
        """
        ...
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve a value by key from the database.
        
        Args:
            key: The key to look up as bytes (cannot be empty)
            
        Returns:
            The value as bytes if found, None if not found
            
        Raises:
            InvalidError: If the key is empty or arguments are invalid
            TransactionError | ClosedError | UnavailableError | DataError | InternalError: On DB errors
        """
        ...
    
    def scan(self, start: bytes, end: Optional[bytes] = None) -> "DbIterator":
        """Create an iterator over key-value pairs within a range."""
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
        """Create an iterator with advanced scan options."""
        ...

class DbIterator(Iterator[Tuple[bytes, bytes]]):
    """Iterator over (key, value) tuples returned by scan operations."""
    def __iter__(self) -> "DbIterator": ...
    def __next__(self) -> Tuple[bytes, bytes]: ...
    def seek(self, key: bytes) -> None:
        """
        Seek forward to the next position within the original scan range.

        Moves the iterator so that the next call to ``next(it)`` yields the
        first key greater than or equal to ``key``. The seek is forward-only:
        attempting to seek to a key less than or equal to the last returned key
        raises ``InvalidError``. Seeking beyond the original scan end bound also
        raises ``InvalidError``. If there are no further entries at or after
        ``key``, the next iteration raises ``StopIteration``.

        Args:
            key: The target key to seek to (bytes, non-empty)
        """
        ...
    
    async def get_async(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve a value by key from the database asynchronously.
        
        Args:
            key: The key to look up as bytes (cannot be empty)
            
        Returns:
            The value as bytes if found, None if not found
        """
        ...
    
    def close(self) -> None:
        """
        Close the database reader.
        
        Raises:
            UnavailableError | InternalError: On errors during close
        """
        ... 


class Checkpoint(TypedDict):
    """Details about a checkpoint returned by the Admin API.

    Keys
    - id: The checkpoint UUID string
    - manifest_id: The manifest version number at which the checkpoint was created
    - expire_time: Optional expiration timestamp in milliseconds since epoch
    - create_time: Creation timestamp in milliseconds since epoch
    """
    id: str
    manifest_id: int
    expire_time: Optional[int]
    create_time: int


class CheckpointCreateResult(TypedDict):
    """Result returned by create_checkpoint.

    Keys
    - id: The created checkpoint UUID string
    - manifest_id: The manifest version number used for the checkpoint
    """
    id: str
    manifest_id: int


class SlateDBAdmin:
    """Administrative interface for SlateDB.

    Provides functions to create and list checkpoints and to run administrative
    operations that do not require an open writer instance.
    """

    def __init__(self, path: str, url: Optional[str] = None, env_file: Optional[str] = None) -> None:
        """Create an Admin handle for a database path.

        Args:
            path: Path to the database
            url: Optional object store URL (e.g., "s3://bucket/prefix", "memory:///")
            env_file: Optional path to an environment file used to resolve the object store

        Raises:
            InvalidError: If configuration is invalid
            UnavailableError: If the object store is unavailable
            InternalError: For unexpected internal errors
        """
        ...

    def create_checkpoint(
        self, *, lifetime: Optional[int] = None, source: Optional[str] = None
    ) -> CheckpointCreateResult:
        """Create a detached checkpoint.

        Args:
            lifetime: Optional lifetime in milliseconds for the checkpoint
            source: Optional source checkpoint id (UUID string) to extend/refresh

        Returns:
            A dict with the created checkpoint id and manifest id

        Raises:
            InvalidError: If `source` is not a valid UUID or arguments are invalid
            UnavailableError | DataError | InternalError: On underlying admin/database errors
        """
        ...

    def list_checkpoints(self) -> List[Checkpoint]:
        """List known checkpoints.

        Returns:
            A list of checkpoint dicts with id, manifest_id, expire_time, create_time

        Raises:
            UnavailableError | InternalError: On errors fetching checkpoint metadata
        """
        ...
