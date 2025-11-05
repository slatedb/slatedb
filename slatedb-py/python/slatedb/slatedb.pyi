"""
Python stub file for slatedb module.

This module provides a Python interface to SlateDB, a key-value database built in Rust.
"""

from typing import Optional, List, Tuple, TypedDict

# Exceptions mirroring SlateDB error kinds
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

    
    def scan(self, start: bytes, end: Optional[bytes] = None) -> List[Tuple[bytes, bytes]]:
        """
        Scan the database for key-value pairs with a given prefix.

        Args:
            start: The start key to scan from as bytes (cannot be empty)
            end: The end key to stop at as bytes, exclusive (optional, defaults to None)
                 if None, scan until the end of start+0xFF

        Raises:
            InvalidError: If the start key is empty or arguments are invalid
            TransactionError | ClosedError | UnavailableError | DataError | InternalError: On DB errors

        Returns:
            A list of tuples containing the key and value as bytes, sorted by key
        """
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

    def scan(self, start: bytes, end: Optional[bytes] = None) -> List[Tuple[bytes, bytes]]:
        """
        Scan the snapshot for key-value pairs within a range.

        Args:
            start: The start key to scan from as bytes (cannot be empty)
            end: The end key to stop at as bytes, exclusive (optional)
                 if None, scan with auto-generated end (start + 0xFF)

        Returns:
            A list of tuples containing the key and value as bytes, sorted by key
        """
        ...

    async def get_async(self, key: bytes) -> Optional[bytes]:
        """
        Retrieve a value by key from the snapshot asynchronously.
        """
        ...

    def close(self) -> None:
        """Close the snapshot, releasing any associated resources."""
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
    
    def scan(self, start: bytes, end: Optional[bytes] = None) -> List[Tuple[bytes, bytes]]:
        """
        Scan the database for key-value pairs within a range.

        Args:
            start: The start key to scan from as bytes (cannot be empty)
            end: The end key to stop at as bytes, exclusive (optional)
                 if None, scan with auto-generated end (start + 0xFF)

        Raises:
            InvalidError: If the start key is empty or arguments are invalid
            TransactionError | ClosedError | UnavailableError | DataError | InternalError: On DB errors

        Returns:
            A list of tuples containing the key and value as bytes, sorted by key
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
