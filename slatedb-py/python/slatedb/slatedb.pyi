"""
Python stub file for slatedb module.

This module provides a Python interface to SlateDB, a key-value database built in Rust.
"""

from typing import Optional, List, Tuple

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
    
    def __init__(self, path: str, env_file: Optional[str] = None, **kwargs) -> None:
        """
        Create a new SlateDB instance.
        
        Args:
            path: The path where the database will be stored
            
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

class SlateDBReader:
    """
    A read-only Python interface to SlateDB.
    
    SlateDBReader provides read-only access to a SlateDB database,
    optionally at a specific checkpoint.
    """
    
    def __init__(
        self, 
        path: str, 
        env_file: Optional[str] = None, 
        checkpoint_id: Optional[str] = None
    ) -> None:
        """
        Create a new SlateDBReader instance.
        
        Args:
            path: The path where the database is stored
            env_file: Optional environment file for object store configuration
            checkpoint_id: Optional checkpoint ID (UUID string) to read from
            
        Raises:
            InvalidError: If checkpoint_id is invalid
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
