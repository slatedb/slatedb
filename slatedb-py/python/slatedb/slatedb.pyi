"""
Python stub file for slatedb module.

This module provides a Python interface to SlateDB, a key-value database built in Rust.
"""

from typing import Optional

class SlateDB:
    """
    A Python interface to SlateDB, a key-value database.
    
    SlateDB is a high-performance key-value store that provides ACID transactions
    and is built with Rust for safety and performance.
    """
    
    def __init__(self, path: str) -> None:
        """
        Create a new SlateDB instance.
        
        Args:
            path: The path where the database will be stored
            
        Raises:
            ValueError: If there's an error opening the database
        """
        ...
    
    def put(self, key: bytes, value: bytes) -> None:
        """
        Store a key-value pair in the database.
        
        Args:
            key: The key as bytes (cannot be empty)
            value: The value as bytes
            
        Raises:
            ValueError: If the key is empty or there's a database error
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
            ValueError: If the key is empty or there's a database error
        """
        ...
    
    def delete(self, key: bytes) -> None:
        """
        Delete a key-value pair from the database.
        
        Args:
            key: The key to delete as bytes (cannot be empty)
            
        Raises:
            ValueError: If the key is empty or there's a database error
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
            ValueError: If there's an error closing the database
        """
        ... 