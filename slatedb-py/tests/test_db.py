import pytest
import asyncio
from slatedb import SlateDB

@pytest.mark.asyncio
async def test_async_put_and_get(db):
    """Test basic async put and get operations."""
    # Test simple put/get
    await db.put_async(b"key1", b"value1")
    result = await db.get_async(b"key1")
    assert result == b"value1"

    # Test overwrite
    await db.put_async(b"key1", b"value2")
    result = await db.get_async(b"key1")
    assert result == b"value2"

    # Test get non-existent key
    result = await db.get_async(b"nonexistent")
    assert result is None

@pytest.mark.asyncio
async def test_async_delete(db):
    """Test async delete operations."""
    # Test delete existing key
    await db.put_async(b"key1", b"value1")
    result = await db.get_async(b"key1")
    assert result == b"value1"
    
    await db.delete_async(b"key1")
    result = await db.get_async(b"key1")
    assert result is None

    # Test delete non-existent key (should not raise error)
    await db.delete_async(b"nonexistent")

@pytest.mark.asyncio
async def test_async_multiple_operations(db):
    """Test multiple async operations in sequence."""
    test_data = [
        (b"key1", b"value1"),
        (b"key2", b"value2"),
        (b"key3", b"value3"),
    ]

    # Put all data asynchronously
    for key, value in test_data:
        await db.put_async(key, value)

    # Verify all data asynchronously
    for key, value in test_data:
        result = await db.get_async(key)
        assert result == value

    # Delete some data
    await db.delete_async(b"key2")
    result1 = await db.get_async(b"key1")
    result2 = await db.get_async(b"key2")
    result3 = await db.get_async(b"key3")
    
    assert result1 == b"value1"
    assert result2 is None
    assert result3 == b"value3"

@pytest.mark.asyncio
async def test_async_concurrent_operations(db):
    """Test concurrent async operations."""
    # Perform multiple put operations concurrently
    tasks = []
    for i in range(10):
        key = f"concurrent_key_{i}".encode()
        value = f"concurrent_value_{i}".encode()
        tasks.append(db.put_async(key, value))
    
    await asyncio.gather(*tasks)
    
    # Verify all data concurrently
    get_tasks = []
    for i in range(10):
        key = f"concurrent_key_{i}".encode()
        get_tasks.append(db.get_async(key))
    
    results = await asyncio.gather(*get_tasks)
    
    for i, result in enumerate(results):
        expected_value = f"concurrent_value_{i}".encode()
        assert result == expected_value

@pytest.mark.asyncio
async def test_mixed_sync_async_operations(db):
    """Test mixing synchronous and asynchronous operations."""
    # Put with sync, get with async
    db.put(b"mixed_key1", b"mixed_value1")
    result = await db.get_async(b"mixed_key1")
    assert result == b"mixed_value1"
    
    # Put with async, get with sync
    await db.put_async(b"mixed_key2", b"mixed_value2")
    result = db.get(b"mixed_key2")
    assert result == b"mixed_value2"
    
    # Delete with async, verify with sync
    await db.delete_async(b"mixed_key1")
    result = db.get(b"mixed_key1")
    assert result is None


def test_merge_operator_callable_concat(db_path):
    """DB merge using a Python callable merge operator (concat)."""
    def concat(existing, value):
        return (existing or b"") + value

    db = SlateDB(db_path, merge_operator=concat)
    try:
        db.merge(b"k", b"a")
        assert db.get(b"k") == b"a"

        db.merge(b"k", b"b")
        db.merge(b"k", b"c")
        assert db.get(b"k") == b"abc"
    finally:
        db.close()
