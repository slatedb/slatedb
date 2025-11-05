import pytest
import asyncio
from slatedb import SlateDB, ClosedError, UnavailableError, InvalidError

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


def test_closed_error_on_operations(db_path):
    db = SlateDB(db_path)
    db.put(b"k", b"v")
    db.close()

    with pytest.raises(ClosedError):
        db.put(b"k2", b"v2")

    with pytest.raises(ClosedError):
        db.get(b"k")


def test_invalid_url_raises_invalid_error(db_path):
    # Bad URL that fails parsing triggers InvalidError mapping
    with pytest.raises(InvalidError):
        SlateDB(db_path, url=":invalid:")


def test_unknown_scheme_raises_unavailable_error(db_path):
    # Unknown scheme parses but is not resolvable by registry -> UnavailableError
    with pytest.raises(UnavailableError):
        SlateDB(db_path, url="unknown:///")


def test_snapshot_basic_isolation(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")

        # Create snapshot of current state
        snap = db.snapshot()

        # Mutate database after snapshot
        db.put(b"k1", b"v2")
        db.put(b"k2", b"v3")

        # Snapshot sees original state
        assert snap.get(b"k1") == b"v1"
        assert snap.get(b"k2") is None

        # Database sees latest state
        assert db.get(b"k1") == b"v2"
        assert db.get(b"k2") == b"v3"
    finally:
        db.close()


def test_snapshot_scan_isolation(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"a1", b"v1")
        db.put(b"a2", b"v2")

        snap = db.snapshot()

        # Insert after snapshot
        db.put(b"a3", b"v3")

        # Snapshot scan should not see a3
        assert list(snap.scan(b"a")) == [(b"a1", b"v1"), (b"a2", b"v2")]
        # DB scan sees all
        assert list(db.scan(b"a")) == [(b"a1", b"v1"), (b"a2", b"v2"), (b"a3", b"v3")]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_snapshot_get_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")
        snap = db.snapshot()

        # Mutate database after snapshot
        db.put(b"k1", b"v2")

        # Async read from snapshot returns original value
        v = await snap.get_async(b"k1")
        assert v == b"v1"
    finally:
        db.close()


def test_snapshot_close(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k", b"v")
        snap = db.snapshot()
        snap.close()
        with pytest.raises(ClosedError):
            _ = snap.get(b"k")
    finally:
        db.close()


def test_db_iterator_seek_forward_and_backward(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        # Populate keys
        db.put(b"k1", b"v1")
        db.put(b"k2", b"v2")
        db.put(b"k3", b"v3")

        it = db.scan(b"k")

        # First element is k1
        assert next(it) == (b"k1", b"v1")

        # Seek forward to k3 and read it
        it.seek(b"k3")
        assert next(it) == (b"k3", b"v3")

        # Seeking backwards to k2 should fail
        with pytest.raises(InvalidError):
            it.seek(b"k2")

        # Exhaust iterator
        with pytest.raises(StopIteration):
            next(it)
    finally:
        db.close()


def test_snapshot_iterator_seek(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"a1", b"v1")
        db.put(b"a2", b"v2")
        snap = db.snapshot()
        # mutate after snapshot; snapshot should not see this
        db.put(b"a3", b"v3")

        it = snap.scan(b"a")
        assert next(it) == (b"a1", b"v1")
        # Seek forward to a2
        it.seek(b"a2")
        assert next(it) == (b"a2", b"v2")
        # No more items in snapshot
        with pytest.raises(StopIteration):
            next(it)
        snap.close()
    finally:
        db.close()

