import pytest

from slatedb import InvalidError, SlateDB, SlateDBReader


@pytest.fixture
def populated_db(db_path, env_file):
    """Create a SlateDB instance with test data."""
    db = SlateDB(db_path, env_file=env_file)

    # Populate with test data
    test_data = [
        (b"key1", b"value1"),
        (b"key2", b"value2"),
        (b"key3", b"value3"),
        (b"prefix_a", b"data_a"),
        (b"prefix_b", b"data_b"),
    ]

    for key, value in test_data:
        db.put(key, value)

    try:
        yield db
    finally:
        db.close()


def test_reader_creation_and_close(db_path, env_file):
    """Test creating and closing a SlateDBReader."""
    # Create a simple database first
    print(f"Creating database at {db_path}")
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"test", b"value")
    db.close()

    # Now test reader
    reader = SlateDBReader(db_path, env_file=env_file)
    assert reader is not None
    reader.close()

@pytest.mark.asyncio
async def test_reader_open_async_and_close_async(db_path, env_file):
    # Seed
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"rk1", b"v1")
    db.close()

    reader = await SlateDBReader.open_async(db_path, env_file=env_file)
    try:
        assert await reader.get_async(b"rk1") == b"v1"
    finally:
        await reader.close_async()


def test_get_operations(db_path, env_file, populated_db):
    """Test basic get operations."""
    reader = SlateDBReader(db_path, env_file=env_file)

    # Test existing keys
    assert reader.get(b"key1") == b"value1"
    assert reader.get(b"key2") == b"value2"

    # Test non-existent key
    assert reader.get(b"nonexistent") is None

    reader.close()


def test_scan_operations(db_path, env_file, populated_db):
    """Test basic scan operations."""
    reader = SlateDBReader(db_path, env_file=env_file)

    # Test range scan
    results = list(reader.scan(b"key1", b"key3"))
    expected = [(b"key1", b"value1"), (b"key2", b"value2")]
    assert results == expected

    # Test prefix scan
    results = list(reader.scan(b"prefix"))
    expected = [(b"prefix_a", b"data_a"), (b"prefix_b", b"data_b")]
    assert results == expected

    reader.close()

@pytest.mark.asyncio
async def test_reader_scan_async(db_path, env_file, populated_db):
    reader = SlateDBReader(db_path, env_file=env_file)
    it = await reader.scan_async(b"key")
    pairs = list(it)
    assert (b"key1", b"value1") in pairs and (b"key2", b"value2") in pairs
    reader.close()


def test_read_then_write_then_read(db_path, env_file, populated_db):
    """Test that a reader can be created after a writer."""
    reader = SlateDBReader(db_path, env_file=env_file)
    assert reader.get(b"key4") is None
    populated_db.put(b"key4", b"value4")
    reader2 = SlateDBReader(db_path, env_file=env_file)
    assert reader2.get(b"key4") == b"value4"
    reader2.close()


def test_empty_key_validation(db_path, env_file, populated_db):
    """Test that empty keys raise ValueError."""
    reader = SlateDBReader(db_path, env_file=env_file)

    with pytest.raises(InvalidError, match="key cannot be empty"):
        reader.get(b"")

    with pytest.raises(InvalidError, match="start cannot be empty"):
        list(reader.scan(b"", b"key3"))

    reader.close()


@pytest.mark.asyncio
async def test_get_async(db_path, env_file, populated_db):
    """Test async get operations."""
    reader = SlateDBReader(db_path, env_file=env_file)

    # Test existing key
    result = await reader.get_async(b"key1")
    assert result == b"value1"

    # Test non-existent key
    result = await reader.get_async(b"nonexistent")
    assert result is None

    reader.close()


@pytest.mark.asyncio
async def test_get_async_empty_key_error(db_path, env_file, populated_db):
    """Test that async get with empty key raises ValueError."""
    reader = SlateDBReader(db_path, env_file=env_file)

    with pytest.raises(InvalidError, match="key cannot be empty"):
        await reader.get_async(b"")

    reader.close()


def test_invalid_path():
    """Test reader creation with invalid path."""
    with pytest.raises(Exception):
        SlateDBReader("/nonexistent/path/that/should/not/exist")


def test_invalid_checkpoint_id(db_path, env_file):
    """Test reader creation with invalid checkpoint_id."""
    # Create a simple database first
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"test", b"value")
    db.close()

    # Invalid UUID format should raise an error
    with pytest.raises(InvalidError):
        SlateDBReader(db_path, env_file=env_file, checkpoint_id="invalid-uuid-format") 


def test_get_with_options(db_path, env_file, populated_db):
    """Ensure get_with_options is exposed and returns expected values."""
    reader = SlateDBReader(db_path, env_file=env_file)
    # Basic retrieval
    assert reader.get_with_options(b"key1") == b"value1"
    # With durability filter and dirty flag (ignored by reader, but should not error)
    assert reader.get_with_options(b"key2", durability_filter="memory", dirty=False) == b"value2"
    # Missing key
    assert reader.get_with_options(b"missing") is None
    reader.close()

def test_reader_scan_with_options(db_path, env_file, populated_db):
    reader = SlateDBReader(db_path, env_file=env_file)
    it = reader.scan_with_options(
        b"prefix",
        durability_filter="memory",
        dirty=False,
        read_ahead_bytes=256,
        cache_blocks=True,
        max_fetch_tasks=1,
    )
    assert list(it) == [(b"prefix_a", b"data_a"), (b"prefix_b", b"data_b")]
    reader.close()

@pytest.mark.asyncio
async def test_reader_scan_with_options_async(db_path, env_file, populated_db):
    reader = SlateDBReader(db_path, env_file=env_file)
    it = await reader.scan_with_options_async(
        b"prefix",
        durability_filter="memory",
        dirty=False,
        read_ahead_bytes=256,
        cache_blocks=True,
        max_fetch_tasks=1,
    )
    assert list(it) == [(b"prefix_a", b"data_a"), (b"prefix_b", b"data_b")]
    reader.close()


@pytest.mark.asyncio
async def test_get_with_options_async(db_path, env_file, populated_db):
    reader = SlateDBReader(db_path, env_file=env_file)
    assert await reader.get_with_options_async(b"key1") == b"value1"
    assert await reader.get_with_options_async(b"missing") is None
    reader.close()


def test_reader_options_validation_min_lifetime(db_path, env_file):
    """checkpoint_lifetime must be >= 1000ms."""
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"t", b"v")
    db.close()
    with pytest.raises(InvalidError):
        SlateDBReader(
            db_path,
            env_file=env_file,
            checkpoint_id=None,
            checkpoint_lifetime=500,  # too small
        )


def test_reader_options_validation_interval_vs_lifetime(db_path, env_file):
    """checkpoint_lifetime must exceed 2x manifest_poll_interval."""
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"t", b"v")
    db.close()
    with pytest.raises(InvalidError):
        SlateDBReader(
            db_path,
            env_file=env_file,
            checkpoint_id=None,
            manifest_poll_interval=700,
            checkpoint_lifetime=1000,  # 1000 < 2 * 700
        )


def test_reader_options_success(db_path, env_file):
    """Providing valid reader options opens and closes successfully."""
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"t", b"v")
    db.close()
    reader = SlateDBReader(
        db_path,
        env_file=env_file,
        manifest_poll_interval=200,
        checkpoint_lifetime=2000,
        max_memtable_bytes=1024 * 1024,
    )
    assert reader.get(b"t") == b"v"
    reader.close()
