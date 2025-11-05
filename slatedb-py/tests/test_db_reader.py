import pytest
from slatedb import SlateDB, SlateDBReader, SlateDBAdmin, InvalidError

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

def test_create_checkpoint(db_path, env_file, populated_db):
    """Test creating a checkpoint."""
    admin = SlateDBAdmin(db_path, env_file=env_file)
    result = admin.create_checkpoint()
    assert result is not None
    assert result["id"] is not None
    assert result["manifest_id"] is not None


def test_read_checkpoint(db_path, env_file, populated_db):
    """Test reading a checkpoint."""
    admin = SlateDBAdmin(db_path, env_file=env_file)
    result = admin.create_checkpoint()
    populated_db.put(b"key4", b"value4")
    reader = SlateDBReader(db_path, env_file=env_file, checkpoint_id=result["id"])
    assert reader.get(b"key4") is None
    reader.close()
    reader2 = SlateDBReader(db_path, env_file=env_file)
    assert reader2.get(b"key4") == b"value4"
    reader2.close()

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
