import pytest
from slatedb import SlateDB

def test_put_and_get(db):
    """Test basic put and get operations."""
    # Test simple put/get
    db.put(b"key1", b"value1")
    assert db.get(b"key1") == b"value1"

    # Test overwrite
    db.put(b"key1", b"value2")
    assert db.get(b"key1") == b"value2"

    # Test get non-existent key
    assert db.get(b"nonexistent") is None

def test_delete(db):
    """Test delete operations."""
    # Test delete existing key
    db.put(b"key1", b"value1")
    assert db.get(b"key1") == b"value1"
    db.delete(b"key1")
    assert db.get(b"key1") is None

    # Test delete non-existent key (should not raise error)
    db.delete(b"nonexistent")

def test_multiple_operations(db):
    """Test multiple operations in sequence."""
    test_data = [
        (b"key1", b"value1"),
        (b"key2", b"value2"),
        (b"key3", b"value3"),
    ]

    # Put all data
    for key, value in test_data:
        db.put(key, value)

    # Verify all data
    for key, value in test_data:
        assert db.get(key) == value

    # Delete some data
    db.delete(b"key2")
    assert db.get(b"key1") == b"value1"
    assert db.get(b"key2") is None
    assert db.get(b"key3") == b"value3"

def test_empty_values(db):
    """Test operations with empty values."""
    # Empty value should be allowed
    db.put(b"empty_value", b"")
    assert db.get(b"empty_value") == b""

    # Empty key should not be allowed
    with pytest.raises(ValueError, match="key cannot be empty"):
        db.put(b"", b"empty_key")

    with pytest.raises(ValueError, match="key cannot be empty"):
        db.get(b"")

    with pytest.raises(ValueError, match="key cannot be empty"):
        db.delete(b"")

    with pytest.raises(ValueError, match="start cannot be empty"):
        db.scan(b"", b"key3")

    with pytest.raises(ValueError, match="start cannot be empty"):
        db.scan(b"", None)

        
def test_large_values(db):
    """Test operations with large values."""
    large_value = b"x" * 1024 * 1024  # 1MB
    db.put(b"large_key", large_value)
    assert db.get(b"large_key") == large_value

def test_binary_data(db):
    """Test operations with binary data."""
    binary_data = bytes(range(256))
    db.put(b"binary", binary_data)
    assert db.get(b"binary") == binary_data

def test_scan(db):
    """Test scan operations."""
    db.put(b"key1", b"value1")
    db.put(b"key2", b"value2")
    db.put(b"key3", b"value3")
    assert list(db.scan(b"key1", b"key4")) == [(b"key1", b"value1"), (b"key2", b"value2"), (b"key3", b"value3")]
    assert list(db.scan(b"key1", b"key3")) == [(b"key1", b"value1"), (b"key2", b"value2")]
    assert list(db.scan(b"key1", b"key2")) == [(b"key1", b"value1")]
    assert list(db.scan(b"key1", None)) == [(b"key1", b"value1")]
    assert list(db.scan(b"key")) == [(b"key1", b"value1"), (b"key2", b"value2"), (b"key3", b"value3")]

def test_scan_iterator(db):
    """Test scan iterator."""
    db.put(b"key1", b"value1")
    db.put(b"key2", b"value2")
    db.put(b"key3", b"value3")
    iterator = db.scan_iter(b"key1", b"key4")
    assert next(iterator) == (b"key1", b"value1")
    assert next(iterator) == (b"key2", b"value2")
    assert next(iterator) == (b"key3", b"value3")
    with pytest.raises(StopIteration):
        next(iterator)

def test_invalid_inputs(db):
    """Test invalid inputs."""
    with pytest.raises(TypeError):
        db.put("not bytes", b"value")  # key must be bytes
    
    with pytest.raises(TypeError):
        db.put(b"key", "not bytes")  # value must be bytes
    
    with pytest.raises(TypeError):
        db.get("not bytes")  # key must be bytes
    
    with pytest.raises(TypeError):
        db.delete("not bytes")  # key must be bytes
