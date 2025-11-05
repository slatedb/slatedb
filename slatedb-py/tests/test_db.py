import asyncio

import pytest

from slatedb import ClosedError, InvalidError, SlateDB, TransactionError, UnavailableError


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


def test_txn_si_read_your_writes_and_commit(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        # Initial write
        db.put(b"k0", b"v0")

        # Begin SI transaction
        txn = db.begin("si")
        # Reads see committed data
        assert txn.get(b"k0") == b"v0"
        # Writes are visible inside the txn (read-your-writes)
        txn.put(b"k1", b"v1")
        assert txn.get(b"k1") == b"v1"

        # A scan created before further writes should not see later writes
        it = txn.scan(b"k")
        # Write after creating the iterator
        txn.put(b"k2", b"v2")
        # Exhaust iterator – should not see k2 inserted after iterator creation
        keys = [k for k, _ in it]
        assert b"k2" not in keys

        # A new scan should see the updated write-set
        keys2 = [k for k, _ in txn.scan(b"k")]
        assert b"k1" in keys2 and b"k2" in keys2

        # Commit and verify
        txn.commit()
        assert db.get(b"k1") == b"v1"
        assert db.get(b"k2") == b"v2"
    finally:
        db.close()


def test_txn_si_conflict_between_txns(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")

        txn1 = db.begin("si")
        txn1.put(b"k1", b"v2")

        txn2 = db.begin("si")
        txn2.put(b"k1", b"v3")

        # First commit succeeds
        txn1.commit()

        # Second commit conflicts
        with pytest.raises(TransactionError):
            txn2.commit()
    finally:
        db.close()


def test_txn_si_conflict_with_db_writes(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")

        txn = db.begin("si")
        txn.put(b"k1", b"v2")

        # Outside write on same key
        db.put(b"k1", b"v3")

        with pytest.raises(TransactionError):
            txn.commit()
    finally:
        db.close()


def test_txn_ssi_read_conflict(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")
        db.put(b"k2", b"v2.1")

        txn1 = db.begin("ssi")
        txn1.put(b"k1", b"v2")
        txn1.put(b"k2", b"v2.2")

        txn2 = db.begin("ssi")
        assert txn2.get(b"k2") == b"v2.1"
        txn2.put(b"k3", b"v3")

        txn1.commit()

        with pytest.raises(TransactionError):
            txn2.commit()
    finally:
        db.close()


def test_txn_ssi_range_conflict(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")
        db.put(b"k2", b"v2.1")
        db.put(b"k3", b"v3")

        txn1 = db.begin("ssi")
        txn2 = db.begin("ssi")

        # Transaction 2 scans k2..k3
        _ = [kv for kv in txn2.scan(b"k2", b"k3\xff")]

        # Transaction 1 writes within the range that transaction 2 scanned
        txn1.put(b"k2", b"v2.2")
        txn1.commit()

        # Transaction 2 writes something and tries to commit -> conflict
        txn2.put(b"k4", b"v4")
        with pytest.raises(TransactionError):
            txn2.commit()
    finally:
        db.close()


def test_txn_rollback(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        txn = db.begin("si")
        txn.put(b"k9", b"v9")
        txn.rollback()
        assert db.get(b"k9") is None
    finally:
        db.close()


def test_db_flush_controls(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"f1", b"v1")
        # basic flush
        db.flush()
        # explicit flush types
        db.flush_with_options("wal")
        db.flush_with_options("memtable")
        assert db.get(b"f1") == b"v1"
    finally:
        db.close()


def test_db_get_with_options(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"g1", b"v1")
        # default
        assert db.get_with_options(b"g1") == b"v1"
        # with options
        assert db.get_with_options(b"g1", durability_filter="memory", dirty=False) == b"v1"
    finally:
        db.close()


def test_txn_get_with_options(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        # Seed data
        db.put(b"tg1", b"v1")

        # Begin transaction and read existing key with default options
        txn = db.begin("si")
        assert txn.get_with_options(b"tg1") == b"v1"
        # Missing key returns None
        assert txn.get_with_options(b"missing") is None

        # Read with explicit read options
        assert (
            txn.get_with_options(b"tg1", durability_filter="memory", dirty=False) == b"v1"
        )

        # Read-your-writes behavior also works with get_with_options
        txn.put(b"tg2", b"v2")
        assert txn.get_with_options(b"tg2") == b"v2"

        txn.rollback()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_txn_get_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"ag1", b"v1")
        txn = db.begin("si")
        # Existing key
        assert await txn.get_async(b"ag1") == b"v1"
        # Missing key
        assert await txn.get_async(b"missing") is None
        # Read-your-writes via async path
        txn.put(b"ag2", b"v2")
        assert await txn.get_async(b"ag2") == b"v2"
        txn.rollback()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_txn_get_with_options_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"agw1", b"v1")
        txn = db.begin("si")
        assert (
            await txn.get_with_options_async(b"agw1", durability_filter="memory", dirty=False)
            == b"v1"
        )
        txn.rollback()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_txn_scan_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"s1", b"v1")
        db.put(b"s2", b"v2")
        txn = db.begin("si")
        it = await txn.scan_async(b"s")
        # Insert after iterator creation; should not be visible in this iterator
        txn.put(b"s3", b"v3")
        items = list(it)
        assert (b"s1", b"v1") in items and (b"s2", b"v2") in items
        assert (b"s3", b"v3") not in items
        # A new iterator should see the new write
        items2 = list(await txn.scan_async(b"s"))
        assert (b"s3", b"v3") in items2
        txn.rollback()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_txn_scan_with_options_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"o1", b"v1")
        db.put(b"o2", b"v2")
        txn = db.begin("si")
        it = await txn.scan_with_options_async(
            b"o",
            durability_filter="memory",
            dirty=False,
            read_ahead_bytes=1024,
            cache_blocks=True,
            max_fetch_tasks=2,
        )
        assert list(it) == [(b"o1", b"v1"), (b"o2", b"v2")]
        txn.rollback()
    finally:
        db.close()


def test_db_put_delete_merge_with_options(db_path, env_file):
    # Use merge operator for merge tests
    def last_write_wins(existing, value):
        return value

    db = SlateDB(db_path, env_file=env_file, merge_operator=last_write_wins)
    try:
        # put with await_durable=False
        db.put_with_options(b"p1", b"v1", await_durable=False)
        assert db.get(b"p1") == b"v1"

        # delete with await_durable=False
        db.delete_with_options(b"p1", await_durable=False)
        assert db.get(b"p1") is None

        # merge with options
        db.merge_with_options(b"m1", b"a", await_durable=False)
        db.merge_with_options(b"m1", b"b", await_durable=False)
        assert db.get(b"m1") == b"b"
    finally:
        db.close()


def test_db_write_batch_and_write_with_options(db_path, env_file):
    from slatedb import WriteBatch

    db = SlateDB(db_path, env_file=env_file)
    try:
        wb = WriteBatch()
        wb.put(b"b1", b"v1")
        wb.put(b"b2", b"v2")
        wb.delete(b"b2")
        db.write(wb)
        assert db.get(b"b1") == b"v1"
        assert db.get(b"b2") is None

        wb2 = WriteBatch()
        wb2.put(b"b3", b"v3")
        db.write_with_options(wb2, await_durable=False)
        assert db.get(b"b3") == b"v3"
    finally:
        db.close()


def test_db_create_checkpoint(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"c1", b"v1")
        res = db.create_checkpoint("all")
        assert isinstance(res["id"], str)
        assert isinstance(res["manifest_id"], int)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_async_put_delete_merge_with_options(db_path, env_file):
    def last_write_wins(existing, value):
        return value

    db = SlateDB(db_path, env_file=env_file, merge_operator=last_write_wins)
    try:
        await db.put_with_options_async(b"ap1", b"v1", await_durable=False)
        assert await db.get_async(b"ap1") == b"v1"

        await db.delete_with_options_async(b"ap1", await_durable=False)
        assert await db.get_async(b"ap1") is None

        await db.merge_with_options_async(b"am1", b"x", await_durable=False)
        await db.merge_with_options_async(b"am1", b"y", await_durable=False)
        assert await db.get_async(b"am1") == b"y"
    finally:
        await db.close_async()


@pytest.mark.asyncio
async def test_async_write_batch_and_write_with_options(db_path, env_file):
    from slatedb import WriteBatch

    db = SlateDB(db_path, env_file=env_file)
    try:
        wb = WriteBatch()
        wb.put(b"ab1", b"v1")
        wb.put(b"ab2", b"v2")
        await db.write_async(wb)
        assert await db.get_async(b"ab1") == b"v1"
        assert await db.get_async(b"ab2") == b"v2"

        wb2 = WriteBatch()
        wb2.put(b"ab3", b"v3")
        await db.write_with_options_async(wb2, await_durable=False)
        assert await db.get_async(b"ab3") == b"v3"
    finally:
        await db.close_async()


def test_txn_merge_and_merge_with_options(db_path, env_file):
    # Define merge operators
    def last_write_wins(existing, value):
        return value

    def concat(existing, value):
        return (existing or b"") + value

    # Test with last_write_wins
    db = SlateDB(db_path, env_file=env_file, merge_operator=last_write_wins)
    try:
        txn = db.begin("si")
        txn.merge(b"mk1", b"a")
        txn.merge_with_options(b"mk1", b"b")
        # Within txn should see last write
        assert txn.get(b"mk1") == b"b"
        txn.commit()
        assert db.get(b"mk1") == b"b"
    finally:
        db.close()

    # Test with concat operator
    db2 = SlateDB(db_path, env_file=env_file, merge_operator=concat)
    try:
        txn = db2.begin("si")
        txn.merge(b"mk2", b"x")
        txn.merge_with_options(b"mk2", b"y")
        txn.merge(b"mk2", b"z")
        # Within txn sees accumulated merge result
        assert txn.get(b"mk2") == b"xyz"
        txn.commit()
        assert db2.get(b"mk2") == b"xyz"
    finally:
        db2.close()
