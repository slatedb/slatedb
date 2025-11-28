import asyncio

import pytest

from slatedb import (
    ClosedError,
    InvalidError,
    SlateDB,
    TransactionError,
    UnavailableError,
    WriteBatch,
)


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
    def concat(key, existing, value):
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

@pytest.mark.asyncio
async def test_db_open_async_and_close_async(db_path, env_file):
    db = await SlateDB.open_async(db_path, env_file=env_file)
    try:
        await db.put_async(b"ka", b"va")
        assert await db.get_async(b"ka") == b"va"
    finally:
        await db.close_async()


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

@pytest.mark.asyncio
async def test_snapshot_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k1", b"v1")
        snap = await db.snapshot_async()
        db.put(b"k1", b"v2")
        assert await snap.get_async(b"k1") == b"v1"
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

@pytest.mark.asyncio
async def test_snapshot_close_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"k", b"v")
        snap = db.snapshot()
        await snap.close_async()
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

@pytest.mark.asyncio
async def test_db_iterator_async_for_and_seek_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        for i in range(3):
            db.put(f"ai{i}".encode(), f"v{i}".encode())
        it = db.scan(b"ai")
        # Async iterate over sync-created iterator
        seen = []
        async for k, v in it:  # __aiter__/__anext__
            seen.append((k, v))
        assert (b"ai0", b"v0") in seen and (b"ai2", b"v2") in seen

        # Now use seek_async on a fresh iterator
        it2 = await db.scan_async(b"ai")
        await it2.seek_async(b"ai2")
        pairs = list(it2)
        # After seeking to ai2, first item should be ai2
        assert pairs[0] == (b"ai2", b"v2")
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

@pytest.mark.asyncio
async def test_db_begin_async_and_commit_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        txn = await db.begin_async("si")
        txn.put(b"bk1", b"bv1")
        await txn.commit_async()
        assert db.get(b"bk1") == b"bv1"
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

def test_db_delete_sync(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"d1", b"v1")
        db.delete(b"d1")
        assert db.get(b"d1") is None
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

def test_txn_scan_with_options_sync(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"to1", b"v1")
        db.put(b"to2", b"v2")
        txn = db.begin("si")
        it = txn.scan_with_options(
            b"to",
            durability_filter="memory",
            dirty=False,
            read_ahead_bytes=1024,
            cache_blocks=True,
            max_fetch_tasks=2,
        )
        assert list(it) == [(b"to1", b"v1"), (b"to2", b"v2")]
        txn.rollback()
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
    def last_write_wins(key, existing, value):
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

def test_db_write_batch_put_with_options_and_merges(db_path, env_file):
    def concat(key, existing, value):
        return (existing or b"") + value

    db = SlateDB(db_path, env_file=env_file, merge_operator=concat)
    try:
        # Put with options in a batch
        wb = WriteBatch()
        wb.put_with_options(b"wpo1", b"v1", ttl=1000)
        db.write(wb)
        assert db.get(b"wpo1") == b"v1"

        # Apply merge and merge_with_options in separate batches to ensure accumulation
        wb1 = WriteBatch()
        wb1.merge(b"wm1", b"a")
        db.write(wb1)
        assert db.get(b"wm1") == b"a"

        wb2 = WriteBatch()
        wb2.merge_with_options(b"wm1", b"b", ttl=2000)
        db.write(wb2)
        # With concat merge operator, we expect accumulated result
        assert db.get(b"wm1") == b"ab"
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
async def test_db_create_checkpoint_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"c2", b"v2")
        res = await db.create_checkpoint_async("durable")
        assert isinstance(res["id"], str)
        assert isinstance(res["manifest_id"], int)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_async_put_delete_merge_with_options(db_path, env_file):
    def last_write_wins(key, existing, value):
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
async def test_db_scan_async_and_scan_with_options_variants(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        for k, v in [(b"sa1", b"v1"), (b"sa2", b"v2")]:
            db.put(k, v)
        it = await db.scan_async(b"sa")
        assert list(it) == [(b"sa1", b"v1"), (b"sa2", b"v2")]
        it2 = db.scan_with_options(
            b"sa",
            durability_filter="memory",
            dirty=False,
            read_ahead_bytes=64,
            cache_blocks=True,
            max_fetch_tasks=1,
        )
        assert list(it2) == [(b"sa1", b"v1"), (b"sa2", b"v2")]
        it3 = await db.scan_with_options_async(
            b"sa",
            durability_filter="memory",
            dirty=False,
            read_ahead_bytes=64,
            cache_blocks=True,
            max_fetch_tasks=1,
        )
        assert list(it3) == [(b"sa1", b"v1"), (b"sa2", b"v2")]
    finally:
        db.close()

@pytest.mark.asyncio
async def test_db_flush_async_and_flush_with_options_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"ff1", b"v1")
        await db.flush_async()
        await db.flush_with_options_async("wal")
        await db.flush_with_options_async("memtable")
        assert db.get(b"ff1") == b"v1"
    finally:
        db.close()

@pytest.mark.asyncio
async def test_db_merge_async(db_path, env_file):
    def last_write_wins(key, existing, value):
        return value

    db = SlateDB(db_path, env_file=env_file, merge_operator=last_write_wins)
    try:
        await db.merge_async(b"ma1", b"x")
        await db.merge_async(b"ma1", b"y")
        assert await db.get_async(b"ma1") == b"y"
    finally:
        await db.close_async()

@pytest.mark.asyncio
async def test_snapshot_scan_async_and_with_options(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"ps1", b"v1")
        db.put(b"ps2", b"v2")
        snap = db.snapshot()
        # mutate after snapshot to ensure isolation
        db.put(b"ps3", b"v3")
        it = await snap.scan_async(b"ps")
        assert list(it) == [(b"ps1", b"v1"), (b"ps2", b"v2")]
        it2 = snap.scan_with_options(
            b"ps",
            durability_filter="memory",
            dirty=False,
            read_ahead_bytes=128,
            cache_blocks=True,
            max_fetch_tasks=1,
        )
        assert list(it2) == [(b"ps1", b"v1"), (b"ps2", b"v2")]
        it3 = await snap.scan_with_options_async(
            b"ps",
            durability_filter="memory",
            dirty=False,
            read_ahead_bytes=128,
            cache_blocks=True,
            max_fetch_tasks=1,
        )
        assert list(it3) == [(b"ps1", b"v1"), (b"ps2", b"v2")]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_async_write_batch_and_write_with_options(db_path, env_file):
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
    def last_write_wins(key, existing, value):
        return value

    def concat(key, existing, value):
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

def test_txn_put_with_options(db_path, env_file):
    """Transaction put_with_options should override prior puts and commit properly."""
    db = SlateDB(db_path, env_file=env_file)
    try:
        txn = db.begin("si")
        # Regular put then put_with_options should see last write inside txn
        txn.put(b"tk1", b"v1")
        txn.put_with_options(b"tk1", b"v2")
        assert txn.get(b"tk1") == b"v2"

        # put_with_options with TTL argument should be accepted
        txn.put_with_options(b"tk2", b"vx", ttl=1000)

        txn.commit()
        assert db.get(b"tk1") == b"v2"
        assert db.get(b"tk2") == b"vx"
    finally:
        db.close()


def test_db_metrics_returns_dict_and_increments(db_path):
    db = SlateDB(db_path)
    try:
        metrics = db.metrics()
        assert isinstance(metrics, dict)
        # Expect at least core db stats to be present
        assert "db/get_requests" in metrics
        start_gets = int(metrics.get("db/get_requests", 0))

        # Trigger a get to increment the counter
        assert db.get(b"missing") is None
        metrics2 = db.metrics()
        assert int(metrics2.get("db/get_requests", 0)) >= start_gets + 1
    finally:
        db.close()


def test_db_metrics_scan_requests_increments(db_path):
    db = SlateDB(db_path)
    try:
        # Seed a couple of keys
        db.put(b"a1", b"v1")
        db.put(b"a2", b"v2")

        before = db.metrics()
        scans_before = int(before.get("db/scan_requests", 0))

        # Perform a scan and fully consume the iterator
        items = list(db.scan(b"a"))
        assert (b"a1", b"v1") in items and (b"a2", b"v2") in items

        after = db.metrics()
        assert int(after.get("db/scan_requests", 0)) >= scans_before + 1
    finally:
        db.close()


def test_db_metrics_write_ops_and_batch_count_and_memtable_flush(db_path):
    db = SlateDB(db_path)
    try:
        base = db.metrics()
        batches_before = int(base.get("db/write_batch_count", 0))
        ops_before = int(base.get("db/write_ops", 0))
        memflush_before = int(base.get("db/immutable_memtable_flushes", 0))

        # Prepare a batch with 3 operations
        wb = WriteBatch()
        wb.put(b"b1", b"v1")
        wb.put(b"b2", b"v2")
        wb.delete(b"b2")
        db.write(wb)

        mid = db.metrics()
        assert int(mid.get("db/write_batch_count", 0)) == batches_before + 1
        # WriteBatch de-duplicates ops per key; put+delete on same key counts once.
        # In this batch we have: put(b1), put(b2), delete(b2) => 2 effective ops.
        assert int(mid.get("db/write_ops", 0)) >= ops_before + 2

        # Force a memtable flush and check the counter increments
        db.flush_with_options("memtable")
        after = db.metrics()
        assert int(after.get("db/immutable_memtable_flushes", 0)) >= memflush_before + 1
    finally:
        db.close()


@pytest.mark.asyncio
async def test_db_get_with_options_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        db.put(b"gaw1", b"v1")
        # default async get_with_options
        assert await db.get_with_options_async(b"gaw1") == b"v1"
        # with explicit options
        assert (
            await db.get_with_options_async(b"gaw1", durability_filter="memory", dirty=False)
            == b"v1"
        )
        # missing key
        assert await db.get_with_options_async(b"missing") is None
    finally:
        db.close()


def test_txn_delete(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    try:
        # Seed a key then delete it within a transaction
        db.put(b"td1", b"v1")
        txn = db.begin("si")
        txn.delete(b"td1")
        txn.commit()
        assert db.get(b"td1") is None
    finally:
        db.close()
