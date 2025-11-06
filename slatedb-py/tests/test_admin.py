import pytest

from slatedb import InvalidError, SlateDB, SlateDBAdmin, SlateDBReader


def test_admin_create_checkpoint(db_path, env_file):
    """Admin can create a detached checkpoint and return id + manifest_id."""
    # Create a simple database first
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"test", b"value")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)
    result = admin.create_checkpoint()
    assert result is not None
    assert isinstance(result["id"], str) and len(result["id"]) > 0
    assert isinstance(result["manifest_id"], int)


def test_admin_list_checkpoints(db_path, env_file):
    """Admin can list checkpoints, and list includes newly created ones."""
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"k", b"v")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)
    before = admin.list_checkpoints()
    assert isinstance(before, list)

    created = admin.create_checkpoint()
    cps = admin.list_checkpoints()
    # Expect at least one checkpoint and that the created id is present
    assert any(c["id"] == created["id"] for c in cps)


def test_admin_read_checkpoint_isolation(db_path, env_file):
    """Reader pinned to checkpoint doesn't see later writes; regular reader does."""
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"keyA", b"valueA")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)
    result = admin.create_checkpoint()

    # Write after checkpoint
    db2 = SlateDB(db_path, env_file=env_file)
    db2.put(b"keyB", b"valueB")
    db2.close()

    # Reader with checkpoint does not see keyB
    reader = SlateDBReader(db_path, env_file=env_file, checkpoint_id=result["id"])
    assert reader.get(b"keyA") == b"valueA"
    assert reader.get(b"keyB") is None
    reader.close()

    # Reader without checkpoint sees keyB
    reader2 = SlateDBReader(db_path, env_file=env_file)
    assert reader2.get(b"keyB") == b"valueB"
    reader2.close()


def test_admin_create_checkpoint_with_options(db_path, env_file):
    """Admin checkpoint accepts lifetime and source; invalid source raises InvalidError."""
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"key", b"value")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)

    # valid lifetime only
    res = admin.create_checkpoint(lifetime=1000)
    assert isinstance(res["id"], str)

    # invalid source UUID format
    with pytest.raises(InvalidError):
        admin.create_checkpoint(source="not-a-uuid")

def test_admin_manifest_read_and_list(db_path, env_file):
    # Create some data
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"m1", b"v1")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)
    # read_manifest (latest)
    m = admin.read_manifest()
    assert m is None or isinstance(m, str)
    # list_manifests default range
    listing = admin.list_manifests()
    assert isinstance(listing, str)
    # list_manifests with explicit range
    listing2 = admin.list_manifests(start=0, end=2**63 - 1)
    assert isinstance(listing2, str)

@pytest.mark.asyncio
async def test_admin_manifest_read_and_list_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"am1", b"v1")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)
    m = await admin.read_manifest_async()
    assert m is None or isinstance(m, str)
    listing = await admin.list_manifests_async()
    assert isinstance(listing, str)

@pytest.mark.asyncio
async def test_admin_create_checkpoint_and_list_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"ck", b"v")
    db.close()

    admin = SlateDBAdmin(db_path, env_file=env_file)
    res = await admin.create_checkpoint_async()
    assert isinstance(res["id"], str)
    cps = await admin.list_checkpoints_async()
    assert any(c["id"] == res["id"] for c in cps)

def test_admin_refresh_and_delete_checkpoint(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"rk", b"v")
    db.close()
    admin = SlateDBAdmin(db_path, env_file=env_file)
    res = admin.create_checkpoint()
    # refresh (no assert on effect, just ensure it doesn't raise)
    admin.refresh_checkpoint(res["id"], lifetime=1000)
    # delete (ensure it doesn't raise)
    admin.delete_checkpoint(res["id"])

@pytest.mark.asyncio
async def test_admin_refresh_and_delete_checkpoint_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"rak", b"v")
    db.close()
    admin = SlateDBAdmin(db_path, env_file=env_file)
    res = admin.create_checkpoint()
    await admin.refresh_checkpoint_async(res["id"], lifetime=1000)
    await admin.delete_checkpoint_async(res["id"])

def test_admin_sequence_time_mapping(db_path, env_file):
    # Just verify methods return int|None without raising
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"st", b"v")
    db.close()
    admin = SlateDBAdmin(db_path, env_file=env_file)
    ts = admin.get_timestamp_for_sequence(0, round_up=False)
    assert ts is None or isinstance(ts, int)
    import time
    now = int(time.time() * 1000)
    seq = admin.get_sequence_for_timestamp(now, round_up=False)
    assert seq is None or isinstance(seq, int)

@pytest.mark.asyncio
async def test_admin_sequence_time_mapping_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"sta", b"v")
    db.close()
    admin = SlateDBAdmin(db_path, env_file=env_file)
    ts = await admin.get_timestamp_for_sequence_async(0, round_up=False)
    assert ts is None or isinstance(ts, int)
    import time
    now = int(time.time() * 1000)
    seq = await admin.get_sequence_for_timestamp_async(now, round_up=False)
    assert seq is None or isinstance(seq, int)

@pytest.mark.asyncio
async def test_admin_create_clone_sync_and_async(db_path, env_file, tmp_path):
    # Prepare parent DB
    parent_path = str(tmp_path / "parent_db")
    child_path = str(tmp_path / "child_db")
    parent_db = SlateDB(parent_path, env_file=env_file)
    parent_db.put(b"cK", b"cV")
    parent_db.close()

    # Create clone via admin
    admin = SlateDBAdmin(child_path, env_file=env_file)
    admin.create_clone(parent_path)
    # Read from cloned DB
    child_db = SlateDB(child_path, env_file=env_file)
    assert child_db.get(b"cK") == b"cV"
    child_db.close()

    # Async variant
    admin2 = SlateDBAdmin(str(tmp_path / "child_db2"), env_file=env_file)
    await admin2.create_clone_async(parent_path)
    child2 = SlateDB(str(tmp_path / "child_db2"), env_file=env_file)
    assert child2.get(b"cK") == b"cV"
    child2.close()

def test_admin_run_gc_variants(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"gk", b"gv")
    db.close()
    admin = SlateDBAdmin(db_path, env_file=env_file)
    # run once with min_age only
    admin.run_gc_once(manifest_min_age=0, wal_min_age=0, compacted_min_age=0)
    # schedule background with no options (no-ops but should not raise)
    admin.run_gc()

@pytest.mark.asyncio
async def test_admin_run_gc_variants_async(db_path, env_file):
    db = SlateDB(db_path, env_file=env_file)
    db.put(b"gka", b"gv")
    db.close()
    admin = SlateDBAdmin(db_path, env_file=env_file)
    await admin.run_gc_once_async(manifest_min_age=0, wal_min_age=0, compacted_min_age=0)
    await admin.run_gc_async()
