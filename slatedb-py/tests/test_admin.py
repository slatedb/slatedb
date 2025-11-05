import pytest
from slatedb import SlateDB, SlateDBReader, SlateDBAdmin, InvalidError


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

