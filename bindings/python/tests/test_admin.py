from __future__ import annotations

import time

import pytest

from conftest import new_memory_store, open_db, open_reader, unique_path, wait_until
from slatedb.uniffi import AdminBuilder, DbBuilder, Error, FlushOptions, FlushType

MAX_I64 = 9_223_372_036_854_775_807
MAX_U64 = 18_446_744_073_709_551_615
VALID_ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"


@pytest.mark.asyncio
async def test_admin_builder_accepts_configuration_and_is_single_use() -> None:
    path = unique_path("admin-builder")
    main_store = new_memory_store()
    wal_store = new_memory_store()

    db_builder = DbBuilder(path, main_store)
    db_builder.with_wal_object_store(wal_store)
    db = await db_builder.build()
    try:
        await db.shutdown()

        admin_builder = AdminBuilder(path, main_store)
        admin_builder.with_seed(7)
        admin_builder.with_wal_object_store(wal_store)

        admin = admin_builder.build()
        latest = await admin.read_manifest(None)
        assert latest is not None
        assert latest.id > 0

        with pytest.raises(Error.Invalid) as configure_after_build:
            admin_builder.with_seed(9)
        assert "builder has already been consumed" in configure_after_build.value.message

        with pytest.raises(Error.Invalid) as second_build:
            admin_builder.build()
        assert "builder has already been consumed" in second_build.value.message
    finally:
        if db.status().close_reason is None:
            await db.shutdown()


@pytest.mark.asyncio
async def test_admin_manifest_read_list_and_state_view() -> None:
    path = unique_path("admin-manifest")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        first_write = await db.put(b"alpha", b"one")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        second_write = await db.put(b"beta", b"two")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        latest = await admin.read_manifest(None)
        assert latest is not None
        assert latest.id >= 3
        assert latest.last_l0_seq >= second_write.seqnum

        first = await admin.read_manifest(1)
        assert first is not None
        assert first.id == 1
        assert first.last_l0_seq == 0

        assert await admin.read_manifest(99_999) is None

        manifests = await admin.list_manifests(None, None)
        assert len(manifests) >= 3
        assert manifests[0].id == 1
        assert manifests[-1].id == latest.id

        bounded = await admin.list_manifests(2, 3)
        assert [manifest.id for manifest in bounded] == [2]

        with pytest.raises(Error.Invalid) as invalid_range:
            await admin.list_manifests(3, 2)
        assert "range start must not be greater than range end" in invalid_range.value.message

        state_view = await admin.read_compactor_state_view()
        assert state_view.manifest.id == latest.id
        assert first_write.seqnum > 0


@pytest.mark.asyncio
async def test_admin_compaction_queries_handle_empty_store_and_invalid_ids() -> None:
    path = unique_path("admin-compactions")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    assert await admin.read_compactions(None) is None
    assert await admin.read_compactions(1) is None
    assert await admin.read_compaction(VALID_ULID, None) is None

    compactions = await admin.list_compactions(None, None)
    assert compactions == []

    with pytest.raises(Error.Invalid) as invalid_range:
        await admin.list_compactions(2, 2)
    assert "range must be non-empty" in invalid_range.value.message

    with pytest.raises(Error.Invalid) as invalid_compaction_id:
        await admin.read_compaction("not-a-ulid", None)
    assert "invalid compaction_id ULID" in invalid_compaction_id.value.message


@pytest.mark.asyncio
async def test_admin_checkpoint_listing_tracks_reader_lifecycle() -> None:
    path = unique_path("admin-checkpoints")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()
    db = await DbBuilder(path, store).build()

    try:
        await db.shutdown()

        async with open_reader(store, path=path) as reader:
            checkpoints = await admin.list_checkpoints(None)
            assert len(checkpoints) == 1

            checkpoint = checkpoints[0]
            assert checkpoint.id
            assert checkpoint.manifest_id > 0
            assert checkpoint.create_time_secs > 0
            assert checkpoint.name is None

            unnamed = await admin.list_checkpoints("")
            assert len(unnamed) == 1
            assert unnamed[0].id == checkpoint.id

            missing = await admin.list_checkpoints("non-existent")
            assert missing == []

            assert reader is not None

        async def checkpoints_cleared() -> bool:
            return await admin.list_checkpoints(None) == []

        await wait_until(checkpoints_cleared)
    finally:
        if db.status().close_reason is None:
            await db.shutdown()


@pytest.mark.asyncio
async def test_admin_sequence_lookups_use_persisted_tracker() -> None:
    path = unique_path("admin-seq-tracker")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        first_write = await db.put(b"k1", b"v1")
        await db.put(b"k2", b"v2")
        third_write = await db.put(b"k3", b"v3")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        first_timestamp = await admin.get_timestamp_for_sequence(first_write.seqnum, True)
        assert first_timestamp is not None

        after_last_timestamp = await admin.get_timestamp_for_sequence(MAX_U64, True)
        assert after_last_timestamp is None

        seq_before_first = await admin.get_sequence_for_timestamp(1, False)
        assert seq_before_first is None

        seq_after_last = await admin.get_sequence_for_timestamp(int(time.time()) + 86_400, False)
        assert seq_after_last is not None
        assert seq_after_last > 0
        assert third_write.seqnum > first_write.seqnum

        with pytest.raises(Error.Invalid) as invalid_timestamp:
            await admin.get_sequence_for_timestamp(MAX_I64, False)
        assert "invalid timestamp seconds" in invalid_timestamp.value.message
