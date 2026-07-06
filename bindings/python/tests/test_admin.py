from __future__ import annotations

import asyncio
import time

import pytest

from conftest import new_memory_store, open_db, open_reader, unique_path, wait_until
from slatedb.uniffi import (
    AdminBuilder,
    CheckpointOptions,
    CloneSourceSpec,
    DbBuilder,
    Error,
    FlushOptions,
    FlushType,
    GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions,
    GarbageCollectorScheduleOptions,
)

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

        reversed_range = await admin.list_manifests(3, 2)
        assert reversed_range == []

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

    empty_range = await admin.list_compactions(2, 2)
    assert empty_range == []

    with pytest.raises(Error.Invalid) as invalid_compaction_id:
        await admin.read_compaction("not-a-ulid", None)
    assert "invalid compaction_id ULID" in invalid_compaction_id.value.message


@pytest.mark.asyncio
async def test_admin_run_gc_once_accepts_default_and_custom_options() -> None:
    path = unique_path("admin-run-gc-once")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key", b"value")
        await db.flush()

        await admin.run_gc_once(None)

        directory_options = GarbageCollectorDirectoryOptions(
            interval_ms=None,
            min_age_ms=0,
            dry_run=True,
        )
        schedule_options = GarbageCollectorScheduleOptions(interval_ms=None)
        options = GarbageCollectorOptions(
            manifest_options=None,
            wal_options=directory_options,
            wal_fence_options=directory_options,
            compacted_options=None,
            compactions_options=None,
            detach_options=schedule_options,
        )

        await admin.run_gc_once(options)


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


@pytest.mark.asyncio
async def test_admin_clone() -> None:
    sources = []
    store = new_memory_store()

    for i in range(3):
        path = unique_path(f"admin-clone-original-{i}")
        async with open_db(store, path=path) as db:
            await db.put(f"k{i}".encode("utf-8"), f"v{i}".encode("utf-8"))
            await db.flush()
        sources.append(CloneSourceSpec(path=path, checkpoint=None, projection_range=None))


    clone_path = unique_path("admin-clone-clone")
    admin = AdminBuilder(clone_path, store).build()
    clone_builder = admin.create_clone_builder_from_source(sources[0])
    for source in sources[1:]:
        clone_builder.with_source(source)
    await clone_builder.build()

    async with open_db(store, path=clone_path) as db:
        for i in range(3):
            assert await db.get(f"k{i}".encode("utf-8")) == f"v{i}".encode("utf-8")


@pytest.mark.asyncio
async def test_admin_create_detached_checkpoint_without_options() -> None:
    path = unique_path("admin-checkpoint-create")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        options = CheckpointOptions(lifetime_ms=None, source=None, name=None)
        result = await admin.create_detached_checkpoint(options)

        assert result is not None
        assert result.id
        assert result.manifest_id > 0

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 1
        assert checkpoints[0].id == result.id
        assert checkpoints[0].manifest_id == result.manifest_id
        assert checkpoints[0].name is None
        assert checkpoints[0].expire_time_secs is None


@pytest.mark.asyncio
async def test_admin_create_detached_checkpoint_with_lifetime() -> None:
    path = unique_path("admin-checkpoint-lifetime")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        lifetime_ms = 60_000
        options = CheckpointOptions(lifetime_ms=lifetime_ms, source=None, name=None)
        result = await admin.create_detached_checkpoint(options)

        assert result is not None
        assert result.id

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]
        assert checkpoint.id == result.id
        assert checkpoint.expire_time_secs is not None
        assert checkpoint.expire_time_secs > checkpoint.create_time_secs

        expected_expire_secs = checkpoint.create_time_secs + (lifetime_ms // 1000)
        delta = abs(checkpoint.expire_time_secs - expected_expire_secs)
        assert delta <= 2, "Expire time should be approximately create time + lifetime"


@pytest.mark.asyncio
async def test_admin_create_detached_checkpoint_with_name() -> None:
    path = unique_path("admin-checkpoint-name")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        checkpoint_name = "backup-2026-06-25"
        options = CheckpointOptions(lifetime_ms=None, source=None, name=checkpoint_name)
        result = await admin.create_detached_checkpoint(options)

        assert result is not None
        assert result.id

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 1
        assert checkpoints[0].id == result.id
        assert checkpoints[0].name == checkpoint_name

        filtered_by_name = await admin.list_checkpoints(checkpoint_name)
        assert len(filtered_by_name) == 1
        assert filtered_by_name[0].id == result.id

        filtered_other = await admin.list_checkpoints("other-name")
        assert len(filtered_other) == 0


@pytest.mark.asyncio
async def test_admin_create_detached_checkpoint_from_source() -> None:
    path = unique_path("admin-checkpoint-source")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        first_options = CheckpointOptions(lifetime_ms=None, source=None, name="first-checkpoint")
        first_result = await admin.create_detached_checkpoint(first_options)

        second_options = CheckpointOptions(
            lifetime_ms=120_000, source=first_result.id, name="second-checkpoint"
        )
        second_result = await admin.create_detached_checkpoint(second_options)

        assert second_result is not None
        assert second_result.id
        assert second_result.manifest_id == first_result.manifest_id

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 2


@pytest.mark.asyncio
async def test_admin_refresh_checkpoint_updates_lifetime() -> None:
    path = unique_path("admin-checkpoint-refresh")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        initial_lifetime_ms = 30_000
        options = CheckpointOptions(
            lifetime_ms=initial_lifetime_ms, source=None, name="refresh-test"
        )
        result = await admin.create_detached_checkpoint(options)

        checkpoints = await admin.list_checkpoints(None)
        initial = checkpoints[0]
        initial_expire_time = initial.expire_time_secs
        assert initial_expire_time is not None

        await asyncio.sleep(1)

        new_lifetime_ms = 90_000
        await admin.refresh_checkpoint(result.id, new_lifetime_ms)

        updated_checkpoints = await admin.list_checkpoints(None)
        refreshed = updated_checkpoints[0]
        assert refreshed.expire_time_secs is not None
        assert (
            refreshed.expire_time_secs > initial_expire_time
        ), "Refreshed expire time should be later than initial"


@pytest.mark.asyncio
async def test_admin_refresh_checkpoint_without_lifetime() -> None:
    path = unique_path("admin-checkpoint-refresh-no-lifetime")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        options = CheckpointOptions(lifetime_ms=None, source=None, name=None)
        result = await admin.create_detached_checkpoint(options)

        await admin.refresh_checkpoint(result.id, None)

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 1
        assert checkpoints[0].id == result.id


@pytest.mark.asyncio
async def test_admin_refresh_checkpoint_with_invalid_id_fails() -> None:
    path = unique_path("admin-checkpoint-refresh-invalid")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        with pytest.raises(Error.Invalid) as error:
            await admin.refresh_checkpoint("invalid-uuid", 60_000)
        assert "invalid checkpoint_id" in error.value.message


@pytest.mark.asyncio
async def test_admin_delete_checkpoint_removes_checkpoint() -> None:
    path = unique_path("admin-checkpoint-delete")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        options = CheckpointOptions(lifetime_ms=None, source=None, name="delete-test")
        result = await admin.create_detached_checkpoint(options)

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 1
        assert checkpoints[0].id == result.id

        await admin.delete_checkpoint(result.id)

        async def checkpoints_cleared() -> bool:
            return await admin.list_checkpoints(None) == []

        await wait_until(checkpoints_cleared)


@pytest.mark.asyncio
async def test_admin_delete_checkpoint_with_invalid_id_fails() -> None:
    path = unique_path("admin-checkpoint-delete-invalid")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        with pytest.raises(Error.Invalid) as error:
            await admin.delete_checkpoint("invalid-uuid")
        assert "invalid checkpoint_id" in error.value.message


@pytest.mark.asyncio
async def test_admin_delete_multiple_checkpoints() -> None:
    path = unique_path("admin-checkpoint-delete-multiple")
    store = new_memory_store()
    admin = AdminBuilder(path, store).build()

    async with open_db(store, path=path) as db:
        await db.put(b"key1", b"value1")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        first = await admin.create_detached_checkpoint(
            CheckpointOptions(lifetime_ms=None, source=None, name="checkpoint-1")
        )
        second = await admin.create_detached_checkpoint(
            CheckpointOptions(lifetime_ms=None, source=None, name="checkpoint-2")
        )
        third = await admin.create_detached_checkpoint(
            CheckpointOptions(lifetime_ms=None, source=None, name="checkpoint-3")
        )

        checkpoints = await admin.list_checkpoints(None)
        assert len(checkpoints) == 3

        await admin.delete_checkpoint(first.id)
        checkpoints = await admin.list_checkpoints(None)
        assert all(c.id != first.id for c in checkpoints)

        await admin.delete_checkpoint(second.id)
        await admin.delete_checkpoint(third.id)

        async def checkpoints_cleared() -> bool:
            return await admin.list_checkpoints(None) == []

        await wait_until(checkpoints_cleared)
