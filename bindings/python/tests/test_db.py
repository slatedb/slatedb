from __future__ import annotations

import pytest

from conftest import (
    ConcatMergeOperator,
    TEST_DB_PATH,
    drain_iterator,
    merge_options,
    new_memory_store,
    open_db,
    put_options,
    read_options,
    require_rows,
    scan_options,
    write_options,
)
from slatedb.uniffi import (
    CloseReason,
    DbBuilder,
    Error,
    FlushOptions,
    FlushType,
    IsolationLevel,
    KeyRange,
    WriteBatch,
)


@pytest.mark.asyncio
async def test_db_lifecycle_and_status() -> None:
    store = new_memory_store()
    db = await DbBuilder(TEST_DB_PATH, store).build()

    status = db.status()
    assert status.close_reason is None
    await db.put(b"lifecycle", b"value")
    await db.shutdown()

    status = db.status()
    assert status.close_reason == CloseReason.CLEAN

    with pytest.raises(Error.Closed) as exc:
        await db.put(b"after-shutdown", b"value")
    assert exc.value.reason == CloseReason.CLEAN
    assert exc.value.message == "Closed error: db is closed"


@pytest.mark.asyncio
async def test_db_crud_and_metadata() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        first_write = await db.put(b"alpha", b"one")
        assert first_write.seqnum > 0
        assert first_write.create_ts > 0

        assert await db.get(b"alpha") == b"one"
        assert await db.get_with_options(b"alpha", read_options()) == b"one"

        metadata = await db.get_key_value(b"alpha")
        assert metadata is not None
        assert metadata.key == b"alpha"
        assert metadata.value == b"one"
        assert metadata.seq == first_write.seqnum
        assert metadata.create_ts == first_write.create_ts

        metadata_with_options = await db.get_key_value_with_options(b"alpha", read_options())
        assert metadata_with_options is not None
        assert metadata_with_options.value == b"one"

        second_write = await db.put_with_options(
            b"beta",
            b"two",
            put_options(),
            write_options(),
        )
        assert second_write.seqnum > first_write.seqnum
        assert second_write.create_ts > 0
        assert await db.get(b"beta") == b"two"

        await db.put(b"empty", b"")
        assert await db.get(b"empty") == b""
        assert await db.get(b"missing") is None

        delete_alpha = await db.delete(b"alpha")
        assert delete_alpha.seqnum > second_write.seqnum
        assert await db.get(b"alpha") is None

        delete_beta = await db.delete_with_options(b"beta", write_options())
        assert delete_beta.seqnum > second_write.seqnum
        assert await db.get(b"beta") is None


@pytest.mark.asyncio
async def test_db_scan_variants() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"item:01", b"first")
        await db.put(b"item:02", b"second")
        await db.put(b"item:03", b"third")
        await db.put(b"other:01", b"other")

        full_scan = await db.scan(
            KeyRange(
                start=None,
                start_inclusive=False,
                end=None,
                end_inclusive=False,
            )
        )
        require_rows(
            await drain_iterator(full_scan),
            ["item:01", "item:02", "item:03", "other:01"],
            ["first", "second", "third", "other"],
        )

        bounded_scan = await db.scan(
            KeyRange(
                start=b"item:02",
                start_inclusive=True,
                end=b"item:03",
                end_inclusive=True,
            )
        )
        require_rows(
            await drain_iterator(bounded_scan),
            ["item:02", "item:03"],
            ["second", "third"],
        )

        scan_with_custom_options = await db.scan_with_options(
            KeyRange(
                start=b"item:01",
                start_inclusive=True,
                end=b"item:99",
                end_inclusive=False,
            ),
            scan_options(64, True, 2),
        )
        require_rows(
            await drain_iterator(scan_with_custom_options),
            ["item:01", "item:02", "item:03"],
            ["first", "second", "third"],
        )

        prefix_scan = await db.scan_prefix(b"item:")
        require_rows(
            await drain_iterator(prefix_scan),
            ["item:01", "item:02", "item:03"],
            ["first", "second", "third"],
        )

        prefix_scan_with_options = await db.scan_prefix_with_options(
            b"item:",
            scan_options(32, False, 1),
        )
        require_rows(
            await drain_iterator(prefix_scan_with_options),
            ["item:01", "item:02", "item:03"],
            ["first", "second", "third"],
        )


@pytest.mark.asyncio
async def test_db_batch_write_and_consumption() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"remove-me", b"old")

        batch = WriteBatch()
        batch.put(b"batch-put", b"value")
        batch.delete(b"remove-me")

        batch_write = await db.write(batch)
        assert batch_write.seqnum > 0
        assert await db.get(b"batch-put") == b"value"
        assert await db.get(b"remove-me") is None

        with pytest.raises(Error.Invalid) as exc:
            await db.write(batch)
        assert exc.value.message == "write batch has already been consumed"

        second_batch = WriteBatch()
        second_batch.put_with_options(b"batch-put-2", b"value-2", put_options())
        await db.write_with_options(second_batch, write_options())
        assert await db.get(b"batch-put-2") == b"value-2"


@pytest.mark.asyncio
async def test_db_flush() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"flush-key", b"value")
        await db.flush()
        await db.flush_with_options(FlushOptions(flush_type=FlushType.WAL))
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))


@pytest.mark.asyncio
async def test_db_merge_and_merge_with_options() -> None:
    store = new_memory_store()

    async with open_db(
        store,
        configure=lambda builder: builder.with_merge_operator(ConcatMergeOperator()),
    ) as db:
        await db.put(b"merge", b"base")
        await db.merge(b"merge", b":one")
        assert await db.get(b"merge") == b"base:one"

        await db.merge_with_options(
            b"merge",
            b":two",
            merge_options(),
            write_options(),
        )
        assert await db.get(b"merge") == b"base:one:two"


@pytest.mark.asyncio
async def test_db_snapshot_isolation() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"snapshot", b"old")

        snapshot = await db.snapshot()
        await db.put(b"snapshot", b"new")

        assert await snapshot.get(b"snapshot") == b"old"
        assert await db.get(b"snapshot") == b"new"


@pytest.mark.asyncio
async def test_db_transactions() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        tx = await db.begin(IsolationLevel.SNAPSHOT)
        assert tx.id()

        await tx.put(b"txn-key", b"pending")
        assert await tx.get(b"txn-key") == b"pending"
        assert await db.get(b"txn-key") is None

        commit_handle = await tx.commit()
        assert commit_handle is not None
        assert commit_handle.seqnum > 0
        assert await db.get(b"txn-key") == b"pending"

        rollback_tx = await db.begin(IsolationLevel.SNAPSHOT)
        await rollback_tx.put(b"rolled-back", b"value")
        await rollback_tx.rollback()
        assert await db.get(b"rolled-back") is None


@pytest.mark.asyncio
async def test_db_invalid_inputs_map_to_typed_errors() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        with pytest.raises(Error.Invalid) as exc:
            await db.put(b"", b"value")
        assert exc.value.message == "key cannot be empty"

        with pytest.raises(Error.Invalid) as exc:
            await db.delete(b"")
        assert exc.value.message == "key cannot be empty"

        with pytest.raises(Error.Invalid) as exc:
            await db.scan(
                KeyRange(
                    start=b"z",
                    start_inclusive=True,
                    end=b"a",
                    end_inclusive=True,
                )
            )
        assert exc.value.message == "range start must not be greater than range end"

        with pytest.raises(Error.Invalid) as exc:
            await db.scan(
                KeyRange(
                    start=b"a",
                    start_inclusive=True,
                    end=b"a",
                    end_inclusive=False,
                )
            )
        assert exc.value.message == "range must be non-empty"

        # Scan with empty start bound should succeed and be treated as unbounded start.
        await db.put(b"seed", b"value")
        scan = await db.scan(
            KeyRange(start=b"", start_inclusive=True, end=None, end_inclusive=False)
        )
        require_rows(await drain_iterator(scan), ["seed"], ["value"])


@pytest.mark.asyncio
async def test_db_writer_fencing_reports_closed_reason() -> None:
    store = new_memory_store()

    primary = await DbBuilder(TEST_DB_PATH, store).build()
    await primary.put(b"primary", b"value")

    secondary = await DbBuilder(TEST_DB_PATH, store).build()
    await secondary.put(b"secondary", b"value")

    with pytest.raises(Error.Closed) as exc:
        await primary.put(b"stale", b"value")
    assert exc.value.reason == CloseReason.FENCED
    assert "detected newer DB client" in exc.value.message

    await secondary.shutdown()
