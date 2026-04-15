from __future__ import annotations

import pytest

from conftest import (
    ConcatMergeOperator,
    TEST_DB_PATH,
    drain_iterator,
    new_memory_store,
    open_db,
    open_reader,
    read_options,
    reader_options,
    require_rows,
    scan_options,
    wait_until,
)
from slatedb.uniffi import (
    CloseReason,
    DbReaderBuilder,
    Error,
    FlushOptions,
    FlushType,
    KeyRange,
)


@pytest.mark.asyncio
async def test_reader_lifecycle() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"reader", b"value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        builder = DbReaderBuilder(TEST_DB_PATH, store)
        reader = await builder.build()

        assert await reader.get(b"reader") == b"value"
        await reader.shutdown()

        with pytest.raises(Error.Closed) as exc:
            await reader.get(b"reader")
        assert exc.value.reason == CloseReason.CLEAN
        assert exc.value.message == "Closed error: db is closed"


@pytest.mark.asyncio
async def test_reader_build_fails_when_database_is_missing() -> None:
    builder = DbReaderBuilder(TEST_DB_PATH, new_memory_store())

    with pytest.raises(Error.Data) as exc:
        await builder.build()
    assert "failed to find latest transactional object" in exc.value.message


@pytest.mark.asyncio
async def test_reader_point_reads() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"alpha", b"one")
        await db.put(b"empty", b"")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        async with open_reader(store) as reader:
            assert await reader.get(b"alpha") == b"one"
            assert await reader.get_with_options(b"alpha", read_options()) == b"one"
            assert await reader.get(b"empty") == b""
            assert await reader.get(b"missing") is None


@pytest.mark.asyncio
async def test_reader_scan_variants() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"item:01", b"first")
        await db.put(b"item:02", b"second")
        await db.put(b"item:03", b"third")
        await db.put(b"other:01", b"other")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        async with open_reader(store) as reader:
            full_scan = await reader.scan(
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

            bounded_scan = await reader.scan(
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

            scan_with_custom_options = await reader.scan_with_options(
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

            prefix_scan = await reader.scan_prefix(b"item:")
            require_rows(
                await drain_iterator(prefix_scan),
                ["item:01", "item:02", "item:03"],
                ["first", "second", "third"],
            )

            prefix_scan_with_options = await reader.scan_prefix_with_options(
                b"item:",
                scan_options(32, False, 1),
            )
            require_rows(
                await drain_iterator(prefix_scan_with_options),
                ["item:01", "item:02", "item:03"],
                ["first", "second", "third"],
            )


@pytest.mark.asyncio
async def test_reader_refresh_polling_updates_visible_state() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"seed", b"value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        async with open_reader(
            store,
            configure=lambda builder: builder.with_options(reader_options(False)),
        ) as reader:
            await db.put(b"refresh", b"updated")
            await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

            async def has_refreshed() -> bool:
                return await reader.get(b"refresh") == b"updated"

            await wait_until(has_refreshed)


@pytest.mark.asyncio
async def test_reader_default_mode_replays_new_wal_data() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        async with open_reader(
            store,
            configure=lambda builder: builder.with_options(reader_options(False)),
        ) as reader:
            await db.put(b"wal-key", b"wal-value")
            await db.flush_with_options(FlushOptions(flush_type=FlushType.WAL))

            async def has_wal_value() -> bool:
                return await reader.get(b"wal-key") == b"wal-value"

            await wait_until(has_wal_value)


@pytest.mark.asyncio
async def test_reader_skip_wal_replay_ignores_wal_only_data() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"flushed-key", b"flushed-value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        await db.put(b"wal-only-key", b"wal-only-value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.WAL))

        async with open_reader(
            store,
            configure=lambda builder: builder.with_options(reader_options(True)),
        ) as reader:
            assert await reader.get(b"flushed-key") == b"flushed-value"
            assert await reader.get(b"wal-only-key") is None

        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        async with open_reader(
            store,
            configure=lambda builder: builder.with_options(reader_options(True)),
        ) as reader:
            assert await reader.get(b"wal-only-key") == b"wal-only-value"


@pytest.mark.asyncio
async def test_reader_merge_operator() -> None:
    store = new_memory_store()

    async with open_db(
        store,
        configure=lambda builder: builder.with_merge_operator(ConcatMergeOperator()),
    ) as db:
        await db.put(b"merge", b"base")
        await db.merge(b"merge", b":reader")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        async with open_reader(
            store,
            configure=lambda builder: builder.with_merge_operator(ConcatMergeOperator()),
        ) as reader:
            assert await reader.get(b"merge") == b"base:reader"


@pytest.mark.asyncio
async def test_reader_builder_validation_and_errors() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"seed", b"value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        invalid_builder = DbReaderBuilder(TEST_DB_PATH, store)
        with pytest.raises(Error.Invalid) as exc:
            invalid_builder.with_checkpoint_id("not-a-uuid")
        assert exc.value.message.startswith("invalid checkpoint_id UUID:")

        missing_builder = DbReaderBuilder(TEST_DB_PATH, store)
        missing_builder.with_checkpoint_id("ffffffff-ffff-ffff-ffff-ffffffffffff")
        with pytest.raises(Error.Data) as exc:
            await missing_builder.build()
        assert "checkpoint missing" in exc.value.message
        assert "ffffffff-ffff-ffff-ffff-ffffffffffff" in exc.value.message

        consumed_builder = DbReaderBuilder(TEST_DB_PATH, store)
        reader = await consumed_builder.build()
        await reader.shutdown()

        with pytest.raises(Error.Invalid) as exc:
            await consumed_builder.build()
        assert exc.value.message == "builder has already been consumed"


@pytest.mark.asyncio
async def test_reader_invalid_ranges_raise_invalid_errors() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"seed", b"value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

        async with open_reader(store) as reader:
            with pytest.raises(Error.Invalid) as exc:
                await reader.scan(
                    KeyRange(
                        start=b"z",
                        start_inclusive=True,
                        end=b"a",
                        end_inclusive=True,
                    )
                )
            assert exc.value.message == "range start must not be greater than range end"

            with pytest.raises(Error.Invalid) as exc:
                await reader.scan(
                    KeyRange(
                        start=b"a",
                        start_inclusive=True,
                        end=b"a",
                        end_inclusive=False,
                    )
                )
            assert exc.value.message == "range must be non-empty"

            # Scan with empty start bound should succeed and be treated as unbounded start.
            scan = await reader.scan(
                KeyRange(start=b"", start_inclusive=True, end=None, end_inclusive=False)
            )
            require_rows(await drain_iterator(scan), ["seed"], ["value"])
