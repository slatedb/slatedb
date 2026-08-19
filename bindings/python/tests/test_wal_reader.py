from __future__ import annotations

import pytest
from conftest import (
    TEST_DB_PATH,
    append_wal_value,
    new_memory_store,
    read_wal_batches_through,
    require_wal_row,
    seed_wal_files,
)

from slatedb.uniffi import RowEntryKind, SlateDbWalReader


@pytest.mark.asyncio
async def test_wal_reader_reports_no_new_files_after_cursor() -> None:
    store = new_memory_store()
    await seed_wal_files(store)
    reader = SlateDbWalReader(TEST_DB_PATH, store)

    cursor = await reader.last_wal_file_id(0)
    assert await reader.last_wal_file_id(cursor) == cursor


@pytest.mark.asyncio
async def test_wal_reader_streams_new_wals_through_one_iterator() -> None:
    store = new_memory_store()
    await seed_wal_files(store)
    reader = SlateDbWalReader(TEST_DB_PATH, store)

    first_tail = await reader.last_wal_file_id(0)
    iterator = await reader.iterator(1)
    first_batches = await read_wal_batches_through(iterator, first_tail)
    assert first_batches
    assert any(not batch.rows for batch in first_batches)
    first_cursors = [batch.last_consumed_wal_file_id for batch in first_batches]
    assert first_cursors[-1] == first_tail
    assert first_cursors == sorted(set(first_cursors))
    assert await reader.last_wal_file_id(first_tail) == first_tail

    await append_wal_value(store, b"next", b"3")
    second_tail = await reader.last_wal_file_id(first_tail)
    assert second_tail > first_tail
    second_batches = await read_wal_batches_through(iterator, second_tail)
    assert second_batches
    assert second_batches[-1].last_consumed_wal_file_id == second_tail
    assert [row.key for batch in second_batches for row in batch.rows] == [b"next"]
    assert await reader.last_wal_file_id(second_tail) == second_tail


@pytest.mark.asyncio
async def test_wal_reader_decodes_value_tombstone_and_merge_rows() -> None:
    store = new_memory_store()
    await seed_wal_files(store)
    reader = SlateDbWalReader(TEST_DB_PATH, store)

    tail = await reader.last_wal_file_id(0)
    batches = await read_wal_batches_through(await reader.iterator(1), tail)
    assert batches[-1].last_consumed_wal_file_id == tail
    rows = [row for batch in batches for row in batch.rows]

    assert len(rows) == 4
    assert all(row.seq > 0 for row in rows)
    require_wal_row(rows[0], RowEntryKind.VALUE, "a", "1")
    require_wal_row(rows[1], RowEntryKind.VALUE, "b", "2")
    require_wal_row(rows[2], RowEntryKind.TOMBSTONE, "a", None)
    require_wal_row(rows[3], RowEntryKind.MERGE, "m", "x")


@pytest.mark.asyncio
async def test_wal_reader_can_start_at_the_next_wal() -> None:
    store = new_memory_store()
    await seed_wal_files(store)
    reader = SlateDbWalReader(TEST_DB_PATH, store)
    tail = await reader.last_wal_file_id(0)

    iterator = await reader.iterator(tail + 1)
    await append_wal_value(store, b"resumed", b"4")
    new_tail = await reader.last_wal_file_id(tail)
    batches = await read_wal_batches_through(iterator, new_tail)
    assert [row.key for batch in batches for row in batch.rows] == [b"resumed"]
