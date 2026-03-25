from __future__ import annotations

import pytest

from conftest import (
    TEST_DB_PATH,
    drain_wal_iterator,
    new_memory_store,
    require_wal_row,
    seed_wal_files,
)
from slatedb.uniffi import Error, RowEntryKind, WalReader


@pytest.mark.asyncio
async def test_wal_reader_empty_store_listing() -> None:
    reader = WalReader(TEST_DB_PATH, new_memory_store())
    assert await reader.list(None, None) == []


@pytest.mark.asyncio
async def test_wal_reader_listing_bounds_and_navigation() -> None:
    store = new_memory_store()
    await seed_wal_files(store)

    reader = WalReader(TEST_DB_PATH, store)
    files = await reader.list(None, None)
    assert len(files) >= 3

    ids = [wal_file.id() for wal_file in files]
    assert ids == sorted(ids)

    bounded = await reader.list(ids[1], ids[2])
    assert [wal_file.id() for wal_file in bounded] == [ids[1]]

    assert await reader.list(ids[-1] + 1_000, None) == []

    first = reader.get(ids[0])
    assert first.id() == ids[0]
    assert first.next_id() == ids[1]

    next_file = first.next_file()
    assert next_file.id() == ids[1]


@pytest.mark.asyncio
async def test_wal_reader_metadata_and_row_decoding() -> None:
    store = new_memory_store()
    await seed_wal_files(store)

    reader = WalReader(TEST_DB_PATH, store)
    files = await reader.list(None, None)
    assert len(files) >= 3

    all_rows = []
    for wal_file in files:
        metadata = await wal_file.metadata()
        assert metadata.size_bytes > 0
        assert metadata.location

        rows = await drain_wal_iterator(await wal_file.iterator())
        assert all(row.seq > 0 for row in rows)
        all_rows.extend(rows)

    assert len(all_rows) == 4
    require_wal_row(all_rows[0], RowEntryKind.VALUE, "a", "1")
    require_wal_row(all_rows[1], RowEntryKind.VALUE, "b", "2")
    require_wal_row(all_rows[2], RowEntryKind.TOMBSTONE, "a", None)
    require_wal_row(all_rows[3], RowEntryKind.MERGE, "m", "x")


@pytest.mark.asyncio
async def test_wal_reader_missing_file_metadata_failure() -> None:
    store = new_memory_store()
    await seed_wal_files(store)

    reader = WalReader(TEST_DB_PATH, store)
    files = await reader.list(None, None)
    assert files

    missing = reader.get(files[-1].id() + 1_000)
    assert missing.id() == files[-1].id() + 1_000

    with pytest.raises(Error.Data) as exc:
        await missing.metadata()
    assert "not found" in exc.value.message
