from __future__ import annotations

from pathlib import Path

import pytest
from conftest import new_memory_store, open_db

from slatedb.uniffi import (
    Error,
    FlushOptions,
    FlushType,
    ObjectStoreMirrorBuilder,
    SegmentFilter,
)


@pytest.mark.asyncio
async def test_mirror_keeps_flushed_ssts_local(tmp_path: Path) -> None:
    mirror = await ObjectStoreMirrorBuilder(str(tmp_path), new_memory_store()).build()

    async with open_db(mirror) as db:
        await db.put(b"key", b"value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))
    assert list(tmp_path.glob("*.obj"))

    # The same mirror serves each open of its database.
    async with open_db(mirror) as db:
        assert await db.get(b"key") == b"value"


@pytest.mark.asyncio
async def test_mirror_rejects_locked_directory(tmp_path: Path) -> None:
    store = new_memory_store()
    _mirror = await ObjectStoreMirrorBuilder(str(tmp_path), store).build()

    with pytest.raises(Error.Unavailable) as exc:
        await ObjectStoreMirrorBuilder(str(tmp_path), store).build()
    assert "locked" in exc.value.message


class RejectingFilter(SegmentFilter):
    def __init__(self) -> None:
        self.asked: list[bytes] = []

    def should_mirror(self, segment: bytes) -> bool:
        self.asked.append(segment)
        return False


class FailingFilter(SegmentFilter):
    def should_mirror(self, segment: bytes) -> bool:
        raise ValueError("boom")


async def assert_nothing_mirrored(tmp_path: Path, segment_filter: SegmentFilter) -> None:
    builder = ObjectStoreMirrorBuilder(str(tmp_path), new_memory_store())
    builder.with_segment_filter(segment_filter)
    mirror = await builder.build()

    async with open_db(mirror) as db:
        await db.put(b"key", b"value")
        await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))
        assert await db.get(b"key") == b"value"
    assert not list(tmp_path.glob("*.obj"))


@pytest.mark.asyncio
async def test_mirror_skips_segments_that_filter_rejects(tmp_path: Path) -> None:
    segment_filter = RejectingFilter()
    await assert_nothing_mirrored(tmp_path, segment_filter)
    # A database without a segment extractor has only the root segment.
    assert set(segment_filter.asked) == {b""}


@pytest.mark.asyncio
async def test_mirror_skips_segments_when_filter_raises(tmp_path: Path) -> None:
    await assert_nothing_mirrored(tmp_path, FailingFilter())
