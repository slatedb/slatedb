from __future__ import annotations

from pathlib import Path

import pytest
from conftest import new_memory_store, open_db

from slatedb.uniffi import Error, FlushOptions, FlushType, ObjectStoreMirrorBuilder


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
