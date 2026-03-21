from __future__ import annotations

import pytest

from conftest import LogCollector, new_memory_store, open_db, unique_path, wait_until
from slatedb.uniffi import Error, LogLevel, init_logging


@pytest.mark.asyncio
async def test_logging_callback_delivery_and_duplicate_init_rejection() -> None:
    collector = LogCollector()

    init_logging(LogLevel.INFO, collector)

    with pytest.raises(Error.Invalid) as exc:
        init_logging(LogLevel.INFO, collector)
    assert exc.value.message == "logging already initialized"

    matched = None
    path = unique_path("test-db-logging")

    async with open_db(new_memory_store(), path=path) as _db:
        async def has_open_record() -> bool:
            nonlocal matched
            matched = collector.matching_record(
                lambda record: record.level == LogLevel.INFO
                and "opening SlateDB database" in record.message
                and path in record.message
            )
            return matched is not None

        await wait_until(has_open_record)

    assert matched is not None
    assert matched.target
    assert matched.module_path
    assert matched.file
    assert matched.line is not None
    assert matched.line > 0
