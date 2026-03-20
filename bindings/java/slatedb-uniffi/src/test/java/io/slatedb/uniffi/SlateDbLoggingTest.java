package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

@ResourceLock("slatedb.logging")
class SlateDbLoggingTest {
    @Test
    void loggingCallbackReceivesOpenRecord() throws Exception {
        TestSupport.LogCollector collector = new TestSupport.LogCollector();

        Slatedb.initLogging(LogLevel.INFO, collector);
        Error.Invalid error =
                TestSupport.expectFailure(
                        Error.Invalid.class, () -> Slatedb.initLogging(LogLevel.INFO, collector));
        assertEquals("logging already initialized", error.message());

        String path = TestSupport.uniquePath("test-db-logging");
        LogRecord[] matched = new LogRecord[1];

        try (ObjectStore store = TestSupport.newMemoryStore();
                DbBuilder builder = new DbBuilder(path, store);
                Db db = TestSupport.await(builder.build())) {
            TestSupport.waitUntil(
                    TestSupport.WAIT_TIMEOUT,
                    TestSupport.WAIT_STEP,
                    () -> {
                        LogRecord record =
                                collector.matchingRecord(
                                        candidate ->
                                                candidate.level() == LogLevel.INFO
                                                        && candidate.message().contains("opening SlateDB database")
                                                        && candidate.message().contains(path));
                        if (record == null) {
                            return false;
                        }
                        matched[0] = record;
                        return true;
                    });

            assertNotNull(matched[0]);
            assertFalse(matched[0].target().isEmpty());
            assertNotNull(matched[0].modulePath());
            assertFalse(matched[0].modulePath().isEmpty());
            assertNotNull(matched[0].file());
            assertFalse(matched[0].file().isEmpty());
            assertNotNull(matched[0].line());
            assertTrue(matched[0].line() > 0);

            TestSupport.await(db.shutdown());
        }
    }

    private static void assertFalse(boolean condition) {
        org.junit.jupiter.api.Assertions.assertFalse(condition);
    }
}
