package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SlateDbStatusSubscriptionTest {
    @Test
    void observesDurableWritesAndShutdown() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store);
                DbStatusSubscription subscription = handle.db().subscribe()) {
            Db db = handle.db();
            long sequence = subscription.status().durableSeq();
            var waiting = subscription.waitForChange(sequence, 5000);
            try {
                assertFalse(waiting.isDone());
                try (WriteHandle write = TestSupport.await(db.put(TestSupport.bytes("key"), TestSupport.bytes("value")))) {
                    TestSupport.await(write.awaitDurable());
                }
                DbStatus changed = TestSupport.await(waiting);
                assertTrue(changed.durableSeq() > sequence);
                TestSupport.await(db.shutdown());
                handle.markClosed();
                assertEquals(CloseReason.CLEAN,
                        TestSupport.await(subscription.waitForChange(changed.durableSeq(), 5000)).closeReason());
            } finally {
                waiting.cancel(true);
            }
        }
    }

    @Test
    void repeatedTimeoutsAndCancellationsReleaseContinuationHandles() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store);
                DbStatusSubscription subscription = handle.db().subscribe()) {
            long sequence = subscription.status().durableSeq();
            awaitNoContinuations();
            for (int i = 0; i < 100; i++) {
                assertNull(TestSupport.await(subscription.waitForChange(sequence, 1)));
                var waiting = subscription.waitForChange(sequence, 5000);
                try {
                    assertFalse(waiting.isDone());
                } finally {
                    waiting.cancel(true);
                }
                assertTrue(waiting.isCancelled());
            }
            awaitNoContinuations();
        }
    }

    private static void awaitNoContinuations() throws Exception {
        TestSupport.waitUntil(Duration.ofSeconds(2), Duration.ofMillis(5),
                () -> UniffiAsyncHelpers.uniffiContinuationHandleMap.size() == 0);
    }
}
