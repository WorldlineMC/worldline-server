package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineMainThreadOperationTest {

    @Test
    void queuedOperationThatTimesOutCannotRunLater() throws Exception {
        AtomicReference<Runnable> queued = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        WorldlineMainThreadOperation<Integer> operation =
            new WorldlineMainThreadOperation<>(calls::incrementAndGet);

        operation.submit(queued::set);

        assertEquals(Optional.empty(), operation.await(0, TimeUnit.NANOSECONDS));
        queued.get().run();
        assertEquals(0, calls.get());
        assertTrue(operation.timedOutBeforeStart());
    }

    @Test
    void startedOperationReturnsItsRealResultAfterTheInitialTimeout() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch awaiting = new CountDownLatch(1);
        WorldlineMainThreadOperation<String> operation = new WorldlineMainThreadOperation<>(() -> {
            started.countDown();
            release.await();
            return "applied";
        });
        operation.submit(Thread::startVirtualThread);
        assertTrue(started.await(1, TimeUnit.SECONDS));

        CompletableFuture<Optional<String>> result = CompletableFuture.supplyAsync(() -> {
            awaiting.countDown();
            try {
                return operation.await(0, TimeUnit.NANOSECONDS);
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });
        assertTrue(awaiting.await(1, TimeUnit.SECONDS));
        assertFalse(result.isDone());

        release.countDown();
        assertEquals(Optional.of("applied"), result.get(1, TimeUnit.SECONDS));
        assertFalse(operation.timedOutBeforeStart());
    }
}
