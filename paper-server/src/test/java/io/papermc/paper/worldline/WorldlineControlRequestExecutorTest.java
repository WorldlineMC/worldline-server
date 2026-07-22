package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineControlRequestExecutorTest {

    @Test
    void blockedRequestDoesNotPreventAnotherAcceptedRequestFromRunning() throws Exception {
        WorldlineControlRequestExecutor executor = new WorldlineControlRequestExecutor(2);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondRan = new CountDownLatch(1);

        assertTrue(executor.execute(() -> {
            firstStarted.countDown();
            await(releaseFirst);
        }));
        assertTrue(firstStarted.await(1, TimeUnit.SECONDS));
        assertTrue(executor.execute(secondRan::countDown));
        assertTrue(secondRan.await(1, TimeUnit.SECONDS));
        releaseFirst.countDown();
    }

    @Test
    void saturationRejectsExcessWorkAndReleasesCapacityAfterCompletion() throws Exception {
        WorldlineControlRequestExecutor executor = new WorldlineControlRequestExecutor(1);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finalRun = new CountDownLatch(1);

        assertTrue(executor.execute(() -> {
            started.countDown();
            await(release);
        }));
        assertTrue(started.await(1, TimeUnit.SECONDS));
        assertFalse(executor.execute(() -> { }));

        release.countDown();
        assertTrue(executor.awaitCapacity(1, TimeUnit.SECONDS));
        assertTrue(executor.execute(finalRun::countDown));
        assertTrue(finalRun.await(1, TimeUnit.SECONDS));
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
