package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlinePlayerCommandLocksTest {
    @Test
    void abortWaitsForFreezeRegistrationWithoutBlockingAnotherPlayer() throws Exception {
        var locks = new WorldlinePlayerCommandLocks();
        var lifecycle = new WorldlineTransferLifecycle(8);
        UUID player = UUID.randomUUID();
        UUID transfer = UUID.randomUUID();
        var physicallyFrozen = new AtomicBoolean();
        var abortStarted = new CountDownLatch(1);
        var aborted = new CompletableFuture<Void>();
        try (var lease = locks.acquire(player)) {
            physicallyFrozen.set(true);
            Thread.startVirtualThread(() -> {
                abortStarted.countDown();
                try {
                    try (var ignored = locks.acquire(player)) {
                        assertEquals(WorldlineTransferLifecycle.Outcome.APPLIED,
                            lifecycle.abortSource(player, transfer, 0));
                        physicallyFrozen.set(false);
                    }
                    aborted.complete(null);
                } catch (Throwable failure) {
                    aborted.completeExceptionally(failure);
                }
            });
            assertTrue(abortStarted.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> aborted.get(30, TimeUnit.MILLISECONDS));
            try (var unrelated = locks.acquire(UUID.randomUUID())) {
                assertEquals(2, locks.size());
            }
            assertEquals(WorldlineTransferLifecycle.Outcome.APPLIED,
                lifecycle.freezeSource(player, transfer, 0));
        }
        aborted.get(1, TimeUnit.SECONDS);
        try (var ignored = locks.acquire(player)) {
            assertFalse(physicallyFrozen.get());
            assertEquals(WorldlineTransferLifecycle.Outcome.REJECTED_MISMATCH,
                lifecycle.freezeSource(player, transfer, 0));
        }
        assertEquals(0, locks.size());
    }

    @Test
    void failureReleasesThePlayerAndRegistryEntry() {
        var locks = new WorldlinePlayerCommandLocks();
        UUID player = UUID.randomUUID();
        assertThrows(IllegalStateException.class, () -> {
            try (var ignored = locks.acquire(player)) {
                throw new IllegalStateException("simulated operation failure");
            }
        });
        assertEquals(0, locks.size());
        try (var ignored = locks.acquire(player)) {
            assertEquals(1, locks.size());
        }
        assertEquals(0, locks.size());
    }
}
