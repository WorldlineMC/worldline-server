package io.papermc.paper.worldline;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Runs control sockets concurrently while enforcing a hard per-server request bound. */
final class WorldlineControlRequestExecutor {
    private final Semaphore capacity;

    WorldlineControlRequestExecutor(final int maximumConcurrentRequests) {
        if (maximumConcurrentRequests < 1) {
            throw new IllegalArgumentException("maximumConcurrentRequests must be positive");
        }
        this.capacity = new Semaphore(maximumConcurrentRequests);
    }

    boolean execute(final Runnable request) {
        if (!this.capacity.tryAcquire()) {
            return false;
        }
        try {
            Thread.startVirtualThread(() -> {
                try {
                    request.run();
                } finally {
                    this.capacity.release();
                }
            });
            return true;
        } catch (RuntimeException | Error failure) {
            this.capacity.release();
            throw failure;
        }
    }

    boolean awaitCapacity(final long timeout, final TimeUnit unit) throws InterruptedException {
        if (!this.capacity.tryAcquire(timeout, unit)) {
            return false;
        }
        this.capacity.release();
        return true;
    }
}
