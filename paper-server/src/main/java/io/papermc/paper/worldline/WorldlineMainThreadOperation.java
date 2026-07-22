package io.papermc.paper.worldline;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Prevents a timed-out queued server-thread operation from mutating state later. */
final class WorldlineMainThreadOperation<T> implements Runnable {
    private final Callable<T> operation;
    private final CompletableFuture<T> result = new CompletableFuture<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.QUEUED);

    WorldlineMainThreadOperation(final Callable<T> operation) {
        this.operation = operation;
    }

    void submit(final Executor executor) {
        executor.execute(this);
    }

    @Override
    public void run() {
        if (!this.state.compareAndSet(State.QUEUED, State.RUNNING)) {
            return;
        }
        try {
            this.result.complete(this.operation.call());
        } catch (Throwable throwable) {
            this.result.completeExceptionally(throwable);
        } finally {
            this.state.set(State.DONE);
        }
    }

    Optional<T> await(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException {
        try {
            return Optional.ofNullable(this.result.get(timeout, unit));
        } catch (TimeoutException timeoutException) {
            if (this.state.compareAndSet(State.QUEUED, State.TIMED_OUT)
                || this.state.get() == State.TIMED_OUT) {
                return Optional.empty();
            }
            return Optional.ofNullable(this.result.get());
        }
    }

    boolean cancelBeforeStart() {
        return this.state.compareAndSet(State.QUEUED, State.TIMED_OUT)
            || this.state.get() == State.TIMED_OUT;
    }

    T awaitCompletionUninterruptibly() throws ExecutionException {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return this.result.get();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    boolean timedOutBeforeStart() {
        return this.state.get() == State.TIMED_OUT;
    }

    private enum State {
        QUEUED,
        RUNNING,
        TIMED_OUT,
        DONE
    }
}
