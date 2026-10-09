package io.papermc.paper.worldline;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/** Serializes a player's control commands, including their awaited server-thread mutations. */
final class WorldlinePlayerCommandLocks {
    // Entries exist only while bounded request workers hold or await them.
    private final Map<UUID, Entry> entries = new HashMap<>();

    Lease acquire(final UUID playerId) {
        final Entry entry;
        synchronized (this.entries) {
            entry = this.entries.computeIfAbsent(playerId, ignored -> new Entry());
            entry.users++;
        }
        entry.lock.lock();
        return new Lease(playerId, entry);
    }

    int size() {
        synchronized (this.entries) {
            return this.entries.size();
        }
    }

    final class Lease implements AutoCloseable {
        private final UUID playerId;
        private final Entry entry;
        private boolean closed;

        private Lease(final UUID playerId, final Entry entry) {
            this.playerId = playerId;
            this.entry = entry;
        }

        @Override
        public void close() {
            if (this.closed) {
                return;
            }
            this.closed = true;
            this.entry.lock.unlock();
            synchronized (entries) {
                if (--this.entry.users == 0) {
                    entries.remove(this.playerId, this.entry);
                }
            }
        }
    }

    private static final class Entry {
        private final ReentrantLock lock = new ReentrantLock();
        private int users;
    }
}
