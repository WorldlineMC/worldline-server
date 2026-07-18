package io.papermc.paper.worldline;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Thread-safe transfer lifecycle registry. This class deliberately owns no Minecraft objects so
 * control-command idempotency can be decided before scheduling work on the server thread.
 */
public final class WorldlineTransferLifecycle {
    private final int maxTombstones;
    private final Map<UUID, DestinationState> destinations = new HashMap<>();
    private final Map<UUID, SourceState> sources = new HashMap<>();
    private final LinkedHashMap<TombstoneKey, Object> tombstones = new LinkedHashMap<>();

    public WorldlineTransferLifecycle(final int maxTombstones) {
        if (maxTombstones < 1) {
            throw new IllegalArgumentException("max tombstones must be positive");
        }
        this.maxTombstones = maxTombstones;
    }

    public synchronized Outcome prepareDestination(final UUID playerId, final UUID transferId,
                                                   final long sourceEpoch) {
        if (!validIdentity(playerId, transferId, sourceEpoch)) {
            return Outcome.REJECTED_MISMATCH;
        }
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (existing != null) {
            return existing.matches(transferId, sourceEpoch)
                && existing.phase == DestinationPhase.PREPARED
                ? Outcome.ALREADY_APPLIED : Outcome.REJECTED_MISMATCH;
        }
        destinations.put(playerId, new DestinationState(playerId, transferId, sourceEpoch, 0,
            DestinationPhase.PREPARED));
        return Outcome.APPLIED;
    }

    public synchronized Outcome stageDestination(final UUID playerId, final UUID transferId,
                                                 final long sourceEpoch) {
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, 0)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.PREPARED) {
            destinations.put(playerId, existing.withPhase(DestinationPhase.SNAPSHOT_STAGED));
            return Outcome.APPLIED;
        }
        return switch (existing.phase) {
            case SNAPSHOT_STAGED, COMMITTED, CONNECTION_ATTACHED, ACTIVE, CLEANED ->
                Outcome.ALREADY_APPLIED;
            case PREPARED -> throw new IllegalStateException("handled prepared state");
            case RETIRED, ABORTED -> Outcome.REJECTED_MISMATCH;
        };
    }

    public synchronized Outcome commitDestination(final UUID playerId, final UUID transferId,
                                                  final long sourceEpoch,
                                                  final long committedEpoch) {
        if (!validCommitEpoch(sourceEpoch, committedEpoch)) {
            return Outcome.REJECTED_MISMATCH;
        }
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.SNAPSHOT_STAGED) {
            destinations.put(playerId, existing.committed(committedEpoch));
            return Outcome.APPLIED;
        }
        return switch (existing.phase) {
            case COMMITTED, CONNECTION_ATTACHED, ACTIVE, CLEANED, RETIRED ->
                Outcome.ALREADY_APPLIED;
            case PREPARED, SNAPSHOT_STAGED, ABORTED -> Outcome.REJECTED_MISMATCH;
        };
    }

    public synchronized Outcome attachDestination(final UUID playerId, final UUID transferId,
                                                  final long sourceEpoch,
                                                  final long committedEpoch) {
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.COMMITTED) {
            destinations.put(playerId, existing.withPhase(DestinationPhase.CONNECTION_ATTACHED));
            return Outcome.APPLIED;
        }
        return switch (existing.phase) {
            case CONNECTION_ATTACHED, ACTIVE, CLEANED -> Outcome.ALREADY_APPLIED;
            case PREPARED, SNAPSHOT_STAGED, COMMITTED, RETIRED, ABORTED ->
                Outcome.REJECTED_MISMATCH;
        };
    }

    public synchronized Outcome activateDestination(final UUID playerId, final UUID transferId,
                                                    final long sourceEpoch,
                                                    final long committedEpoch) {
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.CONNECTION_ATTACHED) {
            destinations.put(playerId, existing.withPhase(DestinationPhase.ACTIVE));
            return Outcome.APPLIED;
        }
        return switch (existing.phase) {
            case ACTIVE, CLEANED -> Outcome.ALREADY_APPLIED;
            case PREPARED, SNAPSHOT_STAGED, COMMITTED, CONNECTION_ATTACHED, RETIRED, ABORTED ->
                Outcome.REJECTED_MISMATCH;
        };
    }

    public synchronized Outcome cleanDestination(final UUID playerId, final UUID transferId,
                                                 final long sourceEpoch,
                                                 final long committedEpoch) {
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.ACTIVE) {
            terminalDestination(existing.withPhase(DestinationPhase.CLEANED));
            return Outcome.APPLIED;
        }
        return existing.phase == DestinationPhase.CLEANED
            ? Outcome.ALREADY_APPLIED : Outcome.REJECTED_MISMATCH;
    }

    public synchronized Outcome retireDestination(final UUID playerId, final UUID transferId,
                                                  final long sourceEpoch,
                                                  final long committedEpoch) {
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.RETIRED
            || existing.phase == DestinationPhase.CLEANED) {
            return Outcome.ALREADY_APPLIED;
        }
        if (existing.phase != DestinationPhase.COMMITTED
            && existing.phase != DestinationPhase.CONNECTION_ATTACHED
            && existing.phase != DestinationPhase.ACTIVE) {
            return Outcome.REJECTED_MISMATCH;
        }
        terminalDestination(existing.withPhase(DestinationPhase.RETIRED));
        return Outcome.APPLIED;
    }

    public synchronized Outcome abortDestination(final UUID playerId, final UUID transferId,
                                                 final long sourceEpoch) {
        final DestinationState existing = findDestination(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, 0)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == DestinationPhase.ABORTED) {
            return Outcome.ALREADY_APPLIED;
        }
        if (existing.phase != DestinationPhase.PREPARED
            && existing.phase != DestinationPhase.SNAPSHOT_STAGED) {
            return Outcome.REJECTED_MISMATCH;
        }
        terminalDestination(existing.withPhase(DestinationPhase.ABORTED));
        return Outcome.APPLIED;
    }

    public synchronized Outcome freezeSource(final UUID playerId, final UUID transferId,
                                             final long sourceEpoch) {
        if (!validIdentity(playerId, transferId, sourceEpoch)) {
            return Outcome.REJECTED_MISMATCH;
        }
        final SourceState existing = findSource(playerId, transferId, sourceEpoch);
        if (existing != null) {
            return existing.matches(transferId, sourceEpoch, existing.committedEpoch)
                ? Outcome.ALREADY_APPLIED : Outcome.REJECTED_MISMATCH;
        }
        sources.put(playerId, new SourceState(playerId, transferId, sourceEpoch, 0,
            SourcePhase.FROZEN));
        return Outcome.APPLIED;
    }

    public synchronized Outcome commitSource(final UUID playerId, final UUID transferId,
                                             final long sourceEpoch,
                                             final long committedEpoch) {
        if (!validCommitEpoch(sourceEpoch, committedEpoch)) {
            return Outcome.REJECTED_MISMATCH;
        }
        final SourceState existing = findSource(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == SourcePhase.FROZEN) {
            sources.put(playerId, existing.committed(committedEpoch));
            return Outcome.APPLIED;
        }
        return Outcome.ALREADY_APPLIED;
    }

    public synchronized Outcome abortSource(final UUID playerId, final UUID transferId,
                                            final long sourceEpoch) {
        final SourceState existing = findSource(playerId, transferId, sourceEpoch);
        if (existing == null) {
            return Outcome.MISSING;
        }
        if (!existing.transferId.equals(transferId) || existing.sourceEpoch != sourceEpoch) {
            return Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase != SourcePhase.FROZEN) {
            return Outcome.REJECTED_MISMATCH;
        }
        sources.remove(playerId);
        return Outcome.APPLIED;
    }

    public synchronized Outcome cleanSource(final UUID playerId, final UUID transferId,
                                            final long sourceEpoch,
                                            final long committedEpoch) {
        final SourceState existing = findSource(playerId, transferId, sourceEpoch);
        if (!matches(existing, transferId, sourceEpoch, committedEpoch)) {
            return existing == null ? Outcome.MISSING : Outcome.REJECTED_MISMATCH;
        }
        if (existing.phase == SourcePhase.COMMITTED_AWAY) {
            terminalSource(existing.withPhase(SourcePhase.CLEANED));
            return Outcome.APPLIED;
        }
        return existing.phase == SourcePhase.CLEANED
            ? Outcome.ALREADY_APPLIED : Outcome.REJECTED_MISMATCH;
    }

    public synchronized Optional<DestinationState> destination(final UUID playerId) {
        final DestinationState active = destinations.get(playerId);
        return Optional.ofNullable(active != null ? active : latestDestination(playerId));
    }

    public synchronized Optional<SourceState> source(final UUID playerId) {
        final SourceState active = sources.get(playerId);
        return Optional.ofNullable(active != null ? active : latestSource(playerId));
    }

    public synchronized int tombstoneCount() {
        return tombstones.size();
    }

    private DestinationState findDestination(final UUID playerId, final UUID transferId,
                                             final long sourceEpoch) {
        final DestinationState active = destinations.get(playerId);
        return active != null ? active
            : (DestinationState) tombstones.get(new TombstoneKey(Role.DESTINATION, playerId,
                transferId, sourceEpoch));
    }

    private SourceState findSource(final UUID playerId, final UUID transferId,
                                   final long sourceEpoch) {
        final SourceState active = sources.get(playerId);
        return active != null ? active
            : (SourceState) tombstones.get(new TombstoneKey(Role.SOURCE, playerId, transferId,
                sourceEpoch));
    }

    private DestinationState latestDestination(final UUID playerId) {
        DestinationState latest = null;
        for (final Map.Entry<TombstoneKey, Object> entry : tombstones.entrySet()) {
            if (entry.getKey().role == Role.DESTINATION
                && entry.getKey().playerId.equals(playerId)) {
                latest = (DestinationState) entry.getValue();
            }
        }
        return latest;
    }

    private SourceState latestSource(final UUID playerId) {
        SourceState latest = null;
        for (final Map.Entry<TombstoneKey, Object> entry : tombstones.entrySet()) {
            if (entry.getKey().role == Role.SOURCE
                && entry.getKey().playerId.equals(playerId)) {
                latest = (SourceState) entry.getValue();
            }
        }
        return latest;
    }

    private void terminalDestination(final DestinationState state) {
        destinations.remove(state.playerId);
        addTombstone(new TombstoneKey(Role.DESTINATION, state.playerId, state.transferId,
            state.sourceEpoch), state);
    }

    private void terminalSource(final SourceState state) {
        sources.remove(state.playerId);
        addTombstone(new TombstoneKey(Role.SOURCE, state.playerId, state.transferId,
            state.sourceEpoch), state);
    }

    private void addTombstone(final TombstoneKey key, final Object state) {
        tombstones.put(key, state);
        while (tombstones.size() > maxTombstones) {
            final Iterator<TombstoneKey> oldest = tombstones.keySet().iterator();
            oldest.next();
            oldest.remove();
        }
    }

    private static boolean validIdentity(final UUID playerId, final UUID transferId,
                                         final long sourceEpoch) {
        return playerId != null && transferId != null && sourceEpoch >= 0;
    }

    private static boolean validCommitEpoch(final long sourceEpoch, final long committedEpoch) {
        return sourceEpoch >= 0 && sourceEpoch < Long.MAX_VALUE
            && committedEpoch == sourceEpoch + 1;
    }

    private static boolean matches(final DestinationState state, final UUID transferId,
                                   final long sourceEpoch, final long committedEpoch) {
        return state != null && state.matches(transferId, sourceEpoch)
            && (state.committedEpoch == 0 || state.committedEpoch == committedEpoch);
    }

    private static boolean matches(final SourceState state, final UUID transferId,
                                   final long sourceEpoch, final long committedEpoch) {
        return state != null && state.matches(transferId, sourceEpoch, committedEpoch);
    }

    public enum Outcome {
        APPLIED,
        ALREADY_APPLIED,
        REJECTED_MISMATCH,
        MISSING
    }

    public enum DestinationPhase {
        PREPARED,
        SNAPSHOT_STAGED,
        COMMITTED,
        CONNECTION_ATTACHED,
        ACTIVE,
        CLEANED,
        RETIRED,
        ABORTED
    }

    public enum SourcePhase {
        FROZEN,
        COMMITTED_AWAY,
        CLEANED
    }

    public record DestinationState(UUID playerId, UUID transferId, long sourceEpoch,
                                   long committedEpoch, DestinationPhase phase) {
        public DestinationState {
            Objects.requireNonNull(playerId, "playerId");
            Objects.requireNonNull(transferId, "transferId");
            Objects.requireNonNull(phase, "phase");
        }

        private boolean matches(final UUID transferId, final long sourceEpoch) {
            return this.transferId.equals(transferId) && this.sourceEpoch == sourceEpoch;
        }

        private DestinationState committed(final long committedEpoch) {
            return new DestinationState(playerId, transferId, sourceEpoch, committedEpoch,
                DestinationPhase.COMMITTED);
        }

        private DestinationState withPhase(final DestinationPhase phase) {
            return new DestinationState(playerId, transferId, sourceEpoch, committedEpoch, phase);
        }
    }

    public record SourceState(UUID playerId, UUID transferId, long sourceEpoch,
                              long committedEpoch, SourcePhase phase) {
        public SourceState {
            Objects.requireNonNull(playerId, "playerId");
            Objects.requireNonNull(transferId, "transferId");
            Objects.requireNonNull(phase, "phase");
        }

        private boolean matches(final UUID transferId, final long sourceEpoch,
                                final long committedEpoch) {
            return this.transferId.equals(transferId) && this.sourceEpoch == sourceEpoch
                && (this.committedEpoch == 0 || this.committedEpoch == committedEpoch);
        }

        private SourceState committed(final long committedEpoch) {
            return new SourceState(playerId, transferId, sourceEpoch, committedEpoch,
                SourcePhase.COMMITTED_AWAY);
        }

        private SourceState withPhase(final SourcePhase phase) {
            return new SourceState(playerId, transferId, sourceEpoch, committedEpoch, phase);
        }
    }

    private enum Role {
        DESTINATION,
        SOURCE
    }

    private record TombstoneKey(Role role, UUID playerId, UUID transferId, long sourceEpoch) {
    }
}
