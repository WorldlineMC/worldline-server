package io.papermc.paper.worldline;

import java.util.Objects;
import java.util.UUID;

/**
 * Owns the one-shot association between a committed M4 preparation and an inbound M5 connection.
 * It contains no Minecraft behavior, so fence matching and consumption stay independently testable.
 */
public final class WorldlineDestinationAttachment<T> {
    private final UUID transferId;
    private final UUID playerId;
    private final UUID clientConnectionId;
    private final String sourceServerId;
    private final String destinationServerId;
    private final String sourcePartitionId;
    private final long sourcePartitionEpoch;
    private final String destinationPartitionId;
    private final long destinationPartitionEpoch;
    private final long sourcePlayerEpoch;
    private final long sourceRouteGeneration;
    private T preparedPlayer;
    private long playerStateVersion;
    private long committedPlayerEpoch;
    private long routeGeneration;
    private boolean consumed;
    private boolean retired;

    public WorldlineDestinationAttachment(final UUID transferId, final UUID playerId,
                                          final UUID clientConnectionId,
                                          final String sourceServerId,
                                          final String destinationServerId,
                                          final String sourcePartitionId,
                                          final long sourcePartitionEpoch,
                                          final String destinationPartitionId,
                                          final long destinationPartitionEpoch,
                                          final long sourcePlayerEpoch,
                                          final long sourceRouteGeneration) {
        this.transferId = Objects.requireNonNull(transferId, "transferId");
        this.playerId = Objects.requireNonNull(playerId, "playerId");
        this.clientConnectionId = Objects.requireNonNull(clientConnectionId,
            "clientConnectionId");
        this.sourceServerId = requireId(sourceServerId, "sourceServerId");
        this.destinationServerId = requireId(destinationServerId, "destinationServerId");
        this.sourcePartitionId = requireId(sourcePartitionId, "sourcePartitionId");
        this.destinationPartitionId = requireId(destinationPartitionId,
            "destinationPartitionId");
        if (sourcePartitionEpoch < 1 || destinationPartitionEpoch < 1
            || sourcePlayerEpoch < 0 || sourcePlayerEpoch == Long.MAX_VALUE
            || sourceRouteGeneration < 0 || sourceRouteGeneration == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid preparation epoch");
        }
        this.sourcePartitionEpoch = sourcePartitionEpoch;
        this.destinationPartitionEpoch = destinationPartitionEpoch;
        this.sourcePlayerEpoch = sourcePlayerEpoch;
        this.sourceRouteGeneration = sourceRouteGeneration;
    }

    public synchronized void bindPreparedPlayer(final T player) {
        Objects.requireNonNull(player, "player");
        if (this.preparedPlayer != null && this.preparedPlayer != player) {
            throw new IllegalStateException("a different prepared player is already bound");
        }
        this.preparedPlayer = player;
    }

    public synchronized void stageSnapshot(final long playerStateVersion) {
        if (playerStateVersion < 1) {
            throw new IllegalArgumentException("player state version must be positive");
        }
        if (this.playerStateVersion != 0 && this.playerStateVersion != playerStateVersion) {
            throw new IllegalStateException("a different snapshot is already staged");
        }
        this.playerStateVersion = playerStateVersion;
    }

    public synchronized void commit(final long committedPlayerEpoch,
                                    final long routeGeneration) {
        if (this.playerStateVersion < 1 || committedPlayerEpoch != this.sourcePlayerEpoch + 1
            || routeGeneration != this.sourceRouteGeneration + 1) {
            throw new IllegalArgumentException("invalid destination commit fence");
        }
        if ((this.committedPlayerEpoch != 0
            && this.committedPlayerEpoch != committedPlayerEpoch)
            || (this.routeGeneration != 0 && this.routeGeneration != routeGeneration)) {
            throw new IllegalStateException("a different destination commit is already recorded");
        }
        this.committedPlayerEpoch = committedPlayerEpoch;
        this.routeGeneration = routeGeneration;
    }

    public synchronized Result<T> attach(final WorldlineResumeContext context,
                                         final UUID loginPlayerId) {
        final Outcome validation = validate(context, loginPlayerId);
        if (validation != Outcome.APPLIED) {
            return new Result<>(validation, null);
        }
        this.consumed = true;
        return new Result<>(Outcome.APPLIED, this.preparedPlayer);
    }

    public synchronized Outcome validate(final WorldlineResumeContext context,
                                         final UUID loginPlayerId) {
        Objects.requireNonNull(context, "context");
        if (!matchesIdentity(context, loginPlayerId)) {
            return Outcome.REJECTED_MISMATCH;
        }
        if (this.retired) {
            return Outcome.ALREADY_CONSUMED;
        }
        if (this.preparedPlayer == null || this.playerStateVersion == 0
            || this.committedPlayerEpoch == 0 || this.routeGeneration == 0) {
            return Outcome.NOT_COMMITTED;
        }
        if (!matchesCommit(context)) {
            return Outcome.REJECTED_MISMATCH;
        }
        if (this.consumed) {
            return Outcome.ALREADY_CONSUMED;
        }
        return Outcome.APPLIED;
    }

    public synchronized boolean isConsumed() {
        return this.consumed;
    }

    /** Returns the prepared resource once and permanently prevents later attachment. */
    public synchronized T retire() {
        if (this.retired) {
            return null;
        }
        this.retired = true;
        final T player = this.preparedPlayer;
        this.preparedPlayer = null;
        return player;
    }

    private boolean matchesIdentity(final WorldlineResumeContext context,
                                    final UUID loginPlayerId) {
        return this.playerId.equals(loginPlayerId)
            && this.transferId.equals(context.transferId())
            && this.playerId.equals(context.playerId())
            && this.clientConnectionId.equals(context.clientConnectionId())
            && this.sourceServerId.equals(context.sourceServerId())
            && this.destinationServerId.equals(context.destinationServerId())
            && this.sourcePartitionId.equals(context.sourcePartitionId())
            && this.sourcePartitionEpoch == context.sourcePartitionEpoch()
            && this.destinationPartitionId.equals(context.destinationPartitionId())
            && this.destinationPartitionEpoch == context.destinationPartitionEpoch()
            && this.sourcePlayerEpoch == context.sourcePlayerEpoch();
    }

    private boolean matchesCommit(final WorldlineResumeContext context) {
        return this.committedPlayerEpoch == context.committedPlayerEpoch()
            && this.playerStateVersion == context.playerStateVersion()
            && this.routeGeneration == context.routeGeneration();
    }

    private static String requireId(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    public enum Outcome {
        APPLIED,
        ALREADY_CONSUMED,
        REJECTED_MISMATCH,
        NOT_COMMITTED
    }

    public record Result<T>(Outcome outcome, T preparedPlayer) {
    }
}
