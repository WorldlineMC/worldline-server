package io.papermc.paper.worldline;

import static io.papermc.paper.worldline.WorldlineTransferLifecycle.DestinationPhase.ACTIVE;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.DestinationPhase.CLEANED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.DestinationPhase.COMMITTED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.DestinationPhase.CONNECTION_ATTACHED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.DestinationPhase.RETIRED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.DestinationPhase.SNAPSHOT_STAGED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.Outcome.ALREADY_APPLIED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.Outcome.APPLIED;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.Outcome.MISSING;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.Outcome.REJECTED_MISMATCH;
import static io.papermc.paper.worldline.WorldlineTransferLifecycle.SourcePhase.COMMITTED_AWAY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineTransferLifecycleTest {
    private static final UUID PLAYER = UUID.fromString("00000000-0000-0000-0000-000000000051");
    private static final UUID TRANSFER = UUID.fromString("00000000-0000-0000-0000-000000000052");
    private static final UUID OTHER_TRANSFER = UUID.fromString("00000000-0000-0000-0000-000000000053");

    @Test
    void destinationFollowsTheCompleteSuccessLifecycle() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(8);

        assertEquals(APPLIED, lifecycle.prepareDestination(PLAYER, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.stageDestination(PLAYER, TRANSFER, 4));
        assertEquals(SNAPSHOT_STAGED, lifecycle.destination(PLAYER).orElseThrow().phase());
        assertEquals(APPLIED, lifecycle.commitDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(COMMITTED, lifecycle.destination(PLAYER).orElseThrow().phase());
        assertEquals(APPLIED, lifecycle.attachDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(CONNECTION_ATTACHED, lifecycle.destination(PLAYER).orElseThrow().phase());
        assertEquals(APPLIED, lifecycle.activateDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(ACTIVE, lifecycle.destination(PLAYER).orElseThrow().phase());
        assertEquals(APPLIED, lifecycle.cleanDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(CLEANED, lifecycle.destination(PLAYER).orElseThrow().phase());
    }

    @Test
    void sourceCommitAdvancesExactlyOneEpochBeforeCleanup() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(8);

        assertEquals(APPLIED, lifecycle.freezeSource(PLAYER, TRANSFER, 0));
        assertEquals(REJECTED_MISMATCH, lifecycle.commitSource(PLAYER, TRANSFER, 0, 2));
        assertEquals(APPLIED, lifecycle.commitSource(PLAYER, TRANSFER, 0, 1));
        assertEquals(COMMITTED_AWAY, lifecycle.source(PLAYER).orElseThrow().phase());
        assertEquals(1, lifecycle.source(PLAYER).orElseThrow().committedEpoch());
        assertEquals(REJECTED_MISMATCH, lifecycle.abortSource(PLAYER, TRANSFER, 0));
        assertEquals(APPLIED, lifecycle.cleanSource(PLAYER, TRANSFER, 0, 1));
    }

    @Test
    void duplicatesAreIdempotentButChangedIdentityIsRejected() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(8);

        assertEquals(APPLIED, lifecycle.prepareDestination(PLAYER, TRANSFER, 4));
        assertEquals(ALREADY_APPLIED, lifecycle.prepareDestination(PLAYER, TRANSFER, 4));
        assertEquals(REJECTED_MISMATCH,
            lifecycle.stageDestination(PLAYER, OTHER_TRANSFER, 4));
        assertEquals(REJECTED_MISMATCH, lifecycle.stageDestination(PLAYER, TRANSFER, 5));
        assertEquals(APPLIED, lifecycle.stageDestination(PLAYER, TRANSFER, 4));
        assertEquals(ALREADY_APPLIED, lifecycle.stageDestination(PLAYER, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.commitDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(ALREADY_APPLIED,
            lifecycle.commitDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(REJECTED_MISMATCH,
            lifecycle.commitDestination(PLAYER, TRANSFER, 4, 6));
    }

    @Test
    void abortIsRejectedAfterCommit() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(8);

        assertEquals(APPLIED, lifecycle.prepareDestination(PLAYER, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.stageDestination(PLAYER, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.commitDestination(PLAYER, TRANSFER, 4, 5));

        assertEquals(REJECTED_MISMATCH,
            lifecycle.abortDestination(PLAYER, TRANSFER, 4));
        assertEquals(COMMITTED, lifecycle.destination(PLAYER).orElseThrow().phase());
    }

    @Test
    void destinationCanRetireFromEveryPostCommitState() {
        for (int stepsAfterCommit = 0; stepsAfterCommit < 3; stepsAfterCommit++) {
            WorldlineTransferLifecycle lifecycle = committedDestination();
            if (stepsAfterCommit >= 1) {
                assertEquals(APPLIED, lifecycle.attachDestination(PLAYER, TRANSFER, 4, 5));
            }
            if (stepsAfterCommit >= 2) {
                assertEquals(APPLIED, lifecycle.activateDestination(PLAYER, TRANSFER, 4, 5));
            }

            assertEquals(APPLIED, lifecycle.retireDestination(PLAYER, TRANSFER, 4, 5));
            assertEquals(RETIRED, lifecycle.destination(PLAYER).orElseThrow().phase());
            assertEquals(ALREADY_APPLIED,
                lifecycle.retireDestination(PLAYER, TRANSFER, 4, 5));
        }
    }

    @Test
    void destinationCannotActivateBeforeConnectionAttachment() {
        WorldlineTransferLifecycle lifecycle = committedDestination();

        assertEquals(REJECTED_MISMATCH,
            lifecycle.activateDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(COMMITTED, lifecycle.destination(PLAYER).orElseThrow().phase());
    }

    @Test
    void terminalResultsAreBoundedAndOldestEntriesBecomeMissing() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(2);
        UUID first = UUID.fromString("00000000-0000-0000-0000-000000000061");
        UUID second = UUID.fromString("00000000-0000-0000-0000-000000000062");
        UUID third = UUID.fromString("00000000-0000-0000-0000-000000000063");

        cleanSource(lifecycle, first);
        cleanSource(lifecycle, second);
        cleanSource(lifecycle, third);

        assertEquals(MISSING, lifecycle.cleanSource(first, TRANSFER, 4, 5));
        assertEquals(ALREADY_APPLIED, lifecycle.cleanSource(second, TRANSFER, 4, 5));
        assertEquals(ALREADY_APPLIED, lifecycle.cleanSource(third, TRANSFER, 4, 5));
        assertEquals(2, lifecycle.tombstoneCount());
    }

    @Test
    void terminalHistoryDoesNotBlockTheNextTransferOrPermitStaleAbaCommands() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(8);
        cleanSource(lifecycle, PLAYER);

        assertEquals(APPLIED, lifecycle.freezeSource(PLAYER, OTHER_TRANSFER, 5));
        assertEquals(OTHER_TRANSFER, lifecycle.source(PLAYER).orElseThrow().transferId());
        assertEquals(REJECTED_MISMATCH, lifecycle.cleanSource(PLAYER, TRANSFER, 4, 5));

        WorldlineTransferLifecycle destinations = committedDestination();
        assertEquals(APPLIED, destinations.attachDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(APPLIED, destinations.activateDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(APPLIED, destinations.cleanDestination(PLAYER, TRANSFER, 4, 5));
        assertEquals(APPLIED, destinations.prepareDestination(PLAYER, OTHER_TRANSFER, 5));
        assertEquals(OTHER_TRANSFER,
            destinations.destination(PLAYER).orElseThrow().transferId());
    }

    private static WorldlineTransferLifecycle committedDestination() {
        WorldlineTransferLifecycle lifecycle = new WorldlineTransferLifecycle(8);
        assertEquals(APPLIED, lifecycle.prepareDestination(PLAYER, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.stageDestination(PLAYER, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.commitDestination(PLAYER, TRANSFER, 4, 5));
        return lifecycle;
    }

    private static void cleanSource(final WorldlineTransferLifecycle lifecycle,
                                    final UUID playerId) {
        assertEquals(APPLIED, lifecycle.freezeSource(playerId, TRANSFER, 4));
        assertEquals(APPLIED, lifecycle.commitSource(playerId, TRANSFER, 4, 5));
        assertEquals(APPLIED, lifecycle.cleanSource(playerId, TRANSFER, 4, 5));
    }
}
