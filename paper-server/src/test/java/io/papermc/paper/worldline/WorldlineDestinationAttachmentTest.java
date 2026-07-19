package io.papermc.paper.worldline;

import static io.papermc.paper.worldline.WorldlineDestinationAttachment.Outcome.ALREADY_CONSUMED;
import static io.papermc.paper.worldline.WorldlineDestinationAttachment.Outcome.APPLIED;
import static io.papermc.paper.worldline.WorldlineDestinationAttachment.Outcome.NOT_COMMITTED;
import static io.papermc.paper.worldline.WorldlineDestinationAttachment.Outcome.REJECTED_MISMATCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.UUID;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineDestinationAttachmentTest {
    private static final UUID TRANSFER =
        UUID.fromString("00000000-0000-0000-0000-000000000091");
    private static final UUID PLAYER =
        UUID.fromString("00000000-0000-0000-0000-000000000092");
    private static final UUID CLIENT =
        UUID.fromString("00000000-0000-0000-0000-000000000093");
    private static final UUID OTHER =
        UUID.fromString("00000000-0000-0000-0000-000000000094");

    @Test
    void consumesTheExactPreparedObjectOnlyOnce() {
        Object preparedPlayer = new Object();
        WorldlineDestinationAttachment<Object> attachment = attachment(preparedPlayer);

        WorldlineDestinationAttachment.Result<Object> first =
            attachment.attach(context(), PLAYER);
        assertEquals(APPLIED, first.outcome());
        assertSame(preparedPlayer, first.preparedPlayer());

        WorldlineDestinationAttachment.Result<Object> duplicate =
            attachment.attach(context(), PLAYER);
        assertEquals(ALREADY_CONSUMED, duplicate.outcome());
        assertNull(duplicate.preparedPlayer());
    }

    @Test
    void rejectsEveryChangedFenceAndTheWrongLoginUuidWithoutConsumption() {
        Object preparedPlayer = new Object();
        WorldlineDestinationAttachment<Object> attachment = attachment(preparedPlayer);
        List<WorldlineResumeContext> mismatches = List.of(
            new WorldlineResumeContext(4, OTHER, PLAYER, CLIENT, "server-a", "server-b",
                "west", 11, "east", 12, 13, 14, 15, 16, 17),
            new WorldlineResumeContext(4, TRANSFER, OTHER, CLIENT, "server-a", "server-b",
                "west", 11, "east", 12, 13, 14, 15, 16, 17),
            new WorldlineResumeContext(4, TRANSFER, PLAYER, OTHER, "server-a", "server-b",
                "west", 11, "east", 12, 13, 14, 15, 16, 17),
            new WorldlineResumeContext(4, TRANSFER, PLAYER, CLIENT, "server-x", "server-b",
                "west", 11, "east", 12, 13, 14, 15, 16, 17),
            new WorldlineResumeContext(4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b",
                "west", 10, "east", 12, 13, 14, 15, 16, 17),
            new WorldlineResumeContext(4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b",
                "west", 11, "east", 12, 13, 14, 99, 16, 17),
            new WorldlineResumeContext(4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b",
                "west", 11, "east", 12, 13, 14, 15, 99, 17)
        );

        assertEquals(REJECTED_MISMATCH, attachment.attach(context(), OTHER).outcome());
        for (WorldlineResumeContext mismatch : mismatches) {
            assertEquals(REJECTED_MISMATCH, attachment.attach(mismatch, PLAYER).outcome());
        }
        assertEquals(APPLIED, attachment.attach(context(), PLAYER).outcome());
    }

    @Test
    void cannotAttachBeforeSnapshotCommit() {
        WorldlineDestinationAttachment<Object> attachment = newAttachment();
        attachment.bindPreparedPlayer(new Object());

        assertEquals(NOT_COMMITTED, attachment.attach(context(), PLAYER).outcome());
        attachment.stageSnapshot(15);
        assertEquals(NOT_COMMITTED, attachment.attach(context(), PLAYER).outcome());
    }

    @Test
    void retirementReleasesThePreparedObjectExactlyOnce() {
        Object preparedPlayer = new Object();
        WorldlineDestinationAttachment<Object> attachment = attachment(preparedPlayer);
        assertEquals(APPLIED, attachment.attach(context(), PLAYER).outcome());

        assertSame(preparedPlayer, attachment.retire());
        assertNull(attachment.retire());
        assertNull(attachment.attach(context(), PLAYER).preparedPlayer());
    }

    private static WorldlineDestinationAttachment<Object> attachment(final Object player) {
        WorldlineDestinationAttachment<Object> attachment = newAttachment();
        attachment.bindPreparedPlayer(player);
        attachment.stageSnapshot(15);
        attachment.commit(14, 16);
        return attachment;
    }

    private static WorldlineDestinationAttachment<Object> newAttachment() {
        return new WorldlineDestinationAttachment<>(TRANSFER, PLAYER, CLIENT,
            "server-a", "server-b", "west", 11, "east", 12, 13, 15);
    }

    private static WorldlineResumeContext context() {
        return new WorldlineResumeContext(4, TRANSFER, PLAYER, CLIENT,
            "server-a", "server-b", "west", 11, "east", 12, 13, 14, 15, 16, 17);
    }
}
