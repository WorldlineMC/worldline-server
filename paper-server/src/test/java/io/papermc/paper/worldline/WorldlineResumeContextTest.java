package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineResumeContextTest {
    private static final UUID TRANSFER =
        UUID.fromString("00000000-0000-0000-0000-000000000081");
    private static final UUID PLAYER =
        UUID.fromString("00000000-0000-0000-0000-000000000082");
    private static final UUID CLIENT =
        UUID.fromString("00000000-0000-0000-0000-000000000083");

    @Test
    void roundTripsEveryFenceAndExtractsFromAHandshakeHostname() {
        WorldlineResumeContext context = context();

        assertEquals(context, WorldlineResumeContext.parseMarker(context.encodeMarker()));
        assertEquals(context, WorldlineResumeContext.parseHostname(
            "play.example.test" + context.encodeMarker() + ":25565").orElseThrow());
        assertEquals(".worldline-resume-v4|00000000-0000-0000-0000-000000000081|"
                + "00000000-0000-0000-0000-000000000082|"
                + "00000000-0000-0000-0000-000000000083|c2VydmVyLWE|c2VydmVyLWI|"
                + "d2VzdA|11|ZWFzdA|12|13|14|15|16|17",
            context.encodeMarker());
    }

    @Test
    void rejectsMalformedOversizedOrTrailingMarkerData() {
        String marker = context().encodeMarker();

        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseMarker(".worldline-resume-v4|broken"));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseMarker(marker.replace(TRANSFER.toString(),
                "bad-uuid")));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseMarker(marker.replace(TRANSFER.toString(),
                "00000000-0000-0000-0000-0000000000810")));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseMarker(marker + "|trailing"));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseMarker(marker + "x".repeat(241)));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseMarker(marker.replace("c2VydmVyLWE", "c2VydmVyLWE=")));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseHostname("host" + marker + ":25565junk"));
        assertThrows(IllegalArgumentException.class,
            () -> WorldlineResumeContext.parseHostname("host" + marker + marker));
    }

    @Test
    void rejectsWrongProtocolAndInvalidEpochsOrGeneration() {
        assertThrows(IllegalArgumentException.class, () -> new WorldlineResumeContext(
            3, TRANSFER, PLAYER, CLIENT, "server-a", "server-b", "west", 11, "east", 12,
            13, 14, 15, 16, 17));
        assertThrows(IllegalArgumentException.class, () -> new WorldlineResumeContext(
            4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b", "west", 11, "east", 12,
            -1, 0, 15, 16, 17));
        assertThrows(IllegalArgumentException.class, () -> new WorldlineResumeContext(
            4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b", "west", 11, "east", 12,
            13, 15, 15, 16, 17));
        assertThrows(IllegalArgumentException.class, () -> new WorldlineResumeContext(
            4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b", "west", 11, "east", 12,
            13, 14, 15, -1, 17));
    }

    private static WorldlineResumeContext context() {
        return new WorldlineResumeContext(4, TRANSFER, PLAYER, CLIENT, "server-a", "server-b",
            "west", 11, "east", 12, 13, 14, 15, 16, 17);
    }
}
