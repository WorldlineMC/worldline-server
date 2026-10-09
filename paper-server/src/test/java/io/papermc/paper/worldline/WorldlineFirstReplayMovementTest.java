package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineFirstReplayMovementTest {
    @Test
    void correctedFirstPacketCannotBeReplacedBySuccessfulSecondPacket() {
        var trace = new WorldlineFirstReplayMovement();
        assertFalse(trace.beginMovement());
        trace.arm();
        assertTrue(trace.beginMovement()); // Receipt consumes the token even if validation returns.
        assertFalse(trace.beginMovement()); // A later applied packet cannot claim to be first.
    }

    @Test
    void aNewTransferCanArmItsOwnFirstMovement() {
        var trace = new WorldlineFirstReplayMovement();
        trace.arm();
        assertTrue(trace.beginMovement());
        trace.arm();
        assertTrue(trace.beginMovement());
        assertFalse(trace.beginMovement());
    }
}
