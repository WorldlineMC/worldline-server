package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.UUID;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.resources.Identifier;
import net.minecraft.world.item.ItemCooldowns;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineControlServerTest {
    private static final int MAGIC = 0x574c4d32;

    @Test
    void protocolV4HasOnlyExplicitCommitAndCleanupCommands() {
        assertEquals(4, WorldlineControlServer.protocolVersionForTesting());
        assertFalse(WorldlineControlServer.isKnownCommandForTesting("COMMIT"));
        assertTrue(WorldlineControlServer.isKnownCommandForTesting("COMMIT_DESTINATION"));
        assertTrue(WorldlineControlServer.isKnownCommandForTesting("COMMIT_SOURCE"));
        assertTrue(WorldlineControlServer.isKnownCommandForTesting("ACTIVATE_DESTINATION"));
        assertTrue(WorldlineControlServer.isKnownCommandForTesting("CLEAN_SOURCE"));
        assertTrue(WorldlineControlServer.isKnownCommandForTesting("RETIRE_DESTINATION"));
    }

    @Test
    void v4ResponseEchoesEveryFenceFieldInProxyOrder() throws Exception {
        UUID transferId = UUID.fromString("00000000-0000-0000-0000-000000000071");
        UUID playerId = UUID.fromString("00000000-0000-0000-0000-000000000072");
        UUID clientId = UUID.fromString("00000000-0000-0000-0000-000000000073");
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        try (DataOutputStream request = new DataOutputStream(requestBytes)) {
            request.writeInt(MAGIC);
            request.writeInt(4);
            request.writeUTF("UNKNOWN");
            writeUuid(request, transferId);
            writeUuid(request, playerId);
            writeUuid(request, clientId);
            request.writeUTF("server-a");
            request.writeUTF("server-b");
            request.writeUTF("west");
            request.writeLong(11);
            request.writeUTF("east");
            request.writeLong(12);
            request.writeLong(13);
            request.writeLong(14);
            request.writeLong(15);
            request.writeBoolean(false);
            request.writeInt(0);
        }
        MemorySocket socket = new MemorySocket(requestBytes.toByteArray());

        WorldlineControlServer.handle(socket, "server-a", "west", 11, "compatible");

        try (DataInputStream response = new DataInputStream(
            new ByteArrayInputStream(socket.response()))) {
            assertEquals(MAGIC, response.readInt());
            assertEquals(4, response.readInt());
            assertFalse(response.readBoolean());
            assertEquals("identity, protocol, or ownership fence rejected", response.readUTF());
            assertEquals(0, response.readInt());
            assertEquals(4, response.readInt());
            assertEquals(transferId, readUuid(response));
            assertEquals(playerId, readUuid(response));
            assertEquals(clientId, readUuid(response));
            assertEquals("server-a", response.readUTF());
            assertEquals("server-b", response.readUTF());
            assertEquals("west", response.readUTF());
            assertEquals("east", response.readUTF());
            assertEquals(11, response.readLong());
            assertEquals(12, response.readLong());
            assertEquals(13, response.readLong());
            assertEquals(14, response.readLong());
            assertEquals(15, response.readLong());
            assertEquals("server-a", response.readUTF());
            assertEquals("west", response.readUTF());
            assertEquals(11, response.readLong());
            assertEquals(0, response.available());
        }
    }

    @Test
    void v3IsRejectedBeforeReadingOrDispatchingACommand() throws Exception {
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        try (DataOutputStream request = new DataOutputStream(requestBytes)) {
            request.writeInt(MAGIC);
            request.writeInt(3);
        }

        assertThrows(IOException.class, () -> WorldlineControlServer.handle(
            new MemorySocket(requestBytes.toByteArray()), "server-a", "west", 11, "compatible"));
    }

    @Test
    void snapshotNbtRejectsTrailingData() throws Exception {
        CompoundTag player = new CompoundTag();
        player.putString("UUID", "00000000-0000-0000-0000-000000000041");
        player.putDouble("Health", 17.5);
        CompoundTag snapshot = new CompoundTag();
        snapshot.putInt("SnapshotSchemaVersion", 1);
        snapshot.putLong("PlayerStateVersion", 7);
        snapshot.put("Player", player);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        NbtIo.write(snapshot, new DataOutputStream(bytes));

        assertDoesNotThrow(() -> WorldlineControlServer.decodeSnapshot(bytes.toByteArray()));
        byte[] withTrailingData = Arrays.copyOf(bytes.toByteArray(), bytes.size() + 1);
        assertThrows(IOException.class,
            () -> WorldlineControlServer.decodeSnapshot(withTrailingData));
    }

    @Test
    void snapshotDifferenceReportsOnlyChangedKeysInStableOrder() {
        CompoundTag expected = new CompoundTag();
        expected.putInt("Health", 20);
        expected.putInt("FoodLevel", 18);
        CompoundTag actual = new CompoundTag();
        actual.putInt("Health", 19);
        actual.putInt("XpLevel", 4);

        assertEquals("FoodLevel,Health,XpLevel",
            WorldlineControlServer.differingKeys(expected, actual));
    }

    @Test
    void snapshotComparisonIgnoresOnlyStorageLocalAndSaveTimeMetadata() {
        CompoundTag source = new CompoundTag();
        source.putLong("WorldUUIDLeast", 1);
        source.putLong("WorldUUIDMost", 2);
        source.putFloat("Health", 17.5F);
        CompoundTag sourceBukkit = new CompoundTag();
        sourceBukkit.putLong("lastPlayed", 10);
        sourceBukkit.putLong("firstPlayed", 5);
        source.put("bukkit", sourceBukkit);
        CompoundTag sourcePaper = new CompoundTag();
        sourcePaper.putLong("LastLogin", 8);
        sourcePaper.putLong("LastSeen", 10);
        source.put("Paper", sourcePaper);

        CompoundTag destination = source.copy();
        destination.putLong("WorldUUIDLeast", 3);
        destination.putLong("WorldUUIDMost", 4);
        destination.getCompound("bukkit").orElseThrow().putLong("lastPlayed", 20);
        destination.getCompound("Paper").orElseThrow().putLong("LastLogin", 18);
        destination.getCompound("Paper").orElseThrow().putLong("LastSeen", 20);

        assertEquals(WorldlineControlServer.comparablePlayerState(source),
            WorldlineControlServer.comparablePlayerState(destination));

        destination.putFloat("Health", 16.5F);
        assertNotEquals(WorldlineControlServer.comparablePlayerState(source),
            WorldlineControlServer.comparablePlayerState(destination));
    }

    @Test
    void snapshotComparisonCanonicalizesAttributeOrder() {
        CompoundTag movementSpeed = new CompoundTag();
        movementSpeed.putString("id", "minecraft:movement_speed");
        movementSpeed.putDouble("base", 0.1);
        CompoundTag maxHealth = new CompoundTag();
        maxHealth.putString("id", "minecraft:max_health");
        maxHealth.putDouble("base", 20.0);

        ListTag sourceAttributes = new ListTag();
        sourceAttributes.add(movementSpeed);
        sourceAttributes.add(maxHealth);
        CompoundTag source = new CompoundTag();
        source.put("attributes", sourceAttributes);

        ListTag destinationAttributes = new ListTag();
        destinationAttributes.add(maxHealth.copy());
        destinationAttributes.add(movementSpeed.copy());
        CompoundTag destination = new CompoundTag();
        destination.put("attributes", destinationAttributes);

        assertEquals(WorldlineControlServer.comparablePlayerState(source),
            WorldlineControlServer.comparablePlayerState(destination));
    }

    @Test
    void itemCooldownsRoundTripAtTheSameLogicalTick() {
        Identifier group = Identifier.parse("worldline:test");
        ItemCooldowns source = new ItemCooldowns();
        source.addCooldown(group, 10);
        source.tick();
        source.tick();

        CompoundTag snapshot = source.worldline$save();
        ItemCooldowns staged = new ItemCooldowns();
        staged.worldline$load(snapshot);

        assertEquals(8, staged.getRemainingCooldown(group));
        assertEquals(snapshot, staged.worldline$save());
    }

    private static void writeUuid(final DataOutputStream output, final UUID value)
        throws IOException {
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(final DataInputStream input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    private static final class MemorySocket extends Socket {
        private final ByteArrayInputStream request;
        private final ByteArrayOutputStream response = new ByteArrayOutputStream();

        private MemorySocket(final byte[] request) {
            this.request = new ByteArrayInputStream(request);
        }

        @Override
        public InputStream getInputStream() {
            return this.request;
        }

        @Override
        public OutputStream getOutputStream() {
            return this.response;
        }

        private byte[] response() {
            return this.response.toByteArray();
        }
    }
}
