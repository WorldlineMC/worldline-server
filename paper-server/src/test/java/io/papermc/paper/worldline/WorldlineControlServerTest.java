package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.resources.Identifier;
import net.minecraft.world.item.ItemCooldowns;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineControlServerTest {

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
}
