package io.papermc.paper.worldline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.resources.Identifier;
import net.minecraft.world.item.ItemCooldowns;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;

@Normal
public class WorldlineControlServerTest {

    @Test
    void snapshotNbtReserializesByteExactly() throws Exception {
        CompoundTag player = new CompoundTag();
        player.putString("UUID", "00000000-0000-0000-0000-000000000041");
        player.putDouble("Health", 17.5);
        CompoundTag snapshot = new CompoundTag();
        snapshot.putInt("SnapshotSchemaVersion", 1);
        snapshot.putLong("PlayerStateVersion", 7);
        snapshot.put("Player", player);

        byte[] bytes = WorldlineControlServer.canonicalSnapshotEncoding(snapshot);

        assertTrue(WorldlineControlServer.isByteExactSnapshotEncoding(bytes));
        byte[] withTrailingData = Arrays.copyOf(bytes, bytes.length + 1);
        assertFalse(WorldlineControlServer.isByteExactSnapshotEncoding(withTrailingData));
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
