package net.minecraft.server.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.spottedleaf.moonrise.common.util.TickThread;
import io.papermc.paper.entity.activation.ActivationRange;
import io.papermc.paper.worldline.WorldlineControlServer;
import io.papermc.paper.worldline.WorldlineTransferLifecycle;
import java.lang.reflect.Field;
import java.util.UUID;
import net.minecraft.core.Holder;
import net.minecraft.network.protocol.game.ClientboundBlockChangedAckPacket;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.util.TickThrottler;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.entity.EntityTypes;
import net.minecraft.world.food.FoodData;
import org.bukkit.support.environment.Normal;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

@Normal
public class WorldlineFrozenTickTest {
    private static final UUID PLAYER = UUID.fromString("00000000-0000-0000-0000-000000000121");
    private static final UUID TRANSFER = UUID.fromString("00000000-0000-0000-0000-000000000122");

    @Test
    void connectionTickKeepsProtocolAliveWithoutSimulationUntilAbort() throws Exception {
        var lifecycle = new WorldlineTransferLifecycle(8);
        var player = player();
        var food = mock(FoodData.class);
        // A sentinel for the simulation/food work entered through ServerPlayer.doTick().
        doAnswer(ignored -> { food.tick(player); return null; }).when(player).doTick();
        var listener = listener(player);
        setField(ServerGamePacketListenerImpl.class, listener, "ackBlockChangesUpTo", 12);
        try (var frozen = frozenFrom(lifecycle)) {
            assertEquals(WorldlineTransferLifecycle.Outcome.APPLIED,
                lifecycle.freezeSource(PLAYER, TRANSFER, 0));
            listener.tick();
            listener.tick();
            verify(player, never()).doTick();
            verify(food, never()).tick(player);
            verify(listener, times(2)).keepConnectionAlive();
            verify((TickThrottler) field(ServerGamePacketListenerImpl.class, listener,
                "chatSpamThrottler"), times(2)).tick();
            verify(listener).send(any(ClientboundBlockChangedAckPacket.class));
            assertEquals(-1, field(ServerGamePacketListenerImpl.class, listener, "ackBlockChangesUpTo"));
            assertEquals(0, field(ServerGamePacketListenerImpl.class, listener, "tickCount"));

            assertEquals(WorldlineTransferLifecycle.Outcome.APPLIED,
                lifecycle.abortSource(PLAYER, TRANSFER, 0));
            listener.tick();
            verify(player).doTick();
            verify(food).tick(player);
            verify(listener, times(3)).keepConnectionAlive();
            assertEquals(1, field(ServerGamePacketListenerImpl.class, listener, "tickCount"));
        }
    }

    @Test
    void committedSourceStaysFrozenAndOrdinaryPlayerStillTicks() throws Exception {
        var lifecycle = new WorldlineTransferLifecycle(8);
        var player = player();
        var listener = listener(player);
        try (var frozen = frozenFrom(lifecycle)) {
            listener.tick();
            verify(player).doTick();
            lifecycle.freezeSource(PLAYER, TRANSFER, 0);
            lifecycle.commitSource(PLAYER, TRANSFER, 0, 1);
            assertEquals(WorldlineTransferLifecycle.Outcome.REJECTED_MISMATCH,
                lifecycle.abortSource(PLAYER, TRANSFER, 0));
            listener.tick();
            verify(player).doTick(); // No second simulation tick after commit.
            verify(listener, times(2)).keepConnectionAlive();
        }
    }

    @Test
    void worldTickAlsoSkipsFrozenPlayersAndResumesAfterAbort() {
        var lifecycle = new WorldlineTransferLifecycle(8);
        var player = player();
        Holder<EntityType<?>> type = EntityTypes.PLAYER.builtInRegistryHolder();
        when(player.typeHolder()).thenReturn(type);
        var level = mock(ServerLevel.class);
        doCallRealMethod().when(level).tickNonPassenger(player);
        try (var frozen = frozenFrom(lifecycle);
             var tickThread = mockStatic(TickThread.class);
             var activation = mockStatic(ActivationRange.class)) {
            activation.when(() -> ActivationRange.checkIfActive(player)).thenReturn(true);
            lifecycle.freezeSource(PLAYER, TRANSFER, 0);
            level.tickNonPassenger(player);
            verify(player, never()).tick();
            verify(player, never()).postTick();
            assertEquals(0, player.tickCount);
            lifecycle.abortSource(PLAYER, TRANSFER, 0);
            level.tickNonPassenger(player);
            verify(player).tick();
            verify(player).postTick();
            assertEquals(1, player.tickCount);
        }
    }

    private static ServerPlayer player() {
        var player = mock(ServerPlayer.class);
        when(player.getUUID()).thenReturn(PLAYER);
        when(player.getRootVehicle()).thenReturn(player);
        return player;
    }

    private static ServerGamePacketListenerImpl listener(final ServerPlayer player) throws Exception {
        var listener = mock(ServerGamePacketListenerImpl.class);
        listener.player = player;
        setField(ServerCommonPacketListenerImpl.class, listener, "server", mock(MinecraftServer.class));
        setField(ServerGamePacketListenerImpl.class, listener, "ackBlockChangesUpTo", -1);
        for (String name : new String[]{"dropSpamThrottler", "chatSpamThrottler", "commandSpamThrottler",
            "tabSpamThrottler", "recipeSpamPackets"}) {
            setField(ServerGamePacketListenerImpl.class, listener, name, mock(TickThrottler.class));
        }
        doCallRealMethod().when(listener).tick();
        return listener;
    }

    private static MockedStatic<WorldlineControlServer> frozenFrom(final WorldlineTransferLifecycle lifecycle) {
        var frozen = mockStatic(WorldlineControlServer.class);
        frozen.when(() -> WorldlineControlServer.isFrozen(PLAYER)).thenAnswer(ignored ->
            lifecycle.source(PLAYER).map(state -> state.phase() == WorldlineTransferLifecycle.SourcePhase.FROZEN
                || state.phase() == WorldlineTransferLifecycle.SourcePhase.COMMITTED_AWAY).orElse(false));
        return frozen;
    }

    private static void setField(final Class<?> owner, final Object object, final String name,
                                 final Object value) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(object, value);
    }

    private static Object field(final Class<?> owner, final Object object, final String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }
}
