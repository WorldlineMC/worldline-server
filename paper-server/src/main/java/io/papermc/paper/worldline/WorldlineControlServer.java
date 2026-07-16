package io.papermc.paper.worldline;

import com.mojang.authlib.GameProfile;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.Identifier;
import net.minecraft.resources.ResourceKey;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ChunkLevel;
import net.minecraft.server.level.ClientInformation;
import net.minecraft.server.level.FullChunkStatus;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.level.TicketType;
import net.minecraft.util.Mth;
import net.minecraft.util.StringUtil;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Slice-only TCP endpoint for the M2/M3 handoff control plane. */
public final class WorldlineControlServer {
    private static final Logger LOGGER = LoggerFactory.getLogger("WorldlineControl");
    private static final int MAGIC = 0x574c4d32;
    private static final int PROTOCOL_VERSION = 2;
    private static final long PREPARE_TIMEOUT_MILLIS = 1_500;
    private static final Set<String> SOURCE_COMMANDS = Set.of(
        "CHECK_PREPARE", "FREEZE_SOURCE", "CLEAN_SOURCE"
    );
    private static final Set<String> DESTINATION_COMMANDS = Set.of(
        "PREPARE", "ABORT", "STAGE_SNAPSHOT", "COMMIT", "ACTIVATE_DESTINATION"
    );
    private static final AtomicBoolean STARTED = new AtomicBoolean();
    private static final Map<UUID, Preparation> PREPARATIONS = new ConcurrentHashMap<>();

    private WorldlineControlServer() {
    }

    public static void start() {
        final String serverId = System.getProperty("worldline.server-id", "");
        final String partitionId = System.getProperty("worldline.partition-id", "");
        final String compatibilityId = System.getProperty("worldline.compatibility-id", "");
        final int port = Integer.getInteger("worldline.control-port", 0);
        final long partitionEpoch = Long.getLong("worldline.partition-epoch", 0L);
        if (serverId.isBlank() || partitionId.isBlank() || port == 0) {
            return;
        }
        if (compatibilityId.isBlank() || port < 1 || port > 65535 || partitionEpoch < 1) {
            throw new IllegalArgumentException("Invalid Worldline control endpoint configuration");
        }
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        final Thread thread = new Thread(
            () -> serve(serverId, partitionId, partitionEpoch, compatibilityId, port),
            "Worldline control " + serverId
        );
        thread.setDaemon(true);
        thread.start();
    }

    private static void serve(final String serverId, final String partitionId,
                              final long partitionEpoch, final String compatibilityId,
                              final int port) {
        try (ServerSocket listener = new ServerSocket()) {
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            LOGGER.info("Worldline control listening on 127.0.0.1:{} as owner of {} epoch {}",
                port, partitionId, partitionEpoch);
            while (!Thread.currentThread().isInterrupted()) {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(2_000);
                    handle(socket, serverId, partitionId, partitionEpoch, compatibilityId);
                } catch (EOFException ignored) {
                } catch (IOException | RuntimeException e) {
                    LOGGER.warn("Worldline control request failed: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            LOGGER.error("Worldline control endpoint failed", e);
        }
    }

    private static void handle(final Socket socket, final String serverId,
                               final String partitionId, final long partitionEpoch,
                               final String compatibilityId) throws IOException {
        final DataInputStream input = new DataInputStream(socket.getInputStream());
        if (input.readInt() != MAGIC) {
            throw new IOException("invalid magic");
        }
        final int protocolVersion = input.readInt();
        final String command = input.readUTF();
        final UUID transferId = readUuid(input);
        final UUID playerId = readUuid(input);
        final String sourceServerId = input.readUTF();
        final String destinationServerId = input.readUTF();
        final String sourcePartitionId = input.readUTF();
        final long sourcePartitionEpoch = input.readLong();
        final String destinationPartitionId = input.readUTF();
        final long destinationPartitionEpoch = input.readLong();
        final long playerSessionEpoch = input.readLong();
        final long playerStateVersion = input.readLong();
        final PrepareTarget target = input.readBoolean() ? readTarget(input) : null;

        final boolean sourceCommand = SOURCE_COMMANDS.contains(command);
        final boolean knownCommand = sourceCommand || DESTINATION_COMMANDS.contains(command);
        final String expectedServer = sourceCommand ? sourceServerId : destinationServerId;
        final String expectedPartition = sourceCommand ? sourcePartitionId : destinationPartitionId;
        final long expectedEpoch = sourceCommand ? sourcePartitionEpoch : destinationPartitionEpoch;
        CommandResult result;
        if (protocolVersion != PROTOCOL_VERSION || !knownCommand
            || !serverId.equals(expectedServer) || !partitionId.equals(expectedPartition)
            || partitionEpoch != expectedEpoch) {
            result = CommandResult.rejected("identity, protocol, or ownership fence rejected");
        } else if (command.equals("CHECK_PREPARE")) {
            result = checkReady(target, compatibilityId, false);
        } else if (command.equals("PREPARE")) {
            result = checkReady(target, compatibilityId, true);
            if (result.accepted()) {
                result = prepareDestination(Objects.requireNonNull(target), transferId, playerId,
                    playerSessionEpoch);
            }
        } else if (command.equals("ABORT")) {
            result = discardPreparation(playerId, transferId);
        } else {
            result = CommandResult.accepted("accepted");
        }

        final DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(MAGIC);
        output.writeInt(PROTOCOL_VERSION);
        output.writeBoolean(result.accepted());
        output.writeUTF(result.detail());
        output.writeInt(protocolVersion);
        writeUuid(output, transferId);
        writeUuid(output, playerId);
        output.writeUTF(sourceServerId);
        output.writeUTF(destinationServerId);
        output.writeUTF(sourcePartitionId);
        output.writeUTF(destinationPartitionId);
        output.writeLong(sourcePartitionEpoch);
        output.writeLong(destinationPartitionEpoch);
        output.writeLong(playerSessionEpoch);
        output.writeLong(playerStateVersion);
        output.writeUTF(serverId);
        output.writeUTF(partitionId);
        output.writeLong(partitionEpoch);
        output.flush();
        LOGGER.info("Worldline control command={} transfer={} accepted={} detail={}",
            command, transferId, result.accepted(), result.detail());
    }

    private static CommandResult checkReady(final PrepareTarget target,
                                            final String compatibilityId,
                                            final boolean destination) {
        final MinecraftServer server = MinecraftServer.getServer();
        if (target == null) {
            return CommandResult.rejected("prepare target missing");
        }
        if (server == null || !server.isRunning() || server.isStopped()
            || server.getPlayerList() == null) {
            return CommandResult.rejected("server is not active");
        }
        if (destination && Boolean.getBoolean("worldline.draining")) {
            return CommandResult.rejected("destination is draining");
        }
        if (!compatibilityId.equals(target.compatibilityId())) {
            return CommandResult.rejected("registry or client configuration is incompatible");
        }
        if (!server.storageSource.getLevelId().equals(target.levelName())) {
            return CommandResult.rejected("level is incompatible");
        }
        final ResourceKey<Level> dimension;
        try {
            dimension = ResourceKey.create(Registries.DIMENSION,
                Identifier.parse(target.dimension()));
        } catch (RuntimeException e) {
            return CommandResult.rejected("dimension is invalid");
        }
        if (server.getLevel(dimension) == null) {
            return CommandResult.rejected("dimension is unavailable");
        }
        return CommandResult.accepted("ready");
    }

    private static CommandResult prepareDestination(final PrepareTarget target,
                                                    final UUID transferId,
                                                    final UUID playerId,
                                                    final long playerSessionEpoch) {
        final MinecraftServer server = MinecraftServer.getServer();
        if (server.getPlayerList().getPlayer(playerId) != null) {
            return CommandResult.rejected("player is already active on destination");
        }
        final Preparation preparation = new Preparation(transferId, playerId,
            playerSessionEpoch, target);
        final Preparation existing = PREPARATIONS.putIfAbsent(playerId, preparation);
        if (existing != null) {
            if (!existing.matches(transferId, playerSessionEpoch, target)) {
                return CommandResult.rejected("another transfer is already prepared");
            }
            return awaitPreparation(existing);
        }
        try {
            server.execute(() -> startPreparation(server, preparation));
        } catch (RuntimeException e) {
            PREPARATIONS.remove(playerId, preparation);
            return CommandResult.rejected("server stopped before preparation");
        }
        return awaitPreparation(preparation);
    }

    private static void startPreparation(final MinecraftServer server,
                                         final Preparation preparation) {
        if (preparation.cancelled || server.isStopped()) {
            failPreparation(preparation, "preparation cancelled");
            return;
        }
        final ResourceKey<Level> dimension = ResourceKey.create(Registries.DIMENSION,
            Identifier.parse(preparation.target.dimension()));
        final ServerLevel level = server.getLevel(dimension);
        if (level == null || server.getPlayerList().getPlayer(preparation.playerId) != null) {
            failPreparation(preparation, "destination became unavailable");
            return;
        }
        final ChunkPos center = new ChunkPos(Mth.floor(preparation.target.x()) >> 4,
            Mth.floor(preparation.target.z()) >> 4);
        final int ticketLevel = ChunkLevel.byStatus(FullChunkStatus.FULL)
            - preparation.target.visibilityRadiusChunks();
        final TicketType<?> ticketType = new TicketType<>(TicketType.NO_TIMEOUT,
            TicketType.FLAG_LOADING);
        preparation.level = level;
        preparation.center = center;
        preparation.ticketLevel = ticketLevel;
        preparation.ticketType = ticketType;
        preparation.ticketAdded = true;
        level.getChunkSource().addTicketAtLevel(ticketType, center, ticketLevel);
        level.getChunkSource().addTicketAndLoadWithRadius(ticketType, center,
            preparation.target.visibilityRadiusChunks()).whenComplete((ignored, failure) -> {
                try {
                    server.execute(() -> finishPreparation(server, preparation, failure));
                } catch (RuntimeException e) {
                    failPreparation(preparation, "server stopped during preparation");
                }
            });
    }

    private static void finishPreparation(final MinecraftServer server,
                                          final Preparation preparation,
                                          final Throwable failure) {
        if (preparation.cancelled || failure != null || server.isStopped()
            || server.getPlayerList().getPlayer(preparation.playerId) != null) {
            failPreparation(preparation, failure == null
                ? "preparation cancelled" : "chunk halo failed to load");
            return;
        }
        final ServerPlayer player = new ServerPlayer(server, preparation.level,
            new GameProfile(preparation.playerId, preparation.target.playerName()),
            ClientInformation.createDefault());
        player.setPos(preparation.target.x(), preparation.target.y(), preparation.target.z());
        preparation.player = player;
        preparation.ready.complete(null);
        LOGGER.info("Worldline prepared non-authoritative player={} transfer={} chunk={} "
                + "halo_radius={}", preparation.playerId, preparation.transferId,
            preparation.center, preparation.target.visibilityRadiusChunks());
    }

    private static CommandResult awaitPreparation(final Preparation preparation) {
        try {
            preparation.ready.get(PREPARE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return CommandResult.accepted("destination ready");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            discardPreparation(preparation.playerId, preparation.transferId);
            return CommandResult.rejected("preparation interrupted");
        } catch (TimeoutException e) {
            discardPreparation(preparation.playerId, preparation.transferId);
            return CommandResult.rejected("preparation timed out");
        } catch (ExecutionException e) {
            final String detail = e.getCause().getMessage();
            return CommandResult.rejected(detail == null ? "preparation failed" : detail);
        }
    }

    private static CommandResult discardPreparation(final UUID playerId,
                                                    final UUID transferId) {
        final Preparation preparation = PREPARATIONS.get(playerId);
        if (preparation == null) {
            return CommandResult.accepted("already discarded");
        }
        if (!preparation.transferId.equals(transferId)) {
            return CommandResult.rejected("different transfer is prepared");
        }
        preparation.cancelled = true;
        PREPARATIONS.remove(playerId, preparation);
        preparation.ready.completeExceptionally(new IllegalStateException("preparation discarded"));
        final MinecraftServer server = MinecraftServer.getServer();
        if (server != null && !server.isStopped()) {
            final CompletableFuture<Void> cleaned = new CompletableFuture<>();
            try {
                server.execute(() -> {
                    cleanupPreparation(preparation);
                    cleaned.complete(null);
                });
                cleaned.get(300, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CommandResult.rejected("discard interrupted");
            } catch (ExecutionException | TimeoutException e) {
                return CommandResult.rejected("discard pending");
            } catch (RuntimeException ignored) {
            }
        }
        return CommandResult.accepted("discarded");
    }

    private static void failPreparation(final Preparation preparation, final String detail) {
        preparation.cancelled = true;
        PREPARATIONS.remove(preparation.playerId, preparation);
        cleanupPreparation(preparation);
        preparation.ready.completeExceptionally(new IllegalStateException(detail));
    }

    private static void cleanupPreparation(final Preparation preparation) {
        if (preparation.ticketAdded && preparation.level != null
            && preparation.center != null && preparation.ticketType != null) {
            preparation.level.getChunkSource().removeTicketAtLevel(preparation.ticketType,
                preparation.center, preparation.ticketLevel);
            preparation.ticketAdded = false;
        }
        preparation.player = null;
    }

    private static PrepareTarget readTarget(final DataInputStream input) throws IOException {
        return new PrepareTarget(input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF(),
            input.readDouble(), input.readDouble(), input.readDouble(), input.readInt());
    }

    private static UUID readUuid(final DataInputStream input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    private static void writeUuid(final DataOutputStream output, final UUID value)
        throws IOException {
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }

    private record PrepareTarget(String playerName, String levelName, String dimension,
                                 String compatibilityId, double x, double y, double z,
                                 int visibilityRadiusChunks) {
        private PrepareTarget {
            if (!StringUtil.isReasonablePlayerName(playerName) || levelName.isBlank()
                || levelName.length() > 256 || dimension.isBlank() || dimension.length() > 256
                || compatibilityId.isBlank() || compatibilityId.length() > 256
                || !Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(z)
                || visibilityRadiusChunks < 1 || visibilityRadiusChunks > 8) {
                throw new IllegalArgumentException("invalid prepare target");
            }
        }
    }

    private record CommandResult(boolean accepted, String detail) {
        private static CommandResult accepted(final String detail) {
            return new CommandResult(true, detail);
        }

        private static CommandResult rejected(final String detail) {
            return new CommandResult(false, detail);
        }
    }

    private static final class Preparation {
        private final UUID transferId;
        private final UUID playerId;
        private final long playerSessionEpoch;
        private final PrepareTarget target;
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private volatile boolean cancelled;
        private ServerLevel level;
        private ChunkPos center;
        private TicketType<?> ticketType;
        private int ticketLevel;
        private boolean ticketAdded;
        private ServerPlayer player;

        private Preparation(final UUID transferId, final UUID playerId,
                            final long playerSessionEpoch, final PrepareTarget target) {
            this.transferId = transferId;
            this.playerId = playerId;
            this.playerSessionEpoch = playerSessionEpoch;
            this.target = target;
        }

        private boolean matches(final UUID transferId, final long playerSessionEpoch,
                                final PrepareTarget target) {
            return this.transferId.equals(transferId)
                && this.playerSessionEpoch == playerSessionEpoch && this.target.equals(target);
        }
    }
}
