package io.papermc.paper.worldline;

import com.mojang.authlib.GameProfile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.network.Connection;
import net.minecraft.network.RegistryFriendlyByteBuf;
import net.minecraft.network.chat.Component;
import net.minecraft.network.protocol.game.GameProtocols;
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
import net.minecraft.server.network.CommonListenerCookie;
import net.minecraft.server.network.ServerGamePacketListenerImpl;
import net.minecraft.util.Mth;
import net.minecraft.util.StringUtil;
import net.minecraft.util.ProblemReporter;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.storage.TagValueInput;
import net.minecraft.world.level.storage.TagValueOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Slice-only TCP endpoint for the Worldline handoff control plane. */
public final class WorldlineControlServer {
    private static final Logger LOGGER = LoggerFactory.getLogger("WorldlineControl");
    private static final int MAGIC = 0x574c4d32;
    private static final int PROTOCOL_VERSION = 4;
    private static final int SNAPSHOT_SCHEMA_VERSION = 1;
    private static final int MAX_PAYLOAD_BYTES = 1_048_576;
    private static final long PREPARE_TIMEOUT_MILLIS = 1_500;
    private static final Set<String> SOURCE_COMMANDS = Set.of(
        "CHECK_PREPARE", "FREEZE_SOURCE", "ABORT_SOURCE", "COMMIT_SOURCE", "CLEAN_SOURCE"
    );
    private static final Set<String> DESTINATION_COMMANDS = Set.of(
        "PREPARE", "ABORT", "STAGE_SNAPSHOT", "COMMIT_DESTINATION",
        "ACTIVATE_DESTINATION", "RETIRE_DESTINATION"
    );
    private static final AtomicBoolean STARTED = new AtomicBoolean();
    private static final Map<UUID, Preparation> PREPARATIONS = new ConcurrentHashMap<>();
    private static final Map<UUID, FrozenPlayer> FROZEN_PLAYERS = new ConcurrentHashMap<>();
    private static final Map<UUID, FrozenPlayer> CLEANED_SOURCES = new ConcurrentHashMap<>();
    private static final Map<UUID, ActivatedDestination> ACTIVATED_DESTINATIONS =
        new ConcurrentHashMap<>();
    private static final Map<UUID, DestinationTerminal> RETIRED_DESTINATIONS =
        new ConcurrentHashMap<>();
    private static final int MAX_TERMINAL_IDENTITIES = 4_096;
    private static final WorldlineTransferLifecycle LIFECYCLE =
        new WorldlineTransferLifecycle(4_096);

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

    static void handle(final Socket socket, final String serverId,
                       final String partitionId, final long partitionEpoch,
                       final String compatibilityId) throws IOException {
        final DataInputStream input = new DataInputStream(socket.getInputStream());
        if (input.readInt() != MAGIC) {
            throw new IOException("invalid magic");
        }
        final int protocolVersion = input.readInt();
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IOException("unsupported protocol version");
        }
        final String command = input.readUTF();
        final UUID transferId = readUuid(input);
        final UUID playerId = readUuid(input);
        final UUID clientConnectionId = readUuid(input);
        final String sourceServerId = input.readUTF();
        final String destinationServerId = input.readUTF();
        final String sourcePartitionId = input.readUTF();
        final long sourcePartitionEpoch = input.readLong();
        final String destinationPartitionId = input.readUTF();
        final long destinationPartitionEpoch = input.readLong();
        final long playerSessionEpoch = input.readLong();
        final long playerStateVersion = input.readLong();
        final long routeGeneration = input.readLong();
        final PreparationIdentity preparationIdentity = new PreparationIdentity(
            clientConnectionId, sourceServerId, destinationServerId, sourcePartitionId,
            sourcePartitionEpoch, destinationPartitionId, destinationPartitionEpoch,
            routeGeneration);
        final PrepareTarget target = input.readBoolean() ? readTarget(input) : null;
        final int payloadLength = input.readInt();
        if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES) {
            throw new IOException("invalid payload length");
        }
        final byte[] payload = input.readNBytes(payloadLength);
        if (payload.length != payloadLength) {
            throw new IOException("truncated payload");
        }

        final boolean sourceCommand = SOURCE_COMMANDS.contains(command);
        final boolean knownCommand = sourceCommand || DESTINATION_COMMANDS.contains(command);
        final String expectedServer = sourceCommand ? sourceServerId : destinationServerId;
        final String expectedPartition = sourceCommand ? sourcePartitionId : destinationPartitionId;
        final long expectedEpoch = sourceCommand ? sourcePartitionEpoch : destinationPartitionEpoch;
        CommandResult result;
        if (!knownCommand || playerSessionEpoch < 0 || playerStateVersion < 0
            || routeGeneration < 0
            || !serverId.equals(expectedServer) || !partitionId.equals(expectedPartition)
            || partitionEpoch != expectedEpoch) {
            result = CommandResult.rejected("identity, protocol, or ownership fence rejected");
        } else if (command.equals("CHECK_PREPARE")) {
            result = checkReady(target, compatibilityId, false);
        } else if (command.equals("PREPARE")) {
            result = checkReady(target, compatibilityId, true);
            if (result.accepted()) {
                result = prepareDestination(Objects.requireNonNull(target), transferId, playerId,
                    playerSessionEpoch, preparationIdentity);
                if (result.accepted()) {
                    final CommandResult lifecycle = lifecycleResult(
                        LIFECYCLE.prepareDestination(playerId, transferId, playerSessionEpoch),
                        "destination prepared");
                    if (!lifecycle.accepted()) {
                        discardPreparation(playerId, transferId);
                        result = lifecycle;
                    }
                }
            }
        } else if (command.equals("ABORT")) {
            final WorldlineTransferLifecycle.Outcome outcome =
                LIFECYCLE.abortDestination(playerId, transferId, playerSessionEpoch);
            result = outcome == WorldlineTransferLifecycle.Outcome.MISSING
                ? discardPreparation(playerId, transferId)
                : lifecycleThen(outcome, "destination aborted",
                    () -> discardPreparation(playerId, transferId));
        } else if (command.equals("FREEZE_SOURCE")) {
            result = freezeSource(transferId, playerId, sourceServerId, destinationServerId,
                sourcePartitionId, sourcePartitionEpoch, destinationPartitionId,
                destinationPartitionEpoch, playerSessionEpoch, playerStateVersion,
                preparationIdentity);
            if (result.accepted()) {
                final CommandResult lifecycle = lifecycleResult(
                    LIFECYCLE.freezeSource(playerId, transferId, playerSessionEpoch),
                    "source frozen");
                if (!lifecycle.accepted()) {
                    unfreezeSource(playerId, transferId);
                    result = lifecycle;
                }
            }
        } else if (command.equals("ABORT_SOURCE")) {
            final WorldlineTransferLifecycle.Outcome outcome =
                LIFECYCLE.abortSource(playerId, transferId, playerSessionEpoch);
            result = outcome == WorldlineTransferLifecycle.Outcome.MISSING
                ? unfreezeSource(playerId, transferId)
                : lifecycleThen(outcome, "source aborted",
                    () -> unfreezeSource(playerId, transferId));
        } else if (command.equals("STAGE_SNAPSHOT")) {
            result = stageSnapshot(payload, transferId, playerId, sourceServerId,
                destinationServerId, sourcePartitionId, sourcePartitionEpoch,
                destinationPartitionId, destinationPartitionEpoch, playerSessionEpoch,
                playerStateVersion, preparationIdentity);
            if (result.accepted()) {
                result = lifecycleResult(
                    LIFECYCLE.stageDestination(playerId, transferId, playerSessionEpoch),
                    "snapshot staged");
            }
        } else if (command.equals("COMMIT_DESTINATION")) {
            result = commitDestination(playerId, transferId, playerSessionEpoch,
                playerStateVersion, preparationIdentity);
        } else if (command.equals("COMMIT_SOURCE")) {
            result = commitSource(playerId, transferId, playerSessionEpoch, playerStateVersion,
                preparationIdentity);
        } else if (command.equals("ACTIVATE_DESTINATION")) {
            result = activateDestination(playerId, transferId, playerSessionEpoch,
                playerStateVersion, preparationIdentity);
        } else if (command.equals("CLEAN_SOURCE")) {
            result = cleanSource(playerId, transferId, playerSessionEpoch, playerStateVersion,
                preparationIdentity);
        } else if (command.equals("RETIRE_DESTINATION")) {
            result = retireDestination(playerId, transferId, playerSessionEpoch,
                playerStateVersion, preparationIdentity);
        } else {
            throw new IllegalStateException("known command was not dispatched");
        }

        final DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(MAGIC);
        output.writeInt(PROTOCOL_VERSION);
        output.writeBoolean(result.accepted());
        output.writeUTF(result.detail());
        output.writeInt(result.payload().length);
        output.write(result.payload());
        output.writeInt(protocolVersion);
        writeUuid(output, transferId);
        writeUuid(output, playerId);
        writeUuid(output, clientConnectionId);
        output.writeUTF(sourceServerId);
        output.writeUTF(destinationServerId);
        output.writeUTF(sourcePartitionId);
        output.writeUTF(destinationPartitionId);
        output.writeLong(sourcePartitionEpoch);
        output.writeLong(destinationPartitionEpoch);
        output.writeLong(playerSessionEpoch);
        output.writeLong(playerStateVersion);
        output.writeLong(routeGeneration);
        output.writeUTF(serverId);
        output.writeUTF(partitionId);
        output.writeLong(partitionEpoch);
        output.flush();
        LOGGER.info("Worldline control command={} transfer={} accepted={} detail={}",
            command, transferId, result.accepted(), result.detail());
    }

    /** Used by the entity tick and damage paths to suppress frozen source simulation. */
    public static boolean isFrozen(final UUID playerId) {
        return FROZEN_PLAYERS.containsKey(playerId);
    }

    static int protocolVersionForTesting() {
        return PROTOCOL_VERSION;
    }

    static boolean isKnownCommandForTesting(final String command) {
        return SOURCE_COMMANDS.contains(command) || DESTINATION_COMMANDS.contains(command);
    }

    /**
     * Consumes the exact committed M4 preparation and binds the inbound play connection without
     * registering or activating the player. Must be called from the server thread.
     */
    public static AttachedDestination attachPreparedDestination(
        final WorldlineResumeContext context, final UUID loginPlayerId,
        final Connection connection, final CommonListenerCookie cookie
    ) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(loginPlayerId, "loginPlayerId");
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(cookie, "cookie");
        if (!Boolean.getBoolean("worldline.resume")) {
            throw new IllegalStateException("Worldline resume is disabled");
        }
        final MinecraftServer server = MinecraftServer.getServer();
        if (server == null || !server.isSameThread() || server.isStopped()) {
            throw new IllegalStateException("destination attachment requires the active server thread");
        }
        final Preparation preparation = PREPARATIONS.get(loginPlayerId);
        if (preparation == null || preparation.cancelled) {
            throw new IllegalStateException("matching committed destination preparation missing");
        }
        synchronized (preparation) {
            final WorldlineDestinationAttachment.Outcome validation =
                preparation.attachment.validate(context, loginPlayerId);
            if (validation != WorldlineDestinationAttachment.Outcome.APPLIED) {
                throw new IllegalStateException("destination attachment fence rejected: "
                    + validation.name().toLowerCase(java.util.Locale.ROOT));
            }
            final WorldlineTransferLifecycle.Outcome lifecycle = LIFECYCLE.attachDestination(
                loginPlayerId, context.transferId(), context.sourcePlayerEpoch(),
                context.committedPlayerEpoch());
            if (lifecycle != WorldlineTransferLifecycle.Outcome.APPLIED
                && lifecycle != WorldlineTransferLifecycle.Outcome.ALREADY_APPLIED) {
                throw new IllegalStateException("destination lifecycle is not committed");
            }
            final ServerPlayer preparedPlayer = Objects.requireNonNull(preparation.player,
                "preparedPlayer");
            if (server.getPlayerList().getPlayer(loginPlayerId) != null) {
                throw new IllegalStateException("destination player is already registered");
            }
            preparedPlayer.setId(context.priorEntityId());
            final ServerGamePacketListenerImpl listener = new ServerGamePacketListenerImpl(
                server, connection, preparedPlayer, cookie);
            connection.setupInboundProtocol(GameProtocols.SERVERBOUND_TEMPLATE.bind(
                RegistryFriendlyByteBuf.decorator(server.registryAccess()), listener), listener);
            listener.suspendFlushing();
            listener.worldlineSuppressInitialPackets = true;
            final WorldlineDestinationAttachment.Result<ServerPlayer> attached =
                preparation.attachment.attach(context, loginPlayerId);
            if (attached.outcome() != WorldlineDestinationAttachment.Outcome.APPLIED
                || attached.preparedPlayer() != preparedPlayer) {
                throw new IllegalStateException("destination preparation was consumed concurrently");
            }
            preparation.attachedConnection = connection;
            preparation.attachedListener = listener;
            listener.worldlineTransferId = context.transferId();
            preparation.attachedCookie = cookie;
            LOGGER.info("Worldline attached inert destination player={} transfer={} entity_id={} "
                    + "route_generation={}", loginPlayerId, context.transferId(),
                context.priorEntityId(), context.routeGeneration());
            return new AttachedDestination(preparedPlayer, listener);
        }
    }

    private static CommandResult commitDestination(final UUID playerId, final UUID transferId,
                                                   final long committedEpoch,
                                                   final long playerStateVersion,
                                                   final PreparationIdentity committedIdentity) {
        if (committedEpoch < 1) {
            return CommandResult.rejected("committed player epoch must be positive");
        }
        final Preparation preparation = PREPARATIONS.get(playerId);
        if (preparation == null || !preparation.transferId.equals(transferId)
            || preparation.playerSessionEpoch != committedEpoch - 1
            || preparation.playerStateVersion != playerStateVersion
            || !preparation.identity.matchesCommitted(committedIdentity)) {
            return CommandResult.rejected("matching destination preparation missing");
        }
        synchronized (preparation) {
            try {
                preparation.attachment.commit(committedEpoch,
                    committedIdentity.routeGeneration);
            } catch (IllegalArgumentException | IllegalStateException e) {
                return CommandResult.rejected("destination commit fence rejected");
            }
            return lifecycleResult(LIFECYCLE.commitDestination(playerId, transferId,
                committedEpoch - 1, committedEpoch), "destination committed");
        }
    }

    private static CommandResult commitSource(final UUID playerId, final UUID transferId,
                                              final long committedEpoch,
                                              final long playerStateVersion,
                                              final PreparationIdentity identity) {
        if (committedEpoch < 1) {
            return CommandResult.rejected("committed player epoch must be positive");
        }
        return onServerThread(() -> {
            final FrozenPlayer frozen = FROZEN_PLAYERS.get(playerId);
            if (frozen == null || !frozen.matchesCommitted(transferId, committedEpoch - 1,
                playerStateVersion, identity)) {
                return CommandResult.rejected("matching frozen source missing");
            }
            if (frozen.committedEpoch == committedEpoch) {
                return CommandResult.accepted("already source committed away");
            }
            final MinecraftServer server = MinecraftServer.getServer();
            final ServerPlayer player = server.getPlayerList().getPlayer(playerId);
            if (player == null) {
                return CommandResult.rejected("frozen source player missing");
            }
            final CommandResult lifecycle = lifecycleResult(LIFECYCLE.commitSource(playerId,
                transferId, committedEpoch - 1, committedEpoch), "source committed away");
            if (!lifecycle.accepted()) {
                return lifecycle;
            }
            player.worldlineCommittedAway = true;
            FROZEN_PLAYERS.put(playerId, frozen.withCommittedEpoch(committedEpoch));
            LOGGER.info("Worldline committed away source player={} transfer={} tick={} "
                    + "committed_epoch={} authority_millis={}", playerId, transferId,
                server.getTickCount(), committedEpoch, System.currentTimeMillis());
            return lifecycle;
        });
    }

    private static CommandResult cleanSource(final UUID playerId, final UUID transferId,
                                             final long committedEpoch,
                                             final long playerStateVersion,
                                             final PreparationIdentity identity) {
        if (committedEpoch < 1) {
            return CommandResult.rejected("committed player epoch must be positive");
        }
        return onServerThread(() -> {
            final FrozenPlayer frozen = FROZEN_PLAYERS.get(playerId);
            if (frozen == null) {
                final FrozenPlayer cleaned = CLEANED_SOURCES.get(playerId);
                if (cleaned == null || cleaned.committedEpoch != committedEpoch
                    || !cleaned.matchesCommitted(transferId, committedEpoch - 1,
                        playerStateVersion, identity)) {
                    return CommandResult.rejected("source cleanup terminal fence mismatch");
                }
                return lifecycleResult(LIFECYCLE.cleanSource(playerId, transferId,
                    committedEpoch - 1, committedEpoch), "source cleaned");
            }
            if (frozen.committedEpoch != committedEpoch
                || !frozen.matchesCommitted(transferId, committedEpoch - 1,
                    playerStateVersion, identity)) {
                return CommandResult.rejected("source cleanup fence mismatch");
            }
            final CommandResult lifecycle = lifecycleResult(LIFECYCLE.cleanSource(playerId,
                transferId, committedEpoch - 1, committedEpoch), "source cleaned");
            if (!lifecycle.accepted()) {
                return lifecycle;
            }
            final MinecraftServer server = MinecraftServer.getServer();
            final ServerPlayer player = server.getPlayerList().getPlayer(playerId);
            if (player != null) {
                server.getPlayerList().worldlineCleanSourcePlayer(player);
            }
            FROZEN_PLAYERS.remove(playerId, frozen);
            ACTIVATED_DESTINATIONS.remove(playerId);
            rememberTerminal(CLEANED_SOURCES, playerId, frozen);
            LOGGER.info("Worldline cleaned source player={} transfer={} tick={} committed_epoch={}",
                playerId, transferId, server.getTickCount(), committedEpoch);
            return lifecycle;
        });
    }

    private static CommandResult activateDestination(final UUID playerId,
                                                     final UUID transferId,
                                                     final long committedEpoch,
                                                     final long playerStateVersion,
                                                     final PreparationIdentity committedIdentity) {
        if (committedEpoch < 1) {
            return CommandResult.rejected("committed player epoch must be positive");
        }
        return onServerThread(() -> {
            final Preparation preparation = PREPARATIONS.get(playerId);
            if (preparation == null) {
                final ActivatedDestination activated = ACTIVATED_DESTINATIONS.get(playerId);
                if (activated != null && activated.matches(transferId, committedEpoch,
                    playerStateVersion, committedIdentity)) {
                    return CommandResult.accepted("already destination activated");
                }
                return CommandResult.rejected("matching attached destination missing");
            }
            if (!preparation.transferId.equals(transferId)
                || preparation.playerSessionEpoch != committedEpoch - 1
                || preparation.playerStateVersion != playerStateVersion
                || !preparation.identity.matchesCommitted(committedIdentity)) {
                return CommandResult.rejected("matching attached destination missing");
            }
            synchronized (preparation) {
                final WorldlineTransferLifecycle.DestinationState state =
                    LIFECYCLE.destination(playerId).orElse(null);
                if (state == null || !state.transferId().equals(transferId)
                    || state.sourceEpoch() != committedEpoch - 1
                    || state.committedEpoch() != committedEpoch) {
                    return CommandResult.rejected("destination activation fence mismatch");
                }
                if (state.phase() == WorldlineTransferLifecycle.DestinationPhase.ACTIVE) {
                    return CommandResult.accepted("already destination activated");
                }
                if (state.phase()
                    != WorldlineTransferLifecycle.DestinationPhase.CONNECTION_ATTACHED
                    || preparation.attachedListener == null || preparation.player == null) {
                    return CommandResult.rejected("destination connection is not attached");
                }
                final String beforeMismatch = preparedSnapshotMismatch(preparation);
                if (beforeMismatch != null) {
                    return CommandResult.rejected(beforeMismatch);
                }
                final MinecraftServer server = MinecraftServer.getServer();
                server.getPlayerList().worldlineActivatePlayer(preparation.player);
                final String afterMismatch = preparedSnapshotMismatch(preparation);
                if (afterMismatch != null) {
                    server.getPlayerList().worldlineRetirePlayer(preparation.player);
                    LIFECYCLE.retireDestination(playerId, transferId, committedEpoch - 1,
                        committedEpoch);
                    return CommandResult.rejected(afterMismatch);
                }
                final CommandResult lifecycle = lifecycleResult(LIFECYCLE.activateDestination(
                    playerId, transferId, committedEpoch - 1, committedEpoch),
                    "destination activated");
                if (!lifecycle.accepted()) {
                    server.getPlayerList().worldlineRetirePlayer(preparation.player);
                    return lifecycle;
                }
                preparation.attachedListener.worldlineSuppressInitialPackets = false;
                preparation.attachedListener.worldlineAwaitingFirstReplayMovement = true;
                preparation.attachedListener.resumeFlushing();
                final CommandResult cleaned = lifecycleResult(LIFECYCLE.cleanDestination(
                    playerId, transferId, committedEpoch - 1, committedEpoch),
                    "destination activation resources released");
                if (!cleaned.accepted()) {
                    preparation.attachedListener.worldlineSuppressInitialPackets = true;
                    server.getPlayerList().worldlineRetirePlayer(preparation.player);
                    return cleaned;
                }
                final ActivatedDestination activated = new ActivatedDestination(transferId,
                    committedEpoch, playerStateVersion, committedIdentity, preparation.player,
                    preparation.attachedListener, preparation.attachedConnection);
                PREPARATIONS.remove(playerId, preparation);
                cleanupPreparation(preparation);
                ACTIVATED_DESTINATIONS.put(playerId, activated);
                LOGGER.info("Worldline activated destination player={} transfer={} tick={} "
                        + "committed_epoch={} snapshot_hash={} authority_millis={}", playerId,
                    transferId, server.getTickCount(), committedEpoch,
                    playerStateHash(activated.player), System.currentTimeMillis());
                return lifecycle;
            }
        });
    }

    private static CommandResult retireDestination(final UUID playerId, final UUID transferId,
                                                   final long committedEpoch,
                                                   final long playerStateVersion,
                                                   final PreparationIdentity committedIdentity) {
        if (committedEpoch < 1) {
            return CommandResult.rejected("committed player epoch must be positive");
        }
        final Connection[] connectionToClose = new Connection[1];
        final CommandResult result = onServerThread(() -> {
            final Preparation preparation = PREPARATIONS.get(playerId);
            if (preparation == null) {
                final ActivatedDestination activated = ACTIVATED_DESTINATIONS.get(playerId);
                if (activated != null) {
                    if (!activated.matches(transferId, committedEpoch, playerStateVersion,
                        committedIdentity)) {
                        return CommandResult.rejected(
                            "destination retirement active fence mismatch");
                    }
                    final CommandResult lifecycle = lifecycleResult(LIFECYCLE.retireDestination(
                        playerId, transferId, committedEpoch - 1, committedEpoch),
                        "destination retired");
                    if (!lifecycle.accepted()) {
                        return lifecycle;
                    }
                    final MinecraftServer server = MinecraftServer.getServer();
                    if (server.getPlayerList().getPlayer(playerId) == activated.player) {
                        server.getPlayerList().worldlineRetirePlayer(activated.player);
                    }
                    activated.listener.worldlineSuppressInitialPackets = true;
                    connectionToClose[0] = activated.connection;
                    ACTIVATED_DESTINATIONS.remove(playerId, activated);
                    rememberTerminal(RETIRED_DESTINATIONS, playerId,
                        new DestinationTerminal(transferId, committedEpoch, playerStateVersion,
                            committedIdentity));
                    return lifecycle;
                }
                final DestinationTerminal retired = RETIRED_DESTINATIONS.get(playerId);
                if (retired == null || !retired.matches(transferId, committedEpoch,
                    playerStateVersion, committedIdentity)) {
                    return CommandResult.rejected("destination retirement terminal fence mismatch");
                }
                return lifecycleResult(LIFECYCLE.retireDestination(playerId, transferId,
                    committedEpoch - 1, committedEpoch), "destination retired");
            }
            synchronized (preparation) {
                if (!preparation.transferId.equals(transferId)
                    || preparation.playerSessionEpoch != committedEpoch - 1
                    || preparation.playerStateVersion != playerStateVersion
                    || !preparation.identity.matchesCommitted(committedIdentity)) {
                    return CommandResult.rejected("destination retirement fence mismatch");
                }
                final CommandResult lifecycle = lifecycleResult(LIFECYCLE.retireDestination(
                    playerId, transferId, committedEpoch - 1, committedEpoch),
                    "destination retired");
                if (!lifecycle.accepted()) {
                    return lifecycle;
                }
                final MinecraftServer server = MinecraftServer.getServer();
                if (preparation.player != null
                    && server.getPlayerList().getPlayer(playerId) == preparation.player) {
                    server.getPlayerList().worldlineRetirePlayer(preparation.player);
                }
                if (preparation.attachedListener != null) {
                    preparation.attachedListener.worldlineSuppressInitialPackets = true;
                }
                connectionToClose[0] = preparation.attachedConnection;
                preparation.attachment.retire();
                cleanupPreparation(preparation);
                PREPARATIONS.remove(playerId, preparation);
                rememberTerminal(RETIRED_DESTINATIONS, playerId,
                    new DestinationTerminal(transferId, committedEpoch, playerStateVersion,
                        committedIdentity));
                LOGGER.info("Worldline retired destination player={} transfer={} tick={}",
                    playerId, transferId, server.getTickCount());
                return lifecycle;
            }
        });
        if (result.accepted() && connectionToClose[0] != null) {
            connectionToClose[0].disconnect(Component.literal("Worldline destination retired"));
        }
        return result;
    }

    private static String preparedSnapshotMismatch(final Preparation preparation) {
        if (preparation.player == null || preparation.stagedSnapshot == null) {
            return "staged destination snapshot missing";
        }
        try {
            final CompoundTag root = decodeSnapshot(preparation.stagedSnapshot);
            final CompoundTag expectedPlayer = comparablePlayerState(
                root.getCompound("Player").orElseThrow());
            final TagValueOutput output = TagValueOutput.createWithContext(
                ProblemReporter.DISCARDING, preparation.player.registryAccess());
            preparation.player.saveWithoutId(output);
            final CompoundTag actualPlayer = comparablePlayerState(output.buildResult());
            if (!expectedPlayer.equals(actualPlayer)) {
                return "prepared destination snapshot changed; differing keys="
                    + differingKeys(expectedPlayer, actualPlayer);
            }
            final CompoundTag expectedTransient = root.getCompound("Transient").orElseThrow();
            if (!expectedTransient.equals(preparation.player.worldline$saveTransientState())) {
                return "prepared destination transient state changed";
            }
            return null;
        } catch (IOException | RuntimeException e) {
            return "prepared destination snapshot verification failed";
        }
    }

    private static CommandResult lifecycleThen(final WorldlineTransferLifecycle.Outcome outcome,
                                               final String detail,
                                               final Supplier<CommandResult> operation) {
        final CommandResult lifecycle = lifecycleResult(outcome, detail);
        if (!lifecycle.accepted()) {
            return lifecycle;
        }
        final CommandResult applied = operation.get();
        return applied.accepted() ? lifecycle : applied;
    }

    private static CommandResult lifecycleResult(
        final WorldlineTransferLifecycle.Outcome outcome, final String detail
    ) {
        return switch (outcome) {
            case APPLIED -> CommandResult.accepted(detail);
            case ALREADY_APPLIED -> CommandResult.accepted("already " + detail);
            case REJECTED_MISMATCH -> CommandResult.rejected(detail + " fence mismatch");
            case MISSING -> CommandResult.rejected(detail + " state missing");
        };
    }

    private static CommandResult freezeSource(final UUID transferId, final UUID playerId,
                                              final String sourceServerId,
                                              final String destinationServerId,
                                              final String sourcePartitionId,
                                              final long sourcePartitionEpoch,
                                              final String destinationPartitionId,
                                              final long destinationPartitionEpoch,
                                              final long playerSessionEpoch,
                                              final long playerStateVersion,
                                              final PreparationIdentity identity) {
        if (playerStateVersion < 1) {
            return CommandResult.rejected("player state version must be positive");
        }
        return onServerThread(() -> {
            final FrozenPlayer existing = FROZEN_PLAYERS.get(playerId);
            if (existing != null) {
                return existing.matches(transferId, playerSessionEpoch, playerStateVersion,
                    identity)
                    ? CommandResult.accepted("already frozen", existing.snapshot)
                    : CommandResult.rejected("player is frozen by another transfer");
            }
            final MinecraftServer server = MinecraftServer.getServer();
            final ServerPlayer player = server.getPlayerList().getPlayer(playerId);
            final String unsupported = unsupportedState(player);
            if (unsupported != null) {
                return CommandResult.rejected(unsupported);
            }
            FROZEN_PLAYERS.put(playerId, new FrozenPlayer(transferId, playerSessionEpoch,
                playerStateVersion, new byte[0], 0, identity));
            try {
                final byte[] snapshot = encodeSnapshot(player, server.getTickCount(), transferId,
                    sourceServerId, destinationServerId, sourcePartitionId,
                    sourcePartitionEpoch, destinationPartitionId, destinationPartitionEpoch,
                    playerSessionEpoch, playerStateVersion);
                CLEANED_SOURCES.remove(playerId);
                player.connection.worldlineTransferId = transferId;
                player.connection.worldlineLoggedRejectedFrozenMovement = false;
                FROZEN_PLAYERS.put(playerId, new FrozenPlayer(transferId, playerSessionEpoch,
                    playerStateVersion, snapshot, 0, identity));
                LOGGER.info("Worldline froze source player={} transfer={} tick={} "
                        + "player_state_version={} snapshot_bytes={} snapshot_hash={} "
                        + "x={} y={} z={}", playerId, transferId, server.getTickCount(),
                    playerStateVersion, snapshot.length, snapshotStateHash(snapshot),
                    player.getX(), player.getY(), player.getZ());
                return CommandResult.accepted("source frozen", snapshot);
            } catch (IOException | RuntimeException e) {
                FROZEN_PLAYERS.remove(playerId);
                return CommandResult.rejected("snapshot failed: " + e.getMessage());
            }
        });
    }

    private static CommandResult unfreezeSource(final UUID playerId, final UUID transferId) {
        return onServerThread(() -> {
            final FrozenPlayer frozen = FROZEN_PLAYERS.get(playerId);
            if (frozen == null) {
                return CommandResult.accepted("already active");
            }
            if (!frozen.transferId.equals(transferId)) {
                return CommandResult.rejected("different transfer froze source");
            }
            FROZEN_PLAYERS.remove(playerId, frozen);
            final ServerPlayer player = MinecraftServer.getServer().getPlayerList()
                .getPlayer(playerId);
            if (player != null && player.connection != null) {
                player.connection.worldlineTransferId = null;
                player.connection.worldlineLoggedRejectedFrozenMovement = false;
            }
            LOGGER.info("Worldline unfroze source player={} transfer={}", playerId, transferId);
            return CommandResult.accepted("source active");
        });
    }

    private static CommandResult stageSnapshot(final byte[] payload, final UUID transferId,
                                               final UUID playerId,
                                               final String sourceServerId,
                                               final String destinationServerId,
                                               final String sourcePartitionId,
                                               final long sourcePartitionEpoch,
                                               final String destinationPartitionId,
                                               final long destinationPartitionEpoch,
                                               final long playerSessionEpoch,
                                               final long playerStateVersion,
                                               final PreparationIdentity identity) {
        if (payload.length == 0) {
            return CommandResult.rejected("snapshot missing");
        }
        return onServerThread(() -> {
            final Preparation preparation = PREPARATIONS.get(playerId);
            if (preparation == null || !preparation.transferId.equals(transferId)
                || preparation.playerSessionEpoch != playerSessionEpoch
                || !preparation.identity.equals(identity)
                || preparation.player == null || preparation.cancelled) {
                return CommandResult.rejected("matching destination preparation missing");
            }
            if (preparation.stagedSnapshot != null) {
                return java.util.Arrays.equals(preparation.stagedSnapshot, payload)
                    ? CommandResult.accepted("already staged")
                    : CommandResult.rejected("different snapshot already staged");
            }
            try {
                final CompoundTag root = decodeSnapshot(payload);
                final String mismatch = validateSnapshot(root, transferId, playerId,
                    sourceServerId, destinationServerId, sourcePartitionId,
                    sourcePartitionEpoch, destinationPartitionId,
                    destinationPartitionEpoch, playerSessionEpoch, playerStateVersion);
                if (mismatch != null) {
                    return CommandResult.rejected(mismatch);
                }
                final CompoundTag playerTag = root.getCompound("Player").orElseThrow();
                final CompoundTag transientState = root.getCompound("Transient").orElseThrow();
                preparation.player.load(TagValueInput.create(ProblemReporter.DISCARDING,
                    preparation.level.registryAccess(), playerTag));
                preparation.player.worldline$loadTransientState(transientState);
                final TagValueOutput staged = TagValueOutput.createWithContext(
                    ProblemReporter.DISCARDING, preparation.player.registryAccess());
                preparation.player.saveWithoutId(staged);
                final CompoundTag reproducedPlayerTag = staged.buildResult();
                final CompoundTag expectedState = comparablePlayerState(playerTag);
                final CompoundTag reproducedState = comparablePlayerState(reproducedPlayerTag);
                if (!expectedState.equals(reproducedState)) {
                    return CommandResult.rejected("prepared player cannot reproduce snapshot exactly; "
                        + "differing keys=" + differingKeys(expectedState, reproducedState));
                }
                if (!transientState.equals(preparation.player.worldline$saveTransientState())) {
                    return CommandResult.rejected("prepared player cannot reproduce transient state exactly");
                }
                preparation.attachment.stageSnapshot(playerStateVersion);
                preparation.playerStateVersion = playerStateVersion;
                preparation.stagedSnapshot = payload.clone();
                LOGGER.info("Worldline staged exact snapshot player={} transfer={} "
                        + "player_state_version={} snapshot_bytes={}", playerId, transferId,
                    playerStateVersion, payload.length);
                return CommandResult.accepted("snapshot staged");
            } catch (IOException | RuntimeException e) {
                return CommandResult.rejected("invalid snapshot: " + e.getMessage());
            }
        });
    }

    private static String unsupportedState(final ServerPlayer player) {
        if (player == null || !player.isAlive()) {
            return "active source player missing";
        }
        if (player.isPassenger() || player.isVehicle()) {
            return "vehicle or passenger state is unsupported";
        }
        if (player.containerMenu != player.inventoryMenu) {
            return "open container state is unsupported";
        }
        if (player.isSleeping()) {
            return "sleeping state is unsupported";
        }
        if (player.portalProcess != null) {
            return "active portal state is unsupported";
        }
        if (player.getCamera() != player) {
            return "remote camera state is unsupported";
        }
        if (player.connection == null || !player.connection.worldline$canFreeze()) {
            return "pending protocol synchronization state is unsupported";
        }
        return null;
    }

    private static byte[] encodeSnapshot(final ServerPlayer player, final long sourceTick,
                                         final UUID transferId, final String sourceServerId,
                                         final String destinationServerId,
                                         final String sourcePartitionId,
                                         final long sourcePartitionEpoch,
                                         final String destinationPartitionId,
                                         final long destinationPartitionEpoch,
                                         final long playerSessionEpoch,
                                         final long playerStateVersion) throws IOException {
        final TagValueOutput playerOutput = TagValueOutput.createWithContext(
            ProblemReporter.DISCARDING, player.registryAccess());
        player.saveWithoutId(playerOutput);
        final CompoundTag root = new CompoundTag();
        root.putInt("SnapshotSchemaVersion", SNAPSHOT_SCHEMA_VERSION);
        root.putLong("SourceTick", sourceTick);
        root.putLong("PlayerStateVersion", playerStateVersion);
        root.putLong("PlayerSessionEpoch", playerSessionEpoch);
        root.putString("TransferId", transferId.toString());
        root.putString("PlayerUuid", player.getUUID().toString());
        root.putString("SourceServerId", sourceServerId);
        root.putString("DestinationServerId", destinationServerId);
        root.putString("SourcePartitionId", sourcePartitionId);
        root.putLong("SourcePartitionEpoch", sourcePartitionEpoch);
        root.putString("DestinationPartitionId", destinationPartitionId);
        root.putLong("DestinationPartitionEpoch", destinationPartitionEpoch);
        root.put("Player", playerOutput.buildResult());
        root.put("Transient", player.worldline$saveTransientState());
        final byte[] bytes = writeNbt(root);
        if (bytes.length > MAX_PAYLOAD_BYTES) {
            throw new IOException("snapshot exceeds " + MAX_PAYLOAD_BYTES + " bytes");
        }
        return bytes;
    }

    private static String sha256(final byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static <T> void rememberTerminal(final Map<UUID, T> terminals,
                                             final UUID playerId, final T value) {
        terminals.put(playerId, value);
        if (terminals.size() > MAX_TERMINAL_IDENTITIES) {
            terminals.keySet().stream().filter(key -> !key.equals(playerId)).findAny()
                .ifPresent(terminals::remove);
        }
    }

    private static String snapshotStateHash(final byte[] snapshot) {
        try {
            final CompoundTag root = decodeSnapshot(snapshot);
            return stateHash(root.getCompound("Player").orElseThrow(),
                root.getCompound("Transient").orElseThrow());
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("cannot hash snapshot state", e);
        }
    }

    private static String playerStateHash(final ServerPlayer player) {
        final TagValueOutput output = TagValueOutput.createWithContext(
            ProblemReporter.DISCARDING, player.registryAccess());
        player.saveWithoutId(output);
        return stateHash(output.buildResult(), player.worldline$saveTransientState());
    }

    private static String stateHash(final CompoundTag player, final CompoundTag transientState) {
        final CompoundTag state = new CompoundTag();
        state.put("Player", comparablePlayerState(player));
        state.put("Transient", transientState.copy());
        try {
            return sha256(writeNbt(state));
        } catch (IOException e) {
            throw new IllegalStateException("cannot encode canonical player state", e);
        }
    }

    static CompoundTag decodeSnapshot(final byte[] payload) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            final CompoundTag root = NbtIo.read(input, NbtAccounter.create(MAX_PAYLOAD_BYTES * 4L));
            if (input.available() != 0) {
                throw new IOException("snapshot has trailing data");
            }
            return root;
        }
    }

    private static byte[] writeNbt(final CompoundTag root) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            NbtIo.write(root, output);
        }
        return bytes.toByteArray();
    }

    static String differingKeys(final CompoundTag expected, final CompoundTag actual) {
        final Set<String> keys = new TreeSet<>(expected.keySet());
        keys.addAll(actual.keySet());
        final StringBuilder differences = new StringBuilder();
        for (final String key : keys) {
            if (Objects.equals(expected.get(key), actual.get(key))) {
                continue;
            }
            if (!differences.isEmpty()) {
                differences.append(',');
            }
            differences.append(key);
        }
        return differences.toString();
    }

    static CompoundTag comparablePlayerState(final CompoundTag playerTag) {
        final CompoundTag state = playerTag.copy();
        state.remove("WorldUUIDLeast");
        state.remove("WorldUUIDMost");
        state.getCompound("bukkit").ifPresent(bukkit -> bukkit.remove("lastPlayed"));
        state.getCompound("Paper").ifPresent(paper -> {
            paper.remove("LastLogin");
            paper.remove("LastSeen");
        });
        return state;
    }

    private static String validateSnapshot(final CompoundTag root, final UUID transferId,
                                           final UUID playerId, final String sourceServerId,
                                           final String destinationServerId,
                                           final String sourcePartitionId,
                                           final long sourcePartitionEpoch,
                                           final String destinationPartitionId,
                                           final long destinationPartitionEpoch,
                                           final long playerSessionEpoch,
                                           final long playerStateVersion) {
        if (root.getIntOr("SnapshotSchemaVersion", -1) != SNAPSHOT_SCHEMA_VERSION
            || root.getLongOr("PlayerStateVersion", -1) != playerStateVersion
            || root.getLongOr("PlayerSessionEpoch", -1) != playerSessionEpoch
            || !root.getStringOr("TransferId", "").equals(transferId.toString())
            || !root.getStringOr("PlayerUuid", "").equals(playerId.toString())
            || !root.getStringOr("SourceServerId", "").equals(sourceServerId)
            || !root.getStringOr("DestinationServerId", "").equals(destinationServerId)
            || !root.getStringOr("SourcePartitionId", "").equals(sourcePartitionId)
            || root.getLongOr("SourcePartitionEpoch", -1) != sourcePartitionEpoch
            || !root.getStringOr("DestinationPartitionId", "").equals(destinationPartitionId)
            || root.getLongOr("DestinationPartitionEpoch", -1) != destinationPartitionEpoch
            || root.getLongOr("SourceTick", -1) < 0 || root.getCompound("Player").isEmpty()
            || root.getCompound("Transient").isEmpty()) {
            return "snapshot metadata fence rejected";
        }
        return null;
    }

    private static CommandResult onServerThread(final Callable<CommandResult> operation) {
        final MinecraftServer server = MinecraftServer.getServer();
        if (server == null || server.isStopped()) {
            return CommandResult.rejected("server is not active");
        }
        final CompletableFuture<CommandResult> result = new CompletableFuture<>();
        try {
            server.execute(() -> {
                try {
                    result.complete(operation.call());
                } catch (Exception e) {
                    result.complete(CommandResult.rejected("server operation failed: "
                        + e.getMessage()));
                }
            });
            return result.get(1_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CommandResult.rejected("server operation interrupted");
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            return CommandResult.rejected("server operation timed out");
        }
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
                                                    final long playerSessionEpoch,
                                                    final PreparationIdentity identity) {
        final MinecraftServer server = MinecraftServer.getServer();
        if (server.getPlayerList().getPlayer(playerId) != null) {
            return CommandResult.rejected("player is already active on destination");
        }
        final Preparation preparation = new Preparation(transferId, playerId,
            playerSessionEpoch, target, identity);
        final Preparation existing = PREPARATIONS.putIfAbsent(playerId, preparation);
        if (existing != null) {
            if (!existing.matches(transferId, playerSessionEpoch, target, identity)) {
                return CommandResult.rejected("another transfer is already prepared");
            }
            return awaitPreparation(existing);
        }
        RETIRED_DESTINATIONS.remove(playerId);
        ACTIVATED_DESTINATIONS.remove(playerId);
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
        preparation.attachment.bindPreparedPlayer(player);
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

    /** Releases the live-transfer identity when an activated destination disconnects normally. */
    public static void releaseActivatedDestination(final UUID playerId,
                                                   final ServerGamePacketListenerImpl listener) {
        final MinecraftServer server = MinecraftServer.getServer();
        if (server == null || !server.isSameThread()) {
            throw new IllegalStateException(
                "activated destination release requires the server thread");
        }
        final ActivatedDestination activated = ACTIVATED_DESTINATIONS.get(playerId);
        if (activated == null || activated.listener != listener
            || !ACTIVATED_DESTINATIONS.remove(playerId, activated)) {
            return;
        }
        rememberTerminal(RETIRED_DESTINATIONS, playerId,
            new DestinationTerminal(activated.transferId, activated.committedEpoch,
                activated.playerStateVersion, activated.committedIdentity));
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

    private record CommandResult(boolean accepted, String detail, byte[] payload) {
        private static CommandResult accepted(final String detail) {
            return accepted(detail, new byte[0]);
        }

        private static CommandResult accepted(final String detail, final byte[] payload) {
            return new CommandResult(true, detail, payload);
        }

        private static CommandResult rejected(final String detail) {
            return new CommandResult(false, detail, new byte[0]);
        }
    }

    public record AttachedDestination(ServerPlayer player,
                                      ServerGamePacketListenerImpl listener) {
    }

    private record PreparationIdentity(UUID clientConnectionId, String sourceServerId,
                                       String destinationServerId, String sourcePartitionId,
                                       long sourcePartitionEpoch, String destinationPartitionId,
                                       long destinationPartitionEpoch, long routeGeneration) {
        private PreparationIdentity {
            Objects.requireNonNull(clientConnectionId, "clientConnectionId");
            Objects.requireNonNull(sourceServerId, "sourceServerId");
            Objects.requireNonNull(destinationServerId, "destinationServerId");
            Objects.requireNonNull(sourcePartitionId, "sourcePartitionId");
            Objects.requireNonNull(destinationPartitionId, "destinationPartitionId");
        }

        private boolean matchesCommitted(final PreparationIdentity committed) {
            return this.clientConnectionId.equals(committed.clientConnectionId)
                && this.sourceServerId.equals(committed.sourceServerId)
                && this.destinationServerId.equals(committed.destinationServerId)
                && this.sourcePartitionId.equals(committed.sourcePartitionId)
                && this.sourcePartitionEpoch == committed.sourcePartitionEpoch
                && this.destinationPartitionId.equals(committed.destinationPartitionId)
                && this.destinationPartitionEpoch == committed.destinationPartitionEpoch
                && this.routeGeneration < Long.MAX_VALUE
                && committed.routeGeneration == this.routeGeneration + 1;
        }
    }

    private record FrozenPlayer(UUID transferId, long playerSessionEpoch,
                                long playerStateVersion, byte[] snapshot,
                                long committedEpoch, PreparationIdentity identity) {
        private boolean matches(final UUID transferId, final long playerSessionEpoch,
                                final long playerStateVersion,
                                final PreparationIdentity identity) {
            return this.transferId.equals(transferId)
                && this.playerSessionEpoch == playerSessionEpoch
                && this.playerStateVersion == playerStateVersion
                && this.identity.equals(identity);
        }

        private boolean matchesCommitted(final UUID transferId, final long playerSessionEpoch,
                                         final long playerStateVersion,
                                         final PreparationIdentity committedIdentity) {
            return this.transferId.equals(transferId)
                && this.playerSessionEpoch == playerSessionEpoch
                && this.playerStateVersion == playerStateVersion
                && this.identity.matchesCommitted(committedIdentity);
        }

        private FrozenPlayer withCommittedEpoch(final long committedEpoch) {
            return new FrozenPlayer(this.transferId, this.playerSessionEpoch,
                this.playerStateVersion, this.snapshot, committedEpoch, this.identity);
        }
    }

    private record DestinationTerminal(UUID transferId, long committedEpoch,
                                       long playerStateVersion,
                                       PreparationIdentity committedIdentity) {
        private boolean matches(final UUID transferId, final long committedEpoch,
                                final long playerStateVersion,
                                final PreparationIdentity committedIdentity) {
            return this.transferId.equals(transferId)
                && this.committedEpoch == committedEpoch
                && this.playerStateVersion == playerStateVersion
                && this.committedIdentity.equals(committedIdentity);
        }
    }

    private record ActivatedDestination(UUID transferId, long committedEpoch,
                                        long playerStateVersion,
                                        PreparationIdentity committedIdentity,
                                        ServerPlayer player,
                                        ServerGamePacketListenerImpl listener,
                                        Connection connection) {
        private boolean matches(final UUID transferId, final long committedEpoch,
                                final long playerStateVersion,
                                final PreparationIdentity committedIdentity) {
            return this.transferId.equals(transferId)
                && this.committedEpoch == committedEpoch
                && this.playerStateVersion == playerStateVersion
                && this.committedIdentity.equals(committedIdentity);
        }
    }

    private static final class Preparation {
        private final UUID transferId;
        private final UUID playerId;
        private final long playerSessionEpoch;
        private final PrepareTarget target;
        private final PreparationIdentity identity;
        private final WorldlineDestinationAttachment<ServerPlayer> attachment;
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private volatile boolean cancelled;
        private ServerLevel level;
        private ChunkPos center;
        private TicketType<?> ticketType;
        private int ticketLevel;
        private boolean ticketAdded;
        private ServerPlayer player;
        private byte[] stagedSnapshot;
        private long playerStateVersion;
        private Connection attachedConnection;
        private ServerGamePacketListenerImpl attachedListener;
        private CommonListenerCookie attachedCookie;

        private Preparation(final UUID transferId, final UUID playerId,
                            final long playerSessionEpoch, final PrepareTarget target,
                            final PreparationIdentity identity) {
            this.transferId = transferId;
            this.playerId = playerId;
            this.playerSessionEpoch = playerSessionEpoch;
            this.target = target;
            this.identity = identity;
            this.attachment = new WorldlineDestinationAttachment<>(transferId, playerId,
                identity.clientConnectionId, identity.sourceServerId, identity.destinationServerId,
                identity.sourcePartitionId, identity.sourcePartitionEpoch,
                identity.destinationPartitionId, identity.destinationPartitionEpoch,
                playerSessionEpoch, identity.routeGeneration);
        }

        private boolean matches(final UUID transferId, final long playerSessionEpoch,
                                final PrepareTarget target,
                                final PreparationIdentity identity) {
            return this.transferId.equals(transferId)
                && this.playerSessionEpoch == playerSessionEpoch && this.target.equals(target)
                && this.identity.equals(identity);
        }
    }
}
