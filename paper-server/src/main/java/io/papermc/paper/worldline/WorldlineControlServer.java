package io.papermc.paper.worldline;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Slice-only TCP endpoint for the M2 handoff control plane. */
public final class WorldlineControlServer {
    private static final Logger LOGGER = LoggerFactory.getLogger("WorldlineControl");
    private static final int MAGIC = 0x574c4d32;
    private static final int PROTOCOL_VERSION = 1;
    private static final Set<String> SOURCE_COMMANDS = Set.of("FREEZE_SOURCE", "CLEAN_SOURCE");
    private static final Set<String> DESTINATION_COMMANDS = Set.of(
        "PREPARE", "ABORT", "STAGE_SNAPSHOT", "COMMIT", "ACTIVATE_DESTINATION"
    );
    private static final AtomicBoolean STARTED = new AtomicBoolean();

    private WorldlineControlServer() {
    }

    public static void start() {
        final String serverId = System.getProperty("worldline.server-id", "");
        final String partitionId = System.getProperty("worldline.partition-id", "");
        final int port = Integer.getInteger("worldline.control-port", 0);
        final long partitionEpoch = Long.getLong("worldline.partition-epoch", 0L);
        if (serverId.isBlank() || partitionId.isBlank() || port == 0) {
            return;
        }
        if (port < 1 || port > 65535 || partitionEpoch < 1) {
            throw new IllegalArgumentException("Invalid Worldline control endpoint configuration");
        }
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        final Thread thread = new Thread(
            () -> serve(serverId, partitionId, partitionEpoch, port),
            "Worldline control " + serverId
        );
        thread.setDaemon(true);
        thread.start();
    }

    private static void serve(final String serverId, final String partitionId,
                              final long partitionEpoch, final int port) {
        try (ServerSocket listener = new ServerSocket()) {
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            LOGGER.info("Worldline control listening on 127.0.0.1:{} as owner of {} epoch {}",
                port, partitionId, partitionEpoch);
            while (!Thread.currentThread().isInterrupted()) {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(2_000);
                    handle(socket, serverId, partitionId, partitionEpoch);
                } catch (EOFException ignored) {
                } catch (IOException e) {
                    LOGGER.warn("Worldline control request failed: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            LOGGER.error("Worldline control endpoint failed", e);
        }
    }

    private static void handle(final Socket socket, final String serverId,
                               final String partitionId, final long partitionEpoch) throws IOException {
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

        final boolean sourceCommand = SOURCE_COMMANDS.contains(command);
        final boolean knownCommand = sourceCommand || DESTINATION_COMMANDS.contains(command);
        final String expectedServer = sourceCommand ? sourceServerId : destinationServerId;
        final String expectedPartition = sourceCommand ? sourcePartitionId : destinationPartitionId;
        final long expectedEpoch = sourceCommand ? sourcePartitionEpoch : destinationPartitionEpoch;
        final boolean accepted = protocolVersion == PROTOCOL_VERSION && knownCommand
            && serverId.equals(expectedServer) && partitionId.equals(expectedPartition)
            && partitionEpoch == expectedEpoch;
        final String detail = accepted ? "accepted" : "identity or ownership fence rejected";

        final DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(MAGIC);
        output.writeInt(PROTOCOL_VERSION);
        output.writeBoolean(accepted);
        output.writeUTF(detail);
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
        LOGGER.info("Worldline control command={} transfer={} accepted={}",
            command, transferId, accepted);
    }

    private static UUID readUuid(final DataInputStream input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    private static void writeUuid(final DataOutputStream output, final UUID value)
        throws IOException {
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }
}
