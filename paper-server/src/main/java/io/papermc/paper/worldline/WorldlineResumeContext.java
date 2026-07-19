package io.papermc.paper.worldline;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Transfer-scoped identity and authority fences carried only on an M5 destination handshake.
 */
public record WorldlineResumeContext(
        int protocolVersion,
        UUID transferId,
        UUID playerId,
        UUID clientConnectionId,
        String sourceServerId,
        String destinationServerId,
        String sourcePartitionId,
        long sourcePartitionEpoch,
        String destinationPartitionId,
        long destinationPartitionEpoch,
        long sourcePlayerEpoch,
        long committedPlayerEpoch,
        long playerStateVersion,
        long routeGeneration,
        int priorEntityId) {

    public static final int PROTOCOL_VERSION = 4;
    public static final String MARKER_PREFIX = ".worldline-resume-v4|";
    public static final int MAX_MARKER_LENGTH = 240;
    private static final int MAX_ID_BYTES = 64;
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public WorldlineResumeContext {
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported Worldline resume protocol");
        }
        Objects.requireNonNull(transferId, "transferId");
        Objects.requireNonNull(playerId, "playerId");
        Objects.requireNonNull(clientConnectionId, "clientConnectionId");
        validateId(sourceServerId, "sourceServerId");
        validateId(destinationServerId, "destinationServerId");
        validateId(sourcePartitionId, "sourcePartitionId");
        validateId(destinationPartitionId, "destinationPartitionId");
        if (sourcePartitionEpoch < 1 || destinationPartitionEpoch < 1
                || sourcePlayerEpoch < 0 || sourcePlayerEpoch == Long.MAX_VALUE
                || committedPlayerEpoch != sourcePlayerEpoch + 1
                || playerStateVersion < 1 || routeGeneration < 1 || priorEntityId < 0) {
            throw new IllegalArgumentException("invalid Worldline resume epoch or generation");
        }
    }

    /** Encodes a deterministic marker suitable for the Minecraft handshake hostname. */
    public String encodeMarker() {
        String marker = MARKER_PREFIX + transferId + "|" + playerId + "|" + clientConnectionId
                + "|" + encodeId(sourceServerId) + "|" + encodeId(destinationServerId)
                + "|" + encodeId(sourcePartitionId) + "|" + sourcePartitionEpoch
                + "|" + encodeId(destinationPartitionId) + "|" + destinationPartitionEpoch
                + "|" + sourcePlayerEpoch + "|" + committedPlayerEpoch + "|" + playerStateVersion
                + "|" + routeGeneration + "|" + priorEntityId;
        if (marker.length() > MAX_MARKER_LENGTH) {
            throw new IllegalArgumentException("Worldline resume marker is too long");
        }
        return marker;
    }

    /**
     * Extracts the marker from Paper's handshake hostname form. An absent marker is not an error;
     * a present malformed marker is.
     */
    public static Optional<WorldlineResumeContext> parseHostname(final String hostname) {
        Objects.requireNonNull(hostname, "hostname");
        int start = hostname.indexOf(MARKER_PREFIX);
        if (start < 0) {
            return Optional.empty();
        }
        if (start != hostname.lastIndexOf(MARKER_PREFIX)) {
            throw new IllegalArgumentException("duplicate Worldline resume marker");
        }
        int end = hostname.indexOf(':', start + MARKER_PREFIX.length());
        if (end < 0) {
            end = hostname.length();
        } else {
            String port = hostname.substring(end + 1);
            if (port.isEmpty() || port.codePoints().anyMatch(value -> !Character.isDigit(value))) {
                throw new IllegalArgumentException("invalid Worldline resume hostname suffix");
            }
        }
        return Optional.of(parseMarker(hostname.substring(start, end)));
    }

    /** Parses a marker and rejects non-canonical, oversized, missing, or trailing fields. */
    public static WorldlineResumeContext parseMarker(final String marker) {
        if (marker == null || marker.length() > MAX_MARKER_LENGTH
                || !marker.startsWith(MARKER_PREFIX)) {
            throw new IllegalArgumentException("invalid Worldline resume marker");
        }
        String[] fields = marker.split("\\|", -1);
        if (fields.length != 15 || !fields[0].equals(".worldline-resume-v4")) {
            throw new IllegalArgumentException("invalid Worldline resume field count");
        }
        try {
            return new WorldlineResumeContext(PROTOCOL_VERSION,
                    parseUuid(fields[1]), parseUuid(fields[2]), parseUuid(fields[3]),
                    decodeId(fields[4]), decodeId(fields[5]), decodeId(fields[6]),
                    Long.parseLong(fields[7]), decodeId(fields[8]), Long.parseLong(fields[9]),
                    Long.parseLong(fields[10]), Long.parseLong(fields[11]), Long.parseLong(fields[12]),
                    Long.parseLong(fields[13]), Integer.parseInt(fields[14]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid Worldline resume number", e);
        }
    }

    private static UUID parseUuid(final String value) {
        if (value.length() != 36) {
            throw new IllegalArgumentException("invalid Worldline resume UUID");
        }
        try {
            UUID parsed = UUID.fromString(value);
            if (!parsed.toString().equals(value.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("non-canonical Worldline resume UUID");
            }
            return parsed;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid Worldline resume UUID", e);
        }
    }

    private static String encodeId(final String value) {
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeId(final String value) {
        try {
            byte[] bytes = DECODER.decode(value);
            if (bytes.length > MAX_ID_BYTES) {
                throw new IllegalArgumentException("Worldline resume identifier is too long");
            }
            if (!ENCODER.encodeToString(bytes).equals(value)) {
                throw new IllegalArgumentException("non-canonical Worldline resume identifier");
            }
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (IllegalArgumentException | CharacterCodingException e) {
            throw new IllegalArgumentException("invalid Worldline resume identifier", e);
        }
    }

    private static void validateId(final String value, final String name) {
        Objects.requireNonNull(value, name);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || bytes.length > MAX_ID_BYTES
                || value.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("invalid " + name);
        }
    }
}
