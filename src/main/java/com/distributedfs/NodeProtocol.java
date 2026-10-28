package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * What the storage node server ({@link NodeApiHandler}) and its client ({@link RemoteStorageNode})
 * agree on: paths, header names, error codes and the body format.
 *
 * <p><b>Bodies</b> are {@code text/plain; charset=utf-8}, one {@code key=value} line per field,
 * each line ending in {@code \n}. Keys are lowercase letters and {@code -}; values are printable
 * ASCII without spaces. A listing has one line per entry instead: {@code <kind> <name>}, the name
 * percent-encoded as UTF-8 so that any file name fits on one line. Error responses are the single
 * line {@code error=<code>}. Nothing a body contains is ever a file path or exception text.
 *
 * <p>A plain line format is used rather than JSON because both sides must parse it, and the
 * project's {@link Json} class only writes.
 */
final class NodeProtocol {

    private NodeProtocol() {
    }

    static final String HEALTH_PATH = "/health";
    static final String ENTRIES_PATH = "/entries";
    static final String SWEEP_PATH = "/sweep";
    static final String CHUNKS_PREFIX = "/chunks/";
    static final String EXISTS_SUFFIX = "/exists";

    /** On every response: the node's ID, from its marker file. */
    static final String NODE_ID_HEADER = "X-Node-Id";
    /** On every store request: the chunk's SHA-256, 64 lowercase hexadecimal characters. */
    static final String CHUNK_SHA256_HEADER = "X-Chunk-Sha256";

    static final String CONTENT_TYPE = "text/plain; charset=utf-8";
    static final int FORMAT = 1;

    /** A node ID: 32 lowercase hexadecimal characters, chosen at random when the node folder is created. */
    static final Pattern NODE_ID = Pattern.compile("[0-9a-f]{32}");

    private static final Pattern KEY = Pattern.compile("[a-z][a-z-]{0,31}");
    private static final Pattern VALUE = Pattern.compile("[!-~]{0,128}");

    /** Every error a node sends, with its HTTP status. The code is the body: {@code error=<code>}. */
    enum Error {
        BAD_REQUEST(400, "bad_request"),
        INVALID_CHUNK_ID(400, "invalid_chunk_id"),
        HOST_NOT_ALLOWED(403, "host_not_allowed"),
        CROSS_ORIGIN_REFUSED(403, "cross_origin_refused"),
        NOT_FOUND(404, "not_found"),
        CHUNK_NOT_FOUND(404, "chunk_not_found"),
        METHOD_NOT_ALLOWED(405, "method_not_allowed"),
        CHUNK_EXISTS(409, "chunk_exists"),
        TOO_LARGE(413, "too_large"),
        CHECKSUM_MISMATCH(422, "checksum_mismatch"),
        UNSAFE_ENTRY(500, "unsafe_entry"),
        STORE_FAILED(500, "store_failed"),
        DELETE_FAILED(500, "delete_failed"),
        INTERNAL_ERROR(500, "internal_error"),
        STORAGE_UNAVAILABLE(503, "storage_unavailable"),
        NODE_BUSY(503, "node_busy");

        private final int status;
        private final String code;

        Error(int status, String code) {
            this.status = status;
            this.code = code;
        }

        int status() {
            return status;
        }

        String code() {
            return code;
        }

        String body() {
            return "error=" + code + "\n";
        }
    }

    /** {@code key=value} lines, in the given order: {@code fields("exists", "true")}. */
    static String fields(String... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("fields needs pairs of keys and values");
        }
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            if (!KEY.matcher(keysAndValues[i]).matches() || !VALUE.matcher(keysAndValues[i + 1]).matches()) {
                throw new IllegalArgumentException("not a valid field: " + keysAndValues[i]);
            }
            body.append(keysAndValues[i]).append('=').append(keysAndValues[i + 1]).append('\n');
        }
        return body.toString();
    }

    /**
     * Parses a body written by {@link #fields}. Returns null, never a partial result, if the body
     * is anything else: a line without {@code =}, a bad key or value, a repeated key, or a missing
     * final newline.
     */
    static Map<String, String> parseFields(String body) {
        if (body == null || (!body.isEmpty() && !body.endsWith("\n"))) {
            return null;
        }
        Map<String, String> fields = new LinkedHashMap<>();
        if (body.isEmpty()) {
            return fields;
        }
        for (String line : body.substring(0, body.length() - 1).split("\n", -1)) {
            int equals = line.indexOf('=');
            if (equals < 0) {
                return null;
            }
            String key = line.substring(0, equals);
            String value = line.substring(equals + 1);
            if (!KEY.matcher(key).matches() || !VALUE.matcher(value).matches() || fields.put(key, value) != null) {
                return null;
            }
        }
        return fields;
    }

    /** The wire name of an entry kind. */
    static String kindName(ChunkStore.EntryKind kind) {
        return switch (kind) {
            case FILE -> "file";
            case DIRECTORY -> "directory";
            case LINK -> "link";
            case OTHER -> "other";
            case UNKNOWN -> "unknown";
        };
    }

    /** The entry kind for a wire name, or null. */
    static ChunkStore.EntryKind kindFromName(String name) {
        return switch (name) {
            case "file" -> ChunkStore.EntryKind.FILE;
            case "directory" -> ChunkStore.EntryKind.DIRECTORY;
            case "link" -> ChunkStore.EntryKind.LINK;
            case "other" -> ChunkStore.EntryKind.OTHER;
            case "unknown" -> ChunkStore.EntryKind.UNKNOWN;
            default -> null;
        };
    }

    /** Percent-encodes every UTF-8 byte except {@code A-Z a-z 0-9 _ - .}. */
    static String encodeName(String name) {
        StringBuilder encoded = new StringBuilder();
        for (byte b : name.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            boolean plain = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.';
            if (plain) {
                encoded.append((char) c);
            } else {
                encoded.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
                        .append(Character.toUpperCase(Character.forDigit(c & 0xF, 16)));
            }
        }
        return encoded.toString();
    }

    /** Reverses {@link #encodeName}; returns null if {@code encoded} is not exactly such a name. */
    static String decodeName(String encoded) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);
            if (c == '%') {
                if (i + 2 >= encoded.length()) {
                    return null;
                }
                int high = Character.digit(encoded.charAt(i + 1), 16);
                int low = Character.digit(encoded.charAt(i + 2), 16);
                if (high < 0 || low < 0) {
                    return null;
                }
                bytes.write(high * 16 + low);
                i += 2;
            } else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.') {
                bytes.write(c);
            } else {
                return null;
            }
        }
        String name = bytes.toString(StandardCharsets.UTF_8);
        return name.isEmpty() || !encodeName(name).equals(encoded) ? null : name;
    }
}
