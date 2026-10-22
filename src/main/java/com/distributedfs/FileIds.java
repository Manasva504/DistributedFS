package com.distributedfs;

import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.Objects;

/**
 * File IDs: how the server creates them, how it checks one that arrives in a request, and how the
 * IDs of a file's chunks are derived from it.
 *
 * <p><b>Format.</b> A file ID is exactly 32 characters, each {@code 0-9} or {@code a-f}: 128
 * random bits from SecureRandom, written as lowercase hexadecimal. The server creates every ID;
 * clients cannot choose one.
 *
 * <p><b>Why the check is strict.</b> An ID that arrives in a request is untrusted, so it is
 * checked against these rules before it is used for anything. The rules leave no room for path
 * tricks: dots, slashes, backslashes, colons, percent signs, uppercase letters and non-ASCII
 * look-alikes all fail. The HTTP layer must check the raw path segment, before any percent
 * decoding, so that an encoded sequence such as {@code %2e%2e} is rejected here rather than
 * decoded first. Each ID also has exactly one valid spelling, so two different strings can never
 * name the same file (Windows ignores letter case in file names).
 *
 * <p><b>Chunk IDs.</b> Chunk {@code i} of file {@code F} is stored under the chunk ID
 * {@code F_i}. Chunk IDs are global within a StorageCluster, so deriving them from the unique
 * file ID keeps the chunks of different files apart. A derived ID is at most 43 characters
 * (32 + "_" + up to 10 digits) and always meets StorageNode's chunk-ID rules.
 */
final class FileIds {

    /** Number of characters in a file ID: 16 random bytes, two hex digits each. */
    static final int LENGTH = 32;

    private static final int RANDOM_BYTES = LENGTH / 2;

    /** SecureRandom is safe to share between threads, so concurrent requests can all use this one. */
    private static final SecureRandom RANDOM = new SecureRandom();

    private FileIds() {
    }

    /** Returns a new random file ID. HexFormat writes lowercase digits by default. */
    static String newFileId() {
        byte[] bytes = new byte[RANDOM_BYTES];
        RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    /**
     * Reports whether {@code fileId} is a well-formed file ID: exactly 32 characters, each
     * {@code 0-9} or {@code a-f}. Returns {@code false} for null. It says nothing about whether
     * a file with this ID is stored.
     */
    static boolean isValid(String fileId) {
        if (fileId == null || fileId.length() != LENGTH) {
            return false;
        }
        for (int i = 0; i < fileId.length(); i++) {
            char c = fileId.charAt(i);
            // Explicit ASCII ranges on purpose, as in StorageNode: Character.isDigit would also
            // accept digits from other scripts, such as the fullwidth '０'.
            boolean allowed = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
            if (!allowed) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the chunk ID of chunk {@code index} (counting from 0) of the file {@code fileId}.
     *
     * @throws NullPointerException if {@code fileId} is null
     * @throws IllegalArgumentException if {@code fileId} is not a valid file ID, or {@code index}
     *         is negative
     */
    static String chunkId(String fileId, int index) {
        Objects.requireNonNull(fileId, "fileId must not be null");
        if (!isValid(fileId)) {
            // The rejected value is not repeated here: it may be untrusted input.
            throw new IllegalArgumentException("fileId is not a valid file ID");
        }
        if (index < 0) {
            throw new IllegalArgumentException("Chunk index must not be negative, but was " + index);
        }
        return fileId + "_" + index;
    }
}
