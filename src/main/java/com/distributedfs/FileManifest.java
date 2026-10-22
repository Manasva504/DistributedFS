package com.distributedfs;

import java.util.Objects;

/**
 * What the server records about one stored file: its ID, size, SHA-256, and how it was split
 * into chunks.
 *
 * <p><b>Text format.</b> A manifest is stored as six lines of plain ASCII, each ending in a line
 * feed, always in this order:
 * <pre>
 *   format=1
 *   fileId=0123456789abcdef0123456789abcdef
 *   size=3145729
 *   sha256=&lt;64 lowercase hexadecimal characters&gt;
 *   chunkSize=1048576
 *   chunkCount=4
 * </pre>
 * Reading is strict: exactly these keys, once each, in this order, written exactly as
 * {@link #toText()} writes them (no spaces, no leading zeros, no "+" signs, no CR). Anything else
 * is reported as a damaged manifest, never quietly accepted or repaired.
 *
 * <p><b>Chunks.</b> Chunk {@code i} has the ID {@link FileIds#chunkId FileIds.chunkId(fileId, i)}.
 * The chunk IDs are derived rather than stored, so a damaged manifest can never point at the
 * chunks of another file. {@code chunkCount} must equal {@code ceil(size / chunkSize)}; an empty
 * file has no chunks.
 *
 * @param fileId     a valid file ID (see {@link FileIds})
 * @param size       the file's size in bytes, 0 or more
 * @param sha256     the SHA-256 of the whole file, 64 lowercase hexadecimal characters
 * @param chunkSize  the size of every chunk except the last, at least 1
 * @param chunkCount the number of chunks: {@code ceil(size / chunkSize)}
 */
record FileManifest(String fileId, long size, String sha256, int chunkSize, int chunkCount) {

    /** The only format version written and accepted so far. */
    static final int FORMAT_VERSION = 1;

    /** Manifests are a few hundred bytes; anything larger than this is rejected without parsing. */
    static final int MAX_TEXT_BYTES = 4 * 1024;

    private static final String[] KEYS = {"format", "fileId", "size", "sha256", "chunkSize", "chunkCount"};

    /**
     * @throws NullPointerException if {@code fileId} or {@code sha256} is null
     * @throws IllegalArgumentException if any value breaks the rules above
     */
    FileManifest {
        Objects.requireNonNull(fileId, "fileId must not be null");
        Objects.requireNonNull(sha256, "sha256 must not be null");
        if (!FileIds.isValid(fileId)) {
            throw new IllegalArgumentException("fileId is not a valid file ID");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative, but was " + size);
        }
        if (!isLowercaseSha256(sha256)) {
            throw new IllegalArgumentException("sha256 must be 64 lowercase hexadecimal characters");
        }
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be at least 1, but was " + chunkSize);
        }
        int expected = expectedChunkCount(size, chunkSize);
        if (chunkCount != expected) {
            throw new IllegalArgumentException("chunkCount must be " + expected + " for size " + size
                    + " and chunkSize " + chunkSize + ", but was " + chunkCount);
        }
    }

    /**
     * The number of chunks a file of {@code size} bytes is split into: {@code ceil(size / chunkSize)},
     * computed without overflow.
     *
     * @throws IllegalArgumentException if {@code size} is negative, {@code chunkSize} is less than 1,
     *         or the result does not fit in an int
     */
    static int expectedChunkCount(long size, int chunkSize) {
        if (size < 0 || chunkSize < 1) {
            throw new IllegalArgumentException("size must be 0 or more and chunkSize at least 1");
        }
        long count = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        if (count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("a file of " + size + " bytes would need more than "
                    + Integer.MAX_VALUE + " chunks of " + chunkSize + " bytes");
        }
        return (int) count;
    }

    /**
     * The chunk ID of chunk {@code index}.
     *
     * @throws IndexOutOfBoundsException if {@code index} is not between 0 and chunkCount - 1
     */
    String chunkId(int index) {
        Objects.checkIndex(index, chunkCount);
        return FileIds.chunkId(fileId, index);
    }

    /** The manifest as stored on disk: six lines, each ending in a line feed. */
    String toText() {
        return "format=" + FORMAT_VERSION + "\n"
                + "fileId=" + fileId + "\n"
                + "size=" + size + "\n"
                + "sha256=" + sha256 + "\n"
                + "chunkSize=" + chunkSize + "\n"
                + "chunkCount=" + chunkCount + "\n";
    }

    /**
     * Reads a manifest written by {@link #toText()}.
     *
     * @param expectedFileId the ID this manifest must describe (the ID in its file name)
     * @throws InvalidManifestException if the text is not exactly a valid manifest for
     *         {@code expectedFileId}
     */
    static FileManifest parse(String text, String expectedFileId) throws InvalidManifestException {
        String[] values = readFields(text, KEYS);
        if (!values[0].equals(String.valueOf(FORMAT_VERSION))) {
            throw new InvalidManifestException("unsupported manifest format \"" + values[0] + "\"");
        }
        if (!values[1].equals(expectedFileId)) {
            throw new InvalidManifestException("the manifest describes a different file ID than its name says");
        }
        FileManifest manifest;
        try {
            manifest = new FileManifest(values[1], parseNumber(values[2], "size"), values[3],
                    toInt(parseNumber(values[4], "chunkSize"), "chunkSize"),
                    toInt(parseNumber(values[5], "chunkCount"), "chunkCount"));
        } catch (IllegalArgumentException e) {
            throw new InvalidManifestException("invalid manifest values: " + e.getMessage(), e);
        }
        requireCanonical(text, manifest.toText());
        return manifest;
    }

    // ---------------------------------------------------------------------
    // Strict "key=value" text, shared with ManifestStore's pending-upload markers
    // ---------------------------------------------------------------------

    /**
     * Splits {@code text} into exactly {@code keys.length} lines of the form {@code key=value},
     * with the keys in exactly the given order, and returns the values. Every line, including the
     * last, must end with a line feed. Only printable ASCII is allowed inside a line.
     */
    static String[] readFields(String text, String... keys) throws InvalidManifestException {
        Objects.requireNonNull(text, "text must not be null");
        if (!text.endsWith("\n")) {
            throw new InvalidManifestException("the text does not end with a line feed");
        }
        String[] lines = text.substring(0, text.length() - 1).split("\n", -1);
        if (lines.length != keys.length) {
            throw new InvalidManifestException("expected " + keys.length + " lines but found " + lines.length);
        }
        String[] values = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            String line = lines[i];
            for (int j = 0; j < line.length(); j++) {
                char c = line.charAt(j);
                if (c < 0x20 || c > 0x7E) {
                    throw new InvalidManifestException("line " + (i + 1) + " contains a character that is not printable ASCII");
                }
            }
            String prefix = keys[i] + "=";
            if (!line.startsWith(prefix)) {
                throw new InvalidManifestException("line " + (i + 1) + " must start with \"" + prefix + "\"");
            }
            values[i] = line.substring(prefix.length());
        }
        return values;
    }

    /**
     * Parses a non-negative decimal number of 1 to 19 digits. Signs, spaces and every other
     * character that is not a digit 0-9 are rejected here, as is a value too large for a long.
     * Leading zeros pass this check; the canonical-text check in {@link #parse} rejects them.
     */
    static long parseNumber(String value, String name) throws InvalidManifestException {
        if (value.isEmpty() || value.length() > 19) {
            throw new InvalidManifestException(name + " must be 1 to 19 decimal digits");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                throw new InvalidManifestException(name + " must contain only the digits 0-9");
            }
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new InvalidManifestException(name + " is too large", e);
        }
    }

    static int toInt(long value, String name) throws InvalidManifestException {
        if (value > Integer.MAX_VALUE) {
            throw new InvalidManifestException(name + " is too large");
        }
        return (int) value;
    }

    /**
     * The final check: the text must be exactly what this class would write for the values it
     * contains. This rejects every remaining variation in one place, such as leading zeros.
     */
    static void requireCanonical(String text, String canonical) throws InvalidManifestException {
        if (!text.equals(canonical)) {
            throw new InvalidManifestException("the text is not in canonical form (for example a number has leading zeros)");
        }
    }

    private static boolean isLowercaseSha256(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }
}
