package com.distributedfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * What the server records about one stored file: its ID, size, SHA-256, how it was split into
 * chunks, and (for files stored since format 2) the SHA-256 of every chunk.
 *
 * <p><b>Text format.</b> A manifest is plain ASCII, one {@code key=value} per line, each line
 * ending in a line feed, always in this order:
 * <pre>
 *   format=2
 *   fileId=0123456789abcdef0123456789abcdef
 *   size=3145729
 *   sha256=&lt;64 lowercase hexadecimal characters: the whole file&gt;
 *   chunkSize=1048576
 *   chunkCount=4
 *   chunk0.sha256=&lt;64 lowercase hexadecimal characters: chunk 0&gt;
 *   chunk1.sha256=...
 *   chunk2.sha256=...
 *   chunk3.sha256=...
 * </pre>
 * An upload without a file name writes format 2. <b>Format 3</b>, written for an upload that gave
 * its original name, is format 2 with {@code format=3} and one more line right after
 * {@code chunkCount}: {@code name=<the name as canonical percent-encoded UTF-8>} (see
 * {@link FileNames#toManifest}), so the text stays printable ASCII. A format 1 or 2 manifest has
 * no name ({@link #name()} is null), and a {@code name} line in one is invalid, as is a format 3
 * manifest without one. <b>Format 1</b>, written before per-chunk checksums existed, is
 * the same first six lines with {@code format=1} and no chunk lines; it is still read, and such a
 * manifest simply has no chunk checksums ({@link #chunkSha256s()} is null). It is never treated as
 * if its chunks had been verified individually.
 *
 * <p>Reading is strict: exactly these keys, once each, in this order, written exactly as
 * {@link #toText()} writes them (no spaces, no leading zeros, no "+" signs, no CR). Anything else
 * is reported as a damaged manifest, never quietly accepted or repaired.
 *
 * <p><b>Chunks.</b> Chunk {@code i} has the ID {@link FileIds#chunkId FileIds.chunkId(fileId, i)}.
 * The chunk IDs are derived rather than stored, so a damaged manifest can never point at the
 * chunks of another file. {@code chunkCount} must equal {@code ceil(size / chunkSize)}; an empty
 * file has no chunks. A format 2 manifest has exactly one checksum per chunk, and at most
 * {@link #MAX_CHECKSUMMED_CHUNKS} chunks, which bounds its size.
 *
 * @param fileId       a valid file ID (see {@link FileIds})
 * @param size         the file's size in bytes, 0 or more
 * @param sha256       the SHA-256 of the whole file, 64 lowercase hexadecimal characters
 * @param chunkSize    the size of every chunk except the last, at least 1
 * @param chunkCount   the number of chunks: {@code ceil(size / chunkSize)}
 * @param chunkSha256s the SHA-256 of each chunk in order (64 lowercase hexadecimal characters
 *                     each, exactly {@code chunkCount} of them), or null for a format 1 manifest
 * @param name         the file's original name ({@link FileNames#isValid}), or null if the upload
 *                     gave none; a manifest with a name is format 3 and must have chunk checksums
 */
record FileManifest(String fileId, long size, String sha256, int chunkSize, int chunkCount, List<String> chunkSha256s,
                    String name) {

    /**
     * Format 1: no chunk checksums. Also the format of pending-upload markers, which never
     * changed (see ManifestStore.PendingUpload).
     */
    static final int FORMAT_VERSION = 1;

    /** Format 2: one SHA-256 per chunk. Every new upload without a file name is written in this format. */
    static final int CHUNK_CHECKSUM_FORMAT = 2;

    /** Format 3: format 2 plus the file's original name. Every new upload with a name is written in this format. */
    static final int NAME_FORMAT = 3;

    /**
     * The most chunks a format 2 manifest may list. With the default 1 MiB chunks this is 64 GiB,
     * far above the largest upload the server accepts (1024 MiB); only very small configured chunk
     * sizes can reach it, and FileService refuses such an upload before storing anything.
     */
    static final int MAX_CHECKSUMMED_CHUNKS = 65_536;

    /**
     * The largest manifest text accepted; anything larger is rejected without parsing. A format 2
     * manifest with the maximum number of chunks is about 5.4 MB (83 bytes per chunk line).
     */
    static final int MAX_TEXT_BYTES = 6 * 1024 * 1024;

    private static final String[] KEYS = {"format", "fileId", "size", "sha256", "chunkSize", "chunkCount"};

    /** The key of format 3's name line, right after {@code chunkCount}. */
    private static final String NAME_KEY = "name";

    /**
     * @throws NullPointerException if {@code fileId}, {@code sha256} or a chunk checksum is null
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
        if (chunkSha256s != null) {
            if (chunkCount > MAX_CHECKSUMMED_CHUNKS) {
                throw new IllegalArgumentException("a manifest with chunk checksums may list at most "
                        + MAX_CHECKSUMMED_CHUNKS + " chunks, but this one has " + chunkCount);
            }
            if (chunkSha256s.size() != chunkCount) {
                throw new IllegalArgumentException("there must be one chunk checksum per chunk: " + chunkCount
                        + " chunks but " + chunkSha256s.size() + " checksums");
            }
            for (int i = 0; i < chunkSha256s.size(); i++) {
                String chunkSha256 = Objects.requireNonNull(chunkSha256s.get(i), "chunk checksum " + i + " must not be null");
                if (!isLowercaseSha256(chunkSha256)) {
                    throw new IllegalArgumentException("the checksum of chunk " + i
                            + " must be 64 lowercase hexadecimal characters");
                }
            }
            chunkSha256s = List.copyOf(chunkSha256s);
        }
        if (name != null) {
            if (chunkSha256s == null) {
                throw new IllegalArgumentException("a manifest with a file name must also have chunk checksums");
            }
            if (!FileNames.isValid(name)) {
                throw new IllegalArgumentException("name is not a valid file name");
            }
        }
    }

    /** A format 2 manifest (or format 1, with null checksums): no file name. */
    FileManifest(String fileId, long size, String sha256, int chunkSize, int chunkCount, List<String> chunkSha256s) {
        this(fileId, size, sha256, chunkSize, chunkCount, chunkSha256s, null);
    }

    /** A format 1 manifest: no chunk checksums. */
    FileManifest(String fileId, long size, String sha256, int chunkSize, int chunkCount) {
        this(fileId, size, sha256, chunkSize, chunkCount, null, null);
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

    /** True for a format 2 or 3 manifest, which records the SHA-256 of every chunk. */
    boolean hasChunkChecksums() {
        return chunkSha256s != null;
    }

    /** The manifest's format number: 3 with a file name, 2 with chunk checksums only, 1 with neither. */
    int format() {
        return name != null ? NAME_FORMAT : hasChunkChecksums() ? CHUNK_CHECKSUM_FORMAT : FORMAT_VERSION;
    }

    /** The manifest as stored on disk, one line per key, each ending in a line feed. */
    String toText() {
        StringBuilder text = new StringBuilder()
                .append("format=").append(format()).append('\n')
                .append("fileId=").append(fileId).append('\n')
                .append("size=").append(size).append('\n')
                .append("sha256=").append(sha256).append('\n')
                .append("chunkSize=").append(chunkSize).append('\n')
                .append("chunkCount=").append(chunkCount).append('\n');
        if (name != null) {
            text.append(NAME_KEY).append('=').append(FileNames.toManifest(name)).append('\n');
        }
        if (chunkSha256s != null) {
            for (int i = 0; i < chunkSha256s.size(); i++) {
                text.append(chunkKey(i)).append('=').append(chunkSha256s.get(i)).append('\n');
            }
        }
        return text.toString();
    }

    /**
     * Reads a manifest written by {@link #toText()}, in format 1, 2 or 3.
     *
     * @param expectedFileId the ID this manifest must describe (the ID in its file name)
     * @throws InvalidManifestException if the text is not exactly a valid manifest for
     *         {@code expectedFileId}
     */
    static FileManifest parse(String text, String expectedFileId) throws InvalidManifestException {
        Objects.requireNonNull(text, "text must not be null");
        // The first six lines are the same in every format; the format line decides what follows.
        String header = firstLines(text, KEYS.length);
        String[] values = readFields(header, KEYS);
        boolean withChunkChecksums;
        boolean withName;
        if (values[0].equals(String.valueOf(FORMAT_VERSION))) {
            withChunkChecksums = false;
            withName = false;
        } else if (values[0].equals(String.valueOf(CHUNK_CHECKSUM_FORMAT))) {
            withChunkChecksums = true;
            withName = false;
        } else if (values[0].equals(String.valueOf(NAME_FORMAT))) {
            withChunkChecksums = true;
            withName = true;
        } else {
            throw new InvalidManifestException("unsupported manifest format \"" + values[0] + "\"");
        }
        if (!values[1].equals(expectedFileId)) {
            throw new InvalidManifestException("the manifest describes a different file ID than its name says");
        }
        int chunkCount = toInt(parseNumber(values[5], "chunkCount"), "chunkCount");
        String rest = text.substring(header.length());
        String name = null;
        if (withName) {
            String nameLine = firstLines(rest, 1);
            name = FileNames.fromManifest(readFields(nameLine, NAME_KEY)[0]);
            rest = rest.substring(nameLine.length());
        }
        List<String> chunkSha256s = null;
        if (withChunkChecksums) {
            if (chunkCount > MAX_CHECKSUMMED_CHUNKS) {
                throw new InvalidManifestException("chunkCount is larger than " + MAX_CHECKSUMMED_CHUNKS);
            }
            String[] keys = new String[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                keys[i] = chunkKey(i);
            }
            chunkSha256s = new ArrayList<>(List.of(readFields(rest, keys)));
        }
        FileManifest manifest;
        try {
            manifest = new FileManifest(values[1], parseNumber(values[2], "size"), values[3],
                    toInt(parseNumber(values[4], "chunkSize"), "chunkSize"), chunkCount, chunkSha256s, name);
        } catch (IllegalArgumentException e) {
            throw new InvalidManifestException("invalid manifest values: " + e.getMessage(), e);
        }
        requireCanonical(text, manifest.toText());
        return manifest;
    }

    /** The key of chunk {@code index}'s checksum line: {@code chunk<index>.sha256}. */
    private static String chunkKey(int index) {
        return "chunk" + index + ".sha256";
    }

    /**
     * The first {@code count} lines of {@code text}, each with its line feed. Fewer lines, or text
     * that does not end in a line feed, is reported as invalid.
     */
    private static String firstLines(String text, int count) throws InvalidManifestException {
        int end = 0;
        for (int i = 0; i < count; i++) {
            int lineFeed = text.indexOf('\n', end);
            if (lineFeed < 0) {
                throw new InvalidManifestException("expected at least " + count + " lines ending in a line feed");
            }
            end = lineFeed + 1;
        }
        return text.substring(0, end);
    }

    // ---------------------------------------------------------------------
    // Strict "key=value" text, shared with ManifestStore's pending-upload markers
    // ---------------------------------------------------------------------

    /**
     * Splits {@code text} into exactly {@code keys.length} lines of the form {@code key=value},
     * with the keys in exactly the given order, and returns the values. Every line, including the
     * last, must end with a line feed. Only printable ASCII is allowed inside a line. No keys means
     * the text must be empty.
     */
    static String[] readFields(String text, String... keys) throws InvalidManifestException {
        Objects.requireNonNull(text, "text must not be null");
        if (keys.length == 0) {
            if (!text.isEmpty()) {
                throw new InvalidManifestException("expected no more lines but found more text");
            }
            return new String[0];
        }
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
