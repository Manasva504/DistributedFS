package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Calculates and verifies SHA-256 checksums of files.
 *
 * <p>A checksum is a short "fingerprint" of a file's bytes. The same bytes always give the
 * same checksum, and changing even one bit gives a completely different one. That makes it
 * useful for checking that data has not been corrupted since the checksum was recorded.
 *
 * <p>Two separate jobs:
 * <ul>
 *   <li>{@link #calculateSha256(Path)} <em>describes</em> a file: bytes in, fingerprint out.</li>
 *   <li>{@link #matches(Path, String)} <em>verifies</em> a file against a fingerprint recorded
 *       earlier. It has three outcomes: {@code true} (same data), {@code false} (the data has
 *       changed), or an exception (the check could not be done at all, for example because the
 *       file is missing or the expected value is malformed). "Could not check" is never
 *       reported as {@code false}.</li>
 * </ul>
 *
 * <p>Files are read as a stream through one fixed-size buffer, so memory use stays the same
 * no matter how large the file is. Files are only read, never modified.
 */
public class ChecksumService {

    /** Size of the reusable read buffer: 64 KiB. Any size gives the same checksum. */
    private static final int BUFFER_SIZE = 64 * 1024;

    private static final String ALGORITHM = "SHA-256";

    /** A SHA-256 checksum is 32 bytes, which is 64 hexadecimal characters. */
    private static final int SHA256_HEX_LENGTH = 64;

    /**
     * Calculates the SHA-256 checksum of a file.
     *
     * @param file the file to read
     * @return the checksum as 64 lowercase hexadecimal characters
     * @throws NullPointerException if {@code file} is null
     * @throws IllegalArgumentException if the file is missing, a directory, not a regular file,
     *         or not readable
     * @throws IOException if reading fails partway through (no checksum is returned in that case)
     */
    public String calculateSha256(Path file) throws IOException {
        return HexFormat.of().formatHex(calculateSha256Bytes(file));
    }

    /**
     * Calculates the SHA-256 checksum of a file and compares it with an expected value.
     *
     * @param file the file to check
     * @param expectedSha256 the checksum the file should have, as 64 hexadecimal characters
     *        (uppercase, lowercase, or mixed)
     * @return {@code true} if the file's checksum equals {@code expectedSha256},
     *         {@code false} if it is a different checksum
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the file is invalid (see {@link #calculateSha256(Path)})
     *         or if {@code expectedSha256} is not exactly 64 hexadecimal characters
     * @throws IOException if reading the file fails
     */
    public boolean matches(Path file, String expectedSha256) throws IOException {
        // Check the expected value first: a malformed value is a programming mistake,
        // and there is no point reading a large file if we cannot compare the result.
        byte[] expected = parseSha256Hex(expectedSha256);
        byte[] actual = calculateSha256Bytes(file);
        // Comparing bytes (not strings) means letter case in the hex text does not matter.
        // MessageDigest.isEqual is Java's standard way to compare two digests.
        return MessageDigest.isEqual(expected, actual);
    }

    /** Reads the whole file through the buffer and returns the raw 32-byte SHA-256 digest. */
    private static byte[] calculateSha256Bytes(Path file) throws IOException {
        validateFile(file);

        // A new MessageDigest for every call: MessageDigest objects remember their state,
        // so they must not be shared between calls that could run at the same time.
        MessageDigest digest = newSha256Digest();
        byte[] buffer = new byte[BUFFER_SIZE];

        try (InputStream in = Files.newInputStream(file)) {
            while (true) {
                int bytesRead = in.read(buffer);
                if (bytesRead == -1) {
                    break; // end of file
                }
                // Feed only the bytes actually read. Using update(buffer) here would be a bug:
                // after a short read, the rest of the buffer still holds old bytes from the
                // previous read, and they would be hashed too.
                digest.update(buffer, 0, bytesRead);
            }
        }

        // digest() finishes the calculation and returns the 32-byte result.
        return digest.digest();
    }

    private static MessageDigest newSha256Digest() {
        try {
            return MessageDigest.getInstance(ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            // Every Java installation is required to support SHA-256, so this cannot happen
            // on a standard JDK. Reporting it as IllegalStateException means callers do not
            // have to handle an exception that never occurs in practice.
            throw new IllegalStateException(ALGORITHM + " is not available in this Java installation", e);
        }
    }

    /** Converts a 64-character hex checksum (any letter case) to 32 bytes, or rejects it. */
    private static byte[] parseSha256Hex(String hex) {
        Objects.requireNonNull(hex, "expectedSha256 must not be null");
        if (hex.length() != SHA256_HEX_LENGTH) {
            throw new IllegalArgumentException("Expected SHA-256 must be " + SHA256_HEX_LENGTH
                    + " hexadecimal characters, but had " + hex.length() + ": \"" + hex + "\"");
        }
        for (int i = 0; i < hex.length(); i++) {
            if (!HexFormat.isHexDigit(hex.charAt(i))) {
                throw new IllegalArgumentException("Expected SHA-256 contains a non-hexadecimal character '"
                        + hex.charAt(i) + "' at position " + i + ": \"" + hex + "\"");
            }
        }
        // parseHex accepts both uppercase and lowercase letters.
        return HexFormat.of().parseHex(hex);
    }

    private static void validateFile(Path file) {
        Objects.requireNonNull(file, "file must not be null");
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("File does not exist: " + file);
        }
        if (Files.isDirectory(file)) {
            throw new IllegalArgumentException("Path is a directory, not a file: " + file);
        }
        if (!Files.isRegularFile(file)) {
            throw new IllegalArgumentException("Path is not a regular file: " + file);
        }
        if (!Files.isReadable(file)) {
            throw new IllegalArgumentException("File is not readable: " + file);
        }
    }
}
