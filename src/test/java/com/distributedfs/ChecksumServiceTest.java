package com.distributedfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Plain-Java tests for ChecksumService (no JUnit or other libraries).
 *
 * <p>Two modes:
 * <ul>
 *   <li>Normal:  {@code java -cp "out;out-test" com.distributedfs.ChecksumServiceTest}</li>
 *   <li>Stress:  {@code java -Xmx16m -cp "out;out-test" com.distributedfs.ChecksumServiceTest --stress}
 *       runs only the 64 MiB test. With a 16 MB heap it would crash with OutOfMemoryError
 *       if the service ever loaded the whole file into memory.</li>
 * </ul>
 *
 * <p>Each test gets its own temporary directory, which is deleted afterwards.
 * The program exits with code 1 if any test fails.
 */
public class ChecksumServiceTest {

    private static final int KIB = 1024;
    private static final int MIB = 1024 * 1024;

    // Reference values that do not come from our own code:
    // SHA-256 of zero bytes, and of "abc", are published reference values (the "abc" one is
    // the official NIST test vector). The all-256-bytes value was produced by PowerShell's
    // Get-FileHash, which is a completely separate SHA-256 implementation.
    private static final String SHA256_OF_EMPTY =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final String SHA256_OF_ABC =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    private static final String SHA256_OF_ALL_256_BYTE_VALUES =
            "40aff2e9d2d8922e47afd4648e6967497158785fbd1da870e7110266bf944880";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stressMode = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stressMode) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.ChecksumServiceTest [--stress]");
            System.exit(2);
            return;
        }

        if (stressMode) {
            runStressTest();
        } else {
            runNormalTests();
        }

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void runNormalTests() {
        // Known reference values
        runTest("empty file has the well-known SHA-256 of zero bytes", ChecksumServiceTest::emptyFile);
        runTest("\"abc\" has the NIST reference SHA-256", ChecksumServiceTest::abcReferenceValue);
        runTest("binary data (all 256 byte values) matches Get-FileHash", ChecksumServiceTest::binaryData);
        runTest("files larger than the buffer match an in-memory SHA-256", ChecksumServiceTest::filesLargerThanBuffer);

        // Repeatability
        runTest("hashing the same file twice gives the same result", ChecksumServiceTest::sameFileTwice);
        runTest("identical content in different files gives the same result",
                ChecksumServiceTest::identicalContentDifferentFiles);
        runTest("hashing does not modify the file", ChecksumServiceTest::fileIsNotModified);

        // Detecting changes
        runTest("a one-bit change in the first, middle, or last byte is detected",
                ChecksumServiceTest::oneBitChangeIsDetected);
        runTest("adding or removing one byte is detected", ChecksumServiceTest::lengthChangeIsDetected);

        // matches()
        runTest("matches() accepts lowercase, uppercase, and mixed-case checksums",
                ChecksumServiceTest::matchesIgnoresLetterCase);
        runTest("matches() returns false for a valid but different checksum",
                ChecksumServiceTest::matchesReturnsFalseForDifferentChecksum);

        // Validation and failures
        runTest("null arguments are rejected", ChecksumServiceTest::nullArgumentsAreRejected);
        runTest("missing file and directory are rejected", ChecksumServiceTest::invalidFilesAreRejected);
        runTest("malformed expected checksums are rejected, not reported as false",
                ChecksumServiceTest::malformedExpectedChecksumIsRejected);
        runTest("matches() on a missing file throws instead of returning false",
                ChecksumServiceTest::matchesOnMissingFileThrows);
    }

    private static void runStressTest() {
        long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
        System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
        if (maxHeapMib >= 64) {
            System.out.println("WARNING: the heap can hold the whole 64 MiB file, so this run does not prove"
                    + " streaming. Run with -Xmx16m.");
        }
        runTest("64 MiB file is hashed correctly with a small heap", ChecksumServiceTest::largeFileIsStreamed);
    }

    // ---------------------------------------------------------------------
    // Known reference values
    // ---------------------------------------------------------------------

    private static void emptyFile(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("empty.bin"), new byte[0]);

        checkEquals(SHA256_OF_EMPTY, new ChecksumService().calculateSha256(file), "SHA-256 of empty file");
    }

    private static void abcReferenceValue(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("abc.txt"), "abc".getBytes(StandardCharsets.US_ASCII));

        checkEquals(SHA256_OF_ABC, new ChecksumService().calculateSha256(file), "SHA-256 of \"abc\"");
    }

    private static void binaryData(Path tempDir) throws IOException {
        byte[] allByteValues = new byte[256];
        for (int i = 0; i < 256; i++) {
            allByteValues[i] = (byte) i;
        }
        Path file = Files.write(tempDir.resolve("all-bytes.bin"), allByteValues);

        checkEquals(SHA256_OF_ALL_256_BYTE_VALUES, new ChecksumService().calculateSha256(file),
                "SHA-256 of bytes 0x00..0xFF");
    }

    private static void filesLargerThanBuffer(Path tempDir) throws IOException {
        // The service reads 64 KiB at a time. These sizes cover: exactly one buffer, one byte
        // more (a tiny last read), a size that is not a multiple of the buffer, and 1 MiB + 1.
        // If the service passed the whole buffer to update() instead of only the bytes read,
        // every size except the exact multiple would give a wrong result.
        int[] sizes = {64 * KIB, 64 * KIB + 1, 100_000, MIB + 1};
        ChecksumService service = new ChecksumService();

        for (int size : sizes) {
            byte[] data = randomBytes(size, size);
            Path file = Files.write(tempDir.resolve("data-" + size + ".bin"), data);

            checkEquals(inMemorySha256(data), service.calculateSha256(file), "SHA-256 of " + size + "-byte file");
        }
    }

    // ---------------------------------------------------------------------
    // Repeatability
    // ---------------------------------------------------------------------

    private static void sameFileTwice(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("data.bin"), randomBytes(200_000, 1));
        ChecksumService service = new ChecksumService();

        String first = service.calculateSha256(file);
        String second = service.calculateSha256(file);

        checkEquals(first, second, "second checksum of the same file");
    }

    private static void identicalContentDifferentFiles(Path tempDir) throws IOException {
        // The checksum depends only on the bytes, not on the file name or location.
        byte[] data = randomBytes(5000, 1);
        Path fileA = Files.write(tempDir.resolve("a.bin"), data);
        Path fileB = Files.write(Files.createDirectory(tempDir.resolve("sub")).resolve("b.dat"), data);
        ChecksumService service = new ChecksumService();

        checkEquals(service.calculateSha256(fileA), service.calculateSha256(fileB), "checksum of the identical copy");
    }

    private static void fileIsNotModified(Path tempDir) throws IOException {
        byte[] data = randomBytes(200_000, 1);
        Path file = Files.write(tempDir.resolve("data.bin"), data);
        // A fixed, old timestamp, so any write to the file would visibly change it.
        FileTime originalTime = FileTime.fromMillis(1_000_000_000_000L);
        Files.setLastModifiedTime(file, originalTime);
        ChecksumService service = new ChecksumService();

        service.calculateSha256(file);
        service.matches(file, SHA256_OF_EMPTY);

        check(Arrays.equals(data, Files.readAllBytes(file)), "file content should be unchanged");
        checkEquals(originalTime, Files.getLastModifiedTime(file), "file last-modified time");
    }

    // ---------------------------------------------------------------------
    // Detecting changes
    // ---------------------------------------------------------------------

    private static void oneBitChangeIsDetected(Path tempDir) throws IOException {
        byte[] original = randomBytes(MIB + 1, 1); // the last byte lands in a separate, 1-byte read
        Path file = tempDir.resolve("data.bin");
        Files.write(file, original);
        ChecksumService service = new ChecksumService();
        String originalChecksum = service.calculateSha256(file);

        int[] positions = {0, original.length / 2, original.length - 1};
        String[] names = {"first", "middle", "last"};
        for (int i = 0; i < positions.length; i++) {
            byte[] modified = original.clone();
            modified[positions[i]] ^= 0x01; // flip the lowest bit of one byte
            Files.write(file, modified);

            String newChecksum = service.calculateSha256(file);
            check(!newChecksum.equals(originalChecksum),
                    "checksum should change when the " + names[i] + " byte changes");
            check(!service.matches(file, originalChecksum),
                    "matches() should return false after the " + names[i] + " byte changes");
        }
    }

    private static void lengthChangeIsDetected(Path tempDir) throws IOException {
        byte[] original = randomBytes(10_000, 1);
        Path file = tempDir.resolve("data.bin");
        Files.write(file, original);
        ChecksumService service = new ChecksumService();
        String originalChecksum = service.calculateSha256(file);

        // One extra zero byte at the end.
        Files.write(file, Arrays.copyOf(original, original.length + 1));
        check(!service.matches(file, originalChecksum), "matches() should return false after appending a byte");

        // Last byte removed.
        Files.write(file, Arrays.copyOf(original, original.length - 1));
        check(!service.matches(file, originalChecksum), "matches() should return false after removing a byte");

        // Restoring the original bytes restores the match.
        Files.write(file, original);
        check(service.matches(file, originalChecksum), "matches() should return true after restoring the bytes");
    }

    // ---------------------------------------------------------------------
    // matches()
    // ---------------------------------------------------------------------

    private static void matchesIgnoresLetterCase(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("abc.txt"), "abc".getBytes(StandardCharsets.US_ASCII));
        ChecksumService service = new ChecksumService();
        String upper = SHA256_OF_ABC.toUpperCase(Locale.ROOT); // the style Get-FileHash prints
        String mixed = "BA7816bf8F01cfea414140de5dae2223b00361a396177a9cb410ff61f20015AD";

        check(service.matches(file, SHA256_OF_ABC), "lowercase checksum should match");
        check(service.matches(file, upper), "uppercase checksum should match");
        check(service.matches(file, mixed), "mixed-case checksum should match");
    }

    private static void matchesReturnsFalseForDifferentChecksum(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("abc.txt"), "abc".getBytes(StandardCharsets.US_ASCII));
        ChecksumService service = new ChecksumService();
        // Same as the correct checksum except for the very last character.
        String almostRight = SHA256_OF_ABC.substring(0, 63) + "e";

        check(!service.matches(file, SHA256_OF_EMPTY), "a different valid checksum should not match");
        check(!service.matches(file, almostRight), "a checksum differing in one character should not match");
    }

    // ---------------------------------------------------------------------
    // Validation and failures
    // ---------------------------------------------------------------------

    private static void nullArgumentsAreRejected(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("data.bin"), randomBytes(10, 1));
        ChecksumService service = new ChecksumService();

        expectThrows(NullPointerException.class, () -> service.calculateSha256(null));
        expectThrows(NullPointerException.class, () -> service.matches(null, SHA256_OF_EMPTY));
        expectThrows(NullPointerException.class, () -> service.matches(file, null));
    }

    private static void invalidFilesAreRejected(Path tempDir) throws IOException {
        Path missing = tempDir.resolve("does-not-exist.bin");
        Path directory = Files.createDirectory(tempDir.resolve("some-dir"));
        ChecksumService service = new ChecksumService();

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class,
                () -> service.calculateSha256(missing));
        checkContains(e1.getMessage(), "does not exist");

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class,
                () -> service.calculateSha256(directory));
        checkContains(e2.getMessage(), "is a directory");
    }

    private static void malformedExpectedChecksumIsRejected(Path tempDir) throws IOException {
        Path file = Files.write(tempDir.resolve("abc.txt"), "abc".getBytes(StandardCharsets.US_ASCII));
        ChecksumService service = new ChecksumService();

        String[] malformed = {
            "",                                       // empty
            SHA256_OF_ABC.substring(0, 63),           // one character too short
            SHA256_OF_ABC + "0",                      // one character too long
            SHA256_OF_ABC.substring(0, 63) + "g",     // right length, but 'g' is not hex
            " " + SHA256_OF_ABC.substring(0, 63),     // space inside the 64 characters
            " " + SHA256_OF_ABC + " ",                // surrounding spaces (66 characters)
            "0x" + SHA256_OF_ABC.substring(0, 62),    // "0x" prefix
        };
        for (String expected : malformed) {
            expectThrows(IllegalArgumentException.class, () -> service.matches(file, expected));
        }
    }

    private static void matchesOnMissingFileThrows(Path tempDir) {
        // "Could not check" must never be reported as false ("data is corrupt").
        Path missing = tempDir.resolve("does-not-exist.bin");

        expectThrows(IllegalArgumentException.class, () -> new ChecksumService().matches(missing, SHA256_OF_EMPTY));
    }

    // ---------------------------------------------------------------------
    // Stress test (run separately with --stress and -Xmx16m)
    // ---------------------------------------------------------------------

    private static void largeFileIsStreamed(Path tempDir) throws IOException {
        // Write 64 MiB using one reusable 1 MiB buffer, and calculate the expected checksum
        // piece by piece at the same time, so the test itself never holds the whole file.
        byte[] buffer = new byte[MIB];
        Random random = new Random(12345);
        MessageDigest expectedDigest = newSha256Digest();
        Path file = tempDir.resolve("large.bin");
        try (OutputStream out = Files.newOutputStream(file)) {
            for (int i = 0; i < 64; i++) {
                random.nextBytes(buffer);
                out.write(buffer);
                expectedDigest.update(buffer);
            }
        }
        String expected = HexFormat.of().formatHex(expectedDigest.digest());

        ChecksumService service = new ChecksumService();
        checkEquals(expected, service.calculateSha256(file), "SHA-256 of 64 MiB file");
        check(service.matches(file, expected), "matches() should return true for the 64 MiB file");
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as FileChunkerTest)
    // ---------------------------------------------------------------------

    /** A test body that receives a fresh temporary directory. */
    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    /** A piece of code that is expected to throw. */
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("checksumservice-test-");
            body.run(tempDir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (AssertionError e) {
            failed++;
            System.out.println("FAIL  " + name);
            System.out.println("      " + e.getMessage());
        } catch (Throwable e) {
            failed++;
            System.out.println("FAIL  " + name + " (unexpected exception)");
            e.printStackTrace(System.out);
        } finally {
            if (tempDir != null) {
                try {
                    deleteRecursively(tempDir);
                } catch (IOException e) {
                    System.out.println("      warning: could not delete " + tempDir + ": " + e);
                }
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkEquals(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void checkContains(String text, String expectedPart) {
        if (text == null || !text.contains(expectedPart)) {
            throw new AssertionError("expected message containing \"" + expectedPart + "\" but was \"" + text + "\"");
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingAction action) {
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError("expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError("expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }

    // ---------------------------------------------------------------------
    // Data and file helpers
    // ---------------------------------------------------------------------

    /** Returns deterministic pseudo-random bytes; different seeds give different contents. */
    private static byte[] randomBytes(int size, long seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    /** SHA-256 of a byte array in one step, used as an independent reference for the streaming code. */
    private static String inMemorySha256(byte[] data) {
        return HexFormat.of().formatHex(newSha256Digest().digest(data));
    }

    private static MessageDigest newSha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void deleteRecursively(Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            // Reverse order puts children before their parent directories.
            List<Path> deepestFirst = paths.sorted(Comparator.reverseOrder()).toList();
            for (Path path : deepestFirst) {
                Files.deleteIfExists(path);
            }
        }
    }
}
