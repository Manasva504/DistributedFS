package com.distributedfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Plain-Java tests for FileIds (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.FileIdsTest}
 *
 * <p>Invalid IDs include path tricks and non-ASCII look-alikes, because the HTTP layer will use
 * FileIds.isValid as the first check on a file ID taken from a request path. Derived chunk IDs
 * are checked against a real StorageNode, not against a copy of its rules.
 * The program exits with code 1 if any test fails.
 */
public class FileIdsTest {

    private static final String VALID_ID = "0123456789abcdef0123456789abcdef";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.FileIdsTest");
            System.exit(2);
            return;
        }

        // New IDs
        runTest("a new file ID is 32 lowercase hexadecimal characters", FileIdsTest::newIdHasTheFormat);
        runTest("10,000 new file IDs are valid, all different, and use every hex digit",
                FileIdsTest::manyNewIdsAreValidAndDistinct);

        // Validation
        runTest("well-formed IDs are accepted", FileIdsTest::wellFormedIdsAreAccepted);
        runTest("null, empty, and wrong-length IDs are rejected", FileIdsTest::wrongLengthIsRejected);
        runTest("uppercase and non-hex letters are rejected", FileIdsTest::wrongLettersAreRejected);
        runTest("path tricks and separators are rejected", FileIdsTest::pathTricksAreRejected);
        runTest("non-ASCII look-alikes are rejected", FileIdsTest::lookAlikesAreRejected);
        runTest("UUID spellings are rejected", FileIdsTest::uuidSpellingsAreRejected);

        // Chunk IDs
        runTest("a chunk ID is the file ID, an underscore, and the index", FileIdsTest::chunkIdFormat);
        runTest("chunkId rejects a null or invalid file ID and a negative index", FileIdsTest::chunkIdRejectsBadInput);
        runTest("chunk IDs of different files and indexes never collide", FileIdsTest::chunkIdsNeverCollide);
        runTest("derived chunk IDs work as chunk IDs on a real StorageNode", FileIdsTest::storageNodeAcceptsChunkIds);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // New IDs
    // ---------------------------------------------------------------------

    private static void newIdHasTheFormat(Path tempDir) {
        String id = FileIds.newFileId();
        checkEquals(32, id.length(), "length of " + id);
        checkEquals(FileIds.LENGTH, id.length(), "FileIds.LENGTH");
        for (char c : id.toCharArray()) {
            check((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'), "unexpected character '" + c + "' in " + id);
        }
        check(FileIds.isValid(id), "a new ID must pass isValid: " + id);
    }

    private static void manyNewIdsAreValidAndDistinct(Path tempDir) {
        Set<String> ids = new HashSet<>();
        Set<Character> digitsSeen = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            String id = FileIds.newFileId();
            check(FileIds.isValid(id), "invalid new ID: " + id);
            check(ids.add(id), "the same ID was generated twice: " + id);
            for (char c : id.toCharArray()) {
                digitsSeen.add(c);
            }
        }
        checkEquals(16, digitsSeen.size(), "number of different hex digits seen");
    }

    // ---------------------------------------------------------------------
    // Validation
    // ---------------------------------------------------------------------

    private static void wellFormedIdsAreAccepted(Path tempDir) {
        for (String id : List.of(VALID_ID, "0".repeat(32), "f".repeat(32), "a1b2c3d4e5f60718293a4b5c6d7e8f90")) {
            check(FileIds.isValid(id), "should be valid: " + id);
        }
    }

    private static void wrongLengthIsRejected(Path tempDir) {
        checkRejected(null, "null");
        checkRejected("", "empty");
        checkRejected(VALID_ID.substring(1), "31 characters");
        checkRejected(VALID_ID + "0", "33 characters");
        checkRejected(VALID_ID + VALID_ID, "64 characters");
        checkRejected(VALID_ID.substring(1) + "\n", "31 characters and a line break");
    }

    private static void wrongLettersAreRejected(Path tempDir) {
        checkRejected(VALID_ID.toUpperCase(Locale.ROOT), "all uppercase");
        for (char c : new char[] {'A', 'F', 'g', 'z', 'G', 'x'}) {
            checkRejected(withCharAt(VALID_ID, 31, c), "ends with '" + c + "'");
        }
    }

    private static void pathTricksAreRejected(Path tempDir) {
        for (char c : new char[] {'.', '/', '\\', ':', '%', ' ', '_', '-', '+', '*', '?', '\n', '\r', '\t', '\0'}) {
            checkRejected(withCharAt(VALID_ID, 0, c), "starts with U+" + String.format("%04X", (int) c));
        }
        checkRejected("../" + "0".repeat(29), "../ prefix");
        checkRejected("..\\" + "0".repeat(29), "..\\ prefix");
        checkRejected("%2e%2e%2f" + "0".repeat(23), "percent-encoded ../");
        checkRejected("%2E%2E%5C" + "0".repeat(23), "percent-encoded ..\\");
        checkRejected("c:" + "0".repeat(30), "drive letter");
        checkRejected("con" + " ".repeat(29), "device name padded with spaces");
    }

    private static void lookAlikesAreRejected(Path tempDir) {
        // Each of these looks like a hex digit or letter, and some pass Character.isDigit.
        char[] lookAlikes = {
                '\uFF10', // fullwidth digit zero
                '\u0660', // Arabic-Indic digit zero
                '\u0966', // Devanagari digit zero
                '\u0430', // Cyrillic small a
                '\u0435', // Cyrillic small ie (looks like e)
                '\u0441', // Cyrillic small es (looks like c)
                '\u00E9', // e with acute accent
                '\uFF41', // fullwidth small a
        };
        for (char c : lookAlikes) {
            checkRejected(withCharAt(VALID_ID, 10, c), "contains U+" + String.format("%04X", (int) c));
        }
    }

    private static void uuidSpellingsAreRejected(Path tempDir) {
        checkRejected("01234567-89ab-cdef-0123-456789abcdef", "UUID with dashes");
        checkRejected("{01234567-89ab-cdef-0123-456789abcdef}", "UUID in braces");
        checkRejected("0x0123456789abcdef0123456789abcd", "0x prefix");
    }

    // ---------------------------------------------------------------------
    // Chunk IDs
    // ---------------------------------------------------------------------

    private static void chunkIdFormat(Path tempDir) {
        checkEquals(VALID_ID + "_0", FileIds.chunkId(VALID_ID, 0), "chunk 0");
        checkEquals(VALID_ID + "_7", FileIds.chunkId(VALID_ID, 7), "chunk 7");
        String last = FileIds.chunkId(VALID_ID, Integer.MAX_VALUE);
        checkEquals(VALID_ID + "_2147483647", last, "chunk Integer.MAX_VALUE");
        checkEquals(43, last.length(), "length of the longest chunk ID");
    }

    private static void chunkIdRejectsBadInput(Path tempDir) {
        expectThrows(NullPointerException.class, "null file ID", () -> FileIds.chunkId(null, 0));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, "invalid file ID",
                () -> FileIds.chunkId("../" + "0".repeat(29), 0));
        check(!e.getMessage().contains(".."), "the message must not repeat the rejected value: " + e.getMessage());
        expectThrows(IllegalArgumentException.class, "uppercase file ID",
                () -> FileIds.chunkId(VALID_ID.toUpperCase(Locale.ROOT), 0));
        expectThrows(IllegalArgumentException.class, "negative index", () -> FileIds.chunkId(VALID_ID, -1));
        expectThrows(IllegalArgumentException.class, "most negative index",
                () -> FileIds.chunkId(VALID_ID, Integer.MIN_VALUE));
    }

    private static void chunkIdsNeverCollide(Path tempDir) {
        List<String> fileIds = List.of(VALID_ID, "0".repeat(32), "f".repeat(32), FileIds.newFileId());
        int[] indexes = {0, 1, 2, 10, 11, 12, 100, 101, 110, 111, Integer.MAX_VALUE};
        Set<String> chunkIds = new HashSet<>();
        for (String fileId : fileIds) {
            for (int index : indexes) {
                String chunkId = FileIds.chunkId(fileId, index);
                check(chunkIds.add(chunkId), "chunk ID produced twice: " + chunkId);
            }
        }
    }

    private static void storageNodeAcceptsChunkIds(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        String fileId = FileIds.newFileId();
        List<String> chunkIds = new ArrayList<>();
        for (int index : new int[] {0, 1, Integer.MAX_VALUE}) {
            chunkIds.add(FileIds.chunkId(fileId, index));
        }

        for (int i = 0; i < chunkIds.size(); i++) {
            String chunkId = chunkIds.get(i);
            Path source = tempDir.resolve("source-" + i);
            byte[] data = ("chunk " + chunkId).getBytes(StandardCharsets.US_ASCII);
            Files.write(source, data);

            check(!node.hasChunk(chunkId), "chunk should not exist yet: " + chunkId);
            checkEquals((long) data.length, node.storeChunk(chunkId, source), "bytes stored for " + chunkId);
            check(node.hasChunk(chunkId), "chunk should exist now: " + chunkId);

            Path copy = tempDir.resolve("copy-" + i);
            node.retrieveChunk(chunkId, copy);
            check(Arrays.equals(data, Files.readAllBytes(copy)), "retrieved bytes differ for " + chunkId);
        }
        checkEquals(chunkIds.stream().sorted().toList(), listNames(tempDir.resolve("node")),
                "the node's folder holds exactly the derived chunk IDs");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** The ID must fail isValid, and chunkId must refuse it. */
    private static void checkRejected(String id, String what) {
        check(!FileIds.isValid(id), "should be rejected (" + what + "): " + id);
        if (id == null) {
            expectThrows(NullPointerException.class, what, () -> FileIds.chunkId(null, 0));
        } else {
            expectThrows(IllegalArgumentException.class, what, () -> FileIds.chunkId(id, 0));
        }
    }

    private static String withCharAt(String text, int index, char c) {
        char[] chars = text.toCharArray();
        chars[index] = c;
        return new String(chars);
    }

    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
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
            tempDir = Files.createTempDirectory("fileids-test-");
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

    /** Like expectThrows in the other suites, with a short description used in failure messages. */
    private static <T extends Throwable> T expectThrows(Class<T> expectedType, String what, ThrowingAction action) {
        String prefix = what.isEmpty() ? "" : what + ": ";
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError(prefix + "expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError(prefix + "expected " + expectedType.getSimpleName() + " but nothing was thrown");
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
