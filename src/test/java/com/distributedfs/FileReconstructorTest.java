package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Plain-Java tests for FileReconstructor (no JUnit or other libraries).
 *
 * <p>Two modes:
 * <ul>
 *   <li>Normal:  {@code java -cp "out;out-test" com.distributedfs.FileReconstructorTest}</li>
 *   <li>Stress:  {@code java -Xmx16m -cp "out;out-test" com.distributedfs.FileReconstructorTest --stress}
 *       runs only the 64 MiB test. With a 16 MB heap it would crash with OutOfMemoryError
 *       if the reconstructor ever loaded the whole file into memory.</li>
 * </ul>
 *
 * <p>Each test gets its own temporary directory, which is deleted afterwards.
 * The program exits with code 1 if any test fails.
 */
public class FileReconstructorTest {

    private static final int MIB = 1024 * 1024;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stressMode = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stressMode) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.FileReconstructorTest [--stress]");
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
        // Basic reconstruction
        runTest("one chunk is reconstructed", FileReconstructorTest::oneChunk);
        runTest("multiple chunks are joined in list order", FileReconstructorTest::multipleChunksInOrder);
        runTest("a different list order gives a different result", FileReconstructorTest::supplierOrderIsRespected);
        runTest("empty chunk list produces an empty file", FileReconstructorTest::emptyChunkList);
        runTest("an empty chunk file contributes no bytes", FileReconstructorTest::emptyChunkFile);
        runTest("missing destination parent directories are created", FileReconstructorTest::parentDirsAreCreated);

        // Exact bytes
        runTest("binary data (all 256 byte values, CR/LF, invalid UTF-8) is preserved",
                FileReconstructorTest::binaryDataIsPreserved);
        runTest("FileChunker output reconstructs to an identical file", FileReconstructorTest::roundTripWithFileChunker);

        // Validation
        runTest("missing chunk is rejected before the destination is created",
                FileReconstructorTest::missingChunkIsRejected);
        runTest("directory used as a chunk is rejected", FileReconstructorTest::directoryAsChunkIsRejected);
        runTest("null arguments are rejected", FileReconstructorTest::nullArgumentsAreRejected);

        // Never overwrite
        runTest("existing destination file is not overwritten", FileReconstructorTest::existingDestinationIsNotOverwritten);
        runTest("destination that is one of the chunks is not overwritten",
                FileReconstructorTest::chunkAsDestinationIsNotOverwritten);
        runTest("existing directory as destination is rejected", FileReconstructorTest::directoryAsDestinationIsRejected);

        // Failure cleanup
        runTest("I/O failure partway through deletes the partial destination",
                FileReconstructorTest::ioFailureDeletesPartialDestination);
    }

    private static void runStressTest() {
        long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
        System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
        if (maxHeapMib >= 64) {
            System.out.println("WARNING: the heap can hold the whole 64 MiB file, so this run does not prove"
                    + " streaming. Run with -Xmx16m.");
        }
        runTest("64 MiB across 64 chunks is reconstructed with a small heap",
                FileReconstructorTest::largeFileIsStreamed);
    }

    // ---------------------------------------------------------------------
    // Basic reconstruction
    // ---------------------------------------------------------------------

    private static void oneChunk(Path tempDir) throws IOException {
        byte[] data = randomBytes(1000, 1);
        Path chunk = Files.write(tempDir.resolve("chunk_0"), data);
        Path destination = tempDir.resolve("result.bin");

        long written = new FileReconstructor().reconstruct(List.of(chunk), destination);

        checkEquals(1000L, written, "bytes written");
        checkBytes(data, destination);
    }

    private static void multipleChunksInOrder(Path tempDir) throws IOException {
        byte[] part0 = randomBytes(300, 1);
        byte[] part1 = randomBytes(300, 2);
        byte[] part2 = randomBytes(17, 3); // a smaller last chunk, like FileChunker produces
        List<Path> chunks = List.of(
                Files.write(tempDir.resolve("chunk_0"), part0),
                Files.write(tempDir.resolve("chunk_1"), part1),
                Files.write(tempDir.resolve("chunk_2"), part2));
        Path destination = tempDir.resolve("result.bin");

        long written = new FileReconstructor().reconstruct(chunks, destination);

        checkEquals(617L, written, "bytes written");
        checkBytes(concat(part0, part1, part2), destination);
    }

    private static void supplierOrderIsRespected(Path tempDir) throws IOException {
        byte[] first = "AAA".getBytes(StandardCharsets.US_ASCII);
        byte[] second = "BBB".getBytes(StandardCharsets.US_ASCII);
        Path chunkA = Files.write(tempDir.resolve("chunk_0"), first);
        Path chunkB = Files.write(tempDir.resolve("chunk_1"), second);
        Path destination = tempDir.resolve("result.bin");

        // The reconstructor must follow the list, not sort by file name.
        new FileReconstructor().reconstruct(List.of(chunkB, chunkA), destination);

        checkBytes(concat(second, first), destination);
    }

    private static void emptyChunkList(Path tempDir) throws IOException {
        Path destination = tempDir.resolve("result.bin");

        long written = new FileReconstructor().reconstruct(List.of(), destination);

        checkEquals(0L, written, "bytes written");
        check(Files.isRegularFile(destination), "an empty destination file should have been created");
        checkEquals(0L, Files.size(destination), "destination size");
    }

    private static void emptyChunkFile(Path tempDir) throws IOException {
        byte[] part0 = randomBytes(50, 1);
        byte[] part2 = randomBytes(50, 2);
        List<Path> chunks = List.of(
                Files.write(tempDir.resolve("chunk_0"), part0),
                Files.write(tempDir.resolve("chunk_1"), new byte[0]),
                Files.write(tempDir.resolve("chunk_2"), part2));
        Path destination = tempDir.resolve("result.bin");

        long written = new FileReconstructor().reconstruct(chunks, destination);

        checkEquals(100L, written, "bytes written");
        checkBytes(concat(part0, part2), destination);
    }

    private static void parentDirsAreCreated(Path tempDir) throws IOException {
        byte[] data = randomBytes(10, 1);
        Path chunk = Files.write(tempDir.resolve("chunk_0"), data);
        Path destination = tempDir.resolve("a").resolve("b").resolve("result.bin");

        new FileReconstructor().reconstruct(List.of(chunk), destination);

        checkBytes(data, destination);
    }

    // ---------------------------------------------------------------------
    // Exact bytes
    // ---------------------------------------------------------------------

    private static void binaryDataIsPreserved(Path tempDir) throws IOException {
        // Every possible byte value, 0x00 to 0xFF.
        byte[] allByteValues = new byte[256];
        for (int i = 0; i < 256; i++) {
            allByteValues[i] = (byte) i;
        }
        // Bytes that text handling commonly changes: Windows line endings, NUL, Ctrl-Z,
        // byte-order marks, and sequences that are not valid UTF-8.
        byte[] trickyBytes = {
            '\r', '\n', '\n', '\r', 0x00, 0x1A,
            (byte) 0xEF, (byte) 0xBB, (byte) 0xBF,   // UTF-8 byte-order mark
            (byte) 0xFF, (byte) 0xFE,                // UTF-16 byte-order mark
            (byte) 0xC3, (byte) 0x28,                // invalid UTF-8 sequence
            (byte) 0x80, (byte) 0xFF                 // lone continuation byte, 0xFF
        };
        List<Path> chunks = List.of(
                Files.write(tempDir.resolve("chunk_0"), allByteValues),
                Files.write(tempDir.resolve("chunk_1"), trickyBytes));
        Path expected = Files.write(tempDir.resolve("expected.bin"), concat(allByteValues, trickyBytes));
        Path destination = tempDir.resolve("result.bin");

        new FileReconstructor().reconstruct(chunks, destination);

        // Files.mismatch returns -1 when both files are byte-for-byte identical.
        checkEquals(-1L, Files.mismatch(expected, destination), "position of first differing byte (-1 = identical)");
    }

    private static void roundTripWithFileChunker(Path tempDir) throws IOException {
        Path original = Files.write(tempDir.resolve("original.bin"), randomBytes(2 * MIB + 12_345, 7));

        List<Path> chunks = new FileChunker().chunk(original, tempDir.resolve("chunks"));
        Path destination = tempDir.resolve("reconstructed.bin");
        long written = new FileReconstructor().reconstruct(chunks, destination);

        checkEquals(3, chunks.size(), "number of chunks from FileChunker");
        checkEquals(Files.size(original), written, "bytes written");
        checkEquals(-1L, Files.mismatch(original, destination), "position of first differing byte (-1 = identical)");
    }

    // ---------------------------------------------------------------------
    // Validation
    // ---------------------------------------------------------------------

    private static void missingChunkIsRejected(Path tempDir) throws IOException {
        // The missing chunk is last, so this also proves all chunks are checked
        // before the destination is created (no partial file is left behind).
        List<Path> chunks = List.of(
                Files.write(tempDir.resolve("chunk_0"), randomBytes(10, 1)),
                tempDir.resolve("chunk_1_missing"));
        Path destination = tempDir.resolve("result.bin");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new FileReconstructor().reconstruct(chunks, destination));

        checkContains(e.getMessage(), "Chunk 1 does not exist");
        check(!Files.exists(destination), "destination should not be created when a chunk is missing");
    }

    private static void directoryAsChunkIsRejected(Path tempDir) throws IOException {
        Path directory = Files.createDirectory(tempDir.resolve("some-dir"));
        Path destination = tempDir.resolve("result.bin");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new FileReconstructor().reconstruct(List.of(directory), destination));

        checkContains(e.getMessage(), "is a directory");
        check(!Files.exists(destination), "destination should not be created when a chunk is invalid");
    }

    private static void nullArgumentsAreRejected(Path tempDir) throws IOException {
        Path chunk = Files.write(tempDir.resolve("chunk_0"), randomBytes(10, 1));
        Path destination = tempDir.resolve("result.bin");
        // List.of(...) does not allow nulls, so use Arrays.asList for a list with a null element.
        List<Path> listWithNull = Arrays.asList(chunk, null);

        expectThrows(NullPointerException.class, () -> new FileReconstructor().reconstruct(null, destination));
        NullPointerException e = expectThrows(NullPointerException.class,
                () -> new FileReconstructor().reconstruct(listWithNull, destination));
        checkContains(e.getMessage(), "chunkFiles[1]");
        expectThrows(NullPointerException.class, () -> new FileReconstructor().reconstruct(List.of(chunk), null));

        check(!Files.exists(destination), "destination should not be created when arguments are null");
    }

    // ---------------------------------------------------------------------
    // Never overwrite
    // ---------------------------------------------------------------------

    private static void existingDestinationIsNotOverwritten(Path tempDir) throws IOException {
        Path chunk = Files.write(tempDir.resolve("chunk_0"), randomBytes(10, 1));
        byte[] existingContent = "do not overwrite me".getBytes(StandardCharsets.UTF_8);
        Path destination = Files.write(tempDir.resolve("result.bin"), existingContent);

        expectThrows(FileAlreadyExistsException.class,
                () -> new FileReconstructor().reconstruct(List.of(chunk), destination));

        // The existing file must be untouched: not overwritten, and not deleted by cleanup.
        checkBytes(existingContent, destination);
    }

    private static void chunkAsDestinationIsNotOverwritten(Path tempDir) throws IOException {
        byte[] content0 = randomBytes(10, 1);
        byte[] content1 = randomBytes(10, 2);
        Path chunk0 = Files.write(tempDir.resolve("chunk_0"), content0);
        Path chunk1 = Files.write(tempDir.resolve("chunk_1"), content1);

        // The destination is chunk_0 itself.
        expectThrows(FileAlreadyExistsException.class,
                () -> new FileReconstructor().reconstruct(List.of(chunk0, chunk1), chunk0));

        checkBytes(content0, chunk0);
        checkBytes(content1, chunk1);
    }

    private static void directoryAsDestinationIsRejected(Path tempDir) throws IOException {
        Path chunk = Files.write(tempDir.resolve("chunk_0"), randomBytes(10, 1));
        Path existingDir = Files.createDirectory(tempDir.resolve("existing-dir"));
        Path fileInsideDir = Files.write(existingDir.resolve("keep.txt"), randomBytes(5, 2));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new FileReconstructor().reconstruct(List.of(chunk), existingDir));

        checkContains(e.getMessage(), "existing directory");
        check(Files.isDirectory(existingDir), "existing directory should still exist");
        check(Files.exists(fileInsideDir), "file inside the existing directory should still exist");
    }

    // ---------------------------------------------------------------------
    // Failure cleanup
    // ---------------------------------------------------------------------

    private static void ioFailureDeletesPartialDestination(Path tempDir) throws IOException {
        byte[] content0 = randomBytes(100, 1);
        byte[] content1 = randomBytes(100, 2);
        byte[] content2 = randomBytes(100, 3);
        Path chunk0 = Files.write(tempDir.resolve("chunk_0"), content0);
        Path chunk1 = Files.write(tempDir.resolve("chunk_1"), content1);
        Path chunk2 = Files.write(tempDir.resolve("chunk_2"), content2);
        Path destination = tempDir.resolve("result.bin");
        IOException simulatedFailure = new IOException("simulated disk error");

        // Use the openChunk testability hook: chunk_1 delivers 40 real bytes and then fails.
        // By then, all of chunk_0 and part of chunk_1 have been written to the destination.
        FileReconstructor failingReconstructor = new FileReconstructor() {
            @Override
            InputStream openChunk(Path chunkFile) throws IOException {
                if (chunkFile.equals(chunk1)) {
                    return failAfter(Arrays.copyOf(content1, 40), simulatedFailure);
                }
                return super.openChunk(chunkFile);
            }
        };

        IOException e = expectThrows(IOException.class,
                () -> failingReconstructor.reconstruct(List.of(chunk0, chunk1, chunk2), destination));

        // The original exception is passed through unchanged.
        check(e == simulatedFailure, "expected the original simulated exception, but got " + e);
        checkEquals(0, e.getSuppressed().length, "number of suppressed cleanup errors");
        // The partial destination was removed...
        check(!Files.exists(destination), "partial destination should have been deleted");
        // ...and the chunks were not touched.
        checkBytes(content0, chunk0);
        checkBytes(content1, chunk1);
        checkBytes(content2, chunk2);
    }

    /** Returns a stream that delivers {@code firstBytes} and then throws {@code failure}. */
    private static InputStream failAfter(byte[] firstBytes, IOException failure) {
        return new InputStream() {
            private int position = 0;

            @Override
            public int read() throws IOException {
                if (position < firstBytes.length) {
                    return firstBytes[position++] & 0xFF;
                }
                throw failure;
            }
        };
    }

    // ---------------------------------------------------------------------
    // Stress test (run separately with --stress and -Xmx16m)
    // ---------------------------------------------------------------------

    private static void largeFileIsStreamed(Path tempDir) throws IOException {
        int chunkCount = 64;
        byte[] buffer = new byte[MIB]; // one reusable buffer, so the test itself stays small in memory
        Random random = new Random(12345);
        List<Path> chunks = new ArrayList<>();

        // Write 64 chunk files, and the same bytes into one "expected" file to compare against.
        Path expected = tempDir.resolve("expected.bin");
        try (OutputStream expectedOut = Files.newOutputStream(expected)) {
            for (int i = 0; i < chunkCount; i++) {
                random.nextBytes(buffer);
                chunks.add(Files.write(tempDir.resolve("chunk_" + i), buffer));
                expectedOut.write(buffer);
            }
        }
        Path destination = tempDir.resolve("reconstructed.bin");

        long written = new FileReconstructor().reconstruct(chunks, destination);

        checkEquals((long) chunkCount * MIB, written, "bytes written");
        // Files.mismatch compares the files in small pieces, so it is also memory-safe.
        checkEquals(-1L, Files.mismatch(expected, destination), "position of first differing byte (-1 = identical)");
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
            tempDir = Files.createTempDirectory("filereconstructor-test-");
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

    private static void checkBytes(byte[] expected, Path file) throws IOException {
        if (!Arrays.equals(expected, Files.readAllBytes(file))) {
            throw new AssertionError("unexpected content in " + file.getFileName());
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

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream joined = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            joined.writeBytes(part);
        }
        return joined.toByteArray();
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
