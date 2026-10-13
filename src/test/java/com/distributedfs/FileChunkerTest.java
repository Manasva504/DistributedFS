package com.distributedfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Plain-Java tests for FileChunker (no JUnit or other libraries).
 *
 * <p>Each test gets its own temporary directory, which is deleted afterwards.
 * The program prints PASS/FAIL for every test and exits with code 1 if any test fails.
 */
public class FileChunkerTest {

    private static final int MIB = 1024 * 1024;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        // Chunk sizes and boundaries
        runTest("empty input produces zero chunks", FileChunkerTest::emptyInputProducesZeroChunks);
        runTest("one-byte input produces one chunk", FileChunkerTest::oneByteInputProducesOneChunk);
        runTest("exactly 1 MiB produces one chunk and no empty extra chunk", FileChunkerTest::exactlyOneChunk);
        runTest("1 MiB + 1 byte produces two chunks", FileChunkerTest::oneChunkPlusOneByte);
        runTest("2.5 MiB produces three chunks of the correct sizes", FileChunkerTest::twoAndAHalfMiB);
        runTest("chunk size 3 on a 10-byte file gives sizes 3, 3, 3, 1", FileChunkerTest::smallChunkSize);

        // Data integrity
        runTest("concatenated chunks equal the original file", FileChunkerTest::chunksReassembleToOriginal);
        runTest("original file is unchanged after chunking", FileChunkerTest::originalFileIsUnchanged);
        runTest("missing nested output directories are created", FileChunkerTest::nestedOutputDirIsCreated);

        // Never overwrite
        runTest("existing chunk_0 is not overwritten", FileChunkerTest::existingChunkIsNotOverwritten);
        runTest("input file named chunk_0 inside the output directory is not overwritten",
                FileChunkerTest::inputInsideOutputDirIsNotOverwritten);

        // Validation
        runTest("missing input file is rejected", FileChunkerTest::missingInputIsRejected);
        runTest("directory used as input file is rejected", FileChunkerTest::directoryAsInputIsRejected);
        runTest("existing file used as output directory is rejected", FileChunkerTest::fileAsOutputDirIsRejected);
        runTest("output directory below an existing file is rejected", FileChunkerTest::outputDirBelowFileIsRejected);
        runTest("null arguments are rejected", FileChunkerTest::nullArgumentsAreRejected);
        runTest("invalid chunk sizes are rejected", FileChunkerTest::invalidChunkSizesAreRejected);

        // Failure cleanup
        runTest("I/O failure deletes only the chunks created by that call",
                FileChunkerTest::ioFailureCleansUpOnlyOwnChunks);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Chunk sizes and boundaries
    // ---------------------------------------------------------------------

    private static void emptyInputProducesZeroChunks(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("empty.bin"), new byte[0]);
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        check(chunks.isEmpty(), "expected no chunks, but got " + chunks);
        check(Files.isDirectory(outputDir), "output directory should have been created");
        checkEquals(0, countFiles(outputDir), "number of files in output directory");
    }

    private static void oneByteInputProducesOneChunk(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("one.bin"), new byte[] {42});
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        checkChunkSizes(chunks, outputDir, 1);
        check(Arrays.equals(new byte[] {42}, Files.readAllBytes(chunks.get(0))), "chunk_0 should contain the byte 42");
    }

    private static void exactlyOneChunk(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(MIB));
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        // checkChunkSizes also verifies there is no extra (empty) chunk_1 on disk.
        checkChunkSizes(chunks, outputDir, MIB);
    }

    private static void oneChunkPlusOneByte(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(MIB + 1));
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        checkChunkSizes(chunks, outputDir, MIB, 1);
    }

    private static void twoAndAHalfMiB(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(2 * MIB + MIB / 2));
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        checkChunkSizes(chunks, outputDir, MIB, MIB, MIB / 2);
    }

    private static void smallChunkSize(Path tempDir) throws IOException {
        byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Path input = Files.write(tempDir.resolve("input.bin"), data);
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker(3).chunk(input, outputDir);

        checkChunkSizes(chunks, outputDir, 3, 3, 3, 1);
        checkBytes(new byte[] {0, 1, 2}, chunks.get(0));
        checkBytes(new byte[] {3, 4, 5}, chunks.get(1));
        checkBytes(new byte[] {6, 7, 8}, chunks.get(2));
        checkBytes(new byte[] {9}, chunks.get(3));
    }

    // ---------------------------------------------------------------------
    // Data integrity
    // ---------------------------------------------------------------------

    private static void chunksReassembleToOriginal(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(3 * MIB + 12_345));
        Path outputDir = tempDir.resolve("chunks");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        // Glue the chunks back together, in the order they were returned.
        Path reassembled = tempDir.resolve("reassembled.bin");
        try (OutputStream out = Files.newOutputStream(reassembled)) {
            for (Path chunk : chunks) {
                Files.copy(chunk, out);
            }
        }

        // Files.mismatch returns -1 when both files are byte-for-byte identical.
        checkEquals(-1L, Files.mismatch(input, reassembled), "position of first differing byte (-1 = identical)");
    }

    private static void originalFileIsUnchanged(Path tempDir) throws IOException {
        byte[] originalBytes = randomBytes(2 * MIB + MIB / 2);
        Path input = Files.write(tempDir.resolve("input.bin"), originalBytes);
        // Set a fixed, old timestamp so any write to the file would visibly change it.
        FileTime originalTime = FileTime.fromMillis(1_000_000_000_000L);
        Files.setLastModifiedTime(input, originalTime);

        new FileChunker().chunk(input, tempDir.resolve("chunks"));

        check(Files.exists(input), "input file should still exist");
        check(Arrays.equals(originalBytes, Files.readAllBytes(input)), "input file content should be unchanged");
        checkEquals(originalTime, Files.getLastModifiedTime(input), "input file last-modified time");
    }

    private static void nestedOutputDirIsCreated(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(10));
        Path outputDir = tempDir.resolve("a").resolve("b").resolve("c");

        List<Path> chunks = new FileChunker().chunk(input, outputDir);

        check(Files.isDirectory(outputDir), "nested output directory should have been created");
        checkChunkSizes(chunks, outputDir, 10);
    }

    // ---------------------------------------------------------------------
    // Never overwrite
    // ---------------------------------------------------------------------

    private static void existingChunkIsNotOverwritten(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(10));
        Path outputDir = Files.createDirectory(tempDir.resolve("chunks"));
        byte[] existingContent = "do not overwrite me".getBytes(StandardCharsets.UTF_8);
        Path existingChunk = Files.write(outputDir.resolve("chunk_0"), existingContent);

        expectThrows(FileAlreadyExistsException.class, () -> new FileChunker(3).chunk(input, outputDir));

        checkBytes(existingContent, existingChunk);
        checkEquals(1, countFiles(outputDir), "number of files in output directory");
    }

    private static void inputInsideOutputDirIsNotOverwritten(Path tempDir) throws IOException {
        // The input file itself is named chunk_0 and lives in the output directory.
        // Without CREATE_NEW, writing chunk_0 would destroy the input.
        Path outputDir = Files.createDirectory(tempDir.resolve("chunks"));
        byte[] originalBytes = randomBytes(10);
        Path input = Files.write(outputDir.resolve("chunk_0"), originalBytes);

        expectThrows(FileAlreadyExistsException.class, () -> new FileChunker(3).chunk(input, outputDir));

        checkBytes(originalBytes, input);
        checkEquals(1, countFiles(outputDir), "number of files in output directory");
    }

    // ---------------------------------------------------------------------
    // Validation
    // ---------------------------------------------------------------------

    private static void missingInputIsRejected(Path tempDir) {
        Path missing = tempDir.resolve("does-not-exist.bin");
        Path outputDir = tempDir.resolve("chunks");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new FileChunker().chunk(missing, outputDir));

        checkContains(e.getMessage(), "does not exist");
        check(!Files.exists(outputDir), "output directory should not be created when the input is invalid");
    }

    private static void directoryAsInputIsRejected(Path tempDir) throws IOException {
        Path directory = Files.createDirectory(tempDir.resolve("some-dir"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new FileChunker().chunk(directory, tempDir.resolve("chunks")));

        checkContains(e.getMessage(), "is a directory");
    }

    private static void fileAsOutputDirIsRejected(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(10));
        byte[] existingContent = "I am a file, not a directory".getBytes(StandardCharsets.UTF_8);
        Path existingFile = Files.write(tempDir.resolve("not-a-dir.txt"), existingContent);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new FileChunker().chunk(input, existingFile));

        checkContains(e.getMessage(), "not a directory");
        checkBytes(existingContent, existingFile);
    }

    private static void outputDirBelowFileIsRejected(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(10));
        Path existingFile = Files.write(tempDir.resolve("file.txt"), randomBytes(5));
        // A directory cannot be created inside a regular file.
        Path impossibleDir = existingFile.resolve("chunks");

        expectThrows(IOException.class, () -> new FileChunker().chunk(input, impossibleDir));

        check(Files.isRegularFile(existingFile), "existing file should still be a regular file");
    }

    private static void nullArgumentsAreRejected(Path tempDir) throws IOException {
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(10));
        Path outputDir = tempDir.resolve("chunks");

        expectThrows(NullPointerException.class, () -> new FileChunker().chunk(null, outputDir));
        expectThrows(NullPointerException.class, () -> new FileChunker().chunk(input, null));
    }

    private static void invalidChunkSizesAreRejected(Path tempDir) {
        expectThrows(IllegalArgumentException.class, () -> new FileChunker(0));
        expectThrows(IllegalArgumentException.class, () -> new FileChunker(-1));
    }

    // ---------------------------------------------------------------------
    // Failure cleanup
    // ---------------------------------------------------------------------

    private static void ioFailureCleansUpOnlyOwnChunks(Path tempDir) throws IOException {
        // A 10-byte file with chunk size 3 needs chunk_0 .. chunk_3.
        // We plant an existing chunk_2, so the chunker writes chunk_0 and chunk_1
        // successfully and then hits a real I/O error (FileAlreadyExistsException) on chunk_2.
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(10));
        Path outputDir = Files.createDirectory(tempDir.resolve("chunks"));
        byte[] plantedContent = "pre-existing chunk".getBytes(StandardCharsets.UTF_8);
        Path plantedChunk = Files.write(outputDir.resolve("chunk_2"), plantedContent);

        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class,
                () -> new FileChunker(3).chunk(input, outputDir));

        // The original exception is preserved and names the file that caused the failure.
        checkEquals(plantedChunk.toString(), e.getFile(), "file named in the exception");
        checkEquals(0, e.getSuppressed().length, "number of suppressed cleanup errors");

        // The chunks created by this call were removed...
        check(!Files.exists(outputDir.resolve("chunk_0")), "chunk_0 (created by this call) should be deleted");
        check(!Files.exists(outputDir.resolve("chunk_1")), "chunk_1 (created by this call) should be deleted");
        check(!Files.exists(outputDir.resolve("chunk_3")), "chunk_3 should never have been created");
        // ...but the file that existed before was left alone.
        checkBytes(plantedContent, plantedChunk);
        checkEquals(1, countFiles(outputDir), "number of files in output directory");
    }

    // ---------------------------------------------------------------------
    // Tiny test framework
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
            tempDir = Files.createTempDirectory("filechunker-test-");
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

    /** Checks the returned chunk list, the chunk file names and sizes, and that no extra files exist. */
    private static void checkChunkSizes(List<Path> chunks, Path outputDir, int... expectedSizes) throws IOException {
        checkEquals(expectedSizes.length, chunks.size(), "number of chunks returned");
        for (int i = 0; i < expectedSizes.length; i++) {
            checkEquals(outputDir.resolve("chunk_" + i), chunks.get(i), "path of chunk " + i);
            checkEquals((long) expectedSizes[i], Files.size(chunks.get(i)), "size of chunk_" + i);
        }
        checkEquals(expectedSizes.length, countFiles(outputDir), "number of files in output directory");
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
    // File helpers
    // ---------------------------------------------------------------------

    /** Returns deterministic pseudo-random bytes (same seed every run, so failures are reproducible). */
    private static byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        new Random(12345).nextBytes(data);
        return data;
    }

    private static int countFiles(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return (int) entries.count();
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
