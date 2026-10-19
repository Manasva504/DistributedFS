package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Crash tests for startup recovery (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.CrashRecoveryTest [--stress]}
 *
 * <p>Each test starts {@link CrashUploadMain} in a separate JVM, which stores one complete file and
 * then halts during a second upload at a chosen stage, exactly as a killed server would stop. The
 * test checks that the crash left the expected state, then starts a real server on the same data
 * directory and checks what startup recovery made of it: the complete file always reads back
 * identically, an incomplete upload leaves nothing behind, and a committed one is kept.
 *
 * <p>{@code --stress} halts a 64 MiB upload after 41 of its 1 MiB chunks; run it with
 * {@code -Xmx16m} (the child JVM gets the same limit). The program exits with code 1 if any test fails.
 */
public class CrashRecoveryTest {

    private static final int CHUNK_SIZE = 4;
    private static final int UPLOAD_BYTES = 18; // 5 chunks of up to 4 bytes
    private static final int UPLOAD_CHUNKS = 5;
    private static final int MIB = 1024 * 1024;
    private static final String F = CrashUploadMain.UPLOAD_ID;
    private static final String G = CrashUploadMain.OTHER_ID;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stress = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stress) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.CrashRecoveryTest [--stress]");
            System.exit(2);
            return;
        }
        if (stress) {
            long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
            System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
            if (maxHeapMib >= 64) {
                System.out.println("WARNING: the heap can hold the whole 64 MiB upload. Run with -Xmx16m.");
            }
            runTest("STRESS: a 64 MiB upload killed after 41 chunks is recovered with a small heap",
                    CrashRecoveryTest::stressLargeUpload);
            finish();
            return;
        }

        runTest("killed while writing the pending marker: no marker, the temp file is swept",
                CrashRecoveryTest::killedWritingMarker);
        runTest("killed after the marker, before any chunk: rolled back", CrashRecoveryTest::killedAfterMarker);
        runTest("killed after 3 of 5 chunks: exactly those chunks are deleted", CrashRecoveryTest::killedAfterChunks);
        runTest("killed while a chunk was being stored: its node temp file is swept",
                CrashRecoveryTest::killedMidChunk);
        runTest("killed with the manifest's temp file written but not renamed: rolled back",
                CrashRecoveryTest::killedWritingManifest);
        runTest("killed after the manifest, before the marker delete: committed, only the marker is removed",
                CrashRecoveryTest::killedAfterManifest);
        runTest("killed during a rollback: the remaining chunks and the marker are removed",
                CrashRecoveryTest::killedDuringRollback);
        runTest("recovery itself killed mid-delete: the next start finishes, the one after does nothing",
                CrashRecoveryTest::recoveryKilledMidDelete);
        finish();
    }

    private static void finish() {
        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    private static void killedWritingMarker(Path tempDir) throws Exception {
        Path data = crash(tempDir, "pending-temp");
        check(names(data.resolve("manifests")).stream().anyMatch(n -> n.startsWith(F + "-") && n.endsWith(".tmp")),
                "the crash left the marker's temp file");
        check(!Files.exists(marker(data)), "the crash left no marker");

        String printed = restartAndCheckIncomplete(data);
        checkContains(printed, "Recovery: 0 interrupted upload(s)");
    }

    private static void killedAfterMarker(Path tempDir) throws Exception {
        Path data = crash(tempDir, "after-marker");
        check(Files.exists(marker(data)), "the crash left the marker");
        checkEquals(0, chunksOf(data, F).size(), "chunks stored before the crash");

        checkContains(restartAndCheckIncomplete(data), "1 rolled back (0 chunk(s) deleted)");
    }

    private static void killedAfterChunks(Path tempDir) throws Exception {
        Path data = crash(tempDir, "after-chunk:2");
        checkEquals(List.of(F + "_0", F + "_1", F + "_2"), chunksOf(data, F), "chunks stored before the crash");

        checkContains(restartAndCheckIncomplete(data), "1 rolled back (3 chunk(s) deleted)");
    }

    private static void killedMidChunk(Path tempDir) throws Exception {
        Path data = crash(tempDir, "mid-chunk:3");
        checkEquals(List.of(F + "_0", F + "_1", F + "_2"), chunksOf(data, F), "chunks stored before the crash");
        checkEquals(1, nodeTempFiles(data, F + "_3").size(), "the interrupted store's temp file");

        checkContains(restartAndCheckIncomplete(data), "1 rolled back (3 chunk(s) deleted)");
    }

    private static void killedWritingManifest(Path tempDir) throws Exception {
        Path data = crash(tempDir, "manifest-temp");
        checkEquals(UPLOAD_CHUNKS, chunksOf(data, F).size(), "every chunk was stored before the crash");
        check(names(data.resolve("manifests")).stream().anyMatch(n -> n.startsWith(F + "-") && n.endsWith(".tmp")),
                "the crash left the manifest's temp file");
        check(!Files.exists(data.resolve("manifests").resolve(F + ".manifest")), "no manifest");

        checkContains(restartAndCheckIncomplete(data), "1 rolled back (5 chunk(s) deleted)");
    }

    private static void killedAfterManifest(Path tempDir) throws Exception {
        Path data = crash(tempDir, "after-manifest");
        check(Files.exists(data.resolve("manifests").resolve(F + ".manifest")), "the manifest is in place");
        check(Files.exists(marker(data)), "the marker is still there");

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        DistributedFsServer server = start(data, outBytes);
        try {
            checkOtherFileIntact(server, tempDir);
            FileManifest manifest = server.manifests().read(F).orElseThrow();
            check(Arrays.equals(pattern(UPLOAD_BYTES, 2), readBack(server, manifest, tempDir)),
                    "the committed upload reads back identically");
            check(!Files.exists(marker(data)), "the marker was deleted");
            checkContains(outBytes.toString(StandardCharsets.UTF_8), "1 committed, 0 rolled back (0 chunk(s) deleted)");
            checkNoLeftovers(data);
        } finally {
            server.stop();
        }
    }

    private static void killedDuringRollback(Path tempDir) throws Exception {
        Path data = crash(tempDir, "mid-rollback");
        check(Files.exists(marker(data)), "the marker is still there");
        checkEquals(List.of(F + "_0", F + "_1", F + "_2", F + "_3"), chunksOf(data, F),
                "the rollback deleted the newest chunk before the crash");

        checkContains(restartAndCheckIncomplete(data), "1 rolled back (4 chunk(s) deleted)");
    }

    private static void recoveryKilledMidDelete(Path tempDir) throws Exception {
        Path data = crash(tempDir, "after-chunk:3");
        checkEquals(4, chunksOf(data, F).size(), "chunks stored before the first crash");
        checkEquals(CrashUploadMain.HALTED, runChild(data, "recover-mid-delete", CHUNK_SIZE, 0, false),
                "the recovery child reached its crash point");
        check(Files.exists(marker(data)), "the marker survived the killed recovery");
        checkEquals(List.of(F + "_0", F + "_1", F + "_2"), chunksOf(data, F),
                "the killed recovery deleted only the highest chunk");

        checkContains(restartAndCheckIncomplete(data), "1 rolled back (3 chunk(s) deleted)");

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        DistributedFsServer server = start(data, outBytes);
        try {
            check(!outBytes.toString(StandardCharsets.UTF_8).contains("Recovery:"), "a third start has nothing to do");
            checkOtherFileIntact(server, tempDir);
        } finally {
            server.stop();
        }
    }

    private static void stressLargeUpload(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer.start(config(data, new ByteArrayOutputStream(), MIB)).stop();
        long started = System.nanoTime();
        checkEquals(CrashUploadMain.HALTED, runChild(data, "after-chunk:40", MIB, 64L * MIB, true),
                "the child reached its crash point");
        checkEquals(41, chunksOf(data, F).size(), "chunks stored before the crash");

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        DistributedFsServer server = DistributedFsServer.start(config(data, outBytes, MIB));
        try {
            checkContains(outBytes.toString(StandardCharsets.UTF_8), "1 rolled back (41 chunk(s) deleted)");
            checkEquals(List.of(), chunksOf(data, F), "the upload's chunks after recovery");
            checkNoLeftovers(data);
            FileManifest other = server.manifests().read(G).orElseThrow();
            check(Arrays.equals(pattern(10, 1), readBack(server, other, tempDir)), "the other file is intact");
        } finally {
            server.stop();
        }
        System.out.println("      crash and recovery took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                + " ms");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** Initializes a data directory, then lets the child crash at {@code stage}. */
    private static Path crash(Path tempDir, String stage) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer.start(config(data, new ByteArrayOutputStream(), CHUNK_SIZE)).stop();
        checkEquals(CrashUploadMain.HALTED, runChild(data, stage, CHUNK_SIZE, UPLOAD_BYTES, false),
                "the child reached the stage " + stage);
        return data;
    }

    /** Restarts on the data directory, checks that the upload left nothing behind, returns stdout. */
    private static String restartAndCheckIncomplete(Path data) throws Exception {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        DistributedFsServer server = start(data, outBytes);
        try {
            checkOtherFileIntact(server, data.getParent());
            check(server.manifests().read(F).isEmpty(), "the incomplete upload has no manifest");
            check(!Files.exists(marker(data)), "the marker was deleted");
            checkEquals(List.of(), chunksOf(data, F), "the incomplete upload's chunks");
            checkNoLeftovers(data);
            String printed = outBytes.toString(StandardCharsets.UTF_8);
            check(!printed.contains("Chunk scan"), "nothing unreferenced remains: " + printed);
            return printed;
        } finally {
            server.stop();
        }
    }

    private static DistributedFsServer start(Path data, ByteArrayOutputStream outBytes) throws IOException {
        return DistributedFsServer.start(config(data, outBytes, CHUNK_SIZE));
    }

    private static DistributedFsServer.Config config(Path data, ByteArrayOutputStream outBytes, int chunkSize) {
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        // A 2-second lock retry, as in production: Windows releases a killed process's lock shortly after it ends.
        return new DistributedFsServer.Config(data, 0, 2, 8, 0, 2_000, out, err, 1024L * MIB, chunkSize);
    }

    /** Runs CrashUploadMain in a new JVM with the same class path and returns its exit code. */
    private static int runChild(Path data, String stage, int chunkSize, long uploadBytes, boolean smallHeap)
            throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        if (smallHeap) {
            command.add("-Xmx16m");
        }
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), CrashUploadMain.class.getName(),
                data.toString(), stage, Integer.toString(chunkSize), Long.toString(uploadBytes)));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = process.getInputStream().readAllBytes();
        if (!process.waitFor(120, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("the child did not finish: " + stage);
        }
        int exitCode = process.exitValue();
        if (exitCode != CrashUploadMain.HALTED) {
            System.out.println("      child output: " + new String(output, StandardCharsets.UTF_8).trim());
        }
        return exitCode;
    }

    private static void checkOtherFileIntact(DistributedFsServer server, Path scratch) throws IOException {
        FileManifest other = server.manifests().read(G).orElseThrow();
        check(Arrays.equals(pattern(10, 1), readBack(server, other, scratch)), "the other, complete file reads back");
        check(!Files.exists(server.dataRoot().resolve("manifests").resolve(G + ".pending")), "the other file has no marker");
    }

    /** No temp files in the manifest folder or on any node, and no workspace in tmp. */
    private static void checkNoLeftovers(Path data) throws IOException {
        for (String name : names(data.resolve("manifests"))) {
            check(name.endsWith(".manifest"), "only manifests remain in the manifest folder, found " + name);
        }
        for (int node = 0; node < 3; node++) {
            for (String name : names(data.resolve("nodes").resolve("node-" + node))) {
                check(!name.endsWith(".tmp"), "no temp file remains on node " + node + ", found " + name);
            }
        }
        checkEquals(List.of(), names(data.resolve("tmp")), "the tmp folder");
    }

    private static byte[] readBack(DistributedFsServer server, FileManifest manifest, Path scratch) throws IOException {
        Path dir = Files.createTempDirectory(scratch, "readback-");
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        for (int i = 0; i < manifest.chunkCount(); i++) {
            Path copy = dir.resolve("chunk-" + i);
            server.cluster().retrieveChunk(FileIds.chunkId(manifest.fileId(), i), copy);
            data.write(Files.readAllBytes(copy));
        }
        return data.toByteArray();
    }

    private static byte[] pattern(int size, int seed) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = CrashUploadMain.patternByte(i, seed);
        }
        return data;
    }

    private static Path marker(Path data) {
        return data.resolve("manifests").resolve(F + ManifestStore.PENDING_SUFFIX);
    }

    /** The canonical chunks of {@code fileId} on all nodes, sorted by index. */
    private static List<String> chunksOf(Path data, String fileId) throws IOException {
        Pattern chunk = Pattern.compile(Pattern.quote(fileId) + "_(0|[1-9][0-9]*)");
        List<String> chunks = new ArrayList<>();
        for (int node = 0; node < 3; node++) {
            for (String name : names(data.resolve("nodes").resolve("node-" + node))) {
                if (chunk.matcher(name).matches()) {
                    chunks.add(name);
                }
            }
        }
        chunks.sort((a, b) -> Integer.compare(index(a), index(b)));
        return chunks;
    }

    private static int index(String chunkId) {
        return Integer.parseInt(chunkId.substring(chunkId.lastIndexOf('_') + 1));
    }

    private static List<String> nodeTempFiles(Path data, String chunkId) throws IOException {
        List<String> temps = new ArrayList<>();
        for (int node = 0; node < 3; node++) {
            for (String name : names(data.resolve("nodes").resolve("node-" + node))) {
                if (name.startsWith(chunkId + "-") && name.endsWith(".tmp")) {
                    temps.add(name);
                }
            }
        }
        return temps;
    }

    private static List<String> names(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("crashrecovery-test-");
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
            throw new AssertionError("expected to find <" + expectedPart + "> in <" + text + ">");
        }
    }

    /** Test cleanup that never follows a junction: a junction is deleted as a link. */
    private static void deleteRecursively(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) throws IOException {
                if (!dir.equals(directory) && attributes.isOther()) {
                    Files.delete(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) {
                    throw error;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
