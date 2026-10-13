package com.distributedfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Plain-Java tests for DistributedFsDemo (no JUnit or other libraries).
 *
 * <p>Two modes:
 * <ul>
 *   <li>Normal:  {@code java -cp "out;out-test" com.distributedfs.DistributedFsDemoTest}</li>
 *   <li>Stress:  {@code java -Xmx16m -cp "out;out-test" com.distributedfs.DistributedFsDemoTest --stress}
 *       runs only the 64 MiB test with a 16 MB heap.</li>
 * </ul>
 *
 * <p>Every test calls {@code run(...)} directly, with a "tmp-root" folder inside its own temporary
 * directory standing in for the system temp folder, and with captured output. No test calls
 * System.exit. Node failures and corrupted data are simulated through the package-private
 * {@code createNode} hook. The program exits with code 1 if any test fails.
 */
public class DistributedFsDemoTest {

    private static final int MIB = 1024 * 1024;

    /** SHA-256 of zero bytes, a published reference value. */
    private static final String SHA256_OF_EMPTY =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stressMode = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stressMode) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.DistributedFsDemoTest [--stress]");
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
        // Command-line arguments
        runTest("no arguments shows usage and creates nothing", DistributedFsDemoTest::noArguments);
        runTest("too many paths or an unknown option shows usage and creates nothing",
                DistributedFsDemoTest::tooManyOrUnknownArguments);
        runTest("a blank or invalid path shows usage and creates nothing", DistributedFsDemoTest::blankOrInvalidPath);

        // Successful runs
        runTest("a multi-chunk file is reconstructed and verified", DistributedFsDemoTest::multiChunkSuccess);
        runTest("an empty file works (0 chunks)", DistributedFsDemoTest::emptyFile);
        runTest("binary data is reconstructed exactly", DistributedFsDemoTest::binaryData);
        runTest("chunk order is kept beyond chunk_9 (12 chunks)", DistributedFsDemoTest::orderBeyondChunk9);
        runTest("--keep keeps the workspace with the documented layout", DistributedFsDemoTest::keepPreservesWorkspace);

        // Safety
        runTest("the input file is unchanged", DistributedFsDemoTest::inputIsUnchanged);
        runTest("existing files and folders next to the workspace are untouched",
                DistributedFsDemoTest::existingFilesUntouched);
        runTest("a missing input file fails before any workspace is created", DistributedFsDemoTest::missingInput);
        runTest("a directory as input fails before any workspace is created", DistributedFsDemoTest::directoryInput);
        runTest("a missing temp folder fails and creates nothing", DistributedFsDemoTest::missingTempRoot);
        runTest("cleanup removes a junction or link without touching its target",
                DistributedFsDemoTest::cleanupDoesNotFollowLinks);

        // Failures
        runTest("all nodes failing ends in FAILURE and the workspace is deleted", DistributedFsDemoTest::allNodesFail);
        runTest("all nodes failing with --keep keeps the workspace", DistributedFsDemoTest::allNodesFailWithKeep);
        runTest("one failing node is bypassed by the cluster's fallback", DistributedFsDemoTest::oneNodeFails);
        runTest("corrupted data on retrieval is caught as a checksum mismatch",
                DistributedFsDemoTest::corruptedRetrievalIsDetected);
    }

    private static void runStressTest() {
        long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
        System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
        if (maxHeapMib >= 64) {
            System.out.println("WARNING: the heap can hold the whole 64 MiB file, so this run does not prove"
                    + " streaming. Run with -Xmx16m.");
        }
        runTest("a 64 MiB file goes through the whole demo with a small heap", DistributedFsDemoTest::largeFile);
    }

    // ---------------------------------------------------------------------
    // Command-line arguments
    // ---------------------------------------------------------------------

    private static void noArguments(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot);

        checkEquals(2, result.exitCode, "exit code");
        checkContains(result.err, "Usage:");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder");
    }

    private static void tooManyOrUnknownArguments(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(100, 1));
        Path other = Files.write(tempDir.resolve("other.bin"), randomBytes(100, 2));

        DemoRun twoPaths = runDemo(new DistributedFsDemo(), tmpRoot, input.toString(), other.toString());
        checkEquals(2, twoPaths.exitCode, "exit code for two input paths");
        checkContains(twoPaths.err, "more than one");

        DemoRun misspelled = runDemo(new DistributedFsDemo(), tmpRoot, input.toString(), "--keepp");
        checkEquals(2, misspelled.exitCode, "exit code for --keepp");
        checkContains(misspelled.err, "unknown option: --keepp");

        DemoRun optionOnly = runDemo(new DistributedFsDemo(), tmpRoot, "--verbose");
        checkEquals(2, optionOnly.exitCode, "exit code for --verbose");
        checkContains(optionOnly.err, "unknown option: --verbose");

        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder");
    }

    private static void blankOrInvalidPath(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));

        for (String blank : List.of("", "   ")) {
            DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, blank);
            checkEquals(2, result.exitCode, "exit code for a blank path");
            checkContains(result.err, "no input file given");
        }
        // '<' is not allowed in Windows file names; on other systems it is a valid character.
        if (isWindows()) {
            DemoRun invalid = runDemo(new DistributedFsDemo(), tmpRoot, "bad<name.bin");
            checkEquals(2, invalid.exitCode, "exit code for an invalid path");
            checkContains(invalid.err, "invalid path");
        }

        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder");
    }

    // ---------------------------------------------------------------------
    // Successful runs
    // ---------------------------------------------------------------------

    private static void multiChunkSuccess(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = randomBytes(2 * MIB + MIB / 2 + 123, 1);
        Path input = Files.write(tempDir.resolve("input.bin"), data);
        String expectedSha256 = sha256Of(data); // calculated here, independently of the demo

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkEquals("", result.err, "error output");
        checkContains(result.out, "simulated cluster of 3 storage nodes");
        checkContains(result.out, "there is no replication");
        checkContains(result.out, "Split into 3 chunk(s)");
        checkContains(result.out, "node 0: 1, node 1: 1, node 2: 1");
        checkContains(result.out, "Original SHA-256:      " + expectedSha256);
        checkContains(result.out, "Reconstructed SHA-256: " + expectedSha256);
        checkContains(result.out, "RESULT: SUCCESS");
        checkContains(result.out, "Workspace deleted.");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder (workspace deleted)");
    }

    private static void emptyFile(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        Path input = Files.write(tempDir.resolve("empty.bin"), new byte[0]);

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkContains(result.out, "Split into 0 chunk(s)");
        checkContains(result.out, "node 0: 0, node 1: 0, node 2: 0");
        checkContains(result.out, "Original SHA-256:      " + SHA256_OF_EMPTY);
        checkContains(result.out, "Reconstructed SHA-256: " + SHA256_OF_EMPTY);
        checkContains(result.out, "RESULT: SUCCESS");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder");
    }

    private static void binaryData(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = new byte[256 + 6];
        for (int i = 0; i < 256; i++) {
            data[i] = (byte) i;
        }
        byte[] tricky = {'\r', '\n', 0x00, (byte) 0xEF, (byte) 0xBB, (byte) 0xBF}; // CR/LF, NUL, UTF-8 BOM
        System.arraycopy(tricky, 0, data, 256, tricky.length);
        Path input = Files.write(tempDir.resolve("binary.bin"), data);

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkContains(result.out, "Reconstructed SHA-256: " + sha256Of(data));
        checkContains(result.out, "RESULT: SUCCESS");
    }

    private static void orderBeyondChunk9(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        // 12 chunks: chunk_0 ... chunk_11. A folder listing would sort chunk_10 and chunk_11
        // before chunk_2, so this only passes if the demo keeps the order explicitly.
        byte[] data = randomBytes(11 * MIB + 1000, 3);
        Path input = Files.write(tempDir.resolve("twelve-chunks.bin"), data);

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkContains(result.out, "Split into 12 chunk(s)");
        checkContains(result.out, "node 0: 4, node 1: 4, node 2: 4");
        checkContains(result.out, "Reconstructed SHA-256: " + sha256Of(data));
        checkContains(result.out, "RESULT: SUCCESS");
    }

    private static void keepPreservesWorkspace(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = randomBytes(2 * MIB + 77, 4);
        Path input = Files.write(tempDir.resolve("photo.jpg"), data);

        // --keep before the path, to show the position does not matter.
        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, "--keep", input.toString());

        checkEquals(0, result.exitCode, "exit code");
        List<String> rootEntries = listNames(tmpRoot);
        checkEquals(1, rootEntries.size(), "number of entries in the temp folder");
        check(rootEntries.get(0).startsWith("distributedfs-demo-"), "workspace name: " + rootEntries.get(0));
        Path workspace = tmpRoot.resolve(rootEntries.get(0));
        checkContains(result.out, "Workspace kept for inspection: " + workspace);

        checkEquals(List.of("chunks", "nodes", "reconstructed", "retrieved"), listNames(workspace), "workspace layout");
        checkEquals(List.of("chunk_0", "chunk_1", "chunk_2"), listNames(workspace.resolve("chunks")), "chunks");
        checkEquals(List.of("node-0", "node-1", "node-2"), listNames(workspace.resolve("nodes")), "node folders");
        for (int i = 0; i < 3; i++) {
            checkEquals(List.of("chunk_" + i), listNames(workspace.resolve("nodes").resolve("node-" + i)),
                    "chunks on node " + i);
        }
        checkEquals(List.of("chunk_0", "chunk_1", "chunk_2"), listNames(workspace.resolve("retrieved")), "retrieved");
        Path rebuilt = workspace.resolve("reconstructed").resolve("photo.jpg");
        checkEquals(-1L, Files.mismatch(input, rebuilt), "position of first differing byte (-1 = identical)");
    }

    // ---------------------------------------------------------------------
    // Safety
    // ---------------------------------------------------------------------

    private static void inputIsUnchanged(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = randomBytes(MIB + 5, 5);
        Path input = Files.write(tempDir.resolve("input.bin"), data);
        FileTime originalTime = FileTime.fromMillis(1_000_000_000_000L);
        Files.setLastModifiedTime(input, originalTime);

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkBytes(data, input);
        checkEquals(originalTime, Files.getLastModifiedTime(input), "input file last-modified time");
    }

    private static void existingFilesUntouched(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        // Things that were in the temp folder before the run, including a folder that looks like
        // a demo workspace. It must be neither reused nor deleted.
        byte[] notes = "existing notes".getBytes(StandardCharsets.UTF_8);
        byte[] lookalikeData = "existing data".getBytes(StandardCharsets.UTF_8);
        Files.write(tmpRoot.resolve("notes.txt"), notes);
        Path lookalike = Files.createDirectory(tmpRoot.resolve("distributedfs-demo-existing"));
        Files.write(lookalike.resolve("data.bin"), lookalikeData);
        List<String> before = listTree(tmpRoot);
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(MIB + 9, 6));

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkEquals(before, listTree(tmpRoot), "files and folders in the temp folder");
        checkBytes(notes, tmpRoot.resolve("notes.txt"));
        checkBytes(lookalikeData, lookalike.resolve("data.bin"));
    }

    private static void missingInput(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, tempDir.resolve("missing.bin").toString());

        checkEquals(1, result.exitCode, "exit code");
        checkContains(result.err, "does not exist");
        checkContains(result.out, "RESULT: FAILURE");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder (no workspace created)");
    }

    private static void directoryInput(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        Path directory = Files.createDirectory(tempDir.resolve("some-dir"));

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, directory.toString());

        checkEquals(1, result.exitCode, "exit code");
        checkContains(result.err, "is a directory");
        checkContains(result.out, "RESULT: FAILURE");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder (no workspace created)");
    }

    private static void missingTempRoot(Path tempDir) throws IOException {
        Path missingRoot = tempDir.resolve("no-such-temp-folder");
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(100, 7));

        DemoRun result = runDemo(new DistributedFsDemo(), missingRoot, input.toString());

        checkEquals(1, result.exitCode, "exit code");
        checkContains(result.out, "RESULT: FAILURE - could not create a workspace");
        check(!Files.exists(missingRoot), "the missing temp folder must not be created");
    }

    private static void cleanupDoesNotFollowLinks(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        // A folder outside the workspace, with a file that must survive the cleanup.
        Path outside = Files.createDirectory(tempDir.resolve("outside"));
        byte[] sentinelData = "must survive the cleanup".getBytes(StandardCharsets.UTF_8);
        Path sentinel = Files.write(outside.resolve("keep-me.txt"), sentinelData);
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(MIB + 3, 8));
        boolean[] linkPlanted = {false};
        boolean[] linkWasJunctionOrSymlink = {false};
        boolean[] targetVisibleThroughLink = {false};

        // While the demo builds node 0, plant a link inside the workspace pointing at "outside".
        // On Windows this is a directory junction (mklink /J, no admin rights needed); elsewhere a
        // symbolic link.
        DistributedFsDemo demo = new DistributedFsDemo() {
            @Override
            StorageNode createNode(Path dir) throws IOException {
                if (dir.getFileName().toString().equals("node-0")) {
                    Path workspace = dir.getParent().getParent();
                    Path link = workspace.resolve("link-to-outside");
                    createDirectoryLink(link, outside);
                    linkPlanted[0] = true;
                    BasicFileAttributes attributes =
                            Files.readAttributes(link, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    linkWasJunctionOrSymlink[0] = attributes.isOther() || attributes.isSymbolicLink();
                    targetVisibleThroughLink[0] = Files.exists(link.resolve("keep-me.txt"));
                }
                return super.createNode(dir);
            }
        };

        DemoRun result = runDemo(demo, tmpRoot, input.toString());

        // The link really existed during the run, so the cleanup really had to deal with it.
        check(linkPlanted[0], "the link was not created, so this test proves nothing");
        check(linkWasJunctionOrSymlink[0], "the planted entry was not a junction or symbolic link");
        check(targetVisibleThroughLink[0], "the target's file should have been visible through the link");
        checkEquals(0, result.exitCode, "exit code");
        checkContains(result.out, "Workspace deleted.");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder (workspace and link removed)");
        // The target, outside the workspace, is untouched.
        check(Files.isDirectory(outside), "the link's target folder must still exist");
        checkBytes(sentinelData, sentinel);
    }

    // ---------------------------------------------------------------------
    // Failures
    // ---------------------------------------------------------------------

    private static void allNodesFail(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = randomBytes(MIB + 11, 9);
        Path input = Files.write(tempDir.resolve("input.bin"), data);

        DemoRun result = runDemo(demoWithFailingNodes("node-0", "node-1", "node-2"), tmpRoot, input.toString());

        checkEquals(1, result.exitCode, "exit code");
        checkContains(result.err, "No node could store chunk");
        checkContains(result.err, "simulated disk error");
        checkContains(result.out, "RESULT: FAILURE");
        check(!result.out.contains("RESULT: SUCCESS"), "a failed run must never report success");
        checkContains(result.out, "Workspace deleted.");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder (workspace deleted)");
        checkBytes(data, input);
    }

    private static void allNodesFailWithKeep(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        Path input = Files.write(tempDir.resolve("input.bin"), randomBytes(MIB + 11, 9));

        DemoRun result = runDemo(demoWithFailingNodes("node-0", "node-1", "node-2"), tmpRoot,
                input.toString(), "--keep");

        checkEquals(1, result.exitCode, "exit code");
        checkContains(result.out, "RESULT: FAILURE");
        List<String> rootEntries = listNames(tmpRoot);
        checkEquals(1, rootEntries.size(), "number of entries in the temp folder (workspace kept)");
        Path workspace = tmpRoot.resolve(rootEntries.get(0));
        checkContains(result.out, "Workspace kept for inspection: " + workspace);
        for (int i = 0; i < 3; i++) {
            checkEquals(List.of(), listNames(workspace.resolve("nodes").resolve("node-" + i)),
                    "files on node " + i + " (no chunk and no temp file)");
        }
    }

    private static void oneNodeFails(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = randomBytes(2 * MIB + 500, 10);
        Path input = Files.write(tempDir.resolve("input.bin"), data);

        DemoRun result = runDemo(demoWithFailingNodes("node-0"), tmpRoot, input.toString());

        // chunk_0: node 0 fails, node 1 takes it; chunk_1: node 2; chunk_2: node 0 fails, node 1.
        checkEquals(0, result.exitCode, "exit code");
        checkContains(result.out, "node 0: 0, node 1: 2, node 2: 1");
        checkContains(result.out, "Reconstructed SHA-256: " + sha256Of(data));
        checkContains(result.out, "RESULT: SUCCESS");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder");
    }

    private static void corruptedRetrievalIsDetected(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        byte[] data = randomBytes(2 * MIB + 500, 11);
        Path input = Files.write(tempDir.resolve("input.bin"), data);
        String originalSha256 = sha256Of(data);

        // Every node flips one bit in the first byte of a chunk when it is read back out.
        DistributedFsDemo demo = new DistributedFsDemo() {
            @Override
            StorageNode createNode(Path dir) throws IOException {
                Path nodeDir = dir.toAbsolutePath().normalize();
                return new StorageNode(dir) {
                    @Override
                    InputStream openForReading(Path file) throws IOException {
                        byte[] bytes = Files.readAllBytes(file);
                        boolean isRetrieval = file.toAbsolutePath().normalize().startsWith(nodeDir);
                        if (isRetrieval && bytes.length > 0) {
                            bytes[0] ^= 0x01;
                        }
                        return new ByteArrayInputStream(bytes);
                    }
                };
            }
        };

        DemoRun result = runDemo(demo, tmpRoot, input.toString());

        checkEquals(1, result.exitCode, "exit code");
        checkContains(result.out, "Original SHA-256:      " + originalSha256);
        check(!result.out.contains("Reconstructed SHA-256: " + originalSha256),
                "the reconstructed checksum should differ from the original");
        checkContains(result.out, "RESULT: FAILURE - checksum mismatch");
        check(!result.out.contains("RESULT: SUCCESS"), "a mismatch must never report success");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder (workspace deleted)");
    }

    // ---------------------------------------------------------------------
    // Stress test (run separately with --stress and -Xmx16m)
    // ---------------------------------------------------------------------

    private static void largeFile(Path tempDir) throws IOException {
        Path tmpRoot = Files.createDirectory(tempDir.resolve("tmp-root"));
        // Write 64 MiB using one reusable 1 MiB buffer, so the test itself stays small in memory.
        byte[] buffer = new byte[MIB];
        Random random = new Random(12345);
        Path input = tempDir.resolve("large.bin");
        try (OutputStream out = Files.newOutputStream(input)) {
            for (int i = 0; i < 64; i++) {
                random.nextBytes(buffer);
                out.write(buffer);
            }
        }

        DemoRun result = runDemo(new DistributedFsDemo(), tmpRoot, input.toString());

        checkEquals(0, result.exitCode, "exit code");
        checkContains(result.out, "Split into 64 chunk(s)");
        checkContains(result.out, "RESULT: SUCCESS");
        checkEquals(List.of(), listNames(tmpRoot), "entries in the temp folder");
    }

    // ---------------------------------------------------------------------
    // Running the demo, failing nodes, and links
    // ---------------------------------------------------------------------

    /** The exit code and everything the demo printed. */
    private record DemoRun(int exitCode, String out, String err) {
    }

    private static DemoRun runDemo(DistributedFsDemo demo, Path tmpRoot, String... args) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        int exitCode = demo.run(args, tmpRoot, out, err);
        return new DemoRun(exitCode, outBytes.toString(StandardCharsets.UTF_8), errBytes.toString(StandardCharsets.UTF_8));
    }

    /** A demo whose nodes with the given folder names fail after 10 bytes of every read. */
    private static DistributedFsDemo demoWithFailingNodes(String... failingNodeNames) {
        List<String> failing = List.of(failingNodeNames);
        return new DistributedFsDemo() {
            @Override
            StorageNode createNode(Path dir) throws IOException {
                String name = dir.getFileName().toString();
                if (!failing.contains(name)) {
                    return super.createNode(dir);
                }
                IOException failure = new IOException("simulated disk error on " + name);
                return new StorageNode(dir) {
                    @Override
                    InputStream openForReading(Path file) throws IOException {
                        return failAfter(Files.readAllBytes(file), 10, failure);
                    }
                };
            }
        };
    }

    /** A stream that delivers {@code bytesBeforeFailure} bytes, then throws {@code failure} on every read. */
    private static InputStream failAfter(byte[] data, int bytesBeforeFailure, IOException failure) {
        return new InputStream() {
            private int position = 0;

            @Override
            public int read() throws IOException {
                if (position >= bytesBeforeFailure) {
                    throw failure;
                }
                if (position >= data.length) {
                    return -1;
                }
                return data[position++] & 0xFF;
            }
        };
    }

    /** Creates a directory junction on Windows (no admin rights needed), or a symbolic link elsewhere. */
    private static void createDirectoryLink(Path link, Path target) throws IOException {
        if (!isWindows()) {
            Files.createSymbolicLink(link, target);
            return;
        }
        Process process = new ProcessBuilder("cmd.exe", "/c", "mklink", "/J", link.toString(), target.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while creating a junction", e);
        }
        if (exitCode != 0) {
            throw new IOException("mklink /J failed with exit code " + exitCode + ": " + output.trim());
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").startsWith("Windows");
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as FileChunkerTest)
    // ---------------------------------------------------------------------

    /** A test body that receives a fresh temporary directory. */
    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("distributedfsdemo-test-");
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
                    // The demo's own cleanup is link-safe, so use it for the test folders too.
                    DistributedFsDemo.deleteWorkspace(tempDir);
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
            throw new AssertionError("expected text containing \"" + expectedPart + "\" but was:\n" + text);
        }
    }

    private static void checkBytes(byte[] expected, Path file) throws IOException {
        if (!Arrays.equals(expected, Files.readAllBytes(file))) {
            throw new AssertionError("unexpected content in " + file.getFileName());
        }
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

    /** SHA-256 of a byte array, calculated directly with MessageDigest (independent of the demo). */
    private static String sha256Of(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Names of the entries in a directory, sorted, so tests can compare them with a fixed list. */
    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    /** Every file and folder below a directory, as sorted relative paths, to spot any change. */
    private static List<String> listTree(Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            return paths.map(path -> directory.relativize(path).toString()).sorted(Comparator.naturalOrder()).toList();
        }
    }
}
