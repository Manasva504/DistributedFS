package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Plain-Java tests for StorageNode (no JUnit or other libraries).
 *
 * <p>Two modes:
 * <ul>
 *   <li>Normal:  {@code java -cp "out;out-test" com.distributedfs.StorageNodeTest}</li>
 *   <li>Stress:  {@code java -Xmx16m -cp "out;out-test" com.distributedfs.StorageNodeTest --stress}
 *       runs only the 64 MiB test. With a 16 MB heap it would crash with OutOfMemoryError
 *       if the node ever loaded a whole chunk into memory.</li>
 * </ul>
 *
 * <p>Each test gets its own temporary directory, which is deleted afterwards. Nodes, source
 * files, destinations and path-traversal targets all live inside that directory.
 * The program exits with code 1 if any test fails.
 */
public class StorageNodeTest {

    private static final int MIB = 1024 * 1024;

    /** A 64-character hex string, the shape of a SHA-256 checksum (this one is SHA-256 of "abc"). */
    private static final String HEX_64 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    private static int passed = 0;
    private static int failed = 0;
    private static int skipped = 0;

    public static void main(String[] args) {
        boolean stressMode = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stressMode) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.StorageNodeTest [--stress]");
            System.exit(2);
            return;
        }

        if (stressMode) {
            runStressTest();
        } else {
            runNormalTests();
        }

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed" + (skipped > 0 ? ", " + skipped + " skipped" : ""));
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void runNormalTests() {
        // Startup
        runTest("a missing nested storage directory is created", StorageNodeTest::missingStorageDirIsCreated);
        runTest("an existing directory and its chunks are reused (simulated restart)",
                StorageNodeTest::existingStorageDirIsReused);
        runTest("a storage path that is an existing file is rejected", StorageNodeTest::fileAsStorageDirIsRejected);
        runTest("a null storage directory is rejected", StorageNodeTest::nullStorageDirIsRejected);

        // Store and retrieve
        runTest("a stored chunk is retrieved byte-for-byte identical", StorageNodeTest::storeAndRetrieve);
        runTest("an empty file is stored and retrieved as an empty file", StorageNodeTest::emptyChunk);
        runTest("binary data (all 256 byte values, CR/LF, invalid UTF-8) is preserved",
                StorageNodeTest::binaryDataIsPreserved);
        runTest("several chunks side by side each return their own content", StorageNodeTest::severalChunks);
        runTest("storing copies: the source file is unchanged", StorageNodeTest::sourceIsUnchanged);
        runTest("retrieving copies: the chunk stays and two retrievals match",
                StorageNodeTest::retrieveLeavesChunkInPlace);
        runTest("hasChunk is true after a store and false for a chunk never stored", StorageNodeTest::hasChunkAnswers);
        runTest("two nodes are isolated from each other", StorageNodeTest::nodesAreIsolated);
        runTest("FileChunker -> StorageNode -> FileReconstructor rebuilds the original", StorageNodeTest::endToEnd);

        // Duplicates and never overwriting
        runTest("storing an existing chunk ID fails and leaves the chunk unchanged",
                StorageNodeTest::duplicateIdIsRejected);
        runTest("retrieving onto an existing file fails and leaves it unchanged",
                StorageNodeTest::existingDestinationIsNotOverwritten);
        runTest("retrieving onto an existing directory is rejected", StorageNodeTest::directoryDestinationIsRejected);
        runTest("retrieving onto the storage directory itself is rejected",
                StorageNodeTest::storageDirAsDestinationIsRejected);
        runTest("retrieving into the storage directory is rejected", StorageNodeTest::destinationInsideStorageDirIsRejected);
        runTest("retrieving into a nested folder of the storage directory is rejected",
                StorageNodeTest::destinationInNestedFolderIsRejected);
        runTest("a path with '.' or '..' that leads into the storage directory is rejected",
                StorageNodeTest::dotDotDestinationIsRejected);
        runTest("destinations outside the storage directory still work", StorageNodeTest::outsideDestinationsAreAllowed);
        runTest("a chunk that appears during a store is not replaced",
                StorageNodeTest::chunkAppearingDuringStoreIsNotReplaced);
        runTest("a chunk is not visible under its ID until its store completes",
                StorageNodeTest::chunkIsInvisibleUntilStoreCompletes);

        // Missing chunks and invalid input
        runTest("a missing chunk throws ChunkNotFoundException and creates nothing", StorageNodeTest::missingChunk);
        runTest("invalid source files are rejected and nothing is stored", StorageNodeTest::invalidSourceIsRejected);
        runTest("null arguments are rejected", StorageNodeTest::nullArgumentsAreRejected);
        runTest("invalid chunk IDs are rejected and edge-case valid IDs accepted", StorageNodeTest::chunkIdRules);
        runTest("Windows device names are rejected and look-alikes accepted", StorageNodeTest::windowsDeviceNames);
        runTest("path traversal attempts are rejected and touch nothing", StorageNodeTest::pathTraversalIsRejected);

        // Failures, errors and cleanup
        runTest("a failed store never exposes a partial chunk and leaves no temp file",
                StorageNodeTest::failedStoreLeavesNoPartialChunk);
        runTest("a failed retrieval leaves no partial destination",
                StorageNodeTest::failedRetrievalLeavesNoPartialDestination);
        runTest("a deleted storage directory is an error, not a missing chunk",
                StorageNodeTest::deletedStorageDirIsAnError);
        runTest("a directory in place of a chunk is an error, not a missing chunk",
                StorageNodeTest::unexpectedDirectoryIsAnError);
        runTest("an orphaned temp file is never treated as a chunk", StorageNodeTest::orphanedTempFileIsIgnored);

        // Deleting
        runTest("deleteChunk removes a stored chunk and returns true", StorageNodeTest::deleteRemovesChunk);
        runTest("deleteChunk returns false for a missing chunk and changes nothing",
                StorageNodeTest::deleteMissingChunkReturnsFalse);
        runTest("deleteChunk leaves other chunks and temp files untouched", StorageNodeTest::deleteLeavesOtherEntriesAlone);
        runTest("deleteChunk rejects invalid IDs before touching anything", StorageNodeTest::deleteRejectsInvalidIds);
        runTest("deleteChunk with a deleted storage directory is an error, not a missing chunk",
                StorageNodeTest::deleteWithMissingStorageDirIsAnError);
        runTest("deleteChunk with a file in place of the storage directory is an error",
                StorageNodeTest::deleteWithStorageDirReplacedByFileIsAnError);
        runTest("deleteChunk with a junction in place of the storage directory is an error and changes nothing",
                StorageNodeTest::deleteWithStorageDirReplacedByJunctionIsAnError);
        runTest("deleteChunk refuses a directory under the chunk's ID and leaves it in place",
                StorageNodeTest::deleteRefusesDirectoryEntry);
        runTest("deleteChunk refuses a junction under the chunk's ID and leaves it and its target untouched",
                StorageNodeTest::deleteRefusesJunctionEntry);
        runTest("a failing delete is reported and the chunk stays", StorageNodeTest::injectedDeleteFailurePropagates);
        runTest("a chunk that vanishes just before the delete is reported as absent after a recheck",
                StorageNodeTest::raceChunkVanishedReturnsFalse);
        runTest("a delete that reports 'missing' while the chunk is still there is an error",
                StorageNodeTest::raceInconsistentStateIsAnError);
        runTest("a storage directory that vanishes during the delete is an error, not a missing chunk",
                StorageNodeTest::raceStorageDirVanishedIsAnError);
        runTest("holdsRegularChunkFile answers true, false or IOException without following links",
                StorageNodeTest::holdsRegularChunkFileAnswers);
        runTest("a deleted ID can be stored again, and a new node object can delete existing chunks",
                StorageNodeTest::deletedIdCanBeReused);
        runTest("deleteChunk refuses a symbolic link to a regular file and leaves the link and its target untouched",
                StorageNodeTest::deleteRefusesFileSymlinkEntry);

        // Retrieving without following links
        runTest("retrieveChunk refuses a dangling junction under the chunk's ID instead of calling the chunk missing",
                StorageNodeTest::retrieveRefusesDanglingJunctionEntry);
        runTest("retrieveChunk refuses a junction in place of the storage directory and reads nothing behind it",
                StorageNodeTest::retrieveRefusesJunctionedStorageDir);
        runTest("retrieveChunk refuses a symbolic link to a regular file and copies nothing",
                StorageNodeTest::retrieveRefusesFileSymlinkEntry);

        // Listing and stale temp files (used by startup recovery)
        runTest("listEntryNames lists every entry, sorted, and refuses a missing or junctioned storage directory",
                StorageNodeTest::listEntryNamesListsEverything);
        runTest("sweepStaleTempFiles deletes only regular temp files and never a chunk or a junction",
                StorageNodeTest::sweepDeletesOnlyStaleTempFiles);
    }

    private static void listEntryNamesListsEverything(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_10", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        node.storeChunk("chunk_2", tempDir.resolve("source.bin"));
        Files.writeString(nodeDir.resolve("chunk_2-55.tmp"), "partial");
        Files.createDirectory(nodeDir.resolve("folder"));

        checkEquals(List.of("chunk_10", "chunk_2", "chunk_2-55.tmp", "folder"), node.listEntryNames(), "names");

        Path target = Files.createDirectory(tempDir.resolve("target"));
        Path junctionDir = tempDir.resolve("junctioned");
        StorageNode junctioned = new StorageNode(junctionDir);
        Files.delete(junctionDir);
        createDirectoryJunction(junctionDir, target);
        expectThrows(IOException.class, "a junction as the storage directory", junctioned::listEntryNames);

        deleteRecursively(nodeDir);
        expectThrows(IOException.class, "a deleted storage directory", node::listEntryNames);
    }

    private static void sweepDeletesOnlyStaleTempFiles(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        byte[] data = randomBytes(100, 1);
        node.storeChunk("chunk_1", Files.write(tempDir.resolve("source.bin"), data));
        Files.writeString(nodeDir.resolve("chunk_1-5.tmp"), "partial");
        Files.writeString(nodeDir.resolve("chunk_1-18446744073709551615.tmp"), "partial");
        Files.writeString(nodeDir.resolve("chunk_1-123456789012345678901.tmp"), "21 digits");
        Files.writeString(nodeDir.resolve("notes.txt"), "notes");
        Path target = Files.createDirectory(tempDir.resolve("target"));
        Files.writeString(target.resolve("precious.txt"), "precious");
        createDirectoryJunction(nodeDir.resolve("chunk_2-6.tmp"), target);

        List<String> warnings = new ArrayList<>();
        checkEquals(2, node.sweepStaleTempFiles(warnings::add), "deleted temp files");

        checkEquals(List.of("chunk_1", "chunk_1-123456789012345678901.tmp", "chunk_2-6.tmp", "notes.txt"),
                listNames(nodeDir), "remaining entries");
        checkBytes(data, nodeDir.resolve("chunk_1"));
        checkEquals("precious", Files.readString(target.resolve("precious.txt")), "the junction's target");
        checkEquals(3, warnings.size(), "one warning per entry left in place (not the chunk): " + warnings);
    }

    private static void runStressTest() {
        long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
        System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
        if (maxHeapMib >= 64) {
            System.out.println("WARNING: the heap can hold the whole 64 MiB chunk, so this run does not prove"
                    + " streaming. Run with -Xmx16m.");
        }
        runTest("a 64 MiB chunk is stored and retrieved with a small heap", StorageNodeTest::largeChunkIsStreamed);
    }

    // ---------------------------------------------------------------------
    // Startup
    // ---------------------------------------------------------------------

    private static void missingStorageDirIsCreated(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("a").resolve("b").resolve("node");

        new StorageNode(nodeDir);

        check(Files.isDirectory(nodeDir), "storage directory should have been created");
        checkEquals(List.of(), listNames(nodeDir), "files in a new node directory");
    }

    private static void existingStorageDirIsReused(Path tempDir) throws IOException {
        byte[] data = randomBytes(500, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);
        Path nodeDir = tempDir.resolve("node");
        new StorageNode(nodeDir).storeChunk("chunk_0", source);

        // A new StorageNode object over the same directory, like a node process restarting.
        StorageNode restarted = new StorageNode(nodeDir);
        Path destination = tempDir.resolve("retrieved.bin");

        check(restarted.hasChunk("chunk_0"), "the restarted node should still have chunk_0");
        restarted.retrieveChunk("chunk_0", destination);
        checkBytes(data, destination);
        checkEquals(List.of("chunk_0"), listNames(nodeDir), "files in the node directory");
    }

    private static void fileAsStorageDirIsRejected(Path tempDir) throws IOException {
        byte[] content = "I am a file, not a directory".getBytes(StandardCharsets.UTF_8);
        Path file = Files.write(tempDir.resolve("not-a-dir"), content);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new StorageNode(file));

        checkContains(e.getMessage(), "not a directory");
        checkBytes(content, file);
    }

    private static void nullStorageDirIsRejected(Path tempDir) {
        expectThrows(NullPointerException.class, () -> new StorageNode(null));
    }

    // ---------------------------------------------------------------------
    // Store and retrieve
    // ---------------------------------------------------------------------

    private static void storeAndRetrieve(Path tempDir) throws IOException {
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(100_000, 1));
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        Path destination = tempDir.resolve("retrieved.bin");

        checkEquals(100_000L, node.storeChunk("chunk_0", source), "bytes stored");
        checkEquals(100_000L, node.retrieveChunk("chunk_0", destination), "bytes retrieved");

        // Files.mismatch returns -1 when both files are byte-for-byte identical.
        checkEquals(-1L, Files.mismatch(source, destination), "position of first differing byte (-1 = identical)");
    }

    private static void emptyChunk(Path tempDir) throws IOException {
        Path source = Files.write(tempDir.resolve("empty.bin"), new byte[0]);
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        Path destination = tempDir.resolve("retrieved.bin");

        checkEquals(0L, node.storeChunk("empty", source), "bytes stored");
        check(node.hasChunk("empty"), "an empty chunk is still a chunk");
        checkEquals(0L, node.retrieveChunk("empty", destination), "bytes retrieved");

        check(Files.isRegularFile(destination), "an empty destination file should have been created");
        checkEquals(0L, Files.size(destination), "destination size");
    }

    private static void binaryDataIsPreserved(Path tempDir) throws IOException {
        // Every possible byte value, 0x00 to 0xFF, followed by bytes that text handling often
        // changes: Windows line endings, NUL, Ctrl-Z, byte-order marks and invalid UTF-8.
        byte[] data = new byte[256 + 15];
        for (int i = 0; i < 256; i++) {
            data[i] = (byte) i;
        }
        byte[] trickyBytes = {
            '\r', '\n', '\n', '\r', 0x00, 0x1A,
            (byte) 0xEF, (byte) 0xBB, (byte) 0xBF,   // UTF-8 byte-order mark
            (byte) 0xFF, (byte) 0xFE,                // UTF-16 byte-order mark
            (byte) 0xC3, (byte) 0x28,                // invalid UTF-8 sequence
            (byte) 0x80, (byte) 0xFF                 // lone continuation byte, 0xFF
        };
        System.arraycopy(trickyBytes, 0, data, 256, trickyBytes.length);
        Path source = Files.write(tempDir.resolve("binary.bin"), data);
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        Path destination = tempDir.resolve("retrieved.bin");

        node.storeChunk("binary", source);
        node.retrieveChunk("binary", destination);

        checkEquals(-1L, Files.mismatch(source, destination), "position of first differing byte (-1 = identical)");
    }

    private static void severalChunks(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        List<byte[]> contents = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            byte[] data = randomBytes(1000 + i, i); // different sizes and contents
            contents.add(data);
            node.storeChunk("chunk_" + i, Files.write(tempDir.resolve("source_" + i), data));
        }

        for (int i = 0; i < 5; i++) {
            Path destination = tempDir.resolve("retrieved_" + i);
            node.retrieveChunk("chunk_" + i, destination);
            checkBytes(contents.get(i), destination);
        }
        checkEquals(List.of("chunk_0", "chunk_1", "chunk_2", "chunk_3", "chunk_4"), listNames(nodeDir),
                "files in the node directory (no temp files)");
    }

    private static void sourceIsUnchanged(Path tempDir) throws IOException {
        byte[] data = randomBytes(200_000, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);
        // A fixed, old timestamp, so any write to the file would visibly change it.
        FileTime originalTime = FileTime.fromMillis(1_000_000_000_000L);
        Files.setLastModifiedTime(source, originalTime);

        new StorageNode(tempDir.resolve("node")).storeChunk("chunk_0", source);

        check(Files.exists(source), "source file should still exist");
        checkBytes(data, source);
        checkEquals(originalTime, Files.getLastModifiedTime(source), "source file last-modified time");
    }

    private static void retrieveLeavesChunkInPlace(Path tempDir) throws IOException {
        byte[] data = randomBytes(5000, 1);
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));
        Path first = tempDir.resolve("first.bin");
        Path second = tempDir.resolve("second.bin");

        node.retrieveChunk("chunk_0", first);
        node.retrieveChunk("chunk_0", second);

        checkBytes(data, first);
        checkBytes(data, second);
        check(node.hasChunk("chunk_0"), "the chunk should still be on the node after retrieval");
        checkBytes(data, nodeDir.resolve("chunk_0"));
    }

    private static void hasChunkAnswers(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));

        check(node.hasChunk("chunk_0"), "hasChunk should be true for a stored chunk");
        check(!node.hasChunk("chunk_1"), "hasChunk should be false for a chunk that was never stored");
    }

    private static void nodesAreIsolated(Path tempDir) throws IOException {
        StorageNode nodeA = new StorageNode(tempDir.resolve("node-a"));
        StorageNode nodeB = new StorageNode(tempDir.resolve("node-b"));
        StorageNode nodeC = new StorageNode(tempDir.resolve("node-c"));
        byte[] dataA = "stored on node A".getBytes(StandardCharsets.UTF_8);
        byte[] dataB = "stored on node B".getBytes(StandardCharsets.UTF_8);

        // The same chunk ID on two nodes, with different content.
        nodeA.storeChunk("x", Files.write(tempDir.resolve("a.bin"), dataA));
        nodeB.storeChunk("x", Files.write(tempDir.resolve("b.bin"), dataB));

        nodeA.retrieveChunk("x", tempDir.resolve("from-a.bin"));
        nodeB.retrieveChunk("x", tempDir.resolve("from-b.bin"));
        checkBytes(dataA, tempDir.resolve("from-a.bin"));
        checkBytes(dataB, tempDir.resolve("from-b.bin"));
        check(!nodeC.hasChunk("x"), "node C never received chunk x");
    }

    private static void endToEnd(Path tempDir) throws IOException {
        Path original = Files.write(tempDir.resolve("original.bin"), randomBytes(2 * MIB + 12_345, 7));
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);

        // Split the file, and store every chunk on the node under its file name (chunk_0, ...).
        List<Path> chunks = new FileChunker().chunk(original, tempDir.resolve("chunks"));
        for (Path chunk : chunks) {
            node.storeChunk(chunk.getFileName().toString(), chunk);
        }

        // Fetch the chunks back from the node, in order, and rebuild the file from those copies.
        List<Path> retrieved = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            Path copy = tempDir.resolve("retrieved").resolve("chunk_" + i);
            node.retrieveChunk("chunk_" + i, copy);
            retrieved.add(copy);
        }
        Path rebuilt = tempDir.resolve("rebuilt.bin");
        new FileReconstructor().reconstruct(retrieved, rebuilt);

        checkEquals(3, chunks.size(), "number of chunks from FileChunker");
        checkEquals(List.of("chunk_0", "chunk_1", "chunk_2"), listNames(nodeDir), "files in the node directory");
        checkEquals(-1L, Files.mismatch(original, rebuilt), "position of first differing byte (-1 = identical)");
    }

    // ---------------------------------------------------------------------
    // Duplicates and never overwriting
    // ---------------------------------------------------------------------

    private static void duplicateIdIsRejected(Path tempDir) throws IOException {
        byte[] firstData = randomBytes(100, 1);
        byte[] secondData = randomBytes(100, 2);
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("first.bin"), firstData));
        Path secondSource = Files.write(tempDir.resolve("second.bin"), secondData);

        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class,
                () -> node.storeChunk("chunk_0", secondSource));

        checkContains(e.getMessage(), "already exists");
        checkBytes(firstData, nodeDir.resolve("chunk_0"));
        checkEquals(List.of("chunk_0"), listNames(nodeDir), "files in the node directory (no temp file left)");
    }

    private static void existingDestinationIsNotOverwritten(Path tempDir) throws IOException {
        byte[] data = randomBytes(100, 1);
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));
        byte[] existingContent = "do not overwrite me".getBytes(StandardCharsets.UTF_8);
        Path existing = Files.write(tempDir.resolve("existing.bin"), existingContent);

        expectThrows(FileAlreadyExistsException.class, () -> node.retrieveChunk("chunk_0", existing));
        // Not overwritten, and not deleted by cleanup either.
        checkBytes(existingContent, existing);

        // Pointing the destination at the chunk's own file inside the node is rejected before
        // anything is opened, because no destination may be inside the node's storage directory.
        Path chunkFile = nodeDir.resolve("chunk_0");
        expectThrows(IllegalArgumentException.class, () -> node.retrieveChunk("chunk_0", chunkFile));
        checkBytes(data, chunkFile);
    }

    private static void directoryDestinationIsRejected(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        Path existingDir = Files.createDirectory(tempDir.resolve("existing-dir"));
        Path fileInsideDir = Files.write(existingDir.resolve("keep.txt"), randomBytes(5, 2));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> node.retrieveChunk("chunk_0", existingDir));

        checkContains(e.getMessage(), "existing directory");
        check(Files.isDirectory(existingDir), "existing directory should still exist");
        check(Files.exists(fileInsideDir), "file inside the existing directory should still exist");
    }

    private static void storageDirAsDestinationIsRejected(Path tempDir) throws IOException {
        StorageNode node = nodeWithOneChunk(tempDir);

        checkDestinationRejected(node, tempDir, tempDir.resolve("node"));
    }

    private static void destinationInsideStorageDirIsRejected(Path tempDir) throws IOException {
        StorageNode node = nodeWithOneChunk(tempDir);
        Path nodeDir = tempDir.resolve("node");

        // The case from the bug report: a copy the node could later mistake for another chunk.
        checkDestinationRejected(node, tempDir, nodeDir.resolve("another_chunk"));
        check(!node.hasChunk("another_chunk"), "no chunk named another_chunk should appear");

        // Windows ignores letter case in paths, so there "NODE" is the same folder as "node".
        if (System.getProperty("os.name").startsWith("Windows")) {
            checkDestinationRejected(node, tempDir, tempDir.resolve("NODE").resolve("copy.bin"));
        }
    }

    private static void destinationInNestedFolderIsRejected(Path tempDir) throws IOException {
        StorageNode node = nodeWithOneChunk(tempDir);
        Path nodeDir = tempDir.resolve("node");

        // These folders do not exist yet; rejecting the destination must also mean not creating them.
        checkDestinationRejected(node, tempDir, nodeDir.resolve("sub").resolve("deeper").resolve("copy.bin"));
        check(!Files.exists(nodeDir.resolve("sub")), "no folder should be created inside the storage directory");
    }

    private static void dotDotDestinationIsRejected(Path tempDir) throws IOException {
        StorageNode node = nodeWithOneChunk(tempDir);
        Path nodeDir = tempDir.resolve("node");

        // Each path is spelled differently, but all of them end up inside the storage directory.
        Path[] disguisedDestinations = {
            tempDir.resolve("elsewhere").resolve("..").resolve("node").resolve("copy.bin"), // out and back in
            nodeDir.resolve("..").resolve("node").resolve("copy.bin"),
            nodeDir.resolve(".").resolve("copy.bin"),
            nodeDir.resolve("sub").resolve("..").resolve("copy.bin"),
            nodeDir.resolve("sub").resolve(".."),                                            // the directory itself
        };
        for (Path destination : disguisedDestinations) {
            checkDestinationRejected(node, tempDir, destination);
        }
        check(!Files.exists(tempDir.resolve("elsewhere")), "no folder should be created for a rejected destination");
    }

    private static void outsideDestinationsAreAllowed(Path tempDir) throws IOException {
        StorageNode node = nodeWithOneChunk(tempDir);
        Path nodeDir = tempDir.resolve("node");

        Path[] allowedDestinations = {
            tempDir.resolve("copy.bin"),                                 // next to the storage directory
            nodeDir.resolve("..").resolve("via-dotdot.bin"),             // a ".." that leads out of it
            tempDir.resolve("node-copy").resolve("chunk_0"),             // a sibling whose name starts with "node"
            tempDir.resolve("nodes").resolve("deep").resolve("chunk_0"), // missing parent folders are still created
        };
        for (Path destination : allowedDestinations) {
            checkEquals((long) DESTINATION_TEST_DATA.length, node.retrieveChunk("chunk_0", destination),
                    "bytes retrieved to " + destination);
            checkBytes(DESTINATION_TEST_DATA, destination);
        }
        checkEquals(List.of("chunk_0"), listNames(nodeDir), "files in the node directory");
    }

    /** The content of chunk_0 in the destination tests, used to check that it never changes. */
    private static final byte[] DESTINATION_TEST_DATA = "the stored chunk".getBytes(StandardCharsets.UTF_8);

    // ---------------------------------------------------------------------
    // Deleting
    // ---------------------------------------------------------------------

    private static void deleteRemovesChunk(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(100, 1)));

        check(node.deleteChunk("chunk_0"), "deleting a stored chunk should return true");

        check(!node.hasChunk("chunk_0"), "the chunk should be gone");
        expectThrows(ChunkNotFoundException.class, () -> node.retrieveChunk("chunk_0", tempDir.resolve("retrieved.bin")));
        checkEquals(List.of(), listNames(nodeDir), "files in the node directory");
    }

    private static void deleteMissingChunkReturnsFalse(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        byte[] data = randomBytes(100, 1);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));

        check(!node.deleteChunk("chunk_9"), "deleting a chunk that was never stored should return false");
        checkEquals(List.of("chunk_0"), listNames(nodeDir), "files in the node directory");
        checkBytes(data, nodeDir.resolve("chunk_0"));

        check(node.deleteChunk("chunk_0"), "first delete of chunk_0");
        check(!node.deleteChunk("chunk_0"), "a second delete of the same chunk should return false");
        checkEquals(List.of(), listNames(nodeDir), "files after both deletes");
    }

    private static void deleteLeavesOtherEntriesAlone(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        byte[] data1 = randomBytes(100, 1);
        byte[] data10 = randomBytes(200, 10);
        byte[] dataDash = randomBytes(50, 2);
        node.storeChunk("chunk_1", Files.write(tempDir.resolve("source1.bin"), data1));
        node.storeChunk("chunk_10", Files.write(tempDir.resolve("source10.bin"), data10));
        node.storeChunk("chunk_1-a", Files.write(tempDir.resolve("sourceDash.bin"), dataDash));
        // An orphaned temp file of chunk_1, as a killed store would leave it.
        byte[] tempData = "orphaned temp file".getBytes(StandardCharsets.UTF_8);
        Files.write(nodeDir.resolve("chunk_1-123.tmp"), tempData);

        check(node.deleteChunk("chunk_1"), "delete chunk_1");

        checkEquals(Stream.of("chunk_1-123.tmp", "chunk_10", "chunk_1-a").sorted().toList(), listNames(nodeDir),
                "files left in the node directory");
        checkBytes(data10, nodeDir.resolve("chunk_10"));
        checkBytes(dataDash, nodeDir.resolve("chunk_1-a"));
        checkBytes(tempData, nodeDir.resolve("chunk_1-123.tmp"));
    }

    private static void deleteRejectsInvalidIds(Path tempDir) throws IOException {
        Path secret = Files.write(tempDir.resolve("secret.txt"), "secret data".getBytes(StandardCharsets.UTF_8));
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        byte[] data = randomBytes(10, 1);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));
        List<String> before = listTree(tempDir);

        expectThrows(NullPointerException.class, () -> node.deleteChunk(null));
        String[] invalidIds = {
            "", "a".repeat(129), "Chunk_0", "CHUNK_0", "chunk_0.tmp", "a b", "con", "nul",
            "../secret.txt", "..\\secret.txt", "..", secret.toAbsolutePath().toString(), "C:secret.txt",
        };
        for (String id : invalidIds) {
            expectThrows(IllegalArgumentException.class, "deleteChunk(" + show(id) + ")", () -> node.deleteChunk(id));
        }

        checkEquals(before, listTree(tempDir), "everything in the test folder");
        checkBytes(data, nodeDir.resolve("chunk_0"));
        checkBytes("secret data".getBytes(StandardCharsets.UTF_8), secret);
    }

    private static void deleteWithMissingStorageDirIsAnError(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        deleteRecursively(nodeDir);

        IOException e = expectThrows(IOException.class, "a chunk that was stored", () -> node.deleteChunk("chunk_0"));
        checkContains(e.getMessage(), "not a plain directory");
        expectThrows(IOException.class, "a chunk that was never stored", () -> node.deleteChunk("chunk_9"));
        check(!Files.exists(nodeDir), "the node must not recreate its storage directory");
    }

    private static void deleteWithStorageDirReplacedByFileIsAnError(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        deleteRecursively(nodeDir);
        Files.writeString(nodeDir, "a file where the storage directory was");

        IOException e = expectThrows(IOException.class, () -> node.deleteChunk("chunk_0"));
        checkContains(e.getMessage(), "not a plain directory");
        checkEquals("a file where the storage directory was", Files.readString(nodeDir), "the file is unchanged");
    }

    private static void deleteWithStorageDirReplacedByJunctionIsAnError(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        // Behind the junction: a regular file with the chunk's name, which must not be deleted.
        Path outside = Files.createDirectory(tempDir.resolve("outside"));
        byte[] outsideData = randomBytes(64, 7);
        Files.write(outside.resolve("chunk_0"), outsideData);
        Files.delete(nodeDir);
        createDirectoryJunction(nodeDir, outside);
        try {
            IOException e = expectThrows(IOException.class, "an ID with a file behind the junction",
                    () -> node.deleteChunk("chunk_0"));
            checkContains(e.getMessage(), "not a plain directory");
            expectThrows(IOException.class, "an ID with nothing behind the junction", () -> node.deleteChunk("chunk_9"));

            check(Files.exists(nodeDir, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
            checkEquals(List.of("chunk_0"), listNames(outside), "files behind the junction");
            checkBytes(outsideData, outside.resolve("chunk_0"));
        } finally {
            Files.deleteIfExists(nodeDir); // removes the junction itself, never its target
        }
    }

    private static void deleteRefusesDirectoryEntry(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path entry = Files.createDirectory(nodeDir.resolve("chunk_0"));
        Files.writeString(entry.resolve("inside.txt"), "inside");

        IOException e = expectThrows(IOException.class, () -> node.deleteChunk("chunk_0"));
        checkContains(e.getMessage(), "not a regular file");
        check(Files.isDirectory(entry), "the directory is left in place");
        checkEquals("inside", Files.readString(entry.resolve("inside.txt")), "the file inside the directory");
    }

    private static void deleteRefusesJunctionEntry(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path outside = Files.createDirectory(tempDir.resolve("outside"));
        byte[] outsideData = randomBytes(64, 7);
        Files.write(outside.resolve("precious.bin"), outsideData);
        Path entry = nodeDir.resolve("chunk_0");
        createDirectoryJunction(entry, outside);
        try {
            IOException e = expectThrows(IOException.class, () -> node.deleteChunk("chunk_0"));
            checkContains(e.getMessage(), "not a regular file");
            check(Files.exists(entry, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
            checkEquals(List.of("precious.bin"), listNames(outside), "files behind the junction");
            checkBytes(outsideData, outside.resolve("precious.bin"));
        } finally {
            Files.deleteIfExists(entry); // removes the junction itself, never its target
        }
    }

    private static void injectedDeleteFailurePropagates(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        AccessDeniedException failure = new AccessDeniedException("chunk_0", null, "simulated: the file is in use");
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            void deleteChunkFile(Path chunkFile) throws IOException {
                throw failure;
            }
        };
        byte[] data = randomBytes(100, 1);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));

        AccessDeniedException e = expectThrows(AccessDeniedException.class, () -> node.deleteChunk("chunk_0"));

        check(e == failure, "the node's own error should be passed through unchanged");
        check(node.hasChunk("chunk_0"), "the chunk should still be there");
        checkBytes(data, nodeDir.resolve("chunk_0"));
    }

    private static void raceChunkVanishedReturnsFalse(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            void deleteChunkFile(Path chunkFile) throws IOException {
                Files.delete(chunkFile); // something else removes the chunk first...
                throw new NoSuchFileException(chunkFile.toString()); // ...so this delete finds nothing
            }
        };
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));

        check(!node.deleteChunk("chunk_0"), "a chunk that was already gone should be reported as absent");
        checkEquals(List.of(), listNames(nodeDir), "files in the node directory");
    }

    private static void raceInconsistentStateIsAnError(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            void deleteChunkFile(Path chunkFile) throws IOException {
                throw new NoSuchFileException(chunkFile.toString()); // says "missing" but deletes nothing
            }
        };
        byte[] data = randomBytes(10, 1);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));

        IOException e = expectThrows(IOException.class, () -> node.deleteChunk("chunk_0"));

        check(!(e instanceof NoSuchFileException), "the recheck should turn this into a plain IOException");
        checkContains(e.getMessage(), "still present");
        check(e.getCause() instanceof NoSuchFileException, "the delete's own error should be kept as the cause");
        checkBytes(data, nodeDir.resolve("chunk_0"));
    }

    private static void raceStorageDirVanishedIsAnError(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            void deleteChunkFile(Path chunkFile) throws IOException {
                deleteRecursively(nodeDir); // the whole storage directory disappears...
                throw new NoSuchFileException(chunkFile.toString()); // ...as the delete runs
            }
        };
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));

        IOException e = expectThrows(IOException.class, () -> node.deleteChunk("chunk_0"));

        checkContains(e.getMessage(), "not a plain directory");
        checkEquals(1, e.getSuppressed().length, "number of attached errors");
        check(e.getSuppressed()[0] instanceof NoSuchFileException, "the delete's own error should be attached");
    }

    private static void holdsRegularChunkFileAnswers(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        Files.createDirectory(nodeDir.resolve("chunk_1"));

        check(node.holdsRegularChunkFile("chunk_0"), "a stored chunk is a regular file");
        check(!node.holdsRegularChunkFile("chunk_9"), "a chunk never stored is absent");
        expectThrows(IOException.class, "a directory under the ID", () -> node.holdsRegularChunkFile("chunk_1"));
        expectThrows(IllegalArgumentException.class, "an invalid ID", () -> node.holdsRegularChunkFile("../x"));
        expectThrows(NullPointerException.class, "null", () -> node.holdsRegularChunkFile(null));
        checkEquals(List.of("chunk_0", "chunk_1"), listNames(nodeDir), "nothing was changed by asking");
    }

    private static void deletedIdCanBeReused(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode first = new StorageNode(nodeDir);
        byte[] oldData = randomBytes(100, 1);
        byte[] otherData = randomBytes(100, 2);
        byte[] newData = randomBytes(150, 3);
        first.storeChunk("chunk_0", Files.write(tempDir.resolve("old.bin"), oldData));
        first.storeChunk("chunk_1", Files.write(tempDir.resolve("other.bin"), otherData));

        // A new node object over the same folder (a simulated restart) deletes what the first stored.
        StorageNode restarted = new StorageNode(nodeDir);
        check(restarted.deleteChunk("chunk_0"), "the restarted node deletes chunk_0");
        restarted.storeChunk("chunk_0", Files.write(tempDir.resolve("new.bin"), newData));

        Path retrieved = tempDir.resolve("retrieved.bin");
        restarted.retrieveChunk("chunk_0", retrieved);
        checkBytes(newData, retrieved);
        checkBytes(otherData, nodeDir.resolve("chunk_1"));
    }

    /**
     * The case only a symbolic link to a file can show: hasChunk (which follows links) sees a
     * regular file here, so deleteChunk must find out with its own no-follow check. Skipped, not
     * passed, where this account may not create symbolic links.
     */
    private static void deleteRefusesFileSymlinkEntry(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        byte[] targetData = randomBytes(64, 7);
        Path target = Files.write(tempDir.resolve("target.bin"), targetData);
        Path link = nodeDir.resolve("chunk_0");
        createFileSymlinkOrSkip(link, target);

        IOException e = expectThrows(IOException.class, "deleteChunk", () -> node.deleteChunk("chunk_0"));
        checkContains(e.getMessage(), "not a regular file");
        expectThrows(IOException.class, "holdsRegularChunkFile", () -> node.holdsRegularChunkFile("chunk_0"));

        check(Files.isSymbolicLink(link), "the link is left in place");
        checkEquals(target, Files.readSymbolicLink(link), "the link still points at its target");
        checkBytes(targetData, target);
    }

    /**
     * A junction whose target is gone: following it finds nothing, which a link-following check
     * would report as "chunk missing". The no-follow check sees an entry that is not a regular
     * file, so the node says it cannot tell.
     */
    private static void retrieveRefusesDanglingJunctionEntry(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path gone = Files.createDirectory(tempDir.resolve("gone"));
        Path entry = nodeDir.resolve("chunk_0");
        createDirectoryJunction(entry, gone);
        Files.delete(gone); // the junction now points at nothing
        Path destination = tempDir.resolve("retrieved.bin");
        try {
            IOException e = expectThrows(IOException.class, () -> node.retrieveChunk("chunk_0", destination));
            check(!(e instanceof ChunkNotFoundException), "a dangling junction must not be reported as a missing chunk");
            checkContains(e.getMessage(), "not a regular file");
            check(!Files.exists(destination, LinkOption.NOFOLLOW_LINKS), "no destination should be created");
            check(Files.exists(entry, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
        } finally {
            Files.deleteIfExists(entry); // removes the junction itself
        }
    }

    private static void retrieveRefusesJunctionedStorageDir(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        // Behind the junction: a regular file with the chunk's name, which must not be read.
        Path outside = Files.createDirectory(tempDir.resolve("outside"));
        byte[] outsideData = randomBytes(64, 7);
        Files.write(outside.resolve("chunk_0"), outsideData);
        Files.delete(nodeDir);
        createDirectoryJunction(nodeDir, outside);
        Path destination = tempDir.resolve("retrieved.bin");
        try {
            IOException e = expectThrows(IOException.class, () -> node.retrieveChunk("chunk_0", destination));
            check(!(e instanceof ChunkNotFoundException), "a junctioned storage directory is not a missing chunk");
            checkContains(e.getMessage(), "not a plain directory");
            check(!Files.exists(destination, LinkOption.NOFOLLOW_LINKS), "nothing behind the junction is copied");

            check(Files.exists(nodeDir, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
            checkEquals(List.of("chunk_0"), listNames(outside), "files behind the junction");
            checkBytes(outsideData, outside.resolve("chunk_0"));
        } finally {
            Files.deleteIfExists(nodeDir); // removes the junction itself, never its target
        }
    }

    /**
     * Skipped, not passed, where this account may not create symbolic links. A link-following
     * check sees a regular file here and would copy the target's bytes out as the chunk.
     */
    private static void retrieveRefusesFileSymlinkEntry(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        byte[] targetData = randomBytes(64, 7);
        Path target = Files.write(tempDir.resolve("target.bin"), targetData);
        Path link = nodeDir.resolve("chunk_0");
        createFileSymlinkOrSkip(link, target);
        Path destination = tempDir.resolve("retrieved.bin");

        IOException e = expectThrows(IOException.class, () -> node.retrieveChunk("chunk_0", destination));
        check(!(e instanceof ChunkNotFoundException), "a link must not be reported as a missing chunk");
        checkContains(e.getMessage(), "not a regular file");
        check(!Files.exists(destination, LinkOption.NOFOLLOW_LINKS), "no destination should be created");

        check(Files.isSymbolicLink(link), "the link is left in place");
        checkEquals(target, Files.readSymbolicLink(link), "the link still points at its target");
        checkBytes(targetData, target);
    }

    /** Creates a node in tempDir/node holding one chunk, chunk_0, for the destination tests. */
    private static StorageNode nodeWithOneChunk(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        node.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), DESTINATION_TEST_DATA));
        return node;
    }

    /**
     * Checks that retrieving chunk_0 to {@code destination} is rejected with a clear message, that
     * nothing new appears anywhere in the test folder (no file and no parent folders), and that
     * the stored chunk is unchanged.
     */
    private static void checkDestinationRejected(StorageNode node, Path tempDir, Path destination) throws IOException {
        List<String> before = listTree(tempDir);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                "retrieveChunk to " + destination, () -> node.retrieveChunk("chunk_0", destination));

        checkContains(e.getMessage(), "outside the node's storage directory");
        checkEquals(before, listTree(tempDir), "files and folders after rejecting " + destination);
        checkBytes(DESTINATION_TEST_DATA, tempDir.resolve("node").resolve("chunk_0"));
    }

    private static void chunkAppearingDuringStoreIsNotReplaced(Path tempDir) throws IOException {
        byte[] ourData = randomBytes(1000, 1);
        byte[] otherWritersData = "written by another writer".getBytes(StandardCharsets.UTF_8);
        Path source = Files.write(tempDir.resolve("source.bin"), ourData);
        Path nodeDir = tempDir.resolve("node");
        Path chunkFile = nodeDir.resolve("chunk_0");

        // StorageNode is used from one thread, so a real race cannot happen in this test. Instead,
        // the hook stages the ordering the code must still survive: chunk_0 appears after the
        // duplicate check but before the final rename (here, right at the end of the copy).
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            InputStream openForReading(Path file) throws IOException {
                byte[] data = Files.readAllBytes(file);
                return new InterruptingStream(data, data.length, () -> Files.write(chunkFile, otherWritersData));
            }
        };

        expectThrows(FileAlreadyExistsException.class, () -> node.storeChunk("chunk_0", source));

        checkBytes(otherWritersData, chunkFile);
        checkEquals(List.of("chunk_0"), listNames(nodeDir), "files in the node directory (no temp file left)");
    }

    private static void chunkIsInvisibleUntilStoreCompletes(Path tempDir) throws IOException {
        byte[] data = randomBytes(1000, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);
        Path nodeDir = tempDir.resolve("node");
        // One-element arrays: a lambda cannot change a local variable, but it can change an array element.
        boolean[] fileVisibleMidCopy = {true};
        boolean[] hasChunkMidCopy = {true};

        StorageNode node = new StorageNode(nodeDir) {
            @Override
            InputStream openForReading(Path file) throws IOException {
                // Halfway through the copy, look at the node from the outside and through its own API.
                return new InterruptingStream(Files.readAllBytes(file), 500, () -> {
                    fileVisibleMidCopy[0] = Files.exists(nodeDir.resolve("chunk_0"));
                    hasChunkMidCopy[0] = hasChunk("chunk_0"); // asks this same node
                });
            }
        };

        node.storeChunk("chunk_0", source);

        check(!fileVisibleMidCopy[0], "no file named chunk_0 should exist while the store is in progress");
        check(!hasChunkMidCopy[0], "hasChunk should be false while the store is in progress");
        check(node.hasChunk("chunk_0"), "hasChunk should be true once the store has finished");
        checkBytes(data, nodeDir.resolve("chunk_0"));
    }

    // ---------------------------------------------------------------------
    // Missing chunks and invalid input
    // ---------------------------------------------------------------------

    private static void missingChunk(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        Path destination = tempDir.resolve("out").resolve("sub").resolve("chunk.bin");

        ChunkNotFoundException e = expectThrows(ChunkNotFoundException.class,
                () -> node.retrieveChunk("chunk_7", destination));

        checkEquals("chunk_7", e.getChunkId(), "chunk ID in the exception");
        checkContains(e.getMessage(), "chunk_7");
        check(!Files.exists(tempDir.resolve("out")), "no destination folders should be created for a missing chunk");
    }

    private static void invalidSourceIsRejected(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path missing = tempDir.resolve("missing.bin");
        Path directory = Files.createDirectory(tempDir.resolve("some-dir"));

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class,
                () -> node.storeChunk("chunk_0", missing));
        checkContains(e1.getMessage(), "does not exist");

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class,
                () -> node.storeChunk("chunk_0", directory));
        checkContains(e2.getMessage(), "is a directory");

        check(!node.hasChunk("chunk_0"), "nothing should have been stored");
        checkEquals(List.of(), listNames(nodeDir), "files in the node directory (no chunk and no temp file)");
    }

    private static void nullArgumentsAreRejected(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1));
        Path destination = tempDir.resolve("retrieved.bin");

        expectThrows(NullPointerException.class, () -> node.storeChunk(null, source));
        expectThrows(NullPointerException.class, () -> node.storeChunk("chunk_0", null));
        expectThrows(NullPointerException.class, () -> node.retrieveChunk(null, destination));
        expectThrows(NullPointerException.class, () -> node.retrieveChunk("chunk_0", null));
        expectThrows(NullPointerException.class, () -> node.hasChunk(null));

        check(!Files.exists(destination), "destination should not be created when arguments are null");
    }

    private static void chunkIdRules(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path source = Files.write(tempDir.resolve("source.bin"), new byte[] {42});
        Path destination = tempDir.resolve("retrieved.bin");

        String[] invalidIds = {
            "",                  // empty
            "a".repeat(129),     // one character too long
            "Chunk_0",           // uppercase: Windows would treat it as the same file as chunk_0
            "CHUNK",
            "a.b",               // dot
            "chunk_0.tmp",       // looks like a temp file
            " a", "a ", "a b",   // spaces
            "a\tb",              // tab
            "a\u0000b",          // NUL character
            "\u00e9",            // a non-ASCII letter (é)
            "a:b",               // colon (Windows drive letters and alternate data streams)
            "a*b", "a?b",        // other punctuation
        };
        for (String id : invalidIds) {
            // hasChunk first: it only reads, so a broken implementation cannot create odd files here.
            expectThrows(IllegalArgumentException.class, "hasChunk(" + show(id) + ")", () -> node.hasChunk(id));
            expectThrows(IllegalArgumentException.class, "storeChunk(" + show(id) + ")",
                    () -> node.storeChunk(id, source));
            expectThrows(IllegalArgumentException.class, "retrieveChunk(" + show(id) + ")",
                    () -> node.retrieveChunk(id, destination));
        }
        // The messages say what is wrong.
        checkContains(expectThrows(IllegalArgumentException.class, () -> node.hasChunk("Chunk_0")).getMessage(),
                "'C' at position 0");
        checkContains(expectThrows(IllegalArgumentException.class, () -> node.hasChunk("a".repeat(129))).getMessage(),
                "1 to 128");

        String[] validIds = {"a", "0", "-", "_", "chunk_0", "a-b_c-9", "a".repeat(128), HEX_64};
        for (String id : validIds) {
            check(!node.hasChunk(id), "valid ID should not be stored yet: " + id);
            node.storeChunk(id, source);
            check(node.hasChunk(id), "valid ID should be stored now: " + id);
        }

        checkEquals(Arrays.stream(validIds).sorted().toList(), listNames(nodeDir),
                "files in the node directory (only the valid IDs)");
        check(!Files.exists(destination), "no destination should be created for invalid IDs");
    }

    private static void windowsDeviceNames(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path source = Files.write(tempDir.resolve("source.bin"), new byte[] {42});
        Path destination = tempDir.resolve("retrieved.bin");

        String[] deviceNames = {"con", "prn", "aux", "nul", "com0", "com1", "com9", "lpt0", "lpt1", "lpt9"};
        for (String id : deviceNames) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, "hasChunk(" + show(id) + ")",
                    () -> node.hasChunk(id));
            checkContains(e.getMessage(), "device name");
            expectThrows(IllegalArgumentException.class, "storeChunk(" + show(id) + ")",
                    () -> node.storeChunk(id, source));
            expectThrows(IllegalArgumentException.class, "retrieveChunk(" + show(id) + ")",
                    () -> node.retrieveChunk(id, destination));
        }

        // Names that only look like device names are ordinary, valid IDs.
        String[] lookAlikes = {"con1", "com10", "lpt10", "console", "nul_", "auxiliary"};
        for (String id : lookAlikes) {
            node.storeChunk(id, source);
            check(node.hasChunk(id), "look-alike ID should be stored: " + id);
        }

        checkEquals(Arrays.stream(lookAlikes).sorted().toList(), listNames(nodeDir),
                "files in the node directory (only the look-alikes)");
    }

    private static void pathTraversalIsRejected(Path tempDir) throws IOException {
        // A file outside the node directory that an attacker might try to read or overwrite.
        byte[] secretContent = "secret data outside the node".getBytes(StandardCharsets.UTF_8);
        Path secret = Files.write(tempDir.resolve("secret.txt"), secretContent);
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path source = Files.write(tempDir.resolve("source.bin"), "attacker data".getBytes(StandardCharsets.UTF_8));
        Path destination = tempDir.resolve("retrieved.bin");
        List<String> entriesBefore = listNames(tempDir);

        String[] traversalIds = {
            "../secret.txt",
            "..\\secret.txt",
            "..",
            "../node/../secret.txt",
            "sub/../../secret.txt",
            "a/b",
            "a\\b",
            secret.toAbsolutePath().toString(),   // an absolute path
            "/secret.txt",
            "C:secret.txt",                        // a Windows drive-relative path
            "%2e%2e%2fsecret.txt",                 // URL-encoded "../"
        };
        for (String id : traversalIds) {
            expectThrows(IllegalArgumentException.class, "hasChunk(" + show(id) + ")", () -> node.hasChunk(id));
            expectThrows(IllegalArgumentException.class, "retrieveChunk(" + show(id) + ")",
                    () -> node.retrieveChunk(id, destination));
            expectThrows(IllegalArgumentException.class, "storeChunk(" + show(id) + ")",
                    () -> node.storeChunk(id, source));
        }

        checkBytes(secretContent, secret);
        checkEquals(entriesBefore, listNames(tempDir), "entries in the test folder (nothing created or removed)");
        checkEquals(List.of(), listNames(nodeDir), "files in the node directory");
    }

    // ---------------------------------------------------------------------
    // Failures, errors and cleanup
    // ---------------------------------------------------------------------

    private static void failedStoreLeavesNoPartialChunk(Path tempDir) throws IOException {
        byte[] keepData = randomBytes(100, 1);
        byte[] data = randomBytes(1000, 2);
        Path keepSource = Files.write(tempDir.resolve("keep.bin"), keepData);
        Path failingSource = Files.write(tempDir.resolve("failing.bin"), data);
        Path nodeDir = tempDir.resolve("node");
        IOException simulatedFailure = new IOException("simulated disk error");
        boolean[] chunkFileExistedAtFailure = {true};
        List<byte[]> tempContentsAtFailure = new ArrayList<>();

        // Use the openForReading hook: the failing source delivers 400 real bytes and then fails.
        // Just before failing, the stream records what the node directory looks like.
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            InputStream openForReading(Path file) throws IOException {
                if (!file.equals(failingSource)) {
                    return super.openForReading(file);
                }
                return new InterruptingStream(data, 400, () -> {
                    chunkFileExistedAtFailure[0] = Files.exists(nodeDir.resolve("chunk_1"));
                    for (Path tempFile : listTempFiles(nodeDir)) {
                        tempContentsAtFailure.add(Files.readAllBytes(tempFile));
                    }
                    throw simulatedFailure;
                });
            }
        };
        node.storeChunk("keep", keepSource);

        IOException e = expectThrows(IOException.class, () -> node.storeChunk("chunk_1", failingSource));

        // At the moment of failure, the 400 bytes copied so far were in a temp file, not under chunk_1.
        check(!chunkFileExistedAtFailure[0], "no file named chunk_1 should exist when the failure happens");
        checkEquals(1, tempContentsAtFailure.size(), "number of temp files at the moment of failure");
        check(Arrays.equals(Arrays.copyOf(data, 400), tempContentsAtFailure.get(0)),
                "the temp file should hold exactly the 400 bytes copied so far");

        // The original exception is passed through unchanged, with no cleanup errors.
        check(e == simulatedFailure, "expected the original simulated exception, but got " + e);
        checkEquals(0, e.getSuppressed().length, "number of suppressed cleanup errors");

        // Afterwards: no chunk_1, no temp file, the other chunk untouched, the source unchanged.
        check(!node.hasChunk("chunk_1"), "chunk_1 should not exist after the failed store");
        checkEquals(List.of("keep"), listNames(nodeDir), "files in the node directory");
        checkBytes(keepData, nodeDir.resolve("keep"));
        checkBytes(data, failingSource);

        // Nothing left behind blocks a retry with the same ID (from a copy of the source, so
        // the failing hook is not used again).
        Path retrySource = Files.copy(failingSource, tempDir.resolve("retry.bin"));
        checkEquals(1000L, node.storeChunk("chunk_1", retrySource), "bytes stored on retry");
        checkBytes(data, nodeDir.resolve("chunk_1"));
    }

    private static void failedRetrievalLeavesNoPartialDestination(Path tempDir) throws IOException {
        byte[] data = randomBytes(1000, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);
        Path nodeDir = tempDir.resolve("node");
        Path destination = tempDir.resolve("retrieved.bin");
        IOException simulatedFailure = new IOException("simulated disk error");
        byte[][] destinationAtFailure = {null};

        // The hook fails partway through reading chunk_0 (it does not affect reading source.bin).
        StorageNode node = new StorageNode(nodeDir) {
            @Override
            InputStream openForReading(Path file) throws IOException {
                if (!file.getFileName().toString().equals("chunk_0")) {
                    return super.openForReading(file);
                }
                return new InterruptingStream(Files.readAllBytes(file), 400, () -> {
                    if (Files.exists(destination)) {
                        destinationAtFailure[0] = Files.readAllBytes(destination);
                    }
                    throw simulatedFailure;
                });
            }
        };
        node.storeChunk("chunk_0", source);

        IOException e = expectThrows(IOException.class, () -> node.retrieveChunk("chunk_0", destination));

        // A partial destination really existed at the moment of failure...
        check(destinationAtFailure[0] != null, "the destination should exist when the failure happens");
        check(Arrays.equals(Arrays.copyOf(data, 400), destinationAtFailure[0]),
                "the partial destination should hold exactly the 400 bytes copied so far");
        // ...and was removed afterwards, with the original exception passed through.
        check(e == simulatedFailure, "expected the original simulated exception, but got " + e);
        checkEquals(0, e.getSuppressed().length, "number of suppressed cleanup errors");
        check(!Files.exists(destination), "the partial destination should have been deleted");

        // The chunk itself is untouched, and a retry to the same destination works (a fresh
        // node object over the same directory, without the failing hook).
        check(node.hasChunk("chunk_0"), "the chunk should still be on the node");
        checkBytes(data, nodeDir.resolve("chunk_0"));
        new StorageNode(nodeDir).retrieveChunk("chunk_0", destination);
        checkBytes(data, destination);
    }

    private static void deletedStorageDirIsAnError(Path tempDir) throws IOException {
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(100, 1));
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        node.storeChunk("chunk_0", source);
        Path destination = tempDir.resolve("retrieved.bin");

        // Simulate the node losing its storage after startup, for example an unplugged disk.
        deleteRecursively(nodeDir);

        // hasChunk cannot tell, so it throws, for a chunk it had and for one it never had.
        IOException e1 = expectThrows(IOException.class, () -> node.hasChunk("chunk_0"));
        checkContains(e1.getMessage(), "not available");
        expectThrows(IOException.class, () -> node.hasChunk("chunk_1"));

        // retrieveChunk must not claim the chunk is simply missing.
        IOException e2 = expectThrows(IOException.class, () -> node.retrieveChunk("chunk_0", destination));
        check(!(e2 instanceof ChunkNotFoundException), "a lost storage directory must not be reported as a missing chunk");
        check(!Files.exists(destination), "no destination should be created");

        // storeChunk fails and does not quietly recreate the directory.
        IOException e3 = expectThrows(IOException.class, () -> node.storeChunk("chunk_1", source));
        checkContains(e3.getMessage(), "not available");
        check(!Files.exists(nodeDir), "the node must not silently recreate its storage directory");
    }

    private static void unexpectedDirectoryIsAnError(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1));
        // The node only ever creates regular files, so a directory under a chunk's name means
        // its storage was changed from outside.
        Path strayDir = Files.createDirectory(nodeDir.resolve("chunk_0"));

        IOException e1 = expectThrows(IOException.class, () -> node.hasChunk("chunk_0"));
        checkContains(e1.getMessage(), "not a regular file");

        IOException e2 = expectThrows(IOException.class,
                () -> node.retrieveChunk("chunk_0", tempDir.resolve("retrieved.bin")));
        check(!(e2 instanceof ChunkNotFoundException), "a damaged entry must not be reported as a missing chunk");

        IOException e3 = expectThrows(IOException.class, () -> node.storeChunk("chunk_0", source));
        checkContains(e3.getMessage(), "not a regular file");

        check(Files.isDirectory(strayDir), "the stray directory should be left alone");
        checkEquals(List.of("chunk_0"), listNames(nodeDir), "files in the node directory (no temp file left)");
    }

    private static void orphanedTempFileIsIgnored(Path tempDir) throws IOException {
        Path nodeDir = tempDir.resolve("node");
        StorageNode node = new StorageNode(nodeDir);
        // What a store that was killed halfway through would leave behind.
        Path orphan = Files.write(nodeDir.resolve("chunk_5-1234567890.tmp"),
                "half-written".getBytes(StandardCharsets.UTF_8));
        byte[] data = randomBytes(100, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);
        Path destination = tempDir.resolve("retrieved.bin");

        check(!node.hasChunk("chunk_5"), "an orphaned temp file is not a chunk");
        // Temp file names contain a dot, so they can never even be asked for as chunk IDs.
        expectThrows(IllegalArgumentException.class, () -> node.hasChunk("chunk_5-1234567890.tmp"));

        node.storeChunk("chunk_5", source);
        node.retrieveChunk("chunk_5", destination);
        checkBytes(data, destination);

        // Orphaned temp files are not cleaned up yet (a documented limitation), so it is still there.
        check(Files.exists(orphan), "the orphaned temp file is left in place");
    }

    // ---------------------------------------------------------------------
    // Stress test (run separately with --stress and -Xmx16m)
    // ---------------------------------------------------------------------

    private static void largeChunkIsStreamed(Path tempDir) throws IOException {
        // Write 64 MiB using one reusable 1 MiB buffer, so the test itself stays small in memory.
        byte[] buffer = new byte[MIB];
        Random random = new Random(12345);
        Path source = tempDir.resolve("large.bin");
        try (OutputStream out = Files.newOutputStream(source)) {
            for (int i = 0; i < 64; i++) {
                random.nextBytes(buffer);
                out.write(buffer);
            }
        }
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        Path destination = tempDir.resolve("retrieved.bin");

        checkEquals(64L * MIB, node.storeChunk("large", source), "bytes stored");
        checkEquals(64L * MIB, node.retrieveChunk("large", destination), "bytes retrieved");
        // Files.mismatch compares the files in small pieces, so it is also memory-safe.
        checkEquals(-1L, Files.mismatch(source, destination), "position of first differing byte (-1 = identical)");
    }

    // ---------------------------------------------------------------------
    // Test stream used with the openForReading hook
    // ---------------------------------------------------------------------

    /** Something a test does partway through a copy. It may throw to simulate a failing disk. */
    private interface IoAction {
        void run() throws IOException;
    }

    /**
     * A stream over fixed bytes that pauses at one position to run a test action.
     *
     * <p>Reads stop just before {@code actionPosition}, so by the time the action runs, every
     * byte before that position has already been handed out and written by the code under test.
     * If the action throws, that read fails, exactly as if the disk had failed at that point.
     */
    private static final class InterruptingStream extends InputStream {
        private final byte[] data;
        private final int actionPosition;
        private final IoAction action;
        private int position = 0;
        private boolean actionDone = false;

        InterruptingStream(byte[] data, int actionPosition, IoAction action) {
            this.data = data;
            this.actionPosition = actionPosition;
            this.action = action;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (position == actionPosition && !actionDone) {
                actionDone = true;
                action.run();
            }
            if (position >= data.length) {
                return -1;
            }
            // Never read past the action position in one go.
            int end = position < actionPosition ? actionPosition : data.length;
            int count = Math.min(length, end - position);
            System.arraycopy(data, position, buffer, offset, count);
            position += count;
            return count;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int count = read(single, 0, 1);
            return count == -1 ? -1 : single[0] & 0xFF;
        }
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
            tempDir = Files.createTempDirectory("storagenode-test-");
            body.run(tempDir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (SkipTest e) {
            skipped++;
            System.out.println("SKIP  " + name);
            System.out.println("      " + e.getMessage());
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
        return expectThrows(expectedType, "", action);
    }

    /** Like expectThrows above, with a short description of the call, used in failure messages. */
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

    // ---------------------------------------------------------------------
    // Data and file helpers
    // ---------------------------------------------------------------------

    /** Returns deterministic pseudo-random bytes; different seeds give different contents. */
    private static byte[] randomBytes(int size, long seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    /** Shows an ID in quotes, with invisible characters made visible, for failure messages. */
    private static String show(String id) {
        return "\"" + id.replace("\t", "\\t").replace("\u0000", "\\0") + "\"";
    }

    /** Thrown by a test that cannot run here; runTest reports it as skipped, never as passed. */
    private static final class SkipTest extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SkipTest(String reason) {
            super(reason);
        }
    }

    /**
     * Creates a symbolic link to a file, or skips the test if this account may not create one
     * (on Windows that needs Developer Mode or admin rights). Any other failure fails the test.
     */
    private static void createFileSymlinkOrSkip(Path link, Path target) throws IOException {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException e) {
            throw new SkipTest("SKIPPED: symbolic links are not supported here (" + e + ")");
        } catch (FileSystemException e) {
            String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
            if (!message.contains("privilege")) {
                throw e;
            }
            throw new SkipTest("SKIPPED: this account may not create symbolic links (Windows needs Developer Mode"
                    + " or admin rights): " + e.getMessage());
        }
    }

    /** Creates a directory junction (mklink /J, no admin rights needed), as DistributedFsDemoTest does. */
    private static void createDirectoryJunction(Path link, Path target) throws IOException {
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
        BasicFileAttributes attributes = Files.readAttributes(link, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        check(attributes.isOther(), "the planted entry is not a junction");
    }

    /** Names of the entries in a directory, sorted, so tests can compare them with a fixed list. */
    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    /** Every file and folder below a directory, as sorted relative paths, to spot anything new. */
    private static List<String> listTree(Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            return paths.map(path -> directory.relativize(path).toString()).sorted().toList();
        }
    }

    /** The temp files ("*.tmp") in a directory, which StorageNode uses while a store is in progress. */
    private static List<Path> listTempFiles(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.filter(path -> path.getFileName().toString().endsWith(".tmp")).toList();
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
