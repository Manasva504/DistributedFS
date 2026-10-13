package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Plain-Java tests for StorageCluster (no JUnit or other libraries).
 *
 * <p>Two modes:
 * <ul>
 *   <li>Normal:  {@code java -cp "out;out-test" com.distributedfs.StorageClusterTest}</li>
 *   <li>Stress:  {@code java -Xmx16m -cp "out;out-test" com.distributedfs.StorageClusterTest --stress}
 *       runs only the 64 MiB test: a file goes through FileChunker, a three-node cluster and
 *       FileReconstructor with a 16 MB heap.</li>
 * </ul>
 *
 * <p>Every node is a real StorageNode in its own folder under the test's temporary directory.
 * Node failures are simulated by deleting a node's folder (a real I/O error) or by overriding
 * StorageNode's openForReading hook (a failure partway through a copy).
 * The program exits with code 1 if any test fails.
 */
public class StorageClusterTest {

    private static final int MIB = 1024 * 1024;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stressMode = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stressMode) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.StorageClusterTest [--stress]");
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
        // Construction
        runTest("a null list or null node is rejected", StorageClusterTest::nullsAreRejected);
        runTest("an empty cluster is rejected", StorageClusterTest::emptyClusterIsRejected);
        runTest("the same node object listed twice is rejected", StorageClusterTest::sameNodeTwiceIsRejected);
        runTest("nodes with overlapping storage directories are rejected",
                StorageClusterTest::overlappingDirectoriesAreRejected);
        runTest("nodeCount is right and later changes to the caller's list have no effect",
                StorageClusterTest::clusterKeepsItsOwnList);

        // Store, placement and retrieve
        runTest("stores go round-robin: nodes 0, 1, 2, 0, 1, 2", StorageClusterTest::roundRobinPlacement);
        runTest("a stored chunk is retrieved byte-for-byte identical", StorageClusterTest::storeAndRetrieve);
        runTest("empty and binary chunks are preserved exactly", StorageClusterTest::emptyAndBinaryChunks);
        runTest("a single-node cluster stores everything on that node", StorageClusterTest::singleNodeCluster);
        runTest("storing copies: the source file is unchanged", StorageClusterTest::sourceIsUnchanged);
        runTest("an invalid source file is rejected before any node is involved",
                StorageClusterTest::invalidSourceIsRejectedFirst);
        runTest("a new cluster over the same folders finds existing chunks (simulated restart)",
                StorageClusterTest::restartFindsExistingChunks);
        runTest("hasChunk is true after a store and false for a chunk never stored", StorageClusterTest::hasChunkAnswers);

        // Duplicates
        runTest("storing an existing ID again fails and changes nothing", StorageClusterTest::duplicateOnSameNode);
        runTest("an ID already on a different node is not stored again", StorageClusterTest::duplicateOnDifferentNode);
        runTest("a chunk that appears on the chosen node mid-store is not stored on another node",
                StorageClusterTest::chunkAppearingDuringStoreIsNotStoredElsewhere);
        runTest("an ID found on two nodes is refused instead of picking a copy",
                StorageClusterTest::duplicatesOnTwoNodesAreRefused);

        // Missing chunks and invalid input
        runTest("a missing chunk throws ChunkNotFoundException and creates nothing", StorageClusterTest::missingChunk);
        runTest("invalid chunk IDs are rejected by every method and create nothing", StorageClusterTest::invalidIdsAreRejected);
        runTest("null arguments are rejected", StorageClusterTest::nullArgumentsAreRejected);
        runTest("a destination inside any node's folder is rejected", StorageClusterTest::destinationInsideAnyNodeIsRejected);
        runTest("retrieving onto an existing file fails and leaves it unchanged",
                StorageClusterTest::existingDestinationIsNotOverwritten);

        // Node failures
        runTest("an unavailable node blocks stores (uniqueness cannot be confirmed)",
                StorageClusterTest::unavailableNodeBlocksStores);
        runTest("an unavailable node does not hide chunks on healthy nodes",
                StorageClusterTest::unavailableNodeDoesNotHideOtherChunks);
        runTest("an unavailable node means 'cannot tell', never 'not found'",
                StorageClusterTest::unavailableNodeMeansCannotTell);
        runTest("a node failing mid-store falls back to the next node", StorageClusterTest::fallbackAfterNodeFailure);
        runTest("when every node fails, all errors are reported and nothing is stored",
                StorageClusterTest::allNodesFail);
        runTest("a node failing mid-retrieve passes its error through and leaves no partial file",
                StorageClusterTest::holderFailsMidRetrieve);

        // Integration
        runTest("FileChunker -> StorageCluster -> FileReconstructor, checked with ChecksumService",
                StorageClusterTest::endToEnd);
    }

    private static void runStressTest() {
        long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
        System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
        if (maxHeapMib >= 64) {
            System.out.println("WARNING: the heap can hold the whole 64 MiB file, so this run does not prove"
                    + " streaming. Run with -Xmx16m.");
        }
        runTest("a 64 MiB file goes through chunker, cluster and reconstructor with a small heap",
                StorageClusterTest::largeFileThroughCluster);
    }

    // ---------------------------------------------------------------------
    // Construction
    // ---------------------------------------------------------------------

    private static void nullsAreRejected(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(nodeDir(tempDir, 0));

        expectThrows(NullPointerException.class, () -> new StorageCluster(null));
        NullPointerException e = expectThrows(NullPointerException.class,
                () -> new StorageCluster(Arrays.asList(node, null)));
        checkContains(e.getMessage(), "nodes[1]");
    }

    private static void emptyClusterIsRejected(Path tempDir) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new StorageCluster(List.of()));
        checkContains(e.getMessage(), "at least one node");
    }

    private static void sameNodeTwiceIsRejected(Path tempDir) throws IOException {
        StorageNode node = new StorageNode(nodeDir(tempDir, 0));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new StorageCluster(List.of(node, node)));
        checkContains(e.getMessage(), "listed twice");
    }

    private static void overlappingDirectoriesAreRejected(Path tempDir) throws IOException {
        Path dirA = tempDir.resolve("node-a");
        StorageNode a = new StorageNode(dirA);
        StorageNode sameFolder = new StorageNode(dirA);              // a second object over the same folder
        StorageNode inside = new StorageNode(dirA.resolve("inner")); // a folder inside node a's folder

        for (List<StorageNode> pair : List.of(List.of(a, sameFolder), List.of(a, inside), List.of(inside, a))) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new StorageCluster(pair));
            checkContains(e.getMessage(), "overlapping storage directories");
        }

        // A sibling whose name merely starts the same way does not overlap.
        StorageNode sibling = new StorageNode(tempDir.resolve("node-a-copy"));
        checkEquals(2, new StorageCluster(List.of(a, sibling)).nodeCount(), "node count with a sibling folder");
    }

    private static void clusterKeepsItsOwnList(Path tempDir) throws IOException {
        List<StorageNode> list = new ArrayList<>(createNodes(tempDir, 2));
        StorageCluster cluster = new StorageCluster(list);

        list.add(new StorageNode(nodeDir(tempDir, 2)));
        checkEquals(2, cluster.nodeCount(), "node count after the caller added a node to their list");
        list.clear();
        checkEquals(2, cluster.nodeCount(), "node count after the caller cleared their list");

        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        checkEquals(0, cluster.locateChunk("chunk_0"), "node holding chunk_0");
    }

    // ---------------------------------------------------------------------
    // Store, placement and retrieve
    // ---------------------------------------------------------------------

    private static void roundRobinPlacement(Path tempDir) throws IOException {
        List<StorageNode> nodes = createNodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes);
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(100, 1));

        for (int i = 0; i < 6; i++) {
            cluster.storeChunk("chunk_" + i, source);
        }

        for (int i = 0; i < 6; i++) {
            checkEquals(i % 3, cluster.locateChunk("chunk_" + i), "node holding chunk_" + i);
            check(nodes.get(i % 3).hasChunk("chunk_" + i), "node " + (i % 3) + " itself should report chunk_" + i);
        }
        for (int n = 0; n < 3; n++) {
            checkEquals(List.of("chunk_" + n, "chunk_" + (n + 3)), listNames(nodeDir(tempDir, n)),
                    "files on node " + n);
        }
    }

    private static void storeAndRetrieve(Path tempDir) throws IOException {
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(100_000, 1));
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        Path destination = tempDir.resolve("retrieved.bin");

        checkEquals(100_000L, cluster.storeChunk("chunk_0", source), "bytes stored");
        checkEquals(100_000L, cluster.retrieveChunk("chunk_0", destination), "bytes retrieved");

        checkEquals(-1L, Files.mismatch(source, destination), "position of first differing byte (-1 = identical)");
    }

    private static void emptyAndBinaryChunks(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 2));
        Path empty = Files.write(tempDir.resolve("empty.bin"), new byte[0]);
        byte[] binary = new byte[256 + 6];
        for (int i = 0; i < 256; i++) {
            binary[i] = (byte) i;
        }
        byte[] tricky = {'\r', '\n', 0x00, (byte) 0xEF, (byte) 0xBB, (byte) 0xBF}; // CR/LF, NUL, UTF-8 BOM
        System.arraycopy(tricky, 0, binary, 256, tricky.length);
        Path binarySource = Files.write(tempDir.resolve("binary.bin"), binary);

        checkEquals(0L, cluster.storeChunk("empty", empty), "bytes stored for the empty chunk");
        cluster.storeChunk("binary", binarySource);

        Path emptyCopy = tempDir.resolve("empty-copy.bin");
        Path binaryCopy = tempDir.resolve("binary-copy.bin");
        checkEquals(0L, cluster.retrieveChunk("empty", emptyCopy), "bytes retrieved for the empty chunk");
        cluster.retrieveChunk("binary", binaryCopy);
        checkEquals(0L, Files.size(emptyCopy), "size of the retrieved empty chunk");
        checkEquals(-1L, Files.mismatch(binarySource, binaryCopy), "position of first differing byte (-1 = identical)");
    }

    private static void singleNodeCluster(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 1));
        byte[] data = randomBytes(100, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);

        for (int i = 0; i < 3; i++) {
            cluster.storeChunk("chunk_" + i, source);
            checkEquals(0, cluster.locateChunk("chunk_" + i), "node holding chunk_" + i);
        }
        Path destination = tempDir.resolve("retrieved.bin");
        cluster.retrieveChunk("chunk_2", destination);
        checkBytes(data, destination);
    }

    private static void sourceIsUnchanged(Path tempDir) throws IOException {
        byte[] data = randomBytes(200_000, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);
        FileTime originalTime = FileTime.fromMillis(1_000_000_000_000L);
        Files.setLastModifiedTime(source, originalTime);

        new StorageCluster(createNodes(tempDir, 3)).storeChunk("chunk_0", source);

        checkBytes(data, source);
        checkEquals(originalTime, Files.getLastModifiedTime(source), "source file last-modified time");
    }

    private static void invalidSourceIsRejectedFirst(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        Path missing = tempDir.resolve("missing.bin");
        Path directory = Files.createDirectory(tempDir.resolve("some-dir"));
        List<String> before = listTree(tempDir);

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class,
                () -> cluster.storeChunk("chunk_0", missing));
        checkContains(e1.getMessage(), "does not exist");
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class,
                () -> cluster.storeChunk("chunk_0", directory));
        checkContains(e2.getMessage(), "is a directory");
        checkEquals(before, listTree(tempDir), "files and folders (nothing stored on any node)");

        // Rejected stores do not use up a round-robin turn.
        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("valid.bin"), randomBytes(10, 1)));
        checkEquals(0, cluster.locateChunk("chunk_0"), "node holding the first successful store");

        // The source is checked before any node is asked anything: even with node 1 unavailable,
        // a missing source is reported as a bad argument, not as a node problem.
        deleteRecursively(nodeDir(tempDir, 1));
        IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class,
                () -> cluster.storeChunk("chunk_1", missing));
        checkContains(e3.getMessage(), "does not exist");
    }

    private static void restartFindsExistingChunks(Path tempDir) throws IOException {
        StorageCluster first = new StorageCluster(createNodes(tempDir, 3));
        List<byte[]> contents = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            byte[] data = randomBytes(100 + i, i);
            contents.add(data);
            first.storeChunk("chunk_" + i, Files.write(tempDir.resolve("source_" + i), data));
        }

        // New StorageNode objects and a new cluster over the same folders, like a restarted process.
        StorageCluster restarted = new StorageCluster(createNodes(tempDir, 3));

        for (int i = 0; i < 3; i++) {
            checkEquals(i, restarted.locateChunk("chunk_" + i), "node holding chunk_" + i + " after the restart");
            Path destination = tempDir.resolve("retrieved_" + i);
            restarted.retrieveChunk("chunk_" + i, destination);
            checkBytes(contents.get(i), destination);
        }
    }

    private static void hasChunkAnswers(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));

        check(cluster.hasChunk("chunk_0"), "hasChunk should be true for a stored chunk");
        check(!cluster.hasChunk("chunk_1"), "hasChunk should be false for a chunk never stored");
    }

    // ---------------------------------------------------------------------
    // Duplicates
    // ---------------------------------------------------------------------

    private static void duplicateOnSameNode(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        byte[] firstData = randomBytes(100, 1);
        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("first.bin"), firstData));
        Path secondSource = Files.write(tempDir.resolve("second.bin"), randomBytes(100, 2));
        List<String> before = listTree(tempDir);

        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class,
                () -> cluster.storeChunk("chunk_0", secondSource));

        checkContains(e.getMessage(), "already exists on node 0");
        checkBytes(firstData, nodeDir(tempDir, 0).resolve("chunk_0"));
        checkEquals(before, listTree(tempDir), "files and folders (nothing written to any node)");
    }

    private static void duplicateOnDifferentNode(Path tempDir) throws IOException {
        List<StorageNode> nodes = createNodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes);
        byte[] directData = "stored directly on node 2".getBytes(StandardCharsets.UTF_8);
        // Stored through the node itself, bypassing the cluster.
        nodes.get(2).storeChunk("chunk_0", Files.write(tempDir.resolve("direct.bin"), directData));
        Path clusterSource = Files.write(tempDir.resolve("cluster.bin"), randomBytes(100, 1));

        // Round-robin would pick node 0, but the ID already lives on node 2.
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class,
                () -> cluster.storeChunk("chunk_0", clusterSource));

        checkContains(e.getMessage(), "already exists on node 2");
        checkEquals(List.of(), listNames(nodeDir(tempDir, 0)), "files on node 0");
        checkEquals(List.of(), listNames(nodeDir(tempDir, 1)), "files on node 1");
        checkBytes(directData, nodeDir(tempDir, 2).resolve("chunk_0"));

        // The refused store did not use up a round-robin turn.
        cluster.storeChunk("chunk_1", clusterSource);
        checkEquals(0, cluster.locateChunk("chunk_1"), "node holding chunk_1");
    }

    private static void chunkAppearingDuringStoreIsNotStoredElsewhere(Path tempDir) throws IOException {
        byte[] otherWritersData = "written by another writer".getBytes(StandardCharsets.UTF_8);
        Path node0Chunk = nodeDir(tempDir, 0).resolve("chunk_0");
        // Node 0 sees chunk_0 appear after the cluster's check, just before its own final rename,
        // so node 0 reports FileAlreadyExistsException in the middle of the store.
        StorageNode node0 = new StorageNode(nodeDir(tempDir, 0)) {
            @Override
            InputStream openForReading(Path file) throws IOException {
                return actionAtEnd(Files.readAllBytes(file), () -> Files.write(node0Chunk, otherWritersData));
            }
        };
        StorageCluster cluster = new StorageCluster(
                List.of(node0, new StorageNode(nodeDir(tempDir, 1)), new StorageNode(nodeDir(tempDir, 2))));
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(1000, 1));

        expectThrows(FileAlreadyExistsException.class, () -> cluster.storeChunk("chunk_0", source));

        checkBytes(otherWritersData, node0Chunk);
        checkEquals(List.of("chunk_0"), listNames(nodeDir(tempDir, 0)), "files on node 0 (no temp file left)");
        // Trying another node here would have created a second copy of chunk_0.
        checkEquals(List.of(), listNames(nodeDir(tempDir, 1)), "files on node 1");
        checkEquals(List.of(), listNames(nodeDir(tempDir, 2)), "files on node 2");
    }

    private static void duplicatesOnTwoNodesAreRefused(Path tempDir) throws IOException {
        List<StorageNode> nodes = createNodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes);
        // Two different copies of "x", planted by bypassing the cluster.
        nodes.get(0).storeChunk("x", Files.write(tempDir.resolve("a.bin"), randomBytes(50, 1)));
        nodes.get(2).storeChunk("x", Files.write(tempDir.resolve("b.bin"), randomBytes(50, 2)));
        Path destination = tempDir.resolve("retrieved.bin");

        IOException e1 = expectThrows(IOException.class, () -> cluster.locateChunk("x"));
        checkContains(e1.getMessage(), "more than one node (nodes [0, 2])");
        check(!(e1 instanceof ChunkNotFoundException), "two copies must not be reported as a missing chunk");

        IOException e2 = expectThrows(IOException.class, () -> cluster.retrieveChunk("x", destination));
        checkContains(e2.getMessage(), "more than one node");
        check(!Files.exists(destination), "no destination should be created when the copy is ambiguous");

        check(cluster.hasChunk("x"), "hasChunk should still be true: the chunk is stored");
    }

    // ---------------------------------------------------------------------
    // Missing chunks and invalid input
    // ---------------------------------------------------------------------

    private static void missingChunk(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1)));
        Path destination = tempDir.resolve("out").resolve("sub").resolve("chunk.bin");

        ChunkNotFoundException e = expectThrows(ChunkNotFoundException.class,
                () -> cluster.retrieveChunk("chunk_9", destination));

        checkEquals("chunk_9", e.getChunkId(), "chunk ID in the exception");
        checkContains(e.getMessage(), "not stored on any of the 3 nodes");
        expectThrows(ChunkNotFoundException.class, () -> cluster.locateChunk("chunk_9"));
        check(!cluster.hasChunk("chunk_9"), "hasChunk should be false for a missing chunk");
        check(!Files.exists(tempDir.resolve("out")), "no destination folders should be created for a missing chunk");
    }

    private static void invalidIdsAreRejected(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1));
        Path destination = tempDir.resolve("retrieved.bin");
        List<String> before = listTree(tempDir);

        String[] invalidIds = {"", "Chunk_0", "a.b", "../x", "a/b", "con"};
        for (String id : invalidIds) {
            String shown = "\"" + id + "\"";
            expectThrows(IllegalArgumentException.class, "hasChunk(" + shown + ")", () -> cluster.hasChunk(id));
            expectThrows(IllegalArgumentException.class, "locateChunk(" + shown + ")", () -> cluster.locateChunk(id));
            expectThrows(IllegalArgumentException.class, "retrieveChunk(" + shown + ")",
                    () -> cluster.retrieveChunk(id, destination));
            expectThrows(IllegalArgumentException.class, "storeChunk(" + shown + ")",
                    () -> cluster.storeChunk(id, source));
        }

        checkEquals(before, listTree(tempDir), "files and folders (nothing created anywhere)");
    }

    private static void nullArgumentsAreRejected(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 2));
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(10, 1));
        Path destination = tempDir.resolve("retrieved.bin");

        expectThrows(NullPointerException.class, () -> cluster.storeChunk(null, source));
        expectThrows(NullPointerException.class, () -> cluster.storeChunk("chunk_0", null));
        expectThrows(NullPointerException.class, () -> cluster.retrieveChunk(null, destination));
        expectThrows(NullPointerException.class, () -> cluster.retrieveChunk("chunk_0", null));
        expectThrows(NullPointerException.class, () -> cluster.hasChunk(null));
        expectThrows(NullPointerException.class, () -> cluster.locateChunk(null));

        check(!Files.exists(destination), "destination should not be created when arguments are null");
    }

    private static void destinationInsideAnyNodeIsRejected(Path tempDir) throws IOException {
        List<StorageNode> nodes = createNodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes);
        byte[] data = randomBytes(100, 1);
        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data)); // lands on node 0
        List<String> before = listTree(tempDir);

        Path[] insideDestinations = {
            nodeDir(tempDir, 1).resolve("copy"),                     // another node's folder
            nodeDir(tempDir, 0).resolve("copy"),                     // the holder's own folder
            nodeDir(tempDir, 2).resolve("sub").resolve("copy"),      // a nested folder of another node
            nodeDir(tempDir, 1),                                     // a node folder itself
            tempDir.resolve("elsewhere").resolve("..").resolve("node-2").resolve("copy"), // disguised with ".."
        };
        for (Path destination : insideDestinations) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    "retrieveChunk to " + destination, () -> cluster.retrieveChunk("chunk_0", destination));
            checkContains(e.getMessage(), "outside every node's storage directory");
        }

        checkEquals(before, listTree(tempDir), "files and folders (nothing created)");
        check(!nodes.get(1).hasChunk("copy"), "node 1 must not gain a chunk named copy");
        checkBytes(data, nodeDir(tempDir, 0).resolve("chunk_0"));

        // A folder whose name merely starts like a node folder is outside every node.
        Path sibling = tempDir.resolve("node-1-copy").resolve("chunk_0");
        cluster.retrieveChunk("chunk_0", sibling);
        checkBytes(data, sibling);
    }

    private static void existingDestinationIsNotOverwritten(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 2));
        cluster.storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), randomBytes(100, 1)));
        byte[] existingContent = "do not overwrite me".getBytes(StandardCharsets.UTF_8);
        Path existing = Files.write(tempDir.resolve("existing.bin"), existingContent);

        expectThrows(FileAlreadyExistsException.class, () -> cluster.retrieveChunk("chunk_0", existing));

        checkBytes(existingContent, existing);
    }

    // ---------------------------------------------------------------------
    // Node failures
    // ---------------------------------------------------------------------

    private static void unavailableNodeBlocksStores(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(100, 1));
        // Node 1 loses its storage after startup, for example an unplugged disk.
        deleteRecursively(nodeDir(tempDir, 1));

        IOException e = expectThrows(IOException.class, () -> cluster.storeChunk("chunk_0", source));

        check(!(e instanceof FileAlreadyExistsException), "an unavailable node must not be reported as a duplicate");
        checkContains(e.getMessage(), "node 1 could not confirm that the ID is unused");
        checkEquals(1, e.getSuppressed().length, "number of node errors attached");
        checkContains(e.getSuppressed()[0].getMessage(), "not available");
        checkEquals(List.of(), listNames(nodeDir(tempDir, 0)), "files on node 0");
        checkEquals(List.of(), listNames(nodeDir(tempDir, 2)), "files on node 2");
        check(!Files.exists(nodeDir(tempDir, 1)), "the cluster must not recreate node 1's folder");
    }

    private static void unavailableNodeDoesNotHideOtherChunks(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        byte[] data0 = randomBytes(100, 0);
        for (int i = 0; i < 3; i++) {
            cluster.storeChunk("chunk_" + i, Files.write(tempDir.resolve("source_" + i), randomBytes(100, i)));
        }
        deleteRecursively(nodeDir(tempDir, 1));
        Path destination = tempDir.resolve("retrieved.bin");

        cluster.retrieveChunk("chunk_0", destination);

        checkBytes(data0, destination);
        check(cluster.hasChunk("chunk_2"), "chunk_2 on healthy node 2 should still be found");
        checkEquals(2, cluster.locateChunk("chunk_2"), "node holding chunk_2");
    }

    private static void unavailableNodeMeansCannotTell(Path tempDir) throws IOException {
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));
        for (int i = 0; i < 3; i++) {
            cluster.storeChunk("chunk_" + i, Files.write(tempDir.resolve("source_" + i), randomBytes(100, i)));
        }
        deleteRecursively(nodeDir(tempDir, 1));
        Path destination = tempDir.resolve("retrieved.bin");

        // chunk_1 was on the lost node; chunk_9 was never stored. With node 1 unreadable, the
        // cluster cannot tell these apart, so neither may be reported as simply missing.
        for (String id : List.of("chunk_1", "chunk_9")) {
            IOException e1 = expectThrows(IOException.class, "hasChunk(" + id + ")", () -> cluster.hasChunk(id));
            checkContains(e1.getMessage(), "node 1 could not be checked");
            IOException e2 = expectThrows(IOException.class, "locateChunk(" + id + ")", () -> cluster.locateChunk(id));
            check(!(e2 instanceof ChunkNotFoundException), "locateChunk(" + id + ") must not say 'not found'");
            checkEquals(1, e2.getSuppressed().length, "number of node errors attached");
            IOException e3 = expectThrows(IOException.class, "retrieveChunk(" + id + ")",
                    () -> cluster.retrieveChunk(id, destination));
            check(!(e3 instanceof ChunkNotFoundException), "retrieveChunk(" + id + ") must not say 'not found'");
        }
        check(!Files.exists(destination), "no destination should be created");
    }

    private static void fallbackAfterNodeFailure(Path tempDir) throws IOException {
        IOException failure = new IOException("simulated disk error on node 0");
        boolean[] failing = {true};
        StorageCluster cluster = new StorageCluster(List.of(
                failingNode(nodeDir(tempDir, 0), failure, failing),
                new StorageNode(nodeDir(tempDir, 1)),
                new StorageNode(nodeDir(tempDir, 2))));
        byte[] data = randomBytes(1000, 1);
        Path source = Files.write(tempDir.resolve("source.bin"), data);

        checkEquals(1000L, cluster.storeChunk("chunk_0", source), "bytes stored");

        checkEquals(1, cluster.locateChunk("chunk_0"), "node holding chunk_0 (node 0 failed, so node 1)");
        checkBytes(data, nodeDir(tempDir, 1).resolve("chunk_0"));
        checkEquals(List.of(), listNames(nodeDir(tempDir, 0)), "files on node 0 (no chunk and no temp file)");

        // Round-robin continues after the node that succeeded.
        cluster.storeChunk("chunk_1", source);
        checkEquals(2, cluster.locateChunk("chunk_1"), "node holding chunk_1");
    }

    private static void allNodesFail(Path tempDir) throws IOException {
        IOException[] failures = {
            new IOException("simulated disk error on node 0"),
            new IOException("simulated disk error on node 1"),
            new IOException("simulated disk error on node 2"),
        };
        boolean[] failing = {true};
        List<StorageNode> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes.add(failingNode(nodeDir(tempDir, i), failures[i], failing));
        }
        StorageCluster cluster = new StorageCluster(nodes);
        Path source = Files.write(tempDir.resolve("source.bin"), randomBytes(1000, 1));

        IOException e = expectThrows(IOException.class, () -> cluster.storeChunk("chunk_0", source));

        checkContains(e.getMessage(), "No node could store chunk");
        checkEquals(3, e.getSuppressed().length, "number of node errors attached");
        for (int i = 0; i < 3; i++) {
            check(e.getSuppressed()[i] == failures[i], "node " + i + "'s original error should be attached, in order");
            checkEquals(List.of(), listNames(nodeDir(tempDir, i)), "files on node " + i + " (no chunk and no temp file)");
        }

        // The round-robin position did not move: once the disks work again, the next store goes to node 0.
        failing[0] = false;
        cluster.storeChunk("chunk_0", source);
        checkEquals(0, cluster.locateChunk("chunk_0"), "node holding chunk_0 after the disks recover");
    }

    private static void holderFailsMidRetrieve(Path tempDir) throws IOException {
        byte[] data = randomBytes(1000, 1);
        Path dir0 = nodeDir(tempDir, 0);
        // Store through a plain node object first; the cluster below uses a failing object for the same folder.
        new StorageNode(dir0).storeChunk("chunk_0", Files.write(tempDir.resolve("source.bin"), data));
        IOException failure = new IOException("simulated read error on node 0");
        boolean[] failing = {true};
        StorageCluster cluster = new StorageCluster(
                List.of(failingNode(dir0, failure, failing), new StorageNode(nodeDir(tempDir, 1))));
        Path destination = tempDir.resolve("retrieved.bin");

        IOException e = expectThrows(IOException.class, () -> cluster.retrieveChunk("chunk_0", destination));

        check(e == failure, "expected the node's original exception, but got " + e);
        checkEquals(0, e.getSuppressed().length, "number of suppressed cleanup errors");
        check(!Files.exists(destination), "the partial destination should have been deleted");
        checkBytes(data, dir0.resolve("chunk_0"));
    }

    // ---------------------------------------------------------------------
    // Integration
    // ---------------------------------------------------------------------

    private static void endToEnd(Path tempDir) throws IOException {
        Path original = Files.write(tempDir.resolve("original.bin"), randomBytes(2 * MIB + 12_345, 7));
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));

        Path rebuilt = sendThroughCluster(original, cluster, tempDir);

        // FileChunker made chunk_0, chunk_1 and chunk_2; round-robin spread them over all three nodes.
        for (int i = 0; i < 3; i++) {
            checkEquals(i, cluster.locateChunk("chunk_" + i), "node holding chunk_" + i);
        }
        ChecksumService checksums = new ChecksumService();
        check(checksums.matches(rebuilt, checksums.calculateSha256(original)),
                "the rebuilt file should have the same SHA-256 as the original");
        checkEquals(-1L, Files.mismatch(original, rebuilt), "position of first differing byte (-1 = identical)");
    }

    /**
     * Splits a file with FileChunker, stores every chunk through the cluster under its file name,
     * retrieves them all, and rebuilds the file with FileReconstructor. Returns the rebuilt file.
     */
    private static Path sendThroughCluster(Path original, StorageCluster cluster, Path tempDir) throws IOException {
        List<Path> chunks = new FileChunker().chunk(original, tempDir.resolve("chunks"));
        for (Path chunk : chunks) {
            cluster.storeChunk(chunk.getFileName().toString(), chunk);
        }

        List<Path> retrieved = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            Path copy = tempDir.resolve("retrieved").resolve("chunk_" + i);
            cluster.retrieveChunk("chunk_" + i, copy);
            retrieved.add(copy);
        }
        Path rebuilt = tempDir.resolve("rebuilt.bin");
        new FileReconstructor().reconstruct(retrieved, rebuilt);
        return rebuilt;
    }

    // ---------------------------------------------------------------------
    // Stress test (run separately with --stress and -Xmx16m)
    // ---------------------------------------------------------------------

    private static void largeFileThroughCluster(Path tempDir) throws IOException {
        // Write 64 MiB using one reusable 1 MiB buffer, so the test itself stays small in memory.
        byte[] buffer = new byte[MIB];
        Random random = new Random(12345);
        Path original = tempDir.resolve("large.bin");
        try (OutputStream out = Files.newOutputStream(original)) {
            for (int i = 0; i < 64; i++) {
                random.nextBytes(buffer);
                out.write(buffer);
            }
        }
        StorageCluster cluster = new StorageCluster(createNodes(tempDir, 3));

        Path rebuilt = sendThroughCluster(original, cluster, tempDir);

        for (int n = 0; n < 3; n++) {
            check(!listNames(nodeDir(tempDir, n)).isEmpty(), "node " + n + " should hold some of the 64 chunks");
        }
        ChecksumService checksums = new ChecksumService();
        check(checksums.matches(rebuilt, checksums.calculateSha256(original)),
                "the rebuilt file should have the same SHA-256 as the original");
    }

    // ---------------------------------------------------------------------
    // Nodes and test streams
    // ---------------------------------------------------------------------

    /** The folder of node i inside the test's temporary directory. */
    private static Path nodeDir(Path tempDir, int index) {
        return tempDir.resolve("node-" + index);
    }

    /** Creates {@code count} real StorageNodes in tempDir/node-0, tempDir/node-1, ... */
    private static List<StorageNode> createNodes(Path tempDir, int count) throws IOException {
        List<StorageNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(new StorageNode(nodeDir(tempDir, i)));
        }
        return nodes;
    }

    /**
     * A real StorageNode whose reads fail with {@code failure} after 10 bytes while
     * {@code failSwitch[0]} is true, like a disk failing partway through a copy. It uses
     * StorageNode's package-private openForReading hook.
     */
    private static StorageNode failingNode(Path dir, IOException failure, boolean[] failSwitch) throws IOException {
        return new StorageNode(dir) {
            @Override
            InputStream openForReading(Path file) throws IOException {
                if (!failSwitch[0]) {
                    return super.openForReading(file);
                }
                return failAfter(Files.readAllBytes(file), 10, failure);
            }
        };
    }

    /** Something a test does at a chosen moment during a copy. */
    private interface IoAction {
        void run() throws IOException;
    }

    /** A stream that delivers {@code bytesBeforeFailure} bytes of data, then throws {@code failure} on every read. */
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

    /** A stream over data that runs {@code action} once, when the reader reaches the end. */
    private static InputStream actionAtEnd(byte[] data, IoAction action) {
        return new InputStream() {
            private int position = 0;
            private boolean actionDone = false;

            @Override
            public int read() throws IOException {
                if (position < data.length) {
                    return data[position++] & 0xFF;
                }
                if (!actionDone) {
                    actionDone = true;
                    action.run();
                }
                return -1;
            }
        };
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
            tempDir = Files.createTempDirectory("storagecluster-test-");
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
