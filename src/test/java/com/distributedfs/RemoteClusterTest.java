package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.createDirectoryJunction;
import static com.distributedfs.NodeTestKit.expectCannotTell;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.fastConfig;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.reply;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Plain-Java tests for StorageCluster over remote nodes: the cluster's rules (one node per chunk
 * ID, "cannot tell" is never "absent", deleting only with every node answering, never trying
 * another node after a store with an unknown outcome) re-tested with node servers in this process.
 * Run: {@code java -cp "out;out-test" com.distributedfs.RemoteClusterTest}.
 */
public class RemoteClusterTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("remotecluster-test-");

    private static final String STUB_ID = "0123456789abcdef0123456789abcdef";

    public static void main(String[] args) {
        RUNNER.run("chunks are placed round-robin over three remote nodes and read back byte for byte",
                RemoteClusterTest::roundRobinOverRemoteNodes);
        RUNNER.run("storing an ID that one remote node holds is refused, and no other node gets a copy",
                RemoteClusterTest::duplicateIsRefused);
        RUNNER.run("with one node down, stores skip it, checks cannot tell, deletes delete nothing,"
                + " but chunks on the other nodes can still be read", RemoteClusterTest::oneNodeDown);
        RUNNER.run("a store with an unknown outcome is not retried on another node", RemoteClusterTest::ambiguousStoreIsNotRetried);
        RUNNER.run("a store that a node certainly refused moves on to the next node", RemoteClusterTest::definiteFailureFallsBack);
        RUNNER.run("a chunk found on two remote nodes is neither read nor deleted", RemoteClusterTest::twoCopiesAreRefused);
        RUNNER.run("the same node reached twice is refused when the cluster is built", RemoteClusterTest::sameNodeTwiceIsRefused);
        RUNNER.run("a node answering with another node's ID counts as 'cannot tell'", RemoteClusterTest::foreignNodeCannotConfirm);
        RUNNER.run("a junction under a chunk's ID on a remote node blocks deleting, and nothing is deleted",
                RemoteClusterTest::junctionBlocksRemoteDelete);
        RUNNER.run("a cluster can mix local and remote nodes", RemoteClusterTest::mixedCluster);
        RUNNER.finish();
    }

    private static void roundRobinOverRemoteNodes(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 3)) {
            StorageCluster cluster = new StorageCluster(nodes.remotes);
            List<byte[]> contents = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                byte[] data = randomBytes(10_000 + i, i);
                contents.add(data);
                cluster.storeChunk("file_" + i, Files.write(tempDir.resolve("source-" + i), data));
            }
            for (int node = 0; node < 3; node++) {
                checkEquals(List.of("file_" + node, "file_" + (node + 3)), listNames(nodes.handles.get(node).chunksDir()),
                        "chunks on node " + node);
            }
            for (int i = 0; i < 6; i++) {
                Path copy = tempDir.resolve("copy-" + i);
                cluster.retrieveChunk("file_" + i, copy);
                check(Arrays.equals(contents.get(i), Files.readAllBytes(copy)), "chunk " + i + " differs");
                checkEquals(i % 3, cluster.locateChunk("file_" + i), "node of chunk " + i);
            }
        }
    }

    private static void duplicateIsRefused(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 3)) {
            Path source = Files.write(tempDir.resolve("source"), randomBytes(100, 1));
            nodes.remotes.get(1).storeChunk("file_0", source);
            StorageCluster cluster = new StorageCluster(nodes.remotes);
            FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, "store",
                    () -> cluster.storeChunk("file_0", source));
            checkContains(e.getMessage(), "node 1");
            checkEquals(List.of(), listNames(nodes.handles.get(0).chunksDir()), "node 0");
            checkEquals(List.of(), listNames(nodes.handles.get(2).chunksDir()), "node 2");
        }
    }

    private static void oneNodeDown(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 3)) {
            Path source = Files.write(tempDir.resolve("source"), randomBytes(100, 2));
            nodes.remotes.get(0).storeChunk("file_0", source);
            StorageCluster cluster = new StorageCluster(nodes.remotes);
            nodes.handles.get(2).close(); // node 2 stops: its port refuses connections

            // A node that cannot be reached at all is skipped by the duplicate check (milestone M2):
            // chunk IDs are unique anyway, and the store goes to the reachable nodes.
            StorageCluster.StoreResult stored = cluster.storeReplicated("file_1", source);
            checkEquals(List.of(2), stored.skipped(), "skipped unreachable nodes");
            checkEquals(List.of("file_0", "file_1"), listNames(nodes.handles.get(0).chunksDir()), "node 0 after the store");
            checkEquals(List.of(), listNames(nodes.handles.get(1).chunksDir()), "node 1 after the store");

            expectCannotTell("hasChunk of a chunk no reachable node holds", () -> cluster.hasChunk("file_9"));
            expectCannotTell("locate of a chunk no reachable node holds", () -> cluster.locateChunk("file_9"));
            expectCannotTell("retrieve of a chunk no reachable node holds", () -> cluster.retrieveChunk("file_9",
                    tempDir.resolve("copy-1")));
            expectCannotTell("delete", () -> cluster.deleteChunk("file_0"));
            checkEquals(List.of("file_0", "file_1"), listNames(nodes.handles.get(0).chunksDir()), "nothing was deleted");

            checkEquals(true, cluster.hasChunk("file_0"), "a chunk a reachable node holds");
            Path copy = tempDir.resolve("copy-0");
            cluster.retrieveChunk("file_0", copy);
            check(Arrays.equals(Files.readAllBytes(source), Files.readAllBytes(copy)), "the copy differs");
        }
    }

    private static void ambiguousStoreIsNotRetried(Path tempDir) throws Exception {
        // Node 0 answers every check with "absent", but drops the connection after receiving a store.
        try (Nodes nodes = Nodes.start(tempDir, 2);
             NodeTestKit.SocketStub flaky = new NodeTestKit.SocketStub(socket -> {
                 NodeTestKit.Request request = NodeTestKit.readRequest(socket);
                 if (request.isMethod("POST")) {
                     NodeTestKit.reset(socket);
                 } else {
                     NodeTestKit.writeReply(socket, 200, STUB_ID, "exists=false\n");
                 }
             });
             RemoteStorageNode flakyNode = new RemoteStorageNode(flaky.port(), STUB_ID, fastConfig())) {
            List<ChunkStore> members = new ArrayList<>();
            members.add(flakyNode);
            members.addAll(nodes.remotes);
            StorageCluster cluster = new StorageCluster(members);
            Path source = Files.write(tempDir.resolve("source"), randomBytes(1000, 3));
            expectThrows(AmbiguousStoreException.class, "store", () -> cluster.storeChunk("file_0", source));
            checkEquals(List.of(), listNames(nodes.handles.get(0).chunksDir()), "node 1 got no copy");
            checkEquals(List.of(), listNames(nodes.handles.get(1).chunksDir()), "node 2 got no copy");
        }
    }

    private static void definiteFailureFallsBack(Path tempDir) throws Exception {
        // Node 0 answers every check with "absent" and refuses every store as busy (nothing stored).
        try (Nodes nodes = Nodes.start(tempDir, 2);
             NodeTestKit.Stub busy = new NodeTestKit.Stub(exchange -> {
                 if (exchange.getRequestMethod().equals("POST")) {
                     reply(exchange, 503, STUB_ID, "error=node_busy\n");
                 } else {
                     reply(exchange, 200, STUB_ID, "exists=false\n");
                 }
             });
             RemoteStorageNode busyNode = new RemoteStorageNode(busy.port(), STUB_ID, fastConfig())) {
            List<ChunkStore> members = new ArrayList<>();
            members.add(busyNode);
            members.addAll(nodes.remotes);
            StorageCluster cluster = new StorageCluster(members);
            Path source = Files.write(tempDir.resolve("source"), randomBytes(1000, 4));
            checkEquals(1000L, cluster.storeChunk("file_0", source), "bytes stored");
            checkEquals(List.of("file_0"), listNames(nodes.handles.get(0).chunksDir()), "the next node got the chunk");
            checkEquals(List.of(), listNames(nodes.handles.get(1).chunksDir()), "the node after it");
            cluster.storeChunk("file_1", source);
            checkEquals(List.of("file_1"), listNames(nodes.handles.get(1).chunksDir()), "round-robin went on");
        }
    }

    private static void twoCopiesAreRefused(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 3)) {
            Path source = Files.write(tempDir.resolve("source"), randomBytes(100, 5));
            nodes.remotes.get(0).storeChunk("file_0", source);
            nodes.remotes.get(2).storeChunk("file_0", source);
            StorageCluster cluster = new StorageCluster(nodes.remotes);
            checkContains(expectCannotTell("locate", () -> cluster.locateChunk("file_0")).getMessage(), "more than one node");
            checkContains(expectCannotTell("delete", () -> cluster.deleteChunk("file_0")).getMessage(), "more than one node");
            checkEquals(List.of("file_0"), listNames(nodes.handles.get(0).chunksDir()), "node 0 kept its copy");
            checkEquals(List.of("file_0"), listNames(nodes.handles.get(2).chunksDir()), "node 2 kept its copy");
        }
    }

    private static void sameNodeTwiceIsRefused(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 1);
             RemoteStorageNode again = RemoteStorageNode.connect(nodes.handles.get(0).port(), fastConfig())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, "cluster",
                    () -> new StorageCluster(List.of(nodes.remotes.get(0), again)));
            checkContains(e.getMessage(), "same storage node");
        }
    }

    private static void foreignNodeCannotConfirm(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 1);
             NodeTestKit.Stub impostor = new NodeTestKit.Stub(exchange ->
                     reply(exchange, 200, "fedcba9876543210fedcba9876543210", "exists=false\n"));
             RemoteStorageNode expected = new RemoteStorageNode(impostor.port(), STUB_ID, fastConfig())) {
            StorageCluster cluster = new StorageCluster(List.of(nodes.remotes.get(0), expected));
            Path source = Files.write(tempDir.resolve("source"), randomBytes(100, 6));
            checkContains(expectCannotTell("store", () -> cluster.storeChunk("file_0", source)).getMessage(),
                    "node 1 could not confirm");
            expectCannotTell("hasChunk", () -> cluster.hasChunk("file_0"));
            checkEquals(List.of(), listNames(nodes.handles.get(0).chunksDir()), "nothing was stored");
        }
    }

    private static void junctionBlocksRemoteDelete(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 2)) {
            Path outside = Files.createDirectory(tempDir.resolve("outside"));
            Files.write(outside.resolve("keep"), new byte[] {1});
            Path entry = nodes.handles.get(1).chunksDir().resolve("file_0");
            createDirectoryJunction(entry, outside);
            try {
                StorageCluster cluster = new StorageCluster(nodes.remotes);
                expectCannotTell("delete", () -> cluster.deleteChunk("file_0"));
                check(Files.exists(entry, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
                checkEquals(List.of("keep"), listNames(outside), "the folder behind it is untouched");
            } finally {
                Files.deleteIfExists(entry);
            }
        }
    }

    private static void mixedCluster(Path tempDir) throws Exception {
        try (Nodes nodes = Nodes.start(tempDir, 1)) {
            StorageNode local = new StorageNode(tempDir.resolve("local-node"));
            StorageCluster cluster = new StorageCluster(List.of(local, nodes.remotes.get(0)));
            Path source = Files.write(tempDir.resolve("source"), randomBytes(500, 7));
            cluster.storeChunk("file_0", source);
            cluster.storeChunk("file_1", source);
            checkEquals(List.of("file_0"), listNames(tempDir.resolve("local-node")), "the local node");
            checkEquals(List.of("file_1"), listNames(nodes.handles.get(0).chunksDir()), "the remote node");
            checkEquals(true, cluster.deleteChunk("file_1"), "delete on the remote node");
            expectThrows(IllegalArgumentException.class, "a destination inside the local node's folder",
                    () -> cluster.retrieveChunk("file_0", tempDir.resolve("local-node").resolve("copy")));
            Path copy = tempDir.resolve("copy");
            cluster.retrieveChunk("file_0", copy);
            check(Arrays.equals(Files.readAllBytes(source), Files.readAllBytes(copy)), "the copy differs");
        }
    }

    /** Node servers in this process, each with a client pinned to its ID. */
    private static final class Nodes implements AutoCloseable {
        final List<NodeTestKit.NodeHandle> handles = new ArrayList<>();
        final List<RemoteStorageNode> remotes = new ArrayList<>();

        static Nodes start(Path tempDir, int count) throws Exception {
            Nodes nodes = new Nodes();
            try {
                for (int i = 0; i < count; i++) {
                    NodeTestKit.NodeHandle handle = NodeTestKit.startNode(tempDir.resolve("node-" + i));
                    nodes.handles.add(handle);
                    nodes.remotes.add(RemoteStorageNode.connect(handle.port(), fastConfig()));
                }
                return nodes;
            } catch (Exception e) {
                nodes.close();
                throw e;
            }
        }

        @Override
        public void close() {
            for (RemoteStorageNode remote : remotes) {
                remote.close();
            }
            for (NodeTestKit.NodeHandle handle : handles) {
                handle.close();
            }
        }
    }
}
