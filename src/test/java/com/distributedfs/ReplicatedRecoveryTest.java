package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.createDirectoryJunction;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.fastConfig;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Plain-Java tests for startup recovery with replication (milestone M2): rule D3 with 0 to 3
 * copies, interrupted uploads whose chunks have two copies, orphan copies, and the chunk scan's
 * replication report (healthy, under-replicated, lost, over-replicated).
 * Run: {@code java -cp "out;out-test" com.distributedfs.ReplicatedRecoveryTest}.
 */
public class ReplicatedRecoveryTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("replrecovery-test-");

    private static final int CHUNK = 64 * 1024;
    private static final String F = "f".repeat(32);
    private static final String ORPHAN = "0".repeat(31) + "9";

    public static void main(String[] args) {
        RUNNER.run("a committed upload with 2 copies per chunk only loses its leftover marker",
                ReplicatedRecoveryTest::committedTwoCopies);
        RUNNER.run("a committed upload with 1 copy of a chunk is accepted and reported as under-replicated",
                ReplicatedRecoveryTest::committedOneCopy);
        RUNNER.run("a committed upload with a chunk on no node stops the start", ReplicatedRecoveryTest::committedNoCopy);
        RUNNER.run("a committed upload with a chunk on 3 nodes is accepted and reported as over-replicated", ReplicatedRecoveryTest::committedThreeCopies);
        RUNNER.run("an interrupted upload's chunks are deleted from both of their nodes", ReplicatedRecoveryTest::twoHolderRecovery);
        RUNNER.run("an interrupted upload with a chunk on 3 nodes has all 3 copies deleted", ReplicatedRecoveryTest::incompleteThreeCopies);
        RUNNER.run("an orphan chunk with 2 copies is reported once per copy and left alone",
                ReplicatedRecoveryTest::orphanTwoCopies);
        RUNNER.run("the scan reports under-replicated, lost and over-replicated chunks of committed files",
                ReplicatedRecoveryTest::scanCategories);
        RUNNER.run("the scan prints at most 20 lines per kind", ReplicatedRecoveryTest::scanIsCapped);
        RUNNER.run("an entry that is not a regular file is not counted as a copy", ReplicatedRecoveryTest::junctionIsNotACopy);
        RUNNER.run("with every chunk healthy, the replication check prints nothing", ReplicatedRecoveryTest::healthyIsQuiet);
        RUNNER.run("a server process killed between copy 1 and copy 2 of a chunk is recovered by the next start",
                ReplicatedRecoveryTest::killedBetweenCopies);
        RUNNER.finish();
    }

    private static void committedTwoCopies(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String id = s.uploadFile(2, 1);
            s.markPending(id, 2);
            String out = s.restart();
            checkContains(out, "1 committed");
            check(!out.contains("Replication check"), "a healthy file is not reported:\n" + out);
            checkEquals(List.of(id + ".manifest"), listNames(s.data.resolve("manifests")), "manifests folder");
        }
    }

    private static void committedOneCopy(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String id = s.uploadFile(2, 2);
            s.deleteCopy(FileIds.chunkId(id, 1), 0);
            s.markPending(id, 2);
            String out = s.restart();
            checkContains(out, "1 committed");
            checkContains(out, "1 chunk(s) healthy, 1 UNDER_REPLICATED, 0 LOST_CHUNK, 0 OVER_REPLICATED");
            checkContains(out, "UNDER_REPLICATED " + FileIds.chunkId(id, 1) + " (1 of 2 copies, on node ");
        }
    }

    private static void committedNoCopy(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String id = s.uploadFile(2, 3);
            s.deleteCopy(FileIds.chunkId(id, 0), 0);
            s.deleteCopy(FileIds.chunkId(id, 0), 0);
            s.markPending(id, 2);
            UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, "start", s::restart);
            checkContains(e.getMessage(), "missing from every node");
            check(Files.exists(s.data.resolve("manifests").resolve(id + ".pending")), "the marker is kept");
        }
    }

    private static void committedThreeCopies(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String id = s.uploadFile(1, 4);
            s.storeOnEveryNode(FileIds.chunkId(id, 0));
            s.markPending(id, 1);
            // Since milestone M3, more copies than the replication factor are accepted and reported.
            String out = s.restart();
            checkContains(out, "1 committed");
            checkContains(out, "OVER_REPLICATED " + FileIds.chunkId(id, 0) + " (3 of 2 copies, on nodes [0, 1, 2])");
            checkEquals(List.of(1, 1, 1), s.cluster.chunkCounts(), "every copy is kept");
        }
    }

    private static void twoHolderRecovery(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            s.markPending(F, 3);
            s.store(0, F + "_0");
            s.store(1, F + "_0");
            s.store(1, F + "_1");
            s.store(2, F + "_1");
            s.store(2, F + "_2");
            String out = s.restart();
            checkContains(out, "1 rolled back (3 chunk(s) deleted)");
            checkEquals(List.of(0, 0, 0), s.cluster.chunkCounts(), "copies left");
            checkEquals(List.of(), listNames(s.data.resolve("manifests")), "the marker is gone");
        }
    }

    private static void incompleteThreeCopies(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            s.markPending(F, 1);
            s.storeOnEveryNode(F + "_0");
            // Since milestone M3 every copy of the candidate is deleted, however many there are.
            checkContains(s.restart(), "1 rolled back (1 chunk(s) deleted)");
            checkEquals(List.of(0, 0, 0), s.cluster.chunkCounts(), "every copy was deleted");
        }
    }

    private static void orphanTwoCopies(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            s.store(0, ORPHAN + "_0");
            s.store(2, ORPHAN + "_0");
            String out = s.restart();
            checkContains(out, "2 ORPHAN_CHUNK");
            checkContains(out, "ORPHAN_CHUNK " + ORPHAN + "_0 node 0 ");
            checkContains(out, "ORPHAN_CHUNK " + ORPHAN + "_0 node 2 ");
            checkEquals(List.of(1, 0, 1), s.cluster.chunkCounts(), "the orphan copies are left alone");
        }
    }

    private static void scanCategories(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String healthy = s.uploadFile(1, 5);
            String under = s.uploadFile(1, 6);
            String lost = s.uploadFile(1, 7);
            String over = s.uploadFile(1, 8);
            s.deleteCopy(FileIds.chunkId(under, 0), 0);
            s.deleteCopy(FileIds.chunkId(lost, 0), 0);
            s.deleteCopy(FileIds.chunkId(lost, 0), 0);
            s.storeOnEveryNode(FileIds.chunkId(over, 0));
            String out = s.restart();
            checkContains(out, "Replication check (2 copies per chunk): 1 chunk(s) healthy, 1 UNDER_REPLICATED,"
                    + " 1 LOST_CHUNK, 1 OVER_REPLICATED. Report only");
            checkContains(out, "UNDER_REPLICATED " + FileIds.chunkId(under, 0));
            checkContains(out, "LOST_CHUNK " + FileIds.chunkId(lost, 0) + " (no copy on any node)");
            checkContains(out, "OVER_REPLICATED " + FileIds.chunkId(over, 0) + " (3 of 2 copies, on nodes [0, 1, 2])");
            check(!out.contains(FileIds.chunkId(healthy, 0)), "the healthy chunk is not listed");
        }
    }

    private static void scanIsCapped(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String id = s.uploadFile(25, 9);
            for (int i = 0; i < 25; i++) {
                s.deleteCopy(FileIds.chunkId(id, i), 0);
            }
            String out = s.restart();
            checkContains(out, "25 UNDER_REPLICATED");
            checkEquals(20L, out.lines().filter(l -> l.startsWith("  UNDER_REPLICATED ")).count(), "lines printed");
            checkContains(out, "  ... and 5 more UNDER_REPLICATED");
        }
    }

    private static void junctionIsNotACopy(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            String id = s.uploadFile(1, 10);
            String chunk = FileIds.chunkId(id, 0);
            List<Integer> holders = ReplicatedServerTest.copies(ReplicatedServerTest.chunkDirs(s.cluster), chunk);
            Path entry = s.cluster.nodes.get(holders.get(0)).chunksDir().resolve(chunk);
            Files.delete(entry);
            Path outside = Files.createDirectory(tempDir.resolve("outside"));
            createDirectoryJunction(entry, outside);
            try {
                String out = s.restart();
                checkContains(out, "UNDER_REPLICATED " + chunk + " (1 of 2 copies");
            } finally {
                Files.deleteIfExists(entry);
            }
        }
    }

    private static void healthyIsQuiet(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            s.uploadFile(3, 11);
            String out = s.restart();
            check(!out.contains("Replication check") && !out.contains("Chunk scan"), "nothing to report, but:\n" + out);
        }
    }

    private static void killedBetweenCopies(Path tempDir) throws Exception {
        try (Setup s = Setup.start(tempDir)) {
            s.server.stop();
            List<String> command = new ArrayList<>(List.of(NodeTestKit.javaExecutable(), "-Dreplicas=2", "-cp",
                    NodeTestKit.absoluteClassPath(), "com.distributedfs.RemoteCrashUploadMain", s.data.toString(), "1",
                    Integer.toString(CHUNK), Integer.toString(3 * CHUNK)));
            for (int port : s.cluster.ports()) {
                command.add(Integer.toString(port));
            }
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            check(process.waitFor(60, TimeUnit.SECONDS), "the crashing server did not stop");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            checkEquals(RemoteCrashUploadMain.HALTED, process.exitValue(), "exit code; output:\n" + output);
            checkEquals(List.of(1, 0, 0), s.cluster.chunkCounts(), "copy 1 of chunk 0, and nothing else");

            String out = s.restart();
            checkContains(out, "1 rolled back (1 chunk(s) deleted)");
            checkEquals(List.of(0, 0, 0), s.cluster.chunkCounts(), "copies after recovery");
            checkEquals(List.of(), listNames(s.data.resolve("manifests")), "manifests folder after recovery");
        }
    }

    /** Three node servers in this process and a server over them (2 copies per chunk). */
    private static final class Setup implements AutoCloseable {
        final RemoteServerTest.Cluster cluster;
        final Path data;
        DistributedFsServer server;

        private Setup(RemoteServerTest.Cluster cluster, Path data) {
            this.cluster = cluster;
            this.data = data;
        }

        static Setup start(Path tempDir) throws IOException {
            RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3);
            Setup setup = new Setup(cluster, tempDir.resolve("data"));
            try {
                setup.server = startServer(setup.data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
                return setup;
            } catch (IOException | RuntimeException e) {
                cluster.close();
                throw e;
            }
        }

        /** Uploads a random file of {@code chunks} chunks through the running server. */
        String uploadFile(int chunks, long seed) throws IOException {
            return fileId(upload(server.port(), randomBytes(chunks * CHUNK, seed)));
        }

        /** Stops the server, starts a new one over the same nodes and data, stops it, and returns its output. */
        String restart() throws IOException {
            server.stop();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            server = startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK);
            server.stop();
            return out.text();
        }

        void markPending(String fileId, int chunkCount) throws IOException {
            server.stop(); // the server's lock is released; markers are written as a crashed upload left them
            new ManifestStore(data.resolve("manifests")).createPending(new ManifestStore.PendingUpload(fileId, chunkCount));
        }

        /** Deletes the copy of {@code chunkId} on the {@code nth} node (in order) that holds it. */
        void deleteCopy(String chunkId, int nth) throws IOException {
            List<Integer> holders = ReplicatedServerTest.copies(ReplicatedServerTest.chunkDirs(cluster), chunkId);
            Files.delete(cluster.nodes.get(holders.get(nth)).chunksDir().resolve(chunkId));
        }

        /** Stores a small chunk directly on one node, as an earlier server process would have. */
        void store(int node, String chunkId) throws IOException {
            Path source = Files.createTempFile("replrecovery-chunk-", ".bin");
            try (RemoteStorageNode remote = RemoteStorageNode.connect(cluster.nodes.get(node).port(), fastConfig())) {
                Files.write(source, randomBytes(100, chunkId.hashCode()));
                remote.storeChunk(chunkId, source);
            } finally {
                Files.deleteIfExists(source);
            }
        }

        /** Copies an existing chunk onto every node that does not have it yet, so it has 3 copies. */
        void storeOnEveryNode(String chunkId) throws IOException {
            Path existing = null;
            for (NodeTestKit.NodeHandle node : cluster.nodes) {
                if (Files.exists(node.chunksDir().resolve(chunkId))) {
                    existing = node.chunksDir().resolve(chunkId);
                }
            }
            for (int i = 0; i < cluster.nodes.size(); i++) {
                if (!Files.exists(cluster.nodes.get(i).chunksDir().resolve(chunkId))) {
                    if (existing == null) {
                        store(i, chunkId);
                    } else {
                        Files.copy(existing, cluster.nodes.get(i).chunksDir().resolve(chunkId));
                    }
                }
            }
        }

        @Override
        public void close() {
            if (server != null) {
                server.stop();
            }
            cluster.close();
        }
    }
}
