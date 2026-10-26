package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.createDirectoryJunction;
import static com.distributedfs.NodeTestKit.download;
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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Plain-Java tests for startup recovery with remote storage nodes: every listing, no-follow check,
 * delete and temp sweep goes through the node API, an unreachable or broken node stops the start,
 * and a server process killed in the middle of an upload (a child JVM) is recovered by the next
 * start. Run: {@code java -cp "out;out-test" com.distributedfs.NetworkedRecoveryTest}.
 */
public class NetworkedRecoveryTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("netrecovery-test-");

    private static final int CHUNK = 64 * 1024;
    private static final String F = "f".repeat(32);
    private static final String ORPHAN = "0".repeat(31) + "9";

    public static void main(String[] args) {
        RUNNER.run("an interrupted upload's chunks on remote nodes are deleted, then its marker",
                NetworkedRecoveryTest::incompleteUploadIsRolledBack);
        RUNNER.run("a committed upload with a leftover marker keeps its chunks and stays downloadable",
                NetworkedRecoveryTest::committedUploadIsKept);
        RUNNER.run("stale temp files on the nodes are removed through the node API", NetworkedRecoveryTest::tempFilesAreSwept);
        RUNNER.run("an orphan chunk on a remote node is reported with the node's address, and left alone",
                NetworkedRecoveryTest::orphanIsReportedWithAddress);
        RUNNER.run("a junction under an interrupted upload's chunk name on a node stops the start, and nothing changes",
                NetworkedRecoveryTest::junctionStopsStart);
        RUNNER.run("a node whose chunk folder is gone stops the start, and nothing changes",
                NetworkedRecoveryTest::brokenNodeStopsStart);
        RUNNER.run("a node that cannot be listed stops the start even when no upload was interrupted",
                NetworkedRecoveryTest::unlistableNodeStopsStart);
        RUNNER.run("a server process killed after storing two chunks is recovered by the next start",
                NetworkedRecoveryTest::killedServerIsRecovered);
        RUNNER.finish();
    }

    private static void incompleteUploadIsRolledBack(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            initialize(data, cluster);
            new ManifestStore(data.resolve("manifests")).createPending(new ManifestStore.PendingUpload(F, 3));
            store(cluster, 1, F + "_0");
            store(cluster, 2, F + "_1");
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK).stop();
            checkContains(out.text(), "1 rolled back (2 chunk(s) deleted)");
            checkEquals(List.of(0, 0, 0), cluster.chunkCounts(), "chunks per node");
            checkEquals(List.of(), listNames(data.resolve("manifests")), "manifests folder");
        }
    }

    private static void committedUploadIsKept(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            byte[] content = randomBytes(2 * CHUNK + 5, 1);
            DistributedFsServer server = startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
            String id = fileId(upload(server.port(), content));
            server.stop();
            new ManifestStore(data.resolve("manifests")).createPending(new ManifestStore.PendingUpload(id, 3));
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            server = startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK);
            try {
                checkContains(out.text(), "1 committed");
                checkEquals(List.of(id + ".manifest"), listNames(data.resolve("manifests")), "manifests folder");
                checkEquals(List.of(2, 2, 2), cluster.chunkCounts(), "copies per node (3 chunks, 2 copies each)");
                NodeTestKit.RawResponse got = download(server.port(), id);
                checkEquals(200, got.status(), "download");
                check(Arrays.equals(content, got.body()), "the file differs");
            } finally {
                server.stop();
            }
        }
    }

    private static void tempFilesAreSwept(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 2)) {
            initialize(data, cluster);
            Files.write(cluster.nodes.get(0).chunksDir().resolve(F + "_0-12345.tmp"), new byte[] {1});
            Files.write(cluster.nodes.get(1).chunksDir().resolve("chunk_7-99.tmp"), new byte[] {2});
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK).stop();
            checkContains(out.text(), "2 stale temp file(s) or workspace(s) removed");
            checkEquals(List.of(0, 0), cluster.chunkCounts(), "chunks per node");
        }
    }

    private static void orphanIsReportedWithAddress(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            initialize(data, cluster);
            store(cluster, 2, ORPHAN + "_0");
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK).stop();
            checkContains(out.text(), "ORPHAN_CHUNK " + ORPHAN + "_0 node 2 storage node " + cluster.nodes.get(2).nodeId()
                    + " at 127.0.0.1:" + cluster.nodes.get(2).port() + ", entry " + ORPHAN + "_0");
            checkEquals(List.of(ORPHAN + "_0"), listNames(cluster.nodes.get(2).chunksDir()), "the orphan is left alone");
        }
    }

    private static void junctionStopsStart(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            initialize(data, cluster);
            new ManifestStore(data.resolve("manifests")).createPending(new ManifestStore.PendingUpload(F, 2));
            store(cluster, 0, F + "_0");
            Path outside = Files.createDirectory(tempDir.resolve("outside"));
            Path junction = cluster.nodes.get(1).chunksDir().resolve(F + "_1");
            createDirectoryJunction(junction, outside);
            try {
                UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, "start",
                        () -> startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
                checkContains(e.getMessage(), F);
                checkEquals(List.of(F + ".pending"), listNames(data.resolve("manifests")), "the marker is kept");
                checkEquals(List.of(F + "_0"), listNames(cluster.nodes.get(0).chunksDir()), "the stored chunk is kept");
                check(Files.exists(junction, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
            } finally {
                Files.deleteIfExists(junction);
            }
        }
    }

    private static void brokenNodeStopsStart(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            initialize(data, cluster);
            new ManifestStore(data.resolve("manifests")).createPending(new ManifestStore.PendingUpload(F, 1));
            store(cluster, 0, F + "_0");
            Files.delete(cluster.nodes.get(2).chunksDir()); // the node still answers its health check
            IOException e = expectThrows(IOException.class, "start",
                    () -> startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkContains(e.getMessage(), "storage_unavailable");
            checkEquals(List.of(F + ".pending"), listNames(data.resolve("manifests")), "the marker is kept");
            checkEquals(List.of(F + "_0"), listNames(cluster.nodes.get(0).chunksDir()), "the chunk is kept");
        }
    }

    private static void unlistableNodeStopsStart(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            initialize(data, cluster);
            Files.delete(cluster.nodes.get(1).chunksDir()); // health still answers; /entries is 503
            IOException e = expectThrows(IOException.class, "start",
                    () -> startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            // The listing itself (step 1) stops the start, before any marker or chunk is looked at.
            checkContains(e.getMessage(), "could not list the data directory");
            checkContains(e.getMessage(), "could not list its chunk folder: it answered 503 storage_unavailable");
        }
    }

    private static void killedServerIsRecovered(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            List<String> command = new ArrayList<>(List.of(NodeTestKit.javaExecutable(), "-cp", NodeTestKit.absoluteClassPath(),
                    "com.distributedfs.RemoteCrashUploadMain", data.toString(), "2", Integer.toString(CHUNK),
                    Integer.toString(5 * CHUNK)));
            for (int port : cluster.ports()) {
                command.add(Integer.toString(port));
            }
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            check(process.waitFor(60, TimeUnit.SECONDS), "the crashing server did not stop");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            checkEquals(RemoteCrashUploadMain.HALTED, process.exitValue(), "exit code; output:\n" + output);
            String upload = RemoteCrashUploadMain.UPLOAD_ID;
            checkEquals(List.of(1, 1, 0), cluster.chunkCounts(), "chunks left by the killed server");
            checkEquals(List.of(upload + ".pending"), listNames(data.resolve("manifests")), "the marker it left");

            NodeTestKit.Lines out = new NodeTestKit.Lines();
            startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK).stop();
            checkContains(out.text(), "1 rolled back (2 chunk(s) deleted)");
            checkEquals(List.of(0, 0, 0), cluster.chunkCounts(), "chunks after recovery");
            checkEquals(List.of(), listNames(data.resolve("manifests")), "manifests folder after recovery");
        }
    }

    /** A first start, so the data directory exists and records the nodes. */
    private static void initialize(Path data, RemoteServerTest.Cluster cluster) throws IOException {
        startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK).stop();
    }

    /** Stores a small chunk directly on one node, as an earlier server process would have. */
    private static void store(RemoteServerTest.Cluster cluster, int node, String chunkId) throws IOException {
        Path source = Files.createTempFile("netrecovery-chunk-", ".bin");
        try (RemoteStorageNode remote = RemoteStorageNode.connect(cluster.nodes.get(node).port(), fastConfig())) {
            Files.write(source, randomBytes(100, chunkId.hashCode()));
            remote.storeChunk(chunkId, source);
        } finally {
            Files.deleteIfExists(source);
        }
    }
}
