package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Plain-Java tests for the DistributedFS server with replication (milestone M2), over HTTP: two
 * copies of every chunk, uploads and downloads with a node down, damaged and missing copies, a
 * lost store answer with two copies, and files stored before replication (one copy).
 * Run: {@code java -cp "out;out-test" com.distributedfs.ReplicatedServerTest}.
 */
public class ReplicatedServerTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("replserver-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("every chunk of an upload has 2 copies on different node processes", ReplicatedServerTest::twoCopiesRemote);
        RUNNER.run("in local mode every chunk has 2 copies in different node folders", ReplicatedServerTest::twoCopiesLocal);
        RUNNER.run("a one-node cluster keeps 1 copy", ReplicatedServerTest::oneNodeCluster);
        RUNNER.run("an upload with a node down skips it, logs it, and still gets 2 copies per chunk",
                ReplicatedServerTest::deadNodeOnUpload);
        RUNNER.run("a download with a node down uses the other copies and logs the node", ReplicatedServerTest::deadNodeOnDownload);
        RUNNER.run("a node that failed is tried last for the rest of the download", ReplicatedServerTest::failedNodeTriedLast);
        RUNNER.run("a damaged copy is skipped with a warning naming its node, and the other copy is served",
                ReplicatedServerTest::damagedCopyIsSkipped);
        RUNNER.run("with both copies damaged the download is 500 integrity_check_failed", ReplicatedServerTest::bothDamaged);
        RUNNER.run("with both copies gone the download is 500 chunk_missing", ReplicatedServerTest::bothMissing);
        RUNNER.run("a lost answer for copy 2 rolls back copy 1 as well", ReplicatedServerTest::lostAnswerRollsBackBothCopies);
        RUNNER.run("a rollback that a down node cannot confirm keeps the marker, and the next start removes every copy",
                ReplicatedServerTest::rollbackWithNodeDown);
        RUNNER.run("a file with one copy per chunk, as stored before replication, still downloads",
                ReplicatedServerTest::oneCopyFileDownloads);
        RUNNER.run("a format 1 file with one copy per chunk still downloads", ReplicatedServerTest::formatOneOneCopy);
        RUNNER.finish();
    }

    private static void twoCopiesRemote(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), out, new NodeTestKit.Lines(), CHUNK);
            try {
                checkContains(out.text(), "Replication: 2 copies of every chunk, each on a different node");
                String id = fileId(upload(server.port(), randomBytes(4 * CHUNK, 1)));
                for (int i = 0; i < 4; i++) {
                    checkEquals(2, copies(chunkDirs(cluster), FileIds.chunkId(id, i)).size(), "copies of chunk " + i);
                }
            } finally {
                server.stop();
            }
        }
    }

    private static void twoCopiesLocal(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer server = startServer(data, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
        try {
            byte[] content = randomBytes(3 * CHUNK, 2);
            String id = fileId(upload(server.port(), content));
            List<Path> dirs = localDirs(data);
            checkEquals(List.of(0, 1), copies(dirs, FileIds.chunkId(id, 0)), "chunk 0");
            checkEquals(List.of(1, 2), copies(dirs, FileIds.chunkId(id, 1)), "chunk 1");
            checkEquals(List.of(0, 2), copies(dirs, FileIds.chunkId(id, 2)), "chunk 2");
            check(Arrays.equals(content, download(server.port(), id).body()), "the file differs");
        } finally {
            server.stop();
        }
    }

    private static void oneNodeCluster(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 1)) {
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), out, new NodeTestKit.Lines(), CHUNK);
            try {
                checkContains(out.text(), "Replication: 1 copy of every chunk");
                byte[] content = randomBytes(2 * CHUNK, 3);
                String id = fileId(upload(server.port(), content));
                checkEquals(2, listNames(cluster.nodes.get(0).chunksDir()).size(), "one copy of each of 2 chunks");
                check(Arrays.equals(content, download(server.port(), id).body()), "the file differs");
            } finally {
                server.stop();
            }
        }
    }

    private static void deadNodeOnUpload(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(), err, CHUNK);
            try {
                cluster.nodes.get(1).close();
                byte[] content = randomBytes(3 * CHUNK, 4);
                String id = fileId(upload(server.port(), content));
                for (int i = 0; i < 3; i++) {
                    checkEquals(List.of(0, 2), copies(chunkDirs(cluster), FileIds.chunkId(id, i)), "copies of chunk " + i);
                }
                checkContains(err.text(), "stored on nodes [0, 2]; skipped unreachable node 1");
                check(Arrays.equals(content, download(server.port(), id).body()), "the file differs");
            } finally {
                server.stop();
            }
        }
    }

    private static void deadNodeOnDownload(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(), err, CHUNK);
            try {
                byte[] content = randomBytes(3 * CHUNK, 5);
                String id = fileId(upload(server.port(), content));
                cluster.nodes.get(1).close();
                NodeTestKit.RawResponse got = download(server.port(), id);
                checkEquals(200, got.status(), "status");
                check(Arrays.equals(content, got.body()), "the file differs");
                checkContains(err.text(), "node 1 (storage node " + cluster.nodes.get(1).nodeId());
                checkContains(err.text(), "could not be read");
            } finally {
                server.stop();
            }
        }
    }

    private static void failedNodeTriedLast(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(), err, CHUNK);
            try {
                // 6 chunks on (0,1) (1,2) (2,0) (0,1) (1,2) (2,0). With node 1 down, chunk 1 finds it
                // down; chunk 4 then tries node 2 before node 1, so node 1 is reported only once.
                String id = fileId(upload(server.port(), randomBytes(6 * CHUNK, 6)));
                cluster.nodes.get(1).close();
                checkEquals(200, download(server.port(), id).status(), "status");
                long warnings = err.text().lines().filter(l -> l.contains("could not be read")).count();
                checkEquals(1L, warnings, "warnings about the down node");
            } finally {
                server.stop();
            }
        }
    }

    private static void damagedCopyIsSkipped(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        DistributedFsServer server = startServer(data, List.of(), new NodeTestKit.Lines(), err, CHUNK);
        try {
            byte[] content = randomBytes(2 * CHUNK, 7);
            String id = fileId(upload(server.port(), content));
            damage(localDirs(data).get(0).resolve(FileIds.chunkId(id, 0))); // chunk 0 is on nodes 0 and 1
            NodeTestKit.RawResponse got = download(server.port(), id);
            checkEquals(200, got.status(), "status");
            check(Arrays.equals(content, got.body()), "the file differs");
            checkContains(err.text(), "chunk " + FileIds.chunkId(id, 0) + ": node 0 (");
            checkContains(err.text(), "has a damaged copy");
            checkContains(err.text(), "the copy on node 1 was used");
        } finally {
            server.stop();
        }
    }

    private static void bothDamaged(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer server = startServer(data, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 8)));
            for (int node : copies(localDirs(data), FileIds.chunkId(id, 1))) {
                damage(localDirs(data).get(node).resolve(FileIds.chunkId(id, 1)));
            }
            NodeTestKit.RawResponse got = download(server.port(), id);
            checkEquals(500, got.status(), "status");
            checkContains(got.text(), "integrity_check_failed");
        } finally {
            server.stop();
        }
    }

    private static void bothMissing(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer server = startServer(data, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 9)));
            for (int node : copies(localDirs(data), FileIds.chunkId(id, 1))) {
                Files.delete(localDirs(data).get(node).resolve(FileIds.chunkId(id, 1)));
            }
            NodeTestKit.RawResponse got = download(server.port(), id);
            checkEquals(500, got.status(), "status");
            checkContains(got.text(), "chunk_missing");
        } finally {
            server.stop();
        }
    }

    private static void lostAnswerRollsBackBothCopies(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3);
             NodeTestKit.Proxy proxy = new NodeTestKit.Proxy(cluster.nodes.get(1).port(),
                     NodeTestKit.Proxy.Mode.DROP_STORE_REPLIES)) {
            List<Integer> ports = new ArrayList<>(cluster.ports());
            ports.set(1, proxy.port());
            Path data = tempDir.resolve("data");
            DistributedFsServer server = startServer(data, ports, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
            try {
                // One chunk: copy 1 on node 0 is confirmed, copy 2 on node 1 is stored but its answer is lost.
                checkEquals(500, upload(server.port(), randomBytes(1000, 10)).status(), "status");
                checkEquals(1, proxy.droppedStores.get(), "lost answers");
                checkEquals(List.of(0, 0, 0), cluster.chunkCounts(), "copies left on the nodes");
                checkEquals(List.of(), listNames(data.resolve("manifests")), "the marker is gone");
            } finally {
                server.stop();
            }
        }
    }

    private static void rollbackWithNodeDown(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3);
             NodeTestKit.Proxy proxy = new NodeTestKit.Proxy(cluster.nodes.get(1).port(),
                     NodeTestKit.Proxy.Mode.DEAD_AFTER_DROP)) {
            List<Integer> ports = new ArrayList<>(cluster.ports());
            ports.set(1, proxy.port());
            DistributedFsServer server = startServer(data, ports, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
            try {
                checkEquals(500, upload(server.port(), randomBytes(1000, 11)).status(), "status");
                checkEquals(List.of(1, 1, 0), cluster.chunkCounts(), "both copies stay while node 1 cannot be reached");
                check(listNames(data.resolve("manifests")).stream().anyMatch(n -> n.endsWith(".pending")), "the marker is kept");
            } finally {
                server.stop();
            }
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK).stop();
            checkContains(out.text(), "1 rolled back (1 chunk(s) deleted)");
            checkEquals(List.of(0, 0, 0), cluster.chunkCounts(), "every copy removed by recovery");
            checkEquals(List.of(), listNames(data.resolve("manifests")), "the marker removed by recovery");
        }
    }

    private static void oneCopyFileDownloads(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer server = startServer(data, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
        try {
            byte[] content = randomBytes(3 * CHUNK, 12);
            String id = fileId(upload(server.port(), content));
            removeOneCopyOfEachChunk(data, id, 3);
            NodeTestKit.RawResponse got = download(server.port(), id);
            checkEquals(200, got.status(), "status");
            check(Arrays.equals(content, got.body()), "the file differs");
        } finally {
            server.stop();
        }
    }

    private static void formatOneOneCopy(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        DistributedFsServer server = startServer(data, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
        try {
            byte[] content = randomBytes(2 * CHUNK + 7, 13);
            String id = fileId(upload(server.port(), content));
            FileManifest stored = server.manifests().read(id).orElseThrow();
            Path manifestFile = data.resolve("manifests").resolve(id + ".manifest");
            Files.delete(manifestFile);
            Files.writeString(manifestFile, new FileManifest(id, stored.size(), stored.sha256(), stored.chunkSize(),
                    stored.chunkCount()).toText());
            removeOneCopyOfEachChunk(data, id, 3);
            NodeTestKit.RawResponse got = download(server.port(), id);
            checkEquals(200, got.status(), "status");
            check(Arrays.equals(content, got.body()), "the format 1 file differs");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    static List<Path> chunkDirs(RemoteServerTest.Cluster cluster) {
        return cluster.nodes.stream().map(NodeTestKit.NodeHandle::chunksDir).toList();
    }

    static List<Path> localDirs(Path data) {
        List<Path> dirs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            dirs.add(data.resolve("nodes").resolve("node-" + i));
        }
        return dirs;
    }

    /** The indexes of the folders that hold a file named {@code chunkId}. */
    static List<Integer> copies(List<Path> dirs, String chunkId) {
        List<Integer> holders = new ArrayList<>();
        for (int i = 0; i < dirs.size(); i++) {
            if (Files.exists(dirs.get(i).resolve(chunkId))) {
                holders.add(i);
            }
        }
        return holders;
    }

    static void damage(Path copy) throws java.io.IOException {
        byte[] content = Files.readAllBytes(copy);
        content[content.length / 2] ^= 0x10;
        Files.write(copy, content);
    }

    /** Deletes the first copy of each chunk, leaving one, as a file stored before replication would be. */
    private static void removeOneCopyOfEachChunk(Path data, String id, int chunkCount) throws java.io.IOException {
        for (int i = 0; i < chunkCount; i++) {
            List<Integer> holders = copies(localDirs(data), FileIds.chunkId(id, i));
            checkEquals(2, holders.size(), "copies of chunk " + i);
            Files.delete(localDirs(data).get(holders.get(0)).resolve(FileIds.chunkId(id, i)));
        }
    }
}
