package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.jsonNumber;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.repair;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.status;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Plain-Java tests for a degraded start (milestone M3): the server starts while some recorded
 * storage nodes do not answer, as long as at least {@code min(2, nodes)} do; the missing nodes are
 * DOWN until they answer as the node recorded for them; and recovery finishes only what it can
 * decide without them.
 * Run: {@code java -cp "out;out-test" com.distributedfs.DegradedStartupTest}.
 */
public class DegradedStartupTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("degraded-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("with 1 of 3 recorded nodes down, the server starts and says which node is missing",
                DegradedStartupTest::oneNodeMissing);
        RUNNER.run("the missing node is DOWN in the status, with its recorded ID and no good check",
                DegradedStartupTest::missingNodeStatus);
        RUNNER.run("uploads and downloads work during a degraded start, using the nodes that answered",
                DegradedStartupTest::filesWhileDegraded);
        RUNNER.run("the missing node is used again once it answers on its port", DegradedStartupTest::missingNodeReturns);
        RUNNER.run("a different node on the missing node's port stays DOWN, gets no copy, and uploads are refused as in M2", DegradedStartupTest::wrongNodeStaysDown);
        RUNNER.run("with 2 of 4 nodes missing, each is pinned to the recorded ID it answers with",
                DegradedStartupTest::twoMissingArePinnedOnAnswer);
        RUNNER.run("with only 1 of 3 recorded nodes answering, the start is refused", DegradedStartupTest::tooFewOfThree);
        RUNNER.run("with 1 of 2 recorded nodes answering, the start is refused", DegradedStartupTest::tooFewOfTwo);
        RUNNER.run("a first start still needs every node, and records nothing", DegradedStartupTest::firstStartNeedsAll);
        RUNNER.run("a degraded start needs one --node per recorded node", DegradedStartupTest::countMustMatch);
        RUNNER.run("a degraded start refuses a node that is not recorded", DegradedStartupTest::unrecordedNodeRefused);
        RUNNER.run("an interrupted upload waits for the missing node, and the next full start rolls it back",
                DegradedStartupTest::incompleteUploadWaits);
        RUNNER.run("a committed upload whose chunk only the missing node holds waits, and the next full start commits it",
                DegradedStartupTest::committedUploadWaits);
        RUNNER.run("the chunk scan skips the missing node and reports no orphans or missing chunks for it",
                DegradedStartupTest::scanSkipsMissingNode);
        RUNNER.run("a repair during a degraded start copies the missing node's chunks to the others",
                DegradedStartupTest::repairWhileDegraded);
        RUNNER.finish();
    }

    private static void oneNodeMissing(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            String missingId = cluster.nodes.get(1).nodeId();
            int missingPort = cluster.nodes.get(1).port();
            cluster.nodes.get(1).close();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer server = start(tempDir, cluster, out, new NodeTestKit.Lines());
            try {
                checkContains(out.text(), "Node 1: storage node " + missingId + " at 127.0.0.1:" + missingPort
                        + " (did not answer; the server started without it)");
                checkContains(out.text(), "Started with node 1 missing. Uploads use the other nodes, and the cluster"
                        + " status shows when it is back.");
                checkContains(out.text(), "Replication: 2 copies of every chunk, each on a different node");
                checkEquals(ClusterMembership.text(new java.util.TreeSet<>(cluster.ids())),
                        Files.readString(tempDir.resolve("data").resolve("nodes.txt")), "nodes.txt is unchanged");
            } finally {
                server.stop();
            }
        }
    }

    private static void missingNodeStatus(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            cluster.nodes.get(1).close();
            DistributedFsServer server = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            try {
                String json = status(server.port()).text();
                checkEquals("DOWN", ClusterStatusEndpointTest.nodeField(json, 1, "state"), "state");
                checkEquals(cluster.nodes.get(1).nodeId(), ClusterStatusEndpointTest.nodeField(json, 1, "nodeId"), "node ID");
                checkEquals("null", ClusterStatusEndpointTest.nodeField(json, 1, "lastGoodCheck"), "last good check");
                checkEquals("3", ClusterStatusEndpointTest.nodeField(json, 1, "failuresInARow"), "failures in a row");
                checkEquals("UP", ClusterStatusEndpointTest.nodeField(json, 0, "state"), "node 0");
                checkEquals("UP", ClusterStatusEndpointTest.nodeField(json, 2, "state"), "node 2");
            } finally {
                server.stop();
            }
        }
    }

    private static void filesWhileDegraded(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            cluster.nodes.get(1).close();
            DistributedFsServer server = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            try {
                byte[] data = randomBytes(3 * CHUNK, 1);
                String id = fileId(upload(server.port(), data));
                checkEquals(3, listNames(cluster.nodes.get(0).chunksDir()).size(), "copies on node 0");
                checkEquals(3, listNames(cluster.nodes.get(2).chunksDir()).size(), "copies on node 2");
                check(Arrays.equals(data, download(server.port(), id).body()), "the downloaded file");
            } finally {
                server.stop();
            }
        }
    }

    private static void missingNodeReturns(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            int port = cluster.nodes.get(1).port();
            cluster.nodes.get(1).close();
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = start(tempDir, cluster, new NodeTestKit.Lines(), err);
            try {
                cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("node-1"), port));
                NodeTestKit.waitUntil(10_000, "node 1 UP",
                        () -> ClusterStatusEndpointTest.nodeState(server.port(), 1).equals("UP"));
                checkContains(err.text(), "is UP again");
                byte[] data = randomBytes(3 * CHUNK, 2);
                String id = fileId(upload(server.port(), data));
                checkEquals(2, listNames(cluster.nodes.get(1).chunksDir()).size(), "copies on node 1 (chunks 0 and 1)");
                check(Arrays.equals(data, download(server.port(), id).body()), "the file");
            } finally {
                server.stop();
            }
        }
    }

    private static void wrongNodeStaysDown(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            int port = cluster.nodes.get(1).port();
            cluster.nodes.get(1).close();
            DistributedFsServer server = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            try {
                byte[] data = randomBytes(3 * CHUNK, 7);
                String id = fileId(upload(server.port(), data));
                cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("stranger"), port));
                long before = Long.parseLong(ClusterStatusEndpointTest.nodeField(status(server.port()).text(), 1,
                        "failuresInARow"));
                NodeTestKit.waitUntil(10_000, "more failed checks", () -> {
                    try {
                        return Long.parseLong(ClusterStatusEndpointTest.nodeField(status(server.port()).text(), 1,
                                "failuresInARow")) >= before + 3;
                    } catch (IOException e) {
                        return false;
                    }
                });
                checkEquals("DOWN", ClusterStatusEndpointTest.nodeState(server.port(), 1), "state");
                check(Arrays.equals(data, download(server.port(), id).body()), "a stored file still downloads");
                // Uploads are unchanged by M3: a node that answers, but as another node, cannot
                // confirm that a chunk ID is unused, so the store is refused, as it was in M2.
                NodeTestKit.RawResponse refused = upload(server.port(), randomBytes(CHUNK, 8));
                checkEquals(500, refused.status(), "an upload with a stranger answering");
                checkEquals(List.of(), listNames(cluster.nodes.get(1).chunksDir()), "copies on the stranger");
            } finally {
                server.stop();
            }
        }
    }

    private static void twoMissingArePinnedOnAnswer(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 4)) {
            int port1 = cluster.nodes.get(1).port();
            cluster.nodes.get(1).close();
            cluster.nodes.get(2).close();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer server = start(tempDir, cluster, out, new NodeTestKit.Lines());
            try {
                checkContains(out.text(), "Started with nodes 1, 2 missing.");
                checkContains(out.text(), "(not yet identified)");
                String json = status(server.port()).text();
                checkEquals("null", ClusterStatusEndpointTest.nodeField(json, 1, "nodeId"), "node 1 ID");
                checkEquals("null", ClusterStatusEndpointTest.nodeField(json, 2, "nodeId"), "node 2 ID");
                // Node 2's folder answers on node 1's port: it is a recorded node nobody answered for,
                // so the client at index 1 is pinned to it.
                String node2Id = cluster.nodes.get(2).nodeId();
                cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("node-2"), port1));
                NodeTestKit.waitUntil(10_000, "index 1 UP",
                        () -> ClusterStatusEndpointTest.nodeState(server.port(), 1).equals("UP"));
                checkEquals(node2Id, ClusterStatusEndpointTest.nodeField(status(server.port()).text(), 1, "nodeId"),
                        "the ID index 1 is pinned to");
                checkEquals("DOWN", ClusterStatusEndpointTest.nodeState(server.port(), 2), "index 2");
                byte[] data = randomBytes(4 * CHUNK, 3);
                String id = fileId(upload(server.port(), data));
                check(Arrays.equals(data, download(server.port(), id).body()), "a file stored with index 1 back");
            } finally {
                server.stop();
            }
        }
    }

    private static void tooFewOfThree(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            cluster.nodes.get(0).close();
            cluster.nodes.get(2).close();
            IOException e = expectThrows(IOException.class, "start",
                    () -> start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines()));
            checkContains(e.getMessage(), "only 1 of the 3 recorded storage nodes answered, and at least 2 are needed to"
                    + " start: the storage node --node 127.0.0.1:" + cluster.nodes.get(0).port() + " did not answer");
            cluster.nodes.set(0, NodeTestKit.startNodeOnPort(tempDir.resolve("node-0"), cluster.nodes.get(0).port()));
            start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines()).stop(); // the lock was released
        }
    }

    private static void tooFewOfTwo(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 2)) {
            cluster.nodes.get(1).close();
            IOException e = expectThrows(IOException.class, "start",
                    () -> start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines()));
            checkContains(e.getMessage(), "only 1 of the 2 recorded storage nodes answered, and at least 2 are needed");
        }
    }

    private static void firstStartNeedsAll(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            cluster.nodes.get(2).close();
            IOException e = expectThrows(IOException.class, "start",
                    () -> start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines()));
            checkContains(e.getMessage(), "the storage node --node 127.0.0.1:" + cluster.nodes.get(2).port()
                    + " did not answer");
            check(!Files.exists(tempDir.resolve("data").resolve("nodes.txt")), "nodes.txt was written");
        }
    }

    private static void countMustMatch(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            NodeTestKit.NodeHandle extra = NodeTestKit.startNode(tempDir.resolve("extra"));
            int extraPort = extra.port();
            extra.close();
            List<Integer> ports = new ArrayList<>(cluster.ports());
            ports.add(extraPort);
            IOException e = expectThrows(IOException.class, "start", () -> startServer(tempDir.resolve("data"), ports,
                    new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 100));
            check(e instanceof DataDirectoryException, "not a DataDirectoryException: " + e);
            checkContains(e.getMessage(), "This data directory uses 3 storage nodes");
            checkContains(e.getMessage(), "but 4 --node options were given");
        }
    }

    private static void unrecordedNodeRefused(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            int port2 = cluster.nodes.get(2).port();
            cluster.nodes.get(2).close();
            cluster.nodes.set(2, NodeTestKit.startNodeOnPort(tempDir.resolve("stranger"), port2));
            cluster.nodes.get(1).close();
            IOException e = expectThrows(IOException.class, "start",
                    () -> start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines()));
            check(e instanceof DataDirectoryException, "not a DataDirectoryException: " + e);
            checkContains(e.getMessage(), "Not recorded: " + cluster.nodes.get(2).nodeId());
        }
    }

    private static void incompleteUploadWaits(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = recordedCluster(tempDir, 3)) {
            String fileId = "a".repeat(32);
            new ManifestStore(tempDir.resolve("data").resolve("manifests"))
                    .createPending(new ManifestStore.PendingUpload(fileId, 2));
            Files.write(cluster.nodes.get(0).chunksDir().resolve(FileIds.chunkId(fileId, 0)), new byte[] {1});
            int port = cluster.nodes.get(1).port();
            cluster.nodes.get(1).close();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            start(tempDir, cluster, out, new NodeTestKit.Lines()).stop();
            checkContains(out.text(), "Recovery: 1 interrupted upload(s) wait for node 1, which did not answer; their"
                    + " markers were kept, and the next start tries again:");
            checkContains(out.text(), "  " + fileId + ": it is incomplete, and node 1 may hold some of its chunks");
            check(Files.exists(tempDir.resolve("data").resolve("manifests").resolve(fileId + ".pending")), "marker kept");
            check(Files.exists(cluster.nodes.get(0).chunksDir().resolve(FileIds.chunkId(fileId, 0))), "candidate kept");

            cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("node-1"), port));
            NodeTestKit.Lines full = new NodeTestKit.Lines();
            start(tempDir, cluster, full, new NodeTestKit.Lines()).stop();
            checkContains(full.text(), "Recovery: 1 interrupted upload(s): 0 committed, 1 rolled back (1 chunk(s) deleted)");
            check(!Files.exists(tempDir.resolve("data").resolve("manifests").resolve(fileId + ".pending")), "marker gone");
            checkEquals(List.of(), listNames(cluster.nodes.get(0).chunksDir()), "node 0 after the rollback");
        }
    }

    private static void committedUploadWaits(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer first = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            String fileId;
            try {
                fileId = fileId(upload(first.port(), randomBytes(CHUNK, 4))); // chunk 0 on nodes 0 and 1
            } finally {
                first.stop();
            }
            String chunk = FileIds.chunkId(fileId, 0);
            Files.delete(cluster.nodes.get(0).chunksDir().resolve(chunk));
            new ManifestStore(tempDir.resolve("data").resolve("manifests"))
                    .createPending(new ManifestStore.PendingUpload(fileId, 1));
            int port = cluster.nodes.get(1).port();
            cluster.nodes.get(1).close();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            start(tempDir, cluster, out, new NodeTestKit.Lines()).stop();
            checkContains(out.text(), "  " + fileId + ": it is committed, but no node that answered holds its chunk "
                    + chunk);
            check(Files.exists(tempDir.resolve("data").resolve("manifests").resolve(fileId + ".pending")), "marker kept");
            check(Files.exists(tempDir.resolve("data").resolve("manifests").resolve(fileId + ".manifest")), "manifest kept");

            cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("node-1"), port));
            NodeTestKit.Lines full = new NodeTestKit.Lines();
            start(tempDir, cluster, full, new NodeTestKit.Lines()).stop();
            checkContains(full.text(), "Recovery: 1 interrupted upload(s): 1 committed, 0 rolled back");
            check(!Files.exists(tempDir.resolve("data").resolve("manifests").resolve(fileId + ".pending")), "marker gone");
        }
    }

    private static void scanSkipsMissingNode(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer first = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            try {
                fileId(upload(first.port(), randomBytes(3 * CHUNK, 5)));
            } finally {
                first.stop();
            }
            cluster.nodes.get(1).close();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            start(tempDir, cluster, out, err).stop();
            checkContains(err.text(), "Warning: the chunk scan skipped node 1: it did not answer when the server started");
            check(!out.text().contains("MISSING") && !out.text().contains("ORPHAN"), "findings: " + out.text());
        }
    }

    private static void repairWhileDegraded(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer first = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            byte[] data = randomBytes(3 * CHUNK, 6);
            String id;
            try {
                id = fileId(upload(first.port(), data)); // chunks (0,1), (1,2), (2,0)
            } finally {
                first.stop();
            }
            cluster.nodes.get(1).close();
            DistributedFsServer server = start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines());
            try {
                String report = repair(server.port(), null).text();
                checkEquals(2L, jsonNumber(report, "copiesAdded"), "copies added");
                checkContains(report, "{\"index\":1,\"state\":\"DOWN\"}");
                checkEquals(3, listNames(cluster.nodes.get(0).chunksDir()).size(), "copies on node 0");
                checkEquals(3, listNames(cluster.nodes.get(2).chunksDir()).size(), "copies on node 2");
                check(Arrays.equals(data, download(server.port(), id).body()), "the file");
            } finally {
                server.stop();
            }
        }
    }

    /** Starts {@code count} nodes and records them in a new data directory with one start and stop. */
    private static RemoteServerTest.Cluster recordedCluster(Path tempDir, int count) throws IOException {
        RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, count);
        try {
            start(tempDir, cluster, new NodeTestKit.Lines(), new NodeTestKit.Lines()).stop();
            return cluster;
        } catch (IOException | RuntimeException e) {
            cluster.close();
            throw e;
        }
    }

    private static DistributedFsServer start(Path tempDir, RemoteServerTest.Cluster cluster, NodeTestKit.Lines out,
                                             NodeTestKit.Lines err) throws IOException {
        return startServer(tempDir.resolve("data"), cluster.ports(), out, err, CHUNK, 100);
    }
}
