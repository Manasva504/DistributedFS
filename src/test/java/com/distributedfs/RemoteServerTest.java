package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Plain-Java tests for the DistributedFS server with remote storage nodes (milestone M1b): node
 * servers in this process on free ports, the server started with {@code --node} ports, and a proxy
 * that can lose a store's answer after the node stored the chunk.
 * Run: {@code java -cp "out;out-test" com.distributedfs.RemoteServerTest}.
 */
public class RemoteServerTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("remoteserver-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("a file is uploaded over three node processes, spread round-robin, and downloads byte for byte",
                RemoteServerTest::roundTripOverThreeNodes);
        RUNNER.run("a download with a node down is 500 storage_error, never a missing chunk or file",
                RemoteServerTest::downloadWithNodeDown);
        RUNNER.run("an upload with a node down is 500 storage_error and stores nothing", RemoteServerTest::uploadWithNodeDown);
        RUNNER.run("a chunk whose store answer was lost is rolled back, and the upload is 500",
                RemoteServerTest::lostStoreAnswerIsRolledBack);
        RUNNER.run("if the node cannot be reached for the rollback, the marker is kept and the next start cleans up",
                RemoteServerTest::unconfirmedRollbackKeepsMarker);
        RUNNER.run("a node that does not answer at startup stops the start, before anything is recorded",
                RemoteServerTest::unreachableNodeStopsStart);
        RUNNER.run("later starts need exactly the recorded nodes, in any order", RemoteServerTest::membershipMustMatch);
        RUNNER.run("the same node given twice stops the start", RemoteServerTest::sameNodeTwice);
        RUNNER.run("a data directory cannot switch between local node folders and remote nodes",
                RemoteServerTest::modeSwitchIsRefused);
        RUNNER.run("a damaged membership file stops the start and is left alone", RemoteServerTest::damagedMembershipFile);
        RUNNER.run("--node takes 127.0.0.1:<port> only, each port once, at most 16", RemoteServerTest::nodeOptions);
        RUNNER.finish();
    }

    private static void roundTripOverThreeNodes(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 3)) {
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), out, new NodeTestKit.Lines(), CHUNK);
            try {
                byte[] data = randomBytes(5 * CHUNK - 1000, 1);
                String id = fileId(upload(server.port(), data));
                NodeTestKit.RawResponse got = download(server.port(), id);
                checkEquals(200, got.status(), "download status");
                check(Arrays.equals(data, got.body()), "the downloaded file differs");
                checkEquals(List.of(2, 2, 1), cluster.chunkCounts(), "chunks per node");
                for (int i = 0; i < 3; i++) {
                    checkEquals(List.of(), listNames(tempDir.resolve("data").resolve("nodes").resolve("node-" + i)),
                            "local node folder " + i);
                    checkContains(out.text(), "Node " + i + ": storage node " + cluster.nodes.get(i).nodeId()
                            + " at 127.0.0.1:" + cluster.nodes.get(i).port());
                }
                checkEquals(ClusterMembership.text(new TreeSet<>(cluster.ids())),
                        Files.readString(tempDir.resolve("data").resolve("nodes.txt")), "nodes.txt");
                checkContains(out.text(), "Recorded the 3 storage nodes of this data directory in nodes.txt");
            } finally {
                server.stop();
            }
        }
    }

    private static void downloadWithNodeDown(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 3)) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK);
            try {
                String id = fileId(upload(server.port(), randomBytes(3 * CHUNK, 2)));
                cluster.nodes.get(1).close();
                NodeTestKit.RawResponse got = download(server.port(), id);
                checkEquals(500, got.status(), "status");
                checkContains(got.text(), "storage_error");
            } finally {
                server.stop();
            }
        }
    }

    private static void uploadWithNodeDown(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 3)) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK);
            try {
                cluster.nodes.get(2).close();
                NodeTestKit.RawResponse response = upload(server.port(), randomBytes(3 * CHUNK, 3));
                checkEquals(500, response.status(), "status");
                checkContains(response.text(), "storage_error");
                checkEquals(List.of(), listNames(cluster.nodes.get(0).chunksDir()), "node 0");
                checkEquals(List.of(), listNames(cluster.nodes.get(1).chunksDir()), "node 1");
                checkEquals(List.of(), listNames(tempDir.resolve("data").resolve("manifests")), "no marker or manifest");
            } finally {
                server.stop();
            }
        }
    }

    private static void lostStoreAnswerIsRolledBack(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 3);
             NodeTestKit.Proxy proxy = new NodeTestKit.Proxy(cluster.nodes.get(0).port(),
                     NodeTestKit.Proxy.Mode.DROP_STORE_REPLIES)) {
            List<Integer> ports = new ArrayList<>(cluster.ports());
            ports.set(0, proxy.port());
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), ports, new NodeTestKit.Lines(), err, CHUNK);
            try {
                NodeTestKit.RawResponse response = upload(server.port(), randomBytes(1000, 4));
                checkEquals(500, response.status(), "status");
                checkEquals(1, proxy.droppedStores.get(), "stores whose answer was lost");
                checkContains(err.text(), "may or may not be stored");
                checkEquals(List.of(), listNames(cluster.nodes.get(0).chunksDir()), "the stored chunk was rolled back");
                checkEquals(List.of(), listNames(cluster.nodes.get(1).chunksDir()), "no copy on node 1");
                checkEquals(List.of(), listNames(cluster.nodes.get(2).chunksDir()), "no copy on node 2");
                checkEquals(List.of(), listNames(tempDir.resolve("data").resolve("manifests")), "the marker is gone");
            } finally {
                server.stop();
            }
        }
    }

    private static void unconfirmedRollbackKeepsMarker(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (Cluster cluster = Cluster.start(tempDir, 3);
             NodeTestKit.Proxy proxy = new NodeTestKit.Proxy(cluster.nodes.get(0).port(),
                     NodeTestKit.Proxy.Mode.DEAD_AFTER_DROP)) {
            List<Integer> ports = new ArrayList<>(cluster.ports());
            ports.set(0, proxy.port());
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(data, ports, new NodeTestKit.Lines(), err, CHUNK);
            String fileId;
            try {
                checkEquals(500, upload(server.port(), randomBytes(1000, 5)).status(), "status");
                checkContains(err.text(), "rollback of upload");
                List<String> chunks = listNames(cluster.nodes.get(0).chunksDir());
                checkEquals(1, chunks.size(), "the chunk the node stored is still there");
                fileId = chunks.get(0).substring(0, 32);
                checkEquals(List.of(fileId + ".pending"), listNames(data.resolve("manifests")), "the marker was kept");
            } finally {
                server.stop();
            }
            // The node itself was fine all along; the next start reaches it directly and recovers.
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer restarted = startServer(data, cluster.ports(), out, new NodeTestKit.Lines(), CHUNK);
            restarted.stop();
            checkContains(out.text(), "1 rolled back (1 chunk(s) deleted)");
            checkEquals(List.of(), listNames(cluster.nodes.get(0).chunksDir()), "recovery deleted the chunk");
            checkEquals(List.of(), listNames(data.resolve("manifests")), "recovery deleted the marker");
        }
    }

    private static void unreachableNodeStopsStart(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 3)) {
            cluster.nodes.get(1).close();
            IOException e = expectThrows(IOException.class, "start", () -> startServer(tempDir.resolve("data"),
                    cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkContains(e.getMessage(), "--node 127.0.0.1:" + cluster.nodes.get(1).port() + " did not answer");
            check(!Files.exists(tempDir.resolve("data").resolve("nodes.txt")), "nodes.txt was written");
        }
    }

    private static void membershipMustMatch(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (Cluster cluster = Cluster.start(tempDir, 4)) {
            List<Integer> abc = cluster.ports().subList(0, 3);
            startServer(data, abc, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK).stop();
            String recorded = Files.readString(data.resolve("nodes.txt"));

            List<Integer> abd = List.of(abc.get(0), abc.get(1), cluster.ports().get(3));
            DataDirectoryException other = expectThrows(DataDirectoryException.class, "another node",
                    () -> startServer(data, abd, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkEquals(DataDirectoryException.Reason.WRONG_NODES, other.reason(), "reason");
            checkContains(other.getMessage(), "Missing: " + cluster.ids().get(2));
            checkContains(other.getMessage(), "not recorded: " + cluster.ids().get(3));

            DataDirectoryException fewer = expectThrows(DataDirectoryException.class, "a node left out",
                    () -> startServer(data, abc.subList(0, 2), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkEquals(DataDirectoryException.Reason.WRONG_NODES, fewer.reason(), "reason");
            checkEquals(recorded, Files.readString(data.resolve("nodes.txt")), "nodes.txt is unchanged");

            List<Integer> reordered = List.of(abc.get(2), abc.get(0), abc.get(1));
            startServer(data, reordered, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK).stop();
        }
    }

    private static void sameNodeTwice(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 1);
             NodeTestKit.Proxy proxy = new NodeTestKit.Proxy(cluster.nodes.get(0).port(), NodeTestKit.Proxy.Mode.PASS)) {
            IOException e = expectThrows(IOException.class, "start", () -> startServer(tempDir.resolve("data"),
                    List.of(cluster.nodes.get(0).port(), proxy.port()), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkContains(e.getMessage(), "are the same storage node");
        }
    }

    private static void modeSwitchIsRefused(Path tempDir) throws Exception {
        try (Cluster cluster = Cluster.start(tempDir, 3)) {
            Path local = tempDir.resolve("local-data");
            DistributedFsServer server = startServer(local, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK);
            fileId(upload(server.port(), randomBytes(1000, 6)));
            server.stop();
            DataDirectoryException toRemote = expectThrows(DataDirectoryException.class, "local data with remote nodes",
                    () -> startServer(local, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkEquals(DataDirectoryException.Reason.WRONG_NODES, toRemote.reason(), "reason");
            checkContains(toRemote.getMessage(), "local node folders");
            check(!Files.exists(local.resolve("nodes.txt")), "nodes.txt was written");

            Path remote = tempDir.resolve("remote-data");
            startServer(remote, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK).stop();
            DataDirectoryException toLocal = expectThrows(DataDirectoryException.class, "remote data without nodes",
                    () -> startServer(remote, List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
            checkEquals(DataDirectoryException.Reason.WRONG_NODES, toLocal.reason(), "reason");
            checkContains(toLocal.getMessage(), "remote storage nodes");
        }
    }

    private static void damagedMembershipFile(Path tempDir) throws Exception {
        Path data = tempDir.resolve("data");
        try (Cluster cluster = Cluster.start(tempDir, 2)) {
            startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK).stop();
            String good = Files.readString(data.resolve("nodes.txt"));
            List<String> ids = new ArrayList<>(new TreeSet<>(cluster.ids()));
            for (String damaged : List.of(good.replace("format=1", "format=2"), good.trim(), good + "node=" + ids.get(0) + "\n",
                    good.replace(ids.get(0), ids.get(0).toUpperCase(java.util.Locale.ROOT)),
                    good.replace("node=" + ids.get(0) + "\nnode=" + ids.get(1), "node=" + ids.get(1) + "\nnode=" + ids.get(0)))) {
                Files.writeString(data.resolve("nodes.txt"), damaged);
                DataDirectoryException e = expectThrows(DataDirectoryException.class, "damaged nodes.txt",
                        () -> startServer(data, cluster.ports(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK));
                checkEquals(DataDirectoryException.Reason.INVALID_MARKER, e.reason(), "reason");
                checkEquals(damaged, Files.readString(data.resolve("nodes.txt")), "the file is left alone");
            }
        }
    }

    private static void nodeOptions(Path tempDir) throws Exception {
        String dir = tempDir.toString();
        DistributedFsServer.Arguments arguments = DistributedFsServer.parseArgs(new String[] {"--data-dir", dir,
                "--node", "127.0.0.1:9101", "--port", "0", "--node", "127.0.0.1:9102"});
        checkEquals(List.of(9101, 9102), arguments.nodePorts(), "node ports");
        checkEquals(List.of(), DistributedFsServer.parseArgs(new String[] {"--data-dir", dir}).nodePorts(), "no nodes");

        List<String[]> mistakes = new ArrayList<>();
        for (String bad : List.of("localhost:9101", "127.0.0.1", "127.0.0.1:", "127.0.0.1:0", "127.0.0.1:65536",
                "127.0.0.1:09101", "127.0.0.2:9101", "10.0.0.1:9101", "127.0.0.1:91a", "127.0.0.1:-1", ":9101",
                "[::1]:9101")) {
            mistakes.add(new String[] {"--data-dir", dir, "--node", bad});
        }
        mistakes.add(new String[] {"--data-dir", dir, "--node", "127.0.0.1:9101", "--node", "127.0.0.1:9101"});
        mistakes.add(new String[] {"--data-dir", dir, "--node"});
        List<String> seventeen = new ArrayList<>(List.of("--data-dir", dir));
        for (int i = 0; i < 17; i++) {
            seventeen.add("--node");
            seventeen.add("127.0.0.1:" + (9100 + i));
        }
        mistakes.add(seventeen.toArray(new String[0]));
        for (String[] args : mistakes) {
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            int code = DistributedFsServer.run(args, new PrintStream(new ByteArrayOutputStream(), true),
                    new PrintStream(err, true, StandardCharsets.UTF_8));
            checkEquals(DistributedFsServer.EXIT_USAGE, code, "exit code for " + Arrays.toString(args));
            checkContains(err.toString(StandardCharsets.UTF_8), DistributedFsServer.USAGE);
        }
    }

    /** Node servers in this process. */
    static final class Cluster implements AutoCloseable {
        final List<NodeTestKit.NodeHandle> nodes = new ArrayList<>();

        static Cluster start(Path tempDir, int count) throws IOException {
            Cluster cluster = new Cluster();
            try {
                for (int i = 0; i < count; i++) {
                    cluster.nodes.add(NodeTestKit.startNode(tempDir.resolve("node-" + i)));
                }
                return cluster;
            } catch (IOException | RuntimeException e) {
                cluster.close();
                throw e;
            }
        }

        List<Integer> ports() {
            return nodes.stream().map(NodeTestKit.NodeHandle::port).toList();
        }

        List<String> ids() {
            return nodes.stream().map(NodeTestKit.NodeHandle::nodeId).toList();
        }

        List<Integer> chunkCounts() throws IOException {
            List<Integer> counts = new ArrayList<>();
            for (NodeTestKit.NodeHandle node : nodes) {
                counts.add(listNames(node.chunksDir()).size());
            }
            return counts;
        }

        @Override
        public void close() {
            for (NodeTestKit.NodeHandle node : nodes) {
                node.close();
            }
        }
    }
}
