package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.status;
import static com.distributedfs.NodeTestKit.upload;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Plain-Java tests for {@code GET /cluster/status} (milestone M3): the exact JSON for local and
 * remote nodes, a node going DOWN and coming back UP, the method, query and Host checks, that the
 * answer contains no path, and that answering it sends nothing to the nodes.
 * Run: {@code java -cp "out;out-test" com.distributedfs.ClusterStatusEndpointTest}.
 */
public class ClusterStatusEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("clusterstatus-test-");

    private static final int CHUNK = 64 * 1024;

    /** An ISO-8601 instant, as Instant.toString writes it. */
    private static final String INSTANT = "\"20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z\"";

    public static void main(String[] args) {
        RUNNER.run("local node folders: the exact JSON, every node UP", ClusterStatusEndpointTest::localStatus);
        RUNNER.run("remote nodes: the exact JSON, with node IDs and 127.0.0.1 addresses", ClusterStatusEndpointTest::remoteStatus);
        RUNNER.run("a stopped node turns DOWN after 3 failed checks, and UP again on 1 good check",
                ClusterStatusEndpointTest::downAndUp);
        RUNNER.run("uploads and downloads still work while a node is DOWN", ClusterStatusEndpointTest::filesWhileDown);
        RUNNER.run("only GET is allowed: other methods get 405 with Allow: GET", ClusterStatusEndpointTest::getOnly);
        RUNNER.run("any query is 400 invalid_query", ClusterStatusEndpointTest::queryRefused);
        RUNNER.run("a request for another Host is 403", ClusterStatusEndpointTest::hostChecked);
        RUNNER.run("the answer contains no file system path", ClusterStatusEndpointTest::noPaths);
        RUNNER.run("answering the status sends no request to the nodes", ClusterStatusEndpointTest::noNodeRequests);
        RUNNER.run("a handler without the cluster endpoints answers 404 not_found", ClusterStatusEndpointTest::testHandler404);
        RUNNER.run("stopping the server stops its heartbeat thread", ClusterStatusEndpointTest::stopEndsHeartbeat);
        RUNNER.finish();
    }

    private static void localStatus(Path tempDir) throws Exception {
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            NodeTestKit.RawResponse response = status(server.port());
            checkEquals(200, response.status(), "status");
            checkEquals("application/json; charset=utf-8", response.header("Content-Type"), "Content-Type");
            StringBuilder expected = new StringBuilder("\\{\"replicationFactor\":2,\"heartbeatIntervalMillis\":60000,"
                    + "\"downAfterFailures\":3,\"nodes\":\\[");
            for (int i = 0; i < 3; i++) {
                expected.append(i == 0 ? "" : ",").append("\\{\"index\":").append(i)
                        .append(",\"nodeId\":null,\"address\":\"local folder node-").append(i)
                        .append("\",\"state\":\"UP\",\"lastGoodCheck\":").append(INSTANT).append(",\"failuresInARow\":0\\}");
            }
            expected.append("\\]\\}");
            check(Pattern.matches(expected.toString(), response.text()), "unexpected JSON: " + response.text());
        } finally {
            server.stop();
        }
    }

    private static void remoteStatus(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK, 60_000);
            try {
                String json = status(server.port()).text();
                StringBuilder expected = new StringBuilder("\\{\"replicationFactor\":2,\"heartbeatIntervalMillis\":60000,"
                        + "\"downAfterFailures\":3,\"nodes\":\\[");
                for (int i = 0; i < 3; i++) {
                    expected.append(i == 0 ? "" : ",").append("\\{\"index\":").append(i)
                            .append(",\"nodeId\":\"").append(cluster.nodes.get(i).nodeId())
                            .append("\",\"address\":\"127\\.0\\.0\\.1:").append(cluster.nodes.get(i).port())
                            .append("\",\"state\":\"UP\",\"lastGoodCheck\":").append(INSTANT)
                            .append(",\"failuresInARow\":0\\}");
                }
                expected.append("\\]\\}");
                check(Pattern.matches(expected.toString(), json), "unexpected JSON: " + json);
            } finally {
                server.stop();
            }
        }
    }

    private static void downAndUp(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            NodeTestKit.Lines err = new NodeTestKit.Lines();
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    err, CHUNK, 100);
            try {
                int nodePort = cluster.nodes.get(1).port();
                String lastGood = nodeField(status(server.port()).text(), 1, "lastGoodCheck");
                cluster.nodes.get(1).close();
                NodeTestKit.waitUntil(10_000, "node 1 DOWN", () -> nodeState(server.port(), 1).equals("DOWN"));
                String down = status(server.port()).text();
                check(Long.parseLong(nodeField(down, 1, "failuresInARow")) >= 3, "failures in a row: " + down);
                check(!nodeField(down, 1, "lastGoodCheck").equals("null"), "the last good check is kept: " + down);
                check(nodeField(down, 1, "lastGoodCheck").compareTo(lastGood) >= 0, "last good check moved back");
                checkEquals("UP", nodeField(down, 0, "state"), "node 0");
                checkEquals("UP", nodeField(down, 2, "state"), "node 2");
                checkContains(err.text(), "Warning: node 1 (");
                checkContains(err.text(), "is DOWN after 3 failed health checks in a row");

                cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("node-1"), nodePort));
                NodeTestKit.waitUntil(10_000, "node 1 UP", () -> nodeState(server.port(), 1).equals("UP"));
                checkEquals("0", nodeField(status(server.port()).text(), 1, "failuresInARow"), "failures after coming back");
                checkContains(err.text(), "Node 1 (");
                checkContains(err.text(), "is UP again");
            } finally {
                server.stop();
            }
        }
    }

    private static void filesWhileDown(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK, 100);
            try {
                byte[] before = randomBytes(3 * CHUNK, 1);
                String first = fileId(upload(server.port(), before));
                cluster.nodes.get(2).close();
                NodeTestKit.waitUntil(10_000, "node 2 DOWN", () -> nodeState(server.port(), 2).equals("DOWN"));
                check(java.util.Arrays.equals(before, NodeTestKit.download(server.port(), first).body()),
                        "a file stored before the node went DOWN");
                byte[] during = randomBytes(2 * CHUNK, 2);
                String second = fileId(upload(server.port(), during));
                check(java.util.Arrays.equals(during, NodeTestKit.download(server.port(), second).body()),
                        "a file stored while the node is DOWN");
            } finally {
                server.stop();
            }
        }
    }

    private static void getOnly(Path tempDir) throws Exception {
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            for (String method : List.of("POST", "PUT", "DELETE", "HEAD")) {
                NodeTestKit.RawResponse response = raw(server.port(), method + " /cluster/status HTTP/1.1\r\n"
                        + NodeTestKit.host(server.port()) + "\r\nContent-Length: 0", null);
                checkEquals(405, response.status(), method + " status");
                checkEquals("GET", response.header("Allow"), method + " Allow");
                if (!method.equals("HEAD")) {
                    checkContains(response.text(), "\"error\":\"method_not_allowed\"");
                }
            }
        } finally {
            server.stop();
        }
    }

    private static void queryRefused(Path tempDir) throws Exception {
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            for (String query : List.of("?", "?x=1", "?dryRun=true", "?verbose")) {
                NodeTestKit.RawResponse response = raw(server.port(), "GET /cluster/status" + query + " HTTP/1.1\r\n"
                        + NodeTestKit.host(server.port()), null);
                checkEquals(400, response.status(), query + " status");
                checkEquals(ApiError.INVALID_QUERY.toJson(), response.text(), query + " body");
            }
            checkEquals(404, raw(server.port(), "GET /cluster/status/ HTTP/1.1\r\n" + NodeTestKit.host(server.port()),
                    null).status(), "a trailing slash");
            checkEquals(404, raw(server.port(), "GET /cluster HTTP/1.1\r\n" + NodeTestKit.host(server.port()),
                    null).status(), "/cluster");
        } finally {
            server.stop();
        }
    }

    private static void hostChecked(Path tempDir) throws Exception {
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            NodeTestKit.RawResponse response = raw(server.port(), "GET /cluster/status HTTP/1.1\r\nHost: example.com",
                    null);
            checkEquals(403, response.status(), "status");
            checkEquals(ApiError.HOST_NOT_ALLOWED.toJson(), response.text(), "body");
        } finally {
            server.stop();
        }
    }

    private static void noPaths(Path tempDir) throws Exception {
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            Path nodeDir = tempDir.resolve("data").resolve("nodes").resolve("node-1");
            Path moved = tempDir.resolve("node-1-moved");
            Files.move(nodeDir, moved);
            for (int i = 0; i < 3; i++) {
                server.monitor().checkNow();
            }
            String json = status(server.port()).text();
            checkEquals("DOWN", nodeField(json, 1, "state"), "node 1, whose folder is gone");
            check(!json.contains(":\\\\") && !json.contains(":/") && !json.contains(tempDir.getFileName().toString()),
                    "a path in " + json);
        } finally {
            server.stop();
        }
    }

    private static void noNodeRequests(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK, 3_600_000);
            try {
                status(server.port()); // the first heartbeat check runs when the monitor starts; let it finish
                NodeTestKit.sleepQuietly(300);
                long[] before = requestLines(cluster);
                for (int i = 0; i < 5; i++) {
                    checkEquals(200, status(server.port()).status(), "status " + i);
                }
                checkEquals(java.util.Arrays.toString(before), java.util.Arrays.toString(requestLines(cluster)),
                        "requests each node logged");
            } finally {
                server.stop();
            }
        }
    }

    private static void testHandler404(Path tempDir) throws Exception {
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(tempDir.resolve("data"), 0,
                4, 64, 0, 0, new NodeTestKit.Lines().stream(), new NodeTestKit.Lines().stream(), 64L * 1024 * 1024, CHUNK,
                List.of(), NodeTestKit.fastConfig()), FileApiHandler::new);
        try {
            NodeTestKit.RawResponse response = status(server.port());
            checkEquals(404, response.status(), "status");
            checkEquals(ApiError.NOT_FOUND.toJson(), response.text(), "body");
            checkEquals(404, NodeTestKit.repair(server.port(), null).status(), "repair");
        } finally {
            server.stop();
        }
    }

    private static void stopEndsHeartbeat(Path tempDir) throws Exception {
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 50);
        try {
            check(heartbeatThreads() >= 1, "no heartbeat thread while the server runs");
        } finally {
            server.stop();
        }
        checkEquals(0L, heartbeatThreads(), "heartbeat threads after stop()");
    }

    private static long heartbeatThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("distributedfs-heartbeat") && t.isAlive()).count();
    }

    /** The node's state in the status JSON, or "" if the status could not be read. */
    static String nodeState(int port, int index) {
        try {
            return nodeField(status(port).text(), index, "state");
        } catch (java.io.IOException e) {
            return "";
        }
    }

    /** One field of one node's object in the status JSON, without quotes ("null" for null). */
    static String nodeField(String json, int index, String field) {
        java.util.regex.Matcher node = Pattern.compile("\\{\"index\":" + index + ",[^}]*\\}").matcher(json);
        check(node.find(), "no node " + index + " in " + json);
        java.util.regex.Matcher value = Pattern.compile("\"" + field + "\":(\"[^\"]*\"|[^,}]*)").matcher(node.group());
        check(value.find(), "no " + field + " for node " + index + " in " + json);
        return value.group(1).replace("\"", "");
    }

    private static long[] requestLines(RemoteServerTest.Cluster cluster) {
        long[] lines = new long[cluster.nodes.size()];
        for (int i = 0; i < lines.length; i++) {
            lines[i] = cluster.nodes.get(i).out().text().lines().count();
        }
        return lines;
    }
}
