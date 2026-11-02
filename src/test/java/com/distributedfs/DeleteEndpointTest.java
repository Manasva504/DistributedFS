package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.startNode;
import static com.distributedfs.NodeTestKit.startNodeOnPort;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;
import static com.distributedfs.NodeTestKit.waitUntil;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Plain-Java tests of {@code DELETE /files/<fileId>} over HTTP (PR #12): the answer, every copy
 * gone on every node, and what downloads, {@code GET /files}, the placement and the activity feed
 * show afterwards; a node that is down (202, the marker kept, and a repair finishing the delete once
 * it is back); and the request rules (IDs, methods, queries, a body).
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.DeleteEndpointTest}.
 */
public class DeleteEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("deleteendpoint-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("DELETE answers 200; the file, its copies and its marker are gone, and other files are untouched",
                DeleteEndpointTest::deletes);
        RUNNER.run("after a delete, the download, the placement and a second delete are 404, and GET /files omits it",
                DeleteEndpointTest::goneEverywhere);
        RUNNER.run("the activity feed records file_deleted with the name and the outcome", DeleteEndpointTest::event);
        RUNNER.run("an upload in progress (a pending marker only) is not a file: 404", DeleteEndpointTest::pendingIsNotAFile);
        RUNNER.run("the raw file ID is checked exactly, and a query is refused", DeleteEndpointTest::idAndQuery);
        RUNNER.run("other methods on /files/<id> are 405 with Allow: GET, DELETE; a declared body is discarded",
                DeleteEndpointTest::methodsAndBody);
        RUNNER.run("with a node down, DELETE answers 202, keeps the marker, and GET /files counts the delete as waiting",
                DeleteEndpointTest::nodeDown);
        RUNNER.run("once the node is back, a repair finishes the delete (delete_finished), and every copy is gone",
                DeleteEndpointTest::repairFinishes);
        RUNNER.run("once the node is back, a second DELETE finishes the delete itself", DeleteEndpointTest::secondDeleteFinishes);
        RUNNER.finish();
    }

    private static void deletes(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String keep = fileId(upload(port, randomBytes(2 * CHUNK, 1)));
            String id = fileId(upload(port, randomBytes(3 * CHUNK + 1, 2)));
            NodeTestKit.RawResponse response = delete(port, id);
            checkEquals(200, response.status(), "status");
            Map<String, Object> json = TestJson.object(response.text());
            checkEquals(Map.of("fileId", id, "chunksDeleted", 4L, "finished", true), withoutNull(json), "body");
            check(json.containsKey("name") && json.get("name") == null, "name: " + json);
            checkEquals(0, copiesOf(server, id), "copies left");
            checkEquals(4, copiesOf(server, keep), "copies of the other file");
            checkEquals(List.of(keep + ".manifest"), listNames(server.dataRoot().resolve("manifests")), "manifest folder");
        } finally {
            server.stop();
        }
    }

    private static void goneEverywhere(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String id = fileId(upload(port, randomBytes(CHUNK, 3)));
            checkEquals(200, delete(port, id).status(), "delete");
            checkError(download(port, id), ApiError.FILE_NOT_FOUND, "download");
            checkError(get(port, "/files/" + id + "/placement"), ApiError.FILE_NOT_FOUND, "placement");
            checkError(delete(port, id), ApiError.FILE_NOT_FOUND, "second delete");
            Map<String, Object> files = TestJson.object(get(port, "/files").text());
            checkEquals(List.of(), files.get("files"), "GET /files");
            checkEquals(Map.of("pending", 0L, "unreadable", 0L, "deleting", 0L), files.get("skipped"), "skipped");
        } finally {
            server.stop();
        }
    }

    private static void event(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(2 * CHUNK, 4);
            String id = fileId(raw(port, "POST /files HTTP/1.1\r\n" + host(port) + "\r\nX-File-Name: report.pdf"
                    + "\r\nContent-Type: application/octet-stream\r\nContent-Length: " + data.length, data));
            checkEquals(200, delete(port, id).status(), "delete");
            Map<String, Object> event = TestJson.objects(TestJson.object(get(port, "/cluster/events").text()), "events").get(0);
            checkEquals("file_deleted", event.get("type"), "type");
            checkEquals(id, event.get("fileId"), "fileId");
            checkEquals("report.pdf", event.get("name"), "name");
            checkEquals(2L, event.get("chunksDeleted"), "chunksDeleted");
            checkEquals(true, event.get("finished"), "finished");
        } finally {
            server.stop();
        }
    }

    private static void pendingIsNotAFile(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = FileIds.newFileId();
            server.manifests().createPending(new ManifestStore.PendingUpload(id, 1));
            checkError(delete(server.port(), id), ApiError.FILE_NOT_FOUND, "pending");
            check(server.manifests().readPending(id).isPresent(), "the pending marker was removed");
        } finally {
            server.stop();
        }
    }

    private static void idAndQuery(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String id = fileId(upload(port, randomBytes(CHUNK, 5)));
            String encoded = "%" + Integer.toHexString(id.charAt(0)) + id.substring(1);
            for (String target : List.of("/files/" + id.toUpperCase(), "/files/" + encoded, "/files/" + id + "/",
                    "/files/../" + id, "/files/")) {
                checkError(raw(port, "DELETE " + target + " HTTP/1.1\r\n" + host(port), null), ApiError.INVALID_FILE_ID, target);
            }
            checkError(raw(port, "DELETE /files/" + id + "?force=1 HTTP/1.1\r\n" + host(port), null), ApiError.INVALID_QUERY, "query");
            check(Arrays.equals(randomBytes(CHUNK, 5), download(port, id).body()), "the file after the refused deletes");
            checkError(raw(port, "DELETE /files HTTP/1.1\r\n" + host(port), null), ApiError.METHOD_NOT_ALLOWED, "DELETE /files");
        } finally {
            server.stop();
        }
    }

    private static void methodsAndBody(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String id = fileId(upload(port, randomBytes(CHUNK, 6)));
            for (String method : List.of("PUT", "POST", "PATCH", "HEAD")) {
                NodeTestKit.RawResponse response = raw(port, method + " /files/" + id + " HTTP/1.1\r\n" + host(port)
                        + "\r\nContent-Length: 0", null);
                checkEquals(405, response.status(), method);
                checkEquals("GET, DELETE", response.header("Allow"), method + " Allow");
            }
            NodeTestKit.RawResponse withBody = raw(port, "DELETE /files/" + id + " HTTP/1.1\r\n" + host(port)
                    + "\r\nContent-Length: 5", "hello".getBytes());
            checkEquals(200, withBody.status(), "a DELETE with a body");
        } finally {
            server.stop();
        }
    }

    private static void nodeDown(Path tempDir) throws Exception {
        try (Cluster c = new Cluster(tempDir)) {
            int port = c.server.port();
            String id = fileId(upload(port, randomBytes(3 * CHUNK, 7)));
            c.stopNode(1);
            NodeTestKit.RawResponse response = delete(port, id);
            checkEquals(202, response.status(), "status");
            checkEquals(false, TestJson.object(response.text()).get("finished"), "finished");
            check(c.server.manifests().readDeletion(id).isPresent(), "the marker was removed");
            checkError(download(port, id), ApiError.FILE_NOT_FOUND, "download after the commit");
            c.server.placement().invalidate();
            Map<String, Object> files = TestJson.object(get(port, "/files").text());
            checkEquals(1L, TestJson.map(files.get("skipped")).get("deleting"), "skipped.deleting");
            checkEquals(List.of(), files.get("files"), "files");
            Map<String, Object> event = TestJson.objects(TestJson.object(get(port, "/cluster/events").text()), "events").get(0);
            checkEquals("file_deleted", event.get("type"), "event");
            checkEquals(false, event.get("finished"), "event finished");
        }
    }

    private static void repairFinishes(Path tempDir) throws Exception {
        try (Cluster c = new Cluster(tempDir)) {
            int port = c.server.port();
            String id = fileId(upload(port, randomBytes(3 * CHUNK, 8)));
            c.stopNode(1);
            checkEquals(202, delete(port, id).status(), "delete with node 1 down");
            Map<String, Object> waiting = TestJson.object(NodeTestKit.repair(port, null).text());
            checkEquals(1L, waiting.get("deletesWaiting"), "deletesWaiting while node 1 is down");
            checkEquals(0L, waiting.get("deletesFinished"), "deletesFinished while node 1 is down");
            check(c.server.manifests().readDeletion(id).isPresent(), "the marker was removed while node 1 is down");
            c.restartNode(1);
            Map<String, Object> dry = TestJson.object(NodeTestKit.repair(port, "dryRun=true").text());
            checkEquals(1L, dry.get("deletesFinished"), "a dry run counts the delete");
            check(c.server.manifests().readDeletion(id).isPresent(), "the dry run removed the marker");
            Map<String, Object> done = TestJson.object(NodeTestKit.repair(port, null).text());
            checkEquals(1L, done.get("deletesFinished"), "deletesFinished");
            checkEquals(0L, done.get("deletesWaiting"), "deletesWaiting");
            check(c.server.manifests().readDeletion(id).isEmpty(), "the marker is still there");
            for (NodeTestKit.NodeHandle node : c.nodes) {
                for (String name : listNames(node.chunksDir())) {
                    check(!name.startsWith(id + "_"), "a copy is left: " + name);
                }
            }
            List<Map<String, Object>> events = TestJson.objects(TestJson.object(get(port, "/cluster/events").text()), "events");
            check(events.stream().anyMatch(e -> "delete_finished".equals(e.get("type")) && id.equals(e.get("fileId"))),
                    "no delete_finished event: " + events);
        }
    }

    private static void secondDeleteFinishes(Path tempDir) throws Exception {
        try (Cluster c = new Cluster(tempDir)) {
            int port = c.server.port();
            String id = fileId(upload(port, randomBytes(2 * CHUNK, 9)));
            c.stopNode(2);
            checkEquals(202, delete(port, id).status(), "first delete");
            c.restartNode(2);
            NodeTestKit.RawResponse second = delete(port, id);
            checkEquals(200, second.status(), "second delete");
            checkEquals(true, TestJson.object(second.text()).get("finished"), "finished");
            check(c.server.manifests().readDeletion(id).isEmpty(), "the marker");
            checkError(delete(port, id), ApiError.FILE_NOT_FOUND, "third delete");
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    /** Three node processes in this JVM and a server with a fast heartbeat; nodes can be stopped and restarted. */
    private static final class Cluster implements AutoCloseable {
        final List<NodeTestKit.NodeHandle> nodes = new ArrayList<>();
        final List<Path> folders = new ArrayList<>();
        final DistributedFsServer server;

        Cluster(Path tempDir) throws IOException {
            List<Integer> ports = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                folders.add(tempDir.resolve("node-" + i));
                nodes.add(startNode(folders.get(i)));
                ports.add(nodes.get(i).port());
            }
            server = startServer(tempDir.resolve("data"), ports, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 50);
        }

        void stopNode(int index) {
            nodes.get(index).close();
            waitUntil(10_000, "node " + index + " DOWN",
                    () -> server.monitor().snapshot().get(index).state() == HeartbeatMonitor.State.DOWN);
        }

        void restartNode(int index) throws IOException {
            nodes.set(index, startNodeOnPort(folders.get(index), nodes.get(index).port()));
            waitUntil(10_000, "node " + index + " UP",
                    () -> server.monitor().snapshot().get(index).state() == HeartbeatMonitor.State.UP);
        }

        @Override
        public void close() {
            server.stop();
            for (NodeTestKit.NodeHandle node : nodes) {
                node.close();
            }
        }
    }

    private static NodeTestKit.RawResponse delete(int port, String id) throws IOException {
        return raw(port, "DELETE /files/" + id + " HTTP/1.1\r\n" + host(port), null);
    }

    private static NodeTestKit.RawResponse get(int port, String target) throws IOException {
        return raw(port, "GET " + target + " HTTP/1.1\r\n" + host(port), null);
    }

    private static void checkError(NodeTestKit.RawResponse response, ApiError error, String what) {
        checkEquals(error.status(), response.status(), what + ": status");
        checkEquals(error.toJson(), response.text(), what + ": body");
    }

    private static Map<String, Object> withoutNull(Map<String, Object> json) {
        Map<String, Object> copy = new java.util.LinkedHashMap<>(json);
        copy.values().removeIf(java.util.Objects::isNull);
        return copy;
    }

    private static int copiesOf(DistributedFsServer server, String id) throws IOException {
        int copies = 0;
        for (int i = 0; i < 3; i++) {
            for (String name : listNames(server.dataRoot().resolve("nodes").resolve("node-" + i))) {
                if (name.startsWith(id + "_")) {
                    copies++;
                }
            }
        }
        return copies;
    }
}
