package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.repair;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plain-Java tests for the web console's activity feed (milestone M4b-2): {@link ClusterEvents}
 * on its own (sequence numbers, order, the 100-event limit, the JSON of each event type), the
 * heartbeat recording only UP and DOWN changes, and {@code GET /cluster/events} on a running
 * server after uploads, a repair and a dry run.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.ClusterEventsTest}.
 */
public class ClusterEventsTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("clusterevents-test-");

    private static final int CHUNK = 64 * 1024;
    private static final String ID = "0123456789abcdef0123456789abcdef";

    public static void main(String[] args) {
        RUNNER.run("a new feed is empty and says when it started and how many events it keeps", ClusterEventsTest::empty);
        RUNNER.run("events get sequence numbers from 1 and the clock's time, and are listed newest first",
                ClusterEventsTest::orderAndTimes);
        RUNNER.run("only the last 100 events are kept", ClusterEventsTest::capacity);
        RUNNER.run("node_down, node_up and upload_finished have exactly their documented fields",
                ClusterEventsTest::nodeAndUploadFields);
        RUNNER.run("repair_finished carries the report's counts and whether it was a dry run", ClusterEventsTest::repairFields);
        RUNNER.run("the heartbeat records a change once: DOWN after the third failure, UP after one good check",
                ClusterEventsTest::heartbeatRecordsChanges);
        RUNNER.run("a heartbeat without a feed records nothing and works as before", ClusterEventsTest::heartbeatWithoutFeed);
        RUNNER.run("GET /cluster/events lists every committed upload with its chunks and copies", ClusterEventsTest::uploadsListed);
        RUNNER.run("a refused or failed upload records nothing", ClusterEventsTest::failedUploadNotListed);
        RUNNER.run("a dry run and a repair are both listed, with their own counts", ClusterEventsTest::repairsListed);
        RUNNER.run("GET /cluster/events takes no query and only GET", ClusterEventsTest::methodAndQuery);
        RUNNER.run("concurrent recording keeps every sequence number exactly once", ClusterEventsTest::concurrent);
        RUNNER.finish();
    }

    private static void empty(Path tempDir) {
        NodeTestKit.TestClock clock = new NodeTestKit.TestClock();
        ClusterEvents events = new ClusterEvents(clock);
        Map<String, Object> json = TestJson.object(events.toJson());
        checkEquals("2026-10-26T12:00:00Z", json.get("startedAt"), "startedAt");
        checkEquals(100L, json.get("capacity"), "capacity");
        checkEquals(List.of(), json.get("events"), "events");
        checkEquals(List.of("startedAt", "capacity", "events"), new ArrayList<>(json.keySet()), "fields");
    }

    private static void orderAndTimes(Path tempDir) {
        NodeTestKit.TestClock clock = new NodeTestKit.TestClock();
        ClusterEvents events = new ClusterEvents(clock);
        events.nodeDown(1, 3);
        clock.advance(1500);
        events.nodeUp(1);
        List<Map<String, Object>> list = TestJson.objects(TestJson.object(events.toJson()), "events");
        checkEquals(2, list.size(), "events");
        checkEquals(2L, list.get(0).get("seq"), "newest seq");
        checkEquals("node_up", list.get(0).get("type"), "newest type");
        checkEquals("2026-10-26T12:00:01.500Z", list.get(0).get("time"), "newest time");
        checkEquals(1L, list.get(1).get("seq"), "oldest seq");
        checkEquals("2026-10-26T12:00:00Z", list.get(1).get("time"), "oldest time");
    }

    private static void capacity(Path tempDir) {
        ClusterEvents events = new ClusterEvents(new NodeTestKit.TestClock());
        for (int i = 0; i < 150; i++) {
            events.nodeUp(i % 3);
        }
        List<Map<String, Object>> list = TestJson.objects(TestJson.object(events.toJson()), "events");
        checkEquals(ClusterEvents.CAPACITY, list.size(), "kept");
        checkEquals(150L, list.get(0).get("seq"), "newest");
        checkEquals(51L, list.get(list.size() - 1).get("seq"), "oldest kept");
        for (int i = 1; i < list.size(); i++) {
            checkEquals((Long) list.get(i - 1).get("seq") - 1, list.get(i).get("seq"), "seq " + i);
        }
    }

    private static void nodeAndUploadFields(Path tempDir) {
        ClusterEvents events = new ClusterEvents(new NodeTestKit.TestClock());
        events.nodeDown(2, 3);
        events.nodeUp(2);
        events.uploadFinished(new FileManifest(ID, 5L * 1024 * 1024, "a".repeat(64), 1024 * 1024, 5), 10);
        List<Map<String, Object>> list = TestJson.objects(TestJson.object(events.toJson()), "events");
        Map<String, Object> expected = new java.util.LinkedHashMap<>(Map.of("seq", 3L, "time", "2026-10-26T12:00:00Z",
                "type", "upload_finished", "fileId", ID, "size", 5L * 1024 * 1024, "chunks", 5L, "copies", 10L));
        expected.put("name", null); // no name: the upload gave none
        checkEquals(expected, list.get(0), "upload_finished");
        checkEquals(Map.of("seq", 2L, "time", "2026-10-26T12:00:00Z", "type", "node_up", "node", 2L), list.get(1), "node_up");
        checkEquals(Map.of("seq", 1L, "time", "2026-10-26T12:00:00Z", "type", "node_down", "node", 2L, "failuresInARow", 3L),
                list.get(2), "node_down");
    }

    private static void repairFields(Path tempDir) {
        ClusterEvents events = new ClusterEvents(new NodeTestKit.TestClock());
        events.repairFinished(new RepairReport(true, 2, List.of(), 1, 0, 10, 6, 4, 0, 0, 0, 0, 0, false, List.of(),
                List.of(), 97));
        events.repairFinished(new RepairReport(false, 2, List.of(), 1, 0, 10, 5, 3, 1, 2, 1, 2, 0, false, List.of(),
                List.of(ID + "_0"), 410));
        List<Map<String, Object>> list = TestJson.objects(TestJson.object(events.toJson()), "events");
        Map<String, Object> real = list.get(0);
        checkEquals(false, real.get("dryRun"), "dryRun of the repair");
        checkEquals(10L, real.get("chunksChecked"), "chunksChecked");
        checkEquals(3L, real.get("copiesAdded"), "copiesAdded");
        checkEquals(1L, real.get("copiesReplaced"), "copiesReplaced");
        checkEquals(2L, real.get("copiesRemoved"), "copiesRemoved");
        checkEquals(1L, real.get("chunksLost"), "chunksLost");
        checkEquals(2L, real.get("chunksUncertain"), "chunksUncertain");
        checkEquals(410L, real.get("durationMillis"), "durationMillis");
        checkEquals(List.of("seq", "time", "type", "dryRun", "chunksChecked", "copiesAdded", "copiesReplaced",
                "copiesRemoved", "chunksLost", "chunksUncertain", "durationMillis"), new ArrayList<>(real.keySet()), "fields");
        checkEquals(true, list.get(1).get("dryRun"), "dryRun of the dry run");
        checkEquals(4L, list.get(1).get("copiesAdded"), "dry run copiesAdded");
    }

    private static void heartbeatRecordsChanges(Path tempDir) throws Exception {
        NodeTestKit.ControlledNode node = new NodeTestKit.ControlledNode(Files.createDirectories(tempDir.resolve("n0")));
        NodeTestKit.ControlledNode other = new NodeTestKit.ControlledNode(Files.createDirectories(tempDir.resolve("n1")));
        NodeTestKit.TestClock clock = new NodeTestKit.TestClock();
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        HeartbeatMonitor monitor = new HeartbeatMonitor(List.of(node, other), List.of(true, true), 1_000, 3, clock, err.stream());
        ClusterEvents events = new ClusterEvents(clock);
        monitor.recordEventsIn(events);
        monitor.checkNow();
        checkEquals(0, events.newestFirst().size(), "events while every node is UP");
        node.down = true;
        monitor.checkNow();
        monitor.checkNow();
        checkEquals(0, events.newestFirst().size(), "events after 2 failures");
        monitor.checkNow();
        monitor.checkNow();
        monitor.checkNow();
        List<ClusterEvents.Event> list = events.newestFirst();
        checkEquals(1, list.size(), "events after 5 failures");
        checkEquals("node_down", list.get(0).type(), "type");
        checkEquals(",\"node\":0,\"failuresInARow\":3", list.get(0).fields(), "fields");
        node.down = false;
        monitor.checkNow();
        monitor.checkNow();
        list = events.newestFirst();
        checkEquals(2, list.size(), "events after coming back");
        checkEquals("node_up", list.get(0).type(), "type");
        checkEquals(",\"node\":0", list.get(0).fields(), "fields");
    }

    private static void heartbeatWithoutFeed(Path tempDir) throws Exception {
        NodeTestKit.ControlledNode node = new NodeTestKit.ControlledNode(Files.createDirectories(tempDir.resolve("n0")));
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        HeartbeatMonitor monitor = new HeartbeatMonitor(List.of(node), List.of(true), 1_000, 1, new NodeTestKit.TestClock(),
                err.stream());
        node.down = true;
        monitor.checkNow();
        checkEquals(HeartbeatMonitor.State.DOWN, monitor.snapshot().get(0).state(), "state");
        NodeTestKit.checkContains(err.text(), "is DOWN after 1 failed health checks");
    }

    private static void uploadsListed(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String first = fileId(upload(server.port(), randomBytes(3 * CHUNK, 1)));
            String empty = fileId(upload(server.port(), new byte[0]));
            List<Map<String, Object>> list = events(server);
            checkEquals(2, list.size(), "events");
            checkEquals(empty, list.get(0).get("fileId"), "newest");
            checkEquals(0L, list.get(0).get("chunks"), "chunks of the empty file");
            checkEquals(0L, list.get(0).get("copies"), "copies of the empty file");
            checkEquals(first, list.get(1).get("fileId"), "oldest");
            checkEquals(3L, list.get(1).get("chunks"), "chunks");
            checkEquals(6L, list.get(1).get("copies"), "copies: 2 per chunk");
            checkEquals((long) 3 * CHUNK, list.get(1).get("size"), "size");
        } finally {
            server.stop();
        }
    }

    private static void failedUploadNotListed(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(CHUNK, 2);
            checkEquals(400, raw(port, "POST /files HTTP/1.1\r\n" + host(port) + "\r\nContent-Type: application/octet-stream"
                    + "\r\nX-Content-SHA256: " + "0".repeat(64) + "\r\nContent-Length: " + data.length, data).status(),
                    "checksum mismatch");
            checkEquals(403, raw(port, "POST /files HTTP/1.1\r\n" + host(port) + "\r\nOrigin: http://evil.example"
                    + "\r\nContent-Type: application/octet-stream\r\nContent-Length: " + data.length, data).status(), "cross-site");
            checkEquals(List.of(), events(server), "events");
        } finally {
            server.stop();
        }
    }

    private static void repairsListed(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 3)));
            Path copy = null;
            for (int i = 0; i < 3 && copy == null; i++) {
                Path candidate = tempDir.resolve("data").resolve("nodes").resolve("node-" + i).resolve(FileIds.chunkId(id, 0));
                if (Files.exists(candidate)) {
                    copy = candidate;
                }
            }
            Files.delete(copy);
            checkEquals(200, repair(server.port(), "dryRun=true").status(), "dry run");
            checkEquals(200, repair(server.port(), null).status(), "repair");
            List<Map<String, Object>> list = events(server);
            checkEquals(3, list.size(), "events");
            checkEquals("repair_finished", list.get(0).get("type"), "newest");
            checkEquals(false, list.get(0).get("dryRun"), "the repair");
            checkEquals(1L, list.get(0).get("copiesAdded"), "copies added by the repair");
            checkEquals(true, list.get(1).get("dryRun"), "the dry run");
            checkEquals(1L, list.get(1).get("copiesAdded"), "copies the dry run would add");
            checkEquals(2L, list.get(1).get("chunksChecked"), "chunks the dry run checked");
            checkEquals("upload_finished", list.get(2).get("type"), "oldest");
        } finally {
            server.stop();
        }
    }

    private static void methodAndQuery(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String target : List.of("/cluster/events?x=1", "/cluster/events?")) {
                NodeTestKit.RawResponse response = raw(port, "GET " + target + " HTTP/1.1\r\n" + host(port), null);
                checkEquals(400, response.status(), target);
                checkEquals(ApiError.INVALID_QUERY.toJson(), response.text(), target + " body");
            }
            for (String method : List.of("POST", "PUT", "DELETE", "HEAD")) {
                NodeTestKit.RawResponse response = raw(port, method + " /cluster/events HTTP/1.1\r\n" + host(port)
                        + "\r\nContent-Length: 0", null);
                checkEquals(405, response.status(), method);
                checkEquals("GET", response.header("Allow"), method + " Allow");
            }
            NodeTestKit.RawResponse ok = raw(port, "GET /cluster/events HTTP/1.1\r\n" + host(port), null);
            checkEquals(200, ok.status(), "GET");
            checkEquals("application/json; charset=utf-8", ok.header("Content-Type"), "type");
            checkEquals("nosniff", ok.header("X-Content-Type-Options"), "nosniff");
            checkEquals(404, raw(port, "GET /cluster/events/ HTTP/1.1\r\n" + host(port), null).status(), "trailing slash");
        } finally {
            server.stop();
        }
    }

    private static void concurrent(Path tempDir) throws Exception {
        ClusterEvents events = new ClusterEvents(new NodeTestKit.TestClock());
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            int node = t;
            threads.add(new Thread(() -> {
                for (int i = 0; i < 25; i++) {
                    events.nodeUp(node);
                }
            }));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(10_000);
        }
        List<ClusterEvents.Event> list = events.newestFirst();
        checkEquals(100, list.size(), "events");
        for (int i = 0; i < list.size(); i++) {
            checkEquals(100L - i, list.get(i).seq(), "seq at " + i);
        }
        check(TestJson.list(TestJson.object(events.toJson()).get("events")).size() == 100, "JSON events");
    }

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    private static List<Map<String, Object>> events(DistributedFsServer server) throws IOException {
        NodeTestKit.RawResponse response = raw(server.port(), "GET /cluster/events HTTP/1.1\r\n" + host(server.port()), null);
        checkEquals(200, response.status(), "GET /cluster/events");
        return TestJson.objects(TestJson.object(response.text()), "events");
    }
}
