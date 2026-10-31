package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.sha256;
import static com.distributedfs.NodeTestKit.startNode;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;
import static com.distributedfs.NodeTestKit.waitUntil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plain-Java tests for {@code GET /files} (milestone M4b-2): the committed files, newest first,
 * with their size, chunks, SHA-256, format and health; uploads in progress and unreadable
 * manifests counted but not listed; each health value (healthy, at_risk, over_replicated, missing,
 * unavailable) from real node folders and nodes that are stopped; the per-node copy counts; the
 * 1,000-file limit; the method and query rules; and that a new upload's chunks are seen at once.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.FilesListEndpointTest}.
 */
public class FilesListEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("fileslist-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("with no files the list is empty, and every node is listed with 0 copies", FilesListEndpointTest::empty);
        RUNNER.run("committed files are listed newest first with size, chunks, chunk size, SHA-256, format and health",
                FilesListEndpointTest::fields);
        RUNNER.run("an empty file is healthy and has no copy counts", FilesListEndpointTest::emptyFile);
        RUNNER.run("an upload in progress (a marker without a manifest) is counted, not listed; a leftover marker is ignored",
                FilesListEndpointTest::pending);
        RUNNER.run("an unreadable manifest is counted, the other files are still listed, and the answer is 200",
                FilesListEndpointTest::unreadable);
        RUNNER.run("a chunk with one copy left makes its file at_risk", FilesListEndpointTest::atRisk);
        RUNNER.run("a chunk with a third copy makes its file over_replicated", FilesListEndpointTest::overReplicated);
        RUNNER.run("a chunk with no copy while every node is up makes its file missing", FilesListEndpointTest::missing);
        RUNNER.run("with a node stopped, its copies do not count: at_risk, then unavailable when no copy is reachable",
                FilesListEndpointTest::nodeDown);
        RUNNER.run("each node's copy count is what its folder holds, and a DOWN node keeps its last count",
                FilesListEndpointTest::nodeCounts);
        RUNNER.run("only regular files with server chunk names count as copies", FilesListEndpointTest::onlyChunkFiles);
        RUNNER.run("a format 1 file (no chunk checksums) is listed with format 1", FilesListEndpointTest::formatOne);
        RUNNER.run("at most 1,000 files are listed, the newest, and truncated says so", FilesListEndpointTest::limit);
        RUNNER.run("a new upload's chunks are seen at once, without waiting for the shared listing to age",
                FilesListEndpointTest::seenAtOnce);
        RUNNER.run("GET /files takes no query; other methods are 405 with Allow: GET, POST", FilesListEndpointTest::methods);
        RUNNER.run("GET /files is answered while the storage lock is held, and reads no chunk", FilesListEndpointTest::noLock);
        RUNNER.finish();
    }

    private static void empty(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            Map<String, Object> json = files(server.port());
            checkEquals(List.of("replicationFactor", "listedAt", "truncated", "skipped", "nodes", "files"),
                    new ArrayList<>(json.keySet()), "fields");
            checkEquals(2L, json.get("replicationFactor"), "replicationFactor");
            checkEquals(false, json.get("truncated"), "truncated");
            checkEquals(Map.of("pending", 0L, "unreadable", 0L), json.get("skipped"), "skipped");
            checkEquals(List.of(), json.get("files"), "files");
            List<Map<String, Object>> nodes = TestJson.objects(json, "nodes");
            checkEquals(3, nodes.size(), "nodes");
            for (int i = 0; i < 3; i++) {
                Map<String, Object> node = nodes.get(i);
                checkEquals((long) i, node.get("index"), "index");
                checkEquals("UP", node.get("state"), "state");
                checkEquals(true, node.get("reachable"), "reachable");
                checkEquals(true, node.get("listed"), "listed");
                checkEquals(0L, node.get("chunkCopies"), "chunkCopies");
                Instant.parse((String) node.get("listedAt"));
            }
            Instant.parse((String) json.get("listedAt"));
        } finally {
            server.stop();
        }
    }

    private static void fields(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            byte[] older = randomBytes(3 * CHUNK + 5, 1);
            byte[] newer = randomBytes(CHUNK, 2);
            String olderId = fileId(upload(server.port(), older));
            String newerId = fileId(upload(server.port(), newer));
            setTime(server, olderId, "2026-10-01T10:00:00Z");
            setTime(server, newerId, "2026-10-02T10:00:00Z");
            List<Map<String, Object>> list = TestJson.objects(files(server.port()), "files");
            checkEquals(2, list.size(), "files");
            checkEquals(newerId, list.get(0).get("fileId"), "newest first");
            Map<String, Object> f = list.get(1);
            checkEquals(List.of("fileId", "name", "size", "chunkCount", "chunkSize", "sha256", "format", "committedAt", "health",
                    "minCopies", "maxCopies"), new ArrayList<>(f.keySet()), "fields");
            checkEquals(olderId, f.get("fileId"), "fileId");
            checkEquals((long) older.length, f.get("size"), "size");
            checkEquals(4L, f.get("chunkCount"), "chunkCount");
            checkEquals((long) CHUNK, f.get("chunkSize"), "chunkSize");
            checkEquals(sha256(older), f.get("sha256"), "sha256");
            checkEquals(2L, f.get("format"), "format");
            checkEquals("2026-10-01T10:00:00Z", f.get("committedAt"), "committedAt");
            checkEquals("healthy", f.get("health"), "health");
            checkEquals(2L, f.get("minCopies"), "minCopies");
            checkEquals(2L, f.get("maxCopies"), "maxCopies");
        } finally {
            server.stop();
        }
    }

    private static void emptyFile(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            fileId(upload(server.port(), new byte[0]));
            Map<String, Object> f = TestJson.objects(files(server.port()), "files").get(0);
            checkEquals(0L, f.get("chunkCount"), "chunkCount");
            checkEquals("healthy", f.get("health"), "health");
            checkEquals(null, f.get("minCopies"), "minCopies");
            checkEquals(null, f.get("maxCopies"), "maxCopies");
        } finally {
            server.stop();
        }
    }

    private static void pending(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String committed = fileId(upload(server.port(), randomBytes(CHUNK, 3)));
            Path manifests = server.dataRoot().resolve("manifests");
            // A committed file whose marker was left behind is still a file.
            Files.writeString(manifests.resolve(committed + ".pending"),
                    new ManifestStore.PendingUpload(committed, 1).toText(), StandardCharsets.US_ASCII);
            String inProgress = "00000000000000000000000000000abc";
            Files.writeString(manifests.resolve(inProgress + ".pending"),
                    new ManifestStore.PendingUpload(inProgress, 2).toText(), StandardCharsets.US_ASCII);
            Map<String, Object> json = files(server.port());
            List<Map<String, Object>> list = TestJson.objects(json, "files");
            checkEquals(1, list.size(), "files");
            checkEquals(committed, list.get(0).get("fileId"), "listed");
            checkEquals(Map.of("pending", 1L, "unreadable", 0L), json.get("skipped"), "skipped");
        } finally {
            server.stop();
        }
    }

    private static void unreadable(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String good = fileId(upload(server.port(), randomBytes(CHUNK, 4)));
            Files.writeString(server.dataRoot().resolve("manifests").resolve("0000000000000000000000000000dead.manifest"),
                    "format=2\nnot a manifest\n", StandardCharsets.US_ASCII);
            NodeTestKit.RawResponse response = raw(server.port(), "GET /files HTTP/1.1\r\n" + host(server.port()), null);
            checkEquals(200, response.status(), "status");
            Map<String, Object> json = TestJson.object(response.text());
            checkEquals(Map.of("pending", 0L, "unreadable", 1L), json.get("skipped"), "skipped");
            checkEquals(good, TestJson.objects(json, "files").get(0).get("fileId"), "the readable file");
            check(!response.text().contains("manifests") && !response.text().contains("\\\\"), "a path in the answer");
        } finally {
            server.stop();
        }
    }

    private static void atRisk(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(3 * CHUNK, 5)));
            Files.delete(holders(server, FileIds.chunkId(id, 1)).get(0));
            server.placement().invalidate();
            Map<String, Object> f = TestJson.objects(files(server.port()), "files").get(0);
            checkEquals("at_risk", f.get("health"), "health");
            checkEquals(1L, f.get("minCopies"), "minCopies");
            checkEquals(2L, f.get("maxCopies"), "maxCopies");
        } finally {
            server.stop();
        }
    }

    private static void overReplicated(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 6)));
            String chunk = FileIds.chunkId(id, 0);
            List<Path> copies = holders(server, chunk);
            for (int i = 0; i < 3; i++) {
                Path target = nodeDir(server, i).resolve(chunk);
                if (!copies.contains(target)) {
                    Files.copy(copies.get(0), target);
                }
            }
            server.placement().invalidate();
            Map<String, Object> f = TestJson.objects(files(server.port()), "files").get(0);
            checkEquals("over_replicated", f.get("health"), "health");
            checkEquals(2L, f.get("minCopies"), "minCopies");
            checkEquals(3L, f.get("maxCopies"), "maxCopies");
        } finally {
            server.stop();
        }
    }

    private static void missing(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 7)));
            for (Path copy : holders(server, FileIds.chunkId(id, 1))) {
                Files.delete(copy);
            }
            server.placement().invalidate();
            Map<String, Object> f = TestJson.objects(files(server.port()), "files").get(0);
            checkEquals("missing", f.get("health"), "health");
            checkEquals(0L, f.get("minCopies"), "minCopies");
        } finally {
            server.stop();
        }
    }

    private static void nodeDown(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle a = startNode(tempDir.resolve("node-a"));
        NodeTestKit.NodeHandle b = startNode(tempDir.resolve("node-b"));
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(a.port(), b.port()), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 50);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 8)));
            b.close();
            waitUntil(10_000, "node 1 DOWN", () -> server.monitor().snapshot().get(1).state() == HeartbeatMonitor.State.DOWN);
            waitUntil(5_000, "at_risk", () -> "at_risk".equals(health(server.port())));
            // Its last copy on the node that is still up goes too: no copy is reachable, but the
            // node that is down may still hold one.
            Files.delete(a.chunksDir().resolve(FileIds.chunkId(id, 0)));
            server.placement().invalidate();
            checkEquals("unavailable", health(server.port()), "health");
        } finally {
            server.stop();
            a.close();
        }
    }

    private static void nodeCounts(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle[] nodes = {startNode(tempDir.resolve("n0")), startNode(tempDir.resolve("n1")),
                startNode(tempDir.resolve("n2"))};
        DistributedFsServer server = startServer(tempDir.resolve("data"),
                List.of(nodes[0].port(), nodes[1].port(), nodes[2].port()), new NodeTestKit.Lines(), new NodeTestKit.Lines(),
                CHUNK, 50);
        try {
            fileId(upload(server.port(), randomBytes(5 * CHUNK, 9)));
            List<Map<String, Object>> listed = TestJson.objects(files(server.port()), "nodes");
            long total = 0;
            for (int i = 0; i < 3; i++) {
                checkEquals((long) NodeTestKit.listNames(nodes[i].chunksDir()).size(), listed.get(i).get("chunkCopies"),
                        "node " + i);
                total += (Long) listed.get(i).get("chunkCopies");
            }
            checkEquals(10L, total, "copies in all");
            long before = (Long) listed.get(1).get("chunkCopies");
            String listedAt = (String) listed.get(1).get("listedAt");
            nodes[1].close();
            waitUntil(10_000, "node 1 DOWN", () -> server.monitor().snapshot().get(1).state() == HeartbeatMonitor.State.DOWN);
            server.placement().invalidate();
            Map<String, Object> down = TestJson.objects(files(server.port()), "nodes").get(1);
            checkEquals("DOWN", down.get("state"), "state");
            checkEquals(false, down.get("reachable"), "reachable");
            checkEquals(true, down.get("listed"), "listed");
            checkEquals(before, down.get("chunkCopies"), "the last count");
            checkEquals(listedAt, down.get("listedAt"), "the time of the last listing");
        } finally {
            server.stop();
            for (NodeTestKit.NodeHandle node : nodes) {
                node.close();
            }
        }
    }

    private static void onlyChunkFiles(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(CHUNK, 10)));
            String chunk = FileIds.chunkId(id, 0);
            for (int i = 0; i < 3; i++) {
                Path dir = nodeDir(server, i);
                if (!Files.exists(dir.resolve(chunk))) {
                    Files.createDirectory(dir.resolve(chunk)); // a folder under the chunk's name is not a copy
                }
                Files.writeString(dir.resolve(chunk + "-123.tmp"), "temp"); // nor is a temp file
                Files.writeString(dir.resolve("notes.txt"), "x"); // nor anything else
            }
            server.placement().invalidate();
            Map<String, Object> json = files(server.port());
            checkEquals("healthy", TestJson.objects(json, "files").get(0).get("health"), "health");
            long total = 0;
            for (Map<String, Object> node : TestJson.objects(json, "nodes")) {
                total += (Long) node.get("chunkCopies");
            }
            checkEquals(2L, total, "copies counted");
        } finally {
            server.stop();
        }
    }

    private static void formatOne(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = "000000000000000000000000000000f1";
            Files.writeString(server.dataRoot().resolve("manifests").resolve(id + ".manifest"),
                    new FileManifest(id, 0, sha256(new byte[0]), CHUNK, 0).toText(), StandardCharsets.US_ASCII);
            Map<String, Object> f = TestJson.objects(files(server.port()), "files").get(0);
            checkEquals(id, f.get("fileId"), "fileId");
            checkEquals(1L, f.get("format"), "format");
        } finally {
            server.stop();
        }
    }

    private static void limit(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            Path manifests = server.dataRoot().resolve("manifests");
            String empty = sha256(new byte[0]);
            Instant base = Instant.parse("2026-01-01T00:00:00Z");
            for (int i = 0; i <= ConsoleApi.MAX_FILES; i++) {
                String id = String.format("%032x", i + 1);
                Path file = manifests.resolve(id + ".manifest");
                Files.writeString(file, new FileManifest(id, 0, empty, CHUNK, 0).toText(), StandardCharsets.US_ASCII);
                Files.setLastModifiedTime(file, FileTime.from(base.plusSeconds(i)));
            }
            Map<String, Object> json = files(server.port());
            checkEquals(true, json.get("truncated"), "truncated");
            List<Map<String, Object>> list = TestJson.objects(json, "files");
            checkEquals(ConsoleApi.MAX_FILES, list.size(), "listed");
            checkEquals(String.format("%032x", ConsoleApi.MAX_FILES + 1), list.get(0).get("fileId"), "the newest first");
            checkEquals(String.format("%032x", 2), list.get(list.size() - 1).get("fileId"), "the oldest left out");
        } finally {
            server.stop();
        }
    }

    private static void seenAtOnce(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            files(server.port()); // the shared listing is now fresh, and holds no chunk
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 11)));
            Map<String, Object> f = TestJson.objects(files(server.port()), "files").get(0);
            checkEquals(id, f.get("fileId"), "fileId");
            checkEquals("healthy", f.get("health"), "health right after the upload");
        } finally {
            server.stop();
        }
    }

    private static void methods(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String target : List.of("/files?x=1", "/files?", "/files?health=true")) {
                NodeTestKit.RawResponse response = raw(port, "GET " + target + " HTTP/1.1\r\n" + host(port), null);
                checkEquals(400, response.status(), target);
                checkEquals(ApiError.INVALID_QUERY.toJson(), response.text(), target + " body");
            }
            for (String method : List.of("PUT", "DELETE", "PATCH", "HEAD")) {
                NodeTestKit.RawResponse response = raw(port, method + " /files HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0",
                        null);
                checkEquals(405, response.status(), method);
                checkEquals("GET, POST", response.header("Allow"), method + " Allow");
            }
            NodeTestKit.RawResponse foreign = raw(port, "GET /files HTTP/1.1\r\n" + host(port) + "\r\nOrigin: http://evil.example",
                    null);
            checkEquals(200, foreign.status(), "a GET with a foreign Origin (it changes nothing)");
            checkEquals(null, foreign.header("Access-Control-Allow-Origin"), "no CORS header");
            checkEquals("application/json; charset=utf-8", foreign.header("Content-Type"), "Content-Type");
        } finally {
            server.stop();
        }
    }

    private static void noLock(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle a = startNode(tempDir.resolve("node-a"));
        NodeTestKit.NodeHandle b = startNode(tempDir.resolve("node-b"));
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(a.port(), b.port()), new NodeTestKit.Lines(),
                new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            fileId(upload(server.port(), randomBytes(3 * CHUNK, 12)));
            long readsBefore = chunkReads(a) + chunkReads(b);
            java.util.concurrent.CountDownLatch held = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
            Thread holder = new Thread(() -> {
                try {
                    server.files().withStorageLock(() -> {
                        held.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            holder.start();
            check(held.await(5, java.util.concurrent.TimeUnit.SECONDS), "the lock was not taken");
            try {
                server.placement().invalidate();
                long start = System.nanoTime();
                checkEquals("healthy", health(server.port()), "health");
                check(System.nanoTime() - start < 3_000_000_000L, "GET /files waited for the storage lock");
            } finally {
                release.countDown();
                holder.join(5_000);
            }
            checkEquals(readsBefore, chunkReads(a) + chunkReads(b), "chunk reads on the nodes");
        } finally {
            server.stop();
            a.close();
            b.close();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    private static Map<String, Object> files(int port) throws IOException {
        NodeTestKit.RawResponse response = raw(port, "GET /files HTTP/1.1\r\n" + host(port), null);
        checkEquals(200, response.status(), "GET /files (" + response.text() + ")");
        return TestJson.object(response.text());
    }

    private static String health(int port) {
        try {
            return (String) TestJson.objects(files(port), "files").get(0).get("health");
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Path nodeDir(DistributedFsServer server, int index) {
        return server.dataRoot().resolve("nodes").resolve("node-" + index);
    }

    private static List<Path> holders(DistributedFsServer server, String chunkId) {
        List<Path> copies = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path copy = nodeDir(server, i).resolve(chunkId);
            if (Files.isRegularFile(copy)) {
                copies.add(copy);
            }
        }
        return copies;
    }

    private static void setTime(DistributedFsServer server, String id, String time) throws IOException {
        Files.setLastModifiedTime(server.dataRoot().resolve("manifests").resolve(id + ".manifest"),
                FileTime.from(Instant.parse(time)));
    }

    /** How many chunk reads ({@code GET /chunks/<id>}) a node has logged. */
    private static long chunkReads(NodeTestKit.NodeHandle node) {
        return node.out().text().lines().filter(line -> line.startsWith("GET /chunks/") && !line.contains("/exists")).count();
    }
}
