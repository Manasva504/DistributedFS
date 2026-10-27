package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.jsonNumber;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.repair;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Plain-Java tests for {@code POST /cluster/repair} (milestone M3): the method and query checks,
 * a dry run, a repair on local node folders, one repair at a time (409), an upload during a
 * repair, and, with remote nodes, a node stopped, repaired around, restarted and trimmed.
 * Run: {@code java -cp "out;out-test" com.distributedfs.RepairEndpointTest}.
 */
public class RepairEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("repairendpoint-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("only POST is allowed: other methods get 405 with Allow: POST", RepairEndpointTest::postOnly);
        RUNNER.run("the query is nothing, dryRun=true or dryRun=false; anything else is 400 invalid_query",
                RepairEndpointTest::queryChecked);
        RUNNER.run("a request for another Host is 403, and nothing is repaired", RepairEndpointTest::hostChecked);
        RUNNER.run("on local node folders, a damaged and a missing copy are replaced", RepairEndpointTest::localRepair);
        RUNNER.run("a dry run answers what a repair would do and changes nothing", RepairEndpointTest::dryRun);
        RUNNER.run("a second repair while one runs is 409 repair_in_progress", RepairEndpointTest::conflict);
        RUNNER.run("an upload during a repair waits for the chunk being repaired and is stored with 2 copies",
                RepairEndpointTest::uploadDuringRepair);
        RUNNER.run("the answer contains no file system path or exception text", RepairEndpointTest::noPaths);
        RUNNER.run("remote: a node stops, the repair copies its chunks to the others, it restarts, and the extras are trimmed",
                RepairEndpointTest::remoteKillRepairRestartTrim);
        RUNNER.finish();
    }

    private static void postOnly(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            for (String method : List.of("GET", "PUT", "DELETE", "HEAD")) {
                NodeTestKit.RawResponse response = raw(server.port(), method + " /cluster/repair HTTP/1.1\r\n"
                        + NodeTestKit.host(server.port()) + "\r\nContent-Length: 0", null);
                checkEquals(405, response.status(), method + " status");
                checkEquals("POST", response.header("Allow"), method + " Allow");
            }
        } finally {
            server.stop();
        }
    }

    private static void queryChecked(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            for (String query : List.of("", "dryRun", "dryRun=yes", "dryrun=true", "dryRun=TRUE", "dryRun=true&x=1",
                    "x=1", "dryRun=true&dryRun=false", "dryRun=%74rue")) {
                NodeTestKit.RawResponse response = repair(server.port(), query);
                checkEquals(400, response.status(), "\"" + query + "\" status");
                checkEquals(ApiError.INVALID_QUERY.toJson(), response.text(), "\"" + query + "\" body");
            }
            for (String query : new String[] {null, "dryRun=true", "dryRun=false"}) {
                NodeTestKit.RawResponse response = repair(server.port(), query);
                checkEquals(200, response.status(), query + " status");
                checkEquals("application/json; charset=utf-8", response.header("Content-Type"), query + " type");
                checkContains(response.text(), "\"dryRun\":" + "dryRun=true".equals(query));
            }
        } finally {
            server.stop();
        }
    }

    private static void hostChecked(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(CHUNK, 1)));
            Path copy = holderFolders(tempDir, FileIds.chunkId(id, 0)).get(0);
            Files.delete(copy);
            NodeTestKit.RawResponse response = raw(server.port(),
                    "POST /cluster/repair HTTP/1.1\r\nHost: example.com\r\nContent-Length: 0", null);
            checkEquals(403, response.status(), "status");
            check(!Files.exists(copy), "the copy was put back by a refused request");
        } finally {
            server.stop();
        }
    }

    private static void localRepair(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            byte[] data = randomBytes(3 * CHUNK, 2);
            String id = fileId(upload(server.port(), data));
            Files.delete(holderFolders(tempDir, FileIds.chunkId(id, 0)).get(0));
            Path damaged = holderFolders(tempDir, FileIds.chunkId(id, 1)).get(1);
            byte[] bytes = Files.readAllBytes(damaged);
            bytes[7] ^= 1;
            Files.write(damaged, bytes);
            NodeTestKit.RawResponse response = repair(server.port(), null);
            checkEquals(200, response.status(), "status");
            String json = response.text();
            checkEquals(1L, jsonNumber(json, "filesChecked"), "filesChecked");
            checkEquals(3L, jsonNumber(json, "chunksChecked"), "chunksChecked");
            checkEquals(1L, jsonNumber(json, "chunksHealthy"), "chunksHealthy");
            checkEquals(1L, jsonNumber(json, "copiesAdded"), "copiesAdded");
            checkEquals(1L, jsonNumber(json, "copiesReplaced"), "copiesReplaced");
            checkEquals(0L, jsonNumber(json, "chunksLost"), "chunksLost");
            for (int i = 0; i < 3; i++) {
                checkEquals(2, holderFolders(tempDir, FileIds.chunkId(id, i)).size(), "copies of chunk " + i);
            }
            check(Arrays.equals(data, download(server.port(), id).body()), "the file after the repair");
            String again = repair(server.port(), null).text();
            checkEquals(3L, jsonNumber(again, "chunksHealthy"), "healthy on a second repair");
        } finally {
            server.stop();
        }
    }

    private static void dryRun(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 3)));
            Files.delete(holderFolders(tempDir, FileIds.chunkId(id, 1)).get(0));
            Map<String, String> before = tree(tempDir);
            String json = repair(server.port(), "dryRun=true").text();
            checkContains(json, "\"dryRun\":true");
            checkEquals(1L, jsonNumber(json, "copiesAdded"), "copiesAdded");
            checkEquals(before, tree(tempDir), "the node folders");
            checkEquals(List.of(), listNames(tempDir.resolve("data").resolve("tmp")), "tmp after the dry run");
        } finally {
            server.stop();
        }
    }

    private static void conflict(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            fileId(upload(server.port(), randomBytes(CHUNK, 4)));
            AtomicReference<NodeTestKit.RawResponse> first = new AtomicReference<>();
            try (HeldLock lock = HeldLock.hold(server.files())) {
                Thread repairing = new Thread(() -> first.set(uncheckedRepair(server.port())));
                repairing.start();
                waitForLockWaiter(repairing);
                NodeTestKit.RawResponse second = repair(server.port(), "dryRun=true");
                checkEquals(409, second.status(), "status of the second repair");
                checkEquals(ApiError.REPAIR_IN_PROGRESS.toJson(), second.text(), "body");
                lock.release();
                repairing.join(10_000);
            }
            checkEquals(200, first.get().status(), "the first repair");
            checkEquals(200, repair(server.port(), null).status(), "a repair after the first finished");
        } finally {
            server.stop();
        }
    }

    private static void uploadDuringRepair(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            fileId(upload(server.port(), randomBytes(CHUNK, 5)));
            byte[] data = randomBytes(2 * CHUNK, 6);
            AtomicReference<NodeTestKit.RawResponse> repaired = new AtomicReference<>();
            AtomicReference<NodeTestKit.RawResponse> uploaded = new AtomicReference<>();
            Thread repairing;
            Thread uploading;
            try (HeldLock lock = HeldLock.hold(server.files())) {
                repairing = new Thread(() -> repaired.set(uncheckedRepair(server.port())));
                repairing.start();
                waitForLockWaiter(repairing);
                uploading = new Thread(() -> {
                    try {
                        uploaded.set(upload(server.port(), data));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
                uploading.start();
                lock.release();
                repairing.join(10_000);
                uploading.join(10_000);
            }
            checkEquals(200, repaired.get().status(), "repair");
            String id = fileId(uploaded.get());
            for (int i = 0; i < 2; i++) {
                checkEquals(2, holderFolders(tempDir, FileIds.chunkId(id, i)).size(), "copies of chunk " + i);
            }
            check(Arrays.equals(data, download(server.port(), id).body()), "the uploaded file");
            checkEquals(0L, jsonNumber(repair(server.port(), "dryRun=true").text(), "copiesAdded"), "nothing left to do");
        } finally {
            server.stop();
        }
    }

    private static void noPaths(Path tempDir) throws Exception {
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), err,
                CHUNK, 60_000);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 7)));
            String chunk = FileIds.chunkId(id, 0);
            for (Path copy : holderFolders(tempDir, chunk)) {
                Files.write(copy, new byte[] {1, 2, 3});
            }
            String json = repair(server.port(), null).text();
            checkEquals(1L, jsonNumber(json, "chunksLost"), "chunksLost");
            checkEquals("\"" + chunk + "\"", NodeTestKit.jsonList(json, "lost"), "lost");
            check(!json.contains(":\\\\") && !json.contains(":/") && !json.contains("Exception")
                    && !json.contains(tempDir.getFileName().toString()), "a path or exception in " + json);
            checkContains(err.text(), chunk);
        } finally {
            server.stop();
        }
    }

    private static void remoteKillRepairRestartTrim(Path tempDir) throws Exception {
        try (RemoteServerTest.Cluster cluster = RemoteServerTest.Cluster.start(tempDir, 3)) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), cluster.ports(), new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK, 100);
            try {
                byte[] data = randomBytes(6 * CHUNK - 10, 8);
                String id = fileId(upload(server.port(), data));
                // 6 chunks, 2 copies each, round-robin: node 1 holds chunks 0, 1, 3 and 4.
                checkEquals(4, listNames(cluster.nodes.get(1).chunksDir()).size(), "copies on node 1");
                int nodePort = cluster.nodes.get(1).port();
                cluster.nodes.get(1).close();
                NodeTestKit.waitUntil(10_000, "node 1 DOWN",
                        () -> ClusterStatusEndpointTest.nodeState(server.port(), 1).equals("DOWN"));

                String dry = repair(server.port(), "dryRun=true").text();
                checkEquals(4L, jsonNumber(dry, "copiesAdded"), "dry run: copies added");
                checkEquals(4, listNames(cluster.nodes.get(0).chunksDir()).size()
                        + listNames(cluster.nodes.get(2).chunksDir()).size() - 4, "dry run changed nothing");
                checkContains(dry, "{\"index\":1,\"state\":\"DOWN\"}");

                String real = repair(server.port(), null).text();
                checkEquals(4L, jsonNumber(real, "copiesAdded"), "copies added");
                checkEquals(6, listNames(cluster.nodes.get(0).chunksDir()).size(), "copies on node 0");
                checkEquals(6, listNames(cluster.nodes.get(2).chunksDir()).size(), "copies on node 2");
                check(Arrays.equals(data, download(server.port(), id).body()), "the file with node 1 down");

                cluster.nodes.set(1, NodeTestKit.startNodeOnPort(tempDir.resolve("node-1"), nodePort));
                NodeTestKit.waitUntil(10_000, "node 1 UP",
                        () -> ClusterStatusEndpointTest.nodeState(server.port(), 1).equals("UP"));
                String dryTrim = repair(server.port(), "dryRun=true").text();
                checkEquals(4L, jsonNumber(dryTrim, "copiesRemoved"), "dry run: copies removed");
                checkEquals(16, cluster.chunkCounts().stream().mapToInt(Integer::intValue).sum(), "copies after the dry run");
                String trim = repair(server.port(), null).text();
                checkEquals(4L, jsonNumber(trim, "copiesRemoved"), "copies removed");
                checkEquals(12, cluster.chunkCounts().stream().mapToInt(Integer::intValue).sum(), "copies after trimming");
                for (int i = 0; i < 6; i++) {
                    String chunk = FileIds.chunkId(id, i);
                    int copies = 0;
                    for (NodeTestKit.NodeHandle node : cluster.nodes) {
                        copies += Files.exists(node.chunksDir().resolve(chunk)) ? 1 : 0;
                    }
                    checkEquals(2, copies, "copies of chunk " + i);
                }
                check(Arrays.equals(data, download(server.port(), id).body()), "the file after trimming");
                checkEquals(6L, jsonNumber(repair(server.port(), null).text(), "chunksHealthy"), "a third repair");
            } finally {
                server.stop();
            }
        }
    }

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(),
                CHUNK, 60_000);
    }

    /** The local node folders' copies of a chunk, in node order. */
    private static List<Path> holderFolders(Path tempDir, String chunkId) {
        List<Path> copies = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path copy = tempDir.resolve("data").resolve("nodes").resolve("node-" + i).resolve(chunkId);
            if (Files.exists(copy)) {
                copies.add(copy);
            }
        }
        return copies;
    }

    private static Map<String, String> tree(Path tempDir) throws IOException {
        Map<String, String> tree = new TreeMap<>();
        for (int i = 0; i < 3; i++) {
            Path dir = tempDir.resolve("data").resolve("nodes").resolve("node-" + i);
            for (String name : listNames(dir)) {
                tree.put(i + "/" + name, NodeTestKit.sha256(Files.readAllBytes(dir.resolve(name))));
            }
        }
        return tree;
    }

    private static NodeTestKit.RawResponse uncheckedRepair(int port) {
        try {
            return repair(port, null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Waits until the request thread of a repair waits for the storage lock the test holds. */
    private static void waitForLockWaiter(Thread client) {
        NodeTestKit.waitUntil(10_000, "a repair to wait for the storage lock", () -> Thread.getAllStackTraces().keySet()
                .stream().anyMatch(t -> t.getState() == Thread.State.BLOCKED && Arrays.stream(t.getStackTrace())
                        .anyMatch(f -> f.getClassName().startsWith(RepairService.class.getName()))));
        check(client.isAlive(), "the client gave up");
    }

    /** The storage lock, held by a thread of the test until released. */
    private static final class HeldLock implements AutoCloseable {
        private final CountDownLatch release = new CountDownLatch(1);
        private final Thread holder;

        private HeldLock(FileService files) throws InterruptedException {
            CountDownLatch held = new CountDownLatch(1);
            holder = new Thread(() -> {
                try {
                    files.withStorageLock(() -> {
                        held.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    });
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
            holder.start();
            held.await();
        }

        static HeldLock hold(FileService files) throws InterruptedException {
            return new HeldLock(files);
        }

        void release() {
            release.countDown();
        }

        @Override
        public void close() {
            release.countDown();
            try {
                holder.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
