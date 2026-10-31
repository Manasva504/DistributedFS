package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.repair;
import static com.distributedfs.NodeTestKit.startNode;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;
import static com.distributedfs.NodeTestKit.waitUntil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Plain-Java tests for {@code GET /files/<fileId>/placement} and the shared listing behind it,
 * {@link PlacementSnapshot} (milestone M4b-2): which nodes hold each chunk's copies, whether each
 * copy is reachable, a DOWN node's last-known copies, a node never listed, the copies a repair
 * would trim (compared with what the repair then really trims), the error answers, and the cost:
 * one listing per node per period however many requests ask, no storage lock, no chunk read, and a
 * fresh listing right after an upload or a repair.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.PlacementEndpointTest}.
 */
public class PlacementEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("placement-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("each chunk lists the nodes whose folders hold a copy, all reachable", PlacementEndpointTest::holders);
        RUNNER.run("an unknown file and an upload in progress are 404; a damaged manifest is 500 metadata_corrupt",
                PlacementEndpointTest::notFoundAndCorrupt);
        RUNNER.run("the raw file ID is checked exactly: anything but 32 lowercase hex characters is 400",
                PlacementEndpointTest::invalidIds);
        RUNNER.run("only GET, without a query", PlacementEndpointTest::methodAndQuery);
        RUNNER.run("a stopped node's copies are listed as unreachable, from its last listing", PlacementEndpointTest::downNode);
        RUNNER.run("a node never listed since a degraded start has no known copies", PlacementEndpointTest::neverListed);
        RUNNER.run("the copies marked trimCandidate are exactly the ones the next repair deletes",
                PlacementEndpointTest::trimMatchesRepair);
        RUNNER.run("no copy is a trim candidate while a node is DOWN", PlacementEndpointTest::noTrimWhileDown);
        RUNNER.run("the placement is answered while the storage lock is held, and reads no chunk",
                PlacementEndpointTest::noLockNoRead);
        RUNNER.run("concurrent requests share one listing per node; invalidate makes the next request list again",
                PlacementEndpointTest::sharedListing);
        RUNNER.run("a fresh listing is reused for its period, and a zero period lists every time", PlacementEndpointTest::period);
        RUNNER.run("a DOWN node is not listed; a node whose listing fails keeps its last listing, unreachable",
                PlacementEndpointTest::downAndFailingNodes);
        RUNNER.run("right after an upload, its chunks are listed", PlacementEndpointTest::freshAfterUpload);
        RUNNER.run("right after a repair, the new copies are listed; a dry run changes nothing", PlacementEndpointTest::freshAfterRepair);
        RUNNER.finish();
    }

    private static void holders(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            byte[] data = randomBytes(3 * CHUNK + 1, 1);
            String id = fileId(upload(server.port(), data));
            Map<String, Object> json = placement(server.port(), id);
            checkEquals(List.of("fileId", "name", "size", "chunkCount", "chunkSize", "replicationFactor", "listedAt", "nodes", "chunks"),
                    new ArrayList<>(json.keySet()), "fields");
            checkEquals(id, json.get("fileId"), "fileId");
            checkEquals((long) data.length, json.get("size"), "size");
            checkEquals(4L, json.get("chunkCount"), "chunkCount");
            checkEquals((long) CHUNK, json.get("chunkSize"), "chunkSize");
            checkEquals(2L, json.get("replicationFactor"), "replicationFactor");
            List<Map<String, Object>> chunks = TestJson.objects(json, "chunks");
            checkEquals(4, chunks.size(), "chunks");
            for (int i = 0; i < 4; i++) {
                Map<String, Object> chunk = chunks.get(i);
                checkEquals((long) i, chunk.get("index"), "index");
                checkEquals(2L, chunk.get("reachableCopies"), "reachableCopies of " + i);
                checkEquals(2L, chunk.get("knownCopies"), "knownCopies of " + i);
                List<Long> nodes = new ArrayList<>();
                for (Map<String, Object> holder : TestJson.objects(chunk, "holders")) {
                    nodes.add((Long) holder.get("node"));
                    checkEquals(true, holder.get("reachable"), "reachable");
                    checkEquals(false, holder.get("trimCandidate"), "trimCandidate");
                }
                checkEquals(onDisk(server, FileIds.chunkId(id, i)), nodes, "holders of chunk " + i);
            }
            for (Map<String, Object> node : TestJson.objects(json, "nodes")) {
                checkEquals(true, node.get("reachable"), "reachable");
                checkEquals(true, node.get("listed"), "listed");
                check(!node.containsKey("chunkCopies"), "chunkCopies in the placement");
            }
        } finally {
            server.stop();
        }
    }

    private static void notFoundAndCorrupt(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            checkError(get(port, "/files/" + FileIds.newFileId() + "/placement"), ApiError.FILE_NOT_FOUND, "unknown");
            Path manifests = server.dataRoot().resolve("manifests");
            String pending = "00000000000000000000000000000abc";
            Files.writeString(manifests.resolve(pending + ".pending"), new ManifestStore.PendingUpload(pending, 1).toText(),
                    StandardCharsets.US_ASCII);
            checkError(get(port, "/files/" + pending + "/placement"), ApiError.FILE_NOT_FOUND, "pending");
            String bad = "0000000000000000000000000000dead";
            Files.writeString(manifests.resolve(bad + ".manifest"), "format=2\nnonsense\n", StandardCharsets.US_ASCII);
            checkError(get(port, "/files/" + bad + "/placement"), ApiError.METADATA_CORRUPT, "damaged manifest");
        } finally {
            server.stop();
        }
    }

    private static void invalidIds(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(CHUNK, 2)));
            // The real ID with its first character percent-encoded: it decodes to a stored file,
            // so it must be refused before any decoding.
            String encoded = "%" + Integer.toHexString(id.charAt(0)) + id.substring(1);
            for (String path : List.of("/files//placement", "/files/" + encoded + "/placement", "/files/" + id.toUpperCase() + "/placement",
                    "/files/%3" + id.substring(1) + "/placement", "/files/" + id + "x/placement",
                    "/files/" + id.substring(1) + "/placement", "/files/../" + id + "/placement", "/files/" + id + "%2Fplacement",
                    "/files/" + id + "/placement/", "/files/" + id + "/Placement", "/files/" + id + "/placement/x")) {
                checkError(get(server.port(), path), ApiError.INVALID_FILE_ID, path);
            }
        } finally {
            server.stop();
        }
    }

    private static void methodAndQuery(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String id = fileId(upload(port, randomBytes(CHUNK, 3)));
            for (String method : List.of("POST", "PUT", "DELETE", "HEAD")) {
                NodeTestKit.RawResponse response = raw(port, method + " /files/" + id + "/placement HTTP/1.1\r\n" + host(port)
                        + "\r\nContent-Length: 0", null);
                checkEquals(405, response.status(), method);
                checkEquals("GET", response.header("Allow"), method + " Allow");
            }
            checkError(get(port, "/files/" + id + "/placement?x=1"), ApiError.INVALID_QUERY, "query");
            checkEquals(200, get(port, "/files/" + id + "/placement").status(), "GET");
        } finally {
            server.stop();
        }
    }

    private static void downNode(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle[] nodes = threeNodes(tempDir);
        DistributedFsServer server = remoteServer(tempDir, nodes, 50);
        try {
            String id = fileId(upload(server.port(), randomBytes(5 * CHUNK, 4)));
            Map<String, Object> before = placement(server.port(), id);
            nodes[1].close();
            waitUntil(10_000, "node 1 DOWN", () -> server.monitor().snapshot().get(1).state() == HeartbeatMonitor.State.DOWN);
            server.placement().invalidate();
            Map<String, Object> after = placement(server.port(), id);
            Map<String, Object> node1 = TestJson.objects(after, "nodes").get(1);
            checkEquals("DOWN", node1.get("state"), "state");
            checkEquals(false, node1.get("reachable"), "reachable");
            checkEquals(true, node1.get("listed"), "listed");
            List<Map<String, Object>> chunksBefore = TestJson.objects(before, "chunks");
            List<Map<String, Object>> chunksAfter = TestJson.objects(after, "chunks");
            for (int i = 0; i < 5; i++) {
                List<Map<String, Object>> holders = TestJson.objects(chunksAfter.get(i), "holders");
                checkEquals(TestJson.objects(chunksBefore.get(i), "holders").size(), holders.size(), "known holders of " + i);
                long reachable = 0;
                for (Map<String, Object> holder : holders) {
                    boolean onDownNode = holder.get("node").equals(1L);
                    checkEquals(!onDownNode, holder.get("reachable"), "chunk " + i + " on node " + holder.get("node"));
                    reachable += onDownNode ? 0 : 1;
                }
                checkEquals(reachable, chunksAfter.get(i).get("reachableCopies"), "reachableCopies of " + i);
                checkEquals((long) holders.size(), chunksAfter.get(i).get("knownCopies"), "knownCopies of " + i);
            }
        } finally {
            server.stop();
            closeAll(nodes);
        }
    }

    private static void neverListed(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle[] nodes = threeNodes(tempDir);
        String id;
        DistributedFsServer first = remoteServer(tempDir, nodes, 60_000);
        try {
            id = fileId(upload(first.port(), randomBytes(3 * CHUNK, 5)));
        } finally {
            first.stop();
        }
        int port2 = nodes[2].port();
        nodes[2].close();
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(nodes[0].port(), nodes[1].port(), port2),
                new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
        try {
            Map<String, Object> json = placement(server.port(), id);
            Map<String, Object> node2 = TestJson.objects(json, "nodes").get(2);
            checkEquals("DOWN", node2.get("state"), "state");
            checkEquals(false, node2.get("listed"), "listed");
            checkEquals(null, node2.get("listedAt"), "listedAt");
            for (Map<String, Object> chunk : TestJson.objects(json, "chunks")) {
                for (Map<String, Object> holder : TestJson.objects(chunk, "holders")) {
                    check(!holder.get("node").equals(2L), "a copy on the never-listed node is reported");
                }
            }
        } finally {
            server.stop();
            closeAll(nodes);
        }
    }

    private static void trimMatchesRepair(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(4 * CHUNK, 6)));
            // Copy chunks 0, 1 and 3 to the node that lacks them: 3 copies each.
            for (int i : new int[] {0, 1, 3}) {
                String chunk = FileIds.chunkId(id, i);
                List<Long> holders = onDisk(server, chunk);
                for (int n = 0; n < 3; n++) {
                    if (!holders.contains((long) n)) {
                        Files.copy(nodeDir(server, holders.get(0).intValue()).resolve(chunk), nodeDir(server, n).resolve(chunk));
                    }
                }
            }
            server.placement().invalidate();
            List<Map<String, Object>> chunks = TestJson.objects(placement(server.port(), id), "chunks");
            List<String> predicted = new ArrayList<>();
            for (Map<String, Object> chunk : chunks) {
                for (Map<String, Object> holder : TestJson.objects(chunk, "holders")) {
                    if (Boolean.TRUE.equals(holder.get("trimCandidate"))) {
                        predicted.add(chunk.get("index") + "@" + holder.get("node"));
                    }
                }
            }
            checkEquals(3, predicted.size(), "trim candidates " + predicted);
            List<String> before = allCopies(server, id, 4);
            checkEquals(200, repair(server.port(), null).status(), "repair");
            List<String> removed = new ArrayList<>(before);
            removed.removeAll(allCopies(server, id, 4));
            checkEquals(predicted, removed, "copies the repair deleted");
        } finally {
            server.stop();
        }
    }

    private static void noTrimWhileDown(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle[] nodes = threeNodes(tempDir);
        DistributedFsServer server = remoteServer(tempDir, nodes, 50);
        try {
            String id = fileId(upload(server.port(), randomBytes(CHUNK, 7)));
            String chunk = FileIds.chunkId(id, 0);
            // A third copy on the node that lacks one; then, with node 0 stopped, the repair would
            // keep every copy (it trims only with every node UP), so the page must mark none.
            for (NodeTestKit.NodeHandle node : nodes) {
                Path copy = node.chunksDir().resolve(chunk);
                if (!Files.exists(copy)) {
                    for (NodeTestKit.NodeHandle other : nodes) {
                        if (Files.exists(other.chunksDir().resolve(chunk))) {
                            Files.copy(other.chunksDir().resolve(chunk), copy);
                            break;
                        }
                    }
                }
            }
            server.placement().invalidate();
            checkEquals(1, trimCandidates(placement(server.port(), id)), "trim candidates with every node UP");
            nodes[0].close();
            waitUntil(10_000, "node 0 DOWN", () -> server.monitor().snapshot().get(0).state() == HeartbeatMonitor.State.DOWN);
            server.placement().invalidate();
            checkEquals(0, trimCandidates(placement(server.port(), id)), "trim candidates with node 0 DOWN");
        } finally {
            server.stop();
            closeAll(nodes);
        }
    }

    private static void noLockNoRead(Path tempDir) throws Exception {
        NodeTestKit.NodeHandle[] nodes = threeNodes(tempDir);
        DistributedFsServer server = remoteServer(tempDir, nodes, 60_000);
        try {
            String id = fileId(upload(server.port(), randomBytes(3 * CHUNK, 8)));
            long reads = chunkReads(nodes);
            CountDownLatch held = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
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
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            holder.start();
            check(held.await(5, TimeUnit.SECONDS), "the lock was not taken");
            try {
                server.placement().invalidate();
                long start = System.nanoTime();
                checkEquals(3, TestJson.objects(placement(server.port(), id), "chunks").size(), "chunks");
                check(System.nanoTime() - start < 3_000_000_000L, "the placement waited for the storage lock");
            } finally {
                release.countDown();
                holder.join(5_000);
            }
            checkEquals(reads, chunkReads(nodes), "chunk reads on the nodes");
        } finally {
            server.stop();
            closeAll(nodes);
        }
    }

    private static void sharedListing(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = controlledNodes(tempDir);
        HeartbeatMonitor monitor = new HeartbeatMonitor(nodes, List.of(true, true, true), 1_000, 3, new NodeTestKit.TestClock(),
                new NodeTestKit.Lines().stream());
        PlacementSnapshot snapshot = new PlacementSnapshot(nodes, monitor, 60_000, new NodeTestKit.TestClock());
        List<Thread> threads = new ArrayList<>();
        CountDownLatch go = new CountDownLatch(1);
        for (int t = 0; t < 12; t++) {
            threads.add(new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                snapshot.get();
            }));
        }
        threads.forEach(Thread::start);
        go.countDown();
        for (Thread thread : threads) {
            thread.join(10_000);
        }
        checkEquals(3, totalListings(nodes), "listings for 12 concurrent requests");
        checkEquals(3, snapshot.listingsMade(), "listingsMade");
        snapshot.invalidate();
        snapshot.get();
        snapshot.get();
        checkEquals(6, totalListings(nodes), "listings after invalidate and two more requests");
    }

    private static void period(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = controlledNodes(tempDir);
        HeartbeatMonitor monitor = new HeartbeatMonitor(nodes, List.of(true, true, true), 1_000, 3, new NodeTestKit.TestClock(),
                new NodeTestKit.Lines().stream());
        PlacementSnapshot cached = new PlacementSnapshot(nodes, monitor, 200, new NodeTestKit.TestClock());
        PlacementSnapshot.Snapshot first = cached.get();
        check(cached.get() == first, "a fresh snapshot was not reused");
        checkEquals(3, totalListings(nodes), "listings within the period");
        Thread.sleep(300);
        check(cached.get() != first, "an old snapshot was reused");
        checkEquals(6, totalListings(nodes), "listings after the period");
        PlacementSnapshot uncached = new PlacementSnapshot(nodes, monitor, 0, new NodeTestKit.TestClock());
        uncached.get();
        uncached.get();
        checkEquals(12, totalListings(nodes), "listings with a zero period");
    }

    private static void downAndFailingNodes(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = controlledNodes(tempDir);
        Files.writeString(nodes.get(2).dir().resolve("0123456789abcdef0123456789abcdef_0"), "copy");
        NodeTestKit.TestClock clock = new NodeTestKit.TestClock();
        HeartbeatMonitor monitor = new HeartbeatMonitor(nodes, List.of(true, false, true), 1_000, 3, clock,
                new NodeTestKit.Lines().stream());
        PlacementSnapshot snapshot = new PlacementSnapshot(nodes, monitor, 0, clock);
        PlacementSnapshot.Snapshot s = snapshot.get();
        checkEquals(0, nodes.get(1).listings.get(), "listings of the DOWN node");
        checkEquals(false, s.nodes().get(1).reachable(), "the DOWN node is reachable");
        checkEquals(false, s.nodes().get(1).listed(), "the DOWN node is listed");
        checkEquals(true, s.nodes().get(2).reachable(), "node 2 reachable");
        checkEquals(java.util.Set.of("0123456789abcdef0123456789abcdef_0"), s.nodes().get(2).chunks(), "node 2's chunks");
        // Node 2 is still UP for the heartbeat, but its listing fails now: its last listing is kept.
        nodes.get(2).down = true;
        clock.advance(5_000);
        PlacementSnapshot.Snapshot later = snapshot.get();
        checkEquals(false, later.nodes().get(2).reachable(), "node 2 reachable after a failed listing");
        checkEquals(true, later.nodes().get(2).listed(), "node 2 listed");
        checkEquals(s.nodes().get(2).listedAt(), later.nodes().get(2).listedAt(), "the time of the last good listing");
        checkEquals(s.nodes().get(2).chunks(), later.nodes().get(2).chunks(), "the last good chunks");
        check(!later.everyNodeReachable(), "every node reachable");
    }

    private static void freshAfterUpload(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String first = fileId(upload(server.port(), randomBytes(CHUNK, 19)));
            placement(server.port(), first); // a fresh listing, made before the next upload
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 9)));
            for (Map<String, Object> chunk : TestJson.objects(placement(server.port(), id), "chunks")) {
                checkEquals(2L, chunk.get("reachableCopies"), "copies of chunk " + chunk.get("index") + " right after the upload");
            }
        } finally {
            server.stop();
        }
    }

    private static void freshAfterRepair(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(upload(server.port(), randomBytes(2 * CHUNK, 10)));
            Files.delete(nodeDir(server, onDisk(server, FileIds.chunkId(id, 0)).get(0).intValue()).resolve(FileIds.chunkId(id, 0)));
            server.placement().invalidate();
            checkEquals(1L, TestJson.objects(placement(server.port(), id), "chunks").get(0).get("reachableCopies"), "before");
            checkEquals(200, repair(server.port(), "dryRun=true").status(), "dry run");
            checkEquals(1L, TestJson.objects(placement(server.port(), id), "chunks").get(0).get("reachableCopies"),
                    "after the dry run");
            checkEquals(200, repair(server.port(), null).status(), "repair");
            checkEquals(2L, TestJson.objects(placement(server.port(), id), "chunks").get(0).get("reachableCopies"),
                    "right after the repair");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    private static NodeTestKit.NodeHandle[] threeNodes(Path tempDir) throws IOException {
        return new NodeTestKit.NodeHandle[] {startNode(tempDir.resolve("n0")), startNode(tempDir.resolve("n1")),
                startNode(tempDir.resolve("n2"))};
    }

    private static DistributedFsServer remoteServer(Path tempDir, NodeTestKit.NodeHandle[] nodes, long heartbeatMillis)
            throws IOException {
        List<Integer> ports = new ArrayList<>();
        for (NodeTestKit.NodeHandle node : nodes) {
            ports.add(node.port());
        }
        return startServer(tempDir.resolve("data"), ports, new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK,
                heartbeatMillis);
    }

    private static void closeAll(NodeTestKit.NodeHandle[] nodes) {
        for (NodeTestKit.NodeHandle node : nodes) {
            node.close();
        }
    }

    private static List<NodeTestKit.ControlledNode> controlledNodes(Path tempDir) throws IOException {
        List<NodeTestKit.ControlledNode> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes.add(new NodeTestKit.ControlledNode(Files.createDirectories(tempDir.resolve("node-" + i))));
        }
        return nodes;
    }

    private static int totalListings(List<NodeTestKit.ControlledNode> nodes) {
        int total = 0;
        for (NodeTestKit.ControlledNode node : nodes) {
            total += node.listings.get();
        }
        return total;
    }

    private static NodeTestKit.RawResponse get(int port, String target) throws IOException {
        return raw(port, "GET " + target + " HTTP/1.1\r\n" + host(port), null);
    }

    private static Map<String, Object> placement(int port, String id) throws IOException {
        NodeTestKit.RawResponse response = get(port, "/files/" + id + "/placement");
        checkEquals(200, response.status(), "placement of " + id + " (" + response.text() + ")");
        return TestJson.object(response.text());
    }

    private static void checkError(NodeTestKit.RawResponse response, ApiError error, String what) {
        checkEquals(error.status(), response.status(), what + ": status");
        checkEquals(error.toJson(), response.text(), what + ": body");
    }

    private static Path nodeDir(DistributedFsServer server, int index) {
        return server.dataRoot().resolve("nodes").resolve("node-" + index);
    }

    /** The local nodes whose folder holds the chunk, in node order. */
    private static List<Long> onDisk(DistributedFsServer server, String chunkId) {
        List<Long> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            if (Files.isRegularFile(nodeDir(server, i).resolve(chunkId))) {
                nodes.add((long) i);
            }
        }
        return nodes;
    }

    /** Every copy of the file's chunks on the local nodes, as "chunk@node", in order. */
    private static List<String> allCopies(DistributedFsServer server, String id, int chunks) {
        List<String> copies = new ArrayList<>();
        for (int i = 0; i < chunks; i++) {
            for (long node : onDisk(server, FileIds.chunkId(id, i))) {
                copies.add(i + "@" + node);
            }
        }
        return copies;
    }

    private static int trimCandidates(Map<String, Object> placement) {
        int count = 0;
        for (Map<String, Object> chunk : TestJson.objects(placement, "chunks")) {
            for (Map<String, Object> holder : TestJson.objects(chunk, "holders")) {
                if (Boolean.TRUE.equals(holder.get("trimCandidate"))) {
                    count++;
                }
            }
        }
        return count;
    }

    private static long chunkReads(NodeTestKit.NodeHandle[] nodes) {
        long reads = 0;
        for (NodeTestKit.NodeHandle node : nodes) {
            reads += node.out().text().lines().filter(line -> line.startsWith("GET /chunks/") && !line.contains("/exists")).count();
        }
        return reads;
    }
}
