package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.expectCannotTell;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.fastConfig;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.reply;
import static com.distributedfs.NodeTestKit.sha256;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Plain-Java tests for RemoteStorageNode, the HTTP client of a storage node: round trips through a
 * real node server, and every row of the mapping from node replies to Java outcomes, using stand-in
 * servers that answer exactly as each test scripts them.
 *
 * <ul>
 *   <li>Normal: {@code java -cp "out;out-test" com.distributedfs.RemoteStorageNodeTest}</li>
 *   <li>Stress: {@code java -Xmx16m -cp "out;out-test" com.distributedfs.RemoteStorageNodeTest --stress}
 *       stores and retrieves one 64 MiB chunk through a node in this process; with a 16 MB heap
 *       it would fail with OutOfMemoryError if the client or the node held a whole chunk in memory.</li>
 * </ul>
 */
public class RemoteStorageNodeTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("remotenode-test-");

    /** The node ID the stand-in servers claim, and a different one. */
    private static final String ID = "0123456789abcdef0123456789abcdef";
    private static final String OTHER_ID = "fedcba9876543210fedcba9876543210";

    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals("--stress")) {
            RUNNER.run("a 64 MiB chunk is stored and retrieved through a node without holding it in memory",
                    RemoteStorageNodeTest::largeChunkIsStreamed);
            RUNNER.finish();
            return;
        }
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.RemoteStorageNodeTest [--stress]");
            System.exit(2);
        }
        // Against a real node
        RUNNER.run("connect learns the node's ID, and health confirms it", RemoteStorageNodeTest::connectLearnsId);
        RUNNER.run("store, exists, retrieve, list, delete and sweep work through a real node",
                RemoteStorageNodeTest::roundTrip);
        RUNNER.run("an existing ID, a missing chunk and an existing destination give the same answers as a local node",
                RemoteStorageNodeTest::sameAnswersAsLocalNode);
        RUNNER.run("an invalid chunk ID is IllegalArgumentException for every method, and nothing is sent",
                RemoteStorageNodeTest::invalidIdSendsNothing);
        RUNNER.run("a store sends the chunk once, with its length and SHA-256", RemoteStorageNodeTest::storeSendsChecksum);

        // Unreachable or silent nodes
        RUNNER.run("a refused connection is 'cannot tell' for every check, and 'not stored' for a store",
                RemoteStorageNodeTest::refusedConnection);
        RUNNER.run("a node that never answers times out: 'cannot tell' for checks, ambiguous for a store",
                RemoteStorageNodeTest::silentNode);
        RUNNER.run("a connection reset after the request: 'cannot tell' for checks, ambiguous for a store",
                RemoteStorageNodeTest::resetConnection);

        // The reply mapping
        RUNNER.run("exists: only exists=true or exists=false from this node is an answer", RemoteStorageNodeTest::existsMapping);
        RUNNER.run("retrieve: only 404 chunk_not_found from this node is 'absent'", RemoteStorageNodeTest::retrieveMapping);
        RUNNER.run("retrieve: a reply from another node writes nothing", RemoteStorageNodeTest::retrieveFromOtherNodeWritesNothing);
        RUNNER.run("retrieve: a body shorter than its Content-Length fails and leaves no destination",
                RemoteStorageNodeTest::truncatedRetrieve);
        RUNNER.run("retrieve: a body that stops is abandoned after the stall timeout and leaves no destination",
                RemoteStorageNodeTest::stalledRetrieve);
        RUNNER.run("retrieve: a slow but steady body is not cut off", RemoteStorageNodeTest::slowRetrieveCompletes);
        RUNNER.run("delete: only deleted=true or deleted=false from this node is an answer", RemoteStorageNodeTest::deleteMapping);
        RUNNER.run("store: 201 with the right count is stored, 409 chunk_exists is a duplicate",
                RemoteStorageNodeTest::storeSuccessAndDuplicate);
        RUNNER.run("store: every refusal that means nothing was published is 'certainly not stored'",
                RemoteStorageNodeTest::storeDefiniteRefusals);
        RUNNER.run("store: every other reply, a wrong count or another node's reply is ambiguous",
                RemoteStorageNodeTest::storeAmbiguousReplies);
        RUNNER.run("list and sweep: malformed, unsorted or foreign replies are 'cannot tell'", RemoteStorageNodeTest::listAndSweepMapping);
        RUNNER.run("connect refuses a program that does not answer as a storage node", RemoteStorageNodeTest::connectRefusesNonNode);
        RUNNER.finish();
    }

    // ---------------------------------------------------------------------
    // Against a real node
    // ---------------------------------------------------------------------

    private static void connectLearnsId(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"));
             RemoteStorageNode remote = RemoteStorageNode.connect(node.port(), fastConfig())) {
            checkEquals(java.util.Optional.of(node.nodeId()), remote.nodeId(), "node ID");
            checkEquals(java.util.Optional.empty(), remote.localDirectory(), "local directory");
            remote.health();
            checkContains(remote.describe(), node.nodeId());
            checkContains(remote.describe(), "127.0.0.1:" + node.port());
        }
    }

    private static void roundTrip(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"));
             RemoteStorageNode remote = RemoteStorageNode.connect(node.port(), fastConfig())) {
            byte[] data = randomBytes(1_500_000, 1);
            Path source = Files.write(tempDir.resolve("source"), data);
            checkEquals(false, remote.hasChunk("file_0"), "hasChunk before");
            checkEquals(false, remote.holdsRegularChunk("file_0"), "holdsRegularChunk before");
            checkEquals(1_500_000L, remote.storeChunk("file_0", source), "bytes stored");
            checkEquals(true, remote.hasChunk("file_0"), "hasChunk after");
            checkEquals(true, remote.holdsRegularChunk("file_0"), "holdsRegularChunk after");
            Path copy = tempDir.resolve("out").resolve("copy");
            checkEquals(1_500_000L, remote.retrieveChunk("file_0", copy), "bytes retrieved");
            check(Arrays.equals(data, Files.readAllBytes(copy)), "the retrieved copy differs");
            check(Arrays.equals(data, Files.readAllBytes(source)), "the source was changed");

            Files.write(node.chunksDir().resolve("file_0-99.tmp"), new byte[] {1});
            checkEquals(List.of(new ChunkStore.Entry("file_0", ChunkStore.EntryKind.FILE),
                    new ChunkStore.Entry("file_0-99.tmp", ChunkStore.EntryKind.FILE)), remote.listEntries(), "entries");
            List<String> warnings = new ArrayList<>();
            checkEquals(1, remote.sweepTempFiles(warnings::add), "temp files removed");
            checkEquals(List.of(), warnings, "warnings");
            checkEquals(true, remote.deleteChunk("file_0"), "first delete");
            checkEquals(false, remote.deleteChunk("file_0"), "second delete");
            checkEquals(List.of(), remote.listEntries(), "entries at the end");
        }
    }

    private static void sameAnswersAsLocalNode(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"));
             RemoteStorageNode remote = RemoteStorageNode.connect(node.port(), fastConfig())) {
            Path source = Files.write(tempDir.resolve("source"), randomBytes(1000, 2));
            remote.storeChunk("file_0", source);
            Path other = Files.write(tempDir.resolve("other"), randomBytes(1000, 3));
            expectThrows(FileAlreadyExistsException.class, "storing an existing ID", () -> remote.storeChunk("file_0", other));
            check(Arrays.equals(Files.readAllBytes(source), Files.readAllBytes(node.chunksDir().resolve("file_0"))),
                    "the existing chunk was changed");

            Path destination = tempDir.resolve("missing-copy");
            expectThrows(ChunkNotFoundException.class, "retrieving a missing chunk", () -> remote.retrieveChunk("file_9", destination));
            check(!Files.exists(destination), "a missing chunk created a destination");

            Path existing = Files.writeString(tempDir.resolve("existing"), "keep me");
            expectThrows(FileAlreadyExistsException.class, "existing destination", () -> remote.retrieveChunk("file_0", existing));
            checkEquals("keep me", Files.readString(existing), "the existing destination");
            expectThrows(IllegalArgumentException.class, "destination is a directory",
                    () -> remote.retrieveChunk("file_0", tempDir));
            expectThrows(IllegalArgumentException.class, "missing source",
                    () -> remote.storeChunk("file_1", tempDir.resolve("nope")));
        }
    }

    private static void invalidIdSendsNothing(Path tempDir) throws Exception {
        try (NodeTestKit.Stub stub = new NodeTestKit.Stub(exchange -> reply(exchange, 200, ID, "exists=false\n"));
             RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            Path source = Files.write(tempDir.resolve("source"), new byte[] {1});
            for (String id : List.of("", "ABC", "a.b", "..", "a/b", "a%2fb", "con", "x".repeat(129))) {
                expectThrows(IllegalArgumentException.class, "store " + id, () -> remote.storeChunk(id, source));
                expectThrows(IllegalArgumentException.class, "retrieve " + id, () -> remote.retrieveChunk(id, tempDir.resolve("c")));
                expectThrows(IllegalArgumentException.class, "hasChunk " + id, () -> remote.hasChunk(id));
                expectThrows(IllegalArgumentException.class, "holdsRegularChunk " + id, () -> remote.holdsRegularChunk(id));
                expectThrows(IllegalArgumentException.class, "delete " + id, () -> remote.deleteChunk(id));
            }
            expectThrows(NullPointerException.class, "null ID", () -> remote.hasChunk(null));
            checkEquals(0, stub.requests.get(), "requests that reached the node");
        }
    }

    private static void storeSendsChecksum(Path tempDir) throws Exception {
        byte[] data = randomBytes(100_000, 4);
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<String> checksum = new AtomicReference<>();
        try (NodeTestKit.Stub stub = new NodeTestKit.Stub(exchange -> {
            checksum.set(exchange.getRequestHeaders().getFirst("X-Chunk-Sha256"));
            received.set(exchange.getRequestBody().readAllBytes());
            reply(exchange, 201, ID, "stored=" + data.length + "\n");
        }); RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            Path source = Files.write(tempDir.resolve("source"), data);
            checkEquals((long) data.length, remote.storeChunk("file_0", source), "bytes stored");
            checkEquals(1, stub.requests.get(), "requests for one store");
            checkEquals("POST /chunks/file_0", stub.requestLines.get(0), "request line");
            checkEquals(sha256(data), checksum.get(), "X-Chunk-Sha256");
            check(Arrays.equals(data, received.get()), "the bytes sent differ from the source");
        }
    }

    // ---------------------------------------------------------------------
    // Unreachable or silent nodes
    // ---------------------------------------------------------------------

    private static void refusedConnection(Path tempDir) throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort(); // closed below, so connecting is refused
        }
        try (RemoteStorageNode remote = new RemoteStorageNode(port, ID, fastConfig())) {
            Path source = Files.write(tempDir.resolve("source"), new byte[] {1});
            checkAllChecksCannotTell(remote, tempDir);
            IOException store = expectCannotTell("store", () -> remote.storeChunk("file_0", source));
            checkContains(store.getMessage(), "could not be reached");
        }
    }

    private static void silentNode(Path tempDir) throws Exception {
        try (NodeTestKit.SocketStub stub = new NodeTestKit.SocketStub(NodeTestKit::holdSilently);
             RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            Path source = Files.write(tempDir.resolve("source"), randomBytes(1000, 5));
            long started = System.nanoTime();
            expectCannotTell("hasChunk", () -> remote.hasChunk("file_0"));
            long millis = (System.nanoTime() - started) / 1_000_000;
            check(millis < 6_000, "the check took " + millis + " ms");
            expectThrows(AmbiguousStoreException.class, "store", () -> remote.storeChunk("file_0", source));
        }
    }

    private static void resetConnection(Path tempDir) throws Exception {
        try (NodeTestKit.SocketStub stub = new NodeTestKit.SocketStub(socket -> {
            NodeTestKit.readRequest(socket);
            NodeTestKit.reset(socket);
        }); RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            Path source = Files.write(tempDir.resolve("source"), randomBytes(1000, 6));
            checkAllChecksCannotTell(remote, tempDir);
            expectThrows(AmbiguousStoreException.class, "store", () -> remote.storeChunk("file_0", source));
        }
    }

    /** Every non-store method must fail with a plain IOException, never answer. */
    private static void checkAllChecksCannotTell(RemoteStorageNode remote, Path tempDir) throws Exception {
        Path destination = tempDir.resolve("copy");
        expectCannotTell("hasChunk", () -> remote.hasChunk("file_0"));
        expectCannotTell("holdsRegularChunk", () -> remote.holdsRegularChunk("file_0"));
        expectCannotTell("retrieve", () -> remote.retrieveChunk("file_0", destination));
        expectCannotTell("delete", () -> remote.deleteChunk("file_0"));
        expectCannotTell("listEntries", remote::listEntries);
        expectCannotTell("sweep", () -> remote.sweepTempFiles(message -> { }));
        expectCannotTell("health", remote::health);
        check(!Files.exists(destination), "a failed retrieve left a destination");
    }

    // ---------------------------------------------------------------------
    // The reply mapping
    // ---------------------------------------------------------------------

    /** A scripted reply: status, node ID header (or null) and body. */
    private record Scripted(int status, String nodeId, String body) {
    }

    private static void existsMapping(Path tempDir) throws Exception {
        checkEquals(true, withReply(new Scripted(200, ID, "exists=true\n"), remote -> remote.hasChunk("file_0")), "true");
        checkEquals(false, withReply(new Scripted(200, ID, "exists=false\n"), remote -> remote.holdsRegularChunk("file_0")),
                "false");
        List<Scripted> notAnswers = List.of(
                new Scripted(200, ID, ""), new Scripted(200, ID, "exists=maybe\n"), new Scripted(200, ID, "exists=false"),
                new Scripted(200, ID, "exists=false\nextra=1\n"), new Scripted(200, ID, "deleted=false\n"),
                new Scripted(404, ID, "error=chunk_not_found\n"), new Scripted(500, ID, "error=unsafe_entry\n"),
                new Scripted(503, ID, "error=storage_unavailable\n"), new Scripted(503, ID, "error=node_busy\n"),
                new Scripted(200, OTHER_ID, "exists=false\n"), new Scripted(200, null, "exists=false\n"),
                new Scripted(200, "not-an-id", "exists=false\n"), new Scripted(302, ID, ""));
        for (Scripted scripted : notAnswers) {
            for (boolean follow : new boolean[] {true, false}) {
                expectCannotTell(scripted + (follow ? " hasChunk" : " holdsRegularChunk"),
                        () -> withReply(scripted, remote -> follow ? remote.hasChunk("file_0") : remote.holdsRegularChunk("file_0")));
            }
        }
    }

    private static void retrieveMapping(Path tempDir) throws Exception {
        Path destination = tempDir.resolve("copy");
        expectThrows(ChunkNotFoundException.class, "404 chunk_not_found", () -> withReply(
                new Scripted(404, ID, "error=chunk_not_found\n"), remote -> remote.retrieveChunk("file_0", destination)));
        List<Scripted> notAbsent = List.of(
                new Scripted(404, ID, "error=not_found\n"), new Scripted(404, ID, ""), new Scripted(404, ID, "chunk_not_found"),
                new Scripted(404, OTHER_ID, "error=chunk_not_found\n"), new Scripted(404, null, "error=chunk_not_found\n"),
                new Scripted(500, ID, "error=unsafe_entry\n"), new Scripted(503, ID, "error=storage_unavailable\n"),
                new Scripted(400, ID, "error=invalid_chunk_id\n"), new Scripted(410, ID, "error=chunk_not_found\n"));
        for (Scripted scripted : notAbsent) {
            expectCannotTell(scripted.toString(), () -> withReply(scripted, remote -> remote.retrieveChunk("file_0", destination)));
            check(!Files.exists(destination), scripted + " created the destination");
        }
    }

    private static void retrieveFromOtherNodeWritesNothing(Path tempDir) throws Exception {
        Path destination = tempDir.resolve("copy");
        expectCannotTell("200 from another node", () -> withReply(new Scripted(200, OTHER_ID, "some chunk bytes"),
                remote -> remote.retrieveChunk("file_0", destination)));
        check(!Files.exists(destination), "a reply from another node was written");
        expectCannotTell("200 without a node ID", () -> withReply(new Scripted(200, null, "some chunk bytes"),
                remote -> remote.retrieveChunk("file_0", destination)));
        check(!Files.exists(destination), "a reply without a node ID was written");
    }

    private static void truncatedRetrieve(Path tempDir) throws Exception {
        Path destination = tempDir.resolve("copy");
        try (NodeTestKit.SocketStub stub = new NodeTestKit.SocketStub(socket -> {
            NodeTestKit.readRequest(socket);
            OutputStream out = socket.getOutputStream();
            out.write(("HTTP/1.1 200 OK\r\nX-Node-Id: " + ID + "\r\nContent-Length: 100000\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.write(randomBytes(40_000, 7));
            out.flush();
            socket.close();
        }); RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            expectCannotTell("truncated body", () -> remote.retrieveChunk("file_0", destination));
            check(!Files.exists(destination), "a partial destination was left");
        }
    }

    private static void stalledRetrieve(Path tempDir) throws Exception {
        Path destination = tempDir.resolve("copy");
        try (NodeTestKit.SocketStub stub = new NodeTestKit.SocketStub(socket -> {
            NodeTestKit.readRequest(socket);
            OutputStream out = socket.getOutputStream();
            out.write(("HTTP/1.1 200 OK\r\nX-Node-Id: " + ID + "\r\nContent-Length: 100000\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.write(randomBytes(40_000, 8));
            out.flush();
            NodeTestKit.holdSilently(socket);
        }); RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            long started = System.nanoTime();
            IOException e = expectCannotTell("stalled body", () -> remote.retrieveChunk("file_0", destination));
            long millis = (System.nanoTime() - started) / 1_000_000;
            checkContains(e.getMessage(), "no data arrived");
            check(millis < 5_000, "the stalled retrieve took " + millis + " ms");
            check(!Files.exists(destination), "a partial destination was left");
        }
    }

    private static void slowRetrieveCompletes(Path tempDir) throws Exception {
        byte[] data = randomBytes(20_000, 9);
        Path destination = tempDir.resolve("copy");
        try (NodeTestKit.SocketStub stub = new NodeTestKit.SocketStub(socket -> {
            NodeTestKit.readRequest(socket);
            OutputStream out = socket.getOutputStream();
            out.write(("HTTP/1.1 200 OK\r\nX-Node-Id: " + ID + "\r\nContent-Length: " + data.length + "\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            // 10 pieces, 250 ms apart: 2.5 s in total, far longer than the 600 ms stall limit,
            // but never 600 ms without new bytes.
            for (int i = 0; i < 10; i++) {
                out.write(data, i * 2_000, 2_000);
                out.flush();
                Thread.sleep(250);
            }
            socket.close();
        }); RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            checkEquals((long) data.length, remote.retrieveChunk("file_0", destination), "bytes");
            check(Arrays.equals(data, Files.readAllBytes(destination)), "the slow copy differs");
        }
    }

    private static void deleteMapping(Path tempDir) throws Exception {
        checkEquals(true, withReply(new Scripted(200, ID, "deleted=true\n"), remote -> remote.deleteChunk("file_0")), "true");
        checkEquals(false, withReply(new Scripted(200, ID, "deleted=false\n"), remote -> remote.deleteChunk("file_0")), "false");
        for (Scripted scripted : List.of(new Scripted(200, ID, "deleted=yes\n"), new Scripted(200, ID, "exists=false\n"),
                new Scripted(404, ID, "error=chunk_not_found\n"), new Scripted(500, ID, "error=delete_failed\n"),
                new Scripted(500, ID, "error=unsafe_entry\n"), new Scripted(503, ID, "error=node_busy\n"),
                new Scripted(200, OTHER_ID, "deleted=false\n"))) {
            expectCannotTell(scripted.toString(), () -> withReply(scripted, remote -> remote.deleteChunk("file_0")));
        }
    }

    private static void storeSuccessAndDuplicate(Path tempDir) throws Exception {
        Path source = Files.write(tempDir.resolve("source"), randomBytes(5, 10));
        checkEquals(5L, withReply(new Scripted(201, ID, "stored=5\n"), remote -> remote.storeChunk("file_0", source)), "stored");
        expectThrows(FileAlreadyExistsException.class, "409 chunk_exists",
                () -> withReply(new Scripted(409, ID, "error=chunk_exists\n"), remote -> remote.storeChunk("file_0", source)));
    }

    private static void storeDefiniteRefusals(Path tempDir) throws Exception {
        Path source = Files.write(tempDir.resolve("source"), randomBytes(5, 11));
        for (Scripted scripted : List.of(new Scripted(400, ID, "error=bad_request\n"),
                new Scripted(400, ID, "error=invalid_chunk_id\n"), new Scripted(403, ID, "error=host_not_allowed\n"),
                new Scripted(413, ID, "error=too_large\n"), new Scripted(422, ID, "error=checksum_mismatch\n"),
                new Scripted(500, ID, "error=store_failed\n"), new Scripted(503, ID, "error=storage_unavailable\n"),
                new Scripted(503, ID, "error=node_busy\n"))) {
            IOException e = expectCannotTell(scripted.toString(),
                    () -> withReply(scripted, remote -> remote.storeChunk("file_0", source)));
            checkContains(e.getMessage(), "did not store");
        }
    }

    private static void storeAmbiguousReplies(Path tempDir) throws Exception {
        Path source = Files.write(tempDir.resolve("source"), randomBytes(5, 12));
        for (Scripted scripted : List.of(new Scripted(201, ID, "stored=4\n"), new Scripted(201, ID, "stored=5"),
                new Scripted(201, ID, ""), new Scripted(200, ID, "stored=5\n"), new Scripted(409, ID, "error=other\n"),
                new Scripted(409, ID, ""), new Scripted(500, ID, "error=internal_error\n"), new Scripted(500, ID, ""),
                new Scripted(404, ID, "error=not_found\n"), new Scripted(503, ID, "error=unknown\n"),
                new Scripted(422, ID, "error=bad_request\n"), new Scripted(302, ID, ""),
                new Scripted(201, OTHER_ID, "stored=5\n"), new Scripted(503, OTHER_ID, "error=node_busy\n"),
                new Scripted(409, OTHER_ID, "error=chunk_exists\n"), new Scripted(201, null, "stored=5\n"))) {
            expectThrows(AmbiguousStoreException.class, scripted.toString(),
                    () -> withReply(scripted, remote -> remote.storeChunk("file_0", source)));
        }
    }

    private static void listAndSweepMapping(Path tempDir) throws Exception {
        checkEquals(List.of(new ChunkStore.Entry("a b", ChunkStore.EntryKind.OTHER)),
                withReply(new Scripted(200, ID, "other a%20b\n"), RemoteStorageNode::listEntries), "decoded entry");
        for (Scripted scripted : List.of(new Scripted(200, ID, "file b\nfile a\n"), new Scripted(200, ID, "file a\nfile a\n"),
                new Scripted(200, ID, "socket a\n"), new Scripted(200, ID, "file a b\n"), new Scripted(200, ID, "file a"),
                new Scripted(200, ID, "file %zz\n"), new Scripted(503, ID, "error=storage_unavailable\n"),
                new Scripted(200, OTHER_ID, "file a\n"))) {
            expectCannotTell("list " + scripted, () -> withReply(scripted, RemoteStorageNode::listEntries));
        }
        List<String> warnings = new ArrayList<>();
        checkEquals(3, withReply(new Scripted(200, ID, "removed=3\nwarnings=2\n"),
                remote -> remote.sweepTempFiles(warnings::add)), "removed");
        checkEquals(1, warnings.size(), "one summary warning");
        checkContains(warnings.get(0), "2 warnings");
        for (Scripted scripted : List.of(new Scripted(200, ID, "removed=3\n"), new Scripted(200, ID, "removed=-1\nwarnings=0\n"),
                new Scripted(503, ID, "error=node_busy\n"), new Scripted(200, OTHER_ID, "removed=0\nwarnings=0\n"))) {
            expectCannotTell("sweep " + scripted, () -> withReply(scripted, remote -> remote.sweepTempFiles(message -> { })));
        }
    }

    private static void connectRefusesNonNode(Path tempDir) throws Exception {
        for (Scripted scripted : List.of(new Scripted(200, ID, "status=ok\n"), new Scripted(200, null, "hello"),
                new Scripted(200, OTHER_ID, "status=ok\nnode-id=" + ID + "\nformat=1\n"),
                new Scripted(200, ID, "status=ok\nnode-id=" + ID + "\nformat=2\n"), new Scripted(404, ID, ""))) {
            try (NodeTestKit.Stub stub = new NodeTestKit.Stub(
                    exchange -> reply(exchange, scripted.status(), scripted.nodeId(), scripted.body()))) {
                expectThrows(IOException.class, scripted.toString(), () -> RemoteStorageNode.connect(stub.port(), fastConfig()));
            }
        }
    }

    // ---------------------------------------------------------------------
    // Stress
    // ---------------------------------------------------------------------

    private static void largeChunkIsStreamed(Path tempDir) throws Exception {
        long size = 64L * 1024 * 1024;
        Path source = tempDir.resolve("source");
        Random random = new Random(64);
        byte[] piece = new byte[1024 * 1024];
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        try (OutputStream out = Files.newOutputStream(source)) {
            for (int i = 0; i < 64; i++) {
                random.nextBytes(piece);
                digest.update(piece);
                out.write(piece);
            }
        }
        String expected = java.util.HexFormat.of().formatHex(digest.digest());
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"), size, 30_000, 10_000);
             RemoteStorageNode remote = RemoteStorageNode.connect(node.port(), RemoteNodeConfig.defaults())) {
            checkEquals(size, remote.storeChunk("big_0", source), "bytes stored");
            Files.delete(source);
            Path copy = tempDir.resolve("copy");
            checkEquals(size, remote.retrieveChunk("big_0", copy), "bytes retrieved");
            checkEquals(expected, new ChecksumService().calculateSha256(copy), "SHA-256 of the copy");
            checkEquals(List.of("big_0"), listNames(node.chunksDir()), "chunk folder");
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private interface RemoteCall<T> {
        T call(RemoteStorageNode remote) throws Exception;
    }

    /** Calls {@code call} on a client of a stand-in node that answers every request with {@code scripted}. */
    private static <T> T withReply(Scripted scripted, RemoteCall<T> call) throws Exception {
        try (NodeTestKit.Stub stub = new NodeTestKit.Stub(
                exchange -> reply(exchange, scripted.status(), scripted.nodeId(), scripted.body()));
             RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            return call.call(remote);
        }
    }
}
