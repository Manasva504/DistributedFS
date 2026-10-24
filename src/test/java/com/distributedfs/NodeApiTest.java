package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.createDirectoryJunction;
import static com.distributedfs.NodeTestKit.createFileSymlinkOrSkip;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.sha256;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Plain-Java tests for the storage node's HTTP API (NodeApiHandler inside a StorageNodeServer on
 * port 0), sending raw requests over a socket so that every header and every broken request can
 * be written exactly. Run: {@code java -cp "out;out-test" com.distributedfs.NodeApiTest}.
 */
public class NodeApiTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("nodeapi-test-");

    public static void main(String[] args) {
        RUNNER.run("GET /health answers ok with the node ID, in the body and in X-Node-Id", NodeApiTest::health);
        RUNNER.run("a store with the right length and SHA-256 is published, byte for byte, with no temp file left",
                NodeApiTest::storeIsPublished);
        RUNNER.run("storing an ID that exists is 409 chunk_exists and the chunk is unchanged", NodeApiTest::duplicateStoreIs409);
        RUNNER.run("every invalid chunk ID is 400 invalid_chunk_id on every route, and nothing is created",
                NodeApiTest::invalidIdsAreRefused);
        RUNNER.run("a store without Content-Length (chunked) or with a missing or malformed SHA-256 is 400",
                NodeApiTest::badStoreHeadersAre400);
        RUNNER.run("a store whose bytes do not match the SHA-256 is 422 and leaves no chunk and no temp file",
                NodeApiTest::checksumMismatchIs422);
        RUNNER.run("a store larger than the node's chunk limit is 413", NodeApiTest::tooLargeIs413);
        RUNNER.run("a store body that ends early gets no answer and leaves no chunk and no temp file",
                NodeApiTest::shortBodyLeavesNothing);
        RUNNER.run("a stalled store body is cut off without an answer, leaves nothing, and the ID can be stored afterwards",
                NodeApiTest::stalledBodyLeavesNothing);
        RUNNER.run("GET /chunks/<id> sends exactly the chunk's bytes, also for an empty chunk", NodeApiTest::readSendsBytes);
        RUNNER.run("a missing chunk is 404 chunk_not_found", NodeApiTest::missingChunkIs404);
        RUNNER.run("exists and delete answer true and false only for a chunk that is or is not there",
                NodeApiTest::existsAndDelete);
        RUNNER.run("a junction under a chunk's ID is 500 unsafe_entry for read, exists and delete, never 404 or false",
                NodeApiTest::junctionUnderIdIsUnsafe);
        RUNNER.run("a symbolic link to a file under a chunk's ID is refused for read, nofollow exists and delete",
                NodeApiTest::symlinkUnderIdIsUnsafe);
        RUNNER.run("with the chunk folder deleted every route is 503 storage_unavailable, never 404 or false",
                NodeApiTest::deletedFolderIs503);
        RUNNER.run("GET /entries lists every entry with its kind, names percent-encoded", NodeApiTest::entriesAreListed);
        RUNNER.run("POST /sweep removes only stale temp files and counts its warnings", NodeApiTest::sweepRemovesTempFiles);
        RUNNER.run("a wrong, missing or repeated Host header, or an absolute target, is 403 and stores nothing",
                NodeApiTest::hostIsChecked);
        RUNNER.run("unknown paths are 404, wrong methods 405 with Allow, unexpected queries 400",
                NodeApiTest::routingErrors);
        RUNNER.run("every response, errors included, carries X-Node-Id and Connection: close",
                NodeApiTest::everyResponseCarriesNodeId);
        RUNNER.run("while a store holds the node lock, other operations get 503 node_busy and the sweep cannot remove its temp file",
                NodeApiTest::busyNodeAnswers503);
        RUNNER.run("each request is logged as one line with method, route, status, bytes and time, without paths",
                NodeApiTest::requestsAreLogged);

        // The node's streaming store (StorageNode.storeChunkFrom), called directly
        RUNNER.run("storeChunkFrom: data shorter than the declared length is refused even when its SHA-256 matches",
                NodeApiTest::shortStreamIsRefused);
        RUNNER.run("storeChunkFrom: exactly the declared length is read, and later bytes are left in the stream",
                NodeApiTest::exactlyLengthIsRead);
        RUNNER.run("storeChunkFrom: an existing ID is refused before anything is read", NodeApiTest::existingIdReadsNothing);
        RUNNER.run("storeChunkFrom: a malformed checksum or a negative length is refused before anything is written",
                NodeApiTest::badArgumentsWriteNothing);
        RUNNER.finish();
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    private static void health(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            NodeTestKit.RawResponse response = raw(node.port(), "GET /health HTTP/1.1\r\n" + host(node.port()), null);
            checkEquals(200, response.status(), "status");
            checkEquals(Map.of("status", "ok", "node-id", node.nodeId(), "format", "1"), fields(response), "fields");
            checkEquals(node.nodeId(), response.header("X-Node-Id"), "X-Node-Id");
            checkEquals("text/plain; charset=utf-8", response.header("Content-Type"), "Content-Type");
        }
    }

    private static void storeIsPublished(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = randomBytes(200_000, 1);
            NodeTestKit.RawResponse response = store(node.port(), "file_0", data, sha256(data));
            checkEquals(201, response.status(), "status");
            checkEquals(Map.of("stored", "200000"), fields(response), "fields");
            checkEquals(List.of("file_0"), listNames(node.chunksDir()), "chunk folder");
            check(Arrays.equals(data, Files.readAllBytes(node.chunksDir().resolve("file_0"))), "stored bytes differ");
        }
    }

    private static void duplicateStoreIs409(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] first = randomBytes(1000, 2);
            byte[] second = randomBytes(1000, 3);
            checkEquals(201, store(node.port(), "file_0", first, sha256(first)).status(), "first store");
            NodeTestKit.RawResponse response = store(node.port(), "file_0", second, sha256(second));
            checkEquals(409, response.status(), "second store");
            checkEquals("error=chunk_exists\n", response.text(), "body");
            check(Arrays.equals(first, Files.readAllBytes(node.chunksDir().resolve("file_0"))), "the chunk was changed");
            checkEquals(List.of("file_0"), listNames(node.chunksDir()), "chunk folder");
        }
    }

    private static void invalidIdsAreRefused(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = {1, 2, 3};
            List<String> ids = List.of("", "ABC", "a.b", "..", "%2e%2e", "a%2fb", "a/b", "con", "nul", "x".repeat(129),
                    "a%20b", "caf%C3%A9", "chunk_0/extra");
            for (String id : ids) {
                checkEquals(400, store(node.port(), id, data, sha256(data)).status(), "store of \"" + id + "\"");
                NodeTestKit.RawResponse read = raw(node.port(), "GET /chunks/" + id + " HTTP/1.1\r\n" + host(node.port()), null);
                checkEquals(400, read.status(), "read of \"" + id + "\"");
                checkEquals("error=invalid_chunk_id\n", read.text(), "body for \"" + id + "\"");
                checkEquals(400, raw(node.port(), "GET /chunks/" + id + "/exists?mode=follow HTTP/1.1\r\n" + host(node.port()),
                        null).status(), "exists of \"" + id + "\"");
                checkEquals(400, raw(node.port(), "DELETE /chunks/" + id + " HTTP/1.1\r\n" + host(node.port()), null).status(),
                        "delete of \"" + id + "\"");
            }
            checkEquals(List.of(), listNames(node.chunksDir()), "nothing was created");
            checkEquals(List.of("chunks", "distributedfs-node.txt", "node.lock"), listNames(tempDir.resolve("node")),
                    "nothing appeared next to the chunk folder");
        }
    }

    private static void badStoreHeadersAre400(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = randomBytes(100, 4);
            String sha = sha256(data);
            int port = node.port();
            NodeTestKit.RawResponse chunked = raw(port, "POST /chunks/file_0 HTTP/1.1\r\n" + host(port)
                    + "\r\nTransfer-Encoding: chunked\r\nX-Chunk-Sha256: " + sha,
                    "5\r\nhello\r\n0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            checkEquals(400, chunked.status(), "chunked body");
            checkEquals(400, raw(port, "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 100", data)
                    .status(), "no checksum");
            checkEquals(400, raw(port, "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 100"
                    + "\r\nX-Chunk-Sha256: " + sha.toUpperCase(java.util.Locale.ROOT), data).status(), "uppercase checksum");
            checkEquals(400, raw(port, "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 100"
                    + "\r\nX-Chunk-Sha256: " + sha.substring(1), data).status(), "short checksum");
            checkEquals(400, raw(port, "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 100"
                    + "\r\nX-Chunk-Sha256: " + sha + "\r\nX-Chunk-Sha256: " + sha, data).status(), "two checksums");
            checkEquals(List.of(), listNames(node.chunksDir()), "nothing was stored");
        }
    }

    private static void checksumMismatchIs422(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = randomBytes(70_000, 5);
            byte[] altered = data.clone();
            altered[12_345] ^= 1;
            NodeTestKit.RawResponse response = store(node.port(), "file_0", altered, sha256(data));
            checkEquals(422, response.status(), "status");
            checkEquals("error=checksum_mismatch\n", response.text(), "body");
            checkEquals(List.of(), listNames(node.chunksDir()), "no chunk and no temp file");
        }
    }

    private static void tooLargeIs413(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"), 1024, 600, 2000)) {
            byte[] data = randomBytes(1025, 6);
            checkEquals(413, store(node.port(), "file_0", data, sha256(data)).status(), "status");
            byte[] fits = randomBytes(1024, 7);
            checkEquals(201, store(node.port(), "file_1", fits, sha256(fits)).status(), "a chunk at the limit");
            checkEquals(List.of("file_1"), listNames(node.chunksDir()), "chunk folder");
        }
    }

    private static void shortBodyLeavesNothing(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = randomBytes(10_000, 8);
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), node.port())) {
                OutputStream out = socket.getOutputStream();
                out.write(storeHead(node.port(), "file_0", data.length, sha256(data)));
                out.write(data, 0, 4_000);
                out.flush();
                socket.shutdownOutput(); // the body ends early
                NodeTestKit.RawResponse response = NodeTestKit.readResponse(socket.getInputStream());
                checkEquals(-1, response.status(), "no answer for an incomplete body");
            }
            waitForLog(node, "POST /chunks/file_0 - ");
            checkEquals(List.of(), listNames(node.chunksDir()), "no chunk and no temp file");
        }
    }

    private static void stalledBodyLeavesNothing(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = randomBytes(10_000, 9);
            long started = System.nanoTime();
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), node.port())) {
                socket.setSoTimeout(10_000);
                OutputStream out = socket.getOutputStream();
                out.write(storeHead(node.port(), "file_0", data.length, sha256(data)));
                out.write(data, 0, 100);
                out.flush();
                NodeTestKit.RawResponse response = NodeTestKit.readResponse(socket.getInputStream());
                checkEquals(-1, response.status(), "no answer for a stalled body");
            }
            long millis = (System.nanoTime() - started) / 1_000_000;
            check(millis < 5_000, "the stalled store was cut off after " + millis + " ms");
            waitForLog(node, "POST /chunks/file_0 - ");
            checkEquals(List.of(), listNames(node.chunksDir()), "no chunk and no temp file");
            checkEquals(201, store(node.port(), "file_0", data, sha256(data)).status(), "storing the ID afterwards");
        }
    }

    private static void readSendsBytes(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            byte[] data = randomBytes(300_000, 10);
            checkEquals(201, store(node.port(), "file_0", data, sha256(data)).status(), "store");
            NodeTestKit.RawResponse response = raw(node.port(), "GET /chunks/file_0 HTTP/1.1\r\n" + host(node.port()), null);
            checkEquals(200, response.status(), "status");
            checkEquals("300000", response.header("Content-Length"), "Content-Length");
            checkEquals("application/octet-stream", response.header("Content-Type"), "Content-Type");
            check(Arrays.equals(data, response.body()), "the bytes sent differ from the chunk");

            checkEquals(201, store(node.port(), "file_1", new byte[0], sha256(new byte[0])).status(), "store empty");
            NodeTestKit.RawResponse empty = raw(node.port(), "GET /chunks/file_1 HTTP/1.1\r\n" + host(node.port()), null);
            checkEquals(200, empty.status(), "empty chunk status");
            checkEquals(0, empty.body().length, "empty chunk body");
        }
    }

    private static void missingChunkIs404(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            NodeTestKit.RawResponse response = raw(node.port(), "GET /chunks/file_0 HTTP/1.1\r\n" + host(node.port()), null);
            checkEquals(404, response.status(), "status");
            checkEquals("error=chunk_not_found\n", response.text(), "body");
        }
    }

    private static void existsAndDelete(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            int port = node.port();
            for (String mode : List.of("follow", "nofollow")) {
                checkEquals(Map.of("exists", "false"), fields(exists(port, "file_0", mode)), "before, " + mode);
            }
            byte[] data = randomBytes(10, 11);
            store(port, "file_0", data, sha256(data));
            for (String mode : List.of("follow", "nofollow")) {
                checkEquals(Map.of("exists", "true"), fields(exists(port, "file_0", mode)), "after, " + mode);
            }
            checkEquals(Map.of("deleted", "true"), fields(delete(port, "file_0")), "first delete");
            checkEquals(Map.of("deleted", "false"), fields(delete(port, "file_0")), "second delete");
            checkEquals(List.of(), listNames(node.chunksDir()), "chunk folder");
        }
    }

    private static void junctionUnderIdIsUnsafe(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            Path outside = Files.createDirectory(tempDir.resolve("outside"));
            Files.write(outside.resolve("secret"), new byte[] {42});
            createDirectoryJunction(node.chunksDir().resolve("file_0"), outside);
            int port = node.port();
            try {
                checkError(raw(port, "GET /chunks/file_0 HTTP/1.1\r\n" + host(port), null), 500, "unsafe_entry", "read");
                checkError(exists(port, "file_0", "nofollow"), 500, "unsafe_entry", "nofollow exists");
                checkError(exists(port, "file_0", "follow"), 500, "unsafe_entry", "follow exists");
                checkError(delete(port, "file_0"), 500, "unsafe_entry", "delete");
                check(Files.exists(node.chunksDir().resolve("file_0"), LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
                checkEquals(List.of("secret"), listNames(outside), "the folder behind the junction is untouched");
            } finally {
                Files.deleteIfExists(node.chunksDir().resolve("file_0"));
            }
        }
    }

    private static void symlinkUnderIdIsUnsafe(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            Path target = Files.write(tempDir.resolve("target"), new byte[] {9, 9});
            createFileSymlinkOrSkip(node.chunksDir().resolve("file_0"), target);
            int port = node.port();
            checkError(raw(port, "GET /chunks/file_0 HTTP/1.1\r\n" + host(port), null), 500, "unsafe_entry", "read");
            checkError(exists(port, "file_0", "nofollow"), 500, "unsafe_entry", "nofollow exists");
            // Following links is what mode=follow means (StorageNode.hasChunk); it is never used to read or delete.
            checkEquals(Map.of("exists", "true"), fields(exists(port, "file_0", "follow")), "follow exists");
            checkError(delete(port, "file_0"), 500, "unsafe_entry", "delete");
            check(Files.isSymbolicLink(node.chunksDir().resolve("file_0")), "the link is left in place");
            check(Arrays.equals(new byte[] {9, 9}, Files.readAllBytes(target)), "the target is untouched");
        }
    }

    private static void deletedFolderIs503(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            int port = node.port();
            Files.delete(node.chunksDir());
            byte[] data = {1};
            checkError(store(port, "file_0", data, sha256(data)), 503, "storage_unavailable", "store");
            checkError(raw(port, "GET /chunks/file_0 HTTP/1.1\r\n" + host(port), null), 503, "storage_unavailable", "read");
            checkError(exists(port, "file_0", "follow"), 503, "storage_unavailable", "follow exists");
            checkError(exists(port, "file_0", "nofollow"), 503, "storage_unavailable", "nofollow exists");
            checkError(delete(port, "file_0"), 503, "storage_unavailable", "delete");
            checkError(raw(port, "GET /entries HTTP/1.1\r\n" + host(port), null), 503, "storage_unavailable", "entries");
            checkError(raw(port, "POST /sweep HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0", null), 503,
                    "storage_unavailable", "sweep");
            check(!Files.exists(node.chunksDir()), "the folder was not recreated");
        }
    }

    private static void entriesAreListed(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            Path chunks = node.chunksDir();
            Files.write(chunks.resolve("file_0"), new byte[] {1});
            Files.write(chunks.resolve("file_0-123.tmp"), new byte[] {2});
            Files.createDirectory(chunks.resolve("sub"));
            Files.write(chunks.resolve("odd name \u00e9"), new byte[] {3});
            Path outside = Files.createDirectory(tempDir.resolve("outside"));
            createDirectoryJunction(chunks.resolve("zz-junction"), outside);
            try {
                NodeTestKit.RawResponse response = raw(node.port(), "GET /entries HTTP/1.1\r\n" + host(node.port()), null);
                checkEquals(200, response.status(), "status");
                checkEquals("file file_0\nfile file_0-123.tmp\nfile odd%20name%20%C3%A9\ndirectory sub\nother zz-junction\n",
                        response.text(), "listing");
            } finally {
                Files.deleteIfExists(chunks.resolve("zz-junction"));
            }
        }
    }

    private static void sweepRemovesTempFiles(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            Path chunks = node.chunksDir();
            Files.write(chunks.resolve("file_0"), new byte[] {1});
            Files.write(chunks.resolve("file_0-123.tmp"), new byte[] {2});
            Files.write(chunks.resolve("file_1-4567.tmp"), new byte[] {3});
            Files.write(chunks.resolve("notes.txt"), new byte[] {4});
            NodeTestKit.RawResponse response = raw(node.port(), "POST /sweep HTTP/1.1\r\n" + host(node.port())
                    + "\r\nContent-Length: 0", null);
            checkEquals(200, response.status(), "status");
            checkEquals(Map.of("removed", "2", "warnings", "1"), fields(response), "fields");
            checkEquals(List.of("file_0", "notes.txt"), listNames(chunks), "what is left");
        }
    }

    private static void hostIsChecked(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            int port = node.port();
            byte[] data = {5};
            String tail = "\r\nContent-Length: 1\r\nX-Chunk-Sha256: " + sha256(data);
            List<String> heads = List.of(
                    "POST /chunks/file_0 HTTP/1.1\r\nHost: evil.example:" + port + tail,
                    "POST /chunks/file_0 HTTP/1.1\r\nHost: 127.0.0.1:" + (port == 65535 ? 1 : port + 1) + tail,
                    "POST /chunks/file_0 HTTP/1.1\r\nHost: 127.0.0.1" + tail,
                    "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\n" + host(port) + tail,
                    "POST /chunks/file_0 HTTP/1.1" + tail,
                    "POST http://127.0.0.1:" + port + "/chunks/file_0 HTTP/1.1\r\n" + host(port) + tail);
            for (String head : heads) {
                NodeTestKit.RawResponse response = raw(port, head, data);
                checkError(response, 403, "host_not_allowed", head.split("\r\n")[0] + " / " + head.split("\r\n")[1]);
            }
            checkEquals(List.of(), listNames(node.chunksDir()), "nothing was stored");
            checkEquals(200, raw(port, "GET /health HTTP/1.1\r\nHost: LocalHost:" + port, null).status(), "localhost is allowed");
        }
    }

    private static void routingErrors(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            int port = node.port();
            checkError(raw(port, "GET /files HTTP/1.1\r\n" + host(port), null), 404, "not_found", "unknown path");
            checkError(raw(port, "GET /chunks HTTP/1.1\r\n" + host(port), null), 404, "not_found", "/chunks without ID");
            NodeTestKit.RawResponse put = raw(port, "PUT /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0", null);
            checkError(put, 405, "method_not_allowed", "PUT");
            checkEquals("GET, POST, DELETE", put.header("Allow"), "Allow");
            checkError(raw(port, "HEAD /health HTTP/1.1\r\n" + host(port), null), 405, null, "HEAD /health");
            checkError(raw(port, "POST /health HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0", null), 405,
                    "method_not_allowed", "POST /health");
            checkError(raw(port, "GET /sweep HTTP/1.1\r\n" + host(port), null), 405, "method_not_allowed", "GET /sweep");
            checkError(raw(port, "GET /chunks/file_0/exists HTTP/1.1\r\n" + host(port), null), 400, "bad_request", "no mode");
            checkError(raw(port, "GET /chunks/file_0/exists?mode=other HTTP/1.1\r\n" + host(port), null), 400, "bad_request",
                    "unknown mode");
            checkError(raw(port, "GET /chunks/file_0?x=1 HTTP/1.1\r\n" + host(port), null), 400, "bad_request", "query on read");
            checkError(raw(port, "GET /health?x=1 HTTP/1.1\r\n" + host(port), null), 400, "bad_request", "query on health");
        }
    }

    private static void everyResponseCarriesNodeId(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            int port = node.port();
            byte[] data = {1};
            List<NodeTestKit.RawResponse> responses = List.of(
                    raw(port, "GET /health HTTP/1.1\r\n" + host(port), null),
                    store(port, "file_0", data, sha256(data)),
                    store(port, "file_0", data, sha256(data)),
                    raw(port, "GET /chunks/file_0 HTTP/1.1\r\n" + host(port), null),
                    raw(port, "GET /chunks/nothing HTTP/1.1\r\n" + host(port), null),
                    raw(port, "GET /chunks/BAD HTTP/1.1\r\n" + host(port), null),
                    raw(port, "GET /nowhere HTTP/1.1\r\n" + host(port), null),
                    raw(port, "GET /health HTTP/1.1\r\nHost: evil:" + port, null));
            for (NodeTestKit.RawResponse response : responses) {
                checkEquals(node.nodeId(), response.header("X-Node-Id"), "X-Node-Id of a " + response.status());
                checkEquals("close", response.header("Connection"), "Connection of a " + response.status());
            }
        }
    }

    private static void busyNodeAnswers503(Path tempDir) throws Exception {
        // A long stall timeout and a short lock wait: a store that stops sending keeps the lock.
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"), 1 << 20, 4_000, 200)) {
            int port = node.port();
            byte[] data = randomBytes(10_000, 12);
            try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                OutputStream out = socket.getOutputStream();
                out.write(storeHead(port, "file_0", data.length, sha256(data)));
                out.write(data, 0, 5_000);
                out.flush();
                waitFor(() -> listNames(node.chunksDir()).size() == 1, "the store's temp file to appear");
                checkError(exists(port, "file_1", "follow"), 503, "node_busy", "exists while busy");
                checkError(raw(port, "POST /sweep HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0", null), 503,
                        "node_busy", "sweep while busy");
                checkEquals(1, listNames(node.chunksDir()).size(), "the store's temp file was left alone");
                out.write(data, 5_000, 5_000);
                out.flush();
                NodeTestKit.RawResponse response = NodeTestKit.readResponse(socket.getInputStream());
                checkEquals(201, response.status(), "the store, finished after the others were refused");
            }
            checkEquals(Map.of("exists", "true"), fields(exists(port, "file_0", "follow")), "afterwards");
        }
    }

    private static void requestsAreLogged(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = NodeTestKit.startNode(tempDir.resolve("node"))) {
            int port = node.port();
            byte[] data = randomBytes(1234, 13);
            store(port, "file_0", data, sha256(data));
            raw(port, "GET /chunks/file_0 HTTP/1.1\r\n" + host(port), null);
            raw(port, "GET /chunks/..%5C..%5Cwindows HTTP/1.1\r\n" + host(port), null);
            raw(port, "GET /C:/Users/someone HTTP/1.1\r\n" + host(port), null);
            waitForLog(node, "GET (unknown path) 404");
            // The request lines only: the startup lines name the node's own folder on purpose.
            String all = node.out().text();
            String log = all.substring(all.indexOf('\n', all.indexOf(" listening on ")) + 1);
            check(log.matches("(?s).*POST /chunks/file_0 201 1234 bytes \\d+ ms\r?\n.*"), "store line missing:\n" + log);
            check(log.matches("(?s).*GET /chunks/file_0 200 1234 bytes \\d+ ms\r?\n.*"), "read line missing:\n" + log);
            check(log.matches("(?s).*GET /chunks/\\(invalid id\\) 400 \\d+ bytes \\d+ ms\r?\n.*"), "invalid ID line missing:\n" + log);
            check(!log.contains("windows") && !log.contains("Users") && !log.contains(tempDir.toString()),
                    "the log shows a client's path or a file path:\n" + log);
        }
    }

    private static void shortStreamIsRefused(Path tempDir) throws Exception {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        byte[] data = randomBytes(500, 14);
        // The bytes that do arrive have exactly the SHA-256 given, so only the length check can refuse them.
        NodeTestKit.expectThrows(java.io.EOFException.class, "short stream",
                () -> node.storeChunkFrom("file_0", new java.io.ByteArrayInputStream(data), 1000, sha256(data)));
        checkEquals(List.of(), listNames(tempDir.resolve("node")), "no chunk and no temp file");
    }

    private static void exactlyLengthIsRead(Path tempDir) throws Exception {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        byte[] data = randomBytes(1000, 15);
        java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(data);
        byte[] first = Arrays.copyOf(data, 600);
        checkEquals(600L, node.storeChunkFrom("file_0", in, 600, sha256(first)), "bytes stored");
        checkEquals(400, in.available(), "bytes left in the stream");
        check(Arrays.equals(first, Files.readAllBytes(tempDir.resolve("node").resolve("file_0"))), "stored bytes differ");
    }

    private static void existingIdReadsNothing(Path tempDir) throws Exception {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        byte[] original = {1, 2, 3};
        node.storeChunkFrom("file_0", new java.io.ByteArrayInputStream(original), 3, sha256(original));
        byte[] other = randomBytes(100, 16);
        java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(other);
        NodeTestKit.expectThrows(java.nio.file.FileAlreadyExistsException.class, "existing ID",
                () -> node.storeChunkFrom("file_0", in, 100, sha256(other)));
        checkEquals(100, in.available(), "bytes left in the stream");
        check(Arrays.equals(original, Files.readAllBytes(tempDir.resolve("node").resolve("file_0"))), "the chunk was changed");
    }

    private static void badArgumentsWriteNothing(Path tempDir) throws Exception {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        byte[] data = {4, 5, 6};
        String sha = sha256(data);
        for (String bad : new String[] {null, "", sha.toUpperCase(java.util.Locale.ROOT), sha.substring(1), sha + "0",
                sha.substring(0, 63) + "g"}) {
            NodeTestKit.expectThrows(IllegalArgumentException.class, "checksum " + bad,
                    () -> node.storeChunkFrom("file_0", new java.io.ByteArrayInputStream(data), 3, bad));
        }
        NodeTestKit.expectThrows(IllegalArgumentException.class, "negative length",
                () -> node.storeChunkFrom("file_0", new java.io.ByteArrayInputStream(data), -1, sha));
        NodeTestKit.expectThrows(IllegalArgumentException.class, "invalid ID",
                () -> node.storeChunkFrom("FILE", new java.io.ByteArrayInputStream(data), 3, sha));
        checkEquals(List.of(), listNames(tempDir.resolve("node")), "nothing was written");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static byte[] storeHead(int port, String id, long length, String sha) {
        return ("POST /chunks/" + id + " HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: " + length
                + "\r\nX-Chunk-Sha256: " + sha + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static NodeTestKit.RawResponse store(int port, String id, byte[] data, String sha) throws IOException {
        return raw(port, "POST /chunks/" + id + " HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: " + data.length
                + "\r\nX-Chunk-Sha256: " + sha, data);
    }

    private static NodeTestKit.RawResponse exists(int port, String id, String mode) throws IOException {
        return raw(port, "GET /chunks/" + id + "/exists?mode=" + mode + " HTTP/1.1\r\n" + host(port), null);
    }

    private static NodeTestKit.RawResponse delete(int port, String id) throws IOException {
        return raw(port, "DELETE /chunks/" + id + " HTTP/1.1\r\n" + host(port), null);
    }

    private static Map<String, String> fields(NodeTestKit.RawResponse response) {
        Map<String, String> fields = NodeProtocol.parseFields(response.text());
        check(fields != null, "the body is not key=value lines: \"" + response.text() + "\"");
        return fields;
    }

    /** The status, and (if given) exactly the body {@code error=<code>}. */
    private static void checkError(NodeTestKit.RawResponse response, int status, String code, String what) {
        checkEquals(status, response.status(), what + ": status");
        if (code != null) {
            checkEquals("error=" + code + "\n", response.text(), what + ": body");
        }
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    private static void waitFor(Condition condition, String what) throws Exception {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!condition.holds()) {
            check(System.nanoTime() < deadline, "timed out waiting for " + what);
            Thread.sleep(20);
        }
    }

    /** Waits until the node's request log contains {@code part}. */
    private static void waitForLog(NodeTestKit.NodeHandle node, String part) throws Exception {
        waitFor(() -> node.out().text().contains(part), "the log line \"" + part + "\" in:\n" + node.out().text());
    }
}
