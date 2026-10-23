package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Plain-Java tests for {@code POST /files} (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.UploadEndpointTest [--stress]}
 *
 * <p>Every test starts its own server in this process, on 127.0.0.1 with a port chosen by the
 * operating system and a fresh data directory, and stops it afterwards. Requests go through
 * java.net.http.HttpClient, or through a raw socket for what HttpClient will not do (a chunked
 * body written by hand, a body cut off halfway, duplicated headers). Raw responses are read by
 * their Content-Length, never by waiting for the server to close the connection. Every network
 * wait has a time limit, so a hang fails a test instead of blocking the run.
 *
 * <p>{@code --stress} runs only the stress tests: an upload of the full 100 MiB limit and four
 * 20 MiB uploads at once. Run it with a small heap ({@code java -Xmx16m ... --stress}) to show
 * that uploads are streamed, never held in memory.
 * The program exits with code 1 if any test fails.
 */
public class UploadEndpointTest {

    private static final int TIMEOUT_MILLIS = 10_000;
    private static final int MIB = 1024 * 1024;
    private static final InetAddress LOOPBACK = loopback();
    private static final String OCTET = "Content-Type: application/octet-stream\r\n";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stress = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stress) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.UploadEndpointTest [--stress]");
            System.exit(2);
            return;
        }

        if (stress) {
            runTest("STRESS: an upload of exactly 100 MiB is stored and reads back identically",
                    UploadEndpointTest::stressFullSizeUpload);
            runTest("STRESS: four 20 MiB uploads at once", UploadEndpointTest::stressConcurrentUploads);
            finish();
            return;
        }

        // Successful uploads
        runTest("an upload is stored and answered with 201, Location and the manifest as JSON",
                UploadEndpointTest::uploadIsStored);
        runTest("an empty upload is stored with no chunks", UploadEndpointTest::emptyUpload);
        runTest("1 MiB, 1 MiB + 1 byte and 3 MiB give 1, 2 and 3 chunks", UploadEndpointTest::chunkBoundaries);
        runTest("a chunked request body is accepted", UploadEndpointTest::chunkedBody);
        runTest("Expect: 100-continue is answered and the upload succeeds", UploadEndpointTest::expectContinue);

        // Request checks
        runTest("a matching X-Content-SHA256 is accepted in either letter case", UploadEndpointTest::checksumAccepted);
        runTest("a checksum mismatch is 400 checksum_mismatch and stores nothing",
                UploadEndpointTest::checksumMismatch);
        runTest("a malformed or repeated X-Content-SHA256 is 400 invalid_checksum and stores nothing",
                UploadEndpointTest::invalidChecksumHeaders);
        runTest("only exactly one application/octet-stream Content-Type is accepted", UploadEndpointTest::contentTypes);
        runTest("an upload needing more chunks than a manifest may list is 413 and stores nothing",
                UploadEndpointTest::tooManyChunksIs413);
        runTest("the size limit is enforced from Content-Length and while reading a chunked body",
                UploadEndpointTest::sizeLimit);
        runTest("a Content-Length over 100 MiB is refused after at most 64 KiB of the body is read",
                UploadEndpointTest::contentLengthRefusedEarly);

        // Disconnects
        runTest("a fixed-length body cut off halfway gets no response and leaves nothing behind",
                UploadEndpointTest::truncatedFixedLengthBody);
        runTest("a chunked body cut off halfway gets no response and leaves nothing behind",
                UploadEndpointTest::truncatedChunkedBody);
        runTest("a connection reset during the upload leaves nothing behind", UploadEndpointTest::resetDuringUpload);

        // Routing and Host
        runTest("other methods on /files are 405 with Allow: POST; POST under /files/ is 405 with Allow: GET",
                UploadEndpointTest::routing);
        runTest("a bad Host is 403 before the upload is looked at", UploadEndpointTest::badHostIsRefused);

        // Failures
        runTest("storage failures are 500 storage_error with a safe body, and the workspace is deleted",
                UploadEndpointTest::storageFailures);
        runTest("an unexpected failure while storing is 500 internal_error, and the workspace is deleted",
                UploadEndpointTest::unexpectedFailure);

        // Concurrency and restart
        runTest("concurrent uploads all succeed with distinct IDs", UploadEndpointTest::concurrentUploads);
        runTest("uploaded files are still there after a restart", UploadEndpointTest::restartKeepsFiles);
        runTest("an upload whose rollback failed keeps its marker, and the next start recovers it",
                UploadEndpointTest::failedRollbackIsRecoveredOnRestart);

        // Settings
        runTest("--max-upload-mb is parsed and validated", UploadEndpointTest::maxUploadArgument);
        runTest("the default settings are a 100 MiB limit and 1 MiB chunks", UploadEndpointTest::defaultSettings);
        finish();
    }

    /** Prints the totals and exits with code 1 if any test failed. */
    private static void finish() {
        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Successful uploads
    // ---------------------------------------------------------------------

    private static void uploadIsStored(Path tempDir) throws Exception {
        withServer(tempDir, 1024 * 1024, 4, server -> {
            byte[] data = bytes(10, 1);
            HttpResponse<String> response = upload(server, data, null);

            checkEquals(201, response.statusCode(), "status");
            String location = response.headers().firstValue("Location").orElseThrow();
            check(location.startsWith("/files/"), "Location: " + location);
            String fileId = location.substring("/files/".length());
            check(FileIds.isValid(fileId), "the Location ends with a valid file ID: " + location);
            FileManifest manifest = server.manifests().read(fileId).orElseThrow();
            checkEquals(new FileManifest(fileId, 10, sha256(data), 4, 3, chunkSha256s(data, 4)), manifest, "the stored manifest");
            checkEquals("{\"fileId\":\"" + fileId + "\",\"size\":10,\"sha256\":\"" + sha256(data)
                    + "\",\"chunkSize\":4,\"chunkCount\":3}", response.body(), "the JSON body");
            checkEquals("application/json; charset=utf-8", response.headers().firstValue("Content-Type").orElse(null),
                    "Content-Type");
            checkEquals("nosniff", response.headers().firstValue("X-Content-Type-Options").orElse(null), "nosniff");
            check(Arrays.equals(data, readBack(server, manifest, tempDir)), "the file reads back identically");
            checkEquals(List.of(), server.manifests().listPendingFileIds(), "pending markers");
            checkTmpIsEmpty(server);
        });
    }

    private static void emptyUpload(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            HttpResponse<String> response = upload(server, new byte[0], null);
            checkEquals(201, response.statusCode(), "status");
            FileManifest manifest = manifestFrom(server, response);
            checkEquals(0L, manifest.size(), "size");
            checkEquals(0, manifest.chunkCount(), "chunk count");
            checkEquals(sha256(new byte[0]), manifest.sha256(), "sha256");
            checkTmpIsEmpty(server);
        });
    }

    private static void chunkBoundaries(Path tempDir) throws Exception {
        withServer(tempDir, 100L * MIB, FileChunker.DEFAULT_CHUNK_SIZE, server -> {
            int[][] sizesAndCounts = {{MIB, 1}, {MIB + 1, 2}, {3 * MIB, 3}};
            for (int[] sizeAndCount : sizesAndCounts) {
                byte[] data = bytes(sizeAndCount[0], sizeAndCount[1]);
                HttpResponse<String> response = upload(server, data, null);
                checkEquals(201, response.statusCode(), sizeAndCount[0] + " bytes: status");
                FileManifest manifest = manifestFrom(server, response);
                checkEquals(sizeAndCount[1], manifest.chunkCount(), sizeAndCount[0] + " bytes: chunk count");
                check(Arrays.equals(data, readBack(server, manifest, tempDir)), sizeAndCount[0] + " bytes: read back");
            }
            checkTmpIsEmpty(server);
        });
    }

    private static void chunkedBody(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            byte[] part1 = bytes(7, 1);
            byte[] part2 = bytes(300, 2);
            RawResponse response = exchange(server,
                    head("POST", "/files", server, OCTET + "Transfer-Encoding: chunked\r\n"), chunked(part1, part2));

            checkEquals(201, response.status(), "status");
            String fileId = response.header("location").substring("/files/".length());
            FileManifest manifest = server.manifests().read(fileId).orElseThrow();
            byte[] all = concat(part1, part2);
            checkEquals(new FileManifest(fileId, all.length, sha256(all), 4, (all.length + 3) / 4, chunkSha256s(all, 4)), manifest,
                    "the manifest");
            check(Arrays.equals(all, readBack(server, manifest, tempDir)), "the file reads back identically");
            checkTmpIsEmpty(server);
        });
    }

    private static void expectContinue(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            byte[] data = bytes(100, 3);
            try (Socket socket = connect(server)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                out.write(head("POST", "/files", server, OCTET + "Content-Length: 100\r\nExpect: 100-continue\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
                checkEquals(100, readOneResponse(in).status(), "the interim response");
                out.write(data);
                out.flush();
                RawResponse response = readOneResponse(in);
                checkEquals(201, response.status(), "the final response");
                FileManifest manifest = server.manifests().read(response.header("location").substring(7)).orElseThrow();
                check(Arrays.equals(data, readBack(server, manifest, tempDir)), "the file reads back identically");
            }
            checkTmpIsEmpty(server);
        });
    }

    // ---------------------------------------------------------------------
    // Request checks
    // ---------------------------------------------------------------------

    private static void checksumAccepted(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            byte[] data = bytes(20, 4);
            String sha = sha256(data);
            for (String header : List.of(sha, sha.toUpperCase(Locale.ROOT))) {
                HttpResponse<String> response = upload(server, data, header);
                checkEquals(201, response.statusCode(), "status for " + header);
                checkEquals(sha, manifestFrom(server, response).sha256(), "the stored checksum is lowercase");
                checkContains(response.body(), "\"sha256\":\"" + sha + "\"");
            }
        });
    }

    private static void checksumMismatch(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            RawResponse response = exchange(server,
                    head("POST", "/files", server, OCTET + "Content-Length: 20\r\nX-Content-SHA256: "
                            + sha256(bytes(20, 5)) + "\r\n"), bytes(20, 6));
            checkErrorResponse(response, ApiError.CHECKSUM_MISMATCH, server, "mismatch");
            checkNothingStored(server);
        });
    }

    private static void invalidChecksumHeaders(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            String valid = sha256(bytes(20, 7));
            List<String> headerLines = List.of(
                    "X-Content-SHA256: " + valid.substring(1) + "\r\n",
                    "X-Content-SHA256: " + valid + "0\r\n",
                    "X-Content-SHA256: " + "g".repeat(64) + "\r\n",
                    "X-Content-SHA256: " + valid.substring(2) + "-+\r\n",
                    "X-Content-SHA256: \r\n",
                    "X-Content-SHA256: " + valid + "\r\nX-Content-SHA256: " + valid + "\r\n");
            for (String lines : headerLines) {
                RawResponse response = exchange(server,
                        head("POST", "/files", server, OCTET + "Content-Length: 20\r\n" + lines), bytes(20, 7));
                checkErrorResponse(response, ApiError.INVALID_CHECKSUM, server, lines.trim());
            }
            checkNothingStored(server);
        });
    }

    private static void contentTypes(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            List<String> refused = List.of(
                    "",
                    "Content-Type: text/plain\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Type: application/octet-streamx\r\n",
                    "Content-Type: application/octet-stream, text/plain\r\n",
                    "Content-Type: application/" + (char) 0x00F6 + "ctet-stream\r\n",
                    "Content-Type: application/octet-stream\r\nContent-Type: application/octet-stream\r\n");
            for (String lines : refused) {
                RawResponse response = exchange(server,
                        head("POST", "/files", server, lines + "Content-Length: 5\r\n"), bytes(5, 1));
                checkErrorResponse(response, ApiError.UNSUPPORTED_MEDIA_TYPE, server, "\"" + lines.trim() + "\"");
            }
            checkNothingStored(server);

            List<String> accepted = List.of(
                    "Content-Type: application/octet-stream\r\n",
                    "Content-Type: Application/Octet-Stream\r\n",
                    "Content-Type: application/octet-stream; name=x\r\n");
            for (String lines : accepted) {
                RawResponse response = exchange(server,
                        head("POST", "/files", server, lines + "Content-Length: 5\r\n"), bytes(5, 1));
                checkEquals(201, response.status(), "status for \"" + lines.trim() + "\"");
            }
        });
    }

    /** Only reachable with a tiny chunk size: 1-byte chunks and one byte more than the maximum chunk count. */
    private static void tooManyChunksIs413(Path tempDir) throws Exception {
        withServer(tempDir, 1024 * 1024, 1, server -> {
            RawResponse response = post(server, bytes(FileManifest.MAX_CHECKSUMMED_CHUNKS + 1, 1));
            checkErrorResponse(response, ApiError.PAYLOAD_TOO_LARGE, server, "too many chunks");
            checkNothingStored(server);
        });
    }

    private static void sizeLimit(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 512, server -> {
            RawResponse atLimit = exchange(server,
                    head("POST", "/files", server, OCTET + "Content-Length: 1024\r\n"), bytes(1024, 1));
            checkEquals(201, atLimit.status(), "exactly at the limit");
            checkEquals(1, listNames(server.dataRoot().resolve("manifests")).size(), "one manifest so far");

            RawResponse declaredOver = exchange(server,
                    head("POST", "/files", server, OCTET + "Content-Length: 1025\r\n"), bytes(1025, 2));
            checkErrorResponse(declaredOver, ApiError.PAYLOAD_TOO_LARGE, server, "Content-Length over the limit");

            RawResponse chunkedOver = exchange(server,
                    head("POST", "/files", server, OCTET + "Transfer-Encoding: chunked\r\n"),
                    chunked(bytes(600, 3), bytes(425, 4)));
            checkErrorResponse(chunkedOver, ApiError.PAYLOAD_TOO_LARGE, server, "a chunked body over the limit");

            RawResponse chunkedAtLimit = exchange(server,
                    head("POST", "/files", server, OCTET + "Transfer-Encoding: chunked\r\n"),
                    chunked(bytes(600, 3), bytes(424, 4)));
            checkEquals(201, chunkedAtLimit.status(), "a chunked body exactly at the limit");

            checkEquals(2, listNames(server.dataRoot().resolve("manifests")).size(), "manifests");
            checkTmpIsEmpty(server);
        });
    }

    private static void contentLengthRefusedEarly(Path tempDir) throws Exception {
        withServer(tempDir, 100L * MIB, FileChunker.DEFAULT_CHUNK_SIZE, server -> {
            // The headers and the first 64 KiB of the body are sent, then nothing more: the answer
            // must come without waiting for the remaining 100 MiB. (The server reads and discards
            // at most 64 KiB before answering, so a client that waits forever on its own headers
            // never gets stuck; StalledRequestTest covers a client that sends no body at all.)
            try (Socket socket = connect(server)) {
                OutputStream out = socket.getOutputStream();
                out.write(head("POST", "/files", server, OCTET + "Content-Length: "
                        + (100L * MIB + 1) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                out.write(bytes(64 * 1024, 1));
                out.flush();
                RawResponse response = readOneResponse(socket.getInputStream());
                checkErrorResponse(response, ApiError.PAYLOAD_TOO_LARGE, server, "64 KiB of a 100 MiB + 1 body");
            }
            // The same with Expect: 100-continue, as curl sends for large bodies: the server says
            // "continue" first, then refuses after the first 64 KiB.
            try (Socket socket = connect(server)) {
                OutputStream out = socket.getOutputStream();
                out.write(head("POST", "/files", server, OCTET + "Expect: 100-continue\r\nContent-Length: "
                        + (100L * MIB + 1) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
                RawResponse interim = readOneResponse(socket.getInputStream());
                checkEquals(100, interim.status(), "the interim response to Expect: 100-continue");
                out.write(bytes(64 * 1024, 2));
                out.flush();
                RawResponse response = readOneResponse(socket.getInputStream());
                checkErrorResponse(response, ApiError.PAYLOAD_TOO_LARGE, server, "with Expect: 100-continue");
            }
            checkNothingStored(server);
        });
    }

    // ---------------------------------------------------------------------
    // Disconnects
    // ---------------------------------------------------------------------

    private static void truncatedFixedLengthBody(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 10L * MIB, 4096, errBytes, server -> {
            try (Socket socket = connect(server)) {
                OutputStream out = socket.getOutputStream();
                out.write(head("POST", "/files", server, OCTET + "Content-Length: 1000000\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                out.write(bytes(200_000, 1)); // more than the read buffer, so part of it reaches the workspace
                out.flush();
                socket.shutdownOutput();
                checkNoResponse(socket);
            }
            checkAbandoned(server, errBytes);
        });
    }

    private static void truncatedChunkedBody(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 10L * MIB, 4096, errBytes, server -> {
            try (Socket socket = connect(server)) {
                OutputStream out = socket.getOutputStream();
                out.write(head("POST", "/files", server, OCTET + "Transfer-Encoding: chunked\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                out.write("30000\r\n".getBytes(StandardCharsets.ISO_8859_1)); // announces 196,608 bytes
                out.write(bytes(100_000, 2));
                out.flush();
                socket.shutdownOutput();
                checkNoResponse(socket);
            }
            checkAbandoned(server, errBytes);
        });
    }

    private static void resetDuringUpload(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 10L * MIB, 4096, errBytes, server -> {
            Socket socket = connect(server);
            OutputStream out = socket.getOutputStream();
            out.write(head("POST", "/files", server, OCTET + "Content-Length: 1000000\r\n")
                    .getBytes(StandardCharsets.ISO_8859_1));
            out.write(bytes(100_000, 3));
            out.flush();
            // Reset only once the handler is copying the body; an earlier reset never reaches it.
            waitUntil(() -> !listNames(server.dataRoot().resolve("tmp")).isEmpty(), "the upload's workspace exists");
            socket.setSoLinger(true, 0); // close with a reset instead of a normal end
            socket.close();
            checkAbandoned(server, errBytes);
        });
    }

    /** Nothing stored, the workspace deleted, the abandonment logged, and the server still serving. */
    private static void checkAbandoned(DistributedFsServer server, ByteArrayOutputStream errBytes) throws Exception {
        waitUntil(() -> errBytes.toString(StandardCharsets.UTF_8).contains("Upload abandoned"), "the abandonment is logged");
        waitUntil(() -> listNames(server.dataRoot().resolve("tmp")).isEmpty(), "the workspace is deleted");
        checkNothingStored(server);
        HttpResponse<String> response = upload(server, bytes(5, 9), null);
        checkEquals(201, response.statusCode(), "the next upload still works");
    }

    /** The server ends the connection without sending a single byte. */
    private static void checkNoResponse(Socket socket) throws IOException {
        byte[] received;
        try {
            received = socket.getInputStream().readAllBytes();
        } catch (SocketException e) {
            return; // reset: the server closed without answering
        }
        checkEquals("", new String(received, StandardCharsets.ISO_8859_1), "bytes received");
    }

    // ---------------------------------------------------------------------
    // Routing and Host
    // ---------------------------------------------------------------------

    private static void routing(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            for (String method : List.of("GET", "PUT", "DELETE", "PATCH", "OPTIONS")) {
                RawResponse response = exchange(server, head(method, "/files", server, "Content-Length: 0\r\n"),
                        new byte[0]);
                checkErrorResponse(response, ApiError.METHOD_NOT_ALLOWED, server, method + " /files");
                checkEquals("POST", response.header("allow"), method + " /files: Allow");
            }
            try (Socket socket = connect(server)) {
                socket.getOutputStream().write(head("HEAD", "/files", server, "").getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                byte[] all = socket.getInputStream().readAllBytes();
                String text = new String(all, StandardCharsets.ISO_8859_1);
                check(text.startsWith("HTTP/1.1 405 "), "HEAD /files: " + text);
                checkContains(text.toLowerCase(Locale.ROOT), "\r\nallow: post\r\n");
                check(text.endsWith("\r\n\r\n"), "HEAD /files has no body: " + text);
            }
            // A path under /files/ names a file, which only GET reads: POST there is never an upload.
            for (String path : List.of("/files/", "/files/0123456789abcdef0123456789abcdef", "/files/..")) {
                RawResponse response = exchange(server, head("POST", path, server, OCTET + "Content-Length: 3\r\n"),
                        bytes(3, 1));
                checkErrorResponse(response, ApiError.METHOD_NOT_ALLOWED, server, "POST " + path);
                checkEquals("GET", response.header("allow"), "POST " + path + ": Allow");
            }
            for (String path : List.of("/FILES", "/files%2F", "/%66iles")) {
                RawResponse response = exchange(server, head("POST", path, server, OCTET + "Content-Length: 3\r\n"),
                        bytes(3, 1));
                checkErrorResponse(response, ApiError.NOT_FOUND, server, "POST " + path);
            }
            checkNothingStored(server);
        });
    }

    private static void badHostIsRefused(Path tempDir) throws Exception {
        withServer(tempDir, 1024, 4, server -> {
            for (String host : List.of("Host: evil.example:" + server.port() + "\r\n", "Host: 127.0.0.1\r\n", "")) {
                String request = "POST /files HTTP/1.1\r\n" + host + "Connection: close\r\n" + OCTET
                        + "Content-Length: 3\r\n\r\n";
                RawResponse response = exchange(server, request, bytes(3, 1));
                checkErrorResponse(response, ApiError.HOST_NOT_ALLOWED, server, "\"" + host.trim() + "\"");
            }
            checkNothingStored(server);
        });
    }

    // ---------------------------------------------------------------------
    // Failures
    // ---------------------------------------------------------------------

    private static void storageFailures(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        ScriptedFiles files = new ScriptedFiles(tempDir.resolve("scripted"), err);
        String secret = "C:\\secret\\path";
        DistributedFsServer server = DistributedFsServer.start(config(tempDir.resolve("data"), 1024, 4, err),
                (port, errStream, realFiles) -> new FileApiHandler(port, errStream, files));
        try {
            files.storeFailure = new FileServiceException(FileServiceException.Reason.STORAGE_ERROR,
                    "simulated storage failure at " + secret, new IOException("disk full at " + secret));
            checkErrorResponse(post(server, bytes(5, 1)), ApiError.STORAGE_ERROR, server, "a storage failure");
            checkContains(errBytes.toString(StandardCharsets.UTF_8), "simulated storage failure");

            files.storeFailure = new IOException("simulated chunking failure at " + secret);
            checkErrorResponse(post(server, bytes(5, 1)), ApiError.STORAGE_ERROR, server, "a workspace failure");
            checkContains(errBytes.toString(StandardCharsets.UTF_8), "simulated chunking failure");

            files.storeFailure = null;
            files.workspaceFailure = new IOException("simulated: cannot create a workspace at " + secret);
            checkErrorResponse(post(server, bytes(5, 1)), ApiError.STORAGE_ERROR, server, "no workspace");
            checkContains(errBytes.toString(StandardCharsets.UTF_8), "cannot create a workspace");

            files.workspaceFailure = null;
            files.bodyBlocked = true;
            checkErrorResponse(post(server, bytes(5, 1)), ApiError.STORAGE_ERROR, server, "a spool write failure");
            checkContains(errBytes.toString(StandardCharsets.UTF_8), "could not write the upload to its workspace");

            // The response is sent before the handler's finally block deletes the workspace.
            waitUntil(() -> listNames(files.tmp).isEmpty(), "every workspace is deleted");
            check(!errBytes.toString(StandardCharsets.UTF_8).contains("could not delete"), "no workspace delete failed");
        } finally {
            server.stop();
        }
    }

    private static void unexpectedFailure(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        ScriptedFiles files = new ScriptedFiles(tempDir.resolve("scripted"), err);
        DistributedFsServer server = DistributedFsServer.start(config(tempDir.resolve("data"), 1024, 4, err),
                (port, errStream, realFiles) -> new FileApiHandler(port, errStream, files));
        try {
            files.storeFailure = new IllegalStateException("simulated bug");
            checkErrorResponse(post(server, bytes(5, 1)), ApiError.INTERNAL_ERROR, server, "a runtime failure");
            checkContains(errBytes.toString(StandardCharsets.UTF_8), "simulated bug");
            checkEquals(List.of(), listNames(files.tmp), "the workspace was deleted");
            checkEquals(200, exchange(server, "GET /health HTTP/1.1\r\nHost: 127.0.0.1:" + server.port()
                    + "\r\nConnection: close\r\n\r\n", new byte[0]).status(), "the server keeps serving");
        } finally {
            server.stop();
        }
    }

    /** A FileService (on its own small storage) whose workspace creation or store can be made to fail. */
    private static final class ScriptedFiles extends FileService {
        final Path tmp;
        volatile IOException workspaceFailure;
        volatile Exception storeFailure;
        /** When set, each new workspace already has a folder named "body", so spooling cannot write it. */
        volatile boolean bodyBlocked;

        ScriptedFiles(Path dir, PrintStream err) throws IOException {
            super(clusterIn(dir), new ManifestStore(Files.createDirectories(dir.resolve("manifests"))),
                    Files.createDirectories(dir.resolve("tmp")), 4, 1024, err);
            this.tmp = dir.resolve("tmp");
        }

        private static StorageCluster clusterIn(Path dir) throws IOException {
            List<StorageNode> nodes = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                nodes.add(new StorageNode(Files.createDirectories(dir.resolve("node-" + i))));
            }
            return new StorageCluster(nodes);
        }

        @Override
        Path newUploadWorkspace() throws IOException {
            IOException failure = workspaceFailure;
            if (failure != null) {
                throw failure;
            }
            Path workspace = super.newUploadWorkspace();
            if (bodyBlocked) {
                Files.createDirectory(workspace.resolve("body"));
            }
            return workspace;
        }

        @Override
        FileManifest store(Path body, Path workspace, String expectedSha256) throws IOException {
            Exception failure = storeFailure;
            if (failure instanceof IOException ioFailure) {
                throw ioFailure;
            }
            if (failure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            return super.store(body, workspace, expectedSha256);
        }
    }

    // ---------------------------------------------------------------------
    // Concurrency and restart
    // ---------------------------------------------------------------------

    private static void concurrentUploads(Path tempDir) throws Exception {
        withServer(tempDir, 10L * MIB, 64 * 1024, server -> {
            List<byte[]> inputs = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                inputs.add(bytes(300_000 + i, i));
            }
            List<HttpResponse<String>> responses = uploadConcurrently(server, inputs);
            Set<String> fileIds = new HashSet<>();
            for (int i = 0; i < inputs.size(); i++) {
                checkEquals(201, responses.get(i).statusCode(), "upload " + i + ": status");
                FileManifest manifest = manifestFrom(server, responses.get(i));
                fileIds.add(manifest.fileId());
                check(Arrays.equals(inputs.get(i), readBack(server, manifest, tempDir)), "upload " + i + " reads back");
            }
            checkEquals(inputs.size(), fileIds.size(), "distinct file IDs");
            checkEquals(List.of(), server.manifests().listPendingFileIds(), "pending markers");
            checkTmpIsEmpty(server);
        });
    }

    private static void restartKeepsFiles(Path tempDir) throws Exception {
        Path dataDir = tempDir.resolve("data");
        byte[] data = bytes(10, 5);
        String fileId;
        DistributedFsServer first = DistributedFsServer.start(config(dataDir, 1024, 4, nullStream()));
        try {
            fileId = manifestFrom(first, upload(first, data, null)).fileId();
        } finally {
            first.stop();
        }
        DistributedFsServer second = DistributedFsServer.start(config(dataDir, 1024, 4, nullStream()));
        try {
            FileManifest manifest = second.manifests().read(fileId).orElseThrow();
            check(Arrays.equals(data, readBack(second, manifest, tempDir)), "the file reads back after the restart");
            checkEquals(201, upload(second, bytes(3, 1), null).statusCode(), "uploads still work");
        } finally {
            second.stop();
        }
    }

    private static void failedRollbackIsRecoveredOnRestart(Path tempDir) throws Exception {
        Path dataDir = tempDir.resolve("data");
        DistributedFsServer.start(config(dataDir, 1024, 4, nullStream())).stop(); // initializes the folder

        // An upload on that data directory whose manifest cannot be written and whose first chunk
        // cannot be deleted again: the rollback is incomplete.
        String fileId = "0123456789abcdef0123456789abcdef";
        try (DataDirectory dataDirectory = DataDirectory.open(dataDir)) {
            List<StorageNode> nodes = new ArrayList<>();
            for (Path nodeDir : dataDirectory.nodeDirs()) {
                nodes.add(new StorageNode(nodeDir) {
                    @Override
                    public boolean deleteChunk(String chunkId) throws IOException {
                        if (chunkId.endsWith("_0")) {
                            throw new IOException("simulated delete failure");
                        }
                        return super.deleteChunk(chunkId);
                    }
                });
            }
            ManifestStore manifests = new ManifestStore(dataDirectory.manifestsDir()) {
                @Override
                void create(FileManifest manifest) throws IOException {
                    throw new IOException("simulated create failure");
                }
            };
            FileService files = new FileService(new StorageCluster(nodes), manifests, dataDirectory.tmpDir(), 4, 1024,
                    () -> fileId, nullStream());
            Path workspace = files.newUploadWorkspace();
            try {
                Path body = Files.write(workspace.resolve("body"), bytes(10, 1));
                FileServiceException e = expectThrows(FileServiceException.class, () -> files.store(body, workspace, null));
                checkEquals(1, e.getSuppressed().length, "the delete failure is attached");
            } finally {
                files.deleteWorkspace(workspace);
            }
            checkEquals(List.of(fileId), manifests.listPendingFileIds(), "the marker was kept");
        }

        check(Files.exists(dataDir.resolve("nodes").resolve("node-0").resolve(fileId + "_0")),
                "the chunk that could not be deleted is still there");

        // The next start recovers the upload: the leftover chunk and the marker are removed.
        DistributedFsServer server = DistributedFsServer.start(config(dataDir, 1024, 4, nullStream()));
        try {
            checkNothingStored(server);
            HttpResponse<String> response = upload(server, bytes(5, 2), null);
            checkEquals(201, response.statusCode(), "the next upload works");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Settings
    // ---------------------------------------------------------------------

    private static void maxUploadArgument(Path tempDir) throws Exception {
        checkEquals(100, DistributedFsServer.parseArgs(new String[] {"--data-dir", "d"}).maxUploadMb(), "default");
        checkEquals(1, DistributedFsServer.parseArgs(new String[] {"--data-dir", "d", "--max-upload-mb", "1"})
                .maxUploadMb(), "1");
        checkEquals(1024, DistributedFsServer.parseArgs(new String[] {"--max-upload-mb", "1024", "--data-dir", "d"})
                .maxUploadMb(), "1024");
        for (String value : List.of("0", "1025", "01", "-1", "+5", "abc", "", "99999", "1.5", " 5")) {
            expectThrows(DistributedFsServer.UsageException.class, () -> DistributedFsServer.parseArgs(
                    new String[] {"--data-dir", "d", "--max-upload-mb", value}));
        }
        expectThrows(DistributedFsServer.UsageException.class, () -> DistributedFsServer.parseArgs(
                new String[] {"--data-dir", "d", "--max-upload-mb", "5", "--max-upload-mb", "5"}));
        expectThrows(DistributedFsServer.UsageException.class, () -> DistributedFsServer.parseArgs(
                new String[] {"--data-dir", "d", "--max-upload-mb"}));
        checkContains(DistributedFsServer.USAGE, "[--max-upload-mb <1-1024>]");
    }

    private static void defaultSettings(Path tempDir) {
        DistributedFsServer.Config defaults = DistributedFsServer.Config.defaults(tempDir, 0, 7L * MIB, nullStream(),
                nullStream());
        checkEquals(7L * MIB, defaults.maxUploadBytes(), "defaults: the given limit");
        checkEquals(FileChunker.DEFAULT_CHUNK_SIZE, defaults.chunkSize(), "defaults: chunk size");
        DistributedFsServer.Config shortForm = new DistributedFsServer.Config(tempDir, 0, 1, 1, 0, 0, nullStream(),
                nullStream());
        checkEquals(100L * MIB, shortForm.maxUploadBytes(), "the short constructor's limit");
        checkEquals(MIB, shortForm.chunkSize(), "the short constructor's chunk size");
        expectThrows(IllegalArgumentException.class, () -> new DistributedFsServer.Config(tempDir, 0, 1, 1, 0, 0,
                nullStream(), nullStream(), 0, 1));
        expectThrows(IllegalArgumentException.class, () -> new DistributedFsServer.Config(tempDir, 0, 1, 1, 0, 0,
                nullStream(), nullStream(), 1, 0));
    }

    // ---------------------------------------------------------------------
    // Stress
    // ---------------------------------------------------------------------

    private static void stressFullSizeUpload(Path tempDir) throws Exception {
        withServer(tempDir, 100L * MIB, FileChunker.DEFAULT_CHUNK_SIZE, server -> {
            Path file = tempDir.resolve("big");
            String expectedSha = writeLargeFile(file, 100L * MIB, 1);
            HttpResponse<String> response;
            try (HttpClient client = newClient()) {
                response = client.send(request(server, "/files")
                        .header("Content-Type", "application/octet-stream")
                        .header("X-Content-SHA256", expectedSha)
                        .timeout(Duration.ofMinutes(5))
                        .POST(HttpRequest.BodyPublishers.ofFile(file)).build(), HttpResponse.BodyHandlers.ofString());
            }
            checkEquals(201, response.statusCode(), "status");
            FileManifest manifest = manifestFrom(server, response);
            checkEquals(100L * MIB, manifest.size(), "size");
            checkEquals(100, manifest.chunkCount(), "chunk count");
            checkEquals(expectedSha, readBackSha256(server, manifest, tempDir), "the stored chunks' SHA-256");
            Files.delete(file);
            checkTmpIsEmpty(server);
        });
    }

    private static void stressConcurrentUploads(Path tempDir) throws Exception {
        withServer(tempDir, 100L * MIB, FileChunker.DEFAULT_CHUNK_SIZE, server -> {
            List<Path> files = new ArrayList<>();
            List<String> shas = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Path file = tempDir.resolve("big-" + i);
                shas.add(writeLargeFile(file, 20L * MIB, i + 10));
                files.add(file);
            }
            List<HttpResponse<String>> responses = new ArrayList<>();
            try (HttpClient client = newClient()) {
                List<CompletableFuture<HttpResponse<String>>> futures = new ArrayList<>();
                for (Path file : files) {
                    futures.add(client.sendAsync(request(server, "/files")
                            .header("Content-Type", "application/octet-stream")
                            .timeout(Duration.ofMinutes(5))
                            .POST(HttpRequest.BodyPublishers.ofFile(file)).build(), HttpResponse.BodyHandlers.ofString()));
                }
                for (CompletableFuture<HttpResponse<String>> future : futures) {
                    responses.add(future.get(5, TimeUnit.MINUTES));
                }
            }
            for (int i = 0; i < files.size(); i++) {
                checkEquals(201, responses.get(i).statusCode(), "upload " + i + ": status");
                FileManifest manifest = manifestFrom(server, responses.get(i));
                checkEquals(shas.get(i), manifest.sha256(), "upload " + i + ": checksum");
                checkEquals(shas.get(i), readBackSha256(server, manifest, tempDir), "upload " + i + ": read back");
            }
            checkTmpIsEmpty(server);
        });
    }

    /** Writes {@code size} bytes of a pattern in 1 MiB blocks and returns their SHA-256. */
    private static String writeLargeFile(Path file, long size, int seed) throws IOException {
        MessageDigest digest = newDigest();
        byte[] block = bytes(MIB, seed);
        try (OutputStream out = Files.newOutputStream(file)) {
            for (long written = 0; written < size; written += MIB) {
                int length = (int) Math.min(MIB, size - written);
                out.write(block, 0, length);
                digest.update(block, 0, length);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** The SHA-256 of all chunks of {@code manifest} in order, read one chunk at a time. */
    private static String readBackSha256(DistributedFsServer server, FileManifest manifest, Path tempDir)
            throws IOException {
        MessageDigest digest = newDigest();
        Path dir = Files.createTempDirectory(tempDir, "readback-");
        byte[] buffer = new byte[64 * 1024];
        for (int i = 0; i < manifest.chunkCount(); i++) {
            Path copy = dir.resolve("chunk_" + i);
            server.cluster().retrieveChunk(FileIds.chunkId(manifest.fileId(), i), copy);
            try (InputStream in = Files.newInputStream(copy)) {
                int read;
                while ((read = in.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            Files.delete(copy);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    // ---------------------------------------------------------------------
    // Server helpers
    // ---------------------------------------------------------------------

    private interface ServerBody {
        void run(DistributedFsServer server) throws Exception;
    }

    private static void withServer(Path tempDir, long maxUploadBytes, int chunkSize, ServerBody body) throws Exception {
        withServer(tempDir, maxUploadBytes, chunkSize, new ByteArrayOutputStream(), body);
    }

    /** Starts a server on a fresh data directory, runs {@code body}, and always stops the server. */
    private static void withServer(Path tempDir, long maxUploadBytes, int chunkSize, ByteArrayOutputStream errBytes,
                                   ServerBody body) throws Exception {
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(tempDir.resolve("data"),
                0, 4, 64, 0, 0, nullStream(), err, maxUploadBytes, chunkSize));
        try {
            body.run(server);
        } finally {
            server.stop();
        }
    }

    /** Test settings: port 0, 4 threads, no grace period on stop, no lock retry. */
    private static DistributedFsServer.Config config(Path dataDir, long maxUploadBytes, int chunkSize, PrintStream err) {
        return new DistributedFsServer.Config(dataDir, 0, 4, 64, 0, 0, nullStream(), err, maxUploadBytes, chunkSize);
    }

    private static FileManifest manifestFrom(DistributedFsServer server, HttpResponse<String> response)
            throws IOException {
        String location = response.headers().firstValue("Location").orElseThrow(
                () -> new AssertionError("no Location; status " + response.statusCode() + ", body " + response.body()));
        return server.manifests().read(location.substring("/files/".length())).orElseThrow();
    }

    /** Retrieves every chunk of {@code manifest} in order and returns the bytes joined. */
    private static byte[] readBack(DistributedFsServer server, FileManifest manifest, Path tempDir) throws IOException {
        Path dir = Files.createTempDirectory(tempDir, "readback-");
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        for (int i = 0; i < manifest.chunkCount(); i++) {
            Path copy = dir.resolve("chunk_" + i);
            server.cluster().retrieveChunk(FileIds.chunkId(manifest.fileId(), i), copy);
            all.write(Files.readAllBytes(copy));
        }
        return all.toByteArray();
    }

    /**
     * The handler deletes the workspace after the response has been sent, so the client can see
     * the response a moment before the workspace is gone: wait for it (up to 5 seconds).
     */
    private static void checkTmpIsEmpty(DistributedFsServer server) throws Exception {
        Path tmp = server.dataRoot().resolve("tmp");
        waitUntil(() -> listNames(tmp).isEmpty(), "the tmp folder is empty, but has " + listNames(tmp));
    }

    /** No chunk on any node, no manifest or marker, and no workspace left. */
    private static void checkNothingStored(DistributedFsServer server) throws Exception {
        for (int i = 0; i < 3; i++) {
            checkEquals(List.of(), listNames(server.dataRoot().resolve("nodes").resolve("node-" + i)), "node " + i);
        }
        checkEquals(List.of(), listNames(server.dataRoot().resolve("manifests")), "the manifest folder");
        checkTmpIsEmpty(server);
    }

    // ---------------------------------------------------------------------
    // HTTP helpers
    // ---------------------------------------------------------------------

    private static HttpClient newClient() {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(TIMEOUT_MILLIS))
                .build();
    }

    private static HttpRequest.Builder request(DistributedFsServer server, String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
                .timeout(Duration.ofMillis(TIMEOUT_MILLIS));
    }

    /** POST /files through HttpClient, with an optional X-Content-SHA256. */
    private static HttpResponse<String> upload(DistributedFsServer server, byte[] data, String sha256)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = request(server, "/files").header("Content-Type", "application/octet-stream");
        if (sha256 != null) {
            builder.header("X-Content-SHA256", sha256);
        }
        try (HttpClient client = newClient()) {
            return client.send(builder.POST(HttpRequest.BodyPublishers.ofByteArray(data)).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    private static List<HttpResponse<String>> uploadConcurrently(DistributedFsServer server, List<byte[]> inputs)
            throws Exception {
        try (HttpClient client = newClient()) {
            List<CompletableFuture<HttpResponse<String>>> futures = new ArrayList<>();
            for (byte[] input : inputs) {
                futures.add(client.sendAsync(request(server, "/files")
                        .header("Content-Type", "application/octet-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(input)).build(),
                        HttpResponse.BodyHandlers.ofString()));
            }
            List<HttpResponse<String>> responses = new ArrayList<>();
            for (CompletableFuture<HttpResponse<String>> future : futures) {
                responses.add(future.get(30, TimeUnit.SECONDS));
            }
            return responses;
        }
    }

    /** A raw POST /files with a valid Host, octet-stream Content-Type and Content-Length. */
    private static RawResponse post(DistributedFsServer server, byte[] body) throws IOException {
        return exchange(server, head("POST", "/files", server, OCTET + "Content-Length: " + body.length + "\r\n"), body);
    }

    /** A request head with a valid Host and Connection: close, plus {@code headerLines}. */
    private static String head(String method, String target, DistributedFsServer server, String headerLines) {
        return method + " " + target + " HTTP/1.1\r\nHost: 127.0.0.1:" + server.port() + "\r\nConnection: close\r\n"
                + headerLines + "\r\n";
    }

    /** {@code parts} as a chunked body, one chunk each, with the final empty chunk. */
    private static byte[] chunked(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes((Integer.toHexString(part.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            out.writeBytes(part);
            out.writeBytes("\r\n".getBytes(StandardCharsets.ISO_8859_1));
        }
        out.writeBytes("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
        return out.toByteArray();
    }

    private static Socket connect(DistributedFsServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(LOOPBACK, server.port()), TIMEOUT_MILLIS);
        socket.setSoTimeout(TIMEOUT_MILLIS);
        return socket;
    }

    /** Sends a raw request on a new connection and reads one response. */
    private static RawResponse exchange(DistributedFsServer server, String head, byte[] body) throws IOException {
        try (Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            out.write(head.getBytes(StandardCharsets.ISO_8859_1));
            out.write(body);
            out.flush();
            return readOneResponse(socket.getInputStream());
        }
    }

    /** A parsed HTTP response: status code, headers (lowercase names) and body bytes. */
    private record RawResponse(int status, Map<String, String> headers, byte[] body) {
        String header(String name) {
            return headers.get(name.toLowerCase(Locale.ROOT));
        }

        String bodyText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    /** Reads exactly one response: the head up to the empty line, then Content-Length bytes of body. */
    private static RawResponse readOneResponse(InputStream in) throws IOException {
        ByteArrayOutputStream headBytes = new ByteArrayOutputStream();
        String terminator = "\r\n\r\n";
        int matched = 0;
        while (matched < terminator.length()) {
            int b = in.read();
            if (b == -1) {
                throw new AssertionError("the connection ended before a complete response; received \""
                        + headBytes.toString(StandardCharsets.ISO_8859_1) + "\"");
            }
            headBytes.write(b);
            matched = b == terminator.charAt(matched) ? matched + 1 : (b == '\r' ? 1 : 0);
        }
        String[] lines = headBytes.toString(StandardCharsets.ISO_8859_1).split("\r\n");
        String[] statusParts = lines[0].split(" ", 3);
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
        }
        String length = headers.get("content-length");
        byte[] body = in.readNBytes(length == null ? 0 : Integer.parseInt(length));
        return new RawResponse(Integer.parseInt(statusParts[1]), headers, body);
    }

    /** An error response must be exactly the fixed JSON of {@code expected}, with nothing that could leak. */
    private static void checkErrorResponse(RawResponse response, ApiError expected, DistributedFsServer server, String what) {
        checkEquals(expected.status(), response.status(), what + ": status");
        checkEquals(expected.toJson(), response.bodyText(), what + ": body");
        checkEquals("application/json; charset=utf-8", response.header("content-type"), what + ": Content-Type");
        checkEquals("nosniff", response.header("x-content-type-options"), what + ": X-Content-Type-Options");
        checkEquals("close", response.header("connection"), what + ": Connection");
        String body = response.bodyText();
        for (String leak : List.of(server.dataRoot().toString(), "\\", "Exception", "at com.", "C:", "simulated")) {
            check(!body.contains(leak), what + ": the error body contains \"" + leak + "\"");
        }
    }

    // ---------------------------------------------------------------------
    // Other helpers
    // ---------------------------------------------------------------------

    private interface Condition {
        boolean holds() throws Exception;
    }

    /** Polls {@code condition} every 20 ms for up to 5 seconds. */
    private static void waitUntil(Condition condition, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out: " + what);
            }
            Thread.sleep(20);
        }
    }

    private static byte[] bytes(int size, int seed) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i * 31 + seed * 7 + 1);
        }
        return data;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] all = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, all, a.length, b.length);
        return all;
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256(byte[] data) {
        return HexFormat.of().formatHex(newDigest().digest(data));
    }

    /** The SHA-256 of every {@code chunkSize}-byte piece of {@code data}, computed independently. */
    private static List<String> chunkSha256s(byte[] data, int chunkSize) {
        List<String> sums = new ArrayList<>();
        for (int offset = 0; offset < data.length; offset += chunkSize) {
            sums.add(sha256(Arrays.copyOfRange(data, offset, Math.min(data.length, offset + chunkSize))));
        }
        return sums;
    }

    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    private static PrintStream nullStream() {
        return new PrintStream(OutputStream.nullOutputStream(), true, StandardCharsets.UTF_8);
    }

    private static InetAddress loopback() {
        try {
            return InetAddress.getByAddress("127.0.0.1", new byte[] {127, 0, 0, 1});
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    /** A test body that receives a fresh temporary directory. */
    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    /** A piece of code that is expected to throw. */
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("uploadendpoint-test-");
            body.run(tempDir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (AssertionError e) {
            failed++;
            System.out.println("FAIL  " + name);
            System.out.println("      " + e.getMessage());
        } catch (Throwable e) {
            failed++;
            System.out.println("FAIL  " + name + " (unexpected exception)");
            e.printStackTrace(System.out);
        } finally {
            if (tempDir != null) {
                try {
                    deleteRecursively(tempDir);
                } catch (IOException e) {
                    System.out.println("      warning: could not delete " + tempDir + ": " + e);
                }
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkEquals(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void checkContains(String text, String expectedPart) {
        if (text == null || !text.contains(expectedPart)) {
            throw new AssertionError("expected text containing \"" + expectedPart + "\" but was \"" + text + "\"");
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingAction action) {
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError("expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError("expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }

    private static void deleteRecursively(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) {
                    throw error;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
