package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
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
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Plain-Java tests for {@code GET /files/<fileId>} (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.DownloadEndpointTest [--stress]}
 *
 * <p>Every test starts its own server in this process, on 127.0.0.1 with a port chosen by the
 * operating system and a fresh data directory, and stops it afterwards. Files are uploaded through
 * {@code POST /files}; damage (a corrupt manifest, a missing or altered chunk) is then made
 * directly in the data directory. Requests that HttpClient will not send as written (encoded
 * paths, a client that stops reading or disconnects) go through a raw socket. Every network wait
 * has a time limit, so a hang fails a test instead of blocking the run.
 *
 * <p>{@code --stress} runs only the stress test: a 100 MiB upload downloaded and checked while
 * being read. Run it with a small heap ({@code java -Xmx16m ... --stress}) to show that a
 * download is never held in memory. The program exits with code 1 if any test fails.
 */
public class DownloadEndpointTest {

    private static final int TIMEOUT_MILLIS = 10_000;
    private static final int MIB = 1024 * 1024;
    private static final InetAddress LOOPBACK = loopback();
    private static final String OCTET = "Content-Type: application/octet-stream\r\n";
    private static final String WATCH_THREAD = "distributedfs-stall-watch";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        boolean stress = args.length == 1 && args[0].equals("--stress");
        if (args.length > 0 && !stress) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.DownloadEndpointTest [--stress]");
            System.exit(2);
            return;
        }

        if (stress) {
            long maxHeapMib = Runtime.getRuntime().maxMemory() / MIB;
            System.out.println("Stress mode: max heap is " + maxHeapMib + " MiB");
            if (maxHeapMib >= 100) {
                System.out.println("WARNING: the heap can hold the whole 100 MiB file. Run with -Xmx16m.");
            }
            runTest("STRESS: a 100 MiB file is downloaded byte-identically with a small heap",
                    DownloadEndpointTest::stressLargeDownload);
            finish();
            return;
        }

        // Successful downloads
        runTest("an upload downloads byte-identically, with the verified length and octet-stream headers",
                DownloadEndpointTest::roundTrip);
        runTest("an empty file downloads as 200 with Content-Length 0", DownloadEndpointTest::emptyFile);
        runTest("files below, at and above the chunk size download byte-identically",
                DownloadEndpointTest::chunkBoundaries);

        // File IDs
        runTest("invalid, non-canonical, percent-encoded, separator and traversal IDs are 400 invalid_file_id",
                DownloadEndpointTest::invalidIdsAreRefused);
        runTest("no file ID form reaches a file outside the data directory or another stored file",
                DownloadEndpointTest::traversalCannotEscape);
        runTest("an unknown file ID is 404 file_not_found and creates nothing", DownloadEndpointTest::unknownId);

        // Uncommitted and damaged files
        runTest("an uncommitted upload (pending marker, no manifest) is 404 and its leftovers stay untouched",
                DownloadEndpointTest::pendingMarkerIsNotAFile);
        runTest("a committed file whose pending marker was left behind is still served",
                DownloadEndpointTest::committedFileWithLeftoverMarker);
        runTest("a corrupt or unreadable manifest is 500 metadata_corrupt and changes nothing",
                DownloadEndpointTest::corruptManifest);
        runTest("a missing chunk of a committed file is 500 chunk_missing, not 404, and changes nothing",
                DownloadEndpointTest::missingChunk);
        runTest("altered or shortened chunks and a wrong manifest checksum are 500 integrity_check_failed",
                DownloadEndpointTest::integrityFailures);
        runTest("a damaged chunk is named in the server log, while the client gets only the fixed error body",
                DownloadEndpointTest::damagedChunkIsNamedInTheLog);
        runTest("a file stored before chunk checksums (format 1 manifest) still downloads byte-identically",
                DownloadEndpointTest::legacyManifestStillDownloads);

        // Methods, Range and Host
        runTest("HEAD and every other method on /files/<id> are 405 with Allow: GET",
                DownloadEndpointTest::methodsOtherThanGet);
        runTest("a Range header is ignored: 200 with the whole file and Accept-Ranges: none",
                DownloadEndpointTest::rangeIsNotSupported);
        runTest("a bad Host is 403 before the file ID or the storage is looked at", DownloadEndpointTest::badHost);
        runTest("existing routes keep their methods and 404s", DownloadEndpointTest::existingRoutesUnchanged);

        // Concurrency and clients
        runTest("concurrent downloads of different files, alongside uploads, are all byte-identical",
                DownloadEndpointTest::concurrentDownloads);
        runTest("a client that disconnects mid-download frees the worker and the workspace",
                DownloadEndpointTest::clientDisconnects);
        runTest("a client that stops reading is cut off after the stall timeout and frees the only worker",
                DownloadEndpointTest::stalledClientIsCutOff);
        runTest("a slow client that keeps reading gets the whole file",
                DownloadEndpointTest::slowClientCompletes);
        runTest("a GET declaring Content-Length: 1 but never sending it is cut off with no workspace; the worker is freed",
                DownloadEndpointTest::declaredLengthBodyNeverSent);
        runTest("a chunked GET that never sends a chunk is cut off with no workspace; the worker is freed",
                DownloadEndpointTest::chunkedBodyNeverSent);
        runTest("a GET whose declared body arrives in full is still served",
                DownloadEndpointTest::completeRequestBodyIsDiscarded);

        // Workspace cleanup on unexpected failures
        runTest("an unexpected failure after the workspace exists is 500 internal_error and the workspace is removed",
                DownloadEndpointTest::unexpectedFailureCleansUp);
        runTest("a workspace that cannot be deleted is reported, and the original error is still the answer",
                DownloadEndpointTest::cleanupFailureIsReported);
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
    // Successful downloads
    // ---------------------------------------------------------------------

    private static void roundTrip(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            byte[] data = bytes(4099, 1);
            String fileId = upload(server, data);
            RawResponse response = get(server, "/files/" + fileId);
            checkEquals(200, response.status(), "status");
            check(Arrays.equals(data, response.body()), "the downloaded bytes equal the uploaded bytes");
            checkEquals(Integer.toString(data.length), response.header("content-length"), "Content-Length");
            checkEquals("application/octet-stream", response.header("content-type"), "Content-Type");
            checkEquals("nosniff", response.header("x-content-type-options"), "X-Content-Type-Options");
            checkEquals("none", response.header("accept-ranges"), "Accept-Ranges");
            checkEquals(null, response.header("content-range"), "Content-Range");

            // The same through HttpClient, as a client would normally do it.
            try (HttpClient client = newClient()) {
                HttpResponse<byte[]> viaClient = client.send(request(server, "/files/" + fileId).GET().build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                checkEquals(200, viaClient.statusCode(), "HttpClient status");
                check(Arrays.equals(data, viaClient.body()), "HttpClient body");
            }
            checkTmpIsEmpty(server);
            checkNoWatchThreads();
        });
    }

    private static void emptyFile(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            String fileId = upload(server, new byte[0]);
            RawResponse response = get(server, "/files/" + fileId);
            checkEquals(200, response.status(), "status");
            checkEquals("0", response.header("content-length"), "Content-Length");
            checkEquals(0, response.body().length, "body length");
            checkEquals("application/octet-stream", response.header("content-type"), "Content-Type");
            checkTmpIsEmpty(server);
        });
    }

    private static void chunkBoundaries(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            for (int size : new int[] {1, 3, 4, 5, 8, 9, 40, 41}) {
                byte[] data = bytes(size, size);
                String fileId = upload(server, data);
                RawResponse response = get(server, "/files/" + fileId);
                checkEquals(200, response.status(), size + " bytes: status");
                check(Arrays.equals(data, response.body()), size + " bytes: body");
            }
            checkTmpIsEmpty(server);
        });
    }

    // ---------------------------------------------------------------------
    // File IDs
    // ---------------------------------------------------------------------

    private static void invalidIdsAreRefused(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            String id = upload(server, bytes(10, 1)); // a real file, so encoded forms of its ID are meaningful
            Map<String, String> before = snapshot(server);
            List<String> targets = new ArrayList<>(List.of(
                    "/files/", "/files/abc", "/files/" + id.toUpperCase(Locale.ROOT), "/files/" + id.substring(1),
                    "/files/" + id + "0", "/files/" + id + "/", "/files/" + id + "/x", "/files//" + id,
                    "/files/x/" + id, "/files/" + id + ";x=1", "/files/" + id + ".manifest", "/files/" + id + "_0",
                    "/files/" + id + "%00", "/files/%2F" + id, "/files/" + id + "%2F", "/files/" + id + "%2f",
                    "/files/..", "/files/.", "/files/%2e%2e", "/files/%2E%2E", "/files/..%2F..%2Fsecret.txt",
                    "/files/..%5C..%5Csecret.txt", "/files/%2e%2e%2fmanifests%2f" + id + ".manifest",
                    "/files/%20" + id, "/files/" + id.substring(0, 31) + "%20"));
            // Percent-encoding any one character of a real ID: decoding would give the real ID back.
            targets.add("/files/%" + HexFormat.of().toHexDigits((byte) id.charAt(0)) + id.substring(1));
            targets.add("/files/" + id.substring(0, 31) + "%" + HexFormat.of().toHexDigits((byte) id.charAt(31)));
            for (String target : targets) {
                RawResponse response = exchange(server, head("GET", target, server, ""), new byte[0]);
                checkErrorResponse(response, ApiError.INVALID_FILE_ID, server, "GET " + target);
            }
            checkEquals(before, snapshot(server), "the storage");
            checkTmpIsEmpty(server);
        });
    }

    private static void traversalCannotEscape(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            Files.writeString(tempDir.resolve("secret.txt"), "TOP SECRET OUTSIDE THE DATA DIRECTORY");
            String id = upload(server, "the stored file".getBytes(StandardCharsets.US_ASCII));
            List<String> targets = List.of("/files/../../secret.txt", "/files/..%2F..%2Fsecret.txt",
                    "/files/%2e%2e%2f%2e%2e%2fsecret.txt", "/files/..%5C..%5Csecret.txt",
                    "/files/../manifests/" + id + ".manifest", "/files/..%2Fmanifests%2F" + id + ".manifest",
                    "/files/..%2Fnodes%2Fnode-0%2F" + id + "_0", "/files/..%2Fdistributedfs-data.txt",
                    "/files/" + id + "%2F..%2F..%2Fsecret.txt");
            for (String target : targets) {
                RawResponse response = exchange(server, head("GET", target, server, ""), new byte[0]);
                check(response.status() == 400 || response.status() == 404,
                        "GET " + target + ": status " + response.status());
                String body = new String(response.body(), StandardCharsets.ISO_8859_1);
                for (String leak : List.of("SECRET", "the stored file", "format=", "DistributedFS data directory")) {
                    check(!body.contains(leak), "GET " + target + ": the body contains \"" + leak + "\"");
                }
            }
            checkTmpIsEmpty(server);
        });
    }

    private static void unknownId(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            upload(server, bytes(10, 1));
            Map<String, String> before = snapshot(server);
            String unknown = FileIds.newFileId();
            checkErrorResponse(get(server, "/files/" + unknown), ApiError.FILE_NOT_FOUND, server, "unknown ID");
            checkEquals(before, snapshot(server), "the storage");
            checkTmpIsEmpty(server);
        });
    }

    // ---------------------------------------------------------------------
    // Uncommitted and damaged files
    // ---------------------------------------------------------------------

    private static void pendingMarkerIsNotAFile(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            // What an upload that never committed leaves: its marker and some chunks, no manifest.
            String id = FileIds.newFileId();
            server.manifests().createPending(new ManifestStore.PendingUpload(id, 3));
            Path source = Files.write(tempDir.resolve("chunk-source"), bytes(4, 2));
            server.cluster().storeChunk(FileIds.chunkId(id, 0), source);
            server.cluster().storeChunk(FileIds.chunkId(id, 1), source);
            Map<String, String> before = snapshot(server);

            checkErrorResponse(get(server, "/files/" + id), ApiError.FILE_NOT_FOUND, server, "pending only");
            checkEquals(before, snapshot(server), "the marker and the chunks");
            checkTmpIsEmpty(server);
        });
    }

    private static void committedFileWithLeftoverMarker(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            byte[] data = bytes(10, 3);
            String id = upload(server, data);
            // The upload committed but its marker could not be deleted (a warning at upload time).
            server.manifests().createPending(new ManifestStore.PendingUpload(id, 3));
            RawResponse response = get(server, "/files/" + id);
            checkEquals(200, response.status(), "status");
            check(Arrays.equals(data, response.body()), "body");
            check(server.manifests().readPending(id).isPresent(), "the download leaves the marker for recovery");
            checkTmpIsEmpty(server);
        });
    }

    private static void corruptManifest(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            Path manifests = server.dataRoot().resolve("manifests");
            String garbled = upload(server, bytes(10, 1));
            Files.writeString(manifests.resolve(garbled + ".manifest"), "format=1\nthis is not a manifest\n");
            String leadingZero = upload(server, bytes(10, 2));
            Path manifestFile = manifests.resolve(leadingZero + ".manifest");
            Files.writeString(manifestFile, Files.readString(manifestFile).replace("size=10", "size=010"));
            String folder = upload(server, bytes(10, 3));
            Files.delete(manifests.resolve(folder + ".manifest"));
            Files.createDirectory(manifests.resolve(folder + ".manifest")); // not a regular file: unreadable
            Map<String, String> before = snapshot(server);

            for (String id : List.of(garbled, leadingZero, folder)) {
                checkErrorResponse(get(server, "/files/" + id), ApiError.METADATA_CORRUPT, server, "manifest " + id);
            }
            checkEquals(before, snapshot(server), "the storage");
            checkTmpIsEmpty(server);
        });
    }

    private static void missingChunk(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            String id = upload(server, bytes(10, 1));
            String intact = upload(server, bytes(10, 2));
            Files.delete(chunkFile(server, FileIds.chunkId(id, 1)));
            Map<String, String> before = snapshot(server);

            checkErrorResponse(get(server, "/files/" + id), ApiError.CHUNK_MISSING, server, "missing chunk");
            checkEquals(before, snapshot(server), "the storage");
            checkEquals(200, get(server, "/files/" + intact).status(), "another file is still served");
            checkTmpIsEmpty(server);
        });
    }

    private static void damagedChunkIsNamedInTheLog(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 4, 4, FileApiHandler.STALL_TIMEOUT_MILLIS, errBytes, server -> {
            String id = upload(server, bytes(10, 1));
            Path damaged = chunkFile(server, FileIds.chunkId(id, 1));
            byte[] content = Files.readAllBytes(damaged);
            content[0] ^= 0x01;
            Files.write(damaged, content);

            RawResponse response = get(server, "/files/" + id);
            checkErrorResponse(response, ApiError.INTEGRITY_CHECK_FAILED, server, "the damaged file");
            String log = errBytes.toString(StandardCharsets.UTF_8);
            checkContains(log, "chunk 1 (" + FileIds.chunkId(id, 1) + ")");
            check(!response.bodyText().contains(FileIds.chunkId(id, 1)), "the chunk ID is only in the server log");
            checkTmpIsEmpty(server);
        });
    }

    private static void legacyManifestStillDownloads(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            byte[] data = bytes(10, 6);
            String id = upload(server, data);
            Path manifestFile = server.dataRoot().resolve("manifests").resolve(id + ".manifest");
            FileManifest stored = server.manifests().read(id).orElseThrow();
            check(stored.hasChunkChecksums(), "a new upload is stored with chunk checksums");
            Files.delete(manifestFile);
            Files.writeString(manifestFile, new FileManifest(id, stored.size(), stored.sha256(), stored.chunkSize(),
                    stored.chunkCount()).toText());

            RawResponse response = get(server, "/files/" + id);
            checkEquals(200, response.status(), "status");
            check(Arrays.equals(data, response.body()), "the format 1 file downloads byte-identically");
            checkTmpIsEmpty(server);
        });
    }

    private static void integrityFailures(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            String flipped = upload(server, bytes(10, 1));
            Path flippedChunk = chunkFile(server, FileIds.chunkId(flipped, 1));
            byte[] content = Files.readAllBytes(flippedChunk);
            content[2] ^= 0x40;
            Files.write(flippedChunk, content);

            String shortened = upload(server, bytes(10, 2));
            Files.write(chunkFile(server, FileIds.chunkId(shortened, 2)), new byte[1]);

            String lengthened = upload(server, bytes(10, 3));
            Path last = chunkFile(server, FileIds.chunkId(lengthened, 2));
            Files.write(last, concat(Files.readAllBytes(last), new byte[] {7}));

            // A well-formed manifest whose checksum does not match its (intact) chunks.
            String wrongSum = upload(server, bytes(10, 4));
            Path manifestFile = server.dataRoot().resolve("manifests").resolve(wrongSum + ".manifest");
            Files.delete(manifestFile);
            Files.writeString(manifestFile, new FileManifest(wrongSum, 10, sha256(bytes(10, 5)), 4, 3).toText());
            Map<String, String> before = snapshot(server);

            for (String id : List.of(flipped, shortened, lengthened, wrongSum)) {
                RawResponse response = get(server, "/files/" + id);
                checkErrorResponse(response, ApiError.INTEGRITY_CHECK_FAILED, server, "file " + id);
            }
            checkEquals(before, snapshot(server), "the storage");
            checkTmpIsEmpty(server);
        });
    }

    // ---------------------------------------------------------------------
    // Methods, Range and Host
    // ---------------------------------------------------------------------

    private static void methodsOtherThanGet(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            String id = upload(server, bytes(10, 1));
            Map<String, String> before = snapshot(server);
            try (Socket socket = connect(server)) {
                socket.getOutputStream().write(head("HEAD", "/files/" + id, server, "").getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                String text = new String(socket.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
                check(text.startsWith("HTTP/1.1 405 "), "HEAD: " + text);
                checkContains(text.toLowerCase(Locale.ROOT), "\r\nallow: get\r\n");
                check(text.endsWith("\r\n\r\n"), "HEAD has no body: " + text);
            }
            for (String method : List.of("POST", "PUT", "DELETE", "PATCH", "OPTIONS")) {
                RawResponse response = exchange(server, head(method, "/files/" + id, server, "Content-Length: 0\r\n"),
                        new byte[0]);
                checkErrorResponse(response, ApiError.METHOD_NOT_ALLOWED, server, method);
                checkEquals("GET", response.header("allow"), method + ": Allow");
            }
            // The method is checked first: a bad ID with a wrong method is still 405.
            RawResponse badIdDelete = exchange(server, head("DELETE", "/files/..", server, ""), new byte[0]);
            checkErrorResponse(badIdDelete, ApiError.METHOD_NOT_ALLOWED, server, "DELETE /files/..");
            checkEquals(before, snapshot(server), "the storage");
            checkTmpIsEmpty(server);
        });
    }

    private static void rangeIsNotSupported(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            byte[] data = bytes(10, 1);
            String id = upload(server, data);
            for (String range : List.of("bytes=0-3", "bytes=5-", "bytes=-2", "bytes=0-0,4-5", "pages=1")) {
                RawResponse response = exchange(server, head("GET", "/files/" + id, server, "Range: " + range + "\r\n"),
                        new byte[0]);
                checkEquals(200, response.status(), range + ": status");
                check(Arrays.equals(data, response.body()), range + ": the whole file");
                checkEquals(null, response.header("content-range"), range + ": Content-Range");
                checkEquals("none", response.header("accept-ranges"), range + ": Accept-Ranges");
            }
            checkTmpIsEmpty(server);
        });
    }

    private static void badHost(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            String id = upload(server, bytes(10, 1));
            for (String target : List.of("/files/" + id, "/files/..", "/files/" + FileIds.newFileId())) {
                for (String host : List.of("Host: evil.example:" + server.port() + "\r\n", "Host: 127.0.0.1\r\n",
                        "Host: localhost:1\r\n")) {
                    String request = "GET " + target + " HTTP/1.1\r\n" + host + "Connection: close\r\n\r\n";
                    checkErrorResponse(exchange(server, request, new byte[0]), ApiError.HOST_NOT_ALLOWED, server,
                            target + " with " + host.trim());
                }
                String noHost = "GET " + target + " HTTP/1.1\r\nConnection: close\r\n\r\n";
                checkErrorResponse(exchange(server, noHost, new byte[0]), ApiError.HOST_NOT_ALLOWED, server,
                        target + " without Host");
            }
            String absolute = "GET http://127.0.0.1:" + server.port() + "/files/" + id + " HTTP/1.1\r\nHost: 127.0.0.1:"
                    + server.port() + "\r\nConnection: close\r\n\r\n";
            checkErrorResponse(exchange(server, absolute, new byte[0]), ApiError.HOST_NOT_ALLOWED, server, "absolute form");
            checkTmpIsEmpty(server); // refused before any workspace was created
        });
    }

    private static void existingRoutesUnchanged(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            checkEquals(200, get(server, "/health").status(), "GET /health");
            RawResponse getFiles = get(server, "/files");
            checkErrorResponse(getFiles, ApiError.METHOD_NOT_ALLOWED, server, "GET /files");
            checkEquals("POST", getFiles.header("allow"), "GET /files: Allow");
            for (String path : List.of("/FILES/" + FileIds.newFileId(), "/files%2F" + FileIds.newFileId(), "/file/x",
                    "/%66iles/" + FileIds.newFileId())) {
                checkErrorResponse(get(server, path), ApiError.NOT_FOUND, server, "GET " + path);
            }
        });
    }

    // ---------------------------------------------------------------------
    // Concurrency and clients
    // ---------------------------------------------------------------------

    private static void concurrentDownloads(Path tempDir) throws Exception {
        withServer(tempDir, 1024, server -> {
            List<byte[]> files = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                byte[] data = bytes(3000 + 977 * i, i);
                files.add(data);
                ids.add(upload(server, data));
            }
            try (HttpClient client = newClient()) {
                List<CompletableFuture<HttpResponse<byte[]>>> downloads = new ArrayList<>();
                List<CompletableFuture<HttpResponse<String>>> uploads = new ArrayList<>();
                for (int round = 0; round < 3; round++) {
                    for (String id : ids) {
                        downloads.add(client.sendAsync(request(server, "/files/" + id).GET().build(),
                                HttpResponse.BodyHandlers.ofByteArray()));
                    }
                    uploads.add(client.sendAsync(request(server, "/files")
                            .header("Content-Type", "application/octet-stream")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(bytes(5000, 50 + round))).build(),
                            HttpResponse.BodyHandlers.ofString()));
                }
                for (int i = 0; i < downloads.size(); i++) {
                    HttpResponse<byte[]> response = downloads.get(i).get(30, TimeUnit.SECONDS);
                    checkEquals(200, response.statusCode(), "download " + i + ": status");
                    check(Arrays.equals(files.get(i % files.size()), response.body()), "download " + i + ": body");
                }
                for (CompletableFuture<HttpResponse<String>> upload : uploads) {
                    checkEquals(201, upload.get(30, TimeUnit.SECONDS).statusCode(), "upload during downloads");
                }
            }
            checkTmpIsEmpty(server);
            checkNoWatchThreads();
        });
    }

    private static void clientDisconnects(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 1, MIB, FileApiHandler.STALL_TIMEOUT_MILLIS, errBytes, server -> {
            byte[] data = bytes(32 * MIB, 1);
            String id = upload(server, data);
            try (Socket socket = smallBufferSocket(server)) {
                socket.getOutputStream().write(head("GET", "/files/" + id, server, "").getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                InputStream in = socket.getInputStream();
                Map<String, String> headers = readHead(in);
                checkEquals(Integer.toString(data.length), headers.get("content-length"), "Content-Length");
                check(in.readNBytes(1000).length == 1000, "the first bytes arrived");
                socket.setSoLinger(true, 0); // close with a reset, as a client that goes away does
            }
            waitUntil(() -> errText(errBytes).contains("Download of file " + id + " abandoned"), 15,
                    "the server noticed the disconnect: " + errText(errBytes));
            checkContains(errText(errBytes), "disconnected");
            checkTmpIsEmpty(server);
            checkNoWatchThreads();
            // The only worker is free again, and the file is intact.
            RawResponse again = get(server, "/files/" + id);
            checkEquals(200, again.status(), "the next download");
            check(Arrays.equals(data, again.body()), "the next download's body");
        });
    }

    private static void stalledClientIsCutOff(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 1, MIB, 500, errBytes, server -> {
            byte[] data = bytes(32 * MIB, 2);
            String id = upload(server, data);
            try (Socket socket = smallBufferSocket(server)) {
                socket.getOutputStream().write(head("GET", "/files/" + id, server, "").getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                InputStream in = socket.getInputStream();
                readHead(in);
                // Stop reading. The only worker is now stuck writing until the stall timeout closes the connection.
                waitUntil(() -> errText(errBytes).contains("accepted no data for 500 ms"), 15,
                        "the server cut the stalled client off: " + errText(errBytes));
                checkTmpIsEmpty(server);
                checkNoWatchThreads();
                checkEquals(200, get(server, "/health").status(), "the only worker serves the next request");

                // The stalled client gets less than the whole file, never a complete-looking body.
                long received = 0;
                byte[] buffer = new byte[64 * 1024];
                try {
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        received += read;
                    }
                } catch (SocketException | SocketTimeoutException e) {
                    // a reset is also an end
                }
                check(received < data.length, "the stalled client received " + received + " of " + data.length + " bytes");
            }
        });
    }

    private static void slowClientCompletes(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 1, MIB, 1000, errBytes, server -> {
            byte[] data = bytes(16 * MIB, 3);
            String id = upload(server, data);
            MessageDigest digest = newDigest();
            long received = 0;
            try (Socket socket = smallBufferSocket(server)) {
                socket.getOutputStream().write(head("GET", "/files/" + id, server, "").getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                InputStream in = socket.getInputStream();
                long length = Long.parseLong(readHead(in).get("content-length"));
                byte[] buffer = new byte[256 * 1024];
                while (received < length) {
                    int read = in.readNBytes(buffer, 0, (int) Math.min(buffer.length, length - received));
                    if (read == 0) {
                        break;
                    }
                    digest.update(buffer, 0, read);
                    received += read;
                    Thread.sleep(40); // well under the 1 s stall timeout per 256 KiB, but far slower than the server
                }
            }
            checkEquals((long) data.length, received, "bytes received");
            checkEquals(sha256(data), HexFormat.of().formatHex(digest.digest()), "SHA-256 of the received bytes");
            check(!errText(errBytes).contains("abandoned"), "no download was abandoned: " + errText(errBytes));
            checkTmpIsEmpty(server);
            checkNoWatchThreads();
        });
    }

    private static void declaredLengthBodyNeverSent(Path tempDir) throws Exception {
        requestBodyNeverSent(tempDir, "Content-Length: 1\r\n");
    }

    private static void chunkedBodyNeverSent(Path tempDir) throws Exception {
        requestBodyNeverSent(tempDir, "Transfer-Encoding: chunked\r\n");
    }

    /**
     * A GET that declares a body and never sends it, from a client that never reads either. With
     * one worker and a 500 ms stall timeout, the server must give up on the body without creating
     * a workspace, close the connection without a response, and serve the next request.
     */
    private static void requestBodyNeverSent(Path tempDir, String bodyHeader) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, 1, MIB, 500, errBytes, server -> {
            byte[] data = bytes(32 * MIB, 4);
            String id = upload(server, data);
            checkTmpIsEmpty(server); // the upload's own workspace goes a moment after its response
            Path tmp = server.dataRoot().resolve("tmp");
            try (Socket socket = smallBufferSocket(server)) {
                socket.setSoTimeout(15_000);
                socket.getOutputStream().write(head("GET", "/files/" + id, server, bodyHeader)
                        .getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                // Until the stall timeout, the server is waiting for the body: no workspace may exist.
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(400);
                while (System.nanoTime() < until) {
                    checkEquals(List.of(), listNames(tmp), "tmp while the request body is awaited");
                    Thread.sleep(20);
                }
                waitUntil(() -> errText(errBytes).contains(
                                "Download of file " + id + " abandoned: its declared request body did not arrive"), 15,
                        "the server gave up on the request body: " + errText(errBytes));
                checkEquals(List.of(), listNames(tmp), "tmp after the server gave up");
                // The connection is closed without any response (EOF or a reset, never a status line).
                ByteArrayOutputStream received = new ByteArrayOutputStream();
                try {
                    InputStream in = socket.getInputStream();
                    byte[] buffer = new byte[8192];
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        received.write(buffer, 0, read);
                    }
                } catch (SocketException e) {
                    // a reset also ends the connection
                }
                checkEquals(0, received.size(), "bytes the client received: " + received.toString(StandardCharsets.ISO_8859_1));
            }
            checkNoWatchThreads();
            checkEquals(200, get(server, "/health").status(), "the only worker serves the next request");
            RawResponse again = get(server, "/files/" + id);
            checkEquals(200, again.status(), "a normal download afterwards");
            check(Arrays.equals(data, again.body()), "a normal download afterwards: body");
            checkTmpIsEmpty(server);
        });
    }

    private static void completeRequestBodyIsDiscarded(Path tempDir) throws Exception {
        withServer(tempDir, 4, server -> {
            byte[] data = bytes(10, 6);
            String id = upload(server, data);
            RawResponse withLength = exchange(server, head("GET", "/files/" + id, server, "Content-Length: 3\r\n"),
                    "abc".getBytes(StandardCharsets.US_ASCII));
            checkEquals(200, withLength.status(), "Content-Length body: status");
            check(Arrays.equals(data, withLength.body()), "Content-Length body: the file");
            RawResponse chunkedBody = exchange(server, head("GET", "/files/" + id, server,
                    "Transfer-Encoding: chunked\r\n"), "3\r\nabc\r\n0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            checkEquals(200, chunkedBody.status(), "chunked body: status");
            check(Arrays.equals(data, chunkedBody.body()), "chunked body: the file");
            checkTmpIsEmpty(server);
        });
    }

    // ---------------------------------------------------------------------
    // Workspace cleanup on unexpected failures
    // ---------------------------------------------------------------------

    /** Fails the download hook after writing into the workspace; optionally keeps a file open there. */
    private static final class FailingDownloadHandler extends FileApiHandler {
        volatile Exception failWith;
        volatile boolean holdFileOpen;
        volatile OutputStream heldOpen;

        FailingDownloadHandler(int port, PrintStream err, FileService files) {
            super(port, err, files);
        }

        @Override
        FileService.Download prepareDownload(String fileId, Path workspace) throws IOException {
            Files.writeString(workspace.resolve("file"), "partly rebuilt");
            if (holdFileOpen) {
                // java.io opens without the delete-sharing that Files.newOutputStream allows on
                // Windows, so while this stream is open the file, and so the workspace, cannot be deleted.
                heldOpen = new FileOutputStream(workspace.resolve("held-open").toFile());
            }
            Exception failure = failWith;
            if (failure instanceof IOException io) {
                throw io;
            }
            throw (RuntimeException) failure;
        }
    }

    private static void unexpectedFailureCleansUp(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        FailingDownloadHandler[] handler = new FailingDownloadHandler[1];
        withHandler(tempDir, errBytes, (port, err, files) -> handler[0] = new FailingDownloadHandler(port, err, files),
                server -> {
                    String id = upload(server, bytes(10, 1));
                    for (Exception failure : List.of(new IllegalStateException("simulated failure in C:\\secret"),
                            new IOException("simulated I/O failure in C:\\secret"))) {
                        handler[0].failWith = failure;
                        checkErrorResponse(get(server, "/files/" + id), ApiError.INTERNAL_ERROR, server,
                                failure.getClass().getSimpleName());
                        checkTmpIsEmpty(server);
                    }
                    checkContains(errText(errBytes), "simulated failure in C:\\secret");
                    checkContains(errText(errBytes), "simulated I/O failure in C:\\secret");
                });
    }

    private static void cleanupFailureIsReported(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        FailingDownloadHandler[] handler = new FailingDownloadHandler[1];
        withHandler(tempDir, errBytes, (port, err, files) -> handler[0] = new FailingDownloadHandler(port, err, files),
                server -> {
                    String id = upload(server, bytes(10, 1));
                    handler[0].failWith = new IllegalStateException("simulated failure in C:\\secret");
                    handler[0].holdFileOpen = true;
                    try {
                        checkErrorResponse(get(server, "/files/" + id), ApiError.INTERNAL_ERROR, server, "the answer");
                        if (isWindows()) {
                            // Windows cannot delete a file that is open, so the workspace deletion fails.
                            waitUntil(() -> errText(errBytes).contains("could not delete the request workspace"), 5,
                                    "the cleanup failure is reported: " + errText(errBytes));
                            checkContains(errText(errBytes), "simulated failure in C:\\secret");
                            List<String> left = listNames(server.dataRoot().resolve("tmp"));
                            checkEquals(1, left.size(), "the workspace that could not be deleted: " + left);
                            check(left.get(0).matches("download-[0-9]{1,20}"), "its name: " + left);
                        }
                    } finally {
                        if (handler[0].heldOpen != null) {
                            handler[0].heldOpen.close();
                        }
                    }
                    // Once the file is closed, the next start's recovery removes the stale workspace.
                });
        if (isWindows()) {
            DistributedFsServer restarted = DistributedFsServer.start(new DistributedFsServer.Config(
                    tempDir.resolve("data"), 0, 1, 8, 0, 0, nullStream(), nullStream(), MIB, 4));
            try {
                checkEquals(List.of(), listNames(restarted.dataRoot().resolve("tmp")), "tmp after the restart");
            } finally {
                restarted.stop();
            }
        }
    }

    // ---------------------------------------------------------------------
    // Stress test (run separately with --stress and -Xmx16m)
    // ---------------------------------------------------------------------

    private static void stressLargeDownload(Path tempDir) throws Exception {
        withServer(tempDir, 1, FileChunker.DEFAULT_CHUNK_SIZE, FileApiHandler.STALL_TIMEOUT_MILLIS,
                new ByteArrayOutputStream(), 100L * MIB, server -> {
            Path file = tempDir.resolve("big");
            String expectedSha = writeLargeFile(file, 100L * MIB, 7);
            String id;
            try (HttpClient client = newClient()) {
                HttpResponse<String> response = client.send(request(server, "/files")
                        .header("Content-Type", "application/octet-stream")
                        .timeout(Duration.ofMinutes(5))
                        .POST(HttpRequest.BodyPublishers.ofFile(file)).build(), HttpResponse.BodyHandlers.ofString());
                checkEquals(201, response.statusCode(), "upload status");
                id = response.headers().firstValue("Location").orElseThrow().substring("/files/".length());
            }
            Files.delete(file);

            // Read the download through a raw socket, hashing as it arrives: nothing holds the whole file.
            MessageDigest digest = newDigest();
            long received = 0;
            try (Socket socket = connect(server)) {
                socket.setSoTimeout(120_000);
                socket.getOutputStream().write(head("GET", "/files/" + id, server, "").getBytes(StandardCharsets.ISO_8859_1));
                socket.getOutputStream().flush();
                InputStream in = socket.getInputStream();
                Map<String, String> headers = readHead(in);
                checkEquals(Long.toString(100L * MIB), headers.get("content-length"), "Content-Length");
                byte[] buffer = new byte[64 * 1024];
                int read;
                while (received < 100L * MIB && (read = in.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                    received += read;
                }
            }
            checkEquals(100L * MIB, received, "bytes received");
            checkEquals(expectedSha, HexFormat.of().formatHex(digest.digest()), "SHA-256 of the download");
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

    // ---------------------------------------------------------------------
    // Server helpers
    // ---------------------------------------------------------------------

    private interface ServerBody {
        void run(DistributedFsServer server) throws Exception;
    }

    /** A server with 4 threads, 1 MiB upload limit and the given chunk size. */
    private static void withServer(Path tempDir, int chunkSize, ServerBody body) throws Exception {
        withServer(tempDir, 4, chunkSize, FileApiHandler.STALL_TIMEOUT_MILLIS, new ByteArrayOutputStream(),
                MIB, body);
    }

    /** A server with the given worker threads, chunk size and stall timeout, and a 64 MiB upload limit. */
    private static void withServer(Path tempDir, int threads, int chunkSize, long stallMillis,
                                   ByteArrayOutputStream errBytes, ServerBody body) throws Exception {
        withServer(tempDir, threads, chunkSize, stallMillis, errBytes, 64L * MIB, body);
    }

    /** Starts a server on a fresh data directory, runs {@code body}, and always stops the server. */
    private static void withServer(Path tempDir, int threads, int chunkSize, long stallMillis,
                                   ByteArrayOutputStream errBytes, long maxUploadBytes, ServerBody body) throws Exception {
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(tempDir.resolve("data"),
                0, threads, 64, 0, 0, nullStream(), err, maxUploadBytes, chunkSize),
                (port, handlerErr, files) -> new FileApiHandler(port, handlerErr, files, stallMillis));
        try {
            body.run(server);
        } finally {
            server.stop();
        }
    }

    /** As withServer, with a handler made by {@code factory}. */
    private static void withHandler(Path tempDir, ByteArrayOutputStream errBytes,
                                    DistributedFsServer.HandlerFactory factory, ServerBody body) throws Exception {
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(tempDir.resolve("data"),
                0, 4, 64, 0, 0, nullStream(), err, MIB, 4), factory);
        try {
            body.run(server);
        } finally {
            server.stop();
        }
    }

    /** POST /files over a raw socket; returns the new file ID. */
    private static String upload(DistributedFsServer server, byte[] data) throws IOException {
        RawResponse response = exchange(server,
                head("POST", "/files", server, OCTET + "Content-Length: " + data.length + "\r\n"), data);
        checkEquals(201, response.status(), "upload status (" + response.bodyText() + ")");
        String location = response.header("location");
        check(location != null && location.startsWith("/files/"), "Location: " + location);
        return location.substring("/files/".length());
    }

    /** The stored chunk file with this ID, on whichever node holds it. */
    private static Path chunkFile(DistributedFsServer server, String chunkId) {
        for (int i = 0; i < 3; i++) {
            Path candidate = server.dataRoot().resolve("nodes").resolve("node-" + i).resolve(chunkId);
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        throw new AssertionError("no node holds " + chunkId);
    }

    /** Every file under manifests and nodes, by relative path, with its SHA-256 ("DIR" for a folder). */
    private static Map<String, String> snapshot(DistributedFsServer server) throws IOException {
        Map<String, String> entries = new TreeMap<>();
        Path root = server.dataRoot();
        for (Path dir : List.of(root.resolve("manifests"), root.resolve("nodes"))) {
            try (Stream<Path> walk = Files.walk(dir)) {
                for (Path path : walk.toList()) {
                    String key = root.relativize(path).toString();
                    entries.put(key, Files.isDirectory(path) ? "DIR" : sha256(Files.readAllBytes(path)));
                }
            }
        }
        return entries;
    }

    /**
     * The handler deletes the workspace after the response has been sent, so the client can see
     * the response a moment before the workspace is gone: wait for it (up to 5 seconds).
     */
    private static void checkTmpIsEmpty(DistributedFsServer server) throws Exception {
        Path tmp = server.dataRoot().resolve("tmp");
        waitUntil(() -> listNames(tmp).isEmpty(), 5, "the tmp folder is empty, but has " + listNames(tmp));
    }

    /** Every stall watchdog has ended (each ends with its download). */
    private static void checkNoWatchThreads() throws Exception {
        waitUntil(() -> liveThreads(WATCH_THREAD) == 0, 5, "no stall watchdog threads are left");
    }

    private static long liveThreads(String name) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.isAlive() && thread.getName().equals(name)).count();
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
                .timeout(Duration.ofMillis(30_000));
    }

    private static RawResponse get(DistributedFsServer server, String target) throws IOException {
        return exchange(server, head("GET", target, server, ""), new byte[0]);
    }

    /** A request head with a valid Host and Connection: close, plus {@code headerLines}. */
    private static String head(String method, String target, DistributedFsServer server, String headerLines) {
        return method + " " + target + " HTTP/1.1\r\nHost: 127.0.0.1:" + server.port() + "\r\nConnection: close\r\n"
                + headerLines + "\r\n";
    }

    private static Socket connect(DistributedFsServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(LOOPBACK, server.port()), TIMEOUT_MILLIS);
        socket.setSoTimeout(TIMEOUT_MILLIS);
        return socket;
    }

    /** A connection with a small receive buffer, so a client that stops reading blocks the server soon. */
    private static Socket smallBufferSocket(DistributedFsServer server) throws IOException {
        Socket socket = new Socket();
        socket.setReceiveBufferSize(4096);
        socket.connect(new InetSocketAddress(LOOPBACK, server.port()), TIMEOUT_MILLIS);
        socket.setSoTimeout(30_000);
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

    /** Reads the status line and headers up to the empty line; the status is stored under ":status". */
    private static Map<String, String> readHead(InputStream in) throws IOException {
        ByteArrayOutputStream headBytes = new ByteArrayOutputStream();
        String terminator = "\r\n\r\n";
        int matched = 0;
        while (matched < terminator.length()) {
            int b = in.read();
            if (b == -1) {
                throw new AssertionError("the connection ended before a complete response head; received \""
                        + headBytes.toString(StandardCharsets.ISO_8859_1) + "\"");
            }
            headBytes.write(b);
            matched = b == terminator.charAt(matched) ? matched + 1 : (b == '\r' ? 1 : 0);
        }
        String[] lines = headBytes.toString(StandardCharsets.ISO_8859_1).split("\r\n");
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":status", lines[0].split(" ", 3)[1]);
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
        }
        return headers;
    }

    /** Reads exactly one response: the head, then Content-Length bytes of body. */
    private static RawResponse readOneResponse(InputStream in) throws IOException {
        Map<String, String> headers = readHead(in);
        int status = Integer.parseInt(headers.remove(":status"));
        String length = headers.get("content-length");
        byte[] body = in.readNBytes(length == null ? 0 : Integer.parseInt(length));
        if (length != null && body.length != Integer.parseInt(length)) {
            throw new AssertionError("the body ended after " + body.length + " of " + length + " bytes");
        }
        return new RawResponse(status, headers, body);
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

    /** Polls {@code condition} every 20 ms for up to {@code seconds}. */
    private static void waitUntil(Condition condition, int seconds, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out: " + what);
            }
            Thread.sleep(20);
        }
    }

    private static String errText(ByteArrayOutputStream errBytes) {
        return errBytes.toString(StandardCharsets.UTF_8);
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").startsWith("Windows");
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

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("downloadendpoint-test-");
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
