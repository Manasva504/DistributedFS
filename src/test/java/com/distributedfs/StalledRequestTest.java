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
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Plain-Java tests for clients that stall: a request that declares a body it never sends, and an
 * upload that stops sending (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.StalledRequestTest}
 *
 * <p>Every test starts its own server with <b>one</b> worker thread and a 500 ms stall timeout, so
 * a single stuck request would block every other one. A stalled test connection is kept open
 * (the client never gives up on its own) while the test checks that the only worker still answers
 * {@code GET /health}, that the server closed the stalled connection without sending anything,
 * and that nothing was stored or left behind. Every network wait has a time limit, so a hang
 * fails a test instead of blocking the run. The program exits with code 1 if any test fails.
 */
public class StalledRequestTest {

    private static final long STALL_MILLIS = 500;
    /** How long a stalled connection is held before the checks: three stall timeouts. */
    private static final long HOLD_MILLIS = 3 * STALL_MILLIS;
    /** The limit for every network wait in these tests. */
    private static final int WAIT_MILLIS = 3_000;
    private static final long MAX_UPLOAD_BYTES = 8L * 1024 * 1024;
    private static final int CHUNK_SIZE = 64 * 1024;
    private static final InetAddress LOOPBACK = loopback();
    private static final String OCTET = "Content-Type: application/octet-stream\r\n";
    private static final String UNKNOWN_FILE_ID = "0123456789abcdef0123456789abcdef";
    private static final String WATCH_THREAD = "distributedfs-stall-watch";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.StalledRequestTest");
            System.exit(2);
            return;
        }

        // A declared body that never arrives, on every kind of response (Content-Length: 1)
        runM1("GET /health (200)", "GET", "/health", "", "Request GET /health abandoned");
        runM1("an unknown path (404)", "GET", "/nope", "", "Request GET /nope abandoned");
        runM1("an invalid file ID (400)", "GET", "/files/not-an-id", "", "Request GET /files/not-an-id abandoned");
        runM1("a wrong method (405)", "PUT", "/health", "", "Request PUT /health abandoned");
        runM1("a wrong Host (403)", "GET", "/health", "BAD-HOST", "Request GET /health abandoned");
        runM1("a wrong Content-Type on POST /files (415)", "POST", "/files", "Content-Type: text/plain\r\n",
                "Request POST /files abandoned");
        runM1("a malformed X-Content-SHA256 on POST /files (400)", "POST", "/files",
                OCTET + "X-Content-SHA256: abc\r\n", "Request POST /files abandoned");
        runM1("a download of an unknown file (the download route)", "GET", "/files/" + UNKNOWN_FILE_ID, "",
                "Download of file " + UNKNOWN_FILE_ID + " abandoned");
        runTest("a Content-Length over the limit, body never sent (413): closed after the timeout, no response",
                tempDir -> stalledRequest(tempDir, "POST", "/files",
                        OCTET + "Content-Length: " + (MAX_UPLOAD_BYTES + 1) + "\r\n", new byte[0],
                        "Request POST /files abandoned: its declared request body did not arrive within 500 ms"));

        // The same with a chunked body that never arrives
        runTest("a chunked body that never arrives on GET /health: closed after the timeout, no response",
                tempDir -> stalledRequest(tempDir, "GET", "/health", "Transfer-Encoding: chunked\r\n", new byte[0],
                        "Request GET /health abandoned: its declared request body did not arrive within 500 ms"));
        runTest("a chunked body that never arrives on a 415: closed after the timeout, no response",
                tempDir -> stalledRequest(tempDir, "POST", "/files",
                        "Content-Type: text/plain\r\nTransfer-Encoding: chunked\r\n", new byte[0],
                        "Request POST /files abandoned: its declared request body did not arrive within 500 ms"));

        // Complete bodies are still read and answered normally
        runTest("requests with a complete small body still get their normal responses",
                StalledRequestTest::completeBodiesAreAnswered);

        // Uploads that stop sending
        runTest("an upload that sends no body at all is abandoned after the timeout and stores nothing",
                tempDir -> stalledRequest(tempDir, "POST", "/files", OCTET + "Content-Length: 1048576\r\n",
                        new byte[0], "Upload abandoned: no data arrived for 500 ms"));
        runTest("an upload that stops halfway is abandoned after the timeout and stores nothing",
                tempDir -> stalledRequest(tempDir, "POST", "/files", OCTET + "Content-Length: 1048576\r\n",
                        bytes(200_000, 1), "Upload abandoned: no data arrived for 500 ms"));
        runTest("a chunked upload that stops halfway is abandoned after the timeout and stores nothing",
                tempDir -> stalledRequest(tempDir, "POST", "/files", OCTET + "Transfer-Encoding: chunked\r\n",
                        concat("30000\r\n".getBytes(StandardCharsets.ISO_8859_1), bytes(100_000, 2)),
                        "Upload abandoned: no data arrived for 500 ms"));
        runTest("a slow upload that keeps sending is never cut off, even when it takes many timeouts",
                StalledRequestTest::slowSteadyUploadSucceeds);
        finish();
    }

    /** One M1 case: Content-Length: 1, and the byte is never sent. */
    private static void runM1(String what, String method, String target, String headerLines, String logStart) {
        runTest("a declared body that never arrives on " + what + ": closed after the timeout, no response",
                tempDir -> stalledRequest(tempDir, method, target, headerLines + "Content-Length: 1\r\n", new byte[0],
                        logStart + ": its declared request body did not arrive within 500 ms"));
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
    // Tests
    // ---------------------------------------------------------------------

    /**
     * Sends a request head and {@code bodyStart}, then nothing more, and keeps the connection open.
     * {@code headerLines} of "BAD-HOST" means a wrong Host header instead of the right one.
     */
    private static void stalledRequest(Path tempDir, String method, String target, String headerLines,
                                       byte[] bodyStart, String expectedLog) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, errBytes, server -> {
            String head = headerLines.startsWith("BAD-HOST")
                    ? method + " " + target + " HTTP/1.1\r\nHost: evil.example:" + server.port()
                            + "\r\nConnection: close\r\n" + headerLines.substring("BAD-HOST".length()) + "\r\n"
                    : head(method, target, server, headerLines);
            try (Socket stalled = connect(server)) {
                OutputStream out = stalled.getOutputStream();
                out.write(head.getBytes(StandardCharsets.ISO_8859_1));
                out.write(bodyStart);
                out.flush();
                Thread.sleep(HOLD_MILLIS);

                // The client still holds its end open, yet the only worker is free again.
                checkHealthAnswers(server, "while a stalled connection is still open");
                // The server closed the stalled connection without sending a single byte.
                checkClosedWithoutResponse(stalled);
            }
            waitUntil(() -> errBytes.toString(StandardCharsets.UTF_8).contains(expectedLog),
                    "the server logs \"" + expectedLog + "\"; its log is: " + errBytes.toString(StandardCharsets.UTF_8));
            checkNothingStored(server);
            checkNoWatchThreads();
            checkHealthAnswers(server, "after the stalled connection was closed");
        });
    }

    private static void completeBodiesAreAnswered(Path tempDir) throws Exception {
        withServer(tempDir, new ByteArrayOutputStream(), server -> {
            byte[] five = bytes(5, 3);
            checkEquals(200, exchange(server, head("GET", "/health", server, "Content-Length: 5\r\n"), five).status,
                    "GET /health with a 5-byte body");
            checkEquals(404, exchange(server, head("GET", "/nope", server, "Content-Length: 5\r\n"), five).status,
                    "an unknown path with a 5-byte body");
            checkEquals(400, exchange(server, head("GET", "/files/not-an-id", server, "Content-Length: 5\r\n"),
                    five).status, "an invalid file ID with a 5-byte body");
            checkEquals(405, exchange(server, head("PUT", "/health", server, "Content-Length: 5\r\n"), five).status,
                    "a wrong method with a 5-byte body");
            checkEquals(403, exchange(server, "GET /health HTTP/1.1\r\nHost: evil.example:" + server.port()
                    + "\r\nConnection: close\r\nContent-Length: 5\r\n\r\n", five).status, "a wrong Host with a 5-byte body");
            checkEquals(415, exchange(server, head("POST", "/files", server,
                    "Content-Type: text/plain\r\nContent-Length: 5\r\n"), five).status, "a wrong Content-Type");
            checkEquals(404, exchange(server, head("GET", "/files/" + UNKNOWN_FILE_ID, server, "Content-Length: 5\r\n"),
                    five).status, "a download of an unknown file with a 5-byte body");
            byte[] chunkedFive = concat("5\r\n".getBytes(StandardCharsets.ISO_8859_1),
                    concat(five, "\r\n0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)));
            checkEquals(200, exchange(server, head("GET", "/health", server, "Transfer-Encoding: chunked\r\n"),
                    chunkedFive).status, "GET /health with a complete chunked body");
            Response stored = exchange(server, head("POST", "/files", server, OCTET + "Content-Length: 5\r\n"), five);
            checkEquals(201, stored.status, "an upload of 5 bytes");
            checkEquals(1, listNames(server.dataRoot().resolve("manifests")).size(), "manifests");
            checkTmpIsEmpty(server);
            checkNoWatchThreads();
        });
    }

    /** 40 pieces of 1 KiB, 100 ms apart: 4 seconds in all, eight stall timeouts. */
    private static void slowSteadyUploadSucceeds(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        withServer(tempDir, errBytes, server -> {
            byte[] data = bytes(40 * 1024, 4);
            try (Socket socket = connect(server)) {
                OutputStream out = socket.getOutputStream();
                out.write(head("POST", "/files", server, OCTET + "Content-Length: " + data.length + "\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
                for (int offset = 0; offset < data.length; offset += 1024) {
                    Thread.sleep(100);
                    out.write(data, offset, 1024);
                    out.flush();
                }
                Response response = readOneResponse(socket.getInputStream());
                checkEquals(201, response.status, "status of the slow upload; server log: "
                        + errBytes.toString(StandardCharsets.UTF_8));
            }
            List<String> manifests = listNames(server.dataRoot().resolve("manifests"));
            checkEquals(1, manifests.size(), "manifests");
            String fileId = manifests.get(0).substring(0, manifests.get(0).indexOf('.'));
            FileManifest manifest = server.manifests().read(fileId).orElseThrow();
            checkEquals((long) data.length, manifest.size(), "stored size");
            checkTmpIsEmpty(server);
            check(!errBytes.toString(StandardCharsets.UTF_8).contains("abandoned"),
                    "nothing was abandoned; server log: " + errBytes.toString(StandardCharsets.UTF_8));
        });
    }

    // ---------------------------------------------------------------------
    // Checks
    // ---------------------------------------------------------------------

    /** GET /health on a new connection is answered with 200 within the wait limit. */
    private static void checkHealthAnswers(DistributedFsServer server, String when) throws IOException {
        try (Socket socket = connect(server)) {
            socket.getOutputStream().write(head("GET", "/health", server, "").getBytes(StandardCharsets.ISO_8859_1));
            socket.getOutputStream().flush();
            Response response;
            try {
                response = readOneResponse(socket.getInputStream());
            } catch (SocketTimeoutException e) {
                throw new AssertionError("GET /health got no answer within " + WAIT_MILLIS + " ms " + when
                        + ": the only worker thread is stuck");
            }
            checkEquals(200, response.status, "GET /health " + when);
        }
    }

    /** The server has ended the connection (end of stream or reset) without sending a byte. */
    private static void checkClosedWithoutResponse(Socket socket) throws IOException {
        byte[] received;
        try {
            received = socket.getInputStream().readAllBytes();
        } catch (SocketTimeoutException e) {
            throw new AssertionError("the server did not close the stalled connection within "
                    + (HOLD_MILLIS + WAIT_MILLIS) + " ms");
        } catch (SocketException e) {
            return; // reset: the server closed without answering
        }
        checkEquals("", new String(received, StandardCharsets.ISO_8859_1), "bytes the stalled client received");
    }

    /** No chunk on any node, no manifest, and no workspace left. */
    private static void checkNothingStored(DistributedFsServer server) throws Exception {
        for (int i = 0; i < 3; i++) {
            checkEquals(List.of(), listNames(server.dataRoot().resolve("nodes").resolve("node-" + i)), "node " + i);
        }
        checkEquals(List.of(), listNames(server.dataRoot().resolve("manifests")), "the manifest folder");
        checkTmpIsEmpty(server);
    }

    private static void checkTmpIsEmpty(DistributedFsServer server) throws Exception {
        Path tmp = server.dataRoot().resolve("tmp");
        waitUntil(() -> listNames(tmp).isEmpty(), "the tmp folder is empty, but has " + listNames(tmp));
    }

    /** Every stall watchdog has ended. */
    private static void checkNoWatchThreads() throws Exception {
        waitUntil(() -> Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals(WATCH_THREAD) && thread.isAlive()).count() == 0,
                "no stall watchdog threads are left");
    }

    // ---------------------------------------------------------------------
    // Server and HTTP helpers
    // ---------------------------------------------------------------------

    private interface ServerBody {
        void run(DistributedFsServer server) throws Exception;
    }

    /** One worker thread, the short stall timeout, a fresh data directory; always stopped. */
    private static void withServer(Path tempDir, ByteArrayOutputStream errBytes, ServerBody body) throws Exception {
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(tempDir.resolve("data"),
                        0, 1, 8, 0, 0, nullStream(), err, MAX_UPLOAD_BYTES, CHUNK_SIZE),
                (port, handlerErr, files) -> new FileApiHandler(port, handlerErr, files, STALL_MILLIS));
        try {
            body.run(server);
        } finally {
            server.stop();
        }
    }

    /** A request head with a valid Host and Connection: close, plus {@code headerLines}. */
    private static String head(String method, String target, DistributedFsServer server, String headerLines) {
        return method + " " + target + " HTTP/1.1\r\nHost: 127.0.0.1:" + server.port() + "\r\nConnection: close\r\n"
                + headerLines + "\r\n";
    }

    private static Socket connect(DistributedFsServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(LOOPBACK, server.port()), WAIT_MILLIS);
        socket.setSoTimeout(WAIT_MILLIS);
        return socket;
    }

    private record Response(int status, byte[] body) {
    }

    /** Sends a raw request on a new connection and reads one response. */
    private static Response exchange(DistributedFsServer server, String head, byte[] body) throws IOException {
        try (Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            out.write(head.getBytes(StandardCharsets.ISO_8859_1));
            out.write(body);
            out.flush();
            return readOneResponse(socket.getInputStream());
        }
    }

    /** Reads exactly one response: the head up to the empty line, then Content-Length bytes of body. */
    private static Response readOneResponse(InputStream in) throws IOException {
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
        int length = 0;
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (lines[i].substring(0, colon).trim().equalsIgnoreCase("Content-Length")) {
                length = Integer.parseInt(lines[i].substring(colon + 1).trim());
            }
        }
        return new Response(Integer.parseInt(lines[0].split(" ", 3)[1]), in.readNBytes(length));
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
        byte[] all = new byte[a.length + b.length];
        System.arraycopy(a, 0, all, 0, a.length);
        System.arraycopy(b, 0, all, a.length, b.length);
        return all;
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
            tempDir = Files.createTempDirectory("stalledrequest-test-");
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
