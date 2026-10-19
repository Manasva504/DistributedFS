package com.distributedfs;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Plain-Java tests for DistributedFsServer and FileApiHandler (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.DistributedFsServerTest}
 *
 * <p>Every test starts its own server in this process, on 127.0.0.1 with a port chosen by the
 * operating system and a fresh data directory, and stops it afterwards. Requests go through
 * java.net.http.HttpClient, or through a raw socket for requests HttpClient will not send (a
 * forged or duplicated Host header, an absolute-form target). Every network wait has a time
 * limit, so a hang fails a test instead of blocking the run.
 * The program exits with code 1 if any test fails; skipped tests are reported as skipped.
 */
public class DistributedFsServerTest {

    private static final int TIMEOUT_MILLIS = 10_000;
    private static final InetAddress LOOPBACK = loopback();
    private static final String HEALTH_BODY = "{\"status\":\"ok\"}";

    private static int passed = 0;
    private static int failed = 0;
    private static int skipped = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.DistributedFsServerTest");
            System.exit(2);
            return;
        }

        // GET /health and routing
        runTest("GET /health answers 200 with the status body and JSON headers", DistributedFsServerTest::healthAnswers);
        runTest("a query string is ignored", DistributedFsServerTest::queryStringIsIgnored);
        runTest("HEAD /health is 405 with Allow: GET and no body", DistributedFsServerTest::headIsNotAllowed);
        runTest("other methods on /health are 405 with Allow: GET", DistributedFsServerTest::otherMethodsAreNotAllowed);
        runTest("every other path is 404, matched exactly and never decoded", DistributedFsServerTest::unknownPathsAre404);

        // Host header
        runTest("127.0.0.1 and localhost with the actual port are accepted", DistributedFsServerTest::allowedHosts);
        runTest("every other Host header, a missing one, or two of them is 403", DistributedFsServerTest::refusedHosts);
        runTest("the Host check runs before routing", DistributedFsServerTest::hostCheckComesFirst);
        runTest("an absolute-form request target is refused", DistributedFsServerTest::absoluteTargetIsRefused);
        runTest("the Host rule needs the explicit port, even for port 80", DistributedFsServerTest::hostRuleNeedsPort);

        // Errors
        runTest("error bodies are the fixed JSON and never contain paths or exception text",
                DistributedFsServerTest::errorBodiesAreSafe);
        runTest("an unexpected handler failure is 500 internal_error, logged, and the server keeps serving",
                DistributedFsServerTest::handlerFailureIs500);
        runTest("a full request queue drops the extra connection without a response, then serving continues",
                DistributedFsServerTest::fullQueueDropsConnection);
        runTest("a request the handler sends nothing for is still ended, never left hanging",
                DistributedFsServerTest::unansweredExchangeIsClosed);

        // Binding and lifecycle
        runTest("the server listens on IPv4 127.0.0.1 only", DistributedFsServerTest::bindsToLoopbackOnly);
        runTest("the server is unreachable through IPv6 loopback", DistributedFsServerTest::notReachableThroughIpv6);
        runTest("the first start initializes the data directory and a restart reuses it",
                DistributedFsServerTest::startInitializesAndRestartReuses);
        runTest("a second server on the same data directory is refused after the retry time and the first keeps serving",
                DistributedFsServerTest::secondServerIsRefused);
        runTest("a lock released during the retry time is picked up", DistributedFsServerTest::lockRetryPicksUpRelease);
        runTest("a port in use fails the start, keeps the original error, and releases everything",
                DistributedFsServerTest::portInUseFailsCleanly);
        runTest("an interrupted upload that cannot be recovered safely stops the start before the port opens,"
                + " and its marker is untouched", DistributedFsServerTest::pendingUploadBlocksStart);
        runTest("an interrupted upload is recovered before the port opens, and recovery reports before \"listening\"",
                DistributedFsServerTest::pendingUploadIsRecoveredBeforeListening);
        runTest("an unrecognized folder is refused at once, untouched", DistributedFsServerTest::unrecognizedFolderIsRefused);
        runTest("stop releases the port, the lock and every thread, and a second stop does nothing",
                DistributedFsServerTest::stopReleasesEverything);
        runTest("cleanup always releases the lock last and keeps the original failure, even when other steps fail",
                DistributedFsServerTest::shutDownIsExceptionSafe);

        // Command line
        runTest("valid command lines are parsed", DistributedFsServerTest::validArguments);
        runTest("invalid command lines are usage errors", DistributedFsServerTest::invalidArguments);
        runTest("run reports usage errors (exit 2) and startup failures (exit 1)", DistributedFsServerTest::runExitCodes);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed" + (skipped > 0 ? ", " + skipped + " skipped" : ""));
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // GET /health and routing
    // ---------------------------------------------------------------------

    private static void healthAnswers(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            try (HttpClient client = newClient()) {
                for (String host : List.of("127.0.0.1", "localhost")) {
                    HttpResponse<String> response = client.send(
                            request("http://" + host + ":" + server.port() + "/health").GET().build(),
                            HttpResponse.BodyHandlers.ofString());
                    checkEquals(200, response.statusCode(), "status via " + host);
                    checkEquals(HEALTH_BODY, response.body(), "body via " + host);
                    checkEquals("application/json; charset=utf-8",
                            response.headers().firstValue("Content-Type").orElse(null), "Content-Type");
                    checkEquals("nosniff", response.headers().firstValue("X-Content-Type-Options").orElse(null),
                            "X-Content-Type-Options");
                }
            }
        });
    }

    private static void queryStringIsIgnored(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            RawResponse response = raw(server.port(), get("/health?x=1&y", validHost(server)));
            checkEquals(200, response.status(), "status");
            checkEquals(HEALTH_BODY, response.bodyText(), "body");
        });
    }

    private static void headIsNotAllowed(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            RawResponse response = raw(server.port(), "HEAD /health HTTP/1.1\r\n" + validHost(server)
                    + "Connection: close\r\n\r\n");
            checkEquals(405, response.status(), "status");
            checkEquals("GET", response.header("allow"), "Allow header");
            checkEquals(0, response.body().length, "number of body bytes");
        });
    }

    private static void otherMethodsAreNotAllowed(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            for (String method : List.of("POST", "PUT", "DELETE", "OPTIONS", "PATCH", "get")) {
                RawResponse response = raw(server.port(), method + " /health HTTP/1.1\r\n" + validHost(server)
                        + "Content-Length: 0\r\nConnection: close\r\n\r\n");
                checkErrorResponse(response, ApiError.METHOD_NOT_ALLOWED, server, method);
                checkEquals("GET", response.header("allow"), method + ": Allow header");
            }
        });
    }

    private static void unknownPathsAre404(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            for (String path : List.of("/", "/health/", "/HEALTH", "/healthz", "/health/x",
                    "/files/0123456789abcdef0123456789abcdef", "/%68ealth", "/health%2F", "/./health", "/x/../health")) {
                RawResponse response = raw(server.port(), get(path, validHost(server)));
                checkErrorResponse(response, ApiError.NOT_FOUND, server, path);
            }

            // "//health" parses as an authority ("health") with an empty path, which no context can
            // match (context paths start with "/"). The JDK's server answers 404 itself, with its own
            // fixed body, before this server's handler runs. Still a 404 that reveals nothing.
            RawResponse jdkAnswer = raw(server.port(), get("//health", validHost(server)));
            checkEquals(404, jdkAnswer.status(), "//health: status");
            checkEquals("<h1>404 Not Found</h1>No context found for request", jdkAnswer.bodyText(), "//health: body");
        });
    }

    // ---------------------------------------------------------------------
    // Host header
    // ---------------------------------------------------------------------

    private static void allowedHosts(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            int port = server.port();
            for (String host : List.of("127.0.0.1:" + port, "localhost:" + port, "LocalHost:" + port, "LOCALHOST:" + port)) {
                RawResponse response = raw(port, get("/health", "Host: " + host + "\r\n"));
                checkEquals(200, response.status(), "status for Host: " + host);
            }
        });
    }

    private static void refusedHosts(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            int port = server.port();
            List<String> hostLines = List.of(
                    "Host: evil.example:" + port + "\r\n",
                    "Host: 127.0.0.1:" + (port == 65535 ? port - 1 : port + 1) + "\r\n",
                    "Host: 127.0.0.1\r\n",
                    "Host: localhost\r\n",
                    "Host: 127.0.0.1:0" + port + "\r\n",
                    "Host: 127.0.0.1:+" + port + "\r\n",
                    "Host: [::1]:" + port + "\r\n",
                    "Host: 127.0.0.01:" + port + "\r\n",
                    "Host: localhost.:" + port + "\r\n",
                    "Host: user@127.0.0.1:" + port + "\r\n",
                    "Host: 127.0.0.1:" + port + ", localhost:" + port + "\r\n",
                    "Host: 127.0.0.1:" + port + ":" + port + "\r\n",
                    "Host: localhost :" + port + "\r\n",
                    "Host: 127.0.0.1:" + port + "\r\nHost: 127.0.0.1:" + port + "\r\n",
                    "");
            for (String hostLine : hostLines) {
                RawResponse response = raw(port, get("/health", hostLine));
                checkErrorResponse(response, ApiError.HOST_NOT_ALLOWED, server, "[" + hostLine.trim() + "]");
            }
            // HTTP/1.0 does not require a Host header, but this server does.
            RawResponse http10 = raw(port, "GET /health HTTP/1.0\r\n\r\n");
            checkErrorResponse(http10, ApiError.HOST_NOT_ALLOWED, server, "HTTP/1.0 without Host");
        });
    }

    private static void hostCheckComesFirst(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            String badHost = "Host: evil.example:" + server.port() + "\r\n";
            checkErrorResponse(raw(server.port(), get("/no-such-path", badHost)), ApiError.HOST_NOT_ALLOWED, server,
                    "unknown path");
            checkErrorResponse(raw(server.port(), "DELETE /health HTTP/1.1\r\n" + badHost + "Connection: close\r\n\r\n"),
                    ApiError.HOST_NOT_ALLOWED, server, "wrong method");
        });
    }

    private static void absoluteTargetIsRefused(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            String target = "http://127.0.0.1:" + server.port() + "/health";
            RawResponse response = raw(server.port(), get(target, validHost(server)));
            checkErrorResponse(response, ApiError.HOST_NOT_ALLOWED, server, target);
        });
    }

    private static void hostRuleNeedsPort(Path tempDir) {
        check(FileApiHandler.isAllowedHost(List.of("127.0.0.1:80"), 80), "127.0.0.1:80 on port 80");
        check(FileApiHandler.isAllowedHost(List.of("localhost:80"), 80), "localhost:80 on port 80");
        check(!FileApiHandler.isAllowedHost(List.of("127.0.0.1"), 80), "a bare 127.0.0.1 is refused even on port 80");
        check(!FileApiHandler.isAllowedHost(List.of("localhost"), 80), "a bare localhost is refused even on port 80");
        check(!FileApiHandler.isAllowedHost(null, 80), "no Host header");
        check(!FileApiHandler.isAllowedHost(List.of(), 80), "an empty header list");
        check(!FileApiHandler.isAllowedHost(List.of("127.0.0.1:80", "127.0.0.1:80"), 80), "two Host headers");
        check(!FileApiHandler.isAllowedHost(List.of("localhost:080"), 80), "a leading zero in the port");
        check(!FileApiHandler.isAllowedHost(List.of("localho" + (char) 0x017F + "t:80"), 80), "a non-ASCII look-alike letter");
    }

    // ---------------------------------------------------------------------
    // Errors
    // ---------------------------------------------------------------------

    private static void errorBodiesAreSafe(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            checkErrorResponse(raw(server.port(), get("/nope", validHost(server))), ApiError.NOT_FOUND, server, "404");
            checkErrorResponse(raw(server.port(), "PUT /health HTTP/1.1\r\n" + validHost(server)
                    + "Content-Length: 0\r\nConnection: close\r\n\r\n"), ApiError.METHOD_NOT_ALLOWED, server, "405");
            checkErrorResponse(raw(server.port(), get("/health", "Host: evil.example\r\n")),
                    ApiError.HOST_NOT_ALLOWED, server, "403");
        });
    }

    private static void handlerFailureIs500(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        TestHandler[] handler = new TestHandler[1];
        DistributedFsServer server = DistributedFsServer.start(config(tempDir.resolve("data"), 4, 64, err),
                (port, errStream, files) -> handler[0] = new TestHandler(port, errStream, files));
        try {
            handler[0].failWith = new IllegalStateException("simulated failure in C:\\secret\\folder");
            RawResponse failedResponse = raw(server.port(), get("/health", validHost(server)));
            checkErrorResponse(failedResponse, ApiError.INTERNAL_ERROR, server, "500");
            check(!failedResponse.bodyText().contains("secret"), "the failure's message must not reach the client");
            checkContains(errBytes.toString(StandardCharsets.UTF_8), "simulated failure in C:\\secret\\folder");

            handler[0].failWith = null;
            RawResponse next = raw(server.port(), get("/health", validHost(server)));
            checkEquals(200, next.status(), "the next request's status");
        } finally {
            server.stop();
        }
    }

    private static void fullQueueDropsConnection(Path tempDir) throws Exception {
        TestHandler[] handler = new TestHandler[1];
        // One worker thread and room for one waiting request.
        DistributedFsServer server = DistributedFsServer.start(config(tempDir.resolve("data"), 1, 1, nullStream()),
                (port, err, files) -> handler[0] = new TestHandler(port, err, files));
        try (Socket first = new Socket(); Socket second = new Socket(); Socket third = new Socket()) {
            handler[0].gate = new CountDownLatch(1);
            String request = get("/health", validHost(server));

            // 1. The first request occupies the only worker thread (it waits at the gate).
            send(first, server.port(), request);
            check(handler[0].entered.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    "the first request should reach the handler");
            // 2. The second request fills the queue.
            send(second, server.port(), request);
            waitUntil(() -> server.executor().getQueue().size() == 1, "the second request should wait in the queue");
            // 3. The third request is rejected: the JDK's dispatcher closes its connection.
            send(third, server.port(), request);
            third.setSoTimeout(5_000);
            String outcome;
            try {
                int firstByte = third.getInputStream().read();
                outcome = firstByte == -1 ? "closed" : "data";
            } catch (SocketTimeoutException e) {
                outcome = "no answer yet"; // would mean the request was queued or delayed, not dropped
            } catch (IOException e) {
                outcome = "closed"; // a reset is also a dropped connection
            }
            checkEquals("closed", outcome, "what happened to the third connection while the gate was shut");

            // 4. Open the gate: the first two requests are answered normally.
            handler[0].gate.countDown();
            checkEquals(200, readResponse(first).status(), "first request");
            checkEquals(200, readResponse(second).status(), "second request");
            checkEquals(200, raw(server.port(), request).status(), "a new request after the overload");
        } finally {
            if (handler[0] != null && handler[0].gate != null) {
                handler[0].gate.countDown();
            }
            server.stop();
        }
    }

    private static void unansweredExchangeIsClosed(Path tempDir) throws Exception {
        TestHandler[] handler = new TestHandler[1];
        DistributedFsServer server = DistributedFsServer.start(config(tempDir.resolve("data"), 4, 64, nullStream()),
                (port, err, files) -> handler[0] = new TestHandler(port, err, files));
        try (Socket socket = new Socket()) {
            handler[0].sendNothing = true;
            send(socket, server.port(), get("/health", validHost(server)));
            socket.setSoTimeout(5_000);
            String outcome;
            try {
                outcome = socket.getInputStream().read() == -1 ? "closed" : "data";
            } catch (SocketTimeoutException e) {
                outcome = "left hanging";
            } catch (IOException e) {
                outcome = "closed";
            }
            checkEquals("closed", outcome, "what happened to a request the handler sent nothing for");

            handler[0].sendNothing = false;
            checkEquals(200, raw(server.port(), get("/health", validHost(server))).status(), "the next request's status");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Binding and lifecycle
    // ---------------------------------------------------------------------

    private static void bindsToLoopbackOnly(Path tempDir) throws Exception {
        InetAddress other = nonLoopbackIpv4Address();
        withServer(tempDir, server -> {
            checkEquals(200, raw(server.port(), get("/health", validHost(server))).status(), "via 127.0.0.1");
            if (other == null) {
                throw new SkipTest("SKIPPED: this machine has no non-loopback IPv4 address to try");
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(other, server.port()), 3_000);
                throw new AssertionError("connected through " + other.getHostAddress() + ", which must be refused");
            } catch (IOException expected) {
                // refused: the server listens on 127.0.0.1 only
            }
        });
    }

    private static void notReachableThroughIpv6(Path tempDir) throws Exception {
        withServer(tempDir, server -> {
            InetAddress ipv6Loopback = InetAddress.getByName("::1");
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(ipv6Loopback, server.port()), 3_000);
                throw new AssertionError("connected through [::1], which must be refused");
            } catch (IOException expected) {
                // refused: the server is bound to the IPv4 loopback address only
            }
        });
    }

    private static void startInitializesAndRestartReuses(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        DistributedFsServer first = DistributedFsServer.start(config(root, 4, 64, nullStream(), out));
        int firstPort = first.port();
        first.stop();
        String output = outBytes.toString(StandardCharsets.UTF_8);
        checkContains(output, "Initialized a new data directory");
        checkContains(output, "listening on http://127.0.0.1:" + firstPort + "/");
        check(Files.isRegularFile(root.resolve(DataDirectory.MARKER_NAME)), "the marker should exist");

        outBytes.reset();
        DistributedFsServer second = DistributedFsServer.start(config(root, 4, 64, nullStream(), out));
        try {
            checkEquals(200, raw(second.port(), get("/health", validHost(second))).status(), "status after restart");
            check(!outBytes.toString(StandardCharsets.UTF_8).contains("Initialized"), "a restart must not initialize again");
        } finally {
            second.stop();
        }
    }

    private static void secondServerIsRefused(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        DistributedFsServer first = DistributedFsServer.start(config(root, 4, 64, nullStream()));
        try {
            DistributedFsServer.Config secondConfig = new DistributedFsServer.Config(root, 0, 4, 64, 0, 300,
                    nullStream(), nullStream());
            long start = System.nanoTime();
            DataDirectoryException e = expectThrows(DataDirectoryException.class,
                    () -> DistributedFsServer.start(secondConfig));
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            checkEquals(DataDirectoryException.Reason.IN_USE, e.reason(), "reason");
            check(millis >= 250 && millis < 5_000, "the refusal should come after the 300 ms retry time, came after "
                    + millis + " ms");
            checkEquals(200, raw(first.port(), get("/health", validHost(first))).status(), "the first server's status");
        } finally {
            first.stop();
        }
    }

    private static void lockRetryPicksUpRelease(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        DataDirectory holder = DataDirectory.open(root);
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(300);
                holder.close();
            } catch (InterruptedException | IOException e) {
                throw new IllegalStateException(e);
            }
        });
        releaser.start();
        long start = System.nanoTime();
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(root, 0, 4, 64, 0, 2_000,
                nullStream(), nullStream()));
        try {
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            check(millis >= 250, "the start should have waited for the lock, but took " + millis + " ms");
            checkEquals(200, raw(server.port(), get("/health", validHost(server))).status(), "status");
        } finally {
            server.stop();
            releaser.join(TIMEOUT_MILLIS);
        }
    }

    private static void portInUseFailsCleanly(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        try (ServerSocket blocker = new ServerSocket(0, 50, LOOPBACK)) {
            DistributedFsServer.Config config = new DistributedFsServer.Config(root, blocker.getLocalPort(), 4, 64, 0, 0,
                    nullStream(), nullStream());
            BindException e = expectThrows(BindException.class, () -> DistributedFsServer.start(config));
            checkEquals(0, e.getSuppressed().length, "cleanup errors attached to the original failure");
        }
        checkLockIsFree(root);
        waitUntil(() -> liveThreads("distributedfs-http-") == 0, "no request threads should be left");
        DistributedFsServer server = DistributedFsServer.start(config(root, 4, 64, nullStream()));
        server.stop(); // the data directory can be used again
    }

    private static void pendingUploadBlocksStart(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        DataDirectory.open(root).close();
        ManifestStore store = new ManifestStore(root.resolve("manifests"));
        String fileId = "0123456789abcdef0123456789abcdef";
        store.createPending(new ManifestStore.PendingUpload(fileId, 2));
        Path marker = root.resolve("manifests").resolve(fileId + ".pending");
        // A valid marker is now recovered; a corrupt one cannot be, so startup must fail closed.
        Files.writeString(marker, "format=1\nfileId=" + fileId + "\nchunkCount=02\n"); // a leading zero
        byte[] markerBefore = Files.readAllBytes(marker);
        int port = freePort();

        DistributedFsServer.Config config = new DistributedFsServer.Config(root, port, 4, 64, 0, 0,
                nullStream(), nullStream());
        // The handler is created only after the HTTP server has been created and bound, so if the
        // check for interrupted uploads comes first, as it must, the handler is never created.
        boolean[] handlerCreated = {false};
        IOException e = expectThrows(IOException.class, () -> DistributedFsServer.start(config, (p, err, files) -> {
            handlerCreated[0] = true;
            return new FileApiHandler(p, err, files);
        }));

        check(!handlerCreated[0], "the HTTP server must not be set up while an upload is interrupted");
        checkContains(e.getMessage(), "interrupted upload");
        checkContains(e.getMessage(), fileId);
        check(e instanceof UnsafeRecoveryException unsafe && !unsafe.changedAnything(),
                "recovery reports that it changed nothing: " + e);
        check(Arrays.equals(markerBefore, Files.readAllBytes(marker)), "the marker must be unchanged");
        checkLockIsFree(root);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(LOOPBACK, port), 3_000);
            throw new AssertionError("the port must never have been opened");
        } catch (IOException expected) {
            // nothing listens on the port
        }
    }

    private static void pendingUploadIsRecoveredBeforeListening(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        DataDirectory.open(root).close();
        ManifestStore store = new ManifestStore(root.resolve("manifests"));
        String fileId = "0123456789abcdef0123456789abcdef";
        store.createPending(new ManifestStore.PendingUpload(fileId, 2));
        Path marker = root.resolve("manifests").resolve(fileId + ".pending");
        Path chunk = Files.writeString(root.resolve("nodes").resolve("node-1").resolve(fileId + "_0"), "chunk");
        Path orphan = Files.writeString(root.resolve("nodes").resolve("node-2").resolve("c".repeat(32) + "_0"), "orphan");

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        DistributedFsServer.Config config = new DistributedFsServer.Config(root, 0, 4, 64, 0, 0, out, nullStream());
        // The handler is created after the HTTP server is bound but before it starts listening, so
        // the marker must already be gone when it is created.
        boolean[] markerGoneAtHandlerCreation = {false};
        DistributedFsServer server = DistributedFsServer.start(config, (p, err, files) -> {
            markerGoneAtHandlerCreation[0] = !Files.exists(marker);
            return new FileApiHandler(p, err, files);
        });
        try {
            check(markerGoneAtHandlerCreation[0], "recovery must finish before the HTTP server is set up");
            check(!Files.exists(marker), "the marker was deleted");
            check(!Files.exists(chunk), "the incomplete upload's chunk was deleted");
            String printed = outBytes.toString(StandardCharsets.UTF_8);
            int recovery = printed.indexOf("Recovery: 1 interrupted upload(s): 0 committed, 1 rolled back");
            int listening = printed.indexOf("listening");
            check(recovery >= 0, "the recovery summary is printed: " + printed);
            check(recovery < listening, "the recovery summary comes before \"listening\": " + printed);
            int orphanLine = printed.indexOf("  ORPHAN_CHUNK " + "c".repeat(32) + "_0 node 2 " + orphan);
            check(orphanLine >= 0 && orphanLine < listening, "the chunk scan reports before \"listening\": " + printed);
            check(Files.exists(orphan), "the orphan was only reported, not deleted");
        } finally {
            server.stop();
        }
    }

    private static void unrecognizedFolderIsRefused(Path tempDir) throws Exception {
        Path root = Files.createDirectory(tempDir.resolve("photos"));
        Files.writeString(root.resolve("notes.txt"), "my notes");
        DistributedFsServer.Config config = new DistributedFsServer.Config(root, 0, 4, 64, 0, 2_000,
                nullStream(), nullStream());
        long start = System.nanoTime();
        DataDirectoryException e = expectThrows(DataDirectoryException.class, () -> DistributedFsServer.start(config));
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        checkEquals(DataDirectoryException.Reason.UNRECOGNIZED, e.reason(), "reason");
        check(millis < 1_000, "only 'in use' is retried; this refusal took " + millis + " ms");
        checkEquals(List.of("notes.txt"), listNames(root), "the folder's contents");
    }

    private static void stopReleasesEverything(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        DistributedFsServer server = DistributedFsServer.start(config(root, 4, 64, nullStream(),
                new PrintStream(outBytes, true, StandardCharsets.UTF_8)));
        int port = server.port();
        checkEquals(200, raw(port, get("/health", validHost(server))).status(), "status before stop");

        server.stop();

        check(server.executor().isTerminated(), "the request thread pool should be terminated");
        waitUntil(() -> liveThreads("distributedfs-http-") == 0, "no request threads should be left");
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(LOOPBACK, port), 3_000);
            throw new AssertionError("the port must be closed after stop");
        } catch (IOException expected) {
            // nothing listens any more
        }
        try (ServerSocket rebind = new ServerSocket(port, 50, LOOPBACK)) {
            checkEquals(port, rebind.getLocalPort(), "the port can be bound again");
        }
        checkLockIsFree(root);

        server.stop(); // does nothing the second time
        String output = outBytes.toString(StandardCharsets.UTF_8);
        checkEquals(1, output.split("server stopped", -1).length - 1, "number of 'stopped' messages");
    }

    private static void shutDownIsExceptionSafe(Path tempDir) throws Exception {
        for (boolean withOriginalFailure : new boolean[] {true, false}) {
            Path root = tempDir.resolve("data-" + withOriginalFailure);
            DataDirectory data = DataDirectory.open(root);
            ThreadPoolExecutor failingExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>()) {
                @Override
                public void shutdown() {
                    throw new IllegalStateException("simulated: the thread pool did not shut down");
                }
            };
            ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
            PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
            IOException original = withOriginalFailure ? new IOException("the original startup failure") : null;
            try {
                DistributedFsServer.shutDown(new FailingHttpServer(), failingExecutor, data, 0, err, original);

                checkLockIsFree(root); // released last, despite both earlier failures
                String log = errBytes.toString(StandardCharsets.UTF_8);
                checkContains(log, "simulated: the HTTP server did not stop");
                checkContains(log, "simulated: the thread pool did not shut down");
                if (original != null) {
                    checkEquals("the original startup failure", original.getMessage(), "the original failure's message");
                    checkEquals(2, original.getSuppressed().length, "cleanup failures attached to the original");
                }
            } finally {
                failingExecutor.shutdownNow();
                data.close();
            }
        }
    }

    // ---------------------------------------------------------------------
    // Command line
    // ---------------------------------------------------------------------

    private static void validArguments(Path tempDir) throws Exception {
        DistributedFsServer.Arguments a = DistributedFsServer.parseArgs(new String[] {"--data-dir", "data"});
        checkEquals(Path.of("data"), a.dataDir(), "data dir");
        checkEquals(8080, a.port(), "default port");
        checkEquals(0, DistributedFsServer.parseArgs(new String[] {"--port", "0", "--data-dir", "d"}).port(), "port 0");
        checkEquals(1, DistributedFsServer.parseArgs(new String[] {"--data-dir", "d", "--port", "1"}).port(), "port 1");
        checkEquals(65535, DistributedFsServer.parseArgs(new String[] {"--data-dir", "d", "--port", "65535"}).port(),
                "port 65535");
    }

    private static void invalidArguments(Path tempDir) {
        List<String[]> invalid = List.of(
                new String[] {},
                new String[] {"--port", "9000"},
                new String[] {"--data-dir"},
                new String[] {"--data-dir", ""},
                new String[] {"--data-dir", "bad" + (char) 0 + "path"},
                new String[] {"--data-dir", "a", "--data-dir", "b"},
                new String[] {"--data-dir", "d", "--port", "9000", "--port", "9001"},
                new String[] {"--data-dir", "d", "--port"},
                new String[] {"--data-dir", "d", "--port", "abc"},
                new String[] {"--data-dir", "d", "--port", "-1"},
                new String[] {"--data-dir", "d", "--port", "65536"},
                new String[] {"--data-dir", "d", "--port", "99999"},
                new String[] {"--data-dir", "d", "--port", "123456"},
                new String[] {"--data-dir", "d", "--port", "+80"},
                new String[] {"--data-dir", "d", "--port", "08"},
                new String[] {"--data-dir", "d", "--port", "00"},
                new String[] {"--data-dir", "d", "--port", ""},
                new String[] {"--data-dir", "d", "--verbose"},
                new String[] {"data"},
                new String[] {"--data-dir", "d", "extra"});
        for (String[] args : invalid) {
            expectThrows(DistributedFsServer.UsageException.class, () -> DistributedFsServer.parseArgs(args));
        }
    }

    private static void runExitCodes(Path tempDir) throws Exception {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);
        checkEquals(DistributedFsServer.EXIT_USAGE, DistributedFsServer.run(new String[] {"--port", "x"}, nullStream(), err),
                "exit code for a usage error");
        checkContains(errBytes.toString(StandardCharsets.UTF_8), DistributedFsServer.USAGE);

        errBytes.reset();
        Path file = Files.writeString(tempDir.resolve("a-file"), "not a folder");
        int exitCode = DistributedFsServer.run(new String[] {"--data-dir", file.toString(), "--port", "0"}, nullStream(), err);
        checkEquals(DistributedFsServer.EXIT_FAILURE, exitCode, "exit code for a startup failure");
        checkContains(errBytes.toString(StandardCharsets.UTF_8), "the server could not start");
        checkContains(errBytes.toString(StandardCharsets.UTF_8), "not a folder");
        checkEquals("not a folder", Files.readString(file), "the file is unchanged");
    }

    // ---------------------------------------------------------------------
    // Server helpers
    // ---------------------------------------------------------------------

    private interface ServerBody {
        void run(DistributedFsServer server) throws Exception;
    }

    /** Starts a server on a fresh data directory, runs {@code body}, and always stops the server. */
    private static void withServer(Path tempDir, ServerBody body) throws Exception {
        DistributedFsServer server = DistributedFsServer.start(config(tempDir.resolve("data"), 4, 64, nullStream()));
        try {
            body.run(server);
        } finally {
            server.stop();
        }
    }

    /** Test settings: port 0, no grace period on stop, no lock retry. */
    private static DistributedFsServer.Config config(Path dataDir, int threads, int queue, PrintStream err) {
        return config(dataDir, threads, queue, err, nullStream());
    }

    private static DistributedFsServer.Config config(Path dataDir, int threads, int queue, PrintStream err,
                                                     PrintStream out) {
        return new DistributedFsServer.Config(dataDir, 0, threads, queue, 0, 0, out, err);
    }

    /** A handler whose GET /health can be made to fail, to wait at a gate, or to send nothing at all. */
    private static final class TestHandler extends FileApiHandler {
        final CountDownLatch entered = new CountDownLatch(1);
        volatile CountDownLatch gate;
        volatile RuntimeException failWith;
        volatile boolean sendNothing;

        TestHandler(int port, PrintStream err, FileService files) {
            super(port, err, files);
        }

        @Override
        void respondHealth(HttpExchange exchange) throws IOException {
            RuntimeException failure = failWith;
            if (failure != null) {
                throw failure;
            }
            if (sendNothing) {
                return; // no headers, no body: only the handler's final close can end this request
            }
            CountDownLatch currentGate = gate;
            if (currentGate != null && entered.getCount() > 0) {
                entered.countDown();
                try {
                    if (!currentGate.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                        throw new IllegalStateException("the test never opened the gate");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            super.respondHealth(exchange);
        }
    }

    /** An HttpServer whose stop always fails, to test that cleanup goes on regardless. */
    private static final class FailingHttpServer extends HttpServer {
        @Override
        public void stop(int delay) {
            throw new IllegalStateException("simulated: the HTTP server did not stop");
        }

        @Override
        public void bind(InetSocketAddress address, int backlog) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setExecutor(Executor executor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Executor getExecutor() {
            return null;
        }

        @Override
        public HttpContext createContext(String path, HttpHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpContext createContext(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeContext(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeContext(HttpContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InetSocketAddress getAddress() {
            return null;
        }
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

    private static HttpRequest.Builder request(String uri) {
        return HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofMillis(TIMEOUT_MILLIS));
    }

    private static String validHost(DistributedFsServer server) {
        return "Host: 127.0.0.1:" + server.port() + "\r\n";
    }

    /** A GET request with the given Host line(s), asking the server to close the connection after answering. */
    private static String get(String target, String hostLines) {
        return "GET " + target + " HTTP/1.1\r\n" + hostLines + "Connection: close\r\n\r\n";
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

    /** Sends a raw request on a new connection and reads the response until the server closes it. */
    private static RawResponse raw(int port, String request) throws IOException {
        try (Socket socket = new Socket()) {
            send(socket, port, request);
            return readResponse(socket);
        }
    }

    private static void send(Socket socket, int port, String request) throws IOException {
        socket.connect(new InetSocketAddress(LOOPBACK, port), TIMEOUT_MILLIS);
        socket.setSoTimeout(TIMEOUT_MILLIS);
        OutputStream out = socket.getOutputStream();
        out.write(request.getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    private static RawResponse readResponse(Socket socket) throws IOException {
        socket.setSoTimeout(TIMEOUT_MILLIS);
        InputStream in = socket.getInputStream();
        byte[] all = in.readAllBytes();
        String text = new String(all, StandardCharsets.ISO_8859_1);
        int headerEnd = text.indexOf("\r\n\r\n");
        if (headerEnd < 0) {
            throw new AssertionError("no complete HTTP response, received: \"" + text + "\"");
        }
        String[] lines = text.substring(0, headerEnd).split("\r\n");
        String[] statusParts = lines[0].split(" ", 3);
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
        }
        byte[] body = Arrays.copyOfRange(all, headerEnd + 4, all.length);
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
        for (String leak : List.of(server.dataRoot().toString(), "\\", "Exception", "at com.", "C:")) {
            check(!body.contains(leak), what + ": the error body contains \"" + leak + "\"");
        }
    }

    // ---------------------------------------------------------------------
    // Other helpers
    // ---------------------------------------------------------------------

    private interface Condition {
        boolean holds();
    }

    /** Polls {@code condition} every 20 ms for up to 5 seconds. */
    private static void waitUntil(Condition condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out: " + what);
            }
            Thread.sleep(20);
        }
    }

    private static long liveThreads(String namePrefix) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.isAlive() && thread.getName().startsWith(namePrefix))
                .count();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, LOOPBACK)) {
            return socket.getLocalPort();
        }
    }

    /** An IPv4 address of this machine that is not loopback, or null if there is none. */
    private static InetAddress nonLoopbackIpv4Address() throws IOException {
        for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                continue;
            }
            for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
                if (address instanceof Inet4Address && !address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
                    return address;
                }
            }
        }
        return null;
    }

    /** Proves that nothing, in this process or another, still holds the data directory's lock. */
    private static void checkLockIsFree(Path root) throws IOException {
        try (FileChannel channel = FileChannel.open(root.resolve(DataDirectory.LOCK_NAME), StandardOpenOption.WRITE)) {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new AssertionError("this process still holds the lock on " + root);
            }
            check(lock != null, "another process holds the lock on " + root);
            lock.release();
        }
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

    /** Thrown by a test that cannot run here; runTest reports it as skipped, never as passed. */
    private static final class SkipTest extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SkipTest(String reason) {
            super(reason);
        }
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("distributedfsserver-test-");
            body.run(tempDir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (SkipTest e) {
            skipped++;
            System.out.println("SKIP  " + name);
            System.out.println("      " + e.getMessage());
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
