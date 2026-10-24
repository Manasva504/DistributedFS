package com.distributedfs;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Shared helpers for the storage-node test suites (NodeDataDirectoryTest, NodeApiTest,
 * RemoteStorageNodeTest, RemoteClusterTest, StorageNodeServerTest): the same tiny test framework
 * as the other suites, node servers on port 0 with short time limits, raw HTTP over a socket, and
 * stand-in servers that answer exactly as a test scripts them. It is not a suite itself.
 */
final class NodeTestKit {

    private NodeTestKit() {
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    /** A test body that receives a fresh temporary directory. */
    interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    /** A piece of code that is expected to throw. */
    interface ThrowingAction {
        void run() throws Exception;
    }

    /** Thrown by a test that cannot run here; it is reported as skipped, never as passed. */
    static final class SkipTest extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SkipTest(String reason) {
            super(reason);
        }
    }

    /** Counts results and prints PASS, FAIL or SKIP per test, then the summary line. */
    static final class Runner {
        private int passed = 0;
        private int failed = 0;
        private int skipped = 0;
        private final String prefix;

        Runner(String tempPrefix) {
            this.prefix = tempPrefix;
        }

        void run(String name, TestBody body) {
            Path tempDir = null;
            try {
                tempDir = Files.createTempDirectory(prefix);
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

        /** Prints the summary and exits with code 1 if any test failed. */
        void finish() {
            System.out.println();
            System.out.println(passed + " passed, " + failed + " failed" + (skipped > 0 ? ", " + skipped + " skipped" : ""));
            if (failed > 0) {
                System.exit(1);
            }
        }
    }

    static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    static void checkEquals(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    static void checkContains(String text, String expectedPart) {
        if (text == null || !text.contains(expectedPart)) {
            throw new AssertionError("expected text containing \"" + expectedPart + "\" but was \"" + text + "\"");
        }
    }

    static <T extends Throwable> T expectThrows(Class<T> expectedType, String what, ThrowingAction action) {
        String prefix = what.isEmpty() ? "" : what + ": ";
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError(prefix + "expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError(prefix + "expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }

    /**
     * Expects a plain "could not tell" IOException: an IOException that is none of the answers
     * that mean something specific (absent, already exists, or an unknown store outcome).
     */
    static IOException expectCannotTell(String what, ThrowingAction action) {
        IOException e = expectThrows(IOException.class, what, action);
        check(!(e instanceof ChunkNotFoundException), what + ": a failure must not be reported as an absent chunk: " + e);
        check(!(e instanceof java.nio.file.FileAlreadyExistsException), what + ": unexpected FileAlreadyExistsException: " + e);
        check(!(e instanceof AmbiguousStoreException), what + ": unexpected AmbiguousStoreException: " + e);
        return e;
    }

    // ---------------------------------------------------------------------
    // Node servers
    // ---------------------------------------------------------------------

    /** Short limits so that stall and timeout tests finish quickly. */
    static RemoteNodeConfig fastConfig() {
        return new RemoteNodeConfig(1_000, 2_000, 3_000, 600, 64 * 1024, 64 * 1024 * 1024);
    }

    /** Collects a node's output lines (its request log) for tests to inspect. */
    static final class Lines extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        @Override
        public synchronized void write(int b) {
            bytes.write(b);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            bytes.write(b, off, len);
        }

        synchronized String text() {
            return bytes.toString(StandardCharsets.UTF_8);
        }

        PrintStream stream() {
            return new PrintStream(this, true, StandardCharsets.UTF_8);
        }
    }

    /** A running in-process node server and its output. */
    record NodeHandle(StorageNodeServer server, Lines out, Lines err) implements AutoCloseable {
        int port() {
            return server.port();
        }

        String nodeId() {
            return server.nodeId();
        }

        Path chunksDir() {
            return server.chunksDir();
        }

        @Override
        public void close() {
            server.stop();
        }
    }

    static NodeHandle startNode(Path folder) throws IOException {
        return startNode(folder, 16 * 1024 * 1024, 600, 2_000);
    }

    static NodeHandle startNode(Path folder, long maxChunkBytes, long stallMillis, long lockWaitMillis) throws IOException {
        Lines out = new Lines();
        Lines err = new Lines();
        StorageNodeServer server = StorageNodeServer.start(new StorageNodeServer.Config(folder, 0, 4, 16, 0, 0,
                out.stream(), err.stream(), maxChunkBytes, stallMillis, lockWaitMillis));
        return new NodeHandle(server, out, err);
    }

    // ---------------------------------------------------------------------
    // The main server with remote nodes
    // ---------------------------------------------------------------------

    /** Starts the DistributedFS server on port 0, with remote nodes on {@code nodePorts} (empty: local folders). */
    static DistributedFsServer startServer(Path dataDir, List<Integer> nodePorts, Lines out, Lines err, int chunkSize)
            throws IOException {
        return DistributedFsServer.start(new DistributedFsServer.Config(dataDir, 0, 4, 64, 0, 0, out.stream(),
                err.stream(), 64L * 1024 * 1024, chunkSize, nodePorts, fastConfig()));
    }

    static RawResponse upload(int port, byte[] data) throws IOException {
        return raw(port, "POST /files HTTP/1.1\r\n" + host(port) + "\r\nContent-Type: application/octet-stream"
                + "\r\nContent-Length: " + data.length, data);
    }

    static RawResponse download(int port, String fileId) throws IOException {
        return raw(port, "GET /files/" + fileId + " HTTP/1.1\r\n" + host(port), null);
    }

    /** The file ID in a 201 upload response. */
    static String fileId(RawResponse response) {
        java.util.regex.Matcher id = Pattern.compile("\"fileId\":\"([0-9a-f]{32})\"").matcher(response.text());
        check(response.status() == 201 && id.find(), "not a successful upload: " + response.status() + " " + response.text());
        return id.group(1);
    }

    /**
     * An HTTP proxy in front of one storage node, which can lose a store's answer: in
     * {@link Mode#DROP_STORE_REPLIES} it passes a store to the node, waits for the node's complete
     * answer (so the chunk is stored) and then resets the client's connection instead of passing
     * the answer on. In {@link Mode#DEAD_AFTER_DROP} it does that once and then resets every later
     * connection, as a node that became unreachable. The Host header is rewritten for the node.
     */
    static final class Proxy implements AutoCloseable {
        enum Mode { PASS, DROP_STORE_REPLIES, DEAD_AFTER_DROP }

        private final SocketStub stub;
        private volatile boolean dead = false;
        final AtomicInteger droppedStores = new AtomicInteger();

        Proxy(int nodePort, Mode mode) throws IOException {
            stub = new SocketStub(client -> {
                Request request = readRequest(client);
                if (dead) {
                    reset(client);
                    return;
                }
                byte[] answer = forward(nodePort, request);
                boolean store = request.isMethod("POST") && request.head().contains(" /chunks/");
                if (store && mode != Mode.PASS) {
                    droppedStores.incrementAndGet();
                    if (mode == Mode.DEAD_AFTER_DROP) {
                        dead = true;
                    }
                    reset(client);
                    return;
                }
                OutputStream out = client.getOutputStream();
                out.write(answer);
                out.flush();
                client.close();
            });
        }

        int port() {
            return stub.port();
        }

        private static byte[] forward(int nodePort, Request request) throws IOException {
            String head = request.head().replaceFirst("(?im)^host:.*$", "Host: 127.0.0.1:" + nodePort);
            try (Socket node = new Socket(InetAddress.getLoopbackAddress(), nodePort)) {
                node.setSoTimeout(10_000);
                OutputStream out = node.getOutputStream();
                out.write(head.getBytes(StandardCharsets.ISO_8859_1));
                out.write(request.body());
                out.flush();
                return node.getInputStream().readAllBytes(); // the node closes every connection after one answer
            }
        }

        @Override
        public void close() throws IOException {
            stub.close();
        }
    }

    // ---------------------------------------------------------------------
    // Raw HTTP
    // ---------------------------------------------------------------------

    /** A response read off a socket: status (-1 if none came), headers (lowercase names) and body. */
    record RawResponse(int status, Map<String, List<String>> headers, byte[] body) {
        String header(String name) {
            List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
            return values == null || values.isEmpty() ? null : values.get(0);
        }

        String text() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    /**
     * Sends {@code head} (request line and headers, without the blank line; "Connection: close" is
     * added) and {@code body} on a new connection, and reads the response until the server closes
     * the connection. A connection closed without any response gives status -1.
     */
    static RawResponse raw(int port, String head, byte[] body) throws IOException {
        try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
            socket.setSoTimeout(10_000);
            OutputStream out = socket.getOutputStream();
            out.write((head + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
            if (body != null) {
                out.write(body);
            }
            out.flush();
            return readResponse(socket.getInputStream());
        }
    }

    /** Reads an HTTP/1.1 response until the end of the stream. */
    static RawResponse readResponse(InputStream in) throws IOException {
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        byte[] buffer = new byte[64 * 1024];
        int read;
        try {
            while ((read = in.read(buffer)) != -1) {
                all.write(buffer, 0, read);
            }
        } catch (IOException e) {
            // a reset after the response is fine; what arrived is parsed below
        }
        byte[] bytes = all.toByteArray();
        String text = new String(bytes, StandardCharsets.ISO_8859_1);
        int end = text.indexOf("\r\n\r\n");
        if (end < 0) {
            return new RawResponse(-1, Map.of(), bytes);
        }
        String[] lines = text.substring(0, end).split("\r\n");
        int status = Integer.parseInt(lines[0].split(" ")[1]);
        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            headers.computeIfAbsent(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), k -> new ArrayList<>())
                    .add(lines[i].substring(colon + 1).trim());
        }
        byte[] body = new byte[bytes.length - end - 4];
        System.arraycopy(bytes, end + 4, body, 0, body.length);
        return new RawResponse(status, headers, body);
    }

    static String host(int port) {
        return "Host: 127.0.0.1:" + port;
    }

    // ---------------------------------------------------------------------
    // Stand-in servers for client tests
    // ---------------------------------------------------------------------

    /** An HttpServer on 127.0.0.1 and a free port, answering with a handler a test supplies. */
    static final class Stub implements AutoCloseable {
        final HttpServer server;
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final AtomicInteger requests = new AtomicInteger();
        final List<String> requestLines = new CopyOnWriteArrayList<>();

        Stub(HttpHandler handler) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
            server.setExecutor(executor);
            server.createContext("/", exchange -> {
                requests.incrementAndGet();
                requestLines.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                handler.handle(exchange);
            });
            server.start();
        }

        int port() {
            return server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    /** Sends a reply with the given status, node ID (or none) and text body, after reading the request body. */
    static void reply(com.sun.net.httpserver.HttpExchange exchange, int status, String nodeId, String body) throws IOException {
        exchange.getRequestBody().readAllBytes();
        if (nodeId != null) {
            exchange.getResponseHeaders().set(NodeProtocol.NODE_ID_HEADER, nodeId);
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
        exchange.close();
    }

    /**
     * A socket server for failures HttpServer cannot script. Each connection is handed to the
     * behaviour, which may read, answer partly, stall or reset.
     */
    static final class SocketStub implements AutoCloseable {
        interface Behaviour {
            void serve(Socket socket) throws Exception;
        }

        final ServerSocket serverSocket;
        final AtomicInteger connections = new AtomicInteger();
        private final Thread thread;
        private final List<Socket> open = new CopyOnWriteArrayList<>();

        SocketStub(Behaviour behaviour) throws IOException {
            serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            thread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket socket = serverSocket.accept();
                        connections.incrementAndGet();
                        open.add(socket);
                        Thread worker = new Thread(() -> {
                            try {
                                behaviour.serve(socket);
                            } catch (Exception ignored) {
                                // the client went away, or the test closed the stub
                            }
                        });
                        worker.setDaemon(true);
                        worker.start();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "socket-stub");
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            for (Socket socket : open) {
                socket.close();
            }
        }
    }

    /** A request read off a socket: its line and headers as text, and its body. */
    record Request(String head, byte[] body) {
        boolean isMethod(String method) {
            return head.startsWith(method + " ");
        }
    }

    /** Reads a request's line and headers, and then the body its Content-Length declares. */
    static Request readRequest(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        byte[] blankLine = {'\r', '\n', '\r', '\n'};
        int matched = 0;
        while (matched < blankLine.length) {
            int b = in.read();
            if (b == -1) {
                throw new IOException("the request ended early");
            }
            head.write(b);
            matched = b == blankLine[matched] ? matched + 1 : (b == '\r' ? 1 : 0);
        }
        String text = head.toString(StandardCharsets.ISO_8859_1);
        java.util.regex.Matcher length = Pattern.compile("(?im)^content-length:\\s*(\\d+)").matcher(text);
        byte[] body = length.find() ? in.readNBytes(Integer.parseInt(length.group(1))) : new byte[0];
        return new Request(text, body);
    }

    /** Writes a complete reply with a node ID and a text body, then closes the socket. */
    static void writeReply(Socket socket, int status, String nodeId, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        OutputStream out = socket.getOutputStream();
        out.write(("HTTP/1.1 " + status + " X\r\nX-Node-Id: " + nodeId + "\r\nContent-Length: " + bytes.length
                + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(bytes);
        out.flush();
        socket.close();
    }

    /** Closes a socket with a reset (RST) instead of a normal close. */
    static void reset(Socket socket) throws IOException {
        socket.setSoLinger(true, 0);
        socket.close();
    }

    /** Waits up to a few seconds for a socket stub to see its connections end; used to keep sockets idle. */
    static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Blocks reading a socket until it closes or a minute passes, sending nothing. */
    static void holdSilently(Socket socket) throws IOException {
        socket.setSoTimeout(60_000);
        try {
            InputStream in = socket.getInputStream();
            byte[] buffer = new byte[4096];
            while (in.read(buffer) != -1) {
                // keep reading, never answer
            }
        } catch (SocketTimeoutException e) {
            // done waiting
        }
    }

    // ---------------------------------------------------------------------
    // Data and file helpers
    // ---------------------------------------------------------------------

    static byte[] randomBytes(int size, long seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            List<Path> deepestFirst = paths.sorted(Comparator.reverseOrder()).toList();
            for (Path path : deepestFirst) {
                Files.deleteIfExists(path);
            }
        }
    }

    /** Creates a directory junction (mklink /J, no admin rights needed). */
    static void createDirectoryJunction(Path link, Path target) throws IOException {
        Process process = new ProcessBuilder("cmd.exe", "/c", "mklink", "/J", link.toString(), target.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while creating a junction", e);
        }
        if (exitCode != 0) {
            throw new IOException("mklink /J failed with exit code " + exitCode + ": " + output.trim());
        }
        BasicFileAttributes attributes = Files.readAttributes(link, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        check(attributes.isOther(), "the planted entry is not a junction");
    }

    /** Creates a symbolic link to a file, or skips the test if this account may not create one. */
    static void createFileSymlinkOrSkip(Path link, Path target) throws IOException {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException e) {
            throw new SkipTest("SKIPPED: symbolic links are not supported here (" + e + ")");
        } catch (FileSystemException e) {
            String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
            if (!message.contains("privilege")) {
                throw e;
            }
            throw new SkipTest("SKIPPED: this account may not create symbolic links (Windows needs Developer Mode"
                    + " or admin rights): " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------------
    // Child JVMs
    // ---------------------------------------------------------------------

    static String javaExecutable() {
        Path bin = Path.of(System.getProperty("java.home"), "bin");
        Path windowsJava = bin.resolve("java.exe");
        return (Files.isRegularFile(windowsJava) ? windowsJava : bin.resolve("java")).toString();
    }

    static String absoluteClassPath() {
        List<String> parts = new ArrayList<>();
        for (String part : System.getProperty("java.class.path").split(Pattern.quote(File.pathSeparator))) {
            parts.add(Path.of(part).toAbsolutePath().toString());
        }
        return String.join(File.pathSeparator, parts);
    }
}
