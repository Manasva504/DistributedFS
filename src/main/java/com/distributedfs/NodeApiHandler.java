package com.distributedfs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The storage node server's request handler: the node API that {@link RemoteStorageNode} calls.
 *
 * <p><b>Endpoints</b> (bodies as described in {@link NodeProtocol}):
 * <ul>
 *   <li>{@code GET /health}: 200 {@code status=ok}, {@code node-id}, {@code format=1}.</li>
 *   <li>{@code POST /chunks/<id>}: store. Needs a {@code Content-Length} (no chunked bodies) of at
 *       most the node's chunk limit and an {@code X-Chunk-Sha256} of 64 lowercase hex characters.
 *       201 {@code stored=<bytes>}; 409 {@code chunk_exists}; 400; 413 {@code too_large}; 422
 *       {@code checksum_mismatch}; 503 {@code storage_unavailable} or {@code node_busy}; 500
 *       {@code store_failed}. Every answer other than 201 means nothing was published under the
 *       ID. A body that ends early, stalls or is cut off gets no answer at all.</li>
 *   <li>{@code GET /chunks/<id>}: 200 with the chunk's bytes; 404 {@code chunk_not_found}; 500
 *       {@code unsafe_entry} (the entry under the ID is a link, junction or directory); 503.</li>
 *   <li>{@code GET /chunks/<id>/exists?mode=follow} ({@link StorageNode#hasChunk}) or
 *       {@code mode=nofollow} ({@link StorageNode#holdsRegularChunkFile}): 200 {@code exists=true}
 *       or {@code false}; 500 {@code unsafe_entry}; 503.</li>
 *   <li>{@code DELETE /chunks/<id>}: 200 {@code deleted=true} or {@code false}; 500
 *       {@code unsafe_entry} or {@code delete_failed}; 503.</li>
 *   <li>{@code GET /entries}: 200, one {@code <kind> <name>} line per entry of the chunk folder; 503.</li>
 *   <li>{@code POST /sweep}: deletes stale temp files; 200 {@code removed}, {@code warnings}; 503.</li>
 * </ul>
 * Any other path is 404 {@code not_found}; a wrong method is 405. "Absent" ({@code exists=false},
 * {@code deleted=false}, 404 {@code chunk_not_found}) is answered only when the node's own checks
 * confirm it: the chunk folder is a plain folder with no entry under the ID. A node that cannot
 * tell answers 500 or 503, never "absent".
 *
 * <p><b>Checks before routing</b>, as in {@link FileApiHandler}: exactly one Host header naming
 * {@code 127.0.0.1} or {@code localhost} with this node's port (403 {@code host_not_allowed}
 * otherwise, also for absolute request targets), so a web page cannot reach the node through DNS
 * rebinding. A request from a web page is refused next, with 403 {@code cross_origin_refused}:
 * any {@code Origin} header, or a {@code Sec-Fetch-Site} other than {@code none}, because a page
 * on another site could otherwise make the browser send a store, delete or sweep that passes the
 * Host check, and no browser has any reason to call a node. Routes are matched on the raw path,
 * never decoded: a chunk ID containing {@code %} or {@code /}, or breaking any {@link StorageNode}
 * ID rule, is 400 {@code invalid_chunk_id} before anything touches the disk.
 *
 * <p><b>One operation at a time.</b> A StorageNode must be used by one thread at a time, and any
 * local process can call a node, so every chunk operation runs under one node-wide lock, including
 * streaming a chunk in or out. A request that cannot get the lock within the configured wait gets
 * 503 {@code node_busy}. Because of the lock, a request that arrives after a store is handled after
 * it: a client that gave up on a store and then asks about the chunk sees the store's outcome.
 *
 * <p><b>Stalled clients.</b> Reading a store's body and writing a chunk run under
 * {@link FileApiHandler.StallWatch}: no progress for the stall timeout closes the connection. A
 * declared body on any other request (and a store body that is refused) is read and discarded the
 * same way before the answer, so the client always finishes sending and then gets its answer.
 *
 * <p><b>Every response</b> carries {@code X-Node-Id} and {@code Connection: close}: each connection
 * serves one request, so no client ever reuses a connection that the node has closed as idle.
 * <b>Every request</b> is logged as one line on
 * the node's output: method, route (with the chunk ID only if it is valid), status ({@code -} if no
 * response was sent), bytes (the chunk's bytes for a store or read, otherwise the response body)
 * and duration. Log lines never contain file paths or exception text; error details go to the
 * node's error stream.
 */
final class NodeApiHandler implements HttpHandler {

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final String OCTET_STREAM = "application/octet-stream";

    private final int port;
    private final String nodeId;
    private final StorageNode node;
    private final Path chunksDir;
    private final PrintStream out;
    private final PrintStream err;
    private final long maxChunkBytes;
    private final long stallTimeoutMillis;
    private final long lockWaitMillis;
    private final ReentrantLock storageLock = new ReentrantLock();

    NodeApiHandler(int port, String nodeId, StorageNode node, PrintStream out, PrintStream err,
                   long maxChunkBytes, long stallTimeoutMillis, long lockWaitMillis) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1 to 65535, but was " + port);
        }
        if (nodeId == null || !NodeProtocol.NODE_ID.matcher(nodeId).matches()) {
            throw new IllegalArgumentException("nodeId must be 32 lowercase hexadecimal characters");
        }
        if (maxChunkBytes < 1 || stallTimeoutMillis < 1 || lockWaitMillis < 0) {
            throw new IllegalArgumentException("invalid chunk limit, stall timeout or lock wait");
        }
        this.port = port;
        this.nodeId = nodeId;
        this.node = Objects.requireNonNull(node, "node must not be null");
        this.chunksDir = node.localDirectory().orElseThrow();
        this.out = Objects.requireNonNull(out, "out must not be null");
        this.err = Objects.requireNonNull(err, "err must not be null");
        this.maxChunkBytes = maxChunkBytes;
        this.stallTimeoutMillis = stallTimeoutMillis;
        this.lockWaitMillis = lockWaitMillis;
    }

    /** What the request log line says about one request. */
    private static final class RequestLog {
        String route = "(unknown path)";
        long bytes = 0;
    }

    @Override
    public void handle(HttpExchange exchange) {
        long started = System.nanoTime();
        RequestLog log = new RequestLog();
        try {
            exchange.getResponseHeaders().set(NodeProtocol.NODE_ID_HEADER, nodeId);
            // One request per connection: a client then never sends a request on a connection the
            // node has already closed as idle, which would make a store's outcome look unknown.
            exchange.getResponseHeaders().set("Connection", "close");
            dispatch(exchange, log);
        } catch (IOException | RuntimeException e) {
            err.println("Error: node request " + safeMethod(exchange) + " " + log.route + " failed: " + e);
            if (exchange.getResponseCode() == -1) {
                try {
                    sendError(exchange, NodeProtocol.Error.INTERNAL_ERROR, null, log);
                } catch (IOException | RuntimeException sendError) {
                    err.println("Error: could not send the 500 response either: " + sendError);
                }
            }
        } finally {
            exchange.close();
            int status = exchange.getResponseCode();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            out.println(safeMethod(exchange) + " " + log.route + " " + (status == -1 ? "-" : Integer.toString(status))
                    + " " + log.bytes + " bytes " + millis + " ms");
        }
    }

    /**
     * True for a request that no web page made: no {@code Origin} header at all, and no
     * {@code Sec-Fetch-Site} header other than exactly one {@code none}. Only the server's
     * {@link RemoteStorageNode} is meant to call a node, and the JDK's HTTP client sends neither.
     */
    static boolean isNotFromBrowserPage(Headers headers) {
        if (headers.containsKey("Origin")) {
            return false;
        }
        List<String> fetchSites = headers.get("Sec-Fetch-Site");
        return fetchSites == null || (fetchSites.size() == 1 && fetchSites.get(0).equals("none"));
    }

    private void dispatch(HttpExchange exchange, RequestLog log) throws IOException {
        URI target = exchange.getRequestURI();
        if (!FileApiHandler.isAllowedHost(exchange.getRequestHeaders().get("Host"), port) || target.isAbsolute()
                || target.getRawAuthority() != null) {
            log.route = "(host not allowed)";
            sendError(exchange, NodeProtocol.Error.HOST_NOT_ALLOWED, null, log);
            return;
        }
        if (!isNotFromBrowserPage(exchange.getRequestHeaders())) {
            log.route = "(cross-origin refused)";
            sendError(exchange, NodeProtocol.Error.CROSS_ORIGIN_REFUSED, null, log);
            return;
        }
        String path = target.getRawPath();
        String query = target.getRawQuery();
        String method = exchange.getRequestMethod();
        if (NodeProtocol.HEALTH_PATH.equals(path) || NodeProtocol.ENTRIES_PATH.equals(path)
                || NodeProtocol.SWEEP_PATH.equals(path)) {
            log.route = path;
            String allowed = NodeProtocol.SWEEP_PATH.equals(path) ? "POST" : "GET";
            if (!allowed.equals(method)) {
                sendError(exchange, NodeProtocol.Error.METHOD_NOT_ALLOWED, allowed, log);
            } else if (query != null) {
                sendError(exchange, NodeProtocol.Error.BAD_REQUEST, null, log);
            } else if (NodeProtocol.HEALTH_PATH.equals(path)) {
                sendFields(exchange, 200, NodeProtocol.fields("status", "ok", "node-id", nodeId,
                        "format", Integer.toString(NodeProtocol.FORMAT)), log);
            } else if (NodeProtocol.ENTRIES_PATH.equals(path)) {
                handleEntries(exchange, log);
            } else {
                handleSweep(exchange, log);
            }
            return;
        }
        if (path == null || !path.startsWith(NodeProtocol.CHUNKS_PREFIX)) {
            sendError(exchange, NodeProtocol.Error.NOT_FOUND, null, log);
            return;
        }
        // Still the raw text: checked exactly as received, so %2e, %2F and the like are refused.
        String rest = path.substring(NodeProtocol.CHUNKS_PREFIX.length());
        boolean existsRoute = rest.endsWith(NodeProtocol.EXISTS_SUFFIX);
        String chunkId = existsRoute ? rest.substring(0, rest.length() - NodeProtocol.EXISTS_SUFFIX.length()) : rest;
        if (!isValidChunkId(chunkId)) {
            log.route = "/chunks/(invalid id)";
            sendError(exchange, NodeProtocol.Error.INVALID_CHUNK_ID, null, log);
            return;
        }
        log.route = NodeProtocol.CHUNKS_PREFIX + chunkId + (existsRoute ? NodeProtocol.EXISTS_SUFFIX : "");
        if (existsRoute) {
            if (!"GET".equals(method)) {
                sendError(exchange, NodeProtocol.Error.METHOD_NOT_ALLOWED, "GET", log);
            } else if ("mode=follow".equals(query) || "mode=nofollow".equals(query)) {
                handleExists(exchange, chunkId, "mode=follow".equals(query), log);
            } else {
                sendError(exchange, NodeProtocol.Error.BAD_REQUEST, null, log);
            }
            return;
        }
        if (query != null) {
            sendError(exchange, NodeProtocol.Error.BAD_REQUEST, null, log);
            return;
        }
        switch (method) {
            case "POST" -> handleStore(exchange, chunkId, log);
            case "GET" -> handleRead(exchange, chunkId, log);
            case "DELETE" -> handleDelete(exchange, chunkId, log);
            default -> sendError(exchange, NodeProtocol.Error.METHOD_NOT_ALLOWED, "GET, POST, DELETE", log);
        }
    }

    /** StorageNode's ID rules, on the raw path text: no percent-encoding and no further path segment. */
    private static boolean isValidChunkId(String chunkId) {
        if (chunkId.indexOf('%') >= 0 || chunkId.indexOf('/') >= 0) {
            return false;
        }
        try {
            StorageNode.validateChunkId(chunkId);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // ---------------------------------------------------------------------
    // Chunk operations
    // ---------------------------------------------------------------------

    private void handleStore(HttpExchange exchange, String chunkId, RequestLog log) throws IOException {
        Headers headers = exchange.getRequestHeaders();
        String contentLength = headers.getFirst("Content-Length");
        if (headers.containsKey("Transfer-Encoding") || contentLength == null) {
            sendError(exchange, NodeProtocol.Error.BAD_REQUEST, null, log);
            return;
        }
        // The JDK has already refused malformed, negative or repeated Content-Length headers.
        long length = Long.parseLong(contentLength.trim());
        if (length > maxChunkBytes) {
            sendError(exchange, NodeProtocol.Error.TOO_LARGE, null, log);
            return;
        }
        List<String> checksums = headers.get(NodeProtocol.CHUNK_SHA256_HEADER);
        if (checksums == null || checksums.size() != 1 || !StorageNode.isSha256Hex(checksums.get(0))) {
            sendError(exchange, NodeProtocol.Error.BAD_REQUEST, null, log);
            return;
        }
        if (!lock(exchange, log)) {
            return;
        }
        long stored;
        try {
            if (!DataDirectory.isPlainDirectory(chunksDir)) {
                storageLock.unlock();
                sendError(exchange, NodeProtocol.Error.STORAGE_UNAVAILABLE, null, log);
                return;
            }
            try (FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, stallTimeoutMillis)) {
                ProgressInput in = new ProgressInput(exchange.getRequestBody(), watch::progress);
                NodeProtocol.Error refusal = null;
                stored = -1;
                try {
                    stored = node.storeChunkFrom(chunkId, in, length, checksums.get(0));
                } catch (FileAlreadyExistsException e) {
                    refusal = NodeProtocol.Error.CHUNK_EXISTS;
                } catch (StorageNode.ChecksumMismatchException e) {
                    err.println("Store of chunk " + chunkId + " refused: " + e.getMessage());
                    refusal = NodeProtocol.Error.CHECKSUM_MISMATCH;
                } catch (IOException e) {
                    if (in.readFailed || watch.fired() || e instanceof EOFException) {
                        // The body is incomplete, so no answer is sent; nothing was published.
                        log.bytes = in.count;
                        err.println("Store of chunk " + chunkId + " abandoned: " + (watch.fired()
                                ? "no data arrived for " + stallTimeoutMillis + " ms"
                                : "the client disconnected or sent an incomplete body") + " (" + e + ")");
                        return;
                    }
                    err.println("Error: store of chunk " + chunkId + " failed: " + e);
                    refusal = NodeProtocol.Error.STORE_FAILED;
                }
                log.bytes = in.count;
                if (!watch.release()) {
                    err.println("Store of chunk " + chunkId + " abandoned: no data arrived for " + stallTimeoutMillis + " ms");
                    return;
                }
                if (refusal != null) {
                    storageLock.unlock();
                    sendError(exchange, refusal, null, log);
                    return;
                }
            }
        } finally {
            if (storageLock.isHeldByCurrentThread()) {
                storageLock.unlock();
            }
        }
        sendFields(exchange, 201, NodeProtocol.fields("stored", Long.toString(stored)), log);
    }

    private void handleRead(HttpExchange exchange, String chunkId, RequestLog log) throws IOException {
        if (!finishRequestBody(exchange)) {
            return;
        }
        if (!lock(exchange, log)) {
            return;
        }
        try {
            StorageNode.OpenChunk chunk;
            try {
                chunk = node.openChunk(chunkId);
            } catch (ChunkNotFoundException e) {
                storageLock.unlock();
                sendError(exchange, NodeProtocol.Error.CHUNK_NOT_FOUND, null, log);
                return;
            } catch (IOException e) {
                NodeProtocol.Error error = cannotTell(e);
                storageLock.unlock();
                sendError(exchange, error, null, log);
                return;
            }
            Headers headers = exchange.getResponseHeaders();
            headers.set("Content-Type", OCTET_STREAM);
            headers.set("X-Content-Type-Options", "nosniff");
            try (InputStream in = chunk.in();
                 FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, stallTimeoutMillis)) {
                // -1: no body (0 would mean "chunked"); otherwise exactly the chunk's size.
                exchange.sendResponseHeaders(200, chunk.size() == 0 ? -1 : chunk.size());
                if (chunk.size() == 0) {
                    return;
                }
                OutputStream body = exchange.getResponseBody();
                byte[] buffer = new byte[BUFFER_SIZE];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    body.write(buffer, 0, read);
                    log.bytes += read;
                    watch.progress();
                }
                body.flush();
                watch.progress();
                if (!watch.release()) {
                    throw new IOException("the stall watchdog closed the connection");
                }
                body.close();
            } catch (IOException e) {
                err.println("Read of chunk " + chunkId + " abandoned after " + log.bytes + " bytes: " + e);
            }
        } finally {
            if (storageLock.isHeldByCurrentThread()) {
                storageLock.unlock();
            }
        }
    }

    private void handleExists(HttpExchange exchange, String chunkId, boolean followLinks, RequestLog log)
            throws IOException {
        if (!finishRequestBody(exchange)) {
            return;
        }
        if (!lock(exchange, log)) {
            return;
        }
        boolean exists;
        NodeProtocol.Error error = null;
        try {
            exists = followLinks ? node.hasChunk(chunkId) : node.holdsRegularChunk(chunkId);
        } catch (IOException e) {
            exists = false;
            error = cannotTell(e);
        } finally {
            storageLock.unlock();
        }
        if (error != null) {
            sendError(exchange, error, null, log);
        } else {
            sendFields(exchange, 200, NodeProtocol.fields("exists", Boolean.toString(exists)), log);
        }
    }

    private void handleDelete(HttpExchange exchange, String chunkId, RequestLog log) throws IOException {
        if (!finishRequestBody(exchange)) {
            return;
        }
        if (!lock(exchange, log)) {
            return;
        }
        boolean deleted = false;
        NodeProtocol.Error error = null;
        try {
            boolean present;
            try {
                present = node.holdsRegularChunk(chunkId);
            } catch (IOException e) {
                present = false;
                error = cannotTell(e);
            }
            if (present) {
                try {
                    deleted = node.deleteChunk(chunkId);
                } catch (IOException e) {
                    err.println("Error: delete of chunk " + chunkId + " failed: " + e);
                    error = NodeProtocol.Error.DELETE_FAILED;
                }
            }
        } finally {
            storageLock.unlock();
        }
        if (error != null) {
            sendError(exchange, error, null, log);
        } else {
            sendFields(exchange, 200, NodeProtocol.fields("deleted", Boolean.toString(deleted)), log);
        }
    }

    private void handleEntries(HttpExchange exchange, RequestLog log) throws IOException {
        if (!finishRequestBody(exchange)) {
            return;
        }
        if (!lock(exchange, log)) {
            return;
        }
        List<ChunkStore.Entry> entries;
        try {
            entries = node.listEntries();
        } catch (IOException e) {
            err.println("Error: the chunk folder could not be listed: " + e);
            entries = null;
        } finally {
            storageLock.unlock();
        }
        if (entries == null) {
            sendError(exchange, NodeProtocol.Error.STORAGE_UNAVAILABLE, null, log);
            return;
        }
        StringBuilder body = new StringBuilder();
        for (ChunkStore.Entry entry : entries) {
            body.append(NodeProtocol.kindName(entry.kind())).append(' ').append(NodeProtocol.encodeName(entry.name()))
                    .append('\n');
        }
        sendFields(exchange, 200, body.toString(), log);
    }

    private void handleSweep(HttpExchange exchange, RequestLog log) throws IOException {
        if (!finishRequestBody(exchange)) {
            return;
        }
        if (!lock(exchange, log)) {
            return;
        }
        int removed;
        int[] warnings = {0};
        try {
            removed = node.sweepTempFiles(message -> {
                warnings[0]++;
                err.println("Warning: " + message);
            });
        } catch (IOException e) {
            err.println("Error: the chunk folder could not be swept: " + e);
            removed = -1;
        } finally {
            storageLock.unlock();
        }
        if (removed < 0) {
            sendError(exchange, NodeProtocol.Error.STORAGE_UNAVAILABLE, null, log);
        } else {
            sendFields(exchange, 200, NodeProtocol.fields("removed", Integer.toString(removed),
                    "warnings", Integer.toString(warnings[0])), log);
        }
    }

    /**
     * Why the node cannot tell: its chunk folder is missing or is not a plain folder (503
     * {@code storage_unavailable}), or something other than a regular file is under the ID (500
     * {@code unsafe_entry}). Either way the answer is never "absent".
     */
    private NodeProtocol.Error cannotTell(IOException e) {
        err.println("Warning: a chunk check could not be answered: " + e);
        try {
            if (!DataDirectory.isPlainDirectory(chunksDir)) {
                return NodeProtocol.Error.STORAGE_UNAVAILABLE;
            }
        } catch (IOException checkError) {
            return NodeProtocol.Error.STORAGE_UNAVAILABLE;
        }
        return NodeProtocol.Error.UNSAFE_ENTRY;
    }

    /** Takes the node-wide lock, or answers 503 node_busy and returns false. */
    private boolean lock(HttpExchange exchange, RequestLog log) throws IOException {
        try {
            if (storageLock.tryLock(lockWaitMillis, TimeUnit.MILLISECONDS)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sendError(exchange, NodeProtocol.Error.NODE_BUSY, null, log);
        return false;
    }

    // ---------------------------------------------------------------------
    // Request bodies and responses
    // ---------------------------------------------------------------------

    /**
     * Reads and discards a declared request body, up to the chunk limit, under the stall watchdog,
     * so that the client finishes sending before it gets an answer and closing the response cannot
     * block on it. Returns false if the body did not arrive (the connection is then closed and no
     * answer can be sent).
     */
    private boolean finishRequestBody(HttpExchange exchange) {
        Headers headers = exchange.getRequestHeaders();
        String length = headers.getFirst("Content-Length");
        if (!headers.containsKey("Transfer-Encoding") && (length == null || length.trim().equals("0"))) {
            return true; // no body: the JDK's body stream is already at its end
        }
        try (FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, stallTimeoutMillis)) {
            try {
                InputStream in = exchange.getRequestBody();
                byte[] buffer = new byte[BUFFER_SIZE];
                long total = 0;
                int read;
                while (total <= maxChunkBytes && (read = in.read(buffer)) != -1) {
                    total += read;
                    watch.progress();
                }
                in.close(); // drains at most 64 KiB more, still under the watchdog
            } catch (IOException e) {
                return false;
            }
            return watch.release();
        }
    }

    private void sendError(HttpExchange exchange, NodeProtocol.Error error, String allow, RequestLog log)
            throws IOException {
        Headers headers = exchange.getResponseHeaders();
        if (allow != null) {
            headers.set("Allow", allow);
        }
        // The request body may not have been read in full, so the connection is not reused.
        headers.set("Connection", "close");
        sendFields(exchange, error.status(), error.body(), log);
    }

    private void sendFields(HttpExchange exchange, int status, String body, RequestLog log) throws IOException {
        if (!finishRequestBody(exchange)) {
            return;
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", NodeProtocol.CONTENT_TYPE);
        headers.set("X-Content-Type-Options", "nosniff");
        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status, -1); // HEAD: status and headers only, as HTTP requires
            return;
        }
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream responseBody = exchange.getResponseBody()) {
                responseBody.write(bytes);
            }
        }
        if (!"POST".equals(exchange.getRequestMethod()) || !log.route.startsWith(NodeProtocol.CHUNKS_PREFIX)) {
            log.bytes = bytes.length;
        }
    }

    /** The request method for a log line: printable ASCII only, at most 10 characters. */
    private static String safeMethod(HttpExchange exchange) {
        String method = String.valueOf(exchange.getRequestMethod());
        StringBuilder safe = new StringBuilder();
        for (int i = 0; i < method.length() && safe.length() < 10; i++) {
            char c = method.charAt(i);
            safe.append(c > 0x20 && c < 0x7F ? c : '?');
        }
        return safe.toString();
    }

    /** Counts the bytes read, reports progress to the stall watchdog, and remembers a failed read. */
    private static final class ProgressInput extends FilterInputStream {
        private final Runnable progress;
        long count = 0;
        boolean readFailed = false;

        ProgressInput(InputStream in, Runnable progress) {
            super(in);
            this.progress = progress;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int n = read(single, 0, 1);
            return n == -1 ? -1 : single[0] & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int n;
            try {
                n = super.read(buffer, offset, length);
            } catch (IOException e) {
                readFailed = true;
                throw e;
            }
            if (n > 0) {
                count += n;
                progress.run();
            }
            return n;
        }
    }
}
