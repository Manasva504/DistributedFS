package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A storage node in another process ({@link StorageNodeServer}), reached over HTTP on 127.0.0.1
 * with the JDK's {@link HttpClient}. It is a {@link ChunkStore}, so a {@link StorageCluster} can
 * use it like a local {@link StorageNode}.
 *
 * <p><b>"Could not check" is never "absent".</b> Only an explicit answer from this very node counts:
 * {@code exists=false}, {@code deleted=false}, or 404 with {@code error=chunk_not_found}. A refused
 * connection, a timeout, a reset, a reply that is malformed or too long, any other status or error
 * code, and a reply from a different node are all a plain {@link IOException}.
 *
 * <p><b>The node ID is pinned.</b> The client is created for one node ID (or learns it from
 * {@code /health} in {@link #connect}), and every reply must carry exactly that ID in
 * {@code X-Node-Id}. A different node process listening on the same port (for example a node
 * started on another folder after a restart) can therefore never answer "absent" for this one.
 *
 * <p><b>Stores</b> send the chunk with its SHA-256 ({@code X-Chunk-Sha256}); the node publishes it
 * only if length and checksum match. Each store uses its own new HttpClient, and the node closes
 * every connection after one request, so a store never runs on a reused connection. The outcome:
 * <ul>
 *   <li>201 with the right byte count: stored.</li>
 *   <li>409 {@code chunk_exists}: {@link FileAlreadyExistsException}.</li>
 *   <li>The connection could not be made (refused, or connect timeout): nothing was sent, so a
 *       plain IOException, "certainly not stored".</li>
 *   <li>An error the node sends only when it has published nothing (400, 403, 413, 422, 500
 *       {@code store_failed}, 503): a plain IOException, "certainly not stored".</li>
 *   <li>Anything else after the connection was made (a timeout, a reset, a malformed or unknown
 *       reply, a wrong byte count, a reply from another node): {@link AmbiguousStoreException}.</li>
 * </ul>
 *
 * <p><b>Streaming.</b> Chunks are sent from their file and received into their destination file
 * through a 64 KiB buffer; nothing holds a whole chunk in memory. The request timeout covers the
 * wait for the reply's headers; after that, a reply body may take as long as it needs while bytes
 * keep arriving, and is abandoned once none arrive for the stall timeout.
 *
 * <p>Safe for use by several threads; a {@link StorageCluster} uses it from one thread anyway.
 * {@link #close} releases the HTTP client.
 */
final class RemoteStorageNode implements ChunkStore, AutoCloseable {

    private static final String HOST = "127.0.0.1";
    private static final int BUFFER_SIZE = 64 * 1024;

    private final int port;
    private final String nodeId;
    private final RemoteNodeConfig config;
    private final HttpClient client;

    /**
     * A client for the node with the given ID listening on {@code 127.0.0.1:port}. Nothing is
     * sent yet.
     *
     * @throws IllegalArgumentException if the port or node ID is invalid
     */
    RemoteStorageNode(int port, String nodeId, RemoteNodeConfig config) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1 to 65535, but was " + port);
        }
        if (nodeId == null || !NodeProtocol.NODE_ID.matcher(nodeId).matches()) {
            throw new IllegalArgumentException("nodeId must be 32 lowercase hexadecimal characters");
        }
        this.port = port;
        this.nodeId = nodeId;
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.client = newClient(config);
    }

    /**
     * Asks the node on {@code 127.0.0.1:port} for its ID ({@code GET /health}) and returns a
     * client pinned to that ID.
     *
     * @throws IOException if the node cannot be reached or does not answer as a storage node
     */
    static RemoteStorageNode connect(int port, RemoteNodeConfig config) throws IOException {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1 to 65535, but was " + port);
        }
        Objects.requireNonNull(config, "config must not be null");
        HttpClient probe = newClient(config);
        Reply reply;
        try {
            reply = send(probe, request(port, NodeProtocol.HEALTH_PATH, config.requestTimeoutMillis()).GET().build(),
                    null, config.maxReplyBytes(), null, config.stallTimeoutMillis());
        } catch (IOException e) {
            throw new IOException("the storage node at " + HOST + ":" + port + " could not be reached: " + e, e);
        } finally {
            probe.shutdownNow();
        }
        Map<String, String> fields = NodeProtocol.parseFields(reply.body());
        String id = fields == null ? null : fields.get("node-id");
        if (reply.status() != 200 || fields == null || fields.size() != 3 || !"ok".equals(fields.get("status"))
                || !Integer.toString(NodeProtocol.FORMAT).equals(fields.get("format"))
                || id == null || !NodeProtocol.NODE_ID.matcher(id).matches() || !id.equals(reply.nodeIdHeader())) {
            throw new IOException("the program at " + HOST + ":" + port + " did not answer as a storage node"
                    + " (status " + reply.status() + ")");
        }
        return new RemoteStorageNode(port, id, config);
    }

    /**
     * Checks that the node answers {@code /health} as itself: status ok, format 1, this node's ID.
     *
     * @throws IOException otherwise
     */
    void health() throws IOException {
        Reply reply = call("check the node's health", get(NodeProtocol.HEALTH_PATH), config.maxReplyBytes());
        Map<String, String> fields = NodeProtocol.parseFields(reply.body());
        if (reply.status() != 200 || fields == null || fields.size() != 3 || !"ok".equals(fields.get("status"))
                || !nodeId.equals(fields.get("node-id"))
                || !Integer.toString(NodeProtocol.FORMAT).equals(fields.get("format"))) {
            throw failure("check the node's health", reply);
        }
    }

    // ---------------------------------------------------------------------
    // ChunkStore
    // ---------------------------------------------------------------------

    @Override
    public long storeChunk(String chunkId, Path sourceFile) throws IOException {
        StorageNode.validateChunkId(chunkId);
        StorageNode.validateSourceFile(sourceFile);
        long size = Files.size(sourceFile);
        String sha256 = new ChecksumService().calculateSha256(sourceFile);
        HttpRequest request;
        try {
            request = request(port, NodeProtocol.CHUNKS_PREFIX + chunkId, config.storeTimeoutMillis())
                    .header(NodeProtocol.CHUNK_SHA256_HEADER, sha256)
                    .header("Content-Type", "application/octet-stream")
                    .POST(HttpRequest.BodyPublishers.ofFile(sourceFile))
                    .build();
        } catch (FileNotFoundException e) {
            throw new IOException("chunk " + chunkId + " was not stored: its source file could not be opened", e);
        }

        // A new client, so this store certainly gets a new connection of its own: a failure to
        // connect then reliably means that nothing was sent.
        HttpClient storeClient = newClient(config);
        Reply reply;
        try {
            reply = send(storeClient, request, nodeId, config.maxReplyBytes(), null, config.stallTimeoutMillis());
        } catch (IOException e) {
            if (neverSent(e)) {
                throw new IOException(describe() + " could not be reached, so chunk " + chunkId + " was not stored: " + e, e);
            }
            throw new AmbiguousStoreException(describe() + " gave no clear answer after chunk " + chunkId
                    + " was sent, so it may or may not be stored there: " + e, e);
        } finally {
            storeClient.shutdownNow();
        }

        if (!nodeId.equals(reply.nodeIdHeader())) {
            // Another node took the chunk: it may now hold it, so this is not "certainly not stored".
            throw new AmbiguousStoreException(describe() + " answered chunk " + chunkId + "'s store as "
                    + describeHeaderId(reply.nodeIdHeader()) + ", so the chunk may be stored on another node", null);
        }
        Map<String, String> fields = NodeProtocol.parseFields(reply.body());
        String error = errorCode(fields);
        if (reply.status() == 201 && fields != null && fields.size() == 1
                && Long.toString(size).equals(fields.get("stored"))) {
            return size;
        }
        if (reply.status() == 409 && "chunk_exists".equals(error)) {
            throw new FileAlreadyExistsException(null, null, "chunk \"" + chunkId + "\" already exists on " + describe());
        }
        if (isRefusalWithNothingStored(reply.status(), error)) {
            throw new IOException(describe() + " did not store chunk " + chunkId + ": it answered "
                    + reply.status() + " " + error);
        }
        throw new AmbiguousStoreException(describe() + " answered the store of chunk " + chunkId + " with "
                + reply.status() + " " + (error == null ? "and an unexpected reply" : error)
                + ", so it may or may not be stored there", null);
    }

    @Override
    public long retrieveChunk(String chunkId, Path destination) throws IOException {
        StorageNode.validateChunkId(chunkId);
        Objects.requireNonNull(destination, "destination must not be null");
        if (Files.isDirectory(destination)) {
            throw new IllegalArgumentException("Destination is an existing directory: " + destination);
        }
        String what = "send chunk " + chunkId;
        Reply reply;
        try {
            reply = send(client, get(NodeProtocol.CHUNKS_PREFIX + chunkId), nodeId, config.maxReplyBytes(),
                    destination, config.stallTimeoutMillis());
        } catch (FileAlreadyExistsException e) {
            throw e; // the destination existed before this call; it was left alone
        } catch (IOException e) {
            throw unreachable(what, e);
        }
        requireThisNode(reply);
        if (reply.toFile()) {
            // send() has checked that exactly Content-Length bytes arrived.
            return reply.bytes();
        }
        Map<String, String> fields = NodeProtocol.parseFields(reply.body());
        if (reply.status() == 404 && fields != null && fields.size() == 1 && "chunk_not_found".equals(errorCode(fields))) {
            throw new ChunkNotFoundException(chunkId, "Chunk \"" + chunkId + "\" is not stored on " + describe());
        }
        throw failure(what, reply);
    }

    @Override
    public boolean hasChunk(String chunkId) throws IOException {
        return exists(chunkId, "follow");
    }

    @Override
    public boolean holdsRegularChunk(String chunkId) throws IOException {
        return exists(chunkId, "nofollow");
    }

    @Override
    public boolean deleteChunk(String chunkId) throws IOException {
        StorageNode.validateChunkId(chunkId);
        String what = "delete chunk " + chunkId;
        Reply reply = call(what, request(port, NodeProtocol.CHUNKS_PREFIX + chunkId, config.requestTimeoutMillis())
                .DELETE().build(), config.maxReplyBytes());
        Boolean deleted = booleanField(reply, "deleted");
        if (deleted == null) {
            throw failure(what, reply);
        }
        return deleted;
    }

    @Override
    public List<Entry> listEntries() throws IOException {
        String what = "list its chunk folder";
        Reply reply = call(what, get(NodeProtocol.ENTRIES_PATH), config.maxListingBytes());
        if (reply.status() != 200 || reply.body() == null || (!reply.body().isEmpty() && !reply.body().endsWith("\n"))) {
            throw failure(what, reply);
        }
        List<Entry> entries = new ArrayList<>();
        if (reply.body().isEmpty()) {
            return entries;
        }
        String previous = null;
        for (String line : reply.body().substring(0, reply.body().length() - 1).split("\n", -1)) {
            int space = line.indexOf(' ');
            EntryKind kind = space < 0 ? null : NodeProtocol.kindFromName(line.substring(0, space));
            String name = space < 0 ? null : NodeProtocol.decodeName(line.substring(space + 1));
            // The node sends each name once, sorted; anything else is not a listing this client trusts.
            if (kind == null || name == null || (previous != null && previous.compareTo(name) >= 0)) {
                throw new IOException(describe() + " could not " + what + ": its listing is malformed");
            }
            entries.add(new Entry(name, kind));
            previous = name;
        }
        return entries;
    }

    @Override
    public int sweepTempFiles(Consumer<String> warnings) throws IOException {
        Objects.requireNonNull(warnings, "warnings must not be null");
        String what = "sweep its temp files";
        Reply reply = call(what, request(port, NodeProtocol.SWEEP_PATH, config.requestTimeoutMillis())
                .POST(HttpRequest.BodyPublishers.noBody()).build(), config.maxReplyBytes());
        Map<String, String> fields = NodeProtocol.parseFields(reply.body());
        Integer removed = fields == null ? null : count(fields.get("removed"));
        Integer warningCount = fields == null ? null : count(fields.get("warnings"));
        if (reply.status() != 200 || fields == null || fields.size() != 2 || removed == null || warningCount == null) {
            throw failure(what, reply);
        }
        if (warningCount > 0) {
            warnings.accept(describe() + " reported " + warningCount + " warning" + (warningCount == 1 ? "" : "s")
                    + " while sweeping its temp files; see that node's own log");
        }
        return removed;
    }

    @Override
    public Optional<Path> localDirectory() {
        return Optional.empty();
    }

    @Override
    public Optional<String> nodeId() {
        return Optional.of(nodeId);
    }

    @Override
    public String describe() {
        return "storage node " + nodeId + " at " + HOST + ":" + port;
    }

    int port() {
        return port;
    }

    /** Releases the HTTP client; requests still running are abandoned. */
    @Override
    public void close() {
        client.shutdownNow();
    }

    // ---------------------------------------------------------------------
    // Requests and replies
    // ---------------------------------------------------------------------

    private boolean exists(String chunkId, String mode) throws IOException {
        StorageNode.validateChunkId(chunkId);
        String what = "check chunk " + chunkId;
        Reply reply = call(what, get(NodeProtocol.CHUNKS_PREFIX + chunkId + NodeProtocol.EXISTS_SUFFIX + "?mode=" + mode),
                config.maxReplyBytes());
        Boolean exists = booleanField(reply, "exists");
        if (exists == null) {
            throw failure(what, reply);
        }
        return exists;
    }

    /** {@code true} or {@code false} from a 200 reply whose only field is {@code key}; otherwise null. */
    private static Boolean booleanField(Reply reply, String key) {
        Map<String, String> fields = NodeProtocol.parseFields(reply.body());
        if (reply.status() != 200 || fields == null || fields.size() != 1) {
            return null;
        }
        String value = fields.get(key);
        return "true".equals(value) ? Boolean.TRUE : "false".equals(value) ? Boolean.FALSE : null;
    }

    /** A small request with the shared client; the reply must come from this node. */
    private Reply call(String what, HttpRequest request, int maxBytes) throws IOException {
        Reply reply;
        try {
            reply = send(client, request, nodeId, maxBytes, null, config.stallTimeoutMillis());
        } catch (IOException e) {
            throw unreachable(what, e);
        }
        requireThisNode(reply);
        return reply;
    }

    private void requireThisNode(Reply reply) throws IOException {
        if (!nodeId.equals(reply.nodeIdHeader())) {
            throw new IOException(describe() + " was answered by " + describeHeaderId(reply.nodeIdHeader())
                    + "; its answer was not used");
        }
    }

    private IOException unreachable(String what, IOException cause) {
        return new IOException(describe() + " could not " + what + " (no usable answer): " + cause, cause);
    }

    private IOException failure(String what, Reply reply) {
        String error = errorCode(NodeProtocol.parseFields(reply.body()));
        return new IOException(describe() + " could not " + what + ": it answered " + reply.status() + " "
                + (error == null ? "with an unexpected reply" : error));
    }

    /** The {@code error} code of an error body, if it looks like one of the node's codes; otherwise null. */
    private static String errorCode(Map<String, String> fields) {
        if (fields == null || fields.size() != 1) {
            return null;
        }
        String code = fields.get("error");
        return code != null && code.matches("[a-z_]{1,32}") ? code : null;
    }

    /** The answers a node sends only when it has published nothing under the chunk's ID. */
    private static boolean isRefusalWithNothingStored(int status, String error) {
        if (error == null) {
            return false;
        }
        return switch (status) {
            case 400 -> error.equals("bad_request") || error.equals("invalid_chunk_id");
            case 403 -> error.equals("host_not_allowed");
            case 413 -> error.equals("too_large");
            case 422 -> error.equals("checksum_mismatch");
            case 500 -> error.equals("store_failed");
            case 503 -> error.equals("storage_unavailable") || error.equals("node_busy");
            default -> false;
        };
    }

    /** True if the request certainly never reached the node: the connection was not made. */
    private static boolean neverSent(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof ConnectException || t instanceof HttpConnectTimeoutException) {
                return true;
            }
        }
        return false;
    }

    private static String describeHeaderId(String headerId) {
        if (headerId == null) {
            return "a program without a node ID";
        }
        return NodeProtocol.NODE_ID.matcher(headerId).matches() ? "node " + headerId : "a program with an invalid node ID";
    }

    private static Integer count(String value) {
        if (value == null || !value.matches("0|[1-9][0-9]{0,8}")) {
            return null;
        }
        return Integer.valueOf(value);
    }

    private HttpRequest get(String path) {
        return request(port, path, config.requestTimeoutMillis()).GET().build();
    }

    private static HttpRequest.Builder request(int port, String path, long timeoutMillis) {
        return HttpRequest.newBuilder(URI.create("http://" + HOST + ":" + port + path))
                .timeout(Duration.ofMillis(timeoutMillis));
    }

    private static HttpClient newClient(RemoteNodeConfig config) {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(config.connectTimeoutMillis()))
                .followRedirects(HttpClient.Redirect.NEVER)
                .proxy(HttpClient.Builder.NO_PROXY)
                .build();
    }

    /**
     * One reply: its status, its {@code X-Node-Id} (null unless exactly one), and either its body
     * as text or, for a 200 reply that went into a file, the number of bytes written.
     */
    private record Reply(int status, String nodeIdHeader, String body, long bytes, boolean toFile) {
    }

    /**
     * Sends {@code request} and collects the reply under the stall watchdog. The body goes into
     * memory (at most {@code maxBytes}), or, when {@code fileForOk} is given and the reply is a 200
     * from the expected node with a Content-Length, into that new file (created with CREATE_NEW,
     * parent folders first), which must then receive exactly Content-Length bytes. A file this
     * call created is deleted if anything fails.
     *
     * @param expectedNodeId the node ID a reply must carry before its body is written to a file;
     *                       null to accept any (only the body is kept in memory then)
     */
    private static Reply send(HttpClient client, HttpRequest request, String expectedNodeId, int maxBytes,
                              Path fileForOk, long stallMillis) throws IOException {
        AtomicReference<Collector> current = new AtomicReference<>();
        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        AtomicReference<OutputStream> file = new AtomicReference<>();
        String[] headerId = {null};
        long[] contentLength = {-1};
        HttpResponse.BodyHandler<Long> handler = info -> {
            List<String> ids = info.headers().allValues(NodeProtocol.NODE_ID_HEADER);
            headerId[0] = ids.size() == 1 ? ids.get(0) : null;
            contentLength[0] = info.headers().firstValueAsLong("Content-Length").orElse(-1);
            boolean rightNode = headerId[0] != null && headerId[0].equals(expectedNodeId);
            Collector collector;
            if (fileForOk != null && info.statusCode() == 200 && rightNode && contentLength[0] >= 0) {
                try {
                    Path parent = fileForOk.toAbsolutePath().getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    file.set(Files.newOutputStream(fileForOk, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
                    collector = new Collector(file.get(), contentLength[0]);
                } catch (IOException e) {
                    collector = Collector.failed(e);
                }
            } else {
                collector = new Collector(memory, maxBytes);
            }
            current.set(collector);
            return collector;
        };

        HttpResponse<Long> response;
        try {
            response = await(client.sendAsync(request, handler), current, stallMillis);
            OutputStream out = file.get();
            if (out != null) {
                out.close();
                if (response.body() != contentLength[0]) {
                    throw new IOException("the chunk ended after " + response.body() + " of " + contentLength[0] + " bytes");
                }
            }
        } catch (IOException | RuntimeException e) {
            OutputStream out = file.get();
            if (out != null) {
                try {
                    out.close();
                } catch (IOException closeError) {
                    e.addSuppressed(closeError);
                }
                try {
                    Files.deleteIfExists(fileForOk); // created by this call (CREATE_NEW), so ours to delete
                } catch (IOException deleteError) {
                    e.addSuppressed(deleteError);
                }
            }
            throw e;
        }
        boolean toFile = file.get() != null;
        return new Reply(response.statusCode(), headerId[0], toFile ? null : memory.toString(StandardCharsets.UTF_8),
                response.body(), toFile);
    }

    /**
     * Waits for the reply. Until its headers arrive, the request's own timeout applies. After
     * that, the body is abandoned (and the exchange cancelled) once no bytes have arrived for
     * {@code stallMillis}.
     */
    private static HttpResponse<Long> await(CompletableFuture<HttpResponse<Long>> future,
                                            AtomicReference<Collector> current, long stallMillis) throws IOException {
        long stallNanos = TimeUnit.MILLISECONDS.toNanos(stallMillis);
        long checkMillis = Math.max(5, Math.min(250, stallMillis / 4));
        while (true) {
            try {
                return future.get(checkMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                Collector collector = current.get();
                if (collector != null && System.nanoTime() - collector.lastProgress >= stallNanos) {
                    IOException stall = new IOException("no data arrived for " + stallMillis + " ms");
                    collector.abort(stall);
                    future.cancel(true);
                    throw stall;
                }
            } catch (ExecutionException e) {
                throw asIOException(e.getCause());
            } catch (CancellationException e) {
                throw new IOException("the request was cancelled", e);
            } catch (InterruptedException e) {
                future.cancel(true);
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while waiting for a storage node");
            }
        }
    }

    private static IOException asIOException(Throwable failure) {
        Throwable t = failure;
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t instanceof IOException io ? io : new IOException(String.valueOf(t), t);
    }

    /**
     * Receives a reply body into a stream, one buffer list at a time, up to a byte limit, and
     * records when bytes last arrived for the stall watchdog.
     */
    private static final class Collector implements HttpResponse.BodySubscriber<Long> {
        private final OutputStream sink;
        private final long limit;
        private final CompletableFuture<Long> result = new CompletableFuture<>();
        private final byte[] buffer = new byte[BUFFER_SIZE];
        private volatile Flow.Subscription subscription;
        volatile long lastProgress = System.nanoTime();
        private long count = 0;

        Collector(OutputStream sink, long limit) {
            this.sink = sink;
            this.limit = limit;
        }

        static Collector failed(IOException failure) {
            Collector collector = new Collector(OutputStream.nullOutputStream(), 0);
            collector.result.completeExceptionally(failure);
            return collector;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (result.isDone()) {
                subscription.cancel();
            } else {
                subscription.request(1);
            }
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            if (result.isDone()) {
                return;
            }
            try {
                for (ByteBuffer item : items) {
                    while (item.hasRemaining()) {
                        int n = Math.min(item.remaining(), buffer.length);
                        if (count + n > limit) {
                            throw new IOException("the reply is longer than the " + limit + " bytes expected");
                        }
                        item.get(buffer, 0, n);
                        sink.write(buffer, 0, n);
                        count += n;
                    }
                }
                lastProgress = System.nanoTime();
                subscription.request(1);
            } catch (IOException e) {
                abort(e);
            }
        }

        @Override
        public void onError(Throwable failure) {
            result.completeExceptionally(failure);
        }

        @Override
        public void onComplete() {
            result.complete(count);
        }

        @Override
        public CompletionStage<Long> getBody() {
            return result;
        }

        void abort(IOException why) {
            result.completeExceptionally(why);
            Flow.Subscription s = subscription;
            if (s != null) {
                s.cancel();
            }
        }
    }
}
