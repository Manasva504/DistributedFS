package com.distributedfs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The HTTP API's single request handler: it checks the Host header, routes by exact path, and
 * writes every response, including errors.
 *
 * <p>Endpoints: {@code GET /health}, {@code POST /files} and {@code GET /files/<fileId>}. Every
 * other path is 404 {@code not_found}.
 *
 * <p><b>Host check.</b> The server listens on 127.0.0.1 only, but a web page in a browser on
 * this machine could still reach it through DNS rebinding. Such requests carry the attacker's host
 * name, so a request is accepted only if it has exactly one Host header, and that header is
 * {@code 127.0.0.1:<port>} or {@code localhost:<port>} (any letter case in "localhost") with this
 * server's actual port. Everything else gets 403 {@code host_not_allowed}, before any routing.
 * A request target in absolute form ({@code GET http://host/path}) names its own host, so it is
 * refused the same way.
 *
 * <p><b>Uploads</b> ({@code POST /files}). The body is the file's raw bytes. Before any of it is
 * read, the request must have exactly one {@code Content-Type: application/octet-stream} (415
 * otherwise), an optional {@code X-Content-SHA256} of exactly 64 hexadecimal characters (400
 * otherwise), and a {@code Content-Length}, if given, within the size limit (413 otherwise). The
 * body is then copied into a new workspace, stopping with 413 as soon as it passes the limit, and
 * handed to {@link FileService#store}. If the client disconnects or sends an incomplete body, no
 * response is sent at all. The workspace is always deleted afterwards.
 *
 * <p><b>Downloads</b> ({@code GET /files/<fileId>}). Any path under {@code /files/} names a
 * file: any method but GET is 405 (HEAD included, as for every endpoint). The rest of the raw path,
 * never percent-decoded, must be exactly a file ID ({@link FileIds#isValid}): 32 lowercase
 * hexadecimal characters. Anything else, including an empty ID, a second path segment, a percent
 * sign or a dot, is 400 {@code invalid_file_id}, so no encoded form can reach the storage. The
 * file is then rebuilt and verified in a new download workspace ({@link FileService#download})
 * before any byte is sent; a file that fails verification is never sent as a success. Range
 * requests are not supported: a Range header is ignored and the whole file is sent with
 * {@code Accept-Ranges: none}. The workspace is always deleted afterwards.
 *
 * <p><b>Stalled downloads.</b> A client that stops reading would otherwise hold a worker thread
 * and the file's workspace forever, so a watchdog ({@link StallWatch}) closes the connection when
 * a download makes no progress for {@link #DOWNLOAD_STALL_TIMEOUT_MILLIS}. Progress is counted
 * only when a whole write of up to 64 KiB has been accepted by the connection; a client that
 * does not take in that much within the timeout is disconnected, even if it is still reading
 * slowly. The watchdog covers two phases, each with its own timeout:
 * <ol>
 *   <li>The request body. A GET normally has none, but one that declares a body is read and
 *       discarded (at most 64 KiB, by the JDK) before anything else happens, so no download
 *       workspace exists while the server waits for it. This matters because closing an exchange
 *       drains an unread request body with a blocking read: a body that is declared but never
 *       sent would otherwise block the watchdog's own close later, and the worker with it.</li>
 *   <li>The response, from its headers to its last byte.</li>
 * </ol>
 * The watchdog is one daemon thread per download phase in progress, so there are never more of
 * them than worker threads.
 *
 * <p><b>Errors.</b> Error bodies are exactly {@link ApiError#toJson()}: fixed text that never
 * contains exception messages, request input or file system paths. If handling a request fails
 * unexpectedly, the client gets 500 {@code internal_error} (if no response has started yet) and
 * the details go to the server's error stream only. The exchange is always closed.
 */
class FileApiHandler implements HttpHandler {

    private static final String HEALTH_PATH = "/health";
    private static final String HEALTH_BODY = "{\"status\":\"ok\"}";
    private static final String FILES_PATH = "/files";
    private static final String FILE_PATH_PREFIX = FILES_PATH + "/";
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final int BUFFER_SIZE = 64 * 1024;

    /**
     * How long a download may go without progress before the connection is closed: 30 seconds,
     * the same as the JDK server's own default for idle connections. While the response is sent,
     * progress is a completed write of up to 64 KiB, so a client must take in roughly 64 KiB
     * every 30 seconds (less only if the connection's buffers have room); a slower client is
     * disconnected. A declared request body must arrive within the same time.
     */
    static final long DOWNLOAD_STALL_TIMEOUT_MILLIS = 30_000;

    /** The port the server actually listens on; the Host header must name exactly this port. */
    private final int port;
    private final PrintStream err;
    private final FileService files;
    private final long downloadStallTimeoutMillis;

    FileApiHandler(int port, PrintStream err, FileService files) {
        this(port, err, files, DOWNLOAD_STALL_TIMEOUT_MILLIS);
    }

    /** As above, with another stall timeout; tests use a short one. */
    FileApiHandler(int port, PrintStream err, FileService files, long downloadStallTimeoutMillis) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1 to 65535, but was " + port);
        }
        if (downloadStallTimeoutMillis < 1) {
            throw new IllegalArgumentException("downloadStallTimeoutMillis must be at least 1");
        }
        this.port = port;
        this.err = Objects.requireNonNull(err, "err must not be null");
        this.files = Objects.requireNonNull(files, "files must not be null");
        this.downloadStallTimeoutMillis = downloadStallTimeoutMillis;
    }

    @Override
    public void handle(HttpExchange exchange) {
        try {
            dispatch(exchange);
        } catch (IOException | RuntimeException e) {
            err.println("Error: " + exchange.getRequestMethod() + " request failed: " + e);
            e.printStackTrace(err);
            // getResponseCode is -1 until response headers have been sent. After that, a clean
            // error response is impossible; closing the exchange ends the connection instead.
            if (exchange.getResponseCode() == -1) {
                try {
                    sendError(exchange, ApiError.INTERNAL_ERROR, null);
                } catch (IOException | RuntimeException sendError) {
                    err.println("Error: could not send the 500 response either: " + sendError);
                }
            }
        } finally {
            exchange.close();
        }
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        URI target = exchange.getRequestURI();
        if (!isAllowedHost(exchange.getRequestHeaders().get("Host"), port) || target.isAbsolute()
                || target.getRawAuthority() != null) {
            sendError(exchange, ApiError.HOST_NOT_ALLOWED, null);
            return;
        }
        // The raw path, never decoded, so encoded tricks such as %2F cannot change the route.
        String path = target.getRawPath();
        if (HEALTH_PATH.equals(path)) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, ApiError.METHOD_NOT_ALLOWED, "GET");
                return;
            }
            respondHealth(exchange);
            return;
        }
        if (FILES_PATH.equals(path)) {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendError(exchange, ApiError.METHOD_NOT_ALLOWED, "POST");
                return;
            }
            handleUpload(exchange);
            return;
        }
        if (path != null && path.startsWith(FILE_PATH_PREFIX)) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, ApiError.METHOD_NOT_ALLOWED, "GET");
                return;
            }
            // Still the raw text: checked exactly as received, so %2e, %2F and the like are refused.
            String fileId = path.substring(FILE_PATH_PREFIX.length());
            if (!FileIds.isValid(fileId)) {
                sendError(exchange, ApiError.INVALID_FILE_ID, null);
                return;
            }
            handleDownload(exchange, fileId);
            return;
        }
        sendError(exchange, ApiError.NOT_FOUND, null);
    }

    /**
     * Answers {@code GET /health}.
     *
     * <p>Why this method is separate: it is a small testability hook, like
     * StorageNode.openForReading. DistributedFsServerTest (same package) overrides it to make a
     * request fail unexpectedly, or to hold a request open while it fills the thread pool.
     */
    void respondHealth(HttpExchange exchange) throws IOException {
        sendJson(exchange, 200, HEALTH_BODY);
    }

    /**
     * True only for exactly one Host header whose value is {@code 127.0.0.1:<port>} or
     * {@code localhost:<port>}, with the given port written as plain digits.
     */
    static boolean isAllowedHost(List<String> hostHeaders, int port) {
        if (hostHeaders == null || hostHeaders.size() != 1) {
            return false;
        }
        String value = hostHeaders.get(0);
        if (value == null || !isPrintableAsciiWithoutSpaces(value)) {
            return false; // only printable ASCII without spaces, so no look-alike letters
        }
        int colon = value.lastIndexOf(':');
        if (colon < 0) {
            return false;
        }
        String host = value.substring(0, colon);
        String portText = value.substring(colon + 1);
        boolean knownHost = host.equals("127.0.0.1") || host.toLowerCase(Locale.ROOT).equals("localhost");
        return knownHost && portText.equals(Integer.toString(port));
    }

    // ---------------------------------------------------------------------
    // POST /files
    // ---------------------------------------------------------------------

    private void handleUpload(HttpExchange exchange) throws IOException {
        Headers headers = exchange.getRequestHeaders();
        if (!isOctetStream(headers.get("Content-Type"))) {
            sendError(exchange, ApiError.UNSUPPORTED_MEDIA_TYPE, null);
            return;
        }
        String expectedSha256 = null;
        List<String> checksums = headers.get("X-Content-SHA256");
        if (checksums != null) {
            if (checksums.size() != 1 || !isSha256Hex(checksums.get(0))) {
                sendError(exchange, ApiError.INVALID_CHECKSUM, null);
                return;
            }
            expectedSha256 = checksums.get(0);
        }
        // The JDK has already refused malformed, negative or repeated Content-Length headers.
        String contentLength = headers.getFirst("Content-Length");
        if (contentLength != null && Long.parseLong(contentLength.trim()) > files.maxUploadBytes()) {
            sendError(exchange, ApiError.PAYLOAD_TOO_LARGE, null);
            return;
        }

        Path workspace;
        try {
            workspace = files.newUploadWorkspace();
        } catch (IOException e) {
            err.println("Error: could not create an upload workspace: " + e);
            sendError(exchange, ApiError.STORAGE_ERROR, null);
            return;
        }
        try {
            Path body = workspace.resolve("body");
            boolean withinLimit;
            try {
                withinLimit = spool(exchange.getRequestBody(), body, files.maxUploadBytes());
            } catch (ClientGoneException e) {
                // The request is incomplete, so no response is sent; closing the exchange ends it.
                err.println("Upload abandoned: the client disconnected or sent an incomplete body ("
                        + e.getCause() + ")");
                return;
            } catch (IOException e) {
                err.println("Error: could not write the upload to its workspace: " + e);
                sendError(exchange, ApiError.STORAGE_ERROR, null);
                return;
            }
            if (!withinLimit) {
                sendError(exchange, ApiError.PAYLOAD_TOO_LARGE, null);
                return;
            }

            FileManifest manifest;
            try {
                manifest = files.store(body, workspace, expectedSha256);
            } catch (FileServiceException e) {
                err.println("Error: upload failed: " + e + (e.getCause() != null ? " (cause: " + e.getCause() + ")" : ""));
                for (Throwable suppressed : e.getSuppressed()) {
                    err.println("  Also: " + suppressed);
                }
                sendError(exchange, errorFor(e.reason()), null);
                return;
            } catch (IOException e) {
                // Reading the spooled body or chunking it inside the workspace failed; nothing was stored.
                err.println("Error: upload failed before anything was stored: " + e);
                sendError(exchange, ApiError.STORAGE_ERROR, null);
                return;
            }
            exchange.getResponseHeaders().set("Location", FILES_PATH + "/" + manifest.fileId());
            sendJson(exchange, 201, manifestJson(manifest));
        } finally {
            files.deleteWorkspace(workspace);
        }
    }

    /** A failure reading the request body: the client disconnected or the body was incomplete. */
    private static final class ClientGoneException extends IOException {
        private static final long serialVersionUID = 1L;

        ClientGoneException(IOException cause) {
            super(cause);
        }
    }

    /**
     * Copies the request body into the new file {@code body}. Returns false, without reading
     * further, as soon as more than {@code maxBytes} have arrived.
     *
     * @throws ClientGoneException if reading the request fails
     * @throws IOException if writing the file fails
     */
    private static boolean spool(InputStream in, Path body, long maxBytes) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long total = 0;
        try (OutputStream out = Files.newOutputStream(body, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (true) {
                int read;
                try {
                    read = in.read(buffer);
                } catch (IOException e) {
                    throw new ClientGoneException(e);
                }
                if (read == -1) {
                    return true;
                }
                total += read;
                if (total > maxBytes) {
                    return false;
                }
                out.write(buffer, 0, read);
            }
        }
    }

    private static String manifestJson(FileManifest manifest) {
        return "{\"fileId\":" + Json.quote(manifest.fileId())
                + ",\"size\":" + manifest.size()
                + ",\"sha256\":" + Json.quote(manifest.sha256())
                + ",\"chunkSize\":" + manifest.chunkSize()
                + ",\"chunkCount\":" + manifest.chunkCount() + "}";
    }

    /** Exactly one Content-Type whose media type, before any ";", is application/octet-stream. */
    private static boolean isOctetStream(List<String> values) {
        if (values == null || values.size() != 1 || values.get(0) == null) {
            return false;
        }
        String value = values.get(0);
        int semicolon = value.indexOf(';');
        String mediaType = (semicolon < 0 ? value : value.substring(0, semicolon)).trim();
        return isPrintableAsciiWithoutSpaces(mediaType) && mediaType.toLowerCase(Locale.ROOT).equals(OCTET_STREAM);
    }

    /** Exactly 64 hexadecimal characters, in either letter case. */
    private static boolean isSha256Hex(String value) {
        if (value == null || value.length() != 64) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPrintableAsciiWithoutSpaces(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c <= 0x20 || c >= 0x7F) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------------
    // GET /files/<fileId>
    // ---------------------------------------------------------------------

    /** {@code fileId} has been checked with {@link FileIds#isValid}, so it is safe to log. */
    private void handleDownload(HttpExchange exchange, String fileId) throws IOException {
        if (!finishRequestBody(exchange, fileId)) {
            return; // the connection was closed; nothing was created and no response can be sent
        }
        Path workspace;
        try {
            workspace = files.newDownloadWorkspace();
        } catch (IOException e) {
            err.println("Error: could not create a download workspace: " + e);
            sendError(exchange, ApiError.STORAGE_ERROR, null);
            return;
        }
        try {
            FileService.Download download;
            try {
                download = prepareDownload(fileId, workspace);
            } catch (FileServiceException e) {
                if (e.reason() != FileServiceException.Reason.FILE_NOT_FOUND) {
                    err.println("Error: download of file " + fileId + " failed: " + e
                            + (e.getCause() != null ? " (cause: " + e.getCause() + ")" : ""));
                }
                sendError(exchange, errorFor(e.reason()), null);
                return;
            }
            sendFile(exchange, fileId, download);
        } finally {
            files.deleteWorkspace(workspace);
        }
    }

    /**
     * Reads and discards the request body, if any, before anything is created (see the class
     * comment). Closing the JDK's request-body stream drains at most 64 KiB with a blocking read,
     * so this runs under a watchdog. If the body does not arrive in time, the watchdog closes the
     * exchange; because no response has started, the JDK then closes the connection directly
     * without reading the body, which makes the blocked read here fail at once.
     *
     * @return true to go on; false if the body never completed and the connection is closed
     */
    private boolean finishRequestBody(HttpExchange exchange, String fileId) {
        try (StallWatch watch = new StallWatch(exchange, downloadStallTimeoutMillis)) {
            try {
                exchange.getRequestBody().close();
            } catch (IOException e) {
                err.println("Download of file " + fileId + " abandoned: "
                        + (watch.fired() ? "its declared request body did not arrive within "
                                + downloadStallTimeoutMillis + " ms" : "its request body could not be read")
                        + " (" + e + ")");
                return false;
            }
            if (!watch.release()) {
                err.println("Download of file " + fileId + " abandoned: its declared request body did not arrive"
                        + " within " + downloadStallTimeoutMillis + " ms");
                return false;
            }
            return true;
        }
    }

    /**
     * Rebuilds and verifies the file in the workspace ({@link FileService#download}).
     *
     * <p>Why this method is separate: it is a small testability hook, like {@link #respondHealth}.
     * DownloadEndpointTest (same package) overrides it to fail unexpectedly after the workspace
     * exists, and to hold a file in the workspace open so that deleting the workspace fails.
     */
    FileService.Download prepareDownload(String fileId, Path workspace) throws IOException {
        return files.download(fileId, workspace);
    }

    /**
     * Sends the verified file. Once the 200 headers are out, a failure can no longer become an
     * error response: the connection is closed instead, which the client sees as a short body.
     */
    private void sendFile(HttpExchange exchange, String fileId, FileService.Download download) throws IOException {
        long size = download.manifest().size();
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", OCTET_STREAM);
        headers.set("X-Content-Type-Options", "nosniff");
        headers.set("Accept-Ranges", "none");
        try (StallWatch watch = new StallWatch(exchange, downloadStallTimeoutMillis)) {
            try {
                // -1: no body (0 would mean "chunked"); otherwise exactly the verified size.
                exchange.sendResponseHeaders(200, size == 0 ? -1 : size);
                if (size == 0) {
                    return;
                }
                byte[] buffer = new byte[BUFFER_SIZE];
                OutputStream out = exchange.getResponseBody();
                try (InputStream in = Files.newInputStream(download.file())) {
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        out.write(buffer, 0, read);
                        watch.progress();
                    }
                }
                // Push out anything still buffered while the watchdog is still watching.
                out.flush();
                watch.progress();
                // From here on only this thread may close the response, never the watchdog: two
                // overlapping closes of the JDK's response stream could report the exchange as
                // finished twice. If the watchdog has already fired, it is closing the exchange.
                if (!watch.release()) {
                    throw new IOException("the stall watchdog closed the connection");
                }
                // Every byte has been written and flushed, and the request body was closed before
                // the download started, so this close does not wait for the client.
                out.close();
            } catch (IOException e) {
                err.println("Download of file " + fileId + " abandoned: "
                        + (watch.fired() ? "the client accepted no data for " + downloadStallTimeoutMillis + " ms"
                                : "the client disconnected or the response could not be written")
                        + " (" + e + ")");
            }
        }
    }

    /**
     * Closes the exchange when no progress has been made for the given time. Closing it makes a
     * read or write that is blocked on the client fail at once. One daemon thread, which ends with
     * the watch.
     *
     * <p>Exactly one side closes: the watchdog only while the state is still WATCHING, and the
     * owner only after {@link #release} has moved it to RELEASED. Both changes are one
     * compare-and-set, so the watchdog can never close an exchange whose owner has taken it back,
     * and the owner learns when the watchdog got there first.
     *
     * <p>Package-private for StallWatchTest (same package).
     */
    static final class StallWatch implements AutoCloseable {
        private static final int WATCHING = 0;
        private static final int RELEASED = 1;
        private static final int FIRED = 2;

        private final HttpExchange exchange;
        private final long timeoutNanos;
        private final Thread thread;
        private final AtomicInteger state = new AtomicInteger(WATCHING);
        private volatile long lastProgress = System.nanoTime();

        StallWatch(HttpExchange exchange, long timeoutMillis) {
            this.exchange = exchange;
            this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            long checkMillis = Math.max(10, Math.min(1000, timeoutMillis / 4));
            this.thread = new Thread(() -> watch(checkMillis), "distributedfs-download-watch");
            thread.setDaemon(true);
            thread.start();
        }

        void progress() {
            lastProgress = System.nanoTime();
        }

        /** True if the watchdog has closed (or is closing) the exchange. */
        boolean fired() {
            return state.get() == FIRED;
        }

        /**
         * Stops the watchdog from closing the exchange from now on. Returns false if it has
         * already fired: the exchange is then being closed and the owner must not close it too.
         */
        boolean release() {
            return state.compareAndSet(WATCHING, RELEASED) || state.get() == RELEASED;
        }

        private void watch(long checkMillis) {
            while (state.get() == WATCHING) {
                try {
                    Thread.sleep(checkMillis);
                } catch (InterruptedException e) {
                    return;
                }
                if (System.nanoTime() - lastProgress >= timeoutNanos && state.compareAndSet(WATCHING, FIRED)) {
                    exchange.close();
                    return;
                }
            }
        }

        /** Releases the watch if it has not fired, and waits for its thread to end. */
        @Override
        public void close() {
            release();
            thread.interrupt();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Whether the watchdog thread is still running (for StallWatchTest). */
        boolean threadAlive() {
            return thread.isAlive();
        }
    }

    /** The API error for a FileService failure. */
    private static ApiError errorFor(FileServiceException.Reason reason) {
        return switch (reason) {
            case CHECKSUM_MISMATCH -> ApiError.CHECKSUM_MISMATCH;
            case FILE_NOT_FOUND -> ApiError.FILE_NOT_FOUND;
            case METADATA_CORRUPT -> ApiError.METADATA_CORRUPT;
            case CHUNK_MISSING -> ApiError.CHUNK_MISSING;
            case INTEGRITY_CHECK_FAILED -> ApiError.INTEGRITY_CHECK_FAILED;
            case STORAGE_ERROR -> ApiError.STORAGE_ERROR;
        };
    }

    // ---------------------------------------------------------------------
    // Responses
    // ---------------------------------------------------------------------

    /** Sends an error response; {@code allow} is the Allow header for 405, otherwise null. */
    private static void sendError(HttpExchange exchange, ApiError error, String allow) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        if (allow != null) {
            headers.set("Allow", allow);
        }
        // Any request body was not read, so the connection is not reused.
        headers.set("Connection", "close");
        sendJson(exchange, error.status(), error.toJson());
    }

    /** Sends a JSON body. HEAD requests get the status and headers only, as HTTP requires. */
    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.US_ASCII);
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json; charset=utf-8");
        headers.set("X-Content-Type-Options", "nosniff");
        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status, -1); // -1: no body (0 would mean "chunked")
            return;
        }
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
