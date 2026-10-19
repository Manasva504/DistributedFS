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

/**
 * The HTTP API's single request handler: it checks the Host header, routes by exact path, and
 * writes every response, including errors.
 *
 * <p>Endpoints so far: {@code GET /health} and {@code POST /files}. Every other path is 404
 * {@code not_found}.
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
 * <p><b>Errors.</b> Error bodies are exactly {@link ApiError#toJson()}: fixed text that never
 * contains exception messages, request input or file system paths. If handling a request fails
 * unexpectedly, the client gets 500 {@code internal_error} (if no response has started yet) and
 * the details go to the server's error stream only. The exchange is always closed.
 */
class FileApiHandler implements HttpHandler {

    private static final String HEALTH_PATH = "/health";
    private static final String HEALTH_BODY = "{\"status\":\"ok\"}";
    private static final String FILES_PATH = "/files";
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final int BUFFER_SIZE = 64 * 1024;

    /** The port the server actually listens on; the Host header must name exactly this port. */
    private final int port;
    private final PrintStream err;
    private final FileService files;

    FileApiHandler(int port, PrintStream err, FileService files) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1 to 65535, but was " + port);
        }
        this.port = port;
        this.err = Objects.requireNonNull(err, "err must not be null");
        this.files = Objects.requireNonNull(files, "files must not be null");
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
                sendError(exchange, e.reason() == FileServiceException.Reason.CHECKSUM_MISMATCH
                        ? ApiError.CHECKSUM_MISMATCH : ApiError.STORAGE_ERROR, null);
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
