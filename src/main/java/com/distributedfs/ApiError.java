package com.distributedfs;

/**
 * Every error the HTTP API can return: the HTTP status, a stable machine-readable code, and a
 * fixed sentence for people.
 *
 * <p>Every error response body has exactly this shape, produced by {@link #toJson()}:
 * <pre>
 *   {"error":"file_not_found","message":"No file with this ID is stored."}
 * </pre>
 *
 * <p><b>Why the messages are fixed.</b> A message never contains exception text, request input,
 * or a file system path. The lower layers put absolute paths into their exception messages (for
 * example StorageNode's FileAlreadyExistsException and ChunkNotFoundException name the storage
 * directory), and those paths must never reach a client. The details of a failure belong in the
 * server console, not in the response.
 *
 * <p>The {@code 500} codes say what kind of problem the stored data has: a file whose metadata
 * exists but whose chunk is gone ({@code chunk_missing}) is reported differently from a file that
 * does not exist at all ({@code file_not_found}, 404) and from data that has changed
 * ({@code integrity_check_failed}).
 */
enum ApiError {

    INVALID_FILE_ID(400, "invalid_file_id",
            "A file ID is exactly 32 characters, each one 0-9 or a-f."),
    INVALID_CHECKSUM(400, "invalid_checksum",
            "The X-Content-SHA256 header must be exactly 64 hexadecimal characters."),
    CHECKSUM_MISMATCH(400, "checksum_mismatch",
            "The uploaded data does not match the X-Content-SHA256 header, so nothing was stored."),
    INVALID_QUERY(400, "invalid_query",
            "This endpoint does not accept this query; /cluster/repair takes only dryRun=true or dryRun=false."),
    INVALID_FILE_NAME(400, "invalid_file_name",
            "The X-File-Name header must be a percent-encoded UTF-8 file name of 1 to 255 characters without a slash."),
    HOST_NOT_ALLOWED(403, "host_not_allowed",
            "This server only accepts requests addressed to 127.0.0.1 or localhost."),
    CROSS_ORIGIN_REFUSED(403, "cross_origin_refused",
            "This server does not accept requests that change data from another website."),
    NOT_FOUND(404, "not_found",
            "There is no endpoint at this path."),
    FILE_NOT_FOUND(404, "file_not_found",
            "No file with this ID is stored."),
    METHOD_NOT_ALLOWED(405, "method_not_allowed",
            "This endpoint does not support this method; the Allow header lists the methods it does support."),
    REPAIR_IN_PROGRESS(409, "repair_in_progress",
            "A repair is already running; try again when it has finished."),
    PAYLOAD_TOO_LARGE(413, "payload_too_large",
            "The upload is larger than this server allows."),
    UNSUPPORTED_MEDIA_TYPE(415, "unsupported_media_type",
            "Send the file as the raw request body with Content-Type: application/octet-stream."),
    CHUNK_MISSING(500, "chunk_missing",
            "The file is recorded, but part of its stored data is missing."),
    INTEGRITY_CHECK_FAILED(500, "integrity_check_failed",
            "The stored data no longer matches the file's recorded SHA-256 checksum."),
    METADATA_CORRUPT(500, "metadata_corrupt",
            "The file's stored metadata is unreadable or invalid."),
    STORAGE_ERROR(500, "storage_error",
            "The storage nodes could not complete the request."),
    INTERNAL_ERROR(500, "internal_error",
            "The server hit an unexpected error."),
    CONSOLE_UNAVAILABLE(503, "console_unavailable",
            "The web console's files were not found when this server started.");

    private final int status;
    private final String code;
    private final String message;

    ApiError(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    /** The HTTP status code, for example 404. */
    int status() {
        return status;
    }

    /** The value of the {@code error} field, for example {@code file_not_found}. Never changes. */
    String code() {
        return code;
    }

    /** The value of the {@code message} field: one fixed sentence. */
    String message() {
        return message;
    }

    /** The complete response body: {@code {"error":"<code>","message":"<message>"}}. */
    String toJson() {
        return "{\"error\":" + Json.quote(code) + ",\"message\":" + Json.quote(message) + "}";
    }
}
