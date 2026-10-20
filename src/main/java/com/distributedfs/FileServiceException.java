package com.distributedfs;

import java.io.IOException;

/**
 * A file operation failed in a way the HTTP API reports with a specific error code.
 * {@link #reason()} says which; the message and cause are for the server console only.
 */
class FileServiceException extends IOException {

    private static final long serialVersionUID = 1L;

    enum Reason {
        /** The uploaded bytes do not match the checksum the client sent; nothing was stored. */
        CHECKSUM_MISMATCH,
        /** No manifest exists for the requested file ID: the file was never committed. */
        FILE_NOT_FOUND,
        /** The requested file's manifest exists but cannot be trusted. */
        METADATA_CORRUPT,
        /** The requested file is committed, but every node confirms one of its chunks is gone. */
        CHUNK_MISSING,
        /** The requested file's rebuilt content does not match its manifest's size or SHA-256. */
        INTEGRITY_CHECK_FAILED,
        /** The storage could not complete the operation. */
        STORAGE_ERROR
    }

    private final Reason reason;

    FileServiceException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    Reason reason() {
        return reason;
    }
}
