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
