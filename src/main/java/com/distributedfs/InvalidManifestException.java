package com.distributedfs;

import java.io.IOException;

/**
 * A manifest or pending-upload marker exists but cannot be trusted: its text breaks the format,
 * its values are inconsistent, or the entry is not a plain file.
 *
 * <p>This is deliberately different from "not found". A file whose metadata is damaged is
 * reported as damaged (the HTTP API's {@code metadata_corrupt}), never as a file that does not
 * exist. The message is for the server console only; it may contain file system paths.
 */
class InvalidManifestException extends IOException {

    private static final long serialVersionUID = 1L;

    InvalidManifestException(String message) {
        super(message);
    }

    InvalidManifestException(String message, Throwable cause) {
        super(message, cause);
    }
}
