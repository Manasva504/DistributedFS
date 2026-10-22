package com.distributedfs;

import java.io.IOException;

/**
 * The server refuses to use a data directory. {@link #reason()} says why; the message is for the
 * server console and names the path involved.
 */
class DataDirectoryException extends IOException {

    private static final long serialVersionUID = 1L;

    enum Reason {
        /** The path exists but is not a folder. */
        NOT_A_DIRECTORY,
        /** The folder has contents but no DistributedFS marker file, so it belongs to someone else. */
        UNRECOGNIZED,
        /** The marker file exists but is not exactly what the server writes. */
        INVALID_MARKER,
        /** Another server process (or another server in this process) holds the folder's lock. */
        IN_USE,
        /** A folder or file the server needs is a link, a junction, or the wrong kind of entry. */
        UNSAFE_LAYOUT
    }

    private final Reason reason;

    DataDirectoryException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    Reason reason() {
        return reason;
    }
}
