package com.distributedfs;

import java.io.IOException;
import java.util.List;

/**
 * Startup recovery stopped because it could not establish that acting was safe. The message lists
 * each interrupted upload concerned and why, and says whether recovery had changed anything before
 * it stopped. The server does not start.
 */
class UnsafeRecoveryException extends IOException {

    private static final long serialVersionUID = 1L;

    private final String[] fileIds;
    private final boolean changedAnything;

    /**
     * @param fileIds         the interrupted uploads that could not be recovered safely
     * @param changedAnything whether recovery had already deleted anything before stopping
     */
    UnsafeRecoveryException(String message, List<String> fileIds, boolean changedAnything, Throwable cause) {
        super(message, cause);
        this.fileIds = fileIds.toArray(new String[0]);
        this.changedAnything = changedAnything;
    }

    /** The file IDs of the interrupted uploads that could not be recovered safely. */
    List<String> fileIds() {
        return List.of(fileIds);
    }

    /** False if nothing on disk was changed before recovery stopped. */
    boolean changedAnything() {
        return changedAnything;
    }
}
