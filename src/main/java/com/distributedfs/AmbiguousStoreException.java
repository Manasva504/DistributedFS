package com.distributedfs;

import java.io.IOException;

/**
 * A store whose outcome is unknown: the request reached a remote node, but no clear answer came
 * back (a timeout, a reset or closed connection, or a reply that does not make sense). The chunk
 * may or may not be stored on that node.
 *
 * <p>{@link StorageCluster} never tries another node after this, because that could put a second
 * copy of the chunk on another node. The caller must treat the chunk as possibly stored.
 */
class AmbiguousStoreException extends IOException {

    private static final long serialVersionUID = 1L;

    AmbiguousStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
