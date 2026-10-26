package com.distributedfs;

import java.io.IOException;

/**
 * A remote storage node could not be reached at all: the connection was refused or could not be
 * set up in time, so nothing was sent and the node said nothing.
 *
 * <p>This is narrower than "could not check". A node that accepted the connection and then timed
 * out, reset it, or answered something unexpected is a plain {@link IOException}: it may be alive
 * and may hold data, so the cluster treats it as unknown. Only a node that is unavailable in this
 * sense is skipped by {@link StorageCluster}'s duplicate check before a store. A local
 * {@link StorageNode} never throws this.
 */
class NodeUnavailableException extends IOException {

    private static final long serialVersionUID = 1L;

    NodeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
