package com.distributedfs;

/**
 * Time limits and reply limits for talking to a remote storage node ({@link RemoteStorageNode}).
 * Production uses {@link #defaults}; tests use limits of a few hundred milliseconds.
 *
 * @param connectTimeoutMillis how long to wait for the connection to be set up
 * @param requestTimeoutMillis how long a request other than a store may wait for the reply's headers
 * @param storeTimeoutMillis   how long a store may take in total, from sending the chunk to the reply's
 *                             headers; a 1 MiB chunk on loopback needs milliseconds
 * @param stallTimeoutMillis   how long a reply's body may go without new bytes (a large chunk keeps
 *                             arriving and is never cut off; a stopped one is)
 * @param maxReplyBytes        the largest small reply accepted (fields or an error)
 * @param maxListingBytes      the largest listing of a node's entries accepted
 */
record RemoteNodeConfig(long connectTimeoutMillis, long requestTimeoutMillis, long storeTimeoutMillis,
                        long stallTimeoutMillis, int maxReplyBytes, int maxListingBytes) {

    RemoteNodeConfig {
        if (connectTimeoutMillis < 1 || requestTimeoutMillis < 1 || storeTimeoutMillis < 1 || stallTimeoutMillis < 1
                || maxReplyBytes < 1 || maxListingBytes < 1) {
            throw new IllegalArgumentException("every limit must be at least 1");
        }
    }

    /** 2 s to connect, 10 s for a reply, 30 s for a store, 30 s without progress, 64 KiB replies, 64 MiB listings. */
    static RemoteNodeConfig defaults() {
        return new RemoteNodeConfig(2_000, 10_000, 30_000, 30_000, 64 * 1024, 64 * 1024 * 1024);
    }
}
