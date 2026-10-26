package com.distributedfs;

import java.io.IOException;
import java.util.List;

/**
 * A chunk was stored on some nodes, but not on as many as the cluster's replication factor
 * requires: too few nodes could take it, or one refused because the ID appeared there meanwhile.
 * The copies that were stored are still there; the caller must remove them (FileService rolls the
 * upload back, which deletes every copy of the chunk).
 */
class IncompleteReplicationException extends IOException {

    private static final long serialVersionUID = 1L;

    private final int[] copies;

    IncompleteReplicationException(String message, List<Integer> copies, Throwable cause) {
        super(message, cause);
        this.copies = copies.stream().mapToInt(Integer::intValue).toArray();
    }

    /** The nodes (by index in the cluster) that hold a confirmed copy. */
    List<Integer> copies() {
        return java.util.Arrays.stream(copies).boxed().toList();
    }
}
