package com.distributedfs;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Thrown when a storage node is asked for a chunk it does not hold.
 *
 * <p>In a distributed file system this is a normal event: a node may never have received a
 * chunk, or may have lost it. Having a dedicated exception lets callers catch exactly this case
 * (and, in later features, ask another node) without confusing it with other I/O problems.
 *
 * <p>It is only thrown when the node can actually tell that the chunk is absent. If the node
 * cannot tell, for example because its storage directory has disappeared, it throws a plain
 * {@link IOException} instead.
 *
 * <p>It extends {@link IOException}, so code that handles every I/O problem the same way
 * still works without changes.
 */
public class ChunkNotFoundException extends IOException {

    private final String chunkId;

    public ChunkNotFoundException(String chunkId, Path storageDir) {
        super("Chunk \"" + chunkId + "\" is not stored on this node (storage directory: " + storageDir + ")");
        this.chunkId = chunkId;
    }

    /**
     * Creates the exception with a custom message, for code that looks in more than one place,
     * such as StorageCluster, which checks every node before reporting a chunk as missing.
     */
    public ChunkNotFoundException(String chunkId, String message) {
        super(message);
        this.chunkId = chunkId;
    }

    /** The ID of the chunk that was not found. */
    public String getChunkId() {
        return chunkId;
    }
}
