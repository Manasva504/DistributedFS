package com.distributedfs;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * One storage node, as {@link StorageCluster} sees it: a place that stores, returns and deletes
 * chunks by ID. {@link StorageNode} keeps them in a local folder; {@link RemoteStorageNode} asks a
 * node process ({@link StorageNodeServer}) over HTTP.
 *
 * <p><b>Answers versus errors.</b> Every implementation keeps the rule StorageNode has always
 * had: {@code false}, an empty answer or {@link ChunkNotFoundException} is given only when a
 * working node has actually confirmed that the chunk is absent. When the node cannot tell (its
 * folder is gone, it cannot be reached, it timed out, or it answered something unexpected), the
 * method throws a plain {@link IOException}. A broken or unreachable node is never reported as one
 * that simply lacks the chunk.
 *
 * <p><b>Chunk IDs</b> follow StorageNode's rules everywhere, and every implementation checks them
 * before doing anything else (an invalid ID is an {@link IllegalArgumentException}, and a remote
 * node is not contacted).
 */
interface ChunkStore {

    /** What a node's folder entry is, read without following links. */
    enum EntryKind {
        /** A regular file: a chunk, a temp file or something else stored as a file. */
        FILE,
        /** A plain directory. */
        DIRECTORY,
        /** A symbolic link. */
        LINK,
        /** Anything else, for example a Windows junction or another reparse point. */
        OTHER,
        /** The entry's attributes could not be read. */
        UNKNOWN
    }

    /** One entry of a node's folder. */
    record Entry(String name, EntryKind kind) {
    }

    /**
     * Stores the chunk, read from {@code sourceFile}, which is never changed, and returns the
     * number of bytes stored. Never overwrites an existing chunk.
     *
     * @throws IllegalArgumentException if the chunk ID or the source file is invalid
     * @throws FileAlreadyExistsException if the node already holds a chunk with this ID
     * @throws AmbiguousStoreException if the node may or may not have stored the chunk (a remote
     *         node that stopped answering after the request was sent)
     * @throws IOException if the chunk was certainly not stored
     */
    long storeChunk(String chunkId, Path sourceFile) throws IOException;

    /**
     * Copies the chunk into a new file at {@code destination} and returns the number of bytes.
     *
     * @throws ChunkNotFoundException if the node confirms it does not hold the chunk
     * @throws FileAlreadyExistsException if {@code destination} already exists
     * @throws IOException if the node cannot tell, or the copy fails (a partial destination is deleted)
     */
    long retrieveChunk(String chunkId, Path destination) throws IOException;

    /** {@code true} or {@code false} as confirmed by the node (following links); otherwise IOException. */
    boolean hasChunk(String chunkId) throws IOException;

    /**
     * Whether the node holds the chunk as a genuine regular file, with the no-follow checks
     * deleting relies on: {@code false} only if the node's folder is a plain folder with no entry
     * under this ID; an IOException for anything else that is not a regular file.
     */
    boolean holdsRegularChunk(String chunkId) throws IOException;

    /** Deletes the chunk; {@code false} only if the node confirms it is absent. */
    boolean deleteChunk(String chunkId) throws IOException;

    /** Every entry of the node's folder, sorted by name, with its kind read without following links. */
    List<Entry> listEntries() throws IOException;

    /**
     * Deletes temp files that a killed store left behind and returns how many were deleted.
     * {@code warnings} receives one message per entry that was left in place or could not be removed.
     */
    int sweepTempFiles(Consumer<String> warnings) throws IOException;

    /** The node's local folder, for a node in this process; empty for a remote node. */
    Optional<Path> localDirectory();

    /** The node's own ID, for a node that has one (a remote node); empty otherwise. */
    Optional<String> nodeId();

    /** A short description for messages, without anything a client sent. */
    String describe();
}
