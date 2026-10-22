package com.distributedfs;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Coordinates several {@link StorageNode}s, so callers can store and retrieve chunks by ID
 * without managing individual nodes. All file copying is done by the nodes themselves.
 *
 * <p><b>Nodes.</b> The caller creates the nodes and passes them in. The cluster keeps its own
 * copy of the list, which never changes; "node i" means the node at position i.
 *
 * <p><b>Placement.</b> Stores go round-robin: node 0, 1, 2, ... then back to node 0. If storing
 * on a node fails with an I/O error, the next node is tried. A failed StorageNode store never
 * leaves a chunk behind, so this cannot create a second copy.
 *
 * <p><b>One node per chunk ID.</b> Before storing, the cluster asks every node whether it already
 * holds the ID. If one does, the store fails with {@link FileAlreadyExistsException}. If any node
 * cannot answer, the store is refused, because the cluster cannot rule out a duplicate.
 *
 * <p><b>No index.</b> The cluster does not remember where chunks are; it asks the nodes every
 * time. A new cluster over the same node directories (for example after a restart) therefore
 * finds every chunk already stored. The cost is one existence check per node per operation.
 *
 * <p><b>Answers versus errors.</b> As with StorageNode, "could not check" is never reported as a
 * normal answer: if no node holds a chunk but some node could not be checked, the cluster throws
 * a plain {@link IOException} rather than returning {@code false} or throwing
 * {@link ChunkNotFoundException}.
 *
 * <p><b>Deleting.</b> {@link #deleteChunk(String)} checks every node before deleting anything,
 * with a check that never follows links (see {@link StorageNode#deleteChunk}). It deletes only
 * when every node answered clearly and exactly one holds the chunk; otherwise nothing is deleted.
 * It does not change the round-robin position.
 *
 * <p><b>Single-threaded.</b> A StorageCluster must be used from one thread at a time, and its
 * nodes should not be written to by anything else meanwhile. Checking every node and then storing
 * is not atomic, so two threads, two clusters sharing nodes, or a caller storing through a
 * StorageNode directly could put the same ID on two nodes.
 */
public class StorageCluster {

    private final List<StorageNode> nodes;

    /** The node the next store tries first. Moves on after every successful store. */
    private int nextNodeIndex = 0;

    /**
     * Creates a cluster over {@code nodes}, in this order.
     *
     * @throws NullPointerException if the list or any element is null
     * @throws IllegalArgumentException if the list is empty, the same node object is listed
     *         twice, or two nodes use the same storage directory or one inside the other
     */
    public StorageCluster(List<StorageNode> nodes) {
        Objects.requireNonNull(nodes, "nodes must not be null");
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) == null) {
                throw new NullPointerException("nodes[" + i + "] must not be null");
            }
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("A cluster needs at least one node");
        }
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                if (nodes.get(i) == nodes.get(j)) {
                    throw new IllegalArgumentException(
                            "The same StorageNode object is listed twice, as nodes " + i + " and " + j);
                }
                Path firstDir = nodes.get(i).storageDir();
                Path secondDir = nodes.get(j).storageDir();
                // Overlapping directories would let one node see, and damage, another node's files.
                if (firstDir.startsWith(secondDir) || secondDir.startsWith(firstDir)) {
                    throw new IllegalArgumentException("Nodes " + i + " and " + j
                            + " use overlapping storage directories: " + firstDir + " and " + secondDir);
                }
            }
        }
        // The cluster's own copy: later changes to the caller's list have no effect.
        this.nodes = List.copyOf(nodes);
    }

    /** The number of nodes in this cluster. */
    public int nodeCount() {
        return nodes.size();
    }

    /**
     * Stores the chunk on one node, chosen round-robin, and returns the number of bytes stored.
     * The source file is only read, never changed.
     *
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the chunk ID is invalid, or the source file is missing,
     *         a directory, not a regular file, or not readable (checked before any node stores anything)
     * @throws FileAlreadyExistsException if a node already holds a chunk with this ID
     * @throws IOException if some node cannot confirm whether it holds the ID (nothing is stored),
     *         or if every node fails to store the chunk (each node's error is attached as suppressed)
     */
    public long storeChunk(String chunkId, Path sourceFile) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        // Check the source here, before any node is involved, so a bad source file is reported
        // as such instead of being treated as a node failure.
        validateSourceFile(sourceFile);

        // Every node must confirm that the ID is unused. Node 0's check also validates the ID.
        NodeAnswers answers = askEveryNode(chunkId);
        if (!answers.holders.isEmpty()) {
            throw new FileAlreadyExistsException(null, null,
                    "Chunk \"" + chunkId + "\" already exists on node " + answers.holders.get(0));
        }
        if (!answers.errors.isEmpty()) {
            throw withNodeErrors("Cannot store chunk \"" + chunkId + "\": " + describeNodes(answers.uncheckedNodes)
                    + " could not confirm that the ID is unused", answers.errors);
        }

        // Try each node once, starting with the next one in round-robin order.
        List<IOException> storeErrors = new ArrayList<>();
        for (int attempt = 0; attempt < nodes.size(); attempt++) {
            int index = (nextNodeIndex + attempt) % nodes.size();
            try {
                long bytesStored = nodes.get(index).storeChunk(chunkId, sourceFile);
                nextNodeIndex = (index + 1) % nodes.size();
                return bytesStored;
            } catch (FileAlreadyExistsException e) {
                // The chunk appeared on this node after the check above. Storing it on another
                // node would create a second copy, so stop here.
                throw e;
            } catch (IOException e) {
                // A failed StorageNode store leaves no chunk behind, so trying the next node
                // cannot create a duplicate.
                storeErrors.add(e);
            }
        }
        throw withNodeErrors("No node could store chunk \"" + chunkId + "\" (tried all " + nodes.size()
                + " nodes; each node's error is attached as suppressed)", storeErrors);
    }

    /**
     * Copies the chunk from the node that holds it into a new file at {@code destination}, and
     * returns the number of bytes copied. The chunk stays on its node.
     *
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the chunk ID is invalid, or {@code destination} is inside
     *         any node's storage directory (checked before anything is created)
     * @throws ChunkNotFoundException if every node confirms it does not hold the chunk
     * @throws IOException if the chunk cannot be located (see {@link #locateChunk}); otherwise
     *         exactly what the holding node's {@link StorageNode#retrieveChunk} throws, for example
     *         {@link FileAlreadyExistsException} if {@code destination} already exists
     */
    public long retrieveChunk(String chunkId, Path destination) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        requireOutsideEveryNode(destination);
        int holder = locateChunk(chunkId);
        return nodes.get(holder).retrieveChunk(chunkId, destination);
    }

    /**
     * Reports whether the cluster holds the chunk: {@code true} if at least one node holds it,
     * {@code false} only if every node confirms it does not.
     *
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid
     * @throws IOException if no node holds it but some node could not be checked
     */
    public boolean hasChunk(String chunkId) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        NodeAnswers answers = askEveryNode(chunkId);
        if (!answers.holders.isEmpty()) {
            return true;
        }
        if (!answers.errors.isEmpty()) {
            throw cannotTell(chunkId, answers);
        }
        return false;
    }

    /**
     * Returns the index of the node that holds the chunk. Nodes that could not be checked are
     * ignored once a holder is found, because the cluster never stores one ID on two nodes.
     *
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid
     * @throws ChunkNotFoundException if every node confirms it does not hold the chunk
     * @throws IOException if no node holds it but some node could not be checked, or if more than
     *         one node holds it (the cluster refuses to choose between copies)
     */
    public int locateChunk(String chunkId) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        NodeAnswers answers = askEveryNode(chunkId);
        if (answers.holders.size() > 1) {
            throw new IOException("Chunk \"" + chunkId + "\" is stored on more than one node ("
                    + describeNodes(answers.holders) + "); refusing to choose between copies");
        }
        if (answers.holders.size() == 1) {
            return answers.holders.get(0);
        }
        if (!answers.errors.isEmpty()) {
            throw cannotTell(chunkId, answers);
        }
        throw new ChunkNotFoundException(chunkId,
                "Chunk \"" + chunkId + "\" is not stored on any of the " + nodes.size() + " nodes in this cluster");
    }

    /**
     * Deletes the chunk from the one node that holds it.
     *
     * <p>First every node is asked, and nothing is deleted while asking. The question is not
     * {@link #hasChunk}, which follows links, but a stricter one: a node holds the chunk only if
     * its storage directory is a plain directory and the chunk's entry is a regular file, both read
     * without following links. The chunk is deleted only if every node answered, and exactly one
     * holds it. The round-robin position for stores is not changed.
     *
     * <p>Like the rest of this class, this assumes a single thread and nodes that nothing else
     * writes to: checking the nodes and then deleting is not atomic.
     *
     * @return {@code true} if the chunk was deleted from its node; {@code false} if every node
     *         confirms it does not hold the chunk, or the holding node found it already gone when
     *         deleting (which that node confirms by checking again)
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid (checked before any node's
     *         files are read)
     * @throws IOException if some node could not be checked safely (for example its storage is
     *         unavailable, or the entry under the chunk's ID is a link, junction or directory) or
     *         more than one node holds the chunk, in which cases nothing is deleted; or exactly
     *         what the holding node's {@link StorageNode#deleteChunk} throws
     */
    public boolean deleteChunk(String chunkId) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        NodeAnswers answers = askEveryNodeForDeletion(chunkId);
        if (!answers.errors.isEmpty()) {
            throw withNodeErrors("Cannot delete chunk \"" + chunkId + "\": " + describeNodes(answers.uncheckedNodes)
                    + " could not be checked safely, so nothing was deleted", answers.errors);
        }
        if (answers.holders.size() > 1) {
            throw new IOException("Chunk \"" + chunkId + "\" is stored on more than one node ("
                    + describeNodes(answers.holders) + "); refusing to delete any copy");
        }
        if (answers.holders.isEmpty()) {
            return false;
        }
        return nodes.get(answers.holders.get(0)).deleteChunk(chunkId);
    }

    /** What every node said about one chunk ID. */
    private static final class NodeAnswers {
        final List<Integer> holders = new ArrayList<>();        // nodes that hold the chunk
        final List<Integer> uncheckedNodes = new ArrayList<>(); // nodes that could not tell
        final List<IOException> errors = new ArrayList<>();     // why they could not tell
    }

    /**
     * Asks every node whether it holds the chunk. An invalid ID is rejected by node 0's
     * {@code hasChunk} with IllegalArgumentException before any file is touched.
     */
    private NodeAnswers askEveryNode(String chunkId) {
        NodeAnswers answers = new NodeAnswers();
        for (int i = 0; i < nodes.size(); i++) {
            try {
                if (nodes.get(i).hasChunk(chunkId)) {
                    answers.holders.add(i);
                }
            } catch (IOException e) {
                answers.uncheckedNodes.add(i);
                answers.errors.add(e);
            }
        }
        return answers;
    }

    /**
     * Like {@link #askEveryNode}, but with the no-follow question deleting needs
     * ({@link StorageNode#holdsRegularChunkFile}). An invalid ID is rejected by node 0 with
     * IllegalArgumentException before any file is read.
     */
    private NodeAnswers askEveryNodeForDeletion(String chunkId) {
        NodeAnswers answers = new NodeAnswers();
        for (int i = 0; i < nodes.size(); i++) {
            try {
                if (nodes.get(i).holdsRegularChunkFile(chunkId)) {
                    answers.holders.add(i);
                }
            } catch (IOException e) {
                answers.uncheckedNodes.add(i);
                answers.errors.add(e);
            }
        }
        return answers;
    }

    private IOException cannotTell(String chunkId, NodeAnswers answers) {
        return withNodeErrors("Cannot tell whether chunk \"" + chunkId + "\" exists: no node holds it, but "
                + describeNodes(answers.uncheckedNodes) + " could not be checked", answers.errors);
    }

    /**
     * Rejects a destination inside any node's storage directory, not only the directory of the
     * node that holds the chunk: a copy written into another node's directory could later be
     * mistaken for a chunk stored there. Like StorageNode's own check, this compares normalized
     * paths as text, so symbolic links, junctions and Windows short names are not resolved.
     */
    private void requireOutsideEveryNode(Path destination) {
        Path normalizedDestination = destination.toAbsolutePath().normalize();
        for (int i = 0; i < nodes.size(); i++) {
            Path nodeDir = nodes.get(i).storageDir();
            if (normalizedDestination.startsWith(nodeDir)) {
                throw new IllegalArgumentException("Destination must be outside every node's storage directory,"
                        + " but is inside node " + i + "'s (" + nodeDir + "): " + destination);
            }
        }
    }

    /**
     * The same checks StorageNode makes, repeated here because StorageNode's are private. Doing
     * them before any node is involved means a bad source file is never tried on node after node.
     */
    private static void validateSourceFile(Path sourceFile) {
        Objects.requireNonNull(sourceFile, "sourceFile must not be null");
        if (!Files.exists(sourceFile)) {
            throw new IllegalArgumentException("Source file does not exist: " + sourceFile);
        }
        if (Files.isDirectory(sourceFile)) {
            throw new IllegalArgumentException("Source path is a directory, not a file: " + sourceFile);
        }
        if (!Files.isRegularFile(sourceFile)) {
            throw new IllegalArgumentException("Source path is not a regular file: " + sourceFile);
        }
        if (!Files.isReadable(sourceFile)) {
            throw new IllegalArgumentException("Source file is not readable: " + sourceFile);
        }
    }

    /** Builds an IOException with the given message, keeping every node's own error as suppressed. */
    private static IOException withNodeErrors(String message, List<IOException> nodeErrors) {
        IOException combined = new IOException(message);
        for (IOException nodeError : nodeErrors) {
            combined.addSuppressed(nodeError);
        }
        return combined;
    }

    /** "node 1" or "nodes [0, 2]", for messages. */
    private static String describeNodes(List<Integer> indexes) {
        return indexes.size() == 1 ? "node " + indexes.get(0) : "nodes " + indexes;
    }
}
