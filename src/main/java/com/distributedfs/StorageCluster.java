package com.distributedfs;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Coordinates several storage nodes ({@link ChunkStore}s: local {@link StorageNode}s, remote
 * {@link RemoteStorageNode}s, or a mix), so callers can store and retrieve chunks by ID without
 * managing individual nodes. All file copying is done by the nodes themselves.
 *
 * <p><b>Nodes.</b> The caller creates the nodes and passes them in. The cluster keeps its own
 * copy of the list, which never changes; "node i" means the node at position i.
 *
 * <p><b>Replication.</b> Each chunk is stored on {@link #replicas()} different nodes (1 unless
 * the caller asks for more; the server uses 2, or 1 in a one-node cluster). A node never holds two
 * copies of one chunk. A chunk may have more copies than the replication factor, for example after
 * a node that was down comes back with copies that were replaced meanwhile: that chunk is
 * over-replicated, which is reported (and trimmed by repair) but is not an error; it is read and
 * deleted like any other.
 *
 * <p><b>Placement.</b> Stores go round-robin. Copy 1 goes on the next node in turn, copy 2 on the
 * next different node after it, and so on; the next store starts at the node after this store's
 * copy 1. If storing on a node fails with an I/O error that means the chunk was certainly not
 * stored, the next node is tried: a failed StorageNode store never leaves a chunk behind, and a
 * remote node that refused the connection or answered with an error stored nothing either. A
 * store succeeds only once every copy is confirmed; with fewer, the copies made are reported in
 * an {@link IncompleteReplicationException} and the caller removes them. A store whose outcome is
 * unknown ({@link AmbiguousStoreException}, for example a remote node that stopped answering after
 * the chunk was sent) ends the store at once, and no other node is tried, because the chunk may
 * already be on that node; the caller must treat the chunk as possibly stored.
 *
 * <p><b>Duplicate check.</b> Before storing, the cluster asks every node whether it already holds
 * the ID ({@link #requireUnusedOnReachableNodes}). If one does, the store fails with
 * {@link FileAlreadyExistsException}. A node that cannot be reached at all
 * ({@link NodeUnavailableException}: the connection was refused or timed out) is skipped, and not
 * used for this store. Any other node that cannot answer refuses the store, because it is alive and
 * may hold data. Chunk IDs are unique anyway, because the server's file IDs are 128-bit random
 * values checked against its manifests.
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
 * <p><b>Verified reads.</b> {@link #retrieveVerifiedChunk} tries the nodes in turn and keeps the
 * first copy whose SHA-256 is the expected one, skipping nodes that are down, have no copy, or have
 * a damaged one. It fails only when no good copy exists.
 *
 * <p><b>Deleting.</b> {@link #deleteChunk(String)} checks every node before deleting anything,
 * with a check that never follows links ({@link ChunkStore#holdsRegularChunk}). It deletes only
 * when every node answered clearly, and then every copy; otherwise nothing is deleted. It does not
 * change the round-robin position.
 *
 * <p><b>Single-threaded.</b> A StorageCluster must be used from one thread at a time, and its
 * nodes should not be written to by anything else meanwhile. Checking every node and then storing
 * is not atomic, so two threads, two clusters sharing nodes, or a caller storing through a
 * node directly could put the same ID on more nodes than intended.
 */
public class StorageCluster {

    private final List<ChunkStore> nodes;
    private final int replicas;

    /** The node the next store tries first. Moves on after every successful store. */
    private int nextNodeIndex = 0;

    /** What a store did: the bytes per copy, the nodes holding the copies, and the unreachable nodes it skipped. */
    record StoreResult(long bytes, List<Integer> copies, List<Integer> skipped) {
    }

    /**
     * What a verified read did: the bytes copied, the node whose copy was used, a description of
     * each copy that was skipped because it was damaged or its node could not be read (nodes that
     * simply have no copy are not listed), and the nodes that could not be read.
     */
    record ReadResult(long bytes, int node, List<String> problems, List<Integer> failedNodes) {
    }

    /** Every copy of a chunk that could be read differs from the expected SHA-256. */
    static final class CorruptCopiesException extends IOException {
        private static final long serialVersionUID = 1L;

        CorruptCopiesException(String message) {
            super(message);
        }
    }

    /**
     * Creates a cluster over {@code nodes}, in this order, that keeps one copy of each chunk.
     *
     * @throws NullPointerException if the list or any element is null
     * @throws IllegalArgumentException if the list is empty, the same node object is listed
     *         twice, two local nodes use the same storage directory or one inside the other, or
     *         two remote nodes have the same node ID (two addresses of one node process)
     */
    public StorageCluster(List<? extends ChunkStore> nodes) {
        this(nodes, 1);
    }

    /**
     * Creates a cluster over {@code nodes}, in this order, that keeps {@code replicas} copies of
     * each chunk, each on a different node.
     *
     * @throws IllegalArgumentException as above, or if {@code replicas} is not from 1 to the number of nodes
     */
    public StorageCluster(List<? extends ChunkStore> nodes, int replicas) {
        Objects.requireNonNull(nodes, "nodes must not be null");
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) == null) {
                throw new NullPointerException("nodes[" + i + "] must not be null");
            }
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("A cluster needs at least one node");
        }
        if (replicas < 1 || replicas > nodes.size()) {
            throw new IllegalArgumentException("replicas must be from 1 to the number of nodes (" + nodes.size()
                    + "), but was " + replicas);
        }
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                if (nodes.get(i) == nodes.get(j)) {
                    throw new IllegalArgumentException(
                            "The same StorageNode object is listed twice, as nodes " + i + " and " + j);
                }
                Optional<Path> firstDir = nodes.get(i).localDirectory();
                Optional<Path> secondDir = nodes.get(j).localDirectory();
                // Overlapping directories would let one node see, and damage, another node's files.
                if (firstDir.isPresent() && secondDir.isPresent() && (firstDir.get().startsWith(secondDir.get())
                        || secondDir.get().startsWith(firstDir.get()))) {
                    throw new IllegalArgumentException("Nodes " + i + " and " + j
                            + " use overlapping storage directories: " + firstDir.get() + " and " + secondDir.get());
                }
                // The same for remote nodes: two addresses that reach one node process would make
                // one stored chunk look like two copies, and a duplicate check count it twice.
                Optional<String> firstId = nodes.get(i).nodeId();
                if (firstId.isPresent() && firstId.equals(nodes.get(j).nodeId())) {
                    throw new IllegalArgumentException("Nodes " + i + " and " + j
                            + " are the same storage node (node ID " + firstId.get() + ")");
                }
            }
        }
        // The cluster's own copy: later changes to the caller's list have no effect.
        this.nodes = List.copyOf(nodes);
        this.replicas = replicas;
    }

    /** The number of nodes in this cluster. */
    public int nodeCount() {
        return nodes.size();
    }

    /** How many copies of each chunk the cluster stores, each on a different node. */
    public int replicas() {
        return replicas;
    }

    /**
     * Node {@code index} itself. Package-private for repair ({@link RepairService}), which must
     * read, store and delete single copies on chosen nodes rather than go through the cluster's
     * placement.
     */
    ChunkStore node(int index) {
        return nodes.get(index);
    }

    /** "node 2", for messages, with the node's own description. */
    String describeNode(int index) {
        return "node " + index + " (" + nodes.get(index).describe() + ")";
    }

    /**
     * Stores the chunk on {@link #replicas()} nodes, as {@link #storeReplicated}, and returns the
     * number of bytes stored (per copy). The source file is only read, never changed.
     *
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the chunk ID is invalid, or the source file is missing,
     *         a directory, not a regular file, or not readable (checked before any node stores anything)
     * @throws FileAlreadyExistsException if a node already holds a chunk with this ID (nothing is stored)
     * @throws AmbiguousStoreException if a node may or may not have stored the chunk; no other
     *         node is tried, and the caller must treat the chunk as possibly stored
     * @throws IncompleteReplicationException if some, but too few, copies were stored
     * @throws IOException if some reachable node cannot confirm whether it holds the ID (nothing is
     *         stored), or if no node could store the chunk (each node's error is attached as suppressed)
     */
    public long storeChunk(String chunkId, Path sourceFile) throws IOException {
        return storeReplicated(chunkId, sourceFile).bytes();
    }

    /**
     * Stores the chunk on {@link #replicas()} different nodes, as described in the class comment,
     * and says where. Exceptions as for {@link #storeChunk}.
     */
    StoreResult storeReplicated(String chunkId, Path sourceFile) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        // Check the source here, before any node is involved, so a bad source file is reported
        // as such instead of being treated as a node failure.
        validateSourceFile(sourceFile);
        List<Integer> unavailable = requireUnusedOnReachableNodes(chunkId);

        // Each node is tried at most once, starting with the next one in round-robin order, so no
        // node can get two copies.
        List<Integer> copies = new ArrayList<>();
        List<IOException> storeErrors = new ArrayList<>();
        long bytesStored = -1;
        for (int attempt = 0; attempt < nodes.size() && copies.size() < replicas; attempt++) {
            int index = (nextNodeIndex + attempt) % nodes.size();
            if (unavailable.contains(index)) {
                continue;
            }
            try {
                bytesStored = nodes.get(index).storeChunk(chunkId, sourceFile);
                copies.add(index);
            } catch (FileAlreadyExistsException e) {
                // The chunk appeared on this node after the check above. Storing it on another
                // node would create one copy too many, so stop here.
                if (copies.isEmpty()) {
                    throw e;
                }
                throw new IncompleteReplicationException("Chunk \"" + chunkId + "\" appeared on node " + index
                        + " while it was being stored; copies were stored on nodes " + copies, copies, e);
            } catch (AmbiguousStoreException e) {
                // The chunk may be on this node now. Storing it on another node could create a
                // copy that nothing knows about, so stop here too.
                for (IOException earlier : storeErrors) {
                    e.addSuppressed(earlier);
                }
                if (!copies.isEmpty()) {
                    e.addSuppressed(new IOException("copies of chunk \"" + chunkId + "\" were already confirmed on nodes "
                            + copies));
                }
                throw e;
            } catch (IOException e) {
                // The chunk was certainly not stored on this node (a failed StorageNode store
                // leaves nothing behind, and a remote node said so or was never reached), so
                // trying the next node cannot create a duplicate.
                storeErrors.add(e);
            }
        }
        if (copies.size() == replicas) {
            nextNodeIndex = (copies.get(0) + 1) % nodes.size();
            return new StoreResult(bytesStored, List.copyOf(copies), List.copyOf(unavailable));
        }
        if (copies.isEmpty()) {
            throw withNodeErrors("No node could store chunk \"" + chunkId + "\" (tried all " + nodes.size()
                    + " nodes" + (unavailable.isEmpty() ? "" : ", skipping unreachable " + describeNodes(unavailable))
                    + "; each node's error is attached as suppressed)", storeErrors);
        }
        IncompleteReplicationException incomplete = new IncompleteReplicationException("Chunk \"" + chunkId
                + "\" was stored on only " + copies.size() + " of the " + replicas + " nodes it needs (nodes " + copies
                + (unavailable.isEmpty() ? "" : "; unreachable: " + describeNodes(unavailable))
                + "); the copies that were stored must be removed", copies, null);
        for (IOException error : storeErrors) {
            incomplete.addSuppressed(error);
        }
        throw incomplete;
    }

    /**
     * The duplicate check before a store: every node must confirm that it does not hold the ID,
     * except nodes that cannot be reached at all, which are skipped. Node 0's check also validates
     * the ID. Returns the skipped nodes; the store does not use them.
     *
     * <p>Kept as one separate step on purpose: it is the rule replication changed (milestone M2),
     * and any later change to it belongs here only.
     *
     * @throws FileAlreadyExistsException if some node holds the ID
     * @throws IOException if some reachable node could not confirm that the ID is unused
     */
    private List<Integer> requireUnusedOnReachableNodes(String chunkId) throws IOException {
        NodeAnswers answers = askEveryNode(chunkId);
        if (!answers.holders.isEmpty()) {
            throw new FileAlreadyExistsException(null, null,
                    "Chunk \"" + chunkId + "\" already exists on node " + answers.holders.get(0));
        }
        List<Integer> refusing = new ArrayList<>();
        List<IOException> errors = new ArrayList<>();
        for (int i = 0; i < answers.uncheckedNodes.size(); i++) {
            if (!(answers.errors.get(i) instanceof NodeUnavailableException)) {
                refusing.add(answers.uncheckedNodes.get(i));
                errors.add(answers.errors.get(i));
            }
        }
        if (!refusing.isEmpty()) {
            throw withNodeErrors("Cannot store chunk \"" + chunkId + "\": " + describeNodes(refusing)
                    + " could not confirm that the ID is unused", errors);
        }
        List<Integer> unavailable = new ArrayList<>();
        for (int i = 0; i < answers.uncheckedNodes.size(); i++) {
            unavailable.add(answers.uncheckedNodes.get(i));
        }
        return unavailable;
    }

    /**
     * Copies the chunk from a node that holds it into a new file at {@code destination}, and
     * returns the number of bytes copied. The chunk stays on its node. The copy is not checked;
     * see {@link #retrieveVerifiedChunk} for that.
     *
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the chunk ID is invalid, or {@code destination} is inside
     *         any node's storage directory (checked before anything is created)
     * @throws ChunkNotFoundException if every node confirms it does not hold the chunk
     * @throws IOException if the chunk cannot be located (see {@link #locateChunk}); otherwise
     *         exactly what the holding node's {@link ChunkStore#retrieveChunk} throws, for example
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
     * Copies a good copy of the chunk into a new file at {@code destination}: the nodes are tried
     * in order (those in {@code tryLast} at the end, so a node that failed earlier in the same
     * download costs no more time unless it is needed), and the first copy whose SHA-256 is
     * {@code expectedSha256} is kept. A node with no copy, one that cannot be read, and a damaged
     * copy (deleted from {@code destination} again) are skipped. With {@code expectedSha256} null
     * (a file stored before chunk checksums existed) the first copy found is kept unchecked.
     *
     * @throws ChunkNotFoundException if every node confirms it has no copy
     * @throws CorruptCopiesException if no good copy exists and at least one copy was damaged
     * @throws FileAlreadyExistsException if {@code destination} already exists
     * @throws IOException if no good copy exists and some node could not be read
     */
    ReadResult retrieveVerifiedChunk(String chunkId, Path destination, String expectedSha256, Set<Integer> tryLast)
            throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(tryLast, "tryLast must not be null");
        StorageNode.validateChunkId(chunkId);
        requireOutsideEveryNode(destination);
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            if (!tryLast.contains(i)) {
                order.add(i);
            }
        }
        for (int i = 0; i < nodes.size(); i++) {
            if (tryLast.contains(i)) {
                order.add(i);
            }
        }
        List<String> problems = new ArrayList<>();
        List<Integer> failedNodes = new ArrayList<>();
        List<IOException> errors = new ArrayList<>();
        boolean damaged = false;
        ChecksumService checksums = new ChecksumService();
        for (int index : order) {
            long bytes;
            try {
                bytes = nodes.get(index).retrieveChunk(chunkId, destination);
            } catch (ChunkNotFoundException e) {
                continue; // no copy on this node; with replication that is normal for most nodes
            } catch (FileAlreadyExistsException e) {
                throw e; // the destination existed before this call
            } catch (IOException e) {
                errors.add(e);
                failedNodes.add(index);
                problems.add(describeNode(index) + " could not be read (" + e.getMessage() + ")");
                continue;
            }
            if (expectedSha256 != null) {
                String actual = checksums.calculateSha256(destination);
                if (!actual.equals(expectedSha256)) {
                    Files.delete(destination);
                    damaged = true;
                    problems.add(describeNode(index) + " has a damaged copy (SHA-256 " + actual + ", expected "
                            + expectedSha256 + ")");
                    continue;
                }
            }
            return new ReadResult(bytes, index, List.copyOf(problems), List.copyOf(failedNodes));
        }
        if (damaged) {
            CorruptCopiesException corrupt = new CorruptCopiesException("No good copy of chunk \"" + chunkId
                    + "\" exists: " + String.join("; ", problems));
            errors.forEach(corrupt::addSuppressed);
            throw corrupt;
        }
        if (errors.isEmpty()) {
            throw new ChunkNotFoundException(chunkId,
                    "Chunk \"" + chunkId + "\" is not stored on any of the " + nodes.size() + " nodes in this cluster");
        }
        throw withNodeErrors("Cannot read chunk \"" + chunkId + "\": no node that could be read has a copy, but "
                + describeNodes(failedNodes) + " could not be read", errors);
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
     * Returns the index of the first node that holds the chunk. Nodes that could not be checked
     * are ignored once a holder is found.
     *
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid
     * @throws ChunkNotFoundException if every node confirms it does not hold the chunk
     * @throws IOException if no node holds it but some node could not be checked
     */
    public int locateChunk(String chunkId) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        NodeAnswers answers = askEveryNode(chunkId);
        if (!answers.holders.isEmpty()) {
            return answers.holders.get(0);
        }
        if (!answers.errors.isEmpty()) {
            throw cannotTell(chunkId, answers);
        }
        throw new ChunkNotFoundException(chunkId,
                "Chunk \"" + chunkId + "\" is not stored on any of the " + nodes.size() + " nodes in this cluster");
    }

    /**
     * Deletes every copy of the chunk.
     *
     * <p>First every node is asked, and nothing is deleted while asking. The question is not
     * {@link #hasChunk}, which follows links, but a stricter one: a node holds the chunk only if
     * its storage directory is a plain directory and the chunk's entry is a regular file, both read
     * without following links. The copies are deleted only if every node answered, and then all
     * of them, however many there are. The round-robin position for stores is not changed.
     *
     * <p>Like the rest of this class, this assumes a single thread and nodes that nothing else
     * writes to: checking the nodes and then deleting is not atomic.
     *
     * @return {@code true} if at least one copy was deleted; {@code false} if every node confirms
     *         it does not hold the chunk, or the holding nodes found it already gone when deleting
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid (checked before any node's
     *         files are read)
     * @throws IOException if some node could not be checked safely (for example it is down, its
     *         storage is unavailable, or the entry under the chunk's ID is a link, junction or
     *         directory), in which case nothing is deleted; or if deleting a copy failed (with one holder, exactly what that node's
     *         {@link ChunkStore#deleteChunk} throws; other copies were still deleted)
     */
    public boolean deleteChunk(String chunkId) throws IOException {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        NodeAnswers answers = askEveryNodeForDeletion(chunkId);
        if (!answers.errors.isEmpty()) {
            throw withNodeErrors("Cannot delete chunk \"" + chunkId + "\": " + describeNodes(answers.uncheckedNodes)
                    + " could not be checked safely, so nothing was deleted", answers.errors);
        }
        if (answers.holders.size() == 1) {
            return nodes.get(answers.holders.get(0)).deleteChunk(chunkId);
        }
        boolean deleted = false;
        IOException failure = null;
        for (int holder : answers.holders) {
            try {
                deleted |= nodes.get(holder).deleteChunk(chunkId);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        return deleted;
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
     * ({@link ChunkStore#holdsRegularChunk}). An invalid ID is rejected by node 0 with
     * IllegalArgumentException before any file is read.
     */
    private NodeAnswers askEveryNodeForDeletion(String chunkId) {
        NodeAnswers answers = new NodeAnswers();
        for (int i = 0; i < nodes.size(); i++) {
            try {
                if (nodes.get(i).holdsRegularChunk(chunkId)) {
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
     * Remote nodes keep their folders in their own processes and are not checked.
     */
    private void requireOutsideEveryNode(Path destination) {
        Path normalizedDestination = destination.toAbsolutePath().normalize();
        for (int i = 0; i < nodes.size(); i++) {
            Optional<Path> localDir = nodes.get(i).localDirectory();
            if (localDir.isEmpty()) {
                continue;
            }
            Path nodeDir = localDir.get();
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
