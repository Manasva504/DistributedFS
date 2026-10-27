package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * The membership file {@code nodes.txt} in the server's data directory: which remote storage nodes
 * hold this data directory's chunks.
 *
 * <p><b>Why it exists.</b> The cluster finds chunks by asking every node. If the server were
 * started with a node missing, or with a different node, that node's chunks would look "absent on
 * every node": downloads would report them missing, and startup recovery would judge interrupted
 * uploads from an incomplete view. So the first start in remote mode records the node IDs, and
 * every later start must present exactly that set.
 *
 * <p><b>What it records</b> is the set of node IDs only, sorted, never their addresses: a node may
 * come back on another port, and a later milestone can let the server run while a recorded member
 * is temporarily down without forgetting that the node belongs to the cluster.
 * <pre>
 *   DistributedFS cluster membership. Do not edit, move or delete this file.
 *   format=1
 *   node=&lt;32 lowercase hex&gt;     (one line per node, sorted)
 * </pre>
 * It is written once, like the data directory's marker: to a new temp file, forced to disk, then
 * renamed without replacing anything. It is read strictly; anything else is refused.
 *
 * <p><b>Modes.</b> A data directory either keeps its chunks in its own {@code nodes\node-*}
 * folders (local mode, the default, no {@code nodes.txt}) or on remote nodes (remote mode, with
 * {@code nodes.txt}). Switching is refused both ways: remote mode on a folder whose local node
 * folders hold anything, and local mode on a folder that has a membership file.
 */
final class ClusterMembership {

    static final String FILE_NAME = "nodes.txt";
    static final int MAX_NODES = 16;

    private static final String FIRST_LINE = "DistributedFS cluster membership. Do not edit, move or delete this file.";
    private static final String TEMP_PREFIX = "nodes-";
    private static final String TEMP_SUFFIX = ".tmp";

    /** The longest valid file: the header and {@link #MAX_NODES} node lines. */
    private static final int MAX_BYTES = text(Set.of()).length() + MAX_NODES * ("node=".length() + 33);

    private ClusterMembership() {
    }

    /**
     * Local mode: refuses a data directory that has a membership file, because its chunks are on
     * remote nodes and the local node folders would look empty.
     */
    static void requireLocalMode(Path dataRoot) throws IOException {
        if (Files.exists(dataRoot.resolve(FILE_NAME), LinkOption.NOFOLLOW_LINKS)) {
            throw new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                    "This data directory keeps its chunks on remote storage nodes (see " + dataRoot.resolve(FILE_NAME)
                            + "); start the server with the same --node options as before");
        }
    }

    /**
     * Remote mode: on the first start, requires the local node folders to be empty and records
     * {@code nodeIds}; on every later start, requires exactly the recorded set.
     *
     * @return true if this call wrote the membership file
     * @throws DataDirectoryException WRONG_NODES if the nodes do not match, INVALID_MARKER if the
     *         file is damaged
     */
    static boolean requireRemoteMembers(Path dataRoot, List<Path> localNodeDirs, List<String> nodeIds)
            throws IOException {
        Set<String> given = new TreeSet<>(nodeIds);
        if (given.size() != nodeIds.size() || given.isEmpty() || given.size() > MAX_NODES) {
            throw new IllegalArgumentException("1 to " + MAX_NODES + " distinct node IDs are needed");
        }
        Optional<Set<String>> recorded = read(dataRoot.resolve(FILE_NAME));
        if (recorded.isPresent()) {
            if (!recorded.get().equals(given)) {
                Set<String> missing = new TreeSet<>(recorded.get());
                missing.removeAll(given);
                Set<String> unexpected = new TreeSet<>(given);
                unexpected.removeAll(recorded.get());
                throw new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                        "The storage nodes given are not the ones this data directory uses. Missing: "
                                + (missing.isEmpty() ? "none" : String.join(", ", missing)) + "; not recorded: "
                                + (unexpected.isEmpty() ? "none" : String.join(", ", unexpected))
                                + ". Start the server with --node options for exactly the recorded nodes (see "
                                + dataRoot.resolve(FILE_NAME) + ").");
            }
            return false;
        }
        for (Path nodeDir : localNodeDirs) {
            if (!DataDirectory.listNames(nodeDir).isEmpty()) {
                throw new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                        "This data directory keeps chunks in its local node folders (" + nodeDir
                                + " is not empty), so it cannot switch to remote storage nodes");
            }
        }
        write(dataRoot, given);
        return true;
    }

    /**
     * A degraded start: not every node answered. There must be a membership file, the server must
     * be given one {@code --node} per recorded node, and every node that answered must be a
     * recorded one. Returns the recorded IDs that no node answered with.
     *
     * @throws DataDirectoryException WRONG_NODES if not, INVALID_MARKER if the file is damaged
     */
    static Set<String> requireRecordedMembers(Path dataRoot, int nodeCount, List<String> answeredIds) throws IOException {
        Optional<Set<String>> recorded = read(dataRoot.resolve(FILE_NAME));
        if (recorded.isEmpty()) {
            throw new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                    "Every storage node must answer the first time a data directory is used with --node options");
        }
        if (recorded.get().size() != nodeCount) {
            throw new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                    "This data directory uses " + recorded.get().size() + " storage nodes (see "
                            + dataRoot.resolve(FILE_NAME) + "), but " + nodeCount + " --node options were given");
        }
        Set<String> unexpected = new TreeSet<>(answeredIds);
        unexpected.removeAll(recorded.get());
        if (!unexpected.isEmpty()) {
            throw new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                    "The storage nodes given are not the ones this data directory uses. Not recorded: "
                            + String.join(", ", unexpected) + " (see " + dataRoot.resolve(FILE_NAME) + ").");
        }
        Set<String> unanswered = new TreeSet<>(recorded.get());
        unanswered.removeAll(answeredIds);
        return unanswered;
    }

    /** The recorded node IDs, or empty if there is no membership file. */
    static Optional<Set<String>> read(Path file) throws IOException {
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
        if (!attributes.isRegularFile() || attributes.size() > MAX_BYTES) {
            throw invalid(file, "it is not a regular file of the expected size");
        }
        String content;
        try (InputStream in = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            content = new String(in.readNBytes(MAX_BYTES + 1), StandardCharsets.US_ASCII);
        }
        String[] lines = content.split("\n", -1);
        Set<String> ids = new TreeSet<>();
        List<String> order = new ArrayList<>();
        boolean valid = content.endsWith("\n") && lines.length >= 4 && lines[0].equals(FIRST_LINE)
                && lines[1].equals("format=1");
        for (int i = 2; valid && i < lines.length - 1; i++) {
            String id = lines[i].startsWith("node=") ? lines[i].substring(5) : "";
            valid = NodeProtocol.NODE_ID.matcher(id).matches() && ids.add(id);
            order.add(id);
        }
        if (!valid || ids.size() > MAX_NODES || !order.equals(new ArrayList<>(ids))) {
            throw invalid(file, "its content is not exactly what the server writes");
        }
        return Optional.of(ids);
    }

    static String text(Set<String> nodeIds) {
        StringBuilder text = new StringBuilder(FIRST_LINE).append("\nformat=1\n");
        for (String id : new TreeSet<>(nodeIds)) {
            text.append("node=").append(id).append('\n');
        }
        return text.toString();
    }

    private static void write(Path dataRoot, Set<String> nodeIds) throws IOException {
        Path file = dataRoot.resolve(FILE_NAME);
        Path tempFile = Files.createTempFile(dataRoot, TEMP_PREFIX, TEMP_SUFFIX);
        try {
            try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
                ByteBuffer content = ByteBuffer.wrap(text(nodeIds).getBytes(StandardCharsets.US_ASCII));
                while (content.hasRemaining()) {
                    channel.write(content);
                }
                channel.force(true);
            }
            Files.move(tempFile, file); // never replaces an existing file
        } catch (FileAlreadyExistsException e) {
            DataDirectoryException refusal = new DataDirectoryException(DataDirectoryException.Reason.WRONG_NODES,
                    "A membership file appeared while the server was writing one; it was not replaced: " + file);
            refusal.initCause(e);
            DataDirectory.deleteAfterFailure(tempFile, refusal);
            throw refusal;
        } catch (IOException | RuntimeException e) {
            DataDirectory.deleteAfterFailure(tempFile, e);
            throw e;
        }
    }

    private static DataDirectoryException invalid(Path file, String problem) {
        return new DataDirectoryException(DataDirectoryException.Reason.INVALID_MARKER,
                "The membership file is invalid (" + problem + "): " + file + "\nThe server did not start and did"
                        + " not touch any stored data. Do not delete the file or the folder: they record which storage"
                        + " nodes hold this data directory's chunks.");
    }
}
