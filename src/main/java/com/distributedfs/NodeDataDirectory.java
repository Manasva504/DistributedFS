package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A storage node process's own folder: checked, initialized if new, and locked so that only one
 * node process uses it at a time. It follows the same steps as the server's {@link DataDirectory}.
 *
 * <p><b>Layout.</b>
 * <pre>
 *   distributedfs-node.txt   marker: identifies the folder as a storage node folder, and holds the node's ID
 *   node.lock                locked while a node process uses the folder
 *   chunks\                  the chunks, kept by a {@link StorageNode} (flat, one file per chunk)
 * </pre>
 *
 * <p><b>Opening</b> ({@link #open}):
 * <ol>
 *   <li>Read-only pre-check. A path that is not a folder is refused. A folder with contents other
 *       than {@code node.lock} and leftover marker temp files, but no marker, belongs to someone
 *       else (for example the server's own data directory) and is refused untouched.</li>
 *   <li>Create the folder if it is missing.</li>
 *   <li>Lock {@code node.lock}; if another process (or another node in this process) holds it, the
 *       folder is refused.</li>
 *   <li>Check again, holding the lock. A valid marker gives the node its ID. A folder with nothing
 *       but leftover marker temp files is new: they are deleted and a marker with a new random ID
 *       is written (temp file, forced to disk, renamed without replacing anything).</li>
 *   <li>Create {@code chunks\} if missing and require it to be a plain folder, not a link or junction.</li>
 * </ol>
 * Every failure after step 3 releases the lock.
 *
 * <p><b>The node ID</b> is 32 random lowercase hexadecimal characters, chosen once when the folder
 * is created and never changed. Every response of the node carries it, so a client can tell
 * whether it is still talking to the same node (and the same folder) as before.
 */
final class NodeDataDirectory implements AutoCloseable {

    static final String MARKER_NAME = "distributedfs-node.txt";
    static final String LOCK_NAME = "node.lock";
    static final String CHUNKS_FOLDER = "chunks";

    private static final String MARKER_FIRST_LINE = "DistributedFS storage node folder. Do not edit, move or delete this file.";

    /** The marker's exact form; the ID is the only part that differs between nodes. */
    private static final Pattern MARKER_CONTENT = Pattern.compile(
            Pattern.quote(MARKER_FIRST_LINE + "\nformat=1\nnode-id=") + "([0-9a-f]{32})\n");

    /** Every marker has the same length, because every node ID has 32 characters. */
    private static final int MARKER_LENGTH = markerText("0".repeat(32)).length();

    private static final String MARKER_TEMP_PREFIX = "distributedfs-node-";
    private static final String MARKER_TEMP_SUFFIX = ".tmp";
    private static final Pattern MARKER_TEMP_NAME = Pattern.compile("distributedfs-node-[0-9]{1,20}\\.tmp");

    private final Path root;
    private final Path chunksDir;
    private final String nodeId;
    private final FileChannel lockChannel;
    private final boolean newlyInitialized;

    private NodeDataDirectory(Path root, String nodeId, FileChannel lockChannel, boolean newlyInitialized) {
        this.root = root;
        this.chunksDir = root.resolve(CHUNKS_FOLDER);
        this.nodeId = nodeId;
        this.lockChannel = lockChannel;
        this.newlyInitialized = newlyInitialized;
    }

    /**
     * Checks, initializes if new, and locks a node folder, in the order described above.
     *
     * @throws DataDirectoryException if a node must not use this folder; {@code reason()} says why
     * @throws IOException if a file system operation fails; the lock is released
     */
    static NodeDataDirectory open(Path folder) throws IOException {
        Objects.requireNonNull(folder, "folder must not be null");
        Path root = folder.toAbsolutePath().normalize();

        // 1. Read-only pre-check: nothing is created in a folder that belongs to someone else.
        if (Files.exists(root)) {
            if (!Files.isDirectory(root)) {
                throw new DataDirectoryException(DataDirectoryException.Reason.NOT_A_DIRECTORY,
                        "The node folder path exists but is not a folder: " + root);
            }
            Set<String> names = DataDirectory.listNames(root);
            names.remove(LOCK_NAME);
            names.removeAll(leftoverMarkerTemps(root, names));
            if (!names.isEmpty() && !names.contains(MARKER_NAME)) {
                throw unrecognized(root);
            }
        }

        // 2. Create the folder (and missing parents) if needed.
        Files.createDirectories(root);

        // 3. Take the lock. From here on, every failure closes the channel, which releases it.
        FileChannel lockChannel = DataDirectory.openLockFile(root.resolve(LOCK_NAME));
        try {
            FileLock lock;
            try {
                lock = lockChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                lock = null; // this process already holds the lock, through another NodeDataDirectory
            }
            if (lock == null) {
                throw new DataDirectoryException(DataDirectoryException.Reason.IN_USE,
                        "The node folder is already in use by another storage node process: " + root);
            }

            // 4. The authoritative check, now that no other node process can change the folder.
            Set<String> names = DataDirectory.listNames(root);
            names.remove(LOCK_NAME);
            String nodeId;
            boolean newlyInitialized;
            if (names.contains(MARKER_NAME)) {
                nodeId = readMarker(root.resolve(MARKER_NAME));
                newlyInitialized = false;
            } else {
                Set<String> leftovers = leftoverMarkerTemps(root, names);
                names.removeAll(leftovers);
                if (!names.isEmpty()) {
                    throw unrecognized(root);
                }
                for (String leftover : leftovers) {
                    Files.delete(root.resolve(leftover));
                }
                nodeId = FileIds.newFileId(); // 32 random lowercase hex characters, the node ID's form
                writeMarker(root, nodeId);
                newlyInitialized = true;
            }

            // 5. The chunk folder, created if missing and checked to be a plain folder.
            DataDirectory.createPlainFolder(root.resolve(CHUNKS_FOLDER));
            return new NodeDataDirectory(root, nodeId, lockChannel, newlyInitialized);
        } catch (Throwable e) {
            DataDirectory.closeAfterFailure(lockChannel, e);
            throw e;
        }
    }

    /** The node folder, as an absolute path without "." or ".." parts. */
    Path root() {
        return root;
    }

    /** The folder the node's StorageNode keeps its chunks in. */
    Path chunksDir() {
        return chunksDir;
    }

    /** The node's ID, from its marker. */
    String nodeId() {
        return nodeId;
    }

    /** True if this open created the marker, that is, the folder was new. */
    boolean newlyInitialized() {
        return newlyInitialized;
    }

    /** Releases the lock. Calling it again does nothing. */
    @Override
    public void close() throws IOException {
        lockChannel.close();
    }

    static String markerText(String nodeId) {
        return MARKER_FIRST_LINE + "\nformat=1\nnode-id=" + nodeId + "\n";
    }

    /** Reads and checks the marker; returns the node ID it holds. */
    private static String readMarker(Path marker) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(marker, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw invalidMarker(marker, "it is not a regular file");
        }
        if (attributes.size() != MARKER_LENGTH) {
            throw invalidMarker(marker, "it does not have the marker's length");
        }
        byte[] content;
        try (InputStream in = Files.newInputStream(marker, LinkOption.NOFOLLOW_LINKS)) {
            content = in.readNBytes(MARKER_LENGTH + 1);
        }
        Matcher matcher = MARKER_CONTENT.matcher(new String(content, StandardCharsets.US_ASCII));
        if (content.length != MARKER_LENGTH || !matcher.matches()) {
            throw invalidMarker(marker, "its content is not exactly what a storage node writes");
        }
        return matcher.group(1);
    }

    /** As DataDirectory writes its marker: temp file, forced to disk, renamed without replacing anything. */
    private static void writeMarker(Path root, String nodeId) throws IOException {
        Path marker = root.resolve(MARKER_NAME);
        Path tempFile = Files.createTempFile(root, MARKER_TEMP_PREFIX, MARKER_TEMP_SUFFIX);
        try {
            try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
                ByteBuffer content = ByteBuffer.wrap(markerText(nodeId).getBytes(StandardCharsets.US_ASCII));
                while (content.hasRemaining()) {
                    channel.write(content);
                }
                channel.force(true);
            }
            Files.move(tempFile, marker);
        } catch (FileAlreadyExistsException e) {
            DataDirectoryException refusal = new DataDirectoryException(DataDirectoryException.Reason.UNRECOGNIZED,
                    "A marker appeared while this node was creating one, so another program is changing the folder;"
                            + " the existing marker was not replaced: " + root);
            refusal.initCause(e);
            DataDirectory.deleteAfterFailure(tempFile, refusal);
            throw refusal;
        } catch (IOException | RuntimeException e) {
            DataDirectory.deleteAfterFailure(tempFile, e);
            throw e;
        }
    }

    private static Set<String> leftoverMarkerTemps(Path root, Set<String> names) throws IOException {
        Set<String> leftovers = new TreeSet<>();
        for (String name : names) {
            if (MARKER_TEMP_NAME.matcher(name).matches()
                    && DataDirectory.isSmallRegularFile(root.resolve(name), MARKER_LENGTH)) {
                leftovers.add(name);
            }
        }
        return leftovers;
    }

    private static DataDirectoryException unrecognized(Path root) {
        return new DataDirectoryException(DataDirectoryException.Reason.UNRECOGNIZED,
                "The folder is not empty and is not a DistributedFS storage node folder (it has no "
                        + MARKER_NAME + "), so it was left untouched: " + root);
    }

    private static DataDirectoryException invalidMarker(Path marker, String problem) {
        return new DataDirectoryException(DataDirectoryException.Reason.INVALID_MARKER,
                "The storage node marker is invalid (" + problem + "): " + marker + "\n"
                        + "The node did not start and did not touch any stored chunk. Do not delete the folder:"
                        + " it may hold stored chunks.");
    }
}
