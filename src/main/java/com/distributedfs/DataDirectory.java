package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * The server's data directory: checked, initialized if new, and locked so that only one server
 * process uses it at a time.
 *
 * <p><b>Layout.</b>
 * <pre>
 *   distributedfs-data.txt   marker: identifies the folder as a DistributedFS data directory
 *   server.lock              locked while a server uses the folder
 *   manifests\               file metadata (see ManifestStore)
 *   tmp\                     per-request workspaces
 *   nodes\node-0..2\         one folder per storage node
 * </pre>
 *
 * <p><b>Opening</b> ({@link #open}) follows a fixed order, so that a folder is never changed
 * before the server knows it is allowed to use it:
 * <ol>
 *   <li>Read-only pre-check. A path that exists but is not a folder is refused. A folder with
 *       contents other than {@code server.lock} and leftover marker temp files (see below) but
 *       no marker belongs to someone else and is refused before anything is created in it.</li>
 *   <li>Create the folder if it is missing.</li>
 *   <li>Open {@code server.lock} (created if missing, never written to) and lock it. If another
 *       server holds the lock, the folder is refused without being inspected further.</li>
 *   <li>Check the folder again, now holding the lock, because the pre-check may be stale.
 *       Ignoring {@code server.lock}: a marker must be exactly what this class writes; a folder
 *       with nothing else but leftover marker temp files is new, so the leftovers are deleted
 *       and the marker is written; other entries without a marker are refused.</li>
 *   <li>Create the subfolders that are missing, then check that each one is a plain folder,
 *       not a file, link or junction, so nothing is ever written through a link.</li>
 * </ol>
 * The marker is written before any subfolder, so a first start that is interrupted still leaves
 * a folder the server recognizes. Every failure after step 3 releases the lock.
 *
 * <p><b>The marker</b> is first written to a new temp file named
 * {@code distributedfs-data-<digits>.tmp}, forced to disk, and then renamed to its real name. The
 * rename never replaces an existing file, so the marker is either absent or complete. A process
 * killed during this leaves only the temp file (and {@code server.lock}); the next start sees a
 * new folder, deletes the leftover and writes the marker again. An entry counts as a leftover
 * only if it has exactly that name form, is a regular file (not followed if it is a link), is no
 * larger than the marker, and no marker exists. Anything else under such a name makes the folder
 * unrecognized and leaves it untouched; in a folder that already has a marker, such entries are
 * left alone.
 *
 * <p><b>The lock</b> is an operating-system file lock on {@code server.lock}, held until
 * {@link #close()}. It belongs to the file itself, not to the path text, and the operating system
 * releases it when the process ends, even if the process is killed.
 *
 * <p><b>Limits.</b> A marker that is damaged later (edited by hand, or by disk corruption) is
 * refused; the server does not start and does not touch the stored data, and the message explains
 * how to restore the marker. It never suggests deleting the folder, which may hold stored files. A
 * {@code server.lock} created during a refused start is left in place, because another process
 * may have it open. Subfolders that are reparse points of any kind (for example OneDrive
 * placeholders) are refused as unsafe.
 */
final class DataDirectory implements AutoCloseable {

    static final String MARKER_NAME = "distributedfs-data.txt";
    static final String LOCK_NAME = "server.lock";
    static final int NODE_COUNT = 3;

    /** The marker's exact content. Anything else, even one extra space, is an invalid marker. */
    static final String MARKER_CONTENT = "DistributedFS data directory. Do not edit, move or delete this file.\n"
            + "format=1\n"
            + "nodes=" + NODE_COUNT + "\n";

    private static final byte[] MARKER_BYTES = MARKER_CONTENT.getBytes(StandardCharsets.US_ASCII);

    /** The marker is written to a temp file with this prefix and suffix, then renamed into place. */
    private static final String MARKER_TEMP_PREFIX = "distributedfs-data-";
    private static final String MARKER_TEMP_SUFFIX = ".tmp";

    /**
     * Exactly the names {@code Files.createTempFile(root, MARKER_TEMP_PREFIX, MARKER_TEMP_SUFFIX)}
     * produces: the JDK inserts {@code Long.toUnsignedString} of a random number, so 1 to 20 digits.
     */
    private static final Pattern MARKER_TEMP_NAME = Pattern.compile("distributedfs-data-[0-9]{1,20}\\.tmp");

    private final Path root;
    private final Path manifestsDir;
    private final Path tmpDir;
    private final List<Path> nodeDirs;
    private final FileChannel lockChannel;
    private final boolean newlyInitialized;

    private DataDirectory(Path root, FileChannel lockChannel, boolean newlyInitialized) {
        this.root = root;
        this.manifestsDir = root.resolve("manifests");
        this.tmpDir = root.resolve("tmp");
        this.nodeDirs = nodeDirsOf(root);
        this.lockChannel = lockChannel;
        this.newlyInitialized = newlyInitialized;
    }

    /**
     * Checks, initializes if new, and locks the data directory, in the order described above.
     *
     * @throws NullPointerException if {@code dataDir} is null
     * @throws DataDirectoryException if the server must not use this folder; {@code reason()} says why
     * @throws IOException if a file system operation fails; the lock is released
     */
    static DataDirectory open(Path dataDir) throws IOException {
        Objects.requireNonNull(dataDir, "dataDir must not be null");
        Path root = dataDir.toAbsolutePath().normalize();

        // 1. Read-only pre-check: nothing is created in a folder that belongs to someone else.
        if (Files.exists(root)) {
            if (!Files.isDirectory(root)) {
                throw new DataDirectoryException(DataDirectoryException.Reason.NOT_A_DIRECTORY,
                        "The data directory path exists but is not a folder: " + root);
            }
            Set<String> names = listNames(root);
            names.remove(LOCK_NAME);
            names.removeAll(leftoverMarkerTemps(root, names));
            if (!names.isEmpty() && !names.contains(MARKER_NAME)) {
                throw unrecognized(root);
            }
        }

        // 2. Create the folder (and missing parents) if needed. Does nothing if it exists.
        Files.createDirectories(root);

        // 3. Take the lock. From here on, every failure closes the channel, which releases it.
        FileChannel lockChannel = openLockFile(root.resolve(LOCK_NAME));
        try {
            FileLock lock;
            try {
                lock = lockChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                lock = null; // this process already holds the lock, through another DataDirectory
            }
            if (lock == null) {
                throw new DataDirectoryException(DataDirectoryException.Reason.IN_USE,
                        "The data directory is already in use by another DistributedFS server: " + root);
            }

            // 4. The authoritative check, now that no other server can change the folder.
            boolean newlyInitialized = claim(root);

            // 5. Subfolders, each created if missing and then checked to be a plain folder.
            for (Path folder : requiredFolders(root)) {
                createPlainFolder(folder);
            }
            return new DataDirectory(root, lockChannel, newlyInitialized);
        } catch (Throwable e) {
            closeAfterFailure(lockChannel, e);
            throw e;
        }
    }

    /** The data directory, as an absolute path without "." or ".." parts. */
    Path root() {
        return root;
    }

    Path manifestsDir() {
        return manifestsDir;
    }

    Path tmpDir() {
        return tmpDir;
    }

    /** The storage folders of nodes 0 to NODE_COUNT - 1, in order. */
    List<Path> nodeDirs() {
        return nodeDirs;
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

    /**
     * True only for a real folder: read without following links, it is a directory and neither a
     * symbolic link nor "other" (Java reports a Windows junction as a directory that is also "other").
     * Returns false if nothing has this name.
     */
    static boolean isPlainDirectory(Path path) throws IOException {
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return false;
        }
        return attributes.isDirectory() && !attributes.isOther() && !attributes.isSymbolicLink();
    }

    // ---------------------------------------------------------------------
    // Steps
    // ---------------------------------------------------------------------

    /**
     * Opens {@code server.lock} for writing, creating it if missing. It is never truncated or
     * written. An existing entry must be a regular file, and the open does not follow links.
     */
    static FileChannel openLockFile(Path lockFile) throws IOException {
        try {
            BasicFileAttributes attributes =
                    Files.readAttributes(lockFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                throw new DataDirectoryException(DataDirectoryException.Reason.UNSAFE_LAYOUT,
                        "The lock file is not a regular file: " + lockFile);
            }
        } catch (NoSuchFileException e) {
            // Missing: created by the open below.
        }
        return FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * Step 4. Returns true if the folder was new and this call wrote its marker, false if it
     * already had a valid marker.
     */
    private static boolean claim(Path root) throws IOException {
        Set<String> names = listNames(root);
        names.remove(LOCK_NAME);
        Path marker = root.resolve(MARKER_NAME);
        if (names.contains(MARKER_NAME)) {
            requireValidMarker(marker);
            return false;
        }
        Set<String> leftovers = leftoverMarkerTemps(root, names);
        names.removeAll(leftovers);
        if (!names.isEmpty()) {
            throw unrecognized(root);
        }
        // A new folder. Leftover marker temp files come from an earlier first start that was
        // interrupted before its marker was renamed into place; each was just checked to be a
        // regular file no larger than the marker.
        for (String leftover : leftovers) {
            Files.delete(root.resolve(leftover));
        }
        writeMarker(root);
        return true;
    }

    /**
     * The names, among {@code names}, of leftover marker temp files: exactly the temp name form,
     * and, read without following links, a regular file no larger than the marker. Anything else
     * under such a name is not a leftover, so it counts as someone else's content.
     */
    private static Set<String> leftoverMarkerTemps(Path root, Set<String> names) throws IOException {
        Set<String> leftovers = new TreeSet<>();
        for (String name : names) {
            if (MARKER_TEMP_NAME.matcher(name).matches() && isSmallRegularFile(root.resolve(name), MARKER_BYTES.length)) {
                leftovers.add(name);
            }
        }
        return leftovers;
    }

    static boolean isSmallRegularFile(Path path, long maxBytes) throws IOException {
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return false;
        }
        return attributes.isRegularFile() && attributes.size() <= maxBytes;
    }

    private static void requireValidMarker(Path marker) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(marker, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw invalidMarker(marker, "it is not a regular file");
        }
        if (attributes.size() > MARKER_BYTES.length) {
            throw invalidMarker(marker, "it is larger than the marker the server writes");
        }
        byte[] content;
        try (InputStream in = Files.newInputStream(marker, LinkOption.NOFOLLOW_LINKS)) {
            content = in.readNBytes(MARKER_BYTES.length + 1);
        }
        if (!Arrays.equals(content, MARKER_BYTES)) {
            throw invalidMarker(marker, "its content is not exactly what the server writes");
        }
    }

    /**
     * Writes the marker: first to a new temp file in the folder, forced to disk, then renamed to
     * its real name. The rename has no options, so it never replaces an existing file (on Windows,
     * ATOMIC_MOVE would), and the marker is either absent or complete. If anything fails, the
     * temp file is deleted; if the process is killed instead, the next start deletes it (see claim).
     */
    private static void writeMarker(Path root) throws IOException {
        Path marker = root.resolve(MARKER_NAME);
        // createTempFile picks a new name, so this call owns the temp file and may delete it.
        Path tempFile = Files.createTempFile(root, MARKER_TEMP_PREFIX, MARKER_TEMP_SUFFIX);
        try {
            try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
                ByteBuffer content = ByteBuffer.wrap(MARKER_BYTES);
                while (content.hasRemaining()) {
                    channel.write(content);
                }
                channel.force(true); // the bytes are on disk before the marker's name can appear
            }
            Files.move(tempFile, marker);
        } catch (FileAlreadyExistsException e) {
            // A marker appeared despite the lock, so another program is changing the folder. Never replace it.
            DataDirectoryException refusal = new DataDirectoryException(DataDirectoryException.Reason.UNRECOGNIZED,
                    "A marker appeared while this server was creating one, so another program is changing the folder;"
                            + " the existing marker was not replaced: " + root);
            refusal.initCause(e);
            deleteAfterFailure(tempFile, refusal);
            throw refusal;
        } catch (IOException | RuntimeException e) {
            deleteAfterFailure(tempFile, e);
            throw e;
        }
    }

    /**
     * Creates the folder if it is missing, then requires it to be a plain folder. An existing
     * file, link or junction under this name is refused and left as it is.
     */
    static void createPlainFolder(Path folder) throws IOException {
        try {
            Files.createDirectory(folder);
        } catch (FileAlreadyExistsException e) {
            // Something already has this name; the check below decides whether it is acceptable.
        }
        if (!isPlainDirectory(folder)) {
            throw new DataDirectoryException(DataDirectoryException.Reason.UNSAFE_LAYOUT,
                    "A required folder is a file, link or junction instead of a plain folder: " + folder);
        }
    }

    /** In creation order: parents before children, so nothing is created inside an unchecked folder. */
    private static List<Path> requiredFolders(Path root) {
        List<Path> folders = new ArrayList<>();
        folders.add(root.resolve("manifests"));
        folders.add(root.resolve("tmp"));
        folders.add(root.resolve("nodes"));
        folders.addAll(nodeDirsOf(root));
        return folders;
    }

    private static List<Path> nodeDirsOf(Path root) {
        List<Path> dirs = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            dirs.add(root.resolve("nodes").resolve("node-" + i));
        }
        return List.copyOf(dirs);
    }

    static Set<String> listNames(Path folder) throws IOException {
        Set<String> names = new TreeSet<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(folder)) {
            for (Path entry : entries) {
                names.add(entry.getFileName().toString());
            }
        }
        return names;
    }

    private static DataDirectoryException unrecognized(Path root) {
        return new DataDirectoryException(DataDirectoryException.Reason.UNRECOGNIZED,
                "The folder is not empty and is not a DistributedFS data directory (it has no "
                        + MARKER_NAME + "), so it was left untouched: " + root);
    }

    private static DataDirectoryException invalidMarker(Path marker, String problem) {
        return new DataDirectoryException(DataDirectoryException.Reason.INVALID_MARKER,
                "The data directory marker is invalid (" + problem + "): " + marker + "\n"
                        + "The server did not start and did not touch any stored data. Do not delete the folder:"
                        + " it may hold stored files.\n"
                        + "If it is a DistributedFS data directory, replace the marker with a file containing exactly"
                        + " these three lines:\n"
                        + MARKER_CONTENT);
    }

    /** Best-effort removal of a temp file this call created; a failure is attached to the original error. */
    static void deleteAfterFailure(Path file, Throwable originalError) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException cleanupError) {
            originalError.addSuppressed(cleanupError);
        }
    }

    static void closeAfterFailure(FileChannel channel, Throwable originalError) {
        try {
            channel.close();
        } catch (IOException closeError) {
            originalError.addSuppressed(closeError);
        }
    }
}
