package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * A simulated storage node. It keeps chunks as files in its own local directory and hands out
 * copies of them on request. Several StorageNode objects with different directories simulate
 * several nodes.
 *
 * <p><b>Storage layout.</b> The directory is flat, and each chunk is one file named exactly after
 * its chunk ID: chunk {@code chunk_0} is stored as {@code <storageDir>/chunk_0}. While a chunk is
 * being stored, its bytes first go into a temporary file named {@code <chunkId>-<digits>.tmp}.
 *
 * <p><b>Chunk IDs</b> must be 1 to 128 characters of lowercase {@code a-z}, digits {@code 0-9},
 * {@code _} or {@code -}, and must not be a Windows device name such as {@code con} or {@code nul}.
 * With these rules an ID cannot point outside the storage directory, two different IDs can never
 * name the same file (Windows ignores letter case in file names), and a temp file (whose name
 * contains a dot) can never be mistaken for a chunk.
 *
 * <p><b>No overwrites, no partial chunks.</b> A chunk is written to a temp file and then renamed
 * to its ID. The rename refuses to replace an existing file, so an existing chunk is never
 * overwritten, and a chunk's name only appears once all of its bytes have been written. If a
 * store fails, only the temp file is deleted.
 *
 * <p><b>Answers versus errors.</b> {@link #hasChunk(String)} answers {@code true} or
 * {@code false}, and {@link #retrieveChunk(String, Path)} throws {@link ChunkNotFoundException},
 * only when the node can actually tell whether it holds the chunk. When it cannot tell (for
 * example, its storage directory was deleted after the node started), they throw a plain
 * {@link IOException}. A broken node is never reported as one that simply lacks the chunk.
 *
 * <p><b>Deleting and retrieving.</b> {@link #deleteChunk(String)} and
 * {@link #retrieveChunk(String, Path)} are stricter than the other methods: they read attributes
 * without following links, and delete or read a chunk only if the storage directory is a plain
 * directory (not a symbolic link or junction) and the chunk's entry is a regular file. If either
 * cannot be established, nothing is deleted or read and an IOException is thrown. Temp files and
 * other chunks are never touched. (The check and the later open are two steps; as below, the
 * node assumes no other program changes its directory in between.)
 *
 * <p><b>Assumptions and limits.</b>
 * <ul>
 *   <li>Single-threaded use: only one thread, through one StorageNode object, should write to a
 *       storage directory at a time. Simultaneous writers are not supported and not tested.</li>
 *   <li>The node assumes it owns its directory: other programs are not expected to change
 *       files there.</li>
 *   <li>Data is not forced to disk (no fsync), so a power cut right after a store can still
 *       lose the chunk.</li>
 *   <li>If the process is killed during a store, an orphaned {@code .tmp} file may remain.
 *       It is never treated as a chunk. The server's startup recovery removes such files
 *       ({@link #sweepStaleTempFiles}); a node used on its own does not.</li>
 * </ul>
 */
public class StorageNode {

    /** Size of the reusable copy buffer: 64 KiB, as in FileReconstructor and ChecksumService. */
    private static final int BUFFER_SIZE = 64 * 1024;

    private static final int MAX_CHUNK_ID_LENGTH = 128;

    /** A name made only of chunk-ID characters: a chunk, never a temp file (which has a dot). */
    private static final Pattern CHUNK_NAME = Pattern.compile("[a-z0-9_-]{1," + MAX_CHUNK_ID_LENGTH + "}");

    /**
     * Exactly the names {@code Files.createTempFile(storageDir, chunkId + "-", ".tmp")} produces:
     * the JDK inserts {@code Long.toUnsignedString} of a random number, so 1 to 20 digits.
     */
    private static final Pattern TEMP_NAME =
            Pattern.compile("[a-z0-9_-]{1," + MAX_CHUNK_ID_LENGTH + "}-[0-9]{1,20}\\.tmp");

    /**
     * Names Windows reserves for devices, in any letter case and even inside a folder. A "file"
     * with one of these names may not be an ordinary file at all; writing to {@code nul}, for
     * example, can silently discard the data. They are rejected on every operating system, so
     * an ID that is valid on one node is valid on all nodes.
     */
    private static final Set<String> WINDOWS_DEVICE_NAMES = Set.of(
            "con", "prn", "aux", "nul",
            "com0", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
            "lpt0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9");

    /** The node's directory, as an absolute path without "." or ".." parts. */
    private final Path storageDir;

    /**
     * Starts a node that keeps its chunks in {@code storageDir}.
     *
     * <p>Like a real node starting up, it prepares its storage straight away: the directory and
     * any missing parent directories are created now, so a node that cannot use its storage
     * fails here rather than on its first write.
     *
     * @throws NullPointerException if {@code storageDir} is null
     * @throws IllegalArgumentException if {@code storageDir} exists but is not a directory
     * @throws IOException if the directory cannot be created
     */
    public StorageNode(Path storageDir) throws IOException {
        Objects.requireNonNull(storageDir, "storageDir must not be null");
        if (Files.exists(storageDir) && !Files.isDirectory(storageDir)) {
            throw new IllegalArgumentException("Storage path exists but is not a directory: " + storageDir);
        }
        this.storageDir = storageDir.toAbsolutePath().normalize();
        Files.createDirectories(this.storageDir);
    }

    /**
     * Copies {@code sourceFile} into this node as the chunk {@code chunkId}.
     * The source file is only read, never changed.
     *
     * @return the number of bytes stored
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the chunk ID is invalid, or the source file is missing,
     *         a directory, not a regular file, or not readable (all checked before anything is written)
     * @throws FileAlreadyExistsException if this node already holds a chunk with this ID
     *         (the existing chunk is left unchanged)
     * @throws IOException if the node's storage is unavailable or copying fails; the temp file
     *         is deleted first, so no partial chunk is ever left under {@code chunkId}
     */
    public long storeChunk(String chunkId, Path sourceFile) throws IOException {
        Path chunkFile = chunkFileFor(chunkId);
        validateSourceFile(sourceFile);

        // Fail fast, before copying anything, if this ID is already taken.
        if (chunkExists(chunkFile)) {
            throw new FileAlreadyExistsException(chunkFile.toString(), null,
                    "chunk \"" + chunkId + "\" already exists on this node");
        }

        // Write into a brand-new temp file first. createTempFile picks a name nobody else is
        // using, so this call owns the file and may safely write to it and delete it.
        Path tempFile = Files.createTempFile(storageDir, chunkId + "-", ".tmp");
        try {
            long bytesStored;
            try (InputStream in = openForReading(sourceFile);
                 OutputStream out = Files.newOutputStream(tempFile)) {
                bytesStored = copy(in, out);
            }
            // Publish the complete copy under its real name. With no options, Files.move refuses
            // to replace an existing file (it throws FileAlreadyExistsException), so a chunk that
            // appeared in the meantime is never overwritten. Do not add ATOMIC_MOVE here: Java
            // allows an atomic move to replace an existing file, and on Windows it does.
            Files.move(tempFile, chunkFile);
            return bytesStored;
        } catch (IOException e) {
            // The chunk's real name was never created, so the temp file is the only thing to remove.
            deleteAfterFailure(tempFile, e);
            throw e;
        }
    }

    /**
     * Copies the chunk {@code chunkId} out of this node into a new file at {@code destination}.
     * The chunk stays on the node. Missing parent directories of {@code destination} are created.
     *
     * @return the number of bytes copied
     * @throws NullPointerException if an argument is null
     * @throws IllegalArgumentException if the chunk ID is invalid, if {@code destination} is the
     *         node's storage directory or anywhere inside it, or if {@code destination} is an
     *         existing directory (all checked before anything is created)
     * @throws ChunkNotFoundException if this node's storage is a plain directory with no entry
     *         under this ID (nothing is created in that case)
     * @throws FileAlreadyExistsException if {@code destination} already exists
     * @throws IOException if the node cannot tell whether it holds the chunk as a regular file:
     *         its storage directory is missing or is not a plain directory, or the entry under this
     *         ID is a directory, symbolic link, junction or other reparse point (read without
     *         following links, so a link is never read through, even if its target exists); or if
     *         copying fails, in which case a partial destination created by this call is deleted
     *         first, but empty parent directories it created may remain
     */
    public long retrieveChunk(String chunkId, Path destination) throws IOException {
        Path chunkFile = chunkFileFor(chunkId);
        Objects.requireNonNull(destination, "destination must not be null");
        requireOutsideStorageDir(destination);
        if (Files.isDirectory(destination)) {
            throw new IllegalArgumentException("Destination is an existing directory: " + destination);
        }
        // No-follow check, as for deleting: a link or junction under the chunk's name (or in place
        // of the storage directory) is refused rather than read, so the node never serves data
        // from outside its own directory.
        if (!regularChunkFileExists(chunkFile)) {
            throw new ChunkNotFoundException(chunkId, storageDir);
        }

        // Create missing parent directories, as FileReconstructor does.
        Path parentDir = destination.toAbsolutePath().getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }

        // Becomes true only once CREATE_NEW has created the destination, so cleanup can never
        // delete a file that existed before this call.
        boolean destinationCreated = false;
        try (InputStream in = openForReading(chunkFile);
             OutputStream out = Files.newOutputStream(destination, StandardOpenOption.CREATE_NEW)) {
            destinationCreated = true;
            return copy(in, out);
        } catch (IOException e) {
            if (destinationCreated) {
                deleteAfterFailure(destination, e);
            }
            throw e;
        }
    }

    /**
     * Reports whether this node holds the chunk {@code chunkId}.
     *
     * <ul>
     *   <li>{@code true}: the storage directory contains a chunk file with this ID.</li>
     *   <li>{@code false}: the storage directory is available and has no chunk with this ID.</li>
     *   <li>{@link IOException}: the node cannot tell, for example because its storage directory
     *       was deleted or moved after the node started, or because something other than a
     *       regular file sits under the chunk's name.</li>
     * </ul>
     *
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid
     */
    public boolean hasChunk(String chunkId) throws IOException {
        return chunkExists(chunkFileFor(chunkId));
    }

    /**
     * Deletes the chunk {@code chunkId} from this node.
     *
     * <p>Unlike {@link #hasChunk}, every check here reads attributes without following links: the
     * storage directory must be a plain directory, not a symbolic link or junction, and the
     * chunk's entry must be a regular file. If either cannot be established, nothing is deleted.
     *
     * @return {@code true} if this call deleted the chunk; {@code false} only if the storage
     *         directory is available and has no entry with this ID. That includes a chunk that
     *         disappeared between the check and the delete, which is confirmed by checking again.
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid (checked before any disk access)
     * @throws IOException if the storage directory is missing or is not a plain directory, if the
     *         entry under this ID is not a regular file (it is left in place), or if deleting fails
     */
    public boolean deleteChunk(String chunkId) throws IOException {
        Path chunkFile = chunkFileFor(chunkId);
        if (!regularChunkFileExists(chunkFile)) {
            return false;
        }
        try {
            deleteChunkFile(chunkFile);
        } catch (NoSuchFileException e) {
            // The chunk vanished between the check and the delete. Check again before calling it
            // absent: the storage directory itself may have gone, or something may be there again.
            boolean stillPresent;
            try {
                stillPresent = regularChunkFileExists(chunkFile);
            } catch (IOException recheckError) {
                recheckError.addSuppressed(e);
                throw recheckError;
            }
            if (stillPresent) {
                throw new IOException("Chunk \"" + chunkId + "\" was reported missing by the delete but is still"
                        + " present: " + chunkFile, e);
            }
            return false;
        }
        return true;
    }

    /**
     * Reports whether this node holds the chunk as a genuine regular file, using the same
     * no-follow checks as {@link #deleteChunk}: {@code true} for a regular file, {@code false} only
     * if the storage directory is a plain directory with no entry under this ID, and an
     * IOException in every other case (a missing or linked storage directory, or an entry that is
     * a directory, symbolic link, junction or other reparse point).
     *
     * <p>Package-private: StorageCluster.deleteChunk uses it, rather than {@link #hasChunk} (which
     * follows links), to decide which node holds a chunk before deleting anything.
     *
     * @throws NullPointerException if {@code chunkId} is null
     * @throws IllegalArgumentException if the chunk ID is invalid (checked before any disk access)
     */
    boolean holdsRegularChunkFile(String chunkId) throws IOException {
        return regularChunkFileExists(chunkFileFor(chunkId));
    }

    /**
     * Deletes one chunk file. Used only by {@link #deleteChunk}.
     *
     * <p>Why this method exists: it is a small testability hook, like {@link #openForReading}.
     * StorageNodeTest and StorageClusterTest (same package) override it to make a delete fail, or
     * to make the file disappear just before the delete, which a real disk cannot be made to do on
     * demand. It is package-private, so it is not part of the public API.
     */
    void deleteChunkFile(Path chunkFile) throws IOException {
        Files.delete(chunkFile);
    }

    /**
     * The names of every entry in the storage directory, sorted, whatever they are: chunks, temp
     * files and anything else. The directory must be a plain directory (not a link or junction).
     *
     * <p>Package-private: startup recovery uses it to see which chunks exist.
     *
     * @throws IOException if the storage directory is missing, is not a plain directory, or
     *         cannot be listed
     */
    List<String> listEntryNames() throws IOException {
        requirePlainStorageDirectory();
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(storageDir)) {
            for (Path entry : entries) {
                names.add(entry.getFileName().toString());
            }
        }
        Collections.sort(names);
        return names;
    }

    /**
     * Deletes temp files that a killed store left behind ({@code <chunkId>-<digits>.tmp}), with
     * the rules of {@link StaleTempFiles#sweep}: regular files only, never following a link, and
     * never touching a chunk. Returns how many were deleted.
     *
     * <p>Package-private: only startup recovery calls it, while no store can be running.
     *
     * @throws IOException if the storage directory is not a plain directory or cannot be listed
     */
    int sweepStaleTempFiles(Consumer<String> warnings) throws IOException {
        requirePlainStorageDirectory();
        return StaleTempFiles.sweep(storageDir, "the storage folder of node " + storageDir, TEMP_NAME,
                name -> CHUNK_NAME.matcher(name).matches(), warnings);
    }

    /**
     * Opens a file for reading: the source file in {@link #storeChunk}, the chunk file in
     * {@link #retrieveChunk}.
     *
     * <p>Why this method exists: it is a small testability hook, like
     * {@code FileReconstructor.openChunk}. Making a real disk fail halfway through a copy is not
     * practical, so StorageNodeTest (which is in the same package) overrides this method to
     * return a stream that fails, or does something else, partway through. It is package-private
     * rather than public, so it is not part of the public API.
     */
    InputStream openForReading(Path file) throws IOException {
        return Files.newInputStream(file);
    }

    /**
     * The node's storage directory, as an absolute path without "." or ".." parts.
     *
     * <p>Package-private, so callers outside this package still never get a path into a node's
     * storage. StorageCluster (same package) uses it to keep node directories from overlapping
     * and to keep retrieved copies out of every node's directory.
     */
    Path storageDir() {
        return storageDir;
    }

    /**
     * Rejects a retrieve destination that is the storage directory itself or anywhere inside it.
     * The node owns its directory: a retrieved copy written there could later be mistaken for a
     * chunk the node stored itself.
     *
     * <p>The check compares absolute, normalized paths, so "." and ".." cannot sneak a destination
     * past it. {@code Path.startsWith} compares whole path parts, so a sibling folder such as
     * {@code node-copy} is not mistaken for {@code node}.
     *
     * <p>Limitation: this is a check on the path's text. It does not follow symbolic links,
     * Windows junctions, or Windows short names (such as {@code STORAG~1}), so a destination that
     * reaches the storage directory through one of those is not detected.
     */
    private void requireOutsideStorageDir(Path destination) {
        Path normalizedDestination = destination.toAbsolutePath().normalize();
        if (normalizedDestination.startsWith(storageDir)) {
            throw new IllegalArgumentException("Destination must be outside the node's storage directory ("
                    + storageDir + "), but was: " + destination);
        }
    }

    /** Validates a chunk ID and returns the file that holds (or would hold) that chunk. */
    private Path chunkFileFor(String chunkId) {
        validateChunkId(chunkId);
        Path chunkFile = storageDir.resolve(chunkId).normalize();
        // Second line of defense. The ID rules already make it impossible to leave the storage
        // directory (no '/', '\', ':' or '.'), so this check cannot fail today. It stays as a
        // safety net in case the rules are ever relaxed.
        if (!storageDir.equals(chunkFile.getParent())) {
            throw new IllegalArgumentException("Chunk ID would point outside the storage directory: " + chunkId);
        }
        return chunkFile;
    }

    private static void validateChunkId(String chunkId) {
        Objects.requireNonNull(chunkId, "chunkId must not be null");
        if (chunkId.isEmpty() || chunkId.length() > MAX_CHUNK_ID_LENGTH) {
            throw new IllegalArgumentException("Chunk ID must be 1 to " + MAX_CHUNK_ID_LENGTH
                    + " characters long, but had " + chunkId.length());
        }
        for (int i = 0; i < chunkId.length(); i++) {
            char c = chunkId.charAt(i);
            // Explicit ASCII ranges on purpose: Character.isLetterOrDigit would also accept
            // uppercase letters and letters such as 'é'.
            boolean allowed = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
            if (!allowed) {
                throw new IllegalArgumentException("Chunk ID may only contain a-z, 0-9, '_' and '-', but has '"
                        + c + "' at position " + i + ": \"" + chunkId + "\"");
            }
        }
        if (WINDOWS_DEVICE_NAMES.contains(chunkId)) {
            throw new IllegalArgumentException("Chunk ID \"" + chunkId + "\" is a reserved Windows device name");
        }
    }

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

    /**
     * Returns true if the chunk file exists and false if it does not. Throws if the node cannot
     * tell, so that a broken node is never mistaken for one that simply lacks the chunk.
     */
    private boolean chunkExists(Path chunkFile) throws IOException {
        BasicFileAttributes attributes;
        try {
            // readAttributes throws NoSuchFileException when nothing has this name, and other
            // IOExceptions (for example "access denied") when the file system cannot answer.
            // Files.exists() would return false in both cases and hide real errors.
            attributes = Files.readAttributes(chunkFile, BasicFileAttributes.class);
        } catch (NoSuchFileException e) {
            // Nothing under this name. That means "chunk absent" only if the storage directory
            // itself is still there; if it has gone, the node is broken, not empty.
            checkStorageDirectoryAvailable();
            return false;
        }
        if (!attributes.isRegularFile()) {
            // The node only ever creates regular files, so anything else means its storage
            // was changed from outside.
            throw new IOException("Storage entry for this chunk is not a regular file: " + chunkFile);
        }
        return true;
    }

    private void checkStorageDirectoryAvailable() throws IOException {
        if (!Files.isDirectory(storageDir)) {
            throw new IOException("Storage directory is not available (deleted, moved, or replaced since"
                    + " the node started): " + storageDir);
        }
    }

    /**
     * The no-follow check used for deleting and retrieving. Returns true if the entry is a regular
     * file, and false only if the storage directory is a plain directory with no entry under this
     * name.
     * Throws if the storage directory is missing or is not a plain directory, if the entry is
     * anything other than a regular file (a directory, symbolic link, junction or other reparse
     * point: Java reports all of these as not regular when links are not followed), or if the
     * attributes cannot be read.
     */
    private boolean regularChunkFileExists(Path chunkFile) throws IOException {
        requirePlainStorageDirectory();
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(chunkFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            // Nothing under this name means "absent" only while the storage directory is still there.
            requirePlainStorageDirectory();
            return false;
        }
        if (!attributes.isRegularFile()) {
            throw new IOException("Storage entry for this chunk is not a regular file (it may be a directory,"
                    + " a link or a junction), so it was left alone: " + chunkFile);
        }
        return true;
    }

    /**
     * Requires the storage directory to be a plain directory, read without following links: not
     * missing, not a file, not a symbolic link, and not a junction or other reparse point. Uses
     * the same check as DataDirectory, which Java's attributes support as "directory, and neither
     * a symbolic link nor other".
     */
    private void requirePlainStorageDirectory() throws IOException {
        if (!DataDirectory.isPlainDirectory(storageDir)) {
            throw new IOException("Storage directory is missing or is not a plain directory (it may have been"
                    + " deleted, or replaced by a file, link or junction), so it was left alone: " + storageDir);
        }
    }

    /** Copies everything from {@code in} to {@code out} through one reusable buffer. */
    private static long copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long totalBytes = 0;
        while (true) {
            int bytesRead = in.read(buffer);
            if (bytesRead == -1) {
                break; // end of input
            }
            // Write exactly what was read: a read may return fewer bytes than the buffer holds.
            out.write(buffer, 0, bytesRead);
            totalBytes += bytesRead;
        }
        return totalBytes;
    }

    /**
     * Best-effort removal of a file this call created. If the delete fails, that error is
     * attached to the original exception as "suppressed" so the original cause is never hidden.
     */
    private static void deleteAfterFailure(Path file, IOException originalError) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException cleanupError) {
            originalError.addSuppressed(cleanupError);
        }
    }
}
