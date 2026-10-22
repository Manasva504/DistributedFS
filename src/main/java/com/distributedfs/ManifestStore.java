package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Keeps the server's file metadata as small text files in one folder (the data directory's
 * {@code manifests} folder):
 * <pre>
 *   &lt;fileId&gt;.manifest   one per stored file (see {@link FileManifest})
 *   &lt;fileId&gt;.pending    an upload in progress (see {@link PendingUpload})
 * </pre>
 *
 * <p><b>Written once, never replaced.</b> Each file is written to a new temp file named
 * {@code <fileId>-<digits>.tmp} and then renamed to its real name. The rename refuses to replace
 * an existing file, so a manifest is never overwritten, and its name only appears once all of
 * its bytes are written. This is the same pattern StorageNode uses for chunks.
 *
 * <p><b>Answers versus errors.</b> A read returns an empty Optional only when the folder is there
 * and has no entry with that name. When the folder itself is gone, it throws an IOException, and
 * when the entry is there but cannot be trusted (wrong text, not a plain file, too large), it
 * throws {@link InvalidManifestException}. Damaged metadata is never reported as "not found".
 *
 * <p><b>Untrusted IDs.</b> Every method checks the file ID with {@link FileIds#isValid} before
 * building a path from it, and checks that the path stays directly inside the folder.
 *
 * <p><b>Assumptions and limits.</b>
 * <ul>
 *   <li>Not thread-safe: callers serialize access (the file service holds its storage lock).</li>
 *   <li>The server owns the folder; other programs are not expected to change it.</li>
 *   <li>Nothing is forced to disk (no fsync), so a power cut can still lose or damage a file.
 *       Damage is then reported as an invalid manifest rather than as wrong data.</li>
 * </ul>
 */
class ManifestStore {

    static final String MANIFEST_SUFFIX = ".manifest";
    static final String PENDING_SUFFIX = ".pending";

    private static final Pattern MANIFEST_NAME = Pattern.compile("[0-9a-f]{32}\\.manifest");
    private static final Pattern PENDING_NAME = Pattern.compile("([0-9a-f]{32})\\.pending");

    /**
     * Exactly the names {@code Files.createTempFile(folder, fileId + "-", ".tmp")} produces: the
     * JDK inserts {@code Long.toUnsignedString} of a random number, so 1 to 20 digits.
     */
    private static final Pattern TEMP_NAME = Pattern.compile("[0-9a-f]{32}-[0-9]{1,20}\\.tmp");

    /** The manifest folder, as an absolute path without "." or ".." parts. */
    private final Path manifestsDir;

    /**
     * @throws NullPointerException if {@code manifestsDir} is null
     * @throws IOException if it is missing or is not a plain folder (a file, link or junction)
     */
    ManifestStore(Path manifestsDir) throws IOException {
        Objects.requireNonNull(manifestsDir, "manifestsDir must not be null");
        this.manifestsDir = manifestsDir.toAbsolutePath().normalize();
        if (!DataDirectory.isPlainDirectory(this.manifestsDir)) {
            throw new IOException("Manifest folder is missing or is not a plain folder (a file, link or junction): "
                    + this.manifestsDir);
        }
    }

    Path directory() {
        return manifestsDir;
    }

    // ---------------------------------------------------------------------
    // Manifests
    // ---------------------------------------------------------------------

    /**
     * Writes the manifest. From this moment the file exists for everyone who reads the store.
     *
     * @throws FileAlreadyExistsException if a manifest (or anything else) already has this name;
     *         nothing is changed
     * @throws IOException if writing fails; the temp file is deleted and no manifest appears
     */
    void create(FileManifest manifest) throws IOException {
        Objects.requireNonNull(manifest, "manifest must not be null");
        writeNewFile(manifest.fileId(), entryFor(manifest.fileId(), MANIFEST_SUFFIX), manifest.toText());
    }

    /**
     * Reads the manifest of {@code fileId}.
     *
     * @return the manifest, or an empty Optional if the folder has no manifest for this ID
     * @throws IllegalArgumentException if {@code fileId} is not a valid file ID (checked before
     *         any file system access)
     * @throws InvalidManifestException if the entry exists but is not a valid manifest for this ID
     * @throws IOException if the folder is unavailable or reading fails
     */
    Optional<FileManifest> read(String fileId) throws IOException {
        Path file = entryFor(fileId, MANIFEST_SUFFIX);
        Optional<String> text = readSmallTextFile(file, FileManifest.MAX_TEXT_BYTES);
        if (text.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(FileManifest.parse(text.get(), fileId));
        } catch (InvalidManifestException e) {
            throw new InvalidManifestException("Manifest " + file + " is invalid: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes the manifest of {@code fileId}.
     *
     * @return true if it was deleted, false if there was none
     * @throws IOException if the folder is unavailable, the entry is not a regular file (it is
     *         left in place), or deleting fails
     */
    boolean delete(String fileId) throws IOException {
        return deleteRegularFile(entryFor(fileId, MANIFEST_SUFFIX));
    }

    // ---------------------------------------------------------------------
    // Pending-upload markers
    // ---------------------------------------------------------------------

    /** Writes a pending-upload marker, with the same never-replace rules as {@link #create}. */
    void createPending(PendingUpload pending) throws IOException {
        Objects.requireNonNull(pending, "pending must not be null");
        writeNewFile(pending.fileId(), entryFor(pending.fileId(), PENDING_SUFFIX), pending.toText());
    }

    /** Reads a pending-upload marker, with the same rules as {@link #read}. */
    Optional<PendingUpload> readPending(String fileId) throws IOException {
        Path file = entryFor(fileId, PENDING_SUFFIX);
        Optional<String> text = readSmallTextFile(file, FileManifest.MAX_TEXT_BYTES);
        if (text.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(PendingUpload.parse(text.get(), fileId));
        } catch (InvalidManifestException e) {
            throw new InvalidManifestException("Pending-upload marker " + file + " is invalid: " + e.getMessage(), e);
        }
    }

    /** Deletes a pending-upload marker, with the same rules as {@link #delete}. */
    boolean deletePending(String fileId) throws IOException {
        return deleteRegularFile(entryFor(fileId, PENDING_SUFFIX));
    }

    /**
     * The file IDs of all pending-upload markers, sorted. Only names of the exact form
     * {@code <fileId>.pending} count; their contents are not read here.
     *
     * @throws IOException if the folder cannot be listed
     */
    List<String> listPendingFileIds() throws IOException {
        List<String> fileIds = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(manifestsDir)) {
            for (Path entry : entries) {
                Matcher matcher = PENDING_NAME.matcher(entry.getFileName().toString());
                if (matcher.matches()) {
                    fileIds.add(matcher.group(1));
                }
            }
        }
        Collections.sort(fileIds);
        return fileIds;
    }

    /**
     * Reports whether {@code fileId} is already used by a manifest or a pending-upload marker.
     * Any entry under either name counts, even a damaged one, so a new upload never reuses it.
     *
     * @throws IOException if the folder is unavailable
     */
    boolean isIdTaken(String fileId) throws IOException {
        return entryExists(entryFor(fileId, MANIFEST_SUFFIX)) || entryExists(entryFor(fileId, PENDING_SUFFIX));
    }

    // ---------------------------------------------------------------------
    // Cleanup of temp files left by a process that was killed while writing
    // ---------------------------------------------------------------------

    /**
     * Deletes stale temp files left in the folder by an interrupted write, and returns how many
     * were deleted.
     *
     * <p>An entry is deleted only if all of these hold: it is listed directly in the folder, its
     * name is exactly {@code <fileId>-<1 to 20 digits>.tmp}, and its attributes, read without
     * following links, say it is a regular file. It is then removed with one delete of that name,
     * so nothing is ever followed or walked. Manifests and pending markers are never touched.
     * Every other entry, and every entry that cannot be inspected or deleted, is left in place
     * and reported through {@code warnings} (see {@link StaleTempFiles#sweep}).
     *
     * @throws IOException if the folder cannot be listed at all
     */
    int sweepStaleTempFiles(Consumer<String> warnings) throws IOException {
        return StaleTempFiles.sweep(manifestsDir, "the manifest folder", TEMP_NAME,
                name -> MANIFEST_NAME.matcher(name).matches() || PENDING_NAME.matcher(name).matches(), warnings);
    }

    /**
     * The names of every entry in the folder, sorted, whatever they are. Startup recovery uses it
     * to see the manifests and pending markers that exist, by exact name and in one listing.
     *
     * @throws IOException if the folder is unavailable or cannot be listed
     */
    List<String> listEntryNames() throws IOException {
        requireFolderAvailable();
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(manifestsDir)) {
            for (Path entry : entries) {
                names.add(entry.getFileName().toString());
            }
        }
        Collections.sort(names);
        return names;
    }

    // ---------------------------------------------------------------------
    // Testability hooks
    // ---------------------------------------------------------------------

    /**
     * Writes the bytes of a new temp file.
     *
     * <p>Why this method and {@link #moveIntoPlace} exist: they are small testability hooks, like
     * StorageNode.openForReading. ManifestStoreTest (same package) overrides them to fail
     * partway through a write or at the rename, which a real disk cannot be made to do on demand.
     */
    void writeContent(Path file, byte[] content) throws IOException {
        Files.write(file, content);
    }

    /** Renames the finished temp file to its real name. With no options, the move never replaces a file. */
    void moveIntoPlace(Path source, Path target) throws IOException {
        Files.move(source, target);
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** Validates the file ID and returns its entry, which is always directly inside the folder. */
    private Path entryFor(String fileId, String suffix) {
        Objects.requireNonNull(fileId, "fileId must not be null");
        if (!FileIds.isValid(fileId)) {
            throw new IllegalArgumentException("fileId is not a valid file ID");
        }
        Path entry = manifestsDir.resolve(fileId + suffix).normalize();
        // Second line of defense. A valid ID has no separators or dots, so this cannot fail today.
        if (!manifestsDir.equals(entry.getParent())) {
            throw new IllegalArgumentException("file ID would point outside the manifest folder");
        }
        return entry;
    }

    private void writeNewFile(String fileId, Path target, String text) throws IOException {
        byte[] content = text.getBytes(StandardCharsets.US_ASCII);
        if (entryExists(target)) {
            throw new FileAlreadyExistsException(target.toString(), null, "it is never replaced");
        }
        // createTempFile picks a new name, so this call owns the temp file and may delete it.
        Path tempFile = Files.createTempFile(manifestsDir, fileId + "-", ".tmp");
        try {
            writeContent(tempFile, content);
            moveIntoPlace(tempFile, target);
        } catch (IOException | RuntimeException e) {
            deleteAfterFailure(tempFile, e);
            throw e;
        }
    }

    /**
     * Returns the file's text, or empty if nothing has this name. The file must be a regular file
     * (checked without following links), at most {@code maxBytes} long, and plain ASCII.
     */
    private Optional<String> readSmallTextFile(Path file, int maxBytes) throws IOException {
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            requireFolderAvailable();
            return Optional.empty();
        }
        if (!attributes.isRegularFile()) {
            throw new InvalidManifestException(file + " is not a regular file");
        }
        if (attributes.size() > maxBytes) {
            throw new InvalidManifestException(file + " is larger than " + maxBytes + " bytes");
        }
        byte[] bytes;
        try (InputStream in = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            // One byte more than allowed, so a file that grew after the size check is still caught.
            bytes = in.readNBytes(maxBytes + 1);
        }
        if (bytes.length > maxBytes) {
            throw new InvalidManifestException(file + " is larger than " + maxBytes + " bytes");
        }
        for (byte b : bytes) {
            if (b < 0) {
                throw new InvalidManifestException(file + " contains bytes that are not ASCII");
            }
        }
        return Optional.of(new String(bytes, StandardCharsets.US_ASCII));
    }

    /** True if anything has this name; false only if nothing does and the folder is available. */
    private boolean entryExists(Path entry) throws IOException {
        try {
            Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException e) {
            requireFolderAvailable();
            return false;
        }
    }

    private boolean deleteRegularFile(Path entry) throws IOException {
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            requireFolderAvailable();
            return false;
        }
        if (!attributes.isRegularFile()) {
            throw new IOException(entry + " is not a regular file; it was left in place");
        }
        Files.delete(entry);
        return true;
    }

    /** A missing name means "absent" only while the folder itself is still there. */
    private void requireFolderAvailable() throws IOException {
        if (!DataDirectory.isPlainDirectory(manifestsDir)) {
            throw new IOException("Manifest folder is not available (deleted, moved, or replaced): " + manifestsDir);
        }
    }

    /** Best-effort removal of a temp file this call created; a failure is attached to the original error. */
    private static void deleteAfterFailure(Path file, Exception originalError) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException cleanupError) {
            originalError.addSuppressed(cleanupError);
        }
    }

    // ---------------------------------------------------------------------
    // Pending-upload marker
    // ---------------------------------------------------------------------

    /**
     * The record that an upload has started: written before its first chunk is stored and
     * deleted once its manifest exists. If the server stops in between, startup recovery uses
     * {@code chunkCount} to find and delete the chunks the upload may have stored.
     *
     * <p>Text format, read as strictly as a manifest:
     * <pre>
     *   format=1
     *   fileId=0123456789abcdef0123456789abcdef
     *   chunkCount=4
     * </pre>
     */
    record PendingUpload(String fileId, int chunkCount) {

        private static final String[] KEYS = {"format", "fileId", "chunkCount"};

        /**
         * @throws NullPointerException if {@code fileId} is null
         * @throws IllegalArgumentException if {@code fileId} is invalid or {@code chunkCount} is negative
         */
        PendingUpload {
            Objects.requireNonNull(fileId, "fileId must not be null");
            if (!FileIds.isValid(fileId)) {
                throw new IllegalArgumentException("fileId is not a valid file ID");
            }
            if (chunkCount < 0) {
                throw new IllegalArgumentException("chunkCount must not be negative, but was " + chunkCount);
            }
        }

        /** The chunk ID of chunk {@code index} of this upload. */
        String chunkId(int index) {
            Objects.checkIndex(index, chunkCount);
            return FileIds.chunkId(fileId, index);
        }

        String toText() {
            return "format=" + FileManifest.FORMAT_VERSION + "\n"
                    + "fileId=" + fileId + "\n"
                    + "chunkCount=" + chunkCount + "\n";
        }

        static PendingUpload parse(String text, String expectedFileId) throws InvalidManifestException {
            String[] values = FileManifest.readFields(text, KEYS);
            if (!values[0].equals(String.valueOf(FileManifest.FORMAT_VERSION))) {
                throw new InvalidManifestException("unsupported marker format \"" + values[0] + "\"");
            }
            if (!values[1].equals(expectedFileId)) {
                throw new InvalidManifestException("the marker describes a different file ID than its name says");
            }
            PendingUpload pending;
            try {
                pending = new PendingUpload(values[1],
                        FileManifest.toInt(FileManifest.parseNumber(values[2], "chunkCount"), "chunkCount"));
            } catch (IllegalArgumentException e) {
                throw new InvalidManifestException("invalid marker values: " + e.getMessage(), e);
            }
            FileManifest.requireCanonical(text, pending.toText());
            return pending;
        }
    }
}
