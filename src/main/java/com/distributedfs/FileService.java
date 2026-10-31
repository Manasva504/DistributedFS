package com.distributedfs;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Stores files (hashing, chunking, placing the chunks on the cluster, and recording the file's
 * manifest) and prepares them for download. It knows nothing about HTTP.
 *
 * <p><b>One storage lock.</b> StorageCluster and ManifestStore are not thread-safe, and an upload
 * is several steps (pending marker, chunks, manifest) that must not interleave with another
 * upload's or with a download reading the same storage. Every step that touches the cluster or
 * the manifests, for uploads and downloads alike, runs while holding one lock. Hashing, chunking,
 * rebuilding and checking, which only touch the request's own workspace, run outside it. The lock
 * covers the whole storage rather than one file ID because the cluster and the manifest store are
 * shared by every file.
 *
 * <p><b>Store, step by step</b> ({@link #store}):
 * <ol>
 *   <li>Hash the spooled body; if the client sent a checksum and it differs, stop. Nothing has
 *       been stored.</li>
 *   <li>Split the body into chunks inside the workspace, and hash every chunk. An upload that
 *       would need more chunks than a manifest may list is refused first; nothing is stored.</li>
 *   <li>Holding the lock: pick an unused file ID, write its pending-upload marker, store the
 *       chunks {@code <fileId>_0, _1, ...}, and write the manifest. <b>Writing the manifest is
 *       the commit point</b>: before it, the file does not exist; after it, it does.</li>
 *   <li>Delete the pending marker. If that fails, the upload has still succeeded; the marker is
 *       left in place and a warning is logged.</li>
 * </ol>
 * If storing a chunk fails, or writing the manifest fails and the manifest is confirmed absent,
 * the upload is rolled back: exactly the chunks this upload recorded as stored are deleted, every
 * copy of each, newest first. A chunk is recorded once all of its copies are confirmed (the
 * cluster's replication factor; see {@link StorageCluster}). A chunk whose store had an unknown
 * outcome, or that got too few copies, counts as "maybe stored" and is rolled back too; if every
 * node confirms it absent, there is nothing to delete. If every delete succeeds, the marker is deleted too. If any delete fails, the marker is
 * kept (so the leftover chunks stay on record for startup recovery) and the delete failures are
 * attached to the thrown exception as suppressed. A failed manifest write is never rolled back on
 * assumption: see {@link #resolveFailedCommit}.
 *
 * <p><b>Download, step by step</b> ({@link #download}):
 * <ol>
 *   <li>Holding the lock: read the manifest, then copy the file's chunks, in order, from the
 *       cluster into the request's workspace. For each chunk the nodes are tried in turn, and the
 *       first copy whose SHA-256 matches the manifest is kept (format 2 manifests); a node that is
 *       down, has no copy or has a damaged one is skipped with a warning, and the download fails
 *       only when no good copy exists. The manifest alone decides whether the file
 *       exists: it is the upload's commit point, so a pending marker without a manifest is an
 *       upload that never committed (not found), and a manifest with a leftover marker is a
 *       committed file whose marker could not be deleted (served). Because uploads write the
 *       manifest last and roll back only uploads without one, all under the same lock, a
 *       download never sees a half-stored or half-rolled-back file.</li>
 *   <li>Without the lock: rebuild the file from the checked copies, delete the copies, and check
 *       the result's size and SHA-256 against the manifest. Only a file that passes every check is
 *       returned, so no byte of a chunk reaches the client before that chunk (and the whole file)
 *       has been verified.</li>
 * </ol>
 */
class FileService {

    /** How many file IDs to try before giving up; a collision of random 128-bit IDs is not expected. */
    static final int ID_ATTEMPTS = 3;

    /** Workspace names; StartupRecovery removes stale workspaces by exactly these prefixes. */
    private static final String WORKSPACE_PREFIX = "upload-";
    private static final String DOWNLOAD_WORKSPACE_PREFIX = "download-";

    /**
     * A file ready to be sent: its manifest and its rebuilt, verified copy in the request's
     * workspace. {@code file} has exactly {@code manifest.size()} bytes and the manifest's SHA-256.
     */
    record Download(FileManifest manifest, Path file) {
    }

    /** Supplies new file IDs. Tests replace it to force collisions. */
    interface IdGenerator {
        String next();
    }

    private final StorageCluster cluster;
    private final ManifestStore manifests;
    private final Path tmpDir;
    private final int chunkSize;
    private final long maxUploadBytes;
    private final IdGenerator ids;
    private final PrintStream err;
    private final Object storageLock = new Object();
    /** Told about every committed upload (the web console's activity feed); null for none. */
    private volatile UploadListener uploadListener;

    /** Told once per committed upload, with the number of chunk copies stored. Must be quick and do no I/O. */
    interface UploadListener {
        void committed(FileManifest manifest, int copies);
    }

    FileService(StorageCluster cluster, ManifestStore manifests, Path tmpDir, int chunkSize, long maxUploadBytes,
                PrintStream err) {
        this(cluster, manifests, tmpDir, chunkSize, maxUploadBytes, FileIds::newFileId, err);
    }

    FileService(StorageCluster cluster, ManifestStore manifests, Path tmpDir, int chunkSize, long maxUploadBytes,
                IdGenerator ids, PrintStream err) {
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.manifests = Objects.requireNonNull(manifests, "manifests must not be null");
        this.tmpDir = Objects.requireNonNull(tmpDir, "tmpDir must not be null");
        this.ids = Objects.requireNonNull(ids, "ids must not be null");
        this.err = Objects.requireNonNull(err, "err must not be null");
        if (chunkSize < 1 || maxUploadBytes < 1) {
            throw new IllegalArgumentException("chunkSize and maxUploadBytes must be at least 1");
        }
        this.chunkSize = chunkSize;
        this.maxUploadBytes = maxUploadBytes;
    }

    /** Tells {@code listener} about every later committed upload (the server sets this before it listens). */
    void setUploadListener(UploadListener listener) {
        this.uploadListener = Objects.requireNonNull(listener, "listener must not be null");
    }

    /** The largest upload accepted, in bytes. */
    long maxUploadBytes() {
        return maxUploadBytes;
    }

    /** Creates a new, empty workspace for one request: a folder named upload-(digits) in the tmp folder. */
    Path newUploadWorkspace() throws IOException {
        return Files.createTempDirectory(tmpDir, WORKSPACE_PREFIX);
    }

    /** Creates a new, empty workspace for one download: a folder named download-(digits) in the tmp folder. */
    Path newDownloadWorkspace() throws IOException {
        return Files.createTempDirectory(tmpDir, DOWNLOAD_WORKSPACE_PREFIX);
    }

    /** Work done while holding the storage lock; see {@link #withStorageLock}. */
    interface StorageAction<T> {
        T run() throws IOException;
    }

    /**
     * Runs {@code action} while holding the same storage lock that uploads and downloads hold
     * while they touch the cluster and the manifests. Repair ({@link RepairService}) uses it for
     * each chunk, so it never works on a chunk while a request does.
     */
    <T> T withStorageLock(StorageAction<T> action) throws IOException {
        synchronized (storageLock) {
            return action.run();
        }
    }

    /**
     * Deletes a request's workspace without following links (see {@link FileTrees#deleteTree}).
     * A failure is logged, not thrown: the request's outcome does not depend on it.
     */
    void deleteWorkspace(Path workspace) {
        try {
            FileTrees.deleteTree(workspace);
        } catch (IOException | RuntimeException e) {
            err.println("Warning: could not delete the request workspace " + workspace + ": " + e);
        }
    }

    /**
     * Stores the spooled upload {@code body} as a new file, as described in the class comment.
     *
     * @param body           the complete upload, inside {@code workspace}
     * @param workspace      this request's workspace; chunks are written to its {@code chunks} folder
     * @param expectedSha256 the checksum the client sent (64 hex characters, either case), or null
     * @return the new file's manifest
     * @throws FileServiceException with CHECKSUM_MISMATCH (nothing stored) or STORAGE_ERROR
     * @throws IOException if reading the body or writing chunks in the workspace fails (nothing stored)
     */
    FileManifest store(Path body, Path workspace, String expectedSha256) throws IOException {
        return store(body, workspace, expectedSha256, null);
    }

    /**
     * As {@link #store(Path, Path, String)}, with the file's original name ({@link FileNames#isValid}),
     * or null for none. A name is stored in the manifest (format 3); without one the manifest is format 2.
     */
    FileManifest store(Path body, Path workspace, String expectedSha256, String name) throws IOException {
        if (name != null && !FileNames.isValid(name)) {
            throw new IllegalArgumentException("name is not a valid file name");
        }
        Objects.requireNonNull(body, "body must not be null");
        Objects.requireNonNull(workspace, "workspace must not be null");

        ChecksumService checksums = new ChecksumService();
        String sha256 = checksums.calculateSha256(body);
        long size = Files.size(body);
        if (expectedSha256 != null && !sha256.equals(expectedSha256.toLowerCase(Locale.ROOT))) {
            throw new FileServiceException(FileServiceException.Reason.CHECKSUM_MISMATCH,
                    "the uploaded data has SHA-256 " + sha256 + ", not the expected " + expectedSha256, null);
        }
        long chunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        if (chunkCount > FileManifest.MAX_CHECKSUMMED_CHUNKS) {
            throw new FileServiceException(FileServiceException.Reason.TOO_MANY_CHUNKS, "an upload of " + size
                    + " bytes needs " + chunkCount + " chunks of " + chunkSize + " bytes, more than the "
                    + FileManifest.MAX_CHECKSUMMED_CHUNKS + " a manifest may list", null);
        }

        List<Path> chunks = new FileChunker(chunkSize).chunk(body, workspace.resolve("chunks"));
        try {
            Files.deleteIfExists(body); // only to save disk space; the workspace is deleted anyway
        } catch (IOException ignored) {
            // the workspace deletion after the request removes it
        }
        // The SHA-256 of every chunk, from the exact files that are about to be stored (each node
        // copies its chunk byte for byte). Like the whole-file hash, this happens before the lock.
        List<String> chunkSha256s = new ArrayList<>();
        for (Path chunk : chunks) {
            chunkSha256s.add(checksums.calculateSha256(chunk));
        }
        synchronized (storageLock) {
            return storeLocked(chunks, size, sha256, chunkSha256s, name);
        }
    }

    /**
     * Rebuilds the committed file {@code fileId} inside {@code workspace} and verifies it, as
     * described in the class comment. Everything this writes stays inside {@code workspace}; the
     * caller deletes the workspace afterwards, whatever happened.
     *
     * @param fileId    a valid file ID (the caller has checked it)
     * @param workspace this request's new, empty download workspace
     * @return the manifest and the verified file
     * @throws IllegalArgumentException if {@code fileId} is not a valid file ID
     * @throws FileServiceException with FILE_NOT_FOUND (no manifest), METADATA_CORRUPT (the
     *         manifest cannot be trusted), CHUNK_MISSING (every node confirms a chunk is gone),
     *         INTEGRITY_CHECK_FAILED (the rebuilt file's size or SHA-256 differs from the
     *         manifest), or STORAGE_ERROR (anything else that stopped the file from being read)
     */
    Download download(String fileId, Path workspace) throws FileServiceException {
        Objects.requireNonNull(workspace, "workspace must not be null");
        if (!FileIds.isValid(fileId)) {
            throw new IllegalArgumentException("fileId is not a valid file ID");
        }
        FileManifest manifest;
        List<Path> copies = new ArrayList<>();
        Path copiesDir = workspace.resolve("chunks");
        synchronized (storageLock) {
            manifest = readManifestForDownload(fileId);
            // Nodes that failed for an earlier chunk of this download are tried last for the
            // others, so a node that is down costs its time limit at most once per download.
            Set<Integer> failedNodes = new HashSet<>();
            for (int i = 0; i < manifest.chunkCount(); i++) {
                String chunkId = manifest.chunkId(i);
                Path copy = copiesDir.resolve(chunkId);
                // Format 2 manifests record each chunk's SHA-256: each copy is checked as it
                // arrives, and a damaged one is replaced by another node's copy. Format 1 files
                // are checked as a whole below.
                String expected = manifest.hasChunkChecksums() ? manifest.chunkSha256s().get(i) : null;
                try {
                    StorageCluster.ReadResult read = cluster.retrieveVerifiedChunk(chunkId, copy, expected, failedNodes);
                    failedNodes.addAll(read.failedNodes());
                    for (String problem : read.problems()) {
                        err.println("Warning: download of file " + fileId + ", chunk " + chunkId + ": " + problem
                                + "; the copy on node " + read.node() + " was used");
                    }
                } catch (ChunkNotFoundException e) {
                    throw new FileServiceException(FileServiceException.Reason.CHUNK_MISSING,
                            "file " + fileId + " is committed, but every node confirms its chunk " + chunkId
                                    + " is gone", e);
                } catch (StorageCluster.CorruptCopiesException e) {
                    throw new FileServiceException(FileServiceException.Reason.INTEGRITY_CHECK_FAILED,
                            "chunk " + i + " (" + chunkId + ") of file " + fileId + " has no copy with the SHA-256 its"
                                    + " manifest records: " + e.getMessage(), e);
                } catch (IOException | RuntimeException e) {
                    throw storageError("chunk " + chunkId + " of file " + fileId + " could not be read", e);
                }
                copies.add(copy);
            }
        }

        // The copies are this request's own; nothing below needs the lock.
        Path file = workspace.resolve("file");
        long size;
        String sha256;
        try {
            new FileReconstructor().reconstruct(copies, file);
            for (Path copy : copies) {
                try {
                    Files.deleteIfExists(copy); // only to save disk space while the file is sent
                } catch (IOException ignored) {
                    // the workspace deletion after the request removes it
                }
            }
            size = Files.size(file);
            sha256 = new ChecksumService().calculateSha256(file);
        } catch (IOException | RuntimeException e) {
            throw storageError("file " + fileId + " could not be rebuilt in its download workspace", e);
        }
        if (size != manifest.size() || !sha256.equals(manifest.sha256())) {
            throw new FileServiceException(FileServiceException.Reason.INTEGRITY_CHECK_FAILED,
                    "file " + fileId + " was rebuilt with " + size + " bytes and SHA-256 " + sha256
                            + ", but its manifest says " + manifest.size() + " bytes and " + manifest.sha256(), null);
        }
        return new Download(manifest, file);
    }

    /** The manifest of a file to download. Must be called holding the storage lock. */
    private FileManifest readManifestForDownload(String fileId) throws FileServiceException {
        Optional<FileManifest> manifest;
        try {
            manifest = manifests.read(fileId);
        } catch (InvalidManifestException e) {
            throw new FileServiceException(FileServiceException.Reason.METADATA_CORRUPT,
                    "the manifest of file " + fileId + " cannot be trusted", e);
        } catch (IOException | RuntimeException e) {
            throw storageError("the manifest of file " + fileId + " could not be read", e);
        }
        if (manifest.isEmpty()) {
            throw new FileServiceException(FileServiceException.Reason.FILE_NOT_FOUND,
                    "no manifest exists for file " + fileId, null);
        }
        return manifest.get();
    }

    /** Steps 3 and 4 of the class comment. Must be called holding the storage lock. */
    private FileManifest storeLocked(List<Path> chunks, long size, String sha256, List<String> chunkSha256s, String name)
            throws FileServiceException {
        String fileId = unusedFileId();
        FileManifest manifest = new FileManifest(fileId, size, sha256, chunkSize, chunks.size(), chunkSha256s, name);
        try {
            manifests.createPending(new ManifestStore.PendingUpload(fileId, chunks.size()));
        } catch (IOException | RuntimeException e) {
            throw storageError("could not record upload " + fileId + " as pending; nothing was stored", e);
        }

        List<String> storedChunkIds = new ArrayList<>();
        String storing = null;
        int copies = 0;
        try {
            for (int i = 0; i < chunks.size(); i++) {
                storing = FileIds.chunkId(fileId, i);
                StorageCluster.StoreResult stored = cluster.storeReplicated(storing, chunks.get(i));
                storedChunkIds.add(storing); // recorded only once every copy is confirmed
                copies += stored.copies().size();
                if (!stored.skipped().isEmpty()) {
                    List<String> skipped = new ArrayList<>();
                    for (int node : stored.skipped()) {
                        skipped.add(cluster.describeNode(node));
                    }
                    err.println("Warning: upload " + fileId + ", chunk " + storing + ": stored on nodes " + stored.copies()
                            + "; skipped unreachable " + String.join(", ", skipped));
                }
            }
        } catch (IOException | RuntimeException e) {
            if (e instanceof AmbiguousStoreException || e instanceof IncompleteReplicationException) {
                // A node may have stored this chunk without saying so, or some copies were stored
                // but too few. Roll it back as "maybe stored": deleting removes every copy, a
                // chunk that every node confirms absent is a no-op, and if a node cannot answer,
                // the rollback is incomplete and the marker stays for recovery.
                storedChunkIds.add(storing);
            }
            FileServiceException failure = storageError("upload " + fileId + " could not be stored", e);
            rollBack(fileId, storedChunkIds, failure);
            throw failure;
        }
        try {
            manifests.create(manifest); // the commit point
        } catch (IOException | RuntimeException e) {
            resolveFailedCommit(manifest, storedChunkIds, e);
        }

        // Committed: the file exists. Nothing after this point may undo it.
        try {
            manifests.deletePending(fileId);
        } catch (IOException | RuntimeException e) {
            err.println("Warning: upload " + fileId + " is stored, but its pending-upload marker could not be"
                    + " deleted and was left in place: " + e);
        }
        UploadListener listener = uploadListener;
        if (listener != null) {
            listener.committed(manifest, copies);
        }
        return manifest;
    }

    /**
     * Decides what a failed manifest write means before anything is undone. ManifestStore.create
     * normally fails before the manifest appears, but a failure reported after its rename took
     * effect would leave a committed file, and rolling back would delete that file's chunks. So
     * the manifest is read back first:
     * <ul>
     *   <li>absent: the upload did not commit, and it is rolled back;</li>
     *   <li>present and identical: the upload did commit; this returns normally and the caller
     *       finishes it like any other;</li>
     *   <li>unreadable, or a different manifest: it is unknown whether the upload committed, so
     *       nothing is deleted. The chunks and the pending marker stay for startup recovery.</li>
     * </ul>
     *
     * @throws FileServiceException in the first and third cases
     */
    private void resolveFailedCommit(FileManifest manifest, List<String> storedChunkIds, Exception createFailure)
            throws FileServiceException {
        String fileId = manifest.fileId();
        FileServiceException failure = storageError("the manifest of upload " + fileId + " could not be written",
                createFailure);
        Optional<FileManifest> written;
        try {
            written = manifests.read(fileId);
        } catch (IOException | RuntimeException e) {
            failure.addSuppressed(e);
            err.println("Warning: writing the manifest of upload " + fileId + " failed and whether it was written"
                    + " cannot be checked; nothing was rolled back, and its chunks and pending-upload marker were kept");
            throw failure;
        }
        if (written.isEmpty()) {
            rollBack(fileId, storedChunkIds, failure);
            throw failure;
        }
        if (!written.get().equals(manifest)) {
            failure.addSuppressed(new IOException("a different manifest is stored under file ID " + fileId));
            err.println("Warning: writing the manifest of upload " + fileId + " failed and a different manifest"
                    + " is in its place; nothing was rolled back, and its chunks and pending-upload marker were kept");
            throw failure;
        }
        err.println("Warning: writing the manifest of upload " + fileId + " reported a failure, but the manifest is"
                + " in place, so the upload is committed: " + createFailure);
    }

    /**
     * Deletes exactly the chunks this upload recorded as stored, newest first, including a chunk
     * whose store had an unknown outcome ({@link AmbiguousStoreException}), which may or may not be
     * on a node. The marker is deleted only if every chunk delete succeeded (a chunk every node
     * confirms absent counts as done); otherwise it is kept, so startup recovery can finish the job.
     * Every failure is attached to {@code failure} as suppressed.
     */
    private void rollBack(String fileId, List<String> storedChunkIds, FileServiceException failure) {
        boolean complete = true;
        for (int i = storedChunkIds.size() - 1; i >= 0; i--) {
            try {
                cluster.deleteChunk(storedChunkIds.get(i)); // false (already absent) is fine
            } catch (IOException | RuntimeException e) {
                complete = false;
                failure.addSuppressed(e);
            }
        }
        if (!complete) {
            err.println("Warning: the rollback of upload " + fileId + " was incomplete; its pending-upload marker"
                    + " was kept so the leftover chunks stay on record");
            return;
        }
        try {
            manifests.deletePending(fileId);
        } catch (IOException | RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    /** A new file ID that no manifest or pending marker uses yet. Must be called holding the storage lock. */
    private String unusedFileId() throws FileServiceException {
        for (int attempt = 0; attempt < ID_ATTEMPTS; attempt++) {
            String candidate = ids.next();
            if (!FileIds.isValid(candidate)) {
                throw storageError("the ID generator produced an invalid file ID", null);
            }
            try {
                if (!manifests.isIdTaken(candidate)) {
                    return candidate;
                }
            } catch (IOException e) {
                throw storageError("could not check whether a file ID is in use", e);
            }
        }
        throw storageError("no unused file ID after " + ID_ATTEMPTS + " attempts", null);
    }

    private static FileServiceException storageError(String message, Throwable cause) {
        return new FileServiceException(FileServiceException.Reason.STORAGE_ERROR, message, cause);
    }
}
