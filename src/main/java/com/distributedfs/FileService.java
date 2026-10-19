package com.distributedfs;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Stores files: hashing, chunking, placing the chunks on the cluster, and recording the file's
 * manifest. It knows nothing about HTTP.
 *
 * <p><b>One storage lock.</b> StorageCluster is not thread-safe, and an upload is several steps
 * (pending marker, chunks, manifest) that must not interleave with another upload's. Every step
 * that touches the cluster or the manifests runs while holding one lock. Hashing and chunking,
 * which only touch the request's own workspace, run outside it.
 *
 * <p><b>Store, step by step</b> ({@link #store}):
 * <ol>
 *   <li>Hash the spooled body; if the client sent a checksum and it differs, stop. Nothing has
 *       been stored.</li>
 *   <li>Split the body into chunks inside the workspace.</li>
 *   <li>Holding the lock: pick an unused file ID, write its pending-upload marker, store the
 *       chunks {@code <fileId>_0, _1, ...}, and write the manifest. <b>Writing the manifest is
 *       the commit point</b>: before it, the file does not exist; after it, it does.</li>
 *   <li>Delete the pending marker. If that fails, the upload has still succeeded; the marker is
 *       left in place and a warning is logged.</li>
 * </ol>
 * If storing a chunk fails, or writing the manifest fails and the manifest is confirmed absent,
 * the upload is rolled back: exactly the chunks this upload recorded as stored are deleted, newest
 * first. If every delete succeeds, the marker is deleted too. If any delete fails, the marker is
 * kept (so the leftover chunks stay on record for startup recovery) and the delete failures are
 * attached to the thrown exception as suppressed. A failed manifest write is never rolled back on
 * assumption: see {@link #resolveFailedCommit}.
 */
class FileService {

    /** How many file IDs to try before giving up; a collision of random 128-bit IDs is not expected. */
    static final int ID_ATTEMPTS = 3;

    private static final String WORKSPACE_PREFIX = "upload-";

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

    /** The largest upload accepted, in bytes. */
    long maxUploadBytes() {
        return maxUploadBytes;
    }

    /** Creates a new, empty workspace for one request: a folder named upload-(digits) in the tmp folder. */
    Path newUploadWorkspace() throws IOException {
        return Files.createTempDirectory(tmpDir, WORKSPACE_PREFIX);
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
        Objects.requireNonNull(body, "body must not be null");
        Objects.requireNonNull(workspace, "workspace must not be null");

        String sha256 = new ChecksumService().calculateSha256(body);
        long size = Files.size(body);
        if (expectedSha256 != null && !sha256.equals(expectedSha256.toLowerCase(Locale.ROOT))) {
            throw new FileServiceException(FileServiceException.Reason.CHECKSUM_MISMATCH,
                    "the uploaded data has SHA-256 " + sha256 + ", not the expected " + expectedSha256, null);
        }

        List<Path> chunks = new FileChunker(chunkSize).chunk(body, workspace.resolve("chunks"));
        try {
            Files.deleteIfExists(body); // only to save disk space; the workspace is deleted anyway
        } catch (IOException ignored) {
            // the workspace deletion after the request removes it
        }
        synchronized (storageLock) {
            return storeLocked(chunks, size, sha256);
        }
    }

    /** Steps 3 and 4 of the class comment. Must be called holding the storage lock. */
    private FileManifest storeLocked(List<Path> chunks, long size, String sha256) throws FileServiceException {
        String fileId = unusedFileId();
        FileManifest manifest = new FileManifest(fileId, size, sha256, chunkSize, chunks.size());
        try {
            manifests.createPending(new ManifestStore.PendingUpload(fileId, chunks.size()));
        } catch (IOException | RuntimeException e) {
            throw storageError("could not record upload " + fileId + " as pending; nothing was stored", e);
        }

        List<String> storedChunkIds = new ArrayList<>();
        try {
            for (int i = 0; i < chunks.size(); i++) {
                String chunkId = FileIds.chunkId(fileId, i);
                cluster.storeChunk(chunkId, chunks.get(i));
                storedChunkIds.add(chunkId); // recorded only once the store has succeeded
            }
        } catch (IOException | RuntimeException e) {
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
     * Deletes exactly the chunks this upload recorded as stored, newest first. The marker is
     * deleted only if every chunk delete succeeded; otherwise it is kept. Every failure is
     * attached to {@code failure} as suppressed.
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
