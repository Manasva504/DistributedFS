package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Plain-Java tests for FileService (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.FileServiceTest}
 *
 * <p>Every test builds a real cluster of three storage nodes and a real manifest store in a fresh
 * temporary folder. Failures are injected through small subclasses: storage nodes that fail to
 * store or delete chosen chunk IDs, and a manifest store whose create, createPending or
 * deletePending can be made to fail. File IDs come from a fixed list where a test needs to know
 * them in advance. The program exits with code 1 if any test fails.
 */
public class FileServiceTest {

    private static final int NODE_COUNT = 3;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.FileServiceTest");
            System.exit(2);
            return;
        }

        // Storing
        runTest("a file is stored as chunks with a manifest and reads back identically", FileServiceTest::storesFile);
        runTest("an empty file is stored with no chunks", FileServiceTest::storesEmptyFile);
        runTest("chunk counts are right at and around chunk boundaries", FileServiceTest::chunkBoundaries);
        runTest("a matching checksum is accepted in either letter case", FileServiceTest::checksumInEitherCase);
        runTest("a checksum mismatch stores nothing", FileServiceTest::checksumMismatchStoresNothing);

        // File IDs
        runTest("an ID used by a manifest or a pending marker is skipped", FileServiceTest::takenIdIsSkipped);
        runTest("running out of ID attempts stores nothing", FileServiceTest::idAttemptsRunOut);
        runTest("an invalid generated ID stores nothing", FileServiceTest::invalidGeneratedId);

        // Failures and rollback
        runTest("a chunk that cannot be stored rolls back the chunks stored before it",
                FileServiceTest::chunkStoreFailureRollsBack);
        runTest("an unexpected runtime failure is rolled back the same way", FileServiceTest::runtimeFailureRollsBack);
        runTest("a manifest that cannot be written rolls back every chunk", FileServiceTest::manifestFailureRollsBack);
        runTest("a manifest written despite a reported failure counts as committed and nothing is rolled back",
                FileServiceTest::manifestWrittenDespiteFailureIsCommitted);
        runTest("a failed manifest write that cannot be checked rolls back nothing and keeps the marker",
                FileServiceTest::uncheckableCommitKeepsEverything);
        runTest("a different manifest in place after a failed write rolls back nothing and keeps the marker",
                FileServiceTest::differentManifestKeepsEverything);
        runTest("a pending marker that cannot be written stores nothing", FileServiceTest::pendingFailureStoresNothing);
        runTest("rollback deletes only this upload's chunks, never one stored by someone else",
                FileServiceTest::rollbackDeletesOnlyOwnChunks);
        runTest("an incomplete rollback keeps the marker and reports every failure as suppressed",
                FileServiceTest::incompleteRollbackKeepsMarker);
        runTest("a marker that cannot be deleted after a complete rollback is reported as suppressed",
                FileServiceTest::rollbackMarkerDeleteFailure);
        runTest("after the manifest is written, a failed marker delete is only a warning and nothing is rolled back",
                FileServiceTest::markerDeleteFailureAfterCommit);

        // Concurrency
        runTest("concurrent uploads never touch the cluster at the same time", FileServiceTest::uploadsAreSerialized);
        runTest("concurrent uploads never pick the same ID", FileServiceTest::concurrentUploadsGetDistinctIds);

        // Downloads
        runTest("a download rebuilds the committed file, verified, inside its workspace only",
                FileServiceTest::downloadRebuildsFile);
        runTest("a download of the ID of an upload in progress waits for the commit and gets the whole file",
                FileServiceTest::downloadWaitsForUploadInProgress);
        runTest("a download of the ID of an upload that fails waits and then finds no file",
                FileServiceTest::downloadDuringFailingUploadFindsNothing);
        runTest("a pending marker without a manifest is not a file; with a manifest the file is served",
                FileServiceTest::pendingMarkersAndDownloads);
        runTest("each download failure has its own reason and changes nothing in the storage",
                FileServiceTest::downloadFailureReasons);
        runTest("an invalid file ID is refused before the storage is touched", FileServiceTest::downloadRejectsInvalidId);

        // Workspaces and settings
        runTest("workspaces are created in the tmp folder and deleted afterwards", FileServiceTest::workspaces);
        runTest("invalid settings are refused", FileServiceTest::invalidSettings);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Storing
    // ---------------------------------------------------------------------

    private static void storesFile(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(1)));
        byte[] data = "0123456789".getBytes(StandardCharsets.US_ASCII);

        FileManifest manifest = store(service, data, null);

        checkEquals(new FileManifest(id(1), 10, sha256(data), 4, 3), manifest, "the manifest");
        checkEquals(manifest, f.manifests.read(id(1)).orElseThrow(), "the stored manifest");
        checkEquals(List.of(), f.manifests.listPendingFileIds(), "pending markers");
        for (int i = 0; i < 3; i++) {
            check(f.cluster.hasChunk(id(1) + "_" + i), "chunk " + i + " is stored");
        }
        check(!f.cluster.hasChunk(id(1) + "_3"), "there is no fourth chunk");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "the chunks read back as the original bytes");
    }

    private static void storesEmptyFile(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileManifest manifest = store(f.service(4, ids(id(1))), new byte[0], null);

        checkEquals(new FileManifest(id(1), 0, sha256(new byte[0]), 4, 0), manifest, "the manifest");
        checkEquals(manifest, f.manifests.read(id(1)).orElseThrow(), "the stored manifest");
        checkEquals(0, f.storedChunkFiles(), "chunk files on the nodes");
        checkEquals(List.of(), f.manifests.listPendingFileIds(), "pending markers");
    }

    private static void chunkBoundaries(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        int[][] sizesAndCounts = {{1, 1}, {3, 1}, {4, 1}, {5, 2}, {8, 2}, {9, 3}};
        String[] fileIds = new String[sizesAndCounts.length];
        for (int i = 0; i < fileIds.length; i++) {
            fileIds[i] = id(100 + i);
        }
        FileService service = f.service(4, ids(fileIds));
        for (int[] sizeAndCount : sizesAndCounts) {
            byte[] data = bytes(sizeAndCount[0], sizeAndCount[0]);
            FileManifest manifest = store(service, data, null);
            checkEquals(sizeAndCount[1], manifest.chunkCount(), sizeAndCount[0] + " bytes: chunk count");
            check(Arrays.equals(data, f.readBack(manifest, tempDir)), sizeAndCount[0] + " bytes: read back");
        }
    }

    private static void checksumInEitherCase(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(1), id(2)));
        byte[] data = bytes(10, 7);
        String sha = sha256(data);
        checkEquals(sha, store(service, data, sha).sha256(), "lowercase checksum");
        checkEquals(sha, store(service, data, sha.toUpperCase(Locale.ROOT)).sha256(),
                "uppercase checksum (the manifest keeps lowercase)");
    }

    private static void checksumMismatchStoresNothing(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(1)));
        byte[] data = bytes(10, 7);
        Path workspace = service.newUploadWorkspace();
        Path body = Files.write(workspace.resolve("body"), data);

        FileServiceException e = expectThrows(FileServiceException.class,
                () -> service.store(body, workspace, sha256(bytes(10, 8))));

        checkEquals(FileServiceException.Reason.CHECKSUM_MISMATCH, e.reason(), "reason");
        check(!Files.exists(workspace.resolve("chunks")), "nothing was chunked");
        checkEquals(0, f.rules.storeCalls.get(), "store calls");
        f.checkNothingStored();
    }

    // ---------------------------------------------------------------------
    // File IDs
    // ---------------------------------------------------------------------

    private static void takenIdIsSkipped(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        // id(1) gets a manifest, id(2) a pending marker; both are then offered again first.
        FileService service = f.service(4, ids(id(1), id(1), id(3), id(2), id(4)));
        store(service, bytes(5, 1), null);
        f.manifests.createPending(new ManifestStore.PendingUpload(id(2), 1));

        checkEquals(id(3), store(service, bytes(5, 2), null).fileId(), "an ID with a manifest is skipped");
        checkEquals(id(4), store(service, bytes(5, 3), null).fileId(), "an ID with a pending marker is skipped");
    }

    private static void idAttemptsRunOut(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, () -> id(1));
        store(service, bytes(5, 1), null);
        int storeCalls = f.rules.storeCalls.get();

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(5, 2), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        checkEquals(storeCalls, f.rules.storeCalls.get(), "no further store calls");
        checkEquals(List.of(), f.manifests.listPendingFileIds(), "pending markers");
        checkEquals(List.of(id(1) + ".manifest"), listNames(f.manifestsDir), "only the first manifest");
    }

    private static void invalidGeneratedId(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, () -> "../not-an-id");
        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(5, 1), null));
        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        f.checkNothingStored();
    }

    // ---------------------------------------------------------------------
    // Failures and rollback
    // ---------------------------------------------------------------------

    private static void chunkStoreFailureRollsBack(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.failStore.add(id(1) + "_2");
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(16, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        check(e.getCause() instanceof IOException, "the cause is the cluster's failure: " + e.getCause());
        checkEquals(0, e.getSuppressed().length, "no rollback failures");
        checkEquals(List.of(id(1) + "_1", id(1) + "_0"), f.rules.deleted, "deleted, newest first");
        f.checkNothingStored();
    }

    private static void runtimeFailureRollsBack(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.failStoreUnexpectedly.add(id(1) + "_1");
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(16, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        check(e.getCause() instanceof IllegalStateException, "the cause is the runtime failure: " + e.getCause());
        checkEquals(List.of(id(1) + "_0"), f.rules.deleted, "deleted");
        f.checkNothingStored();
    }

    private static void manifestFailureRollsBack(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.manifests.failCreate = true;
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(10, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        checkEquals(List.of(id(1) + "_2", id(1) + "_1", id(1) + "_0"), f.rules.deleted, "deleted, newest first");
        f.checkNothingStored();
    }

    private static void manifestWrittenDespiteFailureIsCommitted(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.manifests.failCreateAfterWriting = true;
        FileService service = f.service(4, ids(id(1)));
        byte[] data = bytes(10, 1);

        FileManifest manifest = store(service, data, null);

        checkEquals(id(1), manifest.fileId(), "the upload succeeded");
        checkEquals(manifest, f.manifests.read(id(1)).orElseThrow(), "the manifest is there");
        checkEquals(List.of(), f.rules.deleted, "nothing was rolled back");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "every chunk is still there");
        check(f.manifests.readPending(id(1)).isEmpty(), "the marker was deleted as after any commit");
        checkContains(f.errText(), "the upload is committed");
    }

    private static void uncheckableCommitKeepsEverything(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.manifests.failCreate = true;
        f.manifests.failRead = true;
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(10, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        checkContains(e.getCause().getMessage(), "simulated create failure");
        checkEquals(1, e.getSuppressed().length, "the read failure is attached");
        checkContains(e.getSuppressed()[0].getMessage(), "simulated read failure");
        checkEquals(List.of(), f.rules.deleted, "nothing was rolled back");
        checkEquals(3, f.storedChunkFiles(), "chunk files on the nodes");
        f.manifests.failRead = false;
        checkEquals(new ManifestStore.PendingUpload(id(1), 3), f.manifests.readPending(id(1)).orElseThrow(),
                "the marker is kept for recovery");
        checkContains(f.errText(), "nothing was rolled back");
    }

    private static void differentManifestKeepsEverything(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileManifest other = new FileManifest(id(1), 1, "0".repeat(64), 4, 1);
        f.manifests.createInstead = other;
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(10, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        checkEquals(1, e.getSuppressed().length, "the mismatch is attached");
        checkContains(e.getSuppressed()[0].getMessage(), "different manifest");
        checkEquals(List.of(), f.rules.deleted, "nothing was rolled back");
        checkEquals(3, f.storedChunkFiles(), "chunk files on the nodes");
        checkEquals(other, f.manifests.read(id(1)).orElseThrow(), "the other manifest is untouched");
        check(f.manifests.readPending(id(1)).isPresent(), "the marker is kept for recovery");
    }

    private static void pendingFailureStoresNothing(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.manifests.failCreatePending = true;
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(10, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        checkEquals(0, f.rules.storeCalls.get(), "store calls");
        checkEquals(List.of(), f.rules.deleted, "nothing to delete");
        f.checkNothingStored();
    }

    private static void rollbackDeletesOnlyOwnChunks(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        // Someone else's chunk already uses the ID this upload's third chunk needs.
        Path foreign = Files.writeString(tempDir.resolve("foreign"), "foreign chunk");
        f.cluster.storeChunk(id(1) + "_2", foreign);
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(16, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        check(e.getCause() instanceof FileAlreadyExistsException, "the cause: " + e.getCause());
        checkEquals(List.of(id(1) + "_1", id(1) + "_0"), f.rules.deleted, "only this upload's chunks were deleted");
        Path copy = tempDir.resolve("copy");
        f.cluster.retrieveChunk(id(1) + "_2", copy);
        checkEquals("foreign chunk", Files.readString(copy), "the foreign chunk is untouched");
        checkEquals(1, f.storedChunkFiles(), "chunk files on the nodes");
        checkEquals(List.of(), f.manifests.listPendingFileIds(), "pending markers");
        checkEquals(List.of(), listNames(f.manifestsDir), "manifest folder");
    }

    private static void incompleteRollbackKeepsMarker(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.failStore.add(id(1) + "_3");
        f.rules.failDelete.add(id(1) + "_0");
        f.rules.failDelete.add(id(1) + "_2");
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(16, 1), null));

        checkEquals(FileServiceException.Reason.STORAGE_ERROR, e.reason(), "reason");
        checkEquals(2, e.getSuppressed().length, "both delete failures are attached");
        for (Throwable suppressed : e.getSuppressed()) {
            checkContains(suppressed.getMessage(), "simulated delete failure");
        }
        checkEquals(List.of(id(1) + "_1"), f.rules.deleted, "the delete in between still ran");
        check(f.cluster.hasChunk(id(1) + "_0") && f.cluster.hasChunk(id(1) + "_2"), "the undeletable chunks remain");
        checkEquals(new ManifestStore.PendingUpload(id(1), 4), f.manifests.readPending(id(1)).orElseThrow(),
                "the marker is kept for recovery");
        check(f.manifests.read(id(1)).isEmpty(), "no manifest");
        checkContains(f.errText(), "incomplete");
    }

    private static void rollbackMarkerDeleteFailure(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.failStore.add(id(1) + "_1");
        f.manifests.failDeletePending = true;
        FileService service = f.service(4, ids(id(1)));

        FileServiceException e = expectThrows(FileServiceException.class, () -> store(service, bytes(16, 1), null));

        checkEquals(1, e.getSuppressed().length, "the marker failure is attached");
        checkContains(e.getSuppressed()[0].getMessage(), "simulated deletePending failure");
        checkEquals(List.of(id(1) + "_0"), f.rules.deleted, "the stored chunk was deleted");
        checkEquals(0, f.storedChunkFiles(), "chunk files on the nodes");
        check(f.manifests.readPending(id(1)).isPresent(), "the marker could not be deleted, so it is still there");
    }

    private static void markerDeleteFailureAfterCommit(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.manifests.failDeletePending = true;
        FileService service = f.service(4, ids(id(1)));
        byte[] data = bytes(10, 1);

        FileManifest manifest = store(service, data, null);

        checkEquals(id(1), manifest.fileId(), "the upload succeeded");
        checkEquals(manifest, f.manifests.read(id(1)).orElseThrow(), "the manifest is there");
        checkEquals(List.of(), f.rules.deleted, "nothing was rolled back");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "every chunk is still there");
        check(f.manifests.readPending(id(1)).isPresent(), "the marker was left in place");
        checkContains(f.errText(), "Warning");
    }

    // ---------------------------------------------------------------------
    // Concurrency
    // ---------------------------------------------------------------------

    private static void uploadsAreSerialized(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.storeDelayMillis = 5;
        FileService service = f.service(4, FileIds::newFileId);

        List<byte[]> inputs = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            inputs.add(bytes(12 + i, i));
        }
        List<FileManifest> manifests = storeConcurrently(service, inputs);

        checkEquals(1, f.rules.maxInFlight.get(), "chunk stores running at the same time");
        Set<String> fileIds = new HashSet<>();
        for (int i = 0; i < inputs.size(); i++) {
            fileIds.add(manifests.get(i).fileId());
            check(Arrays.equals(inputs.get(i), f.readBack(manifests.get(i), tempDir)), "upload " + i + " reads back");
        }
        checkEquals(inputs.size(), fileIds.size(), "distinct file IDs");
        checkEquals(List.of(), f.manifests.listPendingFileIds(), "pending markers");
    }

    private static void concurrentUploadsGetDistinctIds(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        // Every ID is offered twice in a row. Only an ID check and marker made under the same lock
        // turns the second offer down; without the lock two uploads would both take it.
        AtomicInteger calls = new AtomicInteger();
        FileService service = f.service(4, () -> id(1000 + calls.getAndIncrement() / 2));

        List<byte[]> inputs = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            inputs.add(bytes(6, i));
        }
        List<FileManifest> manifests = storeConcurrently(service, inputs);

        Set<String> fileIds = new HashSet<>();
        for (int i = 0; i < inputs.size(); i++) {
            fileIds.add(manifests.get(i).fileId());
            check(Arrays.equals(inputs.get(i), f.readBack(manifests.get(i), tempDir)), "upload " + i + " reads back");
        }
        checkEquals(inputs.size(), fileIds.size(), "distinct file IDs");
    }

    /** Stores every input from its own thread, all released at once; returns the manifests in input order. */
    private static List<FileManifest> storeConcurrently(FileService service, List<byte[]> inputs) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(inputs.size());
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<FileManifest>> futures = new ArrayList<>();
            for (byte[] input : inputs) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return store(service, input, null);
                }));
            }
            start.countDown();
            List<FileManifest> manifests = new ArrayList<>();
            for (Future<FileManifest> future : futures) {
                manifests.add(future.get(30, TimeUnit.SECONDS));
            }
            return manifests;
        } finally {
            pool.shutdownNow();
            check(pool.awaitTermination(10, TimeUnit.SECONDS), "the upload threads finished");
        }
    }

    // ---------------------------------------------------------------------
    // Workspaces and settings
    // ---------------------------------------------------------------------

    private static void workspaces(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(1)));

        Path workspace = service.newUploadWorkspace();
        checkEquals(f.tmpDir, workspace.getParent(), "the workspace's folder");
        check(workspace.getFileName().toString().startsWith("upload-"), "the workspace's name: " + workspace);
        Files.writeString(workspace.resolve("body"), "x");
        Files.createDirectories(workspace.resolve("chunks"));
        Files.writeString(workspace.resolve("chunks").resolve("chunk_0"), "x");

        service.deleteWorkspace(workspace);
        checkEquals(List.of(), listNames(f.tmpDir), "the tmp folder");
        checkEquals("", f.errText(), "no warnings");

        service.deleteWorkspace(workspace); // already gone: logged, never thrown
        checkContains(f.errText(), "Warning: could not delete the request workspace");
    }

    private static void invalidSettings(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        expectThrows(IllegalArgumentException.class,
                () -> new FileService(f.cluster, f.manifests, f.tmpDir, 0, 10, f.err));
        expectThrows(IllegalArgumentException.class,
                () -> new FileService(f.cluster, f.manifests, f.tmpDir, 4, 0, f.err));
        expectThrows(NullPointerException.class,
                () -> new FileService(null, f.manifests, f.tmpDir, 4, 10, f.err));
        checkEquals(10L, new FileService(f.cluster, f.manifests, f.tmpDir, 4, 10, f.err).maxUploadBytes(),
                "maxUploadBytes");
    }

    // ---------------------------------------------------------------------
    // Downloads
    // ---------------------------------------------------------------------

    private static void downloadRebuildsFile(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(1), id(2)));
        byte[] data = bytes(10, 5);
        FileManifest manifest = store(service, data, null);
        FileManifest empty = store(service, new byte[0], null);
        Map<String, String> before = f.snapshot();

        Path workspace = service.newDownloadWorkspace();
        checkEquals(f.tmpDir, workspace.getParent(), "the workspace's folder");
        check(workspace.getFileName().toString().matches("download-[0-9]{1,20}"), "workspace name " + workspace);
        try {
            FileService.Download download = service.download(id(1), workspace);
            checkEquals(manifest, download.manifest(), "the manifest");
            checkEquals(workspace, download.file().getParent(), "the file is in the workspace");
            check(Arrays.equals(data, Files.readAllBytes(download.file())), "the rebuilt bytes");
            checkEquals(List.of("chunks", "file"), listNames(workspace), "the workspace");
            checkEquals(List.of(), listNames(workspace.resolve("chunks")), "chunk copies left after rebuilding");
        } finally {
            service.deleteWorkspace(workspace);
        }
        check(Arrays.equals(new byte[0], downloadBytes(service, empty.fileId())), "the empty file");
        checkEquals(List.of(), listNames(f.tmpDir), "tmp after the downloads");
        checkEquals(before, f.snapshot(), "the storage after the downloads");
    }

    private static void downloadWaitsForUploadInProgress(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.gateChunk = id(1) + "_1";
        FileService service = f.service(4, ids(id(1)));
        byte[] data = bytes(10, 3);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<FileManifest> upload = pool.submit(() -> store(service, data, null));
            check(f.rules.gateEntered.await(10, TimeUnit.SECONDS), "the upload reached its second chunk");
            // The upload holds the storage lock: its marker and first chunk exist, its manifest does not.
            check(f.manifests.readPending(id(1)).isPresent(), "the pending marker of the upload in progress");
            check(f.manifests.read(id(1)).isEmpty(), "no manifest yet");

            Future<byte[]> download = pool.submit(() -> downloadBytes(service, id(1)));
            Thread.sleep(300);
            check(!download.isDone(), "the download waits while the upload holds the storage lock");

            f.rules.gateRelease.countDown();
            checkEquals(id(1), upload.get(10, TimeUnit.SECONDS).fileId(), "the upload's file ID");
            check(Arrays.equals(data, download.get(10, TimeUnit.SECONDS)), "the download got the whole committed file");
        } finally {
            f.rules.gateRelease.countDown();
            pool.shutdownNow();
        }
        checkEquals(List.of(), f.manifests.listPendingFileIds(), "pending markers");
        checkEquals(List.of(), listNames(f.tmpDir), "tmp");
    }

    private static void downloadDuringFailingUploadFindsNothing(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.rules.gateChunk = id(1) + "_1";
        f.rules.failStore.add(id(1) + "_2");
        FileService service = f.service(4, ids(id(1)));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<FileManifest> upload = pool.submit(() -> store(service, bytes(10, 3), null));
            check(f.rules.gateEntered.await(10, TimeUnit.SECONDS), "the upload reached its second chunk");
            Future<byte[]> download = pool.submit(() -> downloadBytes(service, id(1)));
            Thread.sleep(300);
            check(!download.isDone(), "the download waits while the upload holds the storage lock");

            f.rules.gateRelease.countDown();
            ExecutionException uploadFailure = expectThrows(ExecutionException.class, () -> upload.get(10, TimeUnit.SECONDS));
            check(uploadFailure.getCause() instanceof FileServiceException,
                    "the upload failed with a FileServiceException: " + uploadFailure.getCause());
            ExecutionException downloadFailure =
                    expectThrows(ExecutionException.class, () -> download.get(10, TimeUnit.SECONDS));
            check(downloadFailure.getCause() instanceof FileServiceException e
                            && e.reason() == FileServiceException.Reason.FILE_NOT_FOUND,
                    "the download found no file after the rollback: " + downloadFailure.getCause());
        } finally {
            f.rules.gateRelease.countDown();
            pool.shutdownNow();
        }
        f.checkNothingStored();
        checkEquals(List.of(), listNames(f.tmpDir), "tmp");
    }

    private static void pendingMarkersAndDownloads(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(2)));

        // An upload that never committed: a marker and one stored chunk, but no manifest.
        f.manifests.createPending(new ManifestStore.PendingUpload(id(1), 2));
        f.cluster.storeChunk(id(1) + "_0", Files.write(tempDir.resolve("source"), bytes(4, 1)));
        Map<String, String> before = f.snapshot();
        checkDownloadFails(service, id(1), FileServiceException.Reason.FILE_NOT_FOUND);
        checkEquals(before, f.snapshot(), "the marker and the chunk of the uncommitted upload");

        // A committed file whose marker could not be deleted is still a file.
        byte[] data = bytes(9, 2);
        FileManifest manifest = store(service, data, null);
        f.manifests.createPending(new ManifestStore.PendingUpload(id(2), manifest.chunkCount()));
        check(Arrays.equals(data, downloadBytes(service, id(2))), "the committed file with a leftover marker");
        check(f.manifests.readPending(id(2)).isPresent(), "the download leaves the marker alone");
        checkEquals(List.of(), listNames(f.tmpDir), "tmp");
    }

    private static void downloadFailureReasons(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids(id(1), id(2), id(3), id(4), id(5)));
        for (int i = 1; i <= 5; i++) {
            store(service, bytes(10, i), null); // chunks _0 and _1 of 4 bytes, _2 of 2 bytes
        }
        Files.writeString(f.manifestsDir.resolve(id(1) + ".manifest"), "format=1\nnot a manifest\n");
        Files.delete(f.chunkFile(id(2) + "_1"));
        Path flipped = f.chunkFile(id(3) + "_2");
        byte[] content = Files.readAllBytes(flipped);
        content[0] ^= 1;
        Files.write(flipped, content);
        Files.write(f.chunkFile(id(4) + "_2"), new byte[1]); // one byte short
        Map<String, String> before = f.snapshot();

        checkDownloadFails(service, id(1), FileServiceException.Reason.METADATA_CORRUPT);
        checkDownloadFails(service, id(2), FileServiceException.Reason.CHUNK_MISSING);
        checkDownloadFails(service, id(3), FileServiceException.Reason.INTEGRITY_CHECK_FAILED);
        checkDownloadFails(service, id(4), FileServiceException.Reason.INTEGRITY_CHECK_FAILED);
        checkDownloadFails(service, id(9), FileServiceException.Reason.FILE_NOT_FOUND);
        f.manifests.failRead = true;
        checkDownloadFails(service, id(5), FileServiceException.Reason.STORAGE_ERROR);
        f.manifests.failRead = false;
        check(Arrays.equals(bytes(10, 5), downloadBytes(service, id(5))), "the undamaged file is still served");

        checkEquals(before, f.snapshot(), "the storage after the failed downloads");
        checkEquals(List.of(), listNames(f.tmpDir), "tmp");
    }

    private static void downloadRejectsInvalidId(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        FileService service = f.service(4, ids());
        Path workspace = service.newDownloadWorkspace();
        try {
            List<String> invalid = Arrays.asList(null, "", "../" + id(1), id(1).toUpperCase(Locale.ROOT).replace('0', 'A'),
                    id(1) + "0", "%30" + id(1).substring(1), id(1).substring(1) + "/");
            for (String bad : invalid) {
                expectThrows(IllegalArgumentException.class, () -> service.download(bad, workspace));
            }
            checkEquals(List.of(), listNames(workspace), "nothing was written to the workspace");
        } finally {
            service.deleteWorkspace(workspace);
        }
        checkEquals(List.of(), listNames(f.tmpDir), "tmp");
    }

    /** Downloads the way the HTTP handler does: in a new workspace, deleted afterwards. */
    private static byte[] downloadBytes(FileService service, String fileId) throws IOException {
        Path workspace = service.newDownloadWorkspace();
        try {
            return Files.readAllBytes(service.download(fileId, workspace).file());
        } finally {
            service.deleteWorkspace(workspace);
        }
    }

    private static void checkDownloadFails(FileService service, String fileId, FileServiceException.Reason expected) {
        FileServiceException e = expectThrows(FileServiceException.class, () -> downloadBytes(service, fileId));
        checkEquals(expected, e.reason(), "the reason a download of " + fileId + " failed");
    }

    // ---------------------------------------------------------------------
    // Fixture
    // ---------------------------------------------------------------------

    /** What the test nodes should do, shared by all nodes of one fixture. */
    private static final class NodeRules {
        final Set<String> failStore = ConcurrentHashMap.newKeySet();
        final Set<String> failStoreUnexpectedly = ConcurrentHashMap.newKeySet();
        final Set<String> failDelete = ConcurrentHashMap.newKeySet();
        final List<String> deleted = new CopyOnWriteArrayList<>();
        final AtomicInteger storeCalls = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        volatile long storeDelayMillis = 0;
        /** A store of this chunk ID signals gateEntered, then waits for gateRelease. */
        volatile String gateChunk;
        final CountDownLatch gateEntered = new CountDownLatch(1);
        final CountDownLatch gateRelease = new CountDownLatch(1);
    }

    /** A real storage node that follows NodeRules. */
    private static final class TestNode extends StorageNode {
        private final NodeRules rules;

        TestNode(Path dir, NodeRules rules) throws IOException {
            super(dir);
            this.rules = rules;
        }

        @Override
        public long storeChunk(String chunkId, Path sourceFile) throws IOException {
            rules.storeCalls.incrementAndGet();
            int now = rules.inFlight.incrementAndGet();
            rules.maxInFlight.accumulateAndGet(now, Math::max);
            try {
                if (chunkId.equals(rules.gateChunk)) {
                    rules.gateEntered.countDown();
                    try {
                        if (!rules.gateRelease.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("the test never released the gate");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                }
                if (rules.storeDelayMillis > 0) {
                    try {
                        Thread.sleep(rules.storeDelayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                }
                if (rules.failStoreUnexpectedly.contains(chunkId)) {
                    throw new IllegalStateException("simulated unexpected store failure");
                }
                if (rules.failStore.contains(chunkId)) {
                    throw new IOException("simulated store failure");
                }
                return super.storeChunk(chunkId, sourceFile);
            } finally {
                rules.inFlight.decrementAndGet();
            }
        }

        @Override
        public boolean deleteChunk(String chunkId) throws IOException {
            if (rules.failDelete.contains(chunkId)) {
                throw new IOException("simulated delete failure");
            }
            boolean deletedNow = super.deleteChunk(chunkId);
            rules.deleted.add(chunkId);
            return deletedNow;
        }
    }

    /** A real manifest store whose writes can be made to fail. */
    private static final class TestManifestStore extends ManifestStore {
        volatile boolean failCreate;
        volatile boolean failCreateAfterWriting;
        volatile FileManifest createInstead;
        volatile boolean failRead;
        volatile boolean failCreatePending;
        volatile boolean failDeletePending;

        TestManifestStore(Path dir) throws IOException {
            super(dir);
        }

        @Override
        void create(FileManifest manifest) throws IOException {
            if (failCreate) {
                throw new IOException("simulated create failure");
            }
            if (createInstead != null) {
                super.create(createInstead);
                throw new IOException("simulated create failure after writing a different manifest");
            }
            super.create(manifest);
            if (failCreateAfterWriting) {
                throw new IOException("simulated create failure after the manifest was written");
            }
        }

        @Override
        Optional<FileManifest> read(String fileId) throws IOException {
            if (failRead) {
                throw new IOException("simulated read failure");
            }
            return super.read(fileId);
        }

        @Override
        void createPending(PendingUpload pending) throws IOException {
            if (failCreatePending) {
                throw new IOException("simulated createPending failure");
            }
            super.createPending(pending);
        }

        @Override
        boolean deletePending(String fileId) throws IOException {
            if (failDeletePending) {
                throw new IOException("simulated deletePending failure");
            }
            return super.deletePending(fileId);
        }
    }

    private static final class Fixture {
        final NodeRules rules = new NodeRules();
        final List<Path> nodeDirs = new ArrayList<>();
        final StorageCluster cluster;
        final Path manifestsDir;
        final TestManifestStore manifests;
        final Path tmpDir;
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8);

        Fixture(Path tempDir) throws IOException {
            List<StorageNode> nodes = new ArrayList<>();
            for (int i = 0; i < NODE_COUNT; i++) {
                Path dir = Files.createDirectories(tempDir.resolve("nodes").resolve("node-" + i));
                nodeDirs.add(dir);
                nodes.add(new TestNode(dir, rules));
            }
            cluster = new StorageCluster(nodes);
            manifestsDir = Files.createDirectories(tempDir.resolve("manifests"));
            manifests = new TestManifestStore(manifestsDir);
            tmpDir = Files.createDirectories(tempDir.resolve("tmp"));
        }

        FileService service(int chunkSize, FileService.IdGenerator ids) {
            return new FileService(cluster, manifests, tmpDir, chunkSize, 1024 * 1024, ids, err);
        }

        String errText() {
            return errBytes.toString(StandardCharsets.UTF_8);
        }

        int storedChunkFiles() throws IOException {
            int count = 0;
            for (Path dir : nodeDirs) {
                count += listNames(dir).size();
            }
            return count;
        }

        /** No chunks on any node, no manifest and no pending marker. */
        void checkNothingStored() throws IOException {
            checkEquals(0, storedChunkFiles(), "chunk files on the nodes");
            checkEquals(List.of(), listNames(manifestsDir), "manifest folder");
        }

        /** The stored chunk file with this ID, on whichever node holds it. */
        Path chunkFile(String chunkId) {
            for (Path dir : nodeDirs) {
                if (Files.exists(dir.resolve(chunkId))) {
                    return dir.resolve(chunkId);
                }
            }
            throw new AssertionError("no node holds " + chunkId);
        }

        /** Every manifest, marker and chunk file, by name, with its SHA-256. */
        Map<String, String> snapshot() throws IOException {
            Map<String, String> files = new TreeMap<>();
            List<Path> dirs = new ArrayList<>(nodeDirs);
            dirs.add(manifestsDir);
            for (Path dir : dirs) {
                for (String name : listNames(dir)) {
                    files.put(dir.getFileName() + "/" + name, sha256(Files.readAllBytes(dir.resolve(name))));
                }
            }
            return files;
        }

        /** Retrieves every chunk of {@code manifest} in order and returns the bytes joined. */
        byte[] readBack(FileManifest manifest, Path tempDir) throws IOException {
            Path dir = Files.createTempDirectory(tempDir, "readback-");
            ByteArrayOutputStream all = new ByteArrayOutputStream();
            for (int i = 0; i < manifest.chunkCount(); i++) {
                Path copy = dir.resolve("chunk_" + i);
                cluster.retrieveChunk(FileIds.chunkId(manifest.fileId(), i), copy);
                all.write(Files.readAllBytes(copy));
            }
            return all.toByteArray();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** Stores {@code data} the way the HTTP handler does: in a new workspace, deleted afterwards. */
    private static FileManifest store(FileService service, byte[] data, String expectedSha256) throws IOException {
        Path workspace = service.newUploadWorkspace();
        try {
            Path body = Files.write(workspace.resolve("body"), data);
            return service.store(body, workspace, expectedSha256);
        } finally {
            service.deleteWorkspace(workspace);
        }
    }

    /** A valid file ID made from a number: 32 hexadecimal digits. */
    private static String id(int n) {
        return String.format(Locale.ROOT, "%032x", n);
    }

    /** Hands out the given IDs in order, then fails the test. */
    private static FileService.IdGenerator ids(String... values) {
        AtomicInteger next = new AtomicInteger();
        return () -> {
            int i = next.getAndIncrement();
            if (i >= values.length) {
                throw new AssertionError("the test ran out of file IDs");
            }
            return values[i];
        };
    }

    private static byte[] bytes(int size, int seed) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i * 31 + seed * 7 + 1);
        }
        return data;
    }

    private static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    /** A test body that receives a fresh temporary directory. */
    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    /** A piece of code that is expected to throw. */
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("fileservice-test-");
            body.run(tempDir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (AssertionError e) {
            failed++;
            System.out.println("FAIL  " + name);
            System.out.println("      " + e.getMessage());
        } catch (Throwable e) {
            failed++;
            System.out.println("FAIL  " + name + " (unexpected exception)");
            e.printStackTrace(System.out);
        } finally {
            if (tempDir != null) {
                try {
                    deleteRecursively(tempDir);
                } catch (IOException e) {
                    System.out.println("      warning: could not delete " + tempDir + ": " + e);
                }
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkEquals(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void checkContains(String text, String expectedPart) {
        if (text == null || !text.contains(expectedPart)) {
            throw new AssertionError("expected text containing \"" + expectedPart + "\" but was \"" + text + "\"");
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingAction action) {
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError("expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError("expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }

    private static void deleteRecursively(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) {
                    throw error;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
