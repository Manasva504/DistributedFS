package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Plain-Java tests of deleting a file in {@link FileService#delete} and of deletion markers in
 * {@link ManifestStore} (PR #12): the steps in order (marker, manifest as the commit point, every
 * copy, marker), what each failure leaves behind, the storage lock, finishing an earlier delete,
 * the listener, and that a deleting file's ID is never given to a new upload.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.FileDeleteTest}.
 */
public class FileDeleteTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("filedelete-test-");

    private static final int CHUNK = 1024;

    public static void main(String[] args) {
        RUNNER.run("a deletion marker is written, read strictly, listed and deleted like a pending marker",
                FileDeleteTest::markers);
        RUNNER.run("a deletion marker makes its file ID taken, and the temp sweep leaves it alone", FileDeleteTest::markerKeepsId);
        RUNNER.run("a delete removes the manifest, every copy of every chunk, then the marker", FileDeleteTest::happyPath);
        RUNNER.run("a file that does not exist, or an upload in progress, is FILE_NOT_FOUND and changes nothing",
                FileDeleteTest::notFound);
        RUNNER.run("if the marker cannot be written, nothing is deleted", FileDeleteTest::markerWriteFails);
        RUNNER.run("if the manifest cannot be removed, the delete is undone: the marker goes, the file stays",
                FileDeleteTest::manifestRemovalFails);
        RUNNER.run("if removing the manifest reports a failure but it is gone, the delete is committed and finishes",
                FileDeleteTest::manifestRemovalReportedButDone);
        RUNNER.run("with a node down, the delete commits, keeps its marker, and a later call finishes it",
                FileDeleteTest::nodeDownKeepsMarker);
        RUNNER.run("if the marker cannot be removed at the end, the delete is not finished, and a later call finishes it",
                FileDeleteTest::markerRemovalFails);
        RUNNER.run("a leftover pending marker of the deleted file is removed with the manifest", FileDeleteTest::leftoverPending);
        RUNNER.run("a new upload never gets the ID of a file whose delete is not finished", FileDeleteTest::idNotReused);
        RUNNER.run("a delete waits for the storage lock", FileDeleteTest::takesTheLock);
        RUNNER.run("the listener hears of each delete once, after its commit point, with its outcome",
                FileDeleteTest::listener);
        RUNNER.finish();
    }

    // ---------------------------------------------------------------------
    // Deletion markers
    // ---------------------------------------------------------------------

    private static void markers(Path tempDir) throws Exception {
        ManifestStore store = new ManifestStore(Files.createDirectories(tempDir.resolve("manifests")));
        String id = FileIds.newFileId();
        store.createDeletion(new ManifestStore.PendingUpload(id, 3));
        checkEquals("format=1\nfileId=" + id + "\nchunkCount=3\n",
                Files.readString(tempDir.resolve("manifests").resolve(id + ".deleting"), StandardCharsets.US_ASCII), "text");
        checkEquals(3, store.readDeletion(id).orElseThrow().chunkCount(), "chunk count");
        checkEquals(List.of(id), store.listDeletionFileIds(), "listed");
        checkEquals(List.of(), store.listPendingFileIds(), "not a pending marker");
        try {
            store.createDeletion(new ManifestStore.PendingUpload(id, 3));
            throw new AssertionError("a second marker replaced the first");
        } catch (java.nio.file.FileAlreadyExistsException expected) {
            // never replaced
        }
        String bad = FileIds.newFileId();
        Files.writeString(tempDir.resolve("manifests").resolve(bad + ".deleting"), "format=1\nfileId=" + bad + "\nchunkCount=03\n");
        try {
            store.readDeletion(bad);
            throw new AssertionError("a marker that is not canonical was read");
        } catch (InvalidManifestException expected) {
            // read strictly
        }
        check(store.deleteDeletion(id), "deleted");
        check(store.readDeletion(id).isEmpty(), "gone");
        check(!store.deleteDeletion(id), "deleted twice");
    }

    private static void markerKeepsId(Path tempDir) throws Exception {
        ManifestStore store = new ManifestStore(Files.createDirectories(tempDir.resolve("manifests")));
        String id = FileIds.newFileId();
        check(!store.isIdTaken(id), "taken before");
        store.createDeletion(new ManifestStore.PendingUpload(id, 1));
        check(store.isIdTaken(id), "a deletion marker does not take its ID");
        checkEquals(0, store.sweepStaleTempFiles(warning -> { }), "swept");
        check(store.readDeletion(id).isPresent(), "the sweep removed the marker");
    }

    // ---------------------------------------------------------------------
    // FileService.delete
    // ---------------------------------------------------------------------

    private static void happyPath(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String keep = f.store(randomBytes(3 * CHUNK, 1));
        String id = f.store(randomBytes(3 * CHUNK + 5, 2));
        FileService.DeleteResult result = f.files.delete(id);
        checkEquals(new FileService.DeleteResult(id, null, 4, true), result, "result");
        check(f.manifests.read(id).isEmpty(), "the manifest is still there");
        check(f.manifests.readDeletion(id).isEmpty(), "the marker is still there");
        checkEquals(0, f.copiesOf(id), "copies left");
        checkEquals(6, f.copiesOf(keep), "copies of the other file");
        check(f.manifests.read(keep).isPresent(), "the other file");
    }

    private static void notFound(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String keep = f.store(randomBytes(CHUNK, 3));
        expectReason(FileServiceException.Reason.FILE_NOT_FOUND, () -> f.files.delete(FileIds.newFileId()));
        String pending = FileIds.newFileId();
        f.manifests.createPending(new ManifestStore.PendingUpload(pending, 1));
        expectReason(FileServiceException.Reason.FILE_NOT_FOUND, () -> f.files.delete(pending));
        check(f.manifests.readPending(pending).isPresent(), "the upload's marker was touched");
        checkEquals(List.of(), f.manifests.listDeletionFileIds(), "deletion markers");
        checkEquals(2, f.copiesOf(keep), "the other file's copies");
    }

    private static void markerWriteFails(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(2 * CHUNK, 4));
        f.manifests.failMove = ManifestStore.DELETING_SUFFIX;
        expectReason(FileServiceException.Reason.STORAGE_ERROR, () -> f.files.delete(id));
        check(f.manifests.read(id).isPresent(), "the manifest is gone");
        checkEquals(List.of(), f.manifests.listDeletionFileIds(), "deletion markers");
        checkEquals(4, f.copiesOf(id), "copies");
        checkEquals(0, f.deletes.size(), "listener calls");
    }

    private static void manifestRemovalFails(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(2 * CHUNK, 5));
        f.manifests.failDelete = ManifestStore.MANIFEST_SUFFIX;
        expectReason(FileServiceException.Reason.STORAGE_ERROR, () -> f.files.delete(id));
        check(f.manifests.read(id).isPresent(), "the manifest is gone");
        checkEquals(List.of(), f.manifests.listDeletionFileIds(), "the marker was kept");
        checkEquals(4, f.copiesOf(id), "copies");
        checkEquals(0, f.deletes.size(), "listener calls");
    }

    private static void manifestRemovalReportedButDone(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(2 * CHUNK, 6));
        f.manifests.failAfterDelete = ManifestStore.MANIFEST_SUFFIX;
        FileService.DeleteResult result = f.files.delete(id);
        check(result.finished(), "not finished");
        check(f.manifests.read(id).isEmpty(), "the manifest is there");
        checkEquals(0, f.copiesOf(id), "copies");
        NodeTestKit.checkContains(f.err.text(), "reported a failure, but the manifest is gone, so the delete is committed");
    }

    private static void nodeDownKeepsMarker(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(3 * CHUNK, 7));
        f.nodes.get(1).down = true;
        FileService.DeleteResult first = f.files.delete(id);
        check(!first.finished(), "finished with a node down");
        check(f.manifests.read(id).isEmpty(), "the manifest is there: the delete did not commit");
        check(f.manifests.readDeletion(id).isPresent(), "the marker was removed");
        f.nodes.get(1).down = false;
        check(f.copiesOf(id) > 0, "every copy is gone already");
        FileService.DeleteResult second = f.files.delete(id);
        check(second.finished(), "the second call did not finish it");
        checkEquals(null, second.name(), "name when finishing");
        checkEquals(0, f.copiesOf(id), "copies after the second call");
        check(f.manifests.readDeletion(id).isEmpty(), "the marker after the second call");
        expectReason(FileServiceException.Reason.FILE_NOT_FOUND, () -> f.files.delete(id));
    }

    private static void markerRemovalFails(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(2 * CHUNK, 8));
        f.manifests.failDelete = ManifestStore.DELETING_SUFFIX;
        FileService.DeleteResult first = f.files.delete(id);
        check(!first.finished(), "finished without removing the marker");
        checkEquals(0, f.copiesOf(id), "copies");
        check(f.manifests.readDeletion(id).isPresent(), "the marker");
        f.manifests.failDelete = null;
        check(f.files.delete(id).finished(), "the second call did not finish it");
        check(f.manifests.readDeletion(id).isEmpty(), "the marker after the second call");
    }

    private static void leftoverPending(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(CHUNK, 9));
        f.manifests.createPending(new ManifestStore.PendingUpload(id, 1)); // left behind by its upload
        check(f.files.delete(id).finished(), "not finished");
        check(f.manifests.readPending(id).isEmpty(), "the leftover pending marker");
    }

    private static void idNotReused(Path tempDir) throws Exception {
        String deleting = FileIds.newFileId();
        String fresh = FileIds.newFileId();
        List<String> ids = new ArrayList<>(List.of(deleting, fresh));
        Fixture f = new Fixture(tempDir, () -> ids.remove(0));
        f.manifests.createDeletion(new ManifestStore.PendingUpload(deleting, 1));
        checkEquals(fresh, f.store(randomBytes(CHUNK, 10)), "the new upload's ID");
    }

    private static void takesTheLock(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String id = f.store(randomBytes(CHUNK, 11));
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            try {
                f.files.withStorageLock(() -> {
                    held.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        holder.start();
        check(held.await(5, TimeUnit.SECONDS), "the lock was not taken");
        AtomicReference<Object> outcome = new AtomicReference<>();
        Thread deleter = new Thread(() -> {
            try {
                outcome.set(f.files.delete(id));
            } catch (Exception e) {
                outcome.set(e);
            }
        });
        deleter.start();
        Thread.sleep(400);
        check(outcome.get() == null, "the delete ran while the storage lock was held");
        check(f.manifests.read(id).isPresent(), "the manifest was removed while the storage lock was held");
        release.countDown();
        deleter.join(10_000);
        holder.join(10_000);
        check(outcome.get() instanceof FileService.DeleteResult r && r.finished(), "outcome: " + outcome.get());
    }

    private static void listener(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        String named = f.storeNamed(randomBytes(CHUNK, 12), "report.pdf");
        String other = f.store(randomBytes(CHUNK, 13));
        f.manifests.failMove = ManifestStore.DELETING_SUFFIX;
        expectReason(FileServiceException.Reason.STORAGE_ERROR, () -> f.files.delete(other));
        f.manifests.failMove = null;
        f.files.delete(named);
        checkEquals(List.of(new FileService.DeleteResult(named, "report.pdf", 1, true)), f.deletes, "listener calls");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private interface ThrowingCall {
        Object call() throws Exception;
    }

    private static void expectReason(FileServiceException.Reason reason, ThrowingCall call) throws Exception {
        try {
            call.call();
            throw new AssertionError("expected " + reason);
        } catch (FileServiceException e) {
            checkEquals(reason, e.reason(), "reason");
        }
    }

    /** A manifest store whose writes and deletes fail on demand, by name suffix. */
    private static final class HookedStore extends ManifestStore {
        volatile String failMove;
        volatile String failDelete;
        volatile String failAfterDelete;

        HookedStore(Path dir) throws IOException {
            super(dir);
        }

        @Override
        void moveIntoPlace(Path source, Path target) throws IOException {
            if (failMove != null && target.getFileName().toString().endsWith(failMove)) {
                throw new IOException("simulated: the rename failed");
            }
            super.moveIntoPlace(source, target);
        }

        @Override
        void deleteEntry(Path entry) throws IOException {
            String name = entry.getFileName().toString();
            if (failDelete != null && name.endsWith(failDelete)) {
                throw new IOException("simulated: the delete failed");
            }
            super.deleteEntry(entry);
            if (failAfterDelete != null && name.endsWith(failAfterDelete)) {
                throw new IOException("simulated: the delete reported a failure after deleting");
            }
        }
    }

    /** Three controllable local nodes, two copies per chunk, 1 KiB chunks. */
    private static final class Fixture {
        final List<NodeTestKit.ControlledNode> nodes = new ArrayList<>();
        final HookedStore manifests;
        final FileService files;
        final NodeTestKit.Lines err = new NodeTestKit.Lines();
        final List<FileService.DeleteResult> deletes = new ArrayList<>();

        Fixture(Path tempDir) throws IOException {
            this(tempDir, FileIds::newFileId);
        }

        Fixture(Path tempDir, FileService.IdGenerator ids) throws IOException {
            for (int i = 0; i < 3; i++) {
                nodes.add(new NodeTestKit.ControlledNode(Files.createDirectories(tempDir.resolve("node-" + i))));
            }
            manifests = new HookedStore(Files.createDirectories(tempDir.resolve("manifests")));
            files = new FileService(new StorageCluster(nodes, 2), manifests, Files.createDirectories(tempDir.resolve("tmp")),
                    CHUNK, 1L << 30, ids, err.stream());
            files.setDeleteListener(deletes::add);
        }

        String store(byte[] data) throws IOException {
            return storeNamed(data, null);
        }

        String storeNamed(byte[] data, String name) throws IOException {
            Path workspace = files.newUploadWorkspace();
            Path body = workspace.resolve("body");
            Files.write(body, data);
            try {
                return files.store(body, workspace, null, name).fileId();
            } finally {
                files.deleteWorkspace(workspace);
            }
        }

        /** Copies of the file's chunks on all nodes, counted on disk. */
        int copiesOf(String fileId) throws IOException {
            int copies = 0;
            for (NodeTestKit.ControlledNode node : nodes) {
                for (String name : listNames(node.dir())) {
                    if (name.startsWith(fileId + "_")) {
                        copies++;
                    }
                }
            }
            return copies;
        }
    }
}
