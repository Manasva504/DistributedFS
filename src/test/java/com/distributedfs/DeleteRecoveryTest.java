package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.startNode;
import static com.distributedfs.NodeTestKit.startNodeOnPort;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plain-Java tests of interrupted deletes (PR #12): startup recovery undoes a delete that did not
 * reach its commit point (the manifest is still there) and finishes one that did, whatever state
 * the copies are in; a damaged deletion marker stops the start; a degraded start leaves a committed
 * delete waiting, and its chunks are not reported as orphans; and a repair never reports a file
 * deleted in the middle of it as lost, nor copies it again. The states are made by hand here;
 * DeleteCrashTest makes them by killing a process at each step.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.DeleteRecoveryTest}.
 */
public class DeleteRecoveryTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("deleterecovery-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("a marker with its manifest (not committed) is undone at startup: the file is kept",
                DeleteRecoveryTest::undone);
        RUNNER.run("a marker without its manifest (committed) is finished at startup: every copy, then the marker",
                DeleteRecoveryTest::finished);
        RUNNER.run("a committed delete with some copies already gone is finished", DeleteRecoveryTest::partlyDone);
        RUNNER.run("a committed delete with every copy already gone only loses its marker", DeleteRecoveryTest::allCopiesGone);
        RUNNER.run("a damaged deletion marker stops the start, and nothing is changed", DeleteRecoveryTest::damagedMarker);
        RUNNER.run("a leftover pending marker and a deletion marker without a manifest are both cleaned up",
                DeleteRecoveryTest::pendingAndDeleting);
        RUNNER.run("a degraded start leaves a committed delete waiting, without orphans; a full start finishes it",
                DeleteRecoveryTest::degradedStartWaits);
        RUNNER.run("a file deleted during a repair is neither reported lost nor copied again",
                DeleteRecoveryTest::deletedDuringRepair);
        RUNNER.finish();
    }

    private static void undone(Path tempDir) throws Exception {
        byte[] data = randomBytes(2 * CHUNK, 1);
        String id = uploadAndStop(tempDir, data);
        manifests(tempDir).createDeletion(new ManifestStore.PendingUpload(id, 2));
        NodeTestKit.Lines out = new NodeTestKit.Lines();
        DistributedFsServer server = start(tempDir, out);
        try {
            checkContains(out.text(), "1 interrupted delete(s): 1 undone (the file is kept)");
            check(server.manifests().readDeletion(id).isEmpty(), "the marker");
            check(Arrays.equals(data, download(server.port(), id).body()), "the file after the start");
        } finally {
            server.stop();
        }
    }

    private static void finished(Path tempDir) throws Exception {
        String keep = uploadAndStop(tempDir, randomBytes(CHUNK, 2));
        String id = uploadAgainAndStop(tempDir, randomBytes(3 * CHUNK, 3));
        commitByHand(tempDir, id, 3);
        NodeTestKit.Lines out = new NodeTestKit.Lines();
        DistributedFsServer server = start(tempDir, out);
        try {
            checkContains(out.text(), "1 interrupted delete(s): 0 undone (the file is kept), 1 finished (3 chunk(s) deleted)");
            check(!out.text().contains("ORPHAN_CHUNK"), "orphans reported: " + out.text());
            checkEquals(0, copiesOf(tempDir, id), "copies");
            checkEquals(2, copiesOf(tempDir, keep), "copies of the other file");
            check(server.manifests().readDeletion(id).isEmpty(), "the marker");
            checkEquals(404, download(server.port(), id).status(), "download");
        } finally {
            server.stop();
        }
    }

    private static void partlyDone(Path tempDir) throws Exception {
        String id = uploadAndStop(tempDir, randomBytes(3 * CHUNK, 4));
        commitByHand(tempDir, id, 3);
        deleteCopies(tempDir, FileIds.chunkId(id, 0)); // the delete got as far as chunk 0
        DistributedFsServer server = start(tempDir, new NodeTestKit.Lines());
        try {
            checkEquals(0, copiesOf(tempDir, id), "copies");
            check(server.manifests().readDeletion(id).isEmpty(), "the marker");
        } finally {
            server.stop();
        }
    }

    private static void allCopiesGone(Path tempDir) throws Exception {
        String id = uploadAndStop(tempDir, randomBytes(2 * CHUNK, 5));
        commitByHand(tempDir, id, 2);
        deleteCopies(tempDir, FileIds.chunkId(id, 0));
        deleteCopies(tempDir, FileIds.chunkId(id, 1));
        NodeTestKit.Lines out = new NodeTestKit.Lines();
        DistributedFsServer server = start(tempDir, out);
        try {
            checkContains(out.text(), "1 finished (0 chunk(s) deleted)");
            check(server.manifests().readDeletion(id).isEmpty(), "the marker");
        } finally {
            server.stop();
        }
    }

    private static void damagedMarker(Path tempDir) throws Exception {
        String id = uploadAndStop(tempDir, randomBytes(CHUNK, 6));
        Path marker = tempDir.resolve("data").resolve("manifests").resolve(id + ManifestStore.DELETING_SUFFIX);
        Files.writeString(marker, "format=1\nfileId=" + id + "\nchunkCount=one\n", StandardCharsets.US_ASCII);
        try {
            start(tempDir, new NodeTestKit.Lines()).stop();
            throw new AssertionError("the server started with a damaged deletion marker");
        } catch (UnsafeRecoveryException e) {
            checkContains(e.getMessage(), "its deletion marker is corrupt or unreadable");
            checkContains(e.getMessage(), "Nothing was changed");
        }
        check(Files.exists(marker), "the marker was removed");
        checkEquals(2, copiesOf(tempDir, id), "copies");
        check(Files.exists(tempDir.resolve("data").resolve("manifests").resolve(id + ".manifest")), "the manifest");
    }

    private static void pendingAndDeleting(Path tempDir) throws Exception {
        String id = uploadAndStop(tempDir, randomBytes(2 * CHUNK, 7));
        ManifestStore store = manifests(tempDir);
        store.createPending(new ManifestStore.PendingUpload(id, 2)); // left behind by its upload
        commitByHand(tempDir, id, 2); // then a delete committed, and stopped before removing it
        DistributedFsServer server = start(tempDir, new NodeTestKit.Lines());
        try {
            check(server.manifests().readPending(id).isEmpty(), "the pending marker");
            check(server.manifests().readDeletion(id).isEmpty(), "the deletion marker");
            checkEquals(0, copiesOf(tempDir, id), "copies");
        } finally {
            server.stop();
        }
    }

    private static void degradedStartWaits(Path tempDir) throws Exception {
        List<Path> folders = new ArrayList<>();
        List<NodeTestKit.NodeHandle> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            folders.add(tempDir.resolve("node-" + i));
            nodes.add(startNode(folders.get(i)));
            ports.add(nodes.get(i).port());
        }
        try {
            DistributedFsServer first = startServer(tempDir.resolve("data"), ports, new NodeTestKit.Lines(),
                    new NodeTestKit.Lines(), CHUNK, 60_000);
            String id;
            try {
                id = fileId(upload(first.port(), randomBytes(3 * CHUNK, 8)));
            } finally {
                first.stop();
            }
            commitByHand(tempDir, id, 3);
            nodes.get(2).close();
            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer degraded = startServer(tempDir.resolve("data"), ports, out, new NodeTestKit.Lines(), CHUNK, 60_000);
            try {
                checkContains(out.text(), "1 waiting for a node that did not answer");
                check(!out.text().contains("ORPHAN_CHUNK"), "orphans reported: " + out.text());
                check(degraded.manifests().readDeletion(id).isPresent(), "the marker was removed in a degraded start");
            } finally {
                degraded.stop();
            }
            nodes.set(2, startNodeOnPort(folders.get(2), ports.get(2)));
            NodeTestKit.Lines out2 = new NodeTestKit.Lines();
            DistributedFsServer full = startServer(tempDir.resolve("data"), ports, out2, new NodeTestKit.Lines(), CHUNK, 60_000);
            try {
                checkContains(out2.text(), "1 finished");
                check(full.manifests().readDeletion(id).isEmpty(), "the marker after a full start");
                for (NodeTestKit.NodeHandle node : nodes) {
                    for (String name : listNames(node.chunksDir())) {
                        check(!name.startsWith(id + "_"), "a copy is left: " + name);
                    }
                }
            } finally {
                full.stop();
            }
        } finally {
            for (NodeTestKit.NodeHandle node : nodes) {
                node.close();
            }
        }
    }

    private static void deletedDuringRepair(Path tempDir) throws Exception {
        DistributedFsServer server = start(tempDir, new NodeTestKit.Lines());
        try {
            String id = fileId(upload(server.port(), randomBytes(3 * CHUNK, 9)));
            deleteOneCopy(tempDir, FileIds.chunkId(id, 1)); // so the repair would copy chunk 1 again
            AtomicBoolean once = new AtomicBoolean();
            server.repair().beforeChunkHook = () -> {
                if (once.compareAndSet(false, true)) {
                    try {
                        server.files().delete(id);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            String report = NodeTestKit.repair(server.port(), null).text();
            checkEquals(0L, NodeTestKit.jsonNumber(report, "chunksLost"), "chunksLost: " + report);
            checkEquals(0L, NodeTestKit.jsonNumber(report, "copiesAdded"), "copiesAdded: " + report);
            checkEquals(0, copiesOf(tempDir, id), "copies after the repair");
            checkEquals(404, download(server.port(), id).status(), "download");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DistributedFsServer start(Path tempDir, NodeTestKit.Lines out) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), out, new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    private static String uploadAndStop(Path tempDir, byte[] data) throws IOException {
        return uploadAgainAndStop(tempDir, data);
    }

    private static String uploadAgainAndStop(Path tempDir, byte[] data) throws IOException {
        DistributedFsServer server = start(tempDir, new NodeTestKit.Lines());
        try {
            return fileId(upload(server.port(), data));
        } finally {
            server.stop();
        }
    }

    private static ManifestStore manifests(Path tempDir) throws IOException {
        return new ManifestStore(tempDir.resolve("data").resolve("manifests"));
    }

    /** What a delete leaves at its commit point: the marker written, the manifest removed. */
    private static void commitByHand(Path tempDir, String id, int chunkCount) throws IOException {
        ManifestStore store = manifests(tempDir);
        store.createDeletion(new ManifestStore.PendingUpload(id, chunkCount));
        check(store.delete(id), "no manifest to remove");
    }

    private static List<Path> chunkFolders(Path tempDir) throws IOException {
        List<Path> folders = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            folders.add(tempDir.resolve("data").resolve("nodes").resolve("node-" + i));
        }
        return folders;
    }

    private static void deleteCopies(Path tempDir, String chunkId) throws IOException {
        for (Path folder : chunkFolders(tempDir)) {
            Files.deleteIfExists(folder.resolve(chunkId));
        }
    }

    private static void deleteOneCopy(Path tempDir, String chunkId) throws IOException {
        for (Path folder : chunkFolders(tempDir)) {
            if (Files.deleteIfExists(folder.resolve(chunkId))) {
                return;
            }
        }
        throw new AssertionError("no copy of " + chunkId);
    }

    private static int copiesOf(Path tempDir, String id) throws IOException {
        int copies = 0;
        for (Path folder : chunkFolders(tempDir)) {
            for (String name : listNames(folder)) {
                if (name.startsWith(id + "_")) {
                    copies++;
                }
            }
        }
        return copies;
    }
}
