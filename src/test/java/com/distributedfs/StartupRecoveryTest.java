package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Plain-Java tests for StartupRecovery (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.StartupRecoveryTest}
 *
 * <p>Every test builds a real data-directory layout (manifests, tmp and three node folders) in a
 * fresh temporary folder, with real storage nodes and a real manifest store. Failures are injected
 * through small subclasses: nodes whose checks, listings or deletes can be made to fail or to
 * change the disk at the worst moment, and a manifest store whose marker reads and deletes can fail.
 * Junctions are real directory junctions (mklink /J, no admin rights needed). Tests that must change
 * nothing compare a no-follow snapshot of the whole data directory, byte for byte, before and after.
 * The program exits with code 1 if any test fails.
 */
public class StartupRecoveryTest {

    private static final int NODE_COUNT = 3;
    private static final int CHUNK_SIZE = 4;

    /** File IDs in sorted order where order matters: A before F before C. */
    private static final String A = "1".repeat(32);
    private static final String F = "5".repeat(32);
    private static final String C = "9".repeat(32);
    private static final String G = "a".repeat(32);
    private static final String H = "b".repeat(32);
    private static final String O = "c".repeat(32);
    private static final String U = "d".repeat(32);
    private static final String O2 = "e".repeat(32);
    /** Differs from F only in its last character. */
    private static final String F_NEIGHBOUR = "5".repeat(31) + "6";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.StartupRecoveryTest");
            System.exit(2);
            return;
        }

        // Basics
        runTest("a clean data directory needs nothing and prints nothing", StartupRecoveryTest::cleanDirectoryIsQuiet);
        runTest("an incomplete upload is rolled back: its chunks, temp files and marker are removed",
                StartupRecoveryTest::incompleteUploadIsRolledBack);
        runTest("a committed upload with a leftover marker loses only the marker; the file reads back identically",
                StartupRecoveryTest::committedUploadKeepsFile);
        runTest("extra chunks of a verified committed upload are kept, reported, and do not block the marker delete",
                StartupRecoveryTest::committedExtraChunksAreReportOnly);
        runTest("rollback touches only the marker's own chunks: other files, demo chunks and orphans stay",
                StartupRecoveryTest::rollbackTouchesOnlyItsOwnChunks);
        runTest("a second recovery changes nothing and prints nothing", StartupRecoveryTest::repeatedRecoveryIsNoOp);

        // Fail closed during assessment
        runTest("D2: a corrupt or unreadable marker stops the start and changes nothing",
                StartupRecoveryTest::corruptMarkersFailClosed);
        runTest("D3: a damaged committed upload stops the start; the file and marker stay byte-identical",
                StartupRecoveryTest::damagedCommittedUploadsFailClosed);
        runTest("an incomplete upload with an unexpected or unsafe entry stops the start and changes nothing",
                StartupRecoveryTest::unexpectedIncompleteEntriesFailClosed);
        runTest("every marker is assessed before any is acted on", StartupRecoveryTest::assessesEverythingBeforeActing);
        runTest("a data directory that cannot be listed stops the start and changes nothing",
                StartupRecoveryTest::unlistableNodeFailsClosed);

        // D1: failures while acting
        runTest("D1: a chunk that cannot be deleted is deferred; the marker is kept and the next start finishes",
                StartupRecoveryTest::chunkDeleteFailureIsDeferred);
        runTest("D1: a marker that cannot be deleted after its chunks is deferred and retried",
                StartupRecoveryTest::markerDeleteFailureIsDeferred);
        runTest("D1: a committed upload whose marker cannot be deleted is deferred with the file intact",
                StartupRecoveryTest::committedMarkerDeleteFailureIsDeferred);
        runTest("D1: an uncertain state after a failed delete stops the start and keeps the marker",
                StartupRecoveryTest::uncertainStateAfterFailureFailsClosed);
        runTest("D1: the stop message names what was resolved, what was in progress and what was not reached",
                StartupRecoveryTest::stopReportsProgress);

        // Stale temp files
        runTest("temp-named junctions in every folder are left alone with their targets; other names stay",
                StartupRecoveryTest::tempSweepNeverFollowsJunctions);
        runTest("a temp file that cannot be deleted is only a warning", StartupRecoveryTest::undeletableTempIsWarning);
        runTest("a stale workspace with a junction inside is removed without touching the junction's target",
                StartupRecoveryTest::workspaceWithJunctionIsRemovedSafely);

        // Chunk scan
        runTest("the chunk scan reports orphans, extra chunks and unreadable manifests, and deletes nothing",
                StartupRecoveryTest::scanReportsEachCategory);
        runTest("the chunk scan ignores names that are not canonical chunk names",
                StartupRecoveryTest::scanIgnoresOtherNames);
        runTest("a node that cannot be listed during the scan is only a warning",
                StartupRecoveryTest::unlistableNodeDuringScanIsWarning);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Basics
    // ---------------------------------------------------------------------

    private static void cleanDirectoryIsQuiet(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        StartupRecovery.Result result = f.recover();
        checkEquals(List.of(), result.committed(), "committed");
        checkEquals(List.of(), result.rolledBack(), "rolled back");
        checkEquals(0, result.tempItemsRemoved(), "temp items");
        checkEquals("", f.outText(), "stdout");
        checkEquals("", f.errText(), "stderr");
    }

    private static void incompleteUploadIsRolledBack(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.pending(F, 4);
        f.putChunk(0, F + "_0", "aaaa");
        f.putChunk(1, F + "_1", "bbbb");
        f.putChunk(2, F + "_2", "cccc");
        f.putChunk(0, F + "_3-123.tmp", "dd"); // the store of chunk 3 was interrupted
        Files.writeString(f.manifestsDir.resolve(F + "-5.tmp"), "partial manifest");
        Path workspace = Files.createDirectories(f.tmpDir.resolve("upload-77"));
        Files.writeString(workspace.resolve("body"), "spooled");

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.rolledBack(), "rolled back");
        checkEquals(3, result.chunksDeleted(), "chunks deleted");
        checkEquals(3, result.tempItemsRemoved(), "temp items removed");
        checkEquals(List.of("2", "1", "0"), f.rules.deletedIndexes(F), "deleted highest index first");
        for (int node = 0; node < NODE_COUNT; node++) {
            checkEquals(List.of(), listNames(f.nodeDirs.get(node)), "node " + node);
        }
        checkEquals(List.of(), listNames(f.manifestsDir), "the manifest folder");
        checkEquals(List.of(), listNames(f.tmpDir), "the tmp folder");
        checkContains(f.outText(), "Recovery: 1 interrupted upload(s): 0 committed, 1 rolled back (3 chunk(s) deleted),"
                + " 0 deferred; 3 stale temp file(s) or workspace(s) removed");
    }

    private static void committedUploadKeepsFile(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        byte[] data = bytes(10, 1);
        FileManifest manifest = f.storeCommitted(F, data);
        f.pending(F, manifest.chunkCount());
        String manifestBefore = Files.readString(f.manifestsDir.resolve(F + ".manifest"));

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.committed(), "committed");
        checkEquals(0, result.chunksDeleted(), "chunks deleted");
        checkEquals(List.of(), f.rules.deleted, "no chunk delete was even attempted");
        check(!Files.exists(f.manifestsDir.resolve(F + ".pending")), "the marker was deleted");
        checkEquals(manifestBefore, Files.readString(f.manifestsDir.resolve(F + ".manifest")), "the manifest");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "the file reads back identically");
        checkContains(f.outText(), "1 committed, 0 rolled back");
    }

    private static void committedExtraChunksAreReportOnly(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        byte[] data = bytes(10, 2);
        FileManifest manifest = f.storeCommitted(F, data); // 3 chunks
        f.pending(F, 3);
        Path extra3 = f.putChunk(1, F + "_3", "extra");
        Path extra7 = f.putChunk(2, F + "_7", "extra");

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.committed(), "committed: the extras did not block the marker delete");
        check(!Files.exists(f.manifestsDir.resolve(F + ".pending")), "the marker was deleted");
        check(Files.exists(extra3) && Files.exists(extra7), "the extra chunks were not deleted");
        checkEquals(List.of(), f.rules.deleted, "no chunk delete was attempted");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "the file reads back identically");
        checkEquals(List.of(
                        new StartupRecovery.Finding(StartupRecovery.FindingType.UNREFERENCED_EXTRA_CHUNK, F + "_3", 1,
                                extra3, "manifest chunkCount 3"),
                        new StartupRecovery.Finding(StartupRecovery.FindingType.UNREFERENCED_EXTRA_CHUNK, F + "_7", 2,
                                extra7, "manifest chunkCount 3")),
                result.findings(), "findings");
        checkContains(f.outText(), "  UNREFERENCED_EXTRA_CHUNK " + F + "_3 node 1 " + extra3 + " (manifest chunkCount 3)");
    }

    private static void rollbackTouchesOnlyItsOwnChunks(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        byte[] otherData = bytes(9, 3);
        FileManifest other = f.storeCommitted(G, otherData);
        Path demoChunk = f.putChunk(0, "chunk_0", "demo");
        Path orphan = f.putChunk(1, O + "_0", "orphan");
        Path neighbour = f.putChunk(2, F_NEIGHBOUR + "_0", "neighbour");
        f.pending(F, 3);
        f.putChunk(0, F + "_0", "f0");
        f.putChunk(1, F + "_1", "f1");
        f.putChunk(2, F + "_2", "f2");

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.rolledBack(), "rolled back");
        checkEquals(List.of(F + "_2", F + "_1", F + "_0"), f.rules.deleted, "exactly F's chunks were deleted");
        check(Arrays.equals(otherData, f.readBack(other, tempDir)), "the other file reads back identically");
        checkEquals("demo", Files.readString(demoChunk), "the demo chunk");
        checkEquals("orphan", Files.readString(orphan), "the orphan");
        checkEquals("neighbour", Files.readString(neighbour), "the chunk of a file ID one character away");
    }

    private static void repeatedRecoveryIsNoOp(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.storeCommitted(G, bytes(6, 4));
        f.pending(F, 2);
        f.putChunk(0, F + "_0", "f0");
        f.recover();
        Map<String, String> before = tree(f.root);
        f.clearOutput();

        StartupRecovery.Result second = f.recover();

        checkEquals(List.of(), second.rolledBack(), "nothing to roll back");
        checkEquals(List.of(), second.committed(), "nothing committed");
        checkEquals(0, second.tempItemsRemoved(), "nothing to sweep");
        checkEquals(before, tree(f.root), "the data directory");
        checkEquals("", f.outText(), "stdout");
    }

    // ---------------------------------------------------------------------
    // Fail closed during assessment
    // ---------------------------------------------------------------------

    /** Sets up one variant of a broken data directory in a fresh fixture. */
    private interface Variant {
        void plant(Fixture f, Path variantDir) throws Exception;
    }

    private static void corruptMarkersFailClosed(Path tempDir) throws Exception {
        Map<String, Variant> variants = new java.util.LinkedHashMap<>();
        variants.put("not the marker format", (f, d) -> f.writeMarker(F, "garbage\n"));
        variants.put("a leading zero in chunkCount", (f, d) -> f.writeMarker(F, markerText(F, "02")));
        variants.put("a file ID that differs from its name", (f, d) -> f.writeMarker(F, markerText(G, "2")));
        variants.put("larger than 4 KiB", (f, d) -> f.writeMarker(F, "x".repeat(5000)));
        variants.put("not ASCII", (f, d) -> Files.write(f.manifestsDir.resolve(F + ".pending"),
                new byte[] {'f', 'o', 'r', (byte) 0xC3, (byte) 0xA9}));
        variants.put("a folder named like a marker", (f, d) -> {
            Path folder = Files.createDirectory(f.manifestsDir.resolve(F + ".pending"));
            Files.writeString(folder.resolve("inside"), "keep");
        });
        variants.put("a junction named like a marker", (f, d) -> {
            Path target = Files.createDirectories(d.resolve("target"));
            Files.writeString(target.resolve("precious"), "precious");
            createDirectoryJunction(f.manifestsDir.resolve(F + ".pending"), target);
        });
        variants.put("a marker that cannot be read", (f, d) -> {
            f.pending(F, 2);
            f.manifests.failReadPending = true;
        });
        runFailClosedVariants(tempDir, variants, "its pending marker is corrupt or unreadable", fixture -> {
            fixture.putChunk(0, F + "_0", "f0"); // a chunk that must survive
        });
    }

    private static void damagedCommittedUploadsFailClosed(Path tempDir) throws Exception {
        Map<String, Variant> variants = new java.util.LinkedHashMap<>();
        variants.put("a missing chunk", (f, d) -> Files.delete(f.chunkPath(F + "_1")));
        variants.put("a duplicated chunk", (f, d) -> {
            Path chunk = f.chunkPath(F + "_1");
            int holder = f.nodeOf(F + "_1");
            Files.copy(chunk, f.nodeDirs.get((holder + 1) % NODE_COUNT).resolve(F + "_1"));
        });
        variants.put("a junction in place of a chunk", (f, d) -> {
            Path chunk = f.chunkPath(F + "_1");
            Files.delete(chunk);
            Path target = Files.createDirectories(d.resolve("target"));
            Files.writeString(target.resolve("precious"), "precious");
            createDirectoryJunction(chunk, target);
        });
        variants.put("a node that cannot be checked", (f, d) -> f.rules.failCheckNodes.add(2));
        variants.put("a chunk count that differs from the marker's", (f, d) -> {
            Files.delete(f.manifestsDir.resolve(F + ".pending"));
            f.pending(F, 4);
        });
        variants.put("an invalid manifest", (f, d) -> Files.writeString(f.manifestsDir.resolve(F + ".manifest"),
                "format=1\nfileId=" + F + "\nsize=ten\n"));
        runFailClosedVariants(tempDir, variants, null, fixture -> {
            fixture.storeCommitted(F, bytes(10, 5)); // chunks F_0, F_1, F_2
            fixture.pending(F, 3);
        });
    }

    private static void unexpectedIncompleteEntriesFailClosed(Path tempDir) throws Exception {
        Map<String, Variant> variants = new java.util.LinkedHashMap<>();
        variants.put("an index at the chunk count", (f, d) -> f.putChunk(1, F + "_3", "x"));
        variants.put("an index with a leading zero", (f, d) -> f.putChunk(1, F + "_01", "x"));
        variants.put("a non-numeric name in the namespace", (f, d) -> f.putChunk(1, F + "_x", "x"));
        variants.put("a temp file of a chunk beyond the count", (f, d) -> f.putChunk(1, F + "_5-12.tmp", "x"));
        variants.put("a candidate on two nodes", (f, d) -> f.putChunk(2, F + "_0", "copy"));
        variants.put("a junction under a candidate's name", (f, d) -> {
            Path target = Files.createDirectories(d.resolve("target"));
            Files.writeString(target.resolve("precious"), "precious");
            createDirectoryJunction(f.nodeDirs.get(1).resolve(F + "_1"), target);
        });
        variants.put("a manifest that cannot be read", (f, d) ->
                Files.writeString(f.manifestsDir.resolve(F + ".manifest"), "garbage"));
        runFailClosedVariants(tempDir, variants, null, fixture -> {
            fixture.pending(F, 3);
            fixture.putChunk(0, F + "_0", "f0");
        });
    }

    private static void assessesEverythingBeforeActing(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.pending(A, 1);
        f.putChunk(0, A + "_0", "a0"); // valid and recoverable, and assessed first
        f.writeMarker(F, "garbage");
        Map<String, String> before = tree(f.root);

        UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, f::recover);

        checkEquals(List.of(F), e.fileIds(), "the unsafe marker");
        check(!e.changedAnything(), "nothing changed");
        checkEquals(before, tree(f.root), "the data directory, including the valid upload");
        checkContains(e.getMessage(), "Nothing was changed");
    }

    private static void unlistableNodeFailsClosed(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.pending(F, 1);
        f.putChunk(0, F + "_0", "f0");
        f.rules.failListingFromCall.put(1, 1);
        Map<String, String> before = tree(f.root);

        UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, f::recover);

        check(!e.changedAnything(), "nothing changed");
        checkContains(e.getMessage(), "could not list");
        checkEquals(before, tree(f.root), "the data directory");
    }

    // ---------------------------------------------------------------------
    // D1: failures while acting
    // ---------------------------------------------------------------------

    private static void chunkDeleteFailureIsDeferred(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.pending(F, 3);
        f.putChunk(0, F + "_0", "f0");
        Path f1 = f.putChunk(1, F + "_1", "f1");
        f.putChunk(2, F + "_2", "f2");
        f.rules.failDelete.add(F + "_1");

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.deferred(), "deferred");
        checkEquals(List.of(), result.rolledBack(), "not rolled back");
        checkEquals(1, result.chunksDeleted(), "F_2 was deleted before the failure");
        check(Files.exists(f.manifestsDir.resolve(F + ".pending")), "the marker was kept");
        checkContains(f.errText(), "Warning: the cleanup of interrupted upload " + F + " could not finish");
        checkContains(f.errText(), F + "_1 on node 1 (" + f1 + ")");
        checkContains(f.errText(), F + "_0 on node 0");
        check(!f.outText().contains("ORPHAN_CHUNK"), "chunks of a deferred upload are not reported as orphans");
        checkContains(f.outText(), "1 deferred");

        f.rules.failDelete.clear();
        f.clearOutput();
        StartupRecovery.Result next = f.recover();
        checkEquals(List.of(F), next.rolledBack(), "the next start finishes the cleanup");
        for (int node = 0; node < NODE_COUNT; node++) {
            checkEquals(List.of(), listNames(f.nodeDirs.get(node)), "node " + node);
        }
        checkEquals(List.of(), listNames(f.manifestsDir), "the manifest folder");
    }

    private static void markerDeleteFailureIsDeferred(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.pending(F, 2);
        f.putChunk(0, F + "_0", "f0");
        f.putChunk(1, F + "_1", "f1");
        f.manifests.failDeletePending = true;

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.deferred(), "deferred");
        checkEquals(2, result.chunksDeleted(), "both chunks were deleted");
        check(Files.exists(f.manifestsDir.resolve(F + ".pending")), "the marker was kept");
        checkContains(f.errText(), "No chunks remain");

        f.manifests.failDeletePending = false;
        StartupRecovery.Result next = f.recover();
        checkEquals(List.of(F), next.rolledBack(), "the next start deletes the marker");
        checkEquals(List.of(), listNames(f.manifestsDir), "the manifest folder");
    }

    private static void committedMarkerDeleteFailureIsDeferred(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        byte[] data = bytes(10, 6);
        FileManifest manifest = f.storeCommitted(F, data);
        f.pending(F, 3);
        f.manifests.failDeletePending = true;

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(F), result.deferred(), "deferred");
        checkContains(f.errText(), "The upload is committed and its file is intact");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "the file reads back identically");
        checkEquals(List.of(), f.rules.deleted, "no chunk delete was attempted");

        f.manifests.failDeletePending = false;
        checkEquals(List.of(F), f.recover().committed(), "the next start deletes the marker");
        check(Arrays.equals(data, f.readBack(manifest, tempDir)), "the file still reads back identically");
    }

    private static void uncertainStateAfterFailureFailsClosed(Path tempDir) throws Exception {
        Map<String, DeleteHook> cases = new java.util.LinkedHashMap<>();
        cases.put("a node that can no longer be checked", (f, chunkFile, d) -> {
            f.rules.failCheckNodes.add(2);
            throw new IOException("simulated delete failure");
        });
        cases.put("the entry became a junction", (f, chunkFile, d) -> {
            Files.delete(chunkFile);
            Path target = Files.createDirectories(d.resolve("target"));
            Files.writeString(target.resolve("precious"), "precious");
            createDirectoryJunction(chunkFile, target);
            throw new IOException("simulated delete failure");
        });
        cases.put("a duplicate appeared", (f, chunkFile, d) -> {
            Files.copy(chunkFile, f.nodeDirs.get(0).resolve(chunkFile.getFileName().toString()));
            throw new IOException("simulated delete failure");
        });
        cases.put("a manifest appeared", (f, chunkFile, d) -> {
            f.manifests.create(new FileManifest(F, 12, "0".repeat(64), CHUNK_SIZE, 3));
            throw new IOException("simulated delete failure");
        });
        cases.put("the re-check itself fails", (f, chunkFile, d) -> {
            f.manifests.failReadPending = true;
            throw new IOException("simulated delete failure");
        });
        cases.put("an unexpected runtime failure", (f, chunkFile, d) -> {
            throw new IllegalStateException("simulated bug");
        });
        int index = 0;
        for (Map.Entry<String, DeleteHook> entry : cases.entrySet()) {
            Path variantDir = Files.createDirectories(tempDir.resolve("case-" + index++));
            Fixture f = new Fixture(variantDir.resolve("fixture"));
            f.pending(F, 3);
            f.putChunk(0, F + "_0", "f0");
            f.putChunk(1, F + "_1", "f1");
            f.putChunk(2, F + "_2", "f2");
            DeleteHook hook = entry.getValue();
            f.rules.onDelete.put(F + "_1", chunkFile -> hook.run(f, chunkFile, variantDir));

            UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, f::recover);

            String what = entry.getKey() + ": ";
            checkEquals(List.of(F), e.fileIds(), what + "the upload in progress");
            check(e.changedAnything(), what + "F_2 had been deleted, and the exception says so");
            checkContains(e.getMessage(), "In progress: " + F + " (its pending marker was kept)");
            f.manifests.failReadPending = false;
            check(Files.exists(f.manifestsDir.resolve(F + ".pending")), what + "the marker was kept");
            check(!Files.exists(f.nodeDirs.get(2).resolve(F + "_2")), what + "F_2 was deleted first");
            check(Files.exists(f.nodeDirs.get(0).resolve(F + "_0")), what + "F_0 was not reached");
        }
    }

    private static void stopReportsProgress(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.pending(A, 1);
        f.putChunk(0, A + "_0", "a0");
        f.pending(F, 1);
        f.putChunk(1, F + "_0", "f0");
        f.pending(C, 1);
        Path c0 = f.putChunk(2, C + "_0", "c0");
        f.rules.onDelete.put(F + "_0", chunkFile -> {
            throw new IllegalStateException("simulated bug");
        });

        UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, f::recover);

        checkContains(e.getMessage(), "Already resolved: 0 committed [], 1 rolled back [" + A + "]");
        checkContains(e.getMessage(), "In progress: " + F);
        checkContains(e.getMessage(), "Not reached: [" + C + "] (left untouched)");
        check(e.changedAnything(), "A was rolled back, and the exception says so");
        check(!Files.exists(f.manifestsDir.resolve(A + ".pending")), "A's marker was deleted");
        check(Files.exists(f.manifestsDir.resolve(F + ".pending")), "F's marker was kept");
        check(Files.exists(f.manifestsDir.resolve(C + ".pending")) && Files.exists(c0), "C was untouched");
    }

    // ---------------------------------------------------------------------
    // Stale temp files
    // ---------------------------------------------------------------------

    private static void tempSweepNeverFollowsJunctions(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir.resolve("fixture"));
        Path target = Files.createDirectories(tempDir.resolve("target"));
        Files.writeString(target.resolve("precious"), "precious");
        Path manifestJunction = f.manifestsDir.resolve(F + "-77.tmp");
        Path nodeJunction = f.nodeDirs.get(0).resolve(G + "_0-77.tmp");
        Path workspaceJunction = f.tmpDir.resolve("upload-77");
        createDirectoryJunction(manifestJunction, target);
        createDirectoryJunction(nodeJunction, target);
        createDirectoryJunction(workspaceJunction, target);
        Path chunk = f.putChunk(1, G + "_0", "chunk");
        List<String> leftNodeNames = List.of(G + "_0-.tmp", G + "_0-123456789012345678901.tmp", "notes.txt", "x-1.TMP");
        for (String name : leftNodeNames) {
            f.putChunk(1, name, "keep");
        }
        for (String name : List.of("upload-", "upload-12x", "other")) {
            Files.createDirectory(f.tmpDir.resolve(name));
        }
        Files.writeString(f.tmpDir.resolve("upload-5"), "a file, not a folder");

        StartupRecovery.Result result = f.recover();

        checkEquals(0, result.tempItemsRemoved(), "nothing was eligible");
        for (Path junction : List.of(manifestJunction, nodeJunction, workspaceJunction)) {
            check(Files.exists(junction, LinkOption.NOFOLLOW_LINKS), "the junction is left in place: " + junction);
        }
        checkEquals(List.of("precious"), listNames(target), "the junctions' target folder");
        checkEquals("precious", Files.readString(target.resolve("precious")), "the junctions' target file");
        checkEquals("chunk", Files.readString(chunk), "the chunk");
        for (String name : leftNodeNames) {
            check(Files.exists(f.nodeDirs.get(1).resolve(name)), "left in place: " + name);
        }
        checkEquals(List.of("other", "upload-", "upload-12x", "upload-5", "upload-77"), listNames(f.tmpDir), "tmp");
        String warnings = f.errText();
        checkEquals(4, countOccurrences(warnings, "without following it"), "junction and file warnings: " + warnings);
    }

    private static void undeletableTempIsWarning(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        Path temp = f.putChunk(1, G + "_0-9.tmp", "partial");
        DosFileAttributeView dos = Files.getFileAttributeView(temp, DosFileAttributeView.class);
        dos.setReadOnly(true); // Windows refuses to delete a read-only file
        try {
            StartupRecovery.Result result = f.recover();
            checkEquals(0, result.tempItemsRemoved(), "nothing removed");
            check(Files.exists(temp), "the temp file is still there");
            checkContains(f.errText(), "Could not remove the stale temp file");
        } finally {
            dos.setReadOnly(false);
        }
    }

    private static void workspaceWithJunctionIsRemovedSafely(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir.resolve("fixture"));
        Path target = Files.createDirectories(tempDir.resolve("target"));
        Files.writeString(target.resolve("precious"), "precious");
        Path workspace = Files.createDirectories(f.tmpDir.resolve("upload-123"));
        Files.writeString(workspace.resolve("body"), "spooled");
        createDirectoryJunction(workspace.resolve("inner"), target);

        StartupRecovery.Result result = f.recover();

        checkEquals(1, result.tempItemsRemoved(), "the workspace was removed");
        checkEquals(List.of(), listNames(f.tmpDir), "tmp");
        checkEquals("precious", Files.readString(target.resolve("precious")), "the junction's target file");
    }

    // ---------------------------------------------------------------------
    // Chunk scan
    // ---------------------------------------------------------------------

    private static void scanReportsEachCategory(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir.resolve("fixture"));
        f.storeCommitted(H, bytes(5, 7)); // 2 chunks
        Path orphan0 = f.putChunk(1, O + "_0", "o0");
        Path orphan1 = f.putChunk(2, O + "_1", "o1");
        Path target = Files.createDirectories(tempDir.resolve("target"));
        Path orphanJunction = f.nodeDirs.get(0).resolve(O2 + "_0");
        createDirectoryJunction(orphanJunction, target);
        Path extra = f.putChunk(0, H + "_2", "extra");
        Path hugeExtra = f.putChunk(1, H + "_99999999999", "extra");
        Files.writeString(f.manifestsDir.resolve(U + ".manifest"), "garbage");
        Path unchecked0 = f.putChunk(0, U + "_0", "u0");
        f.putChunk(1, U + "_5", "u5");
        Map<String, String> before = tree(f.root);

        for (int run = 1; run <= 3; run++) {
            f.clearOutput();
            StartupRecovery.Result result = f.recover();

            // Node by node, each node's names in sorted order.
            checkEquals(List.of(
                    new StartupRecovery.Finding(StartupRecovery.FindingType.UNREFERENCED_EXTRA_CHUNK, H + "_2", 0, extra,
                            "manifest chunkCount 2"),
                    new StartupRecovery.Finding(StartupRecovery.FindingType.ORPHAN_CHUNK, O2 + "_0", 0, orphanJunction,
                            "not a regular file"),
                    new StartupRecovery.Finding(StartupRecovery.FindingType.UNREFERENCED_EXTRA_CHUNK, H + "_99999999999",
                            1, hugeExtra, "manifest chunkCount 2"),
                    new StartupRecovery.Finding(StartupRecovery.FindingType.ORPHAN_CHUNK, O + "_0", 1, orphan0, null),
                    new StartupRecovery.Finding(StartupRecovery.FindingType.ORPHAN_CHUNK, O + "_1", 2, orphan1, null)),
                    result.findings(), "run " + run + ": findings");
            checkEquals(List.of(U), result.uncheckedFiles(), "run " + run + ": unchecked files");
            String printed = f.outText();
            checkContains(printed, "Chunk scan: 5 finding(s): 3 ORPHAN_CHUNK, 2 UNREFERENCED_EXTRA_CHUNK;"
                    + " 1 file(s) could not be checked. Report only: nothing was deleted or changed.");
            checkContains(printed, "  ORPHAN_CHUNK " + O + "_0 node 1 " + orphan0 + System.lineSeparator());
            checkContains(printed, "  ORPHAN_CHUNK " + O2 + "_0 node 0 " + orphanJunction + " (not a regular file)");
            checkContains(printed, "  UNCHECKED_FILE " + U + ": its manifest could not be read");
            check(!printed.contains(U + "_0") && !printed.contains(U + "_5"), "an unchecked file's chunks are not classified");
            checkEquals(before, tree(f.root), "run " + run + ": nothing was changed");
        }
        check(Files.exists(unchecked0), "the unchecked file's chunk is still there");
    }

    private static void scanIgnoresOtherNames(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        f.putChunk(0, "chunk_0", "demo");
        f.putChunk(0, O + "_01", "leading zero");
        f.putChunk(1, O.substring(1) + "_0", "31 hex digits");
        f.putChunk(1, O + "_", "no index");
        f.putChunk(2, O + "_0_1", "extra part");
        f.putChunk(2, "x" + O + "_0", "prefixed");
        Files.writeString(f.nodeDirs.get(2).resolve(O.toUpperCase(java.util.Locale.ROOT) + "_3"), "uppercase");

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(), result.findings(), "findings");
        check(!f.outText().contains("Chunk scan"), "nothing is printed: " + f.outText());
    }

    private static void unlistableNodeDuringScanIsWarning(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir);
        Path orphan = f.putChunk(0, O + "_0", "o0");
        f.putChunk(1, O + "_1", "o1");
        f.rules.failListingFromCall.put(1, 2); // the first listing works; the scan's fails

        StartupRecovery.Result result = f.recover();

        checkEquals(List.of(new StartupRecovery.Finding(StartupRecovery.FindingType.ORPHAN_CHUNK, O + "_0", 0, orphan,
                null)), result.findings(), "findings from the nodes that could be listed");
        checkContains(f.errText(), "the chunk scan skipped node 1");
    }

    // ---------------------------------------------------------------------
    // Shared fail-closed runner
    // ---------------------------------------------------------------------

    /** Common setup for every variant, run before the variant plants its damage. */
    private interface Setup {
        void run(Fixture f) throws Exception;
    }

    /**
     * Runs each variant in its own fixture: recovery must refuse with UnsafeRecoveryException,
     * report that nothing changed, and leave the whole data directory byte-identical.
     */
    private static void runFailClosedVariants(Path tempDir, Map<String, Variant> variants, String expectedReason,
                                              Setup setup) throws Exception {
        int index = 0;
        for (Map.Entry<String, Variant> variant : variants.entrySet()) {
            String what = variant.getKey() + ": ";
            Path variantDir = Files.createDirectories(tempDir.resolve("variant-" + index++));
            Fixture f = new Fixture(variantDir.resolve("fixture"));
            setup.run(f);
            variant.getValue().plant(f, variantDir);
            Map<String, String> before = tree(f.root);

            UnsafeRecoveryException e = expectThrows(UnsafeRecoveryException.class, f::recover);

            checkEquals(List.of(F), e.fileIds(), what + "the unsafe upload");
            check(!e.changedAnything(), what + "nothing changed");
            checkContains(e.getMessage(), "Nothing was changed");
            if (expectedReason != null) {
                checkContains(e.getMessage(), expectedReason);
            }
            f.manifests.failReadPending = false;
            f.rules.failCheckNodes.clear();
            checkEquals(before, tree(f.root), what + "the data directory");
            checkEquals(List.of(), f.rules.deleted, what + "no chunk delete was attempted");
        }
    }

    // ---------------------------------------------------------------------
    // Fixture and test doubles
    // ---------------------------------------------------------------------

    /** Runs when a chunk is about to be deleted; may change the disk, then fail. */
    private interface ChunkHook {
        void run(Path chunkFile) throws IOException;
    }

    /** A delete hook with access to the fixture and the variant's own folder. */
    private interface DeleteHook {
        void run(Fixture f, Path chunkFile, Path variantDir) throws IOException;
    }

    /** Failure rules shared by the nodes of one fixture. */
    private static final class NodeRules {
        final Set<String> failDelete = new HashSet<>();
        final Map<String, ChunkHook> onDelete = new HashMap<>();
        final Set<Integer> failCheckNodes = new HashSet<>();
        /** node index -> the listing call (1-based) from which listing fails. */
        final Map<Integer, Integer> failListingFromCall = new HashMap<>();
        final List<String> deleted = new CopyOnWriteArrayList<>();

        List<String> deletedIndexes(String fileId) {
            return deleted.stream().filter(id -> id.startsWith(fileId + "_"))
                    .map(id -> id.substring(fileId.length() + 1)).toList();
        }
    }

    private static final class TestNode extends StorageNode {
        private final int index;
        private final NodeRules rules;
        private int listings = 0;

        TestNode(Path dir, int index, NodeRules rules) throws IOException {
            super(dir);
            this.index = index;
            this.rules = rules;
        }

        @Override
        boolean holdsRegularChunkFile(String chunkId) throws IOException {
            if (rules.failCheckNodes.contains(index)) {
                throw new IOException("simulated check failure on node " + index);
            }
            return super.holdsRegularChunkFile(chunkId);
        }

        @Override
        List<String> listEntryNames() throws IOException {
            listings++;
            Integer failFrom = rules.failListingFromCall.get(index);
            if (failFrom != null && listings >= failFrom) {
                throw new IOException("simulated listing failure on node " + index);
            }
            return super.listEntryNames();
        }

        @Override
        void deleteChunkFile(Path chunkFile) throws IOException {
            String chunkId = chunkFile.getFileName().toString();
            ChunkHook hook = rules.onDelete.get(chunkId);
            if (hook != null) {
                hook.run(chunkFile);
            }
            if (rules.failDelete.contains(chunkId)) {
                throw new IOException("simulated delete failure for " + chunkId);
            }
            super.deleteChunkFile(chunkFile);
            rules.deleted.add(chunkId);
        }
    }

    private static final class TestManifestStore extends ManifestStore {
        volatile boolean failDeletePending;
        volatile boolean failReadPending;

        TestManifestStore(Path dir) throws IOException {
            super(dir);
        }

        @Override
        Optional<PendingUpload> readPending(String fileId) throws IOException {
            if (failReadPending) {
                throw new IOException("simulated marker read failure");
            }
            return super.readPending(fileId);
        }

        @Override
        boolean deletePending(String fileId) throws IOException {
            if (failDeletePending) {
                throw new IOException("simulated marker delete failure");
            }
            return super.deletePending(fileId);
        }
    }

    private static final class Fixture {
        final Path root;
        final Path manifestsDir;
        final Path tmpDir;
        final List<Path> nodeDirs = new ArrayList<>();
        final NodeRules rules = new NodeRules();
        final List<StorageNode> nodes = new ArrayList<>();
        final StorageCluster cluster;
        final TestManifestStore manifests;
        private ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        private ByteArrayOutputStream errBytes = new ByteArrayOutputStream();

        Fixture(Path root) throws IOException {
            this.root = Files.createDirectories(root);
            manifestsDir = Files.createDirectories(root.resolve("manifests"));
            tmpDir = Files.createDirectories(root.resolve("tmp"));
            for (int i = 0; i < NODE_COUNT; i++) {
                Path dir = Files.createDirectories(root.resolve("nodes").resolve("node-" + i));
                nodeDirs.add(dir);
                nodes.add(new TestNode(dir, i, rules));
            }
            cluster = new StorageCluster(nodes);
            manifests = new TestManifestStore(manifestsDir);
        }

        StartupRecovery.Result recover() throws UnsafeRecoveryException {
            return new StartupRecovery(nodes, cluster, manifests, tmpDir,
                    new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                    new PrintStream(errBytes, true, StandardCharsets.UTF_8)).run();
        }

        String outText() {
            return outBytes.toString(StandardCharsets.UTF_8);
        }

        String errText() {
            return errBytes.toString(StandardCharsets.UTF_8);
        }

        void clearOutput() {
            outBytes = new ByteArrayOutputStream();
            errBytes = new ByteArrayOutputStream();
        }

        /** Stores a complete file the way the server does, with the given file ID. */
        FileManifest storeCommitted(String fileId, byte[] data) throws IOException {
            FileService service = new FileService(cluster, manifests, tmpDir, CHUNK_SIZE, 1024 * 1024,
                    () -> fileId, new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
            Path workspace = service.newUploadWorkspace();
            try {
                return service.store(Files.write(workspace.resolve("body"), data), workspace, null);
            } finally {
                service.deleteWorkspace(workspace);
            }
        }

        void pending(String fileId, int chunkCount) throws IOException {
            manifests.createPending(new ManifestStore.PendingUpload(fileId, chunkCount));
        }

        void writeMarker(String fileId, String text) throws IOException {
            Files.writeString(manifestsDir.resolve(fileId + ManifestStore.PENDING_SUFFIX), text);
        }

        Path putChunk(int node, String name, String content) throws IOException {
            return Files.writeString(nodeDirs.get(node).resolve(name), content);
        }

        int nodeOf(String chunkId) {
            for (int i = 0; i < NODE_COUNT; i++) {
                if (Files.exists(nodeDirs.get(i).resolve(chunkId), LinkOption.NOFOLLOW_LINKS)) {
                    return i;
                }
            }
            throw new AssertionError("no node holds " + chunkId);
        }

        Path chunkPath(String chunkId) {
            return nodeDirs.get(nodeOf(chunkId)).resolve(chunkId);
        }

        byte[] readBack(FileManifest manifest, Path tempDir) throws IOException {
            Path dir = Files.createTempDirectory(tempDir, "readback-");
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            for (int i = 0; i < manifest.chunkCount(); i++) {
                Path copy = dir.resolve("chunk-" + i);
                cluster.retrieveChunk(FileIds.chunkId(manifest.fileId(), i), copy);
                data.write(Files.readAllBytes(copy));
            }
            return data.toByteArray();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static String markerText(String fileId, String chunkCount) {
        return "format=1\nfileId=" + fileId + "\nchunkCount=" + chunkCount + "\n";
    }

    private static byte[] bytes(int size, int seed) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i * 31 + seed * 7);
        }
        return data;
    }

    /**
     * A no-follow snapshot of a folder tree: every entry's relative path, with "dir", "junction",
     * "link" or the file's bytes in hex. A junction's target is never entered.
     */
    private static Map<String, String> tree(Path root) throws IOException {
        Map<String, String> entries = new TreeMap<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
                String name = root.relativize(dir).toString();
                if (!dir.equals(root) && attributes.isOther()) {
                    entries.put(name, "junction");
                    return FileVisitResult.SKIP_SUBTREE;
                }
                entries.put(name, "dir");
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                entries.put(root.relativize(file).toString(), attributes.isRegularFile()
                        ? HexFormat.of().formatHex(Files.readAllBytes(file)) : "link");
                return FileVisitResult.CONTINUE;
            }
        });
        return entries;
    }

    private static List<String> listNames(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> entries = Files.list(directory)) {
            return entries.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    private static int countOccurrences(String text, String part) {
        int count = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + 1)) {
            count++;
        }
        return count;
    }

    /** Creates a directory junction (mklink /J, no admin rights needed), as FileTreesTest does. */
    private static void createDirectoryJunction(Path link, Path target) throws IOException {
        Process process = new ProcessBuilder("cmd.exe", "/c", "mklink", "/J", link.toString(), target.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while creating a junction", e);
        }
        if (exitCode != 0) {
            throw new IOException("mklink /J failed with exit code " + exitCode + ": " + output.trim());
        }
        BasicFileAttributes attributes = Files.readAttributes(link, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        check(attributes.isOther(), "the planted entry is not a junction");
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("startuprecovery-test-");
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
            throw new AssertionError("expected to find <" + expectedPart + "> in <" + text + ">");
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

    /** Test cleanup that never follows a junction: a junction is deleted as a link. */
    private static void deleteRecursively(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) throws IOException {
                if (!dir.equals(directory) && attributes.isOther()) {
                    Files.delete(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

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
