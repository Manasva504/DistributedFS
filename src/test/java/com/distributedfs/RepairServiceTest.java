package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.randomBytes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Plain-Java tests for RepairService (milestone M3), on local nodes whose failures each test
 * switches on ({@link NodeTestKit.ControlledNode}), with a real FileService and manifest store:
 * adding copies, choosing the target, replacing damaged copies, lost chunks, trimming extras, dry
 * runs, nodes that fail during a repair, and one repair at a time.
 * Run: {@code java -cp "out;out-test" com.distributedfs.RepairServiceTest}.
 */
public class RepairServiceTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("repair-test-");

    private static final int CHUNK = 4 * 1024;

    public static void main(String[] args) {
        RUNNER.run("a healthy cluster is checked and left unchanged", RepairServiceTest::healthyIsNoOp);
        RUNNER.run("a chunk with one good copy gets a second, verified and byte-identical", RepairServiceTest::oneGoodCopy);
        RUNNER.run("the new copy goes to the least loaded node that has no copy", RepairServiceTest::leastLoadedTarget);
        RUNNER.run("a node that is DOWN never gets a copy", RepairServiceTest::downNodeIsNoTarget);
        RUNNER.run("a damaged copy is deleted and replaced on another node", RepairServiceTest::damagedCopyReplaced);
        RUNNER.run("the node of a damaged copy is used only when no other node can take the copy",
                RepairServiceTest::damagedNodeUsedLast);
        RUNNER.run("a chunk with no good copy is lost, and its damaged copies are kept", RepairServiceTest::lostChunk);
        RUNNER.run("with a node DOWN, a chunk with no good copy is uncertain, not lost", RepairServiceTest::noCopyWithNodeDown);
        RUNNER.run("extra copies are trimmed to 2 when every node is UP", RepairServiceTest::extrasTrimmed);
        RUNNER.run("extra copies are kept while a node is DOWN", RepairServiceTest::noTrimWithNodeDown);
        RUNNER.run("of 3 copies with 1 damaged, the damaged one goes and 2 verified copies stay",
                RepairServiceTest::trimKeepsVerifiedCopies);
        RUNNER.run("a dry run changes nothing and reports what the repair then does", RepairServiceTest::dryRunChangesNothing);
        RUNNER.run("a node that fails during the repair is not used again, and blocks trimming",
                RepairServiceTest::nodeFailsDuringRepair);
        RUNNER.run("a new copy that fails its check is deleted, and the next node is tried",
                RepairServiceTest::badNewCopyTriesNextNode);
        RUNNER.run("a lost store answer leaves the chunk uncertain", RepairServiceTest::ambiguousStoreIsUncertain);
        RUNNER.run("files with a pending marker and format 1 files are skipped", RepairServiceTest::skippedFiles);
        RUNNER.run("a second repair while one runs is refused", RepairServiceTest::oneRepairAtATime);
        RUNNER.run("the report's JSON has every field, in order", RepairServiceTest::reportJson);
        RUNNER.run("chunks that no UP node can take are counted as not repaired", RepairServiceTest::noTargetLeft);
        RUNNER.finish();
    }

    private static void healthyIsNoOp(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(3, 1);
        Map<String, String> before = f.tree();
        RepairReport report = f.repair(false);
        checkEquals(1, report.filesChecked(), "files checked");
        checkEquals(3, report.chunksChecked(), "chunks checked");
        checkEquals(3, report.chunksHealthy(), "chunks healthy");
        checkEquals(List.of(0, 0, 0, 0), List.of(report.copiesAdded(), report.copiesReplaced(), report.copiesRemoved(),
                report.chunksLost()), "added, replaced, removed, lost");
        checkEquals(before, f.tree(), "the nodes");
        check(id != null, "uploaded");
    }

    private static void oneGoodCopy(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 2);
        String chunk = FileIds.chunkId(id, 0); // on nodes 0 and 1
        Files.delete(f.copy(1, chunk));
        // Nodes 1 and 2 hold more chunks than node 0, so the least-loaded node is the one that
        // already holds the good copy: the repair must still not try to store a copy there.
        f.addLoad(1, 3);
        f.addLoad(2, 3);
        int holderAttempts = f.nodes.get(0).storeAttempts.get();
        RepairReport report = f.repair(false);
        checkEquals(1, report.copiesAdded(), "copies added");
        checkEquals(holderAttempts, f.nodes.get(0).storeAttempts.get(), "store attempts on the node holding the good copy");
        checkEquals(2, f.holders(chunk).size(), "copies after the repair");
        for (int node : f.holders(chunk)) {
            check(Arrays.equals(f.content(id, 0), Files.readAllBytes(f.copy(node, chunk))), "copy on node " + node);
        }
    }

    private static void leastLoadedTarget(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 3);
        String chunk = FileIds.chunkId(id, 0); // on nodes 0 and 1
        Files.delete(f.copy(1, chunk));
        f.addLoad(1, 5); // node 1 now holds 5 other chunks, node 2 none
        f.repair(false);
        checkEquals(List.of(0, 2), f.holders(chunk), "holders");
    }

    private static void downNodeIsNoTarget(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 4);
        String chunk = FileIds.chunkId(id, 0);
        Files.delete(f.copy(1, chunk));
        f.addLoad(1, 5); // node 2 would be chosen, but it is DOWN
        f.markDown(2);
        int downAttempts = f.nodes.get(2).storeAttempts.get();
        f.repair(false);
        checkEquals(List.of(0, 1), f.holders(chunk), "holders");
        checkEquals(downAttempts, f.nodes.get(2).storeAttempts.get(), "store attempts on the DOWN node");
    }

    private static void damagedCopyReplaced(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 5);
        String chunk = FileIds.chunkId(id, 0);
        f.damage(0, chunk);
        RepairReport report = f.repair(false);
        checkEquals(1, report.copiesReplaced(), "copies replaced");
        checkEquals(List.of(1, 2), f.holders(chunk), "holders: the damaged copy on node 0 went, node 2 got a new one");
        check(Arrays.equals(f.content(id, 0), Files.readAllBytes(f.copy(2, chunk))), "the new copy");
    }

    private static void damagedNodeUsedLast(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 2);
        String id = f.upload(1, 6);
        String chunk = FileIds.chunkId(id, 0);
        f.damage(0, chunk);
        RepairReport report = f.repair(false);
        checkEquals(1, report.copiesReplaced(), "copies replaced");
        checkEquals(List.of(0, 1), f.holders(chunk), "holders");
        check(Arrays.equals(f.content(id, 0), Files.readAllBytes(f.copy(0, chunk))), "node 0's copy is good again");
    }

    private static void lostChunk(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(2, 7);
        String chunk = FileIds.chunkId(id, 1);
        for (int node : f.holders(chunk)) {
            f.damage(node, chunk);
        }
        List<Integer> damaged = f.holders(chunk);
        RepairReport report = f.repair(false);
        checkEquals(1, report.chunksLost(), "lost");
        checkEquals(List.of(chunk), report.lost(), "lost IDs");
        checkEquals(damaged, f.holders(chunk), "the damaged copies are kept");
        checkContains(f.err.text(), "chunk " + chunk + " is lost");
    }

    private static void noCopyWithNodeDown(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 8);
        String chunk = FileIds.chunkId(id, 0);
        for (int node : f.holders(chunk)) {
            Files.delete(f.copy(node, chunk));
        }
        f.markDown(2);
        RepairReport report = f.repair(false);
        checkEquals(0, report.chunksLost(), "lost");
        checkEquals(1, report.chunksUncertain(), "uncertain");
    }

    private static void extrasTrimmed(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 9);
        String chunk = FileIds.chunkId(id, 0);
        Files.copy(f.copy(0, chunk), f.copy(2, chunk));
        RepairReport report = f.repair(false);
        checkEquals(1, report.copiesRemoved(), "copies removed");
        checkEquals(2, f.holders(chunk).size(), "copies left");
        for (int node : f.holders(chunk)) {
            check(Arrays.equals(f.content(id, 0), Files.readAllBytes(f.copy(node, chunk))), "copy on node " + node);
        }
    }

    private static void noTrimWithNodeDown(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 4);
        String id = f.upload(1, 10);
        String chunk = FileIds.chunkId(id, 0); // nodes 0 and 1
        Files.copy(f.copy(0, chunk), f.copy(2, chunk));
        f.markDown(3);
        RepairReport report = f.repair(false);
        check(report.trimSkipped(), "trimSkipped");
        checkEquals(0, report.copiesRemoved(), "copies removed");
        checkEquals(List.of(0, 1, 2), f.holders(chunk), "holders");
    }

    private static void trimKeepsVerifiedCopies(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 11);
        String chunk = FileIds.chunkId(id, 0); // nodes 0 and 1
        Files.copy(f.copy(1, chunk), f.copy(2, chunk));
        f.damage(0, chunk);
        RepairReport report = f.repair(false);
        checkEquals(0, report.copiesReplaced(), "copies replaced");
        checkEquals(1, report.copiesRemoved(), "copies removed (the damaged one)");
        checkEquals(List.of(1, 2), f.holders(chunk), "holders");
    }

    private static void dryRunChangesNothing(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(4, 12);
        Files.delete(f.copy(1, FileIds.chunkId(id, 0)));                              // under-replicated
        f.damage(f.holders(FileIds.chunkId(id, 1)).get(0), FileIds.chunkId(id, 1));  // damaged
        String extra = FileIds.chunkId(id, 2);
        int free = 3 - f.holders(extra).stream().mapToInt(Integer::intValue).sum();  // the node without a copy
        Files.copy(f.copy(f.holders(extra).get(0), extra), f.copy(free, extra));    // over-replicated
        Map<String, String> before = f.tree();
        RepairReport dry = f.repair(true);
        check(dry.dryRun(), "dryRun");
        checkEquals(before, f.tree(), "the nodes after the dry run");
        RepairReport real = f.repair(false);
        checkEquals(List.of(dry.chunksHealthy(), dry.copiesAdded(), dry.copiesReplaced(), dry.copiesRemoved(),
                dry.chunksLost()), List.of(real.chunksHealthy(), real.copiesAdded(), real.copiesReplaced(),
                real.copiesRemoved(), real.chunksLost()), "dry run counts equal the real run's");
        checkEquals(List.of(1, 1, 1), List.of(real.copiesAdded(), real.copiesReplaced(), real.copiesRemoved()),
                "added, replaced, removed");
    }

    private static void nodeFailsDuringRepair(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 13);
        String chunk = FileIds.chunkId(id, 0); // nodes 0 and 1
        Files.copy(f.copy(0, chunk), f.copy(2, chunk)); // an extra copy on node 2
        f.nodes.get(1).down = true; // fails, while the heartbeat still says UP
        RepairReport report = f.repair(false);
        checkEquals(List.of(1), report.nodesFailedDuringRepair(), "failed nodes");
        check(report.trimSkipped() || report.copiesRemoved() == 0, "no trim with a failed node");
        checkEquals(0, report.copiesRemoved(), "copies removed");
        checkContains(f.err.text(), "repair stopped using node 1");
    }

    private static void badNewCopyTriesNextNode(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 4);
        String id = f.upload(1, 14);
        String chunk = FileIds.chunkId(id, 0); // nodes 0 and 1
        Files.delete(f.copy(1, chunk));
        f.nodes.get(1).damageStores = true;
        f.nodes.get(2).damageStores = true;
        RepairReport report = f.repair(false);
        checkEquals(1, report.copiesAdded(), "copies added");
        checkEquals(List.of(0, 3), f.holders(chunk), "holders: the bad copies on nodes 1 and 2 were deleted");
        checkContains(f.err.text(), "did not match its checksum and was deleted");
    }

    private static void ambiguousStoreIsUncertain(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 15);
        String chunk = FileIds.chunkId(id, 0);
        Files.delete(f.copy(1, chunk));
        f.nodes.get(1).loseStoreAnswers = true;
        f.nodes.get(2).loseStoreAnswers = true;
        RepairReport report = f.repair(false);
        checkEquals(1, report.chunksUncertain(), "uncertain");
        checkEquals(0, report.copiesAdded(), "copies added");
        checkEquals(1, report.nodesFailedDuringRepair().size(), "the node whose answer was lost is not used again");
    }

    private static void skippedFiles(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String pending = f.upload(1, 16);
        f.manifests.createPending(new ManifestStore.PendingUpload(pending, 1));
        String legacy = f.upload(1, 17);
        FileManifest stored = f.manifests.read(legacy).orElseThrow();
        Path manifestFile = tempDir.resolve("manifests").resolve(legacy + ".manifest");
        Files.delete(manifestFile);
        Files.writeString(manifestFile, new FileManifest(legacy, stored.size(), stored.sha256(), stored.chunkSize(),
                stored.chunkCount()).toText());
        Files.delete(f.copy(1, FileIds.chunkId(legacy, 0)));
        RepairReport report = f.repair(false);
        checkEquals(2, report.filesSkipped(), "files skipped");
        checkEquals(0, report.filesChecked(), "files checked");
        checkEquals(1, f.holders(FileIds.chunkId(legacy, 0)).size(), "the format 1 file was left alone");
    }

    private static void oneRepairAtATime(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        f.upload(1, 18);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            try {
                f.files.withStorageLock(() -> {
                    locked.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                });
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        holder.start();
        locked.await();
        AtomicReference<RepairReport> first = new AtomicReference<>();
        Thread repairing = new Thread(() -> {
            try {
                first.set(f.repair(false));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        repairing.start();
        NodeTestKit.waitUntil(5_000, "the first repair to wait for the lock",
                () -> repairing.getState() == Thread.State.BLOCKED || repairing.getState() == Thread.State.WAITING);
        // In its own thread, with a time limit: a second repair that is not refused would wait for
        // the lock this test holds, and the suite would hang instead of failing.
        java.util.concurrent.FutureTask<RepairReport> second = new java.util.concurrent.FutureTask<>(() -> f.repair(false));
        new Thread(second).start();
        try {
            second.get(5, java.util.concurrent.TimeUnit.SECONDS);
            check(false, "a second repair ran while the first one was running");
        } catch (java.util.concurrent.ExecutionException e) {
            check(e.getCause() instanceof RepairService.RepairInProgressException,
                    "a second repair failed with " + e.getCause() + " instead of RepairInProgressException");
        } catch (java.util.concurrent.TimeoutException e) {
            check(false, "a second repair waited for the storage lock instead of being refused");
        } finally {
            release.countDown();
        }
        holder.join();
        repairing.join();
        checkEquals(1, first.get().chunksHealthy(), "the first repair finished");
    }

    private static void reportJson(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 19);
        Files.delete(f.copy(1, FileIds.chunkId(id, 0)));
        String json = f.repair(false).toJson();
        List<String> keys = List.of("dryRun", "replicationFactor", "nodes", "filesChecked", "filesSkipped",
                "chunksChecked", "chunksHealthy", "copiesAdded", "copiesReplaced", "copiesRemoved", "chunksLost",
                "chunksUncertain", "chunksNotRepaired", "trimSkipped", "nodesFailedDuringRepair", "lost", "durationMillis");
        int position = 0;
        for (String key : keys) {
            int at = json.indexOf("\"" + key + "\":", position);
            check(at >= position, "key " + key + " missing or out of order in " + json);
            position = at;
        }
        checkContains(json, "\"nodes\":[{\"index\":0,\"state\":\"UP\"},{\"index\":1,\"state\":\"UP\"},{\"index\":2,\"state\":\"UP\"}]");
        checkEquals(1L, NodeTestKit.jsonNumber(json, "copiesAdded"), "copiesAdded");
        check(!json.contains(tempDir.toString().replace("\\", "\\\\")) && !json.contains(":\\"), "a path in " + json);
    }

    private static void noTargetLeft(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 3);
        String id = f.upload(1, 20);
        String chunk = FileIds.chunkId(id, 0);
        Files.delete(f.copy(1, chunk));
        f.markDown(1);
        f.markDown(2);
        RepairReport report = f.repair(false);
        checkEquals(1, report.chunksNotRepaired(), "not repaired");
        checkEquals(List.of(0), f.holders(chunk), "holders");
    }

    /** Local nodes, a cluster with 2 copies, a file service, and a repair service with an unstarted heartbeat. */
    private static final class Fixture {
        final List<NodeTestKit.ControlledNode> nodes = new ArrayList<>();
        final StorageCluster cluster;
        final ManifestStore manifests;
        final FileService files;
        final HeartbeatMonitor monitor;
        final RepairService repair;
        final NodeTestKit.Lines err = new NodeTestKit.Lines();
        final Map<String, List<byte[]>> uploaded = new TreeMap<>();
        private int loadCounter = 0;

        Fixture(Path tempDir, int count) throws IOException {
            List<Boolean> answered = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                nodes.add(new NodeTestKit.ControlledNode(tempDir.resolve("node-" + i)));
                answered.add(true);
            }
            cluster = new StorageCluster(nodes, 2);
            manifests = new ManifestStore(Files.createDirectories(tempDir.resolve("manifests")));
            Path tmp = Files.createDirectories(tempDir.resolve("tmp"));
            files = new FileService(cluster, manifests, tmp, CHUNK, 1L << 30, err.stream());
            monitor = new HeartbeatMonitor(nodes, answered, 1_000, 3, new NodeTestKit.TestClock(), err.stream());
            repair = new RepairService(cluster, manifests, files, tmp, monitor, err.stream());
        }

        String upload(int chunks, long seed) throws IOException {
            byte[] data = randomBytes(chunks * CHUNK, seed);
            Path workspace = files.newUploadWorkspace();
            try {
                Path body = Files.write(workspace.resolve("body"), data);
                String id = files.store(body, workspace, null).fileId();
                List<byte[]> pieces = new ArrayList<>();
                for (int i = 0; i < chunks; i++) {
                    pieces.add(Arrays.copyOfRange(data, i * CHUNK, (i + 1) * CHUNK));
                }
                uploaded.put(id, pieces);
                return id;
            } finally {
                files.deleteWorkspace(workspace);
            }
        }

        byte[] content(String id, int chunk) {
            return uploaded.get(id).get(chunk);
        }

        RepairReport repair(boolean dryRun) throws IOException {
            return repair.repair(dryRun);
        }

        Path copy(int node, String chunkId) {
            return nodes.get(node).dir().resolve(chunkId);
        }

        List<Integer> holders(String chunkId) {
            List<Integer> holders = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                if (Files.exists(copy(i, chunkId))) {
                    holders.add(i);
                }
            }
            return holders;
        }

        void damage(int node, String chunkId) throws IOException {
            byte[] content = Files.readAllBytes(copy(node, chunkId));
            content[content.length / 2] ^= 0x10;
            Files.write(copy(node, chunkId), content);
        }

        /** Puts {@code count} unrelated canonical chunks on a node, so it counts as more loaded. */
        void addLoad(int node, int count) throws IOException {
            for (int i = 0; i < count; i++) {
                Files.write(copy(node, "e".repeat(32) + "_" + loadCounter++), new byte[] {1});
            }
        }

        void markDown(int node) {
            nodes.get(node).down = true;
            for (int i = 0; i < 3; i++) {
                monitor.checkNow();
            }
        }

        /** Every file on every node, with its content's hash. */
        Map<String, String> tree() throws IOException {
            Map<String, String> tree = new TreeMap<>();
            for (int i = 0; i < nodes.size(); i++) {
                for (String name : NodeTestKit.listNames(nodes.get(i).dir())) {
                    tree.put(i + "/" + name, NodeTestKit.sha256(Files.readAllBytes(copy(i, name))));
                }
            }
            return tree;
        }
    }
}
