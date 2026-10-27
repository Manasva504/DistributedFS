package com.distributedfs;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

/**
 * Repair: brings every chunk of every committed file back to the cluster's replication factor R,
 * using only nodes the heartbeat ({@link HeartbeatMonitor}) reports UP. Started by hand
 * ({@code POST /cluster/repair}); a dry run finds and counts everything but changes nothing.
 *
 * <p><b>Per chunk</b> (files with a pending marker, and format 1 files, which have no chunk
 * checksums, are skipped):
 * <ol>
 *   <li>Read every copy on the UP nodes into repair's own workspace and check its SHA-256
 *       against the manifest: each copy is good or damaged.</li>
 *   <li>No good copy: the chunk is lost (when every node is UP; otherwise a DOWN node may hold one,
 *       and the chunk counts as uncertain). Damaged copies are kept as evidence.</li>
 *   <li>Damaged copies, with a good one: each is deleted, and replaced by a new copy.</li>
 *   <li>Fewer than R good copies: new copies are made from the verified good copy, on UP nodes
 *       that hold no copy, the least loaded first (the node of a damaged copy last). Each new copy
 *       is read back and checked; a bad one is deleted and the next node is tried.</li>
 *   <li>More than R good copies (a node came back with copies made again meanwhile): the extras
 *       on the most loaded nodes are deleted, but only while every node is UP and none failed in
 *       this run, so exactly R verified copies remain.</li>
 * </ol>
 *
 * <p><b>Failures.</b> A node that fails during the run is not used again in it; chunks that
 * needed it are counted as uncertain and left as they are. A store whose outcome is unknown ends
 * the work on that chunk (at worst it now has an extra copy, which a later run removes).
 *
 * <p><b>Locking.</b> Each chunk is repaired while holding the storage lock that uploads and
 * downloads use ({@link FileService#withStorageLock}), so no request works on a chunk while
 * repair does, and requests wait for at most one chunk's repair. Repair only touches chunks of
 * committed files, which nothing else changes. Only one repair runs at a time.
 *
 * <p><b>Cost.</b> Every copy of every chunk is read, dry runs included.
 */
final class RepairService {

    /** Thrown when a repair is asked for while one is running. */
    static final class RepairInProgressException extends IOException {
        private static final long serialVersionUID = 1L;

        RepairInProgressException() {
            super("a repair is already running");
        }
    }

    private final StorageCluster cluster;
    private final ManifestStore manifests;
    private final FileService files;
    private final Path tmpDir;
    private final HeartbeatMonitor monitor;
    private final PrintStream err;
    private final AtomicBoolean running = new AtomicBoolean();

    RepairService(StorageCluster cluster, ManifestStore manifests, FileService files, Path tmpDir,
                  HeartbeatMonitor monitor, PrintStream err) {
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.manifests = Objects.requireNonNull(manifests, "manifests must not be null");
        this.files = Objects.requireNonNull(files, "files must not be null");
        this.tmpDir = Objects.requireNonNull(tmpDir, "tmpDir must not be null");
        this.monitor = Objects.requireNonNull(monitor, "monitor must not be null");
        this.err = Objects.requireNonNull(err, "err must not be null");
    }

    /**
     * Runs one repair (or a dry run) and returns its report.
     *
     * @throws RepairInProgressException if a repair is already running
     * @throws IOException if the manifests cannot be listed or the workspace cannot be created
     */
    RepairReport repair(boolean dryRun) throws IOException {
        if (!running.compareAndSet(false, true)) {
            throw new RepairInProgressException();
        }
        try {
            return new Run(dryRun).run();
        } finally {
            running.set(false);
        }
    }

    /** One run's state and counts. */
    private final class Run {
        final boolean dryRun;
        final long started = System.nanoTime();
        final List<HeartbeatMonitor.NodeHealth> health = monitor.snapshot();
        final boolean everyNodeUp = health.stream().allMatch(h -> h.state() == HeartbeatMonitor.State.UP);
        final Set<Integer> failed = new TreeSet<>();
        final Map<String, List<Integer>> holders = new HashMap<>();
        final int[] chunksPerNode = new int[cluster.nodeCount()];
        final List<String> lost = new ArrayList<>();
        final ChecksumService checksums = new ChecksumService();
        int filesChecked, filesSkipped, chunksChecked, chunksHealthy, copiesAdded, copiesReplaced, copiesRemoved;
        int chunksLost, chunksUncertain, chunksNotRepaired;
        boolean trimSkipped;
        Path workspace;

        Run(boolean dryRun) {
            this.dryRun = dryRun;
        }

        boolean up(int node) {
            return health.get(node).state() == HeartbeatMonitor.State.UP && !failed.contains(node);
        }

        RepairReport run() throws IOException {
            listCopies();
            Set<String> manifestIds = new TreeSet<>();
            Set<String> pendingIds = new HashSet<>();
            for (String name : manifests.listEntryNames()) {
                if (name.endsWith(ManifestStore.MANIFEST_SUFFIX)) {
                    manifestIds.add(name.substring(0, name.length() - ManifestStore.MANIFEST_SUFFIX.length()));
                } else if (name.endsWith(ManifestStore.PENDING_SUFFIX)) {
                    pendingIds.add(name.substring(0, name.length() - ManifestStore.PENDING_SUFFIX.length()));
                }
            }
            workspace = Files.createTempDirectory(tmpDir, "repair-");
            try {
                for (String fileId : manifestIds) {
                    if (!FileIds.isValid(fileId) || pendingIds.contains(fileId)) {
                        filesSkipped++; // recovery is responsible for pending uploads
                        continue;
                    }
                    Optional<FileManifest> manifest;
                    try {
                        manifest = manifests.read(fileId);
                    } catch (IOException | RuntimeException e) {
                        err.println("Warning: repair skipped file " + fileId + ": its manifest could not be read: " + e);
                        filesSkipped++;
                        continue;
                    }
                    if (manifest.isEmpty() || !manifest.get().hasChunkChecksums()) {
                        filesSkipped++; // a format 1 file: its copies cannot be checked one by one
                        continue;
                    }
                    filesChecked++;
                    FileManifest m = manifest.get();
                    for (int i = 0; i < m.chunkCount(); i++) {
                        String chunkId = m.chunkId(i);
                        String expected = m.chunkSha256s().get(i);
                        files.withStorageLock(() -> {
                            repairChunk(chunkId, expected);
                            return null;
                        });
                    }
                }
            } finally {
                files.deleteWorkspace(workspace);
            }
            List<RepairReport.NodeState> states = new ArrayList<>();
            for (HeartbeatMonitor.NodeHealth h : health) {
                states.add(new RepairReport.NodeState(h.index(), h.state()));
            }
            return new RepairReport(dryRun, cluster.replicas(), states, filesChecked, filesSkipped, chunksChecked,
                    chunksHealthy, copiesAdded, copiesReplaced, copiesRemoved, chunksLost, chunksUncertain,
                    chunksNotRepaired, trimSkipped, List.copyOf(failed), List.copyOf(lost),
                    (System.nanoTime() - started) / 1_000_000);
        }

        /** One listing per UP node: which canonical chunks it holds as regular files, and how many. */
        void listCopies() {
            for (int node = 0; node < cluster.nodeCount(); node++) {
                if (!up(node)) {
                    continue;
                }
                try {
                    for (ChunkStore.Entry entry : cluster.node(node).listEntries()) {
                        Matcher chunk = StartupRecovery.CANONICAL_CHUNK.matcher(entry.name());
                        if (chunk.matches() && entry.kind() == ChunkStore.EntryKind.FILE) {
                            holders.computeIfAbsent(entry.name(), k -> new ArrayList<>()).add(node);
                            chunksPerNode[node]++;
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    nodeFailed(node, "listing its chunks", e);
                }
            }
        }

        void nodeFailed(int node, String what, Exception e) {
            if (failed.add(node)) {
                err.println("Warning: repair stopped using node " + node + " (" + cluster.node(node).describe()
                        + ") after a failure while " + what + ": " + e);
            }
        }

        void repairChunk(String chunkId, String expected) throws IOException {
            chunksChecked++;
            int replicas = cluster.replicas();
            List<Integer> good = new ArrayList<>();
            List<Integer> damaged = new ArrayList<>();
            Path goodCopy = null;
            boolean unreadable = false;
            for (int node : holders.getOrDefault(chunkId, List.of())) {
                if (!up(node)) {
                    unreadable = true;
                    continue;
                }
                Path copy = workspace.resolve(chunkId + "-" + node);
                try {
                    cluster.node(node).retrieveChunk(chunkId, copy);
                } catch (ChunkNotFoundException e) {
                    continue; // gone since the listing
                } catch (IOException | RuntimeException e) {
                    nodeFailed(node, "reading chunk " + chunkId, e);
                    unreadable = true;
                    continue;
                }
                if (checksums.calculateSha256(copy).equals(expected)) {
                    good.add(node);
                    if (goodCopy == null) {
                        goodCopy = copy;
                        continue;
                    }
                } else {
                    damaged.add(node);
                    err.println("Warning: repair found a damaged copy of chunk " + chunkId + " on node " + node);
                }
                Files.delete(copy);
            }
            try {
                if (unreadable) {
                    chunksUncertain++; // a copy that could not be read may be good or damaged: left as it is
                    return;
                }
                if (good.isEmpty()) {
                    if (everyNodeUp && failed.isEmpty()) {
                        chunksLost++;
                        if (lost.size() < RepairReport.MAX_LISTED) {
                            lost.add(chunkId);
                        }
                        err.println("Warning: chunk " + chunkId + " is lost: no node has a good copy"
                                + (damaged.isEmpty() ? "" : "; its damaged copies on nodes " + damaged + " were kept"));
                    } else {
                        chunksUncertain++; // a node that is DOWN may hold a good copy
                    }
                    return;
                }
                if (damaged.isEmpty() && good.size() == replicas) {
                    chunksHealthy++;
                    return;
                }
                int toReplace = 0;
                for (int node : damaged) {
                    if (!dryRun) {
                        try {
                            cluster.node(node).deleteChunk(chunkId);
                        } catch (IOException | RuntimeException e) {
                            nodeFailed(node, "deleting a damaged copy of " + chunkId, e);
                            chunksUncertain++;
                            return;
                        }
                    }
                    chunksPerNode[node]--;
                    toReplace++;
                }
                Set<Integer> tried = new HashSet<>();
                while (good.size() < replicas) {
                    Integer target = chooseTarget(good, damaged, tried);
                    if (target == null) {
                        chunksNotRepaired++;
                        copiesRemoved += toReplace; // damaged copies deleted without a replacement
                        return;
                    }
                    tried.add(target);
                    Boolean stored = dryRun ? Boolean.TRUE : storeVerified(chunkId, expected, goodCopy, target);
                    if (stored == null) {
                        chunksUncertain++; // the store's outcome is unknown
                        return;
                    }
                    if (stored) {
                        good.add(target);
                        chunksPerNode[target]++;
                        if (toReplace > 0) {
                            toReplace--;
                            copiesReplaced++;
                        } else {
                            copiesAdded++;
                        }
                    }
                }
                copiesRemoved += toReplace;
                if (good.size() > replicas) {
                    if (!everyNodeUp || !failed.isEmpty()) {
                        trimSkipped = true;
                        return;
                    }
                    List<Integer> extras = new ArrayList<>(good);
                    extras.sort((a, b) -> chunksPerNode[b] != chunksPerNode[a] ? chunksPerNode[b] - chunksPerNode[a] : b - a);
                    for (int node : extras.subList(0, good.size() - replicas)) {
                        if (!dryRun) {
                            try {
                                cluster.node(node).deleteChunk(chunkId);
                            } catch (IOException | RuntimeException e) {
                                nodeFailed(node, "deleting an extra copy of " + chunkId, e);
                                chunksUncertain++;
                                return;
                            }
                        }
                        chunksPerNode[node]--;
                        copiesRemoved++;
                    }
                }
            } finally {
                if (goodCopy != null) {
                    Files.deleteIfExists(goodCopy);
                }
            }
        }

        /**
         * An UP node that holds no good copy, has not failed, and was not tried for this chunk:
         * the one with the fewest chunks (the lowest index on a tie), a node whose copy was damaged
         * only if no other node qualifies. Null if there is none.
         */
        Integer chooseTarget(List<Integer> good, List<Integer> damaged, Set<Integer> tried) {
            Integer best = null;
            for (boolean allowDamaged : new boolean[] {false, true}) {
                for (int node = 0; node < cluster.nodeCount(); node++) {
                    if (!up(node) || good.contains(node) || tried.contains(node) || damaged.contains(node) != allowDamaged) {
                        continue;
                    }
                    if (best == null || chunksPerNode[node] < chunksPerNode[best]) {
                        best = node;
                    }
                }
                if (best != null) {
                    return best;
                }
            }
            return null;
        }

        /**
         * Stores the verified copy on {@code target} and reads it back to check it. True if the new
         * copy is good; false if it certainly failed (nothing left behind); null if the store's
         * outcome is unknown.
         */
        Boolean storeVerified(String chunkId, String expected, Path goodCopy, int target) {
            ChunkStore node = cluster.node(target);
            try {
                node.storeChunk(chunkId, goodCopy);
            } catch (AmbiguousStoreException e) {
                nodeFailed(target, "storing a copy of " + chunkId, e);
                return null;
            } catch (IOException | RuntimeException e) {
                err.println("Warning: repair could not store a copy of chunk " + chunkId + " on node " + target + ": " + e);
                return false;
            }
            Path check = workspace.resolve(chunkId + "-check-" + target);
            try {
                node.retrieveChunk(chunkId, check);
                if (checksums.calculateSha256(check).equals(expected)) {
                    return true;
                }
                err.println("Warning: the new copy of chunk " + chunkId + " on node " + target
                        + " did not match its checksum and was deleted");
                node.deleteChunk(chunkId);
                return false;
            } catch (IOException | RuntimeException e) {
                nodeFailed(target, "checking the new copy of " + chunkId, e);
                return null;
            } finally {
                try {
                    Files.deleteIfExists(check);
                } catch (IOException ignored) {
                    // the workspace is deleted at the end of the run
                }
            }
        }
    }
}
