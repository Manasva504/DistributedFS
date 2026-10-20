package com.distributedfs;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Startup recovery: finishes or undoes uploads that were interrupted (their pending-upload marker
 * is still there), removes temp files that killed processes left behind, and reports chunks that
 * nothing refers to. The server runs it once, holding the data directory's lock, after the storage
 * is set up and before anything listens, so no upload can run at the same time.
 *
 * <p><b>Steps.</b>
 * <ol>
 *   <li><b>List</b> the manifest folder and every node folder once. If any listing fails, recovery
 *       stops with nothing changed.</li>
 *   <li><b>Assess every marker before acting on any.</b> For each pending file ID F (sorted):
 *       read the marker strictly (a corrupt or unreadable marker is unsafe; its chunk count is never
 *       guessed). Read F's manifest: unreadable or invalid is unsafe; present means the upload
 *       <i>committed</i>; confirmed absent means it is <i>incomplete</i>.
 *       <ul>
 *         <li>Committed: the manifest's chunk count must equal the marker's, and each chunk
 *             {@code F_i} (i &lt; chunkCount) must be a regular file on exactly one node, with every
 *             node answering. The only action is deleting the marker. Canonical extra chunks
 *             {@code F_k} (k &ge; chunkCount) are left alone and reported by the chunk scan.</li>
 *         <li>Incomplete: the candidates are {@code F_0 .. F_(n-1)} for the marker's chunk count n.
 *             Each one present must be a regular file on exactly one node. Every other entry in
 *             F's namespace ({@code F_...}) is unsafe, except node temp files of candidates, which
 *             the temp sweep removes. The actions are deleting the present candidates, highest
 *             index first, then the marker.</li>
 *       </ul>
 *       If any marker is unsafe, recovery stops with nothing changed: {@link UnsafeRecoveryException}.</li>
 *   <li><b>Act</b>, marker by marker. The marker is deleted last, only once every chunk delete
 *       succeeded. If an action fails, F is checked again, read-only. When that re-check shows the
 *       same understood state (same kind, same marker and manifest, every remaining chunk a regular
 *       file on exactly one node or absent, nothing unexpected), the cleanup is deferred: the marker
 *       is kept so the next start retries, a warning is logged, and recovery continues. Anything
 *       else stops recovery at once, and the exception says which uploads were already resolved.</li>
 *   <li><b>Sweep stale temp files</b> in the manifest folder, each node folder and the tmp folder
 *       (see {@link StaleTempFiles}). Junctions, links and other reparse points are never deleted or
 *       entered. Failures are warnings only: temp files hold no recovery information.</li>
 *   <li><b>Scan for unreferenced chunks</b> (report only) with a fresh listing. Only names of the
 *       exact form {@code <32 lowercase hex>_<index without leading zeros>} are considered, and
 *       chunks of uploads still pending are skipped. {@code ORPHAN_CHUNK}: the file ID has neither a
 *       manifest nor a pending marker. {@code UNREFERENCED_EXTRA_CHUNK}: the file has a readable
 *       manifest and the index is at least its chunk count. A manifest that cannot be read is
 *       reported as {@code UNCHECKED_FILE}, and its chunks are not classified. The scan never
 *       deletes or changes anything and never stops the start.</li>
 *   <li><b>Report</b> a one-line summary when anything was done.</li>
 * </ol>
 * Nothing is printed when there was nothing to do and nothing to report.
 *
 * <p><b>Why recovery cannot delete another upload's chunks.</b> Chunk {@code i} of file F is
 * stored as {@code F_i}, and every file ID is exactly 32 characters, so {@code F_i} and
 * {@code G_j} can only be equal when F and G are the same file. Recovery deletes only candidates
 * of a marker's own file ID and chunk count, only while that file has no manifest, and only
 * through StorageCluster.deleteChunk, which deletes a regular file held by exactly one node.
 */
final class StartupRecovery {

    /** A chunk name of a server-stored file: {@code <32 lowercase hex>_<index without leading zeros>}. */
    static final Pattern CANONICAL_CHUNK = Pattern.compile("([0-9a-f]{32})_(0|[1-9][0-9]*)");

    /** A node temp file of such a chunk: StorageNode writes {@code <chunkId>-<1 to 20 digits>.tmp}. */
    private static final Pattern CANONICAL_CHUNK_TEMP = Pattern.compile("([0-9a-f]{32})_(0|[1-9][0-9]*)-[0-9]{1,20}\\.tmp");

    private static final Pattern MANIFEST_ENTRY =
            Pattern.compile("([0-9a-f]{32})" + Pattern.quote(ManifestStore.MANIFEST_SUFFIX));
    private static final Pattern PENDING_ENTRY =
            Pattern.compile("([0-9a-f]{32})" + Pattern.quote(ManifestStore.PENDING_SUFFIX));

    /**
     * FileService.newUploadWorkspace and newDownloadWorkspace: {@code Files.createTempDirectory(tmp,
     * "upload-")} or {@code "download-"}, so 1 to 20 digits.
     */
    private static final Pattern WORKSPACE_NAME = Pattern.compile("(upload|download)-[0-9]{1,20}");

    /** What the chunk scan found. */
    enum FindingType {
        /** The chunk's file ID has neither a manifest nor a pending marker. */
        ORPHAN_CHUNK,
        /** The chunk's file has a readable manifest, and the chunk's index is at least its chunk count. */
        UNREFERENCED_EXTRA_CHUNK
    }

    /** One chunk the scan reports; {@code note} is extra detail, or null. */
    record Finding(FindingType type, String chunkId, int node, Path path, String note) {
    }

    /**
     * What one run did.
     *
     * @param committed        interrupted uploads found committed; only their markers were deleted
     * @param rolledBack       interrupted uploads that were incomplete and were removed
     * @param chunksDeleted    chunks deleted while removing incomplete uploads
     * @param deferred         interrupted uploads whose cleanup could not finish; their markers were kept
     * @param tempItemsRemoved stale temp files and workspaces removed
     * @param findings         the chunk scan's findings
     * @param uncheckedFiles   file IDs whose manifest could not be read, so their chunks were not classified
     */
    record Result(List<String> committed, List<String> rolledBack, int chunksDeleted, List<String> deferred,
                  int tempItemsRemoved, List<Finding> findings, List<String> uncheckedFiles) {
    }

    private final List<StorageNode> nodes;
    private final StorageCluster cluster;
    private final ManifestStore manifests;
    private final Path tmpDir;
    private final PrintStream out;
    private final PrintStream err;

    /**
     * @param nodes     the storage nodes, in the same order as in {@code cluster}
     * @param cluster   the cluster built from {@code nodes}
     * @param manifests the manifest store
     * @param tmpDir    the folder holding request workspaces
     */
    StartupRecovery(List<StorageNode> nodes, StorageCluster cluster, ManifestStore manifests, Path tmpDir,
                    PrintStream out, PrintStream err) {
        this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes must not be null"));
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.manifests = Objects.requireNonNull(manifests, "manifests must not be null");
        this.tmpDir = Objects.requireNonNull(tmpDir, "tmpDir must not be null").toAbsolutePath().normalize();
        this.out = Objects.requireNonNull(out, "out must not be null");
        this.err = Objects.requireNonNull(err, "err must not be null");
    }

    /**
     * Runs every step described in the class comment.
     *
     * @throws UnsafeRecoveryException if recovery could not establish that acting was safe; the
     *         server must not start
     */
    Result run() throws UnsafeRecoveryException {
        // Step 1: list.
        Snapshot before;
        try {
            before = snapshot();
        } catch (IOException | RuntimeException e) {
            throw new UnsafeRecoveryException("Startup recovery could not list the data directory, so it cannot tell"
                    + " whether any interrupted upload needs recovery. The server will not start. Nothing was changed."
                    + " Cause: " + e, List.of(), false, e);
        }

        // Step 2: assess every marker before acting on any.
        List<Assessment> plans = new ArrayList<>();
        Map<String, String> unsafe = new LinkedHashMap<>();
        Throwable firstCause = null;
        for (String fileId : before.pendingIds()) {
            try {
                plans.add(assess(fileId, before));
            } catch (Unsafe e) {
                unsafe.put(fileId, e.getMessage());
                if (firstCause == null) {
                    firstCause = e.getCause();
                }
            }
        }
        if (!unsafe.isEmpty()) {
            StringBuilder message = new StringBuilder("Startup recovery found ").append(unsafe.size())
                    .append(" interrupted upload(s) it cannot recover safely, so the server will not start."
                            + " Nothing was changed.");
            unsafe.forEach((fileId, reason) -> message.append("\n  ").append(fileId).append(": ").append(reason));
            message.append("\nInspect these entries; recovery never deletes what it cannot explain.");
            throw new UnsafeRecoveryException(message.toString(), List.copyOf(unsafe.keySet()), false, firstCause);
        }

        // Step 3: act.
        List<String> committed = new ArrayList<>();
        List<String> rolledBack = new ArrayList<>();
        List<String> deferred = new ArrayList<>();
        int chunksDeleted = 0;
        for (int index = 0; index < plans.size(); index++) {
            Assessment plan = plans.get(index);
            Progress progress = new Progress();
            try {
                execute(plan, progress);
                (plan.kind() == Kind.COMMITTED ? committed : rolledBack).add(plan.fileId());
            } catch (ActionFailure failure) {
                String unsafeReason = failure.deferrable ? recheck(plan) : "the failure was unexpected";
                if (unsafeReason != null) {
                    throw stoppedWhileActing(plan, failure, unsafeReason, committed, rolledBack, deferred,
                            plans.subList(index + 1, plans.size()), chunksDeleted + progress.deleted);
                }
                deferred.add(plan.fileId());
                warnDeferred(plan, failure);
            } finally {
                chunksDeleted += progress.deleted;
            }
        }

        // Step 4: sweep stale temp files.
        int tempItemsRemoved = sweepStaleTempFiles();

        // Step 5: scan for unreferenced chunks (report only).
        List<Finding> findings = new ArrayList<>();
        Set<String> uncheckedFiles = new TreeSet<>();
        scanChunks(findings, uncheckedFiles);
        printScan(findings, uncheckedFiles);

        // Step 6: summary.
        if (!plans.isEmpty() || tempItemsRemoved > 0) {
            out.println("Recovery: " + plans.size() + " interrupted upload(s): " + committed.size() + " committed, "
                    + rolledBack.size() + " rolled back (" + chunksDeleted + " chunk(s) deleted), " + deferred.size()
                    + " deferred; " + tempItemsRemoved + " stale temp file(s) or workspace(s) removed");
        }
        return new Result(List.copyOf(committed), List.copyOf(rolledBack), chunksDeleted, List.copyOf(deferred),
                tempItemsRemoved, List.copyOf(findings), List.copyOf(uncheckedFiles));
    }

    // ---------------------------------------------------------------------
    // Step 1: listing
    // ---------------------------------------------------------------------

    /** One listing of the manifest folder and of every node folder. */
    private record Snapshot(Set<String> manifestIds, List<String> pendingIds, List<List<String>> nodeNames,
                            List<Set<String>> nodeNameSets) {
    }

    private Snapshot snapshot() throws IOException {
        Set<String> manifestIds = new HashSet<>();
        List<String> pendingIds = new ArrayList<>();
        for (String name : manifests.listEntryNames()) {
            Matcher manifest = MANIFEST_ENTRY.matcher(name);
            Matcher pending = PENDING_ENTRY.matcher(name);
            if (manifest.matches()) {
                manifestIds.add(manifest.group(1));
            } else if (pending.matches()) {
                pendingIds.add(pending.group(1));
            }
        }
        List<List<String>> nodeNames = new ArrayList<>();
        List<Set<String>> nodeNameSets = new ArrayList<>();
        for (StorageNode node : nodes) {
            List<String> names = node.listEntryNames();
            nodeNames.add(names);
            nodeNameSets.add(new HashSet<>(names));
        }
        return new Snapshot(manifestIds, pendingIds, nodeNames, nodeNameSets);
    }

    // ---------------------------------------------------------------------
    // Step 2: assessment (read-only)
    // ---------------------------------------------------------------------

    private enum Kind { COMMITTED, INCOMPLETE }

    /** A chunk to delete and the node that holds it. */
    private record Holder(String chunkId, int node) {
    }

    /** What recovery found for one marker and will do about it. */
    private record Assessment(String fileId, Kind kind, ManifestStore.PendingUpload marker, FileManifest manifest,
                              List<Holder> chunksToDelete) {
    }

    /** The marker cannot be recovered safely; the message says why. */
    private static final class Unsafe extends Exception {
        private static final long serialVersionUID = 1L;

        Unsafe(String reason, Throwable cause) {
            super(reason, cause);
        }
    }

    private Assessment assess(String fileId, Snapshot snapshot) throws Unsafe {
        ManifestStore.PendingUpload marker;
        try {
            Optional<ManifestStore.PendingUpload> read = manifests.readPending(fileId);
            if (read.isEmpty()) {
                throw new Unsafe("its pending marker disappeared while recovery was reading it", null);
            }
            marker = read.get();
        } catch (IOException | RuntimeException e) {
            throw new Unsafe("its pending marker is corrupt or unreadable, and its chunk count is never guessed ("
                    + e + ")", e);
        }
        Optional<FileManifest> manifest;
        try {
            manifest = manifests.read(fileId);
        } catch (IOException | RuntimeException e) {
            throw new Unsafe("its manifest is invalid or unreadable (" + e + ")", e);
        }
        int chunkCount = marker.chunkCount();
        if (manifest.isPresent()) {
            if (manifest.get().chunkCount() != chunkCount) {
                throw new Unsafe("it is committed, but its manifest has chunkCount " + manifest.get().chunkCount()
                        + " and its pending marker has " + chunkCount, null);
            }
            checkNamespace(fileId, chunkCount, true, snapshot);
            verifyCommittedChunks(fileId, chunkCount, snapshot);
            return new Assessment(fileId, Kind.COMMITTED, marker, manifest.get(), List.of());
        }
        checkNamespace(fileId, chunkCount, false, snapshot);
        return new Assessment(fileId, Kind.INCOMPLETE, marker, null, presentCandidates(fileId, chunkCount, snapshot));
    }

    /**
     * Every entry named {@code F_...} must be expected: a candidate {@code F_i} (i &lt; chunkCount),
     * a node temp file of a candidate, or, for a committed upload only, a canonical extra chunk.
     */
    private void checkNamespace(String fileId, int chunkCount, boolean committed, Snapshot snapshot) throws Unsafe {
        String prefix = fileId + "_";
        for (int node = 0; node < nodes.size(); node++) {
            for (String name : snapshot.nodeNames().get(node)) {
                if (!name.startsWith(prefix)) {
                    continue;
                }
                Matcher chunk = CANONICAL_CHUNK.matcher(name);
                if (chunk.matches()) {
                    if (indexBelow(chunk.group(2), chunkCount) || committed) {
                        continue; // a candidate, or an extra chunk of a committed file (report-only)
                    }
                } else {
                    Matcher temp = CANONICAL_CHUNK_TEMP.matcher(name);
                    if (temp.matches() && indexBelow(temp.group(2), chunkCount)) {
                        continue; // a store of a candidate was interrupted; the temp sweep removes it
                    }
                }
                throw new Unsafe("an unexpected entry \"" + name + "\" is in its namespace on node " + node + " ("
                        + nodePath(node, name) + ")", null);
            }
        }
    }

    /** D3: every chunk of a committed upload must be a regular file on exactly one node. */
    private void verifyCommittedChunks(String fileId, int chunkCount, Snapshot snapshot) throws Unsafe {
        for (int i = 0; i < chunkCount; i++) {
            String chunkId = FileIds.chunkId(fileId, i);
            List<Integer> listed = listedOn(chunkId, snapshot);
            if (listed.isEmpty()) {
                throw new Unsafe("it is committed, but its chunk " + chunkId + " is missing from every node", null);
            }
            if (listed.size() > 1) {
                throw new Unsafe("it is committed, but its chunk " + chunkId + " is on more than one node "
                        + listed, null);
            }
            List<Integer> holders = regularHolders(chunkId);
            if (!holders.equals(listed)) {
                throw new Unsafe("it is committed, but its chunk " + chunkId + " changed while recovery was checking"
                        + " it (listed on nodes " + listed + ", held as a regular file by nodes " + holders + ")", null);
            }
        }
    }

    /** The candidates of an incomplete upload that are present, highest index first. */
    private List<Holder> presentCandidates(String fileId, int chunkCount, Snapshot snapshot) throws Unsafe {
        TreeSet<Integer> indexes = new TreeSet<>();
        for (List<String> names : snapshot.nodeNames()) {
            for (String name : names) {
                Matcher chunk = CANONICAL_CHUNK.matcher(name);
                if (chunk.matches() && chunk.group(1).equals(fileId) && indexBelow(chunk.group(2), chunkCount)) {
                    indexes.add(Integer.parseInt(chunk.group(2)));
                }
            }
        }
        List<Holder> holders = new ArrayList<>();
        for (int index : indexes.descendingSet()) {
            String chunkId = FileIds.chunkId(fileId, index);
            List<Integer> listed = listedOn(chunkId, snapshot);
            if (listed.size() > 1) {
                throw new Unsafe("its chunk " + chunkId + " is on more than one node " + listed, null);
            }
            List<Integer> regular = regularHolders(chunkId);
            if (!regular.equals(listed)) {
                throw new Unsafe("its chunk " + chunkId + " changed while recovery was checking it (listed on nodes "
                        + listed + ", held as a regular file by nodes " + regular + ")", null);
            }
            holders.add(new Holder(chunkId, listed.get(0)));
        }
        return holders;
    }

    private List<Integer> listedOn(String chunkId, Snapshot snapshot) {
        List<Integer> listed = new ArrayList<>();
        for (int node = 0; node < nodes.size(); node++) {
            if (snapshot.nodeNameSets().get(node).contains(chunkId)) {
                listed.add(node);
            }
        }
        return listed;
    }

    /** Asks every node, without following links; a node that cannot answer, or a non-regular entry, is unsafe. */
    private List<Integer> regularHolders(String chunkId) throws Unsafe {
        List<Integer> holders = new ArrayList<>();
        for (int node = 0; node < nodes.size(); node++) {
            try {
                if (nodes.get(node).holdsRegularChunkFile(chunkId)) {
                    holders.add(node);
                }
            } catch (IOException | RuntimeException e) {
                throw new Unsafe("node " + node + " could not confirm its chunk " + chunkId
                        + " as a regular file or absent (" + e + ")", e);
            }
        }
        return holders;
    }

    /** True if the canonical decimal {@code digits} is less than {@code limit}, without overflow. */
    private static boolean indexBelow(String digits, int limit) {
        if (digits.length() > 10) {
            return false; // more than 10 digits is beyond any int
        }
        return Long.parseLong(digits) < limit;
    }

    // ---------------------------------------------------------------------
    // Step 3: acting
    // ---------------------------------------------------------------------

    /** How many chunks this marker's cleanup has deleted so far. */
    private static final class Progress {
        int deleted;
    }

    /** An action failed. {@code deferrable} is false for unexpected (runtime) failures. */
    private static final class ActionFailure extends Exception {
        private static final long serialVersionUID = 1L;
        final boolean deferrable;

        ActionFailure(String step, Exception cause, boolean deferrable) {
            super(step + ": " + cause, cause);
            this.deferrable = deferrable;
        }
    }

    private void execute(Assessment plan, Progress progress) throws ActionFailure {
        for (Holder holder : plan.chunksToDelete()) {
            String step = "deleting chunk " + holder.chunkId() + " on node " + holder.node();
            try {
                if (cluster.deleteChunk(holder.chunkId())) {
                    progress.deleted++;
                }
            } catch (IOException e) {
                throw new ActionFailure(step, e, true);
            } catch (RuntimeException e) {
                throw new ActionFailure(step, e, false);
            }
        }
        String step = "deleting the pending marker";
        try {
            manifests.deletePending(plan.fileId());
        } catch (IOException e) {
            throw new ActionFailure(step, e, true);
        } catch (RuntimeException e) {
            throw new ActionFailure(step, e, false);
        }
    }

    /**
     * D1: after a failed action, checks the upload again, read-only. Returns null if the state is
     * the same understood state (so the cleanup may be deferred), otherwise why it is not.
     */
    private String recheck(Assessment plan) {
        Snapshot now;
        try {
            now = snapshot();
        } catch (IOException | RuntimeException e) {
            return "the data directory could not be listed again (" + e + ")";
        }
        if (!now.pendingIds().contains(plan.fileId())) {
            return "its pending marker is no longer there";
        }
        Assessment again;
        try {
            again = assess(plan.fileId(), now);
        } catch (Unsafe e) {
            return e.getMessage();
        }
        if (again.kind() != plan.kind()) {
            return plan.kind() == Kind.INCOMPLETE ? "a manifest for it appeared" : "its manifest disappeared";
        }
        if (!again.marker().equals(plan.marker())) {
            return "its pending marker changed";
        }
        if (plan.kind() == Kind.COMMITTED && !again.manifest().equals(plan.manifest())) {
            return "its manifest changed";
        }
        return null;
    }

    private void warnDeferred(Assessment plan, ActionFailure failure) {
        StringBuilder remaining = new StringBuilder();
        if (plan.kind() == Kind.INCOMPLETE) {
            try {
                Snapshot now = snapshot();
                for (Holder holder : presentCandidates(plan.fileId(), plan.marker().chunkCount(), now)) {
                    remaining.append("\n  ").append(holder.chunkId()).append(" on node ").append(holder.node())
                            .append(" (").append(nodePath(holder.node(), holder.chunkId())).append(")");
                }
            } catch (IOException | RuntimeException | Unsafe e) {
                remaining.append("\n  (the remaining chunks could not be listed: ").append(e).append(")");
            }
        }
        err.println("Warning: the cleanup of interrupted upload " + plan.fileId() + " could not finish ("
                + failure.getMessage() + "). It was checked again and its state is understood, so the server starts;"
                + " its pending marker was kept, so the next start retries the cleanup."
                + (plan.kind() == Kind.COMMITTED
                        ? " The upload is committed and its file is intact; only the marker remains."
                        : remaining.length() == 0 ? " No chunks remain." : " Chunks that remain:" + remaining));
    }

    private UnsafeRecoveryException stoppedWhileActing(Assessment plan, ActionFailure failure, String unsafeReason,
                                                        List<String> committed, List<String> rolledBack,
                                                        List<String> deferred, List<Assessment> notReached,
                                                        int chunksDeleted) {
        List<String> notReachedIds = notReached.stream().map(Assessment::fileId).toList();
        boolean changed = chunksDeleted > 0 || !committed.isEmpty() || !rolledBack.isEmpty();
        String message = "Startup recovery stopped while cleaning up interrupted upload " + plan.fileId() + " ("
                + failure.getMessage() + "), because its state could not be confirmed as safe afterwards: "
                + unsafeReason + ". The server will not start."
                + "\n  Already resolved: " + committed.size() + " committed " + committed + ", " + rolledBack.size()
                + " rolled back " + rolledBack + "; deferred " + deferred + "; " + chunksDeleted + " chunk(s) deleted"
                + " in total."
                + "\n  In progress: " + plan.fileId() + " (its pending marker was kept)."
                + "\n  Not reached: " + notReachedIds + " (left untouched).";
        return new UnsafeRecoveryException(message, List.of(plan.fileId()), changed, failure.getCause());
    }

    // ---------------------------------------------------------------------
    // Step 4: stale temp files
    // ---------------------------------------------------------------------

    private int sweepStaleTempFiles() {
        Consumer<String> warn = message -> err.println("Warning: " + message);
        int removed = 0;
        try {
            removed += manifests.sweepStaleTempFiles(warn);
        } catch (IOException | RuntimeException e) {
            warn.accept("could not sweep stale temp files in the manifest folder: " + e);
        }
        for (int node = 0; node < nodes.size(); node++) {
            try {
                removed += nodes.get(node).sweepStaleTempFiles(warn);
            } catch (IOException | RuntimeException e) {
                warn.accept("could not sweep stale temp files on node " + node + ": " + e);
            }
        }
        return removed + sweepWorkspaces(warn);
    }

    /**
     * Deletes request workspaces ({@code upload-<digits>} and {@code download-<digits>}) left by a
     * killed server. Only plain folders are deleted, with FileTrees.deleteTree, which never follows
     * a link; a link, junction or other entry under such a name is left in place and reported.
     */
    private int sweepWorkspaces(Consumer<String> warn) {
        int removed = 0;
        try {
            if (!DataDirectory.isPlainDirectory(tmpDir)) {
                warn.accept("did not sweep the tmp folder: it is missing or not a plain folder: " + tmpDir);
                return 0;
            }
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(tmpDir)) {
                for (Path entry : entries) {
                    String name = entry.getFileName().toString();
                    if (!WORKSPACE_NAME.matcher(name).matches() || !tmpDir.equals(entry.getParent())) {
                        warn.accept("Left an unexpected entry in the tmp folder in place: " + entry);
                        continue;
                    }
                    try {
                        BasicFileAttributes attributes =
                                Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                        if (!attributes.isDirectory() || attributes.isOther() || attributes.isSymbolicLink()) {
                            warn.accept("Left a workspace-named entry that is not a plain folder in place, without"
                                    + " following it: " + entry);
                            continue;
                        }
                        FileTrees.deleteTree(entry);
                        removed++;
                    } catch (IOException e) {
                        warn.accept("Could not remove the stale workspace " + entry + ": " + e);
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            warn.accept("could not sweep the tmp folder: " + e);
        }
        return removed;
    }

    // ---------------------------------------------------------------------
    // Step 5: chunk scan (report only)
    // ---------------------------------------------------------------------

    private void scanChunks(List<Finding> findings, Set<String> uncheckedFiles) {
        Set<String> manifestIds = new HashSet<>();
        Set<String> pendingIds = new HashSet<>();
        try {
            for (String name : manifests.listEntryNames()) {
                Matcher manifest = MANIFEST_ENTRY.matcher(name);
                Matcher pending = PENDING_ENTRY.matcher(name);
                if (manifest.matches()) {
                    manifestIds.add(manifest.group(1));
                } else if (pending.matches()) {
                    pendingIds.add(pending.group(1));
                }
            }
        } catch (IOException | RuntimeException e) {
            err.println("Warning: the chunk scan was skipped: the manifest folder could not be listed: " + e);
            return;
        }
        Map<String, Integer> chunkCounts = new LinkedHashMap<>();
        for (int node = 0; node < nodes.size(); node++) {
            List<String> names;
            try {
                names = nodes.get(node).listEntryNames();
            } catch (IOException | RuntimeException e) {
                err.println("Warning: the chunk scan skipped node " + node + ": its folder could not be listed: " + e);
                continue;
            }
            for (String name : names) {
                Matcher chunk = CANONICAL_CHUNK.matcher(name);
                if (!chunk.matches()) {
                    continue;
                }
                String fileId = chunk.group(1);
                if (pendingIds.contains(fileId)) {
                    continue; // recovery is responsible for these
                }
                Path path = nodePath(node, name);
                if (!manifestIds.contains(fileId)) {
                    findings.add(new Finding(FindingType.ORPHAN_CHUNK, name, node, path, kindNote(path)));
                    continue;
                }
                if (uncheckedFiles.contains(fileId)) {
                    continue;
                }
                Integer chunkCount = chunkCounts.get(fileId);
                if (chunkCount == null) {
                    chunkCount = readChunkCount(fileId);
                    if (chunkCount == null) {
                        uncheckedFiles.add(fileId);
                        continue;
                    }
                    chunkCounts.put(fileId, chunkCount);
                }
                if (!indexBelow(chunk.group(2), chunkCount)) {
                    String note = "manifest chunkCount " + chunkCount;
                    String kind = kindNote(path);
                    findings.add(new Finding(FindingType.UNREFERENCED_EXTRA_CHUNK, name, node, path,
                            kind == null ? note : note + ", " + kind));
                }
            }
        }
    }

    /** The manifest's chunk count, or null if it cannot be read (a warning says why). */
    private Integer readChunkCount(String fileId) {
        try {
            Optional<FileManifest> manifest = manifests.read(fileId);
            if (manifest.isPresent()) {
                return manifest.get().chunkCount();
            }
            err.println("Warning: the chunk scan could not check file " + fileId + ": its manifest disappeared");
        } catch (IOException | RuntimeException e) {
            err.println("Warning: the chunk scan could not check file " + fileId + ": " + e);
        }
        return null;
    }

    /** "not a regular file" for links, junctions and folders; null for a regular file. Never follows links. */
    private static String kindNote(Path path) {
        try {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return attributes.isRegularFile() ? null : "not a regular file";
        } catch (IOException | RuntimeException e) {
            return "its type could not be read";
        }
    }

    private void printScan(List<Finding> findings, Set<String> uncheckedFiles) {
        if (findings.isEmpty() && uncheckedFiles.isEmpty()) {
            return;
        }
        long orphans = findings.stream().filter(f -> f.type() == FindingType.ORPHAN_CHUNK).count();
        long extras = findings.size() - orphans;
        out.println("Chunk scan: " + findings.size() + " finding(s): " + orphans + " " + FindingType.ORPHAN_CHUNK
                + ", " + extras + " " + FindingType.UNREFERENCED_EXTRA_CHUNK + "; " + uncheckedFiles.size()
                + " file(s) could not be checked. Report only: nothing was deleted or changed.");
        for (Finding finding : findings) {
            out.println("  " + finding.type() + " " + finding.chunkId() + " node " + finding.node() + " "
                    + finding.path() + (finding.note() == null ? "" : " (" + finding.note() + ")"));
        }
        for (String fileId : uncheckedFiles) {
            out.println("  UNCHECKED_FILE " + fileId + ": its manifest could not be read; its chunks were not classified");
        }
    }

    private Path nodePath(int node, String name) {
        return nodes.get(node).storageDir().resolve(name);
    }
}
