package com.distributedfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The read-only answers the web console polls: {@code GET /files} (committed files and their
 * health), {@code GET /files/<fileId>/placement} (where each chunk's copies are) and
 * {@code GET /cluster/events} (the activity feed).
 *
 * <p><b>Cost.</b> The files list reads the manifest folder's listing and each manifest (small text
 * files); the placement reads one manifest. Both use the shared {@link PlacementSnapshot}, which
 * lists each UP node at most once per period. None of them takes the storage lock or reads a
 * chunk's bytes, so they never wait for an upload, a download or a repair. The events are copied
 * from memory.
 *
 * <p><b>Health</b> counts, for every chunk, the copies on nodes that were reachable just now:
 * {@code healthy} when every chunk has exactly R (the replication factor), {@code over_replicated}
 * when every chunk has at least R and some more, {@code at_risk} when some chunk has fewer than R
 * but at least one, {@code unavailable} when some chunk has none while a node is unreachable (it
 * may hold one), and {@code missing} when some chunk has none and every node is reachable. Copies
 * are counted by name (see {@link PlacementSnapshot}).
 *
 * <p>A manifest is a committed file even if its pending marker was left behind (as for downloads);
 * a marker without a manifest is an upload in progress or interrupted, which is not listed. Every
 * string in the JSON is a file ID, a hash, a time or a fixed word: nothing a client sent.
 */
final class ConsoleApi {

    /** The most files {@code GET /files} lists; {@code truncated} says whether there were more. */
    static final int MAX_FILES = 1_000;

    private final ManifestStore manifests;
    private final PlacementSnapshot placement;
    private final ClusterEvents events;
    private final int replicas;

    ConsoleApi(ManifestStore manifests, PlacementSnapshot placement, ClusterEvents events, int replicas) {
        this.manifests = Objects.requireNonNull(manifests, "manifests must not be null");
        this.placement = Objects.requireNonNull(placement, "placement must not be null");
        this.events = Objects.requireNonNull(events, "events must not be null");
        this.replicas = replicas;
    }

    /** One listed file: its manifest and when it was committed (the manifest's time; null if unreadable). */
    private record Listed(FileManifest manifest, Instant committedAt) {
    }

    /** The body of {@code GET /files}. */
    String filesJson() throws IOException {
        List<String> names = manifests.listEntryNames();
        Set<String> nameSet = new HashSet<>(names);
        List<Listed> files = new ArrayList<>();
        int unreadable = 0;
        int pending = 0;
        for (String name : names) {
            if (name.endsWith(ManifestStore.PENDING_SUFFIX)) {
                String id = name.substring(0, name.length() - ManifestStore.PENDING_SUFFIX.length());
                if (FileIds.isValid(id) && !nameSet.contains(id + ManifestStore.MANIFEST_SUFFIX)) {
                    pending++;
                }
                continue;
            }
            if (!name.endsWith(ManifestStore.MANIFEST_SUFFIX)) {
                continue;
            }
            String id = name.substring(0, name.length() - ManifestStore.MANIFEST_SUFFIX.length());
            if (!FileIds.isValid(id)) {
                continue;
            }
            Optional<FileManifest> manifest;
            try {
                manifest = manifests.read(id);
            } catch (IOException | RuntimeException e) {
                unreadable++;
                continue;
            }
            if (manifest.isPresent()) {
                files.add(new Listed(manifest.get(), committedAt(name)));
            }
        }
        // Newest first; files whose time cannot be read last; then by ID, so the order is stable.
        files.sort(Comparator.comparing((Listed l) -> l.committedAt(), Comparator.nullsLast(Comparator.reverseOrder()))
                .thenComparing(l -> l.manifest().fileId()));
        boolean truncated = files.size() > MAX_FILES;
        if (truncated) {
            files = files.subList(0, MAX_FILES);
        }

        PlacementSnapshot.Snapshot snapshot = placement.get();
        StringBuilder json = new StringBuilder("{\"replicationFactor\":").append(replicas)
                .append(",\"listedAt\":").append(Json.quote(snapshot.takenAt().toString()))
                .append(",\"truncated\":").append(truncated)
                .append(",\"skipped\":{\"pending\":").append(pending).append(",\"unreadable\":").append(unreadable).append('}')
                .append(",\"nodes\":");
        appendNodes(json, snapshot, true);
        json.append(",\"files\":[");
        for (int f = 0; f < files.size(); f++) {
            FileManifest m = files.get(f).manifest();
            int min = Integer.MAX_VALUE;
            int max = 0;
            for (int i = 0; i < m.chunkCount(); i++) {
                int copies = reachableCopies(snapshot, m.chunkId(i));
                min = Math.min(min, copies);
                max = Math.max(max, copies);
            }
            Instant committedAt = files.get(f).committedAt();
            json.append(f == 0 ? "" : ",")
                    .append("{\"fileId\":").append(Json.quote(m.fileId()))
                    .append(",\"name\":").append(m.name() == null ? "null" : Json.quote(m.name()))
                    .append(",\"size\":").append(m.size())
                    .append(",\"chunkCount\":").append(m.chunkCount())
                    .append(",\"chunkSize\":").append(m.chunkSize())
                    .append(",\"sha256\":").append(Json.quote(m.sha256()))
                    .append(",\"format\":").append(m.format())
                    .append(",\"committedAt\":").append(committedAt == null ? "null" : Json.quote(committedAt.toString()))
                    .append(",\"health\":").append(Json.quote(health(m.chunkCount() == 0 ? replicas : min,
                            m.chunkCount() == 0 ? replicas : max, snapshot.everyNodeReachable())))
                    .append(",\"minCopies\":").append(m.chunkCount() == 0 ? "null" : Integer.toString(min))
                    .append(",\"maxCopies\":").append(m.chunkCount() == 0 ? "null" : Integer.toString(max))
                    .append('}');
        }
        return json.append("]}").toString();
    }

    /**
     * The body of {@code GET /files/<fileId>/placement}.
     *
     * @throws FileServiceException FILE_NOT_FOUND if no manifest exists, METADATA_CORRUPT if it
     *         cannot be read or is invalid
     */
    String placementJson(String fileId) throws FileServiceException {
        Optional<FileManifest> manifest;
        try {
            manifest = manifests.read(fileId);
        } catch (IOException | RuntimeException e) {
            throw new FileServiceException(FileServiceException.Reason.METADATA_CORRUPT,
                    "the manifest of file " + fileId + " could not be read", e);
        }
        if (manifest.isEmpty()) {
            throw new FileServiceException(FileServiceException.Reason.FILE_NOT_FOUND, "no file " + fileId, null);
        }
        FileManifest m = manifest.get();
        PlacementSnapshot.Snapshot snapshot = placement.get();
        List<PlacementSnapshot.NodeListing> nodes = snapshot.nodes();
        // The repair trims only while every node is UP; then it deletes the extras on the most
        // loaded nodes, chunk by chunk, so the counts go down as it goes.
        boolean trimPossible = snapshot.everyNodeReachable();
        int[] chunksPerNode = new int[nodes.size()];
        for (int n = 0; n < nodes.size(); n++) {
            chunksPerNode[n] = nodes.get(n).chunks().size();
        }
        StringBuilder json = new StringBuilder("{\"fileId\":").append(Json.quote(m.fileId()))
                .append(",\"name\":").append(m.name() == null ? "null" : Json.quote(m.name()))
                .append(",\"size\":").append(m.size())
                .append(",\"chunkCount\":").append(m.chunkCount())
                .append(",\"chunkSize\":").append(m.chunkSize())
                .append(",\"replicationFactor\":").append(replicas)
                .append(",\"listedAt\":").append(Json.quote(snapshot.takenAt().toString()))
                .append(",\"nodes\":");
        appendNodes(json, snapshot, false);
        json.append(",\"chunks\":[");
        for (int i = 0; i < m.chunkCount(); i++) {
            String chunkId = m.chunkId(i);
            List<Integer> holders = new ArrayList<>();
            int reachable = 0;
            for (PlacementSnapshot.NodeListing node : nodes) {
                if (node.chunks().contains(chunkId)) {
                    holders.add(node.index());
                    if (node.reachable()) {
                        reachable++;
                    }
                }
            }
            List<Integer> trimmed = trimPossible ? RepairService.trimVictims(holders, chunksPerNode, replicas) : List.of();
            for (int node : trimmed) {
                chunksPerNode[node]--;
            }
            json.append(i == 0 ? "" : ",").append("{\"index\":").append(i)
                    .append(",\"reachableCopies\":").append(reachable)
                    .append(",\"knownCopies\":").append(holders.size())
                    .append(",\"holders\":[");
            for (int h = 0; h < holders.size(); h++) {
                int node = holders.get(h);
                json.append(h == 0 ? "" : ",").append("{\"node\":").append(node)
                        .append(",\"reachable\":").append(nodes.get(node).reachable())
                        .append(",\"trimCandidate\":").append(trimmed.contains(node)).append('}');
            }
            json.append("]}");
        }
        return json.append("]}").toString();
    }

    /** The body of {@code GET /cluster/events}. */
    String eventsJson() {
        return events.toJson();
    }

    private String health(int min, int max, boolean everyNodeReachable) {
        if (min == 0) {
            return everyNodeReachable ? "missing" : "unavailable";
        }
        if (min < replicas) {
            return "at_risk";
        }
        return max > replicas ? "over_replicated" : "healthy";
    }

    private static int reachableCopies(PlacementSnapshot.Snapshot snapshot, String chunkId) {
        int copies = 0;
        for (PlacementSnapshot.NodeListing node : snapshot.nodes()) {
            if (node.reachable() && node.chunks().contains(chunkId)) {
                copies++;
            }
        }
        return copies;
    }

    private static void appendNodes(StringBuilder json, PlacementSnapshot.Snapshot snapshot, boolean withCounts) {
        json.append('[');
        List<PlacementSnapshot.NodeListing> nodes = snapshot.nodes();
        for (int n = 0; n < nodes.size(); n++) {
            PlacementSnapshot.NodeListing node = nodes.get(n);
            json.append(n == 0 ? "" : ",").append("{\"index\":").append(node.index())
                    .append(",\"state\":").append(Json.quote(node.state().name()))
                    .append(",\"reachable\":").append(node.reachable())
                    .append(",\"listed\":").append(node.listed())
                    .append(",\"listedAt\":").append(node.listed() ? Json.quote(node.listedAt().toString()) : "null");
            if (withCounts) {
                json.append(",\"chunkCopies\":").append(node.listed() ? Integer.toString(node.chunks().size()) : "null");
            }
            json.append('}');
        }
        json.append(']');
    }

    /** The manifest file's last-modified time, read without following links; null if it cannot be read. */
    private Instant committedAt(String manifestName) {
        try {
            Path file = manifests.directory().resolve(manifestName);
            return Files.getLastModifiedTime(file, LinkOption.NOFOLLOW_LINKS).toInstant();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
