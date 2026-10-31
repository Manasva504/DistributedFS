package com.distributedfs;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Which chunk copies each node holds, for the web console ({@code GET /files} and
 * {@code GET /files/<id>/placement}): one listing of every UP node's folder
 * ({@link ChunkStore#listEntries}, {@code GET /entries} for a remote node), shared by every request
 * and reused for {@link #DEFAULT_MAX_AGE_MILLIS}.
 *
 * <p><b>What it costs.</b> At most one listing per UP node per period, however many requests or
 * browser tabs ask: a request that finds the snapshot too old makes a new one while holding this
 * object's lock, and requests that arrive meanwhile wait for it and use it. It never takes the
 * storage lock and never reads a chunk's bytes. A committed upload and a finished repair make the
 * next request list again ({@link #invalidate}, which takes no lock).
 *
 * <p><b>What it says.</b> A copy is a regular file under a server chunk name ({@code
 * <fileId>_<index>}): it is counted by its name, not checked. A damaged copy still counts until a
 * download or a repair finds it. A node the heartbeat reports DOWN, or whose listing fails, is not
 * listed again; its last successful listing is kept and marked unreachable, with the time it was
 * made. That is a display cache, not a location index: nothing reads chunks by it. A node never
 * listed since the server started has no listing, and its copies are unknown.
 */
final class PlacementSnapshot {

    static final long DEFAULT_MAX_AGE_MILLIS = 1_500;

    /**
     * One node's part of a snapshot.
     *
     * @param reachable whether this listing was made now, from a node that is UP
     * @param listedAt  when the listing was made; null if the node has never been listed
     * @param chunks    the chunk IDs it holds (empty if never listed)
     */
    record NodeListing(int index, HeartbeatMonitor.State state, boolean reachable, Instant listedAt, Set<String> chunks) {
        boolean listed() {
            return listedAt != null;
        }
    }

    /** Every node's listing, in node order, and when the snapshot was made. */
    record Snapshot(Instant takenAt, List<NodeListing> nodes) {
        boolean everyNodeReachable() {
            return nodes.stream().allMatch(NodeListing::reachable);
        }
    }

    private final List<ChunkStore> nodes;
    private final HeartbeatMonitor monitor;
    private final long maxAgeNanos;
    private final Clock clock;
    private final AtomicLong generation = new AtomicLong();
    private final NodeListing[] lastGood;
    private Snapshot current;
    private long currentNanos;
    private long currentGeneration = -1;
    private int listings;

    PlacementSnapshot(List<? extends ChunkStore> nodes, HeartbeatMonitor monitor, long maxAgeMillis, Clock clock) {
        this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes must not be null"));
        this.monitor = Objects.requireNonNull(monitor, "monitor must not be null");
        if (maxAgeMillis < 0) {
            throw new IllegalArgumentException("maxAgeMillis must not be negative");
        }
        this.maxAgeNanos = TimeUnit.MILLISECONDS.toNanos(maxAgeMillis);
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.lastGood = new NodeListing[this.nodes.size()];
    }

    /** Makes the next {@link #get} list the nodes again, whatever the snapshot's age. Takes no lock. */
    void invalidate() {
        generation.incrementAndGet();
    }

    /** The current snapshot: the shared one if it is recent and nothing changed since, otherwise a new one. */
    synchronized Snapshot get() {
        long gen = generation.get();
        if (current != null && gen == currentGeneration && System.nanoTime() - currentNanos < maxAgeNanos) {
            return current;
        }
        List<HeartbeatMonitor.NodeHealth> health = monitor.snapshot();
        List<NodeListing> listed = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            HeartbeatMonitor.State state = health.get(i).state();
            NodeListing listing = null;
            if (state == HeartbeatMonitor.State.UP) {
                try {
                    listing = new NodeListing(i, state, true, clock.instant(), chunkIds(nodes.get(i).listEntries()));
                    listings++;
                    lastGood[i] = listing;
                } catch (IOException | RuntimeException e) {
                    listing = null; // shown as unreachable, with its last listing
                }
            }
            if (listing == null) {
                NodeListing old = lastGood[i];
                listing = old == null ? new NodeListing(i, state, false, null, Set.of())
                        : new NodeListing(i, state, false, old.listedAt(), old.chunks());
            }
            listed.add(listing);
        }
        current = new Snapshot(clock.instant(), List.copyOf(listed));
        currentNanos = System.nanoTime();
        currentGeneration = gen;
        return current;
    }

    /** How many node listings have been made so far (for tests of the sharing). */
    synchronized int listingsMade() {
        return listings;
    }

    private static Set<String> chunkIds(List<ChunkStore.Entry> entries) {
        Set<String> ids = new HashSet<>();
        for (ChunkStore.Entry entry : entries) {
            if (entry.kind() == ChunkStore.EntryKind.FILE && StartupRecovery.CANONICAL_CHUNK.matcher(entry.name()).matches()) {
                ids.add(entry.name());
            }
        }
        return Set.copyOf(ids);
    }
}
