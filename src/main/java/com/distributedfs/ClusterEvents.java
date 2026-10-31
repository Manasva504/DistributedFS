package com.distributedfs;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * The last {@link #CAPACITY} things that happened to the cluster, in memory only, for the web
 * console's activity feed ({@code GET /cluster/events}): a node going DOWN or coming back UP, an
 * upload committing, and a repair or dry run finishing.
 *
 * <p>Every event gets the next sequence number (from 1) and the time it was recorded. The oldest
 * event is dropped when a new one would make more than {@link #CAPACITY}. Nothing is written to
 * disk: after a restart the list starts empty, and {@code startedAt} in the JSON changes, so a
 * reader knows the sequence numbers started again.
 *
 * <p>Recording takes this object's lock only, for a moment, and does no I/O, so the heartbeat,
 * uploads and repairs can record while holding their own locks. Every value in the JSON is a
 * number, a boolean, or a string the server made itself (an event type, a file ID).
 */
final class ClusterEvents {

    static final int CAPACITY = 100;

    /** One event: {@code fields} is the JSON text of its type-specific fields, starting with a comma. */
    record Event(long seq, Instant time, String type, String fields) {
    }

    private final Clock clock;
    private final Instant startedAt;
    private final ArrayDeque<Event> events = new ArrayDeque<>();
    private long nextSeq = 1;

    ClusterEvents(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.startedAt = clock.instant();
    }

    void nodeDown(int node, int failuresInARow) {
        add("node_down", ",\"node\":" + node + ",\"failuresInARow\":" + failuresInARow);
    }

    void nodeUp(int node) {
        add("node_up", ",\"node\":" + node);
    }

    void uploadFinished(FileManifest manifest, int copies) {
        add("upload_finished", ",\"fileId\":" + Json.quote(manifest.fileId())
                + ",\"name\":" + (manifest.name() == null ? "null" : Json.quote(manifest.name())) + ",\"size\":" + manifest.size()
                + ",\"chunks\":" + manifest.chunkCount() + ",\"copies\":" + copies);
    }

    void repairFinished(RepairReport report) {
        add("repair_finished", ",\"dryRun\":" + report.dryRun() + ",\"chunksChecked\":" + report.chunksChecked()
                + ",\"copiesAdded\":" + report.copiesAdded() + ",\"copiesReplaced\":" + report.copiesReplaced()
                + ",\"copiesRemoved\":" + report.copiesRemoved() + ",\"chunksLost\":" + report.chunksLost()
                + ",\"chunksUncertain\":" + report.chunksUncertain() + ",\"durationMillis\":" + report.durationMillis());
    }

    private synchronized void add(String type, String fields) {
        events.addLast(new Event(nextSeq++, clock.instant(), type, fields));
        while (events.size() > CAPACITY) {
            events.removeFirst();
        }
    }

    /** The events, newest first, as one consistent copy. */
    synchronized List<Event> newestFirst() {
        List<Event> copy = new ArrayList<>(events.size());
        for (Iterator<Event> it = events.descendingIterator(); it.hasNext(); ) {
            copy.add(it.next());
        }
        return copy;
    }

    /** The body of {@code GET /cluster/events}. */
    String toJson() {
        StringBuilder json = new StringBuilder("{\"startedAt\":").append(Json.quote(startedAt.toString()))
                .append(",\"capacity\":").append(CAPACITY).append(",\"events\":[");
        List<Event> list = newestFirst();
        for (int i = 0; i < list.size(); i++) {
            Event e = list.get(i);
            json.append(i == 0 ? "" : ",").append("{\"seq\":").append(e.seq())
                    .append(",\"time\":").append(Json.quote(e.time().toString()))
                    .append(",\"type\":").append(Json.quote(e.type())).append(e.fields()).append('}');
        }
        return json.append("]}").toString();
    }
}
