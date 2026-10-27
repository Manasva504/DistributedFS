package com.distributedfs;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Failure detection: checks every storage node's health ({@link ChunkStore#checkHealth}) at a
 * fixed delay, on one daemon thread of its own, and keeps each node's state.
 *
 * <p><b>States.</b> A node is DOWN after {@code failuresForDown} failed checks in a row (3 by
 * default; an answer with the wrong node ID is a failure) and UP again after one good check. The
 * server seeds the states from its startup checks: a node that answered starts UP, one that did not
 * starts DOWN. State changes are written to the server's error stream.
 *
 * <p><b>Who uses it.</b> Only the cluster status ({@code GET /cluster/status}) and repair
 * ({@link RepairService}) read these states. Uploads and downloads do not: they find out about a
 * node that is down by trying it, as before.
 *
 * <p><b>Threads.</b> The checks take no storage lock: a health check reads no chunk. Each node's
 * state is changed and read under this object's lock, and {@link #snapshot} returns a copy.
 * {@link #close} stops the thread and waits for it, so no check runs after the server stops. Tests
 * call {@link #checkNow} directly, with a fixed clock, instead of starting the thread.
 */
final class HeartbeatMonitor implements AutoCloseable {

    static final long DEFAULT_INTERVAL_MILLIS = 2_000;
    static final int DEFAULT_FAILURES_FOR_DOWN = 3;

    enum State { UP, DOWN }

    /** One node's state: when it last answered well (null if never), and how many checks failed since. */
    record NodeHealth(int index, State state, Instant lastGoodCheck, int failuresInARow) {
    }

    private final List<ChunkStore> nodes;
    private final long intervalMillis;
    private final int failuresForDown;
    private final Clock clock;
    private final PrintStream err;
    private final NodeHealth[] states;
    private ScheduledExecutorService executor;

    /**
     * @param answeredAtStart for each node, whether it answered when the server started
     */
    HeartbeatMonitor(List<? extends ChunkStore> nodes, List<Boolean> answeredAtStart, long intervalMillis,
                     int failuresForDown, Clock clock, PrintStream err) {
        this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes must not be null"));
        if (answeredAtStart.size() != this.nodes.size()) {
            throw new IllegalArgumentException("answeredAtStart must have one entry per node");
        }
        if (intervalMillis < 1 || failuresForDown < 1) {
            throw new IllegalArgumentException("the interval and the failure count must be at least 1");
        }
        this.intervalMillis = intervalMillis;
        this.failuresForDown = failuresForDown;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.err = Objects.requireNonNull(err, "err must not be null");
        this.states = new NodeHealth[this.nodes.size()];
        Instant now = clock.instant();
        for (int i = 0; i < states.length; i++) {
            states[i] = answeredAtStart.get(i) ? new NodeHealth(i, State.UP, now, 0)
                    : new NodeHealth(i, State.DOWN, null, failuresForDown);
        }
    }

    /** Starts the heartbeat thread: the first round of checks after one interval, then one interval after each round. */
    synchronized void start() {
        if (executor != null) {
            throw new IllegalStateException("the heartbeat has already been started");
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "distributedfs-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::checkNowSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /** One round of checks, never throwing: an exception would end a scheduled task for good. */
    private void checkNowSafely() {
        try {
            checkNow();
        } catch (Throwable e) {
            err.println("Warning: a heartbeat round failed unexpectedly: " + e);
        }
    }

    /** Checks every node once, in order, and updates its state. */
    void checkNow() {
        for (int i = 0; i < nodes.size(); i++) {
            String failure = null;
            try {
                nodes.get(i).checkHealth();
            } catch (IOException | RuntimeException e) {
                failure = e.getMessage();
            }
            record(i, failure);
        }
    }

    private synchronized void record(int index, String failure) {
        NodeHealth before = states[index];
        if (failure == null) {
            states[index] = new NodeHealth(index, State.UP, clock.instant(), 0);
            if (before.state() == State.DOWN) {
                err.println("Node " + index + " (" + nodes.get(index).describe() + ") is UP again");
            }
            return;
        }
        int failures = before.failuresInARow() + 1;
        State state = failures >= failuresForDown ? State.DOWN : before.state();
        states[index] = new NodeHealth(index, state, before.lastGoodCheck(), failures);
        if (before.state() == State.UP && state == State.DOWN) {
            err.println("Warning: node " + index + " (" + nodes.get(index).describe() + ") is DOWN after " + failures
                    + " failed health checks in a row (last: " + failure + ")");
        }
    }

    /** Every node's state, in node order, as one consistent copy. */
    synchronized List<NodeHealth> snapshot() {
        return List.of(states.clone());
    }

    /** True if every node is UP. */
    synchronized boolean allUp() {
        for (NodeHealth health : states) {
            if (health.state() != State.UP) {
                return false;
            }
        }
        return true;
    }

    long intervalMillis() {
        return intervalMillis;
    }

    int failuresForDown() {
        return failuresForDown;
    }

    /** The nodes, in order, as the monitor checks them. */
    List<ChunkStore> nodes() {
        return nodes;
    }

    /** True while the heartbeat thread runs (for tests). */
    synchronized boolean running() {
        return executor != null && !executor.isTerminated();
    }

    /** Stops the heartbeat thread and waits for it, up to 3 seconds. Calling it again does nothing. */
    @Override
    public void close() {
        ScheduledExecutorService stopping;
        synchronized (this) {
            stopping = executor;
        }
        if (stopping == null) {
            return;
        }
        stopping.shutdownNow();
        try {
            if (!stopping.awaitTermination(3, TimeUnit.SECONDS)) {
                err.println("Warning: the heartbeat thread was still running 3 seconds after the server stopped");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** The node states as a list of strings, for messages. */
    static List<String> describe(List<NodeHealth> states) {
        List<String> parts = new ArrayList<>();
        for (NodeHealth health : states) {
            parts.add("node " + health.index() + " " + health.state());
        }
        return parts;
    }
}
