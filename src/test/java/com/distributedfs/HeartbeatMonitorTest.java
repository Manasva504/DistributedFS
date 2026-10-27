package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.fastConfig;
import static com.distributedfs.NodeTestKit.reply;
import static com.distributedfs.NodeTestKit.waitUntil;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Plain-Java tests for HeartbeatMonitor (milestone M3): the UP and DOWN rules, the clock, the
 * thread and its shutdown, and identifying a node that was missing at startup. Most tests call
 * {@code checkNow()} with a test clock instead of waiting; the thread tests use a 20 ms interval.
 * Run: {@code java -cp "out;out-test" com.distributedfs.HeartbeatMonitorTest}.
 */
public class HeartbeatMonitorTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("heartbeat-test-");

    private static final String ID = "0123456789abcdef0123456789abcdef";
    private static final String OTHER_ID = "fedcba9876543210fedcba9876543210";

    public static void main(String[] args) {
        RUNNER.run("a node is DOWN only after 3 failed checks in a row", HeartbeatMonitorTest::downAfterThreeFailures);
        RUNNER.run("one good check makes a node UP again and resets its failures", HeartbeatMonitorTest::upAfterOneGoodCheck);
        RUNNER.run("a good check between failures starts the count again", HeartbeatMonitorTest::goodCheckResetsCount);
        RUNNER.run("the last good check comes from the clock, and failures keep it", HeartbeatMonitorTest::lastGoodCheckFromClock);
        RUNNER.run("an answer with another node's ID is a failed check", HeartbeatMonitorTest::wrongIdIsFailure);
        RUNNER.run("a node that did not answer at startup starts DOWN, with no good check yet",
                HeartbeatMonitorTest::missingAtStartStartsDown);
        RUNNER.run("an unidentified node is pinned by its first good check, and only to a candidate ID",
                HeartbeatMonitorTest::unidentifiedNodeIsPinned);
        RUNNER.run("a recorded ID is claimed by one unidentified node only", HeartbeatMonitorTest::candidateClaimedOnce);
        RUNNER.run("a local node whose folder is gone fails its check", HeartbeatMonitorTest::localFolderHealth);
        RUNNER.run("changes to DOWN and back to UP are logged", HeartbeatMonitorTest::changesAreLogged);
        RUNNER.run("the thread checks at its interval and marks a stopped node DOWN", HeartbeatMonitorTest::threadMarksDown);
        RUNNER.run("a check that throws does not end the thread", HeartbeatMonitorTest::exceptionDoesNotKillThread);
        RUNNER.run("close stops the thread, and no check runs after it", HeartbeatMonitorTest::closeStopsThread);
        RUNNER.run("a snapshot is a copy that later checks do not change", HeartbeatMonitorTest::snapshotIsACopy);
        RUNNER.finish();
    }

    private static void downAfterThreeFailures(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 2);
        f.nodes.get(1).down = true;
        f.monitor.checkNow();
        f.monitor.checkNow();
        checkEquals(HeartbeatMonitor.State.UP, f.state(1), "after 2 failures");
        checkEquals(2, f.monitor.snapshot().get(1).failuresInARow(), "failures after 2");
        f.monitor.checkNow();
        checkEquals(HeartbeatMonitor.State.DOWN, f.state(1), "after 3 failures");
        checkEquals(HeartbeatMonitor.State.UP, f.state(0), "the other node");
        check(!f.monitor.allUp(), "not every node is UP");
    }

    private static void upAfterOneGoodCheck(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 1);
        f.nodes.get(0).down = true;
        for (int i = 0; i < 5; i++) {
            f.monitor.checkNow();
        }
        checkEquals(HeartbeatMonitor.State.DOWN, f.state(0), "down");
        f.nodes.get(0).down = false;
        f.monitor.checkNow();
        checkEquals(HeartbeatMonitor.State.UP, f.state(0), "after one good check");
        checkEquals(0, f.monitor.snapshot().get(0).failuresInARow(), "failures");
    }

    private static void goodCheckResetsCount(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 1);
        f.nodes.get(0).down = true;
        f.monitor.checkNow();
        f.monitor.checkNow();
        f.nodes.get(0).down = false;
        f.monitor.checkNow();
        f.nodes.get(0).down = true;
        f.monitor.checkNow();
        f.monitor.checkNow();
        checkEquals(HeartbeatMonitor.State.UP, f.state(0), "2 + 2 failures with a good check between");
        checkEquals(2, f.monitor.snapshot().get(0).failuresInARow(), "failures");
    }

    private static void lastGoodCheckFromClock(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 1);
        f.clock.advance(5_000);
        f.monitor.checkNow();
        java.time.Instant good = f.clock.instant();
        checkEquals(good, f.monitor.snapshot().get(0).lastGoodCheck(), "after a good check");
        f.nodes.get(0).down = true;
        f.clock.advance(5_000);
        f.monitor.checkNow();
        checkEquals(good, f.monitor.snapshot().get(0).lastGoodCheck(), "a failure keeps the last good check");
    }

    private static void wrongIdIsFailure(Path tempDir) throws Exception {
        try (NodeTestKit.Stub stub = new NodeTestKit.Stub(exchange -> reply(exchange, 200, OTHER_ID,
                "status=ok\nnode-id=" + OTHER_ID + "\nformat=1\n"));
             RemoteStorageNode remote = new RemoteStorageNode(stub.port(), ID, fastConfig())) {
            HeartbeatMonitor monitor = new HeartbeatMonitor(List.of(remote), List.of(true), 1_000, 3,
                    new NodeTestKit.TestClock(), new NodeTestKit.Lines().stream());
            for (int i = 0; i < 3; i++) {
                monitor.checkNow();
            }
            checkEquals(HeartbeatMonitor.State.DOWN, monitor.snapshot().get(0).state(), "state");
            checkEquals(java.util.Optional.of(ID), remote.nodeId(), "the client stays pinned to its own ID");
        }
    }

    private static void missingAtStartStartsDown(Path tempDir) throws Exception {
        NodeTestKit.ControlledNode node = new NodeTestKit.ControlledNode(tempDir.resolve("node"));
        HeartbeatMonitor monitor = new HeartbeatMonitor(List.of(node), List.of(false), 1_000, 3,
                new NodeTestKit.TestClock(), new NodeTestKit.Lines().stream());
        HeartbeatMonitor.NodeHealth health = monitor.snapshot().get(0);
        checkEquals(HeartbeatMonitor.State.DOWN, health.state(), "state");
        checkEquals(null, health.lastGoodCheck(), "last good check");
        checkEquals(3, health.failuresInARow(), "failures");
        monitor.checkNow();
        checkEquals(HeartbeatMonitor.State.UP, monitor.snapshot().get(0).state(), "after one good check");
    }

    private static void unidentifiedNodeIsPinned(Path tempDir) throws Exception {
        String[] answer = {"9".repeat(32)};
        try (NodeTestKit.Stub stub = new NodeTestKit.Stub(exchange -> reply(exchange, 200, answer[0],
                "status=ok\nnode-id=" + answer[0] + "\nformat=1\n"));
             RemoteStorageNode remote = RemoteStorageNode.unidentified(stub.port(), candidates(ID, OTHER_ID), fastConfig())) {
            HeartbeatMonitor monitor = new HeartbeatMonitor(List.of(remote), List.of(false), 1_000, 3,
                    new NodeTestKit.TestClock(), new NodeTestKit.Lines().stream());
            NodeTestKit.expectThrows(NodeUnavailableException.class, "a request before it is identified",
                    () -> remote.hasChunk("c_0"));
            monitor.checkNow();
            checkEquals(HeartbeatMonitor.State.DOWN, monitor.snapshot().get(0).state(), "a non-candidate ID");
            checkEquals(java.util.Optional.empty(), remote.nodeId(), "not pinned to a non-candidate ID");
            answer[0] = OTHER_ID;
            monitor.checkNow();
            checkEquals(HeartbeatMonitor.State.UP, monitor.snapshot().get(0).state(), "a candidate ID");
            checkEquals(java.util.Optional.of(OTHER_ID), remote.nodeId(), "pinned");
            answer[0] = ID;
            monitor.checkNow();
            checkEquals(1, monitor.snapshot().get(0).failuresInARow(), "once pinned, another candidate is a failure");
        }
    }

    private static void candidateClaimedOnce(Path tempDir) throws Exception {
        Set<String> shared = candidates(ID, OTHER_ID);
        try (NodeTestKit.Stub stub = new NodeTestKit.Stub(exchange -> reply(exchange, 200, ID,
                "status=ok\nnode-id=" + ID + "\nformat=1\n"));
             RemoteStorageNode first = RemoteStorageNode.unidentified(stub.port(), shared, fastConfig());
             RemoteStorageNode second = RemoteStorageNode.unidentified(stub.port(), shared, fastConfig())) {
            first.checkHealth();
            checkEquals(java.util.Optional.of(ID), first.nodeId(), "the first client is pinned");
            checkEquals(Set.of(OTHER_ID), shared, "the claimed ID left the shared set");
            NodeTestKit.expectThrows(java.io.IOException.class, "the same node answering the second client",
                    second::checkHealth);
            checkEquals(java.util.Optional.empty(), second.nodeId(), "the second client is not pinned");
        }
    }

    /** A modifiable, thread-safe candidate set, as the server makes for its unidentified nodes. */
    private static Set<String> candidates(String... ids) {
        Set<String> set = java.util.concurrent.ConcurrentHashMap.newKeySet();
        set.addAll(List.of(ids));
        return set;
    }

    private static void localFolderHealth(Path tempDir) throws Exception {
        StorageNode node = new StorageNode(tempDir.resolve("node"));
        node.checkHealth();
        Files.delete(tempDir.resolve("node"));
        NodeTestKit.expectThrows(java.io.IOException.class, "a deleted folder", node::checkHealth);
    }

    private static void changesAreLogged(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 1);
        f.nodes.get(0).down = true;
        for (int i = 0; i < 3; i++) {
            f.monitor.checkNow();
        }
        f.nodes.get(0).down = false;
        f.monitor.checkNow();
        checkContains(f.err.text(), "node 0 (");
        checkContains(f.err.text(), "is DOWN after 3 failed health checks in a row");
        checkContains(f.err.text(), "Node 0 (");
        checkContains(f.err.text(), "is UP again");
    }

    private static void threadMarksDown(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = Fixture.nodes(tempDir, 2);
        HeartbeatMonitor monitor = new HeartbeatMonitor(nodes, List.of(true, true), 20, 3, java.time.Clock.systemUTC(),
                new NodeTestKit.Lines().stream());
        monitor.start();
        try {
            nodes.get(1).down = true;
            waitUntil(5_000, "node 1 to be DOWN",
                    () -> monitor.snapshot().get(1).state() == HeartbeatMonitor.State.DOWN);
            check(nodes.get(0).healthChecks.get() >= 3, "node 0 was checked on every round");
        } finally {
            monitor.close();
        }
    }

    private static void exceptionDoesNotKillThread(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = Fixture.nodes(tempDir, 1);
        nodes.get(0).healthBomb = true;
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        HeartbeatMonitor monitor = new HeartbeatMonitor(nodes, List.of(true), 20, 3, java.time.Clock.systemUTC(),
                err.stream());
        monitor.start();
        try {
            waitUntil(5_000, "the node to be DOWN after unexpected failures",
                    () -> monitor.snapshot().get(0).state() == HeartbeatMonitor.State.DOWN);
            nodes.get(0).healthBomb = false;
            waitUntil(5_000, "the node to be UP again", () -> monitor.snapshot().get(0).state() == HeartbeatMonitor.State.UP);
            check(monitor.running(), "the thread is still running");
        } finally {
            monitor.close();
        }
    }

    private static void closeStopsThread(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = Fixture.nodes(tempDir, 1);
        HeartbeatMonitor monitor = new HeartbeatMonitor(nodes, List.of(true), 20, 3, java.time.Clock.systemUTC(),
                new NodeTestKit.Lines().stream());
        monitor.start();
        waitUntil(5_000, "a few checks", () -> nodes.get(0).healthChecks.get() >= 2);
        monitor.close();
        check(!monitor.running(), "the thread is still running after close");
        int checks = nodes.get(0).healthChecks.get();
        NodeTestKit.sleepQuietly(200);
        checkEquals(checks, nodes.get(0).healthChecks.get(), "checks after close");
        check(Thread.getAllStackTraces().keySet().stream().noneMatch(t -> t.getName().equals("distributedfs-heartbeat")
                && t.isAlive()), "a heartbeat thread is still alive");
        monitor.close(); // a second close does nothing
    }

    private static void snapshotIsACopy(Path tempDir) throws Exception {
        Fixture f = new Fixture(tempDir, 1);
        List<HeartbeatMonitor.NodeHealth> before = f.monitor.snapshot();
        f.nodes.get(0).down = true;
        for (int i = 0; i < 3; i++) {
            f.monitor.checkNow();
        }
        checkEquals(HeartbeatMonitor.State.UP, before.get(0).state(), "the earlier snapshot");
        NodeTestKit.expectThrows(UnsupportedOperationException.class, "changing a snapshot", () -> before.clear());
    }

    /** Local nodes that tests switch on and off, a test clock and a monitor that is not started. */
    private static final class Fixture {
        final List<NodeTestKit.ControlledNode> nodes;
        final NodeTestKit.TestClock clock = new NodeTestKit.TestClock();
        final NodeTestKit.Lines err = new NodeTestKit.Lines();
        final HeartbeatMonitor monitor;

        Fixture(Path tempDir, int count) throws java.io.IOException {
            nodes = nodes(tempDir, count);
            List<Boolean> answered = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                answered.add(true);
            }
            monitor = new HeartbeatMonitor(nodes, answered, 1_000, 3, clock, err.stream());
        }

        static List<NodeTestKit.ControlledNode> nodes(Path tempDir, int count) throws java.io.IOException {
            List<NodeTestKit.ControlledNode> nodes = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                nodes.add(new NodeTestKit.ControlledNode(tempDir.resolve("node-" + i)));
            }
            return nodes;
        }

        HeartbeatMonitor.State state(int node) {
            return monitor.snapshot().get(node).state();
        }
    }
}
