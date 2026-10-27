package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.expectCannotTell;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.fastConfig;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.sha256;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Plain-Java tests for StorageCluster with replication (milestone M2): two copies per chunk on
 * different nodes, placement and its failures, the duplicate check that skips unreachable nodes,
 * deleting every copy, and verified reads that skip down, missing and damaged copies. Most tests
 * use local nodes whose failures the test switches on ({@link NodeTestKit.ControlledNode}).
 * Run: {@code java -cp "out;out-test" com.distributedfs.ReplicatedClusterTest}.
 */
public class ReplicatedClusterTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("replcluster-test-");

    public static void main(String[] args) {
        // Placement
        RUNNER.run("copy 1 goes on the next node and copy 2 on the next different one, round-robin",
                ReplicatedClusterTest::copiesOnConsecutiveNodes);
        RUNNER.run("a one-node cluster keeps one copy, and two copies need two nodes", ReplicatedClusterTest::oneNodeCluster);
        RUNNER.run("in a two-node cluster every chunk is on both nodes", ReplicatedClusterTest::twoNodeCluster);
        RUNNER.run("a node that certainly stored nothing is passed over for the next one", ReplicatedClusterTest::definiteFailureMovesOn);
        RUNNER.run("with too few nodes able to store, the copies made are reported for removal",
                ReplicatedClusterTest::tooFewNodes);
        RUNNER.run("with one node alive, it never gets two copies", ReplicatedClusterTest::neverTwoCopiesOnOneNode);
        RUNNER.run("a lost answer for copy 1 ends the store: no other node gets a copy", ReplicatedClusterTest::ambiguousFirstCopy);
        RUNNER.run("a lost answer for copy 2 ends the store too, and keeps copy 1 for the rollback",
                ReplicatedClusterTest::ambiguousSecondCopy);

        // The duplicate check
        RUNNER.run("the duplicate check skips an unreachable node, and the store does not use it",
                ReplicatedClusterTest::duplicateCheckSkipsDeadNode);
        RUNNER.run("a reachable node that cannot tell still refuses the store", ReplicatedClusterTest::cannotTellBlocks);
        RUNNER.run("a remote node that accepts connections but never answers is not skipped",
                ReplicatedClusterTest::silentNodeIsNotSkipped);
        RUNNER.run("a copy already on some node refuses the store", ReplicatedClusterTest::existingCopyRefuses);

        // Locating and deleting
        RUNNER.run("deleting removes both copies", ReplicatedClusterTest::deleteRemovesBothCopies);
        RUNNER.run("deleting with a node down deletes nothing", ReplicatedClusterTest::deleteWithNodeDown);
        RUNNER.run("three copies are located on the first holder and all deleted", ReplicatedClusterTest::threeCopiesAreAllowed);

        // Verified reads
        RUNNER.run("a verified read keeps the first good copy", ReplicatedClusterTest::firstGoodCopy);
        RUNNER.run("a verified read skips nodes without a copy, without reporting them", ReplicatedClusterTest::skipsMissing);
        RUNNER.run("a verified read skips a damaged copy and uses the next one", ReplicatedClusterTest::skipsCorrupt);
        RUNNER.run("a verified read skips a node that is down and reports it", ReplicatedClusterTest::skipsDeadNode);
        RUNNER.run("with every copy damaged, the read fails as corrupt and leaves no destination",
                ReplicatedClusterTest::allCorrupt);
        RUNNER.run("with every node confirming no copy, the chunk is not found", ReplicatedClusterTest::allAbsent);
        RUNNER.run("with no copy found and a node down, the read cannot tell", ReplicatedClusterTest::absentAndDead);
        RUNNER.run("a node that failed earlier in the download is tried last", ReplicatedClusterTest::failedNodeTriedLast);
        RUNNER.run("without an expected checksum (format 1 files) the first copy found is used",
                ReplicatedClusterTest::uncheckedRead);
        RUNNER.finish();
    }

    // ---------------------------------------------------------------------
    // Placement
    // ---------------------------------------------------------------------

    private static void copiesOnConsecutiveNodes(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes, 2);
        checkEquals(2, cluster.replicas(), "replicas");
        List<List<Integer>> placements = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            placements.add(cluster.storeReplicated("c_" + i, source(tempDir, i)).copies());
        }
        checkEquals(List.of(List.of(0, 1), List.of(1, 2), List.of(2, 0), List.of(0, 1)), placements, "placements");
        checkEquals(List.of("c_0", "c_2", "c_3"), listNames(nodes.get(0).dir()), "node 0");
        checkEquals(List.of("c_0", "c_1", "c_3"), listNames(nodes.get(1).dir()), "node 1");
        checkEquals(List.of("c_1", "c_2"), listNames(nodes.get(2).dir()), "node 2");
    }

    private static void oneNodeCluster(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 1);
        expectThrows(IllegalArgumentException.class, "two copies on one node", () -> new StorageCluster(nodes, 2));
        StorageCluster cluster = new StorageCluster(nodes, 1);
        checkEquals(List.of(0), cluster.storeReplicated("c_0", source(tempDir, 0)).copies(), "copies");
        checkEquals(List.of("c_0"), listNames(nodes.get(0).dir()), "node 0");
    }

    private static void twoNodeCluster(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 2);
        StorageCluster cluster = new StorageCluster(nodes, 2);
        for (int i = 0; i < 3; i++) {
            cluster.storeChunk("c_" + i, source(tempDir, i));
        }
        checkEquals(List.of("c_0", "c_1", "c_2"), listNames(nodes.get(0).dir()), "node 0");
        checkEquals(List.of("c_0", "c_1", "c_2"), listNames(nodes.get(1).dir()), "node 1");
    }

    private static void definiteFailureMovesOn(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(1).failStores = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        checkEquals(List.of(0, 2), cluster.storeReplicated("c_0", source(tempDir, 0)).copies(), "copies");
        checkEquals(List.of(), listNames(nodes.get(1).dir()), "the failing node");
    }

    private static void tooFewNodes(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(1).failStores = true;
        nodes.get(2).failStores = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        IncompleteReplicationException e = expectThrows(IncompleteReplicationException.class, "store",
                () -> cluster.storeChunk("c_0", source(tempDir, 0)));
        checkEquals(List.of(0), e.copies(), "the copies that were stored");
        checkContains(e.getMessage(), "stored on only 1 of the 2 nodes");
        checkEquals(List.of("c_0"), listNames(nodes.get(0).dir()), "node 0 keeps its copy until the caller removes it");
    }

    private static void neverTwoCopiesOnOneNode(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(1).down = true;
        nodes.get(2).down = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        IncompleteReplicationException e = expectThrows(IncompleteReplicationException.class, "store",
                () -> cluster.storeChunk("c_0", source(tempDir, 0)));
        checkEquals(List.of(0), e.copies(), "copies");
        checkEquals(List.of("c_0"), listNames(nodes.get(0).dir()), "node 0 has exactly one copy");
    }

    private static void ambiguousFirstCopy(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(0).loseStoreAnswers = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        expectThrows(AmbiguousStoreException.class, "store", () -> cluster.storeChunk("c_0", source(tempDir, 0)));
        checkEquals(List.of(), listNames(nodes.get(1).dir()), "node 1 got no copy");
        checkEquals(List.of(), listNames(nodes.get(2).dir()), "node 2 got no copy");
    }

    private static void ambiguousSecondCopy(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(1).loseStoreAnswers = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        AmbiguousStoreException e = expectThrows(AmbiguousStoreException.class, "store",
                () -> cluster.storeChunk("c_0", source(tempDir, 0)));
        check(Arrays.stream(e.getSuppressed()).anyMatch(s -> String.valueOf(s.getMessage()).contains("nodes [0]")),
                "the exception names the confirmed copy on node 0");
        checkEquals(List.of("c_0"), listNames(nodes.get(0).dir()), "copy 1 is kept for the rollback");
        checkEquals(List.of(), listNames(nodes.get(2).dir()), "node 2 got no copy");
        checkEquals(true, cluster.deleteChunk("c_0"), "the rollback's delete");
        checkEquals(List.of(), listNames(nodes.get(0).dir()) , "node 0 after the delete");
        checkEquals(List.of(), listNames(nodes.get(1).dir()), "node 1 after the delete");
    }

    // ---------------------------------------------------------------------
    // The duplicate check
    // ---------------------------------------------------------------------

    private static void duplicateCheckSkipsDeadNode(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(1).down = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        StorageCluster.StoreResult stored = cluster.storeReplicated("c_0", source(tempDir, 0));
        checkEquals(List.of(0, 2), stored.copies(), "copies");
        checkEquals(List.of(1), stored.skipped(), "skipped");
    }

    private static void cannotTellBlocks(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(1).cannotTell = true;
        StorageCluster cluster = new StorageCluster(nodes, 2);
        IOException e = expectCannotTell("store", () -> cluster.storeChunk("c_0", source(tempDir, 0)));
        checkContains(e.getMessage(), "node 1 could not confirm that the ID is unused");
        for (NodeTestKit.ControlledNode node : nodes) {
            checkEquals(List.of(), listNames(node.dir()), "nothing stored");
        }
    }

    private static void silentNodeIsNotSkipped(Path tempDir) throws Exception {
        String id = "0123456789abcdef0123456789abcdef";
        try (NodeTestKit.SocketStub silent = new NodeTestKit.SocketStub(NodeTestKit::holdSilently);
             RemoteStorageNode remote = new RemoteStorageNode(silent.port(), id, fastConfig())) {
            List<ChunkStore> members = new ArrayList<>(nodes(tempDir, 2));
            members.add(remote);
            StorageCluster cluster = new StorageCluster(members, 2);
            IOException e = expectCannotTell("store", () -> cluster.storeChunk("c_0", source(tempDir, 0)));
            check(!(e instanceof NodeUnavailableException), "the silent node was treated as unreachable");
            checkContains(e.getMessage(), "node 2 could not confirm that the ID is unused");
        }
    }

    private static void existingCopyRefuses(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        nodes.get(2).node.storeChunk("c_0", source(tempDir, 9));
        StorageCluster cluster = new StorageCluster(nodes, 2);
        FileAlreadyExistsException e = expectThrows(FileAlreadyExistsException.class, "store",
                () -> cluster.storeChunk("c_0", source(tempDir, 0)));
        checkContains(e.getMessage(), "already exists on node 2");
        checkEquals(List.of(), listNames(nodes.get(0).dir()), "node 0");
        checkEquals(List.of(), listNames(nodes.get(1).dir()), "node 1");
    }

    // ---------------------------------------------------------------------
    // Locating and deleting
    // ---------------------------------------------------------------------

    private static void deleteRemovesBothCopies(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes, 2);
        cluster.storeChunk("c_0", source(tempDir, 0));
        checkEquals(true, cluster.deleteChunk("c_0"), "first delete");
        checkEquals(false, cluster.deleteChunk("c_0"), "second delete");
        for (NodeTestKit.ControlledNode node : nodes) {
            checkEquals(List.of(), listNames(node.dir()), "every copy is gone");
        }
    }

    private static void deleteWithNodeDown(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        StorageCluster cluster = new StorageCluster(nodes, 2);
        cluster.storeChunk("c_0", source(tempDir, 0)); // nodes 0 and 1
        nodes.get(2).down = true;
        IOException e = expectCannotTell("delete", () -> cluster.deleteChunk("c_0"));
        checkContains(e.getMessage(), "nothing was deleted");
        checkEquals(List.of("c_0"), listNames(nodes.get(0).dir()), "node 0 keeps its copy");
        checkEquals(List.of("c_0"), listNames(nodes.get(1).dir()), "node 1 keeps its copy");
    }

    private static void threeCopiesAreAllowed(Path tempDir) throws Exception {
        List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
        for (NodeTestKit.ControlledNode node : nodes) {
            node.node.storeChunk("c_0", source(tempDir, 0));
        }
        StorageCluster cluster = new StorageCluster(nodes, 2);
        // More copies than the replication factor are allowed since milestone M3: located on the
        // first holder, and a delete removes all of them.
        checkEquals(0, cluster.locateChunk("c_0"), "the first holder");
        check(cluster.deleteChunk("c_0"), "delete all three copies");
        for (NodeTestKit.ControlledNode node : nodes) {
            checkEquals(List.of(), listNames(node.dir()), "every copy is gone");
        }
    }

    // ---------------------------------------------------------------------
    // Verified reads
    // ---------------------------------------------------------------------

    private static void firstGoodCopy(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 0, 1);
        StorageCluster.ReadResult read = f.read(Set.of());
        checkEquals(0, read.node(), "node used");
        checkEquals(List.of(), read.problems(), "problems");
        f.checkCopy();
    }

    private static void skipsMissing(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 2);
        StorageCluster.ReadResult read = f.read(Set.of());
        checkEquals(2, read.node(), "node used");
        checkEquals(List.of(), read.problems(), "nodes without a copy are not problems");
        f.checkCopy();
    }

    private static void skipsCorrupt(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 0, 1);
        f.damage(0);
        StorageCluster.ReadResult read = f.read(Set.of());
        checkEquals(1, read.node(), "node used");
        checkEquals(1, read.problems().size(), "problems");
        checkContains(read.problems().get(0), "node 0");
        checkContains(read.problems().get(0), "damaged copy");
        f.checkCopy();
    }

    private static void skipsDeadNode(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 0, 1);
        f.nodes.get(0).down = true;
        StorageCluster.ReadResult read = f.read(Set.of());
        checkEquals(1, read.node(), "node used");
        checkEquals(List.of(0), read.failedNodes(), "failed nodes");
        checkContains(read.problems().get(0), "could not be read");
        f.checkCopy();
    }

    private static void allCorrupt(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 0, 1);
        f.damage(0);
        f.damage(1);
        StorageCluster.CorruptCopiesException e = expectThrows(StorageCluster.CorruptCopiesException.class, "read",
                () -> f.read(Set.of()));
        checkContains(e.getMessage(), "No good copy");
        check(!Files.exists(f.destination), "a damaged copy was left at the destination");
    }

    private static void allAbsent(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir);
        expectThrows(ChunkNotFoundException.class, "read", () -> f.read(Set.of()));
    }

    private static void absentAndDead(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir);
        f.nodes.get(1).down = true;
        expectCannotTell("read", () -> f.read(Set.of()));
    }

    private static void failedNodeTriedLast(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 0, 1);
        StorageCluster.ReadResult read = f.read(Set.of(0));
        checkEquals(1, read.node(), "node used");
        checkEquals(0, f.nodes.get(0).retrieves.get(), "retrieves on the node to try last");
    }

    private static void uncheckedRead(Path tempDir) throws Exception {
        Fixture f = Fixture.storedOn(tempDir, 0, 1);
        f.damage(0);
        StorageCluster.ReadResult read = f.cluster.retrieveVerifiedChunk("c_0", f.destination, null, Set.of());
        checkEquals(0, read.node(), "the first copy, unchecked");
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static List<NodeTestKit.ControlledNode> nodes(Path tempDir, int count) throws IOException {
        List<NodeTestKit.ControlledNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(new NodeTestKit.ControlledNode(tempDir.resolve("node-" + i)));
        }
        return nodes;
    }

    private static Path source(Path tempDir, int seed) throws IOException {
        return Files.write(tempDir.resolve("source-" + seed + "-" + System.nanoTime()), randomBytes(5000, seed));
    }

    /** Three nodes, with one chunk stored directly on the given nodes, and a cluster with 2 copies. */
    private static final class Fixture {
        final List<NodeTestKit.ControlledNode> nodes;
        final StorageCluster cluster;
        final byte[] data;
        final Path destination;

        private Fixture(List<NodeTestKit.ControlledNode> nodes, byte[] data, Path destination) {
            this.nodes = nodes;
            this.cluster = new StorageCluster(nodes, 2);
            this.data = data;
            this.destination = destination;
        }

        static Fixture storedOn(Path tempDir, int... holders) throws IOException {
            List<NodeTestKit.ControlledNode> nodes = nodes(tempDir, 3);
            byte[] data = randomBytes(5000, 1);
            Path source = Files.write(tempDir.resolve("chunk-source"), data);
            for (int holder : holders) {
                nodes.get(holder).node.storeChunk("c_0", source);
            }
            return new Fixture(nodes, data, tempDir.resolve("out").resolve("copy"));
        }

        StorageCluster.ReadResult read(Set<Integer> tryLast) throws IOException {
            return cluster.retrieveVerifiedChunk("c_0", destination, sha256(data), tryLast);
        }

        void damage(int node) throws IOException {
            Path copy = nodes.get(node).dir().resolve("c_0");
            byte[] content = Files.readAllBytes(copy);
            content[100] ^= 1;
            Files.write(copy, content);
        }

        void checkCopy() throws IOException {
            check(Arrays.equals(data, Files.readAllBytes(destination)), "the copy at the destination differs");
        }
    }
}
