package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.createDirectoryJunction;
import static com.distributedfs.NodeTestKit.expectThrows;
import static com.distributedfs.NodeTestKit.listNames;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Plain-Java tests for NodeDataDirectory, a storage node process's own folder (no JUnit).
 * Run: {@code java -cp "out;out-test" com.distributedfs.NodeDataDirectoryTest}. Exits with code 1
 * if any test fails. (One process per folder across processes is tested in StorageNodeServerTest.)
 */
public class NodeDataDirectoryTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("nodedir-test-");

    public static void main(String[] args) {
        RUNNER.run("a new folder gets a marker with a random node ID, a lock file and a chunks folder",
                NodeDataDirectoryTest::newFolderIsInitialized);
        RUNNER.run("reopening keeps the node ID and the stored chunks", NodeDataDirectoryTest::reopenKeepsId);
        RUNNER.run("two new folders get different node IDs", NodeDataDirectoryTest::differentFoldersDifferentIds);
        RUNNER.run("a folder already open is refused as in use, and the first holder keeps it",
                NodeDataDirectoryTest::secondOpenIsRefused);
        RUNNER.run("closing releases the lock, so the folder can be opened again", NodeDataDirectoryTest::closeReleasesLock);
        RUNNER.run("a folder with someone else's files is refused and left untouched",
                NodeDataDirectoryTest::foreignFolderIsRefused);
        RUNNER.run("the server's data directory is refused as a node folder, and a node folder as a data directory",
                NodeDataDirectoryTest::serverAndNodeFoldersAreNotInterchangeable);
        RUNNER.run("a damaged marker is refused and the chunks are left alone", NodeDataDirectoryTest::damagedMarkerIsRefused);
        RUNNER.run("a chunks folder replaced by a junction is refused, nothing is written behind it, and the lock is released",
                NodeDataDirectoryTest::junctionedChunksFolderIsRefused);
        RUNNER.run("a leftover marker temp file of an interrupted first start is removed", NodeDataDirectoryTest::leftoverMarkerTempIsRemoved);
        RUNNER.run("a path that is a file is refused", NodeDataDirectoryTest::fileIsRefused);
        RUNNER.finish();
    }

    private static void newFolderIsInitialized(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("a").resolve("node");
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            check(dir.newlyInitialized(), "the folder is new");
            check(NodeProtocol.NODE_ID.matcher(dir.nodeId()).matches(), "the node ID is 32 lowercase hex: " + dir.nodeId());
            checkEquals(List.of("chunks", "distributedfs-node.txt", "node.lock"), listNames(folder), "folder contents");
            checkEquals(NodeDataDirectory.markerText(dir.nodeId()),
                    Files.readString(folder.resolve("distributedfs-node.txt"), StandardCharsets.US_ASCII), "marker");
            checkEquals(folder.toAbsolutePath().normalize().resolve("chunks"), dir.chunksDir(), "chunks folder");
            checkEquals(List.of(), listNames(dir.chunksDir()), "the chunks folder is empty");
        }
    }

    private static void reopenKeepsId(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        String id;
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            id = dir.nodeId();
            Files.write(dir.chunksDir().resolve("chunk_0"), new byte[] {1, 2, 3});
        }
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            check(!dir.newlyInitialized(), "the folder is not new any more");
            checkEquals(id, dir.nodeId(), "node ID after reopening");
            checkEquals(List.of("chunk_0"), listNames(dir.chunksDir()), "chunks after reopening");
        }
    }

    private static void differentFoldersDifferentIds(Path tempDir) throws Exception {
        try (NodeDataDirectory first = NodeDataDirectory.open(tempDir.resolve("one"));
             NodeDataDirectory second = NodeDataDirectory.open(tempDir.resolve("two"))) {
            check(!first.nodeId().equals(second.nodeId()), "two folders share a node ID");
        }
    }

    private static void secondOpenIsRefused(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            DataDirectoryException e = expectThrows(DataDirectoryException.class, "second open",
                    () -> NodeDataDirectory.open(folder));
            checkEquals(DataDirectoryException.Reason.IN_USE, e.reason(), "reason");
            checkContains(e.getMessage(), "already in use");
            check(Files.isDirectory(dir.chunksDir()), "the first holder still has its folder");
        }
    }

    private static void closeReleasesLock(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        NodeDataDirectory.open(folder).close();
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            check(!dir.newlyInitialized(), "reopened");
        }
    }

    private static void foreignFolderIsRefused(Path tempDir) throws Exception {
        Path folder = Files.createDirectory(tempDir.resolve("photos"));
        Files.writeString(folder.resolve("holiday.jpg"), "not a node");
        DataDirectoryException e = expectThrows(DataDirectoryException.class, "open", () -> NodeDataDirectory.open(folder));
        checkEquals(DataDirectoryException.Reason.UNRECOGNIZED, e.reason(), "reason");
        checkEquals(List.of("holiday.jpg"), listNames(folder), "the folder is untouched (no lock file, no marker)");
    }

    private static void serverAndNodeFoldersAreNotInterchangeable(Path tempDir) throws Exception {
        Path serverData = tempDir.resolve("server-data");
        DataDirectory.open(serverData).close();
        List<String> serverNames = listNames(serverData);
        DataDirectoryException asNode = expectThrows(DataDirectoryException.class, "node on server data",
                () -> NodeDataDirectory.open(serverData));
        checkEquals(DataDirectoryException.Reason.UNRECOGNIZED, asNode.reason(), "reason for the server folder");
        checkEquals(serverNames, listNames(serverData), "the server's data directory is untouched");

        Path nodeFolder = tempDir.resolve("node");
        NodeDataDirectory.open(nodeFolder).close();
        List<String> nodeNames = listNames(nodeFolder);
        DataDirectoryException asServer = expectThrows(DataDirectoryException.class, "server on node folder",
                () -> DataDirectory.open(nodeFolder));
        checkEquals(DataDirectoryException.Reason.UNRECOGNIZED, asServer.reason(), "reason for the node folder");
        checkEquals(nodeNames, listNames(nodeFolder), "the node folder is untouched");
    }

    private static void damagedMarkerIsRefused(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        String id;
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            id = dir.nodeId();
            Files.write(dir.chunksDir().resolve("chunk_0"), new byte[] {7});
        }
        Path marker = folder.resolve("distributedfs-node.txt");
        for (String damaged : List.of(NodeDataDirectory.markerText(id.toUpperCase(java.util.Locale.ROOT)),
                NodeDataDirectory.markerText(id).replace("format=1", "format=2"),
                NodeDataDirectory.markerText(id).trim(), "")) {
            Files.writeString(marker, damaged, StandardCharsets.US_ASCII);
            DataDirectoryException e = expectThrows(DataDirectoryException.class, "open with a damaged marker",
                    () -> NodeDataDirectory.open(folder));
            checkEquals(DataDirectoryException.Reason.INVALID_MARKER, e.reason(), "reason");
            checkContains(e.getMessage(), "Do not delete the folder");
        }
        checkEquals(List.of("chunk_0"), listNames(folder.resolve("chunks")), "the chunk is left alone");
    }

    private static void junctionedChunksFolderIsRefused(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        NodeDataDirectory.open(folder).close();
        Path outside = Files.createDirectory(tempDir.resolve("outside"));
        Files.delete(folder.resolve("chunks"));
        createDirectoryJunction(folder.resolve("chunks"), outside);
        try {
            DataDirectoryException e = expectThrows(DataDirectoryException.class, "open", () -> NodeDataDirectory.open(folder));
            checkEquals(DataDirectoryException.Reason.UNSAFE_LAYOUT, e.reason(), "reason");
            checkEquals(List.of(), listNames(outside), "nothing was written behind the junction");
        } finally {
            Files.delete(folder.resolve("chunks")); // removes the junction itself, never its target
        }
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            check(Files.isDirectory(dir.chunksDir()), "once fixed, the folder opens again (the lock was released)");
        }
    }

    private static void leftoverMarkerTempIsRemoved(Path tempDir) throws Exception {
        Path folder = Files.createDirectory(tempDir.resolve("node"));
        Files.writeString(folder.resolve("distributedfs-node-123456.tmp"), "DistributedFS storage");
        try (NodeDataDirectory dir = NodeDataDirectory.open(folder)) {
            check(dir.newlyInitialized(), "the folder counts as new");
            checkEquals(List.of("chunks", "distributedfs-node.txt", "node.lock"), listNames(folder), "folder contents");
        }
    }

    private static void fileIsRefused(Path tempDir) throws Exception {
        Path file = Files.writeString(tempDir.resolve("node"), "a file");
        DataDirectoryException e = expectThrows(DataDirectoryException.class, "open", () -> NodeDataDirectory.open(file));
        checkEquals(DataDirectoryException.Reason.NOT_A_DIRECTORY, e.reason(), "reason");
        checkEquals("a file", Files.readString(file), "the file is untouched");
    }
}
