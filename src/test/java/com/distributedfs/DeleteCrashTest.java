package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.startNode;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Plain-Java tests of a delete killed at each step (PR #12). Each test uploads two files, starts
 * {@link CrashDeleteMain} in a separate JVM, which deletes one of them and halts right after a
 * chosen step, as a killed server would stop, then starts the server again and checks what startup
 * recovery made of it: killed before the commit point (the manifest's removal), the file is kept
 * whole; killed after it, at any later step, the delete is finished. The other file is never
 * touched. Once with local node folders, once with storage node processes.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.DeleteCrashTest}.
 */
public class DeleteCrashTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("deletecrash-test-");

    private static final int CHUNK = 64 * 1024;
    private static final int CHUNKS = 3;

    public static void main(String[] args) {
        for (boolean remote : new boolean[] {false, true}) {
            String where = remote ? " (storage node processes)" : " (local node folders)";
            RUNNER.run("killed after the marker, before the commit: the start undoes the delete, the file is whole" + where,
                    tempDir -> crash(tempDir, remote, "marker", false));
            RUNNER.run("killed right after the commit: the start deletes every copy and the marker" + where,
                    tempDir -> crash(tempDir, remote, "manifest", true));
            RUNNER.run("killed after the first chunk: the start finishes the delete" + where,
                    tempDir -> crash(tempDir, remote, "chunk:0", true));
            RUNNER.run("killed after the last chunk, before the marker: the start removes the marker" + where,
                    tempDir -> crash(tempDir, remote, "chunk:" + (CHUNKS - 1), true));
        }
        RUNNER.finish();
    }

    private static void crash(Path tempDir, boolean remote, String step, boolean committed) throws Exception {
        List<NodeTestKit.NodeHandle> nodes = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        if (remote) {
            for (int i = 0; i < 3; i++) {
                nodes.add(startNode(tempDir.resolve("node-" + i)));
                ports.add(nodes.get(i).port());
            }
        }
        try {
            byte[] keepData = randomBytes(CHUNK, 1);
            byte[] data = randomBytes(CHUNKS * CHUNK, 2);
            String keep;
            String id;
            DistributedFsServer first = server(tempDir, ports, new NodeTestKit.Lines());
            try {
                keep = fileId(upload(first.port(), keepData));
                id = fileId(upload(first.port(), data));
            } finally {
                first.stop();
            }

            List<String> childArgs = new ArrayList<>(List.of(tempDir.resolve("data").toString(), id, step));
            for (int port : ports) {
                childArgs.add(String.valueOf(port));
            }
            checkEquals(CrashDeleteMain.HALTED, runChild(childArgs), "the child's exit code (did it reach " + step + "?)");
            Path manifests = tempDir.resolve("data").resolve("manifests");
            checkEquals(!committed, Files.exists(manifests.resolve(id + ".manifest")), "the manifest after the kill");
            check(Files.exists(manifests.resolve(id + ManifestStore.DELETING_SUFFIX)), "no deletion marker after the kill");

            NodeTestKit.Lines out = new NodeTestKit.Lines();
            DistributedFsServer again = server(tempDir, ports, out);
            try {
                check(!Files.exists(manifests.resolve(id + ManifestStore.DELETING_SUFFIX)), "the marker after the start");
                check(!out.text().contains("ORPHAN_CHUNK"), "orphans reported: " + out.text());
                NodeTestKit.RawResponse download = download(again.port(), id);
                if (committed) {
                    checkEquals(404, download.status(), "download after the start");
                    checkEquals(0, copiesOf(tempDir, nodes, id), "copies after the start");
                } else {
                    checkEquals(200, download.status(), "download after the start");
                    check(Arrays.equals(data, download.body()), "the file after the start");
                    checkEquals(2 * CHUNKS, copiesOf(tempDir, nodes, id), "copies after the start");
                }
                check(Arrays.equals(keepData, download(again.port(), keep).body()), "the other file");
                checkEquals(2, copiesOf(tempDir, nodes, keep), "copies of the other file");
            } finally {
                again.stop();
            }
        } finally {
            for (NodeTestKit.NodeHandle node : nodes) {
                node.close();
            }
        }
    }

    private static DistributedFsServer server(Path tempDir, List<Integer> ports, NodeTestKit.Lines out) throws IOException {
        return startServer(tempDir.resolve("data"), ports, out, new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    /** Runs CrashDeleteMain in a new JVM with the same class path and returns its exit code. */
    private static int runChild(List<String> args) throws Exception {
        List<String> command = new ArrayList<>(List.of(NodeTestKit.javaExecutable(), "-cp", NodeTestKit.absoluteClassPath(),
                CrashDeleteMain.class.getName()));
        command.addAll(args);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = process.getInputStream().readAllBytes();
        if (!process.waitFor(120, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("the child did not finish");
        }
        if (process.exitValue() != CrashDeleteMain.HALTED) {
            System.out.println("      child output: " + new String(output, StandardCharsets.UTF_8));
        }
        return process.exitValue();
    }

    private static int copiesOf(Path tempDir, List<NodeTestKit.NodeHandle> nodes, String id) throws IOException {
        List<Path> folders = new ArrayList<>();
        if (nodes.isEmpty()) {
            for (int i = 0; i < 3; i++) {
                folders.add(tempDir.resolve("data").resolve("nodes").resolve("node-" + i));
            }
        } else {
            for (NodeTestKit.NodeHandle node : nodes) {
                folders.add(node.chunksDir());
            }
        }
        int copies = 0;
        for (Path folder : folders) {
            for (String name : listNames(folder)) {
                if (name.startsWith(id + "_")) {
                    copies++;
                }
            }
        }
        return copies;
    }
}
