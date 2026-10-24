package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.expectCannotTell;
import static com.distributedfs.NodeTestKit.fastConfig;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.sha256;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plain-Java tests for StorageNodeServer as a program: command-line errors, and real node
 * processes (child JVMs with the same class path) that are restarted, run twice on one folder, or
 * killed in the middle of a store. Run: {@code java -cp "out;out-test" com.distributedfs.StorageNodeServerTest}.
 */
public class StorageNodeServerTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("nodeserver-test-");

    private static final Pattern READY = Pattern.compile(
            "DistributedFS storage node ([0-9a-f]{32}) listening on http://127\\.0\\.0\\.1:(\\d+)/ .*");

    public static void main(String[] args) {
        RUNNER.run("command-line mistakes are exit code 2 with the usage line", StorageNodeServerTest::usageErrors);
        RUNNER.run("a node process serves chunks, and after a restart has the same ID and the same chunks",
                StorageNodeServerTest::restartKeepsIdAndChunks);
        RUNNER.run("a second node process on the same folder exits with code 1, and the first keeps serving",
                StorageNodeServerTest::secondProcessIsRefused);
        RUNNER.run("a node process will not start in the server's data directory", StorageNodeServerTest::serverDataIsRefused);
        RUNNER.run("a node killed in the middle of a store leaves no chunk, and its restart removes the temp file",
                StorageNodeServerTest::killDuringStore);
        RUNNER.run("a killed node is 'cannot tell' for checks and 'not stored' for stores, never 'absent'",
                StorageNodeServerTest::killedNodeIsNeverAbsent);
        RUNNER.finish();
    }

    private static void usageErrors(Path tempDir) {
        List<String[]> mistakes = List.of(new String[] {}, new String[] {"--data-dir", tempDir.toString()},
                new String[] {"--port", "0"}, new String[] {"--data-dir", tempDir.toString(), "--port", "65536"},
                new String[] {"--data-dir", tempDir.toString(), "--port", "08080"},
                new String[] {"--data-dir", tempDir.toString(), "--port", "0", "--max-chunk-mb", "0"},
                new String[] {"--data-dir", tempDir.toString(), "--port", "0", "--max-chunk-mb", "65"},
                new String[] {"--data-dir", tempDir.toString(), "--port", "0", "--port", "1"},
                new String[] {"--data-dir", tempDir.toString(), "--port", "0", "--verbose"},
                new String[] {"--data-dir", tempDir.toString(), "--port"});
        for (String[] args : mistakes) {
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            int code = StorageNodeServer.run(args, new PrintStream(new ByteArrayOutputStream(), true),
                    new PrintStream(err, true, StandardCharsets.UTF_8));
            checkEquals(StorageNodeServer.EXIT_USAGE, code, "exit code for " + Arrays.toString(args));
            checkContains(err.toString(StandardCharsets.UTF_8), StorageNodeServer.USAGE);
        }
    }

    private static void restartKeepsIdAndChunks(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        byte[] data = randomBytes(300_000, 1);
        Path source = Files.write(tempDir.resolve("source"), data);
        String firstId;
        try (NodeProcess node = NodeProcess.start(folder)) {
            firstId = node.nodeId;
            try (RemoteStorageNode remote = RemoteStorageNode.connect(node.port, fastConfig())) {
                checkEquals(firstId, remote.nodeId().orElseThrow(), "node ID over HTTP");
                remote.storeChunk("file_0", source);
            }
            node.waitForOutput("POST /chunks/file_0 201 300000 bytes");
        }
        try (NodeProcess node = NodeProcess.start(folder);
             RemoteStorageNode remote = RemoteStorageNode.connect(node.port, fastConfig())) {
            checkEquals(firstId, node.nodeId, "node ID after the restart");
            Path copy = tempDir.resolve("copy");
            remote.retrieveChunk("file_0", copy);
            check(Arrays.equals(data, Files.readAllBytes(copy)), "the chunk after the restart differs");
        }
    }

    private static void secondProcessIsRefused(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        try (NodeProcess first = NodeProcess.start(folder)) {
            String output = outputOfExitedProcess(NodeProcess.launch(folder), "the second node process");
            checkContains(output, "already in use");
            try (RemoteStorageNode remote = RemoteStorageNode.connect(first.port, fastConfig())) {
                remote.health();
            }
        }
    }

    private static void serverDataIsRefused(Path tempDir) throws Exception {
        Path serverData = tempDir.resolve("server-data");
        DataDirectory.open(serverData).close();
        List<String> before = listNames(serverData);
        String output = outputOfExitedProcess(NodeProcess.launch(serverData), "the node process");
        checkContains(output, "not a DistributedFS storage node folder");
        checkEquals(before, listNames(serverData), "the server's data directory is untouched");
    }

    private static void killDuringStore(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        byte[] data = randomBytes(200_000, 2);
        try (NodeProcess node = NodeProcess.start(folder);
             Socket socket = new Socket(InetAddress.getLoopbackAddress(), node.port)) {
            socket.setSoTimeout(15_000);
            OutputStream out = socket.getOutputStream();
            out.write(("POST /chunks/file_0 HTTP/1.1\r\n" + host(node.port) + "\r\nContent-Length: " + data.length
                    + "\r\nX-Chunk-Sha256: " + sha256(data) + "\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.write(data, 0, 100_000);
            out.flush();
            Path chunks = folder.resolve("chunks");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (listNames(chunks).isEmpty()) {
                check(System.nanoTime() < deadline, "the store's temp file never appeared");
                Thread.sleep(20);
            }
            node.kill();
            checkEquals(-1, NodeTestKit.readResponse(socket.getInputStream()).status(), "a killed node sends no answer");
            List<String> left = listNames(chunks);
            checkEquals(1, left.size(), "one temp file is left by the kill: " + left);
            check(left.get(0).matches("file_0-[0-9]+\\.tmp"), "the leftover is the store's temp file: " + left);
        }
        try (NodeProcess node = NodeProcess.start(folder)) {
            checkContains(node.output(), "Removed 1 stale temp file from the chunk folder.");
            checkEquals(List.of(), listNames(folder.resolve("chunks")), "no chunk and no temp file after the restart");
        }
    }

    private static void killedNodeIsNeverAbsent(Path tempDir) throws Exception {
        Path folder = tempDir.resolve("node");
        Path source = Files.write(tempDir.resolve("source"), randomBytes(1000, 3));
        try (NodeProcess node = NodeProcess.start(folder);
             RemoteStorageNode remote = RemoteStorageNode.connect(node.port, fastConfig())) {
            checkEquals(false, remote.hasChunk("file_0"), "before the kill");
            node.kill();
            expectCannotTell("hasChunk", () -> remote.hasChunk("file_0"));
            expectCannotTell("holdsRegularChunk", () -> remote.holdsRegularChunk("file_0"));
            expectCannotTell("retrieve", () -> remote.retrieveChunk("file_0", tempDir.resolve("copy")));
            expectCannotTell("delete", () -> remote.deleteChunk("file_0"));
            checkContains(expectCannotTell("store", () -> remote.storeChunk("file_0", source)).getMessage(),
                    "could not be reached");
        }
    }

    /**
     * Waits up to 30 seconds for a node process that must refuse to start, requires exit code 1,
     * and returns its output. A process that keeps running (it started after all) is killed and
     * the test fails, so a broken refusal shows up as a failure instead of a hang.
     */
    private static String outputOfExitedProcess(Process process, String what) throws Exception {
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(30, TimeUnit.SECONDS);
            throw new AssertionError(what + " kept running instead of refusing to start");
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        checkEquals(StorageNodeServer.EXIT_FAILURE, process.exitValue(), "exit code of " + what + "; output:\n" + output);
        return output;
    }

    /** A node server in a child JVM, started on port 0, with its output collected. */
    private static final class NodeProcess implements AutoCloseable {
        final Process process;
        final List<String> lines = new CopyOnWriteArrayList<>();
        String nodeId;
        int port;

        private NodeProcess(Process process) {
            this.process = process;
            Thread reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(),
                        StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        lines.add(line);
                    }
                } catch (IOException e) {
                    // the process ended
                }
            }, "node-process-output");
            reader.setDaemon(true);
            reader.start();
        }

        static Process launch(Path folder) throws IOException {
            return new ProcessBuilder(NodeTestKit.javaExecutable(), "-cp", NodeTestKit.absoluteClassPath(),
                    "com.distributedfs.StorageNodeServer", "--data-dir", folder.toString(), "--port", "0")
                    .redirectErrorStream(true)
                    .start();
        }

        static NodeProcess start(Path folder) throws Exception {
            NodeProcess node = new NodeProcess(launch(folder));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true) {
                for (String line : node.lines) {
                    Matcher ready = READY.matcher(line);
                    if (ready.matches()) {
                        node.nodeId = ready.group(1);
                        node.port = Integer.parseInt(ready.group(2));
                        return node;
                    }
                }
                if (!node.process.isAlive() || System.nanoTime() > deadline) {
                    node.kill();
                    throw new AssertionError("the node process did not start; its output:\n" + node.output());
                }
                Thread.sleep(20);
            }
        }

        String output() {
            return String.join("\n", lines);
        }

        void waitForOutput(String part) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!output().contains(part)) {
                check(System.nanoTime() < deadline, "timed out waiting for \"" + part + "\" in:\n" + output());
                Thread.sleep(20);
            }
        }

        /** Kills the process at once (no shutdown hook runs) and waits until it has ended. */
        void kill() {
            process.destroyForcibly();
            try {
                process.waitFor(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            kill();
        }
    }
}
