package com.distributedfs;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * End-to-end demo: input file, chunking, storage cluster, retrieval, reconstruction, and
 * SHA-256 verification, using the existing components without re-implementing any of them.
 *
 * <p>Usage: {@code java -cp out com.distributedfs.DistributedFsDemo <inputFile> [--keep]}
 *
 * <p>The cluster is simulated: three storage nodes are three folders on this machine, and each
 * chunk is stored on exactly one node. There is no replication and no networking.
 *
 * <p><b>Workspace.</b> Every run creates a brand-new folder named {@code distributedfs-demo-<digits>}
 * in the system temp folder, and writes everything there:
 * <pre>
 *   chunks\          chunk files made by FileChunker
 *   nodes\node-N\    each storage node's folder
 *   retrieved\       chunks copied back out of the cluster
 *   reconstructed\   the rebuilt file
 * </pre>
 * The input file is only read. The workspace is deleted at the end, after success or failure,
 * unless {@code --keep} is given.
 *
 * <p><b>Limitations.</b> If the process is killed (Ctrl+C, Task Manager, power loss) the cleanup
 * does not run, and the workspace stays in the temp folder; its path is printed at the start of
 * the run. The run needs about four times the input file's size in free disk space. If the input
 * file changes while the demo runs, the result is a checksum mismatch.
 *
 * <p>Exit codes: 0 = the rebuilt file is verified identical; 1 = invalid input, an I/O error, or a
 * checksum mismatch; 2 = invalid command-line arguments (the same convention as FileChunker).
 */
public class DistributedFsDemo {

    static final int NODE_COUNT = 3;
    static final String WORKSPACE_PREFIX = "distributedfs-demo-";

    static final int EXIT_SUCCESS = 0;
    static final int EXIT_FAILURE = 1;
    static final int EXIT_USAGE = 2;

    public static void main(String[] args) {
        Path systemTempFolder = Path.of(System.getProperty("java.io.tmpdir"));
        int exitCode = new DistributedFsDemo().run(args, systemTempFolder, System.out, System.err);
        System.exit(exitCode);
    }

    /**
     * Runs the whole demo and returns the exit code instead of calling System.exit, so tests can
     * call it directly with their own folder in place of the system temp folder and with
     * captured output.
     *
     * @param workspaceParent the folder in which this run creates its new workspace
     */
    int run(String[] args, Path workspaceParent, PrintStream out, PrintStream err) {
        // 1. Command-line arguments: exactly one input file, plus an optional --keep.
        String inputArgument = null;
        boolean keep = false;
        for (String arg : args) {
            if (arg.equals("--keep")) {
                keep = true;
            } else if (arg.startsWith("--")) {
                return usageError(err, "unknown option: " + arg);
            } else if (inputArgument != null) {
                return usageError(err, "expected one input file, but got more than one");
            } else {
                inputArgument = arg;
            }
        }
        if (inputArgument == null || inputArgument.isBlank()) {
            return usageError(err, "no input file given");
        }
        Path inputFile;
        try {
            inputFile = Path.of(inputArgument);
        } catch (InvalidPathException e) {
            return usageError(err, "invalid path: " + e.getMessage());
        }

        out.println("DistributedFS demo: a simulated cluster of " + NODE_COUNT
                + " storage nodes (folders on this machine).");
        out.println("Each chunk is stored on exactly one node; there is no replication.");

        // 2. Fingerprint the original file. ChecksumService also validates it (missing, directory,
        //    unreadable), so an invalid input is reported before any workspace exists.
        String originalSha256;
        long inputSize;
        try {
            originalSha256 = new ChecksumService().calculateSha256(inputFile);
            inputSize = Files.size(inputFile);
        } catch (IllegalArgumentException | IOException e) {
            reportError(err, e);
            out.println("RESULT: FAILURE - the input file could not be read");
            return EXIT_FAILURE;
        }
        out.printf(Locale.ROOT, "Input file:            %s (%,d bytes)%n", inputFile.toAbsolutePath(), inputSize);
        out.println("Original SHA-256:      " + originalSha256);

        // 3. A brand-new workspace. createTempDirectory always makes a new folder with a unique
        //    name, so an existing folder is never reused, and everything inside belongs to this run.
        Path workspace;
        try {
            workspace = Files.createTempDirectory(workspaceParent, WORKSPACE_PREFIX);
        } catch (IOException e) {
            reportError(err, e);
            out.println("RESULT: FAILURE - could not create a workspace in " + workspaceParent);
            return EXIT_FAILURE;
        }
        out.println("Workspace:             " + workspace);

        int exitCode = EXIT_FAILURE;
        try {
            exitCode = runPipeline(inputFile, originalSha256, workspace, out);
        } catch (IllegalArgumentException | IOException e) {
            reportError(err, e);
            out.println("RESULT: FAILURE - the demo stopped because of the error above");
        } finally {
            // Runs after success and after failure. Only the exact folder created above is deleted.
            if (keep) {
                printKeptWorkspace(workspace, out);
            } else {
                try {
                    deleteWorkspace(workspace);
                    out.println("Workspace deleted.");
                } catch (IOException e) {
                    err.println("Warning: could not delete the workspace; please remove it manually: "
                            + workspace + " (" + e + ")");
                }
            }
        }
        return exitCode;
    }

    /**
     * Steps 4 to 9: split, store, retrieve, rebuild, and verify. Returns EXIT_SUCCESS only if the
     * rebuilt file has the same SHA-256 as the original.
     */
    private int runPipeline(Path inputFile, String originalSha256, Path workspace, PrintStream out)
            throws IOException {
        // 4. Split the input. FileChunker returns the chunk files in order: chunk_0, chunk_1, ...
        List<Path> chunkFiles = new FileChunker().chunk(inputFile, workspace.resolve("chunks"));
        out.printf(Locale.ROOT, "[1/5] Split into %d chunk(s) of up to %,d bytes%n",
                chunkFiles.size(), FileChunker.DEFAULT_CHUNK_SIZE);

        // 5. Three storage nodes, each with its own folder, coordinated by one cluster.
        List<StorageNode> nodes = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            nodes.add(createNode(workspace.resolve("nodes").resolve("node-" + i)));
        }
        StorageCluster cluster = new StorageCluster(nodes);
        out.println("[2/5] Started " + cluster.nodeCount() + " storage nodes");

        // 6. Store every chunk, recording the chunk IDs in their original order. This list is the
        //    only source of ordering from here on; folder listings are never used, because they
        //    would sort chunk_10 before chunk_2.
        List<String> chunkIdsInOrder = new ArrayList<>();
        int[] chunksPerNode = new int[NODE_COUNT];
        for (Path chunkFile : chunkFiles) {
            String chunkId = chunkFile.getFileName().toString();
            cluster.storeChunk(chunkId, chunkFile);
            chunkIdsInOrder.add(chunkId);
            chunksPerNode[cluster.locateChunk(chunkId)]++;
        }
        out.println("[3/5] Stored " + chunkIdsInOrder.size() + " chunk(s): " + describePlacement(chunksPerNode));

        // 7. Retrieve the chunks in that same order, each into its own new file.
        List<Path> retrievedInOrder = new ArrayList<>();
        Path retrievedDir = workspace.resolve("retrieved");
        for (String chunkId : chunkIdsInOrder) {
            Path copy = retrievedDir.resolve(chunkId);
            cluster.retrieveChunk(chunkId, copy);
            retrievedInOrder.add(copy);
        }
        out.println("[4/5] Retrieved " + retrievedInOrder.size() + " chunk(s) in their original order");

        // 8. Rebuild the file from the retrieved chunks, in order.
        Path reconstructed = workspace.resolve("reconstructed").resolve(inputFile.getFileName().toString());
        long bytesWritten = new FileReconstructor().reconstruct(retrievedInOrder, reconstructed);
        out.printf(Locale.ROOT, "[5/5] Reconstructed %,d bytes%n", bytesWritten);

        // 9. Verify: the rebuilt file must have exactly the same SHA-256 as the original.
        String reconstructedSha256 = new ChecksumService().calculateSha256(reconstructed);
        out.println("Reconstructed SHA-256: " + reconstructedSha256);
        if (reconstructedSha256.equals(originalSha256)) {
            out.println("RESULT: SUCCESS - the reconstructed file is identical to the original (SHA-256 match)");
            return EXIT_SUCCESS;
        }
        out.println("RESULT: FAILURE - checksum mismatch: the reconstructed file differs from the original");
        return EXIT_FAILURE;
    }

    /**
     * Creates the storage node for one folder.
     *
     * <p>Why this method exists: it is a small testability hook, like StorageNode.openForReading.
     * DistributedFsDemoTest (same package) overrides it to supply a node that fails partway
     * through a copy or returns corrupted data, so the failure and checksum-mismatch paths can be
     * tested. It is package-private, so it is not part of the public API.
     */
    StorageNode createNode(Path dir) throws IOException {
        return new StorageNode(dir);
    }

    /** "node 0: 1, node 1: 1, node 2: 1" */
    private static String describePlacement(int[] chunksPerNode) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < chunksPerNode.length; i++) {
            parts.add("node " + i + ": " + chunksPerNode[i]);
        }
        return String.join(", ", parts);
    }

    private static void printKeptWorkspace(Path workspace, PrintStream out) {
        out.println("Workspace kept for inspection: " + workspace);
        out.println("  chunks\\          chunk files made by FileChunker");
        out.println("  nodes\\node-N\\    each storage node's folder");
        out.println("  retrieved\\       chunks copied back out of the cluster");
        out.println("  reconstructed\\   the rebuilt file");
    }

    /** Deletes the workspace without ever following a link out of it (see {@link FileTrees#deleteTree}). */
    static void deleteWorkspace(Path workspace) throws IOException {
        FileTrees.deleteTree(workspace);
    }

    private static void reportError(PrintStream err, Exception e) {
        err.println("Error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        for (Throwable extra : e.getSuppressed()) {
            err.println("  Additional error: " + extra);
        }
    }

    private static int usageError(PrintStream err, String problem) {
        err.println("Error: " + problem);
        err.println("Usage: java -cp out com.distributedfs.DistributedFsDemo <inputFile> [--keep]");
        err.println("Splits <inputFile> into chunks, stores them on " + NODE_COUNT
                + " simulated storage nodes, retrieves and");
        err.println("reconstructs it, and checks that the result has the same SHA-256 as the original.");
        err.println("  --keep   keep the temporary workspace afterwards for inspection");
        return EXIT_USAGE;
    }
}
