package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only program that DeleteCrashTest starts in a separate JVM. It deletes one file with the
 * real {@link FileService#delete} and stops itself with {@link Runtime#halt} right after a chosen
 * step, so no cleanup, finally block or shutdown hook runs: the data directory and the nodes are
 * left exactly as a server killed at that moment would leave them.
 *
 * <p>Usage: {@code CrashDeleteMain <dataDir> <fileId> <step> [<nodePort> ...]}: without node ports
 * the data directory's local node folders are used; with them, the storage node processes on those
 * 127.0.0.1 ports. Steps (FileService.afterDeleteStep): {@code marker} (the deletion marker is
 * written, the manifest is still there), {@code manifest} (the manifest is removed: the commit
 * point), {@code chunk:<i>} (every copy of chunks 0 to i is deleted).
 *
 * <p>Exit code {@link #HALTED} means the step was reached; 0 means it never was (a test failure).
 */
public final class CrashDeleteMain {

    static final int HALTED = 77;

    private CrashDeleteMain() {
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        String fileId = args[1];
        String stage = args[2];
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        try (DataDirectory dataDirectory = DataDirectory.open(dataDir)) {
            List<ChunkStore> nodes = new ArrayList<>();
            if (args.length == 3) {
                for (Path nodeDir : dataDirectory.nodeDirs()) {
                    nodes.add(new StorageNode(nodeDir));
                }
            } else {
                for (int i = 3; i < args.length; i++) {
                    nodes.add(RemoteStorageNode.connect(Integer.parseInt(args[i]), RemoteNodeConfig.defaults()));
                }
            }
            StorageCluster cluster = new StorageCluster(nodes, Math.min(DistributedFsServer.REPLICAS, nodes.size()));
            ManifestStore manifests = new ManifestStore(dataDirectory.manifestsDir());
            FileService files = new FileService(cluster, manifests, dataDirectory.tmpDir(), 1024 * 1024, Long.MAX_VALUE,
                    quiet) {
                @Override
                void afterDeleteStep(String step) {
                    if (step.equals(stage)) {
                        System.out.flush();
                        Runtime.getRuntime().halt(HALTED);
                    }
                }
            };
            files.delete(fileId);
        }
        System.exit(0); // the step was never reached
    }
}
