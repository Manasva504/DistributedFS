package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Test-only program that NetworkedRecoveryTest starts in a separate JVM: a server process with
 * remote storage nodes that is killed in the middle of an upload. It stores one upload
 * ({@link #UPLOAD_ID}) on the given nodes and stops itself with {@link Runtime#halt} right after
 * the K-th chunk has been stored, so no cleanup, finally block or shutdown hook runs: the data
 * directory and the nodes are left exactly as a killed server would leave them.
 *
 * <p>Usage: {@code [-Dreplicas=N] RemoteCrashUploadMain <dataDir> <haltAfterStores> <chunkSize> <uploadBytes> <nodePort>...}
 * (each copy counts as one store, so with 2 replicas, halting after 1 store stops between the
 * first and the second copy of chunk 0)
 *
 * <p>Exit code {@link #HALTED} means the program halted as planned; 0 means it never reached that
 * point (a test failure).
 */
public final class RemoteCrashUploadMain {

    static final int HALTED = 77;
    static final String UPLOAD_ID = "6".repeat(32);

    private RemoteCrashUploadMain() {
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        int haltAfter = Integer.parseInt(args[1]);
        int chunkSize = Integer.parseInt(args[2]);
        int uploadBytes = Integer.parseInt(args[3]);
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        int[] stores = {0};
        try (DataDirectory dataDirectory = DataDirectory.open(dataDir)) {
            List<ChunkStore> nodes = new ArrayList<>();
            for (int i = 4; i < args.length; i++) {
                RemoteStorageNode remote = RemoteStorageNode.connect(Integer.parseInt(args[i]), RemoteNodeConfig.defaults());
                nodes.add(new HaltingStore(remote, () -> {
                    if (++stores[0] == haltAfter) {
                        Runtime.getRuntime().halt(HALTED);
                    }
                }));
            }
            // -Dreplicas=2 stores two copies of each chunk, as the server does; the default is one.
            StorageCluster cluster = new StorageCluster(nodes, Integer.getInteger("replicas", 1));
            ManifestStore manifests = new ManifestStore(dataDirectory.manifestsDir());
            FileService files = new FileService(cluster, manifests, dataDirectory.tmpDir(), chunkSize, 1L << 30,
                    () -> UPLOAD_ID, quiet);
            Path workspace = files.newUploadWorkspace();
            byte[] data = new byte[uploadBytes];
            new Random(6).nextBytes(data);
            Path body = Files.write(workspace.resolve("body"), data);
            files.store(body, workspace, null);
        }
        // Reaching this line means the halt point was never reached.
    }

    /** A remote node that runs {@code afterStore} after each successful store. */
    private record HaltingStore(RemoteStorageNode node, Runnable afterStore) implements ChunkStore {
        @Override
        public long storeChunk(String chunkId, Path sourceFile) throws IOException {
            long stored = node.storeChunk(chunkId, sourceFile);
            afterStore.run();
            return stored;
        }

        @Override
        public long retrieveChunk(String chunkId, Path destination) throws IOException {
            return node.retrieveChunk(chunkId, destination);
        }

        @Override
        public boolean hasChunk(String chunkId) throws IOException {
            return node.hasChunk(chunkId);
        }

        @Override
        public boolean holdsRegularChunk(String chunkId) throws IOException {
            return node.holdsRegularChunk(chunkId);
        }

        @Override
        public boolean deleteChunk(String chunkId) throws IOException {
            return node.deleteChunk(chunkId);
        }

        @Override
        public List<Entry> listEntries() throws IOException {
            return node.listEntries();
        }

        @Override
        public int sweepTempFiles(Consumer<String> warnings) throws IOException {
            return node.sweepTempFiles(warnings);
        }

        @Override
        public Optional<Path> localDirectory() {
            return node.localDirectory();
        }

        @Override
        public Optional<String> nodeId() {
            return node.nodeId();
        }

        @Override
        public String describe() {
            return node.describe();
        }
    }
}
