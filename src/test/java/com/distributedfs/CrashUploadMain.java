package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only program that CrashRecoveryTest starts in a separate JVM. It stops itself with
 * {@link Runtime#halt} at a chosen moment, so no cleanup, finally block or shutdown hook runs: the
 * data directory is left exactly as a killed server would leave it.
 *
 * <p>Usage: {@code CrashUploadMain <dataDir> <stage> <chunkSize> <uploadBytes>}
 *
 * <p>Upload stages: the program first stores one complete file ({@link #OTHER_ID}, 10 bytes), then
 * stores the upload {@link #UPLOAD_ID} and halts:
 * <ul>
 *   <li>{@code pending-temp}: the pending marker's temp file is written but not yet renamed</li>
 *   <li>{@code after-marker}: the marker exists, before the first chunk is stored</li>
 *   <li>{@code after-chunk:K}: chunks 0..K are stored, before chunk K+1</li>
 *   <li>{@code mid-chunk:K}: while chunk K is being stored (its node temp file exists)</li>
 *   <li>{@code manifest-temp}: every chunk is stored, the manifest's temp file is written, not renamed</li>
 *   <li>{@code after-manifest}: the manifest is in place, before the marker is deleted (committed)</li>
 *   <li>{@code mid-rollback}: writing the manifest fails, and the rollback has deleted one chunk</li>
 * </ul>
 * Recovery stage {@code recover-mid-delete}: runs startup recovery on the data directory and halts
 * after its first chunk delete.
 *
 * <p>Exit code {@link #HALTED} means the stage was reached; 0 means it never was (a test failure).
 */
public final class CrashUploadMain {

    static final int HALTED = 77;
    static final String OTHER_ID = "a".repeat(32);
    static final String UPLOAD_ID = "5".repeat(32);

    private static volatile boolean armed = false;
    private static String stage;
    private static int deletes = 0;

    private CrashUploadMain() {
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args[0]);
        stage = args[1];
        int chunkSize = Integer.parseInt(args[2]);
        long uploadBytes = Long.parseLong(args[3]);
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);

        try (DataDirectory dataDirectory = DataDirectory.open(dataDir)) {
            List<StorageNode> nodes = new ArrayList<>();
            for (Path nodeDir : dataDirectory.nodeDirs()) {
                nodes.add(new CrashNode(nodeDir));
            }
            StorageCluster cluster = new StorageCluster(nodes);
            ManifestStore manifests = new CrashManifestStore(dataDirectory.manifestsDir());

            if (stage.equals("recover-mid-delete")) {
                armed = true;
                new StartupRecovery(nodes, cluster, manifests, dataDirectory.tmpDir(), quiet, quiet).run();
                System.exit(0); // the stage was never reached
            }

            String[] ids = {OTHER_ID, UPLOAD_ID};
            int[] next = {0};
            FileService files = new FileService(cluster, manifests, dataDirectory.tmpDir(), chunkSize,
                    Long.MAX_VALUE, () -> ids[next[0]++], quiet);
            store(files, 10, 1);
            armed = true;
            store(files, uploadBytes, 2);
        } catch (IOException e) {
            if (!stage.equals("mid-rollback")) {
                throw e;
            }
        }
        System.exit(0); // the stage was never reached
    }

    /** Stores {@code size} bytes of the deterministic pattern CrashRecoveryTest expects. */
    private static void store(FileService files, long size, int seed) throws IOException {
        Path workspace = files.newUploadWorkspace();
        Path body = workspace.resolve("body");
        try (OutputStream out = Files.newOutputStream(body)) {
            byte[] buffer = new byte[64 * 1024];
            long written = 0;
            while (written < size) {
                int n = (int) Math.min(buffer.length, size - written);
                for (int i = 0; i < n; i++) {
                    buffer[i] = patternByte(written + i, seed);
                }
                out.write(buffer, 0, n);
                written += n;
            }
        }
        files.store(body, workspace, null);
        files.deleteWorkspace(workspace);
    }

    /** Byte {@code index} of the pattern with this seed; CrashRecoveryTest uses the same formula. */
    static byte patternByte(long index, int seed) {
        return (byte) (index * 31 + seed * 7);
    }

    private static void halt() {
        System.out.flush();
        Runtime.getRuntime().halt(HALTED);
    }

    private static String chunk(int index) {
        return FileIds.chunkId(UPLOAD_ID, index);
    }

    private static final class CrashNode extends StorageNode {
        private String storing;

        CrashNode(Path dir) throws IOException {
            super(dir);
        }

        @Override
        public long storeChunk(String chunkId, Path sourceFile) throws IOException {
            if (armed) {
                if (stage.equals("after-marker") && chunkId.equals(chunk(0))) {
                    halt();
                }
                if (stage.startsWith("after-chunk:")
                        && chunkId.equals(chunk(Integer.parseInt(stage.substring("after-chunk:".length())) + 1))) {
                    halt();
                }
            }
            storing = chunkId;
            return super.storeChunk(chunkId, sourceFile);
        }

        @Override
        InputStream openForReading(Path file) throws IOException {
            // Called after storeChunk has created the temp file, before any byte is copied.
            if (armed && stage.startsWith("mid-chunk:")
                    && chunk(Integer.parseInt(stage.substring("mid-chunk:".length()))).equals(storing)) {
                halt();
            }
            return super.openForReading(file);
        }

        @Override
        void deleteChunkFile(Path chunkFile) throws IOException {
            if (armed && (stage.equals("mid-rollback") || stage.equals("recover-mid-delete"))) {
                synchronized (CrashUploadMain.class) {
                    deletes++;
                    if (deletes == 2) {
                        halt(); // the first delete succeeded; this one never happens
                    }
                }
            }
            super.deleteChunkFile(chunkFile);
        }
    }

    private static final class CrashManifestStore extends ManifestStore {
        CrashManifestStore(Path dir) throws IOException {
            super(dir);
        }

        @Override
        void moveIntoPlace(Path source, Path target) throws IOException {
            String name = target.getFileName().toString();
            if (armed && stage.equals("pending-temp") && name.endsWith(PENDING_SUFFIX)) {
                halt();
            }
            if (armed && stage.equals("manifest-temp") && name.endsWith(MANIFEST_SUFFIX)) {
                halt();
            }
            if (armed && stage.equals("mid-rollback") && name.endsWith(MANIFEST_SUFFIX)) {
                throw new IOException("simulated manifest write failure");
            }
            super.moveIntoPlace(source, target);
        }

        @Override
        boolean deletePending(String fileId) throws IOException {
            if (armed && stage.equals("after-manifest")) {
                halt();
            }
            return super.deletePending(fileId);
        }
    }
}
