package com.distributedfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Plain-Java tests for DataDirectory (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.DataDirectoryTest}
 *
 * <p>Lock contention between processes is tested with real child processes: this class, started
 * again with the internal arguments {@code --hold-lock <folder>}, opens the folder, prints
 * {@code LOCKED} (or {@code REFUSED <reason>}), and holds the lock until its standard input is
 * closed. Junctions are real directory junctions (mklink /J, no admin rights needed).
 * The program exits with code 1 if any test fails.
 */
public class DataDirectoryTest {

    /** Upper limit for every wait on a child process, so a hang fails the test instead of blocking it. */
    private static final int CHILD_TIMEOUT_SECONDS = 60;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length == 2 && args[0].equals("--hold-lock")) {
            System.exit(holdLock(Path.of(args[1])));
            return;
        }
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.DataDirectoryTest");
            System.exit(2);
            return;
        }

        // Initialization and restart
        runTest("a missing folder is initialized with marker, lock file and subfolders",
                DataDirectoryTest::missingFolderIsInitialized);
        runTest("an empty folder is initialized", DataDirectoryTest::emptyFolderIsInitialized);
        runTest("a folder holding only server.lock counts as new, and the lock file is not changed",
                DataDirectoryTest::onlyLockFileCountsAsNew);
        runTest("a restart opens an initialized folder without changing anything",
                DataDirectoryTest::restartChangesNothing);

        // Folders the server must not use
        runTest("an unrecognized non-empty folder is refused and left exactly as it was",
                DataDirectoryTest::unrecognizedFolderIsUntouched);
        runTest("an unrecognized folder that also holds server.lock is refused and left as it was",
                DataDirectoryTest::unrecognizedFolderWithLockFile);
        runTest("a path that is a file is refused and the file is unchanged", DataDirectoryTest::pathThatIsAFile);
        runTest("an invalid marker is refused, nothing else is created, and the lock is released",
                DataDirectoryTest::invalidMarkersAreRefused);
        runTest("a leftover marker temp file from an interrupted first start is removed and the folder initialized",
                DataDirectoryTest::leftoverMarkerTempIsReplaced);
        runTest("an entry named like a marker temp file that is not a leftover keeps the folder unrecognized and untouched",
                DataDirectoryTest::foreignTempNamedEntriesAreUntouched);
        runTest("a folder that already has a marker leaves entries named like marker temp files alone",
                DataDirectoryTest::initializedFolderLeavesTempNamedEntriesAlone);
        runTest("a required subfolder that is a file or a junction is refused, and nothing is created through it",
                DataDirectoryTest::unsafeLayoutIsRefused);
        runTest("a server.lock that is a folder is refused", DataDirectoryTest::lockFileThatIsAFolder);

        // Locking
        runTest("a second open in the same process is refused while the first holds the lock",
                DataDirectoryTest::sameProcessContention);
        runTest("the lock follows the folder, not its path: a junction to a locked folder is refused",
                DataDirectoryTest::junctionAliasIsLocked);
        runTest("a lock held by another process refuses the open until that process releases it",
                DataDirectoryTest::otherProcessHoldsLock);
        runTest("a killed process releases its lock", DataDirectoryTest::killedProcessReleasesLock);
        runTest("another process is refused while this process holds the lock",
                DataDirectoryTest::otherProcessIsRefused);
        runTest("every failed open releases the lock, and a refused open leaves the holder's lock alone",
                DataDirectoryTest::failedOpenReleasesLock);

        // API details
        runTest("accessors point inside the normalized root, close is idempotent, null is rejected",
                DataDirectoryTest::accessorsAndClose);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Initialization and restart
    // ---------------------------------------------------------------------

    private static void missingFolderIsInitialized(Path tempDir) throws IOException {
        Path root = tempDir.resolve("parent").resolve("data");
        try (DataDirectory data = DataDirectory.open(root)) {
            check(data.newlyInitialized(), "a new folder must report newlyInitialized");
            checkInitializedLayout(root);
        }
    }

    private static void emptyFolderIsInitialized(Path tempDir) throws IOException {
        Path root = Files.createDirectory(tempDir.resolve("data"));
        try (DataDirectory data = DataDirectory.open(root)) {
            check(data.newlyInitialized(), "an empty folder must report newlyInitialized");
            checkInitializedLayout(root);
        }
    }

    private static void onlyLockFileCountsAsNew(Path tempDir) throws IOException {
        Path root = Files.createDirectory(tempDir.resolve("data"));
        Path lockFile = Files.writeString(root.resolve(DataDirectory.LOCK_NAME), "left over");
        try (DataDirectory data = DataDirectory.open(root)) {
            check(data.newlyInitialized(), "a folder with only server.lock must count as new");
            checkInitializedLayout(root);
        }
        checkEquals("left over", Files.readString(lockFile), "server.lock content (never written)");
    }

    private static void restartChangesNothing(Path tempDir) throws IOException {
        Path root = tempDir.resolve("data");
        try (DataDirectory data = DataDirectory.open(root)) {
            Files.writeString(data.manifestsDir().resolve("0123456789abcdef0123456789abcdef.manifest"), "stored");
            Files.writeString(data.nodeDirs().get(1).resolve("chunk"), "chunk bytes");
        }
        Map<String, String> before = snapshot(root);
        try (DataDirectory data = DataDirectory.open(root)) {
            check(!data.newlyInitialized(), "a restart must not report newlyInitialized");
            checkEquals(before, snapshot(root), "the folder after the restart");
        }
        checkEquals(before, snapshot(root), "the folder after closing again");
    }

    // ---------------------------------------------------------------------
    // Folders the server must not use
    // ---------------------------------------------------------------------

    private static void unrecognizedFolderIsUntouched(Path tempDir) throws IOException {
        Path root = Files.createDirectory(tempDir.resolve("photos"));
        Files.writeString(root.resolve("notes.txt"), "my notes");
        Files.createDirectory(root.resolve("2026"));
        Files.write(root.resolve("2026").resolve("img.bin"), new byte[] {1, 2, 3});
        Files.createDirectory(root.resolve("empty-folder"));
        Map<String, String> before = snapshot(root);

        expectRefused(DataDirectoryException.Reason.UNRECOGNIZED, root);
        checkEquals(before, snapshot(root), "the folder after the refusal");
    }

    private static void unrecognizedFolderWithLockFile(Path tempDir) throws IOException {
        Path root = Files.createDirectory(tempDir.resolve("other"));
        Files.writeString(root.resolve(DataDirectory.LOCK_NAME), "not ours");
        Files.writeString(root.resolve("data.db"), "someone else's data");
        Map<String, String> before = snapshot(root);

        expectRefused(DataDirectoryException.Reason.UNRECOGNIZED, root);
        checkEquals(before, snapshot(root), "the folder after the refusal");
    }

    private static void pathThatIsAFile(Path tempDir) throws IOException {
        Path file = Files.writeString(tempDir.resolve("data"), "a file");
        Map<String, String> before = snapshot(tempDir);
        expectRefused(DataDirectoryException.Reason.NOT_A_DIRECTORY, file);
        checkEquals(before, snapshot(tempDir), "the parent folder after the refusal");
    }

    private static void invalidMarkersAreRefused(Path tempDir) throws IOException {
        byte[] good = DataDirectory.MARKER_CONTENT.getBytes(StandardCharsets.US_ASCII);
        List<Object[]> variants = new ArrayList<>();
        variants.add(new Object[] {"different text", "hello\n".getBytes(StandardCharsets.US_ASCII)});
        variants.add(new Object[] {"an extra line feed", (DataDirectory.MARKER_CONTENT + "\n").getBytes(StandardCharsets.US_ASCII)});
        variants.add(new Object[] {"CRLF line endings",
                DataDirectory.MARKER_CONTENT.replace("\n", "\r\n").getBytes(StandardCharsets.US_ASCII)});
        variants.add(new Object[] {"an empty file", new byte[0]});
        variants.add(new Object[] {"a cut-off marker", Arrays.copyOf(good, good.length - 5)});
        variants.add(new Object[] {"another node count",
                DataDirectory.MARKER_CONTENT.replace("nodes=3", "nodes=4").getBytes(StandardCharsets.US_ASCII)});
        variants.add(new Object[] {"another format",
                DataDirectory.MARKER_CONTENT.replace("format=1", "format=2").getBytes(StandardCharsets.US_ASCII)});
        variants.add(new Object[] {"a much larger file", new byte[64 * 1024]});
        variants.add(new Object[] {"a folder", null});

        int n = 0;
        for (Object[] variant : variants) {
            String what = (String) variant[0];
            Path root = Files.createDirectory(tempDir.resolve("data-" + n++));
            Path marker = root.resolve(DataDirectory.MARKER_NAME);
            if (variant[1] == null) {
                Files.createDirectory(marker);
            } else {
                Files.write(marker, (byte[]) variant[1]);
            }
            Map<String, String> before = snapshot(root);

            DataDirectoryException refusal = expectRefused(DataDirectoryException.Reason.INVALID_MARKER, root);

            // The folder has the marker's name, so creating server.lock is allowed; nothing else is.
            Map<String, String> after = snapshot(root);
            after.remove(DataDirectory.LOCK_NAME);
            checkEquals(before, after, what + ": the folder after the refusal (apart from server.lock)");
            checkLockIsFree(root);

            // The folder may hold stored files, so the message must never suggest deleting it, and
            // must say exactly what to restore.
            checkContains(refusal.getMessage(), "Do not delete the folder");
            checkContains(refusal.getMessage(), DataDirectory.MARKER_CONTENT);
        }
    }

    private static void leftoverMarkerTempIsReplaced(Path tempDir) throws IOException {
        byte[] good = DataDirectory.MARKER_CONTENT.getBytes(StandardCharsets.US_ASCII);
        // What a first start killed while writing its marker can leave: the temp file in any state.
        List<Object[]> leftovers = List.of(
                new Object[] {"an empty temp file", new byte[0]},
                new Object[] {"a partly written temp file", Arrays.copyOf(good, 10)},
                new Object[] {"a full-size temp file of zero bytes (written, never reached the disk)", new byte[good.length]},
                new Object[] {"a complete temp file (killed just before the rename)", good});
        int n = 0;
        for (Object[] leftover : leftovers) {
            Path root = Files.createDirectory(tempDir.resolve("data-" + n++));
            Files.writeString(root.resolve(DataDirectory.LOCK_NAME), "");
            Files.write(root.resolve("distributedfs-data-1234567890.tmp"), (byte[]) leftover[1]);
            try (DataDirectory data = DataDirectory.open(root)) {
                check(data.newlyInitialized(), leftover[0] + ": the folder must count as new");
                checkInitializedLayout(root); // the marker is complete and the leftover is gone
            }
        }

        // Two leftovers from two interrupted starts, the shortest and longest possible names.
        Path root = Files.createDirectory(tempDir.resolve("data-two"));
        Files.write(root.resolve("distributedfs-data-1.tmp"), new byte[0]);
        Files.write(root.resolve("distributedfs-data-18446744073709551615.tmp"), Arrays.copyOf(good, 3));
        try (DataDirectory data = DataDirectory.open(root)) {
            check(data.newlyInitialized(), "two leftovers: the folder must count as new");
            checkInitializedLayout(root);
        }
    }

    private static void foreignTempNamedEntriesAreUntouched(Path tempDir) throws IOException {
        int markerLength = DataDirectory.MARKER_CONTENT.length();
        List<Path> roots = new ArrayList<>();

        Path folderNamedLikeTemp = Files.createDirectory(tempDir.resolve("a"));
        Files.createDirectory(folderNamedLikeTemp.resolve("distributedfs-data-1.tmp"));
        roots.add(folderNamedLikeTemp);

        Path largerThanMarker = Files.createDirectory(tempDir.resolve("b"));
        Files.write(largerThanMarker.resolve("distributedfs-data-2.tmp"), new byte[markerLength + 1]);
        roots.add(largerThanMarker);

        Path leftoverPlusOtherFile = Files.createDirectory(tempDir.resolve("c"));
        Files.write(leftoverPlusOtherFile.resolve("distributedfs-data-3.tmp"), new byte[0]);
        Files.writeString(leftoverPlusOtherFile.resolve("notes.txt"), "mine");
        roots.add(leftoverPlusOtherFile);

        Path junctionNamedLikeTemp = Files.createDirectory(tempDir.resolve("d"));
        Path target = Files.createDirectory(tempDir.resolve("target"));
        Files.writeString(target.resolve("precious.txt"), "precious");
        createDirectoryJunction(junctionNamedLikeTemp.resolve("distributedfs-data-4.tmp"), target);
        roots.add(junctionNamedLikeTemp);

        // Names that differ from the temp name form, each alone in its own folder so that each one
        // is the only reason its folder is refused.
        int n = 0;
        for (String name : List.of("distributedfs-data-.tmp", "DISTRIBUTEDFS-DATA-5.TMP", "distributedfs-data-6.tmp.bak",
                "distributedfs-data-123456789012345678901.tmp")) {
            Path nameThatDoesNotMatch = Files.createDirectory(tempDir.resolve("e" + n++));
            Files.write(nameThatDoesNotMatch.resolve(name), new byte[0]);
            roots.add(nameThatDoesNotMatch);
        }

        Path withLockFile = Files.createDirectory(tempDir.resolve("f"));
        Files.writeString(withLockFile.resolve(DataDirectory.LOCK_NAME), "");
        Files.createDirectory(withLockFile.resolve("distributedfs-data-7.tmp"));
        roots.add(withLockFile);

        for (Path root : roots) {
            Map<String, String> before = snapshot(root);
            expectRefused(DataDirectoryException.Reason.UNRECOGNIZED, root);
            checkEquals(before, snapshot(root), root.getFileName() + ": the folder after the refusal");
        }
        checkEquals("precious", Files.readString(target.resolve("precious.txt")), "the junction's target file");
    }

    private static void initializedFolderLeavesTempNamedEntriesAlone(Path tempDir) throws IOException {
        Path root = tempDir.resolve("data");
        DataDirectory.open(root).close();
        Files.write(root.resolve("distributedfs-data-8.tmp"), new byte[0]);
        Map<String, String> before = snapshot(root);
        try (DataDirectory data = DataDirectory.open(root)) {
            check(!data.newlyInitialized(), "a folder with a marker is not new");
            checkEquals(before, snapshot(root), "the folder after the restart");
        }
    }

    private static void unsafeLayoutIsRefused(Path tempDir) throws IOException {
        // tmp is a regular file
        Path root = tempDir.resolve("data");
        DataDirectory.open(root).close();
        deleteTree(root.resolve("tmp"));
        Files.writeString(root.resolve("tmp"), "a file named tmp");
        expectRefused(DataDirectoryException.Reason.UNSAFE_LAYOUT, root);
        checkEquals("a file named tmp", Files.readString(root.resolve("tmp")), "the file named tmp");
        checkLockIsFree(root);
        Files.delete(root.resolve("tmp"));
        DataDirectory.open(root).close(); // repaired: opens again

        // manifests, nodes and one node folder are each replaced by a junction to an outside folder
        for (String relative : List.of("manifests", "nodes", "nodes/node-1")) {
            Path folder = root.resolve(relative);
            Path target = Files.createDirectory(tempDir.resolve("outside-" + relative.replace('/', '-')));
            deleteTree(folder);
            createDirectoryJunction(folder, target);

            expectRefused(DataDirectoryException.Reason.UNSAFE_LAYOUT, root);
            checkEquals(List.of(), listNames(target), relative + ": nothing was created through the junction");
            check(Files.exists(folder, LinkOption.NOFOLLOW_LINKS), relative + ": the junction is left in place");
            checkLockIsFree(root);

            Files.delete(folder); // removes the junction itself, never its target
            DataDirectory.open(root).close(); // repaired: the folder is created again as a plain folder
        }
    }

    private static void lockFileThatIsAFolder(Path tempDir) throws IOException {
        Path root = tempDir.resolve("data");
        DataDirectory.open(root).close();
        Files.delete(root.resolve(DataDirectory.LOCK_NAME));
        Files.createDirectory(root.resolve(DataDirectory.LOCK_NAME));
        expectRefused(DataDirectoryException.Reason.UNSAFE_LAYOUT, root);
        check(Files.isDirectory(root.resolve(DataDirectory.LOCK_NAME)), "the folder named server.lock is left in place");
    }

    // ---------------------------------------------------------------------
    // Locking
    // ---------------------------------------------------------------------

    private static void sameProcessContention(Path tempDir) throws IOException {
        Path root = tempDir.resolve("data");
        DataDirectory first = DataDirectory.open(root);
        try {
            expectRefused(DataDirectoryException.Reason.IN_USE, root);
            check(Files.isDirectory(first.manifestsDir()), "the first holder's folder is still usable");
        } finally {
            first.close();
        }
        DataDirectory.open(root).close(); // free again once the first one closed
    }

    private static void junctionAliasIsLocked(Path tempDir) throws IOException {
        Path root = tempDir.resolve("data");
        Path alias = tempDir.resolve("alias");
        DataDirectory first = DataDirectory.open(root);
        try {
            createDirectoryJunction(alias, root);
            expectRefused(DataDirectoryException.Reason.IN_USE, alias);
        } finally {
            first.close();
        }
        try (DataDirectory throughAlias = DataDirectory.open(alias)) {
            checkEquals(alias.toAbsolutePath().normalize(), throughAlias.root(), "root as given");
        }
    }

    private static void otherProcessHoldsLock(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        try (Child child = Child.start(root)) {
            checkEquals("LOCKED", child.nextLine(), "child output");
            expectRefused(DataDirectoryException.Reason.IN_USE, root);
            child.releaseLock();
            checkEquals("RELEASED", child.nextLine(), "child output after its stdin was closed");
            checkEquals(0, child.waitForExit(), "child exit code");
        }
        try (DataDirectory data = DataDirectory.open(root)) {
            check(!data.newlyInitialized(), "the child initialized the folder, so it is not new here");
        }
    }

    private static void killedProcessReleasesLock(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        try (Child child = Child.start(root)) {
            checkEquals("LOCKED", child.nextLine(), "child output");
            expectRefused(DataDirectoryException.Reason.IN_USE, root);
            child.kill();
        }
        // Windows releases a killed process's file locks asynchronously ("the time it takes ...
        // depends upon available system resources", LockFileEx documentation), so the lock may
        // still look held for a moment after the process has exited. Poll, with a time limit.
        long start = System.nanoTime();
        long deadline = start + TimeUnit.SECONDS.toNanos(CHILD_TIMEOUT_SECONDS);
        int refusals = 0;
        while (true) {
            try {
                DataDirectory.open(root).close();
                break;
            } catch (DataDirectoryException e) {
                if (e.reason() != DataDirectoryException.Reason.IN_USE || System.nanoTime() > deadline) {
                    throw e;
                }
                refusals++;
                Thread.sleep(25);
            }
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println("      (lock free " + millis + " ms after the killed process exited; "
                + refusals + " refused attempt(s) before that)");
    }

    private static void otherProcessIsRefused(Path tempDir) throws Exception {
        Path root = tempDir.resolve("data");
        try (DataDirectory data = DataDirectory.open(root); Child child = Child.start(root)) {
            checkEquals("REFUSED IN_USE", child.nextLine(), "child output");
            checkEquals(3, child.waitForExit(), "child exit code");
            check(Files.isDirectory(data.manifestsDir()), "this process still holds a usable folder");
        }
    }

    private static void failedOpenReleasesLock(Path tempDir) throws IOException {
        // Failure while checking the marker (after the lock was taken).
        Path root = Files.createDirectory(tempDir.resolve("data"));
        Files.writeString(root.resolve(DataDirectory.MARKER_NAME), "wrong");
        expectRefused(DataDirectoryException.Reason.INVALID_MARKER, root);
        checkLockIsFree(root);

        // Failure while creating subfolders (after the marker was accepted).
        Path second = tempDir.resolve("second");
        DataDirectory.open(second).close();
        deleteTree(second.resolve("nodes"));
        Files.writeString(second.resolve("nodes"), "a file named nodes");
        expectRefused(DataDirectoryException.Reason.UNSAFE_LAYOUT, second);
        checkLockIsFree(second);

        // A refused open must not release, or otherwise disturb, the holder's lock.
        Path third = tempDir.resolve("third");
        try (DataDirectory holder = DataDirectory.open(third)) {
            expectRefused(DataDirectoryException.Reason.IN_USE, third);
            expectRefused(DataDirectoryException.Reason.IN_USE, third); // still held after a refusal
            check(Files.isDirectory(holder.tmpDir()), "the holder's folder is still usable");
        }
        checkLockIsFree(third);
    }

    // ---------------------------------------------------------------------
    // API details
    // ---------------------------------------------------------------------

    private static void accessorsAndClose(Path tempDir) throws IOException {
        Path expectedRoot = tempDir.resolve("data").toAbsolutePath().normalize();
        DataDirectory data = DataDirectory.open(tempDir.resolve("x").resolve("..").resolve("data"));
        checkEquals(expectedRoot, data.root(), "root");
        checkEquals(expectedRoot.resolve("manifests"), data.manifestsDir(), "manifestsDir");
        checkEquals(expectedRoot.resolve("tmp"), data.tmpDir(), "tmpDir");
        checkEquals(List.of(expectedRoot.resolve("nodes").resolve("node-0"), expectedRoot.resolve("nodes").resolve("node-1"),
                expectedRoot.resolve("nodes").resolve("node-2")), data.nodeDirs(), "nodeDirs");
        check(!Files.exists(tempDir.resolve("x")), "the path's \"..\" part did not create a folder named x");
        data.close();
        data.close(); // second close does nothing
        checkLockIsFree(expectedRoot);
        expectThrows(NullPointerException.class, "open(null)", () -> DataDirectory.open(null));
        expectThrows(UnsupportedOperationException.class, "nodeDirs is read-only", () -> data.nodeDirs().clear());
    }

    // ---------------------------------------------------------------------
    // Child process that holds the lock (run as: DataDirectoryTest --hold-lock <folder>)
    // ---------------------------------------------------------------------

    /** The child's side: lock, report, wait for the parent to close stdin, release. */
    private static int holdLock(Path root) {
        DataDirectory data;
        try {
            data = DataDirectory.open(root);
        } catch (DataDirectoryException e) {
            System.out.println("REFUSED " + e.reason());
            System.out.flush();
            return 3;
        } catch (IOException e) {
            System.out.println("ERROR " + e);
            System.out.flush();
            return 4;
        }
        try {
            System.out.println("LOCKED");
            System.out.flush();
            while (System.in.read() != -1) {
                // wait until the parent closes our standard input
            }
            data.close();
        } catch (IOException e) {
            System.out.println("ERROR " + e);
            System.out.flush();
            return 4;
        }
        System.out.println("RELEASED");
        System.out.flush();
        return 0;
    }

    /** The parent's side: starts the child and reads its output lines with a timeout. */
    private static final class Child implements AutoCloseable {
        private final Process process;
        private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Child(Process process) {
            this.process = process;
            Thread reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        lines.add(line);
                    }
                } catch (IOException e) {
                    lines.add("(could not read child output: " + e + ")");
                }
            });
            reader.setDaemon(true);
            reader.start();
        }

        static Child start(Path root) throws IOException {
            Process process = new ProcessBuilder(javaExecutable(), "-cp", absoluteClassPath(),
                    DataDirectoryTest.class.getName(), "--hold-lock", root.toString())
                    .redirectErrorStream(true)
                    .start();
            return new Child(process);
        }

        String nextLine() throws InterruptedException {
            String line = lines.poll(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (line == null) {
                throw new AssertionError("the child process printed nothing within " + CHILD_TIMEOUT_SECONDS + " seconds");
            }
            return line;
        }

        /** Closing the child's standard input tells it to release the lock and exit. */
        void releaseLock() throws IOException {
            process.getOutputStream().close();
        }

        int waitForExit() throws InterruptedException {
            if (!process.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("the child process did not exit within " + CHILD_TIMEOUT_SECONDS + " seconds");
            }
            return process.exitValue();
        }

        /** A hard kill: the child gets no chance to release anything itself. */
        void kill() throws InterruptedException {
            process.destroyForcibly();
            waitForExit();
        }

        @Override
        public void close() {
            process.destroyForcibly();
        }

        private static String javaExecutable() {
            Path bin = Path.of(System.getProperty("java.home"), "bin");
            Path windowsJava = bin.resolve("java.exe");
            return (Files.isRegularFile(windowsJava) ? windowsJava : bin.resolve("java")).toString();
        }

        private static String absoluteClassPath() {
            List<String> parts = new ArrayList<>();
            for (String part : System.getProperty("java.class.path").split(Pattern.quote(File.pathSeparator))) {
                parts.add(Path.of(part).toAbsolutePath().toString());
            }
            return String.join(File.pathSeparator, parts);
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DataDirectoryException expectRefused(DataDirectoryException.Reason reason, Path dataDir) {
        DataDirectoryException e = expectThrows(DataDirectoryException.class, "open " + dataDir,
                () -> DataDirectory.open(dataDir).close());
        checkEquals(reason, e.reason(), "refusal reason (message: " + e.getMessage() + ")");
        return e;
    }

    private static void checkInitializedLayout(Path root) throws IOException {
        checkEquals(List.of(DataDirectory.MARKER_NAME, "manifests", "nodes", DataDirectory.LOCK_NAME, "tmp"),
                listNames(root), "data directory contents");
        checkEquals(DataDirectory.MARKER_CONTENT, Files.readString(root.resolve(DataDirectory.MARKER_NAME)), "marker content");
        checkEquals(List.of("node-0", "node-1", "node-2"), listNames(root.resolve("nodes")), "node folders");
        for (String relative : List.of("manifests", "tmp", "nodes", "nodes/node-0", "nodes/node-1", "nodes/node-2")) {
            check(DataDirectory.isPlainDirectory(root.resolve(relative)), relative + " is not a plain folder");
            if (!relative.equals("nodes")) {
                checkEquals(List.of(), listNames(root.resolve(relative)), relative + " contents");
            }
        }
    }

    /**
     * Proves that no channel in this process still holds the lock (tryLock would then throw
     * OverlappingFileLockException) and that no other process holds it (tryLock would return null).
     */
    private static void checkLockIsFree(Path root) throws IOException {
        try (FileChannel channel = FileChannel.open(root.resolve(DataDirectory.LOCK_NAME), StandardOpenOption.WRITE)) {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new AssertionError("this process still holds the lock on " + root);
            }
            check(lock != null, "another process holds the lock on " + root);
            lock.release();
        }
    }

    /**
     * Every entry below {@code root} with its kind, size, modification time and content, without
     * following junctions. Two equal snapshots mean nothing was created, deleted or rewritten.
     */
    private static Map<String, String> snapshot(Path root) throws IOException {
        Map<String, String> entries = new TreeMap<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
                String name = root.relativize(dir).toString();
                if (!dir.equals(root) && attributes.isOther()) {
                    entries.put(name, "junction");
                    return FileVisitResult.SKIP_SUBTREE;
                }
                entries.put(name, "folder");
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                String description = "file size=" + attributes.size() + " modified=" + attributes.lastModifiedTime();
                // A held lock on Windows blocks every read of server.lock, so its content is not
                // read; its size and modification time still show any write.
                if (!file.getFileName().toString().equals(DataDirectory.LOCK_NAME)) {
                    description += " content=" + HexFormat.of().formatHex(Files.readAllBytes(file));
                }
                entries.put(root.relativize(file).toString(), description);
                return FileVisitResult.CONTINUE;
            }
        });
        return entries;
    }

    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    /** Creates a directory junction (mklink /J, no admin rights needed), as DistributedFsDemoTest does. */
    private static void createDirectoryJunction(Path link, Path target) throws IOException {
        Process process = new ProcessBuilder("cmd.exe", "/c", "mklink", "/J", link.toString(), target.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while creating a junction", e);
        }
        if (exitCode != 0) {
            throw new IOException("mklink /J failed with exit code " + exitCode + ": " + output.trim());
        }
        BasicFileAttributes attributes = Files.readAttributes(link, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        check(attributes.isOther(), "the planted entry is not a junction");
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    /** A test body that receives a fresh temporary directory. */
    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    /** A piece of code that is expected to throw. */
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("datadirectory-test-");
            body.run(tempDir);
            passed++;
            System.out.println("PASS  " + name);
        } catch (AssertionError e) {
            failed++;
            System.out.println("FAIL  " + name);
            System.out.println("      " + e.getMessage());
        } catch (Throwable e) {
            failed++;
            System.out.println("FAIL  " + name + " (unexpected exception)");
            e.printStackTrace(System.out);
        } finally {
            if (tempDir != null) {
                try {
                    deleteTree(tempDir);
                } catch (IOException e) {
                    System.out.println("      warning: could not delete " + tempDir + ": " + e);
                }
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkEquals(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void checkContains(String text, String expectedPart) {
        if (text == null || !text.contains(expectedPart)) {
            throw new AssertionError("expected text containing \"" + expectedPart + "\" but was \"" + text + "\"");
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, String what, ThrowingAction action) {
        String prefix = what.isEmpty() ? "" : what + ": ";
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError(prefix + "expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError(prefix + "expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }

    /**
     * Deletes a folder tree without following junctions: a junction is removed as a link and
     * its target is deleted only through its own path (the same approach as
     * DistributedFsDemo.deleteWorkspace).
     */
    private static void deleteTree(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) throws IOException {
                if (!dir.equals(directory) && attributes.isOther()) {
                    Files.delete(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) {
                    throw error;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
