package com.distributedfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * Plain-Java tests for FileTrees (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.FileTreesTest}
 *
 * <p>Junctions are real directory junctions (mklink /J, no admin rights needed). Every test
 * checks that the files a junction points to are still there afterwards.
 * The program exits with code 1 if any test fails.
 */
public class FileTreesTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.FileTreesTest");
            System.exit(2);
            return;
        }

        runTest("a normal tree is deleted completely", FileTreesTest::normalTreeIsDeleted);
        runTest("an empty folder is deleted", FileTreesTest::emptyFolderIsDeleted);
        runTest("a junction inside the tree is removed, never followed", FileTreesTest::nestedJunctionIsNotFollowed);
        runTest("a root that is a junction is removed as a link only", FileTreesTest::rootJunctionIsNotFollowed);
        runTest("a missing root is NoSuchFileException", FileTreesTest::missingRootFails);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void normalTreeIsDeleted(Path tempDir) throws IOException {
        Path root = Files.createDirectories(tempDir.resolve("root"));
        Files.writeString(root.resolve("a.txt"), "a");
        Path deep = Files.createDirectories(root.resolve("one").resolve("two"));
        Files.writeString(deep.resolve("b.txt"), "b");
        Files.createDirectories(root.resolve("empty"));

        FileTrees.deleteTree(root);

        check(!Files.exists(root, LinkOption.NOFOLLOW_LINKS), "the root is gone");
        check(Files.isDirectory(tempDir), "the parent folder is untouched");
    }

    private static void emptyFolderIsDeleted(Path tempDir) throws IOException {
        Path root = Files.createDirectories(tempDir.resolve("root"));
        FileTrees.deleteTree(root);
        check(!Files.exists(root, LinkOption.NOFOLLOW_LINKS), "the root is gone");
    }

    private static void nestedJunctionIsNotFollowed(Path tempDir) throws IOException {
        Path outside = protectedFolder(tempDir);
        Path root = Files.createDirectories(tempDir.resolve("root"));
        Files.writeString(root.resolve("own.txt"), "own");
        Path sub = Files.createDirectories(root.resolve("sub"));
        createDirectoryJunction(sub.resolve("link"), outside);

        FileTrees.deleteTree(root);

        check(!Files.exists(root, LinkOption.NOFOLLOW_LINKS), "the root is gone");
        checkProtectedFolderIntact(outside);
    }

    private static void rootJunctionIsNotFollowed(Path tempDir) throws IOException {
        Path outside = protectedFolder(tempDir);
        Path root = tempDir.resolve("root-link");
        createDirectoryJunction(root, outside);

        FileTrees.deleteTree(root);

        check(!Files.exists(root, LinkOption.NOFOLLOW_LINKS), "the junction itself is gone");
        checkProtectedFolderIntact(outside);
    }

    private static void missingRootFails(Path tempDir) {
        Path missing = tempDir.resolve("missing");
        expectThrows(NoSuchFileException.class, () -> FileTrees.deleteTree(missing));
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** A folder outside the tree being deleted, with a file and a subfolder that must survive. */
    private static Path protectedFolder(Path tempDir) throws IOException {
        Path outside = Files.createDirectories(tempDir.resolve("outside"));
        Files.writeString(outside.resolve("keep.txt"), "keep");
        Files.createDirectories(outside.resolve("inner"));
        Files.writeString(outside.resolve("inner").resolve("keep2.txt"), "keep2");
        return outside;
    }

    private static void checkProtectedFolderIntact(Path outside) throws IOException {
        checkEquals("keep", Files.readString(outside.resolve("keep.txt")), "the file behind the junction");
        checkEquals("keep2", Files.readString(outside.resolve("inner").resolve("keep2.txt")),
                "the nested file behind the junction");
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
            tempDir = Files.createTempDirectory("filetrees-test-");
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
                    deleteRecursively(tempDir);
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

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingAction action) {
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError("expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError("expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }

    /**
     * Test cleanup that never follows a junction: a junction is deleted as a link (so a failing
     * test cannot make the cleanup delete the protected folder's files through it).
     */
    private static void deleteRecursively(Path directory) throws IOException {
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
