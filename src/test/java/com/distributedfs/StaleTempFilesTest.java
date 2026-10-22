package com.distributedfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Plain-Java tests for StaleTempFiles (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.StaleTempFilesTest}
 *
 * <p>Junctions are real directory junctions (mklink /J, no admin rights needed); every test that
 * plants one checks that the junction's target is untouched. The program exits with code 1 if any
 * test fails.
 */
public class StaleTempFilesTest {

    /** The pattern these tests sweep with: letters, a dash, 1 to 20 digits, ".tmp". */
    private static final Pattern TEMP = Pattern.compile("[a-z]+-[0-9]{1,20}\\.tmp");

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.StaleTempFilesTest");
            System.exit(2);
            return;
        }

        runTest("only exact temp names are deleted (1 and 20 digits yes, 21 no); known names are silent",
                StaleTempFilesTest::onlyExactNamesAreDeleted);
        runTest("only the folder itself is swept; nothing below it is touched", StaleTempFilesTest::onlyOneLevel);
        runTest("a folder or junction with a temp name is left in place, and the junction's target is untouched",
                StaleTempFilesTest::nonRegularEntriesAreLeft);
        runTest("a folder that cannot be listed is an IOException", StaleTempFilesTest::missingFolderFails);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void onlyExactNamesAreDeleted(Path tempDir) throws IOException {
        Path folder = Files.createDirectory(tempDir.resolve("folder"));
        Files.writeString(folder.resolve("a-1.tmp"), "x");
        Files.writeString(folder.resolve("a-12345678901234567890.tmp"), "x");
        List<String> leftAlone = List.of("a-123456789012345678901.tmp", "a-.tmp", "-1.tmp", "a-1.tmp.bak", "A-2.TMP",
                "other.txt");
        for (String name : leftAlone) {
            Files.writeString(folder.resolve(name), "keep");
        }
        Files.writeString(folder.resolve("known"), "known");

        List<String> warnings = new ArrayList<>();
        int deleted = StaleTempFiles.sweep(folder, "the test folder", TEMP, "known"::equals, warnings::add);

        checkEquals(2, deleted, "deleted");
        List<String> expected = new ArrayList<>(leftAlone);
        expected.add("known");
        checkEquals(expected.stream().sorted().toList(), listNames(folder), "remaining");
        checkEquals(leftAlone.size(), warnings.size(), "one warning per unexpected entry, none for the known one: "
                + warnings);
        checkContains(warnings.get(0), "the test folder");
    }

    private static void onlyOneLevel(Path tempDir) throws IOException {
        Path folder = Files.createDirectory(tempDir.resolve("folder"));
        Path sub = Files.createDirectory(folder.resolve("sub"));
        Files.writeString(sub.resolve("a-2.tmp"), "inside a subfolder");

        List<String> warnings = new ArrayList<>();
        checkEquals(0, StaleTempFiles.sweep(folder, "the test folder", TEMP, name -> false, warnings::add), "deleted");
        check(Files.exists(sub.resolve("a-2.tmp")), "the temp-named file in the subfolder is untouched");
        checkEquals(1, warnings.size(), "the subfolder is reported: " + warnings);
    }

    private static void nonRegularEntriesAreLeft(Path tempDir) throws IOException {
        Path folder = Files.createDirectory(tempDir.resolve("folder"));
        Path namedFolder = Files.createDirectory(folder.resolve("a-3.tmp"));
        Files.writeString(namedFolder.resolve("inside"), "keep");
        Path target = Files.createDirectory(tempDir.resolve("target"));
        Files.writeString(target.resolve("precious"), "precious");
        Path junction = folder.resolve("a-4.tmp");
        createDirectoryJunction(junction, target);

        List<String> warnings = new ArrayList<>();
        checkEquals(0, StaleTempFiles.sweep(folder, "the test folder", TEMP, name -> false, warnings::add), "deleted");

        checkEquals("keep", Files.readString(namedFolder.resolve("inside")), "the folder's file");
        check(Files.exists(junction, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
        checkEquals("precious", Files.readString(target.resolve("precious")), "the junction's target");
        checkEquals(2, warnings.size(), "two warnings: " + warnings);
        for (String warning : warnings) {
            checkContains(warning, "not a regular file");
        }
    }

    private static void missingFolderFails(Path tempDir) {
        expectThrows(IOException.class, () -> StaleTempFiles.sweep(tempDir.resolve("missing"), "the test folder",
                TEMP, name -> false, warning -> { }));
    }

    // ---------------------------------------------------------------------
    // Helpers and tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    private static List<String> listNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    /** Creates a directory junction (mklink /J, no admin rights needed), as FileTreesTest does. */
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

    private interface TestBody {
        void run(Path tempDir) throws Exception;
    }

    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("staletempfiles-test-");
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

    private static void checkContains(String text, String expectedPart) {
        if (text == null || !text.contains(expectedPart)) {
            throw new AssertionError("expected to find <" + expectedPart + "> in <" + text + ">");
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

    /** Test cleanup that never follows a junction: a junction is deleted as a link. */
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
