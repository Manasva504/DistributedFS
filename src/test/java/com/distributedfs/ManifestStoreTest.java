package com.distributedfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Plain-Java tests for FileManifest, ManifestStore and its pending-upload markers (no JUnit or
 * other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.ManifestStoreTest}
 *
 * <p>Write failures are simulated by overriding ManifestStore's writeContent and moveIntoPlace
 * hooks. The junction test uses a real directory junction (mklink /J, no admin rights needed).
 * The program exits with code 1 if any test fails.
 */
public class ManifestStoreTest {

    private static final String ID = "0123456789abcdef0123456789abcdef";
    private static final String OTHER_ID = "fedcba9876543210fedcba9876543210";
    private static final String SHA = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
    private static final String EMPTY_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final int MIB = 1024 * 1024;

    /** 2,500,000 bytes in 1 MiB chunks: three chunks. */
    private static final FileManifest SAMPLE = new FileManifest(ID, 2_500_000, SHA, MIB, 3);

    private static final String SAMPLE_TEXT = "format=1\n"
            + "fileId=" + ID + "\n"
            + "size=2500000\n"
            + "sha256=" + SHA + "\n"
            + "chunkSize=1048576\n"
            + "chunkCount=3\n";

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.ManifestStoreTest");
            System.exit(2);
            return;
        }

        // FileManifest
        runTest("a manifest's text is exactly the six documented lines", ManifestStoreTest::manifestTextFormat);
        runTest("toText and parse round-trip, including an empty file", ManifestStoreTest::manifestRoundTrip);
        runTest("chunkCount is ceil(size / chunkSize), computed without overflow", ManifestStoreTest::chunkCountRule);
        runTest("the manifest constructor rejects invalid values", ManifestStoreTest::manifestConstructorRejects);
        runTest("chunkId derives F_i and checks the index", ManifestStoreTest::manifestChunkIds);
        runTest("every malformed manifest text is rejected", ManifestStoreTest::malformedManifestsAreRejected);

        // Pending-upload marker
        runTest("a pending marker has its own strict three-line format", ManifestStoreTest::pendingFormat);

        // ManifestStore
        runTest("the store refuses a missing folder, a file, or a junction as its folder",
                ManifestStoreTest::storeNeedsPlainFolder);
        runTest("create writes <id>.manifest with exactly the manifest text and no temp file is left",
                ManifestStoreTest::createWritesExactText);
        runTest("read returns what create wrote, and empty when there is no manifest", ManifestStoreTest::readAfterCreate);
        runTest("create never replaces an existing manifest or other entry", ManifestStoreTest::createNeverReplaces);
        runTest("a failure while writing leaves neither a manifest nor a temp file", ManifestStoreTest::failureWhileWriting);
        runTest("a failure at the rename leaves neither a manifest nor a temp file", ManifestStoreTest::failureAtRename);
        runTest("read reports damaged manifests as invalid, never as missing", ManifestStoreTest::damagedManifestsAreInvalid);
        runTest("invalid file IDs are rejected before the folder is touched", ManifestStoreTest::invalidIdsAreRejected);
        runTest("a folder that disappeared is an error, not an absent manifest", ManifestStoreTest::missingFolderIsAnError);
        runTest("delete removes a manifest, reports a missing one, and refuses a non-file",
                ManifestStoreTest::deleteManifest);
        runTest("pending markers can be created, read, listed and deleted", ManifestStoreTest::pendingMarkers);
        runTest("a damaged pending marker is reported as invalid", ManifestStoreTest::damagedPendingIsInvalid);
        runTest("isIdTaken counts manifests, markers and damaged entries", ManifestStoreTest::idTaken);

        // Stale temp sweep
        runTest("the sweep deletes only eligible stale temp files and warns about everything else",
                ManifestStoreTest::sweepDeletesOnlyEligibleFiles);
        runTest("the sweep never follows or deletes a junction named like a temp file",
                ManifestStoreTest::sweepLeavesJunctionAlone);
        runTest("the sweep fails loudly if the folder cannot be listed", ManifestStoreTest::sweepNeedsTheFolder);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // FileManifest
    // ---------------------------------------------------------------------

    private static void manifestTextFormat(Path tempDir) {
        checkEquals(SAMPLE_TEXT, SAMPLE.toText(), "manifest text");
    }

    private static void manifestRoundTrip(Path tempDir) throws IOException {
        checkEquals(SAMPLE, FileManifest.parse(SAMPLE_TEXT, ID), "parsed sample");
        FileManifest empty = new FileManifest(ID, 0, EMPTY_SHA, MIB, 0);
        checkEquals(empty, FileManifest.parse(empty.toText(), ID), "parsed empty-file manifest");
        // The largest manifest possible: Integer.MAX_VALUE chunks of Integer.MAX_VALUE bytes (a 19-digit size).
        long largestSize = (long) Integer.MAX_VALUE * Integer.MAX_VALUE;
        FileManifest largest = new FileManifest(ID, largestSize, SHA, Integer.MAX_VALUE, Integer.MAX_VALUE);
        checkEquals(largest, FileManifest.parse(largest.toText(), ID), "parsed manifest with the largest values");
        checkContains(largest.toText(), "size=" + largestSize + "\n");
    }

    private static void chunkCountRule(Path tempDir) {
        checkEquals(0, FileManifest.expectedChunkCount(0, 1), "empty file");
        checkEquals(1, FileManifest.expectedChunkCount(1, MIB), "1 byte");
        checkEquals(1, FileManifest.expectedChunkCount(MIB, MIB), "exactly one chunk");
        checkEquals(2, FileManifest.expectedChunkCount(MIB + 1L, MIB), "one byte more than a chunk");
        checkEquals(3, FileManifest.expectedChunkCount(2_500_000, MIB), "2,500,000 bytes");
        checkEquals(Integer.MAX_VALUE, FileManifest.expectedChunkCount(Integer.MAX_VALUE, 1), "the most chunks");
        // More chunks than an int can count are refused rather than wrapped around. The last case,
        // ceil(Long.MAX_VALUE / Integer.MAX_VALUE) = 4,294,967,298, is where the common shortcut
        // (size + chunkSize - 1) / chunkSize would overflow a long and give a negative count.
        expectThrows(IllegalArgumentException.class, "too many chunks",
                () -> FileManifest.expectedChunkCount(Integer.MAX_VALUE + 1L, 1));
        expectThrows(IllegalArgumentException.class, "Long.MAX_VALUE bytes in 1-byte chunks",
                () -> FileManifest.expectedChunkCount(Long.MAX_VALUE, 1));
        expectThrows(IllegalArgumentException.class, "Long.MAX_VALUE bytes in the largest chunks",
                () -> FileManifest.expectedChunkCount(Long.MAX_VALUE, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, "negative size", () -> FileManifest.expectedChunkCount(-1, 1));
        expectThrows(IllegalArgumentException.class, "chunk size 0", () -> FileManifest.expectedChunkCount(1, 0));
    }

    private static void manifestConstructorRejects(Path tempDir) {
        expectThrows(NullPointerException.class, "null file ID", () -> new FileManifest(null, 1, SHA, 1, 1));
        expectThrows(NullPointerException.class, "null sha256", () -> new FileManifest(ID, 1, null, 1, 1));
        expectThrows(IllegalArgumentException.class, "invalid file ID",
                () -> new FileManifest(ID.toUpperCase(Locale.ROOT), 1, SHA, 1, 1));
        expectThrows(IllegalArgumentException.class, "negative size", () -> new FileManifest(ID, -1, SHA, 1, 0));
        expectThrows(IllegalArgumentException.class, "uppercase sha256",
                () -> new FileManifest(ID, 1, SHA.toUpperCase(Locale.ROOT), 1, 1));
        expectThrows(IllegalArgumentException.class, "63-character sha256",
                () -> new FileManifest(ID, 1, SHA.substring(1), 1, 1));
        expectThrows(IllegalArgumentException.class, "non-hex sha256",
                () -> new FileManifest(ID, 1, "g" + SHA.substring(1), 1, 1));
        expectThrows(IllegalArgumentException.class, "chunk size 0", () -> new FileManifest(ID, 1, SHA, 0, 1));
        expectThrows(IllegalArgumentException.class, "chunk count too small", () -> new FileManifest(ID, 2_500_000, SHA, MIB, 2));
        expectThrows(IllegalArgumentException.class, "chunk count too large", () -> new FileManifest(ID, 2_500_000, SHA, MIB, 4));
        expectThrows(IllegalArgumentException.class, "empty file with a chunk", () -> new FileManifest(ID, 0, EMPTY_SHA, MIB, 1));
    }

    private static void manifestChunkIds(Path tempDir) {
        checkEquals(ID + "_0", SAMPLE.chunkId(0), "chunk 0");
        checkEquals(ID + "_2", SAMPLE.chunkId(2), "last chunk");
        expectThrows(IndexOutOfBoundsException.class, "index -1", () -> SAMPLE.chunkId(-1));
        expectThrows(IndexOutOfBoundsException.class, "index == chunkCount", () -> SAMPLE.chunkId(3));
        FileManifest empty = new FileManifest(ID, 0, EMPTY_SHA, MIB, 0);
        expectThrows(IndexOutOfBoundsException.class, "chunk 0 of an empty file", () -> empty.chunkId(0));
    }

    private static void malformedManifestsAreRejected(Path tempDir) throws IOException {
        checkEquals(SAMPLE, FileManifest.parse(SAMPLE_TEXT, ID), "the unmodified sample (control)");
        String[][] variants = {
                {"empty text", ""},
                {"no final line feed", SAMPLE_TEXT.substring(0, SAMPLE_TEXT.length() - 1)},
                {"CRLF line endings", SAMPLE_TEXT.replace("\n", "\r\n")},
                {"an extra blank line", SAMPLE_TEXT + "\n"},
                {"an extra line", SAMPLE_TEXT + "extra=1\n"},
                {"a missing line", SAMPLE_TEXT.replace("chunkCount=3\n", "")},
                {"a duplicated key", SAMPLE_TEXT.replace("size=2500000\n", "fileId=" + ID + "\n")},
                {"keys in another order", SAMPLE_TEXT.replace("size=2500000\nsha256=" + SHA + "\n",
                        "sha256=" + SHA + "\nsize=2500000\n")},
                {"an unknown key", SAMPLE_TEXT.replace("chunkSize=", "chunk_size=")},
                {"a key in another case", SAMPLE_TEXT.replace("fileId=", "fileid=")},
                {"format 2", SAMPLE_TEXT.replace("format=1", "format=2")},
                {"format with a leading zero", SAMPLE_TEXT.replace("format=1", "format=01")},
                {"spaces around '='", SAMPLE_TEXT.replace("size=2500000", "size = 2500000")},
                {"a trailing space", SAMPLE_TEXT.replace("size=2500000", "size=2500000 ")},
                {"a leading space", SAMPLE_TEXT.replace("size=", " size=")},
                {"a tab", SAMPLE_TEXT.replace("size=2500000", "size=\t2500000")},
                {"a NUL character", SAMPLE_TEXT.replace("size=2500000", "size=2500000\0")},
                {"a non-ASCII character", SAMPLE_TEXT.replace("size=2500000", "size=2500000\u00E9")},
                {"a byte order mark", "\uFEFF" + SAMPLE_TEXT},
                {"an empty value", SAMPLE_TEXT.replace("size=2500000", "size=")},
                {"a negative size", SAMPLE_TEXT.replace("size=2500000", "size=-1")},
                {"a plus sign", SAMPLE_TEXT.replace("size=2500000", "size=+2500000")},
                {"a leading zero", SAMPLE_TEXT.replace("size=2500000", "size=02500000")},
                {"a size that overflows a long", SAMPLE_TEXT.replace("size=2500000", "size=99999999999999999999")},
                {"a size with 19 digits that overflows", SAMPLE_TEXT.replace("size=2500000", "size=9999999999999999999")},
                {"a hexadecimal size", SAMPLE_TEXT.replace("size=2500000", "size=0x2625a0")},
                {"an uppercase sha256", SAMPLE_TEXT.replace(SHA, SHA.toUpperCase(Locale.ROOT))},
                {"a short sha256", SAMPLE_TEXT.replace(SHA, SHA.substring(2))},
                {"chunk size 0", SAMPLE_TEXT.replace("chunkSize=1048576", "chunkSize=0")},
                {"a chunk size larger than an int", SAMPLE_TEXT.replace("chunkSize=1048576", "chunkSize=3000000000")},
                {"an inconsistent chunk count", SAMPLE_TEXT.replace("chunkCount=3", "chunkCount=2")},
                {"another valid file ID", SAMPLE_TEXT.replace(ID, OTHER_ID)},
                {"an uppercase file ID", SAMPLE_TEXT.replace(ID, ID.toUpperCase(Locale.ROOT))},
                {"a path in the file ID", SAMPLE_TEXT.replace(ID, "..\\" + ID.substring(3))},
        };
        for (String[] variant : variants) {
            expectThrows(InvalidManifestException.class, variant[0], () -> FileManifest.parse(variant[1], ID));
        }
    }

    // ---------------------------------------------------------------------
    // Pending-upload marker
    // ---------------------------------------------------------------------

    private static void pendingFormat(Path tempDir) throws IOException {
        ManifestStore.PendingUpload pending = new ManifestStore.PendingUpload(ID, 3);
        String text = "format=1\nfileId=" + ID + "\nchunkCount=3\n";
        checkEquals(text, pending.toText(), "marker text");
        checkEquals(pending, ManifestStore.PendingUpload.parse(text, ID), "parsed marker");
        checkEquals(ID + "_2", pending.chunkId(2), "chunk 2 of the upload");
        expectThrows(IndexOutOfBoundsException.class, "chunk 3 of 3", () -> pending.chunkId(3));
        checkEquals(new ManifestStore.PendingUpload(ID, 0),
                ManifestStore.PendingUpload.parse("format=1\nfileId=" + ID + "\nchunkCount=0\n", ID), "zero chunks");

        expectThrows(IllegalArgumentException.class, "negative chunk count", () -> new ManifestStore.PendingUpload(ID, -1));
        expectThrows(IllegalArgumentException.class, "invalid file ID", () -> new ManifestStore.PendingUpload("x", 1));
        String[][] variants = {
                {"no final line feed", text.substring(0, text.length() - 1)},
                {"CRLF", text.replace("\n", "\r\n")},
                {"an extra line", text + "size=1\n"},
                {"a manifest instead of a marker", SAMPLE_TEXT},
                {"format 2", text.replace("format=1", "format=2")},
                {"another file ID", text.replace(ID, OTHER_ID)},
                {"a negative count", text.replace("chunkCount=3", "chunkCount=-3")},
                {"a leading zero", text.replace("chunkCount=3", "chunkCount=03")},
                {"a count larger than an int", text.replace("chunkCount=3", "chunkCount=2147483648")},
        };
        for (String[] variant : variants) {
            expectThrows(InvalidManifestException.class, variant[0], () -> ManifestStore.PendingUpload.parse(variant[1], ID));
        }
    }

    // ---------------------------------------------------------------------
    // ManifestStore
    // ---------------------------------------------------------------------

    private static void storeNeedsPlainFolder(Path tempDir) throws IOException {
        expectThrows(IOException.class, "missing folder", () -> new ManifestStore(tempDir.resolve("missing")));
        Path file = Files.writeString(tempDir.resolve("a-file"), "x");
        expectThrows(IOException.class, "a file", () -> new ManifestStore(file));
        Path target = Files.createDirectory(tempDir.resolve("target"));
        Path junction = tempDir.resolve("junction");
        createDirectoryJunction(junction, target);
        expectThrows(IOException.class, "a junction", () -> new ManifestStore(junction));
        expectThrows(NullPointerException.class, "null", () -> new ManifestStore(null));
    }

    private static void createWritesExactText(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        store.create(SAMPLE);
        checkEquals(List.of(ID + ".manifest"), listNames(store.directory()), "folder contents");
        checkBytes(SAMPLE_TEXT.getBytes(StandardCharsets.US_ASCII), store.directory().resolve(ID + ".manifest"));
    }

    private static void readAfterCreate(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        checkEquals(Optional.empty(), store.read(ID), "read before create");
        store.create(SAMPLE);
        checkEquals(Optional.of(SAMPLE), store.read(ID), "read after create");
        checkEquals(Optional.empty(), store.read(OTHER_ID), "read of another ID");

        FileManifest empty = new FileManifest(OTHER_ID, 0, EMPTY_SHA, MIB, 0);
        store.create(empty);
        checkEquals(Optional.of(empty), store.read(OTHER_ID), "empty-file manifest");
    }

    private static void createNeverReplaces(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        store.create(SAMPLE);
        Path file = store.directory().resolve(ID + ".manifest");
        byte[] before = Files.readAllBytes(file);
        FileManifest different = new FileManifest(ID, 5, SHA, 1, 5);
        expectThrows(FileAlreadyExistsException.class, "second manifest for the same ID", () -> store.create(different));
        checkBytes(before, file);
        checkEquals(Optional.of(SAMPLE), store.read(ID), "the first manifest is still the one stored");

        // Something that is not a manifest under the name is not replaced either.
        Files.createDirectory(store.directory().resolve(OTHER_ID + ".manifest"));
        expectThrows(FileAlreadyExistsException.class, "a folder under the name",
                () -> store.create(new FileManifest(OTHER_ID, 1, SHA, 1, 1)));
        check(Files.isDirectory(store.directory().resolve(OTHER_ID + ".manifest")), "the folder is still there");
        checkEquals(List.of(ID + ".manifest", OTHER_ID + ".manifest"), listNames(store.directory()), "no temp files");
    }

    private static void failureWhileWriting(Path tempDir) throws IOException {
        Path folder = Files.createDirectory(tempDir.resolve("manifests"));
        for (boolean runtimeFailure : new boolean[] {false, true}) {
            ManifestStore store = new ManifestStore(folder) {
                @Override
                void writeContent(Path file, byte[] content) throws IOException {
                    Files.write(file, Arrays.copyOf(content, content.length / 2)); // half the bytes, then fail
                    if (runtimeFailure) {
                        throw new IllegalStateException("simulated unexpected failure");
                    }
                    throw new IOException("simulated disk failure");
                }
            };
            Class<? extends Exception> expected = runtimeFailure ? IllegalStateException.class : IOException.class;
            expectThrows(expected, "create", () -> store.create(SAMPLE));
            checkEquals(List.of(), listNames(folder), "folder contents after the failure");
            checkEquals(Optional.empty(), newStore(tempDir).read(ID), "no manifest");
            expectThrows(expected, "createPending", () -> store.createPending(new ManifestStore.PendingUpload(ID, 3)));
            checkEquals(List.of(), listNames(folder), "folder contents after the marker failure");
        }
    }

    private static void failureAtRename(Path tempDir) throws IOException {
        Path folder = Files.createDirectory(tempDir.resolve("manifests"));
        ManifestStore store = new ManifestStore(folder) {
            @Override
            void moveIntoPlace(Path source, Path target) throws IOException {
                throw new IOException("simulated rename failure");
            }
        };
        IOException e = expectThrows(IOException.class, "create", () -> store.create(SAMPLE));
        checkEquals("simulated rename failure", e.getMessage(), "the original error is reported");
        checkEquals(List.of(), listNames(folder), "folder contents after the failure");
    }

    private static void damagedManifestsAreInvalid(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        Path file = store.directory().resolve(ID + ".manifest");

        String[][] contents = {
                {"garbage", "not a manifest\n"},
                {"an empty file", ""},
                {"the manifest of another file", SAMPLE_TEXT.replace(ID, OTHER_ID)},
                {"CRLF line endings", SAMPLE_TEXT.replace("\n", "\r\n")},
        };
        for (String[] content : contents) {
            Files.writeString(file, content[1], StandardCharsets.US_ASCII);
            expectThrows(InvalidManifestException.class, content[0], () -> store.read(ID));
            Files.delete(file);
        }

        Files.write(file, ("size=\u00E9\n").getBytes(StandardCharsets.UTF_8));
        expectThrows(InvalidManifestException.class, "non-ASCII bytes", () -> store.read(ID));
        Files.delete(file);

        byte[] tooLarge = new byte[FileManifest.MAX_TEXT_BYTES + 1];
        Arrays.fill(tooLarge, (byte) 'a');
        Files.write(file, tooLarge);
        expectThrows(InvalidManifestException.class, "a file larger than 4 KiB", () -> store.read(ID));
        Files.delete(file);

        Files.createDirectory(file);
        expectThrows(InvalidManifestException.class, "a folder under the name", () -> store.read(ID));
        check(Files.isDirectory(file), "the folder is left in place");
    }

    private static void invalidIdsAreRejected(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        // Plant files that the dangerous IDs would reach if they were used as paths.
        Files.writeString(tempDir.resolve("secret.manifest"), "outside");
        List<String> before = listNames(store.directory());

        for (String badId : List.of("../secret", "..\\secret", ID.toUpperCase(Locale.ROOT), "con", "",
                ID + "0", "%2e%2e%2fsecret")) {
            expectThrows(IllegalArgumentException.class, "read " + badId, () -> store.read(badId));
            expectThrows(IllegalArgumentException.class, "delete " + badId, () -> store.delete(badId));
            expectThrows(IllegalArgumentException.class, "readPending " + badId, () -> store.readPending(badId));
            expectThrows(IllegalArgumentException.class, "deletePending " + badId, () -> store.deletePending(badId));
            expectThrows(IllegalArgumentException.class, "isIdTaken " + badId, () -> store.isIdTaken(badId));
        }
        expectThrows(NullPointerException.class, "read null", () -> store.read(null));
        expectThrows(NullPointerException.class, "create null", () -> store.create(null));
        checkEquals(before, listNames(store.directory()), "the manifest folder is unchanged");
        checkEquals("outside", Files.readString(tempDir.resolve("secret.manifest")), "the outside file is unchanged");
    }

    private static void missingFolderIsAnError(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        Files.delete(store.directory());
        expectThrows(IOException.class, "read", () -> store.read(ID));
        expectThrows(IOException.class, "readPending", () -> store.readPending(ID));
        expectThrows(IOException.class, "delete", () -> store.delete(ID));
        expectThrows(IOException.class, "isIdTaken", () -> store.isIdTaken(ID));
        expectThrows(IOException.class, "listPendingFileIds", store::listPendingFileIds);
        expectThrows(IOException.class, "create", () -> store.create(SAMPLE));
        check(!Files.exists(store.directory()), "nothing recreated the folder");
    }

    private static void deleteManifest(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        store.create(SAMPLE);
        check(store.delete(ID), "delete of an existing manifest returns true");
        checkEquals(Optional.empty(), store.read(ID), "read after delete");
        check(!store.delete(ID), "delete of a missing manifest returns false");

        Path folderUnderName = Files.createDirectory(store.directory().resolve(OTHER_ID + ".manifest"));
        expectThrows(IOException.class, "delete of a folder under the name", () -> store.delete(OTHER_ID));
        check(Files.isDirectory(folderUnderName), "the folder is left in place");
    }

    private static void pendingMarkers(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        checkEquals(List.of(), store.listPendingFileIds(), "no markers yet");
        ManifestStore.PendingUpload second = new ManifestStore.PendingUpload(OTHER_ID, 0);
        ManifestStore.PendingUpload first = new ManifestStore.PendingUpload(ID, 3);
        store.createPending(second);
        store.createPending(first);
        expectThrows(FileAlreadyExistsException.class, "a second marker for the same ID",
                () -> store.createPending(new ManifestStore.PendingUpload(ID, 9)));
        checkEquals(Optional.of(first), store.readPending(ID), "first marker");
        checkEquals(Optional.of(second), store.readPending(OTHER_ID), "second marker");

        // Only names of the exact form <fileId>.pending are listed.
        store.create(SAMPLE);
        Files.writeString(store.directory().resolve(ID + "-123.tmp"), "temp");
        Files.writeString(store.directory().resolve(ID.toUpperCase(Locale.ROOT) + "X.pending"), "x");
        Files.writeString(store.directory().resolve("notes.pending"), "x");
        checkEquals(List.of(ID, OTHER_ID), store.listPendingFileIds(), "listed markers, sorted");

        check(store.deletePending(ID), "deletePending of an existing marker");
        check(!store.deletePending(ID), "deletePending of a missing marker");
        checkEquals(Optional.empty(), store.readPending(ID), "readPending after delete");
        checkEquals(List.of(OTHER_ID), store.listPendingFileIds(), "listed markers after delete");
        checkEquals(Optional.of(SAMPLE), store.read(ID), "the manifest is not affected by its marker");
    }

    private static void damagedPendingIsInvalid(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        Path marker = store.directory().resolve(ID + ".pending");
        Files.writeString(marker, "format=1\nfileId=" + ID + "\nchunkCount=three\n", StandardCharsets.US_ASCII);
        expectThrows(InvalidManifestException.class, "a non-numeric count", () -> store.readPending(ID));
        checkEquals(List.of(ID), store.listPendingFileIds(), "a damaged marker is still listed, so recovery sees it");
        check(Files.exists(marker), "the damaged marker is left in place");
    }

    private static void idTaken(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        check(!store.isIdTaken(ID), "nothing stored yet");
        store.create(SAMPLE);
        check(store.isIdTaken(ID), "taken by a manifest");
        store.createPending(new ManifestStore.PendingUpload(OTHER_ID, 1));
        check(store.isIdTaken(OTHER_ID), "taken by a pending marker");
        String third = "a".repeat(32);
        Files.writeString(store.directory().resolve(third + ".manifest"), "damaged");
        check(store.isIdTaken(third), "taken by a damaged manifest");
        check(!store.isIdTaken("b".repeat(32)), "another ID is free");
    }

    // ---------------------------------------------------------------------
    // Stale temp sweep
    // ---------------------------------------------------------------------

    private static void sweepDeletesOnlyEligibleFiles(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        Path folder = store.directory();
        store.create(SAMPLE);
        store.createPending(new ManifestStore.PendingUpload(OTHER_ID, 2));

        // Eligible: exactly <fileId>-<1 to 20 digits>.tmp, regular files.
        Files.writeString(folder.resolve(ID + "-1.tmp"), "partial");
        Files.writeString(folder.resolve(OTHER_ID + "-18446744073709551615.tmp"), "partial");

        // Not eligible: each must be left in place with a warning. (Windows ignores letter case in
        // file names, so the uppercase name gets its own number rather than reusing "-1".)
        List<String> leftAlone = List.of(
                "notes.txt",
                ID.toUpperCase(Locale.ROOT) + "-3.tmp",
                ID + "-.tmp",
                ID + "-123456789012345678901.tmp",
                ID + "-12.tmp.bak",
                ID + "-12.TMP",
                ID.substring(1) + "-12.tmp");
        for (String name : leftAlone) {
            Files.writeString(folder.resolve(name), "keep");
        }
        Path folderNamedLikeTemp = Files.createDirectory(folder.resolve(ID + "-55.tmp"));
        Files.writeString(folderNamedLikeTemp.resolve("inside.txt"), "keep");

        // Outside the manifest folder: must never be touched.
        Path outside = Files.writeString(tempDir.resolve(ID + "-2.tmp"), "outside");

        List<String> warnings = new ArrayList<>();
        int deleted = store.sweepStaleTempFiles(warnings::add);

        checkEquals(2, deleted, "number of deleted temp files");
        List<String> expectedRemaining = new ArrayList<>(leftAlone);
        expectedRemaining.add(ID + "-55.tmp");
        expectedRemaining.add(ID + ".manifest");
        expectedRemaining.add(OTHER_ID + ".pending");
        checkEquals(expectedRemaining.stream().sorted().toList(), listNames(folder), "remaining entries");
        checkEquals("keep", Files.readString(folderNamedLikeTemp.resolve("inside.txt")), "file inside the folder");
        checkEquals("outside", Files.readString(outside), "file outside the manifest folder");
        checkEquals(leftAlone.size() + 1, warnings.size(), "one warning per entry left in place: " + warnings);
        checkEquals(Optional.of(SAMPLE), store.read(ID), "the manifest is intact");
        checkEquals(List.of(OTHER_ID), store.listPendingFileIds(), "the marker is intact");
    }

    private static void sweepLeavesJunctionAlone(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        Path target = Files.createDirectory(tempDir.resolve("target"));
        Files.writeString(target.resolve("precious.txt"), "precious");
        Path junction = store.directory().resolve(ID + "-77.tmp");
        createDirectoryJunction(junction, target);

        List<String> warnings = new ArrayList<>();
        checkEquals(0, store.sweepStaleTempFiles(warnings::add), "number of deleted entries");
        check(Files.exists(junction, LinkOption.NOFOLLOW_LINKS), "the junction is left in place");
        checkEquals("precious", Files.readString(target.resolve("precious.txt")), "the junction's target file");
        checkEquals(1, warnings.size(), "one warning: " + warnings);
        checkContains(warnings.get(0), "not a regular file");
    }

    private static void sweepNeedsTheFolder(Path tempDir) throws IOException {
        ManifestStore store = newStore(tempDir);
        Files.delete(store.directory());
        expectThrows(IOException.class, "sweep of a deleted folder", () -> store.sweepStaleTempFiles(warning -> { }));
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static ManifestStore newStore(Path tempDir) throws IOException {
        Path folder = tempDir.resolve("manifests");
        Files.createDirectories(folder);
        return new ManifestStore(folder);
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
        BasicFileAttributes attributes =
                Files.readAttributes(link, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
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
            tempDir = Files.createTempDirectory("manifeststore-test-");
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
            throw new AssertionError("expected text containing \"" + expectedPart + "\" but was \"" + text + "\"");
        }
    }

    private static void checkBytes(byte[] expected, Path file) throws IOException {
        if (!Arrays.equals(expected, Files.readAllBytes(file))) {
            throw new AssertionError("unexpected content in " + file.getFileName());
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
     * Deletes a test folder without following junctions: a junction is removed as a link and
     * its target is deleted only through its own path (the same approach as
     * DistributedFsDemo.deleteWorkspace).
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
