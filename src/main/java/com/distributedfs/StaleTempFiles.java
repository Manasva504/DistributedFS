package com.distributedfs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Removes temp files that a killed process left in one folder. Used for the manifest folder
 * (ManifestStore) and for each storage node's folder (StorageNode).
 *
 * <p><b>Safety rules.</b> Only the folder itself is listed; nothing is walked or entered. An
 * entry is deleted only if its name matches the folder's exact temp-name pattern and its
 * attributes, read without following links, say it is a regular file. It is then removed with one
 * delete of that name. Java reports a symbolic link, a Windows junction and every other reparse
 * point (for example a OneDrive placeholder) as not regular when links are not followed, so none
 * of them is ever deleted or followed; each is left in place and reported.
 */
final class StaleTempFiles {

    private StaleTempFiles() {
    }

    /**
     * Deletes the stale temp files directly inside {@code folder} and returns how many were deleted.
     *
     * @param folder      the folder to sweep, as an absolute, normalized path
     * @param description how warnings name the folder, for example "the manifest folder"
     * @param tempName    the exact form of a temp file's name
     * @param isKnown     names that belong in the folder (for example manifests or chunks); they are
     *                    skipped without a warning and never deleted
     * @param warnings    receives one message per entry left in place or not deleted
     * @throws IOException if the folder cannot be listed at all
     */
    static int sweep(Path folder, String description, Pattern tempName, Predicate<String> isKnown,
                     Consumer<String> warnings) throws IOException {
        Objects.requireNonNull(folder, "folder must not be null");
        Objects.requireNonNull(description, "description must not be null");
        Objects.requireNonNull(tempName, "tempName must not be null");
        Objects.requireNonNull(isKnown, "isKnown must not be null");
        Objects.requireNonNull(warnings, "warnings must not be null");
        int deleted = 0;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(folder)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (isKnown.test(name)) {
                    continue;
                }
                if (!tempName.matcher(name).matches() || !folder.equals(entry.getParent())) {
                    warnings.accept("Left an unexpected entry in " + description + " in place: " + entry);
                    continue;
                }
                try {
                    BasicFileAttributes attributes =
                            Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (!attributes.isRegularFile()) {
                        warnings.accept("Left a temp-named entry that is not a regular file in place, without following it: "
                                + entry);
                        continue;
                    }
                    Files.delete(entry);
                    deleted++;
                } catch (IOException e) {
                    warnings.accept("Could not remove the stale temp file " + entry + ": " + e);
                }
            }
        }
        return deleted;
    }
}
