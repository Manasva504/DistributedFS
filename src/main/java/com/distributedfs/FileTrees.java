package com.distributedfs;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/** Deleting a folder tree without ever following a link out of it. */
final class FileTrees {

    private FileTrees() {
    }

    /**
     * Deletes {@code root} and everything below it, deepest entries first, without following links.
     *
     * <p>Files.walkFileTree does not follow symbolic links: a link arrives at visitFile, and
     * deleting it removes only the link. A Windows junction is different: Java reports it as a
     * directory (and as "other", because it is a reparse point), so a plain walk would go inside
     * it and delete files that live outside the tree. Such an entry is therefore deleted as a link
     * and everything behind it is skipped. The same applies to {@code root} itself: if it is a
     * link or junction, only the link is removed.
     *
     * @throws java.nio.file.NoSuchFileException if {@code root} does not exist
     * @throws IOException if an entry cannot be read or deleted; the walk stops there
     */
    static void deleteTree(Path root) throws IOException {
        BasicFileAttributes rootAttributes =
                Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (rootAttributes.isSymbolicLink() || rootAttributes.isOther()) {
            Files.delete(root); // removes the link itself, never its target
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) throws IOException {
                if (!dir.equals(root) && attributes.isOther()) {
                    Files.delete(dir); // removes the junction itself, never its target
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
