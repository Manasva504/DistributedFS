package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;

/**
 * Rebuilds a file by joining chunk files back together, in the order they are given.
 *
 * <p>This is the reverse of {@link FileChunker}: passing the list returned by
 * {@code FileChunker.chunk(...)} to {@link #reconstruct(List, Path)} recreates the original file.
 *
 * <p>Important design decisions:
 * <ul>
 *   <li>Bytes are copied through one small reusable buffer, so memory use stays the same
 *       no matter how large the chunks or the final file are.</li>
 *   <li>Contents are treated as raw bytes and never converted to text, so any file
 *       (images, archives, text in any encoding) is reproduced exactly.</li>
 *   <li>The destination is created with {@code CREATE_NEW}, so an existing file is never
 *       overwritten. This also protects the chunks themselves: every chunk must already
 *       exist, so a destination that points at one of them is always rejected.</li>
 *   <li>If copying fails, the partially written destination is deleted, but only if this
 *       call created it. Missing parent directories that this call created are <em>not</em>
 *       removed, so empty directories may remain after a failure.</li>
 * </ul>
 */
public class FileReconstructor {

    /** Size of the reusable copy buffer: 64 KiB. Any size works; this just limits memory use. */
    private static final int BUFFER_SIZE = 64 * 1024;

    /**
     * Writes the contents of {@code chunkFiles}, one after another in list order,
     * into a new file at {@code destination}. The chunk files are only read, never changed.
     *
     * <p>An empty list produces an empty destination file. Missing parent directories of
     * {@code destination} are created.
     *
     * @param chunkFiles  chunk files in the order their bytes should appear in the result
     * @param destination path of the file to create; must not already exist
     * @return the total number of bytes written to {@code destination}
     * @throws NullPointerException if the list, any element of it, or the destination is null
     * @throws IllegalArgumentException if a chunk is missing, a directory, not a regular file,
     *         or not readable, or if {@code destination} is an existing directory
     *         (all checked before anything is created)
     * @throws FileAlreadyExistsException if {@code destination} already exists
     * @throws IOException if reading or writing fails; a partially written destination
     *         created by this call is deleted before the exception is thrown
     */
    public long reconstruct(List<Path> chunkFiles, Path destination) throws IOException {
        // Validate everything first, so bad input never leaves a destination file behind.
        validateChunkFiles(chunkFiles);
        Objects.requireNonNull(destination, "destination must not be null");
        if (Files.isDirectory(destination)) {
            // Checked explicitly because the error CREATE_NEW gives for a directory differs
            // between operating systems (on Windows it is a confusing AccessDeniedException).
            throw new IllegalArgumentException("Destination is an existing directory: " + destination);
        }

        // Create missing parent directories. toAbsolutePath() makes sure a plain file name
        // like "out.bin" still has a parent (the current directory).
        Path parentDir = destination.toAbsolutePath().getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }

        byte[] buffer = new byte[BUFFER_SIZE];
        long totalBytesWritten = 0;
        // Becomes true only once CREATE_NEW has succeeded, i.e. once the destination is
        // definitely a new file made by this call. Cleanup checks it, so we can never delete
        // a file that existed before (for example, one of the chunks).
        boolean destinationCreated = false;

        try (OutputStream out = Files.newOutputStream(destination, StandardOpenOption.CREATE_NEW)) {
            destinationCreated = true;

            for (Path chunkFile : chunkFiles) {
                try (InputStream in = openChunk(chunkFile)) {
                    while (true) {
                        // read() may return fewer bytes than the buffer holds; we write exactly
                        // what we got, so short reads are handled correctly.
                        int bytesRead = in.read(buffer);
                        if (bytesRead == -1) {
                            break; // end of this chunk
                        }
                        out.write(buffer, 0, bytesRead);
                        totalBytesWritten += bytesRead;
                    }
                }
            }
        } catch (IOException e) {
            // By the time we get here, try-with-resources has already closed the output
            // stream, which Windows requires before a file can be deleted.
            if (destinationCreated) {
                deletePartialDestination(destination, e);
            }
            throw e;
        }

        return totalBytesWritten;
    }

    /**
     * Opens one chunk file for reading.
     *
     * <p>Why this method exists: it is a small testability hook. Making a real disk fail
     * halfway through a test is not practical, so {@code FileReconstructorTest} (which is in
     * the same package) overrides this method to return a stream that throws an
     * {@code IOException} partway through a chunk. That lets the tests check the cleanup code.
     * It is package-private rather than public so it is not part of the public API.
     */
    InputStream openChunk(Path chunkFile) throws IOException {
        return Files.newInputStream(chunkFile);
    }

    private static void validateChunkFiles(List<Path> chunkFiles) {
        Objects.requireNonNull(chunkFiles, "chunkFiles must not be null");
        for (int i = 0; i < chunkFiles.size(); i++) {
            Path chunkFile = chunkFiles.get(i);
            if (chunkFile == null) {
                throw new NullPointerException("chunkFiles[" + i + "] must not be null");
            }
            if (!Files.exists(chunkFile)) {
                throw new IllegalArgumentException("Chunk " + i + " does not exist: " + chunkFile);
            }
            if (Files.isDirectory(chunkFile)) {
                throw new IllegalArgumentException("Chunk " + i + " is a directory, not a file: " + chunkFile);
            }
            if (!Files.isRegularFile(chunkFile)) {
                throw new IllegalArgumentException("Chunk " + i + " is not a regular file: " + chunkFile);
            }
            if (!Files.isReadable(chunkFile)) {
                throw new IllegalArgumentException("Chunk " + i + " is not readable: " + chunkFile);
            }
        }
    }

    /**
     * Best-effort removal of a partially written destination. If the delete fails, that error
     * is attached to the original exception as "suppressed" so the original cause is never hidden.
     */
    private static void deletePartialDestination(Path destination, IOException originalError) {
        try {
            Files.deleteIfExists(destination);
        } catch (IOException cleanupError) {
            originalError.addSuppressed(cleanupError);
        }
    }
}
