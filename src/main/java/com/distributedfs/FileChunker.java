package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Splits a file into fixed-size chunk files named chunk_0, chunk_1, chunk_2, ...
 *
 * <p>The input file is read as a stream through one reusable buffer, so memory use
 * stays at about one chunk no matter how large the input file is.
 */
public class FileChunker {

    /** Default chunk size: 1 MiB. */
    public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    private static final String CHUNK_NAME_PREFIX = "chunk_";

    private final int chunkSize;

    /** Creates a chunker that uses the default 1 MiB chunk size. */
    public FileChunker() {
        this(DEFAULT_CHUNK_SIZE);
    }

    /** Creates a chunker with a custom chunk size (useful for tests with small files). */
    public FileChunker(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0, but was " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    /**
     * Splits {@code inputFile} into chunk files inside {@code outputDir}.
     * The input file is only read, never modified or deleted.
     *
     * @return the chunk files that were created, in order (chunk_0 first);
     *         empty if the input file is empty
     * @throws IllegalArgumentException if the input file or output directory is invalid
     * @throws FileAlreadyExistsException if a chunk file with the same name already exists
     * @throws IOException if reading or writing fails; chunk files created by this call
     *         are deleted before the exception is thrown
     */
    public List<Path> chunk(Path inputFile, Path outputDir) throws IOException {
        validateInputFile(inputFile);
        validateOutputDir(outputDir);

        // Creates the directory and any missing parent directories.
        // Does nothing if the directory already exists.
        Files.createDirectories(outputDir);

        List<Path> createdChunks = new ArrayList<>();
        byte[] buffer = new byte[chunkSize];

        try (InputStream in = Files.newInputStream(inputFile)) {
            int chunkIndex = 0;
            while (true) {
                // readNBytes keeps reading until the buffer is full or the file ends,
                // so every chunk except the last one is exactly chunkSize bytes.
                int bytesRead = in.readNBytes(buffer, 0, chunkSize);
                if (bytesRead == 0) {
                    break; // end of file
                }

                Path chunkFile = outputDir.resolve(CHUNK_NAME_PREFIX + chunkIndex);

                // CREATE_NEW fails if the file already exists, so we never overwrite anything.
                try (OutputStream out = Files.newOutputStream(chunkFile, StandardOpenOption.CREATE_NEW)) {
                    // The file now exists and belongs to this call, so remember it for cleanup.
                    createdChunks.add(chunkFile);
                    // Write only the bytes actually read: the last chunk may be smaller.
                    out.write(buffer, 0, bytesRead);
                }
                chunkIndex++;
            }
        } catch (IOException e) {
            deleteCreatedChunks(createdChunks, e);
            throw e;
        }

        return createdChunks;
    }

    private static void validateInputFile(Path inputFile) {
        Objects.requireNonNull(inputFile, "inputFile must not be null");
        if (!Files.exists(inputFile)) {
            throw new IllegalArgumentException("Input file does not exist: " + inputFile);
        }
        if (Files.isDirectory(inputFile)) {
            throw new IllegalArgumentException("Input path is a directory, not a file: " + inputFile);
        }
        if (!Files.isRegularFile(inputFile)) {
            throw new IllegalArgumentException("Input path is not a regular file: " + inputFile);
        }
        if (!Files.isReadable(inputFile)) {
            throw new IllegalArgumentException("Input file is not readable: " + inputFile);
        }
    }

    private static void validateOutputDir(Path outputDir) {
        Objects.requireNonNull(outputDir, "outputDir must not be null");
        if (Files.exists(outputDir) && !Files.isDirectory(outputDir)) {
            throw new IllegalArgumentException("Output path exists but is not a directory: " + outputDir);
        }
    }

    /**
     * Best-effort cleanup after a failure. Only deletes chunk files that this call created.
     * If a delete fails, that error is attached to the original exception as "suppressed"
     * so the original cause is never hidden.
     */
    private static void deleteCreatedChunks(List<Path> createdChunks, IOException originalError) {
        for (Path chunkFile : createdChunks) {
            try {
                Files.deleteIfExists(chunkFile);
            } catch (IOException cleanupError) {
                originalError.addSuppressed(cleanupError);
            }
        }
    }

    /**
     * Command-line entry point.
     * Usage: java -cp out com.distributedfs.FileChunker &lt;inputFile&gt; &lt;outputDir&gt;
     */
    public static void main(String[] args) {
        if (args.length != 2 || args[0].isBlank() || args[1].isBlank()) {
            printUsage();
            System.exit(2);
            return;
        }

        try {
            Path inputFile = Path.of(args[0]);
            Path outputDir = Path.of(args[1]);
            List<Path> chunks = new FileChunker().chunk(inputFile, outputDir);
            System.out.println("Created " + chunks.size() + " chunk(s) in " + outputDir.toAbsolutePath());
        } catch (InvalidPathException e) {
            // Must come before IllegalArgumentException, because it is a subclass of it.
            System.err.println("Error: invalid path: " + e.getMessage());
            printUsage();
            System.exit(2);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (FileAlreadyExistsException e) {
            System.err.println("Error: file already exists, refusing to overwrite it: " + e.getFile());
            System.err.println("Use a new or empty output directory.");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: I/O failure while chunking: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
            for (Throwable extra : e.getSuppressed()) {
                System.err.println("  Additional error: " + extra);
            }
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("Usage: java -cp out com.distributedfs.FileChunker <inputFile> <outputDir>");
        System.err.println("Splits <inputFile> into " + DEFAULT_CHUNK_SIZE
                + "-byte chunks named chunk_0, chunk_1, ... inside <outputDir>.");
    }
}
