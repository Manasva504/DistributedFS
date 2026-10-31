package com.distributedfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The web console's three files, loaded once from the class path when the server starts and then
 * served from memory: {@code GET /} is {@code index.html}, and {@code /console/console.css} and
 * {@code /console/console.js} are the style sheet and the script.
 *
 * <p><b>Packaging.</b> The files live in {@code src/main/resources/com/distributedfs/console/}.
 * {@code javac} compiles only Java code and copies nothing, so that folder is put on the class path
 * when the server runs: {@code java -cp "out;src\main\resources" ...}. {@code scripts\test.ps1} and
 * the demo scripts do this. If the files are not found, the server still starts and the API works;
 * {@code GET /} then answers 503 {@code console_unavailable}, and the server prints a warning.
 *
 * <p><b>Why no request touches the file system.</b> Each route maps to one of exactly these three
 * resources, compared with the raw request path; their bytes are read here, once. So no form of a
 * request path, encoded or not, can name any other file.
 *
 * <p>Each file must be valid UTF-8 and at most {@link #MAX_BYTES}; a file that is not is treated
 * as missing.
 */
final class ConsoleAssets {

    static final int MAX_BYTES = 256 * 1024;

    /** The resource folder, relative to this class's package. */
    static final String FOLDER = "console/";

    /** One file: its bytes and its Content-Type. */
    record Asset(byte[] bytes, String contentType) {
    }

    /** Route (raw request path) to resource name and content type, in the order they are loaded. */
    private static final Map<String, String[]> ROUTES = new LinkedHashMap<>();

    static {
        ROUTES.put("/", new String[] {"index.html", "text/html; charset=utf-8"});
        ROUTES.put("/console/console.css", new String[] {"console.css", "text/css; charset=utf-8"});
        ROUTES.put("/console/console.js", new String[] {"console.js", "text/javascript; charset=utf-8"});
    }

    private final Map<String, Asset> assets;

    private ConsoleAssets(Map<String, Asset> assets) {
        this.assets = assets;
    }

    /** No console: every route answers 503 (for tests, and when the files are missing). */
    static ConsoleAssets none() {
        return new ConsoleAssets(Map.of());
    }

    /**
     * Loads the three files from the class path. If any is missing, too large or not UTF-8, prints
     * one warning to {@code err} and returns {@link #none()}.
     */
    static ConsoleAssets load(PrintStream err) {
        Map<String, Asset> loaded = new LinkedHashMap<>();
        for (Map.Entry<String, String[]> route : ROUTES.entrySet()) {
            String name = route.getValue()[0];
            Optional<byte[]> bytes = read(FOLDER + name);
            if (bytes.isEmpty()) {
                err.println("Warning: the web console's files were not found on the class path (missing or invalid: "
                        + name + "); add src\\main\\resources to it. GET / answers 503 until then; the API works.");
                return none();
            }
            loaded.put(route.getKey(), new Asset(bytes.get(), route.getValue()[1]));
        }
        return new ConsoleAssets(Map.copyOf(loaded));
    }

    /** True if the files were loaded. */
    boolean available() {
        return !assets.isEmpty();
    }

    /** True if {@code rawPath} is one of the console's routes, whether or not the files were loaded. */
    static boolean isRoute(String rawPath) {
        return ROUTES.containsKey(rawPath);
    }

    /** The file for exactly this raw request path; empty for anything else, or if nothing was loaded. */
    Optional<Asset> get(String rawPath) {
        return Optional.ofNullable(assets.get(rawPath));
    }

    private static Optional<byte[]> read(String resource) {
        try (InputStream in = ConsoleAssets.class.getResourceAsStream(resource)) {
            if (in == null) {
                return Optional.empty();
            }
            byte[] bytes = in.readNBytes(MAX_BYTES + 1);
            if (bytes.length > MAX_BYTES) {
                return Optional.empty();
            }
            StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes));
            return Optional.of(bytes);
        } catch (CharacterCodingException e) {
            return Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
