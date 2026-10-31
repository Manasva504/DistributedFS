package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkContains;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.startServer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plain-Java tests for serving the web console (milestone M4b-2): {@code GET /} and the two asset
 * paths, byte for byte, with their exact security headers; GET only and no query; every other path
 * under {@code /console/}, including encoded and path-traversal attempts, is 404 and never another
 * file; the Host check applies; and a server whose class path lacks the console's files still
 * starts, warns, answers {@code GET /} with 503 and serves the API.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.ConsoleEndpointTest}.
 */
public class ConsoleEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("consoleendpoint-test-");

    private static final int CHUNK = 64 * 1024;

    private static final String CSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self';"
            + " img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none';"
            + " require-trusted-types-for 'script'";

    public static void main(String[] args) {
        RUNNER.run("GET / is index.html, byte for byte, as text/html with every security header",
                tempDir -> assetServed(tempDir, "/", "index.html", "text/html; charset=utf-8"));
        RUNNER.run("GET /console/console.css is the style sheet, as text/css with every security header",
                tempDir -> assetServed(tempDir, "/console/console.css", "console.css", "text/css; charset=utf-8"));
        RUNNER.run("GET /console/console.js is the script, as text/javascript with every security header",
                tempDir -> assetServed(tempDir, "/console/console.js", "console.js", "text/javascript; charset=utf-8"));
        RUNNER.run("the Content-Security-Policy is exactly the strict one: no inline script or style, no framing",
                ConsoleEndpointTest::cspIsStrict);
        RUNNER.run("the console's files are GET only: HEAD, POST, PUT, DELETE and OPTIONS are 405 with Allow: GET",
                ConsoleEndpointTest::getOnly);
        RUNNER.run("a query on a console path is 400 invalid_query", ConsoleEndpointTest::noQuery);
        RUNNER.run("every other path under /console/, and every encoded or traversal form, is 404 and serves no file",
                ConsoleEndpointTest::otherPaths404);
        RUNNER.run("a path that tries to climb out of the console folder never returns a repository or data file",
                ConsoleEndpointTest::traversalNeverLeaks);
        RUNNER.run("a wrong Host or an absolute-form target is 403 before the console is served", ConsoleEndpointTest::hostChecked);
        RUNNER.run("a declared request body on GET / is discarded first, and the page is still served",
                ConsoleEndpointTest::bodyDiscarded);
        RUNNER.run("serving the console creates nothing in the data directory", ConsoleEndpointTest::nothingCreated);
        RUNNER.run("the server says where the console is when it starts", ConsoleEndpointTest::startupLine);
        RUNNER.run("ConsoleAssets serves exactly three routes and nothing else", ConsoleEndpointTest::assetsRoutes);
        RUNNER.run("a handler without the console answers / with 404, as before the console",
                ConsoleEndpointTest::handlerWithoutConsole);
        RUNNER.run("without the console's files on the class path, the server starts, warns, answers / with 503, and the API works",
                ConsoleEndpointTest::missingFiles);
        RUNNER.finish();
    }

    // ---------------------------------------------------------------------
    // The three files
    // ---------------------------------------------------------------------

    private static void assetServed(Path tempDir, String path, String resource, String type) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            NodeTestKit.RawResponse response = get(server.port(), path);
            checkEquals(200, response.status(), "status");
            check(Arrays.equals(resource(resource), response.body()), "the body is not the resource " + resource);
            checkEquals(type, response.header("Content-Type"), "Content-Type");
            checkEquals(String.valueOf(response.body().length), response.header("Content-Length"), "Content-Length");
            checkEquals(CSP, response.header("Content-Security-Policy"), "Content-Security-Policy");
            checkEquals("nosniff", response.header("X-Content-Type-Options"), "X-Content-Type-Options");
            checkEquals("DENY", response.header("X-Frame-Options"), "X-Frame-Options");
            checkEquals("no-referrer", response.header("Referrer-Policy"), "Referrer-Policy");
            checkEquals("no-store", response.header("Cache-Control"), "Cache-Control");
            checkEquals(null, response.header("Access-Control-Allow-Origin"), "no CORS header");
        } finally {
            server.stop();
        }
    }

    private static void cspIsStrict(Path tempDir) {
        String csp = FileApiHandler.CONSOLE_CSP;
        checkEquals(CSP, csp, "the policy");
        for (String forbidden : List.of("unsafe-inline", "unsafe-eval", "unsafe-hashes", "data:", "blob:", "*", "http:", "https:")) {
            check(!csp.contains(forbidden), "the policy allows " + forbidden);
        }
        for (String required : List.of("default-src 'none'", "frame-ancestors 'none'", "form-action 'none'", "base-uri 'none'",
                "script-src 'self'", "style-src 'self'", "connect-src 'self'")) {
            checkContains(csp, required);
        }
    }

    private static void getOnly(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String path : List.of("/", "/console/console.css", "/console/console.js")) {
                for (String method : List.of("HEAD", "POST", "PUT", "DELETE", "OPTIONS")) {
                    NodeTestKit.RawResponse response = raw(port, method + " " + path + " HTTP/1.1\r\n" + host(port)
                            + "\r\nContent-Length: 0", null);
                    checkEquals(405, response.status(), method + " " + path);
                    checkEquals("GET", response.header("Allow"), method + " " + path + ": Allow");
                    check(response.header("Content-Security-Policy") == null, method + " " + path + ": an asset was sent");
                }
            }
        } finally {
            server.stop();
        }
    }

    private static void noQuery(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            for (String target : List.of("/?x=1", "/?", "/console/console.js?v=1", "/console/console.css?")) {
                NodeTestKit.RawResponse response = get(server.port(), target);
                checkEquals(400, response.status(), target);
                checkEquals(ApiError.INVALID_QUERY.toJson(), response.text(), target + " body");
            }
        } finally {
            server.stop();
        }
    }

    private static void otherPaths404(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            for (String path : List.of("/console/", "/console", "/console/index.html", "/index.html", "/console/console.js/",
                    "/console/CONSOLE.JS", "/CONSOLE/console.js", "/console//console.js", "/console/./console.js",
                    "/console/../console/console.js", "/console/%2e%2e/README.md", "/console/..%2Fconsole.js",
                    "/console/%2Fconsole.js", "/console%2Fconsole.js", "/console/console.js%00", "/console/console.js%20",
                    "/console/console.js.", "/console/console.js;x", "/console/console.jsx", "/console/console.map",
                    "/%2F", "/console/../README.md", "/console/../../pom.xml", "/console/com/distributedfs/console/index.html")) {
                NodeTestKit.RawResponse response = get(server.port(), path);
                checkEquals(404, response.status(), path);
                checkEquals(ApiError.NOT_FOUND.toJson(), response.text(), path + " body");
            }
        } finally {
            server.stop();
        }
    }

    private static void traversalNeverLeaks(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            // A marker file next to the data directory and one inside it: no path may return either.
            Files.writeString(tempDir.resolve("secret.txt"), "SECRET-OUTSIDE");
            Files.writeString(server.dataRoot().resolve("tmp").resolve("secret.txt"), "SECRET-INSIDE");
            for (String path : List.of("/console/../secret.txt", "/console/..%2F..%2Fsecret.txt", "/console/%2e%2e%2fsecret.txt",
                    "/console/..\\secret.txt", "/console/%5c..%5csecret.txt", "/..%2Fsecret.txt", "/console/....//secret.txt",
                    "/console/../../../../../../Windows/win.ini", "/console/C:/Windows/win.ini", "/console/%c0%ae%c0%ae/secret.txt")) {
                NodeTestKit.RawResponse response = get(server.port(), path);
                check(response.status() != 200, path + " was answered 200");
                String text = response.text();
                check(!text.contains("SECRET") && !text.contains("[fonts]"), path + " returned a file: " + text);
            }
        } finally {
            server.stop();
        }
    }

    private static void hostChecked(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String hostLine : List.of("Host: evil.example", "Host: 127.0.0.1:" + (port == 65535 ? 1 : port + 1),
                    "Host: evil.example:" + port)) {
                NodeTestKit.RawResponse response = raw(port, "GET / HTTP/1.1\r\n" + hostLine, null);
                checkEquals(403, response.status(), hostLine);
                checkEquals(ApiError.HOST_NOT_ALLOWED.toJson(), response.text(), hostLine + " body");
            }
            NodeTestKit.RawResponse absolute = raw(port, "GET http://127.0.0.1:" + port + "/console/console.js HTTP/1.1\r\n"
                    + host(port), null);
            checkEquals(403, absolute.status(), "absolute form");
            NodeTestKit.RawResponse viaLocalhost = raw(port, "GET / HTTP/1.1\r\nHost: localhost:" + port, null);
            checkEquals(200, viaLocalhost.status(), "Host: localhost");
        } finally {
            server.stop();
        }
    }

    private static void bodyDiscarded(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            NodeTestKit.RawResponse response = raw(port, "GET / HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 10",
                    "0123456789".getBytes(StandardCharsets.US_ASCII));
            checkEquals(200, response.status(), "status");
            check(Arrays.equals(resource("index.html"), response.body()), "the page");
        } finally {
            server.stop();
        }
    }

    private static void nothingCreated(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            List<String> tmpBefore = listNames(server.dataRoot().resolve("tmp"));
            List<String> manifestsBefore = listNames(server.dataRoot().resolve("manifests"));
            for (String path : List.of("/", "/console/console.css", "/console/console.js", "/console/nothing")) {
                get(server.port(), path);
            }
            checkEquals(tmpBefore, listNames(server.dataRoot().resolve("tmp")), "tmp");
            checkEquals(manifestsBefore, listNames(server.dataRoot().resolve("manifests")), "manifests");
        } finally {
            server.stop();
        }
    }

    private static void startupLine(Path tempDir) throws Exception {
        NodeTestKit.Lines out = new NodeTestKit.Lines();
        NodeTestKit.Lines err = new NodeTestKit.Lines();
        DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(), out, err, CHUNK, 60_000);
        try {
            checkContains(out.text(), "Web console: http://127.0.0.1:" + server.port() + "/");
            check(!err.text().contains("web console"), "a warning about the console: " + err.text());
        } finally {
            server.stop();
        }
    }

    private static void assetsRoutes(Path tempDir) {
        ConsoleAssets assets = ConsoleAssets.load(new NodeTestKit.Lines().stream());
        check(assets.available(), "the console's files were not loaded from the class path");
        for (String route : List.of("/", "/console/console.css", "/console/console.js")) {
            check(ConsoleAssets.isRoute(route), route + " is not a route");
            check(assets.get(route).isPresent(), route + " has no file");
        }
        for (String route : List.of("", "/index.html", "/console/", "/console/index.html", "console/console.js",
                "/console/console.js ", "/Console/console.js")) {
            check(!ConsoleAssets.isRoute(route), "\"" + route + "\" is a route");
            check(assets.get(route).isEmpty(), "\"" + route + "\" has a file");
        }
        check(!ConsoleAssets.none().available(), "none() has files");
        check(ConsoleAssets.none().get("/").isEmpty(), "none() serves /");
    }

    private static void handlerWithoutConsole(Path tempDir) throws Exception {
        DistributedFsServer server = DistributedFsServer.start(new DistributedFsServer.Config(tempDir.resolve("data"), 0, 4,
                64, 0, 0, new NodeTestKit.Lines().stream(), new NodeTestKit.Lines().stream(), 1L << 20, CHUNK),
                (port, err, files) -> new FileApiHandler(port, err, files));
        try {
            checkEquals(404, get(server.port(), "/").status(), "GET /");
            checkEquals(404, get(server.port(), "/console/console.js").status(), "GET /console/console.js");
            NodeTestKit.RawResponse files = get(server.port(), "/files");
            checkEquals(405, files.status(), "GET /files");
            checkEquals("POST", files.header("Allow"), "Allow");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Without the files
    // ---------------------------------------------------------------------

    private static void missingFiles(Path tempDir) throws Exception {
        List<String> parts = new ArrayList<>();
        for (String part : NodeTestKit.absoluteClassPath().split(Pattern.quote(File.pathSeparator))) {
            if (!Files.exists(Path.of(part).resolve("com").resolve("distributedfs").resolve("console").resolve("index.html"))) {
                parts.add(part);
            }
        }
        Path outFile = tempDir.resolve("server.out");
        Path errFile = tempDir.resolve("server.err");
        Process process = new ProcessBuilder(NodeTestKit.javaExecutable(), "-cp", String.join(File.pathSeparator, parts),
                "com.distributedfs.DistributedFsServer", "--data-dir", tempDir.resolve("data").toString(), "--port", "0")
                .redirectOutput(outFile.toFile()).redirectError(errFile.toFile()).start();
        try {
            Pattern listening = Pattern.compile("listening on http://127\\.0\\.0\\.1:(\\d+)/");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            int port = -1;
            while (port < 0) {
                check(System.nanoTime() < deadline && process.isAlive(), "the server did not start: " + read(outFile) + read(errFile));
                Matcher m = listening.matcher(read(outFile));
                if (m.find()) {
                    port = Integer.parseInt(m.group(1));
                } else {
                    Thread.sleep(50);
                }
            }
            checkContains(read(errFile), "Warning: the web console's files were not found on the class path");
            check(!read(outFile).contains("Web console:"), "the server announced a console it does not have");
            for (String path : List.of("/", "/console/console.css", "/console/console.js")) {
                NodeTestKit.RawResponse response = get(port, path);
                checkEquals(503, response.status(), path);
                checkEquals(ApiError.CONSOLE_UNAVAILABLE.toJson(), response.text(), path + " body");
            }
            checkEquals(404, get(port, "/console/other.js").status(), "an unknown console path");
            checkEquals(200, get(port, "/health").status(), "GET /health");
            checkEquals(200, get(port, "/files").status(), "GET /files");
            checkEquals(200, get(port, "/cluster/status").status(), "GET /cluster/status");
        } finally {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            // Windows releases a killed process's open files a moment after it ends.
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < until && !deleted(outFile, errFile)) {
                Thread.sleep(100);
            }
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    private static NodeTestKit.RawResponse get(int port, String target) throws IOException {
        return raw(port, "GET " + target + " HTTP/1.1\r\n" + host(port), null);
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = ConsoleAssets.class.getResourceAsStream(ConsoleAssets.FOLDER + name)) {
            check(in != null, "resource " + name + " is not on the class path");
            return in.readAllBytes();
        }
    }

    private static boolean deleted(Path... files) {
        boolean all = true;
        for (Path file : files) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                all = false;
            }
        }
        return all;
    }

    private static String read(Path file) throws IOException {
        return Files.exists(file) ? Files.readString(file, StandardCharsets.UTF_8) : "";
    }
}
