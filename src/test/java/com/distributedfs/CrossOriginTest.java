package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.download;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.sha256;
import static com.distributedfs.NodeTestKit.startNode;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import com.sun.net.httpserver.Headers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Plain-Java tests for the cross-site check (milestone M4b-1): a page on another website must not
 * be able to make the browser change anything on the server or on a storage node. The server
 * refuses every request other than GET whose {@code Origin} is not its own, or whose
 * {@code Sec-Fetch-Site} is not {@code same-origin} or {@code none}; a storage node refuses every
 * request with any {@code Origin}, or with a {@code Sec-Fetch-Site} other than {@code none}. Each
 * refusal is shown to change nothing; requests without those headers (curl, PowerShell, the
 * server's own node client) work as before.
 * Run: {@code java -cp "out;out-test" com.distributedfs.CrossOriginTest}.
 */
public class CrossOriginTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("crossorigin-test-");

    private static final int CHUNK = 64 * 1024;

    public static void main(String[] args) {
        RUNNER.run("the origin rule: no Origin, or exactly http://<the request's Host>; Sec-Fetch-Site absent, same-origin or none",
                CrossOriginTest::serverRuleUnit);
        RUNNER.run("the node rule: no Origin at all, and Sec-Fetch-Site absent or none", CrossOriginTest::nodeRuleUnit);
        RUNNER.run("an upload without Origin or Sec-Fetch-Site is stored as before", CrossOriginTest::uploadWithoutOrigin);
        RUNNER.run("an upload from the server's own origin is stored, through 127.0.0.1 and through localhost",
                CrossOriginTest::uploadSameOrigin);
        RUNNER.run("an upload with a foreign, null, other-port, https, prefixed or mismatched Origin is 403 and stores nothing",
                CrossOriginTest::uploadForeignOrigins);
        RUNNER.run("two Origin headers are refused, even when both are the server's own", CrossOriginTest::twoOrigins);
        RUNNER.run("an upload with Sec-Fetch-Site cross-site, same-site or anything else is refused; same-origin and none pass",
                CrossOriginTest::uploadFetchSite);
        RUNNER.run("a refused upload's body is discarded, it gets its 403, and no workspace is left",
                CrossOriginTest::refusedUploadBody);
        RUNNER.run("a repair with a foreign Origin is refused and repairs nothing; from the own origin it repairs",
                CrossOriginTest::repairForeignOrigin);
        RUNNER.run("dry runs and repairs with dryRun=false are refused the same way", CrossOriginTest::repairQueries);
        RUNNER.run("a repair with Sec-Fetch-Site cross-site is refused", CrossOriginTest::repairFetchSite);
        RUNNER.run("GET requests are answered whatever their Origin: they change nothing", CrossOriginTest::getsIgnoreOrigin);
        RUNNER.run("the Host check comes first: a wrong Host with a foreign Origin is host_not_allowed",
                CrossOriginTest::hostCheckFirst);
        RUNNER.run("every method other than GET is checked before routing, even on unknown paths",
                CrossOriginTest::otherMethodsChecked);
        RUNNER.run("with remote nodes, a refused upload sends nothing to the nodes", CrossOriginTest::remoteNothingSent);
        RUNNER.run("a node refuses a request with any Origin on every route, even its own origin, and stores nothing",
                CrossOriginTest::nodeRefusesOrigin);
        RUNNER.run("a node refuses Sec-Fetch-Site other than none, and accepts none", CrossOriginTest::nodeFetchSite);
        RUNNER.run("a node still serves requests without Origin, and logs a refused request as (cross-origin refused)",
                CrossOriginTest::nodeWithoutOriginAndLog);
        RUNNER.finish();
    }

    // ---------------------------------------------------------------------
    // The two rules on their own
    // ---------------------------------------------------------------------

    private static void serverRuleUnit(Path tempDir) {
        String hostValue = "127.0.0.1:8080";
        checkEquals(true, FileApiHandler.isSameOriginOrNoBrowser(headers(), hostValue), "no headers");
        checkEquals(true, FileApiHandler.isSameOriginOrNoBrowser(headers("Origin", "http://127.0.0.1:8080"), hostValue), "own origin");
        checkEquals(true, FileApiHandler.isSameOriginOrNoBrowser(headers("Origin", "http://localhost:8080"), "localhost:8080"),
                "own origin through localhost");
        for (String origin : List.of("http://evil.example", "null", "", "http://127.0.0.1:8081", "https://127.0.0.1:8080",
                "http://127.0.0.1:8080/", "http://127.0.0.1:80801", "http://127.0.0.1:8080.evil.example",
                "http://localhost:8080", "HTTP://127.0.0.1:8080", "http://127.0.0.1")) {
            checkEquals(false, FileApiHandler.isSameOriginOrNoBrowser(headers("Origin", origin), hostValue), "Origin \"" + origin + "\"");
        }
        checkEquals(false, FileApiHandler.isSameOriginOrNoBrowser(headers("Origin", "http://127.0.0.1:8080"), null), "no Host");
        Headers two = headers("Origin", "http://127.0.0.1:8080");
        two.add("Origin", "http://127.0.0.1:8080");
        checkEquals(false, FileApiHandler.isSameOriginOrNoBrowser(two, hostValue), "two Origin headers");
        for (String site : List.of("same-origin", "none")) {
            checkEquals(true, FileApiHandler.isSameOriginOrNoBrowser(headers("Sec-Fetch-Site", site), hostValue), site);
        }
        for (String site : List.of("cross-site", "same-site", "Same-Origin", "", "same-origin, none")) {
            checkEquals(false, FileApiHandler.isSameOriginOrNoBrowser(headers("Sec-Fetch-Site", site), hostValue), "\"" + site + "\"");
        }
        Headers twoSites = headers("Sec-Fetch-Site", "same-origin");
        twoSites.add("Sec-Fetch-Site", "same-origin");
        checkEquals(false, FileApiHandler.isSameOriginOrNoBrowser(twoSites, hostValue), "two Sec-Fetch-Site headers");
        Headers both = headers("Origin", "http://127.0.0.1:8080");
        both.add("Sec-Fetch-Site", "cross-site");
        checkEquals(false, FileApiHandler.isSameOriginOrNoBrowser(both, hostValue), "own Origin but cross-site");
    }

    private static void nodeRuleUnit(Path tempDir) {
        checkEquals(true, NodeApiHandler.isNotFromBrowserPage(headers()), "no headers");
        checkEquals(true, NodeApiHandler.isNotFromBrowserPage(headers("Sec-Fetch-Site", "none")), "Sec-Fetch-Site none");
        for (String origin : List.of("http://evil.example", "null", "", "http://127.0.0.1:9101")) {
            checkEquals(false, NodeApiHandler.isNotFromBrowserPage(headers("Origin", origin)), "Origin \"" + origin + "\"");
        }
        for (String site : List.of("same-origin", "same-site", "cross-site", "", "None")) {
            checkEquals(false, NodeApiHandler.isNotFromBrowserPage(headers("Sec-Fetch-Site", site)), "\"" + site + "\"");
        }
        Headers twoNone = headers("Sec-Fetch-Site", "none");
        twoNone.add("Sec-Fetch-Site", "none");
        checkEquals(false, NodeApiHandler.isNotFromBrowserPage(twoNone), "two Sec-Fetch-Site headers");
    }

    // ---------------------------------------------------------------------
    // POST /files
    // ---------------------------------------------------------------------

    private static void uploadWithoutOrigin(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            byte[] data = randomBytes(2 * CHUNK, 1);
            String id = fileId(upload(server.port(), data));
            check(Arrays.equals(data, download(server.port(), id).body()), "the file downloads");
        } finally {
            server.stop();
        }
    }

    private static void uploadSameOrigin(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(CHUNK, 2);
            NodeTestKit.RawResponse viaIp = uploadWith(port, "Host: 127.0.0.1:" + port, "Origin: http://127.0.0.1:" + port, data);
            String id = fileId(viaIp);
            check(Arrays.equals(data, download(port, id).body()), "the file uploaded through 127.0.0.1");
            NodeTestKit.RawResponse viaName = uploadWith(port, "Host: localhost:" + port,
                    "Origin: http://localhost:" + port + "\r\nSec-Fetch-Site: same-origin", data);
            fileId(viaName);
            checkEquals(2, manifests(tempDir).size(), "manifests");
        } finally {
            server.stop();
        }
    }

    private static void uploadForeignOrigins(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(CHUNK, 3);
            List<String> cases = new ArrayList<>(List.of(
                    "Host: 127.0.0.1:" + port + "|Origin: http://evil.example",
                    "Host: 127.0.0.1:" + port + "|Origin: null",
                    "Host: 127.0.0.1:" + port + "|Origin: http://127.0.0.1:" + (port == 65535 ? port - 1 : port + 1),
                    "Host: 127.0.0.1:" + port + "|Origin: https://127.0.0.1:" + port,
                    "Host: 127.0.0.1:" + port + "|Origin: http://127.0.0.1:" + port + "/",
                    "Host: 127.0.0.1:" + port + "|Origin: http://127.0.0.1:" + port + "1",
                    "Host: 127.0.0.1:" + port + "|Origin: http://127.0.0.1:" + port + ".evil.example",
                    "Host: 127.0.0.1:" + port + "|Origin: http://localhost:" + port,
                    "Host: localhost:" + port + "|Origin: http://127.0.0.1:" + port,
                    "Host: 127.0.0.1:" + port + "|Origin: HTTP://127.0.0.1:" + port,
                    "Host: 127.0.0.1:" + port + "|Origin:"));
            for (String c : cases) {
                String[] parts = c.split("\\|");
                NodeTestKit.RawResponse response = uploadWith(port, parts[0], parts[1], data);
                checkRefused(response, c);
            }
            checkEquals(List.of(), manifests(tempDir), "manifests after the refused uploads");
            checkEquals(0, chunkCopies(tempDir), "chunk copies after the refused uploads");
        } finally {
            server.stop();
        }
    }

    private static void twoOrigins(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            NodeTestKit.RawResponse response = uploadWith(port, host(port),
                    "Origin: http://127.0.0.1:" + port + "\r\nOrigin: http://127.0.0.1:" + port, randomBytes(CHUNK, 4));
            checkRefused(response, "two Origin headers");
            checkEquals(List.of(), manifests(tempDir), "manifests");
        } finally {
            server.stop();
        }
    }

    private static void uploadFetchSite(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(CHUNK, 5);
            for (String site : List.of("cross-site", "same-site", "Same-Origin", "whatever")) {
                checkRefused(uploadWith(port, host(port), "Sec-Fetch-Site: " + site, data), site);
            }
            checkRefused(uploadWith(port, host(port), "Sec-Fetch-Site: none\r\nSec-Fetch-Site: none", data), "two headers");
            checkEquals(List.of(), manifests(tempDir), "manifests after the refused uploads");
            for (String site : List.of("same-origin", "none")) {
                fileId(uploadWith(port, host(port), "Sec-Fetch-Site: " + site, data));
            }
            checkEquals(2, manifests(tempDir).size(), "manifests after the accepted uploads");
        } finally {
            server.stop();
        }
    }

    private static void refusedUploadBody(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            checkRefused(uploadWith(port, host(port), "Origin: http://evil.example", randomBytes(40 * 1024, 6)), "with a body");
            NodeTestKit.waitUntil(5_000, "an empty tmp folder", () -> {
                try {
                    return listNames(tempDir.resolve("data").resolve("tmp")).isEmpty();
                } catch (IOException e) {
                    return false;
                }
            });
            checkEquals(List.of(), manifests(tempDir), "manifests");
            // The server still works afterwards.
            fileId(upload(port, randomBytes(CHUNK, 7)));
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // POST /cluster/repair
    // ---------------------------------------------------------------------

    private static void repairForeignOrigin(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String id = fileId(upload(port, randomBytes(CHUNK, 8)));
            Path copy = holderFolders(tempDir, FileIds.chunkId(id, 0)).get(0);
            Files.delete(copy);
            for (String origin : List.of("http://evil.example", "null", "http://localhost:" + port)) {
                checkRefused(repairWith(port, null, "Origin: " + origin), origin);
                check(!Files.exists(copy), "the copy was put back by a refused repair (" + origin + ")");
            }
            NodeTestKit.RawResponse own = repairWith(port, null, "Origin: http://127.0.0.1:" + port);
            checkEquals(200, own.status(), "own-origin repair status");
            checkEquals(1L, NodeTestKit.jsonNumber(own.text(), "copiesAdded"), "copiesAdded");
            checkEquals(2, holderFolders(tempDir, FileIds.chunkId(id, 0)).size(), "copies after the repair");
        } finally {
            server.stop();
        }
    }

    private static void repairQueries(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String query : List.of("dryRun=true", "dryRun=false")) {
                checkRefused(repairWith(port, query, "Origin: http://evil.example"), query);
                checkEquals(200, repairWith(port, query, "Origin: http://127.0.0.1:" + port).status(), query + " from the own origin");
            }
            // A refusal is not a repair: it is answered even while a bad query would be 400.
            checkRefused(repairWith(port, "x=1", "Origin: http://evil.example"), "bad query, foreign Origin");
        } finally {
            server.stop();
        }
    }

    private static void repairFetchSite(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String id = fileId(upload(port, randomBytes(CHUNK, 9)));
            Path copy = holderFolders(tempDir, FileIds.chunkId(id, 0)).get(0);
            Files.delete(copy);
            checkRefused(repairWith(port, null, "Sec-Fetch-Site: cross-site"), "cross-site");
            checkRefused(repairWith(port, null, "Origin: http://127.0.0.1:" + port + "\r\nSec-Fetch-Site: cross-site"),
                    "own Origin, cross-site");
            check(!Files.exists(copy), "a refused repair put the copy back");
            checkEquals(200, repairWith(port, null, "Sec-Fetch-Site: same-origin").status(), "same-origin");
            checkEquals(2, holderFolders(tempDir, FileIds.chunkId(id, 0)).size(), "copies after the accepted repair");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Everything else on the server
    // ---------------------------------------------------------------------

    private static void getsIgnoreOrigin(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(CHUNK, 10);
            String id = fileId(upload(port, data));
            String foreign = "\r\nOrigin: http://evil.example\r\nSec-Fetch-Site: cross-site";
            checkEquals(200, raw(port, "GET /health HTTP/1.1\r\n" + host(port) + foreign, null).status(), "health");
            checkEquals(200, raw(port, "GET /cluster/status HTTP/1.1\r\n" + host(port) + foreign, null).status(), "status");
            NodeTestKit.RawResponse file = raw(port, "GET /files/" + id + " HTTP/1.1\r\n" + host(port) + foreign, null);
            checkEquals(200, file.status(), "download");
            check(Arrays.equals(data, file.body()), "download body");
            checkEquals(404, raw(port, "GET /nowhere HTTP/1.1\r\n" + host(port) + foreign, null).status(), "unknown path");
            // No CORS header is ever sent, so another site's script cannot read these answers.
            checkEquals(null, file.header("Access-Control-Allow-Origin"), "Access-Control-Allow-Origin");
        } finally {
            server.stop();
        }
    }

    private static void hostCheckFirst(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            NodeTestKit.RawResponse response = uploadWith(server.port(), "Host: evil.example",
                    "Origin: http://evil.example", randomBytes(CHUNK, 11));
            checkEquals(403, response.status(), "status");
            checkEquals(ApiError.HOST_NOT_ALLOWED.toJson(), response.text(), "body");
        } finally {
            server.stop();
        }
    }

    private static void otherMethodsChecked(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String target : List.of("PUT /files", "DELETE /files/0123456789abcdef0123456789abcdef", "POST /nowhere",
                    "HEAD /health", "PATCH /cluster/repair", "OPTIONS /files")) {
                NodeTestKit.RawResponse response = raw(port, target + " HTTP/1.1\r\n" + host(port)
                        + "\r\nOrigin: http://evil.example\r\nContent-Length: 0", null);
                checkEquals(403, response.status(), target + " status");
                if (!target.startsWith("HEAD")) {
                    checkEquals(ApiError.CROSS_ORIGIN_REFUSED.toJson(), response.text(), target + " body");
                }
            }
            // Without an Origin, the same requests get their usual answers.
            checkEquals(405, raw(port, "PUT /files HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0", null).status(), "PUT /files");
            checkEquals(404, raw(port, "POST /nowhere HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: 0", null).status(),
                    "POST /nowhere");
        } finally {
            server.stop();
        }
    }

    private static void remoteNothingSent(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle a = startNode(tempDir.resolve("node-a"));
             NodeTestKit.NodeHandle b = startNode(tempDir.resolve("node-b"))) {
            DistributedFsServer server = startServer(tempDir.resolve("data"), List.of(a.port(), b.port()),
                    new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
            try {
                int port = server.port();
                long before = nonHealthLines(a) + nonHealthLines(b);
                checkRefused(uploadWith(port, host(port), "Origin: http://evil.example", randomBytes(2 * CHUNK, 12)), "upload");
                checkRefused(repairWith(port, null, "Origin: http://evil.example"), "repair");
                checkEquals(before, nonHealthLines(a) + nonHealthLines(b), "node request log lines");
                checkEquals(List.of(), listNames(a.chunksDir()), "node a chunks");
                checkEquals(List.of(), listNames(b.chunksDir()), "node b chunks");
                byte[] data = randomBytes(2 * CHUNK, 13);
                String id = fileId(uploadWith(port, host(port), "Origin: http://127.0.0.1:" + port, data));
                check(Arrays.equals(data, download(port, id).body()), "an own-origin upload goes through the nodes");
            } finally {
                server.stop();
            }
        }
    }

    // ---------------------------------------------------------------------
    // Storage nodes
    // ---------------------------------------------------------------------

    private static void nodeRefusesOrigin(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = startNode(tempDir.resolve("node"))) {
            int port = node.port();
            byte[] data = randomBytes(100, 14);
            String store = "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: " + data.length
                    + "\r\nX-Chunk-Sha256: " + sha256(data);
            for (String origin : List.of("http://evil.example", "null", "http://127.0.0.1:" + port)) {
                String o = "\r\nOrigin: " + origin;
                checkNodeRefused(node, raw(port, store + o, data), "store, " + origin);
                for (String request : List.of("GET /health", "GET /chunks/file_0", "GET /chunks/file_0/exists?mode=follow",
                        "DELETE /chunks/file_0", "GET /entries", "POST /sweep", "GET /nowhere")) {
                    checkNodeRefused(node, raw(port, request + " HTTP/1.1\r\n" + host(port) + o + "\r\nContent-Length: 0", null),
                            request + ", " + origin);
                }
            }
            checkEquals(List.of(), listNames(node.chunksDir()), "chunks after the refused stores");
        }
    }

    private static void nodeFetchSite(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = startNode(tempDir.resolve("node"))) {
            int port = node.port();
            byte[] data = randomBytes(100, 15);
            String store = "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: " + data.length
                    + "\r\nX-Chunk-Sha256: " + sha256(data);
            for (String site : List.of("cross-site", "same-site", "same-origin", "None")) {
                checkNodeRefused(node, raw(port, store + "\r\nSec-Fetch-Site: " + site, data), site);
            }
            checkEquals(List.of(), listNames(node.chunksDir()), "chunks after the refused stores");
            checkEquals(201, raw(port, store + "\r\nSec-Fetch-Site: none", data).status(), "Sec-Fetch-Site: none");
            checkEquals(List.of("file_0"), listNames(node.chunksDir()), "chunks after the accepted store");
        }
    }

    private static void nodeWithoutOriginAndLog(Path tempDir) throws Exception {
        try (NodeTestKit.NodeHandle node = startNode(tempDir.resolve("node"))) {
            int port = node.port();
            byte[] data = randomBytes(100, 16);
            checkEquals(201, raw(port, "POST /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nContent-Length: " + data.length
                    + "\r\nX-Chunk-Sha256: " + sha256(data), data).status(), "store");
            NodeTestKit.RawResponse read = raw(port, "GET /chunks/file_0 HTTP/1.1\r\n" + host(port), null);
            checkEquals(200, read.status(), "read");
            check(Arrays.equals(data, read.body()), "read body");
            checkNodeRefused(node, raw(port, "DELETE /chunks/file_0 HTTP/1.1\r\n" + host(port) + "\r\nOrigin: http://evil.example",
                    null), "refused delete");
            checkEquals(List.of("file_0"), listNames(node.chunksDir()), "the refused delete deleted nothing");
            NodeTestKit.waitUntil(5_000, "the refusal's log line", () -> node.out().text().lines()
                    .anyMatch(line -> line.startsWith("DELETE (cross-origin refused) 403 ")));
            check(node.out().text().lines().noneMatch(line -> line.contains("evil")), "the log contains the Origin's value");
            checkEquals(200, raw(port, "DELETE /chunks/file_0 HTTP/1.1\r\n" + host(port), null).status(), "delete");
            checkEquals(List.of(), listNames(node.chunksDir()), "chunks after the delete");
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** Request log lines of a node other than the heartbeat's health checks. */
    private static long nonHealthLines(NodeTestKit.NodeHandle node) {
        return node.out().text().lines().filter(line -> !line.startsWith("GET /health ")).count();
    }

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    private static Headers headers(String... namesAndValues) {
        Headers headers = new Headers();
        for (int i = 0; i < namesAndValues.length; i += 2) {
            headers.add(namesAndValues[i], namesAndValues[i + 1]);
        }
        return headers;
    }

    /** POST /files with the given Host line and extra header lines. */
    private static NodeTestKit.RawResponse uploadWith(int port, String hostLine, String extra, byte[] data) throws IOException {
        return raw(port, "POST /files HTTP/1.1\r\n" + hostLine + "\r\n" + extra
                + "\r\nContent-Type: application/octet-stream\r\nContent-Length: " + data.length, data);
    }

    private static NodeTestKit.RawResponse repairWith(int port, String query, String extra) throws IOException {
        return raw(port, "POST /cluster/repair" + (query == null ? "" : "?" + query) + " HTTP/1.1\r\n" + host(port)
                + "\r\n" + extra + "\r\nContent-Length: 0", null);
    }

    private static void checkRefused(NodeTestKit.RawResponse response, String what) {
        checkEquals(403, response.status(), what + ": status");
        checkEquals(ApiError.CROSS_ORIGIN_REFUSED.toJson(), response.text(), what + ": body");
        checkEquals("nosniff", response.header("X-Content-Type-Options"), what + ": nosniff");
    }

    private static void checkNodeRefused(NodeTestKit.NodeHandle node, NodeTestKit.RawResponse response, String what) {
        checkEquals(403, response.status(), what + ": status");
        checkEquals("error=cross_origin_refused\n", response.text(), what + ": body");
        checkEquals(node.nodeId(), response.header(NodeProtocol.NODE_ID_HEADER), what + ": node ID");
    }

    private static List<String> manifests(Path tempDir) throws IOException {
        return listNames(tempDir.resolve("data").resolve("manifests"));
    }

    private static int chunkCopies(Path tempDir) throws IOException {
        int copies = 0;
        for (int i = 0; i < 3; i++) {
            copies += listNames(tempDir.resolve("data").resolve("nodes").resolve("node-" + i)).size();
        }
        return copies;
    }

    private static List<Path> holderFolders(Path tempDir, String chunkId) {
        List<Path> copies = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path copy = tempDir.resolve("data").resolve("nodes").resolve("node-" + i).resolve(chunkId);
            if (Files.exists(copy)) {
                copies.add(copy);
            }
        }
        return copies;
    }
}
