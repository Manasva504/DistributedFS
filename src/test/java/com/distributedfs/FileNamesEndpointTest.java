package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;
import static com.distributedfs.NodeTestKit.fileId;
import static com.distributedfs.NodeTestKit.host;
import static com.distributedfs.NodeTestKit.listNames;
import static com.distributedfs.NodeTestKit.randomBytes;
import static com.distributedfs.NodeTestKit.raw;
import static com.distributedfs.NodeTestKit.repair;
import static com.distributedfs.NodeTestKit.sha256;
import static com.distributedfs.NodeTestKit.startServer;
import static com.distributedfs.NodeTestKit.upload;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Plain-Java tests of original file names over HTTP (milestone M4b-2): an upload's
 * {@code X-File-Name} is stored (manifest format 3) and every real name downloads under exactly the
 * same name ({@code Content-Disposition}'s {@code filename*}); invalid names are refused with
 * nothing stored; uploads without the header work as before (format 2, {@code <fileId>.bin}); old
 * manifests download as {@code <fileId>.bin}; no name can inject a header or a parameter; and the
 * name appears in {@code GET /files}, the placement and the activity feed, survives startup
 * recovery, and does not stop a repair.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.FileNamesEndpointTest}.
 */
public class FileNamesEndpointTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("filenamesendpoint-test-");

    private static final int CHUNK = 64 * 1024;

    /** report.pdf, then the accented name with a space, from FileNamesTest. */
    private static final String RESUME = FileNamesTest.REAL_NAMES.get(1);

    public static void main(String[] args) {
        RUNNER.run("an upload with X-File-Name is stored as format 3 and answered with its name",
                FileNamesEndpointTest::namedUpload);
        RUNNER.run("every real file name downloads under exactly the same name, with the same bytes",
                FileNamesEndpointTest::everyRealNameRoundTrips);
        RUNNER.run("an upload without X-File-Name is format 2, has no name, and downloads as <fileId>.bin",
                FileNamesEndpointTest::unnamedUpload);
        RUNNER.run("invalid names, and a repeated header, are 400 invalid_file_name and store nothing",
                FileNamesEndpointTest::invalidNamesRefused);
        RUNNER.run("a name of 255 code points is stored; 256 are refused", FileNamesEndpointTest::lengthLimit);
        RUNNER.run("no name can add a header or a parameter to the download's answer", FileNamesEndpointTest::noInjection);
        RUNNER.run("a format 1 manifest (no name, no chunk checksums) downloads as <fileId>.bin",
                FileNamesEndpointTest::formatOneDownload);
        RUNNER.run("GET /files, the placement and the activity feed give the name, or null", FileNamesEndpointTest::nameInJson);
        RUNNER.run("a committed named upload whose marker was left behind is recovered at startup with its name",
                FileNamesEndpointTest::recoveryKeepsName);
        RUNNER.run("a repair checks and copies the chunks of a named file (format 3) like any other",
                FileNamesEndpointTest::repairHandlesFormatThree);
        RUNNER.finish();
    }

    private static void namedUpload(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            byte[] data = randomBytes(2 * CHUNK + 3, 1);
            NodeTestKit.RawResponse response = uploadNamed(server.port(), RESUME, data);
            String id = fileId(response);
            Map<String, Object> json = TestJson.object(response.text());
            checkEquals(RESUME, json.get("name"), "name in the 201 answer");
            checkEquals(List.of("fileId", "size", "sha256", "chunkSize", "chunkCount", "name"), new ArrayList<>(json.keySet()),
                    "fields of the 201 answer");
            FileManifest manifest = server.manifests().read(id).orElseThrow();
            checkEquals(3, manifest.format(), "format");
            checkEquals(RESUME, manifest.name(), "stored name");
            String text = Files.readString(server.dataRoot().resolve("manifests").resolve(id + ".manifest"), StandardCharsets.US_ASCII);
            check(text.contains("\nname=r%C3%A9sum%C3%A9%202026.pdf\n"), "the manifest's name line: " + text);
        } finally {
            server.stop();
        }
    }

    private static void everyRealNameRoundTrips(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int seed = 10;
            for (String name : FileNamesTest.REAL_NAMES) {
                byte[] data = randomBytes(CHUNK + 7, seed++);
                String id = fileId(uploadNamed(server.port(), name, data));
                NodeTestKit.RawResponse download = NodeTestKit.download(server.port(), id);
                checkEquals(200, download.status(), "download of " + FileNamesTest.escape(name));
                check(Arrays.equals(data, download.body()), "the bytes of " + FileNamesTest.escape(name));
                String disposition = download.header("Content-Disposition");
                checkEquals(FileNames.contentDisposition(name, id), disposition, "Content-Disposition");
                checkEquals(name, FileNamesTest.decodeFilenameStar(disposition), "the name the browser saves");
            }
        } finally {
            server.stop();
        }
    }

    private static void unnamedUpload(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            NodeTestKit.RawResponse response = upload(server.port(), randomBytes(CHUNK, 2));
            String id = fileId(response);
            checkEquals(null, TestJson.object(response.text()).get("name"), "name in the 201 answer");
            check(TestJson.object(response.text()).containsKey("name"), "the 201 answer has a name field");
            checkEquals(2, server.manifests().read(id).orElseThrow().format(), "format");
            NodeTestKit.RawResponse download = NodeTestKit.download(server.port(), id);
            checkEquals("attachment; filename=\"" + id + ".bin\"", download.header("Content-Disposition"), "Content-Disposition");
        } finally {
            server.stop();
        }
    }

    private static void invalidNamesRefused(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            byte[] data = randomBytes(CHUNK, 3);
            String rlo = new String(Character.toChars(0x202E));
            List<String> values = new ArrayList<>(List.of("", "%2F", "a%2Fb", "a/b", "%00", "a%00b", ".", "..", "%2E%2E", "a b",
                    "%G1", "%E2%82", "%C0%AF", "%ED%A0%80", FileNamesTest.encodeUriComponent("a" + rlo + "fdp.exe"),
                    "%61".repeat(256)));
            for (String value : values) {
                checkRefused(uploadWithHeaders(port, "\r\nX-File-Name: " + value, data), "X-File-Name: " + value);
            }
            checkRefused(uploadWithHeaders(port, "\r\nX-File-Name: a.pdf\r\nX-File-Name: b.pdf", data), "two headers");
            checkEquals(List.of(), listNames(server.dataRoot().resolve("manifests")), "manifests");
            for (int i = 0; i < 3; i++) {
                checkEquals(List.of(), listNames(server.dataRoot().resolve("nodes").resolve("node-" + i)), "node " + i);
            }
        } finally {
            server.stop();
        }
    }

    private static void lengthLimit(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String smile = new String(Character.toChars(0x1F600));
            String longest = "b".repeat(250) + smile.repeat(5);
            checkEquals(255, longest.codePointCount(0, longest.length()), "code points");
            String id = fileId(uploadNamed(server.port(), longest, randomBytes(CHUNK, 4)));
            checkEquals(longest, server.manifests().read(id).orElseThrow().name(), "stored name");
            checkRefused(uploadNamed(server.port(), longest + "c", randomBytes(CHUNK, 5)), "256 code points");
        } finally {
            server.stop();
        }
    }

    private static void noInjection(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            for (String name : List.of("a\"; filename=\"evil.exe", "x\r\nSet-Cookie: a=b\r\n\r\nbody", "\"; filename*=UTF-8''evil.exe",
                    "a\\\"b;c=d", "\r\n", "%0D%0A")) {
                String id = fileId(uploadNamed(port, name, randomBytes(CHUNK, 6)));
                String head = rawHead(port, "GET /files/" + id + " HTTP/1.1\r\n" + host(port) + "\r\nConnection: close\r\n\r\n");
                List<String> lines = new ArrayList<>(Arrays.asList(head.split("\r\n")));
                List<String> dispositions = new ArrayList<>();
                for (String line : lines) {
                    check(!line.toLowerCase().startsWith("set-cookie"), "an injected header for " + FileNamesTest.escape(name));
                    if (line.toLowerCase().startsWith("content-disposition:")) {
                        dispositions.add(line.substring("content-disposition:".length()).trim());
                    }
                }
                checkEquals(1, dispositions.size(), "Content-Disposition lines for " + FileNamesTest.escape(name));
                String value = dispositions.get(0);
                // The structure on its own, not only what FileNames builds: one quoted filename, one filename*.
                checkEquals(2, value.length() - value.replace("\"", "").length(), "quotes in " + value);
                checkEquals(1, value.split("filename=", -1).length - 1, "filename= in " + value);
                checkEquals(1, value.split("filename\\*=", -1).length - 1, "filename*= in " + value);
                checkEquals(FileNames.contentDisposition(name, id), value, "the value for " + FileNamesTest.escape(name));
                checkEquals(name, FileNamesTest.decodeFilenameStar(value), "the name saved for " + FileNamesTest.escape(name));
            }
        } finally {
            server.stop();
        }
    }

    private static void formatOneDownload(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = "000000000000000000000000000000f1";
            Files.writeString(server.dataRoot().resolve("manifests").resolve(id + ".manifest"),
                    new FileManifest(id, 0, sha256(new byte[0]), CHUNK, 0).toText(), StandardCharsets.US_ASCII);
            NodeTestKit.RawResponse download = NodeTestKit.download(server.port(), id);
            checkEquals(200, download.status(), "status");
            checkEquals("attachment; filename=\"" + id + ".bin\"", download.header("Content-Disposition"), "Content-Disposition");
        } finally {
            server.stop();
        }
    }

    private static void nameInJson(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            int port = server.port();
            String named = fileId(uploadNamed(port, RESUME, randomBytes(CHUNK, 7)));
            String unnamed = fileId(upload(port, randomBytes(CHUNK, 8)));
            Map<String, Object> files = getJson(port, "/files");
            for (Map<String, Object> f : TestJson.objects(files, "files")) {
                checkEquals(f.get("fileId").equals(named) ? RESUME : null, f.get("name"), "name in GET /files of " + f.get("fileId"));
                check(f.containsKey("name"), "a name field in GET /files");
            }
            checkEquals(RESUME, getJson(port, "/files/" + named + "/placement").get("name"), "name in the placement");
            checkEquals(null, getJson(port, "/files/" + unnamed + "/placement").get("name"), "no name in the placement");
            List<Map<String, Object>> events = TestJson.objects(getJson(port, "/cluster/events"), "events");
            checkEquals(null, events.get(0).get("name"), "the unnamed upload's event");
            checkEquals(RESUME, events.get(1).get("name"), "the named upload's event");
            String text = raw(port, "GET /files HTTP/1.1\r\n" + host(port), null).text();
            for (char c : text.toCharArray()) {
                check(c < 0x80, "a character that is not ASCII in GET /files: names must be escaped as \\uXXXX");
            }
        } finally {
            server.stop();
        }
    }

    private static void recoveryKeepsName(Path tempDir) throws Exception {
        String id;
        byte[] data = randomBytes(2 * CHUNK, 9);
        DistributedFsServer first = localServer(tempDir);
        try {
            id = fileId(uploadNamed(first.port(), RESUME, data));
        } finally {
            first.stop();
        }
        // As if the server had been killed right after the commit point, before the marker was deleted.
        Files.writeString(tempDir.resolve("data").resolve("manifests").resolve(id + ".pending"),
                new ManifestStore.PendingUpload(id, 2).toText(), StandardCharsets.US_ASCII);
        DistributedFsServer server = localServer(tempDir);
        try {
            check(!Files.exists(server.dataRoot().resolve("manifests").resolve(id + ".pending")), "the marker was kept");
            NodeTestKit.RawResponse download = NodeTestKit.download(server.port(), id);
            check(Arrays.equals(data, download.body()), "the bytes after recovery");
            checkEquals(RESUME, FileNamesTest.decodeFilenameStar(download.header("Content-Disposition")), "the name after recovery");
        } finally {
            server.stop();
        }
    }

    private static void repairHandlesFormatThree(Path tempDir) throws Exception {
        DistributedFsServer server = localServer(tempDir);
        try {
            String id = fileId(uploadNamed(server.port(), "report.pdf", randomBytes(2 * CHUNK, 10)));
            String chunk = FileIds.chunkId(id, 0);
            for (int i = 0; i < 3; i++) {
                Path copy = server.dataRoot().resolve("nodes").resolve("node-" + i).resolve(chunk);
                if (Files.exists(copy)) {
                    Files.delete(copy);
                    break;
                }
            }
            String report = repair(server.port(), null).text();
            checkEquals(1L, NodeTestKit.jsonNumber(report, "filesChecked"), "filesChecked");
            checkEquals(0L, NodeTestKit.jsonNumber(report, "filesSkipped"), "filesSkipped");
            checkEquals(1L, NodeTestKit.jsonNumber(report, "copiesAdded"), "copiesAdded");
            checkEquals("report.pdf", server.manifests().read(id).orElseThrow().name(), "the name after the repair");
        } finally {
            server.stop();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static DistributedFsServer localServer(Path tempDir) throws IOException {
        return startServer(tempDir.resolve("data"), List.of(), new NodeTestKit.Lines(), new NodeTestKit.Lines(), CHUNK, 60_000);
    }

    /** POST /files with the name as the console sends it (encodeURIComponent). */
    private static NodeTestKit.RawResponse uploadNamed(int port, String name, byte[] data) throws IOException {
        return uploadWithHeaders(port, "\r\nX-File-Name: " + FileNamesTest.encodeUriComponent(name), data);
    }

    private static NodeTestKit.RawResponse uploadWithHeaders(int port, String extra, byte[] data) throws IOException {
        return raw(port, "POST /files HTTP/1.1\r\n" + host(port) + extra + "\r\nContent-Type: application/octet-stream"
                + "\r\nContent-Length: " + data.length, data);
    }

    private static void checkRefused(NodeTestKit.RawResponse response, String what) {
        checkEquals(400, response.status(), what + ": status");
        checkEquals(ApiError.INVALID_FILE_NAME.toJson(), response.text(), what + ": body");
    }

    private static Map<String, Object> getJson(int port, String path) throws IOException {
        NodeTestKit.RawResponse response = raw(port, "GET " + path + " HTTP/1.1\r\n" + host(port), null);
        checkEquals(200, response.status(), path);
        return TestJson.object(response.text());
    }

    /** The response head, exactly as the server wrote it, up to the blank line. */
    private static String rawHead(int port, String request) throws IOException {
        try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
            socket.setSoTimeout(10_000);
            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
            InputStream in = socket.getInputStream();
            StringBuilder head = new StringBuilder();
            int b;
            while ((b = in.read()) != -1) {
                head.append((char) b);
                if (head.length() >= 4 && head.substring(head.length() - 4).equals("\r\n\r\n")) {
                    break;
                }
            }
            return head.toString();
        }
    }
}
