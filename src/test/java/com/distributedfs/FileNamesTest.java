package com.distributedfs;

import static com.distributedfs.NodeTestKit.check;
import static com.distributedfs.NodeTestKit.checkEquals;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Plain-Java tests for original file names (milestone M4b-2): {@link FileNames} on its own (which
 * names are accepted, the {@code X-File-Name} header's strict decoding, the canonical form in a
 * manifest, and the download's {@code Content-Disposition}) and {@link FileManifest} format 3,
 * with formats 1 and 2 still read without a name.
 * Run: {@code java -cp "out;out-test;src\main\resources" com.distributedfs.FileNamesTest}.
 */
public class FileNamesTest {

    private static final NodeTestKit.Runner RUNNER = new NodeTestKit.Runner("filenames-test-");

    private static final String ID = "0123456789abcdef0123456789abcdef";
    private static final String SHA = "a".repeat(64);

    /** Names a real file can have, from Windows, macOS or Linux: every one must come back exactly. */
    static final List<String> REAL_NAMES = List.of(
            "report.pdf",
            "r\u00e9sum\u00e9 2026.pdf",                       // precomposed (NFC), as Windows writes it
            "re\u0301sume\u0301 2026.pdf",                     // decomposed (NFD), as macOS writes it
            "\u65e5\u672c\u8a9e \u30d5\u30a1\u30a4\u30eb.txt",  // Japanese
            "\u0645\u0644\u0641 \u0627\u0644\u062a\u0642\u0631\u064a\u0631.pdf", // Arabic, right to left
            "\ud83d\udcc4 notes.md",                           // an emoji (two UTF-16 units)
            "a'b (1) [final].pdf",
            "x;y,z#&=+$@!~%^`{}.txt",
            "  spaces around  ",
            ".hidden",
            "no extension",
            "a\\b.txt",                                        // a backslash: allowed on macOS and Linux
            "line\nbreak\ttab\r.txt",                          // control characters: allowed on Linux
            "CON",
            "a".repeat(254) + "\ud83d\ude00");                 // 255 code points, 256 UTF-16 units

    public static void main(String[] args) {
        RUNNER.run("every real file name is valid and comes back exactly from the header, without normalization",
                FileNamesTest::realNamesRoundTrip);
        RUNNER.run("empty, ., .., a slash, NUL, bidi overrides, unpaired surrogates and 256 code points are refused",
                FileNamesTest::invalidNames);
        RUNNER.run("the header is strict: only encodeURIComponent's characters and %XX, valid UTF-8, at most 3060 characters",
                FileNamesTest::strictHeader);
        RUNNER.run("the header accepts lowercase hex, as some encoders write it", FileNamesTest::lowercaseHexInHeader);
        RUNNER.run("the manifest form is canonical percent-encoded UTF-8, printable ASCII", FileNamesTest::manifestForm);
        RUNNER.run("a manifest name that is not exactly canonical, or not a valid name, is refused",
                FileNamesTest::manifestFormStrict);
        RUNNER.run("Content-Disposition without a name is <fileId>.bin", FileNamesTest::dispositionWithoutName);
        RUNNER.run("Content-Disposition has an ASCII fallback and the exact name as RFC 8187 filename*",
                FileNamesTest::dispositionWithName);
        RUNNER.run("Content-Disposition is one line of printable ASCII and one of each parameter, whatever the name",
                FileNamesTest::dispositionCannotBeInjected);
        RUNNER.run("decoding filename* gives back every real name exactly", FileNamesTest::filenameStarRoundTrip);
        RUNNER.run("a manifest with a name is format 3 and reads back exactly", FileNamesTest::formatThreeRoundTrip);
        RUNNER.run("format 1 and 2 manifests are still read, without a name", FileNamesTest::oldFormatsHaveNoName);
        RUNNER.run("a name line in format 2, format 3 without one, and a damaged name line are invalid manifests",
                FileNamesTest::formatMistakes);
        RUNNER.run("a manifest refuses an invalid name, and a name without chunk checksums", FileNamesTest::manifestRules);
        RUNNER.finish();
    }

    private static void realNamesRoundTrip(Path tempDir) {
        for (String name : REAL_NAMES) {
            check(FileNames.isValid(name), "not valid: " + escape(name));
            checkEquals(name, FileNames.decodeHeader(encodeUriComponent(name)), "header round trip of " + escape(name));
            checkEquals(name, decodeFilenameStar(FileNames.contentDisposition(name, ID)), "download round trip of " + escape(name));
        }
        check(!"re\u0301sume\u0301 2026.pdf".equals("r\u00e9sum\u00e9 2026.pdf"), "the two forms are different strings");
    }

    private static void invalidNames(Path tempDir) {
        for (String name : List.of("", ".", "..", "a/b", "/", "\0", "a\0b", "a\u202eb", "\u202ax", "\u2066x", "x\u2069",
                "\ud83d", "\ude00x", "a".repeat(256), "a".repeat(255) + "\ud83d\ude00")) {
            check(!FileNames.isValid(name), "valid: " + escape(name));
        }
        check(!FileNames.isValid(null), "null is valid");
        check(FileNames.isValid("..."), "... is refused");
        check(FileNames.isValid(".. "), "'.. ' is refused");
        check(FileNames.isValid("\u200f\u05e7\u05d5\u05d1\u05e5.txt"), "a right-to-left mark (not an override) is refused");
    }

    private static void strictHeader(Path tempDir) {
        for (String value : List.of("", "a b", "\u00e9", "a/b", "a\"b", "a\\b", "a;b", "a+b", "%", "%4", "%G1", "%2F",
                "a%2Fb", "%00", "%2E", "%2E%2E", "%E2%82", "%C0%AF", "%ED%A0%80", "%F4%90%80%80", "%E2%80%AE", "%FF",
                "a".repeat(256), "%61".repeat(FileNames.MAX_HEADER_LENGTH / 3 + 1))) {
            try {
                FileNames.decodeHeader(value);
                throw new AssertionError("accepted header value: " + escape(value));
            } catch (IllegalArgumentException expected) {
                // refused, as it must be
            }
        }
        checkEquals("a".repeat(255), FileNames.decodeHeader("%61".repeat(255)), "255 encoded characters");
        checkEquals("a b'(!)*~._-", FileNames.decodeHeader("a%20b'(!)*~._-"), "encodeURIComponent's own characters");
    }

    private static void lowercaseHexInHeader(Path tempDir) {
        checkEquals("r\u00e9sum\u00e9", FileNames.decodeHeader("r%c3%a9sum%C3%a9"), "mixed-case hex");
    }

    private static void manifestForm(Path tempDir) {
        checkEquals("r%C3%A9sum%C3%A9%202026.pdf", FileNames.toManifest("r\u00e9sum\u00e9 2026.pdf"), "r\u00e9sum\u00e9");
        checkEquals("a%27b%20%281%29.pdf", FileNames.toManifest("a'b (1).pdf"), "quote and parentheses");
        checkEquals("~-._AZaz09", FileNames.toManifest("~-._AZaz09"), "unreserved characters");
        checkEquals("a%5Cb%0A", FileNames.toManifest("a\\b\n"), "backslash and line feed");
        for (String name : REAL_NAMES) {
            String form = FileNames.toManifest(name);
            for (char c : form.toCharArray()) {
                check(c > 0x20 && c < 0x7F, "a character that is not printable ASCII in " + form);
            }
        }
    }

    private static void manifestFormStrict(Path tempDir) throws Exception {
        for (String value : List.of("r%c3%a9", "%41", "a'b", "a b", "%2F", "%C3", "", "%2E", "%E2%80%AE", "a%")) {
            try {
                FileNames.fromManifest(value);
                throw new AssertionError("accepted manifest name: " + value);
            } catch (InvalidManifestException expected) {
                // refused, as it must be
            }
        }
        checkEquals("r\u00e9sum\u00e9", FileNames.fromManifest("r%C3%A9sum%C3%A9"), "canonical form");
    }

    private static void dispositionWithoutName(Path tempDir) {
        checkEquals("attachment; filename=\"" + ID + ".bin\"", FileNames.contentDisposition(null, ID), "no name");
    }

    private static void dispositionWithName(Path tempDir) {
        checkEquals("attachment; filename=\"report.pdf\"; filename*=UTF-8''report.pdf",
                FileNames.contentDisposition("report.pdf", ID), "report.pdf");
        checkEquals("attachment; filename=\"r_sum_ 2026.pdf\"; filename*=UTF-8''r%C3%A9sum%C3%A9%202026.pdf",
                FileNames.contentDisposition("r\u00e9sum\u00e9 2026.pdf", ID), "r\u00e9sum\u00e9 2026.pdf");
        checkEquals("attachment; filename=\"___.txt\"; filename*=UTF-8''%E6%97%A5%E6%9C%AC%E8%AA%9E.txt",
                FileNames.contentDisposition("\u65e5\u672c\u8a9e.txt", ID), "Japanese with an ASCII extension");
        checkEquals("attachment; filename=\"" + ID + ".bin\"; filename*=UTF-8''%E6%97%A5%E6%9C%AC",
                FileNames.contentDisposition("\u65e5\u672c", ID), "nothing usable in the fallback");
        checkEquals("attachment; filename=\"a'b (1).pdf\"; filename*=UTF-8''a%27b%20%281%29.pdf",
                FileNames.contentDisposition("a'b (1).pdf", ID), "quote and parentheses");
    }

    private static void dispositionCannotBeInjected(Path tempDir) {
        List<String> names = new ArrayList<>(REAL_NAMES);
        names.addAll(List.of("a\"; filename=\"evil.exe", "x\r\nSet-Cookie: a=b", "\"", ";", "%22", "a\\\"b", "\u0085x",
                "x\u2028y", "\"; filename*=UTF-8''evil.exe"));
        for (String name : names) {
            check(FileNames.isValid(name), "not valid: " + escape(name));
            String value = FileNames.contentDisposition(name, ID);
            for (char c : value.toCharArray()) {
                check(c >= 0x20 && c <= 0x7E, "a character that is not printable ASCII in " + escape(value));
            }
            checkEquals(1, count(value, "filename=\""), "filename= parameters in " + value);
            checkEquals(1, count(value, "filename*="), "filename*= parameters in " + value);
            checkEquals(2, count(value, "\""), "quotes in " + value);
            check(value.startsWith("attachment; filename=\""), "starts with: " + value);
            String star = value.substring(value.indexOf("filename*=UTF-8''") + "filename*=UTF-8''".length());
            for (char c : star.toCharArray()) {
                check(Character.isLetterOrDigit(c) && c < 0x80 || "!#$&+-.^_`|~%".indexOf(c) >= 0,
                        "a character RFC 8187 does not allow in filename*: " + c + " in " + value);
            }
            checkEquals(name, decodeFilenameStar(value), "the exact name in " + value);
        }
    }

    private static void filenameStarRoundTrip(Path tempDir) {
        for (String name : REAL_NAMES) {
            checkEquals(name, decodeFilenameStar(FileNames.contentDisposition(name, ID)), escape(name));
        }
    }

    private static void formatThreeRoundTrip(Path tempDir) throws Exception {
        for (String name : REAL_NAMES) {
            FileManifest manifest = new FileManifest(ID, 5, SHA, 4, 2, List.of(SHA, SHA), name);
            checkEquals(3, manifest.format(), "format");
            String text = manifest.toText();
            check(text.startsWith("format=3\n"), "format line");
            check(text.contains("\nchunkCount=2\nname=" + FileNames.toManifest(name) + "\nchunk0.sha256="), "name line: " + text);
            FileManifest read = FileManifest.parse(text, ID);
            checkEquals(manifest, read, "read back");
            checkEquals(name, read.name(), "name of " + escape(name));
            check(read.hasChunkChecksums(), "format 3 has chunk checksums");
        }
    }

    private static void oldFormatsHaveNoName(Path tempDir) throws Exception {
        FileManifest two = FileManifest.parse(new FileManifest(ID, 5, SHA, 4, 2, List.of(SHA, SHA)).toText(), ID);
        checkEquals(null, two.name(), "format 2 name");
        checkEquals(2, two.format(), "format 2");
        FileManifest one = FileManifest.parse(new FileManifest(ID, 5, SHA, 4, 2).toText(), ID);
        checkEquals(null, one.name(), "format 1 name");
        checkEquals(1, one.format(), "format 1");
        checkEquals("format=2\nfileId=" + ID + "\nsize=5\nsha256=" + SHA + "\nchunkSize=4\nchunkCount=2\nchunk0.sha256=" + SHA
                + "\nchunk1.sha256=" + SHA + "\n", two.toText(), "format 2 is written exactly as before");
    }

    private static void formatMistakes(Path tempDir) {
        String base = "fileId=" + ID + "\nsize=5\nsha256=" + SHA + "\nchunkSize=4\nchunkCount=2\n";
        String chunks = "chunk0.sha256=" + SHA + "\nchunk1.sha256=" + SHA + "\n";
        for (String text : List.of(
                "format=2\n" + base + "name=report.pdf\n" + chunks,          // a name in format 2
                "format=3\n" + base + chunks,                                 // format 3 without a name
                "format=3\n" + base + "name=\n" + chunks,                     // an empty name
                "format=3\n" + base + "name=r%c3%a9.pdf\n" + chunks,          // lowercase hex
                "format=3\n" + base + "name=a%2Fb\n" + chunks,                // a slash
                "format=3\n" + base + "name=a b\n" + chunks,                  // a raw space
                "format=3\n" + base + "name=r\u00e9.pdf\n" + chunks,          // raw UTF-8
                "format=3\n" + base + chunks + "name=report.pdf\n",           // the name in the wrong place
                "format=3\n" + base + "name=a.pdf\nname=b.pdf\n" + chunks,    // two names
                "format=4\n" + base + "name=report.pdf\n" + chunks)) {
            try {
                FileManifest.parse(text, ID);
                throw new AssertionError("accepted manifest:\n" + text);
            } catch (InvalidManifestException expected) {
                // refused, as it must be
            }
        }
    }

    private static void manifestRules(Path tempDir) {
        for (String name : List.of("", ".", "a/b", "a\u202eb")) {
            try {
                new FileManifest(ID, 5, SHA, 4, 2, List.of(SHA, SHA), name);
                throw new AssertionError("accepted name " + escape(name));
            } catch (IllegalArgumentException expected) {
                // refused, as it must be
            }
        }
        try {
            new FileManifest(ID, 5, SHA, 4, 2, null, "report.pdf");
            throw new AssertionError("accepted a name without chunk checksums");
        } catch (IllegalArgumentException expected) {
            // refused, as it must be
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** What JavaScript's encodeURIComponent writes for {@code name}. */
    static String encodeUriComponent(String name) {
        StringBuilder out = new StringBuilder();
        for (byte b : name.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || "-_.!~*'()".indexOf(c) >= 0) {
                out.append((char) c);
            } else {
                out.append('%').append(String.format("%02X", c));
            }
        }
        return out.toString();
    }

    /** The name in a Content-Disposition value's {@code filename*=UTF-8''...}, decoded as RFC 8187 says. */
    static String decodeFilenameStar(String value) {
        String marker = "filename*=UTF-8''";
        int at = value.indexOf(marker);
        check(at >= 0, "no filename* in " + value);
        String encoded = value.substring(at + marker.length());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);
            if (c == '%') {
                bytes.write(Integer.parseInt(encoded.substring(i + 1, i + 3), 16));
                i += 2;
            } else {
                bytes.write(c);
            }
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private static int count(String text, String part) {
        int count = 0;
        for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + 1)) {
            count++;
        }
        return count;
    }

    /** A name with its non-ASCII characters as \\uXXXX, for messages. */
    static String escape(String name) {
        StringBuilder out = new StringBuilder();
        for (char c : name.toCharArray()) {
            out.append(c >= 0x20 && c < 0x7F ? String.valueOf(c) : String.format("\\u%04X", (int) c));
        }
        return out.length() > 80 ? out.substring(0, 80) + "..." : out.toString();
    }
}
