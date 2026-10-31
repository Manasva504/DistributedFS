package com.distributedfs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Original file names: the rule for what a name may be, the {@code X-File-Name} upload header, the
 * name's form inside a manifest, and the download's {@code Content-Disposition} header.
 *
 * <p><b>The rule</b> ({@link #isValid}). A name is 1 to 255 Unicode code points of valid UTF-16
 * (no unpaired surrogate), is not {@code .} or {@code ..}, and contains no {@code /}, no NUL and no
 * bidirectional-override character (U+202A to U+202E, U+2066 to U+2069), which could make a name
 * look like a different one. No real file can break the first parts: every operating system keeps
 * {@code /} and NUL out of names and limits them to 255 units or bytes, so a user never has to
 * rename a file for this server. Everything else a real name may hold is accepted and kept exactly,
 * byte for byte and without Unicode normalization: spaces, accents, any script, emoji, and on macOS
 * or Linux even a backslash or a control character. The server never uses a name as a path, and
 * every place it goes out of the server encodes it (see below), so none of that can do harm.
 *
 * <p><b>Upload header</b> ({@link #decodeHeader}). {@code X-File-Name} carries the name as
 * percent-encoded UTF-8, as JavaScript's {@code encodeURIComponent} writes it: only
 * {@code A-Z a-z 0-9 - _ . ! ~ * ' ( )} as themselves and every other byte as {@code %XX}. The
 * decoding is strict: malformed UTF-8 (including overlong forms and encoded surrogates) is refused,
 * never replaced.
 *
 * <p><b>In a manifest</b> ({@link #toManifest}, {@link #fromManifest}). The name is stored in one
 * canonical percent-encoded form (only {@code A-Z a-z 0-9 - . _ ~} as themselves, every other byte
 * as uppercase {@code %XX}), so a manifest stays printable ASCII and is still read exactly as
 * written.
 *
 * <p><b>Download</b> ({@link #contentDisposition}). {@code attachment; filename="<fallback>";
 * filename*=UTF-8''<encoded>} (RFC 6266 and RFC 8187): browsers use {@code filename*}, the exact
 * name; older clients use the ASCII fallback, in which every character that is not plain printable
 * ASCII, and {@code "}, {@code \}, {@code %}, {@code ;} and {@code =}, is {@code _}. The header is therefore
 * always one line of printable ASCII, whatever the name. A file without a name downloads as
 * {@code <fileId>.bin}.
 */
final class FileNames {

    /** The upload header that carries the original file name. */
    static final String HEADER = "X-File-Name";

    static final int MAX_CODE_POINTS = 255;

    /** The longest header value that can hold a valid name: 255 code points, 4 UTF-8 bytes each, as %XX. */
    static final int MAX_HEADER_LENGTH = MAX_CODE_POINTS * 4 * 3;

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private FileNames() {
    }

    /** Whether {@code name} is a file name this server stores (see the class comment). */
    static boolean isValid(String name) {
        if (name == null || name.isEmpty() || name.equals(".") || name.equals("..")) {
            return false;
        }
        int codePoints = 0;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= name.length() || !Character.isLowSurrogate(name.charAt(i + 1))) {
                    return false;
                }
                i++;
            } else if (Character.isLowSurrogate(c)) {
                return false;
            } else if (c == '/' || c == '\0' || isBidiOverride(c)) {
                return false;
            }
            codePoints++;
        }
        return codePoints <= MAX_CODE_POINTS;
    }

    /** U+202A to U+202E (embeddings and overrides) and U+2066 to U+2069 (isolates). */
    static boolean isBidiOverride(char c) {
        return (c >= '\u202a' && c <= '\u202e') || (c >= '\u2066' && c <= '\u2069');
    }

    /**
     * The name in an {@code X-File-Name} header value.
     *
     * @throws IllegalArgumentException if the value is not exactly a percent-encoded valid name
     */
    static String decodeHeader(String value) {
        if (value == null || value.isEmpty() || value.length() > MAX_HEADER_LENGTH) {
            throw new IllegalArgumentException("the value is empty or too long");
        }
        String name = percentDecode(value, Safe.HEADER);
        if (!isValid(name)) {
            throw new IllegalArgumentException("the decoded value is not a valid file name");
        }
        return name;
    }

    /** The name as written in a manifest: canonical percent-encoded UTF-8. */
    static String toManifest(String name) {
        return percentEncode(name, Safe.MANIFEST);
    }

    /**
     * The name in a manifest's {@code name=} value.
     *
     * @throws InvalidManifestException if the value is not exactly the canonical form of a valid name
     */
    static String fromManifest(String value) throws InvalidManifestException {
        String name;
        try {
            name = percentDecode(value, Safe.MANIFEST);
        } catch (IllegalArgumentException e) {
            throw new InvalidManifestException("the name is not percent-encoded UTF-8: " + e.getMessage(), e);
        }
        if (!isValid(name)) {
            throw new InvalidManifestException("the name is not a valid file name");
        }
        if (!toManifest(name).equals(value)) {
            throw new InvalidManifestException("the name is not in canonical percent-encoded form");
        }
        return name;
    }

    /** The value of a download's {@code Content-Disposition} header; {@code name} may be null. */
    static String contentDisposition(String name, String fileId) {
        if (name == null) {
            return "attachment; filename=\"" + fileId + ".bin\"";
        }
        return "attachment; filename=\"" + asciiFallback(name, fileId) + "\"; filename*=UTF-8''"
                + percentEncode(name, Safe.RFC8187);
    }

    /**
     * The name with every character that is not printable ASCII, and {@code " \ % ; =}, replaced by
     * {@code _}; {@code <fileId>.bin} if nothing but dots, underscores and spaces would be left.
     */
    static String asciiFallback(String name, String fileId) {
        StringBuilder out = new StringBuilder();
        boolean meaningful = false;
        for (int i = 0; i < name.length(); i = name.offsetByCodePoints(i, 1)) {
            int c = name.codePointAt(i);
            // '=' too, so that neither "filename=" nor "filename*=" can appear inside the quotes.
            boolean plain = c >= 0x20 && c <= 0x7E && c != '"' && c != '\\' && c != '%' && c != ';' && c != '=';
            out.append(plain ? (char) c : '_');
            meaningful |= plain && c != '.' && c != ' ' && c != '_';
        }
        return meaningful ? out.toString() : fileId + ".bin";
    }

    // ---------------------------------------------------------------------
    // Percent-encoding
    // ---------------------------------------------------------------------

    /** Which ASCII characters stand for themselves in each form; every other byte is %XX. */
    private enum Safe {
        /** encodeURIComponent: A-Z a-z 0-9 - _ . ! ~ * ' ( ) */
        HEADER("-_.!~*'()"),
        /** RFC 3986 unreserved: A-Z a-z 0-9 - . _ ~ */
        MANIFEST("-._~"),
        /** RFC 8187 attr-char: A-Z a-z 0-9 ! # $ & + - . ^ _ ` | ~ */
        RFC8187("!#$&+-.^_`|~");

        private final String marks;

        Safe(String marks) {
            this.marks = marks;
        }

        boolean plain(int c) {
            return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || marks.indexOf(c) >= 0;
        }
    }

    private static String percentEncode(String name, Safe safe) {
        StringBuilder out = new StringBuilder();
        for (byte b : name.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if (c < 0x80 && safe.plain(c)) {
                out.append((char) c);
            } else {
                out.append('%').append(HEX[c >> 4]).append(HEX[c & 0xF]);
            }
        }
        return out.toString();
    }

    /**
     * Decodes {@code value}, in which only the given safe characters and {@code %XX} (two
     * hexadecimal digits; uppercase only in the manifest form) may appear, as strict UTF-8.
     *
     * @throws IllegalArgumentException for any other character, a bad escape or malformed UTF-8
     */
    private static String percentDecode(String value, Safe safe) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '%') {
                if (i + 2 >= value.length()) {
                    throw new IllegalArgumentException("an incomplete %XX escape");
                }
                int high = hexDigit(value.charAt(i + 1), safe == Safe.MANIFEST);
                int low = hexDigit(value.charAt(i + 2), safe == Safe.MANIFEST);
                bytes.write(high << 4 | low);
                i += 2;
            } else if (c < 0x80 && safe.plain(c)) {
                bytes.write(c);
            } else {
                throw new IllegalArgumentException("a character that must be percent-encoded");
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("malformed UTF-8");
        }
    }

    private static int hexDigit(char c, boolean upperOnly) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        if (!upperOnly && c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        throw new IllegalArgumentException("a %XX escape with a character that is not a hexadecimal digit");
    }
}
