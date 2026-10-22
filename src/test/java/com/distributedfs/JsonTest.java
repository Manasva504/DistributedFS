package com.distributedfs;

import java.util.Objects;

/**
 * Plain-Java tests for Json (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.JsonTest}
 *
 * <p>Besides fixed examples, every one of the 65,536 Java char values is quoted and then read
 * back by a small JSON string reader written for this test from RFC 8259, so the escapes are
 * checked against the JSON rules rather than against the code under test.
 * The program exits with code 1 if any test fails.
 */
public class JsonTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.JsonTest");
            System.exit(2);
            return;
        }

        runTest("printable ASCII is copied unchanged between quotes", JsonTest::printableAsciiIsUnchanged);
        runTest("quotes and backslashes are escaped with a backslash", JsonTest::quotesAndBackslashesAreEscaped);
        runTest("control characters and DEL become \\u escapes", JsonTest::controlCharactersAreEscaped);
        runTest("non-ASCII text becomes lowercase \\u escapes, one per UTF-16 unit",
                JsonTest::nonAsciiIsEscaped);
        runTest("the empty string becomes \"\"", JsonTest::emptyString);
        runTest("null is rejected", JsonTest::nullIsRejected);
        runTest("every char value is quoted as printable ASCII and reads back unchanged",
                JsonTest::everyCharValueRoundTrips);
        runTest("a long mixed string reads back unchanged", JsonTest::longMixedStringRoundTrips);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    private static void printableAsciiIsUnchanged() {
        StringBuilder text = new StringBuilder();
        for (char c = 0x20; c <= 0x7E; c++) {
            if (c != '"' && c != '\\') {
                text.append(c);
            }
        }
        checkEquals("\"" + text + "\"", Json.quote(text.toString()), "quoted printable ASCII");
    }

    private static void quotesAndBackslashesAreEscaped() {
        checkEquals("\"say \\\"hi\\\"\"", Json.quote("say \"hi\""), "quotes");
        checkEquals("\"C:\\\\temp\\\\x\"", Json.quote("C:\\temp\\x"), "backslashes");
        checkEquals("\"\\\\\"", Json.quote("\\"), "a single backslash");
        checkEquals("\"\\\"\"", Json.quote("\""), "a single quote");
    }

    private static void controlCharactersAreEscaped() {
        checkEquals("\"a\\u000ab\"", Json.quote("a\nb"), "line feed");
        checkEquals("\"\\u0000\"", Json.quote("\0"), "NUL");
        checkEquals("\"\\u0009\\u000d\\u001f\\u007f\"", Json.quote("\t\r\u001f\u007f"), "tab, CR, unit separator, DEL");
    }

    private static void nonAsciiIsEscaped() {
        checkEquals("\"\\u00e9\"", Json.quote("\u00e9"), "e with acute accent");
        checkEquals("\"\\u20ac\"", Json.quote("\u20ac"), "euro sign");
        checkEquals("\"\\uff10\"", Json.quote("\uff10"), "fullwidth digit zero");
        // U+1F600 (an emoji) is two UTF-16 units in Java, and JSON writes it as two escapes.
        checkEquals("\"\\ud83d\\ude00\"", Json.quote("\uD83D\uDE00"), "emoji outside the Basic Multilingual Plane");
    }

    private static void emptyString() {
        checkEquals("\"\"", Json.quote(""), "empty string");
    }

    private static void nullIsRejected() {
        expectThrows(NullPointerException.class, () -> Json.quote(null));
    }

    private static void everyCharValueRoundTrips() {
        for (int value = Character.MIN_VALUE; value <= Character.MAX_VALUE; value++) {
            char c = (char) value;
            String original = "x" + c + "y";
            String quoted = Json.quote(original);
            checkPrintableAscii(quoted, "quoted U+" + hex4(c));
            String readBack = readJsonString(quoted);
            if (!original.equals(readBack)) {
                throw new AssertionError("U+" + hex4(c) + " did not survive quoting: " + quoted);
            }
        }
    }

    private static void longMixedStringRoundTrips() {
        String piece = "plain \"quoted\" back\\slash tab\t new\nline \u00e9\u20ac\uD83D\uDE00 \u0000\u007f end";
        String original = piece.repeat(500);
        String quoted = Json.quote(original);
        checkPrintableAscii(quoted, "quoted long string");
        checkEquals(original, readJsonString(quoted), "long string read back");
    }

    // ---------------------------------------------------------------------
    // An independent JSON string reader (RFC 8259, section 7)
    // ---------------------------------------------------------------------

    /**
     * Reads one JSON string literal and returns its value. Fails the test if the literal breaks
     * the JSON rules: a missing quote, an unescaped quote or control character inside, or an
     * unknown or incomplete escape.
     */
    private static String readJsonString(String literal) {
        int last = literal.length() - 1;
        check(literal.length() >= 2 && literal.charAt(0) == '"' && literal.charAt(last) == '"',
                "not a quoted JSON string: " + literal);
        StringBuilder value = new StringBuilder();
        int i = 1;
        while (i < last) {
            char c = literal.charAt(i);
            check(c != '"', "unescaped quote inside: " + literal);
            check(c >= 0x20, "unescaped control character inside: " + literal);
            if (c != '\\') {
                value.append(c);
                i++;
                continue;
            }
            check(i + 1 < last, "backslash at the end of: " + literal);
            char escape = literal.charAt(i + 1);
            switch (escape) {
                case '"' -> value.append('"');
                case '\\' -> value.append('\\');
                case '/' -> value.append('/');
                case 'b' -> value.append('\b');
                case 'f' -> value.append('\f');
                case 'n' -> value.append('\n');
                case 'r' -> value.append('\r');
                case 't' -> value.append('\t');
                case 'u' -> {
                    check(i + 6 <= last, "incomplete \\u escape in: " + literal);
                    String digits = literal.substring(i + 2, i + 6);
                    for (char d : digits.toCharArray()) {
                        check(Character.digit(d, 16) >= 0 && d < 0x80, "bad \\u digits \"" + digits + "\" in: " + literal);
                    }
                    value.append((char) Integer.parseInt(digits, 16));
                    i += 6;
                    continue;
                }
                default -> throw new AssertionError("invalid escape \\" + escape + " in: " + literal);
            }
            i += 2;
        }
        return value.toString();
    }

    private static void checkPrintableAscii(String text, String what) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < 0x20 || c > 0x7E) {
                throw new AssertionError(what + ": character U+" + hex4(c) + " at position " + i + " is not printable ASCII");
            }
        }
    }

    private static String hex4(char c) {
        return String.format("%04X", (int) c);
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites; these tests need no files)
    // ---------------------------------------------------------------------

    private interface TestBody {
        void run() throws Exception;
    }

    /** A piece of code that is expected to throw. */
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static void runTest(String name, TestBody body) {
        try {
            body.run();
            passed++;
            System.out.println("PASS  " + name);
        } catch (AssertionError e) {
            failed++;
            System.out.println("FAIL  " + name);
            System.out.println("      " + e.getMessage());
        } catch (Throwable e) {
            failed++;
            System.out.println("FAIL  " + name + " (unexpected exception)");
            e.printStackTrace(System.out);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkEquals(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, ThrowingAction action) {
        try {
            action.run();
        } catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            throw new AssertionError("expected " + expectedType.getSimpleName() + " but got " + actual, actual);
        }
        throw new AssertionError("expected " + expectedType.getSimpleName() + " but nothing was thrown");
    }
}
