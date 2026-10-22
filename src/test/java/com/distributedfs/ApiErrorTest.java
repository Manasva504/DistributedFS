package com.distributedfs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Plain-Java tests for ApiError (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.ApiErrorTest}
 *
 * <p>The first test pins the whole error table (status and code of every error, in order), so
 * any change to the API's error contract has to be made here on purpose as well.
 * The program exits with code 1 if any test fails.
 */
public class ApiErrorTest {

    /** The planned contract: "status code" for every error, in declaration order. */
    private static final List<String> CONTRACT = List.of(
            "400 invalid_file_id",
            "400 invalid_checksum",
            "400 checksum_mismatch",
            "403 host_not_allowed",
            "404 not_found",
            "404 file_not_found",
            "405 method_not_allowed",
            "413 payload_too_large",
            "415 unsupported_media_type",
            "500 chunk_missing",
            "500 integrity_check_failed",
            "500 metadata_corrupt",
            "500 storage_error",
            "500 internal_error");

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.ApiErrorTest");
            System.exit(2);
            return;
        }

        runTest("the error table is exactly the planned contract", ApiErrorTest::tableMatchesContract);
        runTest("every code is the constant's name in lowercase, and codes are unique",
                ApiErrorTest::codesAreStableAndUnique);
        runTest("every message is one fixed, plain sentence with nothing that could leak details",
                ApiErrorTest::messagesArePlainSentences);
        runTest("toJson writes exactly the error and message fields", ApiErrorTest::toJsonHasExactlyTwoFields);
        runTest("file_not_found produces the documented body", ApiErrorTest::documentedExample);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    private static void tableMatchesContract() {
        List<String> actual = new ArrayList<>();
        for (ApiError error : ApiError.values()) {
            actual.add(error.status() + " " + error.code());
        }
        checkEquals(CONTRACT, actual, "error table");
    }

    private static void codesAreStableAndUnique() {
        Set<String> seen = new HashSet<>();
        for (ApiError error : ApiError.values()) {
            String code = error.code();
            checkEquals(error.name().toLowerCase(Locale.ROOT), code, "code of " + error.name());
            for (char c : code.toCharArray()) {
                check((c >= 'a' && c <= 'z') || c == '_', "code \"" + code + "\" has character '" + c + "'");
            }
            check(seen.add(code), "code \"" + code + "\" is used twice");
        }
    }

    private static void messagesArePlainSentences() {
        for (ApiError error : ApiError.values()) {
            String message = error.message();
            String what = error.name() + " message \"" + message + "\"";
            check(!message.isBlank(), what + " is blank");
            check(message.length() <= 120, what + " is longer than 120 characters");
            for (char c : message.toCharArray()) {
                check(c >= 0x20 && c <= 0x7E, what + " has a character that is not printable ASCII");
            }
            check(Character.isUpperCase(message.charAt(0)), what + " does not start with a capital letter");
            check(message.endsWith(".") && !message.endsWith(".."), what + " does not end with one full stop");
            check(!message.contains(". "), what + " is more than one sentence");
            // Nothing that looks like a path, a placeholder, quoting, or exception text.
            for (String forbidden : List.of("\\", "\"", "%", "{", "}", "Exception", "java.", "at com.")) {
                check(!message.contains(forbidden), what + " contains \"" + forbidden + "\"");
            }
        }
    }

    private static void toJsonHasExactlyTwoFields() {
        for (ApiError error : ApiError.values()) {
            // Exact comparison is possible because the messages contain no character that JSON
            // escapes (checked in the previous test).
            String expected = "{\"error\":\"" + error.code() + "\",\"message\":\"" + error.message() + "\"}";
            checkEquals(expected, error.toJson(), error.name() + ".toJson()");
        }
    }

    private static void documentedExample() {
        checkEquals("{\"error\":\"file_not_found\",\"message\":\"No file with this ID is stored.\"}",
                ApiError.FILE_NOT_FOUND.toJson(), "FILE_NOT_FOUND.toJson()");
        checkEquals(404, ApiError.FILE_NOT_FOUND.status(), "FILE_NOT_FOUND.status()");
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites; these tests need no files)
    // ---------------------------------------------------------------------

    private interface TestBody {
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
}
