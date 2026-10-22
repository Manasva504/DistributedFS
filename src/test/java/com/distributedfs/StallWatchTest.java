package com.distributedfs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plain-Java tests for FileApiHandler's download stall watchdog (no JUnit or other libraries).
 *
 * <p>Run: {@code java -cp "out;out-test" com.distributedfs.StallWatchTest}
 *
 * <p>The watchdog is driven directly with a stub exchange that only counts how often it is
 * closed, so these tests can check the hand-over between the watchdog and the download that owns
 * it: exactly one of them ever closes the exchange. DownloadEndpointTest covers the watchdog
 * against real connections. The program exits with code 1 if any test fails.
 */
public class StallWatchTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.println("Usage: java -cp \"out;out-test\" com.distributedfs.StallWatchTest");
            System.exit(2);
            return;
        }

        runTest("with no progress the watchdog closes the exchange once, and release then reports it",
                StallWatchTest::firesOnceWithoutProgress);
        runTest("progress keeps the watchdog from firing", StallWatchTest::progressPreventsFiring);
        runTest("after release the watchdog never closes the exchange, even long after the timeout",
                StallWatchTest::releasePreventsFiring);
        runTest("a release while the watchdog sleeps past its deadline keeps it from firing when it wakes",
                StallWatchTest::releaseDuringSleepPastDeadline);
        runTest("release racing the watchdog: exactly one of them wins, every time",
                StallWatchTest::releaseAndFiringNeverBothHappen);
        runTest("close always ends the watchdog thread and, while watching, never closes the exchange",
                StallWatchTest::closeEndsThread);

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void firesOnceWithoutProgress() throws Exception {
        CountingExchange exchange = new CountingExchange();
        try (FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, 50)) {
            waitUntil(() -> exchange.closes.get() == 1, "the watchdog closed the exchange");
            check(watch.fired(), "fired");
            check(!watch.release(), "release reports that the watchdog got there first");
            Thread.sleep(150);
            checkEquals(1, exchange.closes.get(), "closes");
        }
        checkEquals(1, exchange.closes.get(), "closes after the watch was closed");
    }

    private static void progressPreventsFiring() throws Exception {
        CountingExchange exchange = new CountingExchange();
        try (FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, 200)) {
            for (int i = 0; i < 30; i++) { // 600 ms in all, three times the timeout
                watch.progress();
                Thread.sleep(20);
            }
            checkEquals(0, exchange.closes.get(), "closes while progress was made");
            check(watch.release(), "release succeeds");
            Thread.sleep(300);
            checkEquals(0, exchange.closes.get(), "closes after release");
        }
    }

    private static void releasePreventsFiring() throws Exception {
        CountingExchange exchange = new CountingExchange();
        FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, 30);
        try {
            check(watch.release(), "release succeeds before the timeout");
            check(watch.release(), "a second release still reports success");
            Thread.sleep(200);
            checkEquals(0, exchange.closes.get(), "closes");
            check(!watch.fired(), "not fired");
        } finally {
            watch.close();
        }
        check(!watch.threadAlive(), "the watchdog thread has ended");
    }

    /**
     * With a 1 ms timeout the watchdog sleeps about 10 ms before its first check, by which time
     * the deadline has long passed. Releasing 3 ms in, while it sleeps, must still stop it.
     */
    private static void releaseDuringSleepPastDeadline() throws Exception {
        for (int i = 0; i < 20; i++) {
            CountingExchange exchange = new CountingExchange();
            FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, 1);
            try {
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(3);
                while (System.nanoTime() < until) {
                    Thread.onSpinWait();
                }
                check(watch.release(), "iteration " + i + ": release 3 ms in succeeds");
                Thread.sleep(60); // several check intervals
                checkEquals(0, exchange.closes.get(), "iteration " + i + ": closes after the release");
                check(!watch.fired(), "iteration " + i + ": not fired");
            } finally {
                watch.close();
            }
        }
    }

    /**
     * With a 1 ms timeout the watchdog fires at its first check, about 10 ms in. The owner
     * releases at a random moment between 0 and 25 ms, so both orders happen many times.
     */
    private static void releaseAndFiringNeverBothHappen() throws Exception {
        Random random = new Random(20261020L);
        int ownerWon = 0;
        int watchdogWon = 0;
        for (int i = 0; i < 400; i++) {
            CountingExchange exchange = new CountingExchange();
            FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, 1);
            boolean released;
            try {
                long until = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(random.nextInt(25_000));
                while (System.nanoTime() < until) {
                    Thread.onSpinWait();
                }
                released = watch.release();
                // Keep the watch alive after the release, as a download does while it closes its
                // response: a watchdog that ignored the release would fire in this time.
                Thread.sleep(30);
            } finally {
                watch.close();
            }
            check(!watch.threadAlive(), "iteration " + i + ": the watchdog thread has ended");
            int closes = exchange.closes.get();
            if (released) {
                checkEquals(0, closes, "iteration " + i + ": closes after the owner released first");
                check(!watch.fired(), "iteration " + i + ": not fired after the owner released first");
                ownerWon++;
            } else {
                checkEquals(1, closes, "iteration " + i + ": closes after the watchdog fired first");
                check(watch.fired(), "iteration " + i + ": fired");
                watchdogWon++;
            }
        }
        System.out.println("      owner released first " + ownerWon + " times, watchdog fired first " + watchdogWon
                + " times");
        check(ownerWon > 0 && watchdogWon > 0, "both orders were exercised");
    }

    private static void closeEndsThread() throws Exception {
        CountingExchange exchange = new CountingExchange();
        FileApiHandler.StallWatch watch = new FileApiHandler.StallWatch(exchange, 60_000);
        check(watch.threadAlive(), "the watchdog thread runs while watching");
        watch.close();
        check(!watch.threadAlive(), "the watchdog thread has ended");
        checkEquals(0, exchange.closes.get(), "closes");
        check(!watch.fired(), "not fired");
    }

    // ---------------------------------------------------------------------
    // A stub exchange that only counts close()
    // ---------------------------------------------------------------------

    private static final class CountingExchange extends HttpExchange {
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
        }

        @Override
        public Headers getRequestHeaders() {
            return new Headers();
        }

        @Override
        public Headers getResponseHeaders() {
            return new Headers();
        }

        @Override
        public URI getRequestURI() {
            return URI.create("/");
        }

        @Override
        public String getRequestMethod() {
            return "GET";
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public InputStream getRequestBody() {
            return InputStream.nullInputStream();
        }

        @Override
        public OutputStream getResponseBody() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public int getResponseCode() {
            return -1;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
        }

        @Override
        public HttpPrincipal getPrincipal() {
            return null;
        }
    }

    // ---------------------------------------------------------------------
    // Tiny test framework (same style as the other suites)
    // ---------------------------------------------------------------------

    private interface TestBody {
        void run() throws Exception;
    }

    private interface Condition {
        boolean holds() throws Exception;
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

    /** Polls {@code condition} every 5 ms for up to 5 seconds. */
    private static void waitUntil(Condition condition, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.holds()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out: " + what);
            }
            Thread.sleep(5);
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
