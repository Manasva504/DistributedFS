package com.distributedfs;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A storage node as its own process: it keeps chunks in its own folder and serves them over HTTP
 * on 127.0.0.1 ({@link NodeApiHandler}), so the DistributedFS server can reach it with a
 * {@link RemoteStorageNode} and nodes can fail independently.
 *
 * <p>Usage: {@code java -cp out com.distributedfs.StorageNodeServer --data-dir <folder>
 * --port <0-65535> [--max-chunk-mb <1-64>]} (0 picks any free port and prints it; chunks of up to
 * 16 MiB by default). Stop it with Ctrl+C.
 *
 * <p><b>Startup order.</b> Nothing listens until every step has succeeded:
 * <ol>
 *   <li>Open the node folder ({@link NodeDataDirectory#open}), which checks, initializes and locks
 *       it. "In use" is retried for up to 2 seconds, as in the server, because Windows releases a
 *       killed process's lock shortly after the process ends.</li>
 *   <li>Delete stale temp files that a killed store left in the chunk folder. Nothing can be
 *       storing yet, because the lock is held and nothing listens. Problems are warnings only.</li>
 *   <li>Bind to 127.0.0.1 only and start serving.</li>
 * </ol>
 * If a step fails, everything already started is undone and the folder's lock is released last.
 *
 * <p><b>Output.</b> The ready line is {@code DistributedFS storage node <node-id> listening on
 * http://127.0.0.1:<port>/ (data directory: <folder>)}; then one line per request (see
 * {@link NodeApiHandler}).
 */
public final class StorageNodeServer {

    static final int EXIT_SUCCESS = 0;
    static final int EXIT_FAILURE = 1;
    static final int EXIT_USAGE = 2;

    static final String USAGE = "Usage: java -cp out com.distributedfs.StorageNodeServer --data-dir <folder>"
            + " --port <0-65535> [--max-chunk-mb <1-64>]";

    static final int DEFAULT_MAX_CHUNK_MB = 16;
    static final int MAX_CHUNK_MB_LIMIT = 64;
    private static final long BYTES_PER_MB = 1024L * 1024L;

    private static final int WORKER_WAIT_SECONDS = 5;
    private static final long LOCK_RETRY_INTERVAL_MILLIS = 50;
    private static final int BACKLOG = 50;

    /** The node binds to this address only: IPv4 loopback, so no other machine can connect. */
    private static final InetAddress LOOPBACK = loopbackAddress();

    /**
     * Node settings. Production uses {@link #defaults}; tests use short timeouts.
     *
     * @param port               0 to 65535; 0 picks any free port
     * @param workerThreads      number of request threads, at least 1
     * @param queueCapacity      requests that may wait for a thread, at least 1
     * @param stopGraceSeconds   how long stop waits for requests in progress, 0 or more
     * @param lockRetryMillis    how long to keep retrying a node folder that is in use, 0 or more
     * @param maxChunkBytes      the largest chunk accepted, at least 1
     * @param stallTimeoutMillis how long a transfer may go without progress, at least 1
     * @param lockWaitMillis     how long a request waits for the node-wide lock before 503 node_busy
     */
    record Config(Path dataDir, int port, int workerThreads, int queueCapacity, int stopGraceSeconds,
                  long lockRetryMillis, PrintStream out, PrintStream err, long maxChunkBytes,
                  long stallTimeoutMillis, long lockWaitMillis) {

        Config {
            Objects.requireNonNull(dataDir, "dataDir must not be null");
            Objects.requireNonNull(out, "out must not be null");
            Objects.requireNonNull(err, "err must not be null");
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port must be 0 to 65535, but was " + port);
            }
            if (workerThreads < 1 || queueCapacity < 1 || stopGraceSeconds < 0 || lockRetryMillis < 0
                    || maxChunkBytes < 1 || stallTimeoutMillis < 1 || lockWaitMillis < 0) {
                throw new IllegalArgumentException("invalid node setting");
            }
        }

        static Config defaults(Path dataDir, int port, long maxChunkBytes, PrintStream out, PrintStream err) {
            return new Config(dataDir, port, 4, 16, 1, 2000, out, err, maxChunkBytes,
                    FileApiHandler.STALL_TIMEOUT_MILLIS, 10_000);
        }
    }

    /** What the command line asked for. */
    record Arguments(Path dataDir, int port, int maxChunkMb) {
    }

    /** A command-line mistake; the message says what is wrong. */
    static final class UsageException extends Exception {
        private static final long serialVersionUID = 1L;

        UsageException(String message) {
            super(message);
        }
    }

    private final Config config;
    private final NodeDataDirectory folder;
    private final StorageNode node;
    private final HttpServer httpServer;
    private final ThreadPoolExecutor executor;
    private final int port;
    private boolean stopped = false;

    private StorageNodeServer(Config config, NodeDataDirectory folder, StorageNode node, HttpServer httpServer,
                              ThreadPoolExecutor executor) {
        this.config = config;
        this.folder = folder;
        this.node = node;
        this.httpServer = httpServer;
        this.executor = executor;
        this.port = httpServer.getAddress().getPort();
    }

    // ---------------------------------------------------------------------
    // Command line
    // ---------------------------------------------------------------------

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != EXIT_SUCCESS) {
            System.exit(exitCode);
        }
        // Started: the HTTP server's own (non-daemon) thread keeps the JVM running until Ctrl+C.
    }

    /** Parses the arguments and starts the node; returns an exit code instead of calling System.exit. */
    static int run(String[] args, PrintStream out, PrintStream err) {
        Arguments arguments;
        try {
            arguments = parseArgs(args);
        } catch (UsageException e) {
            err.println("Error: " + e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }
        StorageNodeServer server;
        try {
            server = start(Config.defaults(arguments.dataDir(), arguments.port(),
                    arguments.maxChunkMb() * BYTES_PER_MB, out, err));
        } catch (IOException | RuntimeException e) {
            err.println("Error: the storage node could not start: " + e.getMessage());
            for (Throwable cleanupError : e.getSuppressed()) {
                err.println("  Also, while undoing the start: " + cleanupError);
            }
            return EXIT_FAILURE;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "distributedfs-node-shutdown"));
        out.println("Press Ctrl+C to stop.");
        return EXIT_SUCCESS;
    }

    /**
     * Reads {@code --data-dir <folder>} and {@code --port <0-65535>} (both required) and
     * {@code --max-chunk-mb <1-64>} (default 16), each at most once, in any order.
     */
    static Arguments parseArgs(String[] args) throws UsageException {
        Path dataDir = null;
        Integer port = null;
        Integer maxChunkMb = null;
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            if (!option.equals("--data-dir") && !option.equals("--port") && !option.equals("--max-chunk-mb")) {
                throw new UsageException("unknown argument: " + option);
            }
            if (i + 1 >= args.length) {
                throw new UsageException(option + " needs a value");
            }
            String value = args[++i];
            if (option.equals("--data-dir")) {
                if (dataDir != null) {
                    throw new UsageException("--data-dir given more than once");
                }
                if (value.isBlank()) {
                    throw new UsageException("--data-dir must not be empty");
                }
                try {
                    dataDir = Path.of(value);
                } catch (InvalidPathException e) {
                    throw new UsageException("--data-dir is not a valid path: " + e.getMessage());
                }
            } else if (option.equals("--port")) {
                if (port != null) {
                    throw new UsageException("--port given more than once");
                }
                port = parseNumber("--port", value, 0, 65535);
            } else {
                if (maxChunkMb != null) {
                    throw new UsageException("--max-chunk-mb given more than once");
                }
                maxChunkMb = parseNumber("--max-chunk-mb", value, 1, MAX_CHUNK_MB_LIMIT);
            }
        }
        if (dataDir == null) {
            throw new UsageException("--data-dir is required");
        }
        if (port == null) {
            throw new UsageException("--port is required");
        }
        return new Arguments(dataDir, port, maxChunkMb == null ? DEFAULT_MAX_CHUNK_MB : maxChunkMb);
    }

    /** Plain decimal digits, no sign and no leading zeros, from min to max. */
    private static int parseNumber(String option, String value, int min, int max) throws UsageException {
        boolean digitsOnly = !value.isEmpty() && value.length() <= 5;
        for (int i = 0; i < value.length() && digitsOnly; i++) {
            digitsOnly = value.charAt(i) >= '0' && value.charAt(i) <= '9';
        }
        if (!digitsOnly || (value.length() > 1 && value.charAt(0) == '0')
                || Integer.parseInt(value) < min || Integer.parseInt(value) > max) {
            throw new UsageException(option + " must be a number from " + min + " to " + max
                    + " without leading zeros, but was \"" + value + "\"");
        }
        return Integer.parseInt(value);
    }

    // ---------------------------------------------------------------------
    // Starting and stopping
    // ---------------------------------------------------------------------

    /**
     * Starts a node in the order described in the class comment. On failure, everything already
     * started is undone (the folder's lock last) and the original failure is thrown.
     */
    static StorageNodeServer start(Config config) throws IOException {
        Objects.requireNonNull(config, "config must not be null");
        NodeDataDirectory folder = openFolder(config);
        HttpServer httpServer = null;
        ThreadPoolExecutor executor = null;
        try {
            StorageNode node = new StorageNode(folder.chunksDir());
            int removed = node.sweepTempFiles(message -> config.err().println("Warning: " + message));
            if (removed > 0) {
                config.out().println("Removed " + removed + " stale temp file" + (removed == 1 ? "" : "s")
                        + " from the chunk folder.");
            }

            executor = newExecutor(config);
            httpServer = HttpServer.create(new InetSocketAddress(LOOPBACK, config.port()), BACKLOG);
            httpServer.setExecutor(executor);
            int actualPort = httpServer.getAddress().getPort();
            httpServer.createContext("/", new NodeApiHandler(actualPort, folder.nodeId(), node, config.out(),
                    config.err(), config.maxChunkBytes(), config.stallTimeoutMillis(), config.lockWaitMillis()));
            httpServer.start();

            StorageNodeServer server = new StorageNodeServer(config, folder, node, httpServer, executor);
            if (folder.newlyInitialized()) {
                config.out().println("Initialized a new storage node folder: " + folder.root());
            }
            config.out().println("DistributedFS storage node " + folder.nodeId() + " listening on http://127.0.0.1:"
                    + actualPort + "/ (data directory: " + folder.root() + ")");
            return server;
        } catch (Throwable e) {
            shutDown(httpServer, executor, folder, 0, config.err(), e);
            throw e;
        }
    }

    /** The port the node listens on (the chosen one if the configuration said 0). */
    int port() {
        return port;
    }

    /** The node's ID, from its folder's marker. */
    String nodeId() {
        return folder.nodeId();
    }

    /** The folder the chunks are kept in (for tests). */
    Path chunksDir() {
        return folder.chunksDir();
    }

    /** The local node this server serves (for tests). */
    StorageNode node() {
        return node;
    }

    /** Stops the node and releases its folder. Calling it again does nothing. */
    synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        shutDown(httpServer, executor, folder, config.stopGraceSeconds(), config.err(), null);
        config.out().println("DistributedFS storage node stopped.");
    }

    private static void shutDown(HttpServer httpServer, ThreadPoolExecutor executor, NodeDataDirectory folder,
                                 int graceSeconds, PrintStream err, Throwable originalFailure) {
        try {
            if (httpServer != null) {
                try {
                    httpServer.stop(graceSeconds);
                } catch (RuntimeException e) {
                    cleanupFailed("stopping the HTTP server", e, err, originalFailure);
                }
            }
            if (executor != null) {
                try {
                    executor.shutdown();
                    if (!executor.awaitTermination(WORKER_WAIT_SECONDS, TimeUnit.SECONDS)) {
                        err.println("Warning: request threads were still running " + WORKER_WAIT_SECONDS
                                + " seconds after the node stopped; they were not interrupted");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    cleanupFailed("waiting for the request threads", e, err, originalFailure);
                } catch (RuntimeException e) {
                    cleanupFailed("stopping the request threads", e, err, originalFailure);
                }
            }
        } finally {
            try {
                folder.close();
            } catch (IOException | RuntimeException e) {
                cleanupFailed("releasing the node folder lock", e, err, originalFailure);
            }
        }
    }

    private static void cleanupFailed(String step, Exception failure, PrintStream err, Throwable originalFailure) {
        err.println("Warning: a cleanup step failed (" + step + "): " + failure);
        if (originalFailure != null) {
            originalFailure.addSuppressed(failure);
        }
    }

    /** Opens the node folder, retrying only "in use", every 50 ms for up to the configured time. */
    private static NodeDataDirectory openFolder(Config config) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.lockRetryMillis());
        while (true) {
            try {
                return NodeDataDirectory.open(config.dataDir());
            } catch (DataDirectoryException e) {
                if (e.reason() != DataDirectoryException.Reason.IN_USE || System.nanoTime() >= deadline) {
                    throw e;
                }
            }
            try {
                Thread.sleep(LOCK_RETRY_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while waiting for the node folder lock");
            }
        }
    }

    private static ThreadPoolExecutor newExecutor(Config config) {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "distributedfs-node-http-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
        return new ThreadPoolExecutor(config.workerThreads(), config.workerThreads(), 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.queueCapacity()), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    private static InetAddress loopbackAddress() {
        try {
            return InetAddress.getByAddress("127.0.0.1", new byte[] {127, 0, 0, 1});
        } catch (IOException e) {
            throw new IllegalStateException("cannot create the address 127.0.0.1", e);
        }
    }
}
