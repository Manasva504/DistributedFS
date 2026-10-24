package com.distributedfs;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The DistributedFS HTTP server: it opens the data directory, prepares the storage, and serves
 * the HTTP API on 127.0.0.1.
 *
 * <p>Usage: {@code java -cp out com.distributedfs.DistributedFsServer --data-dir <folder> [--port <0-65535>]
 * [--max-upload-mb <1-1024>] [--node 127.0.0.1:<port> ...]} (port 8080 by default; 0 picks any free
 * port and prints it; uploads up to 100 MiB by default). Stop it with Ctrl+C.
 *
 * <p><b>Storage nodes.</b> Without {@code --node}, the server keeps its chunks in three local node
 * folders inside its data directory, as it always has. With one to sixteen {@code --node} options,
 * it uses storage node processes ({@link StorageNodeServer}) on those 127.0.0.1 ports instead,
 * through {@link RemoteStorageNode}. Every node must answer its health check with a node ID, the IDs
 * must be distinct, and they must match the data directory's membership file
 * ({@link ClusterMembership}), which the first start in remote mode writes.
 *
 * <p><b>Startup order.</b> Nothing listens until every step has succeeded:
 * <ol>
 *   <li>Open the data directory ({@link DataDirectory#open}), which checks, initializes and locks
 *       it. If another server holds the lock, opening is retried for up to 2 seconds, because
 *       Windows releases a killed process's lock shortly after the process ends.</li>
 *   <li>Create the storage nodes: the local node folders, or, in remote mode, a client per
 *       {@code --node}, each of which must answer its health check; then check the node IDs
 *       against the membership file. Then create the cluster and the manifest store.</li>
 *   <li>Run startup recovery ({@link StartupRecovery}): finish or undo interrupted uploads, remove
 *       stale temp files, and report unreferenced chunks. If recovery cannot establish that acting
 *       is safe, the server does not start. Then create the file service.</li>
 *   <li>Bind to 127.0.0.1 only, on the given port, and start serving.</li>
 * </ol>
 * If any step fails, everything already started is undone in reverse order, and the data
 * directory's lock is always released last. The original failure is what the caller sees;
 * failures while undoing are logged and attached to it as suppressed exceptions.
 *
 * <p><b>Threads.</b> Requests run on a fixed pool of worker threads with a bounded queue. When the
 * queue is full, the JDK's server closes the new connection without a response (its dispatcher
 * catches the executor's rejection and closes the connection), and keeps serving.
 *
 * <p><b>Stopping</b> ({@link #stop}) closes the listening socket, waits up to the grace period for
 * requests in progress (on an idle server the JDK waits the whole period), lets the worker threads
 * finish without interrupting them, and then releases the data directory's lock, even if an
 * earlier step failed.
 */
public final class DistributedFsServer {

    static final int DEFAULT_PORT = 8080;
    static final int EXIT_SUCCESS = 0;
    static final int EXIT_FAILURE = 1;
    static final int EXIT_USAGE = 2;

    static final String USAGE =
            "Usage: java -cp out com.distributedfs.DistributedFsServer --data-dir <folder> [--port <0-65535>]"
                    + " [--max-upload-mb <1-1024>] [--node 127.0.0.1:<port> ...]";

    static final int DEFAULT_MAX_UPLOAD_MB = 100;
    static final int MAX_UPLOAD_MB_LIMIT = 1024;
    private static final long BYTES_PER_MB = 1024L * 1024L;

    /** How long stop waits for worker threads to finish their current request. */
    private static final int WORKER_WAIT_SECONDS = 5;
    private static final long LOCK_RETRY_INTERVAL_MILLIS = 50;
    private static final int BACKLOG = 50;

    /** The server binds to this address only: IPv4 loopback, so no other machine can connect. */
    private static final InetAddress LOOPBACK = loopbackAddress();

    /**
     * Server settings. Production uses {@link #defaults}; tests use smaller pools, no grace
     * period and a shorter lock retry.
     *
     * @param port            0 to 65535; 0 picks any free port
     * @param workerThreads   number of request threads, at least 1
     * @param queueCapacity   requests that may wait for a thread, at least 1
     * @param stopGraceSeconds how long stop waits for requests in progress, 0 or more
     * @param lockRetryMillis how long to keep retrying a data directory that is in use, 0 or more
     * @param maxUploadBytes  the largest upload accepted, at least 1
     * @param chunkSize       the chunk size for new uploads, at least 1
     * @param nodePorts       the 127.0.0.1 ports of remote storage nodes, distinct, at most 16;
     *                        empty for local node folders (the default)
     * @param remoteNodes     time and size limits for talking to remote nodes
     */
    record Config(Path dataDir, int port, int workerThreads, int queueCapacity, int stopGraceSeconds,
                  long lockRetryMillis, PrintStream out, PrintStream err, long maxUploadBytes, int chunkSize,
                  List<Integer> nodePorts, RemoteNodeConfig remoteNodes) {

        Config {
            Objects.requireNonNull(dataDir, "dataDir must not be null");
            Objects.requireNonNull(out, "out must not be null");
            Objects.requireNonNull(err, "err must not be null");
            Objects.requireNonNull(remoteNodes, "remoteNodes must not be null");
            nodePorts = List.copyOf(Objects.requireNonNull(nodePorts, "nodePorts must not be null"));
            if (nodePorts.size() > ClusterMembership.MAX_NODES || nodePorts.stream().distinct().count() != nodePorts.size()
                    || nodePorts.stream().anyMatch(p -> p < 1 || p > 65535)) {
                throw new IllegalArgumentException("nodePorts must be at most " + ClusterMembership.MAX_NODES
                        + " distinct ports from 1 to 65535");
            }
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port must be 0 to 65535, but was " + port);
            }
            if (workerThreads < 1 || queueCapacity < 1 || stopGraceSeconds < 0 || lockRetryMillis < 0) {
                throw new IllegalArgumentException("invalid thread, queue, grace or retry setting");
            }
            if (maxUploadBytes < 1 || chunkSize < 1) {
                throw new IllegalArgumentException("invalid upload limit or chunk size");
            }
        }

        /** Local node folders, with the given upload limit and chunk size. */
        Config(Path dataDir, int port, int workerThreads, int queueCapacity, int stopGraceSeconds,
               long lockRetryMillis, PrintStream out, PrintStream err, long maxUploadBytes, int chunkSize) {
            this(dataDir, port, workerThreads, queueCapacity, stopGraceSeconds, lockRetryMillis, out, err,
                    maxUploadBytes, chunkSize, List.of(), RemoteNodeConfig.defaults());
        }

        /** Local node folders, the default upload limit (100 MiB) and chunk size (1 MiB). */
        Config(Path dataDir, int port, int workerThreads, int queueCapacity, int stopGraceSeconds,
               long lockRetryMillis, PrintStream out, PrintStream err) {
            this(dataDir, port, workerThreads, queueCapacity, stopGraceSeconds, lockRetryMillis, out, err,
                    DEFAULT_MAX_UPLOAD_MB * BYTES_PER_MB, FileChunker.DEFAULT_CHUNK_SIZE);
        }

        static Config defaults(Path dataDir, int port, long maxUploadBytes, PrintStream out, PrintStream err) {
            return defaults(dataDir, port, maxUploadBytes, List.of(), out, err);
        }

        static Config defaults(Path dataDir, int port, long maxUploadBytes, List<Integer> nodePorts,
                               PrintStream out, PrintStream err) {
            return new Config(dataDir, port, 4, 64, 2, 2000, out, err, maxUploadBytes, FileChunker.DEFAULT_CHUNK_SIZE,
                    nodePorts, RemoteNodeConfig.defaults());
        }

        /** True if the chunks are on remote storage nodes rather than in local node folders. */
        boolean remote() {
            return !nodePorts.isEmpty();
        }
    }

    /** Creates the request handler for the actual port. Tests supply one with failure hooks. */
    interface HandlerFactory {
        FileApiHandler create(int port, PrintStream err, FileService files);
    }

    /** What the command line asked for; {@code nodePorts} is empty for local node folders. */
    record Arguments(Path dataDir, int port, int maxUploadMb, List<Integer> nodePorts) {
    }

    /** A command-line mistake; the message says what is wrong. */
    static final class UsageException extends Exception {
        private static final long serialVersionUID = 1L;

        UsageException(String message) {
            super(message);
        }
    }

    private final Config config;
    private final DataDirectory dataDirectory;
    private final StorageCluster cluster;
    private final ManifestStore manifests;
    private final FileService files;
    private final HttpServer httpServer;
    private final ThreadPoolExecutor executor;
    private final int port;
    private final List<RemoteStorageNode> remoteNodes;
    private boolean stopped = false;

    private DistributedFsServer(Config config, DataDirectory dataDirectory, StorageCluster cluster,
                                ManifestStore manifests, FileService files, HttpServer httpServer,
                                ThreadPoolExecutor executor, List<RemoteStorageNode> remoteNodes) {
        this.remoteNodes = List.copyOf(remoteNodes);
        this.config = config;
        this.dataDirectory = dataDirectory;
        this.cluster = cluster;
        this.manifests = manifests;
        this.files = files;
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
        // Started: the server's own (non-daemon) thread keeps the JVM running until Ctrl+C,
        // which runs the shutdown hook registered by run.
    }

    /**
     * Parses the arguments and starts the server, and returns an exit code instead of calling
     * System.exit, so tests can check usage errors and startup failures. On success the server
     * keeps running and a shutdown hook stops it when the JVM exits.
     */
    static int run(String[] args, PrintStream out, PrintStream err) {
        Arguments arguments;
        try {
            arguments = parseArgs(args);
        } catch (UsageException e) {
            err.println("Error: " + e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }
        DistributedFsServer server;
        try {
            server = start(Config.defaults(arguments.dataDir(), arguments.port(),
                    arguments.maxUploadMb() * BYTES_PER_MB, arguments.nodePorts(), out, err));
        } catch (IOException | RuntimeException e) {
            err.println("Error: the server could not start: " + e.getMessage());
            for (Throwable cleanupError : e.getSuppressed()) {
                err.println("  Also, while undoing the start: " + cleanupError);
            }
            return EXIT_FAILURE;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "distributedfs-shutdown"));
        out.println("Press Ctrl+C to stop.");
        return EXIT_SUCCESS;
    }

    /**
     * Reads {@code --data-dir <folder>} (required), {@code --port <0-65535>} (default 8080) and
     * {@code --max-upload-mb <1-1024>} (default 100), each at most once, and {@code --node
     * 127.0.0.1:<1-65535>} once per remote storage node (none for local node folders; at most 16,
     * each port once), in any order.
     */
    static Arguments parseArgs(String[] args) throws UsageException {
        Path dataDir = null;
        Integer port = null;
        Integer maxUploadMb = null;
        List<Integer> nodePorts = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            if (!option.equals("--data-dir") && !option.equals("--port") && !option.equals("--max-upload-mb")
                    && !option.equals("--node")) {
                throw new UsageException("unknown argument: " + option);
            }
            if (i + 1 >= args.length) {
                throw new UsageException(option + " needs a value");
            }
            String value = args[++i];
            if (option.equals("--node")) {
                int nodePort = parseNode(value);
                if (nodePorts.contains(nodePort)) {
                    throw new UsageException("--node 127.0.0.1:" + nodePort + " given more than once");
                }
                if (nodePorts.size() == ClusterMembership.MAX_NODES) {
                    throw new UsageException("at most " + ClusterMembership.MAX_NODES + " --node options may be given");
                }
                nodePorts.add(nodePort);
            } else if (option.equals("--data-dir")) {
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
                port = parsePort(value);
            } else {
                if (maxUploadMb != null) {
                    throw new UsageException("--max-upload-mb given more than once");
                }
                maxUploadMb = parseMaxUploadMb(value);
            }
        }
        if (dataDir == null) {
            throw new UsageException("--data-dir is required");
        }
        return new Arguments(dataDir, port == null ? DEFAULT_PORT : port,
                maxUploadMb == null ? DEFAULT_MAX_UPLOAD_MB : maxUploadMb, List.copyOf(nodePorts));
    }

    /**
     * {@code 127.0.0.1:<port>} exactly, with the port as plain digits from 1 to 65535 without
     * leading zeros. Storage nodes listen on 127.0.0.1 only, so no other host is accepted.
     */
    private static int parseNode(String value) throws UsageException {
        String prefix = "127.0.0.1:";
        String portText = value.startsWith(prefix) ? value.substring(prefix.length()) : "";
        boolean digitsOnly = !portText.isEmpty() && portText.length() <= 5 && portText.charAt(0) != '0';
        for (int i = 0; i < portText.length() && digitsOnly; i++) {
            digitsOnly = portText.charAt(i) >= '0' && portText.charAt(i) <= '9';
        }
        if (!digitsOnly || Integer.parseInt(portText) > 65535) {
            throw new UsageException("--node must be 127.0.0.1:<port> with a port from 1 to 65535 without leading"
                    + " zeros, but was \"" + value + "\"");
        }
        return Integer.parseInt(portText);
    }

    /** Plain decimal digits, no sign and no leading zeros, 1 to 1024. */
    private static int parseMaxUploadMb(String value) throws UsageException {
        boolean digitsOnly = !value.isEmpty() && value.length() <= 4;
        for (int i = 0; i < value.length() && digitsOnly; i++) {
            digitsOnly = value.charAt(i) >= '0' && value.charAt(i) <= '9';
        }
        if (!digitsOnly || value.charAt(0) == '0' || Integer.parseInt(value) > MAX_UPLOAD_MB_LIMIT) {
            throw new UsageException("--max-upload-mb must be a number from 1 to " + MAX_UPLOAD_MB_LIMIT
                    + " without leading zeros, but was \"" + value + "\"");
        }
        return Integer.parseInt(value);
    }

    /** Plain decimal digits, no sign and no leading zeros, 0 to 65535. */
    private static int parsePort(String value) throws UsageException {
        boolean digitsOnly = !value.isEmpty() && value.length() <= 5;
        for (int i = 0; i < value.length() && digitsOnly; i++) {
            digitsOnly = value.charAt(i) >= '0' && value.charAt(i) <= '9';
        }
        if (!digitsOnly || (value.length() > 1 && value.charAt(0) == '0')) {
            throw new UsageException("--port must be a number from 0 to 65535 without leading zeros, but was \""
                    + value + "\"");
        }
        int port = Integer.parseInt(value);
        if (port > 65535) {
            throw new UsageException("--port must be a number from 0 to 65535, but was " + port);
        }
        return port;
    }

    // ---------------------------------------------------------------------
    // Starting and stopping
    // ---------------------------------------------------------------------

    /** Starts a server with the normal request handler. */
    static DistributedFsServer start(Config config) throws IOException {
        return start(config, FileApiHandler::new);
    }

    /**
     * Starts a server in the order described in the class comment. On failure, everything
     * already started is undone (the data directory's lock last) and the original failure is
     * thrown, with any failures from undoing attached as suppressed.
     */
    static DistributedFsServer start(Config config, HandlerFactory handlerFactory) throws IOException {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(handlerFactory, "handlerFactory must not be null");
        DataDirectory dataDirectory = openDataDirectory(config);
        HttpServer httpServer = null;
        ThreadPoolExecutor executor = null;
        List<RemoteStorageNode> remoteNodes = new ArrayList<>();
        try {
            List<ChunkStore> nodes = new ArrayList<>();
            if (config.remote()) {
                remoteNodes.addAll(connectNodes(config));
                List<String> ids = new ArrayList<>();
                for (RemoteStorageNode node : remoteNodes) {
                    ids.add(node.nodeId().orElseThrow());
                }
                if (ClusterMembership.requireRemoteMembers(dataDirectory.root(), dataDirectory.nodeDirs(), ids)) {
                    config.out().println("Recorded the " + ids.size() + " storage node" + (ids.size() == 1 ? "" : "s")
                            + " of this data directory in " + ClusterMembership.FILE_NAME);
                }
                nodes.addAll(remoteNodes);
            } else {
                ClusterMembership.requireLocalMode(dataDirectory.root());
                for (Path nodeDir : dataDirectory.nodeDirs()) {
                    nodes.add(new StorageNode(nodeDir));
                }
            }
            StorageCluster cluster = new StorageCluster(nodes);
            ManifestStore manifests = new ManifestStore(dataDirectory.manifestsDir());
            new StartupRecovery(nodes, cluster, manifests, dataDirectory.tmpDir(), config.out(), config.err()).run();
            FileService files = new FileService(cluster, manifests, dataDirectory.tmpDir(), config.chunkSize(),
                    config.maxUploadBytes(), config.err());

            executor = newExecutor(config);
            httpServer = HttpServer.create(new InetSocketAddress(LOOPBACK, config.port()), BACKLOG);
            httpServer.setExecutor(executor);
            int actualPort = httpServer.getAddress().getPort();
            httpServer.createContext("/", handlerFactory.create(actualPort, config.err(), files));
            httpServer.start();

            DistributedFsServer server = new DistributedFsServer(config, dataDirectory, cluster, manifests,
                    files, httpServer, executor, remoteNodes);
            if (dataDirectory.newlyInitialized()) {
                config.out().println("Initialized a new data directory: " + dataDirectory.root());
            }
            for (int i = 0; i < remoteNodes.size(); i++) {
                config.out().println("Node " + i + ": " + remoteNodes.get(i).describe());
            }
            config.out().println("DistributedFS server listening on http://127.0.0.1:" + actualPort
                    + "/ (data directory: " + dataDirectory.root() + ")");
            return server;
        } catch (Throwable e) {
            shutDown(httpServer, executor, dataDirectory, 0, config.err(), e);
            closeAll(remoteNodes);
            throw e;
        }
    }

    /**
     * One client per {@code --node}, each pinned to the node ID its health check answered. Every
     * node must answer, and no two may be the same node (two ports of one process, which cannot
     * happen today, or two clients that reach the same node through a proxy).
     */
    private static List<RemoteStorageNode> connectNodes(Config config) throws IOException {
        List<RemoteStorageNode> connected = new ArrayList<>();
        try {
            for (int nodePort : config.nodePorts()) {
                RemoteStorageNode node;
                try {
                    node = RemoteStorageNode.connect(nodePort, config.remoteNodes());
                } catch (IOException e) {
                    throw new IOException("the storage node --node 127.0.0.1:" + nodePort + " did not answer: "
                            + e.getMessage(), e);
                }
                for (RemoteStorageNode earlier : connected) {
                    if (earlier.nodeId().equals(node.nodeId())) {
                        node.close();
                        throw new IOException("--node 127.0.0.1:" + earlier.port() + " and --node 127.0.0.1:" + nodePort
                                + " are the same storage node (node ID " + node.nodeId().orElseThrow() + ")");
                    }
                }
                connected.add(node);
            }
            return connected;
        } catch (IOException | RuntimeException e) {
            closeAll(connected);
            throw e;
        }
    }

    private static void closeAll(List<RemoteStorageNode> nodes) {
        for (RemoteStorageNode node : nodes) {
            node.close();
        }
    }

    /** The port the server listens on (the chosen one if the configuration said 0). */
    int port() {
        return port;
    }

    /** The data directory's root folder. */
    Path dataRoot() {
        return dataDirectory.root();
    }

    /** The request thread pool, for tests that need to see its queue. */
    ThreadPoolExecutor executor() {
        return executor;
    }

    /** The storage this server will serve files from (used by the upload and download endpoints). */
    StorageCluster cluster() {
        return cluster;
    }

    ManifestStore manifests() {
        return manifests;
    }

    /** The service that stores uploads. */
    FileService files() {
        return files;
    }

    /**
     * Stops the server and releases the data directory. Calling it again does nothing. Failures
     * of individual steps are logged and do not stop the later steps; the lock is released last.
     */
    synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        shutDown(httpServer, executor, dataDirectory, config.stopGraceSeconds(), config.err(), null);
        closeAll(remoteNodes);
        config.out().println("DistributedFS server stopped.");
    }

    /**
     * Undoes a start, or stops a running server: stop the HTTP server, let the worker threads
     * finish (without interrupting them), and always release the data directory last, in a
     * finally block. Each failure is logged to {@code err}; if {@code originalFailure} is given,
     * it is also attached to it as suppressed, so the original failure stays the one reported.
     * Null server or executor means that step had not happened yet.
     */
    static void shutDown(HttpServer httpServer, ExecutorService executor, DataDirectory dataDirectory,
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
                                + " seconds after the server stopped; they were not interrupted");
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
                dataDirectory.close();
            } catch (IOException | RuntimeException e) {
                cleanupFailed("releasing the data directory lock", e, err, originalFailure);
            }
        }
    }

    private static void cleanupFailed(String step, Exception failure, PrintStream err, Throwable originalFailure) {
        err.println("Warning: a cleanup step failed (" + step + "): " + failure);
        if (originalFailure != null) {
            originalFailure.addSuppressed(failure);
        }
    }

    // ---------------------------------------------------------------------
    // Startup steps
    // ---------------------------------------------------------------------

    /**
     * Opens the data directory. Only "in use" is retried, every 50 ms for up to the configured
     * time: Windows releases a killed process's file lock asynchronously, so a restart right after
     * a crash can briefly see the old lock. Every other refusal fails at once.
     */
    private static DataDirectory openDataDirectory(Config config) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.lockRetryMillis());
        while (true) {
            try {
                return DataDirectory.open(config.dataDir());
            } catch (DataDirectoryException e) {
                if (e.reason() != DataDirectoryException.Reason.IN_USE || System.nanoTime() >= deadline) {
                    throw e;
                }
            }
            try {
                Thread.sleep(LOCK_RETRY_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while waiting for the data directory lock");
            }
        }
    }

    private static ThreadPoolExecutor newExecutor(Config config) {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "distributedfs-http-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
        // AbortPolicy: a full queue rejects the request; the JDK's dispatcher then closes that
        // connection without a response.
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
