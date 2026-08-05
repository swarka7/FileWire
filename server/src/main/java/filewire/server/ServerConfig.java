package filewire.server;

import filewire.protocol.ProtocolConstants;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/** Immutable runtime limits for a {@link FileWireServer}. */
public record ServerConfig(
        Path storageRoot,
        int port,
        int workerThreads,
        int queueCapacity,
        int socketReadTimeoutMillis,
        long maxFileSizeBytes,
        Duration shutdownTimeout) {

    public static final int DEFAULT_SOCKET_READ_TIMEOUT_MILLIS = 60_000;
    public static final long DEFAULT_MAX_FILE_SIZE_BYTES = 16L * 1024 * 1024 * 1024;
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    public ServerConfig {
        storageRoot = Objects.requireNonNull(storageRoot, "storageRoot").toAbsolutePath().normalize();
        shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (workerThreads < 1) {
            throw new IllegalArgumentException("workerThreads must be positive");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }
        Math.addExact(workerThreads, queueCapacity);
        if (socketReadTimeoutMillis
                < ProtocolConstants.PREPARATION_KEEPALIVE_INTERVAL_MILLIS * 2) {
            throw new IllegalArgumentException(
                    "socketReadTimeoutMillis must be at least twice the preparation keepalive interval");
        }
        if (maxFileSizeBytes < 0) {
            throw new IllegalArgumentException("maxFileSizeBytes must not be negative");
        }
        long shutdownTimeoutMillis;
        try {
            shutdownTimeoutMillis = shutdownTimeout.toMillis();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("shutdownTimeout must fit in milliseconds", overflow);
        }
        if (shutdownTimeoutMillis < 1) {
            throw new IllegalArgumentException("shutdownTimeout must be at least one millisecond");
        }
    }

    public static ServerConfig defaults(Path storageRoot, int port) {
        int workers = Math.max(2, Math.min(16, Runtime.getRuntime().availableProcessors() * 2));
        return new ServerConfig(
                storageRoot,
                port,
                workers,
                workers * 2,
                DEFAULT_SOCKET_READ_TIMEOUT_MILLIS,
                DEFAULT_MAX_FILE_SIZE_BYTES,
                DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public int maximumConnections() {
        return Math.addExact(workerThreads, queueCapacity);
    }
}
