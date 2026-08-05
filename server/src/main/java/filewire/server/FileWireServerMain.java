package filewire.server;

import java.io.IOException;
import java.nio.file.Path;

/** Command-line entry point for the FileWire server. */
public final class FileWireServerMain {
    private static final int DEFAULT_PORT = 7777;
    private static final Path DEFAULT_STORAGE = Path.of("server-files");

    private FileWireServerMain() {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (IllegalArgumentException invalidInput) {
            System.err.println("Could not start FileWire server: " + invalidInput.getMessage());
        } catch (IOException startupFailure) {
            System.err.println("Could not start FileWire server: " + startupFailure.getMessage());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            System.err.println("FileWire server was interrupted");
        }
    }

    private static void run(String[] args) throws IOException, InterruptedException {
        if (args.length > 2) {
            System.err.println("Usage: FileWireServerMain [port] [storage-directory]");
            return;
        }
        int port = args.length >= 1 ? parsePort(args[0]) : DEFAULT_PORT;
        Path storage = args.length == 2 ? Path.of(args[1]) : DEFAULT_STORAGE;

        try (FileWireServer server = new FileWireServer(ServerConfig.defaults(storage, port))) {
            Thread shutdownHook = new Thread(server::close, "filewire-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                server.start();
                System.out.printf(
                        "FileWire listening on port %d; storage: %s%n",
                        server.port(),
                        storage.toAbsolutePath().normalize());
                server.awaitTermination();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // The JVM is already running shutdown hooks.
                }
            }
        }
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 0 || port > 65_535) {
                throw new IllegalArgumentException("Port must be between 0 and 65535");
            }
            return port;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Port must be an integer: " + value, exception);
        }
    }
}
