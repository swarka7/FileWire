package filewire.client;

import filewire.protocol.AtomicFilePublisher;
import filewire.protocol.DigestUtil;
import filewire.protocol.ProtocolConstants;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** A create-only local download that becomes visible only after verification. */
final class AtomicDownload implements AutoCloseable {
    private static final LinkOption[] NO_FOLLOW = {LinkOption.NOFOLLOW_LINKS};
    private static final Set<Path> RESERVED_DESTINATIONS = ConcurrentHashMap.newKeySet();

    private final Path root;
    private final Path destination;
    private final Path temporaryFile;
    private final MessageDigest digest = DigestUtil.newSha256();
    private final AtomicBoolean reservationHeld = new AtomicBoolean(true);
    private OutputStream output;
    private long bytesWritten;
    private boolean committed;

    private AtomicDownload(Path root, Path destination, Path temporaryFile, OutputStream output) {
        this.root = root;
        this.destination = destination;
        this.temporaryFile = temporaryFile;
        this.output = output;
    }

    static Path prepareRoot(Path downloadRoot) throws IOException {
        Objects.requireNonNull(downloadRoot, "downloadRoot");
        Path normalized = downloadRoot.toAbsolutePath().normalize();
        if (Files.exists(normalized, NO_FOLLOW) && Files.isSymbolicLink(normalized)) {
            throw new IOException("Download root must not be a symbolic link: " + normalized);
        }
        Files.createDirectories(normalized);
        if (!Files.isDirectory(normalized, NO_FOLLOW)) {
            throw new IOException("Download root is not a directory: " + normalized);
        }
        return normalized.toRealPath();
    }

    static AtomicDownload open(Path preparedRoot, Path requestedDestination) throws IOException {
        Objects.requireNonNull(preparedRoot, "preparedRoot");
        Objects.requireNonNull(requestedDestination, "requestedDestination");

        Path root = preparedRoot.toAbsolutePath().normalize();
        verifyRoot(root);
        Path destination = requestedDestination.isAbsolute()
                ? requestedDestination.toAbsolutePath().normalize()
                : root.resolve(requestedDestination).normalize();
        verifyDestination(root, destination);

        Path parent = destination.getParent();
        verifyDirectoryChain(root, parent);
        if (!RESERVED_DESTINATIONS.add(destination)) {
            throw new IOException("Download destination is already reserved: " + destination);
        }

        Path temporaryFile = null;
        OutputStream output = null;
        try {
            verifyDestination(root, destination);
            verifyDirectoryChain(root, parent);
            temporaryFile = Files.createTempFile(parent, ".filewire-", ".part");
            output = new BufferedOutputStream(Files.newOutputStream(
                    temporaryFile,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING),
                    ProtocolConstants.MAX_CHUNK_BYTES);
            return new AtomicDownload(root, destination, temporaryFile, output);
        } catch (IOException | RuntimeException | Error exception) {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
            }
            if (temporaryFile != null) {
                try {
                    Files.deleteIfExists(temporaryFile);
                } catch (IOException deleteFailure) {
                    exception.addSuppressed(deleteFailure);
                }
            }
            RESERVED_DESTINATIONS.remove(destination);
            throw exception;
        }
    }

    void write(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        ensureOpen();
        output.write(bytes);
        digest.update(bytes);
        try {
            bytesWritten = Math.addExact(bytesWritten, bytes.length);
        } catch (ArithmeticException exception) {
            throw new IOException("Downloaded byte count overflowed", exception);
        }
    }

    Path commit(long expectedSize, byte[] expectedDigest) throws IOException {
        try {
            Objects.requireNonNull(expectedDigest, "expectedDigest");
            if (expectedSize < 0) {
                throw new IllegalArgumentException("expectedSize must not be negative");
            }
            if (expectedDigest.length != ProtocolConstants.SHA256_BYTES) {
                throw new IllegalArgumentException("expectedDigest must contain 32 bytes");
            }
            ensureOpen();
            closeOutput();

            byte[] actualDigest = digest.digest();
            if (bytesWritten != expectedSize) {
                throw new IOException(
                        "Download size mismatch: expected " + expectedSize
                                + " bytes but received " + bytesWritten);
            }
            if (!DigestUtil.matches(expectedDigest, actualDigest)) {
                throw new IOException("Download SHA-256 verification failed");
            }

            verifyDestination(root, destination);
            verifyDirectoryChain(root, destination.getParent());
            AtomicFilePublisher.publishCreateOnly(temporaryFile, destination);
            committed = true;
            return destination;
        } catch (IOException | RuntimeException | Error exception) {
            cleanupAfterFailure(exception);
            throw exception;
        } finally {
            releaseReservation();
        }
    }

    long bytesWritten() {
        return bytesWritten;
    }

    Path destination() {
        return destination;
    }

    Path temporaryFile() {
        return temporaryFile;
    }

    private static void verifyRoot(Path root) throws IOException {
        if (Files.isSymbolicLink(root) || !Files.isDirectory(root, NO_FOLLOW)) {
            throw new IOException("Download root is not a safe directory: " + root);
        }
    }

    private static void verifyDestination(Path root, Path destination) throws IOException {
        if (destination.equals(root) || !destination.startsWith(root)) {
            throw new IOException("Download destination escapes the configured root: " + destination);
        }
        if (Files.exists(destination, NO_FOLLOW)) {
            throw new IOException("Download destination already exists: " + destination);
        }
    }

    private static void verifyDirectoryChain(Path root, Path directory) throws IOException {
        if (directory == null || !directory.startsWith(root)) {
            throw new IOException("Download destination has no safe parent directory");
        }

        Path current = root;
        verifyDirectory(current);
        for (Path component : root.relativize(directory)) {
            current = current.resolve(component);
            verifyDirectory(current);
        }

        Path realRoot = root.toRealPath();
        Path realDirectory = directory.toRealPath();
        if (!realDirectory.startsWith(realRoot)) {
            throw new IOException("Download destination resolves outside the configured root");
        }
    }

    private static void verifyDirectory(Path directory) throws IOException {
        if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, NO_FOLLOW)) {
            throw new IOException("Download path contains an unsafe directory: " + directory);
        }
    }

    private void ensureOpen() throws IOException {
        if (output == null) {
            throw new IOException("Download is already finalized");
        }
    }

    private void closeOutput() throws IOException {
        OutputStream current = output;
        output = null;
        if (current != null) {
            current.close();
        }
    }

    private void cleanupAfterFailure(Throwable primary) {
        try {
            closeOutput();
        } catch (IOException closeFailure) {
            primary.addSuppressed(closeFailure);
        }
        try {
            Files.deleteIfExists(temporaryFile);
        } catch (IOException deleteFailure) {
            primary.addSuppressed(deleteFailure);
        }
    }

    private void releaseReservation() {
        if (reservationHeld.compareAndSet(true, false)) {
            RESERVED_DESTINATIONS.remove(destination);
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            try {
                closeOutput();
            } catch (IOException exception) {
                failure = exception;
            }
            if (!committed) {
                try {
                    Files.deleteIfExists(temporaryFile);
                } catch (IOException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
        } finally {
            releaseReservation();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
