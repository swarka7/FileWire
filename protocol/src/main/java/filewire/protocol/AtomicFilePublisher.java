package filewire.protocol;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;

/** Publishes a completed staging file without ever replacing an existing destination. */
public final class AtomicFilePublisher {
    private AtomicFilePublisher() {
    }

    /**
     * Makes {@code source} visible at {@code target} with create-only semantics.
     *
     * <p>A hard link is preferred because creating the final directory entry is atomic and fails
     * when the target already exists. Providers without hard-link support fall back to a move
     * without {@code REPLACE_EXISTING}; that fallback preserves the no-overwrite guarantee even
     * when the provider cannot promise atomic publication.</p>
     */
    public static void publishCreateOnly(Path source, Path target) throws IOException {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");

        Path normalizedSource = source.toAbsolutePath().normalize();
        Path normalizedTarget = target.toAbsolutePath().normalize();
        if (normalizedSource.equals(normalizedTarget)) {
            throw new IllegalArgumentException("Source and target must be different paths");
        }

        IOException linkFailure;
        try {
            Files.createLink(normalizedTarget, normalizedSource);
            removeStagingLink(normalizedSource);
            return;
        } catch (FileAlreadyExistsException collision) {
            throw collision;
        } catch (UnsupportedOperationException unsupported) {
            linkFailure = new IOException("The filesystem does not support hard-link publication", unsupported);
        } catch (IOException failure) {
            linkFailure = failure;
        }

        if (Files.exists(normalizedTarget, LinkOption.NOFOLLOW_LINKS)) {
            FileAlreadyExistsException collision = new FileAlreadyExistsException(normalizedTarget.toString());
            collision.addSuppressed(linkFailure);
            throw collision;
        }

        try {
            Files.move(normalizedSource, normalizedTarget);
        } catch (IOException moveFailure) {
            moveFailure.addSuppressed(linkFailure);
            throw moveFailure;
        }
    }

    private static void removeStagingLink(Path source) {
        try {
            Files.delete(source);
        } catch (IOException | RuntimeException cleanupFailure) {
            // The verified target is already published. Preserve success and retry cleanup at JVM exit.
            try {
                source.toFile().deleteOnExit();
            } catch (RuntimeException ignored) {
                // Publication already succeeded; cleanup failure cannot be reported as transfer failure.
            }
        }
    }
}
