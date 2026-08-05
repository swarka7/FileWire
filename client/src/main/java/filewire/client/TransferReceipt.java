package filewire.client;

import java.util.Objects;

/** Summary of a successfully verified upload or download. */
public record TransferReceipt(String filename, long bytesTransferred, String sha256) {
    public TransferReceipt {
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(sha256, "sha256");
        if (bytesTransferred < 0) {
            throw new IllegalArgumentException("bytesTransferred must not be negative");
        }
        if (!sha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("sha256 must be a lowercase SHA-256 digest");
        }
    }
}
