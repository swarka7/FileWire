package filewire.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Streaming SHA-256 helpers that never load an entire file into memory. */
public final class DigestUtil {
    private static final int BUFFER_BYTES = ProtocolConstants.MAX_CHUNK_BYTES;

    private DigestUtil() {
    }

    public static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("The Java runtime does not provide SHA-256", impossible);
        }
    }

    public static byte[] sha256(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        try (InputStream input = Files.newInputStream(path)) {
            return sha256(input);
        }
    }

    /** Reads to EOF but deliberately leaves the caller-owned stream open. */
    public static byte[] sha256(InputStream input) throws IOException {
        Objects.requireNonNull(input, "input");
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[BUFFER_BYTES];
        int read;
        while ((read = input.read(buffer)) != -1) {
            if (read > 0) {
                digest.update(buffer, 0, read);
            }
        }
        return digest.digest();
    }

    public static boolean matches(byte[] expected, byte[] actual) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(actual, "actual");
        return MessageDigest.isEqual(expected, actual);
    }

    public static String toHex(byte[] digest) {
        Objects.requireNonNull(digest, "digest");
        return HexFormat.of().formatHex(digest);
    }
}
